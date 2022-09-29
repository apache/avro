// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Logic handling the intermediate representation of Avro values.
use crate::{
    decimal::Decimal,
    duration::Duration,
    schema::{
        Name, NamesRef, Precision, RecordField, ResolvedSchema, Scale, Schema, SchemaKind,
        UnionSchema,
    },
    AvroResult, Error,
};
use serde_json::{Number, Value as JsonValue};
use std::{
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
    hash::BuildHasher,
    str::FromStr,
};
use uuid::Uuid;

/// Compute the maximum decimal value precision of a byte array of length `len` could hold.
fn max_prec_for_len(len: usize) -> Result<usize, Error> {
    let len = i32::try_from(len).map_err(|e| Error::ConvertLengthToI32(e, len))?;
    Ok((2.0_f64.powi(8 * len - 1) - 1.0).log10().floor() as usize)
}

/// A valid Avro value.
///
/// More information about Avro values can be found in the [Avro
/// Specification](https://avro.apache.org/docs/current/spec.html#schemas)
#[derive(Clone, Debug, PartialEq, strum_macros::EnumDiscriminants)]
#[strum_discriminants(name(ValueKind))]
pub enum Value {
    /// A `null` Avro value.
    Null,
    /// A `boolean` Avro value.
    Boolean(bool),
    /// A `int` Avro value.
    Int(i32),
    /// A `long` Avro value.
    Long(i64),
    /// A `float` Avro value.
    Float(f32),
    /// A `double` Avro value.
    Double(f64),
    /// A `bytes` Avro value.
    Bytes(Vec<u8>),
    /// A `string` Avro value.
    String(String),
    /// A `fixed` Avro value.
    /// The size of the fixed value is represented as a `usize`.
    Fixed(usize, Vec<u8>),
    /// An `enum` Avro value.
    ///
    /// An Enum is represented by a symbol and its position in the symbols list
    /// of its corresponding schema.
    /// This allows schema-less encoding, as well as schema resolution while
    /// reading values.
    Enum(u32, String),
    /// An `union` Avro value.
    ///
    /// A Union is represented by the value it holds and its position in the type list
    /// of its corresponding schema
    /// This allows schema-less encoding, as well as schema resolution while
    /// reading values.
    Union(u32, Box<Value>),
    /// An `array` Avro value.
    Array(Vec<Value>),
    /// A `map` Avro value.
    Map(HashMap<String, Value>),
    /// A `record` Avro value.
    ///
    /// A Record is represented by a vector of (`<record name>`, `value`).
    /// This allows schema-less encoding.
    ///
    /// See [Record](types.Record) for a more user-friendly support.
    Record(Vec<(String, Value)>),
    /// A date value.
    ///
    /// Serialized and deserialized as `i32` directly. Can only be deserialized properly with a
    /// schema.
    Date(i32),
    /// An Avro Decimal value. Bytes are in big-endian order, per the Avro spec.
    Decimal(Decimal),
    /// Time in milliseconds.
    TimeMillis(i32),
    /// Time in microseconds.
    TimeMicros(i64),
    /// Timestamp in milliseconds.
    TimestampMillis(i64),
    /// Timestamp in microseconds.
    TimestampMicros(i64),
    /// Avro Duration. An amount of time defined by months, days and milliseconds.
    Duration(Duration),
    /// Universally unique identifier.
    Uuid(Uuid),
}
/// Any structure implementing the [ToAvro](trait.ToAvro.html) trait will be usable
/// from a [Writer](../writer/struct.Writer.html).
#[deprecated(
    since = "0.11.0",
    note = "Please use Value::from, Into::into or value.into() instead"
)]
pub trait ToAvro {
    /// Transforms this value into an Avro-compatible [Value](enum.Value.html).
    fn avro(self) -> Value;
}

#[allow(deprecated)]
impl<T: Into<Value>> ToAvro for T {
    fn avro(self) -> Value {
        self.into()
    }
}

macro_rules! to_value(
    ($type:ty, $variant_constructor:expr) => (
        impl From<$type> for Value {
            fn from(value: $type) -> Self {
                $variant_constructor(value)
            }
        }
    );
);

to_value!(bool, Value::Boolean);
to_value!(i32, Value::Int);
to_value!(i64, Value::Long);
to_value!(f32, Value::Float);
to_value!(f64, Value::Double);
to_value!(String, Value::String);
to_value!(Vec<u8>, Value::Bytes);
to_value!(uuid::Uuid, Value::Uuid);
to_value!(Decimal, Value::Decimal);
to_value!(Duration, Value::Duration);

impl From<()> for Value {
    fn from(_: ()) -> Self {
        Self::Null
    }
}

impl From<usize> for Value {
    fn from(value: usize) -> Self {
        i64::try_from(value)
            .expect("cannot convert usize to i64")
            .into()
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Self::Bytes(value.to_owned())
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Self>,
{
    fn from(value: Option<T>) -> Self {
        // FIXME: this is incorrect in case first type in union is not "none"
        Self::Union(
            value.is_some() as u32,
            Box::new(value.map_or_else(|| Self::Null, Into::into)),
        )
    }
}

impl<K, V, S> From<HashMap<K, V, S>> for Value
where
    K: Into<String>,
    V: Into<Self>,
    S: BuildHasher,
{
    fn from(value: HashMap<K, V, S>) -> Self {
        Self::Map(
            value
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        )
    }
}

/// Utility interface to build `Value::Record` objects.
#[derive(Debug, Clone)]
pub struct Record<'a> {
    /// List of fields contained in the record.
    /// Ordered according to the fields in the schema given to create this
    /// `Record` object. Any unset field defaults to `Value::Null`.
    pub fields: Vec<(String, Value)>,
    schema_lookup: &'a BTreeMap<String, usize>,
}

impl<'a> Record<'a> {
    /// Create a `Record` given a `Schema`.
    ///
    /// If the `Schema` is not a `Schema::Record` variant, `None` will be returned.
    pub fn new(schema: &Schema) -> Option<Record> {
        match *schema {
            Schema::Record {
                fields: ref schema_fields,
                lookup: ref schema_lookup,
                ..
            } => {
                let mut fields = Vec::with_capacity(schema_fields.len());
                for schema_field in schema_fields.iter() {
                    fields.push((schema_field.name.clone(), Value::Null));
                }

                Some(Record {
                    fields,
                    schema_lookup,
                })
            }
            _ => None,
        }
    }

    /// Put a compatible value (implementing the `ToAvro` trait) in the
    /// `Record` for a given `field` name.
    ///
    /// **NOTE** Only ensure that the field name is present in the `Schema` given when creating
    /// this `Record`. Does not perform any schema validation.
    pub fn put<V>(&mut self, field: &str, value: V)
    where
        V: Into<Value>,
    {
        if let Some(&position) = self.schema_lookup.get(field) {
            self.fields[position].1 = value.into()
        }
    }
}

impl<'a> From<Record<'a>> for Value {
    fn from(value: Record<'a>) -> Self {
        Self::Record(value.fields)
    }
}

impl From<JsonValue> for Value {
    fn from(value: JsonValue) -> Self {
        match value {
            JsonValue::Null => Self::Null,
            JsonValue::Bool(b) => b.into(),
            JsonValue::Number(ref n) if n.is_i64() => Value::Long(n.as_i64().unwrap()),
            JsonValue::Number(ref n) if n.is_f64() => Value::Double(n.as_f64().unwrap()),
            JsonValue::Number(n) => Value::Long(n.as_u64().unwrap() as i64), // TODO: Not so great
            JsonValue::String(s) => s.into(),
            JsonValue::Array(items) => Value::Array(items.into_iter().map(Value::from).collect()),
            JsonValue::Object(items) => Value::Map(
                items
                    .into_iter()
                    .map(|(key, value)| (key, value.into()))
                    .collect(),
            ),
        }
    }
}

/// Convert Avro values to Json values
impl std::convert::TryFrom<Value> for JsonValue {
    type Error = crate::error::Error;
    fn try_from(value: Value) -> AvroResult<Self> {
        match value {
            Value::Null => Ok(Self::Null),
            Value::Boolean(b) => Ok(Self::Bool(b)),
            Value::Int(i) => Ok(Self::Number(i.into())),
            Value::Long(l) => Ok(Self::Number(l.into())),
            Value::Float(f) => Number::from_f64(f.into())
                .map(Self::Number)
                .ok_or_else(|| Error::ConvertF64ToJson(f.into())),
            Value::Double(d) => Number::from_f64(d)
                .map(Self::Number)
                .ok_or(Error::ConvertF64ToJson(d)),
            Value::Bytes(bytes) => Ok(Self::Array(bytes.into_iter().map(|b| b.into()).collect())),
            Value::String(s) => Ok(Self::String(s)),
            Value::Fixed(_size, items) => {
                Ok(Self::Array(items.into_iter().map(|v| v.into()).collect()))
            }
            Value::Enum(_i, s) => Ok(Self::String(s)),
            Value::Union(_i, b) => Self::try_from(*b),
            Value::Array(items) => items
                .into_iter()
                .map(Self::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Array),
            Value::Map(items) => items
                .into_iter()
                .map(|(key, value)| Self::try_from(value).map(|v| (key, v)))
                .collect::<Result<Vec<_>, _>>()
                .map(|v| Self::Object(v.into_iter().collect())),
            Value::Record(items) => items
                .into_iter()
                .map(|(key, value)| Self::try_from(value).map(|v| (key, v)))
                .collect::<Result<Vec<_>, _>>()
                .map(|v| Self::Object(v.into_iter().collect())),
            Value::Date(d) => Ok(Self::Number(d.into())),
            Value::Decimal(ref d) => <Vec<u8>>::try_from(d)
                .map(|vec| Self::Array(vec.into_iter().map(|v| v.into()).collect())),
            Value::TimeMillis(t) => Ok(Self::Number(t.into())),
            Value::TimeMicros(t) => Ok(Self::Number(t.into())),
            Value::TimestampMillis(t) => Ok(Self::Number(t.into())),
            Value::TimestampMicros(t) => Ok(Self::Number(t.into())),
            Value::Duration(d) => Ok(Self::Array(
                <[u8; 12]>::from(d).iter().map(|&v| v.into()).collect(),
            )),
            Value::Uuid(uuid) => Ok(Self::String(uuid.as_hyphenated().to_string())),
        }
    }
}

impl Value {
    /// Validate the value against the given [Schema](../schema/enum.Schema.html).
    ///
    /// See the [Avro specification](https://avro.apache.org/docs/current/spec.html)
    /// for the full set of rules of schema validation.
    pub fn validate(&self, schema: &Schema) -> bool {
        let rs = ResolvedSchema::try_from(schema).expect("Schema didn't successfully parse");
        match self.validate_internal(schema, rs.get_names()) {
            Some(error_msg) => {
                error!(
                    "Invalid value: {:?} for schema: {:?}. Reason: {}",
                    self, schema, error_msg
                );
                false
            }
            None => true,
        }
    }

    fn accumulate(accumulator: Option<String>, other: Option<String>) -> Option<String> {
        match (accumulator, other) {
            (None, None) => None,
            (None, s @ Some(_)) => s,
            (s @ Some(_), None) => s,
            (Some(reason1), Some(reason2)) => Some(format!("{}\n{}", reason1, reason2)),
        }
    }

    pub(crate) fn validate_internal<S: std::borrow::Borrow<Schema>>(
        &self,
        schema: &Schema,
        names: &HashMap<Name, S>,
    ) -> Option<String> {
        match (self, schema) {
            (_, &Schema::Ref { ref name }) => names.get(name).map_or_else(
                || {
                    Some(format!(
                        "Unresolved schema reference: '{}'. Parsed names: {:?}",
                        name,
                        names.keys()
                    ))
                },
                |s| self.validate_internal(s.borrow(), names),
            ),
            (&Value::Null, &Schema::Null) => None,
            (&Value::Boolean(_), &Schema::Boolean) => None,
            (&Value::Int(_), &Schema::Int) => None,
            (&Value::Int(_), &Schema::Date) => None,
            (&Value::Int(_), &Schema::TimeMillis) => None,
            (&Value::Int(_), &Schema::Long) => None,
            (&Value::Long(_), &Schema::Long) => None,
            (&Value::Long(_), &Schema::TimeMicros) => None,
            (&Value::Long(_), &Schema::TimestampMillis) => None,
            (&Value::Long(_), &Schema::TimestampMicros) => None,
            (&Value::TimestampMicros(_), &Schema::TimestampMicros) => None,
            (&Value::TimestampMillis(_), &Schema::TimestampMillis) => None,
            (&Value::TimeMicros(_), &Schema::TimeMicros) => None,
            (&Value::TimeMillis(_), &Schema::TimeMillis) => None,
            (&Value::Date(_), &Schema::Date) => None,
            (&Value::Decimal(_), &Schema::Decimal { .. }) => None,
            (&Value::Duration(_), &Schema::Duration) => None,
            (&Value::Uuid(_), &Schema::Uuid) => None,
            (&Value::Float(_), &Schema::Float) => None,
            (&Value::Double(_), &Schema::Double) => None,
            (&Value::Bytes(_), &Schema::Bytes) => None,
            (&Value::Bytes(_), &Schema::Decimal { .. }) => None,
            (&Value::String(_), &Schema::String) => None,
            (&Value::String(_), &Schema::Uuid) => None,
            (&Value::Fixed(n, _), &Schema::Fixed { size, .. }) => {
                if n != size {
                    Some(format!(
                        "The value's size ({}) is different than the schema's size ({})",
                        n, size
                    ))
                } else {
                    None
                }
            }
            (&Value::Bytes(ref b), &Schema::Fixed { size, .. }) => {
                if b.len() != size {
                    Some(format!(
                        "The bytes' length ({}) is different than the schema's size ({})",
                        b.len(),
                        size
                    ))
                } else {
                    None
                }
            }
            (&Value::Fixed(n, _), &Schema::Duration) => {
                if n != 12 {
                    Some(format!(
                        "The value's size ('{}') must be exactly 12 to be a Duration",
                        n
                    ))
                } else {
                    None
                }
            }
            // TODO: check precision against n
            (&Value::Fixed(_n, _), &Schema::Decimal { .. }) => None,
            (&Value::String(ref s), &Schema::Enum { ref symbols, .. }) => {
                if !symbols.contains(s) {
                    Some(format!("'{}' is not a member of the possible symbols", s))
                } else {
                    None
                }
            }
            (&Value::Enum(i, ref s), &Schema::Enum { ref symbols, .. }) => symbols
                .get(i as usize)
                .map(|ref symbol| {
                    if symbol != &s {
                        Some(format!("Symbol '{}' is not at position '{}'", s, i))
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| Some(format!("No symbol at position '{}'", i))),
            // (&Value::Union(None), &Schema::Union(_)) => None,
            (&Value::Union(i, ref value), &Schema::Union(ref inner)) => inner
                .variants()
                .get(i as usize)
                .map(|schema| value.validate_internal(schema, names))
                .unwrap_or_else(|| Some(format!("No schema in the union at position '{}'", i))),
            (v, &Schema::Union(ref inner)) => match inner.find_schema(v) {
                Some(_) => None,
                None => Some("Could not find matching type in union".to_string()),
            },
            (&Value::Array(ref items), &Schema::Array(ref inner)) => {
                items.iter().fold(None, |acc, item| {
                    Value::accumulate(acc, item.validate_internal(inner, names))
                })
            }
            (&Value::Map(ref items), &Schema::Map(ref inner)) => {
                items.iter().fold(None, |acc, (_, value)| {
                    Value::accumulate(acc, value.validate_internal(inner, names))
                })
            }
            (
                &Value::Record(ref record_fields),
                &Schema::Record {
                    ref fields,
                    ref lookup,
                    ..
                },
            ) => {
                let non_nullable_fields_count =
                    fields.iter().filter(|&rf| !rf.is_nullable()).count();

                if record_fields.len() < non_nullable_fields_count {
                    return Some(format!(
                        "The value's records length ({}) doesn't match the schema ({} non-nullable fields)",
                        record_fields.len(),
                        non_nullable_fields_count
                    ));
                } else if record_fields.len() > fields.len() {
                    return Some(format!(
                        "The value's records length ({}) is greater than the schema's ({} fields)",
                        record_fields.len(),
                        fields.len(),
                    ));
                }

                record_fields
                    .iter()
                    .fold(None, |acc, (field_name, record_field)| {
                        match lookup.get(field_name) {
                            Some(idx) => {
                                let field = &fields[*idx];
                                Value::accumulate(
                                    acc,
                                    record_field.validate_internal(&field.schema, names),
                                )
                            }
                            None => Value::accumulate(
                                acc,
                                Some(format!(
                                    "There is no schema field for field '{}'",
                                    field_name
                                )),
                            ),
                        }
                    })
            }
            (&Value::Map(ref items), &Schema::Record { ref fields, .. }) => {
                fields.iter().fold(None, |acc, field| {
                    if let Some(item) = items.get(&field.name) {
                        let res = item.validate_internal(&field.schema, names);
                        Value::accumulate(acc, res)
                    } else if !field.is_nullable() {
                        Value::accumulate(
                            acc,
                            Some(format!(
                                "Field with name '{:?}' is not a member of the map items",
                                field.name
                            )),
                        )
                    } else {
                        acc
                    }
                })
            }
            (_v, _s) => Some("Unsupported value-schema combination".to_string()),
        }
    }

    /// Attempt to perform schema resolution on the value, with the given
    /// [Schema](../schema/enum.Schema.html).
    ///
    /// See [Schema Resolution](https://avro.apache.org/docs/current/spec.html#Schema+Resolution)
    /// in the Avro specification for the full set of rules of schema
    /// resolution.
    pub fn resolve(self, schema: &Schema) -> AvroResult<Self> {
        // FIXME transition to using resolved Schema
        let rs = ResolvedSchema::try_from(schema)?;
        self.resolve_internal(schema, rs.get_names())
    }

    fn resolve_internal(mut self, schema: &Schema, names: &NamesRef) -> AvroResult<Self> {
        // Check if this schema is a union, and if the reader schema is not.
        if SchemaKind::from(&self) == SchemaKind::Union
            && SchemaKind::from(schema) != SchemaKind::Union
        {
            // Pull out the Union, and attempt to resolve against it.
            let v = match self {
                Value::Union(_i, b) => *b,
                _ => unreachable!(),
            };
            self = v;
        }
        match *schema {
            Schema::Ref { ref name } => {
                if let Some(resolved) = names.get(name) {
                    self.resolve_internal(resolved, names)
                } else {
                    Err(Error::SchemaResolutionError(name.clone()))
                }
            }
            Schema::Null => self.resolve_null(),
            Schema::Boolean => self.resolve_boolean(),
            Schema::Int => self.resolve_int(),
            Schema::Long => self.resolve_long(),
            Schema::Float => self.resolve_float(),
            Schema::Double => self.resolve_double(),
            Schema::Bytes => self.resolve_bytes(),
            Schema::String => self.resolve_string(),
            Schema::Fixed { size, .. } => self.resolve_fixed(size),
            Schema::Union(ref inner) => self.resolve_union(inner, names),
            Schema::Enum { ref symbols, .. } => self.resolve_enum(symbols),
            Schema::Array(ref inner) => self.resolve_array(inner, names),
            Schema::Map(ref inner) => self.resolve_map(inner, names),
            Schema::Record { ref fields, .. } => self.resolve_record(fields, names),
            Schema::Decimal {
                scale,
                precision,
                ref inner,
            } => self.resolve_decimal(precision, scale, inner),
            Schema::Date => self.resolve_date(),
            Schema::TimeMillis => self.resolve_time_millis(),
            Schema::TimeMicros => self.resolve_time_micros(),
            Schema::TimestampMillis => self.resolve_timestamp_millis(),
            Schema::TimestampMicros => self.resolve_timestamp_micros(),
            Schema::Duration => self.resolve_duration(),
            Schema::Uuid => self.resolve_uuid(),
        }
    }

    fn resolve_uuid(self) -> Result<Self, Error> {
        Ok(match self {
            uuid @ Value::Uuid(_) => uuid,
            Value::String(ref string) => {
                Value::Uuid(Uuid::from_str(string).map_err(Error::ConvertStrToUuid)?)
            }
            other => return Err(Error::GetUuid(other.into())),
        })
    }

    fn resolve_duration(self) -> Result<Self, Error> {
        Ok(match self {
            duration @ Value::Duration { .. } => duration,
            Value::Fixed(size, bytes) => {
                if size != 12 {
                    return Err(Error::GetDecimalFixedBytes(size));
                }
                Value::Duration(Duration::from([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                    bytes[8], bytes[9], bytes[10], bytes[11],
                ]))
            }
            other => return Err(Error::ResolveDuration(other.into())),
        })
    }

    fn resolve_decimal(
        self,
        precision: Precision,
        scale: Scale,
        inner: &Schema,
    ) -> Result<Self, Error> {
        if scale > precision {
            return Err(Error::GetScaleAndPrecision { scale, precision });
        }
        match inner {
            &Schema::Fixed { size, .. } => {
                if max_prec_for_len(size)? < precision {
                    return Err(Error::GetScaleWithFixedSize { size, precision });
                }
            }
            Schema::Bytes => (),
            _ => return Err(Error::ResolveDecimalSchema(inner.into())),
        };
        match self {
            Value::Decimal(num) => {
                let num_bytes = num.len();
                if max_prec_for_len(num_bytes)? > precision {
                    Err(Error::ComparePrecisionAndSize {
                        precision,
                        num_bytes,
                    })
                } else {
                    Ok(Value::Decimal(num))
                }
                // check num.bits() here
            }
            Value::Fixed(_, bytes) | Value::Bytes(bytes) => {
                if max_prec_for_len(bytes.len())? > precision {
                    Err(Error::ComparePrecisionAndSize {
                        precision,
                        num_bytes: bytes.len(),
                    })
                } else {
                    // precision and scale match, can we assume the underlying type can hold the data?
                    Ok(Value::Decimal(Decimal::from(bytes)))
                }
            }
            other => Err(Error::ResolveDecimal(other.into())),
        }
    }

    fn resolve_date(self) -> Result<Self, Error> {
        match self {
            Value::Date(d) | Value::Int(d) => Ok(Value::Date(d)),
            other => Err(Error::GetDate(other.into())),
        }
    }

    fn resolve_time_millis(self) -> Result<Self, Error> {
        match self {
            Value::TimeMillis(t) | Value::Int(t) => Ok(Value::TimeMillis(t)),
            other => Err(Error::GetTimeMillis(other.into())),
        }
    }

    fn resolve_time_micros(self) -> Result<Self, Error> {
        match self {
            Value::TimeMicros(t) | Value::Long(t) => Ok(Value::TimeMicros(t)),
            Value::Int(t) => Ok(Value::TimeMicros(i64::from(t))),
            other => Err(Error::GetTimeMicros(other.into())),
        }
    }

    fn resolve_timestamp_millis(self) -> Result<Self, Error> {
        match self {
            Value::TimestampMillis(ts) | Value::Long(ts) => Ok(Value::TimestampMillis(ts)),
            Value::Int(ts) => Ok(Value::TimestampMillis(i64::from(ts))),
            other => Err(Error::GetTimestampMillis(other.into())),
        }
    }

    fn resolve_timestamp_micros(self) -> Result<Self, Error> {
        match self {
            Value::TimestampMicros(ts) | Value::Long(ts) => Ok(Value::TimestampMicros(ts)),
            Value::Int(ts) => Ok(Value::TimestampMicros(i64::from(ts))),
            other => Err(Error::GetTimestampMicros(other.into())),
        }
    }

    fn resolve_null(self) -> Result<Self, Error> {
        match self {
            Value::Null => Ok(Value::Null),
            other => Err(Error::GetNull(other.into())),
        }
    }

    fn resolve_boolean(self) -> Result<Self, Error> {
        match self {
            Value::Boolean(b) => Ok(Value::Boolean(b)),
            other => Err(Error::GetBoolean(other.into())),
        }
    }

    fn resolve_int(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Int(n)),
            Value::Long(n) => Ok(Value::Int(n as i32)),
            other => Err(Error::GetInt(other.into())),
        }
    }

    fn resolve_long(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Long(i64::from(n))),
            Value::Long(n) => Ok(Value::Long(n)),
            other => Err(Error::GetLong(other.into())),
        }
    }

    fn resolve_float(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Float(n as f32)),
            Value::Long(n) => Ok(Value::Float(n as f32)),
            Value::Float(x) => Ok(Value::Float(x)),
            Value::Double(x) => Ok(Value::Float(x as f32)),
            other => Err(Error::GetFloat(other.into())),
        }
    }

    fn resolve_double(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Double(f64::from(n))),
            Value::Long(n) => Ok(Value::Double(n as f64)),
            Value::Float(x) => Ok(Value::Double(f64::from(x))),
            Value::Double(x) => Ok(Value::Double(x)),
            other => Err(Error::GetDouble(other.into())),
        }
    }

    fn resolve_bytes(self) -> Result<Self, Error> {
        match self {
            Value::Bytes(bytes) => Ok(Value::Bytes(bytes)),
            Value::String(s) => Ok(Value::Bytes(s.into_bytes())),
            Value::Array(items) => Ok(Value::Bytes(
                items
                    .into_iter()
                    .map(Value::try_u8)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            other => Err(Error::GetBytes(other.into())),
        }
    }

    fn resolve_string(self) -> Result<Self, Error> {
        match self {
            Value::String(s) => Ok(Value::String(s)),
            Value::Bytes(bytes) | Value::Fixed(_, bytes) => Ok(Value::String(
                String::from_utf8(bytes).map_err(Error::ConvertToUtf8)?,
            )),
            other => Err(Error::GetString(other.into())),
        }
    }

    fn resolve_fixed(self, size: usize) -> Result<Self, Error> {
        match self {
            Value::Fixed(n, bytes) => {
                if n == size {
                    Ok(Value::Fixed(n, bytes))
                } else {
                    Err(Error::CompareFixedSizes { size, n })
                }
            }
            Value::String(s) => Ok(Value::Fixed(s.len(), s.into_bytes())),
            other => Err(Error::GetStringForFixed(other.into())),
        }
    }

    fn resolve_enum(self, symbols: &[String]) -> Result<Self, Error> {
        let validate_symbol = |symbol: String, symbols: &[String]| {
            if let Some(index) = symbols.iter().position(|item| item == &symbol) {
                Ok(Value::Enum(index as u32, symbol))
            } else {
                Err(Error::GetEnumDefault {
                    symbol,
                    symbols: symbols.into(),
                })
            }
        };

        match self {
            Value::Enum(raw_index, s) => {
                let index = usize::try_from(raw_index)
                    .map_err(|e| Error::ConvertU32ToUsize(e, raw_index))?;
                if (0..=symbols.len()).contains(&index) {
                    validate_symbol(s, symbols)
                } else {
                    Err(Error::GetEnumValue {
                        index,
                        nsymbols: symbols.len(),
                    })
                }
            }
            Value::String(s) => validate_symbol(s, symbols),
            other => Err(Error::GetEnum(other.into())),
        }
    }

    fn resolve_union(self, schema: &UnionSchema, names: &NamesRef) -> Result<Self, Error> {
        let v = match self {
            // Both are unions case.
            Value::Union(_i, v) => *v,
            // Reader is a union, but writer is not.
            v => v,
        };

        // Find the first match in the reader schema.
        // FIXME: this might be wrong when the union consists of multiple same records that have different names
        let (i, inner) = schema.find_schema(&v).ok_or(Error::FindUnionVariant)?;
        Ok(Value::Union(
            i as u32,
            Box::new(v.resolve_internal(inner, names)?),
        ))
    }

    fn resolve_array(self, schema: &Schema, names: &NamesRef) -> Result<Self, Error> {
        match self {
            Value::Array(items) => Ok(Value::Array(
                items
                    .into_iter()
                    .map(|item| item.resolve_internal(schema, names))
                    .collect::<Result<_, _>>()?,
            )),
            other => Err(Error::GetArray {
                expected: schema.into(),
                other: other.into(),
            }),
        }
    }

    fn resolve_map(self, schema: &Schema, names: &NamesRef) -> Result<Self, Error> {
        match self {
            Value::Map(items) => Ok(Value::Map(
                items
                    .into_iter()
                    .map(|(key, value)| {
                        value
                            .resolve_internal(schema, names)
                            .map(|value| (key, value))
                    })
                    .collect::<Result<_, _>>()?,
            )),
            other => Err(Error::GetMap {
                expected: schema.into(),
                other: other.into(),
            }),
        }
    }

    fn resolve_record(self, fields: &[RecordField], names: &NamesRef) -> Result<Self, Error> {
        let mut items = match self {
            Value::Map(items) => Ok(items),
            Value::Record(fields) => Ok(fields.into_iter().collect::<HashMap<_, _>>()),
            other => Err(Error::GetRecord {
                expected: fields
                    .iter()
                    .map(|field| (field.name.clone(), field.schema.clone().into()))
                    .collect(),
                other: other.into(),
            }),
        }?;

        let new_fields = fields
            .iter()
            .map(|field| {
                let value = match items.remove(&field.name) {
                    Some(value) => value,
                    None => match field.default {
                        Some(ref value) => match field.schema {
                            Schema::Enum { ref symbols, .. } => {
                                Value::from(value.clone()).resolve_enum(symbols)?
                            }
                            Schema::Union(ref union_schema) => {
                                let first = &union_schema.variants()[0];
                                // NOTE: this match exists only to optimize null defaults for large
                                // backward-compatible schemas with many nullable fields
                                match first {
                                    Schema::Null => Value::Union(0, Box::new(Value::Null)),
                                    _ => Value::Union(
                                        0,
                                        Box::new(
                                            Value::from(value.clone())
                                                .resolve_internal(first, names)?,
                                        ),
                                    ),
                                }
                            }
                            _ => Value::from(value.clone()),
                        },
                        None => {
                            return Err(Error::GetField(field.name.clone()));
                        }
                    },
                };
                value
                    .resolve_internal(&field.schema, names)
                    .map(|value| (field.name.clone(), value))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Value::Record(new_fields))
    }

    fn try_u8(self) -> AvroResult<u8> {
        let int = self.resolve(&Schema::Int)?;
        if let Value::Int(n) = int {
            if n >= 0 && n <= i32::from(u8::MAX) {
                return Ok(n as u8);
            }
        }

        Err(Error::GetU8(int.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        decimal::Decimal,
        duration::{Days, Duration, Millis, Months},
        schema::{Name, RecordField, RecordFieldOrder, Schema, UnionSchema},
        to_value,
        types::Value,
    };
    use apache_avro_test_helper::logger::{assert_logged, assert_not_logged};
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[test]
    fn validate() {
        let value_schema_valid = vec![
            (Value::Int(42), Schema::Int, true, ""),
            (
                Value::Int(42),
                Schema::Boolean,
                false,
                "Invalid value: Int(42) for schema: Boolean. Reason: Unsupported value-schema combination",
            ),
            (
                Value::Union(0, Box::new(Value::Null)),
                Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int]).unwrap()),
                true,
                "",
            ),
            (
                Value::Union(1, Box::new(Value::Int(42))),
                Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int]).unwrap()),
                true,
                "",
            ),
            (
                Value::Union(0, Box::new(Value::Null)),
                Schema::Union(UnionSchema::new(vec![Schema::Double, Schema::Int]).unwrap()),
                false,
                "Invalid value: Union(0, Null) for schema: Union(UnionSchema { schemas: [Double, Int], variant_index: {Int: 1, Double: 0} }). Reason: Unsupported value-schema combination",
            ),
            (
                Value::Union(3, Box::new(Value::Int(42))),
                Schema::Union(
                    UnionSchema::new(vec![
                        Schema::Null,
                        Schema::Double,
                        Schema::String,
                        Schema::Int,
                    ])
                    .unwrap(),
                ),
                true,
                "",
            ),
            (
                Value::Union(1, Box::new(Value::Long(42i64))),
                Schema::Union(
                    UnionSchema::new(vec![Schema::Null, Schema::TimestampMillis]).unwrap(),
                ),
                true,
                "",
            ),
            (
                Value::Union(2, Box::new(Value::Long(1_i64))),
                Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int]).unwrap()),
                false,
                "Invalid value: Union(2, Long(1)) for schema: Union(UnionSchema { schemas: [Null, Int], variant_index: {Null: 0, Int: 1} }). Reason: No schema in the union at position '2'",
            ),
            (
                Value::Array(vec![Value::Long(42i64)]),
                Schema::Array(Box::new(Schema::Long)),
                true,
                "",
            ),
            (
                Value::Array(vec![Value::Boolean(true)]),
                Schema::Array(Box::new(Schema::Long)),
                false,
                "Invalid value: Array([Boolean(true)]) for schema: Array(Long). Reason: Unsupported value-schema combination",
            ),
            (Value::Record(vec![]), Schema::Null, false, "Invalid value: Record([]) for schema: Null. Reason: Unsupported value-schema combination"),
            (
                Value::Fixed(12, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
                Schema::Duration,
                true,
                "",
            ),
            (
                Value::Fixed(11, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
                Schema::Duration,
                false,
                "Invalid value: Fixed(11, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) for schema: Duration. Reason: The value's size ('11') must be exactly 12 to be a Duration",
            ),
            (
                Value::Record(vec![("unknown_field_name".to_string(), Value::Null)]),
                Schema::Record {
                    name: Name::new("record_name").unwrap(),
                    aliases: None,
                    doc: None,
                    fields: vec![RecordField {
                        name: "field_name".to_string(),
                        doc: None,
                        default: None,
                        schema: Schema::Int,
                        order: RecordFieldOrder::Ignore,
                        position: 0,
                        custom_attributes: Default::default(),
                    }],
                    lookup: Default::default(),
                    attributes: Default::default(),
                },
                false,
                r#"Invalid value: Record([("unknown_field_name", Null)]) for schema: Record { name: Name { name: "record_name", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "field_name", doc: None, default: None, schema: Int, order: Ignore, position: 0, custom_attributes: {} }], lookup: {}, attributes: {} }. Reason: There is no schema field for field 'unknown_field_name'"#,
            ),
            (
                Value::Record(vec![("field_name".to_string(), Value::Null)]),
                Schema::Record {
                    name: Name::new("record_name").unwrap(),
                    aliases: None,
                    doc: None,
                    fields: vec![RecordField {
                        name: "field_name".to_string(),
                        doc: None,
                        default: None,
                        schema: Schema::Ref {
                            name: Name::new("missing").unwrap(),
                        },
                        order: RecordFieldOrder::Ignore,
                        position: 0,
                        custom_attributes: Default::default(),
                    }],
                    lookup: [("field_name".to_string(), 0)].iter().cloned().collect(),
                    attributes: Default::default(),
                },
                false,
                r#"Invalid value: Record([("field_name", Null)]) for schema: Record { name: Name { name: "record_name", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "field_name", doc: None, default: None, schema: Ref { name: Name { name: "missing", namespace: None } }, order: Ignore, position: 0, custom_attributes: {} }], lookup: {"field_name": 0}, attributes: {} }. Reason: Unresolved schema reference: 'missing'. Parsed names: []"#,
            ),
        ];

        for (value, schema, valid, expected_err_message) in value_schema_valid.into_iter() {
            let err_message = value.validate_internal::<Schema>(&schema, &HashMap::default());
            assert_eq!(valid, err_message.is_none());
            if !valid {
                let full_err_message = format!(
                    "Invalid value: {:?} for schema: {:?}. Reason: {}",
                    value,
                    schema,
                    err_message.unwrap()
                );
                assert_eq!(expected_err_message, full_err_message);
            }
        }
    }

    #[test]
    fn validate_fixed() {
        let schema = Schema::Fixed {
            size: 4,
            name: Name::new("some_fixed").unwrap(),
            aliases: None,
            doc: None,
            attributes: Default::default(),
        };

        assert!(Value::Fixed(4, vec![0, 0, 0, 0]).validate(&schema));
        let value = Value::Fixed(5, vec![0, 0, 0, 0, 0]);
        assert!(!value.validate(&schema));
        assert_logged(
            format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema, "The value's size (5) is different than the schema's size (4)"
            )
            .as_str(),
        );

        assert!(Value::Bytes(vec![0, 0, 0, 0]).validate(&schema));
        let value = Value::Bytes(vec![0, 0, 0, 0, 0]);
        assert!(!value.validate(&schema));
        assert_logged(
            format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema, "The bytes' length (5) is different than the schema's size (4)"
            )
            .as_str(),
        );
    }

    #[test]
    fn validate_enum() {
        let schema = Schema::Enum {
            name: Name::new("some_enum").unwrap(),
            aliases: None,
            doc: None,
            symbols: vec![
                "spades".to_string(),
                "hearts".to_string(),
                "diamonds".to_string(),
                "clubs".to_string(),
            ],
            attributes: Default::default(),
        };

        assert!(Value::Enum(0, "spades".to_string()).validate(&schema));
        assert!(Value::String("spades".to_string()).validate(&schema));

        let value = Value::Enum(1, "spades".to_string());
        assert!(!value.validate(&schema));
        assert_logged(
            format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema, "Symbol 'spades' is not at position '1'"
            )
            .as_str(),
        );

        let value = Value::Enum(1000, "spades".to_string());
        assert!(!value.validate(&schema));
        assert_logged(
            format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema, "No symbol at position '1000'"
            )
            .as_str(),
        );

        let value = Value::String("lorem".to_string());
        assert!(!value.validate(&schema));
        assert_logged(
            format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema, "'lorem' is not a member of the possible symbols"
            )
            .as_str(),
        );

        let other_schema = Schema::Enum {
            name: Name::new("some_other_enum").unwrap(),
            aliases: None,
            doc: None,
            symbols: vec![
                "hearts".to_string(),
                "diamonds".to_string(),
                "clubs".to_string(),
                "spades".to_string(),
            ],
            attributes: Default::default(),
        };

        let value = Value::Enum(0, "spades".to_string());
        assert!(!value.validate(&other_schema));
        assert_logged(
            format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, other_schema, "Symbol 'spades' is not at position '0'"
            )
            .as_str(),
        );
    }

    #[test]
    fn validate_record() {
        // {
        //    "type": "record",
        //    "fields": [
        //      {"type": "long", "name": "a"},
        //      {"type": "string", "name": "b"},
        //      {
        //          "type": ["null", "int"]
        //          "name": "c",
        //          "default": null
        //      }
        //    ]
        // }
        let schema = Schema::Record {
            name: Name::new("some_record").unwrap(),
            aliases: None,
            doc: None,
            fields: vec![
                RecordField {
                    name: "a".to_string(),
                    doc: None,
                    default: None,
                    schema: Schema::Long,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    custom_attributes: Default::default(),
                },
                RecordField {
                    name: "b".to_string(),
                    doc: None,
                    default: None,
                    schema: Schema::String,
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                    custom_attributes: Default::default(),
                },
                RecordField {
                    name: "c".to_string(),
                    doc: None,
                    default: Some(JsonValue::Null),
                    schema: Schema::Union(
                        UnionSchema::new(vec![Schema::Null, Schema::Int]).unwrap(),
                    ),
                    order: RecordFieldOrder::Ascending,
                    position: 2,
                    custom_attributes: Default::default(),
                },
            ],
            lookup: [
                ("a".to_string(), 0),
                ("b".to_string(), 1),
                ("c".to_string(), 2),
            ]
            .iter()
            .cloned()
            .collect(),
            attributes: Default::default(),
        };

        assert!(Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("b".to_string(), Value::String("foo".to_string())),
        ])
        .validate(&schema));

        let value = Value::Record(vec![
            ("b".to_string(), Value::String("foo".to_string())),
            ("a".to_string(), Value::Long(42i64)),
        ]);
        assert!(value.validate(&schema));

        let value = Value::Record(vec![
            ("a".to_string(), Value::Boolean(false)),
            ("b".to_string(), Value::String("foo".to_string())),
        ]);
        assert!(!value.validate(&schema));
        assert_logged(
            r#"Invalid value: Record([("a", Boolean(false)), ("b", String("foo"))]) for schema: Record { name: Name { name: "some_record", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "a", doc: None, default: None, schema: Long, order: Ascending, position: 0, custom_attributes: {} }, RecordField { name: "b", doc: None, default: None, schema: String, order: Ascending, position: 1, custom_attributes: {} }, RecordField { name: "c", doc: None, default: Some(Null), schema: Union(UnionSchema { schemas: [Null, Int], variant_index: {Null: 0, Int: 1} }), order: Ascending, position: 2, custom_attributes: {} }], lookup: {"a": 0, "b": 1, "c": 2}, attributes: {} }. Reason: Unsupported value-schema combination"#,
        );

        let value = Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("c".to_string(), Value::String("foo".to_string())),
        ]);
        assert!(!value.validate(&schema));
        assert_logged(
            r#"Invalid value: Record([("a", Long(42)), ("c", String("foo"))]) for schema: Record { name: Name { name: "some_record", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "a", doc: None, default: None, schema: Long, order: Ascending, position: 0, custom_attributes: {} }, RecordField { name: "b", doc: None, default: None, schema: String, order: Ascending, position: 1, custom_attributes: {} }, RecordField { name: "c", doc: None, default: Some(Null), schema: Union(UnionSchema { schemas: [Null, Int], variant_index: {Null: 0, Int: 1} }), order: Ascending, position: 2, custom_attributes: {} }], lookup: {"a": 0, "b": 1, "c": 2}, attributes: {} }. Reason: Could not find matching type in union"#,
        );
        assert_not_logged(
            r#"Invalid value: String("foo") for schema: Int. Reason: Unsupported value-schema combination"#,
        );

        let value = Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("d".to_string(), Value::String("foo".to_string())),
        ]);
        assert!(!value.validate(&schema));
        assert_logged(
            r#"Invalid value: Record([("a", Long(42)), ("d", String("foo"))]) for schema: Record { name: Name { name: "some_record", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "a", doc: None, default: None, schema: Long, order: Ascending, position: 0, custom_attributes: {} }, RecordField { name: "b", doc: None, default: None, schema: String, order: Ascending, position: 1, custom_attributes: {} }, RecordField { name: "c", doc: None, default: Some(Null), schema: Union(UnionSchema { schemas: [Null, Int], variant_index: {Null: 0, Int: 1} }), order: Ascending, position: 2, custom_attributes: {} }], lookup: {"a": 0, "b": 1, "c": 2}, attributes: {} }. Reason: There is no schema field for field 'd'"#,
        );

        let value = Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("b".to_string(), Value::String("foo".to_string())),
            ("c".to_string(), Value::Null),
            ("d".to_string(), Value::Null),
        ]);
        assert!(!value.validate(&schema));
        assert_logged(
            r#"Invalid value: Record([("a", Long(42)), ("b", String("foo")), ("c", Null), ("d", Null)]) for schema: Record { name: Name { name: "some_record", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "a", doc: None, default: None, schema: Long, order: Ascending, position: 0, custom_attributes: {} }, RecordField { name: "b", doc: None, default: None, schema: String, order: Ascending, position: 1, custom_attributes: {} }, RecordField { name: "c", doc: None, default: Some(Null), schema: Union(UnionSchema { schemas: [Null, Int], variant_index: {Null: 0, Int: 1} }), order: Ascending, position: 2, custom_attributes: {} }], lookup: {"a": 0, "b": 1, "c": 2}, attributes: {} }. Reason: The value's records length (4) is greater than the schema's (3 fields)"#,
        );

        assert!(Value::Map(
            vec![
                ("a".to_string(), Value::Long(42i64)),
                ("b".to_string(), Value::String("foo".to_string())),
            ]
            .into_iter()
            .collect()
        )
        .validate(&schema));

        assert!(!Value::Map(
            vec![("d".to_string(), Value::Long(123_i64)),]
                .into_iter()
                .collect()
        )
        .validate(&schema));
        assert_logged(
            r#"Invalid value: Map({"d": Long(123)}) for schema: Record { name: Name { name: "some_record", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "a", doc: None, default: None, schema: Long, order: Ascending, position: 0, custom_attributes: {} }, RecordField { name: "b", doc: None, default: None, schema: String, order: Ascending, position: 1, custom_attributes: {} }, RecordField { name: "c", doc: None, default: Some(Null), schema: Union(UnionSchema { schemas: [Null, Int], variant_index: {Null: 0, Int: 1} }), order: Ascending, position: 2, custom_attributes: {} }], lookup: {"a": 0, "b": 1, "c": 2}, attributes: {} }. Reason: Field with name '"a"' is not a member of the map items
Field with name '"b"' is not a member of the map items"#,
        );

        let union_schema = Schema::Union(UnionSchema::new(vec![Schema::Null, schema]).unwrap());

        assert!(Value::Union(
            1,
            Box::new(Value::Record(vec![
                ("a".to_string(), Value::Long(42i64)),
                ("b".to_string(), Value::String("foo".to_string())),
            ]))
        )
        .validate(&union_schema));

        assert!(Value::Union(
            1,
            Box::new(Value::Map(
                vec![
                    ("a".to_string(), Value::Long(42i64)),
                    ("b".to_string(), Value::String("foo".to_string())),
                ]
                .into_iter()
                .collect()
            ))
        )
        .validate(&union_schema));
    }

    #[test]
    fn resolve_bytes_ok() {
        let value = Value::Array(vec![Value::Int(0), Value::Int(42)]);
        assert_eq!(
            value.resolve(&Schema::Bytes).unwrap(),
            Value::Bytes(vec![0u8, 42u8])
        );
    }

    #[test]
    fn resolve_string_from_bytes() {
        let value = Value::Bytes(vec![97, 98, 99]);
        assert_eq!(
            value.resolve(&Schema::String).unwrap(),
            Value::String("abc".to_string())
        );
    }

    #[test]
    fn resolve_string_from_fixed() {
        let value = Value::Fixed(3, vec![97, 98, 99]);
        assert_eq!(
            value.resolve(&Schema::String).unwrap(),
            Value::String("abc".to_string())
        );
    }

    #[test]
    fn resolve_bytes_failure() {
        let value = Value::Array(vec![Value::Int(2000), Value::Int(-42)]);
        assert!(value.resolve(&Schema::Bytes).is_err());
    }

    #[test]
    fn resolve_decimal_bytes() {
        let value = Value::Decimal(Decimal::from(vec![1, 2]));
        value
            .clone()
            .resolve(&Schema::Decimal {
                precision: 10,
                scale: 4,
                inner: Box::new(Schema::Bytes),
            })
            .unwrap();
        assert!(value.resolve(&Schema::String).is_err());
    }

    #[test]
    fn resolve_decimal_invalid_scale() {
        let value = Value::Decimal(Decimal::from(vec![1]));
        assert!(value
            .resolve(&Schema::Decimal {
                precision: 2,
                scale: 3,
                inner: Box::new(Schema::Bytes),
            })
            .is_err());
    }

    #[test]
    fn resolve_decimal_invalid_precision_for_length() {
        let value = Value::Decimal(Decimal::from((1u8..=8u8).rev().collect::<Vec<_>>()));
        assert!(value
            .resolve(&Schema::Decimal {
                precision: 1,
                scale: 0,
                inner: Box::new(Schema::Bytes),
            })
            .is_err());
    }

    #[test]
    fn resolve_decimal_fixed() {
        let value = Value::Decimal(Decimal::from(vec![1, 2]));
        assert!(value
            .clone()
            .resolve(&Schema::Decimal {
                precision: 10,
                scale: 1,
                inner: Box::new(Schema::Fixed {
                    name: Name::new("decimal").unwrap(),
                    aliases: None,
                    size: 20,
                    doc: None,
                    attributes: Default::default(),
                })
            })
            .is_ok());
        assert!(value.resolve(&Schema::String).is_err());
    }

    #[test]
    fn resolve_date() {
        let value = Value::Date(2345);
        assert!(value.clone().resolve(&Schema::Date).is_ok());
        assert!(value.resolve(&Schema::String).is_err());
    }

    #[test]
    fn resolve_time_millis() {
        let value = Value::TimeMillis(10);
        assert!(value.clone().resolve(&Schema::TimeMillis).is_ok());
        assert!(value.resolve(&Schema::TimeMicros).is_err());
    }

    #[test]
    fn resolve_time_micros() {
        let value = Value::TimeMicros(10);
        assert!(value.clone().resolve(&Schema::TimeMicros).is_ok());
        assert!(value.resolve(&Schema::TimeMillis).is_err());
    }

    #[test]
    fn resolve_timestamp_millis() {
        let value = Value::TimestampMillis(10);
        assert!(value.clone().resolve(&Schema::TimestampMillis).is_ok());
        assert!(value.resolve(&Schema::Float).is_err());

        let value = Value::Float(10.0f32);
        assert!(value.resolve(&Schema::TimestampMillis).is_err());
    }

    #[test]
    fn resolve_timestamp_micros() {
        let value = Value::TimestampMicros(10);
        assert!(value.clone().resolve(&Schema::TimestampMicros).is_ok());
        assert!(value.resolve(&Schema::Int).is_err());

        let value = Value::Double(10.0);
        assert!(value.resolve(&Schema::TimestampMicros).is_err());
    }

    #[test]
    fn resolve_duration() {
        let value = Value::Duration(Duration::new(
            Months::new(10),
            Days::new(5),
            Millis::new(3000),
        ));
        assert!(value.clone().resolve(&Schema::Duration).is_ok());
        assert!(value.resolve(&Schema::TimestampMicros).is_err());
        assert!(Value::Long(1i64).resolve(&Schema::Duration).is_err());
    }

    #[test]
    fn resolve_uuid() {
        let value = Value::Uuid(Uuid::parse_str("1481531d-ccc9-46d9-a56f-5b67459c0537").unwrap());
        assert!(value.clone().resolve(&Schema::Uuid).is_ok());
        assert!(value.resolve(&Schema::TimestampMicros).is_err());
    }

    #[test]
    fn test_avro_3621_resolve_to_nullable_union() {
        let schema = Schema::parse_str(
            r#"{
            "type": "record",
            "name": "root",
            "fields": [
                {
                    "name": "event",
                    "type": [
                        "null", 
                        {
                            "type": "record",
                            "name": "event",
                            "fields": [
                                {
                                    "name": "amount",
                                    "type": "int"
                                },
                                {
                                    "name": "size",
                                    "type": [
                                        "null",
                                        "int"
                                    ],
                                    "default": null
                                }
                            ]
                        }
                    ],
                    "default": null
                }
            ]
        }"#,
        )
        .unwrap();

        let value = Value::Record(vec![(
            "event".to_string(),
            Value::Record(vec![("amount".to_string(), Value::Int(200))]),
        )]);
        assert!(value.resolve(&schema).is_ok());

        let value = Value::Record(vec![(
            "event".to_string(),
            Value::Record(vec![("size".to_string(), Value::Int(1))]),
        )]);
        assert!(value.resolve(&schema).is_err());
    }

    #[test]
    fn json_from_avro() {
        assert_eq!(JsonValue::try_from(Value::Null).unwrap(), JsonValue::Null);
        assert_eq!(
            JsonValue::try_from(Value::Boolean(true)).unwrap(),
            JsonValue::Bool(true)
        );
        assert_eq!(
            JsonValue::try_from(Value::Int(1)).unwrap(),
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Long(1)).unwrap(),
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Float(1.0)).unwrap(),
            JsonValue::Number(Number::from_f64(1.0).unwrap())
        );
        assert_eq!(
            JsonValue::try_from(Value::Double(1.0)).unwrap(),
            JsonValue::Number(Number::from_f64(1.0).unwrap())
        );
        assert_eq!(
            JsonValue::try_from(Value::Bytes(vec![1, 2, 3])).unwrap(),
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into())
            ])
        );
        assert_eq!(
            JsonValue::try_from(Value::String("test".into())).unwrap(),
            JsonValue::String("test".into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Fixed(3, vec![1, 2, 3])).unwrap(),
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into())
            ])
        );
        assert_eq!(
            JsonValue::try_from(Value::Enum(1, "test_enum".into())).unwrap(),
            JsonValue::String("test_enum".into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Union(1, Box::new(Value::String("test_enum".into()))))
                .unwrap(),
            JsonValue::String("test_enum".into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Array(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3)
            ]))
            .unwrap(),
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into())
            ])
        );
        assert_eq!(
            JsonValue::try_from(Value::Map(
                vec![
                    ("v1".to_string(), Value::Int(1)),
                    ("v2".to_string(), Value::Int(2)),
                    ("v3".to_string(), Value::Int(3))
                ]
                .into_iter()
                .collect()
            ))
            .unwrap(),
            JsonValue::Object(
                vec![
                    ("v1".to_string(), JsonValue::Number(1.into())),
                    ("v2".to_string(), JsonValue::Number(2.into())),
                    ("v3".to_string(), JsonValue::Number(3.into()))
                ]
                .into_iter()
                .collect()
            )
        );
        assert_eq!(
            JsonValue::try_from(Value::Record(vec![
                ("v1".to_string(), Value::Int(1)),
                ("v2".to_string(), Value::Int(2)),
                ("v3".to_string(), Value::Int(3))
            ]))
            .unwrap(),
            JsonValue::Object(
                vec![
                    ("v1".to_string(), JsonValue::Number(1.into())),
                    ("v2".to_string(), JsonValue::Number(2.into())),
                    ("v3".to_string(), JsonValue::Number(3.into()))
                ]
                .into_iter()
                .collect()
            )
        );
        assert_eq!(
            JsonValue::try_from(Value::Date(1)).unwrap(),
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Decimal(vec![1, 2, 3].into())).unwrap(),
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into())
            ])
        );
        assert_eq!(
            JsonValue::try_from(Value::TimeMillis(1)).unwrap(),
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::TimeMicros(1)).unwrap(),
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::TimestampMillis(1)).unwrap(),
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::TimestampMicros(1)).unwrap(),
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Duration(
                [1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8, 9u8, 10u8, 11u8, 12u8].into()
            ))
            .unwrap(),
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into()),
                JsonValue::Number(4.into()),
                JsonValue::Number(5.into()),
                JsonValue::Number(6.into()),
                JsonValue::Number(7.into()),
                JsonValue::Number(8.into()),
                JsonValue::Number(9.into()),
                JsonValue::Number(10.into()),
                JsonValue::Number(11.into()),
                JsonValue::Number(12.into()),
            ])
        );
        assert_eq!(
            JsonValue::try_from(Value::Uuid(
                Uuid::parse_str("936DA01F-9ABD-4D9D-80C7-02AF85C822A8").unwrap()
            ))
            .unwrap(),
            JsonValue::String("936da01f-9abd-4d9d-80c7-02af85c822a8".into())
        );
    }

    #[test]
    fn test_avro_3433_recursive_resolves_record() {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }
                },
                {
                    "name":"b",
                    "type":"Inner"
                }
            ]
        }"#,
        )
        .unwrap();

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer = Value::Record(vec![("a".into(), inner_value1), ("b".into(), inner_value2)]);
        outer
            .resolve(&schema)
            .expect("Record definition defined in one field must be available in other field");
    }

    #[test]
    fn test_avro_3433_recursive_resolves_array() {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"array",
                        "items": {
                            "type":"record",
                            "name": "Inner",
                            "fields": [ {
                                "name":"z",
                                "type":"int"
                            }]
                        }
                    }
                },
                {
                    "name":"b",
                    "type": {
                        "type":"map",
                        "values":"Inner"
                    }
                }
            ]
        }"#,
        )
        .unwrap();

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value = Value::Record(vec![
            ("a".into(), Value::Array(vec![inner_value1])),
            (
                "b".into(),
                Value::Map(vec![("akey".into(), inner_value2)].into_iter().collect()),
            ),
        ]);
        outer_value
            .resolve(&schema)
            .expect("Record defined in array definition must be resolvable from map");
    }

    #[test]
    fn test_avro_3433_recursive_resolves_map() {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }
                },
                {
                    "name":"b",
                    "type": {
                        "type":"map",
                        "values":"Inner"
                    }
                }
            ]
        }"#,
        )
        .unwrap();

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value = Value::Record(vec![
            ("a".into(), inner_value1),
            (
                "b".into(),
                Value::Map(vec![("akey".into(), inner_value2)].into_iter().collect()),
            ),
        ]);
        outer_value
            .resolve(&schema)
            .expect("Record defined in record field must be resolvable from map field");
    }

    #[test]
    fn test_avro_3433_recursive_resolves_record_wrapper() {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }
                },
                {
                    "name":"b",
                    "type": {
                        "type":"record",
                        "name": "InnerWrapper",
                        "fields": [ {
                            "name":"j",
                            "type":"Inner"
                        }]
                    }
                }
            ]
        }"#,
        )
        .unwrap();

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![(
            "j".into(),
            Value::Record(vec![("z".into(), Value::Int(6))]),
        )]);
        let outer_value =
            Value::Record(vec![("a".into(), inner_value1), ("b".into(), inner_value2)]);
        outer_value.resolve(&schema).expect("Record schema defined in field must be resolvable in Record schema defined in other field");
    }

    #[test]
    fn test_avro_3433_recursive_resolves_map_and_array() {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"map",
                        "values": {
                            "type":"record",
                            "name": "Inner",
                            "fields": [ {
                                "name":"z",
                                "type":"int"
                            }]
                        }
                    }
                },
                {
                    "name":"b",
                    "type": {
                        "type":"array",
                        "items":"Inner"
                    }
                }
            ]
        }"#,
        )
        .unwrap();

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value = Value::Record(vec![
            (
                "a".into(),
                Value::Map(vec![("akey".into(), inner_value2)].into_iter().collect()),
            ),
            ("b".into(), Value::Array(vec![inner_value1])),
        ]);
        outer_value
            .resolve(&schema)
            .expect("Record defined in map definition must be resolvable from array");
    }

    #[test]
    fn test_avro_3433_recursive_resolves_union() {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":["null", {
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }]
                },
                {
                    "name":"b",
                    "type":"Inner"
                }
            ]
        }"#,
        )
        .unwrap();

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer1 = Value::Record(vec![
            ("a".into(), inner_value1),
            ("b".into(), inner_value2.clone()),
        ]);
        outer1
            .resolve(&schema)
            .expect("Record definition defined in union must be resolved in other field");
        let outer2 = Value::Record(vec![("a".into(), Value::Null), ("b".into(), inner_value2)]);
        outer2
            .resolve(&schema)
            .expect("Record definition defined in union must be resolved in other field");
    }

    #[test]
    fn test_avro_3461_test_multi_level_resolve_outer_namespace() {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "middle_record_name",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema).unwrap();
        let inner_record = Value::Record(vec![("inner_field_1".into(), Value::Double(5.4))]);
        let middle_record_variation_1 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(0, Box::new(Value::Null)),
        )]);
        let middle_record_variation_2 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(1, Box::new(inner_record.clone())),
        )]);
        let outer_record_variation_1 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_2 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_1)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_3 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_2)),
            ),
            ("outer_field_2".into(), inner_record),
        ]);

        outer_record_variation_1
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
        outer_record_variation_2
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
        outer_record_variation_3
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
    }

    #[test]
    fn test_avro_3461_test_multi_level_resolve_middle_namespace() {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "middle_record_name",
                            "namespace":"middle_namespace",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "middle_namespace.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema).unwrap();
        let inner_record = Value::Record(vec![("inner_field_1".into(), Value::Double(5.4))]);
        let middle_record_variation_1 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(0, Box::new(Value::Null)),
        )]);
        let middle_record_variation_2 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(1, Box::new(inner_record.clone())),
        )]);
        let outer_record_variation_1 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_2 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_1)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_3 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_2)),
            ),
            ("outer_field_2".into(), inner_record),
        ]);

        outer_record_variation_1
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
        outer_record_variation_2
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
        outer_record_variation_3
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
    }

    #[test]
    fn test_avro_3461_test_multi_level_resolve_inner_namespace() {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "middle_record_name",
                            "namespace":"middle_namespace",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "namespace":"inner_namespace",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_namespace.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema).unwrap();

        let inner_record = Value::Record(vec![("inner_field_1".into(), Value::Double(5.4))]);
        let middle_record_variation_1 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(0, Box::new(Value::Null)),
        )]);
        let middle_record_variation_2 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(1, Box::new(inner_record.clone())),
        )]);
        let outer_record_variation_1 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_2 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_1)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_3 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_2)),
            ),
            ("outer_field_2".into(), inner_record),
        ]);

        outer_record_variation_1
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
        outer_record_variation_2
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
        outer_record_variation_3
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
    }

    #[test]
    fn test_avro_3460_validation_with_refs() {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }
                },
                {
                    "name":"b",
                    "type":"Inner"
                }
            ]
        }"#,
        )
        .unwrap();

        let inner_value_right = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value_wrong1 = Value::Record(vec![("z".into(), Value::Null)]);
        let inner_value_wrong2 = Value::Record(vec![("a".into(), Value::String("testing".into()))]);
        let outer1 = Value::Record(vec![
            ("a".into(), inner_value_right.clone()),
            ("b".into(), inner_value_wrong1),
        ]);

        let outer2 = Value::Record(vec![
            ("a".into(), inner_value_right),
            ("b".into(), inner_value_wrong2),
        ]);

        assert!(
            !outer1.validate(&schema),
            "field b record is invalid against the schema"
        ); // this should pass, but doesn't
        assert!(
            !outer2.validate(&schema),
            "field b record is invalid against the schema"
        ); // this should pass, but doesn't
    }

    #[test]
    fn test_avro_3460_validation_with_refs_real_struct() {
        use crate::ser::Serializer;
        use serde::Serialize;

        #[derive(Serialize, Clone)]
        struct TestInner {
            z: i32,
        }

        #[derive(Serialize)]
        struct TestRefSchemaStruct1 {
            a: TestInner,
            b: String, // could be literally anything
        }

        #[derive(Serialize)]
        struct TestRefSchemaStruct2 {
            a: TestInner,
            b: i32, // could be literally anything
        }

        #[derive(Serialize)]
        struct TestRefSchemaStruct3 {
            a: TestInner,
            b: Option<TestInner>, // could be literally anything
        }

        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }
                },
                {
                    "name":"b",
                    "type":"Inner"
                }
            ]
        }"#,
        )
        .unwrap();

        let test_inner = TestInner { z: 3 };
        let test_outer1 = TestRefSchemaStruct1 {
            a: test_inner.clone(),
            b: "testing".into(),
        };
        let test_outer2 = TestRefSchemaStruct2 {
            a: test_inner.clone(),
            b: 24,
        };
        let test_outer3 = TestRefSchemaStruct3 {
            a: test_inner,
            b: None,
        };

        let mut ser = Serializer::default();
        let test_outer1: Value = test_outer1.serialize(&mut ser).unwrap();
        let mut ser = Serializer::default();
        let test_outer2: Value = test_outer2.serialize(&mut ser).unwrap();
        let mut ser = Serializer::default();
        let test_outer3: Value = test_outer3.serialize(&mut ser).unwrap();

        assert!(
            !test_outer1.validate(&schema),
            "field b record is invalid against the schema"
        ); // this should pass, but doesn't
        assert!(
            !test_outer2.validate(&schema),
            "field b record is invalid against the schema"
        ); // this should pass, but doesn't
        assert!(
            !test_outer3.validate(&schema),
            "field b record is invalid against the schema"
        ); // this should pass, but doesn't
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct TestStructFixedField {
        field: [u8; 6],
    }

    #[test]
    fn test_avro_3631_serialize_fixed_fields() {
        let test = TestStructFixedField { field: [1; 6] };
        let value: Value = to_value(test).unwrap();
        let schema = Schema::parse_str(
            r#"
            {
                "type": "record",
                "name": "TestStructFixedField",
                "fields": [
                    {
                        "name": "field",
                        "type": {
                            "name": "field",
                            "type": "fixed",
                            "size": 6
                        }
                    }
                ]
            }
            "#,
        )
        .unwrap();
        assert!(value.validate(&schema));
    }
}
