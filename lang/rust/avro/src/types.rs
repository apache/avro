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
    bigdecimal::{deserialize_big_decimal, serialize_big_decimal},
    decimal::Decimal,
    duration::Duration,
    schema::{
        DecimalSchema, EnumSchema, FixedSchema, Name, Namespace, Precision, RecordField,
        RecordSchema, ResolvedSchema, Scale, Schema, SchemaKind, UnionSchema,
    },
    AvroResult, Error,
};
use bigdecimal::BigDecimal;
use serde_json::{Number, Value as JsonValue};
use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap},
    fmt::Debug,
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
/// Specification](https://avro.apache.org/docs/current/specification/#schema-declaration)
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
    /// An Avro Decimal value.
    BigDecimal(BigDecimal),
    /// Time in milliseconds.
    TimeMillis(i32),
    /// Time in microseconds.
    TimeMicros(i64),
    /// Timestamp in milliseconds.
    TimestampMillis(i64),
    /// Timestamp in microseconds.
    TimestampMicros(i64),
    /// Timestamp in nanoseconds.
    TimestampNanos(i64),
    /// Local timestamp in milliseconds.
    LocalTimestampMillis(i64),
    /// Local timestamp in microseconds.
    LocalTimestampMicros(i64),
    /// Local timestamp in nanoseconds.
    LocalTimestampNanos(i64),
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
to_value!(BigDecimal, Value::BigDecimal);
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
            Schema::Record(RecordSchema {
                fields: ref schema_fields,
                lookup: ref schema_lookup,
                ..
            }) => {
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
            JsonValue::Number(ref n) if n.is_i64() => {
                let n = n.as_i64().unwrap();
                if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                    Value::Int(n as i32)
                } else {
                    Value::Long(n)
                }
            }
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
impl TryFrom<Value> for JsonValue {
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
            Value::BigDecimal(ref bg) => {
                let vec1: Vec<u8> = serialize_big_decimal(bg);
                Ok(Self::Array(vec1.into_iter().map(|b| b.into()).collect()))
            }
            Value::TimeMillis(t) => Ok(Self::Number(t.into())),
            Value::TimeMicros(t) => Ok(Self::Number(t.into())),
            Value::TimestampMillis(t) => Ok(Self::Number(t.into())),
            Value::TimestampMicros(t) => Ok(Self::Number(t.into())),
            Value::TimestampNanos(t) => Ok(Self::Number(t.into())),
            Value::LocalTimestampMillis(t) => Ok(Self::Number(t.into())),
            Value::LocalTimestampMicros(t) => Ok(Self::Number(t.into())),
            Value::LocalTimestampNanos(t) => Ok(Self::Number(t.into())),
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
    /// See the [Avro specification](https://avro.apache.org/docs/current/specification)
    /// for the full set of rules of schema validation.
    pub fn validate(&self, schema: &Schema) -> bool {
        self.validate_schemata(vec![schema])
    }

    pub fn validate_schemata(&self, schemata: Vec<&Schema>) -> bool {
        let rs = ResolvedSchema::try_from(schemata.clone())
            .expect("Schemata didn't successfully resolve");
        let schemata_len = schemata.len();
        schemata.iter().any(|schema| {
            let enclosing_namespace = schema.namespace();

            match self.validate_internal(schema, rs.get_names(), &enclosing_namespace) {
                Some(reason) => {
                    let log_message = format!(
                        "Invalid value: {:?} for schema: {:?}. Reason: {}",
                        self, schema, reason
                    );
                    if schemata_len == 1 {
                        error!("{}", log_message);
                    } else {
                        debug!("{}", log_message);
                    };
                    false
                }
                None => true,
            }
        })
    }

    fn accumulate(accumulator: Option<String>, other: Option<String>) -> Option<String> {
        match (accumulator, other) {
            (None, None) => None,
            (None, s @ Some(_)) => s,
            (s @ Some(_), None) => s,
            (Some(reason1), Some(reason2)) => Some(format!("{reason1}\n{reason2}")),
        }
    }

    /// Validates the value against the provided schema.
    pub(crate) fn validate_internal<S: std::borrow::Borrow<Schema> + Debug>(
        &self,
        schema: &Schema,
        names: &HashMap<Name, S>,
        enclosing_namespace: &Namespace,
    ) -> Option<String> {
        match (self, schema) {
            (_, Schema::Ref { name }) => {
                let name = name.fully_qualified_name(enclosing_namespace);
                names.get(&name).map_or_else(
                    || {
                        Some(format!(
                            "Unresolved schema reference: '{:?}'. Parsed names: {:?}",
                            name,
                            names.keys()
                        ))
                    },
                    |s| self.validate_internal(s.borrow(), names, &name.namespace),
                )
            }
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
            (&Value::Long(_), &Schema::LocalTimestampMillis) => None,
            (&Value::Long(_), &Schema::LocalTimestampMicros) => None,
            (&Value::TimestampMicros(_), &Schema::TimestampMicros) => None,
            (&Value::TimestampMillis(_), &Schema::TimestampMillis) => None,
            (&Value::TimestampNanos(_), &Schema::TimestampNanos) => None,
            (&Value::LocalTimestampMicros(_), &Schema::LocalTimestampMicros) => None,
            (&Value::LocalTimestampMillis(_), &Schema::LocalTimestampMillis) => None,
            (&Value::LocalTimestampNanos(_), &Schema::LocalTimestampNanos) => None,
            (&Value::TimeMicros(_), &Schema::TimeMicros) => None,
            (&Value::TimeMillis(_), &Schema::TimeMillis) => None,
            (&Value::Date(_), &Schema::Date) => None,
            (&Value::Decimal(_), &Schema::Decimal { .. }) => None,
            (&Value::BigDecimal(_), &Schema::BigDecimal) => None,
            (&Value::Duration(_), &Schema::Duration) => None,
            (&Value::Uuid(_), &Schema::Uuid) => None,
            (&Value::Float(_), &Schema::Float) => None,
            (&Value::Float(_), &Schema::Double) => None,
            (&Value::Double(_), &Schema::Double) => None,
            (&Value::Bytes(_), &Schema::Bytes) => None,
            (&Value::Bytes(_), &Schema::Decimal { .. }) => None,
            (&Value::String(_), &Schema::String) => None,
            (&Value::String(_), &Schema::Uuid) => None,
            (&Value::Fixed(n, _), &Schema::Fixed(FixedSchema { size, .. })) => {
                if n != size {
                    Some(format!(
                        "The value's size ({n}) is different than the schema's size ({size})"
                    ))
                } else {
                    None
                }
            }
            (Value::Bytes(b), &Schema::Fixed(FixedSchema { size, .. })) => {
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
                        "The value's size ('{n}') must be exactly 12 to be a Duration"
                    ))
                } else {
                    None
                }
            }
            // TODO: check precision against n
            (&Value::Fixed(_n, _), &Schema::Decimal { .. }) => None,
            (Value::String(s), Schema::Enum(EnumSchema { symbols, .. })) => {
                if !symbols.contains(s) {
                    Some(format!("'{s}' is not a member of the possible symbols"))
                } else {
                    None
                }
            }
            (
                &Value::Enum(i, ref s),
                Schema::Enum(EnumSchema {
                    symbols, default, ..
                }),
            ) => symbols
                .get(i as usize)
                .map(|ref symbol| {
                    if symbol != &s {
                        Some(format!("Symbol '{s}' is not at position '{i}'"))
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| match default {
                    Some(_) => None,
                    None => Some(format!("No symbol at position '{i}'")),
                }),
            // (&Value::Union(None), &Schema::Union(_)) => None,
            (&Value::Union(i, ref value), Schema::Union(inner)) => inner
                .variants()
                .get(i as usize)
                .map(|schema| value.validate_internal(schema, names, enclosing_namespace))
                .unwrap_or_else(|| Some(format!("No schema in the union at position '{i}'"))),
            (v, Schema::Union(inner)) => {
                match inner.find_schema_with_known_schemata(v, Some(names), enclosing_namespace) {
                    Some(_) => None,
                    None => Some("Could not find matching type in union".to_string()),
                }
            }
            (Value::Array(items), Schema::Array(inner)) => items.iter().fold(None, |acc, item| {
                Value::accumulate(
                    acc,
                    item.validate_internal(&inner.items, names, enclosing_namespace),
                )
            }),
            (Value::Map(items), Schema::Map(inner)) => {
                items.iter().fold(None, |acc, (_, value)| {
                    Value::accumulate(
                        acc,
                        value.validate_internal(&inner.types, names, enclosing_namespace),
                    )
                })
            }
            (
                Value::Record(record_fields),
                Schema::Record(RecordSchema {
                    fields,
                    lookup,
                    name,
                    ..
                }),
            ) => {
                let non_nullable_fields_count =
                    fields.iter().filter(|&rf| !rf.is_nullable()).count();

                // If the record contains fewer fields as required fields by the schema, it is invalid.
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
                        let record_namespace = if name.namespace.is_none() {
                            enclosing_namespace
                        } else {
                            &name.namespace
                        };
                        match lookup.get(field_name) {
                            Some(idx) => {
                                let field = &fields[*idx];
                                Value::accumulate(
                                    acc,
                                    record_field.validate_internal(
                                        &field.schema,
                                        names,
                                        record_namespace,
                                    ),
                                )
                            }
                            None => Value::accumulate(
                                acc,
                                Some(format!("There is no schema field for field '{field_name}'")),
                            ),
                        }
                    })
            }
            (Value::Map(items), Schema::Record(RecordSchema { fields, .. })) => {
                fields.iter().fold(None, |acc, field| {
                    if let Some(item) = items.get(&field.name) {
                        let res = item.validate_internal(&field.schema, names, enclosing_namespace);
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
            (v, s) => Some(format!(
                "Unsupported value-schema combination! Value: {:?}, schema: {:?}",
                v, s
            )),
        }
    }

    /// Attempt to perform schema resolution on the value, with the given
    /// [Schema](../schema/enum.Schema.html).
    ///
    /// See [Schema Resolution](https://avro.apache.org/docs/current/specification/#schema-resolution)
    /// in the Avro specification for the full set of rules of schema
    /// resolution.
    pub fn resolve(self, schema: &Schema) -> AvroResult<Self> {
        let enclosing_namespace = schema.namespace();
        let rs = ResolvedSchema::try_from(schema)?;
        self.resolve_internal(schema, rs.get_names(), &enclosing_namespace, &None)
    }

    /// Attempt to perform schema resolution on the value, with the given
    /// [Schema](../schema/enum.Schema.html) and set of schemas to use for Refs resolution.
    ///
    /// See [Schema Resolution](https://avro.apache.org/docs/current/specification/#schema-resolution)
    /// in the Avro specification for the full set of rules of schema
    /// resolution.
    pub fn resolve_schemata(self, schema: &Schema, schemata: Vec<&Schema>) -> AvroResult<Self> {
        let enclosing_namespace = schema.namespace();
        let rs = ResolvedSchema::try_from(schemata)?;
        self.resolve_internal(schema, rs.get_names(), &enclosing_namespace, &None)
    }

    pub(crate) fn resolve_internal<S: Borrow<Schema> + Debug>(
        mut self,
        schema: &Schema,
        names: &HashMap<Name, S>,
        enclosing_namespace: &Namespace,
        field_default: &Option<JsonValue>,
    ) -> AvroResult<Self> {
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
                let name = name.fully_qualified_name(enclosing_namespace);

                if let Some(resolved) = names.get(&name) {
                    debug!("Resolved {:?}", name);
                    self.resolve_internal(resolved.borrow(), names, &name.namespace, field_default)
                } else {
                    error!("Failed to resolve schema {:?}", name);
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
            Schema::Fixed(FixedSchema { size, .. }) => self.resolve_fixed(size),
            Schema::Union(ref inner) => {
                self.resolve_union(inner, names, enclosing_namespace, field_default)
            }
            Schema::Enum(EnumSchema {
                ref symbols,
                ref default,
                ..
            }) => self.resolve_enum(symbols, default, field_default),
            Schema::Array(ref inner) => {
                self.resolve_array(&inner.items, names, enclosing_namespace)
            }
            Schema::Map(ref inner) => self.resolve_map(&inner.types, names, enclosing_namespace),
            Schema::Record(RecordSchema { ref fields, .. }) => {
                self.resolve_record(fields, names, enclosing_namespace)
            }
            Schema::Decimal(DecimalSchema {
                scale,
                precision,
                ref inner,
            }) => self.resolve_decimal(precision, scale, inner),
            Schema::BigDecimal => self.resolve_bigdecimal(),
            Schema::Date => self.resolve_date(),
            Schema::TimeMillis => self.resolve_time_millis(),
            Schema::TimeMicros => self.resolve_time_micros(),
            Schema::TimestampMillis => self.resolve_timestamp_millis(),
            Schema::TimestampMicros => self.resolve_timestamp_micros(),
            Schema::TimestampNanos => self.resolve_timestamp_nanos(),
            Schema::LocalTimestampMillis => self.resolve_local_timestamp_millis(),
            Schema::LocalTimestampMicros => self.resolve_local_timestamp_micros(),
            Schema::LocalTimestampNanos => self.resolve_local_timestamp_nanos(),
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
            other => return Err(Error::GetUuid(other)),
        })
    }

    fn resolve_bigdecimal(self) -> Result<Self, Error> {
        Ok(match self {
            bg @ Value::BigDecimal(_) => bg,
            Value::Bytes(b) => Value::BigDecimal(deserialize_big_decimal(&b).unwrap()),
            other => return Err(Error::GetBigDecimal(other)),
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
            other => return Err(Error::ResolveDuration(other)),
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
            &Schema::Fixed(FixedSchema { size, .. }) => {
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
                if max_prec_for_len(num_bytes)? < precision {
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
                if max_prec_for_len(bytes.len())? < precision {
                    Err(Error::ComparePrecisionAndSize {
                        precision,
                        num_bytes: bytes.len(),
                    })
                } else {
                    // precision and scale match, can we assume the underlying type can hold the data?
                    Ok(Value::Decimal(Decimal::from(bytes)))
                }
            }
            other => Err(Error::ResolveDecimal(other)),
        }
    }

    fn resolve_date(self) -> Result<Self, Error> {
        match self {
            Value::Date(d) | Value::Int(d) => Ok(Value::Date(d)),
            other => Err(Error::GetDate(other)),
        }
    }

    fn resolve_time_millis(self) -> Result<Self, Error> {
        match self {
            Value::TimeMillis(t) | Value::Int(t) => Ok(Value::TimeMillis(t)),
            other => Err(Error::GetTimeMillis(other)),
        }
    }

    fn resolve_time_micros(self) -> Result<Self, Error> {
        match self {
            Value::TimeMicros(t) | Value::Long(t) => Ok(Value::TimeMicros(t)),
            Value::Int(t) => Ok(Value::TimeMicros(i64::from(t))),
            other => Err(Error::GetTimeMicros(other)),
        }
    }

    fn resolve_timestamp_millis(self) -> Result<Self, Error> {
        match self {
            Value::TimestampMillis(ts) | Value::Long(ts) => Ok(Value::TimestampMillis(ts)),
            Value::Int(ts) => Ok(Value::TimestampMillis(i64::from(ts))),
            other => Err(Error::GetTimestampMillis(other)),
        }
    }

    fn resolve_timestamp_micros(self) -> Result<Self, Error> {
        match self {
            Value::TimestampMicros(ts) | Value::Long(ts) => Ok(Value::TimestampMicros(ts)),
            Value::Int(ts) => Ok(Value::TimestampMicros(i64::from(ts))),
            other => Err(Error::GetTimestampMicros(other)),
        }
    }

    fn resolve_timestamp_nanos(self) -> Result<Self, Error> {
        match self {
            Value::TimestampNanos(ts) | Value::Long(ts) => Ok(Value::TimestampNanos(ts)),
            Value::Int(ts) => Ok(Value::TimestampNanos(i64::from(ts))),
            other => Err(Error::GetTimestampNanos(other)),
        }
    }

    fn resolve_local_timestamp_millis(self) -> Result<Self, Error> {
        match self {
            Value::LocalTimestampMillis(ts) | Value::Long(ts) => {
                Ok(Value::LocalTimestampMillis(ts))
            }
            Value::Int(ts) => Ok(Value::LocalTimestampMillis(i64::from(ts))),
            other => Err(Error::GetLocalTimestampMillis(other)),
        }
    }

    fn resolve_local_timestamp_micros(self) -> Result<Self, Error> {
        match self {
            Value::LocalTimestampMicros(ts) | Value::Long(ts) => {
                Ok(Value::LocalTimestampMicros(ts))
            }
            Value::Int(ts) => Ok(Value::LocalTimestampMicros(i64::from(ts))),
            other => Err(Error::GetLocalTimestampMicros(other)),
        }
    }

    fn resolve_local_timestamp_nanos(self) -> Result<Self, Error> {
        match self {
            Value::LocalTimestampNanos(ts) | Value::Long(ts) => Ok(Value::LocalTimestampNanos(ts)),
            Value::Int(ts) => Ok(Value::LocalTimestampNanos(i64::from(ts))),
            other => Err(Error::GetLocalTimestampNanos(other)),
        }
    }

    fn resolve_null(self) -> Result<Self, Error> {
        match self {
            Value::Null => Ok(Value::Null),
            other => Err(Error::GetNull(other)),
        }
    }

    fn resolve_boolean(self) -> Result<Self, Error> {
        match self {
            Value::Boolean(b) => Ok(Value::Boolean(b)),
            other => Err(Error::GetBoolean(other)),
        }
    }

    fn resolve_int(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Int(n)),
            Value::Long(n) => Ok(Value::Int(n as i32)),
            other => Err(Error::GetInt(other)),
        }
    }

    fn resolve_long(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Long(i64::from(n))),
            Value::Long(n) => Ok(Value::Long(n)),
            other => Err(Error::GetLong(other)),
        }
    }

    fn resolve_float(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Float(n as f32)),
            Value::Long(n) => Ok(Value::Float(n as f32)),
            Value::Float(x) => Ok(Value::Float(x)),
            Value::Double(x) => Ok(Value::Float(x as f32)),
            Value::String(ref x) => match Self::parse_special_float(x) {
                Some(f) => Ok(Value::Float(f)),
                None => Err(Error::GetFloat(self)),
            },
            other => Err(Error::GetFloat(other)),
        }
    }

    fn resolve_double(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Double(f64::from(n))),
            Value::Long(n) => Ok(Value::Double(n as f64)),
            Value::Float(x) => Ok(Value::Double(f64::from(x))),
            Value::Double(x) => Ok(Value::Double(x)),
            Value::String(ref x) => match Self::parse_special_float(x) {
                Some(f) => Ok(Value::Double(f64::from(f))),
                None => Err(Error::GetDouble(self)),
            },
            other => Err(Error::GetDouble(other)),
        }
    }

    /// IEEE 754 NaN and infinities are not valid JSON numbers.
    /// So they are represented in JSON as strings.
    fn parse_special_float(value: &str) -> Option<f32> {
        match value {
            "NaN" => Some(f32::NAN),
            "INF" | "Infinity" => Some(f32::INFINITY),
            "-INF" | "-Infinity" => Some(f32::NEG_INFINITY),
            _ => None,
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
            other => Err(Error::GetBytes(other)),
        }
    }

    fn resolve_string(self) -> Result<Self, Error> {
        match self {
            Value::String(s) => Ok(Value::String(s)),
            Value::Bytes(bytes) | Value::Fixed(_, bytes) => Ok(Value::String(
                String::from_utf8(bytes).map_err(Error::ConvertToUtf8)?,
            )),
            other => Err(Error::GetString(other)),
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
            Value::Bytes(s) => {
                if s.len() == size {
                    Ok(Value::Fixed(size, s))
                } else {
                    Err(Error::CompareFixedSizes { size, n: s.len() })
                }
            }
            other => Err(Error::GetStringForFixed(other)),
        }
    }

    pub(crate) fn resolve_enum(
        self,
        symbols: &[String],
        enum_default: &Option<String>,
        _field_default: &Option<JsonValue>,
    ) -> Result<Self, Error> {
        let validate_symbol = |symbol: String, symbols: &[String]| {
            if let Some(index) = symbols.iter().position(|item| item == &symbol) {
                Ok(Value::Enum(index as u32, symbol))
            } else {
                match enum_default {
                    Some(default) => {
                        if let Some(index) = symbols.iter().position(|item| item == default) {
                            Ok(Value::Enum(index as u32, default.clone()))
                        } else {
                            Err(Error::GetEnumDefault {
                                symbol,
                                symbols: symbols.into(),
                            })
                        }
                    }
                    _ => Err(Error::GetEnumDefault {
                        symbol,
                        symbols: symbols.into(),
                    }),
                }
            }
        };

        match self {
            Value::Enum(_raw_index, s) => validate_symbol(s, symbols),
            Value::String(s) => validate_symbol(s, symbols),
            other => Err(Error::GetEnum(other)),
        }
    }

    fn resolve_union<S: Borrow<Schema> + Debug>(
        self,
        schema: &UnionSchema,
        names: &HashMap<Name, S>,
        enclosing_namespace: &Namespace,
        field_default: &Option<JsonValue>,
    ) -> Result<Self, Error> {
        let v = match self {
            // Both are unions case.
            Value::Union(_i, v) => *v,
            // Reader is a union, but writer is not.
            v => v,
        };
        let (i, inner) = schema
            .find_schema_with_known_schemata(&v, Some(names), enclosing_namespace)
            .ok_or(Error::FindUnionVariant)?;

        Ok(Value::Union(
            i as u32,
            Box::new(v.resolve_internal(inner, names, enclosing_namespace, field_default)?),
        ))
    }

    fn resolve_array<S: Borrow<Schema> + Debug>(
        self,
        schema: &Schema,
        names: &HashMap<Name, S>,
        enclosing_namespace: &Namespace,
    ) -> Result<Self, Error> {
        match self {
            Value::Array(items) => Ok(Value::Array(
                items
                    .into_iter()
                    .map(|item| item.resolve_internal(schema, names, enclosing_namespace, &None))
                    .collect::<Result<_, _>>()?,
            )),
            other => Err(Error::GetArray {
                expected: schema.into(),
                other,
            }),
        }
    }

    fn resolve_map<S: Borrow<Schema> + Debug>(
        self,
        schema: &Schema,
        names: &HashMap<Name, S>,
        enclosing_namespace: &Namespace,
    ) -> Result<Self, Error> {
        match self {
            Value::Map(items) => Ok(Value::Map(
                items
                    .into_iter()
                    .map(|(key, value)| {
                        value
                            .resolve_internal(schema, names, enclosing_namespace, &None)
                            .map(|value| (key, value))
                    })
                    .collect::<Result<_, _>>()?,
            )),
            other => Err(Error::GetMap {
                expected: schema.into(),
                other,
            }),
        }
    }

    fn resolve_record<S: Borrow<Schema> + Debug>(
        self,
        fields: &[RecordField],
        names: &HashMap<Name, S>,
        enclosing_namespace: &Namespace,
    ) -> Result<Self, Error> {
        let mut items = match self {
            Value::Map(items) => Ok(items),
            Value::Record(fields) => Ok(fields.into_iter().collect::<HashMap<_, _>>()),
            other => Err(Error::GetRecord {
                expected: fields
                    .iter()
                    .map(|field| (field.name.clone(), field.schema.clone().into()))
                    .collect(),
                other,
            }),
        }?;

        let new_fields = fields
            .iter()
            .map(|field| {
                let value = match items.remove(&field.name) {
                    Some(value) => value,
                    None => match field.default {
                        Some(ref value) => match field.schema {
                            Schema::Enum(EnumSchema {
                                ref symbols,
                                ref default,
                                ..
                            }) => Value::from(value.clone()).resolve_enum(
                                symbols,
                                default,
                                &field.default.clone(),
                            )?,
                            Schema::Union(ref union_schema) => {
                                let first = &union_schema.variants()[0];
                                // NOTE: this match exists only to optimize null defaults for large
                                // backward-compatible schemas with many nullable fields
                                match first {
                                    Schema::Null => Value::Union(0, Box::new(Value::Null)),
                                    _ => Value::Union(
                                        0,
                                        Box::new(Value::from(value.clone()).resolve_internal(
                                            first,
                                            names,
                                            enclosing_namespace,
                                            &field.default,
                                        )?),
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
                    .resolve_internal(&field.schema, names, enclosing_namespace, &field.default)
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

        Err(Error::GetU8(int))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        duration::{Days, Millis, Months},
        schema::RecordFieldOrder,
    };
    use apache_avro_test_helper::{
        logger::{assert_logged, assert_not_logged},
        TestResult,
    };
    use num_bigint::BigInt;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn avro_3809_validate_nested_records_with_implicit_namespace() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "name": "record_name",
            "namespace": "space",
            "type": "record",
            "fields": [
              {
                "name": "outer_field_1",
                "type": {
                  "type": "record",
                  "name": "middle_record_name",
                  "namespace": "middle_namespace",
                  "fields": [
                    {
                      "name": "middle_field_1",
                      "type": {
                        "type": "record",
                        "name": "inner_record_name",
                        "fields": [
                          { "name": "inner_field_1", "type": "double" }
                        ]
                      }
                    },
                    { "name": "middle_field_2", "type": "inner_record_name" }
                  ]
                }
              }
            ]
          }"#,
        )?;
        let value = Value::Record(vec![(
            "outer_field_1".into(),
            Value::Record(vec![
                (
                    "middle_field_1".into(),
                    Value::Record(vec![("inner_field_1".into(), Value::Double(1.2f64))]),
                ),
                (
                    "middle_field_2".into(),
                    Value::Record(vec![("inner_field_1".into(), Value::Double(1.6f64))]),
                ),
            ]),
        )]);

        assert!(value.validate(&schema));
        Ok(())
    }

    #[test]
    fn validate() -> TestResult {
        let value_schema_valid = vec![
            (Value::Int(42), Schema::Int, true, ""),
            (Value::Int(43), Schema::Long, true, ""),
            (Value::Float(43.2), Schema::Float, true, ""),
            (Value::Float(45.9), Schema::Double, true, ""),
            (
                Value::Int(42),
                Schema::Boolean,
                false,
                "Invalid value: Int(42) for schema: Boolean. Reason: Unsupported value-schema combination! Value: Int(42), schema: Boolean",
            ),
            (
                Value::Union(0, Box::new(Value::Null)),
                Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int])?),
                true,
                "",
            ),
            (
                Value::Union(1, Box::new(Value::Int(42))),
                Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int])?),
                true,
                "",
            ),
            (
                Value::Union(0, Box::new(Value::Null)),
                Schema::Union(UnionSchema::new(vec![Schema::Double, Schema::Int])?),
                false,
                "Invalid value: Union(0, Null) for schema: Union(UnionSchema { schemas: [Double, Int], variant_index: {Int: 1, Double: 0} }). Reason: Unsupported value-schema combination! Value: Null, schema: Double",
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
                    ?,
                ),
                true,
                "",
            ),
            (
                Value::Union(1, Box::new(Value::Long(42i64))),
                Schema::Union(
                    UnionSchema::new(vec![Schema::Null, Schema::TimestampMillis])?,
                ),
                true,
                "",
            ),
            (
                Value::Union(2, Box::new(Value::Long(1_i64))),
                Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int])?),
                false,
                "Invalid value: Union(2, Long(1)) for schema: Union(UnionSchema { schemas: [Null, Int], variant_index: {Null: 0, Int: 1} }). Reason: No schema in the union at position '2'",
            ),
            (
                Value::Array(vec![Value::Long(42i64)]),
                Schema::array(Schema::Long),
                true,
                "",
            ),
            (
                Value::Array(vec![Value::Boolean(true)]),
                Schema::array(Schema::Long),
                false,
                "Invalid value: Array([Boolean(true)]) for schema: Array(ArraySchema { items: Long, attributes: {} }). Reason: Unsupported value-schema combination! Value: Boolean(true), schema: Long",
            ),
            (Value::Record(vec![]), Schema::Null, false, "Invalid value: Record([]) for schema: Null. Reason: Unsupported value-schema combination! Value: Record([]), schema: Null"),
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
                Schema::Record(RecordSchema {
                    name: Name::new("record_name").unwrap(),
                    aliases: None,
                    doc: None,
                    fields: vec![RecordField {
                        name: "field_name".to_string(),
                        doc: None,
                        default: None,
                        aliases: None,
                        schema: Schema::Int,
                        order: RecordFieldOrder::Ignore,
                        position: 0,
                        custom_attributes: Default::default(),
                    }],
                    lookup: Default::default(),
                    attributes: Default::default(),
                }),
                false,
                r#"Invalid value: Record([("unknown_field_name", Null)]) for schema: Record(RecordSchema { name: Name { name: "record_name", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "field_name", doc: None, aliases: None, default: None, schema: Int, order: Ignore, position: 0, custom_attributes: {} }], lookup: {}, attributes: {} }). Reason: There is no schema field for field 'unknown_field_name'"#,
            ),
            (
                Value::Record(vec![("field_name".to_string(), Value::Null)]),
                Schema::Record(RecordSchema {
                    name: Name::new("record_name").unwrap(),
                    aliases: None,
                    doc: None,
                    fields: vec![RecordField {
                        name: "field_name".to_string(),
                        doc: None,
                        default: None,
                        aliases: None,
                        schema: Schema::Ref {
                            name: Name::new("missing").unwrap(),
                        },
                        order: RecordFieldOrder::Ignore,
                        position: 0,
                        custom_attributes: Default::default(),
                    }],
                    lookup: [("field_name".to_string(), 0)].iter().cloned().collect(),
                    attributes: Default::default(),
                }),
                false,
                r#"Invalid value: Record([("field_name", Null)]) for schema: Record(RecordSchema { name: Name { name: "record_name", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "field_name", doc: None, aliases: None, default: None, schema: Ref { name: Name { name: "missing", namespace: None } }, order: Ignore, position: 0, custom_attributes: {} }], lookup: {"field_name": 0}, attributes: {} }). Reason: Unresolved schema reference: 'Name { name: "missing", namespace: None }'. Parsed names: []"#,
            ),
        ];

        for (value, schema, valid, expected_err_message) in value_schema_valid.into_iter() {
            let err_message =
                value.validate_internal::<Schema>(&schema, &HashMap::default(), &None);
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

        Ok(())
    }

    #[test]
    fn validate_fixed() -> TestResult {
        let schema = Schema::Fixed(FixedSchema {
            size: 4,
            name: Name::new("some_fixed").unwrap(),
            aliases: None,
            doc: None,
            default: None,
            attributes: Default::default(),
        });

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

        Ok(())
    }

    #[test]
    fn validate_enum() -> TestResult {
        let schema = Schema::Enum(EnumSchema {
            name: Name::new("some_enum").unwrap(),
            aliases: None,
            doc: None,
            symbols: vec![
                "spades".to_string(),
                "hearts".to_string(),
                "diamonds".to_string(),
                "clubs".to_string(),
            ],
            default: None,
            attributes: Default::default(),
        });

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

        let other_schema = Schema::Enum(EnumSchema {
            name: Name::new("some_other_enum").unwrap(),
            aliases: None,
            doc: None,
            symbols: vec![
                "hearts".to_string(),
                "diamonds".to_string(),
                "clubs".to_string(),
                "spades".to_string(),
            ],
            default: None,
            attributes: Default::default(),
        });

        let value = Value::Enum(0, "spades".to_string());
        assert!(!value.validate(&other_schema));
        assert_logged(
            format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, other_schema, "Symbol 'spades' is not at position '0'"
            )
            .as_str(),
        );

        Ok(())
    }

    #[test]
    fn validate_record() -> TestResult {
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
        let schema = Schema::Record(RecordSchema {
            name: Name::new("some_record").unwrap(),
            aliases: None,
            doc: None,
            fields: vec![
                RecordField {
                    name: "a".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Long,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    custom_attributes: Default::default(),
                },
                RecordField {
                    name: "b".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::String,
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                    custom_attributes: Default::default(),
                },
                RecordField {
                    name: "c".to_string(),
                    doc: None,
                    default: Some(JsonValue::Null),
                    aliases: None,
                    schema: Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int])?),
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
        });

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
            r#"Invalid value: Record([("a", Boolean(false)), ("b", String("foo"))]) for schema: Record(RecordSchema { name: Name { name: "some_record", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "a", doc: None, aliases: None, default: None, schema: Long, order: Ascending, position: 0, custom_attributes: {} }, RecordField { name: "b", doc: None, aliases: None, default: None, schema: String, order: Ascending, position: 1, custom_attributes: {} }, RecordField { name: "c", doc: None, aliases: None, default: Some(Null), schema: Union(UnionSchema { schemas: [Null, Int], variant_index: {Null: 0, Int: 1} }), order: Ascending, position: 2, custom_attributes: {} }], lookup: {"a": 0, "b": 1, "c": 2}, attributes: {} }). Reason: Unsupported value-schema combination! Value: Boolean(false), schema: Long"#,
        );

        let value = Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("c".to_string(), Value::String("foo".to_string())),
        ]);
        assert!(!value.validate(&schema));
        assert_logged(
            r#"Invalid value: Record([("a", Long(42)), ("c", String("foo"))]) for schema: Record(RecordSchema { name: Name { name: "some_record", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "a", doc: None, aliases: None, default: None, schema: Long, order: Ascending, position: 0, custom_attributes: {} }, RecordField { name: "b", doc: None, aliases: None, default: None, schema: String, order: Ascending, position: 1, custom_attributes: {} }, RecordField { name: "c", doc: None, aliases: None, default: Some(Null), schema: Union(UnionSchema { schemas: [Null, Int], variant_index: {Null: 0, Int: 1} }), order: Ascending, position: 2, custom_attributes: {} }], lookup: {"a": 0, "b": 1, "c": 2}, attributes: {} }). Reason: Could not find matching type in union"#,
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
            r#"Invalid value: Record([("a", Long(42)), ("d", String("foo"))]) for schema: Record(RecordSchema { name: Name { name: "some_record", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "a", doc: None, aliases: None, default: None, schema: Long, order: Ascending, position: 0, custom_attributes: {} }, RecordField { name: "b", doc: None, aliases: None, default: None, schema: String, order: Ascending, position: 1, custom_attributes: {} }, RecordField { name: "c", doc: None, aliases: None, default: Some(Null), schema: Union(UnionSchema { schemas: [Null, Int], variant_index: {Null: 0, Int: 1} }), order: Ascending, position: 2, custom_attributes: {} }], lookup: {"a": 0, "b": 1, "c": 2}, attributes: {} }). Reason: There is no schema field for field 'd'"#,
        );

        let value = Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("b".to_string(), Value::String("foo".to_string())),
            ("c".to_string(), Value::Null),
            ("d".to_string(), Value::Null),
        ]);
        assert!(!value.validate(&schema));
        assert_logged(
            r#"Invalid value: Record([("a", Long(42)), ("b", String("foo")), ("c", Null), ("d", Null)]) for schema: Record(RecordSchema { name: Name { name: "some_record", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "a", doc: None, aliases: None, default: None, schema: Long, order: Ascending, position: 0, custom_attributes: {} }, RecordField { name: "b", doc: None, aliases: None, default: None, schema: String, order: Ascending, position: 1, custom_attributes: {} }, RecordField { name: "c", doc: None, aliases: None, default: Some(Null), schema: Union(UnionSchema { schemas: [Null, Int], variant_index: {Null: 0, Int: 1} }), order: Ascending, position: 2, custom_attributes: {} }], lookup: {"a": 0, "b": 1, "c": 2}, attributes: {} }). Reason: The value's records length (4) is greater than the schema's (3 fields)"#,
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
            r#"Invalid value: Map({"d": Long(123)}) for schema: Record(RecordSchema { name: Name { name: "some_record", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "a", doc: None, aliases: None, default: None, schema: Long, order: Ascending, position: 0, custom_attributes: {} }, RecordField { name: "b", doc: None, aliases: None, default: None, schema: String, order: Ascending, position: 1, custom_attributes: {} }, RecordField { name: "c", doc: None, aliases: None, default: Some(Null), schema: Union(UnionSchema { schemas: [Null, Int], variant_index: {Null: 0, Int: 1} }), order: Ascending, position: 2, custom_attributes: {} }], lookup: {"a": 0, "b": 1, "c": 2}, attributes: {} }). Reason: Field with name '"a"' is not a member of the map items
Field with name '"b"' is not a member of the map items"#,
        );

        let union_schema = Schema::Union(UnionSchema::new(vec![Schema::Null, schema])?);

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

        Ok(())
    }

    #[test]
    fn resolve_bytes_ok() -> TestResult {
        let value = Value::Array(vec![Value::Int(0), Value::Int(42)]);
        assert_eq!(
            value.resolve(&Schema::Bytes)?,
            Value::Bytes(vec![0u8, 42u8])
        );

        Ok(())
    }

    #[test]
    fn resolve_string_from_bytes() -> TestResult {
        let value = Value::Bytes(vec![97, 98, 99]);
        assert_eq!(
            value.resolve(&Schema::String)?,
            Value::String("abc".to_string())
        );

        Ok(())
    }

    #[test]
    fn resolve_string_from_fixed() -> TestResult {
        let value = Value::Fixed(3, vec![97, 98, 99]);
        assert_eq!(
            value.resolve(&Schema::String)?,
            Value::String("abc".to_string())
        );

        Ok(())
    }

    #[test]
    fn resolve_bytes_failure() {
        let value = Value::Array(vec![Value::Int(2000), Value::Int(-42)]);
        assert!(value.resolve(&Schema::Bytes).is_err());
    }

    #[test]
    fn resolve_decimal_bytes() -> TestResult {
        let value = Value::Decimal(Decimal::from(vec![1, 2, 3, 4, 5]));
        value.clone().resolve(&Schema::Decimal(DecimalSchema {
            precision: 10,
            scale: 4,
            inner: Box::new(Schema::Bytes),
        }))?;
        assert!(value.resolve(&Schema::String).is_err());

        Ok(())
    }

    #[test]
    fn resolve_decimal_invalid_scale() {
        let value = Value::Decimal(Decimal::from(vec![1, 2]));
        assert!(value
            .resolve(&Schema::Decimal(DecimalSchema {
                precision: 2,
                scale: 3,
                inner: Box::new(Schema::Bytes),
            }))
            .is_err());
    }

    #[test]
    fn resolve_decimal_invalid_precision_for_length() {
        let value = Value::Decimal(Decimal::from((1u8..=8u8).rev().collect::<Vec<_>>()));
        assert!(value
            .resolve(&Schema::Decimal(DecimalSchema {
                precision: 1,
                scale: 0,
                inner: Box::new(Schema::Bytes),
            }))
            .is_ok());
    }

    #[test]
    fn resolve_decimal_fixed() {
        let value = Value::Decimal(Decimal::from(vec![1, 2, 3, 4, 5]));
        assert!(value
            .clone()
            .resolve(&Schema::Decimal(DecimalSchema {
                precision: 10,
                scale: 1,
                inner: Box::new(Schema::Fixed(FixedSchema {
                    name: Name::new("decimal").unwrap(),
                    aliases: None,
                    size: 20,
                    doc: None,
                    default: None,
                    attributes: Default::default(),
                }))
            }))
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
    fn test_avro_3914_resolve_timestamp_nanos() {
        let value = Value::TimestampNanos(10);
        assert!(value.clone().resolve(&Schema::TimestampNanos).is_ok());
        assert!(value.resolve(&Schema::Int).is_err());

        let value = Value::Double(10.0);
        assert!(value.resolve(&Schema::TimestampNanos).is_err());
    }

    #[test]
    fn test_avro_3853_resolve_timestamp_millis() {
        let value = Value::LocalTimestampMillis(10);
        assert!(value.clone().resolve(&Schema::LocalTimestampMillis).is_ok());
        assert!(value.resolve(&Schema::Float).is_err());

        let value = Value::Float(10.0f32);
        assert!(value.resolve(&Schema::LocalTimestampMillis).is_err());
    }

    #[test]
    fn test_avro_3853_resolve_timestamp_micros() {
        let value = Value::LocalTimestampMicros(10);
        assert!(value.clone().resolve(&Schema::LocalTimestampMicros).is_ok());
        assert!(value.resolve(&Schema::Int).is_err());

        let value = Value::Double(10.0);
        assert!(value.resolve(&Schema::LocalTimestampMicros).is_err());
    }

    #[test]
    fn test_avro_3916_resolve_timestamp_nanos() {
        let value = Value::LocalTimestampNanos(10);
        assert!(value.clone().resolve(&Schema::LocalTimestampNanos).is_ok());
        assert!(value.resolve(&Schema::Int).is_err());

        let value = Value::Double(10.0);
        assert!(value.resolve(&Schema::LocalTimestampNanos).is_err());
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
    fn resolve_uuid() -> TestResult {
        let value = Value::Uuid(Uuid::parse_str("1481531d-ccc9-46d9-a56f-5b67459c0537")?);
        assert!(value.clone().resolve(&Schema::Uuid).is_ok());
        assert!(value.resolve(&Schema::TimestampMicros).is_err());

        Ok(())
    }

    #[test]
    fn avro_3678_resolve_float_to_double() {
        let value = Value::Float(2345.1);
        assert!(value.resolve(&Schema::Double).is_ok());
    }

    #[test]
    fn test_avro_3621_resolve_to_nullable_union() -> TestResult {
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
        )?;

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

        Ok(())
    }

    #[test]
    fn json_from_avro() -> TestResult {
        assert_eq!(JsonValue::try_from(Value::Null)?, JsonValue::Null);
        assert_eq!(
            JsonValue::try_from(Value::Boolean(true))?,
            JsonValue::Bool(true)
        );
        assert_eq!(
            JsonValue::try_from(Value::Int(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Long(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Float(1.0))?,
            JsonValue::Number(Number::from_f64(1.0).unwrap())
        );
        assert_eq!(
            JsonValue::try_from(Value::Double(1.0))?,
            JsonValue::Number(Number::from_f64(1.0).unwrap())
        );
        assert_eq!(
            JsonValue::try_from(Value::Bytes(vec![1, 2, 3]))?,
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into())
            ])
        );
        assert_eq!(
            JsonValue::try_from(Value::String("test".into()))?,
            JsonValue::String("test".into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Fixed(3, vec![1, 2, 3]))?,
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into())
            ])
        );
        assert_eq!(
            JsonValue::try_from(Value::Enum(1, "test_enum".into()))?,
            JsonValue::String("test_enum".into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Union(1, Box::new(Value::String("test_enum".into()))))?,
            JsonValue::String("test_enum".into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Array(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3)
            ]))?,
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
            ))?,
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
            ]))?,
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
            JsonValue::try_from(Value::Date(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Decimal(vec![1, 2, 3].into()))?,
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into())
            ])
        );
        assert_eq!(
            JsonValue::try_from(Value::TimeMillis(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::TimeMicros(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::TimestampMillis(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::TimestampMicros(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::TimestampNanos(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::LocalTimestampMillis(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::LocalTimestampMicros(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::LocalTimestampNanos(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Duration(
                [1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8, 9u8, 10u8, 11u8, 12u8].into()
            ))?,
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
            JsonValue::try_from(Value::Uuid(Uuid::parse_str(
                "936DA01F-9ABD-4D9D-80C7-02AF85C822A8"
            )?))?,
            JsonValue::String("936da01f-9abd-4d9d-80c7-02af85c822a8".into())
        );

        Ok(())
    }

    #[test]
    fn test_avro_3433_recursive_resolves_record() -> TestResult {
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
        )?;

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer = Value::Record(vec![("a".into(), inner_value1), ("b".into(), inner_value2)]);
        outer
            .resolve(&schema)
            .expect("Record definition defined in one field must be available in other field");

        Ok(())
    }

    #[test]
    fn test_avro_3433_recursive_resolves_array() -> TestResult {
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
        )?;

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

        Ok(())
    }

    #[test]
    fn test_avro_3433_recursive_resolves_map() -> TestResult {
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
        )?;

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

        Ok(())
    }

    #[test]
    fn test_avro_3433_recursive_resolves_record_wrapper() -> TestResult {
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
        )?;

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![(
            "j".into(),
            Value::Record(vec![("z".into(), Value::Int(6))]),
        )]);
        let outer_value =
            Value::Record(vec![("a".into(), inner_value1), ("b".into(), inner_value2)]);
        outer_value.resolve(&schema).expect("Record schema defined in field must be resolvable in Record schema defined in other field");

        Ok(())
    }

    #[test]
    fn test_avro_3433_recursive_resolves_map_and_array() -> TestResult {
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
        )?;

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

        Ok(())
    }

    #[test]
    fn test_avro_3433_recursive_resolves_union() -> TestResult {
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
        )?;

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

        Ok(())
    }

    #[test]
    fn test_avro_3461_test_multi_level_resolve_outer_namespace() -> TestResult {
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
        let schema = Schema::parse_str(schema)?;
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

        Ok(())
    }

    #[test]
    fn test_avro_3461_test_multi_level_resolve_middle_namespace() -> TestResult {
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
        let schema = Schema::parse_str(schema)?;
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

        Ok(())
    }

    #[test]
    fn test_avro_3461_test_multi_level_resolve_inner_namespace() -> TestResult {
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
        let schema = Schema::parse_str(schema)?;

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

        Ok(())
    }

    #[test]
    fn test_avro_3460_validation_with_refs() -> TestResult {
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
        )?;

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

        Ok(())
    }

    #[test]
    fn test_avro_3460_validation_with_refs_real_struct() -> TestResult {
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
        )?;

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
        let test_outer1: Value = test_outer1.serialize(&mut ser)?;
        let mut ser = Serializer::default();
        let test_outer2: Value = test_outer2.serialize(&mut ser)?;
        let mut ser = Serializer::default();
        let test_outer3: Value = test_outer3.serialize(&mut ser)?;

        assert!(
            !test_outer1.validate(&schema),
            "field b record is invalid against the schema"
        );
        assert!(
            !test_outer2.validate(&schema),
            "field b record is invalid against the schema"
        );
        assert!(
            !test_outer3.validate(&schema),
            "field b record is invalid against the schema"
        );

        Ok(())
    }

    fn avro_3674_with_or_without_namespace(with_namespace: bool) -> TestResult {
        use crate::ser::Serializer;
        use serde::Serialize;

        let schema_str = r#"{
            "type": "record",
            "name": "NamespacedMessage",
            [NAMESPACE]
            "fields": [
                {
                    "type": "record",
                    "name": "field_a",
                    "fields": [
                        {
                            "name": "enum_a",
                            "type": {
                                "type": "enum",
                                "name": "EnumType",
                                "symbols": [
                                    "SYMBOL_1",
                                    "SYMBOL_2"
                                ],
                                "default": "SYMBOL_1"
                            }
                        },
                        {
                            "name": "enum_b",
                            "type": "EnumType"
                        }
                    ]
                }
            ]
        }"#;
        let schema_str = schema_str.replace(
            "[NAMESPACE]",
            if with_namespace {
                r#""namespace": "com.domain","#
            } else {
                ""
            },
        );

        let schema = Schema::parse_str(&schema_str)?;

        #[derive(Serialize)]
        enum EnumType {
            #[serde(rename = "SYMBOL_1")]
            Symbol1,
            #[serde(rename = "SYMBOL_2")]
            Symbol2,
        }

        #[derive(Serialize)]
        struct FieldA {
            enum_a: EnumType,
            enum_b: EnumType,
        }

        #[derive(Serialize)]
        struct NamespacedMessage {
            field_a: FieldA,
        }

        let msg = NamespacedMessage {
            field_a: FieldA {
                enum_a: EnumType::Symbol2,
                enum_b: EnumType::Symbol1,
            },
        };

        let mut ser = Serializer::default();
        let test_value: Value = msg.serialize(&mut ser)?;
        assert!(test_value.validate(&schema), "test_value should validate");
        assert!(
            test_value.resolve(&schema).is_ok(),
            "test_value should resolve"
        );

        Ok(())
    }

    #[test]
    fn test_avro_3674_validate_no_namespace_resolution() -> TestResult {
        avro_3674_with_or_without_namespace(false)
    }

    #[test]
    fn test_avro_3674_validate_with_namespace_resolution() -> TestResult {
        avro_3674_with_or_without_namespace(true)
    }

    fn avro_3688_schema_resolution_panic(set_field_b: bool) -> TestResult {
        use crate::ser::Serializer;
        use serde::{Deserialize, Serialize};

        let schema_str = r#"{
            "type": "record",
            "name": "Message",
            "fields": [
                {
                    "name": "field_a",
                    "type": [
                        "null",
                        {
                            "name": "Inner",
                            "type": "record",
                            "fields": [
                                {
                                    "name": "inner_a",
                                    "type": "string"
                                }
                            ]
                        }
                    ],
                    "default": null
                },
                {
                    "name": "field_b",
                    "type": [
                        "null",
                        "Inner"
                    ],
                    "default": null
                }
            ]
        }"#;

        #[derive(Serialize, Deserialize)]
        struct Inner {
            inner_a: String,
        }

        #[derive(Serialize, Deserialize)]
        struct Message {
            field_a: Option<Inner>,
            field_b: Option<Inner>,
        }

        let schema = Schema::parse_str(schema_str)?;

        let msg = Message {
            field_a: Some(Inner {
                inner_a: "foo".to_string(),
            }),
            field_b: if set_field_b {
                Some(Inner {
                    inner_a: "bar".to_string(),
                })
            } else {
                None
            },
        };

        let mut ser = Serializer::default();
        let test_value: Value = msg.serialize(&mut ser)?;
        assert!(test_value.validate(&schema), "test_value should validate");
        assert!(
            test_value.resolve(&schema).is_ok(),
            "test_value should resolve"
        );

        Ok(())
    }

    #[test]
    fn test_avro_3688_field_b_not_set() -> TestResult {
        avro_3688_schema_resolution_panic(false)
    }

    #[test]
    fn test_avro_3688_field_b_set() -> TestResult {
        avro_3688_schema_resolution_panic(true)
    }

    #[test]
    fn test_avro_3764_use_resolve_schemata() -> TestResult {
        let referenced_schema =
            r#"{"name": "enumForReference", "type": "enum", "symbols": ["A", "B"]}"#;
        let main_schema = r#"{"name": "recordWithReference", "type": "record", "fields": [{"name": "reference", "type": "enumForReference"}]}"#;

        let value: serde_json::Value = serde_json::from_str(
            r#"
            {
                "reference": "A"
            }
        "#,
        )?;

        let avro_value = Value::from(value);

        let schemas = Schema::parse_list(&[main_schema, referenced_schema])?;

        let main_schema = schemas.first().unwrap();
        let schemata: Vec<_> = schemas.iter().skip(1).collect();

        let resolve_result = avro_value.clone().resolve_schemata(main_schema, schemata);

        assert!(
            resolve_result.is_ok(),
            "result of resolving with schemata should be ok, got: {:?}",
            resolve_result
        );

        let resolve_result = avro_value.resolve(main_schema);
        assert!(
            resolve_result.is_err(),
            "result of resolving without schemata should be err, got: {:?}",
            resolve_result
        );

        Ok(())
    }

    #[test]
    fn test_avro_3767_union_resolve_complex_refs() -> TestResult {
        let referenced_enum =
            r#"{"name": "enumForReference", "type": "enum", "symbols": ["A", "B"]}"#;
        let referenced_record = r#"{"name": "recordForReference", "type": "record", "fields": [{"name": "refInRecord", "type": "enumForReference"}]}"#;
        let main_schema = r#"{"name": "recordWithReference", "type": "record", "fields": [{"name": "reference", "type": ["null", "recordForReference"]}]}"#;

        let value: serde_json::Value = serde_json::from_str(
            r#"
            {
                "reference": {
                    "refInRecord": "A"
                }
            }
        "#,
        )?;

        let avro_value = Value::from(value);

        let schemata = Schema::parse_list(&[referenced_enum, referenced_record, main_schema])?;

        let main_schema = schemata.last().unwrap();
        let other_schemata: Vec<&Schema> = schemata.iter().take(2).collect();

        let resolve_result = avro_value.resolve_schemata(main_schema, other_schemata);

        assert!(
            resolve_result.is_ok(),
            "result of resolving with schemata should be ok, got: {:?}",
            resolve_result
        );

        assert!(
            resolve_result?.validate_schemata(schemata.iter().collect()),
            "result of validation with schemata should be true"
        );

        Ok(())
    }

    #[test]
    fn test_avro_3782_incorrect_decimal_resolving() -> TestResult {
        let schema = r#"{"name": "decimalSchema", "logicalType": "decimal", "type": "fixed", "precision": 8, "scale": 0, "size": 8}"#;

        let avro_value = Value::Decimal(Decimal::from(
            BigInt::from(12345678u32).to_signed_bytes_be(),
        ));
        let schema = Schema::parse_str(schema)?;
        let resolve_result = avro_value.resolve(&schema);
        assert!(
            resolve_result.is_ok(),
            "resolve result must be ok, got: {resolve_result:?}"
        );

        Ok(())
    }

    #[test]
    fn test_avro_3779_bigdecimal_resolving() -> TestResult {
        let schema =
            r#"{"name": "bigDecimalSchema", "logicalType": "big-decimal", "type": "bytes" }"#;

        let avro_value = Value::BigDecimal(BigDecimal::from(12345678u32));
        let schema = Schema::parse_str(schema)?;
        let resolve_result: AvroResult<Value> = avro_value.resolve(&schema);
        assert!(
            resolve_result.is_ok(),
            "resolve result must be ok, got: {resolve_result:?}"
        );

        Ok(())
    }

    #[test]
    fn test_avro_3892_resolve_fixed_from_bytes() -> TestResult {
        let value = Value::Bytes(vec![97, 98, 99]);
        assert_eq!(
            value.resolve(&Schema::Fixed(FixedSchema {
                name: "test".into(),
                aliases: None,
                doc: None,
                size: 3,
                default: None,
                attributes: Default::default()
            }))?,
            Value::Fixed(3, vec![97, 98, 99])
        );

        let value = Value::Bytes(vec![97, 99]);
        assert!(value
            .resolve(&Schema::Fixed(FixedSchema {
                name: "test".into(),
                aliases: None,
                doc: None,
                size: 3,
                default: None,
                attributes: Default::default()
            }))
            .is_err(),);

        let value = Value::Bytes(vec![97, 98, 99, 100]);
        assert!(value
            .resolve(&Schema::Fixed(FixedSchema {
                name: "test".into(),
                aliases: None,
                doc: None,
                size: 3,
                default: None,
                attributes: Default::default()
            }))
            .is_err(),);

        Ok(())
    }

    #[test]
    fn avro_3928_from_serde_value_to_types_value() {
        assert_eq!(Value::from(serde_json::Value::Null), Value::Null);
        assert_eq!(Value::from(json!(true)), Value::Boolean(true));
        assert_eq!(Value::from(json!(false)), Value::Boolean(false));
        assert_eq!(Value::from(json!(0)), Value::Int(0));
        assert_eq!(Value::from(json!(i32::MIN)), Value::Int(i32::MIN));
        assert_eq!(Value::from(json!(i32::MAX)), Value::Int(i32::MAX));
        assert_eq!(
            Value::from(json!(i32::MIN as i64 - 1)),
            Value::Long(i32::MIN as i64 - 1)
        );
        assert_eq!(
            Value::from(json!(i32::MAX as i64 + 1)),
            Value::Long(i32::MAX as i64 + 1)
        );
        assert_eq!(Value::from(json!(1.23)), Value::Double(1.23));
        assert_eq!(Value::from(json!(-1.23)), Value::Double(-1.23));
        assert_eq!(Value::from(json!(u64::MIN)), Value::Int(u64::MIN as i32));
        assert_eq!(Value::from(json!(u64::MAX)), Value::Long(u64::MAX as i64));
        assert_eq!(
            Value::from(json!("some text")),
            Value::String("some text".into())
        );
        assert_eq!(
            Value::from(json!(["text1", "text2", "text3"])),
            Value::Array(vec![
                Value::String("text1".into()),
                Value::String("text2".into()),
                Value::String("text3".into())
            ])
        );
        assert_eq!(
            Value::from(json!({"key1": "value1", "key2": "value2"})),
            Value::Map(
                vec![
                    ("key1".into(), Value::String("value1".into())),
                    ("key2".into(), Value::String("value2".into()))
                ]
                .into_iter()
                .collect()
            )
        );
    }

    #[test]
    fn avro_4024_resolve_double_from_unknown_string_err() -> TestResult {
        let schema = Schema::parse_str(r#"{"type": "double"}"#)?;
        let value = Value::String("unknown".to_owned());
        match value.resolve(&schema) {
            Err(err @ Error::GetDouble(_)) => {
                assert_eq!(
                    format!("{err:?}"),
                    r#"Expected Value::Double, Value::Float, Value::Int, Value::Long or Value::String ("NaN", "INF", "Infinity", "-INF" or "-Infinity"), got: String("unknown")"#
                );
            }
            other => {
                panic!("Expected Error::GetDouble, got {other:?}");
            }
        }
        Ok(())
    }

    #[test]
    fn avro_4024_resolve_float_from_unknown_string_err() -> TestResult {
        let schema = Schema::parse_str(r#"{"type": "float"}"#)?;
        let value = Value::String("unknown".to_owned());
        match value.resolve(&schema) {
            Err(err @ Error::GetFloat(_)) => {
                assert_eq!(
                    format!("{err:?}"),
                    r#"Expected Value::Float, Value::Double, Value::Int, Value::Long or Value::String ("NaN", "INF", "Infinity", "-INF" or "-Infinity"), got: String("unknown")"#
                );
            }
            other => {
                panic!("Expected Error::GetFloat, got {other:?}");
            }
        }
        Ok(())
    }

    #[test]
    fn avro_4029_resolve_from_unsupported_err() -> TestResult {
        let data: Vec<(&str, Value, &str)> = vec!(
            (r#"{ "name": "NAME", "type": "int" }"#, Value::Float(123_f32), "Expected Value::Int, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "fixed", "size": 3 }"#, Value::Float(123_f32), "String expected for fixed, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "bytes" }"#, Value::Float(123_f32), "Expected Value::Bytes, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "string", "logicalType": "uuid" }"#, Value::String("abc-1234".into()), "Failed to convert &str to UUID: invalid group count: expected 5, found 2"),
            (r#"{ "name": "NAME", "type": "string", "logicalType": "uuid" }"#, Value::Float(123_f32), "Expected Value::Uuid, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "bytes", "logicalType": "big-decimal" }"#, Value::Float(123_f32), "Expected Value::BigDecimal, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "fixed", "size": 12, "logicalType": "duration" }"#, Value::Float(123_f32), "Expected Value::Duration or Value::Fixed(12), got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 3 }"#, Value::Float(123_f32), "Expected Value::Decimal, Value::Bytes or Value::Fixed, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "bytes" }"#, Value::Array(vec!(Value::Long(256_i64))), "Unable to convert to u8, got Int(256)"),
            (r#"{ "name": "NAME", "type": "int", "logicalType": "date" }"#, Value::Float(123_f32), "Expected Value::Date or Value::Int, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "int", "logicalType": "time-millis" }"#, Value::Float(123_f32), "Expected Value::TimeMillis or Value::Int, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "long", "logicalType": "time-micros" }"#, Value::Float(123_f32), "Expected Value::TimeMicros, Value::Long or Value::Int, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "long", "logicalType": "timestamp-millis" }"#, Value::Float(123_f32), "Expected Value::TimestampMillis, Value::Long or Value::Int, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "long", "logicalType": "timestamp-micros" }"#, Value::Float(123_f32), "Expected Value::TimestampMicros, Value::Long or Value::Int, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "long", "logicalType": "timestamp-nanos" }"#, Value::Float(123_f32), "Expected Value::TimestampNanos, Value::Long or Value::Int, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "long", "logicalType": "local-timestamp-millis" }"#, Value::Float(123_f32), "Expected Value::LocalTimestampMillis, Value::Long or Value::Int, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "long", "logicalType": "local-timestamp-micros" }"#, Value::Float(123_f32), "Expected Value::LocalTimestampMicros, Value::Long or Value::Int, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "long", "logicalType": "local-timestamp-nanos" }"#, Value::Float(123_f32), "Expected Value::LocalTimestampNanos, Value::Long or Value::Int, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "null" }"#, Value::Float(123_f32), "Expected Value::Null, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "boolean" }"#, Value::Float(123_f32), "Expected Value::Boolean, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "int" }"#, Value::Float(123_f32), "Expected Value::Int, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "long" }"#, Value::Float(123_f32), "Expected Value::Long or Value::Int, got: Float(123.0)"),
            (r#"{ "name": "NAME", "type": "float" }"#, Value::Boolean(false), r#"Expected Value::Float, Value::Double, Value::Int, Value::Long or Value::String ("NaN", "INF", "Infinity", "-INF" or "-Infinity"), got: Boolean(false)"#),
            (r#"{ "name": "NAME", "type": "double" }"#, Value::Boolean(false), r#"Expected Value::Double, Value::Float, Value::Int, Value::Long or Value::String ("NaN", "INF", "Infinity", "-INF" or "-Infinity"), got: Boolean(false)"#),
            (r#"{ "name": "NAME", "type": "string" }"#, Value::Boolean(false), "Expected Value::String, Value::Bytes or Value::Fixed, got: Boolean(false)"),
            (r#"{ "name": "NAME", "type": "enum", "symbols": ["one", "two"] }"#, Value::Boolean(false), "Expected Value::Enum, got: Boolean(false)"),
        );

        for (schema_str, value, expected_error) in data {
            let schema = Schema::parse_str(schema_str)?;
            match value.resolve(&schema) {
                Err(error) => {
                    assert_eq!(format!("{error}"), expected_error);
                }
                other => {
                    panic!("Expected '{expected_error}', got {other:?}");
                }
            }
        }
        Ok(())
    }
}
