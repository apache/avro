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

//! Logic for parsing and interacting with schemas in Avro format.
use crate::{error::Error, types, util::MapHelper, AvroResult};
use digest::Digest;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Deserialize, Serialize, Serializer,
};
use serde_json::{Map, Value};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    convert::TryInto,
    fmt,
    str::FromStr,
};
use strum_macros::{EnumDiscriminants, EnumString};

lazy_static! {
    static ref ENUM_SYMBOL_NAME: Regex = Regex::new(r"[A-Za-z_][A-Za-z0-9_]*").unwrap();
}

/// Represents an Avro schema fingerprint
/// More information about Avro schema fingerprints can be found in the
/// [Avro Schema Fingerprint documentation](https://avro.apache.org/docs/current/spec.html#schema_fingerprints)
pub struct SchemaFingerprint {
    pub bytes: Vec<u8>,
}

impl fmt::Display for SchemaFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            self.bytes
                .iter()
                .map(|byte| format!("{:02x}", byte))
                .collect::<Vec<String>>()
                .join("")
        )
    }
}

/// Represents any valid Avro schema
/// More information about Avro schemas can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/spec.html#schemas)
#[derive(Clone, Debug, EnumDiscriminants)]
#[strum_discriminants(name(SchemaKind), derive(Hash))]
pub enum Schema {
    /// A `null` Avro schema.
    Null,
    /// A `boolean` Avro schema.
    Boolean,
    /// An `int` Avro schema.
    Int,
    /// A `long` Avro schema.
    Long,
    /// A `float` Avro schema.
    Float,
    /// A `double` Avro schema.
    Double,
    /// A `bytes` Avro schema.
    /// `Bytes` represents a sequence of 8-bit unsigned bytes.
    Bytes,
    /// A `string` Avro schema.
    /// `String` represents a unicode character sequence.
    String,
    /// A `array` Avro schema. Avro arrays are required to have the same type for each element.
    /// This variant holds the `Schema` for the array element type.
    Array(Box<Schema>),
    /// A `map` Avro schema.
    /// `Map` holds a pointer to the `Schema` of its values, which must all be the same schema.
    /// `Map` keys are assumed to be `string`.
    Map(Box<Schema>),
    /// A `union` Avro schema.
    Union(UnionSchema),
    /// A `record` Avro schema.
    ///
    /// The `lookup` table maps field names to their position in the `Vec`
    /// of `fields`.
    Record {
        name: Name,
        doc: Documentation,
        fields: Vec<RecordField>,
        lookup: HashMap<String, usize>,
    },
    /// An `enum` Avro schema.
    Enum {
        name: Name,
        doc: Documentation,
        symbols: Vec<String>,
    },
    /// A `fixed` Avro schema.
    Fixed {
        name: Name,
        doc: Documentation,
        size: usize,
    },
    /// Logical type which represents `Decimal` values. The underlying type is serialized and
    /// deserialized as `Schema::Bytes` or `Schema::Fixed`.
    ///
    /// `scale` defaults to 0 and is an integer greater than or equal to 0 and `precision` is an
    /// integer greater than 0.
    Decimal {
        precision: DecimalMetadata,
        scale: DecimalMetadata,
        inner: Box<Schema>,
    },
    /// A universally unique identifier, annotating a string.
    Uuid,
    /// Logical type which represents the number of days since the unix epoch.
    /// Serialization format is `Schema::Int`.
    Date,
    /// The time of day in number of milliseconds after midnight with no reference any calendar,
    /// time zone or date in particular.
    TimeMillis,
    /// The time of day in number of microseconds after midnight with no reference any calendar,
    /// time zone or date in particular.
    TimeMicros,
    /// An instant in time represented as the number of milliseconds after the UNIX epoch.
    TimestampMillis,
    /// An instant in time represented as the number of microseconds after the UNIX epoch.
    TimestampMicros,
    /// An amount of time defined by a number of months, days and milliseconds.
    Duration,
    // A reference to another schema.
    Ref {
        name: Name,
    },
}

impl PartialEq for Schema {
    /// Assess equality of two `Schema` based on [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
    fn eq(&self, other: &Self) -> bool {
        self.canonical_form() == other.canonical_form()
    }
}

impl SchemaKind {
    pub fn is_primitive(self) -> bool {
        matches!(
            self,
            SchemaKind::Null
                | SchemaKind::Boolean
                | SchemaKind::Int
                | SchemaKind::Long
                | SchemaKind::Double
                | SchemaKind::Float
                | SchemaKind::Bytes
                | SchemaKind::String,
        )
    }

    pub fn is_named(self) -> bool {
        matches!(
            self,
            SchemaKind::Record | SchemaKind::Enum | SchemaKind::Fixed
        )
    }
}

impl<'a> From<&'a types::Value> for SchemaKind {
    fn from(value: &'a types::Value) -> Self {
        use crate::types::Value;
        match value {
            Value::Null => Self::Null,
            Value::Boolean(_) => Self::Boolean,
            Value::Int(_) => Self::Int,
            Value::Long(_) => Self::Long,
            Value::Float(_) => Self::Float,
            Value::Double(_) => Self::Double,
            Value::Bytes(_) => Self::Bytes,
            Value::String(_) => Self::String,
            Value::Array(_) => Self::Array,
            Value::Map(_) => Self::Map,
            Value::Union(_, _) => Self::Union,
            Value::Record(_) => Self::Record,
            Value::Enum(_, _) => Self::Enum,
            Value::Fixed(_, _) => Self::Fixed,
            Value::Decimal { .. } => Self::Decimal,
            Value::Uuid(_) => Self::Uuid,
            Value::Date(_) => Self::Date,
            Value::TimeMillis(_) => Self::TimeMillis,
            Value::TimeMicros(_) => Self::TimeMicros,
            Value::TimestampMillis(_) => Self::TimestampMillis,
            Value::TimestampMicros(_) => Self::TimestampMicros,
            Value::Duration { .. } => Self::Duration,
        }
    }
}

/// Represents names for `record`, `enum` and `fixed` Avro schemas.
///
/// Each of these `Schema`s have a `fullname` composed of two parts:
///   * a name
///   * a namespace
///
/// `aliases` can also be defined, to facilitate schema evolution.
///
/// More information about schema names can be found in the
/// [Avro specification](https://avro.apache.org/docs/current/spec.html#names)
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Name {
    pub name: String,
    pub namespace: Option<String>,
    pub aliases: Option<Vec<String>>,
}

/// Represents documentation for complex Avro schemas.
pub type Documentation = Option<String>;

impl Name {
    /// Create a new `Name`.
    /// No `namespace` nor `aliases` will be defined.
    pub fn new(name: &str) -> Name {
        Name {
            name: name.to_owned(),
            namespace: None,
            aliases: None,
        }
    }

    /// Parse a `serde_json::Value` into a `Name`.
    fn parse(complex: &Map<String, Value>) -> AvroResult<Self> {
        let name = complex.name().ok_or(Error::GetNameField)?;

        let type_name = match complex.get("type") {
            Some(Value::Object(complex_type)) => complex_type.name().or(None),
            _ => None,
        };

        let namespace = complex.string("namespace");

        let aliases: Option<Vec<String>> = complex
            .get("aliases")
            .and_then(|aliases| aliases.as_array())
            .and_then(|aliases| {
                aliases
                    .iter()
                    .map(|alias| alias.as_str())
                    .map(|alias| alias.map(|a| a.to_string()))
                    .collect::<Option<_>>()
            });

        Ok(Name {
            name: type_name.unwrap_or(name),
            namespace,
            aliases,
        })
    }

    /// Return the `fullname` of this `Name`
    ///
    /// More information about fullnames can be found in the
    /// [Avro specification](https://avro.apache.org/docs/current/spec.html#names)
    pub fn fullname(&self, default_namespace: Option<&str>) -> String {
        if self.name.contains('.') {
            self.name.clone()
        } else {
            let namespace = self
                .namespace
                .as_ref()
                .map(|s| s.as_ref())
                .or(default_namespace);

            match namespace {
                Some(ref namespace) => format!("{}.{}", namespace, self.name),
                None => self.name.clone(),
            }
        }
    }
}

/// Represents a `field` in a `record` Avro schema.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordField {
    /// Name of the field.
    pub name: String,
    /// Documentation of the field.
    pub doc: Documentation,
    /// Default value of the field.
    /// This value will be used when reading Avro datum if schema resolution
    /// is enabled.
    pub default: Option<Value>,
    /// Schema of the field.
    pub schema: Schema,
    /// Order of the field.
    ///
    /// **NOTE** This currently has no effect.
    pub order: RecordFieldOrder,
    /// Position of the field in the list of `field` of its parent `Schema`
    pub position: usize,
}

/// Represents any valid order for a `field` in a `record` Avro schema.
#[derive(Clone, Debug, PartialEq, EnumString)]
#[strum(serialize_all = "kebab_case")]
pub enum RecordFieldOrder {
    Ascending,
    Descending,
    Ignore,
}

impl RecordField {
    /// Parse a `serde_json::Value` into a `RecordField`.
    fn parse(field: &Map<String, Value>, position: usize, parser: &mut Parser) -> AvroResult<Self> {
        let name = field.name().ok_or(Error::GetNameFieldFromRecord)?;

        // TODO: "type" = "<record name>"
        let schema = parser.parse_complex(field)?;

        let default = field.get("default").cloned();

        let order = field
            .get("order")
            .and_then(|order| order.as_str())
            .and_then(|order| RecordFieldOrder::from_str(order).ok())
            .unwrap_or(RecordFieldOrder::Ascending);

        Ok(RecordField {
            name,
            doc: field.doc(),
            default,
            schema,
            order,
            position,
        })
    }
}

#[derive(Debug, Clone)]
pub struct UnionSchema {
    pub(crate) schemas: Vec<Schema>,
    // Used to ensure uniqueness of schema inputs, and provide constant time finding of the
    // schema index given a value.
    // **NOTE** that this approach does not work for named types, and will have to be modified
    // to support that. A simple solution is to also keep a mapping of the names used.
    variant_index: HashMap<SchemaKind, usize>,
}

impl UnionSchema {
    pub(crate) fn new(schemas: Vec<Schema>) -> AvroResult<Self> {
        let mut vindex = HashMap::new();
        for (i, schema) in schemas.iter().enumerate() {
            if let Schema::Union(_) = schema {
                return Err(Error::GetNestedUnion);
            }
            let kind = SchemaKind::from(schema);
            if !kind.is_named() && vindex.insert(kind, i).is_some() {
                return Err(Error::GetUnionDuplicate);
            }
        }
        Ok(UnionSchema {
            schemas,
            variant_index: vindex,
        })
    }

    /// Returns a slice to all variants of this schema.
    pub fn variants(&self) -> &[Schema] {
        &self.schemas
    }

    /// Returns true if the first variant of this `UnionSchema` is `Null`.
    pub fn is_nullable(&self) -> bool {
        !self.schemas.is_empty() && self.schemas[0] == Schema::Null
    }

    /// Optionally returns a reference to the schema matched by this value, as well as its position
    /// within this union.
    pub fn find_schema(&self, value: &types::Value) -> Option<(usize, &Schema)> {
        let schema_kind = SchemaKind::from(value);
        if let Some(&i) = self.variant_index.get(&schema_kind) {
            // fast path
            Some((i, &self.schemas[i]))
        } else {
            // slow path (required for matching logical or named types)
            self.schemas
                .iter()
                .enumerate()
                .find(|(_, schema)| value.validate(schema))
        }
    }
}

// No need to compare variant_index, it is derivative of schemas.
impl PartialEq for UnionSchema {
    fn eq(&self, other: &UnionSchema) -> bool {
        self.schemas.eq(&other.schemas)
    }
}

type DecimalMetadata = usize;
pub(crate) type Precision = DecimalMetadata;
pub(crate) type Scale = DecimalMetadata;

fn parse_json_integer_for_decimal(value: &serde_json::Number) -> Result<DecimalMetadata, Error> {
    Ok(if value.is_u64() {
        let num = value
            .as_u64()
            .ok_or_else(|| Error::GetU64FromJson(value.clone()))?;
        num.try_into()
            .map_err(|e| Error::ConvertU64ToUsize(e, num))?
    } else if value.is_i64() {
        let num = value
            .as_i64()
            .ok_or_else(|| Error::GetI64FromJson(value.clone()))?;
        num.try_into()
            .map_err(|e| Error::ConvertI64ToUsize(e, num))?
    } else {
        return Err(Error::GetPrecisionOrScaleFromJson(value.clone()));
    })
}

#[derive(Default)]
struct Parser {
    input_schemas: HashMap<String, Value>,
    // A map of name -> Schema::Ref
    // Used to resolve cyclic references, i.e. when a
    // field's type is a reference to its record's type
    resolving_schemas: HashMap<String, Schema>,
    input_order: Vec<String>,
    // A map of name -> fully parsed Schema
    // Used to avoid parsing the same schema twice
    parsed_schemas: HashMap<String, Schema>,
}

impl Schema {
    /// Converts `self` into its [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
    pub fn canonical_form(&self) -> String {
        let json = serde_json::to_value(self)
            .unwrap_or_else(|e| panic!("cannot parse Schema from JSON: {0}", e));
        parsing_canonical_form(&json)
    }

    /// Generate [fingerprint] of Schema's [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
    /// [fingerprint]:
    /// https://avro.apache.org/docs/current/spec.html#schema_fingerprints
    pub fn fingerprint<D: Digest>(&self) -> SchemaFingerprint {
        let mut d = D::new();
        d.update(self.canonical_form());
        SchemaFingerprint {
            bytes: d.finalize().to_vec(),
        }
    }

    /// Create a `Schema` from a string representing a JSON Avro schema.
    pub fn parse_str(input: &str) -> Result<Schema, Error> {
        let mut parser = Parser::default();
        parser.parse_str(input)
    }

    /// Create a array of `Schema`'s from a list of named JSON Avro schemas (Record, Enum, and
    /// Fixed).
    ///
    /// It is allowed that the schemas have cross-dependencies; these will be resolved
    /// during parsing.
    ///
    /// If two of the input schemas have the same fullname, an Error will be returned.
    pub fn parse_list(input: &[&str]) -> Result<Vec<Schema>, Error> {
        let mut input_schemas: HashMap<String, Value> = HashMap::with_capacity(input.len());
        let mut input_order: Vec<String> = Vec::with_capacity(input.len());
        for js in input {
            let schema: Value = serde_json::from_str(js).map_err(Error::ParseSchemaJson)?;
            if let Value::Object(inner) = &schema {
                let fullname = Name::parse(inner)?.fullname(None);
                let previous_value = input_schemas.insert(fullname.clone(), schema);
                if previous_value.is_some() {
                    return Err(Error::NameCollision(fullname));
                }
                input_order.push(fullname);
            } else {
                return Err(Error::GetNameField);
            }
        }
        let mut parser = Parser {
            input_schemas,
            resolving_schemas: HashMap::default(),
            input_order,
            parsed_schemas: HashMap::with_capacity(input.len()),
        };
        parser.parse_list()
    }

    pub fn parse(value: &Value) -> AvroResult<Schema> {
        let mut parser = Parser::default();
        parser.parse(value)
    }
}

impl Parser {
    /// Create a `Schema` from a string representing a JSON Avro schema.
    fn parse_str(&mut self, input: &str) -> Result<Schema, Error> {
        // TODO: (#82) this should be a ParseSchemaError wrapping the JSON error
        let value = serde_json::from_str(input).map_err(Error::ParseSchemaJson)?;
        self.parse(&value)
    }

    /// Create an array of `Schema`'s from an iterator of JSON Avro schemas. It is allowed that
    /// the schemas have cross-dependencies; these will be resolved during parsing.
    fn parse_list(&mut self) -> Result<Vec<Schema>, Error> {
        while !self.input_schemas.is_empty() {
            let next_name = self
                .input_schemas
                .keys()
                .next()
                .expect("Input schemas unexpectedly empty")
                .to_owned();
            let (name, value) = self
                .input_schemas
                .remove_entry(&next_name)
                .expect("Key unexpectedly missing");
            let parsed = self.parse(&value)?;
            self.parsed_schemas
                .insert(get_schema_type_name(name, value), parsed);
        }

        let mut parsed_schemas = Vec::with_capacity(self.parsed_schemas.len());
        for name in self.input_order.drain(0..) {
            let parsed = self
                .parsed_schemas
                .remove(&name)
                .expect("One of the input schemas was unexpectedly not parsed");
            parsed_schemas.push(parsed);
        }
        Ok(parsed_schemas)
    }

    /// Create a `Schema` from a `serde_json::Value` representing a JSON Avro
    /// schema.
    fn parse(&mut self, value: &Value) -> AvroResult<Schema> {
        match *value {
            Value::String(ref t) => self.parse_known_schema(t.as_str()),
            Value::Object(ref data) => self.parse_complex(data),
            Value::Array(ref data) => self.parse_union(data),
            _ => Err(Error::ParseSchemaFromValidJson),
        }
    }

    /// Parse a `serde_json::Value` representing an Avro type whose Schema is known into a
    /// `Schema`. A Schema for a `serde_json::Value` is known if it is primitive or has
    /// been parsed previously by the parsed and stored in its map of parsed_schemas.
    fn parse_known_schema(&mut self, name: &str) -> AvroResult<Schema> {
        match name {
            "null" => Ok(Schema::Null),
            "boolean" => Ok(Schema::Boolean),
            "int" => Ok(Schema::Int),
            "long" => Ok(Schema::Long),
            "double" => Ok(Schema::Double),
            "float" => Ok(Schema::Float),
            "bytes" => Ok(Schema::Bytes),
            "string" => Ok(Schema::String),
            _ => self.fetch_schema(name),
        }
    }

    /// Given a name, tries to retrieve the parsed schema from `parsed_schemas`.
    /// If a parsed schema is not found, it checks if a currently resolving
    /// schema with that name exists.
    /// If a resolving schema is not found, it checks if a json with that name exists
    /// in `input_schemas` and then parses it (removing it from `input_schemas`)
    /// and adds the parsed schema to `parsed_schemas`.
    ///
    /// This method allows schemas definitions that depend on other types to
    /// parse their dependencies (or look them up if already parsed).
    fn fetch_schema(&mut self, name: &str) -> AvroResult<Schema> {
        if let Some(parsed) = self.parsed_schemas.get(name) {
            return Ok(parsed.clone());
        }
        if let Some(resolving_schema) = self.resolving_schemas.get(name) {
            return Ok(resolving_schema.clone());
        }

        let value = self
            .input_schemas
            .remove(name)
            .ok_or_else(|| Error::ParsePrimitive(name.into()))?;

        let parsed = self.parse(&value)?;
        self.parsed_schemas.insert(
            get_schema_type_name(name.to_string(), value),
            parsed.clone(),
        );
        Ok(parsed)
    }

    fn parse_precision_and_scale(
        complex: &Map<String, Value>,
    ) -> Result<(Precision, Scale), Error> {
        fn get_decimal_integer(
            complex: &Map<String, Value>,
            key: &'static str,
        ) -> Result<DecimalMetadata, Error> {
            match complex.get(key) {
                Some(&Value::Number(ref value)) => parse_json_integer_for_decimal(value),
                None => Err(Error::GetDecimalMetadataFromJson(key)),
                Some(precision) => Err(Error::GetDecimalPrecisionFromJson {
                    key: key.into(),
                    precision: precision.clone(),
                }),
            }
        }
        let precision = get_decimal_integer(complex, "precision")?;
        let scale = get_decimal_integer(complex, "scale")?;
        Ok((precision, scale))
    }

    /// Parse a `serde_json::Value` representing a complex Avro type into a
    /// `Schema`.
    ///
    /// Avro supports "recursive" definition of types.
    /// e.g: {"type": {"type": "string"}}
    fn parse_complex(&mut self, complex: &Map<String, Value>) -> AvroResult<Schema> {
        fn logical_verify_type(
            complex: &Map<String, Value>,
            kinds: &[SchemaKind],
            parser: &mut Parser,
        ) -> AvroResult<Schema> {
            match complex.get("type") {
                Some(value) => {
                    let ty = parser.parse(value)?;

                    if kinds
                        .iter()
                        .any(|&kind| SchemaKind::from(ty.clone()) == kind)
                    {
                        Ok(ty)
                    } else {
                        match get_type_rec(value.clone()) {
                            Ok(v) => Err(Error::GetLogicalTypeVariant(v)),
                            Err(err) => Err(err),
                        }
                    }
                }
                None => Err(Error::GetLogicalTypeField),
            }
        }

        fn get_type_rec(json_value: Value) -> AvroResult<Value> {
            match json_value {
                typ @ Value::String(_) => Ok(typ),
                Value::Object(ref complex) => match complex.get("type") {
                    Some(v) => get_type_rec(v.clone()),
                    None => Err(Error::GetComplexTypeField),
                },
                _ => Err(Error::GetComplexTypeField),
            }
        }

        // checks whether the logicalType is supported by the type
        fn try_logical_type(
            logical_type: &str,
            complex: &Map<String, Value>,
            kinds: &[SchemaKind],
            ok_schema: Schema,
            parser: &mut Parser,
        ) -> AvroResult<Schema> {
            match logical_verify_type(complex, kinds, parser) {
                // type and logicalType match!
                Ok(_) => Ok(ok_schema),
                // the logicalType is not expected for this type!
                Err(Error::GetLogicalTypeVariant(json_value)) => match json_value {
                    Value::String(_) => match parser.parse(&json_value) {
                        Ok(schema) => {
                            warn!(
                                "Ignoring invalid logical type '{}' for schema of type: {:?}!",
                                logical_type, schema
                            );
                            Ok(schema)
                        }
                        Err(parse_err) => Err(parse_err),
                    },
                    _ => Err(Error::GetLogicalTypeVariant(json_value)),
                },
                err => err,
            }
        }

        match complex.get("logicalType") {
            Some(&Value::String(ref t)) => match t.as_str() {
                "decimal" => {
                    let inner = Box::new(logical_verify_type(
                        complex,
                        &[SchemaKind::Fixed, SchemaKind::Bytes],
                        self,
                    )?);

                    let (precision, scale) = Self::parse_precision_and_scale(complex)?;

                    return Ok(Schema::Decimal {
                        precision,
                        scale,
                        inner,
                    });
                }
                "uuid" => {
                    logical_verify_type(complex, &[SchemaKind::String], self)?;
                    return Ok(Schema::Uuid);
                }
                "date" => {
                    return try_logical_type(
                        "date",
                        complex,
                        &[SchemaKind::Int],
                        Schema::Date,
                        self,
                    );
                }
                "time-millis" => {
                    return try_logical_type(
                        "time-millis",
                        complex,
                        &[SchemaKind::Int],
                        Schema::TimeMillis,
                        self,
                    );
                }
                "time-micros" => {
                    return try_logical_type(
                        "time-micros",
                        complex,
                        &[SchemaKind::Long],
                        Schema::TimeMicros,
                        self,
                    );
                }
                "timestamp-millis" => {
                    return try_logical_type(
                        "timestamp-millis",
                        complex,
                        &[SchemaKind::Long],
                        Schema::TimestampMillis,
                        self,
                    );
                }
                "timestamp-micros" => {
                    return try_logical_type(
                        "timestamp-micros",
                        complex,
                        &[SchemaKind::Long],
                        Schema::TimestampMicros,
                        self,
                    );
                }
                "duration" => {
                    logical_verify_type(complex, &[SchemaKind::Fixed], self)?;
                    return Ok(Schema::Duration);
                }
                // In this case, of an unknown logical type, we just pass through to the underlying
                // type.
                _ => {}
            },
            // The spec says to ignore invalid logical types and just continue through to the
            // underlying type - It is unclear whether that applies to this case or not, where the
            // `logicalType` is not a string.
            Some(_) => return Err(Error::GetLogicalTypeFieldType),
            _ => {}
        }
        match complex.get("type") {
            Some(&Value::String(ref t)) => match t.as_str() {
                "record" => self.parse_record(complex),
                "enum" => self.parse_enum(complex),
                "array" => self.parse_array(complex),
                "map" => self.parse_map(complex),
                "fixed" => self.parse_fixed(complex),
                other => self.parse_known_schema(other),
            },
            Some(&Value::Object(ref data)) => self.parse_complex(data),
            Some(&Value::Array(ref variants)) => self.parse_union(variants),
            Some(unknown) => Err(Error::GetComplexType(unknown.clone())),
            None => Err(Error::GetComplexTypeField),
        }
    }

    fn register_resolving_schema(&mut self, name: &Name) {
        let resolving_schema = Schema::Ref { name: name.clone() };
        self.resolving_schemas
            .insert(name.fullname(None), resolving_schema.clone());

        let namespace = &name.namespace;

        if let Some(ref aliases) = name.aliases {
            aliases.iter().for_each(|alias| {
                let alias_fullname = match namespace {
                    Some(ref ns) => format!("{}.{}", ns, alias),
                    None => alias.clone(),
                };
                self.resolving_schemas
                    .insert(alias_fullname, resolving_schema.clone());
            });
        }
    }

    fn register_parsed_schema(&mut self, name: &Name, schema: &Schema) {
        self.parsed_schemas
            .insert(name.fullname(None), schema.clone());
        self.resolving_schemas.remove(name.fullname(None).as_str());

        let namespace = &name.namespace;

        if let Some(ref aliases) = name.aliases {
            aliases.iter().for_each(|alias| {
                let alias_fullname = match namespace {
                    Some(ref ns) => format!("{}.{}", ns, alias),
                    None => alias.clone(),
                };
                self.parsed_schemas
                    .insert(alias_fullname.clone(), schema.clone());
                self.resolving_schemas.remove(alias_fullname.as_str());
            });
        }
    }

    /// Parse a `serde_json::Value` representing a Avro record type into a
    /// `Schema`.
    fn parse_record(&mut self, complex: &Map<String, Value>) -> AvroResult<Schema> {
        let name = Name::parse(complex)?;

        let mut lookup = HashMap::new();

        self.register_resolving_schema(&name);

        let fields: Vec<RecordField> = complex
            .get("fields")
            .and_then(|fields| fields.as_array())
            .ok_or(Error::GetRecordFieldsJson)
            .and_then(|fields| {
                fields
                    .iter()
                    .filter_map(|field| field.as_object())
                    .enumerate()
                    .map(|(position, field)| RecordField::parse(field, position, self))
                    .collect::<Result<_, _>>()
            })?;

        for field in &fields {
            lookup.insert(field.name.clone(), field.position);
        }

        let schema = Schema::Record {
            name: name.clone(),
            doc: complex.doc(),
            fields,
            lookup,
        };

        self.register_parsed_schema(&name, &schema);
        Ok(schema)
    }

    /// Parse a `serde_json::Value` representing a Avro enum type into a
    /// `Schema`.
    fn parse_enum(&mut self, complex: &Map<String, Value>) -> AvroResult<Schema> {
        let name = Name::parse(complex)?;

        let symbols: Vec<String> = complex
            .get("symbols")
            .and_then(|v| v.as_array())
            .ok_or(Error::GetEnumSymbolsField)
            .and_then(|symbols| {
                symbols
                    .iter()
                    .map(|symbol| symbol.as_str().map(|s| s.to_string()))
                    .collect::<Option<_>>()
                    .ok_or(Error::GetEnumSymbols)
            })?;

        let mut existing_symbols: HashSet<&String> = HashSet::with_capacity(symbols.len());
        for symbol in symbols.iter() {
            // Ensure enum symbol names match [A-Za-z_][A-Za-z0-9_]*
            if !ENUM_SYMBOL_NAME.is_match(symbol) {
                return Err(Error::EnumSymbolName(symbol.to_string()));
            }

            // Ensure there are no duplicate symbols
            if existing_symbols.contains(&symbol) {
                return Err(Error::EnumSymbolDuplicate(symbol.to_string()));
            }

            existing_symbols.insert(symbol);
        }

        let schema = Schema::Enum {
            name: name.clone(),
            doc: complex.doc(),
            symbols,
        };

        self.register_parsed_schema(&name, &schema);

        Ok(schema)
    }

    /// Parse a `serde_json::Value` representing a Avro array type into a
    /// `Schema`.
    fn parse_array(&mut self, complex: &Map<String, Value>) -> AvroResult<Schema> {
        complex
            .get("items")
            .ok_or(Error::GetArrayItemsField)
            .and_then(|items| self.parse(items))
            .map(|schema| Schema::Array(Box::new(schema)))
    }

    /// Parse a `serde_json::Value` representing a Avro map type into a
    /// `Schema`.
    fn parse_map(&mut self, complex: &Map<String, Value>) -> AvroResult<Schema> {
        complex
            .get("values")
            .ok_or(Error::GetMapValuesField)
            .and_then(|items| self.parse(items))
            .map(|schema| Schema::Map(Box::new(schema)))
    }

    /// Parse a `serde_json::Value` representing a Avro union type into a
    /// `Schema`.
    fn parse_union(&mut self, items: &[Value]) -> AvroResult<Schema> {
        items
            .iter()
            .map(|v| self.parse(v))
            .collect::<Result<Vec<_>, _>>()
            .and_then(|schemas| Ok(Schema::Union(UnionSchema::new(schemas)?)))
    }

    /// Parse a `serde_json::Value` representing a Avro fixed type into a
    /// `Schema`.
    fn parse_fixed(&mut self, complex: &Map<String, Value>) -> AvroResult<Schema> {
        let name = Name::parse(complex)?;

        let doc = complex.get("doc").and_then(|v| match &v {
            Value::String(ref docstr) => Some(docstr.clone()),
            _ => None,
        });

        let size = complex
            .get("size")
            .and_then(|v| v.as_i64())
            .ok_or(Error::GetFixedSizeField)?;

        let schema = Schema::Fixed {
            name: name.clone(),
            doc,
            size: size as usize,
        };

        self.register_parsed_schema(&name, &schema);

        Ok(schema)
    }
}

fn get_schema_type_name(name: String, value: Value) -> String {
    match value.get("type") {
        Some(Value::Object(complex_type)) => match complex_type.name() {
            Some(name) => name,
            _ => name,
        },
        _ => name,
    }
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            Schema::Ref { ref name } => serializer.serialize_str(&name.name),
            Schema::Null => serializer.serialize_str("null"),
            Schema::Boolean => serializer.serialize_str("boolean"),
            Schema::Int => serializer.serialize_str("int"),
            Schema::Long => serializer.serialize_str("long"),
            Schema::Float => serializer.serialize_str("float"),
            Schema::Double => serializer.serialize_str("double"),
            Schema::Bytes => serializer.serialize_str("bytes"),
            Schema::String => serializer.serialize_str("string"),
            Schema::Array(ref inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "array")?;
                map.serialize_entry("items", &*inner.clone())?;
                map.end()
            }
            Schema::Map(ref inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "map")?;
                map.serialize_entry("values", &*inner.clone())?;
                map.end()
            }
            Schema::Union(ref inner) => {
                let variants = inner.variants();
                let mut seq = serializer.serialize_seq(Some(variants.len()))?;
                for v in variants {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
            Schema::Record {
                ref name,
                ref doc,
                ref fields,
                ..
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "record")?;
                if let Some(ref n) = name.namespace {
                    map.serialize_entry("namespace", n)?;
                }
                map.serialize_entry("name", &name.name)?;
                if let Some(ref docstr) = doc {
                    map.serialize_entry("doc", docstr)?;
                }
                if let Some(ref aliases) = name.aliases {
                    map.serialize_entry("aliases", aliases)?;
                }
                map.serialize_entry("fields", fields)?;
                map.end()
            }
            Schema::Enum {
                ref name,
                ref symbols,
                ..
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "enum")?;
                map.serialize_entry("name", &name.name)?;
                map.serialize_entry("symbols", symbols)?;
                map.end()
            }
            Schema::Fixed {
                ref name,
                ref doc,
                ref size,
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "fixed")?;
                map.serialize_entry("name", &name.name)?;
                if let Some(ref docstr) = doc {
                    map.serialize_entry("doc", docstr)?;
                }
                map.serialize_entry("size", size)?;
                map.end()
            }
            Schema::Decimal {
                ref scale,
                ref precision,
                ref inner,
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", &*inner.clone())?;
                map.serialize_entry("logicalType", "decimal")?;
                map.serialize_entry("scale", scale)?;
                map.serialize_entry("precision", precision)?;
                map.end()
            }
            Schema::Uuid => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "string")?;
                map.serialize_entry("logicalType", "uuid")?;
                map.end()
            }
            Schema::Date => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "int")?;
                map.serialize_entry("logicalType", "date")?;
                map.end()
            }
            Schema::TimeMillis => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "int")?;
                map.serialize_entry("logicalType", "time-millis")?;
                map.end()
            }
            Schema::TimeMicros => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "time-micros")?;
                map.end()
            }
            Schema::TimestampMillis => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "timestamp-millis")?;
                map.end()
            }
            Schema::TimestampMicros => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "timestamp-micros")?;
                map.end()
            }
            Schema::Duration => {
                let mut map = serializer.serialize_map(None)?;

                // the Avro doesn't indicate what the name of the underlying fixed type of a
                // duration should be or typically is.
                let inner = Schema::Fixed {
                    name: Name::new("duration"),
                    doc: None,
                    size: 12,
                };
                map.serialize_entry("type", &inner)?;
                map.serialize_entry("logicalType", "duration")?;
                map.end()
            }
        }
    }
}

impl Serialize for RecordField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("type", &self.schema)?;

        if let Some(ref default) = self.default {
            map.serialize_entry("default", default)?;
        }

        map.end()
    }
}

/// Parses a **valid** avro schema into the Parsing Canonical Form.
/// https://avro.apache.org/docs/1.8.2/spec.html#Parsing+Canonical+Form+for+Schemas
fn parsing_canonical_form(schema: &serde_json::Value) -> String {
    match schema {
        serde_json::Value::Object(map) => pcf_map(map),
        serde_json::Value::String(s) => pcf_string(s),
        serde_json::Value::Array(v) => pcf_array(v),
        json => panic!(
            "got invalid JSON value for canonical form of schema: {0}",
            json
        ),
    }
}

fn pcf_map(schema: &Map<String, serde_json::Value>) -> String {
    // Look for the namespace variant up front.
    let ns = schema.get("namespace").and_then(|v| v.as_str());
    let mut fields = Vec::new();
    for (k, v) in schema {
        // Reduce primitive types to their simple form. ([PRIMITIVE] rule)
        if schema.len() == 1 && k == "type" {
            // Invariant: function is only callable from a valid schema, so this is acceptable.
            if let serde_json::Value::String(s) = v {
                return pcf_string(s);
            }
        }

        // Strip out unused fields ([STRIP] rule)
        if field_ordering_position(k).is_none() || k == "default" || k == "doc" || k == "aliases" {
            continue;
        }

        // Fully qualify the name, if it isn't already ([FULLNAMES] rule).
        if k == "name" {
            // Invariant: Only valid schemas. Must be a string.
            let name = v.as_str().unwrap();
            let n = match ns {
                Some(namespace) if !name.contains('.') => {
                    Cow::Owned(format!("{}.{}", namespace, name))
                }
                _ => Cow::Borrowed(name),
            };

            fields.push((k, format!("{}:{}", pcf_string(k), pcf_string(&*n))));
            continue;
        }

        // Strip off quotes surrounding "size" type, if they exist ([INTEGERS] rule).
        if k == "size" {
            let i = match v.as_str() {
                Some(s) => s.parse::<i64>().expect("Only valid schemas are accepted!"),
                None => v.as_i64().unwrap(),
            };
            fields.push((k, format!("{}:{}", pcf_string(k), i)));
            continue;
        }

        // For anything else, recursively process the result.
        fields.push((
            k,
            format!("{}:{}", pcf_string(k), parsing_canonical_form(v)),
        ));
    }

    // Sort the fields by their canonical ordering ([ORDER] rule).
    fields.sort_unstable_by_key(|(k, _)| field_ordering_position(k).unwrap());
    let inter = fields
        .into_iter()
        .map(|(_, v)| v)
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{}}}", inter)
}

fn pcf_array(arr: &[serde_json::Value]) -> String {
    let inter = arr
        .iter()
        .map(parsing_canonical_form)
        .collect::<Vec<String>>()
        .join(",");
    format!("[{}]", inter)
}

fn pcf_string(s: &str) -> String {
    format!("\"{}\"", s)
}

const RESERVED_FIELDS: &[&str] = &[
    "name",
    "type",
    "fields",
    "symbols",
    "items",
    "values",
    "logicalType",
    "size",
    "order",
    "doc",
    "aliases",
    "default",
];

// Used to define the ordering and inclusion of fields.
fn field_ordering_position(field: &str) -> Option<usize> {
    RESERVED_FIELDS
        .iter()
        .position(|&f| f == field)
        .map(|pos| pos + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_schema() {
        assert!(Schema::parse_str("invalid").is_err());
    }

    #[test]
    fn test_primitive_schema() {
        assert_eq!(Schema::Null, Schema::parse_str("\"null\"").unwrap());
        assert_eq!(Schema::Int, Schema::parse_str("\"int\"").unwrap());
        assert_eq!(Schema::Double, Schema::parse_str("\"double\"").unwrap());
    }

    #[test]
    fn test_array_schema() {
        let schema = Schema::parse_str(r#"{"type": "array", "items": "string"}"#).unwrap();
        assert_eq!(Schema::Array(Box::new(Schema::String)), schema);
    }

    #[test]
    fn test_map_schema() {
        let schema = Schema::parse_str(r#"{"type": "map", "values": "double"}"#).unwrap();
        assert_eq!(Schema::Map(Box::new(Schema::Double)), schema);
    }

    #[test]
    fn test_union_schema() {
        let schema = Schema::parse_str(r#"["null", "int"]"#).unwrap();
        assert_eq!(
            Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int]).unwrap()),
            schema
        );
    }

    #[test]
    fn test_union_unsupported_schema() {
        let schema = Schema::parse_str(r#"["null", ["null", "int"], "string"]"#);
        assert!(schema.is_err());
    }

    #[test]
    fn test_multi_union_schema() {
        let schema = Schema::parse_str(r#"["null", "int", "float", "string", "bytes"]"#);
        assert!(schema.is_ok());
        let schema = schema.unwrap();
        assert_eq!(SchemaKind::from(&schema), SchemaKind::Union);
        let union_schema = match schema {
            Schema::Union(u) => u,
            _ => unreachable!(),
        };
        assert_eq!(union_schema.variants().len(), 5);
        let mut variants = union_schema.variants().iter();
        assert_eq!(SchemaKind::from(variants.next().unwrap()), SchemaKind::Null);
        assert_eq!(SchemaKind::from(variants.next().unwrap()), SchemaKind::Int);
        assert_eq!(
            SchemaKind::from(variants.next().unwrap()),
            SchemaKind::Float
        );
        assert_eq!(
            SchemaKind::from(variants.next().unwrap()),
            SchemaKind::String
        );
        assert_eq!(
            SchemaKind::from(variants.next().unwrap()),
            SchemaKind::Bytes
        );
        assert_eq!(variants.next(), None);
    }

    // AVRO-3248
    #[test]
    fn test_union_of_records() {
        use std::iter::FromIterator;

        // A and B are the same except the name.
        let schema_str_a = r#"{
            "name": "A",
            "type": "record",
            "fields": [
                {"name": "field_one", "type": "float"}
            ]
        }"#;

        let schema_str_b = r#"{
            "name": "B",
            "type": "record",
            "fields": [
                {"name": "field_one", "type": "float"}
            ]
        }"#;

        // we get Error::GetNameField if we put ["A", "B"] directly here.
        let schema_str_c = r#"{
            "name": "C",
            "type": "record",
            "fields": [
                {"name": "field_one",  "type": ["A", "B"]}
            ]
        }"#;

        let schema_a = Schema::parse_str(schema_str_a).unwrap();
        let schema_b = Schema::parse_str(schema_str_b).unwrap();

        let schema_c = Schema::parse_list(&[schema_str_a, schema_str_b, schema_str_c])
            .unwrap()
            .last()
            .unwrap()
            .clone();

        let schema_c_expected = Schema::Record {
            name: Name::new("C"),
            doc: None,
            fields: vec![RecordField {
                name: "field_one".to_string(),
                doc: None,
                default: None,
                schema: Schema::Union(UnionSchema::new(vec![schema_a, schema_b]).unwrap()),
                order: RecordFieldOrder::Ignore,
                position: 0,
            }],
            lookup: HashMap::from_iter(vec![("field_one".to_string(), 0)]),
        };

        assert_eq!(schema_c, schema_c_expected);
    }

    // AVRO-3248
    #[test]
    fn test_nullable_record() {
        use std::iter::FromIterator;

        let schema_str_a = r#"{
            "name": "A",
            "type": "record",
            "fields": [
                {"name": "field_one", "type": "float"}
            ]
        }"#;

        // we get Error::GetNameField if we put ["null", "B"] directly here.
        let schema_str_option_a = r#"{
            "name": "OptionA",
            "type": "record",
            "fields": [
                {"name": "field_one",  "type": ["null", "A"], "default": "null"}
            ]
        }"#;

        let schema_a = Schema::parse_str(schema_str_a).unwrap();

        let schema_option_a = Schema::parse_list(&[schema_str_a, schema_str_option_a])
            .unwrap()
            .last()
            .unwrap()
            .clone();

        let schema_option_a_expected = Schema::Record {
            name: Name::new("OptionA"),
            doc: None,
            fields: vec![RecordField {
                name: "field_one".to_string(),
                doc: None,
                default: Some(Value::Null),
                schema: Schema::Union(UnionSchema::new(vec![Schema::Null, schema_a]).unwrap()),
                order: RecordFieldOrder::Ignore,
                position: 0,
            }],
            lookup: HashMap::from_iter(vec![("field_one".to_string(), 0)]),
        };

        assert_eq!(schema_option_a, schema_option_a_expected);
    }

    #[test]
    fn test_record_schema() {
        let parsed = Schema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }
        "#,
        )
        .unwrap();

        let mut lookup = HashMap::new();
        lookup.insert("a".to_owned(), 0);
        lookup.insert("b".to_owned(), 1);

        let expected = Schema::Record {
            name: Name::new("test"),
            doc: None,
            fields: vec![
                RecordField {
                    name: "a".to_string(),
                    doc: None,
                    default: Some(Value::Number(42i64.into())),
                    schema: Schema::Long,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                },
                RecordField {
                    name: "b".to_string(),
                    doc: None,
                    default: None,
                    schema: Schema::String,
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                },
            ],
            lookup,
        };

        assert_eq!(parsed, expected);
    }

    // AVRO-3302
    #[test]
    fn test_record_schema_with_currently_parsing_schema() {
        let schema = Schema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [{
                    "name": "recordField",
                    "type": {
                        "type": "record",
                        "name": "Node",
                        "fields": [
                            {"name": "label", "type": "string"},
                            {"name": "children", "type": {"type": "array", "items": "Node"}}
                        ]
                    }
                }]
            }
        "#,
        )
        .unwrap();

        let mut lookup = HashMap::new();
        lookup.insert("recordField".to_owned(), 0);

        let mut node_lookup = HashMap::new();
        node_lookup.insert("children".to_owned(), 1);
        node_lookup.insert("label".to_owned(), 0);

        let expected = Schema::Record {
            name: Name::new("test"),
            doc: None,
            fields: vec![RecordField {
                name: "recordField".to_string(),
                doc: None,
                default: None,
                schema: Schema::Record {
                    name: Name::new("Node"),
                    doc: None,
                    fields: vec![
                        RecordField {
                            name: "label".to_string(),
                            doc: None,
                            default: None,
                            schema: Schema::String,
                            order: RecordFieldOrder::Ascending,
                            position: 0,
                        },
                        RecordField {
                            name: "children".to_string(),
                            doc: None,
                            default: None,
                            schema: Schema::Array(Box::new(Schema::Ref {
                                name: Name::new("Node"),
                            })),
                            order: RecordFieldOrder::Ascending,
                            position: 1,
                        },
                    ],
                    lookup: node_lookup,
                },
                order: RecordFieldOrder::Ascending,
                position: 0,
            }],
            lookup,
        };
        assert_eq!(schema, expected);

        let canonical_form = &schema.canonical_form();
        let expected = r#"{"name":"test","type":"record","fields":[{"name":"recordField","type":{"name":"Node","type":"record","fields":[{"name":"label","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}"#;
        assert_eq!(canonical_form, &expected);
    }

    // https://github.com/flavray/avro-rs/pull/99#issuecomment-1016948451
    #[test]
    fn test_parsing_of_recursive_type_enum() {
        let schema = r#"
    {
        "type": "record",
        "name": "User",
        "namespace": "office",
        "fields": [
            {
              "name": "details",
              "type": [
                {
                  "type": "record",
                  "name": "Employee",
                  "fields": [
                    {
                      "name": "gender",
                      "type": {
                        "type": "enum",
                        "name": "Gender",
                        "symbols": [
                          "male",
                          "female"
                        ]
                      },
                      "default": "female"
                    }
                  ]
                },
                {
                  "type": "record",
                  "name": "Manager",
                  "fields": [
                    {
                      "name": "gender",
                      "type": "Gender"
                    }
                  ]
                }
              ]
            }
          ]
        }
        "#;

        let schema = Schema::parse_str(schema).unwrap();
        let schema_str = schema.canonical_form();
        let expected = r#"{"name":"office.User","type":"record","fields":[{"name":"details","type":[{"name":"Employee","type":"record","fields":[{"name":"gender","type":{"name":"Gender","type":"enum","symbols":["male","female"]}}]},{"name":"Manager","type":"record","fields":[{"name":"gender","type":{"name":"Gender","type":"enum","symbols":["male","female"]}}]}]}]}"#;
        assert_eq!(schema_str, expected);
    }

    #[test]
    fn test_parsing_of_recursive_type_fixed() {
        let schema = r#"
    {
        "type": "record",
        "name": "User",
        "namespace": "office",
        "fields": [
            {
              "name": "details",
              "type": [
                {
                  "type": "record",
                  "name": "Employee",
                  "fields": [
                    {
                      "name": "id",
                      "type": {
                        "type": "fixed",
                        "name": "EmployeeId",
                        "size": 16
                      },
                      "default": "female"
                    }
                  ]
                },
                {
                  "type": "record",
                  "name": "Manager",
                  "fields": [
                    {
                      "name": "id",
                      "type": "EmployeeId"
                    }
                  ]
                }
              ]
            }
          ]
        }
        "#;

        let schema = Schema::parse_str(schema).unwrap();
        let schema_str = schema.canonical_form();
        let expected = r#"{"name":"office.User","type":"record","fields":[{"name":"details","type":[{"name":"Employee","type":"record","fields":[{"name":"id","type":{"name":"EmployeeId","type":"fixed","size":16}}]},{"name":"Manager","type":"record","fields":[{"name":"id","type":{"name":"EmployeeId","type":"fixed","size":16}}]}]}]}"#;
        assert_eq!(schema_str, expected);
    }

    // AVRO-3302
    #[test]
    fn test_record_schema_with_currently_parsing_schema_aliases() {
        let schema = Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "LongList",
              "aliases": ["LinkedLongs"],
              "fields" : [
                {"name": "value", "type": "long"},
                {"name": "next", "type": ["null", "LinkedLongs"]}
              ]
            }
        "#,
        )
        .unwrap();

        let mut lookup = HashMap::new();
        lookup.insert("value".to_owned(), 0);
        lookup.insert("next".to_owned(), 1);

        let expected = Schema::Record {
            name: Name {
                name: "LongList".to_owned(),
                namespace: None,
                aliases: Some(vec!["LinkedLongs".to_owned()]),
            },
            doc: None,
            fields: vec![
                RecordField {
                    name: "value".to_string(),
                    doc: None,
                    default: None,
                    schema: Schema::Long,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                },
                RecordField {
                    name: "next".to_string(),
                    doc: None,
                    default: None,
                    schema: Schema::Union(
                        UnionSchema::new(vec![
                            Schema::Null,
                            Schema::Ref {
                                name: Name {
                                    name: "LongList".to_owned(),
                                    namespace: None,
                                    aliases: Some(vec!["LinkedLongs".to_owned()]),
                                },
                            },
                        ])
                        .unwrap(),
                    ),
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                },
            ],
            lookup,
        };
        assert_eq!(schema, expected);

        let canonical_form = &schema.canonical_form();
        let expected = r#"{"name":"LongList","type":"record","fields":[{"name":"value","type":"long"},{"name":"next","type":["null","LongList"]}]}"#;
        assert_eq!(canonical_form, &expected);
    }

    #[test]
    fn test_enum_schema() {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Suit", "symbols": ["diamonds", "spades", "clubs", "hearts"]}"#,
        ).unwrap();

        let expected = Schema::Enum {
            name: Name::new("Suit"),
            doc: None,
            symbols: vec![
                "diamonds".to_owned(),
                "spades".to_owned(),
                "clubs".to_owned(),
                "hearts".to_owned(),
            ],
        };

        assert_eq!(expected, schema);
    }

    #[test]
    fn test_enum_schema_duplicate() {
        // Duplicate "diamonds"
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Suit", "symbols": ["diamonds", "spades", "clubs", "diamonds"]}"#,
        );
        assert!(schema.is_err());
    }

    #[test]
    fn test_enum_schema_name() {
        // Invalid name "0000" does not match [A-Za-z_][A-Za-z0-9_]*
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Enum", "symbols": ["0000", "variant"]}"#,
        );
        assert!(schema.is_err());
    }

    #[test]
    fn test_fixed_schema() {
        let schema = Schema::parse_str(r#"{"type": "fixed", "name": "test", "size": 16}"#).unwrap();

        let expected = Schema::Fixed {
            name: Name::new("test"),
            doc: None,
            size: 16usize,
        };

        assert_eq!(expected, schema);
    }

    #[test]
    fn test_fixed_schema_with_documentation() {
        let schema = Schema::parse_str(
            r#"{"type": "fixed", "name": "test", "size": 16, "doc": "FixedSchema documentation"}"#,
        )
        .unwrap();

        let expected = Schema::Fixed {
            name: Name::new("test"),
            doc: Some(String::from("FixedSchema documentation")),
            size: 16usize,
        };

        assert_eq!(expected, schema);
    }

    #[test]
    fn test_no_documentation() {
        let schema =
            Schema::parse_str(r#"{"type": "enum", "name": "Coin", "symbols": ["heads", "tails"]}"#)
                .unwrap();

        let doc = match schema {
            Schema::Enum { doc, .. } => doc,
            _ => return,
        };

        assert!(doc.is_none());
    }

    #[test]
    fn test_documentation() {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Coin", "doc": "Some documentation", "symbols": ["heads", "tails"]}"#
        ).unwrap();

        let doc = match schema {
            Schema::Enum { doc, .. } => doc,
            _ => None,
        };

        assert_eq!("Some documentation".to_owned(), doc.unwrap());
    }

    // Tests to ensure Schema is Send + Sync. These tests don't need to _do_ anything, if they can
    // compile, they pass.
    #[test]
    fn test_schema_is_send() {
        fn send<S: Send>(_s: S) {}

        let schema = Schema::Null;
        send(schema);
    }

    #[test]
    fn test_schema_is_sync() {
        fn sync<S: Sync>(_s: S) {}

        let schema = Schema::Null;
        sync(&schema);
        sync(schema);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Sha256 uses an inline assembly instructions which is not supported by miri
    fn test_schema_fingerprint() {
        use crate::rabin::Rabin;
        use md5::Md5;
        use sha2::Sha256;

        let raw_schema = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "long", "logicalType": "timestamp-micros"}
        ]
    }
"#;

        let schema = Schema::parse_str(raw_schema).unwrap();
        assert_eq!(
            "abf662f831715ff78f88545a05a9262af75d6406b54e1a8a174ff1d2b75affc4",
            format!("{}", schema.fingerprint::<Sha256>())
        );

        assert_eq!(
            "6e21c350f71b1a34e9efe90970f1bc69",
            format!("{}", schema.fingerprint::<Md5>())
        );
        assert_eq!(
            "28cf0a67d9937bb3",
            format!("{}", schema.fingerprint::<Rabin>())
        )
    }

    #[test]
    fn test_logical_types() {
        let schema = Schema::parse_str(r#"{"type": "int", "logicalType": "date"}"#).unwrap();
        assert_eq!(schema, Schema::Date);

        let schema =
            Schema::parse_str(r#"{"type": "long", "logicalType": "timestamp-micros"}"#).unwrap();
        assert_eq!(schema, Schema::TimestampMicros);
    }

    #[test]
    fn test_nullable_logical_type() {
        let schema = Schema::parse_str(
            r#"{"type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]}"#,
        )
        .unwrap();
        assert_eq!(
            schema,
            Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::TimestampMicros]).unwrap())
        );
    }

    #[test]
    fn record_field_order_from_str() {
        use std::str::FromStr;

        assert_eq!(
            RecordFieldOrder::from_str("ascending").unwrap(),
            RecordFieldOrder::Ascending
        );
        assert_eq!(
            RecordFieldOrder::from_str("descending").unwrap(),
            RecordFieldOrder::Descending
        );
        assert_eq!(
            RecordFieldOrder::from_str("ignore").unwrap(),
            RecordFieldOrder::Ignore
        );
        assert!(RecordFieldOrder::from_str("not an ordering").is_err());
    }
}
