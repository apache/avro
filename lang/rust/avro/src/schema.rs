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
use crate::{
    error::Error,
    schema_equality, types,
    util::MapHelper,
    validator::{
        validate_enum_symbol_name, validate_namespace, validate_record_field_name,
        validate_schema_name,
    },
    AvroResult,
};
use digest::Digest;
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Deserialize, Serialize, Serializer,
};
use serde_json::{Map, Value};
use std::{
    borrow::{Borrow, Cow},
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    fmt::Debug,
    hash::Hash,
    io::Read,
    str::FromStr,
};
use strum_macros::{Display, EnumDiscriminants, EnumString};

/// Represents an Avro schema fingerprint
/// More information about Avro schema fingerprints can be found in the
/// [Avro Schema Fingerprint documentation](https://avro.apache.org/docs/current/specification/#schema-fingerprints)
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
                .map(|byte| format!("{byte:02x}"))
                .collect::<Vec<String>>()
                .join("")
        )
    }
}

/// Represents any valid Avro schema
/// More information about Avro schemas can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/specification/#schema-declaration)
#[derive(Clone, Debug, EnumDiscriminants, Display)]
#[strum_discriminants(name(SchemaKind), derive(Hash, Ord, PartialOrd))]
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
    Array(ArraySchema),
    /// A `map` Avro schema.
    /// `Map` holds a pointer to the `Schema` of its values, which must all be the same schema.
    /// `Map` keys are assumed to be `string`.
    Map(MapSchema),
    /// A `union` Avro schema.
    Union(UnionSchema),
    /// A `record` Avro schema.
    Record(RecordSchema),
    /// An `enum` Avro schema.
    Enum(EnumSchema),
    /// A `fixed` Avro schema.
    Fixed(FixedSchema),
    /// Logical type which represents `Decimal` values. The underlying type is serialized and
    /// deserialized as `Schema::Bytes` or `Schema::Fixed`.
    Decimal(DecimalSchema),
    /// Logical type which represents `Decimal` values without predefined scale.
    /// The underlying type is serialized and deserialized as `Schema::Bytes`
    BigDecimal,
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
    /// An instant in time represented as the number of nanoseconds after the UNIX epoch.
    TimestampNanos,
    /// An instant in localtime represented as the number of milliseconds after the UNIX epoch.
    LocalTimestampMillis,
    /// An instant in local time represented as the number of microseconds after the UNIX epoch.
    LocalTimestampMicros,
    /// An instant in local time represented as the number of nanoseconds after the UNIX epoch.
    LocalTimestampNanos,
    /// An amount of time defined by a number of months, days and milliseconds.
    Duration,
    /// A reference to another schema.
    Ref { name: Name },
}

#[derive(Clone, Debug, PartialEq)]
pub struct MapSchema {
    pub types: Box<Schema>,
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ArraySchema {
    pub items: Box<Schema>,
    pub attributes: BTreeMap<String, Value>,
}

impl PartialEq for Schema {
    /// Assess equality of two `Schema` based on [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.11.1/specification/#parsing-canonical-form-for-schemas
    fn eq(&self, other: &Self) -> bool {
        schema_equality::compare_schemata(self, other)
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
            SchemaKind::Record | SchemaKind::Enum | SchemaKind::Fixed | SchemaKind::Ref
        )
    }
}

impl From<&types::Value> for SchemaKind {
    fn from(value: &types::Value) -> Self {
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
            Value::BigDecimal(_) => Self::BigDecimal,
            Value::Uuid(_) => Self::Uuid,
            Value::Date(_) => Self::Date,
            Value::TimeMillis(_) => Self::TimeMillis,
            Value::TimeMicros(_) => Self::TimeMicros,
            Value::TimestampMillis(_) => Self::TimestampMillis,
            Value::TimestampMicros(_) => Self::TimestampMicros,
            Value::TimestampNanos(_) => Self::TimestampNanos,
            Value::LocalTimestampMillis(_) => Self::LocalTimestampMillis,
            Value::LocalTimestampMicros(_) => Self::LocalTimestampMicros,
            Value::LocalTimestampNanos(_) => Self::LocalTimestampNanos,
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
/// [Avro specification](https://avro.apache.org/docs/current/specification/#names)
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Name {
    pub name: String,
    pub namespace: Namespace,
}

/// Represents documentation for complex Avro schemas.
pub type Documentation = Option<String>;
/// Represents the aliases for Named Schema
pub type Aliases = Option<Vec<Alias>>;
/// Represents Schema lookup within a schema env
pub(crate) type Names = HashMap<Name, Schema>;
/// Represents Schema lookup within a schema
pub type NamesRef<'a> = HashMap<Name, &'a Schema>;
/// Represents the namespace for Named Schema
pub type Namespace = Option<String>;

impl Name {
    /// Create a new `Name`.
    /// Parses the optional `namespace` from the `name` string.
    /// `aliases` will not be defined.
    pub fn new(name: &str) -> AvroResult<Self> {
        let (name, namespace) = Name::get_name_and_namespace(name)?;
        Ok(Self {
            name,
            namespace: namespace.filter(|ns| !ns.is_empty()),
        })
    }

    fn get_name_and_namespace(name: &str) -> AvroResult<(String, Namespace)> {
        validate_schema_name(name)
    }

    /// Parse a `serde_json::Value` into a `Name`.
    pub(crate) fn parse(
        complex: &Map<String, Value>,
        enclosing_namespace: &Namespace,
    ) -> AvroResult<Self> {
        let (name, namespace_from_name) = complex
            .name()
            .map(|name| Name::get_name_and_namespace(name.as_str()).unwrap())
            .ok_or(Error::GetNameField)?;
        // FIXME Reading name from the type is wrong ! The name there is just a metadata (AVRO-3430)
        let type_name = match complex.get("type") {
            Some(Value::Object(complex_type)) => complex_type.name().or(None),
            _ => None,
        };

        let namespace = namespace_from_name
            .or_else(|| {
                complex
                    .string("namespace")
                    .or_else(|| enclosing_namespace.clone())
            })
            .filter(|ns| !ns.is_empty());

        if let Some(ref ns) = namespace {
            validate_namespace(ns)?;
        }

        Ok(Self {
            name: type_name.unwrap_or(name),
            namespace,
        })
    }

    /// Return the `fullname` of this `Name`
    ///
    /// More information about fullnames can be found in the
    /// [Avro specification](https://avro.apache.org/docs/current/specification/#names)
    pub fn fullname(&self, default_namespace: Namespace) -> String {
        if self.name.contains('.') {
            self.name.clone()
        } else {
            let namespace = self.namespace.clone().or(default_namespace);

            match namespace {
                Some(ref namespace) if !namespace.is_empty() => {
                    format!("{}.{}", namespace, self.name)
                }
                _ => self.name.clone(),
            }
        }
    }

    /// Return the fully qualified name needed for indexing or searching for the schema within a schema/schema env context. Puts the enclosing namespace into the name's namespace for clarity in schema/schema env parsing
    /// ```ignore
    /// use apache_avro::schema::Name;
    ///
    /// assert_eq!(
    /// Name::new("some_name")?.fully_qualified_name(&Some("some_namespace".into())),
    /// Name::new("some_namespace.some_name")?
    /// );
    /// assert_eq!(
    /// Name::new("some_namespace.some_name")?.fully_qualified_name(&Some("other_namespace".into())),
    /// Name::new("some_namespace.some_name")?
    /// );
    /// ```
    pub fn fully_qualified_name(&self, enclosing_namespace: &Namespace) -> Name {
        Name {
            name: self.name.clone(),
            namespace: self
                .namespace
                .clone()
                .or_else(|| enclosing_namespace.clone().filter(|ns| !ns.is_empty())),
        }
    }
}

impl From<&str> for Name {
    fn from(name: &str) -> Self {
        Name::new(name).unwrap()
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.fullname(None)[..])
    }
}

impl<'de> Deserialize<'de> for Name {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        Value::deserialize(deserializer).and_then(|value| {
            use serde::de::Error;
            if let Value::Object(json) = value {
                Name::parse(&json, &None).map_err(Error::custom)
            } else {
                Err(Error::custom(format!("Expected a JSON object: {value:?}")))
            }
        })
    }
}

/// Newtype pattern for `Name` to better control the `serde_json::Value` representation.
/// Aliases are serialized as an array of plain strings in the JSON representation.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Alias(Name);

impl Alias {
    pub fn new(name: &str) -> AvroResult<Self> {
        Name::new(name).map(Self)
    }

    pub fn name(&self) -> String {
        self.0.name.clone()
    }

    pub fn namespace(&self) -> Namespace {
        self.0.namespace.clone()
    }

    pub fn fullname(&self, default_namespace: Namespace) -> String {
        self.0.fullname(default_namespace)
    }

    pub fn fully_qualified_name(&self, default_namespace: &Namespace) -> Name {
        self.0.fully_qualified_name(default_namespace)
    }
}

impl From<&str> for Alias {
    fn from(name: &str) -> Self {
        Alias::new(name).unwrap()
    }
}

impl Serialize for Alias {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.fullname(None))
    }
}

#[derive(Debug)]
pub struct ResolvedSchema<'s> {
    names_ref: NamesRef<'s>,
    schemata: Vec<&'s Schema>,
}

impl<'s> TryFrom<&'s Schema> for ResolvedSchema<'s> {
    type Error = Error;

    fn try_from(schema: &'s Schema) -> AvroResult<Self> {
        let names = HashMap::new();
        let mut rs = ResolvedSchema {
            names_ref: names,
            schemata: vec![schema],
        };
        rs.resolve(rs.get_schemata(), &None, None)?;
        Ok(rs)
    }
}

impl<'s> TryFrom<Vec<&'s Schema>> for ResolvedSchema<'s> {
    type Error = Error;

    fn try_from(schemata: Vec<&'s Schema>) -> AvroResult<Self> {
        let names = HashMap::new();
        let mut rs = ResolvedSchema {
            names_ref: names,
            schemata,
        };
        rs.resolve(rs.get_schemata(), &None, None)?;
        Ok(rs)
    }
}

impl<'s> ResolvedSchema<'s> {
    pub fn get_schemata(&self) -> Vec<&'s Schema> {
        self.schemata.clone()
    }

    pub fn get_names(&self) -> &NamesRef<'s> {
        &self.names_ref
    }

    /// Creates `ResolvedSchema` with some already known schemas.
    ///
    /// Those schemata would be used to resolve references if needed.
    pub fn new_with_known_schemata<'n>(
        schemata_to_resolve: Vec<&'s Schema>,
        enclosing_namespace: &Namespace,
        known_schemata: &'n NamesRef<'n>,
    ) -> AvroResult<Self> {
        let names = HashMap::new();
        let mut rs = ResolvedSchema {
            names_ref: names,
            schemata: schemata_to_resolve,
        };
        rs.resolve(rs.get_schemata(), enclosing_namespace, Some(known_schemata))?;
        Ok(rs)
    }

    fn resolve<'n>(
        &mut self,
        schemata: Vec<&'s Schema>,
        enclosing_namespace: &Namespace,
        known_schemata: Option<&'n NamesRef<'n>>,
    ) -> AvroResult<()> {
        for schema in schemata {
            match schema {
                Schema::Array(schema) => {
                    self.resolve(vec![&schema.items], enclosing_namespace, known_schemata)?
                }
                Schema::Map(schema) => {
                    self.resolve(vec![&schema.types], enclosing_namespace, known_schemata)?
                }
                Schema::Union(UnionSchema { schemas, .. }) => {
                    for schema in schemas {
                        self.resolve(vec![schema], enclosing_namespace, known_schemata)?
                    }
                }
                Schema::Enum(EnumSchema { name, .. }) | Schema::Fixed(FixedSchema { name, .. }) => {
                    let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
                    if self
                        .names_ref
                        .insert(fully_qualified_name.clone(), schema)
                        .is_some()
                    {
                        return Err(Error::AmbiguousSchemaDefinition(fully_qualified_name));
                    }
                }
                Schema::Record(RecordSchema { name, fields, .. }) => {
                    let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
                    if self
                        .names_ref
                        .insert(fully_qualified_name.clone(), schema)
                        .is_some()
                    {
                        return Err(Error::AmbiguousSchemaDefinition(fully_qualified_name));
                    } else {
                        let record_namespace = fully_qualified_name.namespace;
                        for field in fields {
                            self.resolve(vec![&field.schema], &record_namespace, known_schemata)?
                        }
                    }
                }
                Schema::Ref { name } => {
                    let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
                    // first search for reference in current schemata, then look into external references.
                    if !self.names_ref.contains_key(&fully_qualified_name) {
                        let is_resolved_with_known_schemas = known_schemata
                            .as_ref()
                            .map(|names| names.contains_key(&fully_qualified_name))
                            .unwrap_or(false);
                        if !is_resolved_with_known_schemas {
                            return Err(Error::SchemaResolutionError(fully_qualified_name));
                        }
                    }
                }
                _ => (),
            }
        }
        Ok(())
    }
}

pub(crate) struct ResolvedOwnedSchema {
    names: Names,
    root_schema: Schema,
}

impl TryFrom<Schema> for ResolvedOwnedSchema {
    type Error = Error;

    fn try_from(schema: Schema) -> AvroResult<Self> {
        let names = HashMap::new();
        let mut rs = ResolvedOwnedSchema {
            names,
            root_schema: schema,
        };
        resolve_names(&rs.root_schema, &mut rs.names, &None)?;
        Ok(rs)
    }
}

impl ResolvedOwnedSchema {
    pub(crate) fn get_root_schema(&self) -> &Schema {
        &self.root_schema
    }
    pub(crate) fn get_names(&self) -> &Names {
        &self.names
    }
}

pub(crate) fn resolve_names(
    schema: &Schema,
    names: &mut Names,
    enclosing_namespace: &Namespace,
) -> AvroResult<()> {
    match schema {
        Schema::Array(schema) => resolve_names(&schema.items, names, enclosing_namespace),
        Schema::Map(schema) => resolve_names(&schema.types, names, enclosing_namespace),
        Schema::Union(UnionSchema { schemas, .. }) => {
            for schema in schemas {
                resolve_names(schema, names, enclosing_namespace)?
            }
            Ok(())
        }
        Schema::Enum(EnumSchema { name, .. }) | Schema::Fixed(FixedSchema { name, .. }) => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            if names
                .insert(fully_qualified_name.clone(), schema.clone())
                .is_some()
            {
                Err(Error::AmbiguousSchemaDefinition(fully_qualified_name))
            } else {
                Ok(())
            }
        }
        Schema::Record(RecordSchema { name, fields, .. }) => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            if names
                .insert(fully_qualified_name.clone(), schema.clone())
                .is_some()
            {
                Err(Error::AmbiguousSchemaDefinition(fully_qualified_name))
            } else {
                let record_namespace = fully_qualified_name.namespace;
                for field in fields {
                    resolve_names(&field.schema, names, &record_namespace)?
                }
                Ok(())
            }
        }
        Schema::Ref { name } => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            names
                .get(&fully_qualified_name)
                .map(|_| ())
                .ok_or(Error::SchemaResolutionError(fully_qualified_name))
        }
        _ => Ok(()),
    }
}

pub(crate) fn resolve_names_with_schemata(
    schemata: &Vec<&Schema>,
    names: &mut Names,
    enclosing_namespace: &Namespace,
) -> AvroResult<()> {
    for schema in schemata {
        resolve_names(schema, names, enclosing_namespace)?;
    }
    Ok(())
}

/// Represents a `field` in a `record` Avro schema.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordField {
    /// Name of the field.
    pub name: String,
    /// Documentation of the field.
    pub doc: Documentation,
    /// Aliases of the field's name. They have no namespace.
    pub aliases: Option<Vec<String>>,
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
    /// A collection of all unknown fields in the record field.
    pub custom_attributes: BTreeMap<String, Value>,
}

/// Represents any valid order for a `field` in a `record` Avro schema.
#[derive(Clone, Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "kebab_case")]
pub enum RecordFieldOrder {
    Ascending,
    Descending,
    Ignore,
}

impl RecordField {
    /// Parse a `serde_json::Value` into a `RecordField`.
    fn parse(
        field: &Map<String, Value>,
        position: usize,
        parser: &mut Parser,
        enclosing_record: &Name,
    ) -> AvroResult<Self> {
        let name = field.name().ok_or(Error::GetNameFieldFromRecord)?;

        validate_record_field_name(&name)?;

        // TODO: "type" = "<record name>"
        let schema = parser.parse_complex(field, &enclosing_record.namespace)?;

        let default = field.get("default").cloned();
        Self::resolve_default_value(
            &schema,
            &name,
            &enclosing_record.fullname(None),
            &parser.parsed_schemas,
            &default,
        )?;

        let aliases = field.get("aliases").and_then(|aliases| {
            aliases.as_array().map(|aliases| {
                aliases
                    .iter()
                    .flat_map(|alias| alias.as_str())
                    .map(|alias| alias.to_string())
                    .collect::<Vec<String>>()
            })
        });

        let order = field
            .get("order")
            .and_then(|order| order.as_str())
            .and_then(|order| RecordFieldOrder::from_str(order).ok())
            .unwrap_or(RecordFieldOrder::Ascending);

        Ok(RecordField {
            name,
            doc: field.doc(),
            default,
            aliases,
            order,
            position,
            custom_attributes: RecordField::get_field_custom_attributes(field, &schema),
            schema,
        })
    }

    fn resolve_default_value(
        field_schema: &Schema,
        field_name: &str,
        record_name: &str,
        names: &Names,
        default: &Option<Value>,
    ) -> AvroResult<()> {
        if let Some(value) = default {
            let avro_value = types::Value::from(value.clone());
            match field_schema {
                Schema::Union(union_schema) => {
                    let schemas = &union_schema.schemas;
                    let resolved = schemas.iter().any(|schema| {
                        avro_value
                            .to_owned()
                            .resolve_internal(schema, names, &schema.namespace(), &None)
                            .is_ok()
                    });

                    if !resolved {
                        let schema: Option<&Schema> = schemas.first();
                        return match schema {
                            Some(first_schema) => Err(Error::GetDefaultUnion(
                                SchemaKind::from(first_schema),
                                types::ValueKind::from(avro_value),
                            )),
                            None => Err(Error::EmptyUnion),
                        };
                    }
                }
                _ => {
                    let resolved = avro_value
                        .resolve_internal(field_schema, names, &field_schema.namespace(), &None)
                        .is_ok();

                    if !resolved {
                        return Err(Error::GetDefaultRecordField(
                            field_name.to_string(),
                            record_name.to_string(),
                            field_schema.canonical_form(),
                        ));
                    }
                }
            };
        }

        Ok(())
    }

    fn get_field_custom_attributes(
        field: &Map<String, Value>,
        schema: &Schema,
    ) -> BTreeMap<String, Value> {
        let mut custom_attributes: BTreeMap<String, Value> = BTreeMap::new();
        for (key, value) in field {
            match key.as_str() {
                "type" | "name" | "doc" | "default" | "order" | "position" | "aliases"
                | "logicalType" => continue,
                key if key == "symbols" && matches!(schema, Schema::Enum(_)) => continue,
                key if key == "size" && matches!(schema, Schema::Fixed(_)) => continue,
                _ => custom_attributes.insert(key.clone(), value.clone()),
            };
        }
        custom_attributes
    }

    /// Returns true if this `RecordField` is nullable, meaning the schema is a `UnionSchema` where the first variant is `Null`.
    pub fn is_nullable(&self) -> bool {
        match self.schema {
            Schema::Union(ref inner) => inner.is_nullable(),
            _ => false,
        }
    }
}

/// A description of an Enum schema.
#[derive(Debug, Clone)]
pub struct RecordSchema {
    /// The name of the schema
    pub name: Name,
    /// The aliases of the schema
    pub aliases: Aliases,
    /// The documentation of the schema
    pub doc: Documentation,
    /// The set of fields of the schema
    pub fields: Vec<RecordField>,
    /// The `lookup` table maps field names to their position in the `Vec`
    /// of `fields`.
    pub lookup: BTreeMap<String, usize>,
    /// The custom attributes of the schema
    pub attributes: BTreeMap<String, Value>,
}

/// A description of an Enum schema.
#[derive(Debug, Clone)]
pub struct EnumSchema {
    /// The name of the schema
    pub name: Name,
    /// The aliases of the schema
    pub aliases: Aliases,
    /// The documentation of the schema
    pub doc: Documentation,
    /// The set of symbols of the schema
    pub symbols: Vec<String>,
    /// An optional default symbol used for compatibility
    pub default: Option<String>,
    /// The custom attributes of the schema
    pub attributes: BTreeMap<String, Value>,
}

/// A description of a Union schema.
#[derive(Debug, Clone)]
pub struct FixedSchema {
    /// The name of the schema
    pub name: Name,
    /// The aliases of the schema
    pub aliases: Aliases,
    /// The documentation of the schema
    pub doc: Documentation,
    /// The size of the fixed schema
    pub size: usize,
    /// An optional default symbol used for compatibility
    pub default: Option<String>,
    /// The custom attributes of the schema
    pub attributes: BTreeMap<String, Value>,
}

impl FixedSchema {
    fn serialize_to_map<S>(&self, mut map: S::SerializeMap) -> Result<S::SerializeMap, S::Error>
    where
        S: Serializer,
    {
        map.serialize_entry("type", "fixed")?;
        if let Some(ref n) = self.name.namespace {
            map.serialize_entry("namespace", n)?;
        }
        map.serialize_entry("name", &self.name.name)?;
        if let Some(ref docstr) = self.doc {
            map.serialize_entry("doc", docstr)?;
        }
        map.serialize_entry("size", &self.size)?;

        if let Some(ref aliases) = self.aliases {
            map.serialize_entry("aliases", aliases)?;
        }

        for attr in &self.attributes {
            map.serialize_entry(attr.0, attr.1)?;
        }

        Ok(map)
    }
}

/// A description of a Union schema.
///
/// `scale` defaults to 0 and is an integer greater than or equal to 0 and `precision` is an
/// integer greater than 0.
#[derive(Debug, Clone)]
pub struct DecimalSchema {
    /// The number of digits in the unscaled value
    pub precision: DecimalMetadata,
    /// The number of digits to the right of the decimal point
    pub scale: DecimalMetadata,
    /// The inner schema of the decimal (fixed or bytes)
    pub inner: Box<Schema>,
}

/// A description of a Union schema
#[derive(Debug, Clone)]
pub struct UnionSchema {
    /// The schemas that make up this union
    pub(crate) schemas: Vec<Schema>,
    // Used to ensure uniqueness of schema inputs, and provide constant time finding of the
    // schema index given a value.
    // **NOTE** that this approach does not work for named types, and will have to be modified
    // to support that. A simple solution is to also keep a mapping of the names used.
    variant_index: BTreeMap<SchemaKind, usize>,
}

impl UnionSchema {
    /// Creates a new UnionSchema from a vector of schemas.
    pub fn new(schemas: Vec<Schema>) -> AvroResult<Self> {
        let mut vindex = BTreeMap::new();
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

    /// Returns true if the any of the variants of this `UnionSchema` is `Null`.
    pub fn is_nullable(&self) -> bool {
        self.schemas.iter().any(|x| matches!(x, Schema::Null))
    }

    /// Optionally returns a reference to the schema matched by this value, as well as its position
    /// within this union.
    #[deprecated(
        since = "0.15.0",
        note = "Please use `find_schema_with_known_schemata` instead"
    )]
    pub fn find_schema(&self, value: &types::Value) -> Option<(usize, &Schema)> {
        self.find_schema_with_known_schemata::<Schema>(value, None, &None)
    }

    /// Optionally returns a reference to the schema matched by this value, as well as its position
    /// within this union.
    ///
    /// Extra arguments:
    /// - `known_schemata` - mapping between `Name` and `Schema` - if passed, additional external schemas would be used to resolve references.
    pub fn find_schema_with_known_schemata<S: Borrow<Schema> + Debug>(
        &self,
        value: &types::Value,
        known_schemata: Option<&HashMap<Name, S>>,
        enclosing_namespace: &Namespace,
    ) -> Option<(usize, &Schema)> {
        let schema_kind = SchemaKind::from(value);
        if let Some(&i) = self.variant_index.get(&schema_kind) {
            // fast path
            Some((i, &self.schemas[i]))
        } else {
            // slow path (required for matching logical or named types)

            // first collect what schemas we already know
            let mut collected_names: HashMap<Name, &Schema> = known_schemata
                .map(|names| {
                    names
                        .iter()
                        .map(|(name, schema)| (name.clone(), schema.borrow()))
                        .collect()
                })
                .unwrap_or_default();

            self.schemas.iter().enumerate().find(|(_, schema)| {
                let resolved_schema = ResolvedSchema::new_with_known_schemata(
                    vec![*schema],
                    enclosing_namespace,
                    &collected_names,
                )
                .expect("Schema didn't successfully parse");
                let resolved_names = resolved_schema.names_ref;

                // extend known schemas with just resolved names
                collected_names.extend(resolved_names);
                let namespace = &schema.namespace().or_else(|| enclosing_namespace.clone());

                value
                    .clone()
                    .resolve_internal(schema, &collected_names, namespace, &None)
                    .is_ok()
            })
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
    input_schemas: HashMap<Name, Value>,
    /// A map of name -> Schema::Ref
    /// Used to resolve cyclic references, i.e. when a
    /// field's type is a reference to its record's type
    resolving_schemas: Names,
    input_order: Vec<Name>,
    /// A map of name -> fully parsed Schema
    /// Used to avoid parsing the same schema twice
    parsed_schemas: Names,
}

impl Schema {
    /// Converts `self` into its [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/current/specification/#parsing-canonical-form-for-schemas
    pub fn canonical_form(&self) -> String {
        let json = serde_json::to_value(self)
            .unwrap_or_else(|e| panic!("Cannot parse Schema from JSON: {e}"));
        parsing_canonical_form(&json)
    }

    /// Generate [fingerprint] of Schema's [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/current/specification/#parsing-canonical-form-for-schemas
    /// [fingerprint]:
    /// https://avro.apache.org/docs/current/specification/#schema-fingerprints
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
    pub fn parse_list(input: &[&str]) -> AvroResult<Vec<Schema>> {
        let mut input_schemas: HashMap<Name, Value> = HashMap::with_capacity(input.len());
        let mut input_order: Vec<Name> = Vec::with_capacity(input.len());
        for js in input {
            let schema: Value = serde_json::from_str(js).map_err(Error::ParseSchemaJson)?;
            if let Value::Object(inner) = &schema {
                let name = Name::parse(inner, &None)?;
                let previous_value = input_schemas.insert(name.clone(), schema);
                if previous_value.is_some() {
                    return Err(Error::NameCollision(name.fullname(None)));
                }
                input_order.push(name);
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

    /// Create a `Schema` from a reader which implements [`Read`].
    pub fn parse_reader(reader: &mut (impl Read + ?Sized)) -> AvroResult<Schema> {
        let mut buf = String::new();
        match reader.read_to_string(&mut buf) {
            Ok(_) => Self::parse_str(&buf),
            Err(e) => Err(Error::ReadSchemaFromReader(e)),
        }
    }

    /// Parses an Avro schema from JSON.
    pub fn parse(value: &Value) -> AvroResult<Schema> {
        let mut parser = Parser::default();
        parser.parse(value, &None)
    }

    /// Parses an Avro schema from JSON.
    /// Any `Schema::Ref`s must be known in the `names` map.
    pub(crate) fn parse_with_names(value: &Value, names: Names) -> AvroResult<Schema> {
        let mut parser = Parser {
            input_schemas: HashMap::with_capacity(1),
            resolving_schemas: Names::default(),
            input_order: Vec::with_capacity(1),
            parsed_schemas: names,
        };
        parser.parse(value, &None)
    }

    /// Returns the custom attributes (metadata) if the schema supports them.
    pub fn custom_attributes(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Schema::Record(RecordSchema { attributes, .. })
            | Schema::Enum(EnumSchema { attributes, .. })
            | Schema::Fixed(FixedSchema { attributes, .. })
            | Schema::Array(ArraySchema { attributes, .. })
            | Schema::Map(MapSchema { attributes, .. }) => Some(attributes),
            _ => None,
        }
    }

    /// Returns the name of the schema if it has one.
    pub fn name(&self) -> Option<&Name> {
        match self {
            Schema::Ref { name, .. }
            | Schema::Record(RecordSchema { name, .. })
            | Schema::Enum(EnumSchema { name, .. })
            | Schema::Fixed(FixedSchema { name, .. }) => Some(name),
            _ => None,
        }
    }

    /// Returns the namespace of the schema if it has one.
    pub fn namespace(&self) -> Namespace {
        self.name().and_then(|n| n.namespace.clone())
    }

    /// Returns the aliases of the schema if it has ones.
    pub fn aliases(&self) -> Option<&Vec<Alias>> {
        match self {
            Schema::Record(RecordSchema { aliases, .. })
            | Schema::Enum(EnumSchema { aliases, .. })
            | Schema::Fixed(FixedSchema { aliases, .. }) => aliases.as_ref(),
            _ => None,
        }
    }

    /// Returns the doc of the schema if it has one.
    pub fn doc(&self) -> Option<&String> {
        match self {
            Schema::Record(RecordSchema { doc, .. })
            | Schema::Enum(EnumSchema { doc, .. })
            | Schema::Fixed(FixedSchema { doc, .. }) => doc.as_ref(),
            _ => None,
        }
    }

    /// Returns a Schema::Map with the given types.
    pub fn map(types: Schema) -> Self {
        Schema::Map(MapSchema {
            types: Box::new(types),
            attributes: Default::default(),
        })
    }

    /// Returns a Schema::Map with the given types and custom attributes.
    pub fn map_with_attributes(types: Schema, attributes: BTreeMap<String, Value>) -> Self {
        Schema::Map(MapSchema {
            types: Box::new(types),
            attributes,
        })
    }

    /// Returns a Schema::Array with the given items.
    pub fn array(items: Schema) -> Self {
        Schema::Array(ArraySchema {
            items: Box::new(items),
            attributes: Default::default(),
        })
    }

    /// Returns a Schema::Array with the given items and custom attributes.
    pub fn array_with_attributes(items: Schema, attributes: BTreeMap<String, Value>) -> Self {
        Schema::Array(ArraySchema {
            items: Box::new(items),
            attributes,
        })
    }
}

impl Parser {
    /// Create a `Schema` from a string representing a JSON Avro schema.
    fn parse_str(&mut self, input: &str) -> Result<Schema, Error> {
        let value = serde_json::from_str(input).map_err(Error::ParseSchemaJson)?;
        self.parse(&value, &None)
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
            let parsed = self.parse(&value, &None)?;
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
    fn parse(&mut self, value: &Value, enclosing_namespace: &Namespace) -> AvroResult<Schema> {
        match *value {
            Value::String(ref t) => self.parse_known_schema(t.as_str(), enclosing_namespace),
            Value::Object(ref data) => self.parse_complex(data, enclosing_namespace),
            Value::Array(ref data) => self.parse_union(data, enclosing_namespace),
            _ => Err(Error::ParseSchemaFromValidJson),
        }
    }

    /// Parse a `serde_json::Value` representing an Avro type whose Schema is known into a
    /// `Schema`. A Schema for a `serde_json::Value` is known if it is primitive or has
    /// been parsed previously by the parsed and stored in its map of parsed_schemas.
    fn parse_known_schema(
        &mut self,
        name: &str,
        enclosing_namespace: &Namespace,
    ) -> AvroResult<Schema> {
        match name {
            "null" => Ok(Schema::Null),
            "boolean" => Ok(Schema::Boolean),
            "int" => Ok(Schema::Int),
            "long" => Ok(Schema::Long),
            "double" => Ok(Schema::Double),
            "float" => Ok(Schema::Float),
            "bytes" => Ok(Schema::Bytes),
            "string" => Ok(Schema::String),
            _ => self.fetch_schema_ref(name, enclosing_namespace),
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
    fn fetch_schema_ref(
        &mut self,
        name: &str,
        enclosing_namespace: &Namespace,
    ) -> AvroResult<Schema> {
        fn get_schema_ref(parsed: &Schema) -> Schema {
            match &parsed {
                Schema::Record(RecordSchema { ref name, .. })
                | Schema::Enum(EnumSchema { ref name, .. })
                | Schema::Fixed(FixedSchema { ref name, .. }) => Schema::Ref { name: name.clone() },
                _ => parsed.clone(),
            }
        }

        let name = Name::new(name)?;
        let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);

        if self.parsed_schemas.contains_key(&fully_qualified_name) {
            return Ok(Schema::Ref {
                name: fully_qualified_name,
            });
        }
        if let Some(resolving_schema) = self.resolving_schemas.get(&fully_qualified_name) {
            return Ok(resolving_schema.clone());
        }

        let value = self
            .input_schemas
            .remove(&fully_qualified_name)
            // TODO make a better descriptive error message here that conveys that a named schema cannot be found
            .ok_or_else(|| Error::ParsePrimitive(fully_qualified_name.fullname(None)))?;

        // parsing a full schema from inside another schema. Other full schema will not inherit namespace
        let parsed = self.parse(&value, &None)?;
        self.parsed_schemas
            .insert(get_schema_type_name(name, value), parsed.clone());

        Ok(get_schema_ref(&parsed))
    }

    fn parse_precision_and_scale(
        complex: &Map<String, Value>,
    ) -> Result<(Precision, Scale), Error> {
        fn get_decimal_integer(
            complex: &Map<String, Value>,
            key: &'static str,
        ) -> Result<DecimalMetadata, Error> {
            match complex.get(key) {
                Some(Value::Number(value)) => parse_json_integer_for_decimal(value),
                None => {
                    if key == "scale" {
                        Ok(0)
                    } else {
                        Err(Error::GetDecimalMetadataFromJson(key))
                    }
                }
                Some(value) => Err(Error::GetDecimalMetadataValueFromJson {
                    key: key.into(),
                    value: value.clone(),
                }),
            }
        }
        let precision = get_decimal_integer(complex, "precision")?;
        let scale = get_decimal_integer(complex, "scale")?;

        if precision < 1 {
            return Err(Error::DecimalPrecisionMuBePositive { precision });
        }

        if precision < scale {
            Err(Error::DecimalPrecisionLessThanScale { precision, scale })
        } else {
            Ok((precision, scale))
        }
    }

    /// Parse a `serde_json::Value` representing a complex Avro type into a
    /// `Schema`.
    ///
    /// Avro supports "recursive" definition of types.
    /// e.g: {"type": {"type": "string"}}
    fn parse_complex(
        &mut self,
        complex: &Map<String, Value>,
        enclosing_namespace: &Namespace,
    ) -> AvroResult<Schema> {
        // Try to parse this as a native complex type.
        fn parse_as_native_complex(
            complex: &Map<String, Value>,
            parser: &mut Parser,
            enclosing_namespace: &Namespace,
        ) -> AvroResult<Schema> {
            match complex.get("type") {
                Some(value) => match value {
                    Value::String(s) if s == "fixed" => {
                        parser.parse_fixed(complex, enclosing_namespace)
                    }
                    _ => parser.parse(value, enclosing_namespace),
                },
                None => Err(Error::GetLogicalTypeField),
            }
        }

        // This crate support some logical types natively, and this function tries to convert
        // a native complex type with a logical type attribute to these logical types.
        // This function:
        // 1. Checks whether the native complex type is in the supported kinds.
        // 2. If it is, using the convert function to convert the native complex type to
        // a logical type.
        fn try_convert_to_logical_type<F>(
            logical_type: &str,
            schema: Schema,
            supported_schema_kinds: &[SchemaKind],
            convert: F,
        ) -> AvroResult<Schema>
        where
            F: Fn(Schema) -> AvroResult<Schema>,
        {
            let kind = SchemaKind::from(schema.clone());
            if supported_schema_kinds.contains(&kind) {
                convert(schema)
            } else {
                warn!(
                    "Ignoring unknown logical type '{}' for schema of type: {:?}!",
                    logical_type, schema
                );
                Ok(schema)
            }
        }

        match complex.get("logicalType") {
            Some(Value::String(t)) => match t.as_str() {
                "decimal" => {
                    return try_convert_to_logical_type(
                        "decimal",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Fixed, SchemaKind::Bytes],
                        |inner| -> AvroResult<Schema> {
                            match Self::parse_precision_and_scale(complex) {
                                Ok((precision, scale)) => Ok(Schema::Decimal(DecimalSchema {
                                    precision,
                                    scale,
                                    inner: Box::new(inner),
                                })),
                                Err(err) => {
                                    warn!("Ignoring invalid decimal logical type: {}", err);
                                    Ok(inner)
                                }
                            }
                        },
                    );
                }
                "big-decimal" => {
                    return try_convert_to_logical_type(
                        "big-decimal",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Bytes],
                        |_| -> AvroResult<Schema> { Ok(Schema::BigDecimal) },
                    );
                }
                "uuid" => {
                    return try_convert_to_logical_type(
                        "uuid",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::String, SchemaKind::Fixed],
                        |schema| match schema {
                            Schema::String => Ok(Schema::Uuid),
                            Schema::Fixed(FixedSchema { size: 16, .. }) => Ok(Schema::Uuid),
                            Schema::Fixed(FixedSchema { size, .. }) => {
                                warn!("Ignoring uuid logical type for a Fixed schema because its size ({size:?}) is not 16! Schema: {:?}", schema);
                                Ok(schema)
                            }
                            _ => {
                                warn!(
                                    "Ignoring invalid uuid logical type for schema: {:?}",
                                    schema
                                );
                                Ok(schema)
                            }
                        },
                    );
                }
                "date" => {
                    return try_convert_to_logical_type(
                        "date",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Int],
                        |_| -> AvroResult<Schema> { Ok(Schema::Date) },
                    );
                }
                "time-millis" => {
                    return try_convert_to_logical_type(
                        "date",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Int],
                        |_| -> AvroResult<Schema> { Ok(Schema::TimeMillis) },
                    );
                }
                "time-micros" => {
                    return try_convert_to_logical_type(
                        "time-micros",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Long],
                        |_| -> AvroResult<Schema> { Ok(Schema::TimeMicros) },
                    );
                }
                "timestamp-millis" => {
                    return try_convert_to_logical_type(
                        "timestamp-millis",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Long],
                        |_| -> AvroResult<Schema> { Ok(Schema::TimestampMillis) },
                    );
                }
                "timestamp-micros" => {
                    return try_convert_to_logical_type(
                        "timestamp-micros",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Long],
                        |_| -> AvroResult<Schema> { Ok(Schema::TimestampMicros) },
                    );
                }
                "timestamp-nanos" => {
                    return try_convert_to_logical_type(
                        "timestamp-nanos",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Long],
                        |_| -> AvroResult<Schema> { Ok(Schema::TimestampNanos) },
                    );
                }
                "local-timestamp-millis" => {
                    return try_convert_to_logical_type(
                        "local-timestamp-millis",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Long],
                        |_| -> AvroResult<Schema> { Ok(Schema::LocalTimestampMillis) },
                    );
                }
                "local-timestamp-micros" => {
                    return try_convert_to_logical_type(
                        "local-timestamp-micros",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Long],
                        |_| -> AvroResult<Schema> { Ok(Schema::LocalTimestampMicros) },
                    );
                }
                "local-timestamp-nanos" => {
                    return try_convert_to_logical_type(
                        "local-timestamp-nanos",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Long],
                        |_| -> AvroResult<Schema> { Ok(Schema::LocalTimestampNanos) },
                    );
                }
                "duration" => {
                    return try_convert_to_logical_type(
                        "duration",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Fixed],
                        |_| -> AvroResult<Schema> { Ok(Schema::Duration) },
                    );
                }
                // In this case, of an unknown logical type, we just pass through the underlying
                // type.
                _ => {}
            },
            // The spec says to ignore invalid logical types and just pass through the
            // underlying type. It is unclear whether that applies to this case or not, where the
            // `logicalType` is not a string.
            Some(value) => return Err(Error::GetLogicalTypeFieldType(value.clone())),
            _ => {}
        }
        match complex.get("type") {
            Some(Value::String(t)) => match t.as_str() {
                "record" => self.parse_record(complex, enclosing_namespace),
                "enum" => self.parse_enum(complex, enclosing_namespace),
                "array" => self.parse_array(complex, enclosing_namespace),
                "map" => self.parse_map(complex, enclosing_namespace),
                "fixed" => self.parse_fixed(complex, enclosing_namespace),
                other => self.parse_known_schema(other, enclosing_namespace),
            },
            Some(Value::Object(data)) => self.parse_complex(data, enclosing_namespace),
            Some(Value::Array(variants)) => self.parse_union(variants, enclosing_namespace),
            Some(unknown) => Err(Error::GetComplexType(unknown.clone())),
            None => Err(Error::GetComplexTypeField),
        }
    }

    fn register_resolving_schema(&mut self, name: &Name, aliases: &Aliases) {
        let resolving_schema = Schema::Ref { name: name.clone() };
        self.resolving_schemas
            .insert(name.clone(), resolving_schema.clone());

        let namespace = &name.namespace;

        if let Some(ref aliases) = aliases {
            aliases.iter().for_each(|alias| {
                let alias_fullname = alias.fully_qualified_name(namespace);
                self.resolving_schemas
                    .insert(alias_fullname, resolving_schema.clone());
            });
        }
    }

    fn register_parsed_schema(
        &mut self,
        fully_qualified_name: &Name,
        schema: &Schema,
        aliases: &Aliases,
    ) {
        // FIXME, this should be globally aware, so if there is something overwriting something
        // else then there is an ambiguous schema definition. An appropriate error should be thrown
        self.parsed_schemas
            .insert(fully_qualified_name.clone(), schema.clone());
        self.resolving_schemas.remove(fully_qualified_name);

        let namespace = &fully_qualified_name.namespace;

        if let Some(ref aliases) = aliases {
            aliases.iter().for_each(|alias| {
                let alias_fullname = alias.fully_qualified_name(namespace);
                self.resolving_schemas.remove(&alias_fullname);
                self.parsed_schemas.insert(alias_fullname, schema.clone());
            });
        }
    }

    /// Returns already parsed schema or a schema that is currently being resolved.
    fn get_already_seen_schema(
        &self,
        complex: &Map<String, Value>,
        enclosing_namespace: &Namespace,
    ) -> Option<&Schema> {
        match complex.get("type") {
            Some(Value::String(ref typ)) => {
                let name = Name::new(typ.as_str())
                    .unwrap()
                    .fully_qualified_name(enclosing_namespace);
                self.resolving_schemas
                    .get(&name)
                    .or_else(|| self.parsed_schemas.get(&name))
            }
            _ => None,
        }
    }

    /// Parse a `serde_json::Value` representing a Avro record type into a
    /// `Schema`.
    fn parse_record(
        &mut self,
        complex: &Map<String, Value>,
        enclosing_namespace: &Namespace,
    ) -> AvroResult<Schema> {
        let fields_opt = complex.get("fields");

        if fields_opt.is_none() {
            if let Some(seen) = self.get_already_seen_schema(complex, enclosing_namespace) {
                return Ok(seen.clone());
            }
        }

        let fully_qualified_name = Name::parse(complex, enclosing_namespace)?;
        let aliases = fix_aliases_namespace(complex.aliases(), &fully_qualified_name.namespace);

        let mut lookup = BTreeMap::new();

        self.register_resolving_schema(&fully_qualified_name, &aliases);

        debug!("Going to parse record schema: {:?}", &fully_qualified_name);

        let fields: Vec<RecordField> = fields_opt
            .and_then(|fields| fields.as_array())
            .ok_or(Error::GetRecordFieldsJson)
            .and_then(|fields| {
                fields
                    .iter()
                    .filter_map(|field| field.as_object())
                    .enumerate()
                    .map(|(position, field)| {
                        RecordField::parse(field, position, self, &fully_qualified_name)
                    })
                    .collect::<Result<_, _>>()
            })?;

        for field in &fields {
            if let Some(_old) = lookup.insert(field.name.clone(), field.position) {
                return Err(Error::FieldNameDuplicate(field.name.clone()));
            }

            if let Some(ref field_aliases) = field.aliases {
                for alias in field_aliases {
                    lookup.insert(alias.clone(), field.position);
                }
            }
        }

        let schema = Schema::Record(RecordSchema {
            name: fully_qualified_name.clone(),
            aliases: aliases.clone(),
            doc: complex.doc(),
            fields,
            lookup,
            attributes: self.get_custom_attributes(complex, vec!["fields"]),
        });

        self.register_parsed_schema(&fully_qualified_name, &schema, &aliases);
        Ok(schema)
    }

    fn get_custom_attributes(
        &self,
        complex: &Map<String, Value>,
        excluded: Vec<&'static str>,
    ) -> BTreeMap<String, Value> {
        let mut custom_attributes: BTreeMap<String, Value> = BTreeMap::new();
        for (key, value) in complex {
            match key.as_str() {
                "type" | "name" | "namespace" | "doc" | "aliases" => continue,
                candidate if excluded.contains(&candidate) => continue,
                _ => custom_attributes.insert(key.clone(), value.clone()),
            };
        }
        custom_attributes
    }

    /// Parse a `serde_json::Value` representing a Avro enum type into a
    /// `Schema`.
    fn parse_enum(
        &mut self,
        complex: &Map<String, Value>,
        enclosing_namespace: &Namespace,
    ) -> AvroResult<Schema> {
        let symbols_opt = complex.get("symbols");

        if symbols_opt.is_none() {
            if let Some(seen) = self.get_already_seen_schema(complex, enclosing_namespace) {
                return Ok(seen.clone());
            }
        }

        let fully_qualified_name = Name::parse(complex, enclosing_namespace)?;
        let aliases = fix_aliases_namespace(complex.aliases(), &fully_qualified_name.namespace);

        let symbols: Vec<String> = symbols_opt
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
            validate_enum_symbol_name(symbol)?;

            // Ensure there are no duplicate symbols
            if existing_symbols.contains(&symbol) {
                return Err(Error::EnumSymbolDuplicate(symbol.to_string()));
            }

            existing_symbols.insert(symbol);
        }

        let mut default: Option<String> = None;
        if let Some(value) = complex.get("default") {
            if let Value::String(ref s) = *value {
                default = Some(s.clone());
            } else {
                return Err(Error::EnumDefaultWrongType(value.clone()));
            }
        }

        if let Some(ref value) = default {
            let resolved = types::Value::from(value.clone())
                .resolve_enum(&symbols, &Some(value.to_string()), &None)
                .is_ok();
            if !resolved {
                return Err(Error::GetEnumDefault {
                    symbol: value.to_string(),
                    symbols,
                });
            }
        }

        let schema = Schema::Enum(EnumSchema {
            name: fully_qualified_name.clone(),
            aliases: aliases.clone(),
            doc: complex.doc(),
            symbols,
            default,
            attributes: self.get_custom_attributes(complex, vec!["symbols"]),
        });

        self.register_parsed_schema(&fully_qualified_name, &schema, &aliases);

        Ok(schema)
    }

    /// Parse a `serde_json::Value` representing a Avro array type into a
    /// `Schema`.
    fn parse_array(
        &mut self,
        complex: &Map<String, Value>,
        enclosing_namespace: &Namespace,
    ) -> AvroResult<Schema> {
        complex
            .get("items")
            .ok_or(Error::GetArrayItemsField)
            .and_then(|items| self.parse(items, enclosing_namespace))
            .map(|items| {
                Schema::array_with_attributes(
                    items,
                    self.get_custom_attributes(complex, vec!["items"]),
                )
            })
    }

    /// Parse a `serde_json::Value` representing a Avro map type into a
    /// `Schema`.
    fn parse_map(
        &mut self,
        complex: &Map<String, Value>,
        enclosing_namespace: &Namespace,
    ) -> AvroResult<Schema> {
        complex
            .get("values")
            .ok_or(Error::GetMapValuesField)
            .and_then(|items| self.parse(items, enclosing_namespace))
            .map(|items| {
                Schema::map_with_attributes(
                    items,
                    self.get_custom_attributes(complex, vec!["values"]),
                )
            })
    }

    /// Parse a `serde_json::Value` representing a Avro union type into a
    /// `Schema`.
    fn parse_union(
        &mut self,
        items: &[Value],
        enclosing_namespace: &Namespace,
    ) -> AvroResult<Schema> {
        items
            .iter()
            .map(|v| self.parse(v, enclosing_namespace))
            .collect::<Result<Vec<_>, _>>()
            .and_then(|schemas| {
                if schemas.is_empty() {
                    error!(
                        "Union schemas should have at least two members! \
                    Please enable debug logging to find out which Record schema \
                    declares the union with 'RUST_LOG=apache_avro::schema=debug'."
                    );
                } else if schemas.len() == 1 {
                    warn!(
                        "Union schema with just one member! Consider dropping the union! \
                    Please enable debug logging to find out which Record schema \
                    declares the union with 'RUST_LOG=apache_avro::schema=debug'."
                    );
                }
                Ok(Schema::Union(UnionSchema::new(schemas)?))
            })
    }

    /// Parse a `serde_json::Value` representing a Avro fixed type into a
    /// `Schema`.
    fn parse_fixed(
        &mut self,
        complex: &Map<String, Value>,
        enclosing_namespace: &Namespace,
    ) -> AvroResult<Schema> {
        let size_opt = complex.get("size");
        if size_opt.is_none() {
            if let Some(seen) = self.get_already_seen_schema(complex, enclosing_namespace) {
                return Ok(seen.clone());
            }
        }

        let doc = complex.get("doc").and_then(|v| match &v {
            Value::String(ref docstr) => Some(docstr.clone()),
            _ => None,
        });

        let size = match size_opt {
            Some(size) => size
                .as_u64()
                .ok_or_else(|| Error::GetFixedSizeFieldPositive(size.clone())),
            None => Err(Error::GetFixedSizeField),
        }?;

        let default = complex.get("default").and_then(|v| match &v {
            Value::String(ref default) => Some(default.clone()),
            _ => None,
        });

        if default.is_some() {
            let len = default.clone().unwrap().len();
            if len != size as usize {
                return Err(Error::FixedDefaultLenSizeMismatch(len, size));
            }
        }

        let fully_qualified_name = Name::parse(complex, enclosing_namespace)?;
        let aliases = fix_aliases_namespace(complex.aliases(), &fully_qualified_name.namespace);

        let schema = Schema::Fixed(FixedSchema {
            name: fully_qualified_name.clone(),
            aliases: aliases.clone(),
            doc,
            size: size as usize,
            default,
            attributes: self.get_custom_attributes(complex, vec!["size"]),
        });

        self.register_parsed_schema(&fully_qualified_name, &schema, &aliases);

        Ok(schema)
    }
}

// A type alias may be specified either as a fully namespace-qualified, or relative
// to the namespace of the name it is an alias for. For example, if a type named "a.b"
// has aliases of "c" and "x.y", then the fully qualified names of its aliases are "a.c"
// and "x.y".
// https://avro.apache.org/docs/current/specification/#aliases
fn fix_aliases_namespace(aliases: Option<Vec<String>>, namespace: &Namespace) -> Aliases {
    aliases.map(|aliases| {
        aliases
            .iter()
            .map(|alias| {
                if alias.find('.').is_none() {
                    match namespace {
                        Some(ref ns) => format!("{ns}.{alias}"),
                        None => alias.clone(),
                    }
                } else {
                    alias.clone()
                }
            })
            .map(|alias| Alias::new(alias.as_str()).unwrap())
            .collect()
    })
}

fn get_schema_type_name(name: Name, value: Value) -> Name {
    match value.get("type") {
        Some(Value::Object(complex_type)) => match complex_type.name() {
            Some(name) => Name::new(name.as_str()).unwrap(),
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
            Schema::Ref { ref name } => serializer.serialize_str(&name.fullname(None)),
            Schema::Null => serializer.serialize_str("null"),
            Schema::Boolean => serializer.serialize_str("boolean"),
            Schema::Int => serializer.serialize_str("int"),
            Schema::Long => serializer.serialize_str("long"),
            Schema::Float => serializer.serialize_str("float"),
            Schema::Double => serializer.serialize_str("double"),
            Schema::Bytes => serializer.serialize_str("bytes"),
            Schema::String => serializer.serialize_str("string"),
            Schema::Array(ref inner) => {
                let mut map = serializer.serialize_map(Some(2 + inner.attributes.len()))?;
                map.serialize_entry("type", "array")?;
                map.serialize_entry("items", &*inner.items.clone())?;
                for attr in &inner.attributes {
                    map.serialize_entry(attr.0, attr.1)?;
                }
                map.end()
            }
            Schema::Map(ref inner) => {
                let mut map = serializer.serialize_map(Some(2 + inner.attributes.len()))?;
                map.serialize_entry("type", "map")?;
                map.serialize_entry("values", &*inner.types.clone())?;
                for attr in &inner.attributes {
                    map.serialize_entry(attr.0, attr.1)?;
                }
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
            Schema::Record(RecordSchema {
                ref name,
                ref aliases,
                ref doc,
                ref fields,
                ref attributes,
                ..
            }) => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "record")?;
                if let Some(ref n) = name.namespace {
                    map.serialize_entry("namespace", n)?;
                }
                map.serialize_entry("name", &name.name)?;
                if let Some(ref docstr) = doc {
                    map.serialize_entry("doc", docstr)?;
                }
                if let Some(ref aliases) = aliases {
                    map.serialize_entry("aliases", aliases)?;
                }
                map.serialize_entry("fields", fields)?;
                for attr in attributes {
                    map.serialize_entry(attr.0, attr.1)?;
                }
                map.end()
            }
            Schema::Enum(EnumSchema {
                ref name,
                ref symbols,
                ref aliases,
                ref attributes,
                ..
            }) => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "enum")?;
                if let Some(ref n) = name.namespace {
                    map.serialize_entry("namespace", n)?;
                }
                map.serialize_entry("name", &name.name)?;
                map.serialize_entry("symbols", symbols)?;

                if let Some(ref aliases) = aliases {
                    map.serialize_entry("aliases", aliases)?;
                }
                for attr in attributes {
                    map.serialize_entry(attr.0, attr.1)?;
                }
                map.end()
            }
            Schema::Fixed(ref fixed_schema) => {
                let mut map = serializer.serialize_map(None)?;
                map = fixed_schema.serialize_to_map::<S>(map)?;
                map.end()
            }
            Schema::Decimal(DecimalSchema {
                ref scale,
                ref precision,
                ref inner,
            }) => {
                let mut map = serializer.serialize_map(None)?;
                match inner.as_ref() {
                    Schema::Fixed(fixed_schema) => {
                        map = fixed_schema.serialize_to_map::<S>(map)?;
                    }
                    Schema::Bytes => {
                        map.serialize_entry("type", "bytes")?;
                    }
                    others => {
                        return Err(serde::ser::Error::custom(format!(
                            "DecimalSchema inner type must be Fixed or Bytes, got {:?}",
                            SchemaKind::from(others)
                        )));
                    }
                }
                map.serialize_entry("logicalType", "decimal")?;
                map.serialize_entry("scale", scale)?;
                map.serialize_entry("precision", precision)?;
                map.end()
            }

            Schema::BigDecimal => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "bytes")?;
                map.serialize_entry("logicalType", "big-decimal")?;
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
            Schema::TimestampNanos => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "timestamp-nanos")?;
                map.end()
            }
            Schema::LocalTimestampMillis => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "local-timestamp-millis")?;
                map.end()
            }
            Schema::LocalTimestampMicros => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "local-timestamp-micros")?;
                map.end()
            }
            Schema::LocalTimestampNanos => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "local-timestamp-nanos")?;
                map.end()
            }
            Schema::Duration => {
                let mut map = serializer.serialize_map(None)?;

                // the Avro doesn't indicate what the name of the underlying fixed type of a
                // duration should be or typically is.
                let inner = Schema::Fixed(FixedSchema {
                    name: Name::new("duration").unwrap(),
                    aliases: None,
                    doc: None,
                    size: 12,
                    default: None,
                    attributes: Default::default(),
                });
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

        if let Some(ref aliases) = self.aliases {
            map.serialize_entry("aliases", aliases)?;
        }

        for attr in &self.custom_attributes {
            map.serialize_entry(attr.0, attr.1)?;
        }

        map.end()
    }
}

/// Parses a **valid** avro schema into the Parsing Canonical Form.
/// https://avro.apache.org/docs/current/specification/#parsing-canonical-form-for-schemas
fn parsing_canonical_form(schema: &Value) -> String {
    match schema {
        Value::Object(map) => pcf_map(map),
        Value::String(s) => pcf_string(s),
        Value::Array(v) => pcf_array(v),
        json => panic!("got invalid JSON value for canonical form of schema: {json}"),
    }
}

fn pcf_map(schema: &Map<String, Value>) -> String {
    // Look for the namespace variant up front.
    let ns = schema.get("namespace").and_then(|v| v.as_str());
    let typ = schema.get("type").and_then(|v| v.as_str());
    let mut fields = Vec::new();
    for (k, v) in schema {
        // Reduce primitive types to their simple form. ([PRIMITIVE] rule)
        if schema.len() == 1 && k == "type" {
            // Invariant: function is only callable from a valid schema, so this is acceptable.
            if let Value::String(s) = v {
                return pcf_string(s);
            }
        }

        // Strip out unused fields ([STRIP] rule)
        if field_ordering_position(k).is_none()
            || k == "default"
            || k == "doc"
            || k == "aliases"
            || k == "logicalType"
        {
            continue;
        }

        // Fully qualify the name, if it isn't already ([FULLNAMES] rule).
        if k == "name" {
            // Invariant: Only valid schemas. Must be a string.
            let name = v.as_str().unwrap();
            let n = match ns {
                Some(namespace) if is_named_type(typ) && !name.contains('.') => {
                    Cow::Owned(format!("{namespace}.{name}"))
                }
                _ => Cow::Borrowed(name),
            };

            fields.push((k, format!("{}:{}", pcf_string(k), pcf_string(&n))));
            continue;
        }

        // Strip off quotes surrounding "size" type, if they exist ([INTEGERS] rule).
        if k == "size" || k == "precision" || k == "scale" {
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
    format!("{{{inter}}}")
}

fn is_named_type(typ: Option<&str>) -> bool {
    matches!(
        typ,
        Some("record") | Some("enum") | Some("fixed") | Some("ref")
    )
}

fn pcf_array(arr: &[Value]) -> String {
    let inter = arr
        .iter()
        .map(parsing_canonical_form)
        .collect::<Vec<String>>()
        .join(",");
    format!("[{inter}]")
}

fn pcf_string(s: &str) -> String {
    format!("\"{s}\"")
}

const RESERVED_FIELDS: &[&str] = &[
    "name",
    "type",
    "fields",
    "symbols",
    "items",
    "values",
    "size",
    "logicalType",
    "order",
    "doc",
    "aliases",
    "default",
    "precision",
    "scale",
];

// Used to define the ordering and inclusion of fields.
fn field_ordering_position(field: &str) -> Option<usize> {
    RESERVED_FIELDS
        .iter()
        .position(|&f| f == field)
        .map(|pos| pos + 1)
}

/// Trait for types that serve as an Avro data model. Derive implementation available
/// through `derive` feature. Do not implement directly!
/// Implement `apache_avro::schema::derive::AvroSchemaComponent` to get this trait
/// through a blanket implementation.
pub trait AvroSchema {
    fn get_schema() -> Schema;
}

#[cfg(feature = "derive")]
pub mod derive {
    use super::*;

    /// Trait for types that serve as fully defined components inside an Avro data model. Derive
    /// implementation available through `derive` feature. This is what is implemented by
    /// the `derive(AvroSchema)` macro.
    ///
    /// # Implementation guide
    ///
    ///### Simple implementation
    /// To construct a non named simple schema, it is possible to ignore the input argument making the
    /// general form implementation look like
    /// ```ignore
    /// impl AvroSchemaComponent for AType {
    ///     fn get_schema_in_ctxt(_: &mut Names, _: &Namespace) -> Schema {
    ///        Schema::?
    ///    }
    ///}
    /// ```
    /// ### Passthrough implementation
    /// To construct a schema for a Type that acts as in "inner" type, such as for smart pointers, simply
    /// pass through the arguments to the inner type
    /// ```ignore
    /// impl AvroSchemaComponent for PassthroughType {
    ///     fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
    ///        InnerType::get_schema_in_ctxt(names, enclosing_namespace)
    ///    }
    ///}
    /// ```
    ///### Complex implementation
    /// To implement this for Named schema there is a general form needed to avoid creating invalid
    /// schemas or infinite loops.
    /// ```ignore
    /// impl AvroSchemaComponent for ComplexType {
    ///     fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
    ///         // Create the fully qualified name for your type given the enclosing namespace
    ///         let name =  apache_avro::schema::Name::new("MyName")
    ///             .expect("Unable to parse schema name")
    ///             .fully_qualified_name(enclosing_namespace);
    ///         let enclosing_namespace = &name.namespace;
    ///         // Check, if your name is already defined, and if so, return a ref to that name
    ///         if named_schemas.contains_key(&name) {
    ///             apache_avro::schema::Schema::Ref{name: name.clone()}
    ///         } else {
    ///             named_schemas.insert(name.clone(), apache_avro::schema::Schema::Ref{name: name.clone()});
    ///             // YOUR SCHEMA DEFINITION HERE with the name equivalent to "MyName".
    ///             // For non-simple sub types delegate to their implementation of AvroSchemaComponent
    ///         }
    ///    }
    ///}
    /// ```
    pub trait AvroSchemaComponent {
        fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace)
            -> Schema;
    }

    impl<T> AvroSchema for T
    where
        T: AvroSchemaComponent,
    {
        fn get_schema() -> Schema {
            T::get_schema_in_ctxt(&mut HashMap::default(), &None)
        }
    }

    macro_rules! impl_schema (
        ($type:ty, $variant_constructor:expr) => (
            impl AvroSchemaComponent for $type {
                fn get_schema_in_ctxt(_: &mut Names, _: &Namespace) -> Schema {
                    $variant_constructor
                }
            }
        );
    );

    impl_schema!(bool, Schema::Boolean);
    impl_schema!(i8, Schema::Int);
    impl_schema!(i16, Schema::Int);
    impl_schema!(i32, Schema::Int);
    impl_schema!(i64, Schema::Long);
    impl_schema!(u8, Schema::Int);
    impl_schema!(u16, Schema::Int);
    impl_schema!(u32, Schema::Long);
    impl_schema!(f32, Schema::Float);
    impl_schema!(f64, Schema::Double);
    impl_schema!(String, Schema::String);
    impl_schema!(uuid::Uuid, Schema::Uuid);
    impl_schema!(core::time::Duration, Schema::Duration);

    impl<T> AvroSchemaComponent for Vec<T>
    where
        T: AvroSchemaComponent,
    {
        fn get_schema_in_ctxt(
            named_schemas: &mut Names,
            enclosing_namespace: &Namespace,
        ) -> Schema {
            Schema::array(T::get_schema_in_ctxt(named_schemas, enclosing_namespace))
        }
    }

    impl<T> AvroSchemaComponent for Option<T>
    where
        T: AvroSchemaComponent,
    {
        fn get_schema_in_ctxt(
            named_schemas: &mut Names,
            enclosing_namespace: &Namespace,
        ) -> Schema {
            let inner_schema = T::get_schema_in_ctxt(named_schemas, enclosing_namespace);
            Schema::Union(UnionSchema {
                schemas: vec![Schema::Null, inner_schema.clone()],
                variant_index: vec![Schema::Null, inner_schema]
                    .iter()
                    .enumerate()
                    .map(|(idx, s)| (SchemaKind::from(s), idx))
                    .collect(),
            })
        }
    }

    impl<T> AvroSchemaComponent for Map<String, T>
    where
        T: AvroSchemaComponent,
    {
        fn get_schema_in_ctxt(
            named_schemas: &mut Names,
            enclosing_namespace: &Namespace,
        ) -> Schema {
            Schema::map(T::get_schema_in_ctxt(named_schemas, enclosing_namespace))
        }
    }

    impl<T> AvroSchemaComponent for HashMap<String, T>
    where
        T: AvroSchemaComponent,
    {
        fn get_schema_in_ctxt(
            named_schemas: &mut Names,
            enclosing_namespace: &Namespace,
        ) -> Schema {
            Schema::map(T::get_schema_in_ctxt(named_schemas, enclosing_namespace))
        }
    }

    impl<T> AvroSchemaComponent for Box<T>
    where
        T: AvroSchemaComponent,
    {
        fn get_schema_in_ctxt(
            named_schemas: &mut Names,
            enclosing_namespace: &Namespace,
        ) -> Schema {
            T::get_schema_in_ctxt(named_schemas, enclosing_namespace)
        }
    }

    impl<T> AvroSchemaComponent for std::sync::Mutex<T>
    where
        T: AvroSchemaComponent,
    {
        fn get_schema_in_ctxt(
            named_schemas: &mut Names,
            enclosing_namespace: &Namespace,
        ) -> Schema {
            T::get_schema_in_ctxt(named_schemas, enclosing_namespace)
        }
    }

    impl<T> AvroSchemaComponent for Cow<'_, T>
    where
        T: AvroSchemaComponent + Clone,
    {
        fn get_schema_in_ctxt(
            named_schemas: &mut Names,
            enclosing_namespace: &Namespace,
        ) -> Schema {
            T::get_schema_in_ctxt(named_schemas, enclosing_namespace)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rabin::Rabin;
    use apache_avro_test_helper::{
        logger::{assert_logged, assert_not_logged},
        TestResult,
    };
    use serde_json::json;

    #[test]
    fn test_invalid_schema() {
        assert!(Schema::parse_str("invalid").is_err());
    }

    #[test]
    fn test_primitive_schema() -> TestResult {
        assert_eq!(Schema::Null, Schema::parse_str("\"null\"")?);
        assert_eq!(Schema::Int, Schema::parse_str("\"int\"")?);
        assert_eq!(Schema::Double, Schema::parse_str("\"double\"")?);
        Ok(())
    }

    #[test]
    fn test_array_schema() -> TestResult {
        let schema = Schema::parse_str(r#"{"type": "array", "items": "string"}"#)?;
        assert_eq!(Schema::array(Schema::String), schema);
        Ok(())
    }

    #[test]
    fn test_map_schema() -> TestResult {
        let schema = Schema::parse_str(r#"{"type": "map", "values": "double"}"#)?;
        assert_eq!(Schema::map(Schema::Double), schema);
        Ok(())
    }

    #[test]
    fn test_union_schema() -> TestResult {
        let schema = Schema::parse_str(r#"["null", "int"]"#)?;
        assert_eq!(
            Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int])?),
            schema
        );
        Ok(())
    }

    #[test]
    fn test_union_unsupported_schema() {
        let schema = Schema::parse_str(r#"["null", ["null", "int"], "string"]"#);
        assert!(schema.is_err());
    }

    #[test]
    fn test_multi_union_schema() -> TestResult {
        let schema = Schema::parse_str(r#"["null", "int", "float", "string", "bytes"]"#);
        assert!(schema.is_ok());
        let schema = schema?;
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

        Ok(())
    }

    #[test]
    fn test_avro_3621_nullable_record_field() -> TestResult {
        let nullable_record_field = RecordField {
            name: "next".to_string(),
            doc: None,
            default: None,
            aliases: None,
            schema: Schema::Union(UnionSchema::new(vec![
                Schema::Null,
                Schema::Ref {
                    name: Name {
                        name: "LongList".to_owned(),
                        namespace: None,
                    },
                },
            ])?),
            order: RecordFieldOrder::Ascending,
            position: 1,
            custom_attributes: Default::default(),
        };

        assert!(nullable_record_field.is_nullable());

        let non_nullable_record_field = RecordField {
            name: "next".to_string(),
            doc: None,
            default: Some(json!(2)),
            aliases: None,
            schema: Schema::Long,
            order: RecordFieldOrder::Ascending,
            position: 1,
            custom_attributes: Default::default(),
        };

        assert!(!non_nullable_record_field.is_nullable());
        Ok(())
    }

    // AVRO-3248
    #[test]
    fn test_union_of_records() -> TestResult {
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

        let schema_c = Schema::parse_list(&[schema_str_a, schema_str_b, schema_str_c])?
            .last()
            .unwrap()
            .clone();

        let schema_c_expected = Schema::Record(RecordSchema {
            name: Name::new("C")?,
            aliases: None,
            doc: None,
            fields: vec![RecordField {
                name: "field_one".to_string(),
                doc: None,
                default: None,
                aliases: None,
                schema: Schema::Union(UnionSchema::new(vec![
                    Schema::Ref {
                        name: Name::new("A")?,
                    },
                    Schema::Ref {
                        name: Name::new("B")?,
                    },
                ])?),
                order: RecordFieldOrder::Ignore,
                position: 0,
                custom_attributes: Default::default(),
            }],
            lookup: BTreeMap::from_iter(vec![("field_one".to_string(), 0)]),
            attributes: Default::default(),
        });

        assert_eq!(schema_c, schema_c_expected);
        Ok(())
    }

    #[test]
    fn avro_3584_test_recursion_records() -> TestResult {
        // A and B are the same except the name.
        let schema_str_a = r#"{
            "name": "A",
            "type": "record",
            "fields": [ {"name": "field_one", "type": "B"} ]
        }"#;

        let schema_str_b = r#"{
            "name": "B",
            "type": "record",
            "fields": [ {"name": "field_one", "type": "A"} ]
        }"#;

        let list = Schema::parse_list(&[schema_str_a, schema_str_b])?;

        let schema_a = list.first().unwrap().clone();

        match schema_a {
            Schema::Record(RecordSchema { fields, .. }) => {
                let f1 = fields.first();

                let ref_schema = Schema::Ref {
                    name: Name::new("B")?,
                };
                assert_eq!(ref_schema, f1.unwrap().schema);
            }
            _ => panic!("Expected a record schema!"),
        }

        Ok(())
    }

    #[test]
    fn test_avro_3248_nullable_record() -> TestResult {
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
                {"name": "field_one",  "type": ["null", "A"], "default": null}
            ]
        }"#;

        let schema_option_a = Schema::parse_list(&[schema_str_a, schema_str_option_a])?
            .last()
            .unwrap()
            .clone();

        let schema_option_a_expected = Schema::Record(RecordSchema {
            name: Name::new("OptionA")?,
            aliases: None,
            doc: None,
            fields: vec![RecordField {
                name: "field_one".to_string(),
                doc: None,
                default: Some(Value::Null),
                aliases: None,
                schema: Schema::Union(UnionSchema::new(vec![
                    Schema::Null,
                    Schema::Ref {
                        name: Name::new("A")?,
                    },
                ])?),
                order: RecordFieldOrder::Ignore,
                position: 0,
                custom_attributes: Default::default(),
            }],
            lookup: BTreeMap::from_iter(vec![("field_one".to_string(), 0)]),
            attributes: Default::default(),
        });

        assert_eq!(schema_option_a, schema_option_a_expected);

        Ok(())
    }

    #[test]
    fn test_record_schema() -> TestResult {
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
        )?;

        let mut lookup = BTreeMap::new();
        lookup.insert("a".to_owned(), 0);
        lookup.insert("b".to_owned(), 1);

        let expected = Schema::Record(RecordSchema {
            name: Name::new("test")?,
            aliases: None,
            doc: None,
            fields: vec![
                RecordField {
                    name: "a".to_string(),
                    doc: None,
                    default: Some(Value::Number(42i64.into())),
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
            ],
            lookup,
            attributes: Default::default(),
        });

        assert_eq!(parsed, expected);

        Ok(())
    }

    #[test]
    fn test_avro_3302_record_schema_with_currently_parsing_schema() -> TestResult {
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
        )?;

        let mut lookup = BTreeMap::new();
        lookup.insert("recordField".to_owned(), 0);

        let mut node_lookup = BTreeMap::new();
        node_lookup.insert("children".to_owned(), 1);
        node_lookup.insert("label".to_owned(), 0);

        let expected = Schema::Record(RecordSchema {
            name: Name::new("test")?,
            aliases: None,
            doc: None,
            fields: vec![RecordField {
                name: "recordField".to_string(),
                doc: None,
                default: None,
                aliases: None,
                schema: Schema::Record(RecordSchema {
                    name: Name::new("Node")?,
                    aliases: None,
                    doc: None,
                    fields: vec![
                        RecordField {
                            name: "label".to_string(),
                            doc: None,
                            default: None,
                            aliases: None,
                            schema: Schema::String,
                            order: RecordFieldOrder::Ascending,
                            position: 0,
                            custom_attributes: Default::default(),
                        },
                        RecordField {
                            name: "children".to_string(),
                            doc: None,
                            default: None,
                            aliases: None,
                            schema: Schema::array(Schema::Ref {
                                name: Name::new("Node")?,
                            }),
                            order: RecordFieldOrder::Ascending,
                            position: 1,
                            custom_attributes: Default::default(),
                        },
                    ],
                    lookup: node_lookup,
                    attributes: Default::default(),
                }),
                order: RecordFieldOrder::Ascending,
                position: 0,
                custom_attributes: Default::default(),
            }],
            lookup,
            attributes: Default::default(),
        });
        assert_eq!(schema, expected);

        let canonical_form = &schema.canonical_form();
        let expected = r#"{"name":"test","type":"record","fields":[{"name":"recordField","type":{"name":"Node","type":"record","fields":[{"name":"label","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}"#;
        assert_eq!(canonical_form, &expected);

        Ok(())
    }

    // https://github.com/flavray/avro-rs/pull/99#issuecomment-1016948451
    #[test]
    fn test_parsing_of_recursive_type_enum() -> TestResult {
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

        let schema = Schema::parse_str(schema)?;
        let schema_str = schema.canonical_form();
        let expected = r#"{"name":"office.User","type":"record","fields":[{"name":"details","type":[{"name":"office.Employee","type":"record","fields":[{"name":"gender","type":{"name":"office.Gender","type":"enum","symbols":["male","female"]}}]},{"name":"office.Manager","type":"record","fields":[{"name":"gender","type":"office.Gender"}]}]}]}"#;
        assert_eq!(schema_str, expected);

        Ok(())
    }

    #[test]
    fn test_parsing_of_recursive_type_fixed() -> TestResult {
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

        let schema = Schema::parse_str(schema)?;
        let schema_str = schema.canonical_form();
        let expected = r#"{"name":"office.User","type":"record","fields":[{"name":"details","type":[{"name":"office.Employee","type":"record","fields":[{"name":"id","type":{"name":"office.EmployeeId","type":"fixed","size":16}}]},{"name":"office.Manager","type":"record","fields":[{"name":"id","type":"office.EmployeeId"}]}]}]}"#;
        assert_eq!(schema_str, expected);

        Ok(())
    }

    #[test]
    fn test_avro_3302_record_schema_with_currently_parsing_schema_aliases() -> TestResult {
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
        )?;

        let mut lookup = BTreeMap::new();
        lookup.insert("value".to_owned(), 0);
        lookup.insert("next".to_owned(), 1);

        let expected = Schema::Record(RecordSchema {
            name: Name {
                name: "LongList".to_owned(),
                namespace: None,
            },
            aliases: Some(vec![Alias::new("LinkedLongs").unwrap()]),
            doc: None,
            fields: vec![
                RecordField {
                    name: "value".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Long,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    custom_attributes: Default::default(),
                },
                RecordField {
                    name: "next".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Union(UnionSchema::new(vec![
                        Schema::Null,
                        Schema::Ref {
                            name: Name {
                                name: "LongList".to_owned(),
                                namespace: None,
                            },
                        },
                    ])?),
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                    custom_attributes: Default::default(),
                },
            ],
            lookup,
            attributes: Default::default(),
        });
        assert_eq!(schema, expected);

        let canonical_form = &schema.canonical_form();
        let expected = r#"{"name":"LongList","type":"record","fields":[{"name":"value","type":"long"},{"name":"next","type":["null","LongList"]}]}"#;
        assert_eq!(canonical_form, &expected);

        Ok(())
    }

    #[test]
    fn test_avro_3370_record_schema_with_currently_parsing_schema_named_record() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type" : "record",
              "name" : "record",
              "fields" : [
                 { "name" : "value", "type" : "long" },
                 { "name" : "next", "type" : "record" }
             ]
            }
        "#,
        )?;

        let mut lookup = BTreeMap::new();
        lookup.insert("value".to_owned(), 0);
        lookup.insert("next".to_owned(), 1);

        let expected = Schema::Record(RecordSchema {
            name: Name {
                name: "record".to_owned(),
                namespace: None,
            },
            aliases: None,
            doc: None,
            fields: vec![
                RecordField {
                    name: "value".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Long,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    custom_attributes: Default::default(),
                },
                RecordField {
                    name: "next".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Ref {
                        name: Name {
                            name: "record".to_owned(),
                            namespace: None,
                        },
                    },
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                    custom_attributes: Default::default(),
                },
            ],
            lookup,
            attributes: Default::default(),
        });
        assert_eq!(schema, expected);

        let canonical_form = &schema.canonical_form();
        let expected = r#"{"name":"record","type":"record","fields":[{"name":"value","type":"long"},{"name":"next","type":"record"}]}"#;
        assert_eq!(canonical_form, &expected);

        Ok(())
    }

    #[test]
    fn test_avro_3370_record_schema_with_currently_parsing_schema_named_enum() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type" : "record",
              "name" : "record",
              "fields" : [
                 {
                    "type" : "enum",
                    "name" : "enum",
                    "symbols": ["one", "two", "three"]
                 },
                 { "name" : "next", "type" : "enum" }
             ]
            }
        "#,
        )?;

        let mut lookup = BTreeMap::new();
        lookup.insert("enum".to_owned(), 0);
        lookup.insert("next".to_owned(), 1);

        let expected = Schema::Record(RecordSchema {
            name: Name {
                name: "record".to_owned(),
                namespace: None,
            },
            aliases: None,
            doc: None,
            fields: vec![
                RecordField {
                    name: "enum".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Enum(EnumSchema {
                        name: Name {
                            name: "enum".to_owned(),
                            namespace: None,
                        },
                        aliases: None,
                        doc: None,
                        symbols: vec!["one".to_string(), "two".to_string(), "three".to_string()],
                        default: None,
                        attributes: Default::default(),
                    }),
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    custom_attributes: Default::default(),
                },
                RecordField {
                    name: "next".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Enum(EnumSchema {
                        name: Name {
                            name: "enum".to_owned(),
                            namespace: None,
                        },
                        aliases: None,
                        doc: None,
                        symbols: vec!["one".to_string(), "two".to_string(), "three".to_string()],
                        default: None,
                        attributes: Default::default(),
                    }),
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                    custom_attributes: Default::default(),
                },
            ],
            lookup,
            attributes: Default::default(),
        });
        assert_eq!(schema, expected);

        let canonical_form = &schema.canonical_form();
        let expected = r#"{"name":"record","type":"record","fields":[{"name":"enum","type":{"name":"enum","type":"enum","symbols":["one","two","three"]}},{"name":"next","type":{"name":"enum","type":"enum","symbols":["one","two","three"]}}]}"#;
        assert_eq!(canonical_form, &expected);

        Ok(())
    }

    #[test]
    fn test_avro_3370_record_schema_with_currently_parsing_schema_named_fixed() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type" : "record",
              "name" : "record",
              "fields" : [
                 {
                    "type" : "fixed",
                    "name" : "fixed",
                    "size": 456
                 },
                 { "name" : "next", "type" : "fixed" }
             ]
            }
        "#,
        )?;

        let mut lookup = BTreeMap::new();
        lookup.insert("fixed".to_owned(), 0);
        lookup.insert("next".to_owned(), 1);

        let expected = Schema::Record(RecordSchema {
            name: Name {
                name: "record".to_owned(),
                namespace: None,
            },
            aliases: None,
            doc: None,
            fields: vec![
                RecordField {
                    name: "fixed".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Fixed(FixedSchema {
                        name: Name {
                            name: "fixed".to_owned(),
                            namespace: None,
                        },
                        aliases: None,
                        doc: None,
                        size: 456,
                        default: None,
                        attributes: Default::default(),
                    }),
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    custom_attributes: Default::default(),
                },
                RecordField {
                    name: "next".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Fixed(FixedSchema {
                        name: Name {
                            name: "fixed".to_owned(),
                            namespace: None,
                        },
                        aliases: None,
                        doc: None,
                        size: 456,
                        default: None,
                        attributes: Default::default(),
                    }),
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                    custom_attributes: Default::default(),
                },
            ],
            lookup,
            attributes: Default::default(),
        });
        assert_eq!(schema, expected);

        let canonical_form = &schema.canonical_form();
        let expected = r#"{"name":"record","type":"record","fields":[{"name":"fixed","type":{"name":"fixed","type":"fixed","size":456}},{"name":"next","type":{"name":"fixed","type":"fixed","size":456}}]}"#;
        assert_eq!(canonical_form, &expected);

        Ok(())
    }

    #[test]
    fn test_enum_schema() -> TestResult {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Suit", "symbols": ["diamonds", "spades", "clubs", "hearts"]}"#,
        )?;

        let expected = Schema::Enum(EnumSchema {
            name: Name::new("Suit")?,
            aliases: None,
            doc: None,
            symbols: vec![
                "diamonds".to_owned(),
                "spades".to_owned(),
                "clubs".to_owned(),
                "hearts".to_owned(),
            ],
            default: None,
            attributes: Default::default(),
        });

        assert_eq!(expected, schema);

        Ok(())
    }

    #[test]
    fn test_enum_schema_duplicate() -> TestResult {
        // Duplicate "diamonds"
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Suit", "symbols": ["diamonds", "spades", "clubs", "diamonds"]}"#,
        );
        assert!(schema.is_err());

        Ok(())
    }

    #[test]
    fn test_enum_schema_name() -> TestResult {
        // Invalid name "0000" does not match [A-Za-z_][A-Za-z0-9_]*
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Enum", "symbols": ["0000", "variant"]}"#,
        );
        assert!(schema.is_err());

        Ok(())
    }

    #[test]
    fn test_fixed_schema() -> TestResult {
        let schema = Schema::parse_str(r#"{"type": "fixed", "name": "test", "size": 16}"#)?;

        let expected = Schema::Fixed(FixedSchema {
            name: Name::new("test")?,
            aliases: None,
            doc: None,
            size: 16_usize,
            default: None,
            attributes: Default::default(),
        });

        assert_eq!(expected, schema);

        Ok(())
    }

    #[test]
    fn test_fixed_schema_with_documentation() -> TestResult {
        let schema = Schema::parse_str(
            r#"{"type": "fixed", "name": "test", "size": 16, "doc": "FixedSchema documentation"}"#,
        )?;

        let expected = Schema::Fixed(FixedSchema {
            name: Name::new("test")?,
            aliases: None,
            doc: Some(String::from("FixedSchema documentation")),
            size: 16_usize,
            default: None,
            attributes: Default::default(),
        });

        assert_eq!(expected, schema);

        Ok(())
    }

    #[test]
    fn test_no_documentation() -> TestResult {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Coin", "symbols": ["heads", "tails"]}"#,
        )?;

        let doc = match schema {
            Schema::Enum(EnumSchema { doc, .. }) => doc,
            _ => unreachable!(),
        };

        assert!(doc.is_none());

        Ok(())
    }

    #[test]
    fn test_documentation() -> TestResult {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Coin", "doc": "Some documentation", "symbols": ["heads", "tails"]}"#,
        )?;

        let doc = match schema {
            Schema::Enum(EnumSchema { doc, .. }) => doc,
            _ => None,
        };

        assert_eq!("Some documentation".to_owned(), doc.unwrap());

        Ok(())
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
    fn test_schema_fingerprint() -> TestResult {
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

        let schema = Schema::parse_str(raw_schema)?;
        assert_eq!(
            "7eb3b28d73dfc99bdd9af1848298b40804a2f8ad5d2642be2ecc2ad34842b987",
            format!("{}", schema.fingerprint::<Sha256>())
        );

        assert_eq!(
            "cb11615e412ee5d872620d8df78ff6ae",
            format!("{}", schema.fingerprint::<Md5>())
        );
        assert_eq!(
            "92f2ccef718c6754",
            format!("{}", schema.fingerprint::<Rabin>())
        );

        Ok(())
    }

    #[test]
    fn test_logical_types() -> TestResult {
        let schema = Schema::parse_str(r#"{"type": "int", "logicalType": "date"}"#)?;
        assert_eq!(schema, Schema::Date);

        let schema = Schema::parse_str(r#"{"type": "long", "logicalType": "timestamp-micros"}"#)?;
        assert_eq!(schema, Schema::TimestampMicros);

        Ok(())
    }

    #[test]
    fn test_nullable_logical_type() -> TestResult {
        let schema = Schema::parse_str(
            r#"{"type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]}"#,
        )?;
        assert_eq!(
            schema,
            Schema::Union(UnionSchema::new(vec![
                Schema::Null,
                Schema::TimestampMicros,
            ])?)
        );

        Ok(())
    }

    #[test]
    fn record_field_order_from_str() -> TestResult {
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

        Ok(())
    }

    #[test]
    fn test_avro_3374_preserve_namespace_for_primitive() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type" : "record",
              "name" : "ns.int",
              "fields" : [
                {"name" : "value", "type" : "int"},
                {"name" : "next", "type" : [ "null", "ns.int" ]}
              ]
            }
            "#,
        )?;

        let json = schema.canonical_form();
        assert_eq!(
            json,
            r#"{"name":"ns.int","type":"record","fields":[{"name":"value","type":"int"},{"name":"next","type":["null","ns.int"]}]}"#
        );

        Ok(())
    }

    #[test]
    fn test_avro_3433_preserve_schema_refs_in_json() -> TestResult {
        let schema = r#"
    {
      "name": "test.test",
      "type": "record",
      "fields": [
        {
          "name": "bar",
          "type": { "name": "test.foo", "type": "record", "fields": [{ "name": "id", "type": "long" }] }
        },
        { "name": "baz", "type": "test.foo" }
      ]
    }
    "#;

        let schema = Schema::parse_str(schema)?;

        let expected = r#"{"name":"test.test","type":"record","fields":[{"name":"bar","type":{"name":"test.foo","type":"record","fields":[{"name":"id","type":"long"}]}},{"name":"baz","type":"test.foo"}]}"#;
        assert_eq!(schema.canonical_form(), expected);

        Ok(())
    }

    #[test]
    fn test_read_namespace_from_name() -> TestResult {
        let schema = r#"
    {
      "name": "space.name",
      "type": "record",
      "fields": [
        {
          "name": "num",
          "type": "int"
        }
      ]
    }
    "#;

        let schema = Schema::parse_str(schema)?;
        if let Schema::Record(RecordSchema { name, .. }) = schema {
            assert_eq!(name.name, "name");
            assert_eq!(name.namespace, Some("space".to_string()));
        } else {
            panic!("Expected a record schema!");
        }

        Ok(())
    }

    #[test]
    fn test_namespace_from_name_has_priority_over_from_field() -> TestResult {
        let schema = r#"
    {
      "name": "space1.name",
      "namespace": "space2",
      "type": "record",
      "fields": [
        {
          "name": "num",
          "type": "int"
        }
      ]
    }
    "#;

        let schema = Schema::parse_str(schema)?;
        if let Schema::Record(RecordSchema { name, .. }) = schema {
            assert_eq!(name.namespace, Some("space1".to_string()));
        } else {
            panic!("Expected a record schema!");
        }

        Ok(())
    }

    #[test]
    fn test_namespace_from_field() -> TestResult {
        let schema = r#"
    {
      "name": "name",
      "namespace": "space2",
      "type": "record",
      "fields": [
        {
          "name": "num",
          "type": "int"
        }
      ]
    }
    "#;

        let schema = Schema::parse_str(schema)?;
        if let Schema::Record(RecordSchema { name, .. }) = schema {
            assert_eq!(name.namespace, Some("space2".to_string()));
        } else {
            panic!("Expected a record schema!");
        }

        Ok(())
    }

    #[test]
    /// Zero-length namespace is considered as no-namespace.
    fn test_namespace_from_name_with_empty_value() -> TestResult {
        let name = Name::new(".name")?;
        assert_eq!(name.name, "name");
        assert_eq!(name.namespace, None);

        Ok(())
    }

    #[test]
    /// Whitespace is not allowed in the name.
    fn test_name_with_whitespace_value() {
        match Name::new(" ") {
            Err(Error::InvalidSchemaName(_, _)) => {}
            _ => panic!("Expected an Error::InvalidSchemaName!"),
        }
    }

    #[test]
    /// The name must be non-empty.
    fn test_name_with_no_name_part() {
        match Name::new("space.") {
            Err(Error::InvalidSchemaName(_, _)) => {}
            _ => panic!("Expected an Error::InvalidSchemaName!"),
        }
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_record_inherited_namespace() -> TestResult {
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
            },
            {
                "name": "outer_field_2",
                "type" : "inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.inner_record_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_record_qualified_namespace() -> TestResult {
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
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.inner_record_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_enum_inherited_namespace() -> TestResult {
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
                            "type":"enum",
                            "name":"inner_enum_name",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_enum_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_enum_qualified_namespace() -> TestResult {
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
                            "type":"enum",
                            "name":"inner_enum_name",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_enum_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_fixed_inherited_namespace() -> TestResult {
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
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "size": 16
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_fixed_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_fixed_qualified_namespace() -> TestResult {
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
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "size": 16
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_fixed_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_record_inner_namespace() -> TestResult {
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
                            "type":"record",
                            "name":"inner_record_name",
                            "namespace":"inner_space",
                            "fields":[
                                {
                                    "name":"inner_field_1",
                                    "type":"double"
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "inner_space.inner_record_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_enum_inner_namespace() -> TestResult {
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
                            "type":"enum",
                            "name":"inner_enum_name",
                            "namespace": "inner_space",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_enum_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "inner_space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_fixed_inner_namespace() -> TestResult {
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
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "namespace": "inner_space",
                            "size": 16
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_fixed_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "inner_space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_multi_level_resolution_inner_record_outer_namespace() -> TestResult {
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
                            "type":"record",
                            "name":"middle_record_name",
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 3);
        for s in &[
            "space.record_name",
            "space.middle_record_name",
            "space.inner_record_name",
        ] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_multi_level_resolution_inner_record_middle_namespace() -> TestResult {
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
                            "type":"record",
                            "name":"middle_record_name",
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 3);
        for s in &[
            "space.record_name",
            "middle_namespace.middle_record_name",
            "middle_namespace.inner_record_name",
        ] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_multi_level_resolution_inner_record_inner_namespace() -> TestResult {
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
                            "type":"record",
                            "name":"middle_record_name",
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 3);
        for s in &[
            "space.record_name",
            "middle_namespace.middle_record_name",
            "inner_namespace.inner_record_name",
        ] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_in_array_resolution_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": {
                  "type":"array",
                  "items":{
                      "type":"record",
                      "name":"in_array_record",
                      "fields": [
                          {
                              "name":"array_record_field",
                              "type":"string"
                          }
                      ]
                  }
              }
            },
            {
                "name":"outer_field_2",
                "type":"in_array_record"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.in_array_record"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_in_map_resolution_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": {
                  "type":"map",
                  "values":{
                      "type":"record",
                      "name":"in_map_record",
                      "fields": [
                          {
                              "name":"map_record_field",
                              "type":"string"
                          }
                      ]
                  }
              }
            },
            {
                "name":"outer_field_2",
                "type":"in_map_record"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.in_map_record"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3466_test_to_json_inner_enum_inner_namespace() -> TestResult {
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
                            "type":"enum",
                            "name":"inner_enum_name",
                            "namespace": "inner_space",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_enum_name"
            }
        ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");

        // confirm we have expected 2 full-names
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "inner_space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        // convert Schema back to JSON string
        let schema_str = serde_json::to_string(&schema).expect("test failed");
        let _schema = Schema::parse_str(&schema_str).expect("test failed");
        assert_eq!(schema, _schema);

        Ok(())
    }

    #[test]
    fn avro_3466_test_to_json_inner_fixed_inner_namespace() -> TestResult {
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
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "namespace": "inner_space",
                            "size":54
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_fixed_name"
            }
        ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");

        // confirm we have expected 2 full-names
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "inner_space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        // convert Schema back to JSON string
        let schema_str = serde_json::to_string(&schema).expect("test failed");
        let _schema = Schema::parse_str(&schema_str).expect("test failed");
        assert_eq!(schema, _schema);

        Ok(())
    }

    fn assert_avro_3512_aliases(aliases: &Aliases) {
        match aliases {
            Some(aliases) => {
                assert_eq!(aliases.len(), 3);
                assert_eq!(aliases[0], Alias::new("space.b").unwrap());
                assert_eq!(aliases[1], Alias::new("x.y").unwrap());
                assert_eq!(aliases[2], Alias::new(".c").unwrap());
            }
            None => {
                panic!("'aliases' must be Some");
            }
        }
    }

    #[test]
    fn avro_3512_alias_with_null_namespace_record() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "a",
              "namespace": "space",
              "aliases": ["b", "x.y", ".c"],
              "fields" : [
                {"name": "time", "type": "long"}
              ]
            }
        "#,
        )?;

        if let Schema::Record(RecordSchema { ref aliases, .. }) = schema {
            assert_avro_3512_aliases(aliases);
        } else {
            panic!("The Schema should be a record: {schema:?}");
        }

        Ok(())
    }

    #[test]
    fn avro_3512_alias_with_null_namespace_enum() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type": "enum",
              "name": "a",
              "namespace": "space",
              "aliases": ["b", "x.y", ".c"],
              "symbols" : [
                "symbol1", "symbol2"
              ]
            }
        "#,
        )?;

        if let Schema::Enum(EnumSchema { ref aliases, .. }) = schema {
            assert_avro_3512_aliases(aliases);
        } else {
            panic!("The Schema should be an enum: {schema:?}");
        }

        Ok(())
    }

    #[test]
    fn avro_3512_alias_with_null_namespace_fixed() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type": "fixed",
              "name": "a",
              "namespace": "space",
              "aliases": ["b", "x.y", ".c"],
              "size" : 12
            }
        "#,
        )?;

        if let Schema::Fixed(FixedSchema { ref aliases, .. }) = schema {
            assert_avro_3512_aliases(aliases);
        } else {
            panic!("The Schema should be a fixed: {schema:?}");
        }

        Ok(())
    }

    #[test]
    fn avro_3518_serialize_aliases_record() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "a",
              "namespace": "space",
              "aliases": ["b", "x.y", ".c"],
              "fields" : [
                {
                    "name": "time",
                    "type": "long",
                    "doc": "The documentation is not serialized",
                    "default": 123,
                    "aliases": ["time1", "ns.time2"]
                }
              ]
            }
        "#,
        )?;

        let value = serde_json::to_value(&schema)?;
        let serialized = serde_json::to_string(&value)?;
        assert_eq!(
            r#"{"aliases":["space.b","x.y","c"],"fields":[{"aliases":["time1","ns.time2"],"default":123,"name":"time","type":"long"}],"name":"a","namespace":"space","type":"record"}"#,
            &serialized
        );
        assert_eq!(schema, Schema::parse_str(&serialized)?);

        Ok(())
    }

    #[test]
    fn avro_3518_serialize_aliases_enum() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type": "enum",
              "name": "a",
              "namespace": "space",
              "aliases": ["b", "x.y", ".c"],
              "symbols" : [
                "symbol1", "symbol2"
              ]
            }
        "#,
        )?;

        let value = serde_json::to_value(&schema)?;
        let serialized = serde_json::to_string(&value)?;
        assert_eq!(
            r#"{"aliases":["space.b","x.y","c"],"name":"a","namespace":"space","symbols":["symbol1","symbol2"],"type":"enum"}"#,
            &serialized
        );
        assert_eq!(schema, Schema::parse_str(&serialized)?);

        Ok(())
    }

    #[test]
    fn avro_3518_serialize_aliases_fixed() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type": "fixed",
              "name": "a",
              "namespace": "space",
              "aliases": ["b", "x.y", ".c"],
              "size" : 12
            }
        "#,
        )?;

        let value = serde_json::to_value(&schema)?;
        let serialized = serde_json::to_string(&value)?;
        assert_eq!(
            r#"{"aliases":["space.b","x.y","c"],"name":"a","namespace":"space","size":12,"type":"fixed"}"#,
            &serialized
        );
        assert_eq!(schema, Schema::parse_str(&serialized)?);

        Ok(())
    }

    #[test]
    fn avro_3130_parse_anonymous_union_type() -> TestResult {
        let schema_str = r#"
        {
            "type": "record",
            "name": "AccountEvent",
            "fields": [
                {"type":
                  ["null",
                   { "name": "accountList",
                      "type": {
                        "type": "array",
                        "items": "long"
                      }
                  }
                  ],
                 "name":"NullableLongArray"
               }
            ]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;

        if let Schema::Record(RecordSchema { name, fields, .. }) = schema {
            assert_eq!(name, Name::new("AccountEvent")?);

            let field = &fields[0];
            assert_eq!(&field.name, "NullableLongArray");

            if let Schema::Union(ref union) = field.schema {
                assert_eq!(union.schemas[0], Schema::Null);

                if let Schema::Array(ref array_schema) = union.schemas[1] {
                    if let Schema::Long = *array_schema.items {
                        // OK
                    } else {
                        panic!("Expected a Schema::Array of type Long");
                    }
                } else {
                    panic!("Expected Schema::Array");
                }
            } else {
                panic!("Expected Schema::Union");
            }
        } else {
            panic!("Expected Schema::Record");
        }

        Ok(())
    }

    #[test]
    fn avro_custom_attributes_schema_without_attributes() -> TestResult {
        let schemata_str = [
            r#"
            {
                "type": "record",
                "name": "Rec",
                "doc": "A Record schema without custom attributes",
                "fields": []
            }
            "#,
            r#"
            {
                "type": "enum",
                "name": "Enum",
                "doc": "An Enum schema without custom attributes",
                "symbols": []
            }
            "#,
            r#"
            {
                "type": "fixed",
                "name": "Fixed",
                "doc": "A Fixed schema without custom attributes",
                "size": 0
            }
            "#,
        ];
        for schema_str in schemata_str.iter() {
            let schema = Schema::parse_str(schema_str)?;
            assert_eq!(schema.custom_attributes(), Some(&Default::default()));
        }

        Ok(())
    }

    const CUSTOM_ATTRS_SUFFIX: &str = r#"
            "string_key": "value",
            "number_key": 1.23,
            "null_key": null,
            "array_key": [1, 2, 3],
            "object_key": {
                "key": "value"
            }
        "#;

    #[test]
    fn avro_3609_custom_attributes_schema_with_attributes() -> TestResult {
        let schemata_str = [
            r#"
            {
                "type": "record",
                "name": "Rec",
                "namespace": "ns",
                "doc": "A Record schema with custom attributes",
                "fields": [],
                {{{}}}
            }
            "#,
            r#"
            {
                "type": "enum",
                "name": "Enum",
                "namespace": "ns",
                "doc": "An Enum schema with custom attributes",
                "symbols": [],
                {{{}}}
            }
            "#,
            r#"
            {
                "type": "fixed",
                "name": "Fixed",
                "namespace": "ns",
                "doc": "A Fixed schema with custom attributes",
                "size": 2,
                {{{}}}
            }
            "#,
        ];

        for schema_str in schemata_str.iter() {
            let schema = Schema::parse_str(
                schema_str
                    .to_owned()
                    .replace("{{{}}}", CUSTOM_ATTRS_SUFFIX)
                    .as_str(),
            )?;

            assert_eq!(
                schema.custom_attributes(),
                Some(&expected_custom_attributes())
            );
        }

        Ok(())
    }

    fn expected_custom_attributes() -> BTreeMap<String, Value> {
        let mut expected_attributes: BTreeMap<String, Value> = Default::default();
        expected_attributes.insert("string_key".to_string(), Value::String("value".to_string()));
        expected_attributes.insert("number_key".to_string(), json!(1.23));
        expected_attributes.insert("null_key".to_string(), Value::Null);
        expected_attributes.insert(
            "array_key".to_string(),
            Value::Array(vec![json!(1), json!(2), json!(3)]),
        );
        let mut object_value: HashMap<String, Value> = HashMap::new();
        object_value.insert("key".to_string(), Value::String("value".to_string()));
        expected_attributes.insert("object_key".to_string(), json!(object_value));
        expected_attributes
    }

    #[test]
    fn avro_3609_custom_attributes_record_field_without_attributes() -> TestResult {
        let schema_str = String::from(
            r#"
            {
                "type": "record",
                "name": "Rec",
                "doc": "A Record schema without custom attributes",
                "fields": [
                    {
                        "name": "field_one",
                        "type": "float",
                        {{{}}}
                    }
                ]
            }
        "#,
        );

        let schema = Schema::parse_str(schema_str.replace("{{{}}}", CUSTOM_ATTRS_SUFFIX).as_str())?;

        match schema {
            Schema::Record(RecordSchema { name, fields, .. }) => {
                assert_eq!(name, Name::new("Rec")?);
                assert_eq!(fields.len(), 1);
                let field = &fields[0];
                assert_eq!(&field.name, "field_one");
                assert_eq!(field.custom_attributes, expected_custom_attributes());
            }
            _ => panic!("Expected Schema::Record"),
        }

        Ok(())
    }

    #[test]
    fn avro_3625_null_is_first() -> TestResult {
        let schema_str = String::from(
            r#"
            {
                "type": "record",
                "name": "union_schema_test",
                "fields": [
                    {"name": "a", "type": ["null", "long"], "default": null}
                ]
            }
        "#,
        );

        let schema = Schema::parse_str(&schema_str)?;

        match schema {
            Schema::Record(RecordSchema { name, fields, .. }) => {
                assert_eq!(name, Name::new("union_schema_test")?);
                assert_eq!(fields.len(), 1);
                let field = &fields[0];
                assert_eq!(&field.name, "a");
                assert_eq!(&field.default, &Some(Value::Null));
                match &field.schema {
                    Schema::Union(union) => {
                        assert_eq!(union.variants().len(), 2);
                        assert!(union.is_nullable());
                        assert_eq!(union.variants()[0], Schema::Null);
                        assert_eq!(union.variants()[1], Schema::Long);
                    }
                    _ => panic!("Expected Schema::Union"),
                }
            }
            _ => panic!("Expected Schema::Record"),
        }

        Ok(())
    }

    #[test]
    fn avro_3625_null_is_last() -> TestResult {
        let schema_str = String::from(
            r#"
            {
                "type": "record",
                "name": "union_schema_test",
                "fields": [
                    {"name": "a", "type": ["long","null"], "default": 123}
                ]
            }
        "#,
        );

        let schema = Schema::parse_str(&schema_str)?;

        match schema {
            Schema::Record(RecordSchema { name, fields, .. }) => {
                assert_eq!(name, Name::new("union_schema_test")?);
                assert_eq!(fields.len(), 1);
                let field = &fields[0];
                assert_eq!(&field.name, "a");
                assert_eq!(&field.default, &Some(json!(123)));
                match &field.schema {
                    Schema::Union(union) => {
                        assert_eq!(union.variants().len(), 2);
                        assert_eq!(union.variants()[0], Schema::Long);
                        assert_eq!(union.variants()[1], Schema::Null);
                    }
                    _ => panic!("Expected Schema::Union"),
                }
            }
            _ => panic!("Expected Schema::Record"),
        }

        Ok(())
    }

    #[test]
    fn avro_3625_null_is_the_middle() -> TestResult {
        let schema_str = String::from(
            r#"
            {
                "type": "record",
                "name": "union_schema_test",
                "fields": [
                    {"name": "a", "type": ["long","null","int"], "default": 123}
                ]
            }
        "#,
        );

        let schema = Schema::parse_str(&schema_str)?;

        match schema {
            Schema::Record(RecordSchema { name, fields, .. }) => {
                assert_eq!(name, Name::new("union_schema_test")?);
                assert_eq!(fields.len(), 1);
                let field = &fields[0];
                assert_eq!(&field.name, "a");
                assert_eq!(&field.default, &Some(json!(123)));
                match &field.schema {
                    Schema::Union(union) => {
                        assert_eq!(union.variants().len(), 3);
                        assert_eq!(union.variants()[0], Schema::Long);
                        assert_eq!(union.variants()[1], Schema::Null);
                        assert_eq!(union.variants()[2], Schema::Int);
                    }
                    _ => panic!("Expected Schema::Union"),
                }
            }
            _ => panic!("Expected Schema::Record"),
        }

        Ok(())
    }

    #[test]
    fn avro_3649_default_notintfirst() -> TestResult {
        let schema_str = String::from(
            r#"
            {
                "type": "record",
                "name": "union_schema_test",
                "fields": [
                    {"name": "a", "type": ["string", "int"], "default": 123}
                ]
            }
        "#,
        );

        let schema = Schema::parse_str(&schema_str)?;

        match schema {
            Schema::Record(RecordSchema { name, fields, .. }) => {
                assert_eq!(name, Name::new("union_schema_test")?);
                assert_eq!(fields.len(), 1);
                let field = &fields[0];
                assert_eq!(&field.name, "a");
                assert_eq!(&field.default, &Some(json!(123)));
                match &field.schema {
                    Schema::Union(union) => {
                        assert_eq!(union.variants().len(), 2);
                        assert_eq!(union.variants()[0], Schema::String);
                        assert_eq!(union.variants()[1], Schema::Int);
                    }
                    _ => panic!("Expected Schema::Union"),
                }
            }
            _ => panic!("Expected Schema::Record"),
        }

        Ok(())
    }

    #[test]
    fn avro_3709_parsing_of_record_field_aliases() -> TestResult {
        let schema = r#"
        {
          "name": "rec",
          "type": "record",
          "fields": [
            {
              "name": "num",
              "type": "int",
              "aliases": ["num1", "num2"]
            }
          ]
        }
        "#;

        let schema = Schema::parse_str(schema)?;
        if let Schema::Record(RecordSchema { fields, .. }) = schema {
            let num_field = &fields[0];
            assert_eq!(num_field.name, "num");
            assert_eq!(num_field.aliases, Some(vec!("num1".into(), "num2".into())));
        } else {
            panic!("Expected a record schema!");
        }

        Ok(())
    }

    #[test]
    fn avro_3735_parse_enum_namespace() -> TestResult {
        let schema = r#"
        {
            "type": "record",
            "name": "Foo",
            "namespace": "name.space",
            "fields":
            [
                {
                    "name": "barInit",
                    "type":
                    {
                        "type": "enum",
                        "name": "Bar",
                        "symbols":
                        [
                            "bar0",
                            "bar1"
                        ]
                    }
                },
                {
                    "name": "barUse",
                    "type": "Bar"
                }
            ]
        } 
        "#;

        #[derive(
            Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, serde::Deserialize, serde::Serialize,
        )]
        pub enum Bar {
            #[serde(rename = "bar0")]
            Bar0,
            #[serde(rename = "bar1")]
            Bar1,
        }

        #[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
        pub struct Foo {
            #[serde(rename = "barInit")]
            pub bar_init: Bar,
            #[serde(rename = "barUse")]
            pub bar_use: Bar,
        }

        let schema = Schema::parse_str(schema)?;

        let foo = Foo {
            bar_init: Bar::Bar0,
            bar_use: Bar::Bar1,
        };

        let avro_value = crate::to_value(foo)?;
        assert!(avro_value.validate(&schema));

        let mut writer = crate::Writer::new(&schema, Vec::new());

        // schema validation happens here
        writer.append(avro_value)?;

        Ok(())
    }

    #[test]
    fn avro_3755_deserialize() -> TestResult {
        #[derive(
            Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, serde::Deserialize, serde::Serialize,
        )]
        pub enum Bar {
            #[serde(rename = "bar0")]
            Bar0,
            #[serde(rename = "bar1")]
            Bar1,
            #[serde(rename = "bar2")]
            Bar2,
        }

        #[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
        pub struct Foo {
            #[serde(rename = "barInit")]
            pub bar_init: Bar,
            #[serde(rename = "barUse")]
            pub bar_use: Bar,
        }

        let writer_schema = r#"{
            "type": "record",
            "name": "Foo",
            "fields":
            [
                {
                    "name": "barInit",
                    "type":
                    {
                        "type": "enum",
                        "name": "Bar",
                        "symbols":
                        [
                            "bar0",
                            "bar1"
                        ]
                    }
                },
                {
                    "name": "barUse",
                    "type": "Bar"
                }
            ]
            }"#;

        let reader_schema = r#"{
            "type": "record",
            "name": "Foo",
            "namespace": "name.space",
            "fields":
            [
                {
                    "name": "barInit",
                    "type":
                    {
                        "type": "enum",
                        "name": "Bar",
                        "symbols":
                        [
                            "bar0",
                            "bar1",
                            "bar2"
                        ]
                    }
                },
                {
                    "name": "barUse",
                    "type": "Bar"
                }
            ]
            }"#;

        let writer_schema = Schema::parse_str(writer_schema)?;
        let foo = Foo {
            bar_init: Bar::Bar0,
            bar_use: Bar::Bar1,
        };
        let avro_value = crate::to_value(foo)?;
        assert!(
            avro_value.validate(&writer_schema),
            "value is valid for schema",
        );
        let datum = crate::to_avro_datum(&writer_schema, avro_value)?;
        let mut x = &datum[..];
        let reader_schema = Schema::parse_str(reader_schema)?;
        let deser_value = crate::from_avro_datum(&writer_schema, &mut x, Some(&reader_schema))?;
        match deser_value {
            types::Value::Record(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "barInit");
                assert_eq!(fields[0].1, types::Value::Enum(0, "bar0".to_string()));
                assert_eq!(fields[1].0, "barUse");
                assert_eq!(fields[1].1, types::Value::Enum(1, "bar1".to_string()));
            }
            _ => panic!("Expected Value::Record"),
        }

        Ok(())
    }

    #[test]
    fn test_avro_3780_decimal_schema_type_with_fixed() -> TestResult {
        let schema = json!(
        {
          "type": "record",
          "name": "recordWithDecimal",
          "fields": [
            {
                "name": "decimal",
                "type": "fixed",
                "name": "nestedFixed",
                "size": 8,
                "logicalType": "decimal",
                "precision": 4
            }
          ]
        });

        let parse_result = Schema::parse(&schema);
        assert!(
            parse_result.is_ok(),
            "parse result must be ok, got: {:?}",
            parse_result
        );

        Ok(())
    }

    #[test]
    fn test_avro_3772_enum_default_wrong_type() -> TestResult {
        let schema = r#"
        {
          "type": "record",
          "name": "test",
          "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"},
            {
              "name": "c",
              "type": {
                "type": "enum",
                "name": "suit",
                "symbols": ["diamonds", "spades", "clubs", "hearts"],
                "default": 123
              }
            }
          ]
        }
        "#;

        match Schema::parse_str(schema) {
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    "Default value for enum must be a string! Got: 123"
                );
            }
            _ => panic!("Expected an error"),
        }
        Ok(())
    }

    #[test]
    fn test_avro_3812_handle_null_namespace_properly() -> TestResult {
        let schema_str = r#"
        {
          "namespace": "",
          "type": "record",
          "name": "my_schema",
          "fields": [
            {
              "name": "a",
              "type": {
                "type": "enum",
                "name": "my_enum",
                "namespace": "",
                "symbols": ["a", "b"]
              }
            },  {
              "name": "b",
              "type": {
                "type": "fixed",
                "name": "my_fixed",
                "namespace": "",
                "size": 10
              }
            }
          ]
         }
         "#;

        let expected = r#"{"name":"my_schema","type":"record","fields":[{"name":"a","type":{"name":"my_enum","type":"enum","symbols":["a","b"]}},{"name":"b","type":{"name":"my_fixed","type":"fixed","size":10}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        let name = Name::new("my_name")?;
        let fullname = name.fullname(Some("".to_string()));
        assert_eq!(fullname, "my_name");
        let qname = name.fully_qualified_name(&Some("".to_string())).to_string();
        assert_eq!(qname, "my_name");

        Ok(())
    }

    #[test]
    fn test_avro_3818_inherit_enclosing_namespace() -> TestResult {
        // Enclosing namespace is specified but inner namespaces are not.
        let schema_str = r#"
        {
          "namespace": "my_ns",
          "type": "record",
          "name": "my_schema",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": "enum1",
                "type": "enum",
                "symbols": ["a"]
              }
            },  {
              "name": "f2",
              "type": {
                "name": "fixed1",
                "type": "fixed",
                "size": 1
              }
            }
          ]
        }
        "#;

        let expected = r#"{"name":"my_ns.my_schema","type":"record","fields":[{"name":"f1","type":{"name":"my_ns.enum1","type":"enum","symbols":["a"]}},{"name":"f2","type":{"name":"my_ns.fixed1","type":"fixed","size":1}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        // Enclosing namespace and inner namespaces are specified
        // but inner namespaces are ""
        let schema_str = r#"
        {
          "namespace": "my_ns",
          "type": "record",
          "name": "my_schema",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": "enum1",
                "type": "enum",
                "namespace": "",
                "symbols": ["a"]
              }
            },  {
              "name": "f2",
              "type": {
                "name": "fixed1",
                "type": "fixed",
                "namespace": "",
                "size": 1
              }
            }
          ]
        }
        "#;

        let expected = r#"{"name":"my_ns.my_schema","type":"record","fields":[{"name":"f1","type":{"name":"enum1","type":"enum","symbols":["a"]}},{"name":"f2","type":{"name":"fixed1","type":"fixed","size":1}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        // Enclosing namespace is "" and inner non-empty namespaces are specified.
        let schema_str = r#"
        {
          "namespace": "",
          "type": "record",
          "name": "my_schema",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": "enum1",
                "type": "enum",
                "namespace": "f1.ns",
                "symbols": ["a"]
              }
            },  {
              "name": "f2",
              "type": {
                "name": "f2.ns.fixed1",
                "type": "fixed",
                "size": 1
              }
            }
          ]
        }
        "#;

        let expected = r#"{"name":"my_schema","type":"record","fields":[{"name":"f1","type":{"name":"f1.ns.enum1","type":"enum","symbols":["a"]}},{"name":"f2","type":{"name":"f2.ns.fixed1","type":"fixed","size":1}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        // Nested complex types with non-empty enclosing namespace.
        let schema_str = r#"
        {
          "type": "record",
          "name": "my_ns.my_schema",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": "inner_record1",
                "type": "record",
                "fields": [
                  {
                    "name": "f1_1",
                    "type": {
                      "name": "enum1",
                      "type": "enum",
                      "symbols": ["a"]
                    }
                  }
                ]
              }
            },  {
              "name": "f2",
                "type": {
                "name": "inner_record2",
                "type": "record",
                "namespace": "inner_ns",
                "fields": [
                  {
                    "name": "f2_1",
                    "type": {
                      "name": "enum2",
                      "type": "enum",
                      "symbols": ["a"]
                    }
                  }
                ]
              }
            }
          ]
        }
        "#;

        let expected = r#"{"name":"my_ns.my_schema","type":"record","fields":[{"name":"f1","type":{"name":"my_ns.inner_record1","type":"record","fields":[{"name":"f1_1","type":{"name":"my_ns.enum1","type":"enum","symbols":["a"]}}]}},{"name":"f2","type":{"name":"inner_ns.inner_record2","type":"record","fields":[{"name":"f2_1","type":{"name":"inner_ns.enum2","type":"enum","symbols":["a"]}}]}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        Ok(())
    }

    #[test]
    fn test_avro_3779_bigdecimal_schema() -> TestResult {
        let schema = json!(
            {
                "name": "decimal",
                "type": "bytes",
                "logicalType": "big-decimal"
            }
        );

        let parse_result = Schema::parse(&schema);
        assert!(
            parse_result.is_ok(),
            "parse result must be ok, got: {:?}",
            parse_result
        );
        match parse_result? {
            Schema::BigDecimal => (),
            other => panic!("Expected Schema::BigDecimal but got: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_avro_3820_deny_invalid_field_names() -> TestResult {
        let schema_str = r#"
        {
          "name": "my_record",
          "type": "record",
          "fields": [
            {
              "name": "f1.x",
              "type": {
                "name": "my_enum",
                "type": "enum",
                "symbols": ["a"]
              }
            },  {
              "name": "f2",
              "type": {
                "name": "my_fixed",
                "type": "fixed",
                "size": 1
              }
            }
          ]
        }
        "#;

        match Schema::parse_str(schema_str) {
            Err(Error::FieldName(x)) if x == "f1.x" => Ok(()),
            other => Err(format!("Expected Error::FieldName, got {other:?}").into()),
        }
    }

    #[test]
    fn test_avro_3827_disallow_duplicate_field_names() -> TestResult {
        let schema_str = r#"
        {
          "name": "my_schema",
          "type": "record",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": "a",
                "type": "record",
                "fields": []
              }
            },  {
              "name": "f1",
              "type": {
                "name": "b",
                "type": "record",
                "fields": []
              }
            }
          ]
        }
        "#;

        match Schema::parse_str(schema_str) {
            Err(Error::FieldNameDuplicate(_)) => (),
            other => {
                return Err(format!("Expected Error::FieldNameDuplicate, got {other:?}").into());
            }
        };

        let schema_str = r#"
        {
          "name": "my_schema",
          "type": "record",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": "a",
                "type": "record",
                "fields": [
                  {
                    "name": "f1",
                    "type": {
                      "name": "b",
                      "type": "record",
                      "fields": []
                    }
                  }
                ]
              }
            }
          ]
        }
        "#;

        let expected = r#"{"name":"my_schema","type":"record","fields":[{"name":"f1","type":{"name":"a","type":"record","fields":[{"name":"f1","type":{"name":"b","type":"record","fields":[]}}]}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        Ok(())
    }

    #[test]
    fn test_avro_3830_null_namespace_in_fully_qualified_names() -> TestResult {
        // Check whether all the named types don't refer to the namespace field
        // if their name starts with a dot.
        let schema_str = r#"
        {
          "name": ".record1",
          "namespace": "ns1",
          "type": "record",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": ".enum1",
                "namespace": "ns2",
                "type": "enum",
                "symbols": ["a"]
              }
            },  {
              "name": "f2",
              "type": {
                "name": ".fxed1",
                "namespace": "ns3",
                "type": "fixed",
                "size": 1
              }
            }
          ]
        }
        "#;

        let expected = r#"{"name":"record1","type":"record","fields":[{"name":"f1","type":{"name":"enum1","type":"enum","symbols":["a"]}},{"name":"f2","type":{"name":"fxed1","type":"fixed","size":1}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        // Check whether inner types don't inherit ns1.
        let schema_str = r#"
        {
          "name": ".record1",
          "namespace": "ns1",
          "type": "record",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": "enum1",
                "type": "enum",
                "symbols": ["a"]
              }
            },  {
              "name": "f2",
              "type": {
                "name": "fxed1",
                "type": "fixed",
                "size": 1
              }
            }
          ]
        }
        "#;

        let expected = r#"{"name":"record1","type":"record","fields":[{"name":"f1","type":{"name":"enum1","type":"enum","symbols":["a"]}},{"name":"f2","type":{"name":"fxed1","type":"fixed","size":1}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        let name = Name::new(".my_name")?;
        let fullname = name.fullname(None);
        assert_eq!(fullname, "my_name");
        let qname = name.fully_qualified_name(&None).to_string();
        assert_eq!(qname, "my_name");

        Ok(())
    }

    #[test]
    fn test_avro_3814_schema_resolution_failure() -> TestResult {
        // Define a reader schema: a nested record with an optional field.
        let reader_schema = json!(
            {
                "type": "record",
                "name": "MyOuterRecord",
                "fields": [
                    {
                        "name": "inner_record",
                        "type": [
                            "null",
                            {
                                "type": "record",
                                "name": "MyRecord",
                                "fields": [
                                    {"name": "a", "type": "string"}
                                ]
                            }
                        ],
                        "default": null
                    }
                ]
            }
        );

        // Define a writer schema: a nested record with an optional field, which
        // may optionally contain an enum.
        let writer_schema = json!(
            {
                "type": "record",
                "name": "MyOuterRecord",
                "fields": [
                    {
                        "name": "inner_record",
                        "type": [
                            "null",
                            {
                                "type": "record",
                                "name": "MyRecord",
                                "fields": [
                                    {"name": "a", "type": "string"},
                                    {
                                        "name": "b",
                                        "type": [
                                            "null",
                                            {
                                                "type": "enum",
                                                "name": "MyEnum",
                                                "symbols": ["A", "B", "C"],
                                                "default": "C"
                                            }
                                        ],
                                        "default": null
                                    },
                                ]
                            }
                        ]
                    }
                ],
                "default": null
            }
        );

        // Use different structs to represent the "Reader" and the "Writer"
        // to mimic two different versions of a producer & consumer application.
        #[derive(Serialize, Deserialize, Debug)]
        struct MyInnerRecordReader {
            a: String,
        }

        #[derive(Serialize, Deserialize, Debug)]
        struct MyRecordReader {
            inner_record: Option<MyInnerRecordReader>,
        }

        #[derive(Serialize, Deserialize, Debug)]
        enum MyEnum {
            A,
            B,
            C,
        }

        #[derive(Serialize, Deserialize, Debug)]
        struct MyInnerRecordWriter {
            a: String,
            b: Option<MyEnum>,
        }

        #[derive(Serialize, Deserialize, Debug)]
        struct MyRecordWriter {
            inner_record: Option<MyInnerRecordWriter>,
        }

        let s = MyRecordWriter {
            inner_record: Some(MyInnerRecordWriter {
                a: "foo".to_string(),
                b: None,
            }),
        };

        // Serialize using the writer schema.
        let writer_schema = Schema::parse(&writer_schema)?;
        let avro_value = crate::to_value(s)?;
        assert!(
            avro_value.validate(&writer_schema),
            "value is valid for schema",
        );
        let datum = crate::to_avro_datum(&writer_schema, avro_value)?;

        // Now, attempt to deserialize using the reader schema.
        let reader_schema = Schema::parse(&reader_schema)?;
        let mut x = &datum[..];

        // Deserialization should succeed and we should be able to resolve the schema.
        let deser_value = crate::from_avro_datum(&writer_schema, &mut x, Some(&reader_schema))?;
        assert!(deser_value.validate(&reader_schema));

        // Verify that we can read a field from the record.
        let d: MyRecordReader = crate::from_value(&deser_value)?;
        assert_eq!(d.inner_record.unwrap().a, "foo".to_string());
        Ok(())
    }

    #[test]
    fn test_avro_3837_disallow_invalid_namespace() -> TestResult {
        // Valid namespace #1 (Single name portion)
        let schema_str = r#"
        {
          "name": "record1",
          "namespace": "ns1",
          "type": "record",
          "fields": []
        }
        "#;

        let expected = r#"{"name":"ns1.record1","type":"record","fields":[]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        // Valid namespace #2 (multiple name portions).
        let schema_str = r#"
        {
          "name": "enum1",
          "namespace": "ns1.foo.bar",
          "type": "enum",
          "symbols": ["a"]
        }
        "#;

        let expected = r#"{"name":"ns1.foo.bar.enum1","type":"enum","symbols":["a"]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        // Invalid namespace #1 (a name portion starts with dot)
        let schema_str = r#"
        {
          "name": "fixed1",
          "namespace": ".ns1.a.b",
          "type": "fixed",
          "size": 1
        }
        "#;

        match Schema::parse_str(schema_str) {
            Err(Error::InvalidNamespace(_, _)) => (),
            other => return Err(format!("Expected Error::InvalidNamespace, got {other:?}").into()),
        };

        // Invalid namespace #2 (invalid character in a name portion)
        let schema_str = r#"
        {
          "name": "record1",
          "namespace": "ns1.a*b.c",
          "type": "record",
          "fields": []
        }
        "#;

        match Schema::parse_str(schema_str) {
            Err(Error::InvalidNamespace(_, _)) => (),
            other => return Err(format!("Expected Error::InvalidNamespace, got {other:?}").into()),
        };

        // Invalid namespace #3 (a name portion starts with a digit)
        let schema_str = r#"
        {
          "name": "fixed1",
          "namespace": "ns1.1a.b",
          "type": "fixed",
          "size": 1
        }
        "#;

        match Schema::parse_str(schema_str) {
            Err(Error::InvalidNamespace(_, _)) => (),
            other => return Err(format!("Expected Error::InvalidNamespace, got {other:?}").into()),
        };

        // Invalid namespace #4 (a name portion is missing - two dots in a row)
        let schema_str = r#"
        {
          "name": "fixed1",
          "namespace": "ns1..a",
          "type": "fixed",
          "size": 1
        }
        "#;

        match Schema::parse_str(schema_str) {
            Err(Error::InvalidNamespace(_, _)) => (),
            other => return Err(format!("Expected Error::InvalidNamespace, got {other:?}").into()),
        };

        // Invalid namespace #5 (a name portion is missing - ends with a dot)
        let schema_str = r#"
        {
          "name": "fixed1",
          "namespace": "ns1.a.",
          "type": "fixed",
          "size": 1
        }
        "#;

        match Schema::parse_str(schema_str) {
            Err(Error::InvalidNamespace(_, _)) => (),
            other => return Err(format!("Expected Error::InvalidNamespace, got {other:?}").into()),
        };

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_simple_record_field() -> TestResult {
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": "int",
                    "default": "invalid"
                }
            ]
        }
        "#;
        let expected = Error::GetDefaultRecordField(
            "f1".to_string(),
            "ns.record1".to_string(),
            r#""int""#.to_string(),
        )
        .to_string();
        let result = Schema::parse_str(schema_str);
        assert!(result.is_err());
        let err = result
            .map_err(|e| e.to_string())
            .err()
            .unwrap_or_else(|| "unexpected".to_string());
        assert_eq!(expected, err);

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_nested_record_field() -> TestResult {
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": {
                        "name": "record2",
                        "type": "record",
                        "fields": [
                            {
                                "name": "f1_1",
                                "type": "int"
                            }
                        ]
                    },
                    "default": "invalid"
                }
            ]
        }
        "#;
        let expected = Error::GetDefaultRecordField(
            "f1".to_string(),
            "ns.record1".to_string(),
            r#"{"name":"ns.record2","type":"record","fields":[{"name":"f1_1","type":"int"}]}"#
                .to_string(),
        )
        .to_string();
        let result = Schema::parse_str(schema_str);
        assert!(result.is_err());
        let err = result
            .map_err(|e| e.to_string())
            .err()
            .unwrap_or_else(|| "unexpected".to_string());
        assert_eq!(expected, err);

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_enum_record_field() -> TestResult {
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": {
                        "name": "enum1",
                        "type": "enum",
                        "symbols": ["a", "b", "c"]
                    },
                    "default": "invalid"
                }
            ]
        }
        "#;
        let expected = Error::GetDefaultRecordField(
            "f1".to_string(),
            "ns.record1".to_string(),
            r#"{"name":"ns.enum1","type":"enum","symbols":["a","b","c"]}"#.to_string(),
        )
        .to_string();
        let result = Schema::parse_str(schema_str);
        assert!(result.is_err());
        let err = result
            .map_err(|e| e.to_string())
            .err()
            .unwrap_or_else(|| "unexpected".to_string());
        assert_eq!(expected, err);

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_fixed_record_field() -> TestResult {
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": {
                        "name": "fixed1",
                        "type": "fixed",
                        "size": 3
                    },
                    "default": 100
                }
            ]
        }
        "#;
        let expected = Error::GetDefaultRecordField(
            "f1".to_string(),
            "ns.record1".to_string(),
            r#"{"name":"ns.fixed1","type":"fixed","size":3}"#.to_string(),
        )
        .to_string();
        let result = Schema::parse_str(schema_str);
        assert!(result.is_err());
        let err = result
            .map_err(|e| e.to_string())
            .err()
            .unwrap_or_else(|| "unexpected".to_string());
        assert_eq!(expected, err);

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_array_record_field() -> TestResult {
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": "array",
                    "items": "int",
                    "default": "invalid"
                }
            ]
        }
        "#;
        let expected = Error::GetDefaultRecordField(
            "f1".to_string(),
            "ns.record1".to_string(),
            r#"{"type":"array","items":"int"}"#.to_string(),
        )
        .to_string();
        let result = Schema::parse_str(schema_str);
        assert!(result.is_err());
        let err = result
            .map_err(|e| e.to_string())
            .err()
            .unwrap_or_else(|| "unexpected".to_string());
        assert_eq!(expected, err);

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_map_record_field() -> TestResult {
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": "map",
                    "values": "string",
                    "default": "invalid"
                }
            ]
        }
        "#;
        let expected = Error::GetDefaultRecordField(
            "f1".to_string(),
            "ns.record1".to_string(),
            r#"{"type":"map","values":"string"}"#.to_string(),
        )
        .to_string();
        let result = Schema::parse_str(schema_str);
        assert!(result.is_err());
        let err = result
            .map_err(|e| e.to_string())
            .err()
            .unwrap_or_else(|| "unexpected".to_string());
        assert_eq!(expected, err);

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_ref_record_field() -> TestResult {
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": {
                        "name": "record2",
                        "type": "record",
                        "fields": [
                            {
                                "name": "f1_1",
                                "type": "int"
                            }
                        ]
                    }
                },  {
                    "name": "f2",
                    "type": "ns.record2",
                    "default": { "f1_1": true }
                }
            ]
        }
        "#;
        let expected = Error::GetDefaultRecordField(
            "f2".to_string(),
            "ns.record1".to_string(),
            r#""ns.record2""#.to_string(),
        )
        .to_string();
        let result = Schema::parse_str(schema_str);
        assert!(result.is_err());
        let err = result
            .map_err(|e| e.to_string())
            .err()
            .unwrap_or_else(|| "unexpected".to_string());
        assert_eq!(expected, err);

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_enum() -> TestResult {
        let schema_str = r#"
        {
            "name": "enum1",
            "namespace": "ns",
            "type": "enum",
            "symbols": ["a", "b", "c"],
            "default": 100
        }
        "#;
        let expected = Error::EnumDefaultWrongType(100.into()).to_string();
        let result = Schema::parse_str(schema_str);
        assert!(result.is_err());
        let err = result
            .map_err(|e| e.to_string())
            .err()
            .unwrap_or_else(|| "unexpected".to_string());
        assert_eq!(expected, err);

        let schema_str = r#"
        {
            "name": "enum1",
            "namespace": "ns",
            "type": "enum",
            "symbols": ["a", "b", "c"],
            "default": "d"
        }
        "#;
        let expected = Error::GetEnumDefault {
            symbol: "d".to_string(),
            symbols: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        }
        .to_string();
        let result = Schema::parse_str(schema_str);
        assert!(result.is_err());
        let err = result
            .map_err(|e| e.to_string())
            .err()
            .unwrap_or_else(|| "unexpected".to_string());
        assert_eq!(expected, err);

        Ok(())
    }

    #[test]
    fn test_avro_3862_get_aliases() -> TestResult {
        // Test for Record
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns1",
            "type": "record",
            "aliases": ["r1", "ns2.r2"],
            "fields": [
                { "name": "f1", "type": "int" },
                { "name": "f2", "type": "string" }
            ]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        let expected = vec![Alias::new("ns1.r1")?, Alias::new("ns2.r2")?];
        match schema.aliases() {
            Some(aliases) => assert_eq!(aliases, &expected),
            None => panic!("Expected Some({:?}), got None", expected),
        }

        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns1",
            "type": "record",
            "fields": [
                { "name": "f1", "type": "int" },
                { "name": "f2", "type": "string" }
            ]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        match schema.aliases() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        // Test for Enum
        let schema_str = r#"
        {
            "name": "enum1",
            "namespace": "ns1",
            "type": "enum",
            "aliases": ["en1", "ns2.en2"],
            "symbols": ["a", "b", "c"]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        let expected = vec![Alias::new("ns1.en1")?, Alias::new("ns2.en2")?];
        match schema.aliases() {
            Some(aliases) => assert_eq!(aliases, &expected),
            None => panic!("Expected Some({:?}), got None", expected),
        }

        let schema_str = r#"
        {
            "name": "enum1",
            "namespace": "ns1",
            "type": "enum",
            "symbols": ["a", "b", "c"]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        match schema.aliases() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        // Test for Fixed
        let schema_str = r#"
        {
            "name": "fixed1",
            "namespace": "ns1",
            "type": "fixed",
            "aliases": ["fx1", "ns2.fx2"],
            "size": 10
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        let expected = vec![Alias::new("ns1.fx1")?, Alias::new("ns2.fx2")?];
        match schema.aliases() {
            Some(aliases) => assert_eq!(aliases, &expected),
            None => panic!("Expected Some({:?}), got None", expected),
        }

        let schema_str = r#"
        {
            "name": "fixed1",
            "namespace": "ns1",
            "type": "fixed",
            "size": 10
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        match schema.aliases() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        // Test for non-named type
        let schema = Schema::Int;
        match schema.aliases() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_avro_3862_get_doc() -> TestResult {
        // Test for Record
        let schema_str = r#"
        {
            "name": "record1",
            "type": "record",
            "doc": "Record Document",
            "fields": [
                { "name": "f1", "type": "int" },
                { "name": "f2", "type": "string" }
            ]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        let expected = "Record Document";
        match schema.doc() {
            Some(doc) => assert_eq!(doc, expected),
            None => panic!("Expected Some({:?}), got None", expected),
        }

        let schema_str = r#"
        {
            "name": "record1",
            "type": "record",
            "fields": [
                { "name": "f1", "type": "int" },
                { "name": "f2", "type": "string" }
            ]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        match schema.doc() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        // Test for Enum
        let schema_str = r#"
        {
            "name": "enum1",
            "type": "enum",
            "doc": "Enum Document",
            "symbols": ["a", "b", "c"]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        let expected = "Enum Document";
        match schema.doc() {
            Some(doc) => assert_eq!(doc, expected),
            None => panic!("Expected Some({:?}), got None", expected),
        }

        let schema_str = r#"
        {
            "name": "enum1",
            "type": "enum",
            "symbols": ["a", "b", "c"]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        match schema.doc() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        // Test for Fixed
        let schema_str = r#"
        {
            "name": "fixed1",
            "type": "fixed",
            "doc": "Fixed Document",
            "size": 10
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        let expected = "Fixed Document";
        match schema.doc() {
            Some(doc) => assert_eq!(doc, expected),
            None => panic!("Expected Some({:?}), got None", expected),
        }

        let schema_str = r#"
        {
            "name": "fixed1",
            "type": "fixed",
            "size": 10
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        match schema.doc() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        // Test for non-named type
        let schema = Schema::Int;
        match schema.doc() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        Ok(())
    }

    #[test]
    fn avro_3886_serialize_attributes() -> TestResult {
        let attributes = BTreeMap::from([
            ("string_key".into(), "value".into()),
            ("number_key".into(), 1.23.into()),
            ("null_key".into(), Value::Null),
            (
                "array_key".into(),
                Value::Array(vec![1.into(), 2.into(), 3.into()]),
            ),
            ("object_key".into(), Value::Object(Map::default())),
        ]);

        // Test serialize enum attributes
        let schema = Schema::Enum(EnumSchema {
            name: Name::new("a")?,
            aliases: None,
            doc: None,
            symbols: vec![],
            default: None,
            attributes: attributes.clone(),
        });
        let serialized = serde_json::to_string(&schema)?;
        assert_eq!(
            r#"{"type":"enum","name":"a","symbols":[],"array_key":[1,2,3],"null_key":null,"number_key":1.23,"object_key":{},"string_key":"value"}"#,
            &serialized
        );

        // Test serialize fixed custom_attributes
        let schema = Schema::Fixed(FixedSchema {
            name: Name::new("a")?,
            aliases: None,
            doc: None,
            size: 1,
            default: None,
            attributes: attributes.clone(),
        });
        let serialized = serde_json::to_string(&schema)?;
        assert_eq!(
            r#"{"type":"fixed","name":"a","size":1,"array_key":[1,2,3],"null_key":null,"number_key":1.23,"object_key":{},"string_key":"value"}"#,
            &serialized
        );

        // Test serialize record custom_attributes
        let schema = Schema::Record(RecordSchema {
            name: Name::new("a")?,
            aliases: None,
            doc: None,
            fields: vec![],
            lookup: BTreeMap::new(),
            attributes,
        });
        let serialized = serde_json::to_string(&schema)?;
        assert_eq!(
            r#"{"type":"record","name":"a","fields":[],"array_key":[1,2,3],"null_key":null,"number_key":1.23,"object_key":{},"string_key":"value"}"#,
            &serialized
        );

        Ok(())
    }

    /// A test cases showing that names and namespaces can be constructed
    /// entirely by underscores.
    #[test]
    fn test_avro_3897_funny_valid_names_and_namespaces() -> TestResult {
        for funny_name in ["_", "_._", "__._", "_.__", "_._._"] {
            let name = Name::new(funny_name);
            assert!(name.is_ok());
        }
        Ok(())
    }

    #[test]
    fn test_avro_3896_decimal_schema() -> TestResult {
        // bytes decimal, represented as native logical type.
        let schema = json!(
        {
          "type": "bytes",
          "name": "BytesDecimal",
          "logicalType": "decimal",
          "size": 38,
          "precision": 9,
          "scale": 2
        });
        let parse_result = Schema::parse(&schema)?;
        assert!(matches!(
            parse_result,
            Schema::Decimal(DecimalSchema {
                precision: 9,
                scale: 2,
                ..
            })
        ));

        // long decimal, represents as native complex type.
        let schema = json!(
        {
          "type": "long",
          "name": "LongDecimal",
          "logicalType": "decimal"
        });
        let parse_result = Schema::parse(&schema)?;
        // assert!(matches!(parse_result, Schema::Long));
        assert_eq!(parse_result, Schema::Long);

        Ok(())
    }

    #[test]
    fn avro_3896_uuid_schema_for_string() -> TestResult {
        // string uuid, represents as native logical type.
        let schema = json!(
        {
          "type": "string",
          "name": "StringUUID",
          "logicalType": "uuid"
        });
        let parse_result = Schema::parse(&schema)?;
        assert_eq!(parse_result, Schema::Uuid);

        Ok(())
    }

    #[test]
    fn avro_3926_uuid_schema_for_fixed_with_size_16() -> TestResult {
        let schema = json!(
        {
            "type": "fixed",
            "name": "FixedUUID",
            "size": 16,
            "logicalType": "uuid"
        });
        let parse_result = Schema::parse(&schema)?;
        assert_eq!(parse_result, Schema::Uuid);
        assert_not_logged(
            r#"Ignoring uuid logical type for a Fixed schema because its size (6) is not 16! Schema: Fixed(FixedSchema { name: Name { name: "FixedUUID", namespace: None }, aliases: None, doc: None, size: 6, attributes: {"logicalType": String("uuid")} })"#,
        );

        Ok(())
    }

    #[test]
    fn avro_3926_uuid_schema_for_fixed_with_size_different_than_16() -> TestResult {
        let schema = json!(
        {
            "type": "fixed",
            "name": "FixedUUID",
            "size": 6,
            "logicalType": "uuid"
        });
        let parse_result = Schema::parse(&schema)?;

        assert_eq!(
            parse_result,
            Schema::Fixed(FixedSchema {
                name: Name::new("FixedUUID")?,
                aliases: None,
                doc: None,
                size: 6,
                default: None,
                attributes: BTreeMap::from([("logicalType".to_string(), "uuid".into())]),
            })
        );
        assert_logged(
            r#"Ignoring uuid logical type for a Fixed schema because its size (6) is not 16! Schema: Fixed(FixedSchema { name: Name { name: "FixedUUID", namespace: None }, aliases: None, doc: None, size: 6, default: None, attributes: {"logicalType": String("uuid")} })"#,
        );

        Ok(())
    }

    #[test]
    fn test_avro_3896_timestamp_millis_schema() -> TestResult {
        // long timestamp-millis, represents as native logical type.
        let schema = json!(
        {
          "type": "long",
          "name": "LongTimestampMillis",
          "logicalType": "timestamp-millis"
        });
        let parse_result = Schema::parse(&schema)?;
        assert_eq!(parse_result, Schema::TimestampMillis);

        // int timestamp-millis, represents as native complex type.
        let schema = json!(
        {
            "type": "int",
            "name": "IntTimestampMillis",
            "logicalType": "timestamp-millis"
        });
        let parse_result = Schema::parse(&schema)?;
        assert_eq!(parse_result, Schema::Int);

        Ok(())
    }

    #[test]
    fn test_avro_3896_custom_bytes_schema() -> TestResult {
        // log type, represents as complex type.
        let schema = json!(
        {
            "type": "bytes",
            "name": "BytesLog",
            "logicalType": "custom"
        });
        let parse_result = Schema::parse(&schema)?;
        assert_eq!(parse_result, Schema::Bytes);
        assert_eq!(parse_result.custom_attributes(), None);

        Ok(())
    }

    #[test]
    fn test_avro_3899_parse_decimal_type() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
             "name": "InvalidDecimal",
             "type": "fixed",
             "size": 16,
             "logicalType": "decimal",
             "precision": 2,
             "scale": 3
         }"#,
        )?;
        match schema {
            Schema::Fixed(fixed_schema) => {
                let attrs = fixed_schema.attributes;
                let precision = attrs
                    .get("precision")
                    .expect("The 'precision' attribute is missing");
                let scale = attrs
                    .get("scale")
                    .expect("The 'scale' attribute is missing");
                assert_logged(&format!("Ignoring invalid decimal logical type: The decimal precision ({}) must be bigger or equal to the scale ({})", precision, scale));
            }
            _ => unreachable!("Expected Schema::Fixed, got {:?}", schema),
        }

        let schema = Schema::parse_str(
            r#"{
            "name": "ValidDecimal",
             "type": "bytes",
             "logicalType": "decimal",
             "precision": 3,
             "scale": 2
         }"#,
        )?;
        match schema {
            Schema::Decimal(_) => {
                assert_not_logged("Ignoring invalid decimal logical type: The decimal precision (2) must be bigger or equal to the scale (3)");
            }
            _ => unreachable!("Expected Schema::Decimal, got {:?}", schema),
        }

        Ok(())
    }

    #[test]
    fn avro_3920_serialize_record_with_custom_attributes() -> TestResult {
        let expected = {
            let mut lookup = BTreeMap::new();
            lookup.insert("value".to_owned(), 0);
            Schema::Record(RecordSchema {
                name: Name {
                    name: "LongList".to_owned(),
                    namespace: None,
                },
                aliases: Some(vec![Alias::new("LinkedLongs").unwrap()]),
                doc: None,
                fields: vec![RecordField {
                    name: "value".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Long,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    custom_attributes: BTreeMap::from([("field-id".to_string(), 1.into())]),
                }],
                lookup,
                attributes: BTreeMap::from([("custom-attribute".to_string(), "value".into())]),
            })
        };

        let value = serde_json::to_value(&expected)?;
        let serialized = serde_json::to_string(&value)?;
        assert_eq!(
            r#"{"aliases":["LinkedLongs"],"custom-attribute":"value","fields":[{"field-id":1,"name":"value","type":"long"}],"name":"LongList","type":"record"}"#,
            &serialized
        );
        assert_eq!(expected, Schema::parse_str(&serialized)?);

        Ok(())
    }

    #[test]
    fn test_avro_3925_serialize_decimal_inner_fixed() -> TestResult {
        let schema = Schema::Decimal(DecimalSchema {
            precision: 36,
            scale: 10,
            inner: Box::new(Schema::Fixed(FixedSchema {
                name: Name::new("decimal_36_10").unwrap(),
                aliases: None,
                doc: None,
                size: 16,
                default: None,
                attributes: Default::default(),
            })),
        });

        let serialized_json = serde_json::to_string_pretty(&schema)?;

        let expected_json = r#"{
  "type": "fixed",
  "name": "decimal_36_10",
  "size": 16,
  "logicalType": "decimal",
  "scale": 10,
  "precision": 36
}"#;

        assert_eq!(serialized_json, expected_json);

        Ok(())
    }

    #[test]
    fn test_avro_3925_serialize_decimal_inner_bytes() -> TestResult {
        let schema = Schema::Decimal(DecimalSchema {
            precision: 36,
            scale: 10,
            inner: Box::new(Schema::Bytes),
        });

        let serialized_json = serde_json::to_string_pretty(&schema)?;

        let expected_json = r#"{
  "type": "bytes",
  "logicalType": "decimal",
  "scale": 10,
  "precision": 36
}"#;

        assert_eq!(serialized_json, expected_json);

        Ok(())
    }

    #[test]
    fn test_avro_3925_serialize_decimal_inner_invalid() -> TestResult {
        let schema = Schema::Decimal(DecimalSchema {
            precision: 36,
            scale: 10,
            inner: Box::new(Schema::String),
        });

        let serialized_json = serde_json::to_string_pretty(&schema);

        assert!(serialized_json.is_err());

        Ok(())
    }

    #[test]
    fn test_avro_3927_serialize_array_with_custom_attributes() -> TestResult {
        let expected = Schema::array_with_attributes(
            Schema::Long,
            BTreeMap::from([("field-id".to_string(), "1".into())]),
        );

        let value = serde_json::to_value(&expected)?;
        let serialized = serde_json::to_string(&value)?;
        assert_eq!(
            r#"{"field-id":"1","items":"long","type":"array"}"#,
            &serialized
        );
        let actual_schema = Schema::parse_str(&serialized)?;
        assert_eq!(expected, actual_schema);
        assert_eq!(
            expected.custom_attributes(),
            actual_schema.custom_attributes()
        );

        Ok(())
    }

    #[test]
    fn test_avro_3927_serialize_map_with_custom_attributes() -> TestResult {
        let expected = Schema::map_with_attributes(
            Schema::Long,
            BTreeMap::from([("field-id".to_string(), "1".into())]),
        );

        let value = serde_json::to_value(&expected)?;
        let serialized = serde_json::to_string(&value)?;
        assert_eq!(
            r#"{"field-id":"1","type":"map","values":"long"}"#,
            &serialized
        );
        let actual_schema = Schema::parse_str(&serialized)?;
        assert_eq!(expected, actual_schema);
        assert_eq!(
            expected.custom_attributes(),
            actual_schema.custom_attributes()
        );

        Ok(())
    }

    #[test]
    fn avro_3928_parse_int_based_schema_with_default() -> TestResult {
        let schema = r#"
        {
          "type": "record",
          "name": "DateLogicalType",
          "fields": [ {
            "name": "birthday",
            "type": {"type": "int", "logicalType": "date"},
            "default": 1681601653
          } ]
        }"#;

        match Schema::parse_str(schema)? {
            Schema::Record(record_schema) => {
                assert_eq!(record_schema.fields.len(), 1);
                let field = record_schema.fields.first().unwrap();
                assert_eq!(field.name, "birthday");
                assert_eq!(field.schema, Schema::Date);
                assert_eq!(
                    types::Value::from(field.default.clone().unwrap()),
                    types::Value::Int(1681601653)
                );
            }
            _ => unreachable!("Expected Schema::Record"),
        }

        Ok(())
    }

    #[test]
    fn avro_3946_union_with_single_type() -> TestResult {
        let schema = r#"
        {
          "type": "record",
          "name": "Issue",
          "namespace": "invalid.example",
          "fields": [
            {
              "name": "myField",
              "type": ["long"]
            }
          ]
        }"#;

        let _ = Schema::parse_str(schema)?;

        assert_logged(
            "Union schema with just one member! Consider dropping the union! \
                    Please enable debug logging to find out which Record schema \
                    declares the union with 'RUST_LOG=apache_avro::schema=debug'.",
        );

        Ok(())
    }

    #[test]
    fn avro_3946_union_without_any_types() -> TestResult {
        let schema = r#"
        {
          "type": "record",
          "name": "Issue",
          "namespace": "invalid.example",
          "fields": [
            {
              "name": "myField",
              "type": []
            }
          ]
        }"#;

        let _ = Schema::parse_str(schema)?;

        assert_logged(
            "Union schemas should have at least two members! \
                    Please enable debug logging to find out which Record schema \
                    declares the union with 'RUST_LOG=apache_avro::schema=debug'.",
        );

        Ok(())
    }

    #[test]
    fn avro_3965_fixed_schema_with_default_bigger_than_size() -> TestResult {
        match Schema::parse_str(
            r#"{
                "type": "fixed",
                "name": "test",
                "size": 1,
                "default": "123456789"
               }"#,
        ) {
            Ok(_schema) => panic!("Must fail!"),
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    "Fixed schema's default value length (9) does not match its size (1)"
                );
            }
        }

        Ok(())
    }

    #[test]
    fn avro_4004_canonical_form_strip_logical_types() -> TestResult {
        let schema_str = r#"
      {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42, "doc": "The field a"},
            {"name": "b", "type": "string", "namespace": "test.a"},
            {"name": "c", "type": "long", "logicalType": "timestamp-micros"}
        ]
    }"#;

        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        let fp_rabin = schema.fingerprint::<Rabin>();
        assert_eq!(
            r#"{"name":"test","type":"record","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"},{"name":"c","type":{"type":"long"}}]}"#,
            canonical_form
        );
        assert_eq!("92f2ccef718c6754", fp_rabin.to_string());
        Ok(())
    }
}
