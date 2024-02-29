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

//! Logic for checking schema compatibility
use crate::{
    error::CompatibilityError,
    schema::{EnumSchema, FixedSchema, RecordSchema, Schema, SchemaKind},
};
use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::Hasher,
    ptr,
};

pub struct SchemaCompatibility;

struct Checker {
    recursion: HashSet<(u64, u64)>,
}

impl Checker {
    /// Create a new checker, with recursion set to an empty set.
    pub(crate) fn new() -> Self {
        Self {
            recursion: HashSet::new(),
        }
    }

    pub(crate) fn can_read(
        &mut self,
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> Result<(), CompatibilityError> {
        self.full_match_schemas(writers_schema, readers_schema)
    }

    pub(crate) fn full_match_schemas(
        &mut self,
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> Result<(), CompatibilityError> {
        if self.recursion_in_progress(writers_schema, readers_schema) {
            return Ok(());
        }

        SchemaCompatibility::match_schemas(writers_schema, readers_schema)?;

        let w_type = SchemaKind::from(writers_schema);
        let r_type = SchemaKind::from(readers_schema);

        if w_type != SchemaKind::Union
            && (r_type.is_primitive()
                || r_type == SchemaKind::Fixed
                || r_type == SchemaKind::Date
                || r_type == SchemaKind::TimeMillis
                || r_type == SchemaKind::TimeMicros
                || r_type == SchemaKind::TimestampMillis
                || r_type == SchemaKind::TimestampMicros
                || r_type == SchemaKind::TimestampNanos
                || r_type == SchemaKind::LocalTimestampMillis
                || r_type == SchemaKind::LocalTimestampMicros
                || r_type == SchemaKind::LocalTimestampNanos)
        {
            return Ok(());
        }

        match r_type {
            SchemaKind::Record => self.match_record_schemas(writers_schema, readers_schema),
            SchemaKind::Map => {
                if let Schema::Map(w_m) = writers_schema {
                    match readers_schema {
                        Schema::Map(r_m) => self.full_match_schemas(&w_m.types, &r_m.types),
                        _ => Err(CompatibilityError::WrongType {
                            writer_schema_type: format!("{:#?}", writers_schema),
                            reader_schema_type: format!("{:#?}", readers_schema),
                        }),
                    }
                } else {
                    Err(CompatibilityError::TypeExpected {
                        schema_type: String::from("writers_schema"),
                        expected_type: vec![SchemaKind::Record],
                    })
                }
            }
            SchemaKind::Array => {
                if let Schema::Array(w_a) = writers_schema {
                    match readers_schema {
                        Schema::Array(r_a) => self.full_match_schemas(&w_a.items, &r_a.items),
                        _ => Err(CompatibilityError::WrongType {
                            writer_schema_type: format!("{:#?}", writers_schema),
                            reader_schema_type: format!("{:#?}", readers_schema),
                        }),
                    }
                } else {
                    Err(CompatibilityError::TypeExpected {
                        schema_type: String::from("writers_schema"),
                        expected_type: vec![SchemaKind::Array],
                    })
                }
            }
            SchemaKind::Union => self.match_union_schemas(writers_schema, readers_schema),
            SchemaKind::Enum => {
                // reader's symbols must contain all writer's symbols
                if let Schema::Enum(EnumSchema {
                    symbols: w_symbols, ..
                }) = writers_schema
                {
                    if let Schema::Enum(EnumSchema {
                        symbols: r_symbols, ..
                    }) = readers_schema
                    {
                        if w_symbols.iter().all(|e| r_symbols.contains(e)) {
                            return Ok(());
                        }
                    }
                }
                Err(CompatibilityError::MissingSymbols)
            }
            _ => {
                if w_type == SchemaKind::Union {
                    if let Schema::Union(r) = writers_schema {
                        if r.schemas.len() == 1 {
                            return self.full_match_schemas(&r.schemas[0], readers_schema);
                        }
                    }
                }
                Err(CompatibilityError::Inconclusive(String::from(
                    "writers_schema",
                )))
            }
        }
    }

    fn match_record_schemas(
        &mut self,
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> Result<(), CompatibilityError> {
        let w_type = SchemaKind::from(writers_schema);

        if w_type == SchemaKind::Union {
            return Err(CompatibilityError::TypeExpected {
                schema_type: String::from("writers_schema"),
                expected_type: vec![SchemaKind::Record],
            });
        }

        if let Schema::Record(RecordSchema {
            fields: w_fields,
            lookup: w_lookup,
            ..
        }) = writers_schema
        {
            if let Schema::Record(RecordSchema {
                fields: r_fields, ..
            }) = readers_schema
            {
                for field in r_fields.iter() {
                    // get all field names in a vector (field.name + aliases)
                    let mut fields_names = vec![&field.name];
                    if let Some(ref aliases) = field.aliases {
                        for alias in aliases {
                            fields_names.push(alias);
                        }
                    }

                    // Check whether any of the possible fields names are in the writer schema.
                    // If the field was found, then it must have the exact same name with the writer field,
                    // otherwise we would have a false positive with the writers aliases
                    let position = fields_names.iter().find_map(|field_name| {
                        if let Some(pos) = w_lookup.get(*field_name) {
                            if &w_fields[*pos].name == *field_name {
                                return Some(pos);
                            }
                        }
                        None
                    });

                    match position {
                        Some(pos) => {
                            if let Err(err) =
                                self.full_match_schemas(&w_fields[*pos].schema, &field.schema)
                            {
                                return Err(CompatibilityError::FieldTypeMismatch(
                                    field.name.clone(),
                                    Box::new(err),
                                ));
                            }
                        }
                        _ => {
                            if field.default.is_none() {
                                return Err(CompatibilityError::MissingDefaultValue(
                                    field.name.clone(),
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn match_union_schemas(
        &mut self,
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> Result<(), CompatibilityError> {
        if let Schema::Union(u) = writers_schema {
            if u.schemas
                .iter()
                .all(|schema| self.full_match_schemas(schema, readers_schema).is_ok())
            {
                return Ok(());
            } else {
                return Err(CompatibilityError::MissingUnionElements);
            }
        } else if let Schema::Union(u) = readers_schema {
            // This check is needed because the writer_schema can be not union
            // but the type can be contain in the union of the reader schema
            // e.g. writer_schema is string and reader_schema is [string, int]
            if u.schemas
                .iter()
                .any(|schema| self.full_match_schemas(writers_schema, schema).is_ok())
            {
                return Ok(());
            }
        }
        Err(CompatibilityError::MissingUnionElements)
    }

    fn recursion_in_progress(&mut self, writers_schema: &Schema, readers_schema: &Schema) -> bool {
        let mut hasher = DefaultHasher::new();
        ptr::hash(writers_schema, &mut hasher);
        let w_hash = hasher.finish();

        hasher = DefaultHasher::new();
        ptr::hash(readers_schema, &mut hasher);
        let r_hash = hasher.finish();

        let key = (w_hash, r_hash);
        // This is a shortcut to add if not exists *and* return false. It will return true
        // if it was able to insert.
        !self.recursion.insert(key)
    }
}

impl SchemaCompatibility {
    /// `can_read` performs a full, recursive check that a datum written using the
    /// writers_schema can be read using the readers_schema.
    pub fn can_read(
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> Result<(), CompatibilityError> {
        let mut c = Checker::new();
        c.can_read(writers_schema, readers_schema)
    }

    /// `mutual_read` performs a full, recursive check that a datum written using either
    /// the writers_schema or the readers_schema can be read using the other schema.
    pub fn mutual_read(
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> Result<(), CompatibilityError> {
        SchemaCompatibility::can_read(writers_schema, readers_schema)?;
        SchemaCompatibility::can_read(readers_schema, writers_schema)
    }

    ///  `match_schemas` performs a basic check that a datum written with the
    ///  writers_schema could be read using the readers_schema. This check only includes
    ///  matching the types, including schema promotion, and matching the full name for
    ///  named types. Aliases for named types are not supported here, and the rust
    ///  implementation of Avro in general does not include support for aliases (I think).
    pub(crate) fn match_schemas(
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> Result<(), CompatibilityError> {
        fn check_reader_type_multi(
            reader_type: SchemaKind,
            allowed_reader_types: Vec<SchemaKind>,
            writer_type: SchemaKind,
        ) -> Result<(), CompatibilityError> {
            if allowed_reader_types.iter().any(|&t| t == reader_type) {
                Ok(())
            } else {
                let mut allowed_types: Vec<SchemaKind> = vec![writer_type];
                allowed_types.extend_from_slice(allowed_reader_types.as_slice());
                Err(CompatibilityError::TypeExpected {
                    schema_type: String::from("readers_schema"),
                    expected_type: allowed_types,
                })
            }
        }

        fn check_reader_type(
            reader_type: SchemaKind,
            allowed_reader_type: SchemaKind,
            writer_type: SchemaKind,
        ) -> Result<(), CompatibilityError> {
            if reader_type == allowed_reader_type {
                Ok(())
            } else {
                Err(CompatibilityError::TypeExpected {
                    schema_type: String::from("readers_schema"),
                    expected_type: vec![writer_type, allowed_reader_type],
                })
            }
        }

        fn check_writer_type(
            writers_schema: &Schema,
            allowed_schema: &Schema,
            expected_schema_types: Vec<SchemaKind>,
        ) -> Result<(), CompatibilityError> {
            if *allowed_schema == *writers_schema {
                Ok(())
            } else {
                Err(CompatibilityError::TypeExpected {
                    schema_type: String::from("writers_schema"),
                    expected_type: expected_schema_types,
                })
            }
        }

        let w_type = SchemaKind::from(writers_schema);
        let r_type = SchemaKind::from(readers_schema);

        if w_type == SchemaKind::Union || r_type == SchemaKind::Union {
            return Ok(());
        }

        if w_type == r_type {
            if r_type.is_primitive() {
                return Ok(());
            }

            match r_type {
                SchemaKind::Record | SchemaKind::Enum => {
                    let msg = format!("A {} type must always has a name", readers_schema);
                    let writers_name = writers_schema.name().expect(&msg);
                    let readers_name = readers_schema.name().expect(&msg);

                    if writers_name.name == readers_name.name {
                        return Ok(());
                    }

                    return Err(CompatibilityError::NameMismatch {
                        writer_name: writers_name.name.clone(),
                        reader_name: readers_name.name.clone(),
                    });
                }
                SchemaKind::Fixed => {
                    if let Schema::Fixed(FixedSchema {
                        name: w_name,
                        aliases: _,
                        doc: _w_doc,
                        size: w_size,
                        attributes: _,
                    }) = writers_schema
                    {
                        if let Schema::Fixed(FixedSchema {
                            name: r_name,
                            aliases: _,
                            doc: _r_doc,
                            size: r_size,
                            attributes: _,
                        }) = readers_schema
                        {
                            return (w_name.name == r_name.name && w_size == r_size)
                                .then_some(())
                                .ok_or(CompatibilityError::FixedMismatch);
                        }
                    }
                }
                SchemaKind::Map => {
                    if let Schema::Map(w_m) = writers_schema {
                        if let Schema::Map(r_m) = readers_schema {
                            return SchemaCompatibility::match_schemas(&w_m.types, &r_m.types);
                        }
                    }
                }
                SchemaKind::Array => {
                    if let Schema::Array(w_a) = writers_schema {
                        if let Schema::Array(r_a) = readers_schema {
                            return SchemaCompatibility::match_schemas(&w_a.items, &r_a.items);
                        }
                    }
                }
                SchemaKind::Date | SchemaKind::TimeMillis => {
                    return check_writer_type(
                        writers_schema,
                        readers_schema,
                        vec![r_type, SchemaKind::Int],
                    );
                }
                SchemaKind::TimeMicros
                | SchemaKind::TimestampNanos
                | SchemaKind::TimestampMillis
                | SchemaKind::TimestampMicros
                | SchemaKind::LocalTimestampMillis
                | SchemaKind::LocalTimestampMicros
                | SchemaKind::LocalTimestampNanos => {
                    return check_writer_type(
                        writers_schema,
                        readers_schema,
                        vec![r_type, SchemaKind::Long],
                    );
                }
                SchemaKind::Duration => {
                    return Ok(());
                }
                _ => {
                    return Err(CompatibilityError::Inconclusive(String::from(
                        "readers_schema",
                    )))
                }
            };
        }

        // Here are the checks for primitive types
        match w_type {
            SchemaKind::Int => check_reader_type_multi(
                r_type,
                vec![
                    SchemaKind::Long,
                    SchemaKind::Float,
                    SchemaKind::Double,
                    SchemaKind::Date,
                    SchemaKind::TimeMillis,
                ],
                w_type,
            ),
            SchemaKind::Long => check_reader_type_multi(
                r_type,
                vec![
                    SchemaKind::Float,
                    SchemaKind::Double,
                    SchemaKind::TimeMicros,
                    SchemaKind::TimestampMillis,
                    SchemaKind::TimestampMicros,
                    SchemaKind::TimestampNanos,
                    SchemaKind::LocalTimestampMillis,
                    SchemaKind::LocalTimestampMicros,
                    SchemaKind::LocalTimestampNanos,
                ],
                w_type,
            ),
            SchemaKind::Float => {
                check_reader_type_multi(r_type, vec![SchemaKind::Float, SchemaKind::Double], w_type)
            }
            SchemaKind::String => check_reader_type(r_type, SchemaKind::Bytes, w_type),
            SchemaKind::Bytes => check_reader_type(r_type, SchemaKind::String, w_type),
            SchemaKind::Date | SchemaKind::TimeMillis => {
                check_reader_type(r_type, SchemaKind::Int, w_type)
            }
            SchemaKind::TimeMicros
            | SchemaKind::TimestampMicros
            | SchemaKind::TimestampMillis
            | SchemaKind::TimestampNanos
            | SchemaKind::LocalTimestampMillis
            | SchemaKind::LocalTimestampMicros
            | SchemaKind::LocalTimestampNanos => {
                check_reader_type(r_type, SchemaKind::Long, w_type)
            }
            _ => Err(CompatibilityError::Inconclusive(String::from(
                "writers_schema",
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        types::{Record, Value},
        Codec, Reader, Writer,
    };
    use apache_avro_test_helper::TestResult;
    use rstest::*;

    fn int_array_schema() -> Schema {
        Schema::parse_str(r#"{"type":"array", "items":"int"}"#).unwrap()
    }

    fn long_array_schema() -> Schema {
        Schema::parse_str(r#"{"type":"array", "items":"long"}"#).unwrap()
    }

    fn string_array_schema() -> Schema {
        Schema::parse_str(r#"{"type":"array", "items":"string"}"#).unwrap()
    }

    fn int_map_schema() -> Schema {
        Schema::parse_str(r#"{"type":"map", "values":"int"}"#).unwrap()
    }

    fn long_map_schema() -> Schema {
        Schema::parse_str(r#"{"type":"map", "values":"long"}"#).unwrap()
    }

    fn string_map_schema() -> Schema {
        Schema::parse_str(r#"{"type":"map", "values":"string"}"#).unwrap()
    }

    fn enum1_ab_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum1", "symbols":["A","B"]}"#).unwrap()
    }

    fn enum1_abc_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum1", "symbols":["A","B","C"]}"#).unwrap()
    }

    fn enum1_bc_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum1", "symbols":["B","C"]}"#).unwrap()
    }

    fn enum2_ab_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum2", "symbols":["A","B"]}"#).unwrap()
    }

    fn empty_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[]}"#).unwrap()
    }

    fn empty_record2_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record2", "fields": []}"#).unwrap()
    }

    fn a_int_record1_schema() -> Schema {
        Schema::parse_str(
            r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}]}"#,
        )
        .unwrap()
    }

    fn a_long_record1_schema() -> Schema {
        Schema::parse_str(
            r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"long"}]}"#,
        )
        .unwrap()
    }

    fn a_int_b_int_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}, {"name":"b", "type":"int"}]}"#).unwrap()
    }

    fn a_dint_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int", "default":0}]}"#).unwrap()
    }

    fn a_int_b_dint_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}, {"name":"b", "type":"int", "default":0}]}"#).unwrap()
    }

    fn a_dint_b_dint_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int", "default":0}, {"name":"b", "type":"int", "default":0}]}"#).unwrap()
    }

    fn nested_record() -> Schema {
        Schema::parse_str(r#"{"type":"record","name":"parent","fields":[{"name":"attribute","type":{"type":"record","name":"child","fields":[{"name":"id","type":"string"}]}}]}"#).unwrap()
    }

    fn nested_optional_record() -> Schema {
        Schema::parse_str(r#"{"type":"record","name":"parent","fields":[{"name":"attribute","type":["null",{"type":"record","name":"child","fields":[{"name":"id","type":"string"}]}],"default":null}]}"#).unwrap()
    }

    fn int_list_record_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"List", "fields": [{"name": "head", "type": "int"},{"name": "tail", "type": "array", "items": "int"}]}"#).unwrap()
    }

    fn long_list_record_schema() -> Schema {
        Schema::parse_str(
            r#"
      {
        "type":"record", "name":"List", "fields": [
          {"name": "head", "type": "long"},
          {"name": "tail", "type": "array", "items": "long"}
      ]}
"#,
        )
        .unwrap()
    }

    fn union_schema(schemas: Vec<Schema>) -> Schema {
        let schema_string = schemas
            .iter()
            .map(|s| s.canonical_form())
            .collect::<Vec<String>>()
            .join(",");
        Schema::parse_str(&format!("[{schema_string}]")).unwrap()
    }

    fn empty_union_schema() -> Schema {
        union_schema(vec![])
    }

    // unused
    // fn null_union_schema() -> Schema { union_schema(vec![Schema::Null]) }

    fn int_union_schema() -> Schema {
        union_schema(vec![Schema::Int])
    }

    fn long_union_schema() -> Schema {
        union_schema(vec![Schema::Long])
    }

    fn string_union_schema() -> Schema {
        union_schema(vec![Schema::String])
    }

    fn int_string_union_schema() -> Schema {
        union_schema(vec![Schema::Int, Schema::String])
    }

    fn string_int_union_schema() -> Schema {
        union_schema(vec![Schema::String, Schema::Int])
    }

    #[test]
    fn test_broken() {
        assert_eq!(
            CompatibilityError::MissingUnionElements,
            SchemaCompatibility::can_read(&int_string_union_schema(), &int_union_schema())
                .unwrap_err()
        )
    }

    #[test]
    fn test_incompatible_reader_writer_pairs() {
        let incompatible_schemas = vec![
            // null
            (Schema::Null, Schema::Int),
            (Schema::Null, Schema::Long),
            // boolean
            (Schema::Boolean, Schema::Int),
            // int
            (Schema::Int, Schema::Null),
            (Schema::Int, Schema::Boolean),
            (Schema::Int, Schema::Long),
            (Schema::Int, Schema::Float),
            (Schema::Int, Schema::Double),
            // long
            (Schema::Long, Schema::Float),
            (Schema::Long, Schema::Double),
            // float
            (Schema::Float, Schema::Double),
            // string
            (Schema::String, Schema::Boolean),
            (Schema::String, Schema::Int),
            // bytes
            (Schema::Bytes, Schema::Null),
            (Schema::Bytes, Schema::Int),
            // logical types
            (Schema::TimeMicros, Schema::Int),
            (Schema::TimestampMillis, Schema::Int),
            (Schema::TimestampMicros, Schema::Int),
            (Schema::TimestampNanos, Schema::Int),
            (Schema::LocalTimestampMillis, Schema::Int),
            (Schema::LocalTimestampMicros, Schema::Int),
            (Schema::LocalTimestampNanos, Schema::Int),
            (Schema::Date, Schema::Long),
            (Schema::TimeMillis, Schema::Long),
            // array and maps
            (int_array_schema(), long_array_schema()),
            (int_map_schema(), int_array_schema()),
            (int_array_schema(), int_map_schema()),
            (int_map_schema(), long_map_schema()),
            // enum
            (enum1_ab_schema(), enum1_abc_schema()),
            (enum1_bc_schema(), enum1_abc_schema()),
            (enum1_ab_schema(), enum2_ab_schema()),
            (Schema::Int, enum2_ab_schema()),
            (enum2_ab_schema(), Schema::Int),
            //union
            (int_union_schema(), int_string_union_schema()),
            (string_union_schema(), int_string_union_schema()),
            //record
            (empty_record2_schema(), empty_record1_schema()),
            (a_int_record1_schema(), empty_record1_schema()),
            (a_int_b_dint_record1_schema(), empty_record1_schema()),
            (int_list_record_schema(), long_list_record_schema()),
            (nested_record(), nested_optional_record()),
        ];

        assert!(incompatible_schemas
            .iter()
            .any(|(reader, writer)| SchemaCompatibility::can_read(writer, reader).is_err()));
    }

    #[rstest]
    // Record type test
    #[case(
        r#"{"type": "record", "name": "record_a", "fields": [{"type": "long", "name": "date"}]}"#,
        r#"{"type": "record", "name": "record_a", "fields": [{"type": "long", "name": "date", "default": 18181}]}"#
    )]
    // Fixed type test
    #[case(
        r#"{"type": "fixed", "name": "EmployeeId", "size": 16}"#,
        r#"{"type": "fixed", "name": "EmployeeId", "size": 16, "default": "u00ffffffffffffx"}"#
    )]
    // Enum type test
    #[case(
        r#"{"type": "enum", "name":"Enum1", "symbols": ["A","B"]}"#,
        r#"{"type": "enum", "name":"Enum1", "symbols": ["A","B", "C"], "default": "C"}"#
    )]
    // Map type test
    #[case(
        r#"{"type": "map", "values": "int"}"#,
        r#"{"type": "map", "values": "long"}"#
    )]
    // Date type
    #[case(r#"{"type": "int"}"#, r#"{"type": "int", "logicalType": "date"}"#)]
    // time-millis type
    #[case(
        r#"{"type": "int"}"#,
        r#"{"type": "int", "logicalType": "time-millis"}"#
    )]
    // time-millis type
    #[case(
        r#"{"type": "long"}"#,
        r#"{"type": "long", "logicalType": "time-micros"}"#
    )]
    // timetimestamp-nanos type
    #[case(
        r#"{"type": "long"}"#,
        r#"{"type": "long", "logicalType": "timestamp-nanos"}"#
    )]
    // timestamp-millis type
    #[case(
        r#"{"type": "long"}"#,
        r#"{"type": "long", "logicalType": "timestamp-millis"}"#
    )]
    // timestamp-micros type
    #[case(
        r#"{"type": "long"}"#,
        r#"{"type": "long", "logicalType": "timestamp-micros"}"#
    )]
    // local-timestamp-millis type
    #[case(
        r#"{"type": "long"}"#,
        r#"{"type": "long", "logicalType": "local-timestamp-millis"}"#
    )]
    // local-timestamp-micros type
    #[case(
        r#"{"type": "long"}"#,
        r#"{"type": "long", "logicalType": "local-timestamp-micros"}"#
    )]
    // local-timestamp-nanos type
    #[case(
        r#"{"type": "long"}"#,
        r#"{"type": "long", "logicalType": "local-timestamp-nanos"}"#
    )]
    // Array type test
    #[case(
        r#"{"type": "array", "items": "int"}"#,
        r#"{"type": "array", "items": "long"}"#
    )]
    fn test_avro_3950_match_schemas_ok(
        #[case] writer_schema_str: &str,
        #[case] reader_schema_str: &str,
    ) {
        let writer_schema = Schema::parse_str(writer_schema_str).unwrap();
        let reader_schema = Schema::parse_str(reader_schema_str).unwrap();

        assert!(SchemaCompatibility::match_schemas(&writer_schema, &reader_schema).is_ok());
    }

    #[rstest]
    // Record type test
    #[case(
        r#"{"type": "record", "name":"record_a", "fields": [{"type": "long", "name": "date"}]}"#,
        r#"{"type": "record", "name":"record_b", "fields": [{"type": "long", "name": "date"}]}"#,
        CompatibilityError::NameMismatch{writer_name: String::from("record_a"), reader_name: String::from("record_b")}
    )]
    // Fixed type test
    #[case(
        r#"{"type": "fixed", "name": "EmployeeId", "size": 16}"#,
        r#"{"type": "fixed", "name": "EmployeeId", "size": 20}"#,
        CompatibilityError::FixedMismatch
    )]
    // Enum type test
    #[case(
        r#"{"type": "enum", "name": "Enum1", "symbols": ["A","B"]}"#,
        r#"{"type": "enum", "name": "Enum2", "symbols": ["A","B"]}"#,
        CompatibilityError::NameMismatch{writer_name: String::from("Enum1"), reader_name: String::from("Enum2")}
    )]
    // Map type test
    #[case(
        r#"{"type":"map", "values": "long"}"#,
        r#"{"type":"map", "values": "int"}"#,
        CompatibilityError::TypeExpected {schema_type: String::from("readers_schema"), expected_type: vec![
            SchemaKind::Long,
            SchemaKind::Float,
            SchemaKind::Double,
            SchemaKind::TimeMicros,
            SchemaKind::TimestampMillis,
            SchemaKind::TimestampMicros,
            SchemaKind::TimestampNanos,
            SchemaKind::LocalTimestampMillis,
            SchemaKind::LocalTimestampMicros,
            SchemaKind::LocalTimestampNanos,
        ]}
    )]
    // Array type test
    #[case(
        r#"{"type": "array", "items": "long"}"#,
        r#"{"type": "array", "items": "int"}"#,
        CompatibilityError::TypeExpected {schema_type: String::from("readers_schema"), expected_type: vec![
            SchemaKind::Long,
            SchemaKind::Float,
            SchemaKind::Double,
            SchemaKind::TimeMicros,
            SchemaKind::TimestampMillis,
            SchemaKind::TimestampMicros,
            SchemaKind::TimestampNanos,
            SchemaKind::LocalTimestampMillis,
            SchemaKind::LocalTimestampMicros,
            SchemaKind::LocalTimestampNanos,
        ]}
    )]
    // Date type test
    #[case(
        r#"{"type": "string"}"#,
        r#"{"type": "int", "logicalType": "date"}"#,
        CompatibilityError::TypeExpected{schema_type: String::from("readers_schema"), expected_type: vec![SchemaKind::String, SchemaKind::Bytes]}
    )]
    // time-millis type
    #[case(
        r#"{"type": "string"}"#,
        r#"{"type": "int", "logicalType": "time-millis"}"#,
        CompatibilityError::TypeExpected{schema_type: String::from("readers_schema"), expected_type: vec![SchemaKind::String, SchemaKind::Bytes]}
    )]
    // time-millis type
    #[case(
        r#"{"type": "int"}"#,
        r#"{"type": "long", "logicalType": "time-micros"}"#,
        CompatibilityError::TypeExpected{schema_type: String::from("readers_schema"), expected_type: vec![
            SchemaKind::Int,
            SchemaKind::Long,
            SchemaKind::Float,
            SchemaKind::Double,
            SchemaKind::Date,
            SchemaKind::TimeMillis
        ]}
    )]
    // timestamp-nanos type. This test should fail because it is not supported on schema parse_complex
    // #[case(
    //     r#"{"type": "string"}"#,
    //     r#"{"type": "long", "logicalType": "timestamp-nanos"}"#,
    //     CompatibilityError::TypeExpected{schema_type: String::from("readers_schema"), expected_type: vec![
    //         SchemaKind::Int,
    //         SchemaKind::Long,
    //         SchemaKind::Float,
    //         SchemaKind::Double,
    //         SchemaKind::Date,
    //         SchemaKind::TimeMillis
    //     ]}
    // )]
    // timestamp-millis type
    #[case(
        r#"{"type": "int"}"#,
        r#"{"type": "long", "logicalType": "timestamp-millis"}"#,
        CompatibilityError::TypeExpected{schema_type: String::from("readers_schema"), expected_type: vec![
            SchemaKind::Int,
            SchemaKind::Long,
            SchemaKind::Float,
            SchemaKind::Double,
            SchemaKind::Date,
            SchemaKind::TimeMillis
        ]}
    )]
    // timestamp-micros type
    #[case(
        r#"{"type": "int"}"#,
        r#"{"type": "long", "logicalType": "timestamp-micros"}"#,
        CompatibilityError::TypeExpected{schema_type: String::from("readers_schema"), expected_type: vec![
            SchemaKind::Int,
            SchemaKind::Long,
            SchemaKind::Float,
            SchemaKind::Double,
            SchemaKind::Date,
            SchemaKind::TimeMillis
        ]}
    )]
    // local-timestamp-millis type
    #[case(
        r#"{"type": "int"}"#,
        r#"{"type": "long", "logicalType": "local-timestamp-millis"}"#,
        CompatibilityError::TypeExpected{schema_type: String::from("readers_schema"), expected_type: vec![
            SchemaKind::Int,
            SchemaKind::Long,
            SchemaKind::Float,
            SchemaKind::Double,
            SchemaKind::Date,
            SchemaKind::TimeMillis
        ]}
    )]
    // local-timestamp-micros type
    #[case(
        r#"{"type": "int"}"#,
        r#"{"type": "long", "logicalType": "local-timestamp-micros"}"#,
        CompatibilityError::TypeExpected{schema_type: String::from("readers_schema"), expected_type: vec![
            SchemaKind::Int,
            SchemaKind::Long,
            SchemaKind::Float,
            SchemaKind::Double,
            SchemaKind::Date,
            SchemaKind::TimeMillis
        ]}
    )]
    // local-timestamp-nanos type. This test should fail because it is not supported on schema parse_complex
    // #[case(
    //     r#"{"type": "int"}"#,
    //     r#"{"type": "long", "logicalType": "local-timestamp-nanos"}"#,
    //     CompatibilityError::TypeExpected{schema_type: String::from("readers_schema"), expected_type: vec![
    //         SchemaKind::Int,
    //         SchemaKind::Long,
    //         SchemaKind::Float,
    //         SchemaKind::Double,
    //         SchemaKind::Date,
    //         SchemaKind::TimeMillis
    //     ]}
    // )]
    // When comparing different types we always get Inconclusive
    #[case(
        r#"{"type": "record", "name":"record_b", "fields": [{"type": "long", "name": "date"}]}"#,
        r#"{"type": "fixed", "name": "EmployeeId", "size": 16}"#,
        CompatibilityError::Inconclusive(String::from("writers_schema"))
    )]
    fn test_avro_3950_match_schemas_error(
        #[case] writer_schema_str: &str,
        #[case] reader_schema_str: &str,
        #[case] expected_error: CompatibilityError,
    ) {
        let writer_schema = Schema::parse_str(writer_schema_str).unwrap();
        let reader_schema = Schema::parse_str(reader_schema_str).unwrap();

        assert_eq!(
            expected_error,
            SchemaCompatibility::match_schemas(&writer_schema, &reader_schema).unwrap_err()
        )
    }

    #[test]
    fn test_compatible_reader_writer_pairs() {
        let compatible_schemas = vec![
            (Schema::Null, Schema::Null),
            (Schema::Long, Schema::Int),
            (Schema::Float, Schema::Int),
            (Schema::Float, Schema::Long),
            (Schema::Double, Schema::Long),
            (Schema::Double, Schema::Int),
            (Schema::Double, Schema::Float),
            (Schema::String, Schema::Bytes),
            (Schema::Bytes, Schema::String),
            // logical types
            (Schema::Date, Schema::Int),
            (Schema::TimeMillis, Schema::Int),
            (Schema::TimeMicros, Schema::Long),
            (Schema::TimestampMillis, Schema::Long),
            (Schema::TimestampMicros, Schema::Long),
            (Schema::TimestampNanos, Schema::Long),
            (Schema::LocalTimestampMillis, Schema::Long),
            (Schema::LocalTimestampMicros, Schema::Long),
            (Schema::LocalTimestampNanos, Schema::Long),
            (Schema::Int, Schema::Date),
            (Schema::Int, Schema::TimeMillis),
            (Schema::Long, Schema::TimeMicros),
            (Schema::Long, Schema::TimestampMillis),
            (Schema::Long, Schema::TimestampMicros),
            (Schema::Long, Schema::TimestampNanos),
            (Schema::Long, Schema::LocalTimestampMillis),
            (Schema::Long, Schema::LocalTimestampMicros),
            (Schema::Long, Schema::LocalTimestampNanos),
            (int_array_schema(), int_array_schema()),
            (long_array_schema(), int_array_schema()),
            (int_map_schema(), int_map_schema()),
            (long_map_schema(), int_map_schema()),
            (enum1_ab_schema(), enum1_ab_schema()),
            (enum1_abc_schema(), enum1_ab_schema()),
            (empty_union_schema(), empty_union_schema()),
            (int_union_schema(), int_union_schema()),
            (int_string_union_schema(), string_int_union_schema()),
            (int_union_schema(), empty_union_schema()),
            (long_union_schema(), int_union_schema()),
            (int_union_schema(), Schema::Int),
            (Schema::Int, int_union_schema()),
            (empty_record1_schema(), empty_record1_schema()),
            (empty_record1_schema(), a_int_record1_schema()),
            (a_int_record1_schema(), a_int_record1_schema()),
            (a_dint_record1_schema(), a_int_record1_schema()),
            (a_dint_record1_schema(), a_dint_record1_schema()),
            (a_int_record1_schema(), a_dint_record1_schema()),
            (a_long_record1_schema(), a_int_record1_schema()),
            (a_int_record1_schema(), a_int_b_int_record1_schema()),
            (a_dint_record1_schema(), a_int_b_int_record1_schema()),
            (a_int_b_dint_record1_schema(), a_int_record1_schema()),
            (a_dint_b_dint_record1_schema(), empty_record1_schema()),
            (a_dint_b_dint_record1_schema(), a_int_record1_schema()),
            (a_int_b_int_record1_schema(), a_dint_b_dint_record1_schema()),
            (int_list_record_schema(), int_list_record_schema()),
            (long_list_record_schema(), long_list_record_schema()),
            (long_list_record_schema(), int_list_record_schema()),
            (nested_optional_record(), nested_record()),
        ];

        assert!(compatible_schemas
            .iter()
            .all(|(reader, writer)| SchemaCompatibility::can_read(writer, reader).is_ok()));
    }

    fn writer_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Record", "fields":[
        {"name":"oldfield1", "type":"int"},
        {"name":"oldfield2", "type":"string"}
      ]}
"#,
        )
        .unwrap()
    }

    #[test]
    fn test_missing_field() -> TestResult {
        let reader_schema = Schema::parse_str(
            r#"
      {"type":"record", "name":"Record", "fields":[
        {"name":"oldfield1", "type":"int"}
      ]}
"#,
        )?;
        assert!(SchemaCompatibility::can_read(&writer_schema(), &reader_schema,).is_ok());
        assert_eq!(
            CompatibilityError::MissingDefaultValue(String::from("oldfield2")),
            SchemaCompatibility::can_read(&reader_schema, &writer_schema()).unwrap_err()
        );

        Ok(())
    }

    #[test]
    fn test_missing_second_field() -> TestResult {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield2", "type":"string"}
        ]}
"#,
        )?;
        assert!(SchemaCompatibility::can_read(&writer_schema(), &reader_schema).is_ok());
        assert_eq!(
            CompatibilityError::MissingDefaultValue(String::from("oldfield1")),
            SchemaCompatibility::can_read(&reader_schema, &writer_schema()).unwrap_err()
        );

        Ok(())
    }

    #[test]
    fn test_all_fields() -> TestResult {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield1", "type":"int"},
          {"name":"oldfield2", "type":"string"}
        ]}
"#,
        )?;
        assert!(SchemaCompatibility::can_read(&writer_schema(), &reader_schema).is_ok());
        assert!(SchemaCompatibility::can_read(&reader_schema, &writer_schema()).is_ok());

        Ok(())
    }

    #[test]
    fn test_new_field_with_default() -> TestResult {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield1", "type":"int"},
          {"name":"newfield1", "type":"int", "default":42}
        ]}
"#,
        )?;
        assert!(SchemaCompatibility::can_read(&writer_schema(), &reader_schema).is_ok());
        assert_eq!(
            CompatibilityError::MissingDefaultValue(String::from("oldfield2")),
            SchemaCompatibility::can_read(&reader_schema, &writer_schema()).unwrap_err()
        );

        Ok(())
    }

    #[test]
    fn test_new_field() -> TestResult {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield1", "type":"int"},
          {"name":"newfield1", "type":"int"}
        ]}
"#,
        )?;
        assert_eq!(
            CompatibilityError::MissingDefaultValue(String::from("newfield1")),
            SchemaCompatibility::can_read(&writer_schema(), &reader_schema).unwrap_err()
        );
        assert_eq!(
            CompatibilityError::MissingDefaultValue(String::from("oldfield2")),
            SchemaCompatibility::can_read(&reader_schema, &writer_schema()).unwrap_err()
        );

        Ok(())
    }

    #[test]
    fn test_array_writer_schema() {
        let valid_reader = string_array_schema();
        let invalid_reader = string_map_schema();

        assert!(SchemaCompatibility::can_read(&string_array_schema(), &valid_reader).is_ok());
        assert_eq!(
            CompatibilityError::Inconclusive(String::from("writers_schema")),
            SchemaCompatibility::can_read(&string_array_schema(), &invalid_reader).unwrap_err()
        );
    }

    #[test]
    fn test_primitive_writer_schema() {
        let valid_reader = Schema::String;
        assert!(SchemaCompatibility::can_read(&Schema::String, &valid_reader).is_ok());
        assert_eq!(
            CompatibilityError::TypeExpected {
                schema_type: String::from("readers_schema"),
                expected_type: vec![
                    SchemaKind::Int,
                    SchemaKind::Long,
                    SchemaKind::Float,
                    SchemaKind::Double,
                    SchemaKind::Date,
                    SchemaKind::TimeMillis
                ],
            },
            SchemaCompatibility::can_read(&Schema::Int, &Schema::String).unwrap_err()
        );
    }

    #[test]
    fn test_union_reader_writer_subset_incompatibility() {
        // reader union schema must contain all writer union branches
        let union_writer = union_schema(vec![Schema::Int, Schema::String]);
        let union_reader = union_schema(vec![Schema::String]);

        assert_eq!(
            CompatibilityError::MissingUnionElements,
            SchemaCompatibility::can_read(&union_writer, &union_reader).unwrap_err()
        );
        assert!(SchemaCompatibility::can_read(&union_reader, &union_writer).is_ok());
    }

    #[test]
    fn test_incompatible_record_field() -> TestResult {
        let string_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"MyRecord", "namespace":"ns", "fields": [
            {"name":"field1", "type":"string"}
        ]}
        "#,
        )?;

        let int_schema = Schema::parse_str(
            r#"
              {"type":"record", "name":"MyRecord", "namespace":"ns", "fields": [
                {"name":"field1", "type":"int"}
              ]}
        "#,
        )?;

        assert_eq!(
            CompatibilityError::FieldTypeMismatch(
                "field1".to_owned(),
                Box::new(CompatibilityError::TypeExpected {
                    schema_type: "readers_schema".to_owned(),
                    expected_type: vec![SchemaKind::String, SchemaKind::Bytes]
                })
            ),
            SchemaCompatibility::can_read(&string_schema, &int_schema).unwrap_err()
        );

        Ok(())
    }

    #[test]
    fn test_enum_symbols() -> TestResult {
        let enum_schema1 = Schema::parse_str(
            r#"
      {"type":"enum", "name":"MyEnum", "symbols":["A","B"]}
"#,
        )?;
        let enum_schema2 =
            Schema::parse_str(r#"{"type":"enum", "name":"MyEnum", "symbols":["A","B","C"]}"#)?;
        assert_eq!(
            CompatibilityError::MissingSymbols,
            SchemaCompatibility::can_read(&enum_schema2, &enum_schema1).unwrap_err()
        );
        assert!(SchemaCompatibility::can_read(&enum_schema1, &enum_schema2).is_ok());

        Ok(())
    }

    fn point_2d_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Point2D", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"}
      ]}
    "#,
        )
        .unwrap()
    }

    fn point_2d_fullname_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Point", "namespace":"written", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"}
      ]}
    "#,
        )
        .unwrap()
    }

    fn point_3d_no_default_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Point", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"},
        {"name":"z", "type":"double"}
      ]}
    "#,
        )
        .unwrap()
    }

    fn point_3d_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Point3D", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"},
        {"name":"z", "type":"double", "default": 0.0}
      ]}
    "#,
        )
        .unwrap()
    }

    fn point_3d_match_name_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Point", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"},
        {"name":"z", "type":"double", "default": 0.0}
      ]}
    "#,
        )
        .unwrap()
    }

    #[test]
    fn test_union_resolution_no_structure_match() {
        // short name match, but no structure match
        let read_schema = union_schema(vec![Schema::Null, point_3d_no_default_schema()]);
        assert_eq!(
            CompatibilityError::MissingUnionElements,
            SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema).unwrap_err()
        );
    }

    #[test]
    fn test_union_resolution_first_structure_match_2d() {
        // multiple structure matches with no name matches
        let read_schema = union_schema(vec![
            Schema::Null,
            point_3d_no_default_schema(),
            point_2d_schema(),
            point_3d_schema(),
        ]);
        assert_eq!(
            CompatibilityError::MissingUnionElements,
            SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema).unwrap_err()
        );
    }

    #[test]
    fn test_union_resolution_first_structure_match_3d() {
        // multiple structure matches with no name matches
        let read_schema = union_schema(vec![
            Schema::Null,
            point_3d_no_default_schema(),
            point_3d_schema(),
            point_2d_schema(),
        ]);
        assert_eq!(
            CompatibilityError::MissingUnionElements,
            SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema).unwrap_err()
        );
    }

    #[test]
    fn test_union_resolution_named_structure_match() {
        // multiple structure matches with a short name match
        let read_schema = union_schema(vec![
            Schema::Null,
            point_2d_schema(),
            point_3d_match_name_schema(),
            point_3d_schema(),
        ]);
        assert_eq!(
            CompatibilityError::MissingUnionElements,
            SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema).unwrap_err()
        );
    }

    #[test]
    fn test_union_resolution_full_name_match() {
        // there is a full name match that should be chosen
        let read_schema = union_schema(vec![
            Schema::Null,
            point_2d_schema(),
            point_3d_match_name_schema(),
            point_3d_schema(),
            point_2d_fullname_schema(),
        ]);
        assert!(SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema).is_ok());
    }

    #[test]
    fn test_avro_3772_enum_default() -> TestResult {
        let writer_raw_schema = r#"
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
                "default": "spades"
              }
            }
          ]
        }
        "#;

        let reader_raw_schema = r#"
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
                 "symbols": ["diamonds", "spades", "ninja", "hearts"],
                 "default": "spades"
              }
            }
          ]
        }
      "#;
        let writer_schema = Schema::parse_str(writer_raw_schema)?;
        let reader_schema = Schema::parse_str(reader_raw_schema)?;
        let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        record.put("c", "clubs");
        writer.append(record).unwrap();
        let input = writer.into_inner()?;
        let mut reader = Reader::with_schema(&reader_schema, &input[..])?;
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::Record(vec![
                ("a".to_string(), Value::Long(27)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Enum(1, "spades".to_string())),
            ])
        );
        assert!(reader.next().is_none());

        Ok(())
    }

    #[test]
    fn test_avro_3772_enum_default_less_symbols() -> TestResult {
        let writer_raw_schema = r#"
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
                "default": "spades"
              }
            }
          ]
        }
        "#;

        let reader_raw_schema = r#"
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
                  "symbols": ["hearts", "spades"],
                  "default": "spades"
              }
            }
          ]
        }
      "#;
        let writer_schema = Schema::parse_str(writer_raw_schema)?;
        let reader_schema = Schema::parse_str(reader_raw_schema)?;
        let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null);
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        record.put("c", "hearts");
        writer.append(record).unwrap();
        let input = writer.into_inner()?;
        let mut reader = Reader::with_schema(&reader_schema, &input[..])?;
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::Record(vec![
                ("a".to_string(), Value::Long(27)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Enum(0, "hearts".to_string())),
            ])
        );
        assert!(reader.next().is_none());

        Ok(())
    }

    #[test]
    fn avro_3894_take_aliases_into_account_when_serializing_for_schema_compatibility() -> TestResult
    {
        let schema_v1 = Schema::parse_str(
            r#"
        {
            "type": "record",
            "name": "Conference",
            "namespace": "advdaba",
            "fields": [
                {"type": "string", "name": "name"},
                {"type": "long", "name": "date"}
            ]
        }"#,
        )?;

        let schema_v2 = Schema::parse_str(
            r#"
        {
            "type": "record",
            "name": "Conference",
            "namespace": "advdaba",
            "fields": [
                {"type": "string", "name": "name"},
                {"type": "long", "name": "date", "aliases" : [ "time" ]}
            ]
        }"#,
        )?;

        assert!(SchemaCompatibility::mutual_read(&schema_v1, &schema_v2).is_ok());

        Ok(())
    }

    #[test]
    fn avro_3917_take_aliases_into_account_for_schema_compatibility() -> TestResult {
        let schema_v1 = Schema::parse_str(
            r#"
        {
            "type": "record",
            "name": "Conference",
            "namespace": "advdaba",
            "fields": [
                {"type": "string", "name": "name"},
                {"type": "long", "name": "date", "aliases" : [ "time" ]}
            ]
        }"#,
        )?;

        let schema_v2 = Schema::parse_str(
            r#"
        {
            "type": "record",
            "name": "Conference",
            "namespace": "advdaba",
            "fields": [
                {"type": "string", "name": "name"},
                {"type": "long", "name": "time"}
            ]
        }"#,
        )?;

        assert!(SchemaCompatibility::can_read(&schema_v2, &schema_v1).is_ok());
        assert_eq!(
            CompatibilityError::MissingDefaultValue(String::from("time")),
            SchemaCompatibility::can_read(&schema_v1, &schema_v2).unwrap_err()
        );

        Ok(())
    }

    #[test]
    fn test_avro_3898_record_schemas_match_by_unqualified_name() -> TestResult {
        let schemas = [
            // Record schemas
            (
                Schema::parse_str(
                    r#"{
              "type": "record",
              "name": "Statistics",
              "fields": [
                { "name": "success", "type": "int" },
                { "name": "fail", "type": "int" },
                { "name": "time", "type": "string" },
                { "name": "max", "type": "int", "default": 0 }
              ]
            }"#,
                )?,
                Schema::parse_str(
                    r#"{
              "type": "record",
              "name": "Statistics",
              "namespace": "my.namespace",
              "fields": [
                { "name": "success", "type": "int" },
                { "name": "fail", "type": "int" },
                { "name": "time", "type": "string" },
                { "name": "average", "type": "int", "default": 0}
              ]
            }"#,
                )?,
            ),
            // Enum schemas
            (
                Schema::parse_str(
                    r#"{
                    "type": "enum",
                    "name": "Suit",
                    "symbols": ["diamonds", "spades", "clubs"]
                }"#,
                )?,
                Schema::parse_str(
                    r#"{
                    "type": "enum",
                    "name": "Suit",
                    "namespace": "my.namespace",
                    "symbols": ["diamonds", "spades", "clubs", "hearts"]
                }"#,
                )?,
            ),
            // Fixed schemas
            (
                Schema::parse_str(
                    r#"{
                    "type": "fixed",
                    "name": "EmployeeId",
                    "size": 16
                }"#,
                )?,
                Schema::parse_str(
                    r#"{
                    "type": "fixed",
                    "name": "EmployeeId",
                    "namespace": "my.namespace",
                    "size": 16
                }"#,
                )?,
            ),
        ];

        for (schema_1, schema_2) in schemas {
            assert!(SchemaCompatibility::can_read(&schema_1, &schema_2).is_ok());
        }

        Ok(())
    }

    #[test]
    fn test_can_read_compatibility_errors() -> TestResult {
        let schemas = [
            (
                Schema::parse_str(
                r#"{
                    "type": "record",
                    "name": "StatisticsMap",
                    "fields": [
                        {"name": "average", "type": "int", "default": 0},
                        {"name": "success", "type": {"type": "map", "values": "int"}}
                    ]
                }"#)?,
                Schema::parse_str(
                        r#"{
                    "type": "record",
                    "name": "StatisticsMap",
                    "fields": [
                        {"name": "average", "type": "int", "default": 0},
                        {"name": "success", "type": ["null", {"type": "map", "values": "int"}], "default": null}
                    ]
                }"#)?,
                "Incompatible schemata! Field 'success' in reader schema does not match the type in the writer schema"
            ),
            (
                Schema::parse_str(
                    r#"{
                        "type": "record",
                        "name": "StatisticsArray",
                        "fields": [
                            {"name": "max_values", "type": {"type": "array", "items": "int"}}
                        ]
                    }"#)?,
                    Schema::parse_str(
                    r#"{
                        "type": "record",
                        "name": "StatisticsArray",
                        "fields": [
                            {"name": "max_values", "type": ["null", {"type": "array", "items": "int"}], "default": null}
                        ]
                    }"#)?,
                    "Incompatible schemata! Field 'max_values' in reader schema does not match the type in the writer schema"
            )
        ];

        for (schema_1, schema_2, error) in schemas {
            assert!(SchemaCompatibility::can_read(&schema_1, &schema_2).is_ok());
            assert_eq!(
                error,
                SchemaCompatibility::can_read(&schema_2, &schema_1)
                    .unwrap_err()
                    .to_string()
            );
        }

        Ok(())
    }
}
