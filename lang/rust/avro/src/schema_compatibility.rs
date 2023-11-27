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
use crate::schema::{EnumSchema, FixedSchema, RecordSchema, Schema, SchemaKind};
use crate::{AvroResult, Error};
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
    ) -> AvroResult<()> {
        self.full_match_schemas(writers_schema, readers_schema)
    }

    pub(crate) fn full_match_schemas(
        &mut self,
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> AvroResult<()> {
        if self.recursion_in_progress(writers_schema, readers_schema) {
            return Ok(());
        }

        SchemaCompatibility::match_schemas(writers_schema, readers_schema)?;

        let w_type = SchemaKind::from(writers_schema);
        let r_type = SchemaKind::from(readers_schema);

        if w_type != SchemaKind::Union && (r_type.is_primitive() || r_type == SchemaKind::Fixed) {
            return Ok(());
        }

        match r_type {
            SchemaKind::Record => self.match_record_schemas(writers_schema, readers_schema),
            SchemaKind::Map => {
                if let Schema::Map(w_m) = writers_schema {
                    match readers_schema {
                        Schema::Map(r_m) => self.full_match_schemas(w_m, r_m),
                        _ => {
                            Err(Error::CompatibilityError(String::from(
                                "readers_schema should have been Schema::Map",
                            )))
                        }
                    }
                } else {
                    Err(Error::CompatibilityError(String::from(
                        "writers_schema should have been Schema::Map",
                    )))
                }
            }
            SchemaKind::Array => {
                if let Schema::Array(w_a) = writers_schema {
                    match readers_schema {
                        Schema::Array(r_a) => self.full_match_schemas(w_a, r_a),
                        _ => {
                            Err(Error::CompatibilityError(String::from(
                                "readers_schema should have been Schema::Array",
                            )))
                        }
                    }
                } else {
                    Err(Error::CompatibilityError(String::from(
                        "writers_schema should have been Schema::Array",
                    )))
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
                Err(Error::CompatibilityError(String::from(
                    "reader's symbols must contain all writer's symbols",
                )))
            }
            _ => {
                if w_type == SchemaKind::Union {
                    if let Schema::Union(r) = writers_schema {
                        if r.schemas.len() == 1 {
                            return self.full_match_schemas(&r.schemas[0], readers_schema);
                        }
                    }
                }
                Err(Error::CompatibilityError(String::from("Unkown")))
            }
        }
    }

    fn match_record_schemas(
        &mut self,
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> AvroResult<()> {
        let w_type = SchemaKind::from(writers_schema);

        if w_type == SchemaKind::Union {
            return Err(Error::CompatibilityError(String::from(
                "Type should be record",
            )));
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
                    if let Some(pos) = w_lookup.get(&field.name) {
                        if self
                            .full_match_schemas(&w_fields[*pos].schema, &field.schema)
                            .is_err()
                        {
                            return Err(Error::CompatibilityError(format!("Field {} in reader schema does not match the type in the writer schema", field.name)));
                        }
                    } else if field.default.is_none() {
                        return Err(Error::CompatibilityError(format!(
                            "Field {} in reader schema must have a default value",
                            field.name
                        )));
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
    ) -> AvroResult<()> {
        // Do not need to check the SchemaKind of reader as this function
        // is only called when the readers_schema is Union
        let w_type = SchemaKind::from(writers_schema);

        if w_type == SchemaKind::Union {
            if let Schema::Union(u) = writers_schema {
                if u.schemas
                    .iter()
                    .all(|schema| self.full_match_schemas(schema, readers_schema).is_ok())
                {
                    return Ok(());
                } else {
                    return Err(Error::CompatibilityError(String::from(
                        "All elements in union must match for both schemas",
                    )));
                }
            } else {
                return Err(Error::CompatibilityError(String::from(
                    "writers_schema should have been Schema::Union",
                )));
            }
        } else if let Schema::Union(u) = readers_schema {
            // This check is nneded because the writer_schema can be a not union
            // but the type can be contain in the union of the reeader schema
            // e.g. writer_schema is string and reader_schema is [string, int]
            if u.schemas
                .iter()
                .any(|schema| self.full_match_schemas(writers_schema, schema).is_ok())
            {
                return Ok(());
            }
        }
        Err(Error::CompatibilityError(String::from("Schemas missmatch")))
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
    pub fn can_read(writers_schema: &Schema, readers_schema: &Schema) -> AvroResult<()> {
        let mut c = Checker::new();
        c.can_read(writers_schema, readers_schema)
    }

    /// `mutual_read` performs a full, recursive check that a datum written using either
    /// the writers_schema or the readers_schema can be read using the other schema.
    pub fn mutual_read(writers_schema: &Schema, readers_schema: &Schema) -> bool {
        SchemaCompatibility::can_read(writers_schema, readers_schema).is_ok()
            && SchemaCompatibility::can_read(readers_schema, writers_schema).is_ok()
    }

    ///  `match_schemas` performs a basic check that a datum written with the
    ///  writers_schema could be read using the readers_schema. This check only includes
    ///  matching the types, including schema promotion, and matching the full name for
    ///  named types. Aliases for named types are not supported here, and the rust
    ///  implementation of Avro in general does not include support for aliases (I think).
    pub(crate) fn match_schemas(
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> AvroResult<()> {
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
                SchemaKind::Record => {
                    if let Schema::Record(RecordSchema { name: w_name, .. }) = writers_schema {
                        if let Schema::Record(RecordSchema { name: r_name, .. }) = readers_schema {
                            if w_name.name == r_name.name {
                                return Ok(());
                            } else {
                                return Err(Error::CompatibilityError(String::from(
                                    "Schema name mismatch. Both names should be the same",
                                )));
                            }
                        } else {
                            return Err(Error::CompatibilityError(String::from(
                                "readers_schema should have been Schema::Record",
                            )));
                        }
                    } else {
                        return Err(Error::CompatibilityError(String::from(
                            "writers_schema should have been Schema::Record",
                        )));
                    }
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
                            return (w_name.name == r_name.name && w_size == r_size).then_some(()).ok_or(
                                Error::CompatibilityError(String::from(
                                "Name and size don't match for fixed.",
                            )));
                        } else {
                            return Err(Error::CompatibilityError(String::from(
                                "writers_schema should have been Schema::Fixed",
                            )));
                        }
                    }
                }
                SchemaKind::Enum => {
                    if let Schema::Enum(EnumSchema { name: w_name, .. }) = writers_schema {
                        if let Schema::Enum(EnumSchema { name: r_name, .. }) = readers_schema {
                            if w_name.name == r_name.name {
                                return Ok(());
                            } else {
                                return Err(Error::CompatibilityError(String::from(
                                    "Names don't match",
                                )));
                            }
                        } else {
                            return Err(Error::CompatibilityError(String::from(
                                "readers_schema should have been Schema::Enum",
                            )));
                        }
                    } else {
                        return Err(Error::CompatibilityError(String::from(
                            "writers_schema should have been Schema::Enum",
                        )));
                    }
                }
                SchemaKind::Map => {
                    if let Schema::Map(w_m) = writers_schema {
                        if let Schema::Map(r_m) = readers_schema {
                            return SchemaCompatibility::match_schemas(w_m, r_m);
                        } else {
                            return Err(Error::CompatibilityError(String::from(
                                "readers_schema should have been Schema::Map",
                            )));
                        }
                    } else {
                        return Err(Error::CompatibilityError(String::from(
                            "writers_schema should have been Schema::Map",
                        )));
                    }
                }
                SchemaKind::Array => {
                    if let Schema::Array(w_a) = writers_schema {
                        if let Schema::Array(r_a) = readers_schema {
                            return SchemaCompatibility::match_schemas(w_a, r_a);
                        } else {
                            return Err(Error::CompatibilityError(String::from(
                                "readers_schema should have been Schema::Array",
                            )));
                        }
                    } else {
                        return Err(Error::CompatibilityError(String::from(
                            "writers_schema should have been Schema::Array",
                        )));
                    }
                }
                _ => {
                    return Err(Error::CompatibilityError(String::from(
                        "Unknown reader type",
                    )))
                }
            };
        }

        // Here are the checks for primitive types
        match w_type { 
            SchemaKind::Int => {
                if [SchemaKind::Long, SchemaKind::Float, SchemaKind::Double]
                    .iter()
                    .any(|&t| t == r_type)
                {
                    Ok(())
                } else {
                    Err(Error::CompatibilityError(String::from("The type in the reader schema should be long, float or double in order to read int types")))
                }
            }
            SchemaKind::Long => {
                if [SchemaKind::Float, SchemaKind::Double]
                    .iter()
                    .any(|&t| t == r_type)
                {
                    Ok(())
                } else {
                    Err(Error::CompatibilityError(String::from(
                        "Type should be float or double",
                    )))
                }
            }
            SchemaKind::Float =>  {
                if [SchemaKind::Float, SchemaKind::Double]
                    .iter()
                    .any(|&t| t == r_type)
                {
                    Ok(())
                } else {
                    Err(Error::CompatibilityError(String::from(
                        "Type should be float or double",
                    )))
                }
            }
            SchemaKind::String => {
                if r_type == SchemaKind::Bytes {
                    Ok(())
                } else {
                    Err(Error::CompatibilityError(String::from(
                        "Types must be bytes",
                    )))
                }
            }
            SchemaKind::Bytes => {
                if r_type == SchemaKind::String {
                    Ok(())
                } else {
                    Err(Error::CompatibilityError(String::from(
                        "Types must be the string",
                    )))
                }
            }
            _ => Err(Error::CompatibilityError(String::from(
                "Unknown type for writer schema. Make sure that the type is a valid one",
            )))
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
        assert_eq!("Schemas are not compatible. 'All elements in union must match for both schemas'",
            SchemaCompatibility::can_read(&int_string_union_schema(), &int_union_schema()).unwrap_err().to_string()
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
        assert_eq!("Schemas are not compatible. 'Field oldfield2 in reader schema must have a default value'", SchemaCompatibility::can_read(
            &reader_schema,
            &writer_schema()
        ).unwrap_err().to_string());

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
        assert_eq!("Schemas are not compatible. 'Field oldfield1 in reader schema must have a default value'", SchemaCompatibility::can_read(
            &reader_schema,
            &writer_schema()
        ).unwrap_err().to_string());

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
        assert_eq!("Schemas are not compatible. 'Field oldfield2 in reader schema must have a default value'",SchemaCompatibility::can_read(
            &reader_schema,
            &writer_schema()
        ).unwrap_err().to_string());

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
        assert_eq!("Schemas are not compatible. 'Field newfield1 in reader schema must have a default value'", SchemaCompatibility::can_read(
            &writer_schema(), 
            &reader_schema).unwrap_err().to_string());
        assert_eq!("Schemas are not compatible. 'Field oldfield2 in reader schema must have a default value'", SchemaCompatibility::can_read(
            &reader_schema,
            &writer_schema()
        ).unwrap_err().to_string());

        Ok(())
    }

    #[test]
    fn test_array_writer_schema() {
        let valid_reader = string_array_schema();
        let invalid_reader = string_map_schema();

        assert!(SchemaCompatibility::can_read(&string_array_schema(), &valid_reader).is_ok());
        assert_eq!("Schemas are not compatible. 'Unknown type for writer schema. Make sure that the type is a valid one'", SchemaCompatibility::can_read(
            &string_array_schema(),
            &invalid_reader
        ).unwrap_err().to_string());
    }

    #[test]
    fn test_primitive_writer_schema() {
        let valid_reader = Schema::String;
        assert!(SchemaCompatibility::can_read(&Schema::String, &valid_reader).is_ok());
        assert_eq!("Schemas are not compatible. 'The type in the reader schema should be long, float or double in order to read int types'", SchemaCompatibility::can_read(
            &Schema::Int,
            &Schema::String
        ).unwrap_err().to_string());
    }

    #[test]
    fn test_union_reader_writer_subset_incompatibility() {
        // reader union schema must contain all writer union branches
        let union_writer = union_schema(vec![Schema::Int, Schema::String]);
        let union_reader = union_schema(vec![Schema::String]);

        assert_eq!(
            "Schemas are not compatible. 'All elements in union must match for both schemas'",
            SchemaCompatibility::can_read(&union_writer, &union_reader)
                .unwrap_err()
                .to_string()
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
            "Schemas are not compatible. 'Field field1 in reader schema does not match the type in the writer schema'",
            SchemaCompatibility::can_read(&string_schema, &int_schema).unwrap_err().to_string()
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
            "Schemas are not compatible. 'reader's symbols must contain all writer's symbols'",
            SchemaCompatibility::can_read(&enum_schema2, &enum_schema1)
                .unwrap_err()
                .to_string()
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
            "Schemas are not compatible. 'Schemas missmatch'",
            SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema)
                .unwrap_err()
                .to_string()
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
            "Schemas are not compatible. 'Schemas missmatch'",
            SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema)
                .unwrap_err()
                .to_string()
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
            "Schemas are not compatible. 'Schemas missmatch'",
            SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema)
                .unwrap_err()
                .to_string()
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
            "Schemas are not compatible. 'Schemas missmatch'",
            SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema)
                .unwrap_err()
                .to_string()
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
        use serde::{Deserialize, Serialize};

        const RAW_SCHEMA_V1: &str = r#"
        {
            "type": "record",
            "name": "Conference",
            "namespace": "advdaba",
            "fields": [
                {"type": "string", "name": "name"},
                {"type": "long", "name": "date"}
            ]
        }"#;
        const RAW_SCHEMA_V2: &str = r#"
        {
            "type": "record",
            "name": "Conference",
            "namespace": "advdaba",
            "fields": [
                {"type": "string", "name": "name"},
                {"type": "long", "name": "date", "aliases" : [ "time" ]}
            ]
        }"#;

        #[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
        pub struct Conference {
            pub name: String,
            pub date: i64,
        }
        #[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
        pub struct ConferenceV2 {
            pub name: String,
            pub time: i64,
        }

        let schema_v1 = Schema::parse_str(RAW_SCHEMA_V1)?;
        let schema_v2 = Schema::parse_str(RAW_SCHEMA_V2)?;

        assert!(SchemaCompatibility::can_read(&schema_v1, &schema_v2).is_ok());

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
                "Schemas are not compatible. 'Field success in reader schema does not match the type in the writer schema'"
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
                    "Schemas are not compatible. 'Field max_values in reader schema does not match the type in the writer schema'"
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
