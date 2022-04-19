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

use crate::{
    schema::{NamesRef, Namespace, ResolvedSchema, Schema, SchemaKind},
    types::{Value, ValueKind},
    util::{zig_i32, zig_i64},
    AvroResult, Error,
};
use std::convert::{TryFrom, TryInto};

/// Encode a `Value` into avro format.
///
/// **NOTE** This will not perform schema validation. The value is assumed to
/// be valid with regards to the schema. Schema are needed only to guide the
/// encoding for complex type values.
pub fn encode(value: &Value, schema: &Schema, buffer: &mut Vec<u8>) -> AvroResult<()> {
    let rs = ResolvedSchema::try_from(schema)?;
    encode_internal(value, schema, rs.get_names(), &None, buffer)
}

fn encode_bytes<B: AsRef<[u8]> + ?Sized>(s: &B, buffer: &mut Vec<u8>) {
    let bytes = s.as_ref();
    encode_long(bytes.len() as i64, buffer);
    buffer.extend_from_slice(bytes);
}

fn encode_long(i: i64, buffer: &mut Vec<u8>) {
    zig_i64(i, buffer)
}

fn encode_int(i: i32, buffer: &mut Vec<u8>) {
    zig_i32(i, buffer)
}

pub(crate) fn encode_internal(
    value: &Value,
    schema: &Schema,
    names: &NamesRef,
    enclosing_namespace: &Namespace,
    buffer: &mut Vec<u8>,
) -> AvroResult<()> {
    if let Schema::Ref { ref name } = schema {
        let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
        let resolved = *names
            .get(&fully_qualified_name)
            .ok_or(Error::SchemaResolutionError(fully_qualified_name))?;
        return encode_internal(value, resolved, names, enclosing_namespace, buffer);
    }

    match value {
        Value::Null => (),
        Value::Boolean(b) => buffer.push(if *b { 1u8 } else { 0u8 }),
        // Pattern | Pattern here to signify that these _must_ have the same encoding.
        Value::Int(i) | Value::Date(i) | Value::TimeMillis(i) => encode_int(*i, buffer),
        Value::Long(i)
        | Value::TimestampMillis(i)
        | Value::TimestampMicros(i)
        | Value::TimeMicros(i) => encode_long(*i, buffer),
        Value::Float(x) => buffer.extend_from_slice(&x.to_le_bytes()),
        Value::Double(x) => buffer.extend_from_slice(&x.to_le_bytes()),
        Value::Decimal(decimal) => match schema {
            Schema::Decimal { inner, .. } => match *inner.clone() {
                Schema::Fixed { size, .. } => {
                    let bytes = decimal.to_sign_extended_bytes_with_len(size).unwrap();
                    let num_bytes = bytes.len();
                    if num_bytes != size {
                        return Err(Error::EncodeDecimalAsFixedError(num_bytes, size));
                    }
                    encode(&Value::Fixed(size, bytes), inner, buffer)?
                }
                Schema::Bytes => encode(&Value::Bytes(decimal.try_into()?), inner, buffer)?,
                _ => {
                    return Err(Error::ResolveDecimalSchema(SchemaKind::from(
                        *inner.clone(),
                    )));
                }
            },
            _ => {
                return Err(Error::EncodeValueAsSchemaError {
                    value_kind: ValueKind::Decimal,
                    supported_schema: vec![SchemaKind::Decimal],
                });
            }
        },
        &Value::Duration(duration) => {
            let slice: [u8; 12] = duration.into();
            buffer.extend_from_slice(&slice);
        }
        Value::Uuid(uuid) => encode_bytes(&uuid.to_string(), buffer),
        Value::Bytes(bytes) => match *schema {
            Schema::Bytes => encode_bytes(bytes, buffer),
            Schema::Fixed { .. } => buffer.extend(bytes),
            _ => {
                return Err(Error::EncodeValueAsSchemaError {
                    value_kind: ValueKind::Bytes,
                    supported_schema: vec![SchemaKind::Bytes, SchemaKind::Fixed],
                });
            }
        },
        Value::String(s) => match *schema {
            Schema::String => {
                encode_bytes(s, buffer);
            }
            Schema::Enum { ref symbols, .. } => {
                if let Some(index) = symbols.iter().position(|item| item == s) {
                    encode_int(index as i32, buffer);
                } else {
                    error!("Invalid symbol string {:?}.", &s[..]);
                    return Err(Error::GetEnumSymbol(s.clone()));
                }
            }
            _ => {
                return Err(Error::EncodeValueAsSchemaError {
                    value_kind: ValueKind::String,
                    supported_schema: vec![SchemaKind::String, SchemaKind::Enum],
                });
            }
        },
        Value::Fixed(_, bytes) => buffer.extend(bytes),
        Value::Enum(i, _) => encode_int(*i as i32, buffer),
        Value::Union(idx, item) => {
            if let Schema::Union(ref inner) = *schema {
                let inner_schema = inner
                    .schemas
                    .get(*idx as usize)
                    .expect("Invalid Union validation occurred");
                encode_long(*idx as i64, buffer);
                encode_internal(&*item, inner_schema, names, enclosing_namespace, buffer)?;
            } else {
                error!("invalid schema type for Union: {:?}", schema);
                return Err(Error::EncodeValueAsSchemaError {
                    value_kind: ValueKind::Union,
                    supported_schema: vec![SchemaKind::Union],
                });
            }
        }
        Value::Array(items) => {
            if let Schema::Array(ref inner) = *schema {
                if !items.is_empty() {
                    encode_long(items.len() as i64, buffer);
                    for item in items.iter() {
                        encode_internal(item, inner, names, enclosing_namespace, buffer)?;
                    }
                }
                buffer.push(0u8);
            } else {
                error!("invalid schema type for Array: {:?}", schema);
                return Err(Error::EncodeValueAsSchemaError {
                    value_kind: ValueKind::Array,
                    supported_schema: vec![SchemaKind::Array],
                });
            }
        }
        Value::Map(items) => {
            if let Schema::Map(ref inner) = *schema {
                if !items.is_empty() {
                    encode_long(items.len() as i64, buffer);
                    for (key, value) in items {
                        encode_bytes(key, buffer);
                        encode_internal(value, inner, names, enclosing_namespace, buffer)?;
                    }
                }
                buffer.push(0u8);
            } else {
                error!("invalid schema type for Map: {:?}", schema);
                return Err(Error::EncodeValueAsSchemaError {
                    value_kind: ValueKind::Map,
                    supported_schema: vec![SchemaKind::Map],
                });
            }
        }
        Value::Record(fields) => {
            if let Schema::Record {
                ref name,
                fields: ref schema_fields,
                ..
            } = *schema
            {
                let record_namespace = name.fully_qualified_name(enclosing_namespace).namespace;
                for (i, &(_, ref value)) in fields.iter().enumerate() {
                    encode_internal(
                        value,
                        &schema_fields[i].schema,
                        names,
                        &record_namespace,
                        buffer,
                    )?;
                }
            } else {
                error!("invalid schema type for Record: {:?}", schema);
                return Err(Error::EncodeValueAsSchemaError {
                    value_kind: ValueKind::Record,
                    supported_schema: vec![SchemaKind::Record],
                });
            }
        }
    };
    Ok(())
}

pub fn encode_to_vec(value: &Value, schema: &Schema) -> AvroResult<Vec<u8>> {
    let mut buffer = Vec::new();
    encode(value, schema, &mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
#[allow(clippy::expect_fun_call)]
pub(crate) mod tests {
    use super::*;
    use std::collections::HashMap;
    pub(crate) fn success(value: &Value, schema: &Schema) -> String {
        format!(
            "Value: {:?}\n should encode with schema:\n{:?}",
            &value, &schema
        )
    }

    #[test]
    fn test_encode_empty_array() {
        let mut buf = Vec::new();
        let empty: Vec<Value> = Vec::new();
        encode(
            &Value::Array(empty.clone()),
            &Schema::Array(Box::new(Schema::Int)),
            &mut buf,
        )
        .expect(&success(
            &Value::Array(empty),
            &Schema::Array(Box::new(Schema::Int)),
        ));
        assert_eq!(vec![0u8], buf);
    }

    #[test]
    fn test_encode_empty_map() {
        let mut buf = Vec::new();
        let empty: HashMap<String, Value> = HashMap::new();
        encode(
            &Value::Map(empty.clone()),
            &Schema::Map(Box::new(Schema::Int)),
            &mut buf,
        )
        .expect(&success(
            &Value::Map(empty),
            &Schema::Map(Box::new(Schema::Int)),
        ));
        assert_eq!(vec![0u8], buf);
    }

    #[test]
    fn test_avro_3433_recursive_definition_encode_record() {
        let mut buf = Vec::new();
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
        let outer_value =
            Value::Record(vec![("a".into(), inner_value1), ("b".into(), inner_value2)]);
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3433_recursive_definition_encode_array() {
        let mut buf = Vec::new();
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
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3433_recursive_definition_encode_map() {
        let mut buf = Vec::new();
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
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3433_recursive_definition_encode_record_wrapper() {
        let mut buf = Vec::new();
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
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3433_recursive_definition_encode_map_and_array() {
        let mut buf = Vec::new();
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
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3433_recursive_definition_encode_union() {
        let mut buf = Vec::new();
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
        let outer_value1 = Value::Record(vec![
            ("a".into(), Value::Union(1, Box::new(inner_value1))),
            ("b".into(), inner_value2.clone()),
        ]);
        encode(&outer_value1, &schema, &mut buf).expect(&success(&outer_value1, &schema));
        assert!(!buf.is_empty());

        buf.drain(..);
        let outer_value2 = Value::Record(vec![
            ("a".into(), Value::Union(0, Box::new(Value::Null))),
            ("b".into(), inner_value2),
        ]);
        encode(&outer_value2, &schema, &mut buf).expect(&success(&outer_value1, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3448_proper_multi_level_encoding_outer_namespace() {
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

        let mut buf = Vec::new();
        encode(&outer_record_variation_1, &schema, &mut buf)
            .expect(&success(&outer_record_variation_1, &schema));
        assert!(!buf.is_empty());
        buf.drain(..);
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        assert!(!buf.is_empty());
        buf.drain(..);
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3448_proper_multi_level_encoding_middle_namespace() {
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

        let mut buf = Vec::new();
        encode(&outer_record_variation_1, &schema, &mut buf)
            .expect(&success(&outer_record_variation_1, &schema));
        assert!(!buf.is_empty());
        buf.drain(..);
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        assert!(!buf.is_empty());
        buf.drain(..);
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3448_proper_multi_level_encoding_inner_namespace() {
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

        let mut buf = Vec::new();
        encode(&outer_record_variation_1, &schema, &mut buf)
            .expect(&success(&outer_record_variation_1, &schema));
        assert!(!buf.is_empty());
        buf.drain(..);
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        assert!(!buf.is_empty());
        buf.drain(..);
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        assert!(!buf.is_empty());
    }
}
