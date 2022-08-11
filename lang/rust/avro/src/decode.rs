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
    decimal::Decimal,
    duration::Duration,
    schema::{Name, Namespace, ResolvedSchema, Schema},
    types::Value,
    util::{safe_len, zag_i32, zag_i64},
    AvroResult, Error,
};
use std::{
    borrow::Borrow,
    collections::HashMap,
    convert::TryFrom,
    io::{ErrorKind, Read},
    str::FromStr,
};
use uuid::Uuid;

#[inline]
fn decode_long<R: Read>(reader: &mut R) -> AvroResult<Value> {
    zag_i64(reader).map(Value::Long)
}

#[inline]
fn decode_int<R: Read>(reader: &mut R) -> AvroResult<Value> {
    zag_i32(reader).map(Value::Int)
}

#[inline]
fn decode_len<R: Read>(reader: &mut R) -> AvroResult<usize> {
    let len = zag_i64(reader)?;
    safe_len(usize::try_from(len).map_err(|e| Error::ConvertI64ToUsize(e, len))?)
}

/// Decode the length of a sequence.
///
/// Maps and arrays are 0-terminated, 0i64 is also encoded as 0 in Avro reading a length of 0 means
/// the end of the map or array.
fn decode_seq_len<R: Read>(reader: &mut R) -> AvroResult<usize> {
    let raw_len = zag_i64(reader)?;
    safe_len(
        usize::try_from(match raw_len.cmp(&0) {
            std::cmp::Ordering::Equal => return Ok(0),
            std::cmp::Ordering::Less => {
                let _size = zag_i64(reader)?;
                raw_len.checked_neg().ok_or(Error::IntegerOverflow)?
            }
            std::cmp::Ordering::Greater => raw_len,
        })
        .map_err(|e| Error::ConvertI64ToUsize(e, raw_len))?,
    )
}

/// Decode a `Value` from avro format given its `Schema`.
pub fn decode<R: Read>(schema: &Schema, reader: &mut R) -> AvroResult<Value> {
    let rs = ResolvedSchema::try_from(schema)?;
    decode_internal(schema, rs.get_names(), &None, reader)
}

pub(crate) fn decode_internal<R: Read, S: Borrow<Schema>>(
    schema: &Schema,
    names: &HashMap<Name, S>,
    enclosing_namespace: &Namespace,
    reader: &mut R,
) -> AvroResult<Value> {
    match *schema {
        Schema::Null => Ok(Value::Null),
        Schema::Boolean => {
            let mut buf = [0u8; 1];
            match reader.read_exact(&mut buf[..]) {
                Ok(_) => match buf[0] {
                    0u8 => Ok(Value::Boolean(false)),
                    1u8 => Ok(Value::Boolean(true)),
                    _ => Err(Error::BoolValue(buf[0])),
                },
                Err(io_err) => {
                    if let ErrorKind::UnexpectedEof = io_err.kind() {
                        Ok(Value::Null)
                    } else {
                        Err(Error::ReadBoolean(io_err))
                    }
                }
            }
        }
        Schema::Decimal { ref inner, .. } => match &**inner {
            Schema::Fixed { .. } => {
                match decode_internal(inner, names, enclosing_namespace, reader)? {
                    Value::Fixed(_, bytes) => Ok(Value::Decimal(Decimal::from(bytes))),
                    value => Err(Error::FixedValue(value.into())),
                }
            }
            Schema::Bytes => match decode_internal(inner, names, enclosing_namespace, reader)? {
                Value::Bytes(bytes) => Ok(Value::Decimal(Decimal::from(bytes))),
                value => Err(Error::BytesValue(value.into())),
            },
            schema => Err(Error::ResolveDecimalSchema(schema.into())),
        },
        Schema::Uuid => Ok(Value::Uuid(
            Uuid::from_str(
                match decode_internal(&Schema::String, names, enclosing_namespace, reader)? {
                    Value::String(ref s) => s,
                    value => return Err(Error::GetUuidFromStringValue(value.into())),
                },
            )
            .map_err(Error::ConvertStrToUuid)?,
        )),
        Schema::Int => decode_int(reader),
        Schema::Date => zag_i32(reader).map(Value::Date),
        Schema::TimeMillis => zag_i32(reader).map(Value::TimeMillis),
        Schema::Long => decode_long(reader),
        Schema::TimeMicros => zag_i64(reader).map(Value::TimeMicros),
        Schema::TimestampMillis => zag_i64(reader).map(Value::TimestampMillis),
        Schema::TimestampMicros => zag_i64(reader).map(Value::TimestampMicros),
        Schema::Duration => {
            let mut buf = [0u8; 12];
            reader.read_exact(&mut buf).map_err(Error::ReadDuration)?;
            Ok(Value::Duration(Duration::from(buf)))
        }
        Schema::Float => {
            let mut buf = [0u8; std::mem::size_of::<f32>()];
            reader.read_exact(&mut buf[..]).map_err(Error::ReadFloat)?;
            Ok(Value::Float(f32::from_le_bytes(buf)))
        }
        Schema::Double => {
            let mut buf = [0u8; std::mem::size_of::<f64>()];
            reader.read_exact(&mut buf[..]).map_err(Error::ReadDouble)?;
            Ok(Value::Double(f64::from_le_bytes(buf)))
        }
        Schema::Bytes => {
            let len = decode_len(reader)?;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf).map_err(Error::ReadBytes)?;
            Ok(Value::Bytes(buf))
        }
        Schema::String => {
            let len = decode_len(reader)?;
            let mut buf = vec![0u8; len];
            match reader.read_exact(&mut buf) {
                Ok(_) => Ok(Value::String(
                    String::from_utf8(buf).map_err(Error::ConvertToUtf8)?,
                )),
                Err(io_err) => {
                    if let ErrorKind::UnexpectedEof = io_err.kind() {
                        Ok(Value::Null)
                    } else {
                        Err(Error::ReadString(io_err))
                    }
                }
            }
        }
        Schema::Fixed { size, .. } => {
            let mut buf = vec![0u8; size];
            reader
                .read_exact(&mut buf)
                .map_err(|e| Error::ReadFixed(e, size))?;
            Ok(Value::Fixed(size, buf))
        }
        Schema::Array(ref inner) => {
            let mut items = Vec::new();

            loop {
                let len = decode_seq_len(reader)?;
                if len == 0 {
                    break;
                }

                items.reserve(len);
                for _ in 0..len {
                    items.push(decode_internal(inner, names, enclosing_namespace, reader)?);
                }
            }

            Ok(Value::Array(items))
        }
        Schema::Map(ref inner) => {
            let mut items = HashMap::new();

            loop {
                let len = decode_seq_len(reader)?;
                if len == 0 {
                    break;
                }

                items.reserve(len);
                for _ in 0..len {
                    match decode_internal(&Schema::String, names, enclosing_namespace, reader)? {
                        Value::String(key) => {
                            let value = decode_internal(inner, names, enclosing_namespace, reader)?;
                            items.insert(key, value);
                        }
                        value => return Err(Error::MapKeyType(value.into())),
                    }
                }
            }

            Ok(Value::Map(items))
        }
        Schema::Union(ref inner) => match zag_i64(reader) {
            Ok(index) => {
                let variants = inner.variants();
                let variant = variants
                    .get(usize::try_from(index).map_err(|e| Error::ConvertI64ToUsize(e, index))?)
                    .ok_or(Error::GetUnionVariant {
                        index,
                        num_variants: variants.len(),
                    })?;
                let value = decode_internal(variant, names, enclosing_namespace, reader)?;
                Ok(Value::Union(index as u32, Box::new(value)))
            }
            Err(Error::ReadVariableIntegerBytes(io_err)) => {
                if let ErrorKind::UnexpectedEof = io_err.kind() {
                    Ok(Value::Union(0, Box::new(Value::Null)))
                } else {
                    Err(Error::ReadVariableIntegerBytes(io_err))
                }
            }
            Err(io_err) => Err(io_err),
        },
        Schema::Record {
            ref name,
            ref fields,
            ..
        } => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            // Benchmarks indicate ~10% improvement using this method.
            let mut items = Vec::with_capacity(fields.len());
            for field in fields {
                // TODO: This clone is also expensive. See if we can do away with it...
                items.push((
                    field.name.clone(),
                    decode_internal(
                        &field.schema,
                        names,
                        &fully_qualified_name.namespace,
                        reader,
                    )?,
                ));
            }
            Ok(Value::Record(items))
        }
        Schema::Enum { ref symbols, .. } => {
            Ok(if let Value::Int(raw_index) = decode_int(reader)? {
                let index = usize::try_from(raw_index)
                    .map_err(|e| Error::ConvertI32ToUsize(e, raw_index))?;
                if (0..=symbols.len()).contains(&index) {
                    let symbol = symbols[index].clone();
                    Value::Enum(raw_index as u32, symbol)
                } else {
                    return Err(Error::GetEnumValue {
                        index,
                        nsymbols: symbols.len(),
                    });
                }
            } else {
                return Err(Error::GetEnumUnknownIndexValue);
            })
        }
        Schema::Ref { ref name } => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            if let Some(resolved) = names.get(&fully_qualified_name) {
                decode_internal(
                    resolved.borrow(),
                    names,
                    &fully_qualified_name.namespace,
                    reader,
                )
            } else {
                Err(Error::SchemaResolutionError(fully_qualified_name))
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_fun_call)]
mod tests {
    use crate::{
        decode::decode,
        encode::{encode, tests::success},
        schema::Schema,
        types::{
            Value,
            Value::{Array, Int, Map},
        },
        Decimal,
    };
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;

    #[test]
    fn test_decode_array_without_size() {
        let mut input: &[u8] = &[6, 2, 4, 6, 0];
        let result = decode(&Schema::Array(Box::new(Schema::Int)), &mut input);
        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result.unwrap());
    }

    #[test]
    fn test_decode_array_with_size() {
        let mut input: &[u8] = &[5, 6, 2, 4, 6, 0];
        let result = decode(&Schema::Array(Box::new(Schema::Int)), &mut input);
        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result.unwrap());
    }

    #[test]
    fn test_decode_map_without_size() {
        let mut input: &[u8] = &[0x02, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let result = decode(&Schema::Map(Box::new(Schema::Int)), &mut input);
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result.unwrap());
    }

    #[test]
    fn test_decode_map_with_size() {
        let mut input: &[u8] = &[0x01, 0x0C, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let result = decode(&Schema::Map(Box::new(Schema::Int)), &mut input);
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result.unwrap());
    }

    #[test]
    fn test_negative_decimal_value() {
        use crate::{encode::encode, schema::Name};
        use num_bigint::ToBigInt;
        let inner = Box::new(Schema::Fixed {
            size: 2,
            doc: None,
            name: Name::new("decimal").unwrap(),
            aliases: None,
        });
        let schema = Schema::Decimal {
            inner,
            precision: 4,
            scale: 2,
        };
        let bigint = (-423).to_bigint().unwrap();
        let value = Value::Decimal(Decimal::from(bigint.to_signed_bytes_be()));

        let mut buffer = Vec::new();
        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));

        let mut bytes = &buffer[..];
        let result = decode(&schema, &mut bytes).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn test_decode_decimal_with_bigger_than_necessary_size() {
        use crate::{encode::encode, schema::Name};
        use num_bigint::ToBigInt;
        let inner = Box::new(Schema::Fixed {
            size: 13,
            name: Name::new("decimal").unwrap(),
            aliases: None,
            doc: None,
        });
        let schema = Schema::Decimal {
            inner,
            precision: 4,
            scale: 2,
        };
        let value = Value::Decimal(Decimal::from(
            ((-423).to_bigint().unwrap()).to_signed_bytes_be(),
        ));
        let mut buffer = Vec::<u8>::new();

        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));
        let mut bytes: &[u8] = &buffer[..];
        let result = decode(&schema, &mut bytes).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn test_avro_3448_recursive_definition_decode_union() {
        // if encoding fails in this test check the corresponding test in encode
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":[ "null", {
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
        let mut buf = Vec::new();
        encode(&outer_value1, &schema, &mut buf).expect(&success(&outer_value1, &schema));
        assert!(!buf.is_empty());
        let mut bytes = &buf[..];
        assert_eq!(
            outer_value1,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        let outer_value2 = Value::Record(vec![
            ("a".into(), Value::Union(0, Box::new(Value::Null))),
            ("b".into(), inner_value2),
        ]);
        encode(&outer_value2, &schema, &mut buf).expect(&success(&outer_value2, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_value2,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {:?}\n",
                &schema
            ))
        );
    }

    #[test]
    fn test_avro_3448_recursive_definition_decode_array() {
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
                    "type": "Inner"
                }
            ]
        }"#,
        )
        .unwrap();

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value = Value::Record(vec![
            ("a".into(), Value::Array(vec![inner_value1])),
            ("b".into(), inner_value2),
        ]);
        let mut buf = Vec::new();
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_value,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {:?}\n",
                &schema
            ))
        )
    }

    #[test]
    fn test_avro_3448_recursive_definition_decode_map() {
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
                    "type": "Inner"
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
                Value::Map(vec![("akey".into(), inner_value1)].into_iter().collect()),
            ),
            ("b".into(), inner_value2),
        ]);
        let mut buf = Vec::new();
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_value,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {:?}\n",
                &schema
            ))
        )
    }

    #[test]
    fn test_avro_3448_proper_multi_level_decoding_middle_namespace() {
        // if encoding fails in this test check the corresponding test in encode
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
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_1,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_2,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_3,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );
    }

    #[test]
    fn test_avro_3448_proper_multi_level_decoding_inner_namespace() {
        // if encoding fails in this test check the corresponding test in encode
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
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_1,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_2,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_3,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );
    }
}
