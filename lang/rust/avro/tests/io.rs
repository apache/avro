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

//! Port of https://github.com/apache/avro/blob/release-1.9.1/lang/py/test/test_io.py
use apache_avro::{from_avro_datum, to_avro_datum, types::Value, Error, Schema};
use apache_avro_test_helper::TestResult;
use pretty_assertions::assert_eq;
use std::{io::Cursor, sync::OnceLock};

fn schemas_to_validate() -> &'static Vec<(&'static str, Value)> {
    static SCHEMAS_TO_VALIDATE_ONCE: OnceLock<Vec<(&'static str, Value)>> = OnceLock::new();
    SCHEMAS_TO_VALIDATE_ONCE.get_or_init(|| {
        vec![
            (r#""null""#, Value::Null),
            (r#""boolean""#, Value::Boolean(true)),
            (
                r#""string""#,
                Value::String("adsfasdf09809dsf-=adsf".to_string()),
            ),
            (
                r#""bytes""#,
                Value::Bytes("12345abcd".to_string().into_bytes()),
            ),
            (r#""int""#, Value::Int(1234)),
            (r#""long""#, Value::Long(1234)),
            (r#""float""#, Value::Float(1234.0)),
            (r#""double""#, Value::Double(1234.0)),
            (
                r#"{"type": "fixed", "name": "Test", "size": 1}"#,
                Value::Fixed(1, vec![b'B']),
            ),
            (
                r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"]}"#,
                Value::Enum(1, "B".to_string()),
            ),
            (
                r#"{"type": "array", "items": "long"}"#,
                Value::Array(vec![Value::Long(1), Value::Long(3), Value::Long(2)]),
            ),
            (
                r#"{"type": "map", "values": "long"}"#,
                Value::Map(
                    [
                        ("a".to_string(), Value::Long(1i64)),
                        ("b".to_string(), Value::Long(3i64)),
                        ("c".to_string(), Value::Long(2i64)),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                ),
            ),
            (
                r#"["string", "null", "long"]"#,
                Value::Union(1, Box::new(Value::Null)),
            ),
            (
                r#"{"type": "record", "name": "Test", "fields": [{"name": "f", "type": "long"}]}"#,
                Value::Record(vec![("f".to_string(), Value::Long(1))]),
            ),
        ]
    })
}

fn binary_encodings() -> &'static Vec<(i64, Vec<u8>)> {
    static BINARY_ENCODINGS_ONCE: OnceLock<Vec<(i64, Vec<u8>)>> = OnceLock::new();
    BINARY_ENCODINGS_ONCE.get_or_init(|| {
        vec![
            (0, vec![0x00]),
            (-1, vec![0x01]),
            (1, vec![0x02]),
            (-2, vec![0x03]),
            (2, vec![0x04]),
            (-64, vec![0x7f]),
            (64, vec![0x80, 0x01]),
            (8192, vec![0x80, 0x80, 0x01]),
            (-8193, vec![0x81, 0x80, 0x01]),
        ]
    })
}

fn default_value_examples() -> &'static Vec<(&'static str, &'static str, Value)> {
    static DEFAULT_VALUE_EXAMPLES_ONCE: OnceLock<Vec<(&'static str, &'static str, Value)>> =
        OnceLock::new();
    DEFAULT_VALUE_EXAMPLES_ONCE.get_or_init(|| {
        vec![
            (r#""null""#, "null", Value::Null),
            (r#""boolean""#, "true", Value::Boolean(true)),
            (r#""string""#, r#""foo""#, Value::String("foo".to_string())),
            (r#""bytes""#, r#""a""#, Value::Bytes(vec![97])), // ASCII 'a' => one byte
            (r#""bytes""#, r#""\u00FF""#, Value::Bytes(vec![195, 191])), // The value is between U+0080 and U+07FF => two bytes
            (r#""int""#, "5", Value::Int(5)),
            (r#""long""#, "5", Value::Long(5)),
            (r#""float""#, "1.1", Value::Float(1.1)),
            (r#""double""#, "1.1", Value::Double(1.1)),
            (
                r#"{"type": "fixed", "name": "F", "size": 2}"#,
                r#""a""#,
                Value::Fixed(1, vec![97]),
            ), // ASCII 'a' => one byte
            (
                r#"{"type": "fixed", "name": "F", "size": 2}"#,
                r#""\u00FF""#,
                Value::Fixed(2, vec![195, 191]),
            ), // The value is between U+0080 and U+07FF => two bytes
            (
                r#"{"type": "enum", "name": "F", "symbols": ["FOO", "BAR"]}"#,
                r#""FOO""#,
                Value::Enum(0, "FOO".to_string()),
            ),
            (
                r#"{"type": "array", "items": "int"}"#,
                "[1, 2, 3]",
                Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            ),
            (
                r#"{"type": "map", "values": "int"}"#,
                r#"{"a": 1, "b": 2}"#,
                Value::Map(
                    [
                        ("a".to_string(), Value::Int(1)),
                        ("b".to_string(), Value::Int(2)),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                ),
            ),
            (
                r#"["int", "null"]"#,
                "5",
                Value::Union(0, Box::new(Value::Int(5))),
            ),
            (
                r#"{"type": "record", "name": "F", "fields": [{"name": "A", "type": "int"}]}"#,
                r#"{"A": 5}"#,
                Value::Record(vec![("A".to_string(), Value::Int(5))]),
            ),
            (
                r#"["null", "int"]"#,
                "null",
                Value::Union(0, Box::new(Value::Null)),
            ),
        ]
    })
}

fn long_record_schema() -> &'static Schema {
    static LONG_RECORD_SCHEMA_ONCE: OnceLock<Schema> = OnceLock::new();
    LONG_RECORD_SCHEMA_ONCE.get_or_init(|| {
        Schema::parse_str(
            r#"
{
    "type": "record",
    "name": "Test",
    "fields": [
        {"name": "A", "type": "int"},
        {"name": "B", "type": "int"},
        {"name": "C", "type": "int"},
        {"name": "D", "type": "int"},
        {"name": "E", "type": "int"},
        {"name": "F", "type": "int"},
        {"name": "G", "type": "int"}
    ]
}
"#,
        )
        .unwrap()
    })
}

fn long_record_datum() -> &'static Value {
    static LONG_RECORD_DATUM_ONCE: OnceLock<Value> = OnceLock::new();
    LONG_RECORD_DATUM_ONCE.get_or_init(|| {
        Value::Record(vec![
            ("A".to_string(), Value::Int(1)),
            ("B".to_string(), Value::Int(2)),
            ("C".to_string(), Value::Int(3)),
            ("D".to_string(), Value::Int(4)),
            ("E".to_string(), Value::Int(5)),
            ("F".to_string(), Value::Int(6)),
            ("G".to_string(), Value::Int(7)),
        ])
    })
}

#[test]
fn test_validate() -> TestResult {
    for (raw_schema, value) in schemas_to_validate().iter() {
        let schema = Schema::parse_str(raw_schema)?;
        assert!(
            value.validate(&schema),
            "value {value:?} does not validate schema: {raw_schema}"
        );
    }

    Ok(())
}

#[test]
fn test_round_trip() -> TestResult {
    for (raw_schema, value) in schemas_to_validate().iter() {
        let schema = Schema::parse_str(raw_schema)?;
        let encoded = to_avro_datum(&schema, value.clone()).unwrap();
        let decoded = from_avro_datum(&schema, &mut Cursor::new(encoded), None).unwrap();
        assert_eq!(value, &decoded);
    }

    Ok(())
}

#[test]
fn test_binary_int_encoding() -> TestResult {
    for (number, hex_encoding) in binary_encodings().iter() {
        let encoded = to_avro_datum(&Schema::Int, Value::Int(*number as i32))?;
        assert_eq!(&encoded, hex_encoding);
    }

    Ok(())
}

#[test]
fn test_binary_long_encoding() -> TestResult {
    for (number, hex_encoding) in binary_encodings().iter() {
        let encoded = to_avro_datum(&Schema::Long, Value::Long(*number))?;
        assert_eq!(&encoded, hex_encoding);
    }

    Ok(())
}

#[test]
fn test_schema_promotion() -> TestResult {
    // Each schema is present in order of promotion (int -> long, long -> float, float -> double)
    // Each value represents the expected decoded value when promoting a value previously encoded with a promotable schema
    let promotable_schemas = [r#""int""#, r#""long""#, r#""float""#, r#""double""#];
    let promotable_values = vec![
        Value::Int(219),
        Value::Long(219),
        Value::Float(219.0),
        Value::Double(219.0),
    ];
    for (i, writer_raw_schema) in promotable_schemas.iter().enumerate() {
        let writer_schema = Schema::parse_str(writer_raw_schema)?;
        let original_value = &promotable_values[i];
        for (j, reader_raw_schema) in promotable_schemas.iter().enumerate().skip(i + 1) {
            let reader_schema = Schema::parse_str(reader_raw_schema)?;
            let encoded = to_avro_datum(&writer_schema, original_value.clone())?;
            let decoded = from_avro_datum(
                &writer_schema,
                &mut Cursor::new(encoded),
                Some(&reader_schema),
            )
            .unwrap_or_else(|_| {
                panic!("failed to decode {original_value:?} with schema: {reader_raw_schema:?}",)
            });
            assert_eq!(decoded, promotable_values[j]);
        }
    }

    Ok(())
}

#[test]
fn test_unknown_symbol() -> TestResult {
    let writer_schema =
        Schema::parse_str(r#"{"type": "enum", "name": "Test", "symbols": ["FOO", "BAR"]}"#)?;
    let reader_schema =
        Schema::parse_str(r#"{"type": "enum", "name": "Test", "symbols": ["BAR", "BAZ"]}"#)?;
    let original_value = Value::Enum(0, "FOO".to_string());
    let encoded = to_avro_datum(&writer_schema, original_value)?;
    let decoded = from_avro_datum(
        &writer_schema,
        &mut Cursor::new(encoded),
        Some(&reader_schema),
    );
    assert!(decoded.is_err());

    Ok(())
}

#[test]
fn test_default_value() -> TestResult {
    for (field_type, default_json, default_datum) in default_value_examples().iter() {
        let reader_schema = Schema::parse_str(&format!(
            r#"{{
                "type": "record",
                "name": "Test",
                "fields": [
                    {{"name": "H", "type": {field_type}, "default": {default_json}}}
                ]
            }}"#
        ))?;
        let datum_to_read = Value::Record(vec![("H".to_string(), default_datum.clone())]);
        let encoded = to_avro_datum(long_record_schema(), long_record_datum().clone())?;
        let datum_read = from_avro_datum(
            long_record_schema(),
            &mut Cursor::new(encoded),
            Some(&reader_schema),
        )?;
        assert_eq!(
            datum_read, datum_to_read,
            "{} -> {}",
            *field_type, *default_json
        );
    }

    Ok(())
}

#[test]
fn test_no_default_value() -> TestResult {
    let reader_schema = Schema::parse_str(
        r#"{
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "H", "type": "int"}
            ]
        }"#,
    )?;
    let encoded = to_avro_datum(long_record_schema(), long_record_datum().clone())?;
    let result = from_avro_datum(
        long_record_schema(),
        &mut Cursor::new(encoded),
        Some(&reader_schema),
    );
    assert!(result.is_err());

    Ok(())
}

#[test]
fn test_projection() -> TestResult {
    let reader_schema = Schema::parse_str(
        r#"
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "E", "type": "int"},
                {"name": "F", "type": "int"}
            ]
        }
    "#,
    )?;
    let datum_to_read = Value::Record(vec![
        ("E".to_string(), Value::Int(5)),
        ("F".to_string(), Value::Int(6)),
    ]);
    let encoded = to_avro_datum(long_record_schema(), long_record_datum().clone())?;
    let datum_read = from_avro_datum(
        long_record_schema(),
        &mut Cursor::new(encoded),
        Some(&reader_schema),
    )?;
    assert_eq!(datum_to_read, datum_read);

    Ok(())
}

#[test]
fn test_field_order() -> TestResult {
    let reader_schema = Schema::parse_str(
        r#"
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "F", "type": "int"},
                {"name": "E", "type": "int"}
            ]
        }
    "#,
    )?;
    let datum_to_read = Value::Record(vec![
        ("F".to_string(), Value::Int(6)),
        ("E".to_string(), Value::Int(5)),
    ]);
    let encoded = to_avro_datum(long_record_schema(), long_record_datum().clone())?;
    let datum_read = from_avro_datum(
        long_record_schema(),
        &mut Cursor::new(encoded),
        Some(&reader_schema),
    )?;
    assert_eq!(datum_to_read, datum_read);

    Ok(())
}

#[test]
fn test_type_exception() -> Result<(), String> {
    let writer_schema = Schema::parse_str(
        r#"
        {
             "type": "record",
             "name": "Test",
             "fields": [
                {"name": "F", "type": "int"},
                {"name": "E", "type": "int"}
             ]
        }
    "#,
    )
    .unwrap();
    let datum_to_write = Value::Record(vec![
        ("E".to_string(), Value::Int(5)),
        ("F".to_string(), Value::String(String::from("Bad"))),
    ]);
    let encoded = to_avro_datum(&writer_schema, datum_to_write);
    match encoded {
        Ok(_) => Err(String::from("Expected ValidationError, got Ok")),
        Err(Error::Validation) => Ok(()),
        Err(ref e) => Err(format!("Expected ValidationError, got {e:?}")),
    }
}
