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

use apache_avro::{
    schema::{Name, RecordField},
    to_avro_datum, to_value,
    types::{Record, Value},
    Codec, Error, Reader, Schema, Writer,
};
use apache_avro_test_helper::init;
use lazy_static::lazy_static;

const PRIMITIVE_EXAMPLES: &[(&str, bool)] = &[
    (r#""null""#, true),
    (r#"{"type": "null"}"#, true),
    (r#""boolean""#, true),
    (r#"{"type": "boolean"}"#, true),
    (r#""string""#, true),
    (r#"{"type": "string"}"#, true),
    (r#""bytes""#, true),
    (r#"{"type": "bytes"}"#, true),
    (r#""int""#, true),
    (r#"{"type": "int"}"#, true),
    (r#""long""#, true),
    (r#"{"type": "long"}"#, true),
    (r#""float""#, true),
    (r#"{"type": "float"}"#, true),
    (r#""double""#, true),
    (r#"{"type": "double"}"#, true),
    (r#""true""#, false),
    (r#"true"#, false),
    (r#"{"no_type": "test"}"#, false),
    (r#"{"type": "panther"}"#, false),
];

const FIXED_EXAMPLES: &[(&str, bool)] = &[
    (r#"{"type": "fixed", "name": "Test", "size": 1}"#, true),
    (
        r#"{
                "type": "fixed",
                "name": "MyFixed",
                "namespace": "org.apache.hadoop.avro",
                "size": 1
            }"#,
        true,
    ),
    (r#"{"type": "fixed", "name": "MissingSize"}"#, false),
    (r#"{"type": "fixed", "size": 314}"#, false),
];

const ENUM_EXAMPLES: &[(&str, bool)] = &[
    (
        r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"]}"#,
        true,
    ),
    (
        r#"{
                "type": "enum",
                "name": "Status",
                "symbols": "Normal Caution Critical"
            }"#,
        false,
    ),
    (
        r#"{
                "type": "enum",
                "name": [ 0, 1, 1, 2, 3, 5, 8 ],
                "symbols": ["Golden", "Mean"]
            }"#,
        false,
    ),
    (
        r#"{
                "type": "enum",
                "symbols" : ["I", "will", "fail", "no", "name"]
            }"#,
        false,
    ),
    (
        r#"{
                "type": "enum",
                 "name": "Test"
                 "symbols" : ["AA", "AA"]
            }"#,
        false,
    ),
];

const ARRAY_EXAMPLES: &[(&str, bool)] = &[
    (r#"{"type": "array", "items": "long"}"#, true),
    (
        r#"{
                "type": "array",
                 "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
            }"#,
        true,
    ),
];

const MAP_EXAMPLES: &[(&str, bool)] = &[
    (r#"{"type": "map", "values": "long"}"#, true),
    (
        r#"{
                "type": "map",
                "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
            }"#,
        true,
    ),
];

const UNION_EXAMPLES: &[(&str, bool)] = &[
    (r#"["string", "null", "long"]"#, true),
    (r#"["null", "null"]"#, false),
    (r#"["long", "long"]"#, false),
    (
        r#"[
                {"type": "array", "items": "long"}
                {"type": "array", "items": "string"}
            ]"#,
        false,
    ),
    // Unions with default values
    (
        r#"{"name": "foo", "type": ["string", "long"], "default": "bar"}"#,
        true,
    ),
    (
        r#"{"name": "foo", "type": ["long", "string"], "default": 1}"#,
        true,
    ),
    (
        r#"{"name": "foo", "type": ["null", "string"], "default": null}"#,
        true,
    ),
    (
        r#"{"name": "foo", "type": ["string", "long"], "default": 1}"#,
        false,
    ),
    (
        r#"{"name": "foo", "type": ["string", "null"], "default": null}"#,
        false,
    ),
    (
        r#"{"name": "foo", "type": ["null", "string"], "default": "null"}"#,
        false,
    ),
    (
        r#"{"name": "foo", "type": ["long", "string"], "default": "str"}"#,
        false,
    ),
];

const RECORD_EXAMPLES: &[(&str, bool)] = &[
    (
        r#"{
                "type": "record",
                "name": "Test",
                "fields": [{"name": "f", "type": "long"}]
            }"#,
        true,
    ),
    (
        r#"{
            "type": "error",
            "name": "Test",
            "fields": [{"name": "f", "type": "long"}]
        }"#,
        false,
    ),
    (
        r#"{
            "type": "record",
            "name": "Node",
            "fields": [
                {"name": "label", "type": "string"},
                {"name": "children", "type": {"type": "array", "items": "Node"}}
            ]
        }"#,
        true,
    ),
    (
        r#"{
            "type": "record",
            "name": "Lisp",
            "fields": [
                {
                    "name": "value",
                    "type": [
                        "null", "string",
                        {
                            "type": "record",
                            "name": "Cons",
                            "fields": [
                                {"name": "car", "type": "Lisp"},
                                {"name": "cdr", "type": "Lisp"}
                            ]
                        }
                    ]
                }
            ]
        }"#,
        true,
    ),
    (
        r#"{
            "type": "record",
            "name": "HandshakeRequest",
            "namespace": "org.apache.avro.ipc",
            "fields": [
                {"name": "clientHash", "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "clientProtocol", "type": ["null", "string"]},
                {"name": "serverHash", "type": "MD5"},
                {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
            ]
        }"#,
        true,
    ),
    (
        r#"{
                "type":"record",
                "name":"HandshakeResponse",
                "namespace":"org.apache.avro.ipc",
                "fields":[
                    {
                        "name":"match",
                        "type":{
                           "type":"enum",
                           "name":"HandshakeMatch",
                           "symbols":["BOTH", "CLIENT", "NONE"]
                        }
                    },
                    {"name":"serverProtocol", "type":["null", "string"]},
                    {
                        "name":"serverHash",
                        "type":["null", {"name":"MD5", "size":16, "type":"fixed"}]
                    },
                    {
                        "name":"meta",
                        "type":["null", {"type":"map", "values":"bytes"}]
                    }
                ]
            }"#,
        true,
    ),
    (
        r#"{
                "type":"record",
                "name":"HandshakeResponse",
                "namespace":"org.apache.avro.ipc",
                "fields":[
                    {
                        "name":"match",
                        "type":{
                            "type":"enum",
                            "name":"HandshakeMatch",
                            "symbols":["BOTH", "CLIENT", "NONE"]
                        }
                    },
                    {"name":"serverProtocol", "type":["null", "string"]},
                    {
                        "name":"serverHash",
                        "type":["null", { "name":"MD5", "size":16, "type":"fixed"}]
                    },
                    {"name":"meta", "type":["null", { "type":"map", "values":"bytes"}]}
                ]
            }"#,
        true,
    ),
    // Unions may not contain more than one schema with the same type, except for the named
    // types record, fixed and enum. For example, unions containing two array types or two map
    // types are not permitted, but two types with different names are permitted.
    // (Names permit efficient resolution when reading and writing unions.)
    (
        r#"{
            "type": "record",
            "name": "ipAddr",
            "fields": [
                {
                    "name": "addr",
                    "type": [
                        {"name": "IPv6", "type": "fixed", "size": 16},
                        {"name": "IPv4", "type": "fixed", "size": 4}
                    ]
                }
            ]
        }"#,
        true,
    ),
    (
        r#"{
                "type": "record",
                "name": "Address",
                "fields": [
                    {"type": "string"},
                    {"type": "string", "name": "City"}
                ]
            }"#,
        false,
    ),
    (
        r#"{
                "type": "record",
                "name": "Event",
                "fields": [{"name": "Sponsor"}, {"name": "City", "type": "string"}]
            }"#,
        false,
    ),
    (
        r#"{
                "type": "record",
                "fields": "His vision, from the constantly passing bars,"
                "name",
                "Rainer"
            }"#,
        false,
    ),
    (
        r#"{
                "name": ["Tom", "Jerry"],
                "type": "record",
                "fields": [{"name": "name", "type": "string"}]
            }"#,
        false,
    ),
];

const DOC_EXAMPLES: &[(&str, bool)] = &[
    (
        r#"{
                "type": "record",
                "name": "TestDoc",
                "doc":  "Doc string",
                "fields": [{"name": "name", "type": "string", "doc" : "Doc String"}]
            }"#,
        true,
    ),
    (
        r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"], "doc": "Doc String"}"#,
        true,
    ),
    (
        r#"{"type": "fixed", "name": "Test", "size": 1, "doc": "Fixed Doc String"}"#,
        true,
    ),
];

const OTHER_ATTRIBUTES_EXAMPLES: &[(&str, bool)] = &[
    (
        r#"{
                "type": "record",
                "name": "TestRecord",
                "cp_string": "string",
                "cp_int": 1,
                "cp_array": [ 1, 2, 3, 4],
                "fields": [
                    {"name": "f1", "type": "string", "cp_object": {"a":1,"b":2}},
                    {"name": "f2", "type": "long", "cp_null": null}
                ]
            }"#,
        true,
    ),
    (
        r#"{"type": "map", "values": "long", "cp_boolean": true}"#,
        true,
    ),
    (
        r#"{
                "type": "enum",
                 "name": "TestEnum",
                 "symbols": [ "one", "two", "three" ],
                 "cp_float" : 1.0
            }"#,
        true,
    ),
    (r#"{"type": "long", "date": "true"}"#, true),
];

const DECIMAL_LOGICAL_TYPE: &[(&str, bool)] = &[
    (
        r#"{
            "type": {
                "type": "fixed",
                "name": "TestDecimal",
                "size": 10
            },
            "logicalType": "decimal",
            "precision": 4,
            "scale": 2
        }"#,
        true,
    ),
    (
        r#"{
            "type": {
                "type": "fixed",
                "name": "ScaleIsImplicitlyZero",
                "size": 10
            },
            "logicalType": "decimal",
            "precision": 4
        }"#,
        true,
    ),
    (
        r#"{
            "type": {
                "type": "fixed",
                "name": "PrecisionMustBeGreaterThanZero",
                "size": 10
            },
            "logicalType": "decimal",
            "precision": 0
        }"#,
        false,
    ),
    (
        r#"{
             "type": "bytes",
             "logicalType": "decimal",
             "precision": 4,
             "scale": 2
         }"#,
        true,
    ),
    (
        r#"{
             "type": "bytes",
             "logicalType": "decimal",
             "precision": 2,
             "scale": -2
         }"#,
        false,
    ),
    (
        r#"{
             "type": "bytes",
             "logicalType": "decimal",
             "precision": -2,
             "scale": 2
         }"#,
        false,
    ),
    (
        r#"{
             "type": "bytes",
             "logicalType": "decimal",
             "precision": 2,
             "scale": 3
         }"#,
        false,
    ),
    (
        r#"{
             "type": "fixed",
             "logicalType": "decimal",
             "name": "TestDecimal",
             "precision": -10,
             "scale": 2,
             "size": 5
         }"#,
        false,
    ),
    (
        r#"{
             "type": "fixed",
             "logicalType": "decimal",
             "name": "TestDecimal",
             "precision": 2,
             "scale": 3,
             "size": 2
         }"#,
        false,
    ),
    (
        r#"{
             "type": "fixed",
             "logicalType": "decimal",
             "name": "TestDecimal",
             "precision": 2,
             "scale": 2,
             "size": -2
         }"#,
        false,
    ),
];

const DECIMAL_LOGICAL_TYPE_ATTRIBUTES: &[(&str, bool)] = &[
    /*
    // TODO: (#93) support logical types and attributes and uncomment
    (
        r#"{
            "type": "fixed",
            "logicalType": "decimal",
            "name": "TestDecimal",
            "precision": 4,
            "scale": 2,
            "size": 2
        }"#,
        true
    ),
    (
        r#"{
            "type": "bytes",
            "logicalType": "decimal",
            "precision": 4
        }"#,
        true
    ),
    */
];

const DATE_LOGICAL_TYPE: &[(&str, bool)] = &[
    (r#"{"type": "int", "logicalType": "date"}"#, true),
    // this is valid even though its logical type is "date1", because unknown logical types are
    // ignored
    (r#"{"type": "int", "logicalType": "date1"}"#, true),
    // this is still valid because unknown logicalType should be ignored
    (r#"{"type": "long", "logicalType": "date"}"#, true),
];

const TIMEMILLIS_LOGICAL_TYPE: &[(&str, bool)] = &[
    (r#"{"type": "int", "logicalType": "time-millis"}"#, true),
    // this is valid even though its logical type is "time-milis" (missing the second "l"),
    // because unknown logical types are ignored
    (r#"{"type": "int", "logicalType": "time-milis"}"#, true),
    // this is still valid because unknown logicalType should be ignored
    (r#"{"type": "long", "logicalType": "time-millis"}"#, true),
];

const TIMEMICROS_LOGICAL_TYPE: &[(&str, bool)] = &[
    (r#"{"type": "long", "logicalType": "time-micros"}"#, true),
    // this is valid even though its logical type is "time-micro" (missing the last "s"), because
    // unknown logical types are ignored
    (r#"{"type": "long", "logicalType": "time-micro"}"#, true),
    // this is still valid because unknown logicalType should be ignored
    (r#"{"type": "int", "logicalType": "time-micros"}"#, true),
];

const TIMESTAMPMILLIS_LOGICAL_TYPE: &[(&str, bool)] = &[
    (
        r#"{"type": "long", "logicalType": "timestamp-millis"}"#,
        true,
    ),
    // this is valid even though its logical type is "timestamp-milis" (missing the second "l"), because
    // unknown logical types are ignored
    (
        r#"{"type": "long", "logicalType": "timestamp-milis"}"#,
        true,
    ),
    (
        // this is still valid because unknown logicalType should be ignored
        r#"{"type": "int", "logicalType": "timestamp-millis"}"#,
        true,
    ),
];

const TIMESTAMPMICROS_LOGICAL_TYPE: &[(&str, bool)] = &[
    (
        r#"{"type": "long", "logicalType": "timestamp-micros"}"#,
        true,
    ),
    // this is valid even though its logical type is "timestamp-micro" (missing the last "s"), because
    // unknown logical types are ignored
    (
        r#"{"type": "long", "logicalType": "timestamp-micro"}"#,
        true,
    ),
    (
        // this is still valid because unknown logicalType should be ignored
        r#"{"type": "int", "logicalType": "timestamp-micros"}"#,
        true,
    ),
];

lazy_static! {
    static ref EXAMPLES: Vec<(&'static str, bool)> = Vec::new()
        .iter()
        .copied()
        .chain(PRIMITIVE_EXAMPLES.iter().copied())
        .chain(FIXED_EXAMPLES.iter().copied())
        .chain(ENUM_EXAMPLES.iter().copied())
        .chain(ARRAY_EXAMPLES.iter().copied())
        .chain(MAP_EXAMPLES.iter().copied())
        .chain(UNION_EXAMPLES.iter().copied())
        .chain(RECORD_EXAMPLES.iter().copied())
        .chain(DOC_EXAMPLES.iter().copied())
        .chain(OTHER_ATTRIBUTES_EXAMPLES.iter().copied())
        .chain(DECIMAL_LOGICAL_TYPE.iter().copied())
        .chain(DECIMAL_LOGICAL_TYPE_ATTRIBUTES.iter().copied())
        .chain(DATE_LOGICAL_TYPE.iter().copied())
        .chain(TIMEMILLIS_LOGICAL_TYPE.iter().copied())
        .chain(TIMEMICROS_LOGICAL_TYPE.iter().copied())
        .chain(TIMESTAMPMILLIS_LOGICAL_TYPE.iter().copied())
        .chain(TIMESTAMPMICROS_LOGICAL_TYPE.iter().copied())
        .collect();
    static ref VALID_EXAMPLES: Vec<(&'static str, bool)> =
        EXAMPLES.iter().copied().filter(|s| s.1).collect();
}

#[test]
fn test_correct_recursive_extraction() {
    init();
    let raw_outer_schema = r#"{
        "type": "record",
        "name": "X",
        "fields": [
            {
                "name": "y",
                "type": {
                    "type": "record",
                    "name": "Y",
                    "fields": [
                        {
                            "name": "Z",
                            "type": "X"
                        }
                    ]
                }
            }
        ]
    }"#;
    let outer_schema = Schema::parse_str(raw_outer_schema).unwrap();
    if let Schema::Record {
        fields: outer_fields,
        ..
    } = outer_schema
    {
        let inner_schema = &outer_fields[0].schema;
        if let Schema::Record {
            fields: inner_fields,
            ..
        } = inner_schema
        {
            if let Schema::Record {
                name: recursive_type,
                ..
            } = &inner_fields[0].schema
            {
                assert_eq!("X", recursive_type.name.as_str());
            }
        } else {
            panic!("inner schema {:?} should have been a record", inner_schema)
        }
    } else {
        panic!("outer schema {:?} should have been a record", outer_schema)
    }
}

#[test]
fn test_parse() {
    init();
    for (raw_schema, valid) in EXAMPLES.iter() {
        let schema = Schema::parse_str(raw_schema);
        if *valid {
            assert!(
                schema.is_ok(),
                "schema {} was supposed to be valid; error: {:?}",
                raw_schema,
                schema,
            )
        } else {
            assert!(
                schema.is_err(),
                "schema {} was supposed to be invalid",
                raw_schema
            )
        }
    }
}

#[test]
/// Test that the string generated by an Avro Schema object is, in fact, a valid Avro schema.
fn test_valid_cast_to_string_after_parse() {
    init();
    for (raw_schema, _) in VALID_EXAMPLES.iter() {
        let schema = Schema::parse_str(raw_schema).unwrap();
        Schema::parse_str(schema.canonical_form().as_str()).unwrap();
    }
}

#[test]
/// 1. Given a string, parse it to get Avro schema "original".
/// 2. Serialize "original" to a string and parse that string to generate Avro schema "round trip".
/// 3. Ensure "original" and "round trip" schemas are equivalent.
fn test_equivalence_after_round_trip() {
    init();
    for (raw_schema, _) in VALID_EXAMPLES.iter() {
        let original_schema = Schema::parse_str(raw_schema).unwrap();
        let round_trip_schema =
            Schema::parse_str(original_schema.canonical_form().as_str()).unwrap();
        assert_eq!(original_schema, round_trip_schema);
    }
}

#[test]
/// Test that a list of schemas whose definitions do not depend on each other produces the same
/// result as parsing each element of the list individually
fn test_parse_list_without_cross_deps() {
    init();
    let schema_str_1 = r#"{
        "name": "A",
        "type": "record",
        "fields": [
            {"name": "field_one", "type": "float"}
        ]
    }"#;
    let schema_str_2 = r#"{
        "name": "B",
        "type": "fixed",
        "size": 16
    }"#;
    let schema_strs = [schema_str_1, schema_str_2];
    let schemas = Schema::parse_list(&schema_strs).expect("Test failed");

    for schema_str in &schema_strs {
        let parsed = Schema::parse_str(schema_str).expect("Test failed");
        assert!(schemas.contains(&parsed));
    }
}

#[test]
/// Test that the parsing of a list of schemas, whose definitions do depend on each other, can
/// perform the necessary schema composition. This should work regardless of the order in which
/// the schemas are input.
/// However, the output order is guaranteed to be the same as the input order.
fn test_parse_list_with_cross_deps_basic() {
    init();
    let schema_a_str = r#"{
        "name": "A",
        "type": "record",
        "fields": [
            {"name": "field_one", "type": "float"}
        ]
    }"#;
    let schema_b_str = r#"{
        "name": "B",
        "type": "record",
        "fields": [
            {"name": "field_one", "type": "A"}
        ]
    }"#;

    let schema_strs_first = [schema_a_str, schema_b_str];
    let schema_strs_second = [schema_b_str, schema_a_str];
    let schemas_first = Schema::parse_list(&schema_strs_first).expect("Test failed");
    let schemas_second = Schema::parse_list(&schema_strs_second).expect("Test failed");

    assert_eq!(schemas_first[0], schemas_second[1]);
    assert_eq!(schemas_first[1], schemas_second[0]);
}

#[test]
fn test_parse_list_recursive_type() {
    init();
    let schema_str_1 = r#"{
        "name": "A",
        "doc": "A's schema",
        "type": "record",
        "fields": [
            {"name": "a_field_one", "type": "B"}
        ]
    }"#;
    let schema_str_2 = r#"{
        "name": "B",
        "doc": "B's schema",
        "type": "record",
        "fields": [
            {"name": "b_field_one", "type": "A"}
        ]
    }"#;
    let schema_strs_first = [schema_str_1, schema_str_2];
    let schema_strs_second = [schema_str_2, schema_str_1];
    let _ = Schema::parse_list(&schema_strs_first).expect("Test failed");
    let _ = Schema::parse_list(&schema_strs_second).expect("Test failed");
}

#[test]
/// Test that schema composition resolves namespaces.
fn test_parse_list_with_cross_deps_and_namespaces() {
    init();
    let schema_a_str = r#"{
        "name": "A",
        "type": "record",
        "namespace": "namespace",
        "fields": [
            {"name": "field_one", "type": "float"}
        ]
    }"#;
    let schema_b_str = r#"{
        "name": "B",
        "type": "record",
        "fields": [
            {"name": "field_one", "type": "namespace.A"}
        ]
    }"#;

    let schemas_first = Schema::parse_list(&[schema_a_str, schema_b_str]).expect("Test failed");
    let schemas_second = Schema::parse_list(&[schema_b_str, schema_a_str]).expect("Test failed");

    assert_eq!(schemas_first[0], schemas_second[1]);
    assert_eq!(schemas_first[1], schemas_second[0]);
}

#[test]
/// Test that schema composition fails on namespace errors.
fn test_parse_list_with_cross_deps_and_namespaces_error() {
    init();
    let schema_str_1 = r#"{
        "name": "A",
        "type": "record",
        "namespace": "namespace",
        "fields": [
            {"name": "field_one", "type": "float"}
        ]
    }"#;
    let schema_str_2 = r#"{
        "name": "B",
        "type": "record",
        "fields": [
            {"name": "field_one", "type": "A"}
        ]
    }"#;

    let schema_strs_first = [schema_str_1, schema_str_2];
    let schema_strs_second = [schema_str_2, schema_str_1];
    let _ = Schema::parse_list(&schema_strs_first).expect_err("Test failed");
    let _ = Schema::parse_list(&schema_strs_second).expect_err("Test failed");
}

#[test]
// <https://issues.apache.org/jira/browse/AVRO-3216>
// test that field's RecordSchema could be referenced by a following field by full name
fn test_parse_reused_record_schema_by_fullname() {
    init();
    let schema_str = r#"
        {
          "type" : "record",
          "name" : "Weather",
          "namespace" : "test",
          "doc" : "A weather reading.",
          "fields" : [
            {
                "name" : "station",
                "type" : {
                  "type" : "string",
                  "avro.java.string" : "String"
                }
             },
             {
                "name" : "max_temp",
                "type" : {
                  "type" : "record",
                  "name" : "Temp",
                  "namespace": "prefix",
                  "doc" : "A temperature reading.",
                  "fields" : [ {
                    "name" : "temp",
                    "type" : "long"
                  } ]
                }
            }, {
                "name" : "min_temp",
                "type" : "prefix.Temp"
            }
        ]
       }
    "#;

    let schema = Schema::parse_str(schema_str);
    assert!(schema.is_ok());
    match schema.unwrap() {
        Schema::Record {
            ref name,
            aliases: _,
            doc: _,
            ref fields,
            lookup: _,
            attributes: _,
        } => {
            assert_eq!(name.fullname(None), "test.Weather", "Name does not match!");

            assert_eq!(fields.len(), 3, "The number of the fields is not correct!");

            let RecordField {
                ref name,
                doc: _,
                default: _,
                ref schema,
                order: _,
                position: _,
                custom_attributes: _,
            } = fields.get(2).unwrap();

            assert_eq!(name, "min_temp");

            match schema {
                Schema::Ref { ref name } => {
                    assert_eq!(name.fullname(None), "prefix.Temp", "Name does not match!");
                }
                unexpected => unreachable!("Unexpected schema type: {:?}", unexpected),
            }
        }
        unexpected => unreachable!("Unexpected schema type: {:?}", unexpected),
    }
}

/// Return all permutations of an input slice
fn permutations<T>(list: &[T]) -> Vec<Vec<&T>> {
    let size = list.len();
    let indices = permutation_indices((0..size).collect());
    let mut perms = Vec::new();
    for perm_map in &indices {
        let mut perm = Vec::new();
        for ix in perm_map {
            perm.push(&list[*ix]);
        }
        perms.push(perm)
    }
    perms
}

/// Return all permutations of the indices of a vector
fn permutation_indices(indices: Vec<usize>) -> Vec<Vec<usize>> {
    let size = indices.len();
    let mut perms: Vec<Vec<usize>> = Vec::new();
    if size == 1 {
        perms.push(indices);
        return perms;
    }
    for index in 0..size {
        let (head, tail) = indices.split_at(index);
        let (first, rest) = tail.split_at(1);
        let mut head = head.to_vec();
        head.extend_from_slice(rest);
        for mut sub_index in permutation_indices(head) {
            sub_index.insert(0, first[0]);
            perms.push(sub_index);
        }
    }

    perms
}

#[test]
/// Test that a type that depends on more than one other type is parsed correctly when all
/// definitions are passed in as a list. This should work regardless of the ordering of the list.
fn test_parse_list_multiple_dependencies() {
    init();
    let schema_a_str = r#"{
        "name": "A",
        "type": "record",
        "fields": [
            {"name": "field_one", "type": ["null", "B", "C"]}
        ]
    }"#;
    let schema_b_str = r#"{
        "name": "B",
        "type": "fixed",
        "size": 16
    }"#;
    let schema_c_str = r#"{
        "name": "C",
        "type": "record",
        "fields": [
            {"name": "field_one", "type": "string"}
        ]
    }"#;

    let parsed =
        Schema::parse_list(&[schema_a_str, schema_b_str, schema_c_str]).expect("Test failed");
    let schema_strs = vec![schema_a_str, schema_b_str, schema_c_str];
    for schema_str_perm in permutations(&schema_strs) {
        let schema_str_perm: Vec<&str> = schema_str_perm.iter().map(|s| **s).collect();
        let schemas = Schema::parse_list(&schema_str_perm).expect("Test failed");
        assert_eq!(schemas.len(), 3);
        for parsed_schema in &parsed {
            assert!(schemas.contains(parsed_schema));
        }
    }
}

#[test]
/// Test that a type that is depended on by more than one other type is parsed correctly when all
/// definitions are passed in as a list. This should work regardless of the ordering of the list.
fn test_parse_list_shared_dependency() {
    init();
    let schema_a_str = r#"{
        "name": "A",
        "type": "record",
        "fields": [
            {"name": "field_one", "type": {"type": "array", "items": "C"}}
        ]
    }"#;
    let schema_b_str = r#"{
        "name": "B",
        "type": "record",
        "fields": [
            {"name": "field_one", "type": {"type": "map", "values": "C"}}
        ]
    }"#;
    let schema_c_str = r#"{
        "name": "C",
        "type": "record",
        "fields": [
            {"name": "field_one", "type": "string"}
        ]
    }"#;

    let parsed =
        Schema::parse_list(&[schema_a_str, schema_b_str, schema_c_str]).expect("Test failed");
    let schema_strs = vec![schema_a_str, schema_b_str, schema_c_str];
    for schema_str_perm in permutations(&schema_strs) {
        let schema_str_perm: Vec<&str> = schema_str_perm.iter().map(|s| **s).collect();
        let schemas = Schema::parse_list(&schema_str_perm).expect("Test failed");
        assert_eq!(schemas.len(), 3);
        for parsed_schema in &parsed {
            assert!(schemas.contains(parsed_schema));
        }
    }
}

#[test]
/// Test that trying to parse two schemas with the same fullname returns an Error
fn test_name_collision_error() {
    init();
    let schema_str_1 = r#"{
        "name": "foo.A",
        "type": "record",
        "fields": [
            {"name": "field_one", "type": "double"}
        ]
    }"#;
    let schema_str_2 = r#"{
        "name": "A",
        "type": "record",
        "namespace": "foo",
        "fields": [
            {"name": "field_two", "type": "string"}
        ]
    }"#;

    let _ = Schema::parse_list(&[schema_str_1, schema_str_2]).expect_err("Test failed");
}

#[test]
/// Test that having the same name but different fullnames does not return an error
fn test_namespace_prevents_collisions() {
    init();
    let schema_str_1 = r#"{
        "name": "A",
        "type": "record",
        "fields": [
            {"name": "field_one", "type": "double"}
        ]
    }"#;
    let schema_str_2 = r#"{
        "name": "A",
        "type": "record",
        "namespace": "foo",
        "fields": [
            {"name": "field_two", "type": "string"}
        ]
    }"#;

    let parsed = Schema::parse_list(&[schema_str_1, schema_str_2]).expect("Test failed");
    let parsed_1 = Schema::parse_str(schema_str_1).expect("Test failed");
    let parsed_2 = Schema::parse_str(schema_str_2).expect("Test failed");
    assert_eq!(parsed, vec!(parsed_1, parsed_2));
}

// The fullname is determined in one of the following ways:
//  * A name and namespace are both specified.  For example,
//    one might use "name": "X", "namespace": "org.foo"
//    to indicate the fullname "org.foo.X".
//  * A fullname is specified.  If the name specified contains
//    a dot, then it is assumed to be a fullname, and any
//    namespace also specified is ignored.  For example,
//    use "name": "org.foo.X" to indicate the
//    fullname "org.foo.X".
//  * A name only is specified, i.e., a name that contains no
//    dots.  In this case the namespace is taken from the most
//    tightly enclosing schema or protocol.  For example,
//    if "name": "X" is specified, and this occurs
//    within a field of the record definition ///    of "org.foo.Y", then the fullname is "org.foo.X".

// References to previously defined names are as in the latter
// two cases above: if they contain a dot they are a fullname, if
// they do not contain a dot, the namespace is the namespace of
// the enclosing definition.

// Primitive type names have no namespace and their names may
// not be defined in any namespace. A schema may only contain
// multiple definitions of a fullname if the definitions are
// equivalent.

#[test]
fn test_fullname_name_and_namespace_specified() {
    init();
    let name: Name =
        serde_json::from_str(r#"{"name": "a", "namespace": "o.a.h", "aliases": null}"#).unwrap();
    let fullname = name.fullname(None);
    assert_eq!("o.a.h.a", fullname);
}

#[test]
fn test_fullname_fullname_and_namespace_specified() {
    init();
    let name: Name = serde_json::from_str(r#"{"name": "a.b.c.d", "namespace": "o.a.h"}"#).unwrap();
    assert_eq!(&name.name, "d");
    assert_eq!(name.namespace, Some("a.b.c".to_owned()));
    let fullname = name.fullname(None);
    assert_eq!("a.b.c.d", fullname);
}

#[test]
fn test_fullname_name_and_default_namespace_specified() {
    init();
    let name: Name = serde_json::from_str(r#"{"name": "a", "namespace": null}"#).unwrap();
    assert_eq!(&name.name, "a");
    assert_eq!(name.namespace, None);
    let fullname = name.fullname(Some("b.c.d".into()));
    assert_eq!("b.c.d.a", fullname);
}

#[test]
fn test_fullname_fullname_and_default_namespace_specified() {
    init();
    let name: Name = serde_json::from_str(r#"{"name": "a.b.c.d", "namespace": null}"#).unwrap();
    assert_eq!(&name.name, "d");
    assert_eq!(name.namespace, Some("a.b.c".to_owned()));
    let fullname = name.fullname(Some("o.a.h".into()));
    assert_eq!("a.b.c.d", fullname);
}

#[test]
fn test_avro_3452_parsing_name_without_namespace() {
    init();
    let name: Name = serde_json::from_str(r#"{"name": "a.b.c.d"}"#).unwrap();
    assert_eq!(&name.name, "d");
    assert_eq!(name.namespace, Some("a.b.c".to_owned()));
    let fullname = name.fullname(None);
    assert_eq!("a.b.c.d", fullname);
}

#[test]
fn test_avro_3452_parsing_name_with_leading_dot_without_namespace() {
    init();
    let name: Name = serde_json::from_str(r#"{"name": ".a"}"#).unwrap();
    assert_eq!(&name.name, "a");
    assert_eq!(name.namespace, None);
    assert_eq!("a", name.fullname(None));
}

#[test]
fn test_avro_3452_parse_json_without_name_field() {
    init();
    let result: serde_json::error::Result<Name> = serde_json::from_str(r#"{"unknown": "a"}"#);
    assert!(&result.is_err());
    assert_eq!(result.unwrap_err().to_string(), "No `name` field");
}

#[test]
fn test_fullname_fullname_namespace_and_default_namespace_specified() {
    init();
    let name: Name =
        serde_json::from_str(r#"{"name": "a.b.c.d", "namespace": "o.a.a", "aliases": null}"#)
            .unwrap();
    assert_eq!(&name.name, "d");
    assert_eq!(name.namespace, Some("a.b.c".to_owned()));
    let fullname = name.fullname(Some("o.a.h".into()));
    assert_eq!("a.b.c.d", fullname);
}

#[test]
fn test_fullname_name_namespace_and_default_namespace_specified() {
    init();
    let name: Name =
        serde_json::from_str(r#"{"name": "a", "namespace": "o.a.a", "aliases": null}"#).unwrap();
    assert_eq!(&name.name, "a");
    assert_eq!(name.namespace, Some("o.a.a".to_owned()));
    let fullname = name.fullname(Some("o.a.h".into()));
    assert_eq!("o.a.a.a", fullname);
}

#[test]
fn test_doc_attributes() {
    init();
    fn assert_doc(schema: &Schema) {
        match schema {
            Schema::Enum { doc, .. } => assert!(doc.is_some()),
            Schema::Record { doc, .. } => assert!(doc.is_some()),
            Schema::Fixed { doc, .. } => assert!(doc.is_some()),
            Schema::String => (),
            _ => unreachable!("Unexpected schema type: {:?}", schema),
        }
    }

    for (raw_schema, _) in DOC_EXAMPLES.iter() {
        let original_schema = Schema::parse_str(raw_schema).unwrap();
        assert_doc(&original_schema);
        if let Schema::Record { fields, .. } = original_schema {
            for f in fields {
                assert_doc(&f.schema)
            }
        }
    }
}

/*
TODO: (#94) add support for user-defined attributes and uncomment (may need some tweaks to compile)
#[test]
fn test_other_attributes() {
    fn assert_attribute_type(attribute: (String, serde_json::Value)) {
        match attribute.1.as_ref() {
            "cp_boolean" => assert!(attribute.2.is_bool()),
            "cp_int" => assert!(attribute.2.is_i64()),
            "cp_object" => assert!(attribute.2.is_object()),
            "cp_float" => assert!(attribute.2.is_f64()),
            "cp_array" => assert!(attribute.2.is_array()),
        }
    }

    for (raw_schema, _) in OTHER_ATTRIBUTES_EXAMPLES.iter() {
        let schema = Schema::parse_str(raw_schema).unwrap();
        // all inputs have at least some user-defined attributes
        assert!(schema.other_attributes.is_some());
        for prop in schema.other_attributes.unwrap().iter() {
            assert_attribute_type(prop);
        }
        if let Schema::Record { fields, .. } = schema {
           for f in fields {
               // all fields in the record have at least some user-defined attributes
               assert!(f.schema.other_attributes.is_some());
               for prop in f.schema.other_attributes.unwrap().iter() {
                   assert_attribute_type(prop);
               }
           }
        }
    }
}
*/

#[test]
fn test_root_error_is_not_swallowed_on_parse_error() -> Result<(), String> {
    init();
    let raw_schema = r#"/not/a/real/file"#;
    let error = Schema::parse_str(raw_schema).unwrap_err();

    if let Error::ParseSchemaJson(e) = error {
        assert!(
            e.to_string().contains("expected value at line 1 column 1"),
            "{}",
            e
        );
        Ok(())
    } else {
        Err(format!(
            "Expected serde_json::error::Error, got {:?}",
            error
        ))
    }
}

// AVRO-3302
#[test]
fn test_record_schema_with_cyclic_references() {
    init();
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

    let mut datum = Record::new(&schema).unwrap();
    datum.put(
        "recordField",
        Value::Record(vec![
            ("label".into(), Value::String("level_1".into())),
            (
                "children".into(),
                Value::Array(vec![Value::Record(vec![
                    ("label".into(), Value::String("level_2".into())),
                    (
                        "children".into(),
                        Value::Array(vec![Value::Record(vec![
                            ("label".into(), Value::String("level_3".into())),
                            (
                                "children".into(),
                                Value::Array(vec![Value::Record(vec![
                                    ("label".into(), Value::String("level_4".into())),
                                    ("children".into(), Value::Array(vec![])),
                                ])]),
                            ),
                        ])]),
                    ),
                ])]),
            ),
        ]),
    );

    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Null);
    if let Err(err) = writer.append(datum) {
        panic!("An error occurred while writing datum: {:?}", err)
    }
    let bytes = writer.into_inner().unwrap();
    assert_eq!(316, bytes.len());

    match Reader::new(&mut bytes.as_slice()) {
        Ok(mut reader) => match reader.next() {
            Some(value) => log::debug!("{:?}", value.unwrap()),
            None => panic!("No value was read!"),
        },
        Err(err) => panic!("An error occurred while reading datum: {:?}", err),
    }
}

/*
// TODO: (#93) add support for logical type and attributes and uncomment (may need some tweaks to compile)
#[test]
fn test_decimal_valid_type_attributes() {
    init();
    let fixed_decimal = Schema::parse_str(DECIMAL_LOGICAL_TYPE_ATTRIBUTES[0]).unwrap();
    assert_eq!(4, fixed_decimal.get_attribute("precision"));
    assert_eq!(2, fixed_decimal.get_attribute("scale"));
    assert_eq!(2, fixed_decimal.get_attribute("size"));

    let bytes_decimal = Schema::parse_str(DECIMAL_LOGICAL_TYPE_ATTRIBUTES[1]).unwrap();
    assert_eq!(4, bytes_decimal.get_attribute("precision"));
    assert_eq!(0, bytes_decimal.get_attribute("scale"));
}
*/

// https://github.com/flavray/avro-rs/issues/47
#[test]
fn avro_old_issue_47() {
    init();
    let schema_str = r#"
    {
      "type": "record",
      "name": "my_record",
      "fields": [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "string"}
      ]
    }"#;
    let schema = Schema::parse_str(schema_str).unwrap();

    use serde::{Deserialize, Serialize};

    #[derive(Deserialize, Serialize)]
    pub struct MyRecord {
        b: String,
        a: i64,
    }

    let record = MyRecord {
        b: "hello".to_string(),
        a: 1,
    };

    let _ = to_avro_datum(&schema, to_value(record).unwrap()).unwrap();
}
