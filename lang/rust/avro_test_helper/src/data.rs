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

//! Provides a set of Avro schema examples that are used in the tests.

use std::sync::OnceLock;

pub const PRIMITIVE_EXAMPLES: &[(&str, bool)] = &[
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

pub const FIXED_EXAMPLES: &[(&str, bool)] = &[
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

pub const ENUM_EXAMPLES: &[(&str, bool)] = &[
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

pub const ARRAY_EXAMPLES: &[(&str, bool)] = &[
    (r#"{"type": "array", "items": "long"}"#, true),
    (
        r#"{
                "type": "array",
                 "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
            }"#,
        true,
    ),
];

pub const MAP_EXAMPLES: &[(&str, bool)] = &[
    (r#"{"type": "map", "values": "long"}"#, true),
    (
        r#"{
                "type": "map",
                "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
            }"#,
        true,
    ),
];

pub const UNION_EXAMPLES: &[(&str, bool)] = &[
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
        true,
    ),
    (
        r#"{"name": "foo", "type": ["string", "null"], "default": null}"#,
        true,
    ),
    (
        r#"{"name": "foo", "type": ["null", "string"], "default": "null"}"#,
        true,
    ),
    (
        r#"{"name": "foo", "type": ["long", "string"], "default": "str"}"#,
        true,
    ),
];

pub const RECORD_EXAMPLES: &[(&str, bool)] = &[
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

pub const DOC_EXAMPLES: &[(&str, bool)] = &[
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

pub const OTHER_ATTRIBUTES_EXAMPLES: &[(&str, bool)] = &[
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

pub const DECIMAL_LOGICAL_TYPE: &[(&str, bool)] = &[
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
        true,
    ),
    (
        r#"{
             "type": "fixed",
             "logicalType": "decimal",
             "name": "TestDecimal",
             "precision": 10,
             "scale": 2,
             "size": 18
         }"#,
        true,
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
        true,
    ),
    (
        r#"{
             "type": "bytes",
             "logicalType": "decimal",
             "precision": -2,
             "scale": 2
         }"#,
        true,
    ),
    (
        r#"{
             "type": "bytes",
             "logicalType": "decimal",
             "precision": 2,
             "scale": 3
         }"#,
        true,
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
        true,
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
        true,
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

pub const DATE_LOGICAL_TYPE: &[(&str, bool)] = &[
    (r#"{"type": "int", "logicalType": "date"}"#, true),
    // this is valid even though its logical type is "date1", because unknown logical types are
    // ignored
    (r#"{"type": "int", "logicalType": "date1"}"#, true),
    // this is still valid because unknown logicalType should be ignored
    (r#"{"type": "long", "logicalType": "date"}"#, true),
];

pub const TIMEMILLIS_LOGICAL_TYPE: &[(&str, bool)] = &[
    (r#"{"type": "int", "logicalType": "time-millis"}"#, true),
    // this is valid even though its logical type is "time-milis" (missing the second "l"),
    // because unknown logical types are ignored
    (r#"{"type": "int", "logicalType": "time-milis"}"#, true),
    // this is still valid because unknown logicalType should be ignored
    (r#"{"type": "long", "logicalType": "time-millis"}"#, true),
];

pub const TIMEMICROS_LOGICAL_TYPE: &[(&str, bool)] = &[
    (r#"{"type": "long", "logicalType": "time-micros"}"#, true),
    // this is valid even though its logical type is "time-micro" (missing the last "s"), because
    // unknown logical types are ignored
    (r#"{"type": "long", "logicalType": "time-micro"}"#, true),
    // this is still valid because unknown logicalType should be ignored
    (r#"{"type": "int", "logicalType": "time-micros"}"#, true),
];

pub const TIMESTAMPMILLIS_LOGICAL_TYPE: &[(&str, bool)] = &[
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

pub const TIMESTAMPMICROS_LOGICAL_TYPE: &[(&str, bool)] = &[
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

pub const LOCAL_TIMESTAMPMILLIS_LOGICAL_TYPE: &[(&str, bool)] = &[
    (
        r#"{"type": "long", "logicalType": "local-timestamp-millis"}"#,
        true,
    ),
    // this is valid even though its logical type is "local-timestamp-milis" (missing the second "l"), because
    // unknown logical types are ignored
    (
        r#"{"type": "long", "logicalType": "local-timestamp-milis"}"#,
        true,
    ),
    (
        // this is still valid because unknown logicalType should be ignored
        r#"{"type": "int", "logicalType": "local-timestamp-millis"}"#,
        true,
    ),
];

pub const LOCAL_TIMESTAMPMICROS_LOGICAL_TYPE: &[(&str, bool)] = &[
    (
        r#"{"type": "long", "logicalType": "local-timestamp-micros"}"#,
        true,
    ),
    // this is valid even though its logical type is "local-timestamp-micro" (missing the last "s"), because
    // unknown logical types are ignored
    (
        r#"{"type": "long", "logicalType": "local-timestamp-micro"}"#,
        true,
    ),
    (
        // this is still valid because unknown logicalType should be ignored
        r#"{"type": "int", "logicalType": "local-timestamp-micros"}"#,
        true,
    ),
];

pub fn examples() -> &'static Vec<(&'static str, bool)> {
    static EXAMPLES_ONCE: OnceLock<Vec<(&'static str, bool)>> = OnceLock::new();
    EXAMPLES_ONCE.get_or_init(|| {
        Vec::new()
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
            .chain(DATE_LOGICAL_TYPE.iter().copied())
            .chain(TIMEMILLIS_LOGICAL_TYPE.iter().copied())
            .chain(TIMEMICROS_LOGICAL_TYPE.iter().copied())
            .chain(TIMESTAMPMILLIS_LOGICAL_TYPE.iter().copied())
            .chain(TIMESTAMPMICROS_LOGICAL_TYPE.iter().copied())
            .chain(LOCAL_TIMESTAMPMILLIS_LOGICAL_TYPE.iter().copied())
            .chain(LOCAL_TIMESTAMPMICROS_LOGICAL_TYPE.iter().copied())
            .collect()
    })
}

pub fn valid_examples() -> &'static Vec<(&'static str, bool)> {
    static VALID_EXAMPLES_ONCE: OnceLock<Vec<(&'static str, bool)>> = OnceLock::new();
    VALID_EXAMPLES_ONCE.get_or_init(|| examples().iter().copied().filter(|s| s.1).collect())
}
