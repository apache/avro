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

use apache_avro::{from_value, to_value, types::Value, Schema};
use pretty_assertions::assert_eq;
use serde::{Deserialize, Serialize};

const SCHEMA_STR: &str = r#"
        {
            "type": "record",
            "name": "serde_enum",
            "fields": [
                {
                    "name": "a",
                    "type": {
                        "type": "record",
                        "name": "TestEnum",
                        "fields": [
                            {
                                "name": "type",
                                "type": {
                                    "type": "enum",
                                    "name": "TestEnumType",
                                    "symbols": [
                                        "Null",
                                        "Int",
                                        "Tuple",
                                        "Struct"
                                    ]
                                }
                            },
                            {
                                "name": "value",
                                "type": [
                                    "null",
                                    "long",
                                    {
                                        "type": "array",
                                        "items": "long"
                                    },
                                    {
                                        "type": "record",
                                        "name": "Struct",
                                        "fields": [
                                            {
                                                "name": "b",
                                                "type": "long"
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                }
            ]
        }
        "#;

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct TestExternalEnum {
    a: TestEnum,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
enum TestEnum {
    Null,
    Int(i64),
    Tuple(i64, i64),
    Struct { b: i64 },
}

#[test]
fn avro_3646_test_to_value_mixed_enum() {
    let schema = Schema::parse_str(SCHEMA_STR).unwrap();

    let data = vec![
        (
            TestExternalEnum { a: TestEnum::Null },
            Value::Record(vec![(
                "a".to_owned(),
                Value::Record(vec![
                    ("type".to_owned(), Value::Enum(0, "Null".to_owned())),
                    ("value".to_owned(), Value::Union(0, Box::new(Value::Null))),
                ]),
            )]),
        ),
        (
            TestExternalEnum {
                a: TestEnum::Int(1),
            },
            Value::Record(vec![(
                "a".to_owned(),
                Value::Record(vec![
                    ("type".to_owned(), Value::Enum(1, "Int".to_owned())),
                    (
                        "value".to_owned(),
                        Value::Union(1, Box::new(Value::Long(1))),
                    ),
                ]),
            )]),
        ),
        (
            TestExternalEnum {
                a: TestEnum::Tuple(1, 2),
            },
            Value::Record(vec![(
                "a".to_owned(),
                Value::Record(vec![
                    ("type".to_owned(), Value::Enum(2, "Tuple".to_owned())),
                    (
                        "value".to_owned(),
                        Value::Union(
                            2,
                            Box::new(Value::Array(vec![Value::Long(1), Value::Long(2)])),
                        ),
                    ),
                ]),
            )]),
        ),
        (
            TestExternalEnum {
                a: TestEnum::Struct { b: 1 },
            },
            Value::Record(vec![(
                "a".to_owned(),
                Value::Record(vec![
                    ("type".to_owned(), Value::Enum(3, "Struct".to_owned())),
                    (
                        "value".to_owned(),
                        Value::Union(
                            3,
                            Box::new(Value::Record(vec![("b".to_owned(), Value::Long(1))])),
                        ),
                    ),
                ]),
            )]),
        ),
    ];

    for (test, expected) in &data {
        let actual = to_value(test).unwrap();
        assert_eq!(actual, *expected, "Cannot serialize");
        if !actual.validate(&schema) {
            panic!("Cannot validate value: {:?}", actual);
        }
        let deserialized: TestExternalEnum = from_value(&actual).unwrap();
        assert_eq!(deserialized, *test, "Cannot deserialize");
    }
}
