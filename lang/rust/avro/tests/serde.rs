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

mod enums {
    use apache_avro::{to_value, types::Value, Schema};
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
                        "name": "TestNullExternalEnum",
                        "fields": [
                            {
                                "name": "type",
                                "type": {
                                    "type": "enum",
                                    "name": "NullExternalEnum",
                                    "symbols": ["Null", "Long"]
                                }
                            },
                            {
                                "name": "value",
                                "type": [
                                    "null",
                                    "long"
                                ]
                            }
                        ]
                    }
                }
            ]
        }
        "#;

    #[test]
    fn avro_3646_test_to_value_enum_with_unit_variant() {
        let schema = Schema::parse_str(SCHEMA_STR).unwrap();

        #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
        struct TestNullExternalEnum {
            a: NullExternalEnum,
        }

        #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
        enum NullExternalEnum {
            Null,      // unit_variant
            Long(u64), // newtype_variant
        }

        let data = vec![
            (
                TestNullExternalEnum {
                    a: NullExternalEnum::Null,
                },
                Value::Record(vec![(
                    "a".to_owned(),
                    Value::Record(vec![
                        ("type".to_owned(), Value::Enum(0, "Null".to_owned())),
                        ("value".to_owned(), Value::Union(0, Box::new(Value::Null))),
                    ]),
                )]),
            ),
            (
                TestNullExternalEnum {
                    a: NullExternalEnum::Long(123),
                },
                Value::Record(vec![(
                    "a".to_owned(),
                    Value::Record(vec![
                        ("type".to_owned(), Value::Enum(1, "Long".to_owned())),
                        ("value".to_owned(), Value::Union(1, Value::Long(123).into())),
                    ]),
                )]),
            ),
        ];

        for (test, expected) in &data {
            let actual = to_value(test).unwrap();
            assert_eq!(actual, *expected,);
            if !actual.validate(&schema) {
                panic!("Cannot validate value: {:?}", actual);
            }
        }
    }

    #[test]
    fn avro_3646_test_to_value_enum_with_tuple_variant() {
        let schema = Schema::parse_str(SCHEMA_STR).unwrap();

        #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
        struct TestNullExternalEnum {
            a: NullExternalEnum,
        }

        #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
        enum NullExternalEnum {
            Null(),    // tuple_variant
            Long(u64), // newtype_variant
        }

        let data = vec![
            (
                TestNullExternalEnum {
                    a: NullExternalEnum::Null(),
                },
                Value::Record(vec![(
                    "a".to_owned(),
                    Value::Record(vec![
                        ("type".to_owned(), Value::Enum(0, "Null".to_owned())),
                        ("value".to_owned(), Value::Union(0, Box::new(Value::Null))),
                    ]),
                )]),
            ),
            (
                TestNullExternalEnum {
                    a: NullExternalEnum::Long(123),
                },
                Value::Record(vec![(
                    "a".to_owned(),
                    Value::Record(vec![
                        ("type".to_owned(), Value::Enum(1, "Long".to_owned())),
                        ("value".to_owned(), Value::Union(1, Value::Long(123).into())),
                    ]),
                )]),
            ),
        ];

        for (test, expected) in &data {
            let actual = to_value(test).unwrap();
            assert_eq!(actual, *expected,);
            if !actual.validate(&schema) {
                panic!("Cannot validate value: {:?}", actual);
            }
        }
    }

    #[test]
    fn avro_3646_test_to_value_enum_with_newtype_variant_with_unit() {
        let schema = Schema::parse_str(SCHEMA_STR).unwrap();

        #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
        struct TestNullExternalEnum {
            a: NullExternalEnum,
        }

        #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
        enum NullExternalEnum {
            Null(()),  // newtype_variant
            Long(u64), // newtype_variant
        }

        let data = vec![
            (
                TestNullExternalEnum {
                    a: NullExternalEnum::Null(()),
                },
                Value::Record(vec![(
                    "a".to_owned(),
                    Value::Record(vec![
                        ("type".to_owned(), Value::Enum(0, "Null".to_owned())),
                        ("value".to_owned(), Value::Union(0, Box::new(Value::Null))),
                    ]),
                )]),
            ),
            (
                TestNullExternalEnum {
                    a: NullExternalEnum::Long(123),
                },
                Value::Record(vec![(
                    "a".to_owned(),
                    Value::Record(vec![
                        ("type".to_owned(), Value::Enum(1, "Long".to_owned())),
                        ("value".to_owned(), Value::Union(1, Value::Long(123).into())),
                    ]),
                )]),
            ),
        ];

        for (test, expected) in &data {
            let actual = to_value(test).unwrap();
            assert_eq!(actual, *expected,);
            if !actual.validate(&schema) {
                panic!("Cannot validate value: {:?}", actual);
            }
        }
    }

    #[test]
    fn avro_3646_test_to_value_enum_with_struct_variant() {
        let schema = Schema::parse_str(SCHEMA_STR).unwrap();

        #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
        struct TestNullExternalEnum {
            a: NullExternalEnum,
        }

        #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
        enum NullExternalEnum {
            Null {},   // struct_variant
            Long(u64), // newtype_variant
        }

        let data = vec![
            (
                TestNullExternalEnum {
                    a: NullExternalEnum::Null {},
                },
                Value::Record(vec![(
                    "a".to_owned(),
                    Value::Record(vec![
                        ("type".to_owned(), Value::Enum(0, "Null".to_owned())),
                        ("value".to_owned(), Value::Union(0, Box::new(Value::Null))),
                    ]),
                )]),
            ),
            (
                TestNullExternalEnum {
                    a: NullExternalEnum::Long(123),
                },
                Value::Record(vec![(
                    "a".to_owned(),
                    Value::Record(vec![
                        ("type".to_owned(), Value::Enum(1, "Long".to_owned())),
                        ("value".to_owned(), Value::Union(1, Value::Long(123).into())),
                    ]),
                )]),
            ),
        ];

        for (test, expected) in &data {
            let actual = to_value(test).unwrap();
            assert_eq!(actual, *expected,);
            if !actual.validate(&schema) {
                panic!("Cannot validate value: {:?}", actual);
            }
        }
    }
}
