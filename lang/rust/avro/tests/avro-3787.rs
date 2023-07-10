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

use apache_avro::{from_avro_datum, to_avro_datum, to_value, types, Schema};
use apache_avro_test_helper::TestResult;

#[test]
fn avro_3787_deserialize_union_with_unknown_symbol() -> TestResult {
    #[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
    pub struct BarUseParent {
        #[serde(rename = "barUse")]
        pub bar_use: Bar,
    }

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
        #[serde(rename = "barUseParent")]
        pub bar_use_parent: Option<BarUseParent>,
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
              "bar1",
              "bar2"
            ],
            "default": "bar0"
          }
        },
        {
          "name": "barUseParent",
          "type": [
            "null",
            {
              "type": "record",
              "name": "BarUseParent",
              "fields": [
                {
                  "name": "barUse",
                  "type": "Bar"
                }
              ]
            }
          ]
        }
      ]
    }"#;

    let reader_schema = r#"{
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
            ],
            "default": "bar0"
          }
        },
        {
          "name": "barUseParent",
          "type": [
            "null",
            {
              "type": "record",
              "name": "BarUseParent",
              "fields": [
                {
                  "name": "barUse",
                  "type": "Bar"
                }
              ]
            }
          ]
        }
      ]
      }"#;

    let writer_schema = Schema::parse_str(writer_schema)?;
    let foo1 = Foo {
        bar_init: Bar::Bar1,
        bar_use_parent: Some(BarUseParent { bar_use: Bar::Bar2 }),
    };
    let avro_value = to_value(foo1)?;
    assert!(
        avro_value.validate(&writer_schema),
        "value is valid for schema",
    );
    let datum = to_avro_datum(&writer_schema, avro_value)?;
    let mut x = &datum[..];
    let reader_schema = Schema::parse_str(reader_schema)?;
    let deser_value = from_avro_datum(&writer_schema, &mut x, Some(&reader_schema))?;
    match deser_value {
        types::Value::Record(fields) => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].0, "barInit");
            assert_eq!(fields[0].1, types::Value::Enum(1, "bar1".to_string()));
            assert_eq!(fields[1].0, "barUseParent");
            // TODO: test value
        }
        _ => panic!("Expected Value::Record"),
    }

    Ok(())
}

#[test]
fn avro_3787_deserialize_union_with_unknown_symbol_no_ref() -> TestResult {
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
    #[serde(default)]
    pub struct BarParent {
        #[serde(rename = "Bar")]
        pub bar: Bar,
    }

    #[inline(always)]
    fn default_barparent_bar() -> Bar {
        Bar::Bar0
    }
    impl Default for BarParent {
        fn default() -> BarParent {
            BarParent {
                bar: default_barparent_bar(),
            }
        }
    }

    #[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
    pub struct Foo {
        #[serde(rename = "barParent")]
        pub bar_parent: Option<BarParent>,
    }

    let writer_schema = r#"{
            "type": "record",
            "name": "Foo",
            "fields":
            [
                {
                    "name": "barParent",
                    "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "BarParent",
                            "fields": [
                                {
                                    "type": "enum",
                                    "name": "Bar",
                                    "symbols":
                                    [
                                        "bar0",
                                        "bar1",
                                        "bar2"
                                    ],
                                    "default": "bar0"
                                }
                            ]
                        }
                    ]
                }
            ]
        }"#;

    let reader_schema = r#"{
            "type": "record",
            "name": "Foo",
            "fields":
            [
                {
                    "name": "barParent",
                    "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "BarParent",
                            "fields": [
                                {
                                    "type": "enum",
                                    "name": "Bar",
                                    "symbols":
                                    [
                                        "bar0",
                                        "bar1"
                                    ],
                                    "default": "bar0"
                                }
                            ]
                        }
                    ]
                }
            ]
        }"#;

    let writer_schema = Schema::parse_str(writer_schema)?;
    let foo2 = Foo {
        bar_parent: Some(BarParent { bar: Bar::Bar2 }),
    };
    let avro_value = to_value(foo2)?;
    assert!(
        avro_value.validate(&writer_schema),
        "value is valid for schema",
    );
    let datum = to_avro_datum(&writer_schema, avro_value)?;
    let mut x = &datum[..];
    let reader_schema = Schema::parse_str(reader_schema)?;
    let deser_value = from_avro_datum(&writer_schema, &mut x, Some(&reader_schema))?;
    match deser_value {
        types::Value::Record(fields) => {
            assert_eq!(fields.len(), 1);
            // assert_eq!(fields[0].0, "barInit");
            // assert_eq!(fields[0].1, types::Value::Enum(0, "bar0".to_string()));
            assert_eq!(fields[0].0, "barParent");
            // assert_eq!(fields[1].1, types::Value::Enum(1, "bar1".to_string()));
        }
        _ => panic!("Expected Value::Record"),
    }

    Ok(())
}
