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

#![cfg(target_arch = "wasm32")]

extern crate wasm_bindgen_test;

use std::io::BufWriter;
use wasm_bindgen_test::*;

use apache_avro::{from_value, to_value, types::Record, Codec, Reader, Schema, Writer};
use serde::{Deserialize, Serialize};

wasm_bindgen_test_configure!(run_in_browser);

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct MyRecord {
    b: String,
    a: i64,
}

#[wasm_bindgen_test]
fn serialization_roundtrip() {
    console_error_panic_hook::set_once();

    let record = MyRecord {
        b: "hello".to_string(),
        a: 1,
    };

    let serialized = to_value(&record).unwrap();
    let deserialized = from_value::<MyRecord>(&serialized).unwrap();
    assert_eq!(deserialized, record);
}

#[wasm_bindgen_test]
fn write_read() {
    console_error_panic_hook::set_once();

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

    let mut record = Record::new(&schema).unwrap();
    record.put("a", 12_i32);
    record.put("b", "hello".to_owned());

    let mut writer = Writer::with_codec(&schema, BufWriter::new(Vec::with_capacity(200)), Codec::Null);
    writer.append(record).unwrap();
    writer.flush().unwrap();
    let bytes = writer.into_inner().unwrap().into_inner().unwrap();

    let reader = Reader::new(&bytes[..]).unwrap();

    for value in reader {
        match value {
            Ok(record) => println!("Successfully read {:?}", record),
            Err(err) => panic!("An error occurred while reading: {:?}", err),
        }
    }
}
