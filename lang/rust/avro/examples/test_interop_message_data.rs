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

use apache_avro::{schema::AvroSchema, types::Value};

struct InteropMessage;

impl AvroSchema for InteropMessage {
    fn get_schema() -> apache_avro::Schema {
        let schema = std::fs::read_to_string("../../share/test/data/messageV1/test_schema.avsc")
            .expect("File should exist with schema inside");
        apache_avro::Schema::parse_str(schema.as_str())
            .expect("File should exist with schema inside")
    }
}

impl From<InteropMessage> for Value {
    fn from(_: InteropMessage) -> Value {
        Value::Record(vec![
            ("id".into(), 42i64.into()),
            ("name".into(), "Bill".into()),
            (
                "tags".into(),
                Value::Array(
                    vec!["dog_lover", "cat_hater"]
                        .into_iter()
                        .map(|s| s.into())
                        .collect(),
                ),
            ),
        ])
    }
}

fn main() {
    let file_message = std::fs::read("../../share/test/data/messageV1/test_message.bin")
        .expect("File not found or error reading");
    let mut generated_encoding: Vec<u8> = Vec::new();
    apache_avro::SingleObjectWriter::<InteropMessage>::with_capacity(1024)
        .expect("resolve expected")
        .write_value(InteropMessage, &mut generated_encoding)
        .expect("Should encode");
    assert_eq!(file_message, generated_encoding)
}
