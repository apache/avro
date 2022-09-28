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
    read_marker,
    types::{Record, Value},
    AvroResult, Reader, Schema, Writer,
};

#[test]
fn avro_3630_append_to_an_existing_file() {
    let schema_str = r#"
            {
                "type": "record",
                "name": "append_to_existing_file",
                "fields": [
                    {"name": "a", "type": "int"}
                ]
            }
        "#;

    let schema = Schema::parse_str(schema_str).expect("Cannot parse the schema");

    let bytes = get_avro_bytes(&schema);

    let marker = read_marker(&bytes[..]);

    let mut writer = Writer::append_to(&schema, bytes, marker);

    writer
        .append(create_datum(&schema, 2))
        .expect("An error occurred while appending more data");

    let new_bytes = writer.into_inner().expect("Cannot get the new bytes");

    let reader = Reader::new(&*new_bytes).expect("Cannot read the new bytes");
    let mut i = 1;
    for value in reader {
        check(value, i);
        i += 1
    }
}

/// Simulates reading from a pre-existing .avro file and returns its bytes
fn get_avro_bytes(schema: &Schema) -> Vec<u8> {
    let mut writer = Writer::new(schema, Vec::new());
    writer
        .append(create_datum(schema, 1))
        .expect("An error while appending data");
    writer.into_inner().expect("Cannot get the Avro bytes")
}

/// Creates a new datum to write
fn create_datum(schema: &Schema, value: i32) -> Record {
    let mut datum = Record::new(schema).unwrap();
    datum.put("a", value);
    datum
}

/// Checks the read values
fn check(value: AvroResult<Value>, expected: i32) {
    match value {
        Ok(value) => match value {
            Value::Record(fields) => match &fields[0] {
                (_, Value::Int(actual)) => assert_eq!(&expected, actual),
                _ => panic!("The field value type must be an Int: {:?}!", &fields[0]),
            },
            _ => panic!("The value type must be a Record: {:?}!", value),
        },
        Err(e) => panic!("Error while reading the data: {:?}", e),
    }
}
