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

use apache_avro::{from_avro_datum_schemata, to_avro_datum_schemata, types::Value, Schema};
use apache_avro_test_helper::init;

#[test]
fn test_avro_3683_multiple_schemata_to_avro_datum() {
    init();

    let schema_a_str = r#"{
        "name": "A",
        "type": "record",
        "fields": [
            {"name": "field_a", "type": "float"}
        ]
    }"#;
    let schema_b_str = r#"{
        "name": "B",
        "type": "record",
        "fields": [
            {"name": "field_b", "type": "A"}
        ]
    }"#;

    let schemata: Vec<Schema> = Schema::parse_list(&[schema_a_str, schema_b_str]).unwrap();
    let schemata: Vec<&Schema> = schemata.iter().collect();
    let record = Value::Record(vec![(
        "field_b".into(),
        Value::Record(vec![("field_a".into(), Value::Float(1.0))]),
    )]);

    // this is the Schema we want to use for write/read
    let schema_b = schemata[1];
    let expected: Vec<u8> = vec![0, 0, 128, 63];
    let actual = to_avro_datum_schemata(schema_b, &schemata.as_slice(), record.clone()).unwrap();
    assert_eq!(actual, expected);

    let value =
        from_avro_datum_schemata(schema_b, &schemata.as_slice(), &mut actual.as_slice(), None)
            .unwrap();
    assert_eq!(value, record);
}
