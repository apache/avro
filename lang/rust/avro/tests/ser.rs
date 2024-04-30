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

use apache_avro::{to_value, types::Value};
use serde::{Deserialize, Serialize};

#[test]
fn avro_3631_visibility_of_avro_serialize_bytes_type() {
    use apache_avro::{avro_serialize_bytes, avro_serialize_fixed};

    #[derive(Debug, Serialize, Deserialize)]
    struct TestStructFixedField<'a> {
        // will be serialized as Value::Bytes
        #[serde(serialize_with = "avro_serialize_bytes")]
        bytes_field: &'a [u8],

        // will be serialized as Value::Fixed
        #[serde(serialize_with = "avro_serialize_fixed")]
        fixed_field: [u8; 6],
    }

    let test = TestStructFixedField {
        bytes_field: &[2, 22, 222],
        fixed_field: [1; 6],
    };

    let expected = Value::Record(vec![
        (
            "bytes_field".to_owned(),
            Value::Bytes(Vec::from(test.bytes_field)),
        ),
        (
            "fixed_field".to_owned(),
            Value::Fixed(6, Vec::from(test.fixed_field)),
        ),
    ]);

    assert_eq!(expected, to_value(test).unwrap());
}
