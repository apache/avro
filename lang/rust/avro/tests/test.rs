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
    schema_equality::{set_schemata_equality_comparator, SchemataEq, StructFieldEq},
    types::Record,
    Codec, Schema, Writer,
};
use apache_avro_test_helper::TestResult;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;

#[test]
#[ignore]
fn test_11111() -> TestResult {
    let schema_str = r#"
    {
      "type": "record",
      "name": "test",
      "fields": [
        {
          "name": "field_name",
          "type": "bytes",
          "logicalType": "decimal", "precision": 4,    "scale": 2
        }
      ]
    }
    "#;
    let schema = Schema::parse_str(schema_str)?;

    let mut record = Record::new(&schema).unwrap();
    let val = BigDecimal::new(BigInt::from(12), 2);
    record.put("field_name", val);

    let codec = Codec::Null;
    let mut writer = Writer::builder()
        .schema(&schema)
        .codec(codec)
        .writer(Vec::new())
        .build();

    writer.append(record.clone())?;

    Ok(())
}

#[test]
fn test_avro_3939_22222() -> Result<(), Box<dyn SchemataEq>> {
    let a = StructFieldEq {
        include_attributes: false,
    };

    set_schemata_equality_comparator(Box::new(a))?;

    Ok(())
}
