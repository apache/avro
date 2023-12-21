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
    schema::Namespace,
    validator::{set_schema_name_validator, SchemaNameValidator},
    AvroResult,
};
use apache_avro_test_helper::TestResult;

struct CustomValidator;

#[test]
fn avro_3900_custom_schema_name_validator_with_spec_invalid_name() -> TestResult {
    impl SchemaNameValidator for CustomValidator {
        fn validate(&self, schema_name: &str) -> AvroResult<(String, Namespace)> {
            Ok((schema_name.to_string(), None))
        }
    }

    assert!(set_schema_name_validator(Box::new(CustomValidator)).is_ok());

    let schema = r#"{
        "name": "com-example",
        "type": "int"}
    "#;
    apache_avro::Schema::parse_str(schema)?;

    Ok(())
}
