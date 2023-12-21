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
    validator::{set_enum_symbol_name_validator, EnumSymbolNameValidator},
    AvroResult,
};
use apache_avro_test_helper::TestResult;

struct CustomValidator;

#[test]
fn avro_3900_custom_enum_symbol_validator_with_spec_invalid_enum_symbol_names() -> TestResult {
    impl EnumSymbolNameValidator for CustomValidator {
        fn validate(&self, _ns: &str) -> AvroResult<()> {
            Ok(())
        }
    }

    assert!(set_enum_symbol_name_validator(Box::new(CustomValidator)).is_ok());

    let schema = r#"{
        "type": "enum",
        "name": "Test",
        "symbols": ["A-B", "B-A"]
    }"#;
    apache_avro::Schema::parse_str(schema)?;

    Ok(())
}
