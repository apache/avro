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
    validator::{
        set_enum_symbol_name_validator, set_namespace_validator, set_record_field_name_validator,
        set_schema_name_validator, EnumSymbolNameValidator, NameValidator, NamespaceValidator,
        RecordFieldNameValidator,
    },
    AvroResult,
};
use apache_avro_test_helper::TestResult;
use regex_lite::Regex;

#[test]
fn avro_3900_custom_name_validator_with_spec_invalid_ns() -> TestResult {
    #[derive(Debug)]
    struct CustomNameValidator;
    impl NameValidator<(String, Namespace)> for CustomNameValidator {
        fn regex(&self) -> &'static Regex {
            unimplemented!()
        }

        fn validate(&self, schema_name: &str) -> AvroResult<(String, Namespace)> {
            Ok((schema_name.to_string(), None))
        }
    }

    assert!(set_schema_name_validator(Box::new(CustomNameValidator)).is_ok());

    let schema = r#"{
        "name": "com-example",
        "type": "int"}
    "#;
    apache_avro::Schema::parse_str(schema)?;

    Ok(())
}

#[test]
fn avro_3900_custom_namespace_validator_with_spec_invalid_ns() -> TestResult {
    #[derive(Debug)]
    struct CustomNamespaceValidator;
    impl NamespaceValidator for CustomNamespaceValidator {
        fn validate(&self, _ns: &str) -> AvroResult<()> {
            Ok(())
        }
    }

    assert!(set_namespace_validator(Box::new(CustomNamespaceValidator)).is_ok());

    let schema = r#"{
        "name": "name",
        "namespace": "com-example",
        "type": "int"}
    "#;
    apache_avro::Schema::parse_str(schema)?;

    Ok(())
}

#[test]
fn avro_3900_custom_enum_symbol_validator_with_spec_invalid_enum_symbol_names() -> TestResult {
    #[derive(Debug)]
    struct CustomValidator;
    impl EnumSymbolNameValidator<()> for CustomValidator {
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

#[test]
fn avro_3900_custom_record_field_validator_with_spec_invalid_field_name() -> TestResult {
    #[derive(Debug)]
    struct CustomValidator;
    impl RecordFieldNameValidator<()> for CustomValidator {
        fn validate(&self, _ns: &str) -> AvroResult<()> {
            Ok(())
        }
    }

    assert!(set_record_field_name_validator(Box::new(CustomValidator)).is_ok());

    let schema = r#"{
        "type": "record",
        "name": "Test",
        "fields": [
            {
                "name": "A-B",
                "type": "int"
            }
        ]
    }"#;
    apache_avro::Schema::parse_str(schema)?;

    Ok(())
}
