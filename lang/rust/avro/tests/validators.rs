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
        set_enum_symbol_name_validator, set_record_field_name_validator, set_schema_name_validator,
        set_schema_namespace_validator, EnumSymbolNameValidator, RecordFieldNameValidator,
        SchemaNameValidator, SchemaNamespaceValidator,
    },
    AvroResult,
};
use apache_avro_test_helper::TestResult;

struct CustomValidator;

// Setup the custom validators before the schema is parsed
// because the parsing will trigger the validation and will
// setup the default validator (SpecificationValidator)!
impl SchemaNameValidator for CustomValidator {
    fn validate(&self, schema_name: &str) -> AvroResult<(String, Namespace)> {
        Ok((schema_name.to_string(), None))
    }
}

impl SchemaNamespaceValidator for CustomValidator {
    fn validate(&self, _ns: &str) -> AvroResult<()> {
        Ok(())
    }
}

impl EnumSymbolNameValidator for CustomValidator {
    fn validate(&self, _ns: &str) -> AvroResult<()> {
        Ok(())
    }
}

impl RecordFieldNameValidator for CustomValidator {
    fn validate(&self, _ns: &str) -> AvroResult<()> {
        Ok(())
    }
}

#[test]
fn avro_3900_custom_validator_with_spec_invalid_names() -> TestResult {
    assert!(set_schema_name_validator(Box::new(CustomValidator)).is_ok());
    assert!(set_schema_namespace_validator(Box::new(CustomValidator)).is_ok());
    assert!(set_enum_symbol_name_validator(Box::new(CustomValidator)).is_ok());
    assert!(set_record_field_name_validator(Box::new(CustomValidator)).is_ok());

    let invalid_schema = r#"{
        "name": "invalid-schema-name",
        "namespace": "invalid-namespace",
        "type": "record",
        "fields": [
            {
                "name": "invalid-field-name",
                "type": "int"
            },
            {
                "type": "enum",
                "name": "Test",
                "symbols": ["A-B", "B-A"]
            }
        ]
    }"#;

    apache_avro::Schema::parse_str(invalid_schema)?;

    Ok(())
}
