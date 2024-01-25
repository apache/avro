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

use crate::{schema::Namespace, AvroResult, Error};
use regex_lite::Regex;
use std::sync::OnceLock;

/// A validator that validates names and namespaces according to the Avro specification.
struct SpecificationValidator;

/// A trait that validates schema names.
/// To register a custom one use [set_schema_name_validator].
pub trait SchemaNameValidator: Send + Sync {
    /// Returns the regex used to validate the schema name
    /// according to the Avro specification.
    fn regex(&self) -> &'static Regex {
        static SCHEMA_NAME_ONCE: OnceLock<Regex> = OnceLock::new();
        SCHEMA_NAME_ONCE.get_or_init(|| {
            Regex::new(
                // An optional namespace (with optional dots) followed by a name without any dots in it.
                r"^((?P<namespace>([A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*)?)\.)?(?P<name>[A-Za-z_][A-Za-z0-9_]*)$",
            )
                .unwrap()
        })
    }

    /// Validates the schema name and returns the name and the optional namespace,
    /// or [Error::InvalidSchemaName] if it is invalid.
    fn validate(&self, schema_name: &str) -> AvroResult<(String, Namespace)>;
}

impl SchemaNameValidator for SpecificationValidator {
    fn validate(&self, schema_name: &str) -> AvroResult<(String, Namespace)> {
        let regex = SchemaNameValidator::regex(self);
        let caps = regex
            .captures(schema_name)
            .ok_or_else(|| Error::InvalidSchemaName(schema_name.to_string(), regex.as_str()))?;
        Ok((
            caps["name"].to_string(),
            caps.name("namespace").map(|s| s.as_str().to_string()),
        ))
    }
}

static NAME_VALIDATOR_ONCE: OnceLock<Box<dyn SchemaNameValidator + Send + Sync>> = OnceLock::new();

/// Sets a custom schema name validator.
///
/// Returns a unit if the registration was successful or the already
/// registered validator if the registration failed.
///
/// **Note**: This function must be called before parsing any schema because this will
/// register the default validator and the registration is one time only!
pub fn set_schema_name_validator(
    validator: Box<dyn SchemaNameValidator + Send + Sync>,
) -> Result<(), Box<dyn SchemaNameValidator + Send + Sync>> {
    debug!("Setting a custom schema name validator.");
    NAME_VALIDATOR_ONCE.set(validator)
}

pub(crate) fn validate_schema_name(schema_name: &str) -> AvroResult<(String, Namespace)> {
    NAME_VALIDATOR_ONCE
        .get_or_init(|| {
            debug!("Going to use the default name validator.");
            Box::new(SpecificationValidator)
        })
        .validate(schema_name)
}

/// A trait that validates schema namespaces.
/// To register a custom one use [set_schema_namespace_validator].
pub trait SchemaNamespaceValidator: Send + Sync {
    /// Returns the regex used to validate the schema namespace
    /// according to the Avro specification.
    fn regex(&self) -> &'static Regex {
        static NAMESPACE_ONCE: OnceLock<Regex> = OnceLock::new();
        NAMESPACE_ONCE.get_or_init(|| {
            Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*)?$").unwrap()
        })
    }

    /// Validates the schema namespace or [Error::InvalidNamespace] if it is invalid.
    fn validate(&self, namespace: &str) -> AvroResult<()>;
}

impl SchemaNamespaceValidator for SpecificationValidator {
    fn validate(&self, ns: &str) -> AvroResult<()> {
        let regex = SchemaNamespaceValidator::regex(self);
        if !regex.is_match(ns) {
            return Err(Error::InvalidNamespace(ns.to_string(), regex.as_str()));
        } else {
            Ok(())
        }
    }
}

static NAMESPACE_VALIDATOR_ONCE: OnceLock<Box<dyn SchemaNamespaceValidator + Send + Sync>> =
    OnceLock::new();

/// Sets a custom schema namespace validator.
///
/// Returns a unit if the registration was successful or the already
/// registered validator if the registration failed.
///
/// **Note**: This function must be called before parsing any schema because this will
/// register the default validator and the registration is one time only!
pub fn set_schema_namespace_validator(
    validator: Box<dyn SchemaNamespaceValidator + Send + Sync>,
) -> Result<(), Box<dyn SchemaNamespaceValidator + Send + Sync>> {
    NAMESPACE_VALIDATOR_ONCE.set(validator)
}

pub(crate) fn validate_namespace(ns: &str) -> AvroResult<()> {
    NAMESPACE_VALIDATOR_ONCE
        .get_or_init(|| {
            debug!("Going to use the default namespace validator.");
            Box::new(SpecificationValidator)
        })
        .validate(ns)
}

/// A trait that validates enum symbol names.
/// To register a custom one use [set_enum_symbol_name_validator].
pub trait EnumSymbolNameValidator: Send + Sync {
    /// Returns the regex used to validate the symbols of enum schema
    /// according to the Avro specification.
    fn regex(&self) -> &'static Regex {
        static ENUM_SYMBOL_NAME_ONCE: OnceLock<Regex> = OnceLock::new();
        ENUM_SYMBOL_NAME_ONCE.get_or_init(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap())
    }

    /// Validates the symbols of an Enum schema name and returns nothing (unit),
    /// or [Error::EnumSymbolName] if it is invalid.
    fn validate(&self, name: &str) -> AvroResult<()>;
}

impl EnumSymbolNameValidator for SpecificationValidator {
    fn validate(&self, symbol: &str) -> AvroResult<()> {
        let regex = EnumSymbolNameValidator::regex(self);
        if !regex.is_match(symbol) {
            return Err(Error::EnumSymbolName(symbol.to_string()));
        }

        Ok(())
    }
}

static ENUM_SYMBOL_NAME_VALIDATOR_ONCE: OnceLock<Box<dyn EnumSymbolNameValidator + Send + Sync>> =
    OnceLock::new();

/// Sets a custom enum symbol name validator.
///
/// Returns a unit if the registration was successful or the already
/// registered validator if the registration failed.
///
/// **Note**: This function must be called before parsing any schema because this will
/// register the default validator and the registration is one time only!
pub fn set_enum_symbol_name_validator(
    validator: Box<dyn EnumSymbolNameValidator + Send + Sync>,
) -> Result<(), Box<dyn EnumSymbolNameValidator + Send + Sync>> {
    ENUM_SYMBOL_NAME_VALIDATOR_ONCE.set(validator)
}

pub(crate) fn validate_enum_symbol_name(symbol: &str) -> AvroResult<()> {
    ENUM_SYMBOL_NAME_VALIDATOR_ONCE
        .get_or_init(|| {
            debug!("Going to use the default enum symbol name validator.");
            Box::new(SpecificationValidator)
        })
        .validate(symbol)
}

/// A trait that validates record field names.
/// To register a custom one use [set_record_field_name_validator].
pub trait RecordFieldNameValidator: Send + Sync {
    /// Returns the regex used to validate the record field names
    /// according to the Avro specification.
    fn regex(&self) -> &'static Regex {
        static FIELD_NAME_ONCE: OnceLock<Regex> = OnceLock::new();
        FIELD_NAME_ONCE.get_or_init(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap())
    }

    /// Validates the record field's names and returns nothing (unit),
    /// or [Error::FieldName] if it is invalid.
    fn validate(&self, name: &str) -> AvroResult<()>;
}

impl RecordFieldNameValidator for SpecificationValidator {
    fn validate(&self, field_name: &str) -> AvroResult<()> {
        let regex = RecordFieldNameValidator::regex(self);
        if !regex.is_match(field_name) {
            return Err(Error::FieldName(field_name.to_string()));
        }

        Ok(())
    }
}

static RECORD_FIELD_NAME_VALIDATOR_ONCE: OnceLock<Box<dyn RecordFieldNameValidator + Send + Sync>> =
    OnceLock::new();

/// Sets a custom record field name validator.
///
/// Returns a unit if the registration was successful or the already
/// registered validator if the registration failed.
///
/// **Note**: This function must be called before parsing any schema because this will
/// register the default validator and the registration is one time only!
pub fn set_record_field_name_validator(
    validator: Box<dyn RecordFieldNameValidator + Send + Sync>,
) -> Result<(), Box<dyn RecordFieldNameValidator + Send + Sync>> {
    RECORD_FIELD_NAME_VALIDATOR_ONCE.set(validator)
}

pub(crate) fn validate_record_field_name(field_name: &str) -> AvroResult<()> {
    RECORD_FIELD_NAME_VALIDATOR_ONCE
        .get_or_init(|| {
            debug!("Going to use the default record field name validator.");
            Box::new(SpecificationValidator)
        })
        .validate(field_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Name;
    use apache_avro_test_helper::TestResult;

    #[test]
    fn avro_3900_default_name_validator_with_valid_ns() -> TestResult {
        validate_schema_name("example")?;
        Ok(())
    }

    #[test]
    fn avro_3900_default_name_validator_with_invalid_ns() -> TestResult {
        assert!(validate_schema_name("com-example").is_err());
        Ok(())
    }

    #[test]
    fn test_avro_3897_disallow_invalid_namespaces_in_fully_qualified_name() -> TestResult {
        let full_name = "ns.0.record1";
        let name = Name::new(full_name);
        assert!(name.is_err());
        let validator = SpecificationValidator;
        let expected = Error::InvalidSchemaName(
            full_name.to_string(),
            SchemaNameValidator::regex(&validator).as_str(),
        )
        .to_string();
        let err = name.map_err(|e| e.to_string()).err().unwrap();
        pretty_assertions::assert_eq!(expected, err);

        let full_name = "ns..record1";
        let name = Name::new(full_name);
        assert!(name.is_err());
        let expected = Error::InvalidSchemaName(
            full_name.to_string(),
            SchemaNameValidator::regex(&validator).as_str(),
        )
        .to_string();
        let err = name.map_err(|e| e.to_string()).err().unwrap();
        pretty_assertions::assert_eq!(expected, err);
        Ok(())
    }

    #[test]
    fn avro_3900_default_namespace_validator_with_valid_ns() -> TestResult {
        validate_namespace("com.example")?;
        Ok(())
    }

    #[test]
    fn avro_3900_default_namespace_validator_with_invalid_ns() -> TestResult {
        assert!(validate_namespace("com-example").is_err());
        Ok(())
    }

    #[test]
    fn avro_3900_default_enum_symbol_validator_with_valid_symbol_name() -> TestResult {
        validate_enum_symbol_name("spades")?;
        Ok(())
    }

    #[test]
    fn avro_3900_default_enum_symbol_validator_with_invalid_symbol_name() -> TestResult {
        assert!(validate_enum_symbol_name("com-example").is_err());
        Ok(())
    }

    #[test]
    fn avro_3900_default_record_field_validator_with_valid_name() -> TestResult {
        validate_record_field_name("test")?;
        Ok(())
    }

    #[test]
    fn avro_3900_default_record_field_validator_with_invalid_name() -> TestResult {
        assert!(validate_record_field_name("com-example").is_err());
        Ok(())
    }
}
