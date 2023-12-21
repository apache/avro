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
use std::{fmt::Debug, sync::OnceLock};

#[derive(Debug)]
struct DefaultValidator;

pub trait NameValidator<T>: Send + Sync {
    fn regex(&self) -> &'static Regex;

    fn validate(&self, name: &str) -> AvroResult<(String, Namespace)>;
}

impl NameValidator<(String, Namespace)> for DefaultValidator {
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

    fn validate(&self, schema_name: &str) -> AvroResult<(String, Namespace)> {
        let regex = NameValidator::regex(self);
        let caps = regex
            .captures(schema_name)
            .ok_or_else(|| Error::InvalidSchemaName(schema_name.to_string(), regex.as_str()))?;
        Ok((
            caps["name"].to_string(),
            caps.name("namespace").map(|s| s.as_str().to_string()),
        ))
    }
}

static NAME_VALIDATOR_ONCE: OnceLock<Box<dyn NameValidator<(String, Namespace)> + Send + Sync>> =
    OnceLock::new();

#[allow(dead_code)]
pub fn set_schema_name_validator(
    validator: Box<dyn NameValidator<(String, Namespace)> + Send + Sync>,
) -> Result<(), Box<dyn NameValidator<(String, Namespace)> + Send + Sync>> {
    NAME_VALIDATOR_ONCE.set(validator)
}

pub(crate) fn validate_name(schema_name: &str) -> AvroResult<(String, Namespace)> {
    NAME_VALIDATOR_ONCE
        .get_or_init(|| {
            debug!("Going to use the default name validator.");
            Box::new(DefaultValidator)
        })
        .validate(schema_name)
}

pub trait NamespaceValidator: Sync + Debug {
    fn validate(&self, name: &str) -> AvroResult<()>;
}

impl NamespaceValidator for DefaultValidator {
    fn validate(&self, ns: &str) -> AvroResult<()> {
        static NAMESPACE_ONCE: OnceLock<Regex> = OnceLock::new();
        let regex = NAMESPACE_ONCE.get_or_init(|| {
            Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*)?$").unwrap()
        });

        if !regex.is_match(ns) {
            return Err(Error::InvalidNamespace(ns.to_string(), regex.as_str()));
        } else {
            Ok(())
        }
    }
}

static NAMESPACE_VALIDATOR_ONCE: OnceLock<Box<dyn NamespaceValidator + Send + Sync>> =
    OnceLock::new();

#[allow(dead_code)]
pub fn set_namespace_validator(
    validator: Box<dyn NamespaceValidator + Send + Sync>,
) -> Result<(), Box<dyn NamespaceValidator + Send + Sync>> {
    NAMESPACE_VALIDATOR_ONCE.set(validator)
}

pub(crate) fn validate_namespace(ns: &str) -> AvroResult<()> {
    NAMESPACE_VALIDATOR_ONCE
        .get_or_init(|| {
            debug!("Going to use the default namespace validator.");
            Box::new(DefaultValidator)
        })
        .validate(ns)
}

pub trait EnumSymbolNameValidator<T> {
    fn regex(&self) -> &'static Regex;

    fn validate(&self, name: &str) -> AvroResult<T>;
}

impl EnumSymbolNameValidator<()> for DefaultValidator {
    fn regex(&self) -> &'static Regex {
        static ENUM_SYMBOL_NAME_ONCE: OnceLock<Regex> = OnceLock::new();
        ENUM_SYMBOL_NAME_ONCE.get_or_init(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap())
    }

    fn validate(&self, symbol: &str) -> AvroResult<()> {
        let regex = EnumSymbolNameValidator::regex(self);
        if !regex.is_match(symbol) {
            return Err(Error::EnumSymbolName(symbol.to_string()));
        }

        Ok(())
    }
}

static ENUM_SYMBOL_NAME_VALIDATOR_ONCE: OnceLock<
    Box<dyn EnumSymbolNameValidator<()> + Send + Sync>,
> = OnceLock::new();

#[allow(dead_code)]
pub fn set_enum_symbol_name_validator(
    validator: Box<dyn EnumSymbolNameValidator<()> + Send + Sync>,
) -> Result<(), Box<dyn EnumSymbolNameValidator<()> + Send + Sync>> {
    ENUM_SYMBOL_NAME_VALIDATOR_ONCE.set(validator)
}

pub(crate) fn validate_enum_symbol_name(schema_name: &str) -> AvroResult<()> {
    ENUM_SYMBOL_NAME_VALIDATOR_ONCE
        .get_or_init(|| {
            debug!("Going to use the default enum symbol name validator.");
            Box::new(DefaultValidator)
        })
        .validate(schema_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Name;
    use apache_avro_test_helper::TestResult;

    #[test]
    fn avro_3900_default_name_validator_with_valid_ns() -> TestResult {
        validate_name("example")?;

        Ok(())
    }

    #[test]
    fn avro_3900_default_name_validator_with_invalid_ns() -> TestResult {
        assert!(validate_name("com-example").is_err());

        Ok(())
    }

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
        validate_name("com-example")?;

        Ok(())
    }

    #[test]
    fn test_avro_3897_disallow_invalid_namespaces_in_fully_qualified_name() -> TestResult {
        let full_name = "ns.0.record1";
        let name = Name::new(full_name);
        assert!(name.is_err());
        let validator = DefaultValidator;
        let expected = Error::InvalidSchemaName(
            full_name.to_string(),
            NameValidator::regex(&validator).as_str(),
        )
        .to_string();
        let err = name.map_err(|e| e.to_string()).err().unwrap();
        pretty_assertions::assert_eq!(expected, err);

        let full_name = "ns..record1";
        let name = Name::new(full_name);
        assert!(name.is_err());
        let expected = Error::InvalidSchemaName(
            full_name.to_string(),
            NameValidator::regex(&validator).as_str(),
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
    fn avro_3900_custom_namespace_validator_with_spec_invalid_ns() -> TestResult {
        #[derive(Debug)]
        struct CustomNamespaceValidator;
        impl NamespaceValidator for CustomNamespaceValidator {
            fn validate(&self, _ns: &str) -> AvroResult<()> {
                Ok(())
            }
        }

        assert!(set_namespace_validator(Box::new(CustomNamespaceValidator)).is_ok());
        validate_namespace("com-example")?;

        Ok(())
    }
}
