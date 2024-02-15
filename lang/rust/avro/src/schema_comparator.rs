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

use crate::{
    schema::{EnumSchema, FixedSchema, RecordField, RecordSchema},
    Schema,
};
use std::sync::OnceLock;

/// A trait that compares two schemata.
/// To register a custom one use [set_schemata_comparator].
pub trait SchemataComparator: Send + Sync {
    /// Compares two schemata for equality.
    fn compare(&self, schema_one: &Schema, schema_two: &Schema) -> bool;
}

/// Compares two schemas according to the Avro specification by using
/// their canonical forms.
/// See https://avro.apache.org/docs/1.11.1/specification/#parsing-canonical-form-for-schemas
struct SpecificationComparator;
impl SchemataComparator for SpecificationComparator {
    fn compare(&self, schema_one: &Schema, schema_two: &Schema) -> bool {
        schema_one.canonical_form() == schema_two.canonical_form()
    }
}

/// Compares two schemas field by field, using only the fields that
/// are used to construct their canonical forms.
/// See https://avro.apache.org/docs/1.11.1/specification/#parsing-canonical-form-for-schemas
struct StructFieldComparator;
impl SchemataComparator for StructFieldComparator {
    fn compare(&self, schema_one: &Schema, schema_two: &Schema) -> bool {
        if schema_one.name() != schema_two.name() {
            return false;
        }

        if let Schema::Null = schema_one {
            if let Schema::Null = schema_two {
                return true;
            }
            return false;
        }

        if let Schema::Boolean = schema_one {
            if let Schema::Boolean = schema_two {
                return true;
            }
            return false;
        }

        if let Schema::Int = schema_one {
            if let Schema::Int = schema_two {
                return true;
            }
            return false;
        }

        if let Schema::Long = schema_one {
            if let Schema::Long = schema_two {
                return true;
            }
            return false;
        }

        if let Schema::Float = schema_one {
            if let Schema::Float = schema_two {
                return true;
            }
            return false;
        }

        if let Schema::Double = schema_one {
            if let Schema::Double = schema_two {
                return true;
            }
            return false;
        }

        if let Schema::Bytes = schema_one {
            if let Schema::Bytes = schema_two {
                return true;
            }
            return false;
        }

        if let Schema::String = schema_one {
            if let Schema::String = schema_two {
                return true;
            }
            return false;
        }

        if let Schema::Record(RecordSchema {
            fields: fields_one, ..
        }) = schema_one
        {
            if let Schema::Record(RecordSchema {
                fields: fields_two, ..
            }) = schema_two
            {
                return self.compare_fields(fields_one, fields_two);
            }
            return false;
        }

        if let Schema::Enum(EnumSchema {
            symbols: symbols_one,
            ..
        }) = schema_one
        {
            if let Schema::Enum(EnumSchema {
                symbols: symbols_two,
                ..
            }) = schema_two
            {
                return symbols_one == symbols_two;
            }
            return false;
        }

        if let Schema::Fixed(FixedSchema { size: size_one, .. }) = schema_one {
            if let Schema::Fixed(FixedSchema { size: size_two, .. }) = schema_two {
                return size_one == size_two;
            }
            return false;
        }

        true
    }
}

impl StructFieldComparator {
    fn compare_fields(&self, fields_one: &[RecordField], fields_two: &[RecordField]) -> bool {
        fields_one.len() == fields_two.len()
            && fields_one
                .iter()
                .zip(fields_two.iter())
                .all(|(f1, f2)| self.compare(&f1.schema, &f2.schema))
    }
}

static SCHEMATA_COMPARATOR_ONCE: OnceLock<Box<dyn SchemataComparator + Send + Sync>> =
    OnceLock::new();

/// Sets a custom schemata comparator.
///
/// Returns a unit if the registration was successful or the already
/// registered comparator if the registration failed.
///
/// **Note**: This function must be called before parsing any schema because this will
/// register the default comparator and the registration is one time only!
pub fn set_schemata_comparator(
    comparator: Box<dyn SchemataComparator + Send + Sync>,
) -> Result<(), Box<dyn SchemataComparator + Send + Sync>> {
    debug!("Setting a custom schemata comparator.");
    SCHEMATA_COMPARATOR_ONCE.set(comparator)
}

pub(crate) fn compare_schemata(schema_one: &Schema, schema_two: &Schema) -> bool {
    SCHEMATA_COMPARATOR_ONCE
        .get_or_init(|| {
            // debug!("Going to use the default schemata comparator.");
            // Box::new(SpecificationComparator)
            Box::new(StructFieldComparator) // TEMPORARY
        })
        .compare(schema_one, schema_two)
}

#[cfg(test)]
mod tests {}
