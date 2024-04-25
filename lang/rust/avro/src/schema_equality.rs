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
    schema::{
        ArraySchema, DecimalSchema, EnumSchema, FixedSchema, MapSchema, RecordField, RecordSchema,
        UnionSchema,
    },
    Schema,
};
use std::{fmt::Debug, sync::OnceLock};

/// A trait that compares two schemata for equality.
/// To register a custom one use [set_schemata_equality_comparator].
pub trait SchemataEq: Debug + Send + Sync {
    /// Compares two schemata for equality.
    fn compare(&self, schema_one: &Schema, schema_two: &Schema) -> bool;
}

/// Compares two schemas according to the Avro specification by using
/// their canonical forms.
/// See <https://avro.apache.org/docs/1.11.1/specification/#parsing-canonical-form-for-schemas>
#[derive(Debug)]
pub struct SpecificationEq;
impl SchemataEq for SpecificationEq {
    fn compare(&self, schema_one: &Schema, schema_two: &Schema) -> bool {
        schema_one.canonical_form() == schema_two.canonical_form()
    }
}

/// Compares two schemas for equality field by field, using only the fields that
/// are used to construct their canonical forms.
/// See <https://avro.apache.org/docs/1.11.1/specification/#parsing-canonical-form-for-schemas>
#[derive(Debug)]
pub struct StructFieldEq {
    /// Whether to include custom attributes in the comparison.
    /// The custom attributes are not used to construct the canonical form of the schema!
    pub include_attributes: bool,
}

impl SchemataEq for StructFieldEq {
    fn compare(&self, schema_one: &Schema, schema_two: &Schema) -> bool {
        macro_rules! compare_primitive {
            ($primitive:ident) => {
                if let Schema::$primitive = schema_one {
                    if let Schema::$primitive = schema_two {
                        return true;
                    }
                    return false;
                }
            };
        }

        if schema_one.name() != schema_two.name() {
            return false;
        }

        compare_primitive!(Null);
        compare_primitive!(Boolean);
        compare_primitive!(Int);
        compare_primitive!(Int);
        compare_primitive!(Long);
        compare_primitive!(Float);
        compare_primitive!(Double);
        compare_primitive!(Bytes);
        compare_primitive!(String);
        compare_primitive!(Uuid);
        compare_primitive!(BigDecimal);
        compare_primitive!(Date);
        compare_primitive!(Duration);
        compare_primitive!(TimeMicros);
        compare_primitive!(TimeMillis);
        compare_primitive!(TimestampMicros);
        compare_primitive!(TimestampMillis);
        compare_primitive!(TimestampNanos);
        compare_primitive!(LocalTimestampMicros);
        compare_primitive!(LocalTimestampMillis);
        compare_primitive!(LocalTimestampNanos);

        if self.include_attributes
            && schema_one.custom_attributes() != schema_two.custom_attributes()
        {
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

        if let Schema::Union(UnionSchema {
            schemas: schemas_one,
            ..
        }) = schema_one
        {
            if let Schema::Union(UnionSchema {
                schemas: schemas_two,
                ..
            }) = schema_two
            {
                return schemas_one.len() == schemas_two.len()
                    && schemas_one
                        .iter()
                        .zip(schemas_two.iter())
                        .all(|(s1, s2)| self.compare(s1, s2));
            }
            return false;
        }

        if let Schema::Decimal(DecimalSchema {
            precision: precision_one,
            scale: scale_one,
            ..
        }) = schema_one
        {
            if let Schema::Decimal(DecimalSchema {
                precision: precision_two,
                scale: scale_two,
                ..
            }) = schema_two
            {
                return precision_one == precision_two && scale_one == scale_two;
            }
            return false;
        }

        if let Schema::Array(ArraySchema {
            items: items_one, ..
        }) = schema_one
        {
            if let Schema::Array(ArraySchema {
                items: items_two, ..
            }) = schema_two
            {
                return items_one == items_two;
            }
            return false;
        }

        if let Schema::Map(MapSchema {
            types: types_one, ..
        }) = schema_one
        {
            if let Schema::Map(MapSchema {
                types: types_two, ..
            }) = schema_two
            {
                return self.compare(types_one, types_two);
            }
            return false;
        }

        if let Schema::Ref { name: name_one } = schema_one {
            if let Schema::Ref { name: name_two } = schema_two {
                return name_one == name_two;
            }
            return false;
        }

        error!(
            "This is a bug in schema_equality.rs! The following schemata types are not checked! \
            Please report it to the Avro library maintainers! \
            \n{:?}\n\n{:?}",
            schema_one, schema_two
        );
        false
    }
}

impl StructFieldEq {
    fn compare_fields(&self, fields_one: &[RecordField], fields_two: &[RecordField]) -> bool {
        fields_one.len() == fields_two.len()
            && fields_one
                .iter()
                .zip(fields_two.iter())
                .all(|(f1, f2)| self.compare(&f1.schema, &f2.schema))
    }
}

static SCHEMATA_COMPARATOR_ONCE: OnceLock<Box<dyn SchemataEq>> = OnceLock::new();

/// Sets a custom schemata equality comparator.
///
/// Returns a unit if the registration was successful or the already
/// registered comparator if the registration failed.
///
/// **Note**: This function must be called before parsing any schema because this will
/// register the default comparator and the registration is one time only!
pub fn set_schemata_equality_comparator(
    comparator: Box<dyn SchemataEq>,
) -> Result<(), Box<dyn SchemataEq>> {
    debug!(
        "Setting a custom schemata equality comparator: {:?}.",
        comparator
    );
    SCHEMATA_COMPARATOR_ONCE.set(comparator)
}

pub(crate) fn compare_schemata(schema_one: &Schema, schema_two: &Schema) -> bool {
    SCHEMATA_COMPARATOR_ONCE
        .get_or_init(|| {
            debug!("Going to use the default schemata equality comparator: SpecificationEq.",);
            Box::new(StructFieldEq {
                include_attributes: false,
            })
        })
        .compare(schema_one, schema_two)
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;
    use crate::schema::{Name, RecordFieldOrder};
    use apache_avro_test_helper::TestResult;
    use serde_json::Value;
    use std::collections::BTreeMap;

    const SPECIFICATION_EQ: SpecificationEq = SpecificationEq;
    const STRUCT_FIELD_EQ: StructFieldEq = StructFieldEq {
        include_attributes: false,
    };

    macro_rules! test_primitives {
        ($primitive:ident) => {
            paste::item! {
                #[test]
                fn [<test_avro_3939_compare_schemata_$primitive>]() {
                    let specification_eq_res = SPECIFICATION_EQ.compare(&Schema::$primitive, &Schema::$primitive);
                    let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&Schema::$primitive, &Schema::$primitive);
                    assert_eq!(specification_eq_res, struct_field_eq_res)
                }
            }
        };
    }

    test_primitives!(Null);
    test_primitives!(Boolean);
    test_primitives!(Int);
    test_primitives!(Long);
    test_primitives!(Float);
    test_primitives!(Double);
    test_primitives!(Bytes);
    test_primitives!(String);
    test_primitives!(Uuid);
    test_primitives!(BigDecimal);
    test_primitives!(Date);
    test_primitives!(Duration);
    test_primitives!(TimeMicros);
    test_primitives!(TimeMillis);
    test_primitives!(TimestampMicros);
    test_primitives!(TimestampMillis);
    test_primitives!(TimestampNanos);
    test_primitives!(LocalTimestampMicros);
    test_primitives!(LocalTimestampMillis);
    test_primitives!(LocalTimestampNanos);

    #[test]
    fn test_avro_3939_compare_named_schemata_with_different_names() {
        let schema_one = Schema::Ref {
            name: Name::from("name1"),
        };

        let schema_two = Schema::Ref {
            name: Name::from("name2"),
        };

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        assert!(!specification_eq_res);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(!struct_field_eq_res);

        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_schemata_not_including_attributes() {
        let schema_one = Schema::map_with_attributes(
            Schema::Boolean,
            BTreeMap::from_iter([("key1".to_string(), Value::Bool(true))]),
        );
        let schema_two = Schema::map_with_attributes(
            Schema::Boolean,
            BTreeMap::from_iter([("key2".to_string(), Value::Bool(true))]),
        );
        // STRUCT_FIELD_EQ does not include attributes !
        assert!(STRUCT_FIELD_EQ.compare(&schema_one, &schema_two));
    }

    #[test]
    fn test_avro_3939_compare_schemata_including_attributes() {
        let struct_field_eq = StructFieldEq {
            include_attributes: true,
        };
        let schema_one = Schema::map_with_attributes(
            Schema::Boolean,
            BTreeMap::from_iter([("key1".to_string(), Value::Bool(true))]),
        );
        let schema_two = Schema::map_with_attributes(
            Schema::Boolean,
            BTreeMap::from_iter([("key2".to_string(), Value::Bool(true))]),
        );
        assert!(!struct_field_eq.compare(&schema_one, &schema_two));
    }

    #[test]
    fn test_avro_3939_compare_map_schemata() {
        let schema_one = Schema::map(Schema::Boolean);
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::map(Schema::Boolean);

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Map failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Map failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_array_schemata() {
        let schema_one = Schema::array(Schema::Boolean);
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::array(Schema::Boolean);

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Array failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Array failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_decimal_schemata() {
        let schema_one = Schema::Decimal(DecimalSchema {
            precision: 10,
            scale: 2,
            inner: Box::new(Schema::Bytes),
        });
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::Decimal(DecimalSchema {
            precision: 10,
            scale: 2,
            inner: Box::new(Schema::Bytes),
        });

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Decimal failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Decimal failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_fixed_schemata() {
        let schema_one = Schema::Fixed(FixedSchema {
            name: Name::from("fixed"),
            doc: None,
            size: 10,
            aliases: None,
            attributes: BTreeMap::new(),
        });
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::Fixed(FixedSchema {
            name: Name::from("fixed"),
            doc: None,
            size: 10,
            aliases: None,
            attributes: BTreeMap::new(),
        });

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Fixed failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Fixed failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_enum_schemata() {
        let schema_one = Schema::Enum(EnumSchema {
            name: Name::from("enum"),
            doc: None,
            symbols: vec!["A".to_string(), "B".to_string()],
            default: None,
            aliases: None,
            attributes: BTreeMap::new(),
        });
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::Enum(EnumSchema {
            name: Name::from("enum"),
            doc: None,
            symbols: vec!["A".to_string(), "B".to_string()],
            default: None,
            aliases: None,
            attributes: BTreeMap::new(),
        });

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Enum failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Enum failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_ref_schemata() {
        let schema_one = Schema::Ref {
            name: Name::from("ref"),
        };
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::Ref {
            name: Name::from("ref"),
        };

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Ref failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Ref failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_record_schemata() {
        let schema_one = Schema::Record(RecordSchema {
            name: Name::from("record"),
            doc: None,
            fields: vec![RecordField {
                name: "field".to_string(),
                doc: None,
                default: None,
                schema: Schema::Boolean,
                order: RecordFieldOrder::Ignore,
                aliases: None,
                custom_attributes: BTreeMap::new(),
                position: 0,
            }],
            aliases: None,
            attributes: BTreeMap::new(),
            lookup: Default::default(),
        });
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::Record(RecordSchema {
            name: Name::from("record"),
            doc: None,
            fields: vec![RecordField {
                name: "field".to_string(),
                doc: None,
                default: None,
                schema: Schema::Boolean,
                order: RecordFieldOrder::Ignore,
                aliases: None,
                custom_attributes: BTreeMap::new(),
                position: 0,
            }],
            aliases: None,
            attributes: BTreeMap::new(),
            lookup: Default::default(),
        });

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Record failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Record failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_union_schemata() -> TestResult {
        let schema_one = Schema::Union(UnionSchema::new(vec![Schema::Boolean, Schema::Int])?);
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::Union(UnionSchema::new(vec![Schema::Boolean, Schema::Int])?);

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Union failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Union failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
        Ok(())
    }
}
