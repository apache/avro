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
    from_value,
    schema::{AvroSchema, AvroSchemaWithResolved},
    Reader, Schema, Writer,
};
use avro_derive::*;
use serde::{de::DeserializeOwned, ser::Serialize};
use std::collections::HashMap;

#[macro_use]
extern crate serde;

#[cfg(test)]
mod test_derive {
    use std::{
        borrow::{Borrow, Cow},
        sync::Mutex,
    };

    use super::*;

    /// Takes in a type that implements the right combination of traits and runs it through a Serde Cycle and asserts the result is the same
    fn freeze_dry_assert<T>(obj: T)
    where
        T: std::fmt::Debug + Serialize + DeserializeOwned + AvroSchema + Clone + PartialEq,
    {
        let encoded = freeze(obj.clone());
        let dried: T = dry(encoded);
        assert_eq!(obj, dried);
    }

    fn freeze_dry<T>(obj: T) -> T
    where
        T: Serialize + DeserializeOwned + AvroSchema,
    {
        dry(freeze(obj))
    }

    // serialize
    fn freeze<T>(obj: T) -> Vec<u8>
    where
        T: Serialize + AvroSchema,
    {
        let schema = T::get_schema();
        let mut writer = Writer::new(&schema, Vec::new());
        if let Err(e) = writer.append_ser(obj) {
            panic!("{}", e.to_string());
        }
        writer.into_inner().unwrap()
    }

    // deserialize
    fn dry<T>(encoded: Vec<u8>) -> T
    where
        T: DeserializeOwned + AvroSchema,
    {
        assert!(!encoded.is_empty());
        let schema = T::get_schema();
        let reader = Reader::with_schema(&schema, &encoded[..]).unwrap();
        for res in reader {
            match res {
                Ok(value) => {
                    return from_value::<T>(&value).unwrap();
                }
                Err(e) => panic!("{}", e.to_string()),
            }
        }
        unreachable!()
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct TestBasic {
        a: i32,
        b: String,
    }

    #[test]
    fn test_smoke_test() {
        let test = TestBasic {
            a: 27,
            b: "foo".to_owned(),
        };
        freeze_dry_assert(test);
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    #[namespace = "com.testing.namespace"]
    struct TestBasicNamesapce {
        a: i32,
        b: String,
    }

    #[test]
    fn test_basic_namesapce() {
        // TODO assert the schema and its namespace
        println!("{:?}", TestBasicNamesapce::get_schema())
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    #[namespace = "com.testing.complex.namespace"]
    struct TestComplexNamespace {
        a: TestBasicNamesapce,
        b: String,
    }

    #[test]
    fn test_complex_namespace() {
        // TODO assert the schema and the namespaces
        println!("{:?}", TestComplexNamespace::get_schema())
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct TestAllSupportedBaseTypes {
        //Basics test
        a: bool,
        b: i8,
        c: i16,
        d: i32,
        e: u8,
        f: u16,
        g: i64,
        h: f32,
        i: f64,
        j: String,
    }

    #[test]
    fn test_basic_types() {
        // TODO mgrigorov Use property based testing in the future
        let all_basic = TestAllSupportedBaseTypes {
            a: true,
            b: 8_i8,
            c: 16_i16,
            d: 32_i32,
            e: 8_u8,
            f: 16_u16,
            g: 64_i64,
            h: 32.3333_f32,
            i: 64.4444_f64,
            j: "testing string".to_owned(),
        };
        freeze_dry_assert(all_basic);
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct TestNested {
        a: i32,
        b: TestAllSupportedBaseTypes,
    }

    #[test]
    fn test_inner_struct() {
        // TODO mgrigorov Use property based testing in the future
        let all_basic = TestAllSupportedBaseTypes {
            a: true,
            b: 8_i8,
            c: 16_i16,
            d: 32_i32,
            e: 8_u8,
            f: 16_u16,
            g: 64_i64,
            h: 32.3333_f32,
            i: 64.4444_f64,
            j: "testing string".to_owned(),
        };
        let inner_struct = TestNested {
            a: -1600,
            b: all_basic,
        };
        freeze_dry_assert(inner_struct);
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct TestOptional {
        a: Option<i32>,
    }

    #[test]
    fn test_optional_field_some() {
        let optional_field = TestOptional { a: Some(4) };
        freeze_dry_assert(optional_field);
    }

    #[test]
    fn test_optional_field_none() {
        let optional_field = TestOptional { a: None };
        freeze_dry_assert(optional_field);
    }

    /// Generic Containers
    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct TestGeneric<T: AvroSchemaWithResolved> {
        a: String,
        b: Vec<T>,
        c: HashMap<String, T>,
    }

    #[test]
    fn test_generic_container_1() {
        let test_generic = TestGeneric::<i32> {
            a: "testing".to_owned(),
            b: vec![0, 1, 2, 3],
            c: vec![("key".to_owned(), 3)].into_iter().collect(),
        };
        freeze_dry_assert(test_generic);
    }

    #[test]
    fn test_generic_container_2() {
        let test_generic = TestGeneric::<TestAllSupportedBaseTypes> {
            a: "testing".to_owned(),
            b: vec![TestAllSupportedBaseTypes {
                a: true,
                b: 8_i8,
                c: 16_i16,
                d: 32_i32,
                e: 8_u8,
                f: 16_u16,
                g: 64_i64,
                h: 32.3333_f32,
                i: 64.4444_f64,
                j: "testing string".to_owned(),
            }],
            c: vec![(
                "key".to_owned(),
                TestAllSupportedBaseTypes {
                    a: true,
                    b: 8_i8,
                    c: 16_i16,
                    d: 32_i32,
                    e: 8_u8,
                    f: 16_u16,
                    g: 64_i64,
                    h: 32.3333_f32,
                    i: 64.4444_f64,
                    j: "testing string".to_owned(),
                },
            )]
            .into_iter()
            .collect(),
        };
        freeze_dry_assert(test_generic);
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    enum TestAllowedEnum {
        A,
        B,
        C,
        D,
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct TestAllowedEnumNested {
        a: TestAllowedEnum,
        b: String,
    }

    #[test]
    fn test_enum() {
        let enum_included = TestAllowedEnumNested {
            a: TestAllowedEnum::B,
            b: "hey".to_owned(),
        };
        freeze_dry_assert(enum_included);
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct ConsList {
        value: i32,
        next: Option<Box<ConsList>>,
    }

    #[test]
    fn test_cons() {
        let list = ConsList {
            value: 34,
            next: Some(Box::new(ConsList {
                value: 42,
                next: None,
            })),
        };
        freeze_dry_assert(list)
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct ConsListGeneric<T: AvroSchemaWithResolved> {
        value: T,
        next: Option<Box<ConsListGeneric<T>>>,
    }

    #[test]
    fn test_cons_generic() {
        let list = ConsListGeneric::<TestAllowedEnumNested> {
            value: TestAllowedEnumNested {
                a: TestAllowedEnum::B,
                b: "testing".into(),
            },
            next: Some(Box::new(ConsListGeneric::<TestAllowedEnumNested> {
                value: TestAllowedEnumNested {
                    a: TestAllowedEnum::D,
                    b: "testing2".into(),
                },
                next: None,
            })),
        };
        freeze_dry_assert(list)
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct TestArraysSimple {
        a: [i32; 4],
    }

    #[test]
    fn test_simple_array() {
        let test = TestArraysSimple { a: [2, 3, 4, 5] };
        freeze_dry_assert(test)
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct TestComplexArray<T: AvroSchemaWithResolved> {
        a: [T; 2],
    }

    #[test]
    fn test_complex_array() {
        let test = TestComplexArray::<TestBasic> {
            a: [
                TestBasic {
                    a: 27,
                    b: "foo".to_owned(),
                },
                TestBasic {
                    a: 28,
                    b: "bar".to_owned(),
                },
            ],
        };
        freeze_dry_assert(test)
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct Testu8 {
        a: Vec<u8>,
        b: [u8; 2],
    }
    #[test]
    fn test_bytes_handled() {
        let test = Testu8 {
            a: vec![1, 2],
            b: [3, 4],
        };
        freeze_dry_assert(test)
        // don't check for schema equality to allow for transitioning to bytes or fixed types in the future
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema)]
    #[allow(unknown_lints)] // Rust 1.51.0 (MSRV) does not support #[allow(clippy::box_collection)]
    #[allow(clippy::box_collection)]
    struct TestSmartPointers<'a> {
        a: Box<String>,
        b: std::sync::Mutex<Vec<i64>>,
        c: Cow<'a, i32>,
    }

    #[test]
    fn test_smart_pointers() {
        let test = TestSmartPointers {
            a: Box::new("hey".into()),
            b: Mutex::new(vec![42]),
            c: Cow::Owned(32),
        };
        // test serde with manual equality for mutex
        let test = freeze_dry(test);
        assert_eq!(Box::new("hey".into()), test.a);
        assert_eq!(vec![42], *test.b.borrow().lock().unwrap());
        assert_eq!(Cow::Owned::<i32>(32), test.c);
    }

    #[derive(Debug, Serialize, AvroSchema, Clone, PartialEq)]
    struct TestReference<'a> {
        a: &'a Vec<i32>,
        b: &'static str,
        c: &'a f64,
    }

    #[test]
    fn test_reference_struct() {
        let a = vec![34];
        let c = 4.55555555_f64;
        let test = TestReference {
            a: &a,
            b: "testing_static",
            c: &c,
        };
        freeze(test);
    }
}
