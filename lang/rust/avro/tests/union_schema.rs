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

use apache_avro::{from_value, AvroResult, Codec, Reader, Schema, Writer};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

static SCHEMA_A_STR: &str = r#"{
        "name": "A",
        "type": "record",
        "fields": [
            {"name": "field_a", "type": "float"}
        ]
    }"#;

static SCHEMA_B_STR: &str = r#"{
        "name": "B",
        "type": "record",
        "fields": [
            {"name": "field_b", "type": "long"}
        ]
    }"#;

static SCHEMA_C_STR: &str = r#"{
        "name": "C",
        "type": "record",
        "fields": [
            {"name": "field_union", "type": ["A", "B"]},
            {"name": "field_c", "type": "string"}
        ]
    }"#;

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct A {
    field_a: f32,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct B {
    field_b: i64,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
#[serde(untagged)]
enum UnionAB {
    A(A),
    B(B),
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct C {
    field_union: UnionAB,
    field_c: String,
}

fn encode_decode<T>(input: &T, schema: &Schema, schemata: &[Schema]) -> AvroResult<T>
where
    T: DeserializeOwned + Serialize,
{
    let mut encoded: Vec<u8> = Vec::new();
    let mut writer =
        Writer::with_schemata(schema, schemata.iter().collect(), &mut encoded, Codec::Null);
    writer.append_ser(input)?;
    writer.flush()?;

    let mut reader = Reader::with_schemata(schema, schemata.iter().collect(), encoded.as_slice())?;
    from_value::<T>(&reader.next().expect("")?)
}

#[test]
fn test_avro_3901_union_schema_round_trip_no_null() -> AvroResult<()> {
    let schemata: Vec<Schema> =
        Schema::parse_list(&[SCHEMA_A_STR, SCHEMA_B_STR, SCHEMA_C_STR]).expect("parsing schemata");

    let input = C {
        field_union: (UnionAB::A(A { field_a: 45.5 })),
        field_c: "foo".to_string(),
    };
    let output = encode_decode(&input, &schemata[2], &schemata)?;
    assert_eq!(input, output);

    let input = C {
        field_union: (UnionAB::B(B { field_b: 73 })),
        field_c: "bar".to_string(),
    };
    let output = encode_decode(&input, &schemata[2], &schemata)?;
    assert_eq!(input, output);

    Ok(())
}

static SCHEMA_D_STR: &str = r#"{
        "name": "D",
        "type": "record",
        "fields": [
            {"name": "field_union", "type": ["null", "A", "B"]},
            {"name": "field_d", "type": "string"}
        ]
    }"#;

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
#[serde(untagged)]
enum UnionNoneAB {
    None,
    A(A),
    B(B),
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct D {
    field_union: UnionNoneAB,
    field_d: String,
}

#[test]
fn test_avro_3901_union_schema_round_trip_null_at_start() -> AvroResult<()> {
    let schemata: Vec<Schema> =
        Schema::parse_list(&[SCHEMA_A_STR, SCHEMA_B_STR, SCHEMA_D_STR]).expect("parsing schemata");

    let input = D {
        field_union: UnionNoneAB::A(A { field_a: 54.25 }),
        field_d: "fooy".to_string(),
    };
    let output = encode_decode(&input, &schemata[2], &schemata)?;
    assert_eq!(input, output);

    let input = D {
        field_union: UnionNoneAB::None,
        field_d: "fooyy".to_string(),
    };
    let output = encode_decode(&input, &schemata[2], &schemata)?;
    assert_eq!(input, output);

    let input = D {
        field_union: UnionNoneAB::B(B { field_b: 103 }),
        field_d: "foov".to_string(),
    };
    let output = encode_decode(&input, &schemata[2], &schemata)?;
    assert_eq!(input, output);

    Ok(())
}

static SCHEMA_E_STR: &str = r#"{
        "name": "E",
        "type": "record",
        "fields": [
            {"name": "field_union", "type": ["A", "null", "B"]},
            {"name": "field_e", "type": "string"}
        ]
    }"#;

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
#[serde(untagged)]
enum UnionANoneB {
    A(A),
    None,
    B(B),
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct E {
    field_union: UnionANoneB,
    field_e: String,
}

#[test]
fn test_avro_3901_union_schema_round_trip_with_out_of_order_null() -> AvroResult<()> {
    let schemata: Vec<Schema> =
        Schema::parse_list(&[SCHEMA_A_STR, SCHEMA_B_STR, SCHEMA_E_STR]).expect("parsing schemata");

    let input = E {
        field_union: UnionANoneB::A(A { field_a: 23.75 }),
        field_e: "barme".to_string(),
    };
    let output = encode_decode(&input, &schemata[2], &schemata)?;
    assert_eq!(input, output);

    let input = E {
        field_union: UnionANoneB::None,
        field_e: "barme2".to_string(),
    };
    let output = encode_decode(&input, &schemata[2], &schemata)?;
    assert_eq!(input, output);

    let input = E {
        field_union: UnionANoneB::B(B { field_b: 89 }),
        field_e: "barme3".to_string(),
    };
    let output = encode_decode(&input, &schemata[2], &schemata)?;
    assert_eq!(input, output);

    Ok(())
}

static SCHEMA_F_STR: &str = r#"{
        "name": "F",
        "type": "record",
        "fields": [
            {"name": "field_union", "type": ["A", "B", "null"]},
            {"name": "field_f", "type": "string"}
        ]
    }"#;

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
#[serde(untagged)]
enum UnionABNone {
    A(A),
    B(B),
    None,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct F {
    field_union: UnionABNone,
    field_f: String,
}

#[test]
fn test_avro_3901_union_schema_round_trip_with_end_null() -> AvroResult<()> {
    let schemata: Vec<Schema> =
        Schema::parse_list(&[SCHEMA_A_STR, SCHEMA_B_STR, SCHEMA_F_STR]).expect("parsing schemata");

    let input = F {
        field_union: UnionABNone::A(A { field_a: 23.75 }),
        field_f: "aoe".to_string(),
    };
    let output = encode_decode(&input, &schemata[2], &schemata)?;
    assert_eq!(input, output);

    let input = F {
        field_union: UnionABNone::B(B { field_b: 89 }),
        field_f: "aoe3".to_string(),
    };
    let output = encode_decode(&input, &schemata[2], &schemata)?;
    assert_eq!(input, output);

    let input = F {
        field_union: UnionABNone::None,
        field_f: "aoee2".to_string(),
    };
    let output = encode_decode(&input, &schemata[2], &schemata)?;
    assert_eq!(input, output);

    Ok(())
}

/*
One could make an argument that we should be able to represent a union schema of [null,A,B] as Option<Enum<A,B>>.
This is a failing test to show that we *can't*.  My (Simon Gittins's) feeling is that this should NOT be implemented
static SCHEMA_G_STR: &str = r#"{
        "name": "G",
        "type": "record",
        "fields": [
            {"name": "field_union", "type": ["null", "A", "B"]},
            {"name": "field_g", "type": "string"}
        ]
    }"#;

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct G {
    field_union: Option<UnionAB>,
    field_g: String,
}

#[test]
fn test_avro_3901_union_schema_as_optional_2() -> AvroResult<()> {
    let schemata: Vec<Schema> =
        Schema::parse_list(&[SCHEMA_A_STR, SCHEMA_B_STR, SCHEMA_G_STR]).expect("parsing schemata");

    let input = G {
        field_union: Some(UnionAB::A(A { field_a: 32.25 })),
        field_g: "aj".to_string(),
    };
    let output = encode_decode(&input, &schemata[2], &schemata)?;
    assert_eq!(input, output);

    let input = G {
        field_union: None,
        field_g: "aja".to_string(),
    };
    let output = encode_decode(&input, &schemata[2], &schemata)?;
    assert_eq!(input, output);

    let input = G {
        field_union: Some(UnionAB::B(B { field_b: 44 })),
        field_g: "aju".to_string(),
    };
    let output = encode_decode(&input, &schemata[2], &schemata)?;
    assert_eq!(input, output);

    Ok(())
}
*/
static SCHEMA_H_STR: &str = r#"{
        "name": "H",
        "type": "record",
        "fields": [
            {"name": "field_union", "type": ["null", "long"]},
            {"name": "field_h", "type": "string"}
        ]
    }"#;

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct H {
    field_union: Option<i64>,
    field_h: String,
}

#[test]
fn test_avro_3901_union_schema_as_optional() -> AvroResult<()> {
    let schemata: Vec<Schema> = Schema::parse_list(&[SCHEMA_H_STR]).expect("parsing schemata");

    let input = H {
        field_union: Some(23),
        field_h: "aaa".to_string(),
    };
    let output = encode_decode(&input, &schemata[0], &schemata)?;
    assert_eq!(input, output);

    let input = H {
        field_union: None,
        field_h: "bbb".to_string(),
    };
    let output = encode_decode(&input, &schemata[0], &schemata)?;
    assert_eq!(input, output);

    Ok(())
}
