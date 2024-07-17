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

use std::cell::Cell;

thread_local! {
    /// A thread local that is used to decide how to serialize Rust bytes into an Avro
    /// `types::Value` of type bytes.
    ///
    /// Relies on the fact that serde's serialization process is single-threaded.
    pub(crate) static SER_BYTES_TYPE: Cell<BytesType> = const { Cell::new(BytesType::Bytes) };

    /// A thread local that is used to decide how to deserialize an Avro `types::Value`
    /// of type bytes into Rust bytes.
    ///
    /// Relies on the fact that serde's deserialization process is single-threaded.
    pub(crate) static DE_BYTES_BORROWED: Cell<bool> = const { Cell::new(false) };
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum BytesType {
    Bytes,
    Fixed,
}

/// Efficient (de)serialization of Avro bytes values.
///
/// This module is intended to be used through the Serde `with` attribute. See below
/// example:
///
/// ```rust
/// use apache_avro::{serde_avro_bytes, serde_avro_fixed};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct StructWithBytes {
///     #[serde(with = "serde_avro_bytes")]
///     vec_field: Vec<u8>,
///
///     #[serde(with = "serde_avro_fixed")]
///     fixed_field: [u8; 6],
/// }
/// ```
pub mod serde_avro_bytes {
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serde_bytes::serialize(bytes, serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        serde_bytes::deserialize(deserializer)
    }
}

/// Efficient (de)serialization of Avro fixed values.
///
/// This module is intended to be used through the Serde `with` attribute. See below
/// example:
///
/// ```rust
/// use apache_avro::{serde_avro_bytes, serde_avro_fixed};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct StructWithBytes {
///     #[serde(with = "serde_avro_bytes")]
///     vec_field: Vec<u8>,
///
///     #[serde(with = "serde_avro_fixed")]
///     fixed_field: [u8; 6],
/// }
/// ```
pub mod serde_avro_fixed {
    use super::{BytesType, SER_BYTES_TYPE};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        SER_BYTES_TYPE.set(BytesType::Fixed);
        let res = serde_bytes::serialize(bytes, serializer);
        SER_BYTES_TYPE.set(BytesType::Bytes);
        res
    }

    pub fn deserialize<'de, D: Deserializer<'de>, const N: usize>(
        deserializer: D,
    ) -> Result<[u8; N], D::Error> {
        serde_bytes::deserialize(deserializer)
    }
}

/// Efficient (de)serialization of Avro bytes/fixed borrowed values.
///
/// This module is intended to be used through the Serde `with` attribute. Note that
/// `bytes: &[u8]` are always serialized as
/// [`Value::Bytes`](crate::types::Value::Bytes). However, both
/// [`Value::Bytes`](crate::types::Value::Bytes) and
/// [`Value::Fixed`](crate::types::Value::Fixed) can be deserialized as `bytes:
/// &[u8]`. See below example:
///
/// ```rust
/// use apache_avro::serde_avro_slice;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct StructWithBytes<'a> {
///     #[serde(with = "serde_avro_slice")]
///     slice_field: &'a [u8],
/// }
/// ```
pub mod serde_avro_slice {
    use super::DE_BYTES_BORROWED;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serde_bytes::serialize(bytes, serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<&'de [u8], D::Error> {
        DE_BYTES_BORROWED.set(true);
        let res = serde_bytes::deserialize(deserializer);
        DE_BYTES_BORROWED.set(false);
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{from_value, to_value, types::Value, Schema};
    use serde::{Deserialize, Serialize};

    #[test]
    fn avro_3631_validate_schema_for_struct_with_byte_types() {
        #[derive(Debug, Serialize)]
        struct TestStructWithBytes<'a> {
            #[serde(with = "serde_avro_bytes")]
            vec_field: Vec<u8>,

            #[serde(with = "serde_avro_fixed")]
            fixed_field: [u8; 6],

            #[serde(with = "serde_avro_slice")]
            slice_field: &'a [u8],
        }

        let test = TestStructWithBytes {
            vec_field: vec![2, 3, 4],
            fixed_field: [1; 6],
            slice_field: &[1, 2, 3],
        };
        let value: Value = to_value(test).unwrap();
        let schema = Schema::parse_str(
            r#"
            {
                "type": "record",
                "name": "TestStructFixedField",
                "fields": [
                    {
                        "name": "vec_field",
                        "type": "bytes"
                    },
                    {
                        "name": "fixed_field",
                        "type": {
                            "name": "fixed_field",
                            "type": "fixed",
                            "size": 6
                        }
                    },
                    {
                        "name": "slice_field",
                        "type": "bytes"
                    }
                ]
            }
            "#,
        )
        .unwrap();
        assert!(value.validate(&schema));
    }

    #[test]
    fn avro_3631_deserialize_value_to_struct_with_byte_types() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct TestStructWithBytes<'a> {
            #[serde(with = "serde_avro_bytes")]
            vec_field: Vec<u8>,

            #[serde(with = "serde_avro_fixed")]
            fixed_field: [u8; 6],

            #[serde(with = "serde_avro_slice")]
            slice_field: &'a [u8],
            #[serde(with = "serde_avro_slice")]
            slice_field2: &'a [u8],
        }

        let expected = TestStructWithBytes {
            vec_field: vec![3, 33],
            fixed_field: [1; 6],
            slice_field: &[1, 11, 111],
            slice_field2: &[2, 22, 222],
        };

        let value = Value::Record(vec![
            (
                "vec_field".to_owned(),
                Value::Bytes(expected.vec_field.clone()),
            ),
            (
                "fixed_field".to_owned(),
                Value::Fixed(expected.fixed_field.len(), expected.fixed_field.to_vec()),
            ),
            (
                "slice_field".to_owned(),
                Value::Bytes(expected.slice_field.to_vec()),
            ),
            (
                "slice_field2".to_owned(),
                Value::Fixed(expected.slice_field2.len(), expected.slice_field2.to_vec()),
            ),
        ]);
        assert_eq!(expected, from_value(&value).unwrap());
    }

    #[test]
    fn avro_3631_serialize_struct_to_value_with_byte_types() {
        #[derive(Debug, Serialize)]
        struct TestStructFixedField<'a> {
            array_field: &'a [u8],

            vec_field: Vec<u8>,
            #[serde(with = "serde_avro_fixed")]
            vec_field2: Vec<u8>,
            #[serde(with = "serde_avro_bytes")]
            vec_field3: Vec<u8>,

            #[serde(with = "serde_avro_fixed")]
            fixed_field: [u8; 6],
            #[serde(with = "serde_avro_fixed")]
            fixed_field2: &'a [u8],

            #[serde(with = "serde_avro_bytes")]
            bytes_field: &'a [u8],
            #[serde(with = "serde_avro_bytes")]
            bytes_field2: [u8; 6],
        }

        let test = TestStructFixedField {
            array_field: &[1, 11, 111],
            vec_field: vec![3, 33],
            vec_field2: vec![4, 44],
            vec_field3: vec![5, 55],
            fixed_field: [1; 6],
            fixed_field2: &[6, 66],
            bytes_field: &[2, 22, 222],
            bytes_field2: [2; 6],
        };
        let expected = Value::Record(vec![
            (
                "array_field".to_owned(),
                Value::Array(
                    test.array_field
                        .iter()
                        .map(|&i| Value::Int(i as i32))
                        .collect(),
                ),
            ),
            (
                "vec_field".to_owned(),
                Value::Array(
                    test.vec_field
                        .iter()
                        .map(|&i| Value::Int(i as i32))
                        .collect(),
                ),
            ),
            (
                "vec_field2".to_owned(),
                Value::Fixed(2, test.vec_field2.clone()),
            ),
            (
                "vec_field3".to_owned(),
                Value::Bytes(test.vec_field3.clone()),
            ),
            (
                "fixed_field".to_owned(),
                Value::Fixed(6, test.fixed_field.to_vec()),
            ),
            (
                "fixed_field2".to_owned(),
                Value::Fixed(2, test.fixed_field2.to_vec()),
            ),
            (
                "bytes_field".to_owned(),
                Value::Bytes(test.bytes_field.to_vec()),
            ),
            (
                "bytes_field2".to_owned(),
                Value::Bytes(test.bytes_field2.to_vec()),
            ),
        ]);
        assert_eq!(expected, to_value(test).unwrap());
    }
}
