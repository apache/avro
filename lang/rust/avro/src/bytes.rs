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
/// This module is intended to be used through the Serde `with` attribute. Use
/// [`serde_avro_bytes_opt`](crate::serde_avro_bytes_opt) for optional bytes.
///
/// See usage with below example:
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

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_bytes::serialize(bytes, serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_bytes::deserialize(deserializer)
    }
}

/// Efficient (de)serialization of optional Avro bytes values.
///
/// This module is intended to be used through the Serde `with` attribute. Use
/// [`serde_avro_bytes`](crate::serde_avro_bytes) for non optional bytes.
///
/// See usage with below example:
/// ```rust
/// use apache_avro::{serde_avro_bytes_opt, serde_avro_fixed_opt};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct StructWithBytes {
///     #[serde(with = "serde_avro_bytes_opt")]
///     vec_field: Option<Vec<u8>>,
///
///     #[serde(with = "serde_avro_fixed_opt")]
///     fixed_field: Option<[u8; 6]>,
/// }
/// ```
pub mod serde_avro_bytes_opt {
    use serde::{Deserializer, Serializer};
    use std::borrow::Borrow;

    pub fn serialize<S, B>(bytes: &Option<B>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        B: Borrow<[u8]> + serde_bytes::Serialize,
    {
        serde_bytes::serialize(bytes, serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_bytes::deserialize(deserializer)
    }
}

/// Efficient (de)serialization of Avro fixed values.
///
/// This module is intended to be used through the Serde `with` attribute. Use
/// [`serde_avro_fixed_opt`](crate::serde_avro_fixed_opt) for optional fixed values.
///
/// See usage with below example:
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

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SER_BYTES_TYPE.set(BytesType::Fixed);
        let res = serde_bytes::serialize(bytes, serializer);
        SER_BYTES_TYPE.set(BytesType::Bytes);
        res
    }

    pub fn deserialize<'de, D, const N: usize>(deserializer: D) -> Result<[u8; N], D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_bytes::deserialize(deserializer)
    }
}

/// Efficient (de)serialization of optional Avro fixed values.
///
/// This module is intended to be used through the Serde `with` attribute. Use
/// [`serde_avro_fixed`](crate::serde_avro_fixed) for non optional fixed values.
///
/// See usage with below example:
/// ```rust
/// use apache_avro::{serde_avro_bytes_opt, serde_avro_fixed_opt};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct StructWithBytes {
///     #[serde(with = "serde_avro_bytes_opt")]
///     vec_field: Option<Vec<u8>>,
///
///     #[serde(with = "serde_avro_fixed_opt")]
///     fixed_field: Option<[u8; 6]>,
/// }
/// ```
pub mod serde_avro_fixed_opt {
    use super::{BytesType, SER_BYTES_TYPE};
    use serde::{Deserializer, Serializer};
    use std::borrow::Borrow;

    pub fn serialize<S, B>(bytes: &Option<B>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        B: Borrow<[u8]> + serde_bytes::Serialize,
    {
        SER_BYTES_TYPE.set(BytesType::Fixed);
        let res = serde_bytes::serialize(bytes, serializer);
        SER_BYTES_TYPE.set(BytesType::Bytes);
        res
    }

    pub fn deserialize<'de, D, const N: usize>(deserializer: D) -> Result<Option<[u8; N]>, D::Error>
    where
        D: Deserializer<'de>,
    {
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
/// &[u8]`. Use [`serde_avro_slice_opt`](crate::serde_avro_slice_opt) for optional
/// bytes/fixed borrowed values.
///
/// See usage with below example:
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

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_bytes::serialize(bytes, serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<&'de [u8], D::Error>
    where
        D: Deserializer<'de>,
    {
        DE_BYTES_BORROWED.set(true);
        let res = serde_bytes::deserialize(deserializer);
        DE_BYTES_BORROWED.set(false);
        res
    }
}

/// Efficient (de)serialization of optional Avro bytes/fixed borrowed values.
///
/// This module is intended to be used through the Serde `with` attribute. Note that
/// `bytes: &[u8]` are always serialized as
/// [`Value::Bytes`](crate::types::Value::Bytes). However, both
/// [`Value::Bytes`](crate::types::Value::Bytes) and
/// [`Value::Fixed`](crate::types::Value::Fixed) can be deserialized as `bytes:
/// &[u8]`. Use [`serde_avro_slice`](crate::serde_avro_slice) for non optional
/// bytes/fixed borrowed values.
///
/// See usage with below example:
/// ```rust
/// use apache_avro::serde_avro_slice_opt;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct StructWithBytes<'a> {
///     #[serde(with = "serde_avro_slice_opt")]
///     slice_field: Option<&'a [u8]>,
/// }
/// ```
pub mod serde_avro_slice_opt {
    use super::DE_BYTES_BORROWED;
    use serde::{Deserializer, Serializer};
    use std::borrow::Borrow;

    pub fn serialize<S, B>(bytes: &Option<B>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        B: Borrow<[u8]> + serde_bytes::Serialize,
    {
        serde_bytes::serialize(&bytes, serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<&'de [u8]>, D::Error>
    where
        D: Deserializer<'de>,
    {
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
            #[serde(with = "serde_avro_bytes_opt")]
            vec_field_opt: Option<Vec<u8>>,

            #[serde(with = "serde_avro_fixed")]
            fixed_field: [u8; 6],
            #[serde(with = "serde_avro_fixed_opt")]
            fixed_field_opt: Option<[u8; 7]>,

            #[serde(with = "serde_avro_slice")]
            slice_field: &'a [u8],
            #[serde(with = "serde_avro_slice_opt")]
            slice_field_opt: Option<&'a [u8]>,
        }

        let test = TestStructWithBytes {
            vec_field: vec![2, 3, 4],
            vec_field_opt: Some(vec![2, 3, 4]),
            fixed_field: [1; 6],
            fixed_field_opt: Some([1; 7]),
            slice_field: &[1, 2, 3],
            slice_field_opt: Some(&[1, 2, 3]),
        };
        let value: Value = to_value(test).unwrap();
        let schema = Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "TestStructWithBytes",
              "fields": [ {
                "name": "vec_field",
                "type": "bytes"
              }, {
                "name": "vec_field_opt",
                "type": ["null", "bytes"]
              }, {
                "name": "fixed_field",
                "type": {
                  "name": "ByteData",
                  "type": "fixed",
                  "size": 6
                }
              }, {
                "name": "fixed_field_opt",
                "type": ["null", {
                  "name": "ByteData2",
                  "type": "fixed",
                  "size": 7
                } ]
              }, {
                "name": "slice_field",
                "type": "bytes"
              }, {
                "name": "slice_field_opt",
                "type": ["null", "bytes"]
              } ]
            }"#,
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
            #[serde(with = "serde_avro_bytes_opt")]
            vec_field_opt: Option<Vec<u8>>,
            #[serde(with = "serde_avro_bytes_opt")]
            vec_field_opt2: Option<Vec<u8>>,

            #[serde(with = "serde_avro_fixed")]
            fixed_field: [u8; 6],
            #[serde(with = "serde_avro_fixed_opt")]
            fixed_field_opt: Option<[u8; 7]>,
            #[serde(with = "serde_avro_fixed_opt")]
            fixed_field_opt2: Option<[u8; 8]>,

            #[serde(with = "serde_avro_slice")]
            slice_bytes_field: &'a [u8],
            #[serde(with = "serde_avro_slice_opt")]
            slice_bytes_field_opt: Option<&'a [u8]>,
            #[serde(with = "serde_avro_slice_opt")]
            slice_bytes_field_opt2: Option<&'a [u8]>,

            #[serde(with = "serde_avro_slice")]
            slice_fixed_field: &'a [u8],
            #[serde(with = "serde_avro_slice_opt")]
            slice_fixed_field_opt: Option<&'a [u8]>,
            #[serde(with = "serde_avro_slice_opt")]
            slice_fixed_field_opt2: Option<&'a [u8]>,
        }

        let expected = TestStructWithBytes {
            vec_field: vec![3, 33],
            vec_field_opt: Some(vec![4, 44]),
            vec_field_opt2: None,
            fixed_field: [1; 6],
            fixed_field_opt: Some([7; 7]),
            fixed_field_opt2: None,
            slice_bytes_field: &[1, 11, 111],
            slice_bytes_field_opt: Some(&[5, 5, 5, 5, 5]),
            slice_bytes_field_opt2: None,
            slice_fixed_field: &[2, 22, 222],
            slice_fixed_field_opt: Some(&[3, 3, 3]),
            slice_fixed_field_opt2: None,
        };

        let value = Value::Record(vec![
            (
                "vec_field".to_owned(),
                Value::Bytes(expected.vec_field.clone()),
            ),
            (
                "vec_field_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Bytes(
                        expected.vec_field_opt.as_ref().unwrap().clone(),
                    )),
                ),
            ),
            (
                "vec_field_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "fixed_field".to_owned(),
                Value::Fixed(expected.fixed_field.len(), expected.fixed_field.to_vec()),
            ),
            (
                "fixed_field_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Fixed(
                        expected.fixed_field_opt.as_ref().unwrap().len(),
                        expected.fixed_field_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "fixed_field_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "slice_bytes_field".to_owned(),
                Value::Bytes(expected.slice_bytes_field.to_vec()),
            ),
            (
                "slice_bytes_field_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Bytes(
                        expected.slice_bytes_field_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "slice_bytes_field_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "slice_fixed_field".to_owned(),
                Value::Fixed(
                    expected.slice_fixed_field.len(),
                    expected.slice_fixed_field.to_vec(),
                ),
            ),
            (
                "slice_fixed_field_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Fixed(
                        expected.slice_fixed_field_opt.as_ref().unwrap().len(),
                        expected.slice_fixed_field_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "slice_fixed_field_opt2".to_owned(),
                Value::Union(1, Box::new(Value::Null)),
            ),
        ]);
        assert_eq!(expected, from_value(&value).unwrap());
    }

    #[test]
    fn avro_3631_serialize_struct_to_value_with_byte_types() {
        #[derive(Debug, Serialize)]
        struct TestStructWithBytes<'a> {
            array_field: &'a [u8],
            vec_field: Vec<u8>,

            #[serde(with = "serde_avro_fixed")]
            vec_field2: Vec<u8>,
            #[serde(with = "serde_avro_fixed_opt")]
            vec_field2_opt: Option<Vec<u8>>,
            #[serde(with = "serde_avro_fixed_opt")]
            vec_field2_opt2: Option<Vec<u8>>,

            #[serde(with = "serde_avro_bytes")]
            vec_field3: Vec<u8>,
            #[serde(with = "serde_avro_bytes_opt")]
            vec_field3_opt: Option<Vec<u8>>,
            #[serde(with = "serde_avro_bytes_opt")]
            vec_field3_opt2: Option<Vec<u8>>,

            #[serde(with = "serde_avro_fixed")]
            fixed_field: [u8; 6],
            #[serde(with = "serde_avro_fixed_opt")]
            fixed_field_opt: Option<[u8; 5]>,
            #[serde(with = "serde_avro_fixed_opt")]
            fixed_field_opt2: Option<[u8; 4]>,

            #[serde(with = "serde_avro_fixed")]
            fixed_field2: &'a [u8],
            #[serde(with = "serde_avro_fixed_opt")]
            fixed_field2_opt: Option<&'a [u8]>,
            #[serde(with = "serde_avro_fixed_opt")]
            fixed_field2_opt2: Option<&'a [u8]>,

            #[serde(with = "serde_avro_bytes")]
            bytes_field: &'a [u8],
            #[serde(with = "serde_avro_bytes_opt")]
            bytes_field_opt: Option<&'a [u8]>,
            #[serde(with = "serde_avro_bytes_opt")]
            bytes_field_opt2: Option<&'a [u8]>,

            #[serde(with = "serde_avro_bytes")]
            bytes_field2: [u8; 6],
            #[serde(with = "serde_avro_bytes_opt")]
            bytes_field2_opt: Option<[u8; 7]>,
            #[serde(with = "serde_avro_bytes_opt")]
            bytes_field2_opt2: Option<[u8; 8]>,
        }

        let test = TestStructWithBytes {
            array_field: &[1, 11, 111],
            vec_field: vec![3, 33],
            vec_field2: vec![4, 44],
            vec_field2_opt: Some(vec![14, 144]),
            vec_field2_opt2: None,
            vec_field3: vec![5, 55],
            vec_field3_opt: Some(vec![15, 155]),
            vec_field3_opt2: None,
            fixed_field: [1; 6],
            fixed_field_opt: Some([6; 5]),
            fixed_field_opt2: None,
            fixed_field2: &[6, 66],
            fixed_field2_opt: Some(&[7, 77]),
            fixed_field2_opt2: None,
            bytes_field: &[2, 22, 222],
            bytes_field_opt: Some(&[3, 33, 233]),
            bytes_field_opt2: None,
            bytes_field2: [2; 6],
            bytes_field2_opt: Some([2; 7]),
            bytes_field2_opt2: None,
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
                Value::Fixed(test.vec_field2.len(), test.vec_field2.clone()),
            ),
            (
                "vec_field2_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Fixed(
                        test.vec_field2_opt.as_ref().unwrap().len(),
                        test.vec_field2_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "vec_field2_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "vec_field3".to_owned(),
                Value::Bytes(test.vec_field3.clone()),
            ),
            (
                "vec_field3_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Bytes(test.vec_field3_opt.as_ref().unwrap().clone())),
                ),
            ),
            (
                "vec_field3_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "fixed_field".to_owned(),
                Value::Fixed(test.fixed_field.len(), test.fixed_field.to_vec()),
            ),
            (
                "fixed_field_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Fixed(
                        test.fixed_field_opt.as_ref().unwrap().len(),
                        test.fixed_field_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "fixed_field_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "fixed_field2".to_owned(),
                Value::Fixed(test.fixed_field2.len(), test.fixed_field2.to_vec()),
            ),
            (
                "fixed_field2_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Fixed(
                        test.fixed_field2_opt.as_ref().unwrap().len(),
                        test.fixed_field2_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "fixed_field2_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "bytes_field".to_owned(),
                Value::Bytes(test.bytes_field.to_vec()),
            ),
            (
                "bytes_field_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Bytes(
                        test.bytes_field_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "bytes_field_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "bytes_field2".to_owned(),
                Value::Bytes(test.bytes_field2.to_vec()),
            ),
            (
                "bytes_field2_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Bytes(
                        test.bytes_field2_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "bytes_field2_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
        ]);
        assert_eq!(expected, to_value(test).unwrap());
    }
}
