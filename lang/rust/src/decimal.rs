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

use crate::{AvroResult, Error};
use num_bigint::{BigInt, Sign};
use num_traits::ToPrimitive;
use std::str::FromStr;

/// A struct used to hold decimal values
///
/// Usage:
///
/// ```rust
/// use apache_avro::{Decimal, Error};
/// use num_bigint::ToBigInt;
/// use std::convert::TryFrom;
///
/// let decimal1 = Decimal::from_f64(9936.45).unwrap();
/// let decimal1_as_f64 = decimal1.to_f64();
///
/// let precision = 4;
/// let scale = 2;
/// let decimal2 = Decimal::from_bytes(vec![9, 9, 3, 6], precision, scale);
/// let decimal2_as_f64 = decimal2.to_f64();
///
/// ```
#[derive(Debug, Clone)]
pub struct Decimal {
    value: BigInt,
    len: usize,
    precision: usize,
    scale: usize,
}

// We only care about value equality, not byte length. Can two equal `BigInt`s have two different
// byte lengths?
impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Decimal {
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    fn to_vec(&self) -> AvroResult<Vec<u8>> {
        self.to_sign_extended_bytes_with_len(self.len)
    }

    pub(crate) fn to_sign_extended_bytes_with_len(&self, len: usize) -> AvroResult<Vec<u8>> {
        let sign_byte = 0xFF * u8::from(self.value.sign() == Sign::Minus);
        let mut decimal_bytes = vec![sign_byte; len];
        let raw_bytes = self.value.to_signed_bytes_be();
        let raw_bytes_len = raw_bytes.len();
        let start_byte_index = len.checked_sub(raw_bytes_len).ok_or(Error::SignExtend {
            requested: len,
            needed: raw_bytes_len,
        })?;
        decimal_bytes[start_byte_index..].copy_from_slice(&raw_bytes);
        Ok(decimal_bytes)
    }

    pub fn from_f64(decimal: f64) -> AvroResult<Self> {
        if decimal.is_nan() {
            return Err(Error::DecimalPrecisionNAN);
        }

        let as_str = format!("{}", decimal);
        let (decimal_str, precision, scale) = match as_str.find('.') {
            Some(index) => {
                let lhs = as_str.get(..index).unwrap();
                let rhs = as_str.get(index + 1..).unwrap();
                let number = format!("{}{}", lhs, rhs);
                let rhs_len = rhs.len();
                (number, lhs.len() + rhs_len, rhs_len)
            }
            None => (as_str.clone(), as_str.len(), 0),
        };

        // f64::MAX is 309 digits, so we can't have more than 308.
        if precision > 308 {
            return Err(Error::DecimalPrecisionTooLarge { precision });
        }

        Ok(Self {
            value: BigInt::from_str(decimal_str.as_str()).unwrap(),
            len: decimal_str.len(),
            precision,
            scale,
        })
    }

    /// Create a new decimal from a signed Big-Endian encoded byte array
    pub fn from_bytes<T: AsRef<[u8]>>(bytes: T, precision: usize, scale: usize) -> Self {
        let bytes_ref = bytes.as_ref();
        Self {
            value: BigInt::from_signed_bytes_be(bytes_ref),
            len: bytes_ref.len(),
            precision,
            scale,
        }
    }

    pub fn scale(&self) -> usize {
        self.scale
    }

    pub fn precision(&self) -> usize {
        self.precision
    }

    pub fn to_f64(&self) -> f64 {
        let float = self.value.to_f64().unwrap();

        if self.scale == 0 {
            float
        } else {
            let mut f = float;
            let mut scale = self.scale;
            while scale > 0 {
                f /= 10.0;
                scale -= 1;
            }
            f
        }
    }
}

impl std::convert::TryFrom<&Decimal> for Vec<u8> {
    type Error = Error;

    #[inline]
    fn try_from(decimal: &Decimal) -> Result<Self, Self::Error> {
        decimal.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{schema::Schema, types::Record, Codec, Reader, Writer};
    
    #[test]
    fn test_decimal_from_positive_f64() {
        let precision = 10;
        let scale = 2;
        assert_success(9936.23_f64, precision, scale);
    }

    #[test]
    fn test_decimal_from_negative_f64() {
        let precision = 10;
        let scale = 2;
        assert_success(-9936.23_f64, precision, scale);
    }

    #[test]
    fn test_decimal_from_0_f64() {
        let precision = 10;
        let scale = 2;
        assert_success(0_f64, precision, scale);
    }

    #[test]
    #[should_panic]
    fn test_decimal_from_max_f64() {
        let precision = usize::MAX;
        let scale = 2;
        let decimal_value = f64::MAX;
        get_deserialized_decimal(decimal_value, precision, scale).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_decimal_from_min_f64() {
        let precision = usize::MAX;
        let scale = 2;
        let decimal_value = f64::MIN;
        get_deserialized_decimal(decimal_value, precision, scale).unwrap();
    }

    #[test]
    fn test_decimal_from_f64_with_128_digits() {
        let precision = 128;
        let scale = 0;
        let decimal_value = 1e+127_f64;
        assert_success(decimal_value, precision, scale);
    }

    #[test]
    #[should_panic]
    fn test_decimal_from_f64_with_309_digits_big() {
        let precision = 500;
        let scale = 2;
        let decimal_value = 1e+308_f64;
        get_deserialized_decimal(decimal_value, precision, scale).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_decimal_from_f64_with_309_digits_small() {
        let precision = 500;
        let scale = 2;
        let decimal_value = 1e-308_f64;
        get_deserialized_decimal(decimal_value, precision, scale).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_decimal_from_f64_nan() {
        let precision = usize::MAX;
        let scale = 2;
        let decimal_value = f64::NAN;
        get_deserialized_decimal(decimal_value, precision, scale).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_decimal_from_f64_negative_infinity() {
        let precision = usize::MAX;
        let scale = 2;
        let decimal_value = f64::NEG_INFINITY;
        get_deserialized_decimal(decimal_value, precision, scale).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_decimal_from_f64_positive_infinity() {
        let precision = usize::MAX;
        let scale = 2;
        let decimal_value = f64::INFINITY;
        get_deserialized_decimal(decimal_value, precision, scale).unwrap();
    }

    fn assert_success(decimal_value: f64, precision: usize, scale: usize) {
        match get_deserialized_decimal(decimal_value, precision, scale) {
            Ok(decimal) => {
                assert_eq!(decimal.to_f64(), decimal_value);
                assert_eq!(decimal.precision(), precision);
                assert_eq!(decimal.scale(), scale);
            }
            Err(msg) => panic!("{}", msg),
        }
    }

    fn get_deserialized_decimal(number: f64, precision: usize, scale: usize) -> Result<Decimal, String> {
        let schema_str = r#"
           {
                "type": "record",
                "name":"DecimalTest",
                "fields": [
                   {
                        "name": "decimal",
                        "type": {
                            "type": "bytes",
                            "logicalType": "decimal",
                            "precision": {{PRECISION}},
                            "scale": {{SCALE}}
                        }
                    }
                ]
            }
        "#
            .replace("{{PRECISION}}", &precision.to_string())
            .replace("{{SCALE}}", &scale.to_string());

        let schema = Schema::parse_str(schema_str.as_str()).unwrap();
        let mut datum = Record::new(&schema).unwrap();

        datum.put("decimal", Decimal::from_f64(number).unwrap());
        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Null);
        writer.append(datum).unwrap();
        let bytes = writer.into_inner().unwrap();

        let reader = Reader::new(&bytes[..]).unwrap();
        for value in reader {
            let value = value.unwrap();
            match value {
                crate::types::Value::Record(fields) => {
                    let (_name, decimal) = fields.get(0).unwrap();
                    match decimal {
                        crate::types::Value::Decimal(decimal) => {
                            return Ok(decimal.clone());
                        }
                        _ => panic!("unexpected value: {:?}", decimal),
                    }
                }
                _ => panic!("unexpected type: {:?}", value),
            }
        }
        panic!("unexpected end of reader");
    }
}
