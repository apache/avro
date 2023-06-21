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

use crate::{AvroResult, Error };
use num_bigint::{BigInt, Sign};
use bigdecimal::{BigDecimal};

#[derive(Debug, Clone)]
pub struct Decimal {
    value: BigInt,
    len: usize,
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
        let num_raw_bytes = raw_bytes.len();
        let start_byte_index = len.checked_sub(num_raw_bytes).ok_or(Error::SignExtend {
            requested: len,
            needed: num_raw_bytes,
        })?;
        decimal_bytes[start_byte_index..].copy_from_slice(&raw_bytes);
        Ok(decimal_bytes)
    }
}

impl From<Decimal> for BigInt {
    fn from(decimal: Decimal) -> Self {
        decimal.value
    }
}

/// Gets the internal byte array representation of a referenced decimal.
/// Usage:
/// ```
/// use apache_avro::Decimal;
/// use std::convert::TryFrom;
///
/// let decimal = Decimal::from(vec![1, 24]);
/// let maybe_bytes = <Vec<u8>>::try_from(&decimal);
/// ```
impl std::convert::TryFrom<&Decimal> for Vec<u8> {
    type Error = Error;

    fn try_from(decimal: &Decimal) -> Result<Self, Self::Error> {
        decimal.to_vec()
    }
}

/// Gets the internal byte array representation of an owned decimal.
/// Usage:
/// ```
/// use apache_avro::Decimal;
/// use std::convert::TryFrom;
///
/// let decimal = Decimal::from(vec![1, 24]);
/// let maybe_bytes = <Vec<u8>>::try_from(decimal);
/// ```
impl std::convert::TryFrom<Decimal> for Vec<u8> {
    type Error = Error;

    fn try_from(decimal: Decimal) -> Result<Self, Self::Error> {
        decimal.to_vec()
    }
}

impl<T: AsRef<[u8]>> From<T> for Decimal {
    fn from(bytes: T) -> Self {
        let bytes_ref = bytes.as_ref();
        Self {
            value: BigInt::from_signed_bytes_be(bytes_ref),
            len: bytes_ref.len(),
        }
    }
}


pub(crate) fn serialize_big_decimal(bg: &BigDecimal) -> Result<Vec<u8>, Error> {
    let mut buffer : Vec<u8> = Vec::new();
    let inner: (BigInt, i64) = bg.as_bigint_and_exponent();
    let mut bigint :(Sign, Vec<u8>) = inner.0.to_bytes_le();

    match bigint.0 {
        Sign::Minus => { buffer.push(2u8) }
        Sign::NoSign => { buffer.push(0u8)}
        Sign::Plus => { buffer.push(1u8) }
    }
    let mut vec = bigint.1.len().to_le_bytes().to_vec();

    buffer.append(&mut vec);
    buffer.append(&mut bigint.1);

    let scale: [u8; 8] = inner.1.to_be_bytes();

    buffer.append(&mut scale.to_vec());
    Ok(buffer)
}

pub(crate) fn deserialize_big_decimal(stream: &Vec<u8>) -> Result<BigDecimal, Error>  {
    let sign: Option<&u8> = stream.get(0);
    let the_sign: Sign = match sign {
        Some(0) => Sign::NoSign,
        Some(1) => Sign::Plus,
        Some(2) => Sign::Minus,
        _ => Sign::NoSign
    };
    let x:[u8; 8] = stream[1..=8].try_into().unwrap();
    let size: usize = usize::from_le_bytes(x);
    let end: usize = 9 + size;

    let unsigned_int = stream[9..end].to_vec();
    let bigint = BigInt::from_bytes_le(the_sign, unsigned_int.as_slice());

    let start_scale = end;
    let end_scale = start_scale + 8;
    let scale : i64 = i64::from_be_bytes(stream[start_scale..end_scale].try_into().unwrap());

    let bg = BigDecimal::new(bigint, scale);
    Ok(bg)
}


#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro_test_helper::TestResult;
    use pretty_assertions::assert_eq;
    use std::convert::TryFrom;
    use std::ops::{Div, Mul};
    use bigdecimal::{One, Zero};

    #[test]
    fn test_decimal_from_bytes_from_ref_decimal() -> TestResult {
        let input = vec![1, 24];
        let d = Decimal::from(&input);

        let output = <Vec<u8>>::try_from(&d)?;
        assert_eq!(output, input);

        Ok(())
    }

    #[test]
    fn test_decimal_from_bytes_from_owned_decimal() -> TestResult {
        let input = vec![1, 24];
        let d = Decimal::from(&input);

        let output = <Vec<u8>>::try_from(d)?;
        assert_eq!(output, input);

        Ok(())
    }

    #[test]
    fn test_bigdecimal_serial() -> TestResult {
        let value: bigdecimal::BigDecimal = bigdecimal::BigDecimal::from(-1421).div(bigdecimal::BigDecimal::from(2));
        let mut current: bigdecimal::BigDecimal = bigdecimal::BigDecimal::one();

        for iter in 1..180 {
            let result: Result<Vec<u8>, Error> = serialize_big_decimal(&current);
            assert!(result.is_ok(), "can't serialize for iter {iter}");
            //println!("serialize 1 => {:?}", &result.unwrap().len());
            let deserialize_big_decimal: Result<bigdecimal::BigDecimal, Error> = deserialize_big_decimal(&result.unwrap());
            assert!(deserialize_big_decimal.is_ok(), "can't deserialize for iter {iter}");
            assert_eq!(current, deserialize_big_decimal.unwrap(), "not equals for ${iter}");
            current = current.mul(&value);
        }

        let result: Result<Vec<u8>, Error> = serialize_big_decimal(&BigDecimal::zero());
        assert!(result.is_ok(), "can't serialize for zero");
        //println!("serialize 1 => {:?}", &result.unwrap().len());
        let deserialize_big_decimal: Result<bigdecimal::BigDecimal, Error> = deserialize_big_decimal(&result.unwrap());
        assert!(deserialize_big_decimal.is_ok(), "can't deserialize for zero");
        assert_eq!(BigDecimal::zero(), deserialize_big_decimal.unwrap(), "not equals for zero");

        Ok(())
    }
}
