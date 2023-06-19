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

use std::str::FromStr;

use crate::{AvroResult, Error};
use num_bigint::{BigInt, Sign};
use thiserror::Error;

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

    fn from_bigint(bigint: BigInt) -> Self {
        let len = (bigint.bits() as f64 / 8.0).ceil() as usize;
        Self { value: bigint, len }
    }

    // NOTE: conversion implementations below might be not that performant.
    // It might be best to rewrite them using bits extraction and float number unpacking.

    /// Converts from f32 by converting number to string firstly, then parsing it.
    pub(crate) fn try_from_f32(num: f32) -> Result<Self, DecimalParsingError> {
        if !num.is_finite() {}
        let string = num.to_string();
        string.parse()
    }

    /// Converts from f64 by converting number to string firstly, then parsing it.
    pub(crate) fn try_from_f64(num: f64) -> Result<Self, DecimalParsingError> {
        let string = num.to_string();
        string.parse()
    }

    /// Returns byte size of the inner `BigInt`.
    pub(crate) fn inner_byte_size(&self) -> u64 {
        let mut bits = self.value.bits();
        if bits % 8 != 0 {
            bits += 8;
        }
        bits / 8
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
#[derive(Debug, Copy, Clone, Error, PartialEq)]
pub enum DecimalParsingError {
    #[error("Exponent of decimal number is invalid - either incomplete or malformed")]
    MalformedExponent,

    #[error("Unexpected symbol occurred: {}", .0)]
    UnexpectedSymbol(char),

    #[error("Failed to initialize bigint to represent decimal")]
    BigIntInitializeFailed,

    #[error("Float is infinite, so could not be represented as a decimal")]
    InfiniteFloat,
}

impl FromStr for Decimal {
    type Err = DecimalParsingError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_decimal(s)
    }
}

/// Extract decimal as bigint from string representation of floating point number.
fn parse_decimal(source: &str) -> Result<Decimal, DecimalParsingError> {
    let mut be_digits = Vec::<u8>::new();
    let mut sign = Sign::Plus;

    // amount of digits before dot.
    let mut dot_position = None;

    let mut exponent = None;

    for (index, symbol) in source.chars().enumerate() {
        match (index, symbol) {
            (0, '+') => {
                sign = Sign::Plus;
            }
            (0, '-') => {
                sign = Sign::Minus;
            }
            // add digit to the resulting vec.
            (_, symbol) if symbol.is_ascii_digit() => {
                be_digits.push(symbol.to_digit(10).unwrap() as u8)
            }
            (_, '.') => {
                // dot was already met.
                if dot_position.is_some() {
                    return Err(DecimalParsingError::UnexpectedSymbol('.'));
                }
                dot_position = Some(be_digits.len());
            }
            (index, 'e' | 'E') => {
                // no digits after exponent.
                if index == source.len() - 1 {
                    return Err(DecimalParsingError::MalformedExponent);
                }
                let parsed_exponent: i64 = source[index + 1..]
                    .parse()
                    .map_err(|_| DecimalParsingError::MalformedExponent)?;
                exponent = Some(parsed_exponent);
                // we must exist the loop, as exponent is always the end of the float.
                break;
            }
            (_, symbol) => return Err(DecimalParsingError::UnexpectedSymbol(symbol)),
        }
    }

    let trailing_zeroes = exponent
        .into_iter()
        // we only care about positive exponent, as it add trailing zeroes to the final number.
        .filter(|exponent| exponent > &0)
        .map(|exponent| exponent as usize)
        // determine, how much zeroes would exponent contribute.
        .map(|exponent| {
            if let Some(dot_position) = dot_position {
                exponent.saturating_sub(be_digits.len() - dot_position)
            } else {
                exponent
            }
        })
        .next()
        .unwrap_or_default();

    // add trailing zeroes.
    be_digits.extend(std::iter::repeat(0).take(trailing_zeroes));

    let bigint = BigInt::from_radix_be(sign, &be_digits, 10)
        .ok_or(DecimalParsingError::BigIntInitializeFailed)?;

    Ok(Decimal::from_bigint(bigint))
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro_test_helper::TestResult;
    use pretty_assertions::assert_eq;
    use std::convert::TryFrom;

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

    #[derive(Debug)]
    struct TestCase<'a> {
        source: &'a str,
        expected: Option<&'a str>,
    }

    impl<'a> TestCase<'a> {
        fn check_str_parsing(&self) {
            let result = parse_decimal(self.source).map(|result| result.value.to_string());
            assert_eq!(
                result.clone().ok(),
                self.expected.map(|value| value.to_string()),
                "test case({self:?}) failed, parsing result is: {result:?}"
            );
        }

        fn check_f32_parsing(&self) {
            let parsed: f32 = self.source.parse().unwrap();
            let result = Decimal::try_from_f32(parsed).map(|result| result.value.to_string());
            assert_eq!(
                result.clone().ok(),
                self.expected.map(|value| value.to_string()),
                "test case({self:?}) failed, parsing result is: {result:?}"
            );
        }

        fn check_f64_parsing(&self) {
            let parsed: f64 = self.source.parse().unwrap();
            let result = Decimal::try_from_f64(parsed).map(|result| result.value.to_string());
            assert_eq!(
                result.clone().ok(),
                self.expected.map(|value| value.to_string()),
                "test case({self:?}) failed, parsing result is: {result:?}"
            );
        }
    }

    const INVALID_FLOATS_TESTCASES: [TestCase; 4] = [
        TestCase {
            source: "1.ee",
            expected: None,
        },
        TestCase {
            source: "+-+1.2",
            expected: None,
        },
        TestCase {
            source: "NaN",
            expected: None,
        },
        TestCase {
            source: "inf",
            expected: None,
        },
    ];

    #[test]
    fn test_str_parsing() -> TestResult {
        // these are two "compatible" decimals - they have the same BigInt representation.
        let huge_decimal = format!("1123456789{}", "0".repeat(10_000));
        let decimal_with_explicit_huge_scale = format!("1.123456789{}", "0".repeat(10_000));

        let success_cases = vec![
            TestCase {
                source: "123",
                expected: Some("123"),
            },
            TestCase {
                source: "1.1",
                expected: Some("11"),
            },
            // even malformed float ideally should be working, as, for example, f64 would allow that.
            TestCase {
                source: "000001.1",
                expected: Some("11"),
            },
            TestCase {
                source: "1.",
                expected: Some("1"),
            },
            TestCase {
                source: ".1",
                expected: Some("1"),
            },
            // exponent must compensate scale.
            TestCase {
                source: "1.123456789e10009",
                expected: Some(&huge_decimal),
            },
            TestCase {
                source: &decimal_with_explicit_huge_scale,
                expected: Some(&huge_decimal),
            },
            TestCase {
                source: "1e5",
                expected: Some("100000"),
            },
            TestCase {
                source: "1e-5",
                expected: Some("1"),
            },
            TestCase {
                source: "-12345.6789",
                expected: Some("-123456789"),
            },
            TestCase {
                source: "-1e+5",
                expected: Some("-100000"),
            },
        ];

        for case in success_cases {
            case.check_str_parsing()
        }

        for case in INVALID_FLOATS_TESTCASES.iter() {
            case.check_str_parsing()
        }

        Ok(())
    }

    #[test]
    fn test_floats_parsing() -> TestResult {
        let success_cases = vec![
            TestCase {
                source: "123",
                expected: Some("123"),
            },
            TestCase {
                source: "1.1",
                expected: Some("11"),
            },
            // even malformed float ideally should be working, as, for example, f64 would allow that.
            TestCase {
                source: "000001.1",
                expected: Some("11"),
            },
            TestCase {
                source: "1.",
                expected: Some("1"),
            },
            TestCase {
                source: ".1",
                expected: Some("1"),
            },
            TestCase {
                source: "1e5",
                expected: Some("100000"),
            },
            TestCase {
                source: "1e-5",
                expected: Some("1"),
            },
            TestCase {
                source: "-12345.678",
                expected: Some("-12345678"),
            },
            TestCase {
                source: "-1e+5",
                expected: Some("-100000"),
            },
        ];

        for case in success_cases {
            case.check_f32_parsing();
            case.check_f64_parsing();
        }

        for case in INVALID_FLOATS_TESTCASES.iter() {
            case.check_str_parsing()
        }

        Ok(())
    }
}
