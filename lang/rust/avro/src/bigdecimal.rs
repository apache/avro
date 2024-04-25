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
    decode::{decode_len, decode_long},
    encode::{encode_bytes, encode_long},
    types::Value,
    Error,
};
pub use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use std::io::Read;

pub(crate) fn serialize_big_decimal(decimal: &BigDecimal) -> Vec<u8> {
    // encode big decimal, without global size
    let mut buffer: Vec<u8> = Vec::new();
    let (big_int, exponent): (BigInt, i64) = decimal.as_bigint_and_exponent();
    let big_endian_value: Vec<u8> = big_int.to_signed_bytes_be();
    encode_bytes(&big_endian_value, &mut buffer);
    encode_long(exponent, &mut buffer);

    // encode global size and content
    let mut final_buffer: Vec<u8> = Vec::new();
    encode_bytes(&buffer, &mut final_buffer);
    final_buffer
}

pub(crate) fn deserialize_big_decimal(bytes: &Vec<u8>) -> Result<BigDecimal, Error> {
    let mut bytes: &[u8] = bytes.as_slice();
    let mut big_decimal_buffer = match decode_len(&mut bytes) {
        Ok(size) => vec![0u8; size],
        Err(err) => return Err(Error::BigDecimalLen(Box::new(err))),
    };

    bytes
        .read_exact(&mut big_decimal_buffer[..])
        .map_err(Error::ReadDouble)?;

    match decode_long(&mut bytes) {
        Ok(Value::Long(scale_value)) => {
            let big_int: BigInt = BigInt::from_signed_bytes_be(&big_decimal_buffer);
            let decimal = BigDecimal::new(big_int, scale_value);
            Ok(decimal)
        }
        _ => Err(Error::BigDecimalScale),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{types::Record, Codec, Reader, Schema, Writer};
    use apache_avro_test_helper::TestResult;
    use bigdecimal::{One, Zero};
    use pretty_assertions::assert_eq;
    use std::{
        fs::File,
        io::BufReader,
        ops::{Div, Mul},
        str::FromStr,
    };

    #[test]
    fn test_avro_3779_bigdecimal_serial() -> TestResult {
        let value: BigDecimal =
            bigdecimal::BigDecimal::from(-1421).div(bigdecimal::BigDecimal::from(2));
        let mut current: BigDecimal = BigDecimal::one();

        for iter in 1..180 {
            let buffer: Vec<u8> = serialize_big_decimal(&current);

            let mut as_slice = buffer.as_slice();
            decode_long(&mut as_slice)?;

            let mut result: Vec<u8> = Vec::new();
            result.extend_from_slice(as_slice);

            let deserialize_big_decimal: Result<BigDecimal, Error> =
                deserialize_big_decimal(&result);
            assert!(
                deserialize_big_decimal.is_ok(),
                "can't deserialize for iter {iter}"
            );
            assert_eq!(current, deserialize_big_decimal?, "not equals for {iter}");
            current = current.mul(&value);
        }

        let buffer: Vec<u8> = serialize_big_decimal(&BigDecimal::zero());
        let mut as_slice = buffer.as_slice();
        decode_long(&mut as_slice)?;

        let mut result: Vec<u8> = Vec::new();
        result.extend_from_slice(as_slice);

        let deserialize_big_decimal: Result<BigDecimal, Error> = deserialize_big_decimal(&result);
        assert!(
            deserialize_big_decimal.is_ok(),
            "can't deserialize for zero"
        );
        assert_eq!(
            BigDecimal::zero(),
            deserialize_big_decimal?,
            "not equals for zero"
        );

        Ok(())
    }

    #[test]
    fn test_avro_3779_record_with_bg() -> TestResult {
        let schema_str = r#"
        {
          "type": "record",
          "name": "test",
          "fields": [
            {
              "name": "field_name",
              "type": "bytes",
              "logicalType": "big-decimal"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;

        // build record with big decimal value
        let mut record = Record::new(&schema).unwrap();
        let val = BigDecimal::new(BigInt::from(12), 2);
        record.put("field_name", val.clone());

        // write a record
        let codec = Codec::Null;
        let mut writer = Writer::builder()
            .schema(&schema)
            .codec(codec)
            .writer(Vec::new())
            .build();

        writer.append(record.clone())?;
        writer.flush()?;

        // read record
        let wrote_data = writer.into_inner()?;
        let mut reader = Reader::new(&wrote_data[..])?;

        let value = reader.next().unwrap()?;

        // extract field value
        let big_decimal_value: &Value = match value {
            Value::Record(ref fields) => Ok(&fields[0].1),
            other => Err(format!("Expected a Value::Record, got: {other:?}")),
        }?;

        let x1res: &BigDecimal = match big_decimal_value {
            Value::BigDecimal(ref s) => Ok(s),
            other => Err(format!("Expected Value::BigDecimal, got: {other:?}")),
        }?;
        assert_eq!(&val, x1res);

        Ok(())
    }

    #[test]
    fn test_avro_3779_from_java_file() -> TestResult {
        // Open file generated with Java code to ensure compatibility
        // with Java big decimal logical type.
        let file: File = File::open("./tests/bigdec.avro")?;
        let mut reader = Reader::new(BufReader::new(&file))?;
        let next_element = reader.next();
        assert!(next_element.is_some());
        let value = next_element.unwrap()?;
        let bg = match value {
            Value::Record(ref fields) => Ok(&fields[0].1),
            other => Err(format!("Expected a Value::Record, got: {other:?}")),
        }?;
        let value_big_decimal = match bg {
            Value::BigDecimal(val) => Ok(val),
            other => Err(format!("Expected a Value::BigDecimal, got: {other:?}")),
        }?;

        let ref_value = BigDecimal::from_str("2.24")?;
        assert_eq!(&ref_value, value_big_decimal);

        Ok(())
    }
}
