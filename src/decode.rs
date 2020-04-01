use std::collections::HashMap;
use std::io::Read;

use failure::Error;

use crate::schema::Schema;
use crate::types::Value;
use crate::util::{safe_len, zag_i32, zag_i64, DecodeError};

#[inline]
fn decode_long<R: Read>(reader: &mut R) -> Result<Value, Error> {
    zag_i64(reader).map(Value::Long)
}

#[inline]
fn decode_int<R: Read>(reader: &mut R) -> Result<Value, Error> {
    zag_i32(reader).map(Value::Int)
}

#[inline]
fn decode_len<R: Read>(reader: &mut R) -> Result<usize, Error> {
    zag_i64(reader).and_then(|len| safe_len(len as usize))
}

/// Decode a `Value` from avro format given its `Schema`.
pub fn decode<R: Read>(schema: &Schema, reader: &mut R) -> Result<Value, Error> {
    match *schema {
        Schema::Null => Ok(Value::Null),
        Schema::Boolean => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf[..])?;

            match buf[0] {
                0u8 => Ok(Value::Boolean(false)),
                1u8 => Ok(Value::Boolean(true)),
                _ => Err(DecodeError::new("not a bool").into()),
            }
        }
        Schema::Int => decode_int(reader),
        Schema::Long => decode_long(reader),
        Schema::Float => {
            let mut buf = [0u8; std::mem::size_of::<f32>()];
            reader.read_exact(&mut buf[..])?;
            Ok(Value::Float(f32::from_le_bytes(buf)))
        }
        Schema::Double => {
            let mut buf = [0u8; std::mem::size_of::<f64>()];
            reader.read_exact(&mut buf[..])?;
            Ok(Value::Double(f64::from_le_bytes(buf)))
        }
        Schema::Bytes => {
            let len = decode_len(reader)?;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            Ok(Value::Bytes(buf))
        }
        Schema::String => {
            let len = decode_len(reader)?;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;

            String::from_utf8(buf)
                .map(Value::String)
                .map_err(|_| DecodeError::new("not a valid utf-8 string").into())
        }
        Schema::Fixed { size, .. } => {
            let mut buf = vec![0u8; size as usize];
            reader.read_exact(&mut buf)?;
            Ok(Value::Fixed(size, buf))
        }
        Schema::Array(ref inner) => {
            let mut items = Vec::new();

            loop {
                let mut len = zag_i64(reader)?;
                // arrays are 0-terminated, 0i64 is also encoded as 0 in Avro
                // reading a length of 0 means the end of the array
                if len == 0 {
                    break;
                } else if len < 0 {
                    let _size = zag_i64(reader)?;
                    len = -len;
                }
                let len = safe_len(len as usize)?;

                items.reserve(len as usize);
                for _ in 0..len {
                    items.push(decode(inner, reader)?);
                }
            }

            Ok(Value::Array(items))
        }
        Schema::Map(ref inner) => {
            let mut items = HashMap::new();

            loop {
                let mut len = zag_i64(reader)?;
                // maps are 0-terminated, 0i64 is also encoded as 0 in Avro
                // reading a length of 0 means the end of the map
                if len == 0 {
                    break;
                } else if len < 0 {
                    let _size = zag_i64(reader)?;
                    len = -len;
                }
                let len = safe_len(len as usize)?;

                items.reserve(len as usize);
                for _ in 0..len {
                    if let Value::String(key) = decode(&Schema::String, reader)? {
                        let value = decode(inner, reader)?;
                        items.insert(key, value);
                    } else {
                        return Err(DecodeError::new("map key is not a string").into());
                    }
                }
            }

            Ok(Value::Map(items))
        }
        Schema::Union(ref inner) => {
            let index = zag_i64(reader)?;
            let variants = inner.variants();
            match variants.get(index as usize) {
                Some(variant) => decode(variant, reader).map(|x| Value::Union(Box::new(x))),
                None => Err(DecodeError::new("Union index out of bounds").into()),
            }
        }
        Schema::Record { ref fields, .. } => {
            // Benchmarks indicate ~10% improvement using this method.
            let mut items = Vec::with_capacity(fields.len());
            for field in fields {
                // This clone is also expensive. See if we can do away with it...
                items.push((field.name.clone(), decode(&field.schema, reader)?));
            }
            Ok(Value::Record(items))
        }
        Schema::Enum { ref symbols, .. } => {
            if let Value::Int(index) = decode_int(reader)? {
                if index >= 0 && (index as usize) <= symbols.len() {
                    let symbol = symbols[index as usize].clone();
                    Ok(Value::Enum(index, symbol))
                } else {
                    Err(DecodeError::new("enum symbol index out of bounds").into())
                }
            } else {
                Err(DecodeError::new("enum symbol not found").into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value::{Array, Int, Map};

    #[test]
    fn test_decode_array_without_size() {
        let mut input: &[u8] = &[6, 2, 4, 6, 0];
        let result = decode(&Schema::Array(Box::new(Schema::Int)), &mut input);
        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result.unwrap());
    }

    #[test]
    fn test_decode_array_with_size() {
        let mut input: &[u8] = &[5, 6, 2, 4, 6, 0];
        let result = decode(&Schema::Array(Box::new(Schema::Int)), &mut input);
        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result.unwrap());
    }

    #[test]
    fn test_decode_map_without_size() {
        let mut input: &[u8] = &[0x02, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let result = decode(&Schema::Map(Box::new(Schema::Int)), &mut input);
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result.unwrap());
    }

    #[test]
    fn test_decode_map_with_size() {
        let mut input: &[u8] = &[0x01, 0x0C, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let result = decode(&Schema::Map(Box::new(Schema::Int)), &mut input);
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result.unwrap());
    }
}
