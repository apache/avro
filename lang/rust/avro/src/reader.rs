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

//! Logic handling reading from Avro format at user level.
use crate::{
    decode::{decode, decode_internal},
    from_value,
    rabin::Rabin,
    schema::{AvroSchema, ResolvedOwnedSchema, Schema},
    types::Value,
    util, AvroResult, Codec, Error,
};
use serde::de::DeserializeOwned;
use serde_json::from_slice;
use std::{
    collections::HashMap,
    convert::TryFrom,
    io::{ErrorKind, Read},
    marker::PhantomData,
    str::FromStr,
};

// Internal Block reader.
#[derive(Debug, Clone)]
struct Block<R> {
    reader: R,
    // Internal buffering to reduce allocation.
    buf: Vec<u8>,
    buf_idx: usize,
    // Number of elements expected to exist within this block.
    message_count: usize,
    marker: [u8; 16],
    codec: Codec,
    writer_schema: Schema,
    user_metadata: HashMap<String, Vec<u8>>,
}

impl<R: Read> Block<R> {
    fn new(reader: R) -> AvroResult<Block<R>> {
        let mut block = Block {
            reader,
            codec: Codec::Null,
            writer_schema: Schema::Null,
            buf: vec![],
            buf_idx: 0,
            message_count: 0,
            marker: [0; 16],
            user_metadata: Default::default(),
        };

        block.read_header()?;
        Ok(block)
    }

    /// Try to read the header and to set the writer `Schema`, the `Codec` and the marker based on
    /// its content.
    fn read_header(&mut self) -> AvroResult<()> {
        let meta_schema = Schema::Map(Box::new(Schema::Bytes));

        let mut buf = [0u8; 4];
        self.reader
            .read_exact(&mut buf)
            .map_err(Error::ReadHeader)?;

        if buf != [b'O', b'b', b'j', 1u8] {
            return Err(Error::HeaderMagic);
        }

        if let Value::Map(metadata) = decode(&meta_schema, &mut self.reader)? {
            self.read_writer_schema(&metadata)?;
            self.read_codec(&metadata);

            for (key, value) in metadata {
                if key == "avro.schema" || key == "avro.codec" {
                    // already processed
                } else if key.starts_with("avro.") {
                    warn!("Ignoring unknown metadata key: {}", key);
                } else {
                    self.read_user_metadata(key, value);
                }
            }
        } else {
            return Err(Error::GetHeaderMetadata);
        }

        self.reader
            .read_exact(&mut self.marker)
            .map_err(Error::ReadMarker)
    }

    fn fill_buf(&mut self, n: usize) -> AvroResult<()> {
        // The buffer needs to contain exactly `n` elements, otherwise codecs will potentially read
        // invalid bytes.
        //
        // The are two cases to handle here:
        //
        // 1. `n > self.buf.len()`:
        //    In this case we call `Vec::resize`, which guarantees that `self.buf.len() == n`.
        // 2. `n < self.buf.len()`:
        //    We need to resize to ensure that the buffer len is safe to read `n` elements.
        //
        // TODO: Figure out a way to avoid having to truncate for the second case.
        self.buf.resize(n, 0);
        self.reader
            .read_exact(&mut self.buf)
            .map_err(Error::ReadIntoBuf)?;
        self.buf_idx = 0;
        Ok(())
    }

    /// Try to read a data block, also performing schema resolution for the objects contained in
    /// the block. The objects are stored in an internal buffer to the `Reader`.
    fn read_block_next(&mut self) -> AvroResult<()> {
        assert!(self.is_empty(), "Expected self to be empty!");
        match util::read_long(&mut self.reader) {
            Ok(block_len) => {
                self.message_count = block_len as usize;
                let block_bytes = util::read_long(&mut self.reader)?;
                self.fill_buf(block_bytes as usize)?;
                let mut marker = [0u8; 16];
                self.reader
                    .read_exact(&mut marker)
                    .map_err(Error::ReadBlockMarker)?;

                if marker != self.marker {
                    return Err(Error::GetBlockMarker);
                }

                // NOTE (JAB): This doesn't fit this Reader pattern very well.
                // `self.buf` is a growable buffer that is reused as the reader is iterated.
                // For non `Codec::Null` variants, `decompress` will allocate a new `Vec`
                // and replace `buf` with the new one, instead of reusing the same buffer.
                // We can address this by using some "limited read" type to decode directly
                // into the buffer. But this is fine, for now.
                self.codec.decompress(&mut self.buf)
            }
            Err(Error::ReadVariableIntegerBytes(io_err)) => {
                if let ErrorKind::UnexpectedEof = io_err.kind() {
                    // to not return any error in case we only finished to read cleanly from the stream
                    Ok(())
                } else {
                    Err(Error::ReadVariableIntegerBytes(io_err))
                }
            }
            Err(e) => Err(e),
        }
    }

    fn len(&self) -> usize {
        self.message_count
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn read_next(&mut self, read_schema: Option<&Schema>) -> AvroResult<Option<Value>> {
        if self.is_empty() {
            self.read_block_next()?;
            if self.is_empty() {
                return Ok(None);
            }
        }

        let mut block_bytes = &self.buf[self.buf_idx..];
        let b_original = block_bytes.len();
        let item = from_avro_datum(&self.writer_schema, &mut block_bytes, read_schema)?;
        self.buf_idx += b_original - block_bytes.len();
        self.message_count -= 1;
        Ok(Some(item))
    }

    fn read_writer_schema(&mut self, metadata: &HashMap<String, Value>) -> AvroResult<()> {
        let json = metadata
            .get("avro.schema")
            .and_then(|bytes| {
                if let Value::Bytes(ref bytes) = *bytes {
                    from_slice(bytes.as_ref()).ok()
                } else {
                    None
                }
            })
            .ok_or(Error::GetAvroSchemaFromMap)?;
        self.writer_schema = Schema::parse(&json)?;
        Ok(())
    }

    fn read_codec(&mut self, metadata: &HashMap<String, Value>) {
        if let Some(codec) = metadata
            .get("avro.codec")
            .and_then(|codec| {
                if let Value::Bytes(ref bytes) = *codec {
                    std::str::from_utf8(bytes.as_ref()).ok()
                } else {
                    None
                }
            })
            .and_then(|codec| Codec::from_str(codec).ok())
        {
            self.codec = codec;
        }
    }

    fn read_user_metadata(&mut self, key: String, value: Value) {
        match value {
            Value::Bytes(ref vec) => {
                self.user_metadata.insert(key, vec.clone());
            }
            wrong => {
                warn!(
                    "User metadata values must be Value::Bytes, found {:?}",
                    wrong
                );
            }
        }
    }
}

/// Main interface for reading Avro formatted values.
///
/// To be used as an iterator:
///
/// ```no_run
/// # use apache_avro::Reader;
/// # use std::io::Cursor;
/// # let input = Cursor::new(Vec::<u8>::new());
/// for value in Reader::new(input).unwrap() {
///     match value {
///         Ok(v) => println!("{:?}", v),
///         Err(e) => println!("Error: {}", e),
///     };
/// }
/// ```
pub struct Reader<'a, R> {
    block: Block<R>,
    reader_schema: Option<&'a Schema>,
    errored: bool,
    should_resolve_schema: bool,
}

impl<'a, R: Read> Reader<'a, R> {
    /// Creates a `Reader` given something implementing the `io::Read` trait to read from.
    /// No reader `Schema` will be set.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the `Reader`.
    pub fn new(reader: R) -> AvroResult<Reader<'a, R>> {
        let block = Block::new(reader)?;
        let reader = Reader {
            block,
            reader_schema: None,
            errored: false,
            should_resolve_schema: false,
        };
        Ok(reader)
    }

    /// Creates a `Reader` given a reader `Schema` and something implementing the `io::Read` trait
    /// to read from.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the `Reader`.
    pub fn with_schema(schema: &'a Schema, reader: R) -> AvroResult<Reader<'a, R>> {
        let block = Block::new(reader)?;
        let mut reader = Reader {
            block,
            reader_schema: Some(schema),
            errored: false,
            should_resolve_schema: false,
        };
        // Check if the reader and writer schemas disagree.
        reader.should_resolve_schema = reader.writer_schema() != schema;
        Ok(reader)
    }

    /// Get a reference to the writer `Schema`.
    #[inline]
    pub fn writer_schema(&self) -> &Schema {
        &self.block.writer_schema
    }

    /// Get a reference to the optional reader `Schema`.
    #[inline]
    pub fn reader_schema(&self) -> Option<&Schema> {
        self.reader_schema
    }

    /// Get a reference to the user metadata
    #[inline]
    pub fn user_metadata(&self) -> &HashMap<String, Vec<u8>> {
        &self.block.user_metadata
    }

    #[inline]
    fn read_next(&mut self) -> AvroResult<Option<Value>> {
        let read_schema = if self.should_resolve_schema {
            self.reader_schema
        } else {
            None
        };

        self.block.read_next(read_schema)
    }
}

impl<'a, R: Read> Iterator for Reader<'a, R> {
    type Item = AvroResult<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        // to prevent keep on reading after the first error occurs
        if self.errored {
            return None;
        };
        match self.read_next() {
            Ok(opt) => opt.map(Ok),
            Err(e) => {
                self.errored = true;
                Some(Err(e))
            }
        }
    }
}

/// Decode a `Value` encoded in Avro format given its `Schema` and anything implementing `io::Read`
/// to read from.
///
/// In case a reader `Schema` is provided, schema resolution will also be performed.
///
/// **NOTE** This function has a quite small niche of usage and does NOT take care of reading the
/// header and consecutive data blocks; use [`Reader`](struct.Reader.html) if you don't know what
/// you are doing, instead.
pub fn from_avro_datum<R: Read>(
    writer_schema: &Schema,
    reader: &mut R,
    reader_schema: Option<&Schema>,
) -> AvroResult<Value> {
    let value = decode(writer_schema, reader)?;
    match reader_schema {
        Some(schema) => value.resolve(schema),
        None => Ok(value),
    }
}

pub struct GenericSingleObjectReader {
    write_schema: ResolvedOwnedSchema,
    expected_header: [u8; 10],
}

impl GenericSingleObjectReader {
    pub fn new(schema: Schema) -> AvroResult<GenericSingleObjectReader> {
        let fingerprint = schema.fingerprint::<Rabin>();
        let expected_header = [
            0xC3,
            0x01,
            fingerprint.bytes[0],
            fingerprint.bytes[1],
            fingerprint.bytes[2],
            fingerprint.bytes[3],
            fingerprint.bytes[4],
            fingerprint.bytes[5],
            fingerprint.bytes[6],
            fingerprint.bytes[7],
        ];
        Ok(GenericSingleObjectReader {
            write_schema: ResolvedOwnedSchema::try_from(schema)?,
            expected_header,
        })
    }

    pub fn read_value<R: Read>(&self, reader: &mut R) -> AvroResult<Value> {
        let mut header: [u8; 10] = [0; 10];
        match reader.read(&mut header) {
            Ok(size) => {
                if size == 10 && self.expected_header == header {
                    decode_internal(
                        self.write_schema.get_root_schema(),
                        self.write_schema.get_names(),
                        &None,
                        reader,
                    )
                } else {
                    Err(Error::SingleObjectHeaderMismatch(
                        self.expected_header,
                        header,
                    ))
                }
            }
            Err(io_error) => Err(Error::ReadHeader(io_error)),
        }
    }
}

pub struct SpecificSingleObjectReader<T>
where
    T: AvroSchema,
{
    inner: GenericSingleObjectReader,
    _model: PhantomData<T>,
}

impl<T> SpecificSingleObjectReader<T>
where
    T: AvroSchema,
{
    pub fn new() -> AvroResult<SpecificSingleObjectReader<T>> {
        Ok(SpecificSingleObjectReader {
            inner: GenericSingleObjectReader::new(T::get_schema())?,
            _model: PhantomData,
        })
    }
}

impl<T> SpecificSingleObjectReader<T>
where
    T: AvroSchema + From<Value>,
{
    pub fn read_from_value<R: Read>(&self, reader: &mut R) -> AvroResult<T> {
        self.inner.read_value(reader).map(|v| v.into())
    }
}

impl<T> SpecificSingleObjectReader<T>
where
    T: AvroSchema + DeserializeOwned,
{
    pub fn read<R: Read>(&self, reader: &mut R) -> AvroResult<T> {
        from_value::<T>(&self.inner.read_value(reader)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{encode::encode, from_value, types::Record, Reader};
    use serde::Deserialize;
    use std::io::Cursor;

    const SCHEMA: &str = r#"
    {
      "type": "record",
      "name": "test",
      "fields": [
        {
          "name": "a",
          "type": "long",
          "default": 42
        },
        {
          "name": "b",
          "type": "string"
        }
      ]
    }
    "#;
    const UNION_SCHEMA: &str = r#"["null", "long"]"#;
    const ENCODED: &[u8] = &[
        79u8, 98u8, 106u8, 1u8, 4u8, 22u8, 97u8, 118u8, 114u8, 111u8, 46u8, 115u8, 99u8, 104u8,
        101u8, 109u8, 97u8, 222u8, 1u8, 123u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8,
        114u8, 101u8, 99u8, 111u8, 114u8, 100u8, 34u8, 44u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8,
        58u8, 34u8, 116u8, 101u8, 115u8, 116u8, 34u8, 44u8, 34u8, 102u8, 105u8, 101u8, 108u8,
        100u8, 115u8, 34u8, 58u8, 91u8, 123u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8, 58u8, 34u8,
        97u8, 34u8, 44u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 108u8, 111u8, 110u8,
        103u8, 34u8, 44u8, 34u8, 100u8, 101u8, 102u8, 97u8, 117u8, 108u8, 116u8, 34u8, 58u8, 52u8,
        50u8, 125u8, 44u8, 123u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8, 58u8, 34u8, 98u8, 34u8,
        44u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 115u8, 116u8, 114u8, 105u8,
        110u8, 103u8, 34u8, 125u8, 93u8, 125u8, 20u8, 97u8, 118u8, 114u8, 111u8, 46u8, 99u8, 111u8,
        100u8, 101u8, 99u8, 8u8, 110u8, 117u8, 108u8, 108u8, 0u8, 94u8, 61u8, 54u8, 221u8, 190u8,
        207u8, 108u8, 180u8, 158u8, 57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239u8, 4u8, 20u8, 54u8,
        6u8, 102u8, 111u8, 111u8, 84u8, 6u8, 98u8, 97u8, 114u8, 94u8, 61u8, 54u8, 221u8, 190u8,
        207u8, 108u8, 180u8, 158u8, 57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239u8,
    ];

    #[test]
    fn test_from_avro_datum() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut encoded: &'static [u8] = &[54, 6, 102, 111, 111];

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        let expected = record.into();

        assert_eq!(
            from_avro_datum(&schema, &mut encoded, None).unwrap(),
            expected
        );
    }

    #[test]
    fn test_from_avro_datum_with_union_to_struct() {
        const TEST_RECORD_SCHEMA_3240: &str = r#"
    {
      "type": "record",
      "name": "test",
      "fields": [
        {
          "name": "a",
          "type": "long",
          "default": 42
        },
        {
          "name": "b",
          "type": "string"
        },
        {
            "name": "a_nullable_array",
            "type": ["null", {"type": "array", "items": {"type": "string"}}],
            "default": null
        },
        {
            "name": "a_nullable_boolean",
            "type": ["null", {"type": "boolean"}],
            "default": null
        },
        {
            "name": "a_nullable_string",
            "type": ["null", {"type": "string"}],
            "default": null
        }
      ]
    }
    "#;
        #[derive(Default, Debug, Deserialize, PartialEq)]
        struct TestRecord3240 {
            a: i64,
            b: String,
            a_nullable_array: Option<Vec<String>>,
            // we are missing the 'a_nullable_boolean' field to simulate missing keys
            // a_nullable_boolean: Option<bool>,
            a_nullable_string: Option<String>,
        }

        let schema = Schema::parse_str(TEST_RECORD_SCHEMA_3240).unwrap();
        let mut encoded: &'static [u8] = &[54, 6, 102, 111, 111];

        let expected_record: TestRecord3240 = TestRecord3240 {
            a: 27i64,
            b: String::from("foo"),
            a_nullable_array: None,
            a_nullable_string: None,
        };

        let avro_datum = from_avro_datum(&schema, &mut encoded, None).unwrap();
        let parsed_record: TestRecord3240 = match &avro_datum {
            Value::Record(_) => from_value::<TestRecord3240>(&avro_datum).unwrap(),
            unexpected => panic!(
                "could not map avro data to struct, found unexpected: {:?}",
                unexpected
            ),
        };

        assert_eq!(parsed_record, expected_record);
    }

    #[test]
    fn test_null_union() {
        let schema = Schema::parse_str(UNION_SCHEMA).unwrap();
        let mut encoded: &'static [u8] = &[2, 0];

        assert_eq!(
            from_avro_datum(&schema, &mut encoded, None).unwrap(),
            Value::Union(1, Box::new(Value::Long(0)))
        );
    }

    #[test]
    fn test_reader_iterator() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let reader = Reader::with_schema(&schema, ENCODED).unwrap();

        let mut record1 = Record::new(&schema).unwrap();
        record1.put("a", 27i64);
        record1.put("b", "foo");

        let mut record2 = Record::new(&schema).unwrap();
        record2.put("a", 42i64);
        record2.put("b", "bar");

        let expected = vec![record1.into(), record2.into()];

        for (i, value) in reader.enumerate() {
            assert_eq!(value.unwrap(), expected[i]);
        }
    }

    #[test]
    fn test_reader_invalid_header() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let invalid = ENCODED.iter().copied().skip(1).collect::<Vec<u8>>();
        assert!(Reader::with_schema(&schema, &invalid[..]).is_err());
    }

    #[test]
    fn test_reader_invalid_block() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let invalid = ENCODED
            .iter()
            .copied()
            .rev()
            .skip(19)
            .collect::<Vec<u8>>()
            .into_iter()
            .rev()
            .collect::<Vec<u8>>();
        let reader = Reader::with_schema(&schema, &invalid[..]).unwrap();
        for value in reader {
            assert!(value.is_err());
        }
    }

    #[test]
    fn test_reader_empty_buffer() {
        let empty = Cursor::new(Vec::new());
        assert!(Reader::new(empty).is_err());
    }

    #[test]
    fn test_reader_only_header() {
        let invalid = ENCODED.iter().copied().take(165).collect::<Vec<u8>>();
        let reader = Reader::new(&invalid[..]).unwrap();
        for value in reader {
            assert!(value.is_err());
        }
    }

    #[test]
    fn test_avro_3405_read_user_metadata_success() {
        use crate::writer::Writer;

        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        let mut user_meta_data: HashMap<String, Vec<u8>> = HashMap::new();
        user_meta_data.insert(
            "stringKey".to_string(),
            "stringValue".to_string().into_bytes(),
        );
        user_meta_data.insert("bytesKey".to_string(), b"bytesValue".to_vec());
        user_meta_data.insert("vecKey".to_string(), vec![1, 2, 3]);

        for (k, v) in user_meta_data.iter() {
            writer.add_user_metadata(k.to_string(), v).unwrap();
        }

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        writer.append(record.clone()).unwrap();
        writer.append(record.clone()).unwrap();
        writer.flush().unwrap();
        let result = writer.into_inner().unwrap();

        let reader = Reader::new(&result[..]).unwrap();
        assert_eq!(reader.user_metadata(), &user_meta_data);
    }

    #[derive(Deserialize, Clone, PartialEq, Debug)]
    struct TestSingleObjectReader {
        a: i64,
        b: f64,
        c: Vec<String>,
    }

    impl AvroSchema for TestSingleObjectReader {
        fn get_schema() -> Schema {
            let schema = r#"
            {
                "type":"record",
                "name":"TestSingleObjectWrtierSerialize",
                "fields":[
                    {
                        "name":"a",
                        "type":"long"
                    },
                    {
                        "name":"b",
                        "type":"double"
                    },
                    {
                        "name":"c",
                        "type":{
                            "type":"array",
                            "items":"string"
                        }
                    }
                ]
            }
            "#;
            Schema::parse_str(schema).unwrap()
        }
    }

    impl From<Value> for TestSingleObjectReader {
        fn from(obj: Value) -> TestSingleObjectReader {
            if let Value::Record(fields) = obj {
                let mut a = None;
                let mut b = None;
                let mut c = vec![];
                for (field_name, v) in fields {
                    match (field_name.as_str(), v) {
                        ("a", Value::Long(i)) => a = Some(i),
                        ("b", Value::Double(d)) => b = Some(d),
                        ("c", Value::Array(v)) => {
                            for inner_val in v {
                                if let Value::String(s) = inner_val {
                                    c.push(s);
                                }
                            }
                        }
                        (key, value) => panic!("Unexpected pair: {:?} -> {:?}", key, value),
                    }
                }
                TestSingleObjectReader {
                    a: a.unwrap(),
                    b: b.unwrap(),
                    c,
                }
            } else {
                panic!("Expected a Value::Record but was {:?}", obj)
            }
        }
    }

    impl From<TestSingleObjectReader> for Value {
        fn from(obj: TestSingleObjectReader) -> Value {
            Value::Record(vec![
                ("a".into(), obj.a.into()),
                ("b".into(), obj.b.into()),
                (
                    "c".into(),
                    Value::Array(obj.c.into_iter().map(|s| s.into()).collect()),
                ),
            ])
        }
    }

    #[test]
    fn test_avro_3507_single_object_reader() {
        let obj = TestSingleObjectReader {
            a: 42,
            b: 3.33,
            c: vec!["cat".into(), "dog".into()],
        };
        let mut to_read = Vec::<u8>::new();
        to_read.extend_from_slice(&[0xC3, 0x01]);
        to_read.extend_from_slice(
            &TestSingleObjectReader::get_schema()
                .fingerprint::<Rabin>()
                .bytes[..],
        );
        encode(
            &obj.clone().into(),
            &TestSingleObjectReader::get_schema(),
            &mut to_read,
        )
        .expect("Encode should succeed");
        let mut to_read = &to_read[..];
        let generic_reader = GenericSingleObjectReader::new(TestSingleObjectReader::get_schema())
            .expect("Schema should resolve");
        let val = generic_reader
            .read_value(&mut to_read)
            .expect("Should read");
        let expected_value: Value = obj.into();
        assert_eq!(expected_value, val);
    }

    #[test]
    fn test_avro_3507_reader_parity() {
        let obj = TestSingleObjectReader {
            a: 42,
            b: 3.33,
            c: vec!["cat".into(), "dog".into()],
        };

        let mut to_read = Vec::<u8>::new();
        to_read.extend_from_slice(&[0xC3, 0x01]);
        to_read.extend_from_slice(
            &TestSingleObjectReader::get_schema()
                .fingerprint::<Rabin>()
                .bytes[..],
        );
        encode(
            &obj.clone().into(),
            &TestSingleObjectReader::get_schema(),
            &mut to_read,
        )
        .expect("Encode should succeed");
        let generic_reader = GenericSingleObjectReader::new(TestSingleObjectReader::get_schema())
            .expect("Schema should resolve");
        let specific_reader = SpecificSingleObjectReader::<TestSingleObjectReader>::new()
            .expect("schema should resolve");
        let mut to_read1 = &to_read.clone()[..];
        let mut to_read2 = &to_read.clone()[..];
        let mut to_read3 = &to_read.clone()[..];

        let val = generic_reader
            .read_value(&mut to_read1)
            .expect("Should read");
        let read_obj1 = specific_reader
            .read_from_value(&mut to_read2)
            .expect("Should read from value");
        let read_obj2 = specific_reader
            .read(&mut to_read3)
            .expect("Should read from deserilize");
        let expected_value: Value = obj.clone().into();
        assert_eq!(obj, read_obj1);
        assert_eq!(obj, read_obj2);
        assert_eq!(val, expected_value)
    }
}
