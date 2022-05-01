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

//! Logic handling writing in Avro format at user level.
use crate::{
    encode::{encode, encode_internal, encode_to_vec},
    rabin::Rabin,
    schema::{AvroSchema, ResolvedOwnedSchema, ResolvedSchema, Schema},
    ser::Serializer,
    types::Value,
    AvroResult, Codec, Error,
};
use rand::random;
use serde::Serialize;
use std::{collections::HashMap, convert::TryFrom, io::Write, marker::PhantomData};

const DEFAULT_BLOCK_SIZE: usize = 16000;
const AVRO_OBJECT_HEADER: &[u8] = b"Obj\x01";

/// Main interface for writing Avro formatted values.
#[derive(typed_builder::TypedBuilder)]
pub struct Writer<'a, W> {
    schema: &'a Schema,
    writer: W,
    #[builder(default, setter(skip))]
    resolved_schema: Option<ResolvedSchema<'a>>,
    #[builder(default = Codec::Null)]
    codec: Codec,
    #[builder(default = DEFAULT_BLOCK_SIZE)]
    block_size: usize,
    #[builder(default = Vec::with_capacity(block_size), setter(skip))]
    buffer: Vec<u8>,
    #[builder(default, setter(skip))]
    serializer: Serializer,
    #[builder(default = 0, setter(skip))]
    num_values: usize,
    #[builder(default = std::iter::repeat_with(random).take(16).collect(), setter(skip))]
    marker: Vec<u8>,
    #[builder(default = false, setter(skip))]
    has_header: bool,
    #[builder(default)]
    user_metadata: HashMap<String, Value>,
}

impl<'a, W: Write> Writer<'a, W> {
    /// Creates a `Writer` given a `Schema` and something implementing the `io::Write` trait to write
    /// to.
    /// No compression `Codec` will be used.
    pub fn new(schema: &'a Schema, writer: W) -> Self {
        let mut w = Self::builder().schema(schema).writer(writer).build();
        w.resolved_schema = ResolvedSchema::try_from(schema).ok();
        w
    }

    /// Creates a `Writer` with a specific `Codec` given a `Schema` and something implementing the
    /// `io::Write` trait to write to.
    pub fn with_codec(schema: &'a Schema, writer: W, codec: Codec) -> Self {
        let mut w = Self::builder()
            .schema(schema)
            .writer(writer)
            .codec(codec)
            .build();
        w.resolved_schema = ResolvedSchema::try_from(schema).ok();
        w
    }

    /// Get a reference to the `Schema` associated to a `Writer`.
    pub fn schema(&self) -> &'a Schema {
        self.schema
    }

    /// Append a compatible value (implementing the `ToAvro` trait) to a `Writer`, also performing
    /// schema validation.
    ///
    /// Return the number of bytes written (it might be 0, see below).
    ///
    /// **NOTE** This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](struct.Writer.html#method.flush).
    pub fn append<T: Into<Value>>(&mut self, value: T) -> AvroResult<usize> {
        let n = self.maybe_write_header()?;

        let avro = value.into();
        self.append_value_ref(&avro).map(|m| m + n)
    }

    /// Append a compatible value to a `Writer`, also performing schema validation.
    ///
    /// Return the number of bytes written (it might be 0, see below).
    ///
    /// **NOTE** This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](struct.Writer.html#method.flush).
    pub fn append_value_ref(&mut self, value: &Value) -> AvroResult<usize> {
        let n = self.maybe_write_header()?;

        // Lazy init for users using the builder pattern with error throwing
        match self.resolved_schema {
            Some(ref rs) => {
                write_value_ref_resolved(rs, value, &mut self.buffer)?;
                self.num_values += 1;

                if self.buffer.len() >= self.block_size {
                    return self.flush().map(|b| b + n);
                }

                Ok(n)
            }
            None => {
                let rs = ResolvedSchema::try_from(self.schema)?;
                self.resolved_schema = Some(rs);
                self.append_value_ref(value)
            }
        }
    }

    /// Append anything implementing the `Serialize` trait to a `Writer` for
    /// [`serde`](https://docs.serde.rs/serde/index.html) compatibility, also performing schema
    /// validation.
    ///
    /// Return the number of bytes written.
    ///
    /// **NOTE** This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](struct.Writer.html#method.flush).
    pub fn append_ser<S: Serialize>(&mut self, value: S) -> AvroResult<usize> {
        let avro_value = value.serialize(&mut self.serializer)?;
        self.append(avro_value)
    }

    /// Extend a `Writer` with an `Iterator` of compatible values (implementing the `ToAvro`
    /// trait), also performing schema validation.
    ///
    /// Return the number of bytes written.
    ///
    /// **NOTE** This function forces the written data to be flushed (an implicit
    /// call to [`flush`](struct.Writer.html#method.flush) is performed).
    pub fn extend<I, T: Into<Value>>(&mut self, values: I) -> AvroResult<usize>
    where
        I: IntoIterator<Item = T>,
    {
        /*
        https://github.com/rust-lang/rfcs/issues/811 :(
        let mut stream = values
            .filter_map(|value| value.serialize(&mut self.serializer).ok())
            .map(|value| value.encode(self.schema))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| err_msg("value does not match given schema"))?
            .into_iter()
            .fold(Vec::new(), |mut acc, stream| {
                num_values += 1;
                acc.extend(stream); acc
            });
        */

        let mut num_bytes = 0;
        for value in values {
            num_bytes += self.append(value)?;
        }
        num_bytes += self.flush()?;

        Ok(num_bytes)
    }

    /// Extend a `Writer` with an `Iterator` of anything implementing the `Serialize` trait for
    /// [`serde`](https://docs.serde.rs/serde/index.html) compatibility, also performing schema
    /// validation.
    ///
    /// Return the number of bytes written.
    ///
    /// **NOTE** This function forces the written data to be flushed (an implicit
    /// call to [`flush`](struct.Writer.html#method.flush) is performed).
    pub fn extend_ser<I, T: Serialize>(&mut self, values: I) -> AvroResult<usize>
    where
        I: IntoIterator<Item = T>,
    {
        /*
        https://github.com/rust-lang/rfcs/issues/811 :(
        let mut stream = values
            .filter_map(|value| value.serialize(&mut self.serializer).ok())
            .map(|value| value.encode(self.schema))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| err_msg("value does not match given schema"))?
            .into_iter()
            .fold(Vec::new(), |mut acc, stream| {
                num_values += 1;
                acc.extend(stream); acc
            });
        */

        let mut num_bytes = 0;
        for value in values {
            num_bytes += self.append_ser(value)?;
        }
        num_bytes += self.flush()?;

        Ok(num_bytes)
    }

    /// Extend a `Writer` by appending each `Value` from a slice, while also performing schema
    /// validation on each value appended.
    ///
    /// Return the number of bytes written.
    ///
    /// **NOTE** This function forces the written data to be flushed (an implicit
    /// call to [`flush`](struct.Writer.html#method.flush) is performed).
    pub fn extend_from_slice(&mut self, values: &[Value]) -> AvroResult<usize> {
        let mut num_bytes = 0;
        for value in values {
            num_bytes += self.append_value_ref(value)?;
        }
        num_bytes += self.flush()?;

        Ok(num_bytes)
    }

    /// Flush the content appended to a `Writer`. Call this function to make sure all the content
    /// has been written before releasing the `Writer`.
    ///
    /// Return the number of bytes written.
    pub fn flush(&mut self) -> AvroResult<usize> {
        if self.num_values == 0 {
            return Ok(0);
        }

        self.codec.compress(&mut self.buffer)?;

        let num_values = self.num_values;
        let stream_len = self.buffer.len();

        let num_bytes = self.append_raw(&num_values.into(), &Schema::Long)?
            + self.append_raw(&stream_len.into(), &Schema::Long)?
            + self
                .writer
                .write(self.buffer.as_ref())
                .map_err(Error::WriteBytes)?
            + self.append_marker()?;

        self.buffer.clear();
        self.num_values = 0;

        Ok(num_bytes)
    }

    /// Return what the `Writer` is writing to, consuming the `Writer` itself.
    ///
    /// **NOTE** This function forces the written data to be flushed (an implicit
    /// call to [`flush`](struct.Writer.html#method.flush) is performed).
    pub fn into_inner(mut self) -> AvroResult<W> {
        self.flush()?;
        Ok(self.writer)
    }

    /// Generate and append synchronization marker to the payload.
    fn append_marker(&mut self) -> AvroResult<usize> {
        // using .writer.write directly to avoid mutable borrow of self
        // with ref borrowing of self.marker
        self.writer.write(&self.marker).map_err(Error::WriteMarker)
    }

    /// Append a raw Avro Value to the payload avoiding to encode it again.
    fn append_raw(&mut self, value: &Value, schema: &Schema) -> AvroResult<usize> {
        self.append_bytes(encode_to_vec(value, schema)?.as_ref())
    }

    /// Append pure bytes to the payload.
    fn append_bytes(&mut self, bytes: &[u8]) -> AvroResult<usize> {
        self.writer.write(bytes).map_err(Error::WriteBytes)
    }

    /// Adds custom metadata to the file.
    /// This method could be used only before adding the first record to the writer.
    pub fn add_user_metadata<T: AsRef<[u8]>>(&mut self, key: String, value: T) -> AvroResult<()> {
        if !self.has_header {
            if key.starts_with("avro.") {
                return Err(Error::InvalidMetadataKey(key));
            }
            self.user_metadata
                .insert(key, Value::Bytes(value.as_ref().to_vec()));
            Ok(())
        } else {
            Err(Error::FileHeaderAlreadyWritten)
        }
    }

    /// Create an Avro header based on schema, codec and sync marker.
    fn header(&self) -> Result<Vec<u8>, Error> {
        let schema_bytes = serde_json::to_string(self.schema)
            .map_err(Error::ConvertJsonToString)?
            .into_bytes();

        let mut metadata = HashMap::with_capacity(2);
        metadata.insert("avro.schema", Value::Bytes(schema_bytes));
        metadata.insert("avro.codec", self.codec.into());

        for (k, v) in &self.user_metadata {
            metadata.insert(k.as_str(), v.clone());
        }

        let mut header = Vec::new();
        header.extend_from_slice(AVRO_OBJECT_HEADER);
        encode(
            &metadata.into(),
            &Schema::Map(Box::new(Schema::Bytes)),
            &mut header,
        )?;
        header.extend_from_slice(&self.marker);

        Ok(header)
    }

    fn maybe_write_header(&mut self) -> AvroResult<usize> {
        if !self.has_header {
            let header = self.header()?;
            let n = self.append_bytes(header.as_ref())?;
            self.has_header = true;
            Ok(n)
        } else {
            Ok(0)
        }
    }
}

/// Encode a compatible value (implementing the `ToAvro` trait) into Avro format, also performing
/// schema validation.
///
/// This is an internal function which gets the bytes buffer where to write as parameter instead of
/// creating a new one like `to_avro_datum`.
fn write_avro_datum<T: Into<Value>>(
    schema: &Schema,
    value: T,
    buffer: &mut Vec<u8>,
) -> Result<(), Error> {
    let avro = value.into();
    if !avro.validate(schema) {
        return Err(Error::Validation);
    }
    encode(&avro, schema, buffer)?;
    Ok(())
}

/// Writer that encodes messages according to the single object encoding v1 spec
/// Uses an API similar to the current File Writer
/// Writes all object bytes at once, and drains internal buffer
pub struct GenericSingleObjectWriter {
    buffer: Vec<u8>,
    resolved: ResolvedOwnedSchema,
}

impl GenericSingleObjectWriter {
    pub fn new_with_capacity(
        schema: &Schema,
        initial_buffer_cap: usize,
    ) -> AvroResult<GenericSingleObjectWriter> {
        let fingerprint = schema.fingerprint::<Rabin>();
        let mut buffer = Vec::with_capacity(initial_buffer_cap);
        let header = [
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
        buffer.extend_from_slice(&header);

        Ok(GenericSingleObjectWriter {
            buffer,
            resolved: ResolvedOwnedSchema::try_from(schema.clone())?,
        })
    }

    /// Write the referenced Value to the provided Write object. Returns a result with the number of bytes written including the header
    pub fn write_value_ref<W: Write>(&mut self, v: &Value, writer: &mut W) -> AvroResult<usize> {
        if self.buffer.len() != 10 {
            Err(Error::IllegalSingleObjectWriterState)
        } else {
            write_value_ref_owned_resolved(&self.resolved, v, &mut self.buffer)?;
            writer.write_all(&self.buffer).map_err(Error::WriteBytes)?;
            let len = self.buffer.len();
            self.buffer.truncate(10);
            Ok(len)
        }
    }

    /// Write the Value to the provided Write object. Returns a result with the number of bytes written including the header
    pub fn write_value<W: Write>(&mut self, v: Value, writer: &mut W) -> AvroResult<usize> {
        self.write_value_ref(&v, writer)
    }
}

/// Writer that encodes messages according to the single object encoding v1 spec
pub struct SingleObjectWriter<T>
where
    T: AvroSchema,
{
    inner: GenericSingleObjectWriter,
    _model: PhantomData<T>,
}

impl<T> SingleObjectWriter<T>
where
    T: AvroSchema,
{
    pub fn with_capacity(buffer_cap: usize) -> AvroResult<SingleObjectWriter<T>> {
        let schema = T::get_schema();
        Ok(SingleObjectWriter {
            inner: GenericSingleObjectWriter::new_with_capacity(&schema, buffer_cap)?,
            _model: PhantomData,
        })
    }
}

impl<T> SingleObjectWriter<T>
where
    T: AvroSchema + Into<Value>,
{
    /// Write the Into<Value> to the provided Write object. Returns a result with the number of bytes written including the header
    pub fn write_value<W: Write>(&mut self, data: T, writer: &mut W) -> AvroResult<usize> {
        let v: Value = data.into();
        self.inner.write_value_ref(&v, writer)
    }
}

impl<T> SingleObjectWriter<T>
where
    T: AvroSchema + Serialize,
{
    /// Write the referenced Serialize object to the provided Write object. Returns a result with the number of bytes written including the header
    pub fn write_ref<W: Write>(&mut self, data: &T, writer: &mut W) -> AvroResult<usize> {
        let mut serializer = Serializer::default();
        let v = data.serialize(&mut serializer)?;
        self.inner.write_value_ref(&v, writer)
    }

    /// Wrtite the Serialize object to the provided Write object. Returns a result with the number of bytes writtern including the header
    pub fn write<W: Write>(&mut self, data: T, writer: &mut W) -> AvroResult<usize> {
        self.write_ref(&data, writer)
    }
}

fn write_value_ref_resolved(
    resolved_schema: &ResolvedSchema,
    value: &Value,
    buffer: &mut Vec<u8>,
) -> AvroResult<()> {
    if let Some(err) = value.validate_internal(
        resolved_schema.get_root_schema(),
        resolved_schema.get_names(),
    ) {
        return Err(Error::ValidationWithReason(err));
    }
    encode_internal(
        value,
        resolved_schema.get_root_schema(),
        resolved_schema.get_names(),
        &None,
        buffer,
    )?;
    Ok(())
}

fn write_value_ref_owned_resolved(
    resolved_schema: &ResolvedOwnedSchema,
    value: &Value,
    buffer: &mut Vec<u8>,
) -> AvroResult<()> {
    if let Some(err) = value.validate_internal(
        resolved_schema.get_root_schema(),
        resolved_schema.get_names(),
    ) {
        return Err(Error::ValidationWithReason(err));
    }
    encode_internal(
        value,
        resolved_schema.get_root_schema(),
        resolved_schema.get_names(),
        &None,
        buffer,
    )?;
    Ok(())
}

/// Encode a compatible value (implementing the `ToAvro` trait) into Avro format, also
/// performing schema validation.
///
/// **NOTE** This function has a quite small niche of usage and does NOT generate headers and sync
/// markers; use [`Writer`](struct.Writer.html) to be fully Avro-compatible if you don't know what
/// you are doing, instead.
pub fn to_avro_datum<T: Into<Value>>(schema: &Schema, value: T) -> AvroResult<Vec<u8>> {
    let mut buffer = Vec::new();
    write_avro_datum(schema, value, &mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        decimal::Decimal,
        duration::{Days, Duration, Millis, Months},
        schema::Name,
        types::Record,
        util::zig_i64,
    };
    use serde::{Deserialize, Serialize};

    const AVRO_OBJECT_HEADER_LEN: usize = AVRO_OBJECT_HEADER.len();

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

    #[test]
    fn test_to_avro_datum() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        let mut expected = Vec::new();
        zig_i64(27, &mut expected);
        zig_i64(3, &mut expected);
        expected.extend(vec![b'f', b'o', b'o'].into_iter());

        assert_eq!(to_avro_datum(&schema, record).unwrap(), expected);
    }

    #[test]
    fn test_union_not_null() {
        let schema = Schema::parse_str(UNION_SCHEMA).unwrap();
        let union = Value::Union(1, Box::new(Value::Long(3)));

        let mut expected = Vec::new();
        zig_i64(1, &mut expected);
        zig_i64(3, &mut expected);

        assert_eq!(to_avro_datum(&schema, union).unwrap(), expected);
    }

    #[test]
    fn test_union_null() {
        let schema = Schema::parse_str(UNION_SCHEMA).unwrap();
        let union = Value::Union(0, Box::new(Value::Null));

        let mut expected = Vec::new();
        zig_i64(0, &mut expected);

        assert_eq!(to_avro_datum(&schema, union).unwrap(), expected);
    }

    type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

    fn logical_type_test<T: Into<Value> + Clone>(
        schema_str: &'static str,

        expected_schema: &Schema,
        value: Value,

        raw_schema: &Schema,
        raw_value: T,
    ) -> TestResult<()> {
        let schema = Schema::parse_str(schema_str)?;
        assert_eq!(&schema, expected_schema);
        // The serialized format should be the same as the schema.
        let ser = to_avro_datum(&schema, value.clone())?;
        let raw_ser = to_avro_datum(raw_schema, raw_value)?;
        assert_eq!(ser, raw_ser);

        // Should deserialize from the schema into the logical type.
        let mut r = ser.as_slice();
        let de = crate::from_avro_datum(&schema, &mut r, None).unwrap();
        assert_eq!(de, value);
        Ok(())
    }

    #[test]
    fn date() -> TestResult<()> {
        logical_type_test(
            r#"{"type": "int", "logicalType": "date"}"#,
            &Schema::Date,
            Value::Date(1_i32),
            &Schema::Int,
            1_i32,
        )
    }

    #[test]
    fn time_millis() -> TestResult<()> {
        logical_type_test(
            r#"{"type": "int", "logicalType": "time-millis"}"#,
            &Schema::TimeMillis,
            Value::TimeMillis(1_i32),
            &Schema::Int,
            1_i32,
        )
    }

    #[test]
    fn time_micros() -> TestResult<()> {
        logical_type_test(
            r#"{"type": "long", "logicalType": "time-micros"}"#,
            &Schema::TimeMicros,
            Value::TimeMicros(1_i64),
            &Schema::Long,
            1_i64,
        )
    }

    #[test]
    fn timestamp_millis() -> TestResult<()> {
        logical_type_test(
            r#"{"type": "long", "logicalType": "timestamp-millis"}"#,
            &Schema::TimestampMillis,
            Value::TimestampMillis(1_i64),
            &Schema::Long,
            1_i64,
        )
    }

    #[test]
    fn timestamp_micros() -> TestResult<()> {
        logical_type_test(
            r#"{"type": "long", "logicalType": "timestamp-micros"}"#,
            &Schema::TimestampMicros,
            Value::TimestampMicros(1_i64),
            &Schema::Long,
            1_i64,
        )
    }

    #[test]
    fn decimal_fixed() -> TestResult<()> {
        let size = 30;
        let inner = Schema::Fixed {
            name: Name::new("decimal").unwrap(),
            aliases: None,
            doc: None,
            size,
        };
        let value = vec![0u8; size];
        logical_type_test(
            r#"{"type": {"type": "fixed", "size": 30, "name": "decimal"}, "logicalType": "decimal", "precision": 20, "scale": 5}"#,
            &Schema::Decimal {
                precision: 20,
                scale: 5,
                inner: Box::new(inner.clone()),
            },
            Value::Decimal(Decimal::from(value.clone())),
            &inner,
            Value::Fixed(size, value),
        )
    }

    #[test]
    fn decimal_bytes() -> TestResult<()> {
        let inner = Schema::Bytes;
        let value = vec![0u8; 10];
        logical_type_test(
            r#"{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 3}"#,
            &Schema::Decimal {
                precision: 4,
                scale: 3,
                inner: Box::new(inner.clone()),
            },
            Value::Decimal(Decimal::from(value.clone())),
            &inner,
            value,
        )
    }

    #[test]
    fn duration() -> TestResult<()> {
        let inner = Schema::Fixed {
            name: Name::new("duration").unwrap(),
            aliases: None,
            doc: None,
            size: 12,
        };
        let value = Value::Duration(Duration::new(
            Months::new(256),
            Days::new(512),
            Millis::new(1024),
        ));
        logical_type_test(
            r#"{"type": {"type": "fixed", "name": "duration", "size": 12}, "logicalType": "duration"}"#,
            &Schema::Duration,
            value,
            &inner,
            Value::Fixed(12, vec![0, 1, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0]),
        )
    }

    #[test]
    fn test_writer_append() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        let n1 = writer.append(record.clone()).unwrap();
        let n2 = writer.append(record.clone()).unwrap();
        let n3 = writer.flush().unwrap();
        let result = writer.into_inner().unwrap();

        assert_eq!(n1 + n2 + n3, result.len());

        let mut data = Vec::new();
        zig_i64(27, &mut data);
        zig_i64(3, &mut data);
        data.extend(b"foo");
        data.extend(data.clone());

        // starts with magic
        assert_eq!(&result[..AVRO_OBJECT_HEADER_LEN], AVRO_OBJECT_HEADER);
        // ends with data and sync marker
        let last_data_byte = result.len() - 16;
        assert_eq!(
            &result[last_data_byte - data.len()..last_data_byte],
            data.as_slice()
        );
    }

    #[test]
    fn test_writer_extend() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        let record_copy = record.clone();
        let records = vec![record, record_copy];

        let n1 = writer.extend(records.into_iter()).unwrap();
        let n2 = writer.flush().unwrap();
        let result = writer.into_inner().unwrap();

        assert_eq!(n1 + n2, result.len());

        let mut data = Vec::new();
        zig_i64(27, &mut data);
        zig_i64(3, &mut data);
        data.extend(b"foo");
        data.extend(data.clone());

        // starts with magic
        assert_eq!(&result[..AVRO_OBJECT_HEADER_LEN], AVRO_OBJECT_HEADER);
        // ends with data and sync marker
        let last_data_byte = result.len() - 16;
        assert_eq!(
            &result[last_data_byte - data.len()..last_data_byte],
            data.as_slice()
        );
    }

    #[derive(Debug, Clone, Deserialize, Serialize)]
    struct TestSerdeSerialize {
        a: i64,
        b: String,
    }

    #[test]
    fn test_writer_append_ser() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        let record = TestSerdeSerialize {
            a: 27,
            b: "foo".to_owned(),
        };

        let n1 = writer.append_ser(record).unwrap();
        let n2 = writer.flush().unwrap();
        let result = writer.into_inner().unwrap();

        assert_eq!(n1 + n2, result.len());

        let mut data = Vec::new();
        zig_i64(27, &mut data);
        zig_i64(3, &mut data);
        data.extend(b"foo");

        // starts with magic
        assert_eq!(&result[..AVRO_OBJECT_HEADER_LEN], AVRO_OBJECT_HEADER);
        // ends with data and sync marker
        let last_data_byte = result.len() - 16;
        assert_eq!(
            &result[last_data_byte - data.len()..last_data_byte],
            data.as_slice()
        );
    }

    #[test]
    fn test_writer_extend_ser() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        let record = TestSerdeSerialize {
            a: 27,
            b: "foo".to_owned(),
        };
        let record_copy = record.clone();
        let records = vec![record, record_copy];

        let n1 = writer.extend_ser(records.into_iter()).unwrap();
        let n2 = writer.flush().unwrap();
        let result = writer.into_inner().unwrap();

        assert_eq!(n1 + n2, result.len());

        let mut data = Vec::new();
        zig_i64(27, &mut data);
        zig_i64(3, &mut data);
        data.extend(b"foo");
        data.extend(data.clone());

        // starts with magic
        assert_eq!(&result[..AVRO_OBJECT_HEADER_LEN], AVRO_OBJECT_HEADER);
        // ends with data and sync marker
        let last_data_byte = result.len() - 16;
        assert_eq!(
            &result[last_data_byte - data.len()..last_data_byte],
            data.as_slice()
        );
    }

    fn make_writer_with_codec(schema: &Schema) -> Writer<'_, Vec<u8>> {
        Writer::with_codec(schema, Vec::new(), Codec::Deflate)
    }

    fn make_writer_with_builder(schema: &Schema) -> Writer<'_, Vec<u8>> {
        Writer::builder()
            .writer(Vec::new())
            .schema(schema)
            .codec(Codec::Deflate)
            .block_size(100)
            .build()
    }

    fn check_writer(mut writer: Writer<'_, Vec<u8>>, schema: &Schema) {
        let mut record = Record::new(schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        let n1 = writer.append(record.clone()).unwrap();
        let n2 = writer.append(record.clone()).unwrap();
        let n3 = writer.flush().unwrap();
        let result = writer.into_inner().unwrap();

        assert_eq!(n1 + n2 + n3, result.len());

        let mut data = Vec::new();
        zig_i64(27, &mut data);
        zig_i64(3, &mut data);
        data.extend(b"foo");
        data.extend(data.clone());
        Codec::Deflate.compress(&mut data).unwrap();

        // starts with magic
        assert_eq!(&result[..AVRO_OBJECT_HEADER_LEN], AVRO_OBJECT_HEADER);
        // ends with data and sync marker
        let last_data_byte = result.len() - 16;
        assert_eq!(
            &result[last_data_byte - data.len()..last_data_byte],
            data.as_slice()
        );
    }

    #[test]
    fn test_writer_with_codec() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let writer = make_writer_with_codec(&schema);
        check_writer(writer, &schema);
    }

    #[test]
    fn test_writer_with_builder() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let writer = make_writer_with_builder(&schema);
        check_writer(writer, &schema);
    }

    #[test]
    fn test_logical_writer() {
        const LOGICAL_TYPE_SCHEMA: &str = r#"
        {
          "type": "record",
          "name": "logical_type_test",
          "fields": [
            {
              "name": "a",
              "type": [
                "null",
                {
                  "type": "long",
                  "logicalType": "timestamp-micros"
                }
              ]
            }
          ]
        }
        "#;
        let codec = Codec::Deflate;
        let schema = Schema::parse_str(LOGICAL_TYPE_SCHEMA).unwrap();
        let mut writer = Writer::builder()
            .schema(&schema)
            .codec(codec)
            .writer(Vec::new())
            .build();

        let mut record1 = Record::new(&schema).unwrap();
        record1.put(
            "a",
            Value::Union(1, Box::new(Value::TimestampMicros(1234_i64))),
        );

        let mut record2 = Record::new(&schema).unwrap();
        record2.put("a", Value::Union(0, Box::new(Value::Null)));

        let n1 = writer.append(record1).unwrap();
        let n2 = writer.append(record2).unwrap();
        let n3 = writer.flush().unwrap();
        let result = writer.into_inner().unwrap();

        assert_eq!(n1 + n2 + n3, result.len());

        let mut data = Vec::new();
        // byte indicating not null
        zig_i64(1, &mut data);
        zig_i64(1234, &mut data);

        // byte indicating null
        zig_i64(0, &mut data);
        codec.compress(&mut data).unwrap();

        // starts with magic
        assert_eq!(&result[..AVRO_OBJECT_HEADER_LEN], AVRO_OBJECT_HEADER);
        // ends with data and sync marker
        let last_data_byte = result.len() - 16;
        assert_eq!(
            &result[last_data_byte - data.len()..last_data_byte],
            data.as_slice()
        );
    }

    #[test]
    fn test_avro_3405_writer_add_metadata_success() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        writer
            .add_user_metadata("stringKey".to_string(), String::from("stringValue"))
            .unwrap();
        writer
            .add_user_metadata("strKey".to_string(), "strValue")
            .unwrap();
        writer
            .add_user_metadata("bytesKey".to_string(), b"bytesValue")
            .unwrap();
        writer
            .add_user_metadata("vecKey".to_string(), vec![1, 2, 3])
            .unwrap();

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        writer.append(record.clone()).unwrap();
        writer.append(record.clone()).unwrap();
        writer.flush().unwrap();
        let result = writer.into_inner().unwrap();

        assert_eq!(result.len(), 260);
    }

    #[test]
    fn test_avro_3405_writer_add_metadata_failure() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        writer.append(record.clone()).unwrap();

        match writer.add_user_metadata("stringKey".to_string(), String::from("value2")) {
            Err(e @ Error::FileHeaderAlreadyWritten) => {
                assert_eq!(e.to_string(), "The file metadata is already flushed.")
            }
            Err(e) => panic!(
                "Unexpected error occurred while writing user metadata: {:?}",
                e
            ),
            Ok(_) => panic!("Expected an error that metadata cannot be added after adding data"),
        }
    }

    #[test]
    fn test_avro_3405_writer_add_metadata_reserved_prefix_failure() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        let key = "avro.stringKey".to_string();
        match writer.add_user_metadata(key.clone(), "value") {
            Err(ref e @ Error::InvalidMetadataKey(_)) => {
                assert_eq!(e.to_string(), format!("Metadata keys starting with 'avro.' are reserved for internal usage: {}.", key))
            }
            Err(e) => panic!(
                "Unexpected error occurred while writing user metadata with reserved prefix ('avro.'): {:?}",
                e
            ),
            Ok(_) => panic!("Expected an error that the metadata key cannot be prefixed with 'avro.'"),
        }
    }

    #[test]
    fn test_avro_3405_writer_add_metadata_with_builder_api_success() {
        let schema = Schema::parse_str(SCHEMA).unwrap();

        let mut user_meta_data: HashMap<String, Value> = HashMap::new();
        user_meta_data.insert(
            "stringKey".to_string(),
            Value::String("stringValue".to_string()),
        );
        user_meta_data.insert("bytesKey".to_string(), Value::Bytes(b"bytesValue".to_vec()));
        user_meta_data.insert("vecKey".to_string(), Value::Bytes(vec![1, 2, 3]));

        let writer: Writer<'_, Vec<u8>> = Writer::builder()
            .writer(Vec::new())
            .schema(&schema)
            .user_metadata(user_meta_data.clone())
            .build();

        assert_eq!(writer.user_metadata, user_meta_data);
    }

    #[derive(Serialize, Clone)]
    struct TestSingleObjectWriter {
        a: i64,
        b: f64,
        c: Vec<String>,
    }

    impl AvroSchema for TestSingleObjectWriter {
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

    impl From<TestSingleObjectWriter> for Value {
        fn from(obj: TestSingleObjectWriter) -> Value {
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
    fn test_single_object_writer() {
        let mut buf: Vec<u8> = Vec::new();
        let obj = TestSingleObjectWriter {
            a: 300,
            b: 34.555,
            c: vec!["cat".into(), "dog".into()],
        };
        let mut writer = GenericSingleObjectWriter::new_with_capacity(
            &TestSingleObjectWriter::get_schema(),
            1024,
        )
        .expect("Should resolve schema");
        let value = obj.into();
        let written_bytes = writer
            .write_value_ref(&value, &mut buf)
            .expect("Error serializing properly");

        assert!(buf.len() > 10, "no bytes written");
        assert_eq!(buf.len(), written_bytes);
        assert_eq!(buf[0], 0xC3);
        assert_eq!(buf[1], 0x01);
        assert_eq!(
            &buf[2..10],
            &TestSingleObjectWriter::get_schema()
                .fingerprint::<Rabin>()
                .bytes[..]
        );
        let mut msg_binary = Vec::new();
        encode(
            &value,
            &TestSingleObjectWriter::get_schema(),
            &mut msg_binary,
        )
        .expect("encode should have failed by here as a depndency of any writing");
        assert_eq!(&buf[10..], &msg_binary[..])
    }

    #[test]
    fn test_writer_parity() {
        let obj1 = TestSingleObjectWriter {
            a: 300,
            b: 34.555,
            c: vec!["cat".into(), "dog".into()],
        };

        let mut buf1: Vec<u8> = Vec::new();
        let mut buf2: Vec<u8> = Vec::new();
        let mut buf3: Vec<u8> = Vec::new();

        let mut generic_writer = GenericSingleObjectWriter::new_with_capacity(
            &TestSingleObjectWriter::get_schema(),
            1024,
        )
        .expect("Should resolve schema");
        let mut specifc_writer = SingleObjectWriter::<TestSingleObjectWriter>::with_capacity(1024)
            .expect("Resolved should pass");
        specifc_writer
            .write(obj1.clone(), &mut buf1)
            .expect("Serialization expected");
        specifc_writer
            .write_value(obj1.clone(), &mut buf2)
            .expect("Serialization expected");
        generic_writer
            .write_value(obj1.into(), &mut buf3)
            .expect("Serialization expected");
        assert_eq!(buf1, buf2);
        assert_eq!(buf1, buf3);
    }
}
