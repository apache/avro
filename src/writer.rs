//! Logic handling writing in Avro format at user level.
use std::collections::HashMap;
use std::io::Write;

use failure::{Error, Fail};
use rand::random;
use serde::Serialize;

use crate::encode::{encode, encode_ref, encode_to_vec};
use crate::schema::Schema;
use crate::ser::Serializer;
use crate::types::{ToAvro, Value};
use crate::Codec;

const DEFAULT_BLOCK_SIZE: usize = 16000;
const AVRO_OBJECT_HEADER: &[u8] = b"Obj\x01";

/// Describes errors happened while validating Avro data.
#[derive(Fail, Debug)]
#[fail(display = "Validation error: {}", _0)]
pub struct ValidationError(String);

impl ValidationError {
    pub fn new<S>(msg: S) -> ValidationError
    where
        S: Into<String>,
    {
        ValidationError(msg.into())
    }
}

/// Main interface for writing Avro formatted values.
#[derive(typed_builder::TypedBuilder)]
pub struct Writer<'a, W> {
    schema: &'a Schema,
    writer: W,
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
}

impl<'a, W: Write> Writer<'a, W> {
    /// Creates a `Writer` given a `Schema` and something implementing the `io::Write` trait to write
    /// to.
    /// No compression `Codec` will be used.
    pub fn new(schema: &'a Schema, writer: W) -> Self {
        Self::builder().schema(schema).writer(writer).build()
    }

    /// Creates a `Writer` with a specific `Codec` given a `Schema` and something implementing the
    /// `io::Write` trait to write to.
    pub fn with_codec(schema: &'a Schema, writer: W, codec: Codec) -> Self {
        Self::builder()
            .schema(schema)
            .writer(writer)
            .codec(codec)
            .build()
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
    pub fn append<T: ToAvro>(&mut self, value: T) -> Result<usize, Error> {
        let n = if !self.has_header {
            let header = self.header()?;
            let n = self.append_bytes(header.as_ref())?;
            self.has_header = true;
            n
        } else {
            0
        };

        let avro = value.avro();
        write_value_ref(self.schema, &avro, &mut self.buffer)?;

        self.num_values += 1;

        if self.buffer.len() >= self.block_size {
            return self.flush().map(|b| b + n);
        }

        Ok(n)
    }

    /// Append a compatible value to a `Writer`, also performing schema validation.
    ///
    /// Return the number of bytes written (it might be 0, see below).
    ///
    /// **NOTE** This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](struct.Writer.html#method.flush).
    pub fn append_value_ref(&mut self, value: &Value) -> Result<usize, Error> {
        let n = if !self.has_header {
            let header = self.header()?;
            let n = self.append_bytes(header.as_ref())?;
            self.has_header = true;
            n
        } else {
            0
        };

        write_value_ref(self.schema, value, &mut self.buffer)?;

        self.num_values += 1;

        if self.buffer.len() >= self.block_size {
            return self.flush().map(|b| b + n);
        }

        Ok(n)
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
    pub fn append_ser<S: Serialize>(&mut self, value: S) -> Result<usize, Error> {
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
    pub fn extend<I, T: ToAvro>(&mut self, values: I) -> Result<usize, Error>
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
    pub fn extend_ser<I, T: Serialize>(&mut self, values: I) -> Result<usize, Error>
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
    pub fn extend_from_slice(&mut self, values: &[Value]) -> Result<usize, Error> {
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
    pub fn flush(&mut self) -> Result<usize, Error> {
        if self.num_values == 0 {
            return Ok(0);
        }

        self.codec.compress(&mut self.buffer)?;

        let num_values = self.num_values;
        let stream_len = self.buffer.len();

        let num_bytes = self.append_raw(&num_values.avro(), &Schema::Long)?
            + self.append_raw(&stream_len.avro(), &Schema::Long)?
            + self.writer.write(self.buffer.as_ref())?
            + self.append_marker()?;

        self.buffer.clear();
        self.num_values = 0;

        Ok(num_bytes)
    }

    /// Return what the `Writer` is writing to, consuming the `Writer` itself.
    ///
    /// **NOTE** This function forces the written data to be flushed (an implicit
    /// call to [`flush`](struct.Writer.html#method.flush) is performed).
    pub fn into_inner(mut self) -> Result<W, Error> {
        self.flush()?;
        Ok(self.writer)
    }

    /// Generate and append synchronization marker to the payload.
    fn append_marker(&mut self) -> Result<usize, Error> {
        // using .writer.write directly to avoid mutable borrow of self
        // with ref borrowing of self.marker
        Ok(self.writer.write(&self.marker)?)
    }

    /// Append a raw Avro Value to the payload avoiding to encode it again.
    fn append_raw(&mut self, value: &Value, schema: &Schema) -> Result<usize, Error> {
        self.append_bytes(encode_to_vec(&value, schema).as_ref())
    }

    /// Append pure bytes to the payload.
    fn append_bytes(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        Ok(self.writer.write(bytes)?)
    }

    /// Create an Avro header based on schema, codec and sync marker.
    fn header(&self) -> Result<Vec<u8>, Error> {
        let schema_bytes = serde_json::to_string(self.schema)?.into_bytes();

        let mut metadata = HashMap::with_capacity(2);
        metadata.insert("avro.schema", Value::Bytes(schema_bytes));
        metadata.insert("avro.codec", self.codec.avro());

        let mut header = Vec::new();
        header.extend_from_slice(AVRO_OBJECT_HEADER);
        encode(
            &metadata.avro(),
            &Schema::Map(Box::new(Schema::Bytes)),
            &mut header,
        );
        header.extend_from_slice(&self.marker);

        Ok(header)
    }
}

/// Encode a compatible value (implementing the `ToAvro` trait) into Avro format, also performing
/// schema validation.
///
/// This is an internal function which gets the bytes buffer where to write as parameter instead of
/// creating a new one like `to_avro_datum`.
fn write_avro_datum<T: ToAvro>(
    schema: &Schema,
    value: T,
    buffer: &mut Vec<u8>,
) -> Result<(), Error> {
    let avro = value.avro();
    if !avro.validate(schema) {
        return Err(ValidationError::new("value does not match schema").into());
    }
    encode(&avro, schema, buffer);
    Ok(())
}

fn write_value_ref(schema: &Schema, value: &Value, buffer: &mut Vec<u8>) -> Result<(), Error> {
    if !value.validate(schema) {
        return Err(ValidationError::new("value does not match schema").into());
    }
    encode_ref(value, schema, buffer);
    Ok(())
}

/// Encode a compatible value (implementing the `ToAvro` trait) into Avro format, also
/// performing schema validation.
///
/// **NOTE** This function has a quite small niche of usage and does NOT generate headers and sync
/// markers; use [`Writer`](struct.Writer.html) to be fully Avro-compatible if you don't know what
/// you are doing, instead.
pub fn to_avro_datum<T: ToAvro>(schema: &Schema, value: T) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    write_avro_datum(schema, value, &mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decimal::Decimal;
    use crate::duration::{Days, Duration, Millis, Months};
    use crate::schema::Name;
    use crate::types::Record;
    use crate::util::zig_i64;
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
        let union = Value::Union(Box::new(Value::Long(3)));

        let mut expected = Vec::new();
        zig_i64(1, &mut expected);
        zig_i64(3, &mut expected);

        assert_eq!(to_avro_datum(&schema, union).unwrap(), expected);
    }

    #[test]
    fn test_union_null() {
        let schema = Schema::parse_str(UNION_SCHEMA).unwrap();
        let union = Value::Union(Box::new(Value::Null));

        let mut expected = Vec::new();
        zig_i64(0, &mut expected);

        assert_eq!(to_avro_datum(&schema, union).unwrap(), expected);
    }

    type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

    fn logical_type_test<T: ToAvro + Clone>(
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
        let raw_ser = to_avro_datum(&raw_schema, raw_value)?;
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
            name: Name::new("decimal"),
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
            name: Name::new("duration"),
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
            Value::Union(Box::new(Value::TimestampMicros(1234_i64))),
        );

        let mut record2 = Record::new(&schema).unwrap();
        record2.put("a", Value::Union(Box::new(Value::Null)));

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
}
