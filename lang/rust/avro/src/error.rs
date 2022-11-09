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
    schema::{Name, SchemaKind},
    types::ValueKind,
};
use std::fmt;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Bad Snappy CRC32; expected {expected:x} but got {actual:x}")]
    SnappyCrc32 { expected: u32, actual: u32 },

    #[error("Invalid u8 for bool: {0}")]
    BoolValue(u8),

    #[error("Not a fixed value, required for decimal with fixed schema: {0:?}")]
    FixedValue(ValueKind),

    #[error("Not a bytes value, required for decimal with bytes schema: {0:?}")]
    BytesValue(ValueKind),

    #[error("Not a string value, required for uuid: {0:?}")]
    GetUuidFromStringValue(ValueKind),

    #[error("Two schemas with the same fullname were given: {0:?}")]
    NameCollision(String),

    #[error("Not a fixed or bytes type, required for decimal schema, got: {0:?}")]
    ResolveDecimalSchema(SchemaKind),

    #[error("Invalid utf-8 string")]
    ConvertToUtf8(#[source] std::string::FromUtf8Error),

    #[error("Invalid utf-8 string")]
    ConvertToUtf8Error(#[source] std::str::Utf8Error),

    /// Describes errors happened while validating Avro data.
    #[error("Value does not match schema")]
    Validation,

    /// Describes errors happened while validating Avro data.
    #[error("Value does not match schema: Reason: {0}")]
    ValidationWithReason(String),

    #[error("Unable to allocate {desired} bytes (maximum allowed: {maximum})")]
    MemoryAllocation { desired: usize, maximum: usize },

    /// Describe a specific error happening with decimal representation
    #[error("Number of bytes requested for decimal sign extension {requested} is less than the number of bytes needed to decode {needed}")]
    SignExtend { requested: usize, needed: usize },

    #[error("Failed to read boolean bytes")]
    ReadBoolean(#[source] std::io::Error),

    #[error("Failed to read bytes")]
    ReadBytes(#[source] std::io::Error),

    #[error("Failed to read string")]
    ReadString(#[source] std::io::Error),

    #[error("Failed to read double")]
    ReadDouble(#[source] std::io::Error),

    #[error("Failed to read float")]
    ReadFloat(#[source] std::io::Error),

    #[error("Failed to read duration")]
    ReadDuration(#[source] std::io::Error),

    #[error("Failed to read fixed number of bytes: {1}")]
    ReadFixed(#[source] std::io::Error, usize),

    #[error("Failed to convert &str to UUID")]
    ConvertStrToUuid(#[source] uuid::Error),

    #[error("Map key is not a string; key type is {0:?}")]
    MapKeyType(ValueKind),

    #[error("Union index {index} out of bounds: {num_variants}")]
    GetUnionVariant { index: i64, num_variants: usize },

    #[error("Enum symbol index out of bounds: {num_variants}")]
    EnumSymbolIndex { index: usize, num_variants: usize },

    #[error("Enum symbol not found {0}")]
    GetEnumSymbol(String),

    #[error("Unable to decode enum index")]
    GetEnumUnknownIndexValue,

    #[error("Scale {scale} is greater than precision {precision}")]
    GetScaleAndPrecision { scale: usize, precision: usize },

    #[error(
        "Fixed type number of bytes {size} is not large enough to hold decimal values of precision {precision}"
    )]
    GetScaleWithFixedSize { size: usize, precision: usize },

    #[error("expected UUID, got: {0:?}")]
    GetUuid(ValueKind),

    #[error("expected Decimal, got: {0:?}")]
    GetDecimal(ValueKind),

    #[error("expected Duration, got: {0:?}")]
    GetDuration(ValueKind),

    #[error("Fixed bytes of size 12 expected, got Fixed of size {0}")]
    GetDecimalFixedBytes(usize),

    #[error("Duration expected, got {0:?}")]
    ResolveDuration(ValueKind),

    #[error("Decimal expected, got {0:?}")]
    ResolveDecimal(ValueKind),

    #[error("Missing field in record: {0:?}")]
    GetField(String),

    #[error("Unable to convert to u8, got {0:?}")]
    GetU8(ValueKind),

    #[error("Precision {precision} too small to hold decimal values with {num_bytes} bytes")]
    ComparePrecisionAndSize { precision: usize, num_bytes: usize },

    #[error("Cannot convert length to i32: {1}")]
    ConvertLengthToI32(#[source] std::num::TryFromIntError, usize),

    #[error("Date expected, got {0:?}")]
    GetDate(ValueKind),

    #[error("TimeMillis expected, got {0:?}")]
    GetTimeMillis(ValueKind),

    #[error("TimeMicros expected, got {0:?}")]
    GetTimeMicros(ValueKind),

    #[error("TimestampMillis expected, got {0:?}")]
    GetTimestampMillis(ValueKind),

    #[error("TimestampMicros expected, got {0:?}")]
    GetTimestampMicros(ValueKind),

    #[error("Null expected, got {0:?}")]
    GetNull(ValueKind),

    #[error("Boolean expected, got {0:?}")]
    GetBoolean(ValueKind),

    #[error("Int expected, got {0:?}")]
    GetInt(ValueKind),

    #[error("Long expected, got {0:?}")]
    GetLong(ValueKind),

    #[error("Double expected, got {0:?}")]
    GetDouble(ValueKind),

    #[error("Float expected, got {0:?}")]
    GetFloat(ValueKind),

    #[error("Bytes expected, got {0:?}")]
    GetBytes(ValueKind),

    #[error("String expected, got {0:?}")]
    GetString(ValueKind),

    #[error("Enum expected, got {0:?}")]
    GetEnum(ValueKind),

    #[error("Fixed size mismatch, {size} expected, got {n}")]
    CompareFixedSizes { size: usize, n: usize },

    #[error("String expected for fixed, got {0:?}")]
    GetStringForFixed(ValueKind),

    #[error("Enum default {symbol:?} is not among allowed symbols {symbols:?}")]
    GetEnumDefault {
        symbol: String,
        symbols: Vec<String>,
    },

    #[error("Enum value index {index} is out of bounds {nsymbols}")]
    GetEnumValue { index: usize, nsymbols: usize },

    #[error("Key {0} not found in decimal metadata JSON")]
    GetDecimalMetadataFromJson(&'static str),

    #[error("Could not find matching type in union")]
    FindUnionVariant,

    #[error("Array({expected:?}) expected, got {other:?}")]
    GetArray {
        expected: SchemaKind,
        other: ValueKind,
    },

    #[error("Map({expected:?}) expected, got {other:?}")]
    GetMap {
        expected: SchemaKind,
        other: ValueKind,
    },

    #[error("Record with fields {expected:?} expected, got {other:?}")]
    GetRecord {
        expected: Vec<(String, SchemaKind)>,
        other: ValueKind,
    },

    #[error("No `name` field")]
    GetNameField,

    #[error("No `name` in record field")]
    GetNameFieldFromRecord,

    #[error("Unions may not directly contain a union")]
    GetNestedUnion,

    #[error("Unions cannot contain duplicate types")]
    GetUnionDuplicate,

    #[error("Union's first type {0:?} must match the `default`'s value type {1:?}")]
    GetDefaultUnion(SchemaKind, ValueKind),

    #[error("JSON value {0} claims to be u64 but cannot be converted")]
    GetU64FromJson(serde_json::Number),

    #[error("JSON value {0} claims to be i64 but cannot be converted")]
    GetI64FromJson(serde_json::Number),

    #[error("Failed to convert from type to apache_avro::types::Value")]
    ConvertFromValue(String),

    #[error("Cannot convert u64 to usize: {1}")]
    ConvertU64ToUsize(#[source] std::num::TryFromIntError, u64),

    #[error("Cannot convert u32 to usize: {1}")]
    ConvertU32ToUsize(#[source] std::num::TryFromIntError, u32),

    #[error("Cannot convert i64 to usize: {1}")]
    ConvertI64ToUsize(#[source] std::num::TryFromIntError, i64),

    #[error("Cannot convert i32 to usize: {1}")]
    ConvertI32ToUsize(#[source] std::num::TryFromIntError, i32),

    #[error("Invalid JSON value for decimal precision/scale integer: {0}")]
    GetPrecisionOrScaleFromJson(serde_json::Number),

    #[error("Failed to parse schema from JSON")]
    ParseSchemaJson(#[source] serde_json::Error),

    #[error("Must be a JSON string, object or array")]
    ParseSchemaFromValidJson,

    #[error("Unknown primitive type: {0}")]
    ParsePrimitive(String),

    #[error("invalid JSON for {key:?}: {value:?}")]
    GetDecimalMetadataValueFromJson {
        key: String,
        value: serde_json::Value,
    },

    #[error("The decimal precision ({precision}) must be bigger or equal to the scale ({scale})")]
    DecimalPrecisionLessThanScale { precision: usize, scale: usize },

    #[error("The decimal precision ({precision}) must be a positive number")]
    DecimalPrecisionMuBePositive { precision: usize },

    #[error("Unexpected `type` {0} variant for `logicalType`")]
    GetLogicalTypeVariant(serde_json::Value),

    #[error("No `type` field found for `logicalType`")]
    GetLogicalTypeField,

    #[error("logicalType must be a string")]
    GetLogicalTypeFieldType,

    #[error("Unknown complex type: {0}")]
    GetComplexType(serde_json::Value),

    #[error("No `type` in complex type")]
    GetComplexTypeField,

    #[error("No `fields` in record")]
    GetRecordFieldsJson,

    #[error("No `symbols` field in enum")]
    GetEnumSymbolsField,

    #[error("Unable to parse `symbols` in enum")]
    GetEnumSymbols,

    #[error("Invalid enum symbol name {0}")]
    EnumSymbolName(String),

    #[error("Invalid schema name {0}. It must match the regex '{1}'")]
    InvalidSchemaName(String, &'static str),

    #[error("Duplicate enum symbol {0}")]
    EnumSymbolDuplicate(String),

    #[error("No `items` in array")]
    GetArrayItemsField,

    #[error("No `values` in map")]
    GetMapValuesField,

    #[error("No `size` in fixed")]
    GetFixedSizeField,

    #[error("Failed to compress with flate")]
    DeflateCompress(#[source] std::io::Error),

    #[error("Failed to finish flate compressor")]
    DeflateCompressFinish(std::io::Error),

    #[error("Failed to decompress with flate")]
    DeflateDecompress(#[source] std::io::Error),

    #[cfg(feature = "snappy")]
    #[error("Failed to compress with snappy")]
    SnappyCompress(#[source] snap::Error),

    #[cfg(feature = "snappy")]
    #[error("Failed to get snappy decompression length")]
    GetSnappyDecompressLen(#[source] snap::Error),

    #[cfg(feature = "snappy")]
    #[error("Failed to decompress with snappy")]
    SnappyDecompress(#[source] snap::Error),

    #[error("Failed to compress with zstd")]
    ZstdCompress(#[source] std::io::Error),

    #[error("Failed to decompress with zstd")]
    ZstdDecompress(#[source] std::io::Error),

    #[error("Failed to read header")]
    ReadHeader(#[source] std::io::Error),

    #[error("wrong magic in header")]
    HeaderMagic,

    #[error("Message Header mismatch. Expected: {0:?}. Actual: {1:?}")]
    SingleObjectHeaderMismatch([u8; 10], [u8; 10]),

    #[error("Failed to get JSON from avro.schema key in map")]
    GetAvroSchemaFromMap,

    #[error("no metadata in header")]
    GetHeaderMetadata,

    #[error("Failed to read marker bytes")]
    ReadMarker(#[source] std::io::Error),

    #[error("Failed to read block marker bytes")]
    ReadBlockMarker(#[source] std::io::Error),

    #[error("Read into buffer failed")]
    ReadIntoBuf(#[source] std::io::Error),

    #[error("block marker does not match header marker")]
    GetBlockMarker,

    #[error("Overflow when decoding integer value")]
    IntegerOverflow,

    #[error("Failed to read bytes for decoding variable length integer")]
    ReadVariableIntegerBytes(#[source] std::io::Error),

    #[error("Decoded integer out of range for i32: {1}")]
    ZagI32(#[source] std::num::TryFromIntError, i64),

    #[error("unable to read block")]
    ReadBlock,

    #[error("Failed to serialize value into Avro value: {0}")]
    SerializeValue(String),

    #[error("Failed to deserialize Avro value into value: {0}")]
    DeserializeValue(String),

    #[error("Failed to write buffer bytes during flush")]
    WriteBytes(#[source] std::io::Error),

    #[error("Failed to write marker")]
    WriteMarker(#[source] std::io::Error),

    #[error("Failed to convert JSON to string")]
    ConvertJsonToString(#[source] serde_json::Error),

    /// Error while converting float to json value
    #[error("failed to convert avro float to json: {0}")]
    ConvertF64ToJson(f64),

    /// Error while resolving Schema::Ref
    #[error("Unresolved schema reference: {0}")]
    SchemaResolutionError(Name),

    #[error("The file metadata is already flushed.")]
    FileHeaderAlreadyWritten,

    #[error("Metadata keys starting with 'avro.' are reserved for internal usage: {0}.")]
    InvalidMetadataKey(String),

    /// Error when two named schema have the same fully qualified name
    #[error("Two named schema defined for same fullname: {0}.")]
    AmbiguousSchemaDefinition(Name),

    #[error("Signed decimal bytes length {0} not equal to fixed schema size {1}.")]
    EncodeDecimalAsFixedError(usize, usize),

    #[error("There is no entry for {0} in the lookup table: {1}.")]
    NoEntryInLookupTable(String, String),

    #[error("Can only encode value type {value_kind:?} as one of {supported_schema:?}")]
    EncodeValueAsSchemaError {
        value_kind: ValueKind,
        supported_schema: Vec<SchemaKind>,
    },
    #[error(
        "Internal buffer not drained properly. Re-initialize the single object writer struct!"
    )]
    IllegalSingleObjectWriterState,

    #[error("Codec '{0}' is not supported/enabled")]
    CodecNotSupported(String),

    #[error("Invalid Avro data! Cannot read codec type from value that is not Value::Bytes.")]
    BadCodecMetadata,
}

impl serde::ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::SerializeValue(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::DeserializeValue(msg.to_string())
    }
}
