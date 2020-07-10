use thiserror::Error;

pub(crate) type AvroResult<T> = Result<T, Error>;

#[non_exhaustive]
#[derive(Error, Debug)]
/// Error type returned from the library
pub enum Error {
    /// All cases of `std::io::Error`
    #[error(transparent)]
    IO(#[from] std::io::Error),

    /// Error due to unrecognized coded
    #[error("unrecognized codec: {0:?}")]
    Codec(String),

    /// Errors happened while decoding Avro data (except for `std::io::Error`)
    #[error("decoding error: {0}")]
    Decode(String),

    /// Errors happened while parsing Avro schemas
    #[error("failed to parse schema: {0}")]
    Parse(String),

    /// Errors happened while performing schema resolution on Avro data
    #[error("schema resolution error: {0}")]
    SchemaResolution(String),

    /// Errors happened while validating Avro data
    #[error("validation error: {0}")]
    Validation(String),

    /// Errors that could be encountered while serializing data, implements `serde::ser::Error`
    #[error("data serialization error: {0}")]
    Ser(String),

    /// Errors that could be encountered while deserializing data, implements `serde::de::Error`
    #[error("data deserialization error: {0}")]
    De(String),

    /// Error happened trying to allocate too many bytes
    #[error("unable to allocate {desired} bytes (maximum allowed: {maximum})")]
    MemoryAllocation { desired: usize, maximum: usize },

    /// All cases of `uuid::Error`
    #[error(transparent)]
    Uuid(#[from] uuid::Error),

    /// Error happening with decimal representation
    #[error("number of bytes requested for decimal sign extension {requested} is less than the number of bytes needed to decode {needed}")]
    SignExtend { requested: usize, needed: usize },

    /// All cases of `std::num::TryFromIntError`
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),

    /// All cases of `serde_json::Error`
    #[error(transparent)]
    JSON(#[from] serde_json::Error),

    /// All cases of `std::string::FromUtf8Error`
    #[error(transparent)]
    FromUtf8(#[from] std::string::FromUtf8Error),

    /// Error happening when there is a mismatch of the snappy CRC
    #[error("bad Snappy CRC32; expected {expected:x} but got {found:x}")]
    SnappyCrcError { expected: u32, found: u32 },

    /// Errors coming from Snappy encoding and decoding
    #[cfg(feature = "snappy")]
    #[error(transparent)]
    Snappy(#[from] snap::Error),
}
