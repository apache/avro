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

//! Logic for all supported compression codecs in Avro.
use crate::{types::Value, AvroResult, Error};
use libflate::deflate::{Decoder, Encoder};
use std::io::{Read, Write};
use strum_macros::{EnumIter, EnumString, IntoStaticStr};

#[cfg(feature = "bzip")]
use bzip2::{
    read::{BzDecoder, BzEncoder},
    Compression,
};
#[cfg(feature = "snappy")]
extern crate crc32fast;
#[cfg(feature = "snappy")]
use crc32fast::Hasher;
#[cfg(feature = "xz")]
use xz2::read::{XzDecoder, XzEncoder};

/// The compression codec used to compress blocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumIter, EnumString, IntoStaticStr)]
#[strum(serialize_all = "kebab_case")]
pub enum Codec {
    /// The `Null` codec simply passes through data uncompressed.
    Null,
    /// The `Deflate` codec writes the data block using the deflate algorithm
    /// as specified in RFC 1951, and typically implemented using the zlib library.
    /// Note that this format (unlike the "zlib format" in RFC 1950) does not have a checksum.
    Deflate,
    #[cfg(feature = "snappy")]
    /// The `Snappy` codec uses Google's [Snappy](http://google.github.io/snappy/)
    /// compression library. Each compressed block is followed by the 4-byte, big-endian
    /// CRC32 checksum of the uncompressed data in the block.
    Snappy,
    #[cfg(feature = "zstandard")]
    Zstandard,
    #[cfg(feature = "bzip")]
    /// The `BZip2` codec uses [BZip2](https://sourceware.org/bzip2/)
    /// compression library.
    Bzip2,
    #[cfg(feature = "xz")]
    /// The `Xz` codec uses [Xz utils](https://tukaani.org/xz/)
    /// compression library.
    Xz,
}

impl From<Codec> for Value {
    fn from(value: Codec) -> Self {
        Self::Bytes(<&str>::from(value).as_bytes().to_vec())
    }
}

impl Codec {
    /// Compress a stream of bytes in-place.
    pub fn compress(self, stream: &mut Vec<u8>) -> AvroResult<()> {
        match self {
            Codec::Null => (),
            Codec::Deflate => {
                let mut encoder = Encoder::new(Vec::new());
                encoder.write_all(stream).map_err(Error::DeflateCompress)?;
                // Deflate errors seem to just be io::Error
                *stream = encoder
                    .finish()
                    .into_result()
                    .map_err(Error::DeflateCompressFinish)?;
            }
            #[cfg(feature = "snappy")]
            Codec::Snappy => {
                use byteorder::ByteOrder;

                let mut encoded: Vec<u8> = vec![0; snap::raw::max_compress_len(stream.len())];
                let compressed_size = snap::raw::Encoder::new()
                    .compress(&stream[..], &mut encoded[..])
                    .map_err(Error::SnappyCompress)?;

                let mut hasher = Hasher::new();
                hasher.update(&stream[..]);
                let checksum = hasher.finalize();
                byteorder::BigEndian::write_u32(&mut encoded[compressed_size..], checksum);
                encoded.truncate(compressed_size + 4);

                *stream = encoded;
            }
            #[cfg(feature = "zstandard")]
            Codec::Zstandard => {
                let mut encoder = zstd::Encoder::new(Vec::new(), 0).unwrap();
                encoder.write_all(stream).map_err(Error::ZstdCompress)?;
                *stream = encoder.finish().unwrap();
            }
            #[cfg(feature = "bzip")]
            Codec::Bzip2 => {
                let mut encoder = BzEncoder::new(&stream[..], Compression::best());
                let mut buffer = Vec::new();
                encoder.read_to_end(&mut buffer).unwrap();
                *stream = buffer;
            }
            #[cfg(feature = "xz")]
            Codec::Xz => {
                let compression_level = 9;
                let mut encoder = XzEncoder::new(&stream[..], compression_level);
                let mut buffer = Vec::new();
                encoder.read_to_end(&mut buffer).unwrap();
                *stream = buffer;
            }
        };

        Ok(())
    }

    /// Decompress a stream of bytes in-place.
    pub fn decompress(self, stream: &mut Vec<u8>) -> AvroResult<()> {
        *stream = match self {
            Codec::Null => return Ok(()),
            Codec::Deflate => {
                let mut decoded = Vec::new();
                let mut decoder = Decoder::new(&stream[..]);
                decoder
                    .read_to_end(&mut decoded)
                    .map_err(Error::DeflateDecompress)?;
                decoded
            }
            #[cfg(feature = "snappy")]
            Codec::Snappy => {
                use byteorder::ByteOrder;

                let decompressed_size = snap::raw::decompress_len(&stream[..stream.len() - 4])
                    .map_err(Error::GetSnappyDecompressLen)?;
                let mut decoded = vec![0; decompressed_size];
                snap::raw::Decoder::new()
                    .decompress(&stream[..stream.len() - 4], &mut decoded[..])
                    .map_err(Error::SnappyDecompress)?;

                let expected = byteorder::BigEndian::read_u32(&stream[stream.len() - 4..]);
                let mut hasher = Hasher::new();
                hasher.update(&decoded);
                let actual = hasher.finalize();

                if expected != actual {
                    return Err(Error::SnappyCrc32 { expected, actual });
                }
                decoded
            }
            #[cfg(feature = "zstandard")]
            Codec::Zstandard => {
                let mut decoded = Vec::new();
                let mut decoder = zstd::Decoder::new(&stream[..]).unwrap();
                std::io::copy(&mut decoder, &mut decoded).map_err(Error::ZstdDecompress)?;
                decoded
            }
            #[cfg(feature = "bzip")]
            Codec::Bzip2 => {
                let mut decoder = BzDecoder::new(&stream[..]);
                let mut decoded = Vec::new();
                decoder.read_to_end(&mut decoded).unwrap();
                decoded
            }
            #[cfg(feature = "xz")]
            Codec::Xz => {
                let mut decoder = XzDecoder::new(&stream[..]);
                let mut decoded: Vec<u8> = Vec::new();
                decoder.read_to_end(&mut decoded).unwrap();
                decoded
            }
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::{assert_eq, assert_ne};

    const INPUT: &[u8] = b"theanswertolifetheuniverseandeverythingis42theanswertolifetheuniverseandeverythingis4theanswertolifetheuniverseandeverythingis2";

    #[test]
    fn null_compress_and_decompress() {
        let codec = Codec::Null;
        let mut stream = INPUT.to_vec();
        codec.compress(&mut stream).unwrap();
        assert_eq!(INPUT, stream.as_slice());
        codec.decompress(&mut stream).unwrap();
        assert_eq!(INPUT, stream.as_slice());
    }

    #[test]
    fn deflate_compress_and_decompress() {
        compress_and_decompress(Codec::Deflate);
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn snappy_compress_and_decompress() {
        compress_and_decompress(Codec::Snappy);
    }

    #[cfg(feature = "zstandard")]
    #[test]
    fn zstd_compress_and_decompress() {
        compress_and_decompress(Codec::Zstandard);
    }

    #[cfg(feature = "bzip")]
    #[test]
    fn bzip_compress_and_decompress() {
        compress_and_decompress(Codec::Bzip2);
    }

    #[cfg(feature = "xz")]
    #[test]
    fn xz_compress_and_decompress() {
        compress_and_decompress(Codec::Xz);
    }

    fn compress_and_decompress(codec: Codec) {
        let mut stream = INPUT.to_vec();
        codec.compress(&mut stream).unwrap();
        assert_ne!(INPUT, stream.as_slice());
        assert!(INPUT.len() > stream.len());
        codec.decompress(&mut stream).unwrap();
        assert_eq!(INPUT, stream.as_slice());
    }

    #[test]
    fn codec_to_str() {
        assert_eq!(<&str>::from(Codec::Null), "null");
        assert_eq!(<&str>::from(Codec::Deflate), "deflate");

        #[cfg(feature = "snappy")]
        assert_eq!(<&str>::from(Codec::Snappy), "snappy");

        #[cfg(feature = "zstandard")]
        assert_eq!(<&str>::from(Codec::Zstandard), "zstandard");

        #[cfg(feature = "bzip")]
        assert_eq!(<&str>::from(Codec::Bzip2), "bzip2");

        #[cfg(feature = "xz")]
        assert_eq!(<&str>::from(Codec::Xz), "xz");
    }

    #[test]
    fn codec_from_str() {
        use std::str::FromStr;

        assert_eq!(Codec::from_str("null").unwrap(), Codec::Null);
        assert_eq!(Codec::from_str("deflate").unwrap(), Codec::Deflate);

        #[cfg(feature = "snappy")]
        assert_eq!(Codec::from_str("snappy").unwrap(), Codec::Snappy);

        #[cfg(feature = "zstandard")]
        assert_eq!(Codec::from_str("zstandard").unwrap(), Codec::Zstandard);

        #[cfg(feature = "bzip")]
        assert_eq!(Codec::from_str("bzip2").unwrap(), Codec::Bzip2);

        #[cfg(feature = "xz")]
        assert_eq!(Codec::from_str("xz").unwrap(), Codec::Xz);

        assert!(Codec::from_str("not a codec").is_err());
    }
}
