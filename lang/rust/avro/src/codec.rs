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
    /// The `Zstandard` codec uses Facebook's [Zstandard](https://facebook.github.io/zstd/)
    Zstandard(zstandard::ZstandardSettings),
    #[cfg(feature = "bzip")]
    /// The `BZip2` codec uses [BZip2](https://sourceware.org/bzip2/)
    /// compression library.
    Bzip2(bzip::Bzip2Settings),
    #[cfg(feature = "xz")]
    /// The `Xz` codec uses [Xz utils](https://tukaani.org/xz/)
    /// compression library.
    Xz(xz::XzSettings),
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
                let mut encoded: Vec<u8> = vec![0; snap::raw::max_compress_len(stream.len())];
                let compressed_size = snap::raw::Encoder::new()
                    .compress(&stream[..], &mut encoded[..])
                    .map_err(Error::SnappyCompress)?;

                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&stream[..]);
                let checksum = hasher.finalize();
                let checksum_as_bytes = checksum.to_be_bytes();
                let checksum_len = checksum_as_bytes.len();
                encoded.truncate(compressed_size + checksum_len);
                encoded[compressed_size..].copy_from_slice(&checksum_as_bytes);

                *stream = encoded;
            }
            #[cfg(feature = "zstandard")]
            Codec::Zstandard(settings) => {
                let mut encoder =
                    zstd::Encoder::new(Vec::new(), settings.compression_level as i32).unwrap();
                encoder.write_all(stream).map_err(Error::ZstdCompress)?;
                *stream = encoder.finish().unwrap();
            }
            #[cfg(feature = "bzip")]
            Codec::Bzip2(settings) => {
                use bzip2::read::BzEncoder;

                let mut encoder = BzEncoder::new(&stream[..], settings.compression());
                let mut buffer = Vec::new();
                encoder.read_to_end(&mut buffer).unwrap();
                *stream = buffer;
            }
            #[cfg(feature = "xz")]
            Codec::Xz(settings) => {
                use xz2::read::XzEncoder;

                let mut encoder = XzEncoder::new(&stream[..], settings.compression_level as u32);
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
                let decompressed_size = snap::raw::decompress_len(&stream[..stream.len() - 4])
                    .map_err(Error::GetSnappyDecompressLen)?;
                let mut decoded = vec![0; decompressed_size];
                snap::raw::Decoder::new()
                    .decompress(&stream[..stream.len() - 4], &mut decoded[..])
                    .map_err(Error::SnappyDecompress)?;

                let mut last_four: [u8; 4] = [0; 4];
                last_four.copy_from_slice(&stream[(stream.len() - 4)..]);
                let expected: u32 = u32::from_be_bytes(last_four);

                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&decoded);
                let actual = hasher.finalize();

                if expected != actual {
                    return Err(Error::SnappyCrc32 { expected, actual });
                }
                decoded
            }
            #[cfg(feature = "zstandard")]
            Codec::Zstandard(_settings) => {
                use std::io::BufReader;
                use zstd::zstd_safe;

                let mut decoded = Vec::new();
                let buffer_size = zstd_safe::DCtx::in_size();
                let buffer = BufReader::with_capacity(buffer_size, &stream[..]);
                let mut decoder = zstd::Decoder::new(buffer).unwrap();
                std::io::copy(&mut decoder, &mut decoded).map_err(Error::ZstdDecompress)?;
                decoded
            }
            #[cfg(feature = "bzip")]
            Codec::Bzip2(_) => {
                use bzip2::read::BzDecoder;

                let mut decoder = BzDecoder::new(&stream[..]);
                let mut decoded = Vec::new();
                decoder.read_to_end(&mut decoded).unwrap();
                decoded
            }
            #[cfg(feature = "xz")]
            Codec::Xz(_) => {
                use xz2::read::XzDecoder;

                let mut decoder = XzDecoder::new(&stream[..]);
                let mut decoded: Vec<u8> = Vec::new();
                decoder.read_to_end(&mut decoded).unwrap();
                decoded
            }
        };
        Ok(())
    }
}

#[cfg(feature = "bzip")]
pub mod bzip {
    use bzip2::Compression;

    #[derive(Clone, Copy, Eq, PartialEq, Debug)]
    pub struct Bzip2Settings {
        pub compression_level: u8,
    }

    impl Bzip2Settings {
        pub fn new(compression_level: u8) -> Self {
            Self { compression_level }
        }

        pub(crate) fn compression(&self) -> Compression {
            Compression::new(self.compression_level as u32)
        }
    }

    impl Default for Bzip2Settings {
        fn default() -> Self {
            Bzip2Settings::new(Compression::best().level() as u8)
        }
    }
}

#[cfg(feature = "zstandard")]
pub mod zstandard {
    #[derive(Clone, Copy, Eq, PartialEq, Debug)]
    pub struct ZstandardSettings {
        pub compression_level: u8,
    }

    impl ZstandardSettings {
        pub fn new(compression_level: u8) -> Self {
            Self { compression_level }
        }
    }

    impl Default for ZstandardSettings {
        fn default() -> Self {
            Self::new(0)
        }
    }
}

#[cfg(feature = "xz")]
pub mod xz {
    #[derive(Clone, Copy, Eq, PartialEq, Debug)]
    pub struct XzSettings {
        pub compression_level: u8,
    }

    impl XzSettings {
        pub fn new(compression_level: u8) -> Self {
            Self { compression_level }
        }
    }

    impl Default for XzSettings {
        fn default() -> Self {
            XzSettings::new(9)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro_test_helper::TestResult;
    use pretty_assertions::{assert_eq, assert_ne};

    const INPUT: &[u8] = b"theanswertolifetheuniverseandeverythingis42theanswertolifetheuniverseandeverythingis4theanswertolifetheuniverseandeverythingis2";

    #[test]
    fn null_compress_and_decompress() -> TestResult {
        let codec = Codec::Null;
        let mut stream = INPUT.to_vec();
        codec.compress(&mut stream)?;
        assert_eq!(INPUT, stream.as_slice());
        codec.decompress(&mut stream)?;
        assert_eq!(INPUT, stream.as_slice());
        Ok(())
    }

    #[test]
    fn deflate_compress_and_decompress() -> TestResult {
        compress_and_decompress(Codec::Deflate)
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn snappy_compress_and_decompress() -> TestResult {
        compress_and_decompress(Codec::Snappy)
    }

    #[cfg(feature = "zstandard")]
    #[test]
    fn zstd_compress_and_decompress() -> TestResult {
        compress_and_decompress(Codec::Zstandard(zstandard::ZstandardSettings::default()))
    }

    #[cfg(feature = "bzip")]
    #[test]
    fn bzip_compress_and_decompress() -> TestResult {
        compress_and_decompress(Codec::Bzip2(bzip::Bzip2Settings::default()))
    }

    #[cfg(feature = "xz")]
    #[test]
    fn xz_compress_and_decompress() -> TestResult {
        compress_and_decompress(Codec::Xz(xz::XzSettings::default()))
    }

    fn compress_and_decompress(codec: Codec) -> TestResult {
        let mut stream = INPUT.to_vec();
        codec.compress(&mut stream)?;
        assert_ne!(INPUT, stream.as_slice());
        assert!(INPUT.len() > stream.len());
        codec.decompress(&mut stream)?;
        assert_eq!(INPUT, stream.as_slice());
        Ok(())
    }

    #[test]
    fn codec_to_str() {
        assert_eq!(<&str>::from(Codec::Null), "null");
        assert_eq!(<&str>::from(Codec::Deflate), "deflate");

        #[cfg(feature = "snappy")]
        assert_eq!(<&str>::from(Codec::Snappy), "snappy");

        #[cfg(feature = "zstandard")]
        assert_eq!(
            <&str>::from(Codec::Zstandard(zstandard::ZstandardSettings::default())),
            "zstandard"
        );

        #[cfg(feature = "bzip")]
        assert_eq!(
            <&str>::from(Codec::Bzip2(bzip::Bzip2Settings::default())),
            "bzip2"
        );

        #[cfg(feature = "xz")]
        assert_eq!(<&str>::from(Codec::Xz(xz::XzSettings::default())), "xz");
    }

    #[test]
    fn codec_from_str() {
        use std::str::FromStr;

        assert_eq!(Codec::from_str("null").unwrap(), Codec::Null);
        assert_eq!(Codec::from_str("deflate").unwrap(), Codec::Deflate);

        #[cfg(feature = "snappy")]
        assert_eq!(Codec::from_str("snappy").unwrap(), Codec::Snappy);

        #[cfg(feature = "zstandard")]
        assert_eq!(
            Codec::from_str("zstandard").unwrap(),
            Codec::Zstandard(zstandard::ZstandardSettings::default())
        );

        #[cfg(feature = "bzip")]
        assert_eq!(
            Codec::from_str("bzip2").unwrap(),
            Codec::Bzip2(bzip::Bzip2Settings::default())
        );

        #[cfg(feature = "xz")]
        assert_eq!(
            Codec::from_str("xz").unwrap(),
            Codec::Xz(xz::XzSettings::default())
        );

        assert!(Codec::from_str("not a codec").is_err());
    }
}
