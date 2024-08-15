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

use apache_avro::{
    types::{Record, Value},
    Codec, Reader, Schema, Writer,
};
use apache_avro_test_helper::TestResult;

#[test]
fn avro_4032_null_codec_settings() -> TestResult {
    avro_4032_codec_settings(Codec::Null)
}
#[test]
fn avro_4032_deflate_codec_settings() -> TestResult {
    avro_4032_codec_settings(Codec::Deflate)
}

#[test]
#[cfg(feature = "bzip")]
fn avro_4032_bzip_codec_settings() -> TestResult {
    use apache_avro::Bzip2Settings;
    use bzip2::Compression;
    let codec = Codec::Bzip2(Bzip2Settings::new(Compression::fast().level() as u8));
    avro_4032_codec_settings(codec)
}

#[test]
#[cfg(feature = "xz")]
fn avro_4032_xz_codec_settings() -> TestResult {
    use apache_avro::XzSettings;
    let codec = Codec::Xz(XzSettings::new(8));
    avro_4032_codec_settings(codec)
}

#[test]
#[cfg(feature = "zstandard")]
fn avro_4032_zstandard_codec_settings() -> TestResult {
    use apache_avro::ZstandardSettings;
    let compression_level = 13;
    let codec = Codec::Zstandard(ZstandardSettings::new(compression_level));
    avro_4032_codec_settings(codec)
}

fn avro_4032_codec_settings(codec: Codec) -> TestResult {
    let schema = Schema::parse_str(
        r#"
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "f1", "type": "int"},
                {"name": "f2", "type": "string"}
            ]
        }"#,
    )?;

    let mut writer = Writer::with_codec(&schema, Vec::new(), codec);
    let mut record = Record::new(writer.schema()).unwrap();
    record.put("f1", 27_i32);
    record.put("f2", "foo");
    writer.append(record)?;
    let input = writer.into_inner()?;
    let mut reader = Reader::new(&input[..])?;
    assert_eq!(
        reader.next().unwrap()?,
        Value::Record(vec![
            ("f1".to_string(), Value::Int(27)),
            ("f2".to_string(), Value::String("foo".to_string())),
        ])
    );
    assert!(reader.next().is_none());

    Ok(())
}
