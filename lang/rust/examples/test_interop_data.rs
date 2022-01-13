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

use avro_rs::Reader;
use std::ffi::OsStr;

fn main() -> anyhow::Result<()> {
    let data_dir = std::fs::read_dir("../../build/interop/data/")
        .expect("Unable to list the interop data directory");

    for entry in data_dir {
        let path = entry
            .expect("Unable to read the interop data directory's files")
            .path();

        if path.is_file() {
            let ext = path.extension().and_then(OsStr::to_str).unwrap();

            if ext == "avro" {
                // let file_name = path.file_name().unwrap();
                // let _codec = get_codec(&file_name)?;

                let content = std::fs::File::open(path)?;

                let reader = Reader::new(&content)?;
                for value in reader {
                    if let Err(e) = value {
                        eprintln!(
                            "There is a problem with reading of '{:?}', \n {:?}\n",
                            &content, e
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

// fn get_codec(file_name: &OsStr) -> Result<Codec, anyhow::Error> {
//     let file_name = file_name.to_str().unwrap();
//     println!("filename: {}", file_name);
//     if file_name.ends_with("-deflate.avro") {
//         Ok(Codec::Deflate)
//     } else if file_name.ends_with("-snappy.avro") {
//         Ok(Codec::Snappy)
//     } else if file_name.ends_with("-bzip2.avro") {
//         Ok(Codec::Bzip2)
//     } else if file_name.ends_with("-zstandard.avro") {
//         Ok(Codec::Zstd)
//     // } else if (file_name.ends_with("-xz.avro")) {
//     //     Ok(Codec::Xz)
//     } else if file_name.ends_with(".avro") {
//         Ok(Codec::Null)
//     } else {
//         Err(anyhow::anyhow!("Unable to determine the codec for {}", file_name))
//     }
// }
