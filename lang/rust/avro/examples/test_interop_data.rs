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

use apache_avro::Reader;
use std::{
    collections::HashMap,
    ffi::OsStr,
    io::{BufReader, Read},
};

fn main() -> anyhow::Result<()> {
    let mut expected_user_metadata: HashMap<String, Vec<u8>> = HashMap::new();
    expected_user_metadata.insert("user_metadata".to_string(), b"someByteArray".to_vec());

    let data_dir = std::fs::read_dir("../../build/interop/data/")
        .expect("Unable to list the interop data directory");

    let mut errors = Vec::new();

    for entry in data_dir {
        let path = entry
            .expect("Unable to read the interop data directory's files")
            .path();

        if path.is_file() {
            let ext = path.extension().and_then(OsStr::to_str).unwrap();

            if ext == "avro" {
                println!("Checking {:?}", &path);
                let content = std::fs::File::open(&path)?;
                let reader = Reader::new(BufReader::new(&content))?;

                test_user_metadata(&reader, &expected_user_metadata);

                for value in reader {
                    if let Err(e) = value {
                        errors.push(format!(
                            "There is a problem with reading of '{:?}', \n {:?}\n",
                            &path, e
                        ));
                    }
                }
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        panic!(
            "There were errors reading some .avro files:\n{}",
            errors.join(", ")
        );
    }
}

fn test_user_metadata<R: Read>(
    reader: &Reader<BufReader<R>>,
    expected_user_metadata: &HashMap<String, Vec<u8>>,
) {
    let user_metadata = reader.user_metadata();
    if !user_metadata.is_empty() {
        assert_eq!(user_metadata, expected_user_metadata);
    }
}
