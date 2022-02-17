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
use std::{collections::HashMap, ffi::OsStr, fs::File};

fn main() -> anyhow::Result<()> {
    let expected_user_metadata: HashMap<String, Vec<u8>> = create_expected_user_metadata();

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
                let reader = Reader::new(&content)?;

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

fn create_expected_user_metadata() -> HashMap<String, Vec<u8>> {
    let mut user_metadata: HashMap<String, Vec<u8>> = HashMap::new();
    user_metadata.insert(
        "stringKey".to_string(),
        "stringValue".to_string().into_bytes(),
    );
    user_metadata.insert("strKey".to_string(), "strValue".as_bytes().to_vec());
    user_metadata.insert("bytesKey".to_string(), b"bytesValue".to_vec());
    user_metadata.insert("vecKey".to_string(), vec![1, 2, 3]);
    user_metadata
}

fn test_user_metadata(reader: &Reader<&File>, expected_user_metadata: &HashMap<String, Vec<u8>>) {
    let user_metadata = reader.user_metadata();
    if !user_metadata.is_empty() {
        assert_eq!(user_metadata, expected_user_metadata);
    }
}
