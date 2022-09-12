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

use apache_avro::types::Value;
use apache_avro::{Codec, Reader, Schema, Writer};
use std::fs::{DirEntry, File, ReadDir};
use std::io::{BufReader, Error};
use std::path::Path;
use std::slice::Iter;

const ROOT_DIRECTORY: &str = "../../../share/test/data/schemas";

#[test]
fn test_schema() {
    let directory: ReadDir = scan_shared_folder();
    directory.for_each(|f: Result<DirEntry, Error>| {
        let e: DirEntry = match f {
            Err(error) => panic!("Can't get file {:?}", error.to_string()),
            Ok(entry) => entry,
        };
        log::warn!("{:?}", e.file_name());
        let sub_folder = ROOT_DIRECTORY.to_owned() + "/" + e.file_name().to_str().unwrap();

        test_folder(sub_folder.as_str());
    });
}

fn test_folder(folder: &str) {
    let file_name = folder.to_owned() + "/schema.json";
    let content = std::fs::read_to_string(file_name).expect("Unable to find schema.jon file");

    let schema: Schema = Schema::parse_str(content.as_str()).expect("Can't read schema");

    let data_file_name = folder.to_owned() + "/data.avro";
    let data_path: &Path = Path::new(data_file_name.as_str());
    if !data_path.exists() {
        log::warn!("data file does not exist");
    } else {
        let file: File = File::open(data_path).expect("Can't open data.avro");
        let reader =
            Reader::with_schema(&schema, BufReader::new(&file)).expect("Can't read data.avro");

        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Null);

        let mut records: Vec<Value> = vec![];

        for r in reader {
            let record: Value = r.expect("Error on reading");
            writer.append(record.clone()).expect("Error on write item");
            records.push(record);
        }

        writer.flush().expect("Error on flush");
        let bytes: Vec<u8> = writer.into_inner().unwrap();
        let reader_bis =
            Reader::with_schema(&schema, &bytes[..]).expect("Can't read fflushed vector");

        let mut records_iter: Iter<Value> = records.iter();
        for r2 in reader_bis {
            let record: Value = r2.expect("Error on reading");
            let original = records_iter.next().expect("Error, no next");
            assert_eq!(*original, record);
        }
    }
}

fn scan_shared_folder() -> ReadDir {
    std::fs::read_dir(ROOT_DIRECTORY).expect("Can't read root folder")
}
