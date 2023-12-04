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

use apache_avro::{types::Value, Codec, Reader, Schema, Writer};
use apache_avro_test_helper::TestResult;
use std::{
    fmt,
    fs::{DirEntry, File, ReadDir},
    io::BufReader,
    path::Path,
    slice::Iter,
};

const ROOT_DIRECTORY: &str = "../../../share/test/data/schemas";

#[test]
fn test_schema() -> TestResult {
    let directory: ReadDir = match std::fs::read_dir(ROOT_DIRECTORY) {
        Ok(root_folder) => root_folder,
        Err(err) => {
            log::warn!("Can't read the root folder: {err}");
            return Ok(());
        }
    };
    let mut result: Result<(), ErrorsDesc> = Ok(());
    for f in directory {
        let entry: DirEntry = match f {
            Ok(entry) => entry,
            Err(e) => core::panic!("Can't get file {}", e),
        };
        log::debug!("{:?}", entry.file_name());
        if let Ok(ft) = entry.file_type() {
            if ft.is_dir() {
                let sub_folder =
                    ROOT_DIRECTORY.to_owned() + "/" + entry.file_name().to_str().unwrap();

                let dir_result = test_folder(sub_folder.as_str());
                if let Err(ed) = dir_result {
                    result = match result {
                        Ok(()) => Err(ed),
                        Err(e) => Err(e.merge(&ed)),
                    }
                }
            }
        }
    }
    result?;

    Ok(())
}

#[derive(Debug)]
struct ErrorsDesc {
    details: Vec<String>,
}

impl ErrorsDesc {
    fn new(msg: &str) -> ErrorsDesc {
        ErrorsDesc {
            details: vec![msg.to_string()],
        }
    }

    fn add(&self, msg: &str) -> Self {
        let mut new_vec = self.details.clone();
        new_vec.push(msg.to_string());
        Self { details: new_vec }
    }

    fn merge(&self, err: &ErrorsDesc) -> Self {
        let mut new_vec = self.details.clone();
        err.details
            .iter()
            .for_each(|d: &String| new_vec.push(d.clone()));
        Self { details: new_vec }
    }
}

impl fmt::Display for ErrorsDesc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details.join("\n").as_str())
    }
}

fn test_folder(folder: &str) -> Result<(), ErrorsDesc> {
    let file_name = folder.to_owned() + "/schema.json";
    let content = std::fs::read_to_string(file_name).expect("Unable to find schema.json file");

    let schema: Schema = Schema::parse_str(content.as_str()).expect("Can't read schema");

    let data_file_name = folder.to_owned() + "/data.avro";
    let data_path: &Path = Path::new(data_file_name.as_str());
    let mut result = Ok(());
    if !data_path.exists() {
        log::error!("{}", format!("folder {folder} does not exist"));
        return Err(ErrorsDesc::new(
            format!("folder {folder} does not exist").as_str(),
        ));
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
            Reader::with_schema(&schema, &bytes[..]).expect("Can't read flushed vector");

        let mut records_iter: Iter<Value> = records.iter();
        for r2 in reader_bis {
            let record: Value = r2.expect("Error on reading");
            let original = records_iter.next().expect("Error, no next");
            if original != &record {
                result = match result {
                    Ok(_) => Result::Err(ErrorsDesc::new(
                        format!("Records are not equals for folder : {folder}").as_str(),
                    )),
                    Err(e) => {
                        Err(e.add(format!("Records are not equals for folder : {folder}").as_str()))
                    }
                }
            }
        }
    }
    result
}
