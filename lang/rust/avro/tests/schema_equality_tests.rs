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
    schema_equality::{set_schemata_equality_comparator, SpecificationEq},
    Schema,
};
use apache_avro_test_helper::{data::valid_examples, init, TestResult};

#[test]
/// 1. Given a string, parse it to get Avro schema "original".
/// 2. Serialize "original" to a string and parse that string to generate Avro schema "round trip".
/// 3. Ensure "original" and "round trip" schemas are equivalent.
fn test_equivalence_after_round_trip() -> TestResult {
    // This test would only pass if the equality comparator is uses the canonical form of the schema!
    assert!(set_schemata_equality_comparator(Box::new(SpecificationEq)).is_ok());

    init();
    for (raw_schema, _) in valid_examples().iter() {
        let original_schema = Schema::parse_str(raw_schema)?;
        let round_trip_schema = Schema::parse_str(original_schema.canonical_form().as_str())?;
        assert_eq!(original_schema, round_trip_schema);
    }
    Ok(())
}
