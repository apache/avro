<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->


# avro_derive

A proc-macro module for automatically deriving the avro schema for structs or enums. The macro produces the logic necessary to implement the `AvroSchema` trait for the type.

```rust
pub trait AvroSchema {
    // constructs the schema for the type
    fn get_schema() -> Schema;
}
```
## How-to use
Add the "derive" feature to your apache-avro dependency inside cargo.toml
```
apache-avro = { version = "X.Y.Z", features = ["derive"] }
```

Add to your data model
```rust
#[derive(AvroSchema)]
struct Test {
    a: i64,
    b: String,
}
```


### Example
```rust
use apache_avro::Writer;

#[derive(Debug, Serialize, AvroSchema)]
struct Test {
    a: i64,
    b: String,
}
// derived schema, always valid or code fails to compile with a descriptive message
let schema = Test::get_schema();

let mut writer = Writer::new(&schema, Vec::new());
let test = Test {
    a: 27,
    b: "foo".to_owned(),
};
writer.append_ser(test).unwrap();
let encoded = writer.into_inner();
```

### Compatibility Notes
This module is designed to work in concert with the Serde implementation. If your use case dictates needing to manually convert to a `Value` type in order to encode then the derived schema may not be correct.
