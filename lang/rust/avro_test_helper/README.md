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


# Avro Test Helper

A module that provides several test related goodies to the other Avro crates:

### Custom Logger

The logger both collects the logged messages and delegates to env_logger so that they printed on the stderr

### Colorized Backtraces

Uses `color-backtrace` to make the backtraces easier to read.

# Setup

### Unit tests

The module is automatically setup for all unit tests when this crate is listed as a `[dev-dependency]` in Cargo.toml.

### Integration tests

Since integration tests are actually crates without Cargo.toml, the test author needs to call `test_logger::init()` in the beginning of a test.

# Usage

To assert that a given message was logged, use the `assert_logged` function.
```rust
assert_logged("An expected message");
```
