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

use ctor::{ctor, dtor};

use ref_thread_local::ref_thread_local;

ref_thread_local! {
    // The unit tests run in parallel
    // We need to keep the log messages in a thread-local variable
    // and clear them after assertion
    pub(crate) static managed LOG_MESSAGES: Vec<String> = Vec::new();
}

pub mod logger;

#[ctor]
fn before_all() {
    // better stacktraces in tests
    color_backtrace::install();

    // enable logging in tests
    logger::install();
}

#[dtor]
fn after_all() {
    logger::clear_log_messages();
}

/// Does nothing. Just loads the crate.
/// Should be used in the integration tests, because they do not use [dev-dependencies]
/// and do not auto-load this crate.
pub fn init() {}
