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

#[cfg(not(target_arch = "wasm32"))]
use ctor::{ctor, dtor};
use std::cell::RefCell;

thread_local! {
    // The unit tests run in parallel
    // We need to keep the log messages in a thread-local variable
    // and clear them after assertion
    pub(crate) static LOG_MESSAGES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

pub mod logger;

#[cfg(not(target_arch = "wasm32"))]
#[ctor]
fn before_all() {
    // better stacktraces in tests
    better_panic::Settings::new()
        .most_recent_first(true)
        .lineno_suffix(false)
        .backtrace_first(true)
        .install();

    // enable logging in tests
    logger::install();
}

#[cfg(not(target_arch = "wasm32"))]
#[dtor]
fn after_all() {
    logger::clear_log_messages();
}

/// A custom error type for tests.
#[derive(Debug)]
pub enum TestError {}

/// A converter of any error into [TestError].
/// It is used to print better error messages in the tests.
/// Borrowed from <https://bluxte.net/musings/2023/01/08/improving_failure_messages_rust_tests/>
impl<Err: std::fmt::Display> From<Err> for TestError {
    #[track_caller]
    fn from(err: Err) -> Self {
        panic!("{}: {}", std::any::type_name::<Err>(), err);
    }
}

pub type TestResult = anyhow::Result<(), TestError>;

/// Does nothing. Just loads the crate.
/// Should be used in the integration tests, because they do not use [dev-dependencies]
/// and do not auto-load this crate.
pub fn init() {}
