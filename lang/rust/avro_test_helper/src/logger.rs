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

use crate::LOG_MESSAGES;
use lazy_static::lazy_static;
use log::{LevelFilter, Log, Metadata};
use ref_thread_local::RefThreadLocal;

struct TestLogger {
    delegate: env_logger::Logger,
}

impl Log for TestLogger {
    #[inline]
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            LOG_MESSAGES.borrow_mut().push(format!("{}", record.args()));

            self.delegate.log(record);
        }
    }

    fn flush(&self) {}
}

lazy_static! {
    // Lazy static because the Logger has to be 'static
    static ref TEST_LOGGER: TestLogger = TestLogger {
        delegate: env_logger::Builder::from_default_env()
            .filter_level(LevelFilter::Off)
            .parse_default_env()
            .build(),
    };
}

pub fn clear_log_messages() {
    LOG_MESSAGES.borrow_mut().clear();
}

pub fn assert_not_logged(unexpected_message: &str) {
    match LOG_MESSAGES.borrow().last() {
        Some(last_log) if last_log == unexpected_message => panic!(
            "The following log message should not have been logged: '{}'",
            unexpected_message
        ),
        _ => (),
    }
}

pub fn assert_logged(expected_message: &str) {
    assert_eq!(LOG_MESSAGES.borrow_mut().pop().unwrap(), expected_message);
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn install() {
    log::set_logger(&*TEST_LOGGER)
        .map(|_| log::set_max_level(LevelFilter::Trace))
        .map_err(|err| {
            eprintln!("Failed to set the custom logger: {:?}", err);
        })
        .unwrap();
}
