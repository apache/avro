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
use log::{LevelFilter, Log, Metadata};
use std::sync::OnceLock;

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
            LOG_MESSAGES.with(|msgs| msgs.borrow_mut().push(format!("{}", record.args())));

            self.delegate.log(record);
        }
    }

    fn flush(&self) {}
}

fn test_logger() -> &'static TestLogger {
    // Lazy static because the Logger has to be 'static
    static TEST_LOGGER_ONCE: OnceLock<TestLogger> = OnceLock::new();
    TEST_LOGGER_ONCE.get_or_init(|| TestLogger {
        delegate: env_logger::Builder::from_default_env()
            .filter_level(LevelFilter::Off)
            .parse_default_env()
            .build(),
    })
}

pub fn clear_log_messages() {
    LOG_MESSAGES.with(|msgs| match msgs.try_borrow_mut() {
        Ok(mut log_messages) => log_messages.clear(),
        Err(err) => panic!("Failed to clear log messages: {err:?}"),
    });
}

pub fn assert_not_logged(unexpected_message: &str) {
    LOG_MESSAGES.with(|msgs| match msgs.borrow().last() {
        Some(last_log) if last_log == unexpected_message => {
            panic!("The following log message should not have been logged: '{unexpected_message}'")
        }
        _ => (),
    });
}

pub fn assert_logged(expected_message: &str) {
    let mut deleted = false;
    LOG_MESSAGES.with(|msgs| {
        msgs.borrow_mut().retain(|msg| {
            if msg == expected_message {
                deleted = true;
                false
            } else {
                true
            }
        })
    });

    if !deleted {
        panic!("Expected log message has not been logged: '{expected_message}'");
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn install() {
    log::set_logger(test_logger())
        .map(|_| log::set_max_level(LevelFilter::Trace))
        .map_err(|err| {
            eprintln!("Failed to set the custom logger: {err:?}");
        })
        .unwrap();
}
