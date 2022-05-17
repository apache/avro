use ctor::{ctor, dtor};

use ref_thread_local::ref_thread_local;

ref_thread_local! {
    // The unit tests run in parallel
    // We need to keep the log messages in a thread-local variable
    // and clear them after assertion
    pub static managed LOG_MESSAGES: Vec<String> = Vec::new();
}

pub mod logger;

#[ctor]
fn setup() {
    // better stacktraces in tests
    color_backtrace::install();

    // enable logging in tests
    logger::setup();
}

#[dtor]
fn teardown() {
    logger::clear_log_messages();
}

/// Does nothing. Just loads the crate.
/// Should be used in the integration tests, because they do not use [dev-dependencies]
/// and do not auto-load this crate.
pub const fn init() {}
