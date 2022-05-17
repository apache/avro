use crate::LOG_MESSAGES;
use lazy_static::lazy_static;
use log::{LevelFilter, Log, Metadata};
use ref_thread_local::RefThreadLocal;

struct TestLogger {
    delegate: env_logger::Logger,
}

impl Log for TestLogger {
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

pub fn assert_logged(expected_message: &str) {
    assert_eq!(LOG_MESSAGES.borrow_mut().pop().unwrap(), expected_message);
}

pub(crate) fn setup() {
    log::set_logger(&*TEST_LOGGER)
        .map(|_| log::set_max_level(LevelFilter::Trace))
        .map_err(|err| {
            eprintln!("========= Failed to set the custom logger: {}", err);
        })
        .unwrap();
}
