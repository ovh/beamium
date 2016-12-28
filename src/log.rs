//! # Log module.
//!
//! The Config module provides the log facility.
use slog::*;
use slog_stream;
use slog_term;
use slog_json;
use slog_scope;
use std::fs::OpenOptions;
use std;

use config;

/// Bare logger that only write to console.
pub fn bootstrap() {
    slog_scope::set_global_logger(Logger::root(slog_term::streamer().full().build().ignore_err(),
                                               o![]));
}

/// Full featured logger.
/// Send log to console and log file, also handle log level.
pub fn log(parameters: &config::Parameters, console_level: Level) {
    // Stdout drain
    let drain_term = slog_term::streamer().full().build().ignore_err();

    // File drain
    let log_file = OpenOptions::new().create(true).append(true).open(&parameters.log_file);
    if log_file.is_err() {
        crit!("Fail to open log file at {:?}: {}",
              &parameters.log_file,
              log_file.err().unwrap());
        std::process::exit(-1);
    }
    let file_drain = slog_stream::async_stream(log_file.unwrap(), slog_json::default())
        .ignore_err();

    // Setup root logger
    let root_log = Logger::root(Duplicate::new(LevelFilter::new(drain_term, console_level),
                                               LevelFilter::new(file_drain, parameters.log_level))
                                    .ignore_err(),
                                o!());

    slog_scope::set_global_logger(root_log);
}
