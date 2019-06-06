//! # Log module.
//!
//! This module provides the log facility.
use std::cmp::min;
use std::fs::create_dir_all;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use failure::{Error, ResultExt};
use slog::{Drain, Duplicate, Level, LevelFilter, Logger};
use slog_async::Async;
use slog_scope::{set_global_logger, GlobalLoggerGuard as Guard};
use slog_syslog::{unix_3164, Facility};
use slog_term::{FullFormat, PlainSyncDecorator, TermDecorator};

use crate::conf::Parameters;

/// Bare logger that only write to console.
#[must_use = "logger guard must be keep as reference or else all messages will be discarded"]
pub fn bootstrap() -> Guard {
    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();

    set_global_logger(Logger::root(drain, o!()))
}

/// Full featured logger.
/// Send log to console and log file, also handle log level.
#[must_use = "logger guard must be keep as reference or else all messages will be discarded"]
pub fn initialize(verbose: usize, parameters: &Parameters) -> Result<Guard, Error> {
    // Ensure log directory is present
    if let Some(log_path) = Path::new(&parameters.log_file).parent() {
        create_dir_all(log_path).with_context(|err| {
            format!(
                "could not create directory '{}', {}",
                log_path.display(),
                err
            )
        })?
    }

    // Stdout drain
    let term_decorator = TermDecorator::new().build();
    let term_drain = FullFormat::new(term_decorator).build().fuse();
    let term_drain = Async::new(term_drain).build().fuse();

    // File drain
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o640)
        .open(&parameters.log_file)
        .with_context(|err| format!("could not create file '{}', {}", parameters.log_file, err))?;

    let decorator = PlainSyncDecorator::new(log_file);
    let file_drain = FullFormat::new(decorator).build().fuse();
    let file_drain = Async::new(file_drain).build().fuse();

    // increase console log level if needed. Cap to trace
    let console_level = parameters.log_level + verbose;
    let console_level = min(console_level, Level::Trace.as_usize());
    let console_level = Level::from_usize(console_level).unwrap_or_else(|| Level::Trace);

    if parameters.syslog {
        let syslog_drain = unix_3164(Facility::LOG_DAEMON)?;
        let drain = Duplicate::new(
            LevelFilter::new(
                syslog_drain,
                Level::from_usize(parameters.log_level).unwrap_or_else(|| Level::Info),
            ),
            Duplicate::new(
                LevelFilter::new(term_drain, console_level),
                LevelFilter::new(
                    file_drain,
                    Level::from_usize(parameters.log_level).unwrap_or_else(|| Level::Info),
                ),
            ),
        )
        .fuse();

        return Ok(set_global_logger(Logger::root(drain, o!())));
    }

    let drain = Duplicate::new(
        LevelFilter::new(term_drain, console_level),
        LevelFilter::new(
            file_drain,
            Level::from_usize(parameters.log_level).unwrap_or_else(|| Level::Info),
        ),
    )
    .fuse();

    Ok(set_global_logger(Logger::root(drain, o!())))
}
