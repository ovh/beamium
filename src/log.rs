//! # Log module.
//!
//! The Config module provides the log facility.
use slog::Logger;
use slog::Level;
use slog::Duplicate;
use slog::LevelFilter;
use slog::Drain;
use slog_async;
use slog_term;
use slog_syslog;
use slog_syslog::Facility;
use slog_scope;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std;
use std::path::Path;
use std::error::Error;
use std::fs;

use config;

/// Bare logger that only write to console.
pub fn bootstrap() {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    slog_scope::set_global_logger(Logger::root(drain, o!())).cancel_reset();
}

/// Full featured logger.
/// Send log to console and log file, also handle log level.
pub fn log(parameters: &config::Parameters, verbose: u64) -> Result<(), Box<Error>> {
    // Ensure log directory is present
    match Path::new(&parameters.log_file).parent() {
        Some(log_path) => fs::create_dir_all(log_path)?,
        None => {}
    };

    // Stdout drain
    let term_decorator = slog_term::TermDecorator::new().build();
    let term_drain = slog_term::FullFormat::new(term_decorator).build().fuse();
    let term_drain = slog_async::Async::new(term_drain).build().fuse();

    // File drain
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o640)
        .open(&parameters.log_file);
    if log_file.is_err() {
        crit!(
            "Fail to open log file at {:?}: {}",
            &parameters.log_file,
            log_file.err().unwrap()
        );
        std::process::exit(-1);
    }

    let decorator = slog_term::PlainSyncDecorator::new(log_file.unwrap());
    let file_drain = slog_term::FullFormat::new(decorator).build().fuse();
    let file_drain = slog_async::Async::new(file_drain).build().fuse();

    // increase console log level if needed. Cap to trace
    let console_level = parameters.log_level.as_usize() as u64 + verbose;
    let console_level = std::cmp::min(console_level, Level::Trace.as_usize() as u64);
    let console_level = Level::from_usize(console_level as usize).unwrap_or(Level::Trace);

    if parameters.syslog {
        let syslog_drain = slog_syslog::unix_3164(Facility::LOG_DAEMON).unwrap();

        let drain = Duplicate::new(
            LevelFilter::new(syslog_drain, parameters.log_level),
            Duplicate::new(
                LevelFilter::new(term_drain, console_level),
                LevelFilter::new(file_drain, parameters.log_level),
            ),
        ).fuse();

        slog_scope::set_global_logger(Logger::root(drain, o!())).cancel_reset();
    } else {
        let drain = Duplicate::new(
            LevelFilter::new(term_drain, console_level),
            LevelFilter::new(file_drain, parameters.log_level),
        ).fuse();

        slog_scope::set_global_logger(Logger::root(drain, o!())).cancel_reset();
    }

    Ok(())
}
