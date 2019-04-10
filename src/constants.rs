//! # Constants module.
//!
//! The Constants module provides beamium's constants.
use std::time::Duration;

/// Keep only files that have the following extension
pub(crate) const EXTENSION: &str = "metrics";

/// Time to sleep for thread in waiting to achieve an action
pub(crate) const THREAD_SLEEP: Duration = Duration::from_millis(100);

/// Size of a chunk to send
pub(crate) const CHUNK_SIZE: u64 = 1024 * 1024;

/// Number of threads used by hyper to resolve dns request
pub(crate) const NUMBER_DNS_WORKER_THREADS: usize = 4;

/// Number of handlers per tokio reactor
pub(crate) const MAX_HANDLERS_PER_REACTOR: usize = 20;

/// Warn if backoff is greater that this 1 second
pub(crate) const BACKOFF_WARN: Duration = Duration::from_millis(1_000);

/// Keep alive duration of threads in tokio runtime
pub(crate) const KEEP_ALIVE_TOKIO_RUNTIME: Duration = Duration::from_millis(5_000);
