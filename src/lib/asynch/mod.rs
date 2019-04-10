//! # async module
//!
//! The `async` module provide asynchronous helpers.
pub mod fs;
pub mod http;

/// The `try_future` macro provide an elegant way to manage errors in future.
#[macro_export]
macro_rules! try_future {
    ($x: expr) => {
        match $x {
            Ok(item) => item,
            Err(err) => {
                return futures::future::err(failure::format_err!("{}", err));
            }
        }
    };
}

/// The `arc` macro provide an easy way to wrap standard `Arc`
#[macro_export]
macro_rules! arc {
    ($x: expr) => {
        std::sync::Arc::new($x)
    };
}

/// The `mutex` macro provide an easy way to wrap standard `Arc<Mutex<...>>`
#[macro_export]
macro_rules! mutex {
    ($x: expr) => {
        std::sync::Arc::new(std::sync::Mutex::new($x))
    };
}

/// The `poll_ready` macro provide a shortcut to the `Poll` alias
#[macro_export]
macro_rules! poll_not_ready {
    () => {
        Ok(futures::Async::NotReady)
    };
}

/// The `poll_ready` macro provide a shortcut to the `Poll` alias
#[macro_export]
macro_rules! poll_ready {
    ($x: expr) => {
        Ok(futures::Async::Ready($x))
    };
}
