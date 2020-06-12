//! # Beamium.
//!
//! Beamium scrap Prometheus endpoint and forward metrics to Warp10.
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate slog_scope;

use std::convert::TryFrom;
use std::process::abort;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use failure::{format_err, Error};

use crate::cmd::{version, Opts};
use crate::conf::Conf;
use crate::version::PROFILE;

#[macro_use]
pub(crate) mod lib;
pub(crate) mod cmd;
pub(crate) mod conf;
pub(crate) mod constants;
pub(crate) mod log;
pub(crate) mod router;
pub(crate) mod scraper;
pub(crate) mod sink;
pub(crate) mod version;

#[paw::main]
fn main(opts: Opts) -> Result<(), Error> {
    let _guard = log::bootstrap();

    // -------------------------------------------------------------------------
    // check if beamium is compiled in the 'release' profile
    if PROFILE != "release" {
        warn!(
            "{} is running in '{}' mode",
            env!("CARGO_PKG_NAME"),
            PROFILE
        );
    }

    // -------------------------------------------------------------------------
    // Display version
    if opts.version {
        return version();
    }

    // -------------------------------------------------------------------------
    // Manage termination signals from the system
    let sigint = arc!(AtomicBool::new(false));
    let tx = sigint.to_owned();
    let result = ctrlc::set_handler(move || {
        tx.store(true, Ordering::SeqCst);
    });

    if let Err(err) = result {
        crit!("could not set handler on signal int"; "error" => err.to_string());
        return Err(format_err!("{}", err));
    }

    // -------------------------------------------------------------------------
    // Retrieve configuration
    let result = match opts.config {
        Some(ref path) => Conf::try_from(path),
        None => Conf::default(),
    };

    let conf = match result {
        Ok(conf) => conf,
        Err(err) => {
            crit!("configuration is not healthy"; "error" => err.to_string());
            return Err(format_err!("{}", err));
        }
    };

    // We need to keep a reference of _tx and _watcher as they implement Drop trait they will stop to watch files
    // and closed the communication channel
    let (_tx, watcher_rx, _watcher) = match Conf::watch(opts.config.to_owned()) {
        Ok(watcher) => watcher,
        Err(err) => {
            crit!("could not watch configuration"; "error" => err.to_string());
            return Err(format_err!("{}", err));
        }
    };

    // -------------------------------------------------------------------------
    // Quit if it is only for check configuration
    if opts.check {
        // 0  is info level
        // 1  is debug level
        // 2+ is trace level
        if opts.verbose >= 1 {
            debug!("{:#?}", conf);
        }

        info!("configuration is healthy");
        return Ok(());
    }

    // -------------------------------------------------------------------------
    // Initialize full featured logger and keep a reference of the guard
    let _guard = match log::initialize(opts.verbose, &conf.parameters) {
        Ok(guard) => guard,
        Err(err) => {
            crit!("could not instantiate full featured logger"; "error" => err.to_string());
            return Err(format_err!("{}", err));
        }
    };

    // -------------------------------------------------------------------------
    // Start beamium scraper, sinks and metrics
    let signal = arc!(AtomicBool::new(true));
    let rx = signal.to_owned();
    let mut handler = thread::spawn(move || {
        if let Err(err) = cmd::main(conf, rx) {
            crit!("{}", err);
            thread::sleep(Duration::from_millis(100)); // Sleep the time to display the message
            abort();
        }
    });

    // Wait for termination signals
    loop {
        if sigint.load(Ordering::SeqCst) {
            signal.store(false, Ordering::SeqCst);
            if handler.join().is_err() {
                crit!("could not stop main thread");
            }

            break;
        }

        if let Ok(event) = watcher_rx.try_recv() {
            debug!("received a watcher event {:#?}", event);
            info!("reload configuration");
            signal.store(false, Ordering::SeqCst);
            if handler.join().is_err() {
                crit!("could not stop main thread");
                break;
            }

            let path = opts.config.to_owned();
            let tx = signal.to_owned();

            handler = thread::spawn(move || {
                let result = match path {
                    Some(ref path) => Conf::try_from(path),
                    None => Conf::default(),
                };

                let conf = match result {
                    Ok(conf) => conf,
                    Err(err) => {
                        crit!("configuration is not healthy"; "error" => err.to_string());
                        thread::sleep(Duration::from_millis(100)); // Sleep the time to display the message
                        abort();
                    }
                };

                tx.store(true, Ordering::SeqCst);
                if let Err(err) = cmd::main(conf, tx) {
                    crit!("{}", err);
                    thread::sleep(Duration::from_millis(100)); // Sleep the time to display the message
                    abort();
                }
            });
        }
    }

    info!("Beamium halted!");
    Ok(())
}
