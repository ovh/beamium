//! # Beamium.
//!
//! Beamium scrap Prometheus endpoint and forward metrics to Warp10.
extern crate bytes;
extern crate cast;
extern crate clap;
extern crate core;
extern crate ctrlc;
extern crate flate2;
extern crate futures;
extern crate hyper;
extern crate hyper_timeout;
extern crate hyper_tls;
extern crate nix;
extern crate regex;
#[macro_use]
extern crate slog;
extern crate slog_async;
#[macro_use]
extern crate slog_scope;
extern crate slog_stream;
extern crate slog_syslog;
extern crate slog_term;
extern crate time;
extern crate tokio_core;
extern crate tokio_timer;
extern crate yaml_rust;

use clap::App;
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::fs;
use std::time::Duration;

mod config;
mod scraper;
mod router;
mod sink;
mod lib;
mod log;

include!("version.rs");

/// Main loop.
fn main() {
    // Setup a bare logger
    log::bootstrap();

    let matches = App::new("beamium")
        .version(&*format!("{} ({})", env!("CARGO_PKG_VERSION"), COMMIT))
        .author("d33d33 <kevin@d33d33.fr>")
        .about("Send Prometheus metrics to Warp10")
        .args_from_usage(
            "-c, --config=[FILE] 'Sets a custom config file'
                              \
                          -v...                'Increase verbosity level (console only)'
                          -t                   'Test config'",
        )
        .get_matches();

    // Bootstrap config
    let config_path = matches.value_of("config").unwrap_or("");
    let config = match config::load_config(&config_path) {
        Ok(config) => config,
        Err(err) => {
            crit!("Fail to load config {}: {}", &config_path, err);
            std::process::abort();
        }
    };

    if matches.is_present("t") {
        info!("config ok");
        std::process::exit(0);
    }

    info!("starting");

    // Setup logging
    match log::log(&config.parameters, matches.occurrences_of("v")) {
        Ok(()) => {}
        Err(err) => {
            crit!("Log setup failure: {}", err);
            std::process::abort();
        }
    }

    // Ensure dirs
    match fs::create_dir_all(&config.parameters.source_dir) {
        Ok(()) => {}
        Err(err) => {
            crit!(
                "Fail to create source directory {}: {}",
                &config.parameters.source_dir,
                err
            );
            std::process::abort();
        }
    };
    match fs::create_dir_all(&config.parameters.sink_dir) {
        Ok(()) => {}
        Err(err) => {
            crit!(
                "Fail to create sink directory {}: {}",
                &config.parameters.source_dir,
                err
            );
            std::process::abort();
        }
    };

    // Synchronisation stuff
    let sigint = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(config.scrapers.len() + 1);

    // Sigint handling
    let r = sigint.clone();
    ctrlc::set_handler(move || {
        r.store(true, Ordering::SeqCst);
    }).expect("Error setting sigint handler");

    // Spawn scrapers
    info!("spawning scrapers");
    for scraper in config.scrapers.clone() {
        let (parameters, sigint) = (config.parameters.clone(), sigint.clone());
        handles.push(thread::spawn(move || {
            slog_scope::scope(
                &slog_scope::logger().new(o!("scraper" => scraper.name.clone())),
                || scraper::scraper(&scraper, &parameters, sigint),
            );
        }));
    }

    // Spawn router
    info!("spawning router");
    {
        let (sinks, labels, parameters, sigint) = (
            config.sinks.clone(),
            config.labels.clone(),
            config.parameters.clone(),
            sigint.clone(),
        );
        handles.push(thread::spawn(move || {
            slog_scope::scope(&slog_scope::logger().new(o!()), || {
                router::router(&sinks, &labels, &parameters, sigint)
            });
        }));
    }

    // Spawn sinks
    info!("spawning sinks");
    let mut sinks: Vec<sink::Sink> = config
        .sinks
        .iter()
        .map(|sink| sink::Sink::new(&sink, &config.parameters))
        .collect();

    sinks.iter_mut().for_each(|s| s.start());

    info!("started");

    // Wait for sigint
    loop {
        thread::sleep(Duration::from_millis(10));

        if sigint.load(Ordering::Relaxed) {
            break;
        }
    }

    info!("shutting down");
    for handle in handles {
        handle.join().unwrap();
    }

    for s in sinks {
        s.stop();
    }
    info!("halted");
}
