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
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;

use ctrlc;
use failure::{format_err, Error};
use prometheus::{gather, Encoder, TextEncoder};
use structopt::StructOpt;
use tokio::prelude::*;
use tokio::runtime::Builder;
use warp::{path, serve, Filter};

use crate::conf::Conf;
use crate::constants::{KEEP_ALIVE_TOKIO_RUNTIME, MAX_HANDLERS_PER_REACTOR, THREAD_SLEEP};
use crate::lib::{Named, Runner};
use crate::router::Router;
use crate::scraper::Scraper;
use crate::sink::Sink;
use crate::version::{BUILD_DATE, GITHASH, PROFILE};

pub(crate) mod conf;
#[macro_use]
pub(crate) mod lib;
pub(crate) mod constants;
pub(crate) mod log;
pub(crate) mod router;
pub(crate) mod scraper;
pub(crate) mod sink;
pub(crate) mod version;

#[derive(StructOpt, Clone, Debug)]
struct Opts {
    /// Prints version information
    #[structopt(short = "V", long = "version")]
    version: bool,

    /// Increase verbosity level (console only)
    #[structopt(short = "v", parse(from_occurrences))]
    verbose: usize,

    /// Sets a custom config file
    #[structopt(short = "c", long = "config", parse(from_os_str))]
    config: Option<PathBuf>,

    /// Test configuration
    #[structopt(short = "t", long = "check")]
    check: bool,
}

#[paw::main]
fn main(opts: Opts) -> Result<(), Error> {
    let _guard = log::bootstrap();

    // Display version
    if opts.version {
        let mut version = String::new();

        version += &format!(
            "{} version {} {}\n",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
            GITHASH
        );
        version += &format!("{} build date {}\n", env!("CARGO_PKG_NAME"), BUILD_DATE);
        version += &format!("{} profile {}\n", env!("CARGO_PKG_NAME"), PROFILE);

        println!("{}", version);
        return Ok(());
    }

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

    if PROFILE != "release" {
        warn!(
            "{} is running in '{}' mode",
            env!("CARGO_PKG_NAME"),
            PROFILE
        );
    }

    // Initialize full featured logger and keep a reference of the guard
    let _guard = match log::initialize(opts.verbose, &conf.parameters) {
        Ok(guard) => guard,
        Err(err) => {
            crit!("could not instantiate full featured logger"; "error" => err.to_string());
            return Err(format_err!("{}", err));
        }
    };

    // Ensure that directories are presents
    if let Err(err) = create_dir_all(conf.parameters.source_dir.to_owned()) {
        crit!("could not create source directory"; "error" => err.to_string());
        return Err(format_err!("{}", err));
    }

    if let Err(err) = create_dir_all(conf.parameters.sink_dir.to_owned()) {
        crit!("could not create sink directory"; "error" => err.to_string());
        return Err(format_err!("{}", err));
    }

    // Manage termination signals from the system
    let sigint = arc!(AtomicBool::new(true));
    let tx = sigint.to_owned();
    let result = ctrlc::set_handler(move || {
        tx.store(false, Ordering::SeqCst);
    });

    if let Err(err) = result {
        crit!("could not set handler on signal int"; "error" => err.to_string());
        return Err(format_err!("{}", err));
    }

    // Create metrics http server
    let mut metrics_rt = None;

    if let Some(addr) = conf.parameters.metrics {
        let mut rt = Builder::new()
            .keep_alive(Some(KEEP_ALIVE_TOKIO_RUNTIME))
            .core_threads(1)
            .blocking_threads(1)
            .name_prefix("metrics-")
            .build()?;

        let router = path!("metrics").map(|| {
            let encoder = TextEncoder::new();
            let metric_families = gather();

            let mut buffer = vec![];
            if let Err(err) = encoder.encode(&metric_families, &mut buffer) {
                error!("could not encode prometheus metrics"; "error" => err.to_string());
            }

            buffer
        });

        info!("start metrics http server"; "uri" => format!("http://{}/metrics", addr));
        rt.spawn(serve(router).bind(addr));
        metrics_rt = Some(rt);
    }

    // Create scrapers and associated runtimes
    let mut scrapers = vec![];
    for scraper in conf.scrapers.to_owned() {
        debug!("create scraper and associated runtime"; "scraper" => scraper.name.as_str());
        let result = Builder::new()
            .keep_alive(Some(KEEP_ALIVE_TOKIO_RUNTIME))
            .core_threads(scraper.pool + 1)
            .blocking_threads(conf.parameters.filesystem_threads)
            .name_prefix(format!("{}-", scraper.name.as_str()))
            .build();

        let scraper = Scraper::from((scraper.to_owned(), conf.parameters.to_owned()));
        let mut rt = match result {
            Ok(rt) => rt,
            Err(err) => {
                crit!("could not build the runtime for scraper";  "scraper" => scraper.name(), "error" => err.to_string());
                return Err(format_err!("{}", err));
            }
        };

        if let Err(err) = scraper.start(&mut rt) {
            crit!("could not start scraper"; "scraper" => scraper.name(), "error" => err.to_string());
            return Err(format_err!("{}", err));
        }

        scrapers.push((scraper, rt));
    }

    // Create router and associated runtime
    let result = Builder::new()
        .keep_alive(Some(KEEP_ALIVE_TOKIO_RUNTIME))
        .core_threads(conf.parameters.router_parallel)
        .blocking_threads(conf.parameters.filesystem_threads)
        .name_prefix("router-")
        .build();

    let mut rt = match result {
        Ok(rt) => rt,
        Err(err) => {
            crit!("could not build the runtime for router"; "error" => err.to_string());
            return Err(format_err!("{}", err));
        }
    };

    let router = Router::from((
        conf.parameters.to_owned(),
        conf.labels,
        conf.sinks.to_owned(),
    ));

    if let Err(err) = router.start(&mut rt) {
        crit!("could not start the router"; "error" => err.to_string());
        return Err(format_err!("{}", err));
    }

    let router = (router, rt);

    // Create sinks and associated runtimes
    let mut sinks = vec![];
    for sink in conf.sinks {
        debug!("create sink and associated runtime"; "sink" => sink.name.to_owned());
        let result = Builder::new()
            .keep_alive(Some(KEEP_ALIVE_TOKIO_RUNTIME))
            .core_threads(
                (sink.parallel as f64 / MAX_HANDLERS_PER_REACTOR as f64).ceil() as usize + 1,
            )
            .blocking_threads(conf.parameters.filesystem_threads)
            .name_prefix(format!("{}-", sink.name))
            .build();

        let sink = Sink::from((sink.to_owned(), conf.parameters.to_owned()));
        let mut rt = match result {
            Ok(rt) => rt,
            Err(err) => {
                crit!("could not build the runtime for sink";  "sink" => sink.name(), "error" => err.to_string());
                return Err(format_err!("{}", err));
            }
        };

        if let Err(err) = sink.start(&mut rt) {
            crit!("could not start sink"; "sink" => sink.name(), "error" => err.to_string());
            return Err(format_err!("{}", err));
        }

        sinks.push((sink, rt));
    }

    // Wait for termination signals
    let rx = sigint.to_owned();
    while rx.load(Ordering::SeqCst) {
        sleep(THREAD_SLEEP);
    }

    // Shutdown runtime for each scrapers
    for (scraper, rt) in scrapers {
        debug!("shutdown scraper's runtime"; "scraper" => scraper.name());
        if rt.shutdown_now().wait().is_err() {
            error!("could not shutdown the runtime"; "scraper" => scraper.name());
        }
    }

    // Shutdown router runtime
    debug!("shutdown router's runtime");
    if router.1.shutdown_now().wait().is_err() {
        error!("could not shutdown the router's runtime");
    }

    // Shutdown runtime for each sinks
    for (sink, rt) in sinks {
        debug!("shutdown sink's runtime"; "sink" => sink.name());
        if rt.shutdown_now().wait().is_err() {
            error!("could not shutdown the runtime"; "sink" => sink.name());
        }
    }

    // Shutdown the metrics server
    if let Some(rt) = metrics_rt {
        debug!("shutdown metrics server's runtime");
        if rt.shutdown_now().wait().is_err() {
            error!("could not shutdown the metrics server");
        }
    }

    info!("Beamium halted!");
    Ok(())
}
