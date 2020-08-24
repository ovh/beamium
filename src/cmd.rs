//! # Command module
//!
//! The command provide useful stuffs to handle the command line interface
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use failure::{format_err, Error, ResultExt};
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

#[derive(StructOpt, Clone, Debug)]
pub(crate) struct Opts {
    /// Prints version information
    #[structopt(short = "V", long = "version")]
    pub version: bool,

    /// Increase verbosity level (console only)
    #[structopt(short = "v", parse(from_occurrences))]
    pub verbose: usize,

    /// Sets a custom config file
    #[structopt(short = "c", long = "config", parse(from_os_str))]
    pub config: Option<PathBuf>,

    /// Test configuration
    #[structopt(short = "t", long = "check")]
    pub check: bool,
}

pub(crate) fn version() -> Result<(), Error> {
    let mut version = String::new();

    version += &format!(
        "{} version {} {}\n",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        GITHASH
    );

    version += &format!("{} build date {}\n", env!("CARGO_PKG_NAME"), BUILD_DATE);
    version += &format!("{} profile {}\n", env!("CARGO_PKG_NAME"), PROFILE);

    print!("{}", version);
    Ok(())
}

pub(crate) fn main(
    conf: Conf,
    sigint: Arc<AtomicBool>,
    is_started_notifier: Arc<AtomicBool>,
) -> Result<(), Error> {
    // -------------------------------------------------------------------------
    // Ensure that directories are presents
    if let Err(err) = create_dir_all(conf.parameters.source_dir.to_owned()) {
        crit!("could not create source directory"; "error" => err.to_string());
        return Err(format_err!("{}", err));
    }

    if let Err(err) = create_dir_all(conf.parameters.sink_dir.to_owned()) {
        crit!("could not create sink directory"; "error" => err.to_string());
        return Err(format_err!("{}", err));
    }

    // -------------------------------------------------------------------------
    // Create metrics http server
    let mut metrics_rt = None;

    if let Some(addr) = conf.parameters.metrics {
        let mut rt = Builder::new()
            .keep_alive(Some(KEEP_ALIVE_TOKIO_RUNTIME))
            .core_threads(1)
            .blocking_threads(1)
            .name_prefix("metrics-")
            .build()
            .with_context(|err| format_err!("could not start metrics runtime, {}", err))?;

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
                return Err(format_err!(
                    "could not build the runtime for scraper '{}', {}",
                    scraper.name(),
                    err
                ));
            }
        };

        if let Err(err) = scraper.start(&mut rt) {
            return Err(format_err!(
                "could not start scraper '{}', {}",
                scraper.name(),
                err
            ));
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
            return Err(format_err!(
                "could not build the runtime for router, {}",
                err
            ));
        }
    };

    let router = Router::from((
        conf.parameters.to_owned(),
        conf.labels,
        conf.sinks.to_owned(),
    ));

    if let Err(err) = router.start(&mut rt) {
        return Err(format_err!("could not start the router, {}", err));
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
                return Err(format_err!(
                    "could not build the runtime for sink '{}', {}",
                    sink.name(),
                    err
                ));
            }
        };

        if let Err(err) = sink.start(&mut rt) {
            return Err(format_err!(
                "could not start sink '{}', {}",
                sink.name(),
                err
            ));
        }

        sinks.push((sink, rt));
    }

    debug!("cmd::main is started");
    is_started_notifier.store(true, Ordering::SeqCst);

    // Wait for termination signals
    while sigint.load(Ordering::SeqCst) {
        thread::sleep(THREAD_SLEEP);
    }

    // Shutdown runtime for each scrapers
    for (scraper, rt) in scrapers {
        debug!("shutdown scraper's runtime"; "scraper" => scraper.name());
        if rt.shutdown_now().wait().is_err() {
            error!("could not shutdown the runtime"; "scraper" => scraper.name());
        }
    }

    // Shutdown runtime for each sinks
    for (sink, rt) in sinks {
        debug!("shutdown sink's runtime"; "sink" => sink.name());
        if rt.shutdown_now().wait().is_err() {
            error!("could not shutdown the runtime"; "sink" => sink.name());
        }
    }

    // Shutdown router runtime
    debug!("shutdown router's runtime");
    if router.1.shutdown_now().wait().is_err() {
        error!("could not shutdown the router's runtime");
    }

    // Shutdown the metrics server
    if let Some(rt) = metrics_rt {
        debug!("shutdown metrics server's runtime");
        if rt.shutdown_now().wait().is_err() {
            error!("could not shutdown the metrics server");
        }
    }

    debug!("cmd::main is stopped");
    is_started_notifier.store(false, Ordering::SeqCst);
    Ok(())
}
