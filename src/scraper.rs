//! # Scraper module.
//!
//! The Scraper module fetch metrics from an HTTP endpoint.
use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use time;
use std::io;
use std::cmp;
use hyper;
use hyper_tls::HttpsConnector;
use hyper_timeout::TimeoutConnector;
use futures::future::Future;
use futures::Stream;
use std::io::prelude::*;
use std::fs;
use std::fs::File;
use std::error::Error;
use std::path::Path;

use config;

use lib;

/// Thread sleeping time.
const REST_TIME: u64 = 10;

/// Scraper loop.
pub fn scraper(
    scraper: &config::Scraper,
    parameters: &config::Parameters,
    sigint: Arc<AtomicBool>,
) {
    let labels: String = scraper.labels.iter().fold(String::new(), |acc, (k, v)| {
        let sep = if acc.is_empty() { "" } else { "," };
        acc + sep + k + "=" + v
    });
    loop {
        let start = time::now_utc();

        match fetch(scraper, &labels, parameters) {
            Err(err) => error!("fetch fail: {}", err),
            Ok(_) => info!("fetch success"),
        }

        let elapsed = (time::now_utc() - start).num_milliseconds() as u64;
        let sleep_time = if elapsed > scraper.period {
            REST_TIME
        } else {
            cmp::max(scraper.period - elapsed, REST_TIME)
        };
        for _ in 0..sleep_time / REST_TIME {
            thread::sleep(Duration::from_millis(REST_TIME));
            if sigint.load(Ordering::Relaxed) {
                return;
            }
        }
    }
}

/// Fetch retrieve metrics.
fn fetch(
    scraper: &config::Scraper,
    labels: &str,
    parameters: &config::Parameters,
) -> Result<(), Box<Error>> {
    debug!("fetch {}", &scraper.url);

    // Fetch metrics
    let mut core = try!(::tokio_core::reactor::Core::new());
    let handle = core.handle();
    let connector = try!(HttpsConnector::new(4, &handle));

    let mut tm = TimeoutConnector::new(connector, &handle);
    tm.set_connect_timeout(Some(Duration::from_secs(parameters.timeout)));
    tm.set_read_timeout(Some(Duration::from_secs(parameters.timeout)));
    tm.set_write_timeout(Some(Duration::from_secs(parameters.timeout)));

    let client = hyper::Client::configure().connector(tm).build(&handle);
    let mut req = hyper::Request::new(hyper::Method::Get, scraper.url.clone());

    for (key, value) in scraper.headers.iter() {
        req.headers_mut()
            .set_raw(key.clone(), vec![value.clone().into_bytes()]);
    }

    let get = client
        .request(req)
        .and_then(|res| {
            if res.status().is_success() {
                Ok(res)
            } else {
                Err(hyper::error::Error::from(io::Error::new(
                    io::ErrorKind::Other,
                    format!("{}", res.status()),
                )))
            }
        })
        .and_then(|res| res.body().concat2());

    let got = core.run(get)?;

    // Read body
    let body = String::from_utf8_lossy(&got);
    trace!("data {}", &body);

    // Get now as millis
    let start = time::now_utc();
    let now = start.to_timespec().sec * 1000 * 1000 + (start.to_timespec().nsec as i64 / 1000);

    let dir = Path::new(&parameters.source_dir);
    let temp_file = dir.join(format!("{}.tmp", scraper.name));
    debug!("write to tmp file {}", format!("{:?}", temp_file));
    {
        // Open tmp file
        let mut file = try!(File::create(&temp_file));
        let ts = lib::transcompiler::Transcompiler::new(&scraper.format);

        for line in body.lines() {
            let line = match ts.format(line) {
                Ok(v) => v,
                Err(_) => {
                    warn!("fail to format line {}", &line);
                    continue;
                }
            };

            if line.is_empty() {
                continue;
            }

            let line = match lib::add_labels(&line, labels) {
                Ok(v) => v,
                Err(_) => {
                    warn!("fail to add label on {}", &line);
                    continue;
                }
            };

            if scraper.metrics.is_some() {
                if !scraper.metrics.as_ref().unwrap().is_match(&line) {
                    continue;
                }
            }

            try!(file.write(line.as_bytes()));
            try!(file.write(b"\n"));
        }

        try!(file.flush());
    }

    // Rotate scraped file
    let dest_file = dir.join(format!("{}-{}.metrics", scraper.name, now));
    debug!("rotate tmp file to {}", format!("{:?}", dest_file));
    try!(fs::rename(&temp_file, &dest_file));

    Ok(())
}
