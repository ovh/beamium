//! # Scraper module.
//!
//! The Scraper module fetch metrics to Prometheus.
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

/// Thread sleeping time.
const REST_TIME: u64 = 10;

/// Scraper loop.
pub fn scraper(
    scraper: &config::Scraper,
    parameters: &config::Parameters,
    sigint: Arc<AtomicBool>,
) {
    loop {
        let start = time::now_utc();

        match fetch(scraper, parameters) {
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

/// Fetch retrieve metrics from Prometheus.
fn fetch(scraper: &config::Scraper, parameters: &config::Parameters) -> Result<(), Box<Error>> {
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

        for line in body.lines() {
            let line = match scraper.format {
                config::ScraperFormat::Sensision => String::from(line.trim()),
                config::ScraperFormat::Prometheus => match format_prometheus(line.trim(), now) {
                    Err(_) => {
                        warn!("bad row {}", &line);
                        continue;
                    }
                    Ok(v) => v,
                },
            };

            if line.is_empty() {
                continue;
            }

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

/// Format Warp10 metrics from Prometheus one.
fn format_prometheus(line: &str, now: i64) -> Result<String, Box<Error>> {
    // Skip comments
    if line.starts_with("#") {
        return Ok(String::new());
    }

    // Extract Prometheus metric
    let index = if line.contains("{") {
        try!(line.rfind('}').ok_or("bad class"))
    } else {
        try!(line.find(' ').ok_or("bad class"))
    };
    let (class, v) = line.split_at(index + 1);
    let mut tokens = v.split_whitespace();

    let value = try!(tokens.next().ok_or("no value"));
    let timestamp = tokens
        .next()
        .map(|v| i64::from_str_radix(v, 10).map(|v| v * 1000).unwrap_or(now))
        .unwrap_or(now);

    // Format class
    let mut parts = class.splitn(2, "{");
    let class = String::from(try!(parts.next().ok_or("no_class")));
    let class = class.trim();
    let plabels = parts.next();
    let slabels = if plabels.is_some() {
        let mut labels = plabels.unwrap().split("\",")
            .map(|v| v.replace("=", "%3D")) // escape
            .map(|v| v.replace("%3D\"", "=")) // remove left double quote
            .map(|v| v.replace("\"}", "")) // remove right double quote
            .map(|v| v.replace(",", "%2C")) // escape
            .map(|v| v.replace("}", "%7D")) // escape
            .map(|v| v.replace(r"\\", r"\")) // unescape
            .map(|v| v.replace("\\\"", "\"")) // unescape
            .map(|v| v.replace(r"\n", "%0A")) // unescape
            .fold(String::new(), |acc, x| {
                // skip invalid values
                if !x.contains("=") {
                    return acc
                }
                acc + &x + ","
            });
        labels.pop();
        labels
    } else {
        String::new()
    };

    let class = format!("{}{{{}}}", class, slabels);

    Ok(format!("{}// {} {}", timestamp, class, value))
}
