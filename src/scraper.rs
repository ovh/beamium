//! # Scraper module.
//!
//! The Scraper module fetch metrics from an HTTP endpoint.
use futures::future::Future;
use futures::Stream;
use hyper;
use hyper_timeout::TimeoutConnector;
use hyper_tls::HttpsConnector;
use std::cmp;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use time;

use config;

use lib;

/// Thread sleeping time.
const REST_TIME: u64 = 10;

/// Scraper loop.
pub fn scraper(
    scraper: &config::Scraper,
    parameters: &config::Parameters,
    sigint: &Arc<AtomicBool>,
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
    let mut core = ::tokio_core::reactor::Core::new()?;
    let handle = core.handle();
    let connector = HttpsConnector::new(4, &handle)?;

    let mut tm = TimeoutConnector::new(connector, &handle);
    tm.set_connect_timeout(Some(Duration::from_secs(parameters.timeout)));
    tm.set_read_timeout(Some(Duration::from_secs(parameters.timeout)));
    tm.set_write_timeout(Some(Duration::from_secs(parameters.timeout)));

    let client = hyper::Client::configure().connector(tm).build(&handle);
    let mut req = hyper::Request::new(hyper::Method::Get, scraper.url.clone());

    for (key, value) in &scraper.headers {
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
    let now = start.to_timespec().sec * 1000 * 1000 + (i64::from(start.to_timespec().nsec) / 1000);

    let dir = Path::new(&parameters.source_dir);
    let temp_file = dir.join(format!("{}.tmp", scraper.name));
    let mut batch_size = 0;
    let mut batch_count = 0;
    debug!("write to tmp file {}", format!("{:?}", temp_file));
    {
        // Open tmp file
        let mut file = File::create(&temp_file)?;
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

            if scraper.metrics.is_some() && !scraper.metrics.as_ref().unwrap().is_match(&line) {
                continue;
            }

            batch_size += line.len();
            if batch_size > parameters.batch_size as usize && !line.starts_with('=') {
                // Rotate scraped file
                file.flush()?;

                let dest_file =
                    dir.join(format!("{}-{}-{}.metrics", scraper.name, now, batch_count));
                debug!("rotate tmp file to {}", format!("{:?}", dest_file));
                fs::rename(&temp_file, &dest_file)?;

                file = File::create(&temp_file)?;
                batch_size = 0;
                batch_count += 1;
            }

            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
        }

        file.flush()?;
    }

    // Rotate scraped file
    let dest_file = dir.join(format!("{}-{}-{}.metrics", scraper.name, now, batch_count));
    debug!("rotate tmp file to {}", format!("{:?}", dest_file));
    fs::rename(&temp_file, &dest_file)?;

    Ok(())
}
