//! # Sink module.
//!
//! The Sink module send metrics to Warp10.
use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use time;
use std::cmp;
use std::io::prelude::*;
use std::fs;
use std::fs::File;
use std::error::Error;
use std::ffi::OsStr;
use std::path::PathBuf;
use hyper;

use config;

/// Thread sleeping time.
const REST_TIME: u64 = 10;

/// Sink loop.
pub fn sink(sink: &config::Sink, parameters: &config::Parameters, sigint: Arc<AtomicBool>) {
    loop {
        let start = time::now_utc();

        match send(sink, parameters) {
            Err(err) => error!("post fail: {}", err),
            Ok(_) => info!("post success"),
        }

        let elapsed = (time::now_utc() - start).num_milliseconds() as u64;
        let sleep_time = if elapsed > parameters.scan_period {
            REST_TIME
        } else {
            cmp::max(parameters.scan_period - elapsed, REST_TIME)
        };
        for _ in 0..sleep_time / REST_TIME {
            thread::sleep(Duration::from_millis(REST_TIME));
            if sigint.load(Ordering::Relaxed) {
                return;
            }
        }
    }
}

/// Send send sink metrics to Warp10.
fn send(sink: &config::Sink, parameters: &config::Parameters) -> Result<(), Box<Error>> {
    debug!("post {}", &sink.url);
    loop {
        let entries = try!(fs::read_dir(&parameters.sink_dir));
        let mut files = Vec::with_capacity(parameters.batch_count as usize);
        let mut metrics = String::new();

        // Load metrics
        let mut batch_size = 0;
        for (i, entry) in entries.enumerate() {
            let entry = try!(entry);
            let file_name = String::from(entry.file_name().to_str().unwrap_or(""));
            // Look only for metrics files of the sink
            if entry.path().extension() != Some(OsStr::new("metrics")) ||
               !file_name.starts_with(&sink.name) {
                continue;
            }

            // Split metrics in capped batch
            if i > parameters.batch_count as usize || batch_size > parameters.batch_size as usize {
                break;
            }

            debug!("open sink file {}", format!("{:?}", entry.path()));
            let file = match read(entry.path()) {
                Err(_) => continue,
                Ok(v) => v,
            };

            files.push(entry.path());
            batch_size += file.len();
            metrics.push_str(&file);
            metrics.push_str("\n");
        }

        // Nothing to do
        if metrics.len() == 0 {
            break;
        }

        // Send metrics
        let client = hyper::Client::new();
        let mut headers = hyper::header::Headers::new();
        headers.set_raw(sink.token_header.clone(), vec![sink.token.clone().into()]);

        debug!("post metrics");
        let request = client.post(&sink.url).headers(headers).body(&metrics);
        let mut res = try!(request.send());
        if !res.status.is_success() {
            let mut body = String::new();
            try!(res.read_to_string(&mut body));
            debug!("data {}", &body);

            return Err(From::from("non 200 received"));
        }

        // Delete sended data
        for f in files {
            debug!("delete sink file {}", format!("{:?}", f));
            try!(fs::remove_file(f));
        }
    }

    Ok(())
}

/// Read a file as String.
fn read(path: PathBuf) -> Result<String, Box<Error>> {
    let mut file = try!(File::open(path));

    let mut content = String::new();
    try!(file.read_to_string(&mut content));

    Ok(content)
}
