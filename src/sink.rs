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
use hyper::net::HttpsConnector;
use hyper_native_tls::NativeTlsClient;
use std::os::unix::fs::MetadataExt;

use config;

/// Thread sleeping time.
const REST_TIME: u64 = 10;

/// Sink loop.
pub fn sink(sink: &config::Sink, parameters: &config::Parameters, sigint: Arc<AtomicBool>) {
    loop {
        let start = time::now_utc();

        match send(sink, parameters, sigint.clone()) {
            Err(err) => error!("post fail: {}", err),
            Ok(size) => {
                if size > 0 {
                info!("post success - {}", size)
                }
            },
        }

        let res = cappe(sink, parameters);
        if res.is_err() {
            error!("cappe fail: {}", res.unwrap_err());
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

/// Send sink metrics to Warp10.
fn send(sink: &config::Sink, parameters: &config::Parameters, sigint: Arc<AtomicBool>) -> Result<usize, Box<Error>> {
    let mut proc_size = 0;

    loop {
        if sigint.load(Ordering::Relaxed) {
            return Ok(proc_size);
        }

        let entries = try!(files(&parameters.sink_dir, &sink.name));
        let mut files = Vec::with_capacity(parameters.batch_count as usize);
        let mut metrics = String::new();

        // Load metrics
        let mut batch_size = 0;
        for (i, entry) in entries.iter().enumerate() {
            // Split metrics in capped batch
            if i > parameters.batch_count as usize || batch_size > parameters.batch_size as usize {
                break;
            }

            debug!("open sink file {:?}", entry.path());
            let file = match read(entry.path()) {
                Err(_) => continue,
                Ok(v) => v,
            };

            files.push(entry.path());
            batch_size += file.len();
            metrics.push_str(&file);
            metrics.push_str("\n");
        }

        proc_size += metrics.len();

        // Nothing to do
        if metrics.len() == 0 {
            break;
        }

        // Send metrics
        let ssl = NativeTlsClient::new().unwrap();
        let connector = HttpsConnector::new(ssl);
        let mut client = hyper::Client::with_connector(connector);
        client.set_write_timeout(Some(Duration::from_secs(parameters.timeout)));
        client.set_read_timeout(Some(Duration::from_secs(parameters.timeout)));

        let mut headers = hyper::header::Headers::new();
        headers.set_raw(sink.token_header.clone(), vec![sink.token.clone().into()]);

        debug!("post {}", &sink.url);
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

    Ok(proc_size)
}

fn cappe(sink: &config::Sink, parameters: &config::Parameters) -> Result<(), Box<Error>> {
    let entries = try!(files(&parameters.sink_dir, &sink.name));
    let mut sinks_size: u64 = 0;

    for entry in &entries {
        let meta = try!(entry.metadata());

        let modified = meta.modified();

        if modified.is_ok() {
            let modified = modified.unwrap();
            let age = modified.elapsed().unwrap_or(Duration::new(0, 0));

            if age.as_secs() > sink.ttl {
                warn!("skip file {:?}", entry.path());
                try!(fs::remove_file(entry.path()));
                continue;
            }
        }

        sinks_size += meta.size();
        if sinks_size > sink.size {
            warn!("skip file {:?}", entry.path());
            try!(fs::remove_file(entry.path()));
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

fn files(dir: &str, sink_name: &str) -> Result<Vec<fs::DirEntry>, Box<Error>> {
    let mut entries: Vec<fs::DirEntry> = try!(fs::read_dir(dir)).filter_map(|entry| {
        if entry.is_err() {
            return None;
        }
        let entry = entry.unwrap();
        if entry.path().extension() != Some(OsStr::new("metrics")) {
            return None;
        }

        let file_name = String::from(entry.file_name().to_str().unwrap_or(""));

        if !file_name.starts_with(sink_name) {
            return None;
        }

        Some(entry)
    }).collect();

    entries.sort_by(|a, b| b.file_name().cmp(&a.file_name()));

    Ok(entries)
}
