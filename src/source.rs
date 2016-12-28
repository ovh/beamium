//! # Source module.
//!
//! The Source module fetch metrics to Promotheus.
use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use time;
use std::cmp;
use hyper;
use std::io::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::error::Error;
use std::path::Path;

use config;

/// Thread sleeping time.
const REST_TIME: u64 = 10;

/// Source loop.
pub fn source(source: &config::Source,
              labels: &HashMap<String, String>,
              parameters: &config::Parameters,
              sigint: Arc<AtomicBool>) {
    let labels: String = labels.iter()
        .fold(String::new(), |acc, (k, v)| {
            let sep = if acc.is_empty() { "" } else { "," };
            acc + sep + k + "=" + v
        });

    loop {
        let start = time::now_utc();

        match fetch(source, &labels, parameters) {
            Err(err) => error!("fetch fail: {}", err),
            Ok(_) => info!("fetch success"),
        }

        let elapsed = (time::now_utc() - start).num_milliseconds() as u64;
        let sleep_time = if elapsed > source.period {
            REST_TIME
        } else {
            cmp::max(source.period - elapsed, REST_TIME)
        };
        for _ in 0..sleep_time / REST_TIME {
            thread::sleep(Duration::from_millis(REST_TIME));
            if sigint.load(Ordering::Relaxed) {
                return;
            }
        }
    }
}

/// Fetch retrieve metrics from Promotheus.
fn fetch(source: &config::Source,
         labels: &String,
         parameters: &config::Parameters)
         -> Result<(), Box<Error>> {
    debug!("fetch {}", &source.url);

    // Fetch metrics
    let client = hyper::Client::new();
    let mut res = try!(client.get(&source.url).send());
    if !res.status.is_success() {
        return Err(From::from("non 200 received"));
    }

    // Read body
    let mut body = String::new();
    try!(res.read_to_string(&mut body));
    trace!("data {}", &body);


    // Get now as millis
    let start = time::now_utc();
    let now = start.to_timespec().sec * 1000 * 1000 + (start.to_timespec().nsec as i64 / 1000);

    let dir = Path::new(&parameters.source_dir);
    let temp_file = dir.join(format!("{}.tmp", source.name));
    debug!("write to tmp file {}", format!("{:?}", temp_file));
    {
        // Open tmp file
        let mut file = try!(File::create(&temp_file));

        for line in body.lines() {
            let line = match format(line.trim(), labels, now) {
                Err(_) => {
                    warn!("bad row {}", &line);
                    continue;
                }
                Ok(v) => v,
            };

            if line.is_empty() {
                continue;
            }

            try!(file.write(line.as_bytes()));
            try!(file.write(b"\n"));
        }

        try!(file.flush());
    }

    // Rotate source file
    let dest_file = dir.join(format!("{}-{}.metrics", source.name, now));
    debug!("rotate tmp file to {}", format!("{:?}", dest_file));
    try!(fs::rename(&temp_file, &dest_file));

    Ok(())
}

/// Format Wqrp10 metrics from Promotheus one.
fn format(line: &str, labels: &String, now: i64) -> Result<String, Box<Error>> {
    // Skip comments
    if line.starts_with("#") {
        return Ok(String::new());
    }

    // Extract promotheus metric
    let mut tokens = line.split_whitespace();
    let class = try!(tokens.next().ok_or("no class"));
    let value = try!(tokens.next().ok_or("no value"));
    let timestamp = tokens.next()
        .map(|v| {
            i64::from_str_radix(v, 10)
                .map(|v| v * 1000 * 1000)
                .unwrap_or(now)
        })
        .unwrap_or(now);

    // Manage labels
    let class = if labels.is_empty() {
        String::from(class)
    } else {
        if class.ends_with("}") {
            // Has promotheus labels
            let mut cl = String::from(class);
            cl.pop();
            if !class.ends_with("{") {
                // Non empty labels, append ours
                cl.push(',')
            }
            format!("{}{}}}", cl, labels)
        } else {
            // No labels
            format!("{}{{{}}}", class, labels)
        }
    };

    Ok(format!("{}// {} {}", timestamp, class, value))
}
