use std::collections::VecDeque;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use time;

use config;
use futures::future::Shared;
use futures::sync::oneshot;
use lib;
use router::RouterConfig;
use std::sync::{Arc, Mutex};

pub fn route_thread(
    config: &RouterConfig,
    todo: &Arc<Mutex<VecDeque<PathBuf>>>,
    sigint: &Shared<oneshot::Receiver<()>>,
    id: u64,
) {
    loop {
        match todo.lock().unwrap().pop_front() {
            Some(path) => {
                debug!("Processing: {:?}", path);
                if let Err(err) = route(&path, &config, id) {
                    warn!("Fail to process:{:?} - {}", path, err);
                }
            }
            None => {
                thread::sleep(Duration::from_millis(config::REST_TIME));
            }
        }

        if sigint.peek().is_some() {
            return;
        }
    }
}

fn route(path: &PathBuf, config: &RouterConfig, id: u64) -> Result<(), Box<Error>> {
    let start = time::now_utc().to_timespec();
    let run_id = format!("{}#{}", start.sec, start.nsec);

    // Load metrics
    debug!("open {}", format!("{:?}", path));
    let mut content = String::new();
    File::open(&path)?.read_to_string(&mut content)?;

    let mut metrics: Vec<String> = Vec::new();
    for line in content.lines() {
        metrics.push(lib::add_labels(&line, &config.labels)?);
    }

    // Nothing to do
    if metrics.is_empty() {
        // Delete source file
        debug!("delete  {}", format!("{:?}", path));
        fs::remove_file(&path)?;

        return Ok(());
    }

    // Setup sinks files
    let dir = Path::new(&config.sink_dir);

    // Open tmp files
    for sink in &config.sinks {
        let sink_file_name = dir.join(format!("{}-{}.tmp", sink.name, id));
        debug!("open tmp sink file {}", format!("{:?}", sink_file_name));
        let mut sink_file = BufWriter::new(File::create(&sink_file_name)?);

        // Write metrics
        for line in &metrics {
            if line.is_empty() {
                continue;
            }

            if sink.selector.is_some() {
                let selector = sink.selector.as_ref().unwrap();
                if !line
                    .split_whitespace()
                    .nth(1)
                    .map_or(false, |class| selector.is_match(class))
                {
                    continue;
                }
            }
            sink_file.write_all(line.as_bytes())?;
            sink_file.write_all(b"\n")?;
        }

        // Write buffered datas
        sink_file.flush()?;

        // Rotate
        let dest_file = dir.join(format!("{}-{}-{}.metrics", sink.name, id, run_id));
        debug!("rename {:?} to {:?}", sink_file_name, dest_file);
        fs::rename(sink_file_name, dest_file)?;
    }

    // Delete source file
    debug!("delete  {}", format!("{:?}", path));
    fs::remove_file(&path)?;

    Ok(())
}
