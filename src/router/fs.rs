use std::cmp;
use std::collections::{HashSet, VecDeque};
use std::error::Error;
use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use time;

use futures::future::Shared;
use futures::sync::oneshot;

use config;
use router::RouterConfig;

pub fn fs_thread(
    config: &RouterConfig,
    todo: &Arc<Mutex<VecDeque<PathBuf>>>,
    sigint: &Shared<oneshot::Receiver<()>>,
) {
    let mut files: HashSet<PathBuf> = HashSet::new();

    loop {
        let start = time::now_utc();
        match list(&config.dir) {
            Err(err) => error!("list fail: {}", err),
            Ok(entries) => {
                let deleted: Vec<PathBuf> = files.difference(&entries).cloned().collect();
                let new: Vec<PathBuf> = entries.difference(&files).cloned().collect();

                if !new.is_empty() {
                    debug!("found {} files", new.len());
                }

                {
                    let mut todo = todo.lock().unwrap();
                    for f in new {
                        files.insert(f.clone());
                        todo.push_front(f);
                    }
                }
                for f in deleted {
                    files.remove(&f);
                }
            }
        }

        let elapsed = (time::now_utc() - start).num_milliseconds() as u64;
        let sleep_time = if elapsed > config.watch_period {
            config::REST_TIME
        } else {
            cmp::max(config.watch_period - elapsed, config::REST_TIME)
        };
        for _ in 0..sleep_time / config::REST_TIME {
            thread::sleep(Duration::from_millis(config::REST_TIME));
            if sigint.peek().is_some() {
                return;
            }
        }
    }
}

fn list(dir: &str) -> Result<HashSet<PathBuf>, Box<Error>> {
    let entries: HashSet<PathBuf> = fs::read_dir(dir)?
        .filter_map(|entry| {
            if entry.is_err() {
                return None;
            }
            let entry = entry.unwrap();
            if entry.path().extension() != Some(OsStr::new("metrics")) {
                return None;
            }

            Some(entry.path())
        })
        .collect();

    Ok(entries)
}
