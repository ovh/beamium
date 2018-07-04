use std::cmp;
use std::collections::{HashSet, VecDeque};
use std::error::Error;
use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration};

use time;

use futures::future::Shared;
use futures::sync::oneshot;


use config;

pub fn fs_thread(
    dir: &str,
    period: u64,
    todo: Arc<Mutex<VecDeque<PathBuf>>>,
    sigint: Shared<oneshot::Receiver<()>>,
) {
    let mut files: HashSet<PathBuf> = HashSet::new();

    loop {
        let start = time::now_utc();
        match list(dir, &files) {
            Err(err) => error!("list fail: {}", err),
            Ok((new, deleted)) => {
                if new.len() > 0 {
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
        let sleep_time = if elapsed > period {
            config::REST_TIME
        } else {
            cmp::max(period - elapsed, config::REST_TIME)
        };
        for _ in 0..sleep_time / config::REST_TIME {
            thread::sleep(Duration::from_millis(config::REST_TIME));
            if sigint.peek().is_some() {
                return;
            }
        }
    }
}

fn list(
    dir: &str,
    files: &HashSet<PathBuf>,
) -> Result<(Vec<PathBuf>, Vec<PathBuf>), Box<Error>> {
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

    let deleted = files.difference(&entries).cloned().collect();
    let new = entries.difference(&files).cloned().collect();

    Ok((new, deleted))
}
