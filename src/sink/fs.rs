use std::cmp;
use std::collections::{HashSet, VecDeque};
use std::error::Error;
use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use time;

use futures::future::Shared;
use futures::future::{ok, FutureResult};
use futures::sync::mpsc::Receiver;
use futures::sync::oneshot;
use futures::task::Task;
use futures::{lazy, Async, Stream};

use tokio_core::reactor::Core;

use config;
use sink::SinkConfig;

pub fn fs_thread(
    todo: &Arc<Mutex<VecDeque<PathBuf>>>,
    config: &SinkConfig,
    sigint: &Shared<oneshot::Receiver<()>>,
    mut notify_rx: Receiver<Task>,
) {
    let mut files: HashSet<PathBuf> = HashSet::new();

    let mut core = Core::new().expect("Fail to start tokio reactor");

    loop {
        let start = time::now_utc();
        let work = lazy(|| -> FutureResult<(), ()> {
            match list(&config.name, &config.dir, config.ttl) {
                Err(err) => error!("list fail: {}", err),
                Ok((entries, size)) => {
                    let new: Vec<PathBuf> = entries.difference(&files).cloned().collect();
                    let deleted: Vec<PathBuf> = files.difference(&entries).cloned().collect();

                    if !new.is_empty() {
                        debug!("found {} files", new.len());
                    }

                    {
                        let mut todo = todo.lock().unwrap();
                        for f in new {
                            match remove_empty(&f) {
                                Err(err) => error!("cannot remove empty file: {}", err),
                                Ok(removed) => {
                                    if removed {
                                        debug!("remove empty file: {:?}", f);
                                        files.remove(&f);
                                        continue;
                                    }

                                    files.insert(f.clone());
                                    todo.push_front(f);
                                    match notify_rx.poll().expect("poll never failed") {
                                        Async::Ready(t) => t.map(|t| t.notify()),
                                        Async::NotReady => None,
                                    };
                                }
                            }
                        }
                    }
                    for f in deleted {
                        files.remove(&f);
                    }

                    match cappe(size, config.max_size, &todo) {
                        Err(err) => error!("cappe fail: {}", err),
                        Ok(()) => {}
                    }
                }
            }
            ok(())
        });

        core.run(work).expect("always ok");

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

fn list(sink_name: &str, dir: &str, ttl: u64) -> Result<(HashSet<PathBuf>, u64), Box<Error>> {
    let mut sink_name = String::from(sink_name);
    sink_name.push('-');

    let mut files_size = 0;

    let entries: HashSet<PathBuf> = fs::read_dir(dir)?
        .filter_map(|entry| {
            if entry.is_err() {
                return None;
            }
            let entry = entry.unwrap();
            if entry.path().extension() != Some(OsStr::new("metrics")) {
                return None;
            }

            let file_name = String::from(entry.file_name().to_str().unwrap_or(""));

            if !file_name.starts_with(&sink_name) {
                return None;
            }

            let meta = entry.metadata();
            if meta.is_ok() {
                let meta = meta.unwrap();
                files_size += meta.len();

                let modified = meta.modified().unwrap_or_else(|_| SystemTime::now());
                let age = modified.elapsed().unwrap_or_else(|_| Duration::new(0, 0));

                if age.as_secs() > ttl {
                    warn!("skip file {:?}", entry.path());
                    match fs::remove_file(entry.path()) {
                        Ok(()) => {}
                        Err(err) => error!("{}", err),
                    }
                    return None;
                }
            }

            Some(entry.path())
        })
        .collect();

    Ok((entries, files_size))
}

fn cappe(actual: u64, target: u64, todo: &Arc<Mutex<VecDeque<PathBuf>>>) -> Result<(), Box<Error>> {
    if actual < target {
        return Ok(());
    }

    let mut size = actual;

    loop {
        let path = { todo.lock().unwrap().pop_back() };
        if path.is_none() {
            break;
        }
        let path = path.unwrap();

        let meta = fs::metadata(&path)?;
        size -= meta.len();

        warn!("skip file {:?}", path);
        fs::remove_file(path)?;

        if size < target {
            break;
        }
    }

    Ok(())
}

fn remove_empty(path: &PathBuf) -> Result<bool, Box<Error>> {
    let metadata = fs::metadata(path.clone())?;
    if metadata.len() == 0 {
        fs::remove_file(path.clone())?;

        return Ok(true);
    }

    Ok(false)
}
