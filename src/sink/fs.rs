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

use futures::sync::oneshot;
use futures::future::Shared;
use futures::sync::mpsc::Receiver;
use futures::{lazy, Async, Stream};
use futures::future::{ok, FutureResult};
use futures::task::Task;

use tokio_core::reactor::Core;

use config;

pub fn fs_thread(
    name: &str,
    dir: &str,
    period: u64,
    todo: Arc<Mutex<VecDeque<PathBuf>>>,
    max_size: u64,
    ttl: u64,
    sigint: Shared<oneshot::Receiver<()>>,
    mut notify_rx: Receiver<Task>,
) {
    let mut files: HashSet<PathBuf> = HashSet::new();

    let mut core = Core::new().expect("Fail to start tokio reactor");

    loop {
        let start = time::now_utc();
        let work = lazy(|| -> FutureResult<(), ()> {
            match list(name, dir, &files, ttl) {
                Err(err) => error!("list fail: {}", err),
                Ok((new, deleted, size)) => {
                    if new.len() > 0 {
                        debug!("found {} files", new.len());
                    }

                    {
                        let mut todo = todo.lock().unwrap();
                        for f in new {
                            files.insert(f.clone());
                            todo.push_front(f);
                            match notify_rx.poll().expect("poll never failed") {
                                Async::Ready(t) => t.map(|t| t.notify()),
                                Async::NotReady => None,
                            };
                        }
                    }
                    for f in deleted {
                        files.remove(&f);
                    }

                    match cappe(size, max_size, &todo) {
                        Err(err) => error!("cappe fail: {}", err),
                        Ok(()) => {}
                    }
                }
            }
            ok(())
        });

        core.run(work).expect("always ok");

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
    sink_name: &str,
    dir: &str,
    files: &HashSet<PathBuf>,
    ttl: u64,
) -> Result<(Vec<PathBuf>, Vec<PathBuf>, u64), Box<Error>> {
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
                files_size = files_size + meta.len();

                let modified = meta.modified().unwrap_or(SystemTime::now());
                let age = modified.elapsed().unwrap_or(Duration::new(0, 0));

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

    let deleted = files.difference(&entries).cloned().collect();
    let new = entries.difference(&files).cloned().collect();

    Ok((new, deleted, files_size))
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
        size = size - meta.len();

        warn!("skip file {:?}", path);
        fs::remove_file(path)?;

        if size < target {
            break;
        }
    }

    Ok(())
}
