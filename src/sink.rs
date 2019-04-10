use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::From;
use std::fs::Metadata;
use std::path::PathBuf;
use std::process::abort;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, SystemTime};

use failure::Error;
use futures::future::ok;
use futures::Stream;
use prometheus::CounterVec;
use tokio::fs::remove_file;
use tokio::prelude::*;
use tokio::runtime::Runtime;

use crate::conf;
use crate::lib::asynch::fs::Scanner;
use crate::lib::asynch::http::Sender;
use crate::lib::{Named, Runner};

lazy_static! {
    static ref BEAMIUM_SKIP_TTL: CounterVec = register_counter_vec!(
        opts!(
            "beamium_skip_ttl",
            "Number of files skipped due to a too old ttl"
        ),
        &["sink"]
    )
    .expect("create metric: 'beamium_skip_ttl'");
    static ref BEAMIUM_SKIP_MAX_SIZE: CounterVec = register_counter_vec!(
        opts!(
            "beamium_skip_max_size",
            "Number of files skipped due to a max size in sink"
        ),
        &["sink"]
    )
    .expect("create metric: 'beamium_skip_max_size'");
}

#[derive(Debug, Clone)]
pub struct Sink {
    conf: Arc<conf::Sink>,
    params: Arc<conf::Parameters>,
    queue: Arc<Mutex<VecDeque<PathBuf>>>,
}

impl From<(conf::Sink, conf::Parameters)> for Sink {
    fn from(tuple: (conf::Sink, conf::Parameters)) -> Self {
        let (conf, params) = tuple;

        Self {
            conf: arc!(conf),
            params: arc!(params),
            queue: mutex!(VecDeque::new()),
        }
    }
}

impl Named for Sink {
    fn name(&self) -> String {
        self.conf.name.to_owned()
    }
}

impl Runner for Sink {
    type Error = Error;

    fn start(&self, rt: &mut Runtime) -> Result<(), Self::Error> {
        let name = self.name();
        let dir = self.params.sink_dir.to_owned();

        for _ in 0..self.conf.parallel.to_owned() {
            let name = self.name();
            let task = Sender::from((
                self.queue.to_owned(),
                self.conf.to_owned(),
                self.params.to_owned(),
            ))
            .for_each(move |_| ok(()))
            .map_err(move |err| {
                crit!("could not send data"; "sink" => name.as_str(), "error" => err.to_string());
                sleep(Duration::from_millis(100)); // Sleep the time to display the message
                abort();
            });

            rt.spawn(task);
        }

        let conf = self.conf.to_owned();
        let mutex = self.queue.to_owned();
        let executor = rt.executor();

        let scanner = Scanner::from((PathBuf::from(dir.to_owned()), self.params.scan_period.to_owned()))
            .fold(HashSet::new(), move |acc, entries| {
                // Owned variables
                let conf = conf.to_owned();
                let mutex = mutex.to_owned();

                // Compute useful information
                // Retrieve files which have expired
                let expired: HashMap<PathBuf, Metadata> = entries.iter()
                    .filter_map(|(path, meta)| {
                        let file_name = path.file_name()?.to_str()?;
                        if !file_name.starts_with(conf.name.as_str()) {
                            return None;
                        }

                        let modified = meta.modified().unwrap_or_else(|_| SystemTime::now());
                        let age = modified.elapsed().unwrap_or_else(|_| Duration::new(0, 0));

                        if age > conf.ttl {
                            return Some((path.to_owned(), meta.to_owned()));
                        }

                        None
                    })
                    .collect();

                for (path, _) in expired {
                    let path = path.to_owned();
                    let name = conf.name.to_owned();

                    warn!("skip file"; "sink" => name.as_str(), "path" => path.to_str(), "reason" => "file is too old");
                    BEAMIUM_SKIP_TTL
                        .with_label_values(&[name.as_str()])
                        .inc();

                    executor.spawn(
                        remove_file(path.to_owned())
                            .and_then(|_| ok(()))
                            .map_err(move |err| { error!("could not remove file"; "error" => err.to_string(), "sink" => name.as_str(), "path" => path.to_str()); })
                    )
                }

                // Retrieve files that are not expired
                let entries: HashMap<PathBuf, Metadata> = entries.iter()
                    .filter_map(|(path, meta)| {
                        let file_name = path.file_name()?.to_str()?;
                        if !file_name.starts_with(conf.name.as_str()) {
                            return None;
                        }

                        let modified = meta.modified().unwrap_or_else(|_| SystemTime::now());
                        let age = modified.elapsed().unwrap_or_else(|_| Duration::new(0, 0));

                        if age < conf.ttl {
                            return Some((path.to_owned(), meta.to_owned()));
                        }

                        None
                    })
                    .collect();

                let paths = entries.iter().fold(HashSet::new(), |mut acc, (path, _)| {
                    acc.insert(path.to_owned());
                    acc
                });

                let mut current_size = entries.iter().fold(0, |acc, (_, meta)| acc + meta.len());
                let new: Vec<PathBuf> = paths.difference(&acc).cloned().collect();

                if !new.is_empty() {
                    info!("found files"; "sink" => conf.name.as_str(), "number" => new.len());
                }

                {
                    let mut queue = try_future!(mutex.lock());
                    for path in new {
                        queue.push_front(path);
                    }

                    while current_size > conf.size {
                        let path = match queue.pop_back() {
                            Some(path) => path,
                            None => break,
                        };

                        let meta = match entries.get(&path) {
                            Some(meta) => meta,
                            None => continue,
                        };

                        let name = conf.name.to_owned();

                        warn!("skip file"; "sink" => name.as_str(), "path" => path.to_str(), "reason" => "sink is too large");
                        BEAMIUM_SKIP_MAX_SIZE
                            .with_label_values(&[name.as_str()])
                            .inc();

                        executor.spawn(
                            remove_file(path.to_owned())
                                .and_then(|_| ok(()))
                                .map_err(move |err| { error!("could not remove file"; "error" => err.to_string(), "sink" => name.as_str(), "path" => path.to_str()); })
                        );

                        current_size -= meta.len();
                    }
                }

                ok::<_, Error>(paths)
            })
            .and_then(|_| ok(()))
            .map_err(move |err| {
                crit!("could not scan sink directory"; "sink" => name.as_str(), "dir" => dir.as_str(), "error" => err.to_string());
                sleep(Duration::from_millis(100)); // Sleep the time to display the message
                abort();
            });

        rt.spawn(scanner);

        Ok(())
    }
}
