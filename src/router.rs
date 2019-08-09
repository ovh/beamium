//! # Router module.
//!
//! The Router module forward sources to sinks.
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::abort;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use failure::{Error, format_err};
use futures::future::ok;
use tokio::fs::{File, rename};
use tokio::fs::remove_file;
use tokio::prelude::*;
use tokio::runtime::Runtime;

use crate::conf;
use crate::lib::{add_labels, Runner};
use crate::lib::asynch::fs::Scanner;

#[derive(Clone, Debug)]
pub struct Router {
    params: Arc<conf::Parameters>,
    labels: Arc<HashMap<String, String>>,
    sinks: Arc<Vec<conf::Sink>>,
}

impl From<(conf::Parameters, HashMap<String, String>, Vec<conf::Sink>)> for Router {
    fn from(tuple: (conf::Parameters, HashMap<String, String>, Vec<conf::Sink>)) -> Self {
        let (params, labels, sinks) = tuple;

        Self {
            params: arc!(params),
            labels: arc!(labels),
            sinks: arc!(sinks),
        }
    }
}

impl Runner for Router {
    type Error = Error;

    fn start(&self, rt: &mut Runtime) -> Result<(), Self::Error> {
        // Owned variables by creating a new reference using Arc.
        let labels = self.labels.to_owned();
        let sinks = self.sinks.to_owned();
        let params = self.params.to_owned();

        let dir = PathBuf::from(self.params.source_dir.to_owned());
        let executor = rt.executor();

        let scanner = Scanner::from((dir, self.params.scan_period.to_owned()))
            .fold(HashSet::new(), move |acc, entries| {
                let paths: HashSet<PathBuf> =
                    entries.iter().fold(HashSet::new(), |mut acc, (path, _)| {
                        acc.insert(path.to_owned());
                        acc
                    });

                let new: Vec<PathBuf> = paths.difference(&acc).cloned().collect();
                for path in new {
                    let labels = labels.to_owned();
                    let sinks = sinks.to_owned();
                    let params = params.to_owned();
                    let epath = path.to_owned();

                    executor.spawn(
                        Self::load(path.to_owned())
                            .and_then(move |lines| Self::process(&lines, &labels))
                            .and_then(move |lines| Self::write(&lines, &params, &sinks))
                            .and_then(move |_| Self::remove(path))
                            .map_err(move |err| {
                                error!("could not process file in router"; "path" => epath.to_str(), "error" => err.to_string());
                            }),
                    );
                }

                ok::<_, Error>(paths)
            })
            .and_then(|_| ok(()))
            .map_err(|err| {
                crit!("could not scan source directory"; "error" => err.to_string());
                sleep(Duration::from_millis(100)); // Sleep the time to display the message
                abort();
            });

        // Spawn the ticker on router's runtime
        rt.spawn(scanner);

        Ok(())
    }
}

impl Router {
    fn load(path: PathBuf) -> impl Future<Item=Vec<String>, Error=Error> {
        trace!("open file"; "path" => path.to_str());
        File::open(path)
            .map_err(|err| format_err!("{}", err))
            .and_then(|mut file| {
                let mut buf = String::new();
                try_future!(file.read_to_string(&mut buf));
                ok(buf.split('\n').map(String::from).collect())
            })
    }

    fn process(
        lines: &[String],
        labels: &Arc<HashMap<String, String>>,
    ) -> impl Future<Item=Vec<String>, Error=Error> {
        let labels: Vec<String> = labels
            .to_owned()
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();

        let labels = labels.join(",");
        let mut body = vec![];
        for line in lines {
            if !line.is_empty() {
                body.push(try_future!(add_labels(&line, &labels)))
            }
        }

        ok(body)
    }

    fn write(
        lines: &[String],
        params: &conf::Parameters,
        sinks: &[conf::Sink],
    ) -> impl Future<Item=(), Error=Error> {
        let mut bulk = vec![];

        let start = time::now_utc().to_timespec();
        let run_id = format!("{}#{}", start.sec, start.nsec);

        let mut idx = -1;
        for sink in sinks {
            idx += 1;
            let body = match &sink.selector {
                None => lines.to_owned(),
                Some(selector) => {
                    let mut body = vec![];
                    for line in lines.to_owned() {
                        if line
                            .split_whitespace()
                            .nth(1)
                            .map_or(false, |class| selector.is_match(class))
                        {
                            body.push(line);
                        }
                    }

                    body
                }
            };

            if body.is_empty() {
                continue;
            }

            let run_id = run_id.to_owned();
            let name = sink.name.to_owned();
            let dir = PathBuf::from(params.sink_dir.to_owned());
            let temp_file = dir.join(format!("{}-{}-{}.tmp", sink.name, idx, run_id.to_owned()));

            trace!("create tmp sink file"; "path" => temp_file.to_str());
            bulk.push(
                File::create(temp_file.to_owned())
                    .and_then(move |mut file| {
                        file.poll_write(&body.join("\n").into_bytes())
                            .and_then(|_| file.poll_flush())
                    })
                    .and_then(move |_| {
                        let new = dir.join(format!("{}-{}-{}.metrics", name, idx, run_id));

                        debug!("rotate file"; "old" => temp_file.to_str(), "new" => new.to_str());
                        rename(temp_file, new)
                    })
                    .and_then(|_| Ok(()))
                    .map_err(|err| format_err!("{}", err)),
            )
        }

        future::join_all(bulk).and_then(|_| ok(()))
    }

    fn remove(path: PathBuf) -> impl Future<Item=(), Error=Error> {
        trace!("remove file"; "path" => path.to_str());
        remove_file(path.to_owned())
            .map_err(|err| format_err!("{}", err))
            .and_then(|_| ok(()))
    }
}
