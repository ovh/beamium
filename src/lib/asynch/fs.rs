use std::collections::{HashMap, HashSet};
use std::convert::From;
use std::ffi::OsStr;
use std::fs::Metadata;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use failure::{format_err, Error};
use futures::future::{err, join_all, ok};
use futures::{try_ready, Async, Future, Poll, Stream};
use prometheus::GaugeVec;
use tokio::fs::{metadata, read_dir, remove_file};
use tokio::timer::Interval;

use crate::constants::EXTENSION;

lazy_static! {
    static ref BEAMIUM_DIRECTORY_FILES: GaugeVec = register_gauge_vec!(
        opts!(
            "beamium_directory_files",
            "Number of files in the directory"
        ),
        &["directory"]
    )
    .expect("create metric: 'beamium_directory_files'");
}

#[derive(Debug)]
pub struct Scanner {
    interval: Interval,
    dir: PathBuf,
}

impl From<(PathBuf, Duration)> for Scanner {
    fn from(tuple: (PathBuf, Duration)) -> Self {
        let (dir, period) = tuple;

        Self {
            interval: Interval::new(Instant::now(), period),
            dir,
        }
    }
}

impl Stream for Scanner {
    type Item = HashMap<PathBuf, Metadata>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        try_ready!(self.interval.poll().map_err(|err| format_err!("{}", err)));

        let mut scan = Self::scan(self.dir.to_owned());

        loop {
            return match scan.poll()? {
                Async::NotReady => continue,
                Async::Ready(entries) => {
                    let dir = self
                        .dir
                        .to_str()
                        .expect("directory name is utf-8 compliant");

                    BEAMIUM_DIRECTORY_FILES
                        .with_label_values(&[dir])
                        .set(entries.len() as f64);

                    Ok(Async::Ready(Some(entries)))
                }
            };
        }
    }
}

impl Scanner {
    fn scan(path: PathBuf) -> impl Future<Item = HashMap<PathBuf, Metadata>, Error = Error> {
        read_dir(path)
            .map_err(|err| format_err!("{}", err))
            .and_then(move |entries| {
                entries
                    .map_err(|err| format_err!("{}", err))
                    .filter_map(move |entry| {
                        let path = entry.path();
                        if path.extension() != Some(OsStr::new(EXTENSION)) {
                            return None;
                        }

                        Some(path)
                    })
                    .fold(HashSet::new(), |mut acc, path| {
                        ok::<_, Error>({
                            acc.insert(path);
                            acc
                        })
                    })
            })
            .and_then(|entries| {
                let mut bulk = vec![];
                for entry in entries {
                    let entry = entry.to_owned();

                    bulk.push(
                        // In some cases, metadata failed to retrieve the meta of the file.
                        // This occurred when a file is deleted by a sink.
                        metadata(entry.to_owned())
                            .and_then(move |meta| ok(Some((entry, meta))))
                            .or_else(|_| ok(None)),
                    );
                }

                join_all(bulk)
            })
            .and_then(|tuples| {
                let mut bulk = vec![];
                for tuple in tuples {
                    let (entry, meta) = match tuple.to_owned() {
                        Some(tuple) => tuple,
                        None => continue,
                    };

                    let fut = if meta.len() > 0 {
                        ok(Some((entry, meta)))
                    } else {
                        err((entry, meta))
                    };

                    bulk.push(fut.or_else(|(entry, _)| {
                        trace!("remove empty file"; "path" => entry.to_str());
                        remove_file(entry)
                            .map_err(|err| format_err!("{}", err))
                            .and_then(|_| ok::<Option<(PathBuf, Metadata)>, _>(None))
                    }));
                }

                join_all(bulk).and_then(|entries| {
                    ok(entries.iter().filter_map(ToOwned::to_owned).fold(
                        HashMap::new(),
                        |mut acc, (path, meta)| {
                            acc.insert(path, meta);
                            acc
                        },
                    ))
                })
            })
    }
}
