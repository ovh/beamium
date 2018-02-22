use std::collections::VecDeque;
use std::error::Error;
use std::fs;
use std::fmt;
use std::error;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use hyper;
use hyper_tls::HttpsConnector;
use hyper_timeout::TimeoutConnector;
use futures::{Async, Future, Poll, Stream};
use futures::future::{err, ok};
use std::time::Duration;
use std::ops::Deref;
use std::ops::DerefMut;
// use flate2::Compression;
// use flate2::write::ZlibEncoder;

use config;

const CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Debug)]
enum SendError {
    Io(io::Error),
    Format(Box<Error>),
    Hyper(hyper::Error),
}

impl From<io::Error> for SendError {
    fn from(err: io::Error) -> SendError {
        SendError::Io(err)
    }
}
impl From<Box<Error>> for SendError {
    fn from(err: Box<Error>) -> SendError {
        SendError::Format(err)
    }
}
impl<'a> From<&'a str> for SendError {
    fn from(err: &str) -> SendError {
        SendError::Format(From::from(err))
    }
}
impl From<String> for SendError {
    fn from(err: String) -> SendError {
        SendError::Format(From::from(err))
    }
}
impl From<hyper::Error> for SendError {
    fn from(err: hyper::Error) -> SendError {
        SendError::Hyper(err)
    }
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SendError::Io(ref err) => err.fmt(f),
            SendError::Format(ref err) => err.fmt(f),
            SendError::Hyper(ref err) => err.fmt(f),
        }
    }
}

impl error::Error for SendError {
    fn description(&self) -> &str {
        match *self {
            SendError::Io(ref err) => err.description(),
            SendError::Format(ref err) => err.description(),
            SendError::Hyper(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            SendError::Io(ref err) => Some(err),
            SendError::Format(ref err) => Some(err.as_ref()),
            SendError::Hyper(ref err) => Some(err),
        }
    }
}

enum HttpResult {
    Error(SendError),
    Ok((hyper::StatusCode, String)),
}

pub fn send_thread(
    token: &str,
    token_header: &str,
    url: hyper::Uri,
    todo: Arc<Mutex<VecDeque<PathBuf>>>,
    timeout: u64,
    batch_count: u64,
    batch_size: u64,
    sigint: Arc<AtomicBool>,
) {
    let mut core = ::tokio_core::reactor::Core::new().expect("Fail to start tokio reactor");
    let handle = core.handle();
    let connector = HttpsConnector::new(4, &handle).expect("Fail to start https connector");

    // Handle connection timeouts
    let mut tm = TimeoutConnector::new(connector, &handle);
    tm.set_connect_timeout(Some(Duration::from_secs(timeout)));
    tm.set_read_timeout(Some(Duration::from_secs(timeout)));
    tm.set_write_timeout(Some(Duration::from_secs(timeout)));

    // Client should be shared accross sinks
    let client = hyper::Client::configure()
        .body::<Payload>()
        .keep_alive(true)
        .connector(tm)
        .build(&handle);

    loop {
        Payload::new(sigint.clone(), todo.clone(), batch_count, batch_size).map(|p| {
            let processed = p.processed();

            let mut req: hyper::Request<Payload> = hyper::Request::new(hyper::Post, url.clone());
            req.set_body(p);
            req.headers_mut().set_raw(String::from(token_header), token);

            info!("Req");
            let req = client
                .request(req)
                .and_then(|res| {
                    let status = res.status();
                    // TODO not read body if not debug
                    let body = res.body()
                        .fold(String::new(), |mut acc, chunk| {
                            ok::<String, hyper::Error>({
                                acc.push_str(&String::from_utf8_lossy(&chunk));
                                acc
                            })
                        })
                        .or_else(|_| ok(String::new())); // Default body as an empty string

                    ok(status)
                        .join(body)
                        .and_then(|res| ok(HttpResult::Ok(res)))
                })
                .or_else(|err| ok(HttpResult::Error(SendError::Hyper(err))));

            core.run(req.then(|res: Result<HttpResult, Box<Error>>| match res {
                Err(error) => {
                    crit!("unrecoverable error: {}", error);
                    err(())
                }
                Ok(res) => {
                    let state = match res {
                        HttpResult::Ok(result) => {
                            let status = result.0;
                            if status.is_success() {
                                Ok(())
                            } else {
                                error!("HTTP error: {}", status);
                                Err(())
                            }
                        }
                        HttpResult::Error(error) => {
                            error!("HTTP error: {}", error);
                            Err(())
                        }
                    };

                    match state {
                        Err(_) => {
                            // recover processing file
                            let mut processed = processed.lock().unwrap();
                            let mut todo = todo.lock().unwrap();
                            for entry in processed.deref_mut().drain(0..) {
                                debug!("pushback sink file {}", format!("{:?}", entry.display()));
                                todo.push_front(entry);
                            }
                        }
                        Ok(_) => {
                            info!("post success");
                            // info!("post success - {}", payload.sent_lines);
                            // debug!("{}", b);
                            let processed = processed.lock().unwrap();
                            for entry in processed.deref() {
                                debug!("delete sink file {}", format!("{:?}", entry.display()));
                                match fs::remove_file(entry) {
                                    Err(err) => error!("{}", err),
                                    Ok(()) => {}
                                }
                            }
                        }
                    };
                    ok(())
                }
            }))
        });

        thread::sleep(Duration::from_millis(config::REST_TIME));
        if sigint.load(Ordering::Relaxed) {
            return;
        }
    }
}

struct Payload {
    batch_count: u64,
    batch_size: i64,
    sigint: Arc<AtomicBool>,
    todo: Arc<Mutex<VecDeque<PathBuf>>>,
    processing: Option<PathBuf>,
    processed: Arc<Mutex<Vec<PathBuf>>>,
    reader: Option<BufReader<File>>,
    remaining: String,
    sent_lines: usize,
}

impl Payload {
    pub fn new(
        sigint: Arc<AtomicBool>,
        todo: Arc<Mutex<VecDeque<PathBuf>>>,
        batch_count: u64,
        batch_size: u64,
    ) -> Option<Payload> {
        let mut p = Payload {
            sigint: sigint,
            batch_count: batch_count,
            batch_size: batch_size as i64,
            todo: todo,
            processing: None,
            processed: Arc::new(Mutex::new(Vec::new())),
            reader: None,
            remaining: String::new(),
            sent_lines: 0,
            // encoder: ZlibEncoder::new(Vec::new(), Compression::Default),
        };

        p.processing = p.get_file();

        match p.processing {
            Some(_) => Some(p),
            None => None,
        }
    }

    fn processed(&self) -> Arc<Mutex<Vec<PathBuf>>> {
        self.processed.clone()
    }

    fn get_file(&mut self) -> Option<PathBuf> {
        let mut todo = self.todo.lock().unwrap();
        let file = todo.pop_front();

        match file {
            Some(ref f) => self.processed.lock().unwrap().push(f.clone()),
            None => {}
        }

        file
    }

    fn load_file(&self, path: &PathBuf) -> Option<BufReader<File>> {
        debug!("load file {}", path.display());
        match File::open(path) {
            Err(err) => {
                error!("{}", err);
                None
            }
            Ok(f) => Some(BufReader::new(f)),
        }
    }
}

impl Stream for Payload {
    type Item = hyper::Chunk;
    type Error = hyper::Error;

    fn poll(&mut self) -> Poll<Option<hyper::Chunk>, hyper::Error> {
        // Abort on sigint
        if self.sigint.load(Ordering::Relaxed) && self.processing.is_none() {
            return Ok(Async::Ready(None));
        }

        // Get a file to process
        if self.processing.is_none() && self.batch_count > 0 && self.batch_size > 0 {
            self.processing = self.get_file();
        }

        // Load reader
        if self.reader.is_none() && self.processing.is_some() {
            let to_load = self.processing.as_ref().unwrap();
            self.batch_count = self.batch_count - 1;
            self.reader = self.load_file(to_load);
        }

        // Chunk by 1M or less
        let mut s = String::with_capacity(CHUNK_SIZE);
        let mut idx = 0;

        // Process remaining data from previous chunk
        if self.remaining.len() > 0 {
            idx = self.remaining.len();
            s.push_str(&self.remaining);
            s.clear();
        }

        // if self.processing.is_some() {
        //     Ok(Async::Ready(Some(Ok(hyper::Chunk::from(
        //         self.load_file_as_bytes(self.processing.as_ref().unwrap())
        //             .unwrap(),
        //     )))))
        // } else {
        //     Ok(Async::Ready(None))
        // }

        // send file
        let mut drained = false;
        if idx < CHUNK_SIZE && self.reader.is_some() {
            let r = self.reader.as_mut().unwrap();
            while idx < CHUNK_SIZE {
                let size = r.read_line(&mut s)?; // TODO handle chunk size overflow
                if size == 0 {
                    drained = true; // TODO continue to loop throught new file
                    break;
                }
                idx = idx + size;
                self.sent_lines = self.sent_lines + 1;
            }
        }

        if drained {
            self.reader = None;
            self.processing = None;
        }

        self.batch_size = self.batch_size - idx as i64;

        if s.len() > 0 {
            Ok(Async::Ready(Some(hyper::Chunk::from(s))))
        } else {
            Ok(Async::Ready(None))
        }
    }
}
