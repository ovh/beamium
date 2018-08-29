use std::collections::VecDeque;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::ops::Deref;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use hyper;
use hyper_timeout::TimeoutConnector;
use hyper_tls::HttpsConnector;

use futures::future::ok;
use futures::sync::mpsc::Sender;
use futures::task::{current, Task};
use futures::{Async, Future, Poll, Stream};

use tokio_timer::Delay;

use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;

// use flate2::Compression;
// use flate2::write::ZlibEncoder;

use config;
use sink::SinkConfig;

pub fn send_thread(
    client: Arc<
        hyper::Client<TimeoutConnector<HttpsConnector<hyper::client::HttpConnector>>, PayloadBody>,
    >,
    config: SinkConfig,
    todo: Arc<Mutex<VecDeque<PathBuf>>>,
    notify_tx: Sender<Task>,
) -> Box<Future<Item = (), Error = ()>> {
    let mut backoff = ExponentialBackoff::default();
    backoff.initial_interval = config.backoff.initial;
    backoff.max_interval = config.backoff.max;
    backoff.multiplier = config.backoff.multiplier;
    backoff.randomization_factor = config.backoff.randomization;
    backoff.max_elapsed_time = None;

    let work = PayloadStream::new(todo, config.batch_count, config.batch_size, notify_tx)
        .fold(backoff, move |mut backoff, mut p| {
            let mut req: hyper::Request<PayloadBody> =
                hyper::Request::new(hyper::Post, config.url.clone());
            req.set_body(p.body().expect("Body is never None"));
            req.headers_mut()
                .set_raw(config.token_header.clone(), config.token.clone());

            client
                .clone()
                .request(req)
                .and_then(|res| {
                    let status = res.status();
                    // TODO not read body if not debug
                    let body = res
                        .body()
                        .fold(String::new(), |mut acc, chunk| {
                            ok::<String, hyper::Error>({
                                acc.push_str(&String::from_utf8_lossy(&chunk));
                                acc
                            })
                        })
                        .or_else(|_| ok(String::new())); // Default body as an empty string

                    ok(status).join(body)
                })
                .then(move |res| {
                    let state = match res {
                        Err(error) => {
                            error!("HTTP error: {}", error);
                            Err(())
                        }
                        Ok(res) => {
                            let status = res.0;
                            let body = res.1;
                            if status.is_success() {
                                debug!("{}", body);
                                Ok(())
                            } else {
                                error!("HTTP error: {}", status);
                                debug!("{}", body);
                                Err(())
                            }
                        }
                    };

                    match state {
                        Err(_) => {
                            // recover processed file
                            p.abort();
                            Err(backoff)
                        }
                        Ok(_) => {
                            let sent = p.commit();
                            backoff.reset();
                            info!("post success - {}", sent);
                            Ok(backoff)
                        }
                    }
                })
                .or_else(|mut backoff| {
                    let delay = backoff
                        .next_backoff()
                        .expect("never None as max_elapsed_time is None");
                    if delay > config::BACKOFF_WARN {
                        warn!("backoff: {:?}", delay);
                    }
                    Delay::new(Instant::now() + delay).then(|_| ok(backoff))
                })
        })
        .then(|_| ok(()));

    Box::new(work)
}

struct PayloadStream {
    batch_count: u64,
    batch_size: u64,
    todo: Arc<Mutex<VecDeque<PathBuf>>>,
    notify_tx: Sender<Task>,
}

impl PayloadStream {
    pub fn new(
        todo: Arc<Mutex<VecDeque<PathBuf>>>,
        batch_count: u64,
        batch_size: u64,
        notify_tx: Sender<Task>,
    ) -> PayloadStream {
        PayloadStream {
            batch_count,
            batch_size,
            todo,
            notify_tx,
        }
    }
}

impl Stream for PayloadStream {
    type Item = Payload;
    type Error = ();
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.todo.lock().unwrap().pop_front() {
            Some(path) => {
                let s = Payload::new(self.todo.clone(), self.batch_count, self.batch_size, path);
                Ok(Async::Ready(Some(s)))
            }
            None => {
                self.notify_tx
                    .try_send(current())
                    .expect("send could never failed");
                Ok(Async::NotReady)
            }
        }
    }
}

struct Payload {
    todo: Arc<Mutex<VecDeque<PathBuf>>>,
    processed: Arc<Mutex<Vec<PathBuf>>>,
    sent_lines: Arc<AtomicUsize>,
    body: Option<PayloadBody>,
}

impl Payload {
    pub fn new(
        todo: Arc<Mutex<VecDeque<PathBuf>>>,
        batch_count: u64,
        batch_size: u64,
        file: PathBuf,
    ) -> Payload {
        let mut body = PayloadBody::new(todo.clone(), batch_count, batch_size);
        body.load(Some(file));
        Payload {
            todo,
            processed: body.processed(),
            sent_lines: body.sent_lines(),
            body: Some(body),
        }
    }

    fn body(&mut self) -> Option<PayloadBody> {
        self.body.take()
    }

    fn abort(&mut self) {
        let mut processed = self.processed.lock().unwrap();
        let mut todo = self.todo.lock().unwrap();
        for entry in processed.deref_mut().drain(0..) {
            debug!("pushback sink file {}", format!("{:?}", entry.display()));
            todo.push_back(entry);
        }
    }

    fn commit(&mut self) -> usize {
        let processed = self.processed.lock().unwrap();
        for entry in processed.deref() {
            debug!("delete sink file {}", format!("{:?}", entry.display()));
            match fs::remove_file(entry) {
                Err(err) => error!("{}", err),
                Ok(()) => {}
            }
        }

        self.sent_lines.load(Ordering::Relaxed)
    }
}

pub struct PayloadBody {
    remaining_count: u64,
    remaining_size: i64,
    todo: Arc<Mutex<VecDeque<PathBuf>>>,
    processed: Arc<Mutex<Vec<PathBuf>>>,
    reader: Option<BufReader<File>>,
    sent_lines: Arc<AtomicUsize>,
}

impl PayloadBody {
    pub fn new(
        todo: Arc<Mutex<VecDeque<PathBuf>>>,
        batch_count: u64,
        batch_size: u64,
    ) -> PayloadBody {
        PayloadBody {
            remaining_count: batch_count,
            remaining_size: batch_size as i64,
            todo,
            processed: Arc::new(Mutex::new(Vec::new())),
            reader: None,
            sent_lines: Arc::new(AtomicUsize::new(0)),
            // encoder: ZlibEncoder::new(Vec::new(), Compression::Default),
        }
    }

    fn processed(&self) -> Arc<Mutex<Vec<PathBuf>>> {
        self.processed.clone()
    }

    fn sent_lines(&self) -> Arc<AtomicUsize> {
        self.sent_lines.clone()
    }

    fn try_load(&mut self) {
        if self.remaining_count == 0 || self.remaining_size <= 0 {
            self.reader = None;
            return;
        }

        let path = self.todo.lock().unwrap().pop_front();
        self.load(path);
    }

    fn load(&mut self, path: Option<PathBuf>) {
        self.reader = path.and_then(|path| {
            debug!("load file {}", path.display());
            self.processed.lock().unwrap().push(path.clone());

            self.remaining_count -= 1;

            match File::open(path.clone()) {
                Err(err) => {
                    error!("{} - {:?}", err, path);
                    None
                }
                Ok(f) => Some(BufReader::new(f)),
            }
        })
    }
}

impl Stream for PayloadBody {
    type Item = hyper::Chunk;
    type Error = hyper::Error;

    fn poll(&mut self) -> Poll<Option<hyper::Chunk>, hyper::Error> {
        // Hover?
        if self.reader.is_none() {
            return Ok(Async::Ready(None));
        }

        let mut s = String::with_capacity(config::CHUNK_SIZE);
        let mut idx = 0;
        let mut sent_lines = 0;

        // send file
        while idx < config::CHUNK_SIZE {
            let size = self
                .reader
                .as_mut()
                .expect("reader is never None")
                .read_line(&mut s)?;
            if size == 0 {
                self.try_load();

                if self.reader.is_none() {
                    break; // No more work to do
                }
            } else {
                idx += size;
                sent_lines += 1;
            }
        }

        self.sent_lines.fetch_add(sent_lines, Ordering::Relaxed);

        self.remaining_size -= idx as i64;
        Ok(Async::Ready(Some(hyper::Chunk::from(s))))
    }
}
