use std::collections::VecDeque;
use std::convert::From;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use crossbeam::queue::SegQueue;
use failure::{format_err, Error, ResultExt};
use futures::future::{err, join_all, ok};
use futures::{try_ready, Poll, Stream};
use hyper::body::{Chunk, Payload};
use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
use hyper::{Client, Method, Request};
use hyper_rustls::HttpsConnector;
use prometheus::CounterVec;
use tokio::fs::remove_file;
use tokio::prelude::*;
use tokio::timer::{Delay, Interval};

use crate::conf;
use crate::constants::{BACKOFF_WARN, CHUNK_SIZE, NUMBER_DNS_WORKER_THREADS, THREAD_SLEEP};

/// Alias for the hyper's https client
type HttpsClient = Client<HttpsConnector<HttpConnector<GaiResolver>>, Body>;

lazy_static! {
    static ref BEAMIUM_PUSH_DP: CounterVec = register_counter_vec!(
        opts!("beamium_push_datapoints", "Number of datapoints pushed"),
        &["sink"]
    )
    .expect("create metric: 'beamium_fetch_datapoints'");
    static ref BEAMIUM_PUSH_ERRORS: CounterVec = register_counter_vec!(
        opts!("beamium_push_errors", "Number of push error"),
        &["sink"]
    )
    .expect("create metric: 'beamium_push_errors'");
    static ref BEAMIUM_PUSH_HTTP_STATUS: CounterVec = register_counter_vec!(
        opts!("beamium_push_http_status", "Push response http status code"),
        &["sink", "status"]
    )
    .expect("create metric: 'beamium_push_http_status'");
}

pub enum State {
    Idle,
    Sending(Box<dyn Future<Item = (), Error = Error> + Send>),
    Waiting,
    Backoff(Box<dyn Future<Item = (), Error = Error> + Send>),
}

pub struct Sender {
    interval: Interval,
    queue: Arc<Mutex<VecDeque<PathBuf>>>,
    files: Arc<SegQueue<PathBuf>>,
    conf: Arc<conf::Sink>,
    params: Arc<conf::Parameters>,
    client: Arc<HttpsClient>,
    state: State,
    backoff: ExponentialBackoff,
}

impl
    From<(
        Arc<Mutex<VecDeque<PathBuf>>>,
        Arc<conf::Sink>,
        Arc<conf::Parameters>,
    )> for Sender
{
    fn from(
        tuple: (
            Arc<Mutex<VecDeque<PathBuf>>>,
            Arc<conf::Sink>,
            Arc<conf::Parameters>,
        ),
    ) -> Self {
        let (queue, conf, params) = tuple;
        let client = Client::builder()
            .keep_alive(conf.keep_alive)
            .keep_alive_timeout(conf.keep_alive_timeout)
            .build(HttpsConnector::new(NUMBER_DNS_WORKER_THREADS));

        let mut backoff = ExponentialBackoff::default();

        backoff.initial_interval = params.backoff.initial;
        backoff.max_interval = params.backoff.max;
        backoff.multiplier = params.backoff.multiplier;
        backoff.randomization_factor = params.backoff.randomization;
        backoff.max_elapsed_time = None;

        Self {
            interval: Interval::new(Instant::now(), THREAD_SLEEP),
            queue,
            files: Arc::new(SegQueue::new()),
            conf,
            params,
            client: Arc::new(client),
            state: State::Idle,
            backoff,
        }
    }
}

impl Stream for Sender {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let conf = self.conf.to_owned();
        match &mut self.state {
            State::Idle => {
                let is_empty = {
                    match self.queue.try_lock() {
                        Ok(queue) => queue.is_empty(),
                        Err(_) => {
                            // Wait in order to retrieve the lock later
                            true
                        }
                    }
                };

                if is_empty {
                    self.state = State::Waiting;
                    return Ok(Async::Ready(Some(())));
                }

                let body = Body::from((
                    self.queue.to_owned(),
                    self.conf.to_owned(),
                    self.params.to_owned(),
                ));

                self.files = body.get_files();
                let request = Request::builder()
                    .method(Method::POST)
                    .uri(self.conf.url.to_owned())
                    .header(self.conf.token_header.as_str(), self.conf.token.as_str())
                    .body(body)
                    .with_context(|err| format!("could not create the http request, {}", err))?;

                let name = self.conf.name.to_owned();
                let sink = self.conf.name.to_owned();
                let files = self.files.to_owned();

                let request = self
                    .client
                    .to_owned()
                    .request(request)
                    .timeout(self.params.timeout.to_owned())
                    .map_err(|err| format_err!("{}", err))
                    .and_then(move |res| {
                        let status = res.status();

                        BEAMIUM_PUSH_HTTP_STATUS
                            .with_label_values(&[sink.as_str(), status.as_str()])
                            .inc();
                        if status.is_success() {
                            info!("post success"; "sink" => sink.as_str());
                            return ok(());
                        }

                        err(format_err!("http request failed, got {}", status.as_u16()))
                    })
                    .and_then(move |_| {
                        let mut bulk = vec![];
                        while let Ok(file) = files.pop() {
                            trace!("remove file"; "sink" => name.as_str(), "path" => file.to_str());
                            bulk.push(Sender::remove(file));
                        }

                        join_all(bulk).and_then(|_| ok(()))
                    });

                self.state = State::Sending(Box::new(request));
                Ok(Async::Ready(Some(())))
            }
            State::Sending(req) => match req.poll() {
                Err(err) => {
                    error!("post failed"; "sink" => conf.name.as_str(), "error" => err.to_string());
                    {
                        let mut queue = self.queue.lock().map_err(|err| format_err!("{}", err))?;
                        while let Ok(file) = self.files.pop() {
                            debug!("push back file in queue"; "sink" => conf.name.as_str(), "path" => file.to_str());
                            queue.push_front(file);
                        }
                    }

                    BEAMIUM_PUSH_ERRORS
                        .with_label_values(&[conf.name.as_str()])
                        .inc();

                    let delay = self
                        .backoff
                        .next_backoff()
                        .expect("never None as max_elapsed_time is None");
                    if delay > BACKOFF_WARN {
                        warn!("backoff on request"; "delay" => format!("{:?}", delay), "sink" => conf.name.as_str());
                    }

                    let delay =
                        Delay::new(Instant::now() + delay).map_err(|err| format_err!("{}", err));

                    self.state = State::Backoff(Box::new(delay));
                    Ok(Async::Ready(Some(())))
                }
                Ok(poll) => {
                    if let Async::Ready(_) = poll {
                        self.backoff.reset();
                        self.state = State::Idle;
                        return Ok(Async::Ready(Some(())));
                    }

                    // Should return not ready else all cpus goes straight to full usage
                    Ok(Async::NotReady)
                }
            },
            State::Waiting => {
                try_ready!(self.interval.poll().map_err(|err| format_err!("{}", err)));

                self.state = State::Idle;
                Ok(Async::Ready(Some(())))
            }
            State::Backoff(delay) => {
                try_ready!(delay.poll().map_err(|err| format_err!("{}", err)));

                self.state = State::Idle;
                Ok(Async::Ready(Some(())))
            }
        }
    }
}

impl Sender {
    fn remove(path: PathBuf) -> impl Future<Item = (), Error = Error> {
        remove_file(path.to_owned())
            .map_err(|err| format_err!("{}", err))
            .and_then(|_| ok(()))
    }
}

pub struct Body {
    queue: Arc<Mutex<VecDeque<PathBuf>>>,
    conf: Arc<conf::Sink>,
    params: Arc<conf::Parameters>,
    files: Arc<SegQueue<PathBuf>>,
    current_batch_size: u64,
    current_batch_count: u64,
    reader: Option<BufReader<File>>,
}

impl
    From<(
        Arc<Mutex<VecDeque<PathBuf>>>,
        Arc<conf::Sink>,
        Arc<conf::Parameters>,
    )> for Body
{
    fn from(
        tuple: (
            Arc<Mutex<VecDeque<PathBuf>>>,
            Arc<conf::Sink>,
            Arc<conf::Parameters>,
        ),
    ) -> Self {
        let (queue, conf, params) = tuple;

        Self {
            queue,
            conf,
            params,
            files: Arc::new(SegQueue::new()),
            current_batch_size: 0,
            current_batch_count: 0,
            reader: None,
        }
    }
}

impl Payload for Body {
    type Data = Chunk;
    type Error = Error;

    fn poll_data(&mut self) -> Poll<Option<Self::Data>, Self::Error> {
        match &mut self.reader {
            None => {
                if self.current_batch_count >= self.params.batch_count
                    || self.current_batch_size >= self.params.batch_size
                {
                    // We reach the maximum of the batch
                    return Ok(Async::Ready(None));
                }

                let path = {
                    match self.queue.try_lock() {
                        Ok(mut queue) => queue.pop_front(),
                        Err(_) => {
                            task::current().notify();
                            return Ok(Async::NotReady);
                        }
                    }
                };

                let path = match path {
                    Some(path) => path,
                    None => {
                        // We don't have more files to send
                        return Ok(Async::Ready(None));
                    }
                };

                trace!("open file"; "sink" => self.conf.name.as_str(), "path" => path.to_str());
                self.reader = Some(BufReader::new(File::open(path.to_owned())?));
                self.files.push(path);
                self.current_batch_count += 1;

                task::current().notify();
                Ok(Async::NotReady)
            }
            Some(reader) => {
                let mut acc = String::new();
                let mut len = 0;

                while len < CHUNK_SIZE {
                    let mut line = String::new();
                    let mut line_len = reader.read_line(&mut line)? as u64;

                    if line_len == 0 {
                        // We have read all the file
                        trace!("we have read all the file");
                        self.reader = None;
                        break;
                    }

                    if &line == "\n" {
                        continue;
                    }

                    if !line.ends_with('\n') {
                        line += "\n";
                        line_len += "\n".len() as u64;
                    }

                    acc += &line;
                    len += line_len;

                    self.current_batch_size += line_len;

                    BEAMIUM_PUSH_DP
                        .with_label_values(&[self.conf.name.as_str()])
                        .inc();
                }

                Ok(Async::Ready(Some(Chunk::from(acc))))
            }
        }
    }
}

impl Body {
    pub fn get_files(&self) -> Arc<SegQueue<PathBuf>> {
        self.files.to_owned()
    }
}
