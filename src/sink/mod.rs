//! # Sink module.
//!
//! The Sink module send metrics to Warp10.
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use hyper;
use hyper_tls::HttpsConnector;
use hyper_timeout::TimeoutConnector;

use tokio_core::reactor::Core;

use futures::future::Shared;
use futures::Future;
use futures::sync::oneshot;
use futures::sync::mpsc::{channel, Sender};
use futures::task::Task;

use slog_scope;

use config;

mod fs;
mod send;

const REACTOR_CLIENT: usize = 30;

pub struct Sink<'a> {
    name: &'a String,
    todo: Arc<Mutex<VecDeque<PathBuf>>>,
    handles: Vec<thread::JoinHandle<()>>,
    dir: &'a String,
    watch_period: u64,
    timeout: u64,
    keep_alive: bool,
    token: &'a String,
    token_header: &'a String,
    url: &'a hyper::Uri,
    batch_count: u64,
    batch_size: u64,
    max_size: u64,
    ttl: u64,
    parallel: u64,
    sigint: (oneshot::Sender<()>, Shared<oneshot::Receiver<()>>),
}

impl<'a> Sink<'a> {
    pub fn new(sink: &'a config::Sink, parameters: &'a config::Parameters) -> Sink<'a> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        Sink {
            name: &sink.name,
            todo: Arc::new(Mutex::new(VecDeque::new())),
            handles: Vec::new(),
            sigint: (shutdown_tx, shutdown_rx.shared()),
            dir: &parameters.sink_dir,
            watch_period: parameters.scan_period,
            timeout: parameters.timeout,
            keep_alive: sink.keep_alive,
            token: &sink.token,
            token_header: &sink.token_header,
            url: &sink.url,
            batch_count: parameters.batch_count,
            batch_size: parameters.batch_size,
            max_size: sink.size,
            ttl: sink.ttl,
            parallel: sink.parallel,
        }
    }

    pub fn start(&mut self) {
        debug!("start sink: {}", self.name);
        let (notify_tx, notify_rx) = channel(self.parallel as usize);

        // spawn fs thread
        let (name, dir, todo) = (self.name.clone(), self.dir.clone(), self.todo.clone());
        let (period, sigint) = (self.watch_period, self.sigint.1.clone());
        let (max_size, ttl) = (self.max_size, self.ttl);

        self.handles.push(thread::spawn(move || {
            slog_scope::scope(
                &slog_scope::logger().new(o!("sink" => name.clone())),
                || fs::fs_thread(&name, &dir, period, todo, max_size, ttl, sigint, notify_rx),
            );
        }));

        // spawn sender threads
        let reactor_count = (self.parallel as f64 / REACTOR_CLIENT as f64).ceil() as u64;
        let client_count = (self.parallel as f64 / reactor_count as f64).ceil() as u64;
        for _ in 0..reactor_count {
            let (name, sigint) = (self.name.clone(), self.sigint.1.clone());
            let todo = self.todo.clone();
            let url = self.url.clone();
            let (timeout, keep_alive) = (self.timeout, self.keep_alive);
            let (token, token_header) = (self.token.clone(), self.token_header.clone());
            let (batch_count, batch_size) = (self.batch_count, self.batch_size);
            let notify_tx: Sender<Task> = notify_tx.clone();

            self.handles.push(thread::spawn(move || {
                slog_scope::scope(
                    &slog_scope::logger().new(o!("sink" => name.clone())),
                    || {
                        let mut core = Core::new().expect("Fail to start tokio reactor");
                        let handle = core.handle();
                        let connector =
                            HttpsConnector::new(4, &handle).expect("Fail to start https connector");

                        // Handle connection timeouts
                        let mut tm = TimeoutConnector::new(connector, &handle);
                        tm.set_connect_timeout(Some(Duration::from_secs(timeout)));
                        tm.set_read_timeout(Some(Duration::from_secs(timeout)));
                        tm.set_write_timeout(Some(Duration::from_secs(timeout)));

                        let client = Arc::new(
                            hyper::Client::configure()
                                .body::<send::PayloadBody>()
                                .keep_alive(keep_alive)
                                .connector(tm)
                                .build(&handle),
                        );

                        // spawn send threads
                        for _ in 0..client_count {
                            let work = send::send_thread(
                                client.clone(),
                                token.clone(),
                                token_header.clone(),
                                url.clone(),
                                todo.clone(),
                                batch_count,
                                batch_size,
                                notify_tx.clone(),
                            );

                            core.handle().spawn(work);
                        }

                        core.run(sigint).expect("Sigint could not fail");
                    },
                )
            }));
        }
    }

    pub fn stop(self) {
        self.sigint.0.send(()).unwrap();
        for handle in self.handles {
            handle.join().unwrap();
        }
    }
}
