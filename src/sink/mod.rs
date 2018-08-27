//! # Sink module.
//!
//! The Sink module send metrics to Warp10.
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use hyper;
use hyper_timeout::TimeoutConnector;
use hyper_tls::HttpsConnector;

use tokio_core::reactor::Core;

use futures::future::Shared;
use futures::sync::mpsc::{channel, Sender};
use futures::sync::oneshot;
use futures::task::Task;
use futures::Future;

use slog_scope;

use config;

mod fs;
mod send;

const REACTOR_CLIENT: usize = 30;

#[derive(Debug, Clone)]
pub struct SinkConfig {
    name: String,
    dir: String,
    max_size: u64,
    ttl: u64,
    watch_period: u64,
    parallel: u64,
    url: hyper::Uri,
    timeout: u64,
    keep_alive: bool,
    token: String,
    token_header: String,
    batch_count: u64,
    batch_size: u64,
}

pub struct Sink {
    config: SinkConfig,
    todo: Arc<Mutex<VecDeque<PathBuf>>>,
    handles: Vec<thread::JoinHandle<()>>,
    sigint: (oneshot::Sender<()>, Shared<oneshot::Receiver<()>>),
}

impl Sink {
    pub fn new(sink: &config::Sink, parameters: &config::Parameters) -> Sink {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let config = SinkConfig {
            name: sink.name.clone(),
            dir: parameters.sink_dir.clone(),
            max_size: sink.size,
            ttl: sink.ttl,
            watch_period: parameters.scan_period,
            parallel: sink.parallel,
            url: sink.url.clone(),
            timeout: parameters.timeout,
            keep_alive: sink.keep_alive,
            token: sink.token.clone(),
            token_header: sink.token_header.clone(),
            batch_count: parameters.batch_count,
            batch_size: parameters.batch_size,
        };

        Sink {
            config,
            todo: Arc::new(Mutex::new(VecDeque::new())),
            handles: Vec::new(),
            sigint: (shutdown_tx, shutdown_rx.shared()),
        }
    }

    pub fn start(&mut self) {
        debug!("start sink: {}", self.config.name);
        let (notify_tx, notify_rx) = channel(self.config.parallel as usize);

        // spawn fs thread
        let (todo, sigint) = (self.todo.clone(), self.sigint.1.clone());
        let config = self.config.clone();

        self.handles.push(thread::spawn(move || {
            slog_scope::scope(
                &slog_scope::logger().new(o!("sink" => config.name.clone())),
                || fs::fs_thread(&todo, &config, &sigint, notify_rx),
            );
        }));

        // spawn sender threads
        let reactor_count = (self.config.parallel as f64 / REACTOR_CLIENT as f64).ceil() as u64;
        let client_count = (self.config.parallel as f64 / reactor_count as f64).ceil() as u64;
        for _ in 0..reactor_count {
            let (todo, sigint) = (self.todo.clone(), self.sigint.1.clone());
            let notify_tx: Sender<Task> = notify_tx.clone();
            let config = self.config.clone();

            self.handles.push(thread::spawn(move || {
                slog_scope::scope(
                    &slog_scope::logger().new(o!("sink" => config.name.clone())),
                    || {
                        let mut core = Core::new().expect("Fail to start tokio reactor");
                        let handle = core.handle();
                        let connector =
                            HttpsConnector::new(4, &handle).expect("Fail to start https connector");

                        // Handle connection timeouts
                        let mut tm = TimeoutConnector::new(connector, &handle);
                        tm.set_connect_timeout(Some(Duration::from_secs(config.timeout)));

                        let client = Arc::new(
                            hyper::Client::configure()
                                .body::<send::PayloadBody>()
                                .keep_alive(config.keep_alive)
                                .connector(tm)
                                .build(&handle),
                        );

                        // spawn send threads
                        for _ in 0..client_count {
                            let work = send::send_thread(
                                client.clone(),
                                config.clone(),
                                todo.clone(),
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
