//! # Sink module.
//!
//! The Sink module send metrics to Warp10.
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use hyper;

use slog_scope;

use config;

mod fs;
mod send;

pub struct Sink<'a> {
    name: &'a String,
    todo: Arc<Mutex<VecDeque<PathBuf>>>,
    handles: Vec<thread::JoinHandle<()>>,
    sigint: Arc<AtomicBool>,
    dir: &'a String,
    watch_period: u64,
    timeout: u64,
    token: &'a String,
    token_header: &'a String,
    url: &'a hyper::Uri,
    batch_count: u64,
    batch_size: u64,
    max_size: u64,
    ttl: u64,
    parallel: u64,
}

impl<'a> Sink<'a> {
    pub fn new(sink: &'a config::Sink, parameters: &'a config::Parameters) -> Sink<'a> {
        Sink {
            name: &sink.name,
            todo: Arc::new(Mutex::new(VecDeque::new())),
            handles: Vec::new(),
            sigint: Arc::new(AtomicBool::new(false)),
            dir: &parameters.sink_dir,
            watch_period: parameters.scan_period,
            timeout: parameters.timeout,
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
        slog_scope::scope(
            &slog_scope::logger().new(o!("sink" => self.name.clone())),
            || {
                // spawn fs thread
                let (name, dir, period, todo, sigint) = (
                    self.name.clone(),
                    self.dir.clone(),
                    self.watch_period,
                    self.todo.clone(),
                    self.sigint.clone(),
                );
                let (max_size, ttl) = (self.max_size, self.ttl);

                self.handles.push(thread::spawn(move || {
                    slog_scope::scope(
                        &slog_scope::logger().new(o!("sink" => name.clone())),
                        || fs::fs_thread(&name, &dir, period, todo, max_size, ttl, sigint),
                    );
                }));

                // spawn send threads
                for _ in 0..self.parallel {
                    let (name, sigint) = (self.name.clone(), self.sigint.clone());
                    let todo = self.todo.clone();
                    let (url, timeout) = (self.url.clone(), self.timeout);
                    let (token, token_header) = (self.token.clone(), self.token_header.clone());
                    let (batch_count, batch_size) = (self.batch_count, self.batch_size);

                    self.handles.push(thread::spawn(move || {
                        slog_scope::scope(
                            &slog_scope::logger().new(o!("sink" => name.clone())),
                            || {
                                send::send_thread(
                                    &token,
                                    &token_header,
                                    url,
                                    todo,
                                    timeout,
                                    batch_count,
                                    batch_size,
                                    sigint,
                                )
                            },
                        );
                    }));
                }
            },
        );
    }

    pub fn stop(self) {
        self.sigint.store(true, Ordering::SeqCst);
        for handle in self.handles {
            handle.join().unwrap();
        }
    }
}
