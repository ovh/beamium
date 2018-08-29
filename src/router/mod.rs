//! # Router module.
//!
//! The Router module forward sources to sinks.
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use config;
use slog_scope;

mod fs;
mod route;

#[derive(Debug, Clone)]
pub struct RouterConfig {
    dir: String,
    watch_period: u64,
    parallel: u64,
    sinks: Vec<config::Sink>,
    sink_dir: String,
    labels: String,
}

pub struct Router {
    config: RouterConfig,
    todo: Arc<Mutex<VecDeque<PathBuf>>>,
    handles: Vec<thread::JoinHandle<()>>,
    sigint: Arc<AtomicBool>,
}

impl Router {
    pub fn new(
        sinks: &[config::Sink],
        parameters: &config::Parameters,
        labels: &HashMap<String, String>,
    ) -> Router {
        let sigint = Arc::new(AtomicBool::new(false));
        // Build labels string
        let labels: String = labels.iter().fold(String::new(), |acc, (k, v)| {
            let sep = if acc.is_empty() { "" } else { "," };
            acc + sep + k + "=" + v
        });

        let config = RouterConfig {
            dir: parameters.source_dir.clone(),
            watch_period: parameters.scan_period,
            parallel: parameters.router_parallel,
            sinks: sinks.to_owned(),
            sink_dir: parameters.sink_dir.clone(),
            labels,
        };

        Router {
            config,
            todo: Arc::new(Mutex::new(VecDeque::new())),
            handles: Vec::new(),
            sigint: sigint,
        }
    }

    pub fn start(&mut self) {
        debug!("start router");

        // spawn fs thread
        let (todo, sigint) = (self.todo.clone(), self.sigint.clone());
        let config = self.config.clone();

        self.handles.push(thread::spawn(move || {
            slog_scope::scope(&slog_scope::logger().new(o!()), || {
                fs::fs_thread(&config, &todo, &sigint)
            });
        }));

        // spawn router threads
        for idx in 0..self.config.parallel {
            let (todo, sigint) = (self.todo.clone(), self.sigint.clone());
            let config = self.config.clone();

            self.handles.push(thread::spawn(move || {
                slog_scope::scope(&slog_scope::logger().new(o!()), || {
                    route::route_thread(&config, &todo, &sigint, idx);
                })
            }));
        }
    }

    pub fn stop(self) {
        self.sigint.store(true, Ordering::SeqCst);
        for handle in self.handles {
            handle.join().unwrap();
        }
    }
}
