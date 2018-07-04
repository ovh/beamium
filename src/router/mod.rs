//! # Router module.
//!
//! The Router module forward sources to sinks.
use std::thread;
use std::collections::HashMap;
use std::path::PathBuf;
use std::collections::VecDeque;

use std::sync::{Arc, Mutex};
use futures::future::Shared;
use futures::sync::oneshot;
use futures::Future;

use slog_scope;
use config;

mod fs;
mod router;

pub struct Router<'a> {
    todo: Arc<Mutex<VecDeque<PathBuf>>>,
    handles: Vec<thread::JoinHandle<()>>,
    dir: &'a String,
    watch_period: u64,
    parallel: u64,
    sinks: &'a Vec<config::Sink>,
    sink_dir: &'a String,
    labels: &'a HashMap<String, String>,
    sigint: (oneshot::Sender<()>, Shared<oneshot::Receiver<()>>),
}

impl<'a> Router<'a> {
    pub fn new(sinks: &'a Vec<config::Sink>, parameters: &'a config::Parameters, labels: &'a HashMap<String, String>) -> Router<'a> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        Router {
            todo: Arc::new(Mutex::new(VecDeque::new())),
            handles: Vec::new(),
            sigint: (shutdown_tx, shutdown_rx.shared()),
            dir: &parameters.source_dir,
            parallel: parameters.router_parallel,
            sinks: sinks,
            sink_dir: &parameters.sink_dir,
            labels: labels,
            watch_period: parameters.scan_period,
        }
    }

    pub fn start(&mut self) {
        debug!("start router");
        // Build labels string
        let labels: String = self.labels.iter().fold(String::new(), |acc, (k, v)| {
            let sep = if acc.is_empty() { "" } else { "," };
            acc + sep + k + "=" + v
        });

        // spawn fs thread
        let (dir, todo) = (self.dir.clone(), self.todo.clone());
        let (period, sigint) = (self.watch_period, self.sigint.1.clone());

        self.handles.push(thread::spawn(move || {
            slog_scope::scope(
                &slog_scope::logger().new(o!()),
                || fs::fs_thread(&dir, period, todo, sigint),
            );
        }));

        // spawn router threads
        for idx in 0..self.parallel {
            let sinks = self.sinks.clone();
            let sink_dir = self.sink_dir.clone();
            let sigint = self.sigint.1.clone();
            let todo = self.todo.clone();
            let labels = labels.clone();

            self.handles.push(thread::spawn(move || {
                slog_scope::scope(
                    &slog_scope::logger().new(o!()),
                    || {
                      router::router_thread(sinks, sink_dir, todo, labels, sigint, idx);
                    },
                )
            }));
        }
    }

    pub fn stop(self) {
        self.sigint.0.send(()).unwrap(); // FIXME
        // for handle in self.handles {
        //     handle.join().unwrap();
        // }
    }
}

// /// Thread sleeping time.
// const REST_TIME: u64 = 10;

// /// Router loop.
// pub fn router(
//     sinks: &Vec<config::Sink>,
//     labels: &HashMap<String, String>,
//     parameters: &config::Parameters,
//     sigint: Arc<AtomicBool>,
// ) {
//     let labels: String = labels.iter().fold(String::new(), |acc, (k, v)| {
//         let sep = if acc.is_empty() { "" } else { "," };
//         acc + sep + k + "=" + v
//     });

//     loop {
//         let start = time::now_utc();

//         match route(sinks, parameters, &labels, sigint.clone()) {
//             Err(err) => error!("route fail: {}", err),
//             Ok(size) => if size > 0 {
//                 info!("route success - {}", size)
//             },
//         }

//         let elapsed = (time::now_utc() - start).num_milliseconds() as u64;
//         let sleep_time = if elapsed > parameters.scan_period {
//             REST_TIME
//         } else {
//             cmp::max(parameters.scan_period - elapsed, REST_TIME)
//         };
//         for _ in 0..sleep_time / REST_TIME {
//             thread::sleep(Duration::from_millis(REST_TIME));
//             if sigint.load(Ordering::Relaxed) {
//                 return;
//             }
//         }
//     }
// }

// /// Route handle sources forwarding.
// fn route(
//     sinks: &Vec<config::Sink>,
//     parameters: &config::Parameters,
//     labels: &String,
//     sigint: Arc<AtomicBool>,
// ) -> Result<usize, Box<Error>> {
//     let mut proc_size = 0;
//     let mut batch_count = 0;
//     let start = time::now_utc().to_timespec();
//     let run_id = format!("{}#{}", start.sec, start.nsec);

//     loop {
//         if sigint.load(Ordering::Relaxed) {
//             return Ok(proc_size);
//         }
//         let entries = try!(fs::read_dir(&parameters.source_dir));
//         let mut files = Vec::with_capacity(parameters.batch_count as usize);
//         let mut metrics: Vec<String> = Vec::new();

//         // Load metrics
//         let mut batch_size = 0;
//         for (i, entry) in entries.enumerate() {
//             let entry = try!(entry);
//             // Look only for metrics files
//             if entry.path().extension() != Some(OsStr::new("metrics")) {
//                 continue;
//             }

//             // Split metrics in capped batch
//             if i > parameters.batch_count as usize || batch_size > parameters.batch_size as usize {
//                 break;
//             }

//             debug!("open source file {}", format!("{:?}", entry.path()));
//             let file = match read(entry.path()) {
//                 Err(err) => {
//                     warn!("{}", err);
//                     continue;
//                 }
//                 Ok(v) => v,
//             };

//             for line in file.lines() {
//                 match lib::add_labels(&line, labels) {
//                     Ok(v) => metrics.push(v),
//                     Err(_) => {
//                         warn!("bad line {}", &line);
//                         continue;
//                     }
//                 };
//             }

//             files.push(entry.path());
//             batch_size += file.len();
//         }

//         proc_size += metrics.len();
//         batch_count += 1;

//         // Nothing to do
//         if files.len() == 0 {
//             break;
//         }

//         // Setup sinks files
//         let dir = Path::new(&parameters.sink_dir);
//         {
//             let mut sink_files = Vec::with_capacity(sinks.len() as usize);
//             // Open tmp files
//             for sink in sinks {
//                 let sink_file = dir.join(format!("{}.tmp", sink.name));
//                 debug!("open tmp sink file {}", format!("{:?}", sink_file));
//                 sink_files.push(try!(File::create(sink_file)));
//             }

//             // Write metrics
//             debug!("write sink files");
//             for line in metrics {
//                 if line.is_empty() {
//                     continue;
//                 }

//                 for (i, sink) in sinks.iter().enumerate() {
//                     if sink.selector.is_some() {
//                         let selector = sink.selector.as_ref().unwrap();
//                         if !line.split_whitespace()
//                             .nth(1)
//                             .map_or(false, |class| selector.is_match(class))
//                         {
//                             continue;
//                         }
//                     }
//                     try!(sink_files[i].write(line.as_bytes()));
//                     try!(sink_files[i].write(b"\n"));
//                 }
//             }

//             // Flush
//             for i in 0..sinks.len() {
//                 try!(sink_files[i].flush());
//             }
//         }

//         // Rotate
//         for sink in sinks {
//             let dest_file = dir.join(format!("{}-{}-{}.metrics", sink.name, run_id, batch_count));
//             debug!("rotate tmp sink file to {}", format!("{:?}", dest_file));
//             try!(fs::rename(
//                 dir.join(format!("{}.tmp", sink.name)),
//                 dest_file
//             ));
//         }

//         // Delete forwarded data
//         for f in files {
//             debug!("delete source file {}", format!("{:?}", f));
//             try!(fs::remove_file(f));
//         }
//     }

//     Ok(proc_size)
// }

// /// Read a file as String
// fn read(path: PathBuf) -> Result<String, Box<Error>> {
//     let mut file = try!(File::open(path));

//     let mut content = String::new();
//     try!(file.read_to_string(&mut content));

//     Ok(content)
// }
