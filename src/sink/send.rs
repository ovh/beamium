use std::cmp;
use std::collections::VecDeque;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

// use flate2::Compression;
// use flate2::write::ZlibEncoder;
use hyper;
use hyper::client::RedirectPolicy;
use hyper::client::Body::ChunkedBody;
use hyper::header::Headers;
use hyper::net::HttpsConnector;
use hyper_native_tls::NativeTlsClient;

use config;

pub fn send_thread(token: &str,
                   token_header: &str,
                   url: &str,
                   todo: Arc<Mutex<VecDeque<PathBuf>>>,
                   timeout: u64,
                   batch_count: u64,
                   batch_size: i64,
                   sigint: Arc<AtomicBool>) {
    loop {
        Data::new(sigint.clone(), todo.clone(), batch_count, batch_size)
            .map(|mut data| match send(token, token_header, url, &mut data, timeout) {
                     Err(err) => {
                         error!("post fail: {}", err);

                         // recover processing file
                         data.processing.take().map(|p| data.processed.push(p));
                         let mut todo = data.todo.lock().unwrap();
                         for entry in data.processed {
                             debug!("pushback sink file {}", format!("{:?}", entry.display()));
                             todo.push_front(entry);
                         }
                     }
                     Ok(_) => {
                         info!("post success - {}", data.sent_lines);
                         for entry in data.processed {
                             debug!("delete sink file {}", format!("{:?}", entry.display()));
                             match fs::remove_file(entry) {
                                 Err(err) => error!(err),
                                 Ok(()) => {}
                             }
                         }
                     }
                 });

        thread::sleep(Duration::from_millis(config::REST_TIME));
        if sigint.load(Ordering::Relaxed) {
            return;
        }
    }
}

fn send(token: &str,
        token_header: &str,
        url: &str,
        data: &mut Data,
        timeout: u64)
        -> Result<(()), Box<Error>> {
    // Send metrics
    let ssl = NativeTlsClient::new().unwrap();
    let mut client = hyper::Client::with_connector(HttpsConnector::new(ssl));
    client.set_write_timeout(Some(Duration::from_secs(timeout)));
    client.set_read_timeout(Some(Duration::from_secs(timeout)));
    client.set_redirect_policy(RedirectPolicy::FollowAll);

    let mut headers = Headers::new();
    headers.set_raw(String::from(token_header), vec![token.into()]);
    // headers.set(ContentEncoding(vec![Encoding::Gzip]));

    let request = client.post(url).headers(headers).body(ChunkedBody(data));
    let mut res = request.send()?;
    if !res.status.is_success() {
        let mut body = String::new();
        res.read_to_string(&mut body)?;
        debug!("data {}", &body);

        return Err(From::from(format!("{}", res.status)));
    }

    Ok(())
}

struct Data {
    batch_count: u64,
    batch_size: i64,
    sigint: Arc<AtomicBool>,
    todo: Arc<Mutex<VecDeque<PathBuf>>>,
    processing: Option<PathBuf>,
    processed: Vec<PathBuf>,
    reader: Option<BufReader<File>>,
    remaining: String,
    sent_lines: usize,
    // encoder: ZlibEncoder<Vec<u8>>,
}

impl Data {
    pub fn new(sigint: Arc<AtomicBool>,
               todo: Arc<Mutex<VecDeque<PathBuf>>>,
               batch_count: u64,
               batch_size: i64)
               -> Option<Data> {
        let mut d = Data {
            sigint: sigint,
            batch_count: batch_count,
            batch_size: batch_size,
            todo: todo,
            processing: None,
            processed: Vec::new(),
            reader: None,
            remaining: String::new(),
            sent_lines: 0,
            // encoder: ZlibEncoder::new(Vec::new(), Compression::Default),
        };

        d.processing = d.get_file();

        match d.processing {
            Some(_) => Some(d),
            None => None,
        }
    }

    fn get_file(&mut self) -> Option<PathBuf> {
        let mut todo = self.todo.lock().unwrap();
        todo.pop_front()
    }

    fn load_file(&self, path: &PathBuf) -> Option<BufReader<File>> {
        debug!("load file {}", path.display());
        match File::open(path) {
            Err(err) => {
                error!(err);
                None
            }
            Ok(f) => Some(BufReader::new(f)),
        }
    }
}

impl io::Read for Data {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // abort on sigint
        if self.sigint.load(Ordering::Relaxed) {
            self.batch_size = 0;
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
        let buf_size = cmp::min(buf.len(), 1024 * 1024);
        let mut idx = 0;

        // Process remaining data from previous chunk
        if self.remaining.len() > 0 {
            idx = move_into(&mut self.remaining, buf, idx)?;
        }

        // send file
        if idx < buf_size && self.reader.is_some() {
            let mut splitted = false;
            {
                let reader = self.reader.as_mut().unwrap();
                let lines = reader.lines();
                for line in lines {
                    let l = line?;
                    self.sent_lines = self.sent_lines + 1;

                    if idx + l.len() + 2 >= buf_size {
                        self.remaining.push_str(&l);
                        self.remaining.push_str("\r\n");

                        idx = move_into(&mut self.remaining, buf, idx)?;

                        splitted = true;
                        break;
                    }

                    {
                        let s = buf.get_mut(idx..idx + l.len())
                            .ok_or(io::ErrorKind::InvalidData)?;
                        s.copy_from_slice(l.as_ref());
                    }
                    idx = idx + l.len();

                    buf[idx] = '\r' as u8;
                    buf[idx + 1] = '\n' as u8;
                    idx = idx + 2;
                }
            }

            // no split occured, entire file has been sent
            if !splitted {
                self.reader = None;
                self.processing.take().map(|p| self.processed.push(p));
            }
        }

        self.batch_size = self.batch_size - idx as i64;

        Ok(idx as usize)
    }
}

fn move_into(from: &mut String, to: &mut [u8], from_idx: usize) -> io::Result<usize> {
    let mut sending = cmp::min(to.len() - from_idx, from.len());
    while !from.is_char_boundary(sending) {
        sending = sending - 1;
    }

    let to_idx = from_idx + sending;

    *from = {
        let (send, remain) = from.split_at(sending);
        let s = to.get_mut(from_idx..to_idx)
            .ok_or(io::ErrorKind::InvalidData)?;
        s.copy_from_slice(send.as_ref());
        String::from(remain)
    };
    Ok(to_idx)
}
