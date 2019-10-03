//! # Scraper module.
//!
//! The Scraper module fetch metrics from an HTTP endpoint.
use std::convert::From;
use std::path::Path;
use std::process::abort;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use failure::{format_err, Error};
use futures::future::{err, ok};
use futures::{Future, Stream};
use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
use hyper::{Body, Client, Method, Request};
use hyper_rustls::HttpsConnector;
use prometheus::CounterVec;
use time::now_utc;
use tokio::fs::{rename, File};
use tokio::io::AsyncWrite;
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::timer::Interval;

use crate::conf;
use crate::constants::NUMBER_DNS_WORKER_THREADS;
use crate::lib::transcompiler::Transcompiler;
use crate::lib::{add_labels, remove_labels};
use crate::lib::{Named, Runner};

/// Alias for the hyper's https client
type HttpsClient = Client<HttpsConnector<HttpConnector<GaiResolver>>, Body>;

lazy_static! {
    static ref BEAMIUM_FETCH_DP: CounterVec = register_counter_vec!(
        opts!("beamium_fetch_datapoints", "Number of datapoints fetched"),
        &["scraper"]
    )
    .expect("create metric: 'beamium_fetch_datapoints'");
    static ref BEAMIUM_FETCH_ERRORS: CounterVec = register_counter_vec!(
        opts!("beamium_fetch_errors", "Number of fetch errors"),
        &["scraper"]
    )
    .expect("create metric: 'beamium_fetch_errors'");
}

#[derive(Clone, Debug)]
pub struct Scraper {
    conf: Arc<conf::Scraper>,
    params: Arc<conf::Parameters>,
    client: Arc<HttpsClient>,
}

impl From<(conf::Scraper, conf::Parameters)> for Scraper {
    fn from(tuple: (conf::Scraper, conf::Parameters)) -> Self {
        let (conf, params) = tuple;
        let client = Client::builder()
            .keep_alive(true)
            .keep_alive_timeout(params.timeout)
            .build(HttpsConnector::new(NUMBER_DNS_WORKER_THREADS));

        Self {
            conf: arc!(conf),
            params: arc!(params),
            client: arc!(client),
        }
    }
}

impl Named for Scraper {
    fn name(&self) -> String {
        self.conf.name.to_owned()
    }
}

impl Runner for Scraper {
    type Error = Error;

    fn start(&self, rt: &mut Runtime) -> Result<(), Self::Error> {
        // Owned variables by creating a new reference using Arc.
        let name = self.name();
        let conf = self.conf.to_owned();
        let params = self.params.to_owned();
        let client = self.client.to_owned();

        let executor = rt.executor();

        // Create a ticker for the scraper for the configured period
        let ticker = Interval::new(Instant::now(), conf.period.to_owned())
            .map_err(|err| format_err!("{}", err))
            .for_each(move |_| {
                // Owned variables by creating a new reference using Arc.
                let name = conf.name.to_owned();
                let conf = conf.to_owned();
                let conf2 = conf.to_owned();
                let params = params.to_owned();
                let compiler = Transcompiler::new(conf.format.to_owned());

                let mut request = Request::builder();
                let request = request.method(Method::GET).uri(conf.url.to_owned());

                for (header, value) in conf.headers.to_owned() {
                    request.header(header.as_str(), value.as_str());
                }

                info!("fetch success"; "uri" => conf.url.to_string(), "scraper" => name.as_str());
                let request = try_future!(request.body(Body::empty()));
                let process = Self::fetch(&client, request, params.timeout)
                    .and_then(move |body| Self::process(&compiler, &body, &conf))
                    .and_then(move |lines| {
                        BEAMIUM_FETCH_DP.with_label_values(&[conf2.name.as_str()]).inc_by(lines.len() as f64);
                        Self::write(lines, &conf2, &params)
                    })
                    .map_err(move |err| {
                        BEAMIUM_FETCH_ERRORS.with_label_values(&[name.as_str()]).inc();
                        error!("fetch failed"; "error" => err.to_string(), "scraper" => name.as_str())
                    });

                // Spawn the request on executor to send it
                executor.spawn(process);

                // return that everything is good
                ok(())
            })
            .map_err(move |err| {
                crit!("could not handle ticker"; "error" => err.to_string(), "scraper" => name);
                sleep(Duration::from_millis(100)); // Sleep the time to display the message
                abort();
            });

        // Spawn the ticker on the runtime
        rt.spawn(ticker);

        Ok(())
    }
}

impl Scraper {
    /// Fetch the source of the scraper using the http(s) [`Client`], the given [`Request`] and the
    /// timeout [`Duration`].
    fn fetch(
        client: &HttpsClient,
        request: Request<Body>,
        timeout: Duration,
    ) -> impl Future<Item = String, Error = Error> {
        client
            .request(request)
            .map_err(|err| format_err!("{}", err))
            .timeout(timeout)
            .map_err(|err| format_err!("{}", err))
            .and_then(|response| {
                let status = response.status();
                if !status.is_success() {
                    return err(format_err!("http request failed, got: {}", status.as_u16()));
                }

                ok(response
                    .into_body()
                    .concat2()
                    .map_err(|err| format_err!("{}", err)))
            })
            .flatten()
            .and_then(|body| ok(String::from_utf8_lossy(&body).to_string()))
    }

    /// Process scraper's data in order to add/remove labels and format time series into sensision
    /// format.
    fn process(
        transcompiler: &Transcompiler,
        body: &str,
        conf: &conf::Scraper,
    ) -> impl Future<Item = Vec<String>, Error = Error> {
        let mut lines = vec![];
        let labels: Vec<String> = conf
            .labels
            .to_owned()
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();

        let labels = labels.join(",");
        for line in body.lines() {
            let mut line = try_future!(transcompiler.format(line));
            if line.is_empty() {
                continue;
            }

            if let Some(ref regex) = &conf.metrics {
                if !regex.is_match(&line) {
                    continue;
                }
            }

            if !line.starts_with('=') {
                line = try_future!(add_labels(&line, &labels));
                line = try_future!(remove_labels(&line, &conf.filtered_labels));
            }

            lines.push(line);
        }

        ok(lines)
    }

    /// Write time series into the disk
    fn write(
        lines: Vec<String>,
        conf: &conf::Scraper,
        params: &conf::Parameters,
    ) -> impl Future<Item = (), Error = Error> {
        let start = now_utc();
        let now =
            start.to_timespec().sec * 1_000_000 + (i64::from(start.to_timespec().nsec) / 1000);

        let dir = Path::new(&params.source_dir);

        let mut batch_size = 0;
        let mut batch_count = -1;
        let mut chunks = vec![];
        let mut chunk = vec![];
        for line in lines {
            batch_size += line.len() as u64;
            if batch_size > params.batch_size && !line.starts_with('=') {
                batch_size = 0;
                batch_count += 1;

                let file_name = format!("{}-{}-{}.tmp", conf.name, now, batch_count);
                let dir = dir.to_owned();
                let batch_count = batch_count.to_owned();
                let now = now.to_owned();
                let name = conf.name.to_owned();
                let name2 = conf.name.to_owned();
                let temp_file = dir.join(file_name.to_owned());

                debug!("create file"; "scraper" => name.to_owned(), "file" => temp_file.to_str());
                chunks.push(
                    File::create(temp_file.to_owned())
                        .and_then(move |mut file| {
                            trace!("write chunk on file"; "scraper" => name, "file" => temp_file.to_str());
                            file.poll_write((chunk.join("\n") + "\n").as_bytes())
                                .and_then(|_| file.poll_flush())
                        })
                        .and_then(move |_| {
                            let old = dir.join(file_name);
                            let new = dir.join(format!("{}-{}-{}.metrics", name2, now, batch_count));

                            debug!("rotate source file"; "scraper" => name2, "old" => old.to_str(), "new" => new.to_str());
                            rename(old, new)
                        })
                        .and_then(|_| ok(()))
                        .map_err(|err| format_err!("{}", err)),
                );

                chunk = vec![];
            }

            chunk.push(line);
        }

        let bulk = future::join_all(chunks).and_then(|_| Ok(()));

        batch_count += 1;

        let name = conf.name.to_owned();
        let name2 = conf.name.to_owned();
        let dir = dir.to_owned();
        let file_name = format!("{}-{}-{}.tmp", conf.name, now, batch_count);
        let temp_file = dir.join(file_name.to_owned());

        trace!("create tmp source file"; "scraper" => name, "file" => temp_file.to_str());
        let chunk = File::create(temp_file.to_owned())
            .and_then(move |mut file| {
                file.poll_write((chunk.join("\n") + "\n").as_bytes())
                    .and_then(|_| file.poll_flush())
            })
            .and_then(move |_| {
                let old = dir.join(file_name);
                let new = dir.join(format!("{}-{}-{}.metrics", name2, now, batch_count));

                debug!("rotate file"; "scraper" => name2, "old" => old.to_str(), "new" => new.to_str());
                rename(old, new)
            })
            .and_then(|_| ok(()))
            .map_err(|err| format_err!("{}", err));

        bulk.join(chunk).and_then(|_| ok(()))
    }
}
