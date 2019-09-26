//! # Conf module.
//!
//! The Conf module provides the beamium configuration.
//! It set defaults and then load config from '/etc', local dir and provided path.
use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use config::{Config, File};
use failure::{format_err, Error, ResultExt};
use humanize_rs::bytes::{Bytes, Unit};
use humanize_rs::duration::parse;
use hyper::Uri;
use regex::{Regex, RegexSet};
use serde_derive::{Deserialize, Serialize};

use glob::glob;

/// `Scraper` config.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub(crate) struct RawScraper {
    pub url: String,
    pub period: String,
    pub format: Option<String>,
    pub metrics: Option<Vec<String>>,
    pub headers: Option<HashMap<String, String>>,
    pub labels: Option<HashMap<String, String>>,
    #[serde(rename = "filtered-labels")]
    pub filtered_labels: Option<Vec<String>>,
    pub pool: Option<usize>,
}

/// `RawSink` config.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub(crate) struct RawSink {
    pub url: String,
    pub token: String,
    #[serde(rename = "token-header")]
    pub token_header: Option<String>,
    pub selector: Option<String>,
    pub ttl: Option<String>,
    pub size: Option<String>,
    pub parallel: Option<usize>,
    #[serde(rename = "keep-alive")]
    pub keep_alive: Option<bool>,
    #[serde(rename = "keep-alive-timeout")]
    pub keep_alive_timeout: Option<String>,
}

/// `RawBackoff` config.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub(crate) struct RawBackoff {
    pub initial: String,
    pub max: String,
    pub multiplier: f64,
    pub randomization: f64,
}

/// `RawParameters` config.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub(crate) struct RawParameters {
    #[serde(rename = "scan-period")]
    pub scan_period: String,
    #[serde(rename = "sink-dir")]
    pub sink_dir: String,
    #[serde(rename = "source-dir")]
    pub source_dir: String,
    #[serde(rename = "batch-size")]
    pub batch_size: String,
    #[serde(rename = "batch-count")]
    pub batch_count: u64,
    #[serde(rename = "log-file")]
    pub log_file: String,
    #[serde(rename = "log-level")]
    pub log_level: usize,
    pub syslog: bool,
    pub timeout: String,
    #[serde(rename = "router-parallel")]
    pub router_parallel: usize,
    pub backoff: RawBackoff,
    pub metrics: Option<String>,
    #[serde(rename = "filesystem-threads")]
    pub filesystem_threads: usize,
}

/// `RawConfig` root.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub(crate) struct RawConf {
    pub sources: Option<HashMap<String, RawScraper>>,
    pub scrapers: Option<HashMap<String, RawScraper>>,
    pub sinks: Option<HashMap<String, RawSink>>,
    pub labels: Option<HashMap<String, String>>,
    pub parameters: RawParameters,
}

impl TryFrom<&PathBuf> for RawConf {
    type Error = Error;

    /// Import configuration from a file
    fn try_from(path: &PathBuf) -> Result<Self, Self::Error> {
        let mut config = Self::initialize()
            .with_context(|err| format!("could not initialize the configuration, {}", err))?;

        config
            .merge(File::from(path.to_owned()).required(true))
            .with_context(|err| format!("could not merge configuration with file, {}", err))?;

        Ok(config.try_into::<Self>()?)
    }
}

impl RawConf {
    fn initialize() -> Result<Config, Error> {
        let mut config = Config::default();

        // parameters
        config.set_default("parameters.scan-period", "1s")?;
        config.set_default("parameters.sink-dir", "sinks")?;
        config.set_default("parameters.source-dir", "sources")?;
        config.set_default("parameters.batch-size", 200_000)?;
        config.set_default("parameters.batch-count", 250)?;
        config.set_default("parameters.log-file", "beamium.log")?;
        config.set_default("parameters.log-level", 4)?;
        config.set_default("parameters.syslog", false)?;
        config.set_default("parameters.timeout", "500s")?;
        config.set_default("parameters.router-parallel", 1)?;
        config.set_default("parameters.filesystem-threads", 100)?;

        // backoff parameters
        config.set_default("parameters.backoff.initial", "500ms")?;
        config.set_default("parameters.backoff.max", "1m")?;
        config.set_default("parameters.backoff.multiplier", 1.5)?;
        config.set_default("parameters.backoff.randomization", 0.3)?;

        Ok(config)
    }

    /// Import configuration from '/etc' and local directory
    pub(crate) fn default() -> Result<Self, Error> {
        let mut config = Self::initialize()?;
        let mut paths = glob("/etc/beamium.d/**/*")?
            .filter_map(|result| {
                if result.is_err() {
                    return None;
                }

                let path = result.expect("error is filter above");

                Some(File::from(path).required(false))
            })
            .collect::<Vec<_>>();

        paths.push(File::with_name("/etc/beamium/config").required(false));

        paths.append(
            &mut glob(format!("{}/.beamium.d/**/*", env!("HOME")).as_str())?
                .filter_map(|result| {
                    if result.is_err() {
                        return None;
                    }

                    let path = result.expect("error is filter above");

                    Some(File::from(path).required(false))
                })
                .collect::<Vec<_>>(),
        );

        paths.push(
            File::with_name(format!("{}/.beamium/config", env!("HOME")).as_str()).required(false),
        );

        config.merge(paths).with_context(|err| {
            format!("could not merge configuration using default paths, {}", err)
        })?;

        Ok(config.try_into::<Self>()?)
    }
}

/// `Scraper` format.
#[derive(Debug, Clone)]
pub enum ScraperFormat {
    Prometheus,
    Sensision,
}

impl TryFrom<&str> for ScraperFormat {
    type Error = Error;

    fn try_from(v: &str) -> Result<Self, Self::Error> {
        match v {
            "sensision" => Ok(ScraperFormat::Sensision),
            "prometheus" => Ok(ScraperFormat::Prometheus),
            _ => Err(format_err!(
                "the scraper's format field should be one of 'sensision' or 'prometheus'"
            )),
        }
    }
}

/// `Scraper` config.
#[derive(Clone, Debug)]
pub struct Scraper {
    pub name: String,
    pub url: Uri,
    pub period: Duration,
    pub format: ScraperFormat,
    pub metrics: Option<RegexSet>,
    pub headers: HashMap<String, String>,
    pub labels: HashMap<String, String>,
    pub filtered_labels: Vec<String>,
    pub pool: usize,
}

impl TryFrom<(String, RawScraper)> for Scraper {
    type Error = Error;

    fn try_from(value: (String, RawScraper)) -> Result<Self, Self::Error> {
        let name = value.0;
        let raw_scraper = value.1;

        let metrics = match raw_scraper.metrics {
            Some(ref patterns) => Some(RegexSet::new(patterns).with_context(|err| {
                format!("Could not create regex set from 'metrics' field, {}", err)
            })?),
            None => None,
        };

        let period = match raw_scraper.period.parse::<u64>() {
            Ok(period) => Duration::from_millis(period),
            Err(_) => parse(raw_scraper.period.as_str())
                .with_context(|err| format!("could not parse 'period' setting, {}", err))?,
        };

        let headers = match raw_scraper.headers {
            None => HashMap::new(),
            Some(headers) => headers,
        };

        let labels = match raw_scraper.labels {
            None => HashMap::new(),
            Some(labels) => labels,
        };

        let pool = match raw_scraper.pool {
            Some(pool) => pool,
            None => 1,
        };

        let filtered_labels = match raw_scraper.filtered_labels {
            Some(filtered_labels) => filtered_labels,
            None => vec![],
        };

        let format = match raw_scraper.format {
            None => String::from("prometheus"),
            Some(format) => format,
        };

        if !raw_scraper.url.starts_with("http://") && !raw_scraper.url.starts_with("https://") {
            Err(format_err!(
                "protocol is missing or incorrect, it should be one of 'http' or 'https'"
            ))
            .with_context(|err| format!("could not parse 'url' setting, {}", err))?;
        }

        Ok(Self {
            name,
            url: raw_scraper
                .url
                .parse::<Uri>()
                .with_context(|err| format!("could not parse 'url' setting, {}", err))?,
            period,
            format: ScraperFormat::try_from(format.as_str())
                .with_context(|err| format!("could not parse 'format' setting, {}", err))?,
            metrics,
            headers,
            labels,
            filtered_labels,
            pool,
        })
    }
}

/// `Sink` config.
#[derive(Clone, Debug)]
pub struct Sink {
    pub name: String,
    pub url: Uri,
    pub token: String,
    pub token_header: String,
    pub selector: Option<Regex>,
    pub ttl: Duration,
    pub size: u64,
    pub parallel: usize,
    pub keep_alive: bool,
    pub keep_alive_timeout: Duration,
}

impl TryFrom<(String, RawSink)> for Sink {
    type Error = Error;

    fn try_from(value: (String, RawSink)) -> Result<Self, Self::Error> {
        let name = value.0;
        let raw_sink = value.1;

        let selector = match raw_sink.selector {
            None => None,
            Some(ref pattern) => {
                Some(Regex::new(&format!("^{}", pattern)).with_context(|err| {
                    format!("could not create regex from 'selector' field, {}", err)
                })?)
            }
        };

        let keep_alive_timeout = match raw_sink.keep_alive_timeout {
            None => Duration::from_secs(3600),
            Some(timeout) => parse(timeout.as_str()).with_context(|err| {
                format!("could not parse 'keep-alive-timeout' setting, {}", err)
            })?,
        };

        let token_header = match raw_sink.token_header {
            None => String::from("X-Warp10-Token"),
            Some(token_header) => token_header,
        };

        let ttl = match raw_sink.ttl {
            None => String::from("1h"),
            Some(ttl) => ttl,
        };

        let ttl = match ttl.parse::<u64>() {
            Ok(ttl) => Duration::from_secs(ttl),
            Err(_) => parse(ttl.as_str())
                .with_context(|err| format!("could not parse 'ttl' setting, {}", err))?,
        };

        let size = match raw_sink.size {
            None => String::from("1Gb"),
            Some(size) => size,
        };

        let size = match size.parse::<u64>() {
            Ok(size) => Bytes::new(size, Unit::Byte)?.size() as u64,
            Err(_) => size
                .parse::<Bytes>()
                .with_context(|err| format!("could not parse 'size' setting, {}", err))?
                .size() as u64,
        };

        let parallel = match raw_sink.parallel {
            None => 1,
            Some(parallel) => parallel,
        };

        let keep_alive = match raw_sink.keep_alive {
            None => true,
            Some(keep_alive) => keep_alive,
        };

        Ok(Self {
            name,
            url: raw_sink
                .url
                .parse::<Uri>()
                .with_context(|err| format!("could not parse 'url' setting, {}", err))?,
            token: raw_sink.token,
            token_header,
            ttl,
            size,
            selector,
            parallel,
            keep_alive,
            keep_alive_timeout,
        })
    }
}

/// `Backoff` config.
#[derive(Clone, Debug)]
pub struct Backoff {
    pub initial: Duration,
    pub max: Duration,
    pub multiplier: f64,
    pub randomization: f64,
}

impl TryFrom<&RawBackoff> for Backoff {
    type Error = Error;

    fn try_from(raw_backoff: &RawBackoff) -> Result<Self, Self::Error> {
        Ok(Self {
            initial: parse(raw_backoff.initial.as_str()).with_context(|err| {
                format!("could not parse 'backoff.initial' setting, {}", err)
            })?,
            max: parse(raw_backoff.max.as_str())
                .with_context(|err| format!("could not parse 'backoff.max' setting, {}", err))?,
            multiplier: raw_backoff.multiplier,
            randomization: raw_backoff.randomization,
        })
    }
}

/// `RawParameters` config.
#[derive(Clone, Debug)]
pub struct Parameters {
    pub scan_period: Duration,
    pub sink_dir: String,
    pub source_dir: String,
    pub batch_size: u64,
    pub batch_count: u64,
    pub log_file: String,
    pub log_level: usize,
    pub syslog: bool,
    pub timeout: Duration,
    pub router_parallel: usize,
    pub backoff: Backoff,
    pub metrics: Option<SocketAddr>,
    pub filesystem_threads: usize,
}

impl TryFrom<RawParameters> for Parameters {
    type Error = Error;

    fn try_from(raw_parameters: RawParameters) -> Result<Self, Self::Error> {
        let scan_period = match raw_parameters.scan_period.parse::<u64>() {
            Ok(scan_period) => Duration::from_millis(scan_period),
            Err(_) => parse(raw_parameters.scan_period.as_str())
                .with_context(|err| format!("could not parse 'scan-period' setting, {}", err))?,
        };

        let timeout = match raw_parameters.timeout.parse::<u64>() {
            Ok(timeout) => Duration::from_secs(timeout),
            Err(_) => parse(raw_parameters.timeout.as_str())
                .with_context(|err| format!("could not parse 'timeout' setting, {}", err))?,
        };

        let batch_size = match raw_parameters.batch_size.parse::<u64>() {
            Ok(batch_size) => batch_size,
            Err(_) => raw_parameters
                .batch_size
                .parse::<Bytes>()
                .with_context(|err| format!("could not parse 'size' setting, {}", err))?
                .size() as u64,
        };

        let metrics = match raw_parameters.metrics {
            None => None,
            Some(metrics) => Some(
                metrics
                    .parse::<SocketAddr>()
                    .with_context(|err| format!("could not parse 'metrics' setting, {}", err))?,
            ),
        };

        Ok(Self {
            scan_period,
            sink_dir: raw_parameters.sink_dir,
            source_dir: raw_parameters.source_dir,
            batch_size,
            batch_count: raw_parameters.batch_count,
            log_file: raw_parameters.log_file,
            log_level: raw_parameters.log_level,
            syslog: raw_parameters.syslog,
            timeout,
            router_parallel: raw_parameters.router_parallel,
            backoff: Backoff::try_from(&raw_parameters.backoff)?,
            metrics,
            filesystem_threads: raw_parameters.filesystem_threads,
        })
    }
}

/// `Config` root.
#[derive(Clone, Debug)]
pub struct Conf {
    pub scrapers: Vec<Scraper>,
    pub sinks: Vec<Sink>,
    pub labels: HashMap<String, String>,
    pub parameters: Parameters,
}

impl TryFrom<RawConf> for Conf {
    type Error = Error;

    fn try_from(raw_config: RawConf) -> Result<Self, Self::Error> {
        let mut scrapers: Vec<Scraper> = vec![];
        let mut sinks: Vec<Sink> = vec![];

        if let Some(raw_scrapers) = raw_config.sources {
            warn!("configuration key 'sources' is deprecated and will be removed in further revision. Please use 'scrapers' instead.");
            for (name, raw_scraper) in raw_scrapers {
                scrapers.push(
                    Scraper::try_from((name.to_owned(), raw_scraper.to_owned()))
                        .with_context(|err| format!("source '{}' is malformed, {}", name, err))?,
                );
            }
        }

        if let Some(raw_scrapers) = raw_config.scrapers {
            for (name, raw_scraper) in raw_scrapers {
                scrapers.push(
                    Scraper::try_from((name.to_owned(), raw_scraper.to_owned()))
                        .with_context(|err| format!("scraper '{}' is malformed, {}", name, err))?,
                );
            }
        }

        if let Some(raw_sinks) = raw_config.sinks {
            for (name, raw_sink) in raw_sinks {
                sinks.push(
                    Sink::try_from((name.to_owned(), raw_sink.to_owned()))
                        .with_context(|err| format!("sink '{}' is malformed, {}", name, err))?,
                )
            }
        }

        let labels = match raw_config.labels {
            None => HashMap::new(),
            Some(map) => map,
        };

        Ok(Self {
            scrapers,
            sinks,
            labels,
            parameters: Parameters::try_from(raw_config.parameters)
                .with_context(|err| format!("'parameters' is malformed, {}", err))?,
        })
    }
}

impl TryFrom<&PathBuf> for Conf {
    type Error = Error;

    /// Import configuration using raw configuration
    fn try_from(path: &PathBuf) -> Result<Self, Self::Error> {
        let config = RawConf::try_from(path)?;

        Ok(Self::try_from(config)?)
    }
}

impl Conf {
    /// Import configuration from '/etc' and local directory using raw config
    pub fn default() -> Result<Self, Error> {
        let config = RawConf::default()?;

        Ok(Self::try_from(config)?)
    }
}
