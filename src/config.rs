//! # Config module.
//!
//! The Config module provides the beamium configuration.
//! It set defaults and then load config from '/etc', local dir and provided path.

use cast;
use humantime::parse_duration;
use hyper;
use regex;
use slog;
use std::collections::HashMap;
use std::error;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io;
use std::io::Read;
use std::path::Path;
use std::string::String;
use std::time::Duration;
use yaml_rust::{ScanError, YamlLoader};

pub const REST_TIME: u64 = 10;
pub const BACKOFF_WARN: Duration = Duration::from_millis(1000);
pub const CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Debug, Clone)]
/// Config root.
pub struct Config {
    pub scrapers: Vec<Scraper>,
    pub sinks: Vec<Sink>,
    pub labels: HashMap<String, String>,
    pub parameters: Parameters,
}

#[derive(Debug, Clone)]
/// Scraper config.
pub struct Scraper {
    pub name: String,
    pub url: hyper::Uri,
    pub period: u64,
    pub format: ScraperFormat,
    pub metrics: Option<regex::RegexSet>,
    pub headers: HashMap<String, String>,
    pub labels: HashMap<String, String>,
    pub filtered_labels: Vec<String>,
}

#[derive(Debug, Clone)]
/// Scraper format.
pub enum ScraperFormat {
    Prometheus,
    Sensision,
}

#[derive(Debug, Clone)]
/// Sink config.
pub struct Sink {
    pub name: String,
    pub url: hyper::Uri,
    pub token: String,
    pub token_header: String,
    pub selector: Option<regex::Regex>,
    pub ttl: u64,
    pub size: u64,
    pub parallel: u64,
    pub keep_alive: bool,
}

#[derive(Debug, Clone)]
/// Parameters config.
pub struct Parameters {
    pub scan_period: u64,
    pub sink_dir: String,
    pub source_dir: String,
    pub batch_size: u64,
    pub batch_count: u64,
    pub log_file: String,
    pub log_level: slog::Level,
    pub syslog: bool,
    pub timeout: u64,
    pub router_parallel: u64,
    pub backoff: Backoff,
}

#[derive(Debug, Clone)]
/// Backoff config.
pub struct Backoff {
    pub initial: Duration,
    pub max: Duration,
    pub multiplier: f64,
    pub randomization: f64,
}

#[derive(Debug)]
/// Config Error.
pub enum ConfigError {
    Io(io::Error),
    Yaml(ScanError),
    Regex(regex::Error),
    Format(Box<Error>),
    Uri(hyper::error::UriError),
}

impl From<io::Error> for ConfigError {
    fn from(err: io::Error) -> ConfigError {
        ConfigError::Io(err)
    }
}
impl From<ScanError> for ConfigError {
    fn from(err: ScanError) -> ConfigError {
        ConfigError::Yaml(err)
    }
}
impl From<regex::Error> for ConfigError {
    fn from(err: regex::Error) -> ConfigError {
        ConfigError::Regex(err)
    }
}
impl From<Box<Error>> for ConfigError {
    fn from(err: Box<Error>) -> ConfigError {
        ConfigError::Format(err)
    }
}
impl<'a> From<&'a str> for ConfigError {
    fn from(err: &str) -> ConfigError {
        ConfigError::Format(From::from(err))
    }
}
impl From<String> for ConfigError {
    fn from(err: String) -> ConfigError {
        ConfigError::Format(From::from(err))
    }
}
impl From<hyper::error::UriError> for ConfigError {
    fn from(err: hyper::error::UriError) -> ConfigError {
        ConfigError::Uri(err)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConfigError::Io(ref err) => err.fmt(f),
            ConfigError::Yaml(ref err) => err.fmt(f),
            ConfigError::Regex(ref err) => err.fmt(f),
            ConfigError::Format(ref err) => err.fmt(f),
            ConfigError::Uri(ref err) => err.fmt(f),
        }
    }
}

impl error::Error for ConfigError {
    fn description(&self) -> &str {
        match *self {
            ConfigError::Io(ref err) => err.description(),
            ConfigError::Yaml(ref err) => err.description(),
            ConfigError::Regex(ref err) => err.description(),
            ConfigError::Format(ref err) => err.description(),
            ConfigError::Uri(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            ConfigError::Io(ref err) => Some(err),
            ConfigError::Yaml(ref err) => Some(err),
            ConfigError::Regex(ref err) => Some(err),
            ConfigError::Format(ref err) => Some(err.as_ref()),
            ConfigError::Uri(ref err) => Some(err),
        }
    }
}

/// Load config.
///
/// Setup a defaults config and then load config from '/etc', local dir and provided path.
/// Return Err if provided path is not found or if config is unprocessable.
pub fn load_config(config_path: &str) -> Result<Config, ConfigError> {
    // Defaults
    let mut config = Config {
        scrapers: Vec::new(),
        labels: HashMap::new(),
        sinks: Vec::new(),
        parameters: Parameters {
            scan_period: 1000,
            sink_dir: String::from("sinks"),
            source_dir: String::from("sources"),
            batch_size: 200_000,
            batch_count: 250,
            log_file: String::from(env!("CARGO_PKG_NAME")) + ".log",
            log_level: slog::Level::Info,
            syslog: false,
            timeout: 300,
            router_parallel: 1,
            backoff: Backoff {
                initial: Duration::from_millis(500),
                max: Duration::from_millis(60_000),
                multiplier: 1.5,
                randomization: 0.3,
            },
        },
    };

    if config_path.is_empty() {
        // Load from etc
        if Path::new("/etc/beamium/config.yaml").exists() {
            load_path("/etc/beamium/config.yaml", &mut config)?;
        } else if Path::new("config.yaml").exists() {
            // Load local
            load_path("config.yaml", &mut config)?;
        }
    } else {
        // Load from provided path
        load_path(config_path, &mut config)?;
    }

    Ok(config)
}

/// Extend config from file.
#[allow(unknown_lints, cyclomatic_complexity)]
fn load_path<P: AsRef<Path>>(file_path: P, config: &mut Config) -> Result<(), ConfigError> {
    let mut file = File::open(file_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let docs = YamlLoader::load_from_str(&contents)?;
    let config_scraper_keys = ["sources", "scrapers"];
    for doc in &docs {
        for config_scraper_key in &config_scraper_keys {
            let key = *config_scraper_key;
            if !doc[key].is_badvalue() {
                if "sources" == key {
                    warn!(
                        "'sources' is deprecated and will be removed in further revision. \
                         Please use 'scrapers' instead.",
                    )
                }

                let scrapers = doc[key]
                    .as_hash()
                    .ok_or_else(|| format!("{} should be a map", key))?;

                for (k, v) in scrapers {
                    let name = k
                        .as_str()
                        .ok_or_else(|| format!("{} keys should be a string", key))?;
                    let url = v["url"].as_str().ok_or_else(|| {
                        format!("{}.{}.url is required and should be a string", key, name)
                    })?;
                    let url = url.parse::<hyper::Uri>()?;
                    let period = v["period"].as_i64().ok_or_else(|| {
                        format!("{}.{}.period is required and should be a number", key, name)
                    })?;
                    let period = cast::u64(period)
                        .map_err(|_| format!("scrapers.{}.period is invalid", name))?;
                    let format = if v["format"].is_badvalue() {
                        ScraperFormat::Prometheus
                    } else {
                        let f = v["format"]
                            .as_str()
                            .ok_or_else(|| format!("scrapers.{}.format should be a string", name))?;

                        if f == "prometheus" {
                            ScraperFormat::Prometheus
                        } else if f == "sensision" {
                            ScraperFormat::Sensision
                        } else {
                            return Err(format!(
                                "scrapers.{}.format should be 'prometheus' or \
                                 'sensision'",
                                name
                            ).into());
                        }
                    };

                    let filtered_labels = if v["filtered_labels"].is_badvalue() {
                        vec![]
                    } else {
                        let mut labels = vec![];
                        let values = v["filtered_labels"].as_vec().ok_or_else(|| {
                            format!("scrapers.{}.filtered_labels should be an array", name)
                        })?;
                        for v in values {
                            let value = v.as_str().ok_or_else(|| {
                                format!("scrapers.{}.filtered_labels is invalid", name)
                            })?;
                            labels.push(String::from(value));
                        }
                        labels
                    };

                    let metrics = if v["metrics"].is_badvalue() {
                        None
                    } else {
                        let mut metrics = Vec::new();
                        let values = v["metrics"].as_vec().ok_or_else(|| {
                            format!("scrapers.{}.metrics should be an array", name)
                        })?;
                        for v in values {
                            let value =
                                regex::Regex::new(v.as_str().ok_or_else(|| {
                                    format!("scrapers.{}.metrics is invalid", name)
                                })?)?;
                            metrics.push(String::from(r"^(\S*)\s") + value.as_str());
                        }

                        Some(regex::RegexSet::new(&metrics)?)
                    };

                    // let headers = HashMap::new();
                    let headers = if v["headers"].is_badvalue() {
                        HashMap::new()
                    } else {
                        let heads = v["headers"]
                            .as_hash()
                            .ok_or_else(|| format!("scrapers.{}.headers should be a map", name))?;
                        let mut ret = HashMap::new();
                        for (k, v) in heads {
                            let hname = k.as_str().ok_or_else(|| {
                                format!("scrapers.{}.headers keys should be a string", name)
                            })?;
                            let value = v.as_str().ok_or_else(|| {
                                format!(
                                    "scrapers.{}.headers.{} value should be a string",
                                    hname, name
                                )
                            })?;
                            ret.insert(String::from(hname), String::from(value));
                        }

                        ret
                    };

                    let mut labels = HashMap::new();
                    if !v["labels"].is_badvalue() {
                        let slabels = v["labels"]
                            .as_hash()
                            .ok_or_else(|| "labels should be a map")?;
                        for (k, v) in slabels {
                            let lname = k.as_str().ok_or_else(|| {
                                format!("scrapers.{}.labels keys should be a string", name)
                            })?;
                            let value = v.as_str().ok_or_else(|| {
                                format!(
                                    "scrapers.{}.labels.{} value should be a string",
                                    name, lname
                                )
                            })?;
                            labels.insert(String::from(lname), String::from(value));
                        }
                    }

                    config.scrapers.push(Scraper {
                        name: String::from(name),
                        url,
                        period,
                        format,
                        metrics,
                        headers,
                        labels,
                        filtered_labels,
                    })
                }
            }
        }

        if !doc["sinks"].is_badvalue() {
            let sinks = doc["sinks"]
                .as_hash()
                .ok_or_else(|| "sinks should be a map")?;
            for (k, v) in sinks {
                let name = k.as_str().ok_or_else(|| "sinks keys should be a string")?;
                let url = v["url"].as_str().ok_or_else(|| {
                    format!("sinks.{}.url is required and should be a string", name)
                })?;
                let url = url.parse::<hyper::Uri>()?;
                let token = v["token"].as_str().ok_or_else(|| {
                    format!("sinks.{}.token is required and should be a string", name)
                })?;
                let token_header = if v["token-header"].is_badvalue() {
                    "X-Warp10-Token"
                } else {
                    v["token-header"]
                        .as_str()
                        .ok_or_else(|| format!("sinks.{}.token-header should be a string", name))?
                };

                let selector = if v["selector"].is_badvalue() {
                    None
                } else {
                    Some(regex::Regex::new(
                        format!(
                            "^{}",
                            v["selector"].as_str().ok_or_else(|| format!(
                                "sinks.{}.selector \
                                 is invalid",
                                name
                            ))?
                        ).as_str(),
                    )?)
                };

                let ttl = if v["ttl"].is_badvalue() {
                    3600
                } else {
                    let ttl = v["ttl"]
                        .as_i64()
                        .ok_or_else(|| format!("sinks.{}.ttl should be a number", name))?;
                    cast::u64(ttl)
                        .map_err(|_| format!("sinks.{}.ttl should be a positive number", name))?
                };

                let size = if v["size"].is_badvalue() {
                    1_073_741_824
                } else {
                    let size = v["size"]
                        .as_i64()
                        .ok_or_else(|| format!("sinks.{}.size should be a number", name))?;
                    cast::u64(size)
                        .map_err(|_| format!("sinks.{}.size should be a positive number", name))?
                };

                let parallel = if v["parallel"].is_badvalue() {
                    1
                } else {
                    let parallel = v["parallel"]
                        .as_i64()
                        .ok_or_else(|| format!("sinks.{}.parallel should be a number", name))?;
                    cast::u64(parallel).map_err(|_| {
                        format!("sinks.{}.parallel should be a positive number", name)
                    })?
                };

                let keep_alive = if v["keep-alive"].is_badvalue() {
                    true
                } else {
                    v["keep-alive"]
                        .as_bool()
                        .ok_or_else(|| format!("sinks.{}.keep-alive should be a boolean", name))?
                };

                config.sinks.push(Sink {
                    name: String::from(name),
                    url,
                    token: String::from(token),
                    token_header: String::from(token_header),
                    selector,
                    ttl,
                    size,
                    parallel,
                    keep_alive,
                })
            }
        }

        if !doc["labels"].is_badvalue() {
            let labels = doc["labels"]
                .as_hash()
                .ok_or_else(|| "labels should be a map")?;
            for (k, v) in labels {
                let name = k.as_str().ok_or_else(|| "labels keys should be a string")?;
                let value = v
                    .as_str()
                    .ok_or_else(|| format!("labels.{} value should be a string", name))?;
                config
                    .labels
                    .insert(String::from(name), String::from(value));
            }
        }

        if !doc["parameters"].is_badvalue() {
            if !doc["parameters"]["source-dir"].is_badvalue() {
                let source_dir = doc["parameters"]["source-dir"]
                    .as_str()
                    .ok_or_else(|| "parameters.source-dir should be a string".to_string())?;
                config.parameters.source_dir = String::from(source_dir);
            }

            if !doc["parameters"]["sink-dir"].is_badvalue() {
                let sink_dir = doc["parameters"]["sink-dir"]
                    .as_str()
                    .ok_or_else(|| "parameters.sink-dir should be a string".to_string())?;
                config.parameters.sink_dir = String::from(sink_dir);
            }

            if !doc["parameters"]["scan-period"].is_badvalue() {
                let scan_period = doc["parameters"]["scan-period"]
                    .as_i64()
                    .ok_or_else(|| "parameters.scan-period should be a number".to_string())?;
                let scan_period = cast::u64(scan_period)
                    .map_err(|_| "parameters.scan-period is invalid".to_string())?;
                config.parameters.scan_period = scan_period;
            }

            if !doc["parameters"]["batch-size"].is_badvalue() {
                let batch_size = doc["parameters"]["batch-size"]
                    .as_i64()
                    .ok_or_else(|| "parameters.batch-size should be a number".to_string())?;
                let batch_size = cast::u64(batch_size)
                    .map_err(|_| "parameters.batch-size is invalid".to_string())?;
                config.parameters.batch_size = batch_size;
            }

            if !doc["parameters"]["batch-count"].is_badvalue() {
                let batch_count = doc["parameters"]["batch-count"]
                    .as_i64()
                    .ok_or_else(|| "parameters.batch-count should be a number".to_string())?;
                let batch_count = cast::u64(batch_count)
                    .map_err(|_| "parameters.batch-count is invalid".to_string())?;
                config.parameters.batch_count = batch_count;
            }

            if !doc["parameters"]["log-file"].is_badvalue() {
                let log_file = doc["parameters"]["log-file"]
                    .as_str()
                    .ok_or_else(|| "parameters.log-file should be a string".to_string())?;
                config.parameters.log_file = String::from(log_file);
            }

            if !doc["parameters"]["log-level"].is_badvalue() {
                let log_level = doc["parameters"]["log-level"]
                    .as_i64()
                    .ok_or_else(|| "parameters.log-level should be a number".to_string())?;
                let log_level = cast::u64(log_level)
                    .map_err(|_| "parameters.log-level is invalid".to_string())?;
                let log_level = slog::Level::from_usize(log_level as usize)
                    .ok_or_else(|| "parameters.log-level is invalid".to_string())?;
                config.parameters.log_level = log_level;
            }

            if !doc["parameters"]["syslog"].is_badvalue() {
                let syslog = doc["parameters"]["syslog"]
                    .as_bool()
                    .ok_or_else(|| "parameters.bool should be a boolean")?;
                config.parameters.syslog = syslog;
            }
            if !doc["parameters"]["timeout"].is_badvalue() {
                let timeout = doc["parameters"]["timeout"]
                    .as_i64()
                    .ok_or_else(|| "parameters.timeout should be a number")?;
                let timeout =
                    cast::u64(timeout).map_err(|_| "parameters.timeout is invalid".to_string())?;
                config.parameters.timeout = timeout;
            }
            if !doc["parameters"]["router-parallel"].is_badvalue() {
                let router_parallel = doc["parameters"]["router-parallel"]
                    .as_i64()
                    .ok_or_else(|| "parameters.router-parallel should be a number")?;
                let router_parallel = cast::u64(router_parallel)
                    .map_err(|_| "parameters.router-parallel is invalid".to_string())?;
                config.parameters.router_parallel = router_parallel;
            }

            if !doc["parameters"]["backoff"].is_badvalue() {
                if !doc["parameters"]["backoff"]["initial"].is_badvalue() {
                    let v = &doc["parameters"]["backoff"]["initial"];
                    let initial = v
                        .as_i64()
                        .and_then(|initial| cast::u64(initial).ok())
                        .map(|initial| Duration::from_millis(initial))
                        .or_else(|| v.as_str().and_then(|initial| parse_duration(initial).ok()))
                        .ok_or_else(|| {
                            "parameters.backoff.initial should be a duration string".to_string()
                        })?;
                    config.parameters.backoff.initial = initial;
                }

                if !doc["parameters"]["backoff"]["max"].is_badvalue() {
                    let v = &doc["parameters"]["backoff"]["max"];
                    let max = v
                        .as_i64()
                        .and_then(|max| cast::u64(max).ok())
                        .map(|max| Duration::from_millis(max))
                        .or_else(|| v.as_str().and_then(|max| parse_duration(max).ok()))
                        .ok_or_else(|| {
                            "parameters.backoff.max should be a duration string".to_string()
                        })?;
                    config.parameters.backoff.max = max;
                }

                if !doc["parameters"]["backoff"]["multiplier"].is_badvalue() {
                    let v = &doc["parameters"]["backoff"]["multiplier"];
                    let multiplier = v.as_f64().ok_or_else(|| {
                        "parameters.backoff.multiplier should be a number".to_string()
                    })?;

                    if multiplier < 0.0 {
                        return Err(ConfigError::from(
                            "parameters.backoff.multiplier is negative",
                        ));
                    }
                    config.parameters.backoff.multiplier = multiplier;
                }

                if !doc["parameters"]["backoff"]["randomization"].is_badvalue() {
                    let v = &doc["parameters"]["backoff"]["randomization"];
                    let randomization = v.as_f64().ok_or_else(|| {
                        "parameters.backoff.randomization should be a number".to_string()
                    })?;

                    if randomization < 0.0 || randomization > 1.0 {
                        return Err(ConfigError::from(
                            "parameters.backoff.randomization should in [0-1]",
                        ));
                    }
                    config.parameters.backoff.randomization = randomization;
                }
            }
        }
    }
    Ok(())
}
