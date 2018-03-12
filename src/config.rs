//! # Config module.
//!
//! The Config module provides the beamium configuration.
//! It set defaults and then load config from '/etc', local dir and provided path.

use std::fs::File;
use std::io::Read;
use std::io;
use std::fmt;
use std::string::String;
use std::path::Path;
use std::error;
use std::error::Error;
use yaml_rust::{ScanError, YamlLoader};
use cast;
use std::collections::HashMap;
use regex;
use slog;
use hyper;

pub const REST_TIME: u64 = 10;

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
            batch_size: 200000,
            batch_count: 250,
            log_file: String::from(env!("CARGO_PKG_NAME")) + ".log",
            log_level: slog::Level::Info,
            syslog: false,
            timeout: 300,
        },
    };

    if config_path.is_empty() {
        // Load from etc
        if Path::new("/etc/beamium/config.yaml").exists() {
            try!(load_path("/etc/beamium/config.yaml", &mut config));
        } else if Path::new("config.yaml").exists() {
            // Load local
            try!(load_path("config.yaml", &mut config));
        }
    } else {
        // Load from provided path
        try!(load_path(config_path, &mut config));
    }

    Ok(config)
}

/// Extend config from file.
fn load_path<P: AsRef<Path>>(file_path: P, config: &mut Config) -> Result<(), ConfigError> {
    let mut file = try!(File::open(file_path));
    let mut contents = String::new();
    try!(file.read_to_string(&mut contents));
    let docs = try!(YamlLoader::load_from_str(&contents));
    let config_scraper_keys = ["sources", "scrapers"];
    for doc in &docs {
        for config_scraper_key in config_scraper_keys.iter() {
            let key = *config_scraper_key;
            if !doc[key].is_badvalue() {
                if "sources" == key {
                    warn!(
                        "'sources' is deprecated and will be removed in further revision. \
                         Please use 'scrapers' instead.",
                    )
                }

                let scrapers = try!(doc[key].as_hash().ok_or(format!("{} should be a map", key)));

                for (k, v) in scrapers {
                    let name = try!(k.as_str().ok_or(format!("{} keys should be a string", key)));
                    let url = try!(v["url"].as_str().ok_or(format!(
                        "{}.{}.url is required and should be a string",
                        key, name
                    )));
                    let url = try!(url.parse::<hyper::Uri>());
                    let period = try!(v["period"].as_i64().ok_or(format!(
                        "{}.{}.period is required and should be a number",
                        key, name
                    )));
                    let period = try!(
                        cast::u64(period)
                            .map_err(|_| format!("scrapers.{}.period is invalid", name))
                    );
                    let format = if v["format"].is_badvalue() {
                        ScraperFormat::Prometheus
                    } else {
                        let f = try!(
                            v["format"]
                                .as_str()
                                .ok_or(format!("scrapers.{}.format should be a string", name))
                        );

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
                    let metrics = if v["metrics"].is_badvalue() {
                        None
                    } else {
                        let mut metrics = Vec::new();
                        let values = try!(
                            v["metrics"]
                                .as_vec()
                                .ok_or(format!("scrapers.{}.metrics should be an array", name))
                        );
                        for v in values {
                            let value = try!(regex::Regex::new(try!(
                                v.as_str()
                                    .ok_or(format!("scrapers.{}.metrics is invalid", name))
                            )));
                            metrics.push(String::from(r"^(\S*)\s") + value.as_str());
                        }

                        Some(try!(regex::RegexSet::new(&metrics)))
                    };

                    // let headers = HashMap::new();
                    let headers = if v["headers"].is_badvalue() {
                        HashMap::new()
                    } else {
                        let heads = try!(
                            v["headers"]
                                .as_hash()
                                .ok_or(format!("scrapers.{}.headers should be a map", name))
                        );
                        let mut ret = HashMap::new();
                        for (k, v) in heads {
                            let hname = try!(k.as_str().ok_or(format!(
                                "scrapers.{}.headers keys should be a string",
                                name
                            )));
                            let value = try!(v.as_str().ok_or(format!(
                                "scrapers.{}.headers.{} value should be a string",
                                hname, name
                            )));
                            ret.insert(String::from(name), String::from(value));
                        }

                        ret
                    };

                    let mut labels = HashMap::new();
                    if !v["labels"].is_badvalue() {
                        let slabels = try!(v["labels"].as_hash().ok_or("labels should be a map"));
                        for (k, v) in slabels {
                            let lname = try!(k.as_str().ok_or(format!(
                                "scrapers.{}.labels keys should be a string",
                                name
                            )));
                            let value = try!(v.as_str().ok_or(format!(
                                "scrapers.{}.labels.{} value should be a string",
                                name, lname
                            )));
                            labels.insert(String::from(lname), String::from(value));
                        }
                    }

                    config.scrapers.push(Scraper {
                        name: String::from(name),
                        url: url,
                        period: period,
                        format: format,
                        metrics: metrics,
                        headers: headers,
                        labels: labels,
                    })
                }
            }
        }

        if !doc["sinks"].is_badvalue() {
            let sinks = try!(doc["sinks"].as_hash().ok_or("sinks should be a map"));
            for (k, v) in sinks {
                let name = try!(k.as_str().ok_or("sinks keys should be a string"));
                let url = try!(v["url"].as_str().ok_or(format!(
                    "sinks.{}.url is required and should be a string",
                    name
                )));
                let url = try!(url.parse::<hyper::Uri>());
                let token = try!(v["token"].as_str().ok_or(format!(
                    "sinks.{}.token is required and should be a string",
                    name
                )));
                let token_header = if v["token-header"].is_badvalue() {
                    "X-Warp10-Token"
                } else {
                    try!(
                        v["token-header"]
                            .as_str()
                            .ok_or(format!("sinks.{}.token-header should be a string", name))
                    )
                };

                let selector = if v["selector"].is_badvalue() {
                    None
                } else {
                    Some(try!(regex::Regex::new(
                        format!(
                            "^{}",
                            try!(v["selector"].as_str().ok_or(format!(
                                "sinks.{}.selector \
                                 is invalid",
                                name
                            )))
                        ).as_str()
                    )))
                };

                let ttl = if v["ttl"].is_badvalue() {
                    3600
                } else {
                    let ttl = try!(
                        v["ttl"]
                            .as_i64()
                            .ok_or(format!("sinks.{}.ttl should be a number", name))
                    );
                    try!(
                        cast::u64(ttl)
                            .map_err(|_| format!("sinks.{}.ttl should be a positive number", name))
                    )
                };

                let size =
                    if v["size"].is_badvalue() {
                        1073741824
                    } else {
                        let size = try!(
                            v["size"]
                                .as_i64()
                                .ok_or(format!("sinks.{}.size should be a number", name))
                        );
                        try!(cast::u64(size).map_err(|_| format!(
                            "sinks.{}.size should be a positive number",
                            name
                        )))
                    };

                let parallel = if v["parallel"].is_badvalue() {
                    1
                } else {
                    let parallel = try!(
                        v["parallel"]
                            .as_i64()
                            .ok_or(format!("sinks.{}.parallel should be a number", name))
                    );
                    try!(cast::u64(parallel).map_err(|_| format!(
                        "sinks.{}.parallel should be a positive number",
                        name
                    )))
                };

                let keep_alive = if v["keep-alive"].is_badvalue() {
                    true
                } else {
                    try!(
                        v["keep-alive"]
                            .as_bool()
                            .ok_or(format!("sinks.{}.keep-alive should be a boolean", name))
                    )
                };

                config.sinks.push(Sink {
                    name: String::from(name),
                    url: url,
                    token: String::from(token),
                    token_header: String::from(token_header),
                    selector: selector,
                    ttl: ttl,
                    size: size,
                    parallel: parallel,
                    keep_alive: keep_alive,
                })
            }
        }

        if !doc["labels"].is_badvalue() {
            let labels = try!(doc["labels"].as_hash().ok_or("labels should be a map"));
            for (k, v) in labels {
                let name = try!(k.as_str().ok_or("labels keys should be a string"));
                let value = try!(
                    v.as_str()
                        .ok_or(format!("labels.{} value should be a string", name))
                );
                config
                    .labels
                    .insert(String::from(name), String::from(value));
            }
        }

        if !doc["parameters"].is_badvalue() {
            if !doc["parameters"]["source-dir"].is_badvalue() {
                let source_dir = try!(
                    doc["parameters"]["source-dir"]
                        .as_str()
                        .ok_or(format!("parameters.source-dir should be a string"))
                );
                config.parameters.source_dir = String::from(source_dir);
            }

            if !doc["parameters"]["sink-dir"].is_badvalue() {
                let sink_dir = try!(
                    doc["parameters"]["sink-dir"]
                        .as_str()
                        .ok_or(format!("parameters.sink-dir should be a string"))
                );
                config.parameters.sink_dir = String::from(sink_dir);
            }

            if !doc["parameters"]["scan-period"].is_badvalue() {
                let scan_period = try!(
                    doc["parameters"]["scan-period"]
                        .as_i64()
                        .ok_or(format!("parameters.scan-period should be a number"))
                );
                let scan_period = try!(
                    cast::u64(scan_period)
                        .map_err(|_| format!("parameters.scan-period is invalid"))
                );
                config.parameters.scan_period = scan_period;
            }

            if !doc["parameters"]["batch-size"].is_badvalue() {
                let batch_size = try!(
                    doc["parameters"]["batch-size"]
                        .as_i64()
                        .ok_or(format!("parameters.batch-size should be a number"))
                );
                let batch_size = try!(
                    cast::u64(batch_size).map_err(|_| format!("parameters.batch-size is invalid"))
                );
                config.parameters.batch_size = batch_size;
            }

            if !doc["parameters"]["batch-count"].is_badvalue() {
                let batch_count = try!(
                    doc["parameters"]["batch-count"]
                        .as_i64()
                        .ok_or(format!("parameters.batch-count should be a number"))
                );
                let batch_count = try!(
                    cast::u64(batch_count)
                        .map_err(|_| format!("parameters.batch-count is invalid"))
                );
                config.parameters.batch_count = batch_count;
            }

            if !doc["parameters"]["log-file"].is_badvalue() {
                let log_file = try!(
                    doc["parameters"]["log-file"]
                        .as_str()
                        .ok_or(format!("parameters.log-file should be a string"))
                );
                config.parameters.log_file = String::from(log_file);
            }

            if !doc["parameters"]["log-level"].is_badvalue() {
                let log_level = try!(
                    doc["parameters"]["log-level"]
                        .as_i64()
                        .ok_or(format!("parameters.log-level should be a number"))
                );
                let log_level = try!(
                    cast::u64(log_level).map_err(|_| format!("parameters.log-level is invalid"))
                );
                let log_level = try!(
                    slog::Level::from_usize(log_level as usize)
                        .ok_or(format!("parameters.log-level is invalid"))
                );
                config.parameters.log_level = log_level;
            }

            if !doc["parameters"]["syslog"].is_badvalue() {
                let syslog = try!(
                    doc["parameters"]["syslog"]
                        .as_bool()
                        .ok_or("parameters.bool should be a boolean")
                );
                config.parameters.syslog = syslog;
            }
            if !doc["parameters"]["timeout"].is_badvalue() {
                let timeout = try!(
                    doc["parameters"]["timeout"]
                        .as_i64()
                        .ok_or("parameters.timeout should be a number")
                );
                let timeout =
                    try!(cast::u64(timeout).map_err(|_| format!("parameters.timeout is invalid")));
                config.parameters.timeout = timeout;
            }
        }
    }
    Ok(())
}
