use std::error::Error;
use time;

use config;

pub struct Transcompiler<'a> {
    format: &'a config::ScraperFormat,
    now: i64,
}

impl<'a> Transcompiler<'a> {
    pub fn new(format: &config::ScraperFormat) -> Transcompiler {
        let start = time::now_utc();
        let now = start.to_timespec().sec * 1000 * 1000 + (start.to_timespec().nsec as i64 / 1000);
        Transcompiler {
            format: format,
            now: now,
        }
    }

    pub fn format(&self, line: &str) -> Result<String, Box<Error>> {
        match self.format {
            &config::ScraperFormat::Sensision => format_warp10(line),
            &config::ScraperFormat::Prometheus => format_prometheus(line, self.now),
        }
    }
}

/// Format Warp10 metrics from Prometheus one.
fn format_warp10(line: &str) -> Result<String, Box<Error>> {
    Ok(String::from(line.trim()))
}

/// Format Warp10 metrics from Prometheus one.
fn format_prometheus(line: &str, now: i64) -> Result<String, Box<Error>> {
    let line = line.trim();

    // Skip comments
    if line.starts_with("#") {
        return Ok(String::new());
    }

    // Extract Prometheus metric
    let index = if line.contains("{") {
        line.rfind('}').ok_or("bad class")?
    } else {
        line.find(' ').ok_or("bad class")?
    };
    let (class, v) = line.split_at(index + 1);
    let mut tokens = v.split_whitespace();

    let value = tokens.next().ok_or("no value")?;
    let timestamp = tokens
        .next()
        .map(|v| i64::from_str_radix(v, 10).map(|v| v * 1000).unwrap_or(now))
        .unwrap_or(now);

    // Format class
    let mut parts = class.splitn(2, "{");
    let class = String::from(parts.next().ok_or("no_class")?);
    let class = class.trim();
    let plabels = parts.next();
    let slabels = if plabels.is_some() {
        let mut labels = plabels.unwrap().split("\",")
            .map(|v| v.replace("=", "%3D")) // escape
            .map(|v| v.replace("%3D\"", "=")) // remove left double quote
            .map(|v| v.replace("\"}", "")) // remove right double quote
            .map(|v| v.replace(",", "%2C")) // escape
            .map(|v| v.replace("}", "%7D")) // escape
            .map(|v| v.replace(r"\\", r"\")) // unescape
            .map(|v| v.replace("\\\"", "\"")) // unescape
            .map(|v| v.replace(r"\n", "%0A")) // unescape
            .fold(String::new(), |acc, x| {
                // skip invalid values
                if !x.contains("=") {
                    return acc
                }
                acc + &x + ","
            });
        labels.pop();
        labels
    } else {
        String::new()
    };

    let class = format!("{}{{{}}}", class, slabels);

    Ok(format!("{}// {} {}", timestamp, class, value))
}
