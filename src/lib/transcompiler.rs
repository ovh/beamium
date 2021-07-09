use std::error::Error;

use time::now_utc;

use urlencoding::encode;

use crate::conf::ScraperFormat;

#[derive(Clone, Debug)]
pub struct Transcompiler {
    format: ScraperFormat,
    now: i64,
}

impl Transcompiler {
    pub fn new(format: ScraperFormat) -> Self {
        let start = now_utc();
        let now = start.to_timespec().sec * 1_000_000
            + (i64::from(start.to_timespec().nsec) as i64 / 1_000);

        Self { format, now }
    }

    pub fn format(&self, line: &str) -> Result<String, Box<dyn Error>> {
        match self.format {
            ScraperFormat::Sensision => format_warp10(line),
            ScraperFormat::Prometheus => format_prometheus(line, self.now),
        }
    }
}

/// Format Warp10 metrics from Prometheus one.
fn format_warp10(line: &str) -> Result<String, Box<dyn Error>> {
    Ok(String::from(line.trim()))
}

/// Format Warp10 metrics from Prometheus one.
fn format_prometheus(line: &str, now: i64) -> Result<String, Box<dyn Error>> {
    let line = line.trim();

    // Skip comments or empty line
    if line.starts_with('#') || line.is_empty() {
        return Ok(String::new());
    }

    // Extract Prometheus metric
    let index = if line.contains('{') {
        line.rfind('}').ok_or_else(|| "bad class")?
    } else {
        line.find(' ').ok_or_else(|| "bad class")?
    };
    let (class, v) = line.split_at(index + 1);
    let mut tokens = v.split_whitespace();

    let value = tokens.next().ok_or_else(|| "no value")?;

    // Prometheus value can be '-Inf', '+Inf', 'nan', 'NaN' skipping if so
    if value == "+Inf" || value == "-Inf" || value == "nan" || value == "NaN" {
        return Ok(String::new());
    }

    let timestamp = tokens.next().map_or(now, |v| {
        i64::from_str_radix(v, 10)
            .map(|v| v * 1000)
            .unwrap_or_else(|_| now)
    });

    // Format class
    let mut parts = class.splitn(2, '{');
    let class = String::from(parts.next().ok_or_else(|| "no_class")?);
    let class = encode(class.trim());
    let plabels = parts.next();
    let slabels = match plabels {
        None => String::new(),
        Some(plabels) => {
            let mut labels = String::new();
            let mut in_label = false;
            let mut buffer = String::new();
            for c in plabels.chars() {
                if c == '"' {
                    in_label = !in_label;
                    continue;
                }

                if !in_label {
                    if c == '=' || c == ',' || c == '}' {
                        labels.push_str(& encode(&buffer));
                        buffer = String::new();

                        if c == ',' {
                            labels.push(',');
                        }
                        if c == '=' {
                            labels.push('=');
                        }
                        continue;
                    }
                }

                buffer.push(c);                
            }
            labels
        }
    };

    let class = format!("{}{{{}}}", class, slabels);

    Ok(format!("{}// {} {}", timestamp, class, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_skip_infinity() {
        let line = "f{job_id=\"123\"} +Inf";
        let expected: Result<String, Box<dyn Error>> = Ok(String::new());
        let result = super::format_prometheus(line, 1);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());

        let line = "f{job_id=\"123\"} -Inf";
        let expected: Result<String, Box<dyn Error>> = Ok(String::new());
        let result = super::format_prometheus(line, 1);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());
    }

    #[test]
    fn prometheus_skip_empty() {
        let line = "";
        let expected: Result<String, Box<dyn Error>> = Ok(String::new());
        let result = super::format_prometheus(line, 1);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());
    }

    #[test]
    fn prometheus_skip_comment() {
        let line = "# HELP ...";
        let expected: Result<String, Box<dyn Error>> = Ok(String::new());
        let result = super::format_prometheus(line, 1);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());
    }

    #[test]
    fn prometheus_skip_nan() {
        let line = "f{job_id=\"123\"} nan";
        let expected: Result<String, Box<dyn Error>> = Ok(String::new());
        let result = super::format_prometheus(line, 1);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());

        let line = "f{job_id=\"123\"} NaN";
        let expected: Result<String, Box<dyn Error>> = Ok(String::new());
        let result = super::format_prometheus(line, 1);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());
    }

    #[test]
    fn prometheus_urlencoding() {
        let line = "f{job_id=\"1%3\"} 1";
        let expected: Result<String, Box<dyn Error>> = Ok(String::from("1// f{job_id=1%253} 1"));
        let result = super::format_prometheus(line, 1);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());

        let line = "f{job_id=\"1%3\"} 1";
        let expected: Result<String, Box<dyn Error>> = Ok(String::from("1// f{job_id=1%253} 1"));
        let result = super::format_prometheus(line, 1);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());

        let line = "f{job_id=\"1%3\"} 1";
        let expected: Result<String, Box<dyn Error>> = Ok(String::from("1// f{job_id=1%253} 1"));
        let result = super::format_prometheus(line, 1);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());

        let line = "f{job_id=\"1 3\"} 1";
        let expected: Result<String, Box<dyn Error>> = Ok(String::from("1// f{job_id=1%203} 1"));
        let result = super::format_prometheus(line, 1);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());

        let line = "f{job_id=\"1+3\"} 1";
        let expected: Result<String, Box<dyn Error>> = Ok(String::from("1// f{job_id=1%2B3} 1"));
        let result = super::format_prometheus(line, 1);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());
    }
}
