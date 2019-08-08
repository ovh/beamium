//! # Library module.
//!
//! This module provide traits and standard stuffs.
use failure::{format_err, Error, ResultExt};
use tokio::runtime::Runtime;

#[macro_use]
pub mod asynch;
pub mod transcompiler;

/// `Runner` trait provide a method to start a job on the given runtime
pub trait Runner {
    type Error;

    /// Start the runner
    fn start(&self, runtime: &mut Runtime) -> Result<(), Self::Error>;
}

/// `Named` trait provide a method to retrieve the name of the structure
pub trait Named {
    /// Retrieve the name
    fn name(&self) -> String;
}

/// `add_labels` to the time series
pub fn add_labels(line: &str, labels: &str) -> Result<String, Error> {
    if labels.is_empty() {
        return Ok(String::from(line));
    }

    let mut parts = line.splitn(2, '{');

    let class = parts
        .next()
        .ok_or_else(|| format_err!("no_class"))
        .with_context(|err| format!("could not parse '{}', {}", line, err))?;
    let class = String::from(class);

    let plabels = parts
        .next()
        .ok_or_else(|| format_err!("no_labels"))
        .with_context(|err| format!("could not parse '{}', {}", line, err))?;
    let plabels = String::from(plabels);

    let sep = if plabels.trim().starts_with('}') {
        ""
    } else {
        ","
    };

    Ok(format!("{}{{{}{}{}", class, labels, sep, plabels))
}

/// `remove_labels` to the time series
pub fn remove_labels(line: &str, labels_to_drop: &[String]) -> Result<String, Error> {
    if labels_to_drop.is_empty() {
        return Ok(String::from(line));
    }

    let mut parts = line.splitn(2, '{');

    let class = parts
        .next()
        .ok_or_else(|| format_err!("no_class"))
        .with_context(|err| format!("could not parse '{}', {}", line, err))?;
    let class = String::from(class);

    let plabels = parts
        .next()
        .ok_or_else(|| format_err!("no_labels"))
        .with_context(|err| format!("could not parse '{}', {}", line, err))?;
    let plabels = String::from(plabels);

    let mut end_parts = plabels.splitn(2, "} ");

    let plabels = end_parts
        .next()
        .ok_or_else(|| format_err!("no_end"))
        .with_context(|err| format!("could not parse '{}', {}", line, err))?;
    let plabels = String::from(plabels);

    let value = end_parts
        .next()
        .ok_or_else(|| format_err!("no_value"))
        .with_context(|err| format!("could not parse '{}', {}", line, err))?;
    let value = String::from(value);

    let labels: Vec<String> = plabels
        .split(',')
        .filter_map(|l| {
            let mut label_splits: Vec<String> = l.split('=').map(String::from).collect();
            let value = label_splits.pop()?;
            let key = label_splits.pop()?;
            Some((key, value))
        })
        .filter(|(key, _)| !labels_to_drop.contains(&key.to_owned()))
        .map(|(key, value)| format!("{}={}", key, value))
        .collect();

    Ok(format!("{}{{{}}} {}", class, labels.join(","), value))
}

#[cfg(test)]
mod tests {
    use failure::Error;

    #[test]
    fn no_labels_at_all() {
        let line = "1484828198557102// f{} 10";
        let expected: Result<String, Error> = Ok(String::from("1484828198557102// f{} 10"));
        let labels = vec![String::from("job_name"), String::from("another_id")];
        let result = super::remove_labels(line, &labels);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());
    }

    #[test]
    fn remove_no_labels() {
        let line = "1484828198557102// f{job_id=123,job_name=job1,another_id=456} 10";
        let expected: Result<String, Error> = Ok(String::from(line));
        let labels = vec![];
        let result = super::remove_labels(line, &labels);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());
    }

    #[test]
    fn remove_one_labels() {
        let line = "1484828198557102// f{job_id=123,job_name=job1,another_id=456} 10";
        let expected: Result<String, Error> = Ok(String::from(
            "1484828198557102// f{job_id=123,another_id=456} 10",
        ));
        let labels = vec![String::from("job_name")];
        let result = super::remove_labels(line, &labels);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());
    }

    #[test]
    fn remove_multiple_labels() {
        let line = "1484828198557102// f{job_id=123,job_name=job1,another_id=456} 10";
        let expected: Result<String, Error> =
            Ok(String::from("1484828198557102// f{job_id=123} 10"));
        let labels = vec![String::from("job_name"), String::from("another_id")];
        let result = super::remove_labels(line, &labels);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());
    }

    #[test]
    fn add_one_label() {
        let line = "1562656816000000// f{type=count} 1486";
        let label = "host=foo";
        let expected: Result<String, Error> = Ok(String::from(
            "1562656816000000// f{host=foo,type=count} 1486",
        ));
        let result = super::add_labels(line, label);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());
    }

    #[test]
    fn add_multiple_labels() {
        let line = "1562656816000000// f{type=count} 1486";
        let label = "host=foo,rack=toto";
        let expected: Result<String, Error> = Ok(String::from(
            "1562656816000000// f{host=foo,rack=toto,type=count} 1486",
        ));
        let result = super::add_labels(line, label);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());
    }
}
