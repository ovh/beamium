use std::error::Error;

pub mod transcompiler;

pub fn add_labels(line: &str, labels: &str) -> Result<String, Box<Error>> {
    if labels.is_empty() {
        return Ok(String::from(line));
    }

    let mut parts = line.splitn(2, '{');

    let class = parts.next().ok_or_else(||"no_class")?;
    let class = String::from(class);

    let plabels = parts.next().ok_or_else(||"no_labels")?;
    let plabels = String::from(plabels);

    let sep = if plabels.trim().starts_with('}') {
        ""
    } else {
        ","
    };

    Ok(format!("{}{{{}{}{}", class, labels, sep, plabels))
}

pub fn remove_labels(line: &str, labels_to_drop: &Vec<String>) -> Result<String, Box<Error>> {
    if labels_to_drop.is_empty() {
        return Ok(String::from(line));
    }

    let mut parts = line.splitn(2, '{');

    let class = parts.next().ok_or_else(||"no_class")?;
    let class = String::from(class);

    let plabels = parts.next().ok_or_else(||"no_labels")?;
    let plabels = String::from(plabels);

    let mut end_parts = plabels.splitn(2, "} ");

    let plabels = end_parts.next().ok_or_else(||"no_end")?;
    let plabels = String::from(plabels);

    let value = end_parts.next().ok_or_else(||"no_value")?;
    let value = String::from(value);

    let labels: Vec<String> = plabels
        .split(",")
        .filter_map(|l| {
            let mut label_splits: Vec<String> = l.split("=").map(|s| String::from(s)).collect();
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
    use std::error::Error;

    #[test]
    fn remove_no_labels() {
        let line = "1484828198557102// f{job_id=123,job_name=job1,another_id=456} 10";
        let expected: Result<String, Box<Error>> = Ok(String::from(line));
        let labels = vec![];
        let result = super::remove_labels(line, &labels);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());
    }
    #[test]
    fn remove_one_labels() {
        let line = "1484828198557102// f{job_id=123,job_name=job1,another_id=456} 10";
        let expected: Result<String, Box<Error>> = Ok(String::from(
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
        let expected: Result<String, Box<Error>> =
            Ok(String::from("1484828198557102// f{job_id=123} 10"));
        let labels = vec![String::from("job_name"), String::from("another_id")];
        let result = super::remove_labels(line, &labels);
        assert_eq!(expected.is_ok(), result.is_ok());
        assert_eq!(expected.unwrap(), result.unwrap());
    }
}
