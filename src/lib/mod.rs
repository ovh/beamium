use std::error::Error;

pub mod transcompiler;

pub fn add_labels(line: &str, labels: &str) -> Result<String, Box<Error>> {
    if labels.is_empty() {
        return Ok(String::from(line));
    }

    let mut parts = line.splitn(2, "{");

    let class = parts.next().ok_or("no_class")?;
    let class = String::from(class);

    let plabels = parts.next().ok_or("no_labels")?;
    let plabels = String::from(plabels);

    let sep = if plabels.trim().starts_with("}") {
        ""
    } else {
        ","
    };

    Ok(format!("{}{{{}{}{}", class, labels, sep, plabels))
}
