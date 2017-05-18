use std::process::Command;
use std::path::Path;
use std::fs::File;
use std::io::prelude::*;

fn main() {
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .expect("failed to execute process");

    let hash = String::from_utf8_lossy(&output.stdout);
    let content = format!("static COMMIT: &'static str = {:?};\n", hash.trim());

    let path = Path::new("./src/version.rs");

    if path.exists() {
        let mut f = File::open(path).expect("fail to open result.rs");
        let mut current = String::new();
        f.read_to_string(&mut current)
            .expect("fail to read result.rs");

        if current == content {
            return;
        }
    };

    let mut out = File::create(path).unwrap();
    out.write(content.as_bytes()).unwrap();
}
