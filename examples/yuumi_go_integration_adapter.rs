use std::path::PathBuf;
use std::process::Command;

use serde_json::{json, Value};

fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let build = Command::new("cargo")
        .args([
            "build",
            "--example",
            "yuumi_interop_engine",
            "--message-format=json",
        ])
        .current_dir(&manifest)
        .output()
        .expect("build Rust interop fixture");
    assert!(
        build.status.success(),
        "fixture build failed: {}",
        String::from_utf8_lossy(&build.stderr)
    );
    let executable = String::from_utf8_lossy(&build.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|message| {
            let target = message.get("target")?;
            (target.get("name")?.as_str()? == "yuumi_interop_engine")
                .then(|| message.get("executable")?.as_str().map(str::to_owned))
                .flatten()
        })
        .expect("cargo did not report the fixture executable");
    let command = json!({
        "executable": executable,
        "arguments": [],
        "engine": "rust"
    })
    .to_string();
    let status = Command::new("go")
        .args([
            "test",
            "-tags=interop",
            "-run",
            "^TestGoEngineInterop$",
            "-count=1",
            "-timeout",
            "150s",
        ])
        .current_dir(manifest.parent().unwrap().join("Yuumi"))
        .env("YUUMI_INTEROP_COMMAND", command)
        .status()
        .expect("run shared Go interop suite");
    assert!(status.success(), "shared Go interop suite failed");
}

/*
The explicit Rust adapter builds a private fixture and delegates all
expectations to the shared Go suite. Missing prerequisites fail, never skip.
*/
