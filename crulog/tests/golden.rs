// Copyright 2026 Oxide Computer Company

//! Summarize trimmed copies of real propolis-server logs and compare the
//! result against the saved output.  Run with EXPECTORATE=overwrite to
//! update the saved output after an intended change.
use std::path::Path;

/// Read every file in a fixture directory as (file name, contents).
fn load(dir: &str) -> Vec<(String, String)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(dir);
    let mut inputs: Vec<(String, String)> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
        .map(|entry| {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into();
            (name, std::fs::read_to_string(&path).unwrap())
        })
        .collect();
    // Sorted by name, the current log comes before the older rotated
    // one, so this also checks that crulog puts the files in order.
    inputs.sort();
    inputs
}

fn check(name: &str) {
    let summary = crulog::summarize(&load(&format!("tests/fixtures/{name}")));
    expectorate::assert_contents(
        format!("tests/output/{name}.txt"),
        &crulog::render::text(&summary),
    );
    expectorate::assert_contents(
        format!("tests/output/{name}.json"),
        &serde_json::to_string_pretty(&summary).unwrap(),
    );
}

#[test]
fn brm42220009() {
    check("BRM42220009");
}
