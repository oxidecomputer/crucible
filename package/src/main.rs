// Copyright 2022 Oxide Computer Company

use std::fs::OpenOptions;
use std::path::Path;

use serde::Serialize;
use tar::{Builder, Header};

#[derive(Serialize)]
pub struct OmicronZoneMetadata {
    #[serde(rename = "v")]
    version: usize,

    #[serde(rename = "t")]
    type_string: String,
}

fn main() -> anyhow::Result<()> {
    if Path::new("crucible.tar.gz").exists() {
        anyhow::bail!("crucible.tar.gz exists!");
    }

    let tarfile = OpenOptions::new()
        .write(true)
        .read(true)
        .truncate(true)
        .create(true)
        .open(&"crucible.tar.gz")?;

    let gzw =
        flate2::write::GzEncoder::new(tarfile, flate2::Compression::fast());

    let mut archive = Builder::new(gzw);
    archive.mode(tar::HeaderMode::Deterministic);

    // Add Omicron branded zone metadata
    let metadata = serde_json::to_string(&OmicronZoneMetadata {
        version: 1,
        type_string: "layer".to_string(),
    })?;
    let mut header = Header::new_gnu();
    header.set_size(metadata.len() as u64);
    header.set_cksum();
    archive.append_data(
        &mut header,
        "oxide.json",
        std::io::Cursor::new(metadata),
    )?;

    // SMF
    archive.append_path_with_name(
        "agent/downstairs.xml",
        "root/var/svc/manifest/site/downstairs.xml",
    )?;
    archive.append_path_with_name(
        "agent/agent.xml",
        "root/var/svc/manifest/site/agent.xml",
    )?;

    // Binaries
    archive.append_path_with_name(
        "target/release/crucible-downstairs",
        "root/opt/oxide/crucible/bin/downstairs",
    )?;
    archive.append_path_with_name(
        "target/release/crucible-agent",
        "root/opt/oxide/crucible/bin/agent",
    )?;
    archive.append_path_with_name(
        "agent/downstairs_method_script.sh",
        "root/opt/oxide/crucible/bin/downstairs_method_script.sh",
    )?;

    // Ok!
    Ok(())
}
