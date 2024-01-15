// Copyright 2022 Oxide Computer Company

use anyhow::Result;
use omicron_zone_package::config;
use omicron_zone_package::target::Target;
use std::fs::create_dir_all;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = config::parse("package-manifest.toml")?;

    let output_dir = Path::new("out");
    create_dir_all(output_dir)?;

    let packages = cfg.packages_to_deploy(&Target::default());
    let mut package_iter = packages.build_order();

    while let Some(batch) = package_iter.next() {
        for (name, package) in &batch {
            println!("Building '{name}'");
            package
                .create_for_target(&Target::default(), &name, output_dir)
                .await?;
        }
    }

    Ok(())
}
