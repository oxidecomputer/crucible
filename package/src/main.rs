// Copyright 2022 Oxide Computer Company

use anyhow::{bail, Result};
use omicron_zone_package::config;
use omicron_zone_package::package::{PackageOutput, PackageSource};
use omicron_zone_package::target::Target;
use std::collections::BTreeMap;
use std::fs::create_dir_all;
use std::path::Path;
use topological_sort::TopologicalSort;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = config::parse("package-manifest.toml")?;

    let output_dir = Path::new("out");
    create_dir_all(output_dir)?;

    // Reverse lookup of "output" -> "package creating this output".
    let all_packages = cfg
        .packages
        .iter()
        .map(|(name, package)| (package.get_output_file(name), (name, package)))
        .collect::<BTreeMap<_, _>>();

    // Collect all packages, and sort them in dependency order,
    // so we know which ones to build first.
    let mut outputs = TopologicalSort::<String>::new();
    for (package_output, (_, package)) in &all_packages {
        match &package.source {
            PackageSource::Local { .. }
            | PackageSource::Prebuilt { .. }
            | PackageSource::Manual => {
                // Skip intermediate leaf packages; if necessary they'll be
                // added to the dependency graph by whatever composite package
                // actually depends on them.
                if !matches!(
                    package.output,
                    PackageOutput::Zone {
                        intermediate_only: true
                    }
                ) {
                    outputs.insert(package_output);
                }
            }
            PackageSource::Composite { packages: deps } => {
                for dep in deps {
                    outputs.add_dependency(dep, package_output);
                }
            }
        }
    }

    while !outputs.is_empty() {
        let batch = outputs.pop_all();
        assert!(
            !batch.is_empty() || outputs.is_empty(),
            "cyclic dependency in package manifest!"
        );

        for output in &batch {
            println!("Creating '{output}'");

            let Some((name, package)) = all_packages.get(output) else {
                bail!(
                    "Cannot find a package to create output: '{output}' \n\
                     \tThis can happen when building a composite package, where one of \n\
                     \tthe 'source.packages' has not been found."
                );
            };

            package
                .create_for_target(&Target::default(), &name, output_dir)
                .await?;
        }
    }

    Ok(())
}
