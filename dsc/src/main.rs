// Copyright 2022 Oxide Computer Company
#![feature(exit_status_error)]
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};

/// dsc  DownStairs Control
#[derive(Debug, Parser)]
#[clap(name = "dsc")]
#[clap(about = "A downstairs controller", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Test creation of downstairs regions
    Create {
        /// Delete any required directories before starting
        #[clap(long)]
        cleanup: bool,
    },
    // Create and start downstairs regions
    Start,
}

#[derive(Debug, Clone)]
struct DownstairsInfo {
    ds_bin: String,
    region_dir: String,
    port: u32,
    _create_output: String,
    output_file: PathBuf,
}

impl DownstairsInfo {
    fn start(self) -> Result<Child> {
        println!("Make output file at {:?}", self.output_file);
        let outputs = File::create(self.output_file)
            .context("Failed to create test file")?;
        let errors = outputs.try_clone()?;

        let port_value = format!("{}", self.port);

        let region_dir = self.region_dir;
        let cmd = Command::new(self.ds_bin)
            .args(&["run", "-p", &port_value, "-d", &region_dir])
            .stdout(Stdio::from(outputs))
            .stderr(Stdio::from(errors))
            .spawn()
            .context("Failed trying to run downstairs")?;

        println!(
            "Downstaris {} port {} PID:{:?}",
            region_dir,
            self.port,
            cmd.id()
        );

        Ok(cmd)
    }
}
#[derive(Debug)]
struct RegionSet {
    ds: Vec<DownstairsInfo>,
    ds_bin: String,
    region_dir: String,
}
#[derive(Debug)]
struct TestInfo {
    output_dir: PathBuf,
    rs: RegionSet,
    cmd: Vec<Child>,
}

impl TestInfo {
    fn new(
        downstairs_bin: String,
        output_dir: PathBuf,
        region_dir: PathBuf,
    ) -> Result<Self> {
        println!(
            "Creating test directory at: {}",
            output_dir.clone().into_os_string().into_string().unwrap()
        );
        fs::create_dir_all(&output_dir)
            .context("Failed to create test directory")?;

        println!(
            "Creating region directory at: {}",
            region_dir.clone().into_os_string().into_string().unwrap()
        );
        fs::create_dir_all(&region_dir)
            .context("Failed to create region directory")?;

        let rs = RegionSet {
            ds: Vec::new(),
            ds_bin: downstairs_bin,
            region_dir: region_dir.into_os_string().into_string().unwrap(),
        };
        Ok(TestInfo {
            output_dir,
            rs,
            cmd: Vec::new(),
        })
    }

    /**
     * Create a region as part of the region set at the given port with
     * the provided extent size and count.
     */
    fn create_ds_region(
        &mut self,
        port: u32,
        extent_count: u32,
        extent_size: u32,
        block_size: u32,
    ) -> Result<f32> {
        // Create the path for this region by combining the region
        // directory and the port this downstairs will use.
        let mut my_region = PathBuf::new();
        my_region.push(self.rs.region_dir.clone());
        my_region.push(format!("{}", port));

        let new_region_dir =
            my_region.clone().into_os_string().into_string().unwrap();
        let extent_count = format!("{}", extent_count);
        let extent_size = format!("{}", extent_size);
        let block_size = format!("{}", block_size);
        let uuid = format!("12345678-{0}-{0}-{0}-00000000{0}", port);
        let start = Instant::now();
        let output = Command::new(self.rs.ds_bin.clone())
            .args(&[
                "create",
                "-d",
                &new_region_dir,
                "--uuid",
                &uuid,
                "--extent-count",
                &extent_count,
                "--extent-size",
                &extent_size,
                "--block-size",
                &block_size,
            ])
            .output()
            .unwrap();

        let end = start.elapsed();
        let time_f = end.as_secs() as f32 + (end.subsec_nanos() as f32 / 1e9);

        if !output.status.success() {
            println!(
                "Create failed for {:?} {:?}",
                self.rs.region_dir, output.status
            );
            println!(
                "dir:{} uuid: {} es:{} ec:{}",
                new_region_dir, uuid, extent_size, extent_count,
            );
            println!("Output:\n{}", String::from_utf8(output.stdout).unwrap());
            println!("Error:\n{}", String::from_utf8(output.stderr).unwrap());
            bail!("Creating region failed");
        } else {
            println!(
                "Downstairs region created at {} in {:04}",
                new_region_dir,
                time_f,
            );
        }

        let output_file = format!("downstairs-{}.txt", port);
        let output_path = {
            let mut t = self.output_dir.clone();
            t.push(output_file);
            t
        };

        let dsi = DownstairsInfo {
            ds_bin: self.rs.ds_bin.clone(),
            region_dir: new_region_dir,
            port,
            _create_output: String::from_utf8(output.stdout).unwrap(),
            output_file: output_path,
        };
        self.rs.ds.push(dsi);
        Ok(time_f)
    }

    // Start all downstairs.
    fn start_all_downstairs(&mut self) -> Result<()> {
        for ds in self.rs.ds.iter() {
            let cmd = ds.clone().start().unwrap();
            println!("started ds: {:?}", cmd);
            self.cmd.push(cmd);
        }
        Ok(())
    }
}

fn startall() -> Result<()> {
    let ds_bin: String = "../target/release/crucible-downstairs".into();

    let output_dir: PathBuf = "/tmp/dsc".into();
    let region_dir: PathBuf = "/var/tmp/dsc/region".into();
// Put this in create function, return ti
    let mut ti = TestInfo::new(ds_bin, output_dir, region_dir).unwrap();
    let _ = ti.create_ds_region(3810, 20, 10, 4096).unwrap();
    let _ = ti.create_ds_region(3820, 20, 10, 4096).unwrap();
    let _ = ti.create_ds_region(3830, 20, 10, 4096).unwrap();

    println!("All regions created, now start all downstairs");
    ti.start_all_downstairs().unwrap();

    println!("ti: {:?}", ti);

    // Spawn a thread to watch each downstairs??
    //
    // Messages to tell each monitor thread what to do?
    for ads in ti.cmd.iter_mut() {
        match ads.try_wait() {
            Ok(Some(status)) => println!("exited with: {}", status),
            Ok(None) => {
                println!("status not ready yet, lets really wait");
                let res = ads.wait();
                println!("result: {:?}", res);
            }
            Err(e) => println!("error attempting to wait: {}", e),
        }
    }
    Ok(())

}
fn _create(cleanup: bool, extent_count: u32, extent_size: u32) -> Result<()> {
    let ds_bin: String = "../target/release/crucible-downstairs".into();

    let output_dir: PathBuf = "/tmp/dsc".into();
    let region_dir: PathBuf = "/var/tmp/dsc/region".into();

    if Path::new(&output_dir).exists() {
        if cleanup {
            std::fs::remove_dir_all(&output_dir)?;
        } else {
            bail!("Remove output {:?} before running", output_dir);
        }
    }
    if Path::new(&region_dir).exists() {
        if cleanup {
            std::fs::remove_dir_all(&region_dir)?;
        } else {
            bail!("Remove region {:?} before running", region_dir);
        }
    }

    let mut ti = TestInfo::new(ds_bin, output_dir, region_dir.clone()).unwrap();
    let _ = ti.create_ds_region(3810, extent_count, extent_size, 4096).unwrap();
    let _ = ti.create_ds_region(3820, extent_count, extent_size, 4096).unwrap();
    let _ = ti.create_ds_region(3830, extent_count, extent_size, 4096).unwrap();

    println!("All regions created");
    Ok(())
}

fn create_test(cleanup: bool, extent_count: u32, extent_size: u32) -> Result<()> {
    let ds_bin: String = "../target/release/crucible-downstairs".into();

    let output_dir: PathBuf = "/tmp/dsc".into();
    let region_dir: PathBuf = "/var/tmp/dsc/region".into();

    if Path::new(&output_dir).exists() {
        if cleanup {
            std::fs::remove_dir_all(&output_dir)?;
        } else {
            bail!("Remove output {:?} before running", output_dir);
        }
    }
    if Path::new(&region_dir).exists() {
        if cleanup {
            std::fs::remove_dir_all(&region_dir)?;
        } else {
            bail!("Remove region {:?} before running", region_dir);
        }
    }

    let mut times = Vec::new();
    let mut ti = TestInfo::new(ds_bin, output_dir, region_dir.clone())?;
    let ct = ti.create_ds_region(3810, extent_count, extent_size, 4096)?;
    times.push(ct);
    let ct = ti.create_ds_region(3820, extent_count, extent_size, 4096)?;
    times.push(ct);
    let ct = ti.create_ds_region(3830, extent_count, extent_size, 4096)?;
    times.push(ct);

    times.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    println!(
        "ES:{} EC:{} mean:{:.5} stdv:{:.5} \
        min:{:.5} max:{:.5}",
        extent_size, extent_count,
        statistical::mean(&times),
        statistical::standard_deviation(&times, None),
        times.first().unwrap(),
        times.last().unwrap(),
    );

    println!("All regions created");
    Ok(())
}

fn main() -> Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Create { cleanup } => {
            println!("Test cleanup:{}", cleanup);
            let extent_count = vec![10, 25, 50, 100, 150, 200, 256];
            for ec in extent_count.iter() {
                let es = (1024 * 200) / ec;
                create_test(cleanup, es, *ec)?;
            }
        }
	Commands::Start => {
            startall()?;
        }
    }
    Ok(())
}
