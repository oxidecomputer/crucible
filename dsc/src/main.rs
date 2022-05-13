// Copyright 2022 Oxide Computer Company
#![feature(exit_status_error)]
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use byte_unit::Byte;
use clap::{Parser, Subcommand};
use csv::WriterBuilder;

/// dsc  DownStairs Controller
#[derive(Debug, Parser)]
#[clap(name = "dsc", term_width = 80)]
#[clap(about = "A downstairs controller", long_about = None)]
struct Cli {
    /// Delete any required directories before starting
    #[clap(long, global = true)]
    cleanup: bool,

    #[clap(subcommand)]
    command: Commands,

    /// Downstairs binary location
    #[clap(
        long,
        global = true,
        default_value = "target/release/crucible-downstairs"
    )]
    ds_bin: String,

    /// default output directory
    #[clap(long, global = true, default_value = "/tmp/dsc")]
    output_dir: PathBuf,

    /// default region directory
    #[clap(long, global = true, default_value = "/var/tmp/dsc/region")]
    region_dir: PathBuf,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Test creation of downstairs regions
    RegionPerf {
        /// Run a longer test, do 10 loops for each region size combo
        /// and report mean min max and stddev.
        #[clap(long)]
        long: bool,
        /// If supplied, also write create performance numbers in .csv
        /// format to the provided file name.
        #[clap(long, parse(from_os_str), name = "CSV")]
        csv_out: Option<PathBuf>,
    },
    /// Create and start downstairs regions
    Start {
        #[clap(long, default_value = "4096")]
        block_size: u32,

        #[clap(long, default_value = "100")]
        extent_size: u64,

        #[clap(long, default_value = "15")]
        extent_count: u64,
    },
}

/// Information about a single downstairs.
#[derive(Debug, Clone)]
struct DownstairsInfo {
    ds_bin: String,
    region_dir: String,
    port: u32,
    _create_output: String,
    output_file: PathBuf,
}

impl DownstairsInfo {
    fn new(
        ds_bin: String,
        region_dir: String,
        port: u32,
        _create_output: String,
        output_file: PathBuf,
    ) -> DownstairsInfo {
        DownstairsInfo {
            ds_bin,
            region_dir,
            port,
            _create_output,
            output_file,
        }
    }

    fn start(&self) -> Result<Child> {
        println!("Make output file at {:?}", self.output_file);
        let outputs = File::create(&self.output_file)
            .context("Failed to create test file")?;
        let errors = outputs.try_clone()?;

        let port_value = format!("{}", self.port);

        let region_dir = self.region_dir.clone();
        let cmd = Command::new(self.ds_bin.clone())
            .args(&["run", "-p", &port_value, "-d", &region_dir])
            .stdout(Stdio::from(outputs))
            .stderr(Stdio::from(errors))
            .spawn()
            .context("Failed trying to run downstairs")?;

        println!(
            "Downstairs {} port {} PID:{:?}",
            region_dir,
            self.port,
            cmd.id()
        );
        Ok(cmd)
    }
}

// Describing the downstairs that together make a region.
#[derive(Debug)]
struct RegionSet {
    ds: Vec<Arc<DownstairsInfo>>,
    ds_bin: String,
    region_dir: String,
}

// This holds the overall info for the regions we have created.
#[derive(Debug)]
struct TestInfo {
    output_dir: PathBuf,
    rs: RegionSet,
}

impl TestInfo {
    fn new(
        downstairs_bin: String,
        output_dir: PathBuf,
        region_dir: PathBuf,
    ) -> Result<Self> {
        /*
         * Verify binary exists, and paths do not
         */
        if !Path::new(&downstairs_bin).exists() {
            bail!("Can't find downstairs binary at {:?}", downstairs_bin);
        }
        let md = std::fs::metadata(&downstairs_bin).unwrap();
        if !md.is_file() {
            bail!("{} is not a file", downstairs_bin);
        }

        if Path::new(&output_dir).exists() {
            bail!("Remove output {:?} before running", output_dir);
        }
        if Path::new(&region_dir).exists() {
            bail!("Remove region {:?} before running", region_dir);
        }

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
        Ok(TestInfo { output_dir, rs })
    }

    /**
     * Create a region as part of the region set at the given port with
     * the provided extent size and count.
     *
     * TODO: Add encryption option
     */
    fn create_ds_region(
        &mut self,
        port: u32,
        extent_size: u64,
        extent_count: u64,
        block_size: u32,
        quiet: bool,
    ) -> Result<f32> {
        // Create the path for this region by combining the region
        // directory and the port this downstairs will use.
        let new_region_dir = port_to_region(self.rs.region_dir.clone(), port)?;
        let extent_size = format!("{}", extent_size);
        let extent_count = format!("{}", extent_count);
        let block_size = format!("{}", block_size);
        let uuid = format!("12345678-0000-0000-0000-{:012}", port);
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
        } else if !quiet {
            println!(
                "Downstairs region created at {} in {:04}",
                new_region_dir, time_f,
            );
        }

        let output_file = format!("downstairs-{}.txt", port);
        let output_path = {
            let mut t = self.output_dir.clone();
            t.push(output_file);
            t
        };

        let dsi = DownstairsInfo::new(
            self.rs.ds_bin.clone(),
            new_region_dir,
            port,
            String::from_utf8(output.stdout).unwrap(),
            output_path,
        );
        self.rs.ds.push(Arc::new(dsi));
        Ok(time_f)
    }

    /**
     * Delete a region directory at the given port.
     */
    fn delete_ds_region(&mut self, port: u32) -> Result<()> {
        // Create the path for this region by combining the region
        // directory and the port this downstairs will use.
        let region_dir = port_to_region(self.rs.region_dir.clone(), port)?;
        std::fs::remove_dir_all(&region_dir)?;

        // If this region was part of the ds vec, remove it.
        self.rs.ds.retain(|ds| ds.port != port);
        Ok(())
    }

    /**
     * Start all downstairs.
     * Walk the list of downstairs in our region set and spawn
     * a thread that will then start a downstairs and watch it.
     *
     * TODO: There is more coming here, this currently never returns
     * unless there is an error.  Eventually this will become the
     * controller of the three downstairs and will be able to stop/start
     * them as so desired.
     */
    fn start_all_downstairs(&mut self) {
        let (tx, rx) = mpsc::channel::<MonitorInfo>();
        let mut handles = vec![];

        for ds in self.rs.ds.iter() {
            println!("start ds: {:?}", ds.port);
            let txc = tx.clone();
            let dsc = ds.clone();
            let handle = thread::spawn(move || {
                ds_start_monitor(txc, dsc);
            });
            handles.push(handle);
        }
        drop(tx);
        loop {
            let res = rx.recv();
            match res {
                Ok(mi) => {
                    println!("[{}] reports {:?}", mi.port, mi.status);
                }
                Err(e) => {
                    println!("Receive thread stops with error {:?}", e);
                    break;
                }
            }
        }

        println!("recv channel is done");
    }
}

fn port_to_region(region_dir: String, port: u32) -> Result<String> {
    // Create the path for this region by combining the region
    // directory and the port this downstairs will use.
    let mut my_region = PathBuf::new();
    my_region.push(region_dir);
    my_region.push(format!("{}", port));

    let mr = my_region.into_os_string().into_string().unwrap();
    Ok(mr)
}

/*
 * This is used to communicate between the main thread and a thread that
 * is responsible for starting and monitoring a downstairs process.
 */
#[derive(Debug)]
struct MonitorInfo {
    port: u32,
    status: DownstairsStatus,
}

// Status of a downstairs.
#[derive(Debug)]
enum DownstairsStatus {
    Starting,
    Running,
    Exit,
    Error,
}

/*
 * Start up a downstairs. Monitor that downstairs and send a message to the
 * channel if the downstairs exits.
 *
 * TODO: This is really just the first stage of much more work here.
 * Right now all this does is start a downstairs then watch for what
 * happens, sending a message to another thread when things change. This
 * will become smarter in the future and will restart on failure and or
 * possibly also listen for commands from another thread.
 */
fn ds_start_monitor(tx: mpsc::Sender<MonitorInfo>, ds: Arc<DownstairsInfo>) {
    println!("starting downstairs at port {}", ds.port);
    let _ = tx.send(MonitorInfo {
        port: ds.port,
        status: DownstairsStatus::Starting,
    });

    let mut cmd = ds.start().unwrap();
    let _ = tx.send(MonitorInfo {
        port: ds.port,
        status: DownstairsStatus::Running,
    });

    match cmd.wait() {
        Ok(status) => {
            println!("[{}] exited with: {}", ds.port, status);
            let _ = tx.send(MonitorInfo {
                port: ds.port,
                status: DownstairsStatus::Exit,
            });
        }
        Err(e) => {
            println!("[{}] wait error {} from downstairs", ds.port, e);
            let _ = tx.send(MonitorInfo {
                port: ds.port,
                status: DownstairsStatus::Error,
            });
        }
    }
    println!("[{}] thread all done", ds.port);
}

/*
 * Create a default region set.  Attach it to our test info struct
 */
fn create_region_set(
    ti: &mut TestInfo,
    extent_size: u64,
    extent_count: u64,
    block_size: u32,
    port_base: u32,
) -> Result<()> {
    // How far apart the ports are for a default region set.
    // Note that this value is sprinkled all over the tests scripts,
    // so when you change it here, go forth and search.
    const DEFAULT_PORT_STEP: u32 = 10;
    let mut port = port_base;

    for _ in 0..3 {
        let _ = ti
            .create_ds_region(
                port,
                extent_size,
                extent_count,
                block_size,
                false,
            )
            .unwrap();
        port += DEFAULT_PORT_STEP;
    }
    println!("Region set was created");
    Ok(())
}

/*
 * Create a region with the given values in a loop.  Report the mean,
 * standard deviation, min, and max for the creation.
 * The region is created and deleted each time.
 */
fn loop_create_test(
    ti: &mut TestInfo,
    extent_size: u64,
    extent_count: u64,
    block_size: u32,
) -> Result<()> {
    let mut times = Vec::new();
    for _ in 0..5 {
        let ct = ti.create_ds_region(
            3810,
            extent_size,
            extent_count,
            block_size,
            true,
        )?;
        times.push(ct);
        ti.delete_ds_region(3810)?;
    }

    let size = region_si(extent_size, extent_count, block_size);
    let extent_file_size = efile_si(extent_size, block_size);
    times.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    println!(
        "{:>9.3} {}  {} {:>6} {:>6} {:>4}  {:5.3} {:8.3} {:8.3}",
        statistical::mean(&times),
        size,
        extent_file_size,
        extent_size,
        extent_count,
        block_size,
        statistical::standard_deviation(&times, None),
        times.first().unwrap(),
        times.last().unwrap(),
    );

    Ok(())
}

/*
 * Return a formatted string of the region size in SI units.
 */
fn region_si(es: u64, ec: u64, bs: u32) -> String {
    let sz = Byte::from_bytes((bs as u64 * es * ec).into());
    let bu = sz.get_appropriate_unit(true);
    format!("{:>11}", bu.to_string())
}

/*
 * Return a formatted string of the extent file size in SI units
 */
fn efile_si(es: u64, bs: u32) -> String {
    let sz = Byte::from_bytes((bs as u64 * es).into());
    let bu = sz.get_appropriate_unit(true);
    format!("{:>11}", bu.to_string())
}

/*
 * Create a single downstairs region with the passed in values.
 * Report the time and stats in the standard format, then delete the region.
 */
fn single_create_test(
    ti: &mut TestInfo,
    extent_size: u64,
    extent_count: u64,
    block_size: u32,
    csv: &mut Option<&mut csv::Writer<File>>,
) -> Result<()> {
    let ct =
        ti.create_ds_region(3810, extent_size, extent_count, block_size, true)?;

    let size = region_si(extent_size, extent_count, block_size);
    let extent_file_size = efile_si(extent_size, block_size);
    println!(
        "{:>9.3} {}  {} {:>6} {:>6} {:>4}",
        ct, size, extent_file_size, extent_size, extent_count, block_size,
    );
    ti.delete_ds_region(3810)?;

    // If requested, also write out the results to the csv file
    if let Some(csv) = csv {
        csv.serialize((
            ct,
            block_size as u64 * extent_size * extent_count,
            block_size as u64 * extent_size,
            extent_size,
            extent_count,
            block_size,
        ))?;
        csv.flush().unwrap();
    }

    Ok(())
}

/*
 * Run the region create test.
 * This will run a bunch of region creation commands in a loop, changing
 * the overall region size as well as blocks per extent (extent_size) and
 * total number of extent files (extent_count).
 */
fn region_create_test(
    ti: &mut TestInfo,
    long: bool,
    csv_out: Option<PathBuf>,
) -> Result<()> {
    let block_size: u32 = 4096;

    // The total region size we want for the test.  The total region
    // divided by the extent_size will give us the number of extents
    // the creation will require.
    //  XXX I've hard coded some "interesting" values here.  We may
    //  decide to either keep these, or set some different ones, or make
    //  an option to the test to allow it from the command line.
    //  Since the larger sizes can currently take minutes/hours, those
    //  are commented out as well.
    let region_size = vec![
        1024 * 1024 * 1024,        //   1 GiB
        1024 * 1024 * 1024 * 10,   //  10 GiB
        1024 * 1024 * 1024 * 100,  // 100 GiB
        1024 * 1024 * 1024 * 250,  // 250 GiB
        1024 * 1024 * 1024 * 500,  // 500 GiB
        1024 * 1024 * 1024 * 750,  // 750 GiB
        1024 * 1024 * 1024 * 1024, //   1 TiB
    ];

    // The list of blocks per extent file, in crucible: extent_size
    // XXX This is again some self selected interesting values.  Expect
    // these to change as we learn more.
    let extent_size = vec![4096, 8192, 16384, 32768];

    // This header is the same for both the regular and the long test.
    print!(
        "{:>9} {:>11}  {:>11} {:>6} {:>6} {:>4}",
        "SECONDS", "REGION_SIZE", "EXTENT_SIZE", "ES", "EC", "BS",
    );

    if long {
        // The longer test will print more info than the default
        print!("  {:>5} {:>8} {:>8}", "STDV", "MIN", "MAX");
    }
    println!();
    let mut csv_file = None;
    let mut csv;
    if let Some(csv_out) = csv_out {
        csv = WriterBuilder::new().from_path(csv_out).unwrap();
        csv.serialize((
            "SECONDS",
            "REGION_SIZE",
            "EXTENT_SIZE",
            "ES",
            "EC",
            "BS",
        ))?;
        csv.flush().unwrap();
        csv_file = Some(&mut csv);
    }

    for rs in region_size.iter() {
        for es in extent_size.iter() {
            // With power of 2 region sizes, the rs/es should always yield
            // a correct ec.
            let ec = (rs / (block_size as u64)) / es;
            if long {
                loop_create_test(ti, *es, ec, block_size)?;
            } else {
                single_create_test(ti, *es, ec, block_size, &mut csv_file)?;
            }
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Cli::parse();

    // XXX Some WIP here.  The eventual idea is to allow this tool to
    // use an existing region instead of creating one each time.
    // To avoid destroying the region by accident, we have a flag that
    // specifies it is okay to delete it.
    if Path::new(&args.output_dir).exists() {
        if args.cleanup {
            std::fs::remove_dir_all(&args.output_dir)?;
        } else {
            bail!("Remove output {:?} before running", args.output_dir);
        }
    }
    if Path::new(&args.region_dir).exists() {
        if args.cleanup {
            std::fs::remove_dir_all(&args.region_dir)?;
        } else {
            bail!("Remove region {:?} before running", args.region_dir);
        }
    }

    let mut ti =
        TestInfo::new(args.ds_bin, args.output_dir, args.region_dir).unwrap();

    match args.command {
        Commands::RegionPerf { long, csv_out } => {
            region_create_test(&mut ti, long, csv_out)?;
        }
        Commands::Start {
            block_size,
            extent_size,
            extent_count,
        } => {
            create_region_set(
                &mut ti,
                extent_size,
                extent_count,
                block_size,
                8810,
            )?;
            ti.start_all_downstairs();
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn new_ti() {
        // Test a typical creation
        // The empty file fake_bin is good enough for our test here.
        let fake_bin = "./dsbin".to_string();
        File::create(fake_bin.clone()).unwrap();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let res = TestInfo::new(fake_bin, dir.clone(), dir.clone());
        assert!(res.is_ok());
        assert!(Path::new(&dir).exists());
    }

    #[test]
    fn bad_bin() {
        // Send a directory instead of a file for downstairs_bin, should
        // return error.
        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let res = TestInfo::new("/dev/null".to_string(), dir.clone(), dir);

        assert!(res.is_err());
    }

    #[test]
    fn existing_ti() {
        // Try to create the same directories twice, which should
        // return error on the second try.
        let fake_bin = "./dsbin".to_string();
        File::create(fake_bin.clone()).unwrap();

        let output_dir = tempdir().unwrap().as_ref().to_path_buf();
        let region_dir = tempdir().unwrap().as_ref().to_path_buf();
        // First create the new directories.
        TestInfo::new(fake_bin.clone(), output_dir.clone(), region_dir.clone())
            .unwrap();
        // Now, create them again and expect an error.
        let res = TestInfo::new(fake_bin, output_dir, region_dir);
        assert!(res.is_err());
    }

    #[test]
    fn delete_bad_region() {
        // Test deletion of a region that does not exist, should report error.
        let fake_bin = "./dsbin".to_string();
        File::create(fake_bin.clone()).unwrap();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let mut ti = TestInfo::new(fake_bin, dir.clone(), dir).unwrap();

        let res = ti.delete_ds_region(8810);
        assert!(res.is_err());
    }

    #[test]
    fn delete_region() {
        // Test creation then deletion of a region
        let fake_bin = "./dsbin".to_string();
        File::create(fake_bin.clone()).unwrap();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let mut ti = TestInfo::new(fake_bin, dir.clone(), dir.clone()).unwrap();

        // Manually create the region directory.  We have to convert the
        // PathBuf back into a string.
        let ds_region_dir = port_to_region(
            dir.clone().into_os_string().into_string().unwrap(),
            8810,
        )
        .unwrap();
        fs::create_dir_all(&ds_region_dir).unwrap();

        let res = ti.delete_ds_region(8810);
        assert!(res.is_ok());
        assert!(!Path::new(&ds_region_dir).exists());
    }
}
