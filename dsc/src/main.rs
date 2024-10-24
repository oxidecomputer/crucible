// Copyright 2023 Oxide Computer Company
use std::collections::VecDeque;
use std::fs;
use std::fs::File;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use byte_unit::Byte;
use clap::{Parser, Subcommand};
use csv::WriterBuilder;
use rand::prelude::*;
use rand_chacha::rand_core::SeedableRng;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use tokio::process::{Child, Command};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::{sleep_until, Duration, Instant};
use uuid::Uuid;

pub mod client;
pub mod control;
use client::{client_main, ClientCommand};
use crucible_client_types::RegionExtentInfo;
use crucible_common::{config_path, read_json, RegionDefinition};

/// dsc  DownStairs Controller
#[derive(Debug, Parser)]
#[clap(name = "dsc", term_width = 80)]
#[clap(about = "A downstairs controller", long_about = None)]
struct Args {
    #[clap(subcommand)]
    action: Action,
}

#[derive(Debug, Subcommand)]
enum Action {
    /// Send a command to a running dsc server
    Cmd {
        #[clap(subcommand)]
        client_command: ClientCommand,

        /// URL location of the Crucible control server
        #[clap(long, default_value = "http://127.0.0.1:9998", action)]
        server: String,
    },
    /// Create a downstairs region then exit.
    Create {
        /// The block size for the region
        #[clap(long, default_value = "4096", action)]
        block_size: u64,

        /// Delete all existing test and region directories
        #[clap(long, action)]
        cleanup: bool,

        /// Downstairs binary location
        #[clap(
            long,
            global = true,
            default_value = "target/release/crucible-downstairs",
            action
        )]
        ds_bin: String,

        /// If the regions will require encryption.
        #[clap(long, action)]
        encrypted: bool,

        /// The extent size for the region (in blocks NOT bytes!)
        #[clap(long, default_value = "100", action)]
        extent_size: u64,

        /// The extent count for the region
        #[clap(long, default_value = "15", action)]
        extent_count: u32,

        /// default output directory
        #[clap(long, global = true, default_value = "/tmp/dsc", action)]
        output_dir: PathBuf,

        /// The port_base where the downstairs will listen.
        /// Note: is used as part of the expected region directory path.
        #[clap(long, default_value = "8810", action)]
        port_base: u32,

        /// The directory where the downstairs regions will be created.
        /// Either provide once, or "N" times.  One means all downstairs
        /// share the same top level directory (each downstairs has its own
        /// subdirectory).  "N" means we will create "N" downstairs and each
        /// downstairs will get its own region directory.  If using N other
        /// than 3, you must also provide a matching "region-dir" value.
        /// Note that, for each downstairs, dsc adds a "port" to the
        /// region directory path.
        #[clap(
            long,
            global = true,
            default_value = "/var/tmp/dsc/region",
            action
        )]
        region_dir: Vec<PathBuf>,

        /// The number of region directories to create.
        #[clap(long, default_value = "3", action)]
        region_count: usize,
    },
    /// Test creation of downstairs regions
    RegionPerf {
        /// If supplied, also write create performance numbers in .csv
        /// format to the provided file name.
        #[clap(long, name = "CSV", action)]
        csv_out: Option<PathBuf>,

        /// Downstairs binary location
        #[clap(
            long,
            global = true,
            default_value = "target/release/crucible-downstairs",
            action
        )]
        ds_bin: String,

        /// Run a longer test, do 10 loops for each region size combo
        /// and report mean min max and stddev.
        #[clap(long, action)]
        long: bool,

        /// default output directory
        #[clap(long, global = true, default_value = "/tmp/dsc", action)]
        output_dir: PathBuf,

        /// The directory where the downstairs regions will be created.
        /// Either provide once, or three times.  One means all downstairs
        /// share the same top level directory (each downstairs has its own
        /// subdirectory).  Three means each downstairs will get its own
        /// region directory.
        #[clap(
            long,
            global = true,
            default_value = "/var/tmp/dsc/region",
            action
        )]
        region_dir: Vec<PathBuf>,
    },
    /// Start the requested downstairs regions
    /// This requires the region is already created, unless you include
    /// the --create option.
    Start {
        /// If creating, the block size for the region
        #[clap(long, default_value = "4096", action)]
        block_size: u64,

        /// Delete all existing test and region directories
        #[clap(long, action, requires = "create")]
        cleanup: bool,

        /// The IP/Port where the control server will listen
        #[clap(long, default_value = "127.0.0.1:9998", action)]
        control: SocketAddr,

        /// Delete any existing region and create a new one using the
        /// default or provided block-size, extent-size, and extent-count.
        #[clap(long, action)]
        create: bool,

        /// Downstairs binary location
        #[clap(
            long,
            global = true,
            default_value = "target/release/crucible-downstairs",
            action
        )]
        ds_bin: String,

        /// (Only used when creating) If the regions will require encryption.
        #[clap(long, action)]
        encrypted: bool,

        /// If creating, the extent size for the region (in blocks NOT bytes!)
        #[clap(long, default_value = "100", action)]
        extent_size: u64,

        /// If creating, the extent count for the region
        #[clap(long, default_value = "15", action)]
        extent_count: u32,

        /// Downstairs will all be started read only (default: false)
        #[clap(long, action, default_value = "false")]
        read_only: bool,

        /// default output directory
        #[clap(long, global = true, default_value = "/tmp/dsc", action)]
        output_dir: PathBuf,

        /// The port_base where the downstairs will listen
        /// Note: is used as part of the expected region directory path.
        #[clap(long, default_value = "8810", action)]
        port_base: u32,

        /// The directory where the downstairs regions will be created.
        /// Either provide once, or "N" times.  One means all downstairs
        /// share the same top level directory (each downstairs has its own
        /// subdirectory).  "N" means we will create "N" downstairs and each
        /// downstairs will get its own region directory.  If using N other
        /// than 3, you must also provide a matching "region-dir" value.
        /// Note that, for each downstairs, dsc adds a "port" to the
        /// region directory path.
        #[clap(
            long,
            global = true,
            default_value = "/var/tmp/dsc/region",
            action
        )]
        region_dir: Vec<PathBuf>,

        /// If creating The number of region directories to create.
        #[clap(long, default_value = "3", action)]
        region_count: usize,
    },
}

/// Information about a single downstairs.
#[derive(Debug, Clone)]
struct DownstairsInfo {
    ds_bin: String,
    region_dir: String,
    port: u32,
    uuid: Uuid,
    _create_output: String,
    output_file: PathBuf,
    client_id: usize,
    read_only: bool,
}

impl DownstairsInfo {
    #[allow(clippy::too_many_arguments)]
    fn new(
        ds_bin: String,
        region_dir: String,
        port: u32,
        uuid: Uuid,
        _create_output: String,
        output_file: PathBuf,
        client_id: usize,
        read_only: bool,
    ) -> DownstairsInfo {
        DownstairsInfo {
            ds_bin,
            region_dir,
            port,
            uuid,
            _create_output,
            output_file,
            client_id,
            read_only,
        }
    }

    fn start(&self) -> Result<Child> {
        println!("Make output file at {:?}", self.output_file);
        let outputs = File::create(&self.output_file)
            .context("Failed to create output file")?;
        let errors = outputs.try_clone()?;

        let port_value = format!("{}", self.port);

        let mode = if self.read_only {
            "ro".to_string()
        } else {
            "rw".to_string()
        };

        let region_dir = self.region_dir.clone();
        let cmd = Command::new(self.ds_bin.clone())
            .args([
                "run",
                "-p",
                &port_value,
                "-d",
                &region_dir,
                "--mode",
                &mode,
            ])
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

// Describing all the downstairs regions we know about.
#[derive(Debug)]
struct Regions {
    ds: Vec<Arc<DownstairsInfo>>,
    ds_bin: String,
    region_dir: Vec<String>,
    ds_state: Vec<DownstairsState>,
    ds_pid: Vec<Option<u32>>,
    port_base: u32,
    port_step: u32,
    region_info: Option<RegionExtentInfo>,
}

// This holds the overall info for the regions we have created.
#[derive(Debug)]
pub struct DscInfo {
    /// The directory location where output files are
    output_dir: PathBuf,
    /// The regions this dsc knows about
    rs: Mutex<Regions>,
    /// Work for the dsc to do, what downstairs to start/stop/etc
    work: Mutex<DscWork>,
    /// If the downstairs are started read only
    read_only: bool,
}

impl DscInfo {
    #[allow(clippy::too_many_arguments)]
    fn new(
        downstairs_bin: String,
        output_dir: PathBuf,
        region_dir: Vec<PathBuf>,
        notify_tx: watch::Sender<u64>,
        create: bool,
        port_base: u32,
        region_count: usize,
        read_only: bool,
    ) -> Result<Arc<Self>> {
        // Verify the downstairs binary exists as is a file
        if !Path::new(&downstairs_bin).exists() {
            bail!("Can't find downstairs binary at {:?}", downstairs_bin);
        }
        let md = std::fs::metadata(&downstairs_bin).unwrap();
        if !md.is_file() {
            bail!("{} is not a file", downstairs_bin);
        }

        // There should either be one region dir in the vec, or as many
        // directories as the region_count requested..
        // If there is one directory, then the downstairs will all share it.
        // If there are more, then each downstairs will get its own
        // directory.
        if region_dir.len() != 1 && region_dir.len() != region_count {
            bail!("Region directory needs one or {} elements", region_count);
        }

        if create {
            for rd in region_dir.iter() {
                // If the caller has requested to create a region, then
                // we expect it not to be present.
                if Path::new(&rd).exists() {
                    bail!("Remove region {:?} before running", rd);
                }
                println!(
                    "Creating region directory at: {}",
                    rd.clone().into_os_string().into_string().unwrap()
                );
                fs::create_dir_all(rd)
                    .context("Failed to create region directory")?;
            }

            // We only create the output directory if we need it.
            if !Path::new(&output_dir).exists() {
                println!(
                    "Creating dsc directory at: {}",
                    output_dir.clone().into_os_string().into_string().unwrap()
                );
                fs::create_dir_all(&output_dir)
                    .context("Failed to create dsc directory")?;
            }
        } else {
            // Everything should already exist
            for rd in region_dir.iter() {
                if !Path::new(&rd).exists() {
                    bail!("Failed to find region {:?}", rd);
                }
            }

            if Path::new(&output_dir).exists() {
                println!("Using existing output directory {:?}", output_dir);
            } else {
                println!(
                    "Creating dsc directory at: {}",
                    output_dir.clone().into_os_string().into_string().unwrap()
                );
                fs::create_dir_all(&output_dir)
                    .context("Failed to create dsc directory")?;
            }
        }

        let mut ds_state = Vec::new();
        let mut ds_pid = Vec::new();

        for _ in 0..region_count {
            ds_state.push(DownstairsState::Stopped);
            ds_pid.push(None);
        }

        // If we only have one region directory, then use that for all
        // downstairs.  If we have provided more than one region directory
        // (and a matching region_count) then each downstairs will have
        // its own region directory.
        let mut rv = Vec::new();

        if region_dir.len() == 1 {
            for _ in 0..region_count {
                rv.push(
                    region_dir[0]
                        .clone()
                        .into_os_string()
                        .into_string()
                        .unwrap(),
                );
            }
        } else {
            assert_eq!(region_dir.len(), region_count);
            for rd in region_dir.iter().take(region_count) {
                rv.push(rd.clone().into_os_string().into_string().unwrap());
            }
        };

        assert_eq!(rv.len(), region_count);
        let rs = Regions {
            ds: Vec::new(),
            ds_bin: downstairs_bin,
            region_dir: rv,
            ds_state,
            ds_pid,
            port_base,
            port_step: 10,
            region_info: None,
        };

        let mrs = Mutex::new(rs);

        let dsc_work = DscWork::new(notify_tx);
        let work = Mutex::new(dsc_work);
        // Received actions from the dropshot server.
        Ok(Arc::new(DscInfo {
            output_dir,
            rs: mrs,
            work,
            read_only,
        }))
    }

    /*
     * Create the requested set of regions.  Attach it to our dsc info struct
     */
    async fn create_regions(
        &self,
        extent_size: u64,
        extent_count: u32,
        block_size: u64,
        encrypted: bool,
        region_count: usize,
    ) -> Result<()> {
        for ds_id in 0..region_count {
            let _ = self
                .create_ds_region(
                    ds_id,
                    extent_size,
                    extent_count,
                    block_size,
                    false,
                    encrypted,
                )
                .await
                .unwrap();
        }
        println!("Created {region_count} regions");
        Ok(())
    }

    /**
     * Create a region with the provided extent size and extent_count.
     */
    async fn create_ds_region(
        &self,
        ds_id: usize,
        extent_size: u64,
        extent_count: u32,
        block_size: u64,
        quiet: bool,
        encrypted: bool,
    ) -> Result<f32> {
        // Create the path for this region by combining the region
        // directory and the port this downstairs will use.
        let mut rs = self.rs.lock().await;

        if let Some(cur_ri) = &rs.region_info {
            // Verify the new region info matches what we already have.
            if cur_ri.block_size != block_size {
                println!(
                    "WARNING: block size difference: {} vs. {}",
                    cur_ri.block_size, block_size
                );
            }
            if cur_ri.blocks_per_extent != extent_size {
                println!(
                    "WARNING: extent size difference: {} vs. {}",
                    cur_ri.blocks_per_extent, extent_size
                );
            }
            if cur_ri.extent_count != extent_count {
                println!(
                    "WARNING: extent count difference: {} vs. {}",
                    cur_ri.extent_count, extent_count
                );
            }
        } else {
            // If we don't have region info yet, set it now.
            rs.region_info = Some(RegionExtentInfo {
                block_size,
                blocks_per_extent: extent_size,
                extent_count,
            });
        }
        // The port is determined by ds_id and the port step value.
        let port = rs.port_base + (ds_id as u32 * rs.port_step);
        let rd = &rs.region_dir[ds_id];
        let new_region_dir = port_to_region(rd.clone(), port)?;
        let extent_size = format!("{}", extent_size);
        let extent_count = format!("{}", extent_count);
        let block_size = format!("{}", block_size);
        let uuid = format!("12345678-0000-0000-0000-{:012}", port);
        let ds_uuid = Uuid::parse_str(&uuid).unwrap();
        let start = std::time::Instant::now();
        let mut cmd_args = vec![
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
        ];
        if encrypted {
            cmd_args.push("--encrypted");
        }

        let output = Command::new(rs.ds_bin.clone())
            .args(&cmd_args)
            .output()
            .await
            .unwrap();

        let end = start.elapsed();
        let time_f = end.as_secs() as f32 + (end.subsec_nanos() as f32 / 1e9);

        if !output.status.success() {
            println!("Create failed for {:?} {:?}", rd, output.status);
            println!(
                "dir:{} uuid: {} es:{} ec:{}",
                new_region_dir, uuid, extent_size, extent_count,
            );
            println!("Output:\n{}", String::from_utf8(output.stdout).unwrap());
            println!("Error:\n{}", String::from_utf8(output.stderr).unwrap());
            bail!("Creating region failed");
        }

        if !quiet {
            println!(
                "Downstairs region {} created at {} in {:04}",
                ds_id, new_region_dir, time_f,
            );
        }

        let output_file = format!("downstairs-{}.txt", port);
        let output_path = {
            let mut t = self.output_dir.clone();
            t.push(output_file);
            t
        };

        let dsi = DownstairsInfo::new(
            rs.ds_bin.clone(),
            new_region_dir,
            port,
            ds_uuid,
            String::from_utf8(output.stdout).unwrap(),
            output_path,
            ds_id,
            self.read_only,
        );
        rs.ds.push(Arc::new(dsi));

        Ok(time_f)
    }

    /**
     * Delete a region directory at the given ds_id.
     */
    async fn delete_ds_region(&self, ds_id: usize) -> Result<()> {
        let rs = self.rs.lock().await;
        if rs.region_dir.len() < ds_id {
            bail!("Invalid index {} for downstairs regions", ds_id);
        }
        let rd = rs.region_dir[ds_id].clone();
        let port = rs.port_base + (ds_id as u32 * rs.port_step);
        let full_region_dir = port_to_region(rd, port)?;
        std::fs::remove_dir_all(full_region_dir)?;
        Ok(())
    }

    /*
     * Generate regions using the starting port and region directories.
     * Return error if any of them don't already exist.
     * TODO: This is assuming a fair amount of stuff.
     * Make fewer assumptions...
     */
    async fn generate_region_set(&self, region_count: usize) -> Result<()> {
        let mut rs = self.rs.lock().await;
        let mut port = rs.port_base;

        // Since we are generating our regions, we must create the required
        // directories and files.
        let mut region_info: Option<RegionExtentInfo> = None;
        for ds_id in 0..region_count {
            let rd = rs.region_dir[ds_id].clone();
            let new_region_dir = port_to_region(rd.clone(), port)?;
            let output_file = format!("downstairs-{}.txt", port);
            let output_path = {
                let mut t = self.output_dir.clone();
                t.push(output_file);
                t
            };
            if !Path::new(&new_region_dir).exists() {
                bail!("Can't find region dir {:?}", new_region_dir);
            }

            // Read the region config information from this region.
            let cp = config_path::<&Path>(new_region_dir.as_ref());
            let def: RegionDefinition = match read_json(&cp) {
                Ok(def) => def,
                Err(e) => {
                    bail!("Error {:?} opening region config {:?}", e, cp)
                }
            };
            let new_ri = RegionExtentInfo {
                block_size: def.block_size(),
                blocks_per_extent: def.extent_size().value,
                extent_count: def.extent_count(),
            };

            // We do expect all regions in a region set to be the same but
            // we can verify that here and warn if it is not.
            if let Some(cur_ri) = &region_info {
                if cur_ri.block_size != new_ri.block_size {
                    println!(
                        "WARNING: block size difference: {} vs. {}",
                        cur_ri.block_size, new_ri.block_size
                    );
                }
                if cur_ri.blocks_per_extent != new_ri.blocks_per_extent {
                    println!(
                        "WARNING: extent size difference: {} vs. {}",
                        cur_ri.blocks_per_extent, new_ri.blocks_per_extent
                    );
                }
                if cur_ri.extent_count != new_ri.extent_count {
                    println!(
                        "WARNING: extent count difference: {} vs. {}",
                        cur_ri.extent_count, new_ri.extent_count
                    );
                }
            } else {
                region_info = Some(new_ri);
            }

            let dsi = DownstairsInfo::new(
                rs.ds_bin.clone(),
                new_region_dir,
                port,
                def.uuid(),
                "/dev/null".to_string(),
                output_path,
                ds_id,
                self.read_only,
            );
            rs.ds.push(Arc::new(dsi));
            port += rs.port_step;
        }

        // Update our region information with what we found in the config file.
        println!("Update our region info with: {:?}", region_info);
        rs.region_info = region_info;

        Ok(())
    }

    async fn all_running(&self) -> bool {
        let rs = self.rs.lock().await;
        for state in rs.ds_state.iter() {
            if *state != DownstairsState::Running {
                return false;
            }
        }
        true
    }

    async fn get_ds_state(&self, client_id: usize) -> Result<DownstairsState> {
        let rs = self.rs.lock().await;
        if rs.ds_state.len() <= client_id {
            bail!("Invalid client ID: {}", client_id);
        }
        Ok(rs.ds_state[client_id])
    }

    async fn get_ds_pid(&self, client_id: usize) -> Result<Option<u32>> {
        let rs = self.rs.lock().await;
        if rs.ds_state.len() <= client_id {
            bail!("Invalid client ID: {}", client_id);
        }
        Ok(rs.ds_pid[client_id])
    }

    async fn get_ds_port(&self, client_id: usize) -> Result<u32> {
        let rs = self.rs.lock().await;
        if rs.ds.len() <= client_id {
            bail!("Invalid client ID: {}", client_id);
        }
        Ok(rs.ds[client_id].port)
    }

    async fn get_ds_uuid(&self, client_id: usize) -> Result<Uuid> {
        let rs = self.rs.lock().await;
        if rs.ds.len() <= client_id {
            bail!("Invalid client ID: {}", client_id);
        }
        Ok(rs.ds[client_id].uuid)
    }

    async fn get_region_info(&self) -> Result<RegionExtentInfo> {
        let rs = self.rs.lock().await;
        if let Some(ri) = &rs.region_info {
            Ok(ri.clone())
        } else {
            bail!("No region info found");
        }
    }

    async fn get_region_count(&self) -> usize {
        let rs = self.rs.lock().await;
        rs.ds.len()
    }
}

// This holds the work queue for the main task.  Work is added
// by the dropshot server, and the main task takes work off that
// queue and performs it on the required downstairs.  The notify
// channel is how the main task is told there is new work to do.
#[derive(Debug)]
pub struct DscWork {
    cmd: VecDeque<DscCmd>,
    notify_tx: watch::Sender<u64>,
}

impl DscWork {
    fn new(notify_tx: watch::Sender<u64>) -> Self {
        let cmd: VecDeque<DscCmd> = VecDeque::new();
        DscWork { cmd, notify_tx }
    }

    // Add a new command to the work list, and notify the main task.
    fn add_cmd(&mut self, cmd: DscCmd) {
        self.cmd.push_back(cmd);
        self.notify_tx.send(1).unwrap();
    }

    // Take the oldest command off the work queue.
    fn get_cmd(&mut self) -> Option<DscCmd> {
        self.cmd.pop_front()
    }
}

// Perform the work requested of us.  Some work is handled in the main
// loop. Work here are things that don't require modification of state in
// the main loop.
async fn do_dsc_work(
    work: DscCmd,
    action_tx_list: &[mpsc::Sender<DownstairsAction>],
) {
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();

    match work {
        DscCmd::Start(cid) => {
            println!("start {}", cid);
            action_tx_list[cid]
                .send(DownstairsAction::Start)
                .await
                .unwrap();
        }
        DscCmd::StartAll => {
            println!("start all downstairs: {}", action_tx_list.len());
            for action_tx in action_tx_list {
                action_tx.send(DownstairsAction::Start).await.unwrap();
            }
        }
        DscCmd::Stop(cid) => {
            println!("stop {}", cid);
            action_tx_list[cid]
                .send(DownstairsAction::Stop)
                .await
                .unwrap();
        }
        DscCmd::StopRand => {
            let cid = rng.gen_range(0..3) as usize;
            println!("stop rand {}", cid);
            action_tx_list[cid]
                .send(DownstairsAction::Stop)
                .await
                .unwrap();
        }
        DscCmd::StopAll => {
            println!("Stop all downstairs: {}", action_tx_list.len());
            for action_tx in action_tx_list {
                action_tx.send(DownstairsAction::Stop).await.unwrap();
            }
        }
        DscCmd::DisableRestart(cid) => {
            println!("disable restart {}", cid);
            action_tx_list[cid]
                .send(DownstairsAction::DisableRestart)
                .await
                .unwrap();
        }
        DscCmd::DisableRestartAll => {
            println!("disable restart on all: {}", action_tx_list.len());
            for action_tx in action_tx_list {
                action_tx
                    .send(DownstairsAction::DisableRestart)
                    .await
                    .unwrap();
            }
        }
        DscCmd::EnableRestart(cid) => {
            println!("enable restart {}", cid);
            action_tx_list[cid]
                .send(DownstairsAction::EnableRestart)
                .await
                .unwrap();
        }
        DscCmd::EnableRestartAll => {
            println!("enable restart on all: {}", action_tx_list.len());
            for action_tx in action_tx_list {
                action_tx
                    .send(DownstairsAction::EnableRestart)
                    .await
                    .unwrap();
            }
        }
        // Any other commands should not arrive here.
        _ => {
            panic!("Command {:?} not handled here", work);
        }
    }
}

/// Start the DownStairs Controller (dsc).
///
/// This task is the "main task" for the dsc server.  It is responsible for
/// the initial startup of all the downstairs, then to respond to commands
/// from the dropshot server (via a channel and work queue) and submit
/// those commands to the downstairs.
///
/// The dropshot server should already be running when this function
/// is called.
///
///                                       +-------------+
///              Http or API requests --> |  Dropshot   |   Async
///                                       |   server    |   task
///                                       +-------------+
///                                              |
///                                           notify_*
///                                           channel
///                  +-------------+             |
///                  |  DSC main   | <-----------+          Async
///                  |    task     |                        task
///                  +---+--+--+---+
///                      |  |  |
///                     action_*x
///                      channels
///                      |  |  |
///        +-------------+  |  +-------------+
///        |                |                |
/// +------+------+  +------+------+  +------+------+
/// |  Client 0   |  |  Client 1   |  |  Client 2   |       Async
/// |             |  |             |  |             |       task
/// |------|------|  |------|------|  |------|------|
/// | Downstairs  |  | Downstairs  |  | Downstairs  |       Spawned
/// |             |  |             |  |             |       process
/// +-------------+  +-------------+  +-------------+
///
/// Each client task spawns a process for their downstairs
/// and watches that process as well as the action channel
/// for changes requested by the main task.
///
/// When a command is received from the dropshot server, it puts that
/// command on a queue, then notifies the main task that new work has
/// arrived.  When the main task receives a notification, it walks
/// the queue and performs all jobs it finds.
async fn start_dsc(
    dsci: &DscInfo,
    mut notify_rx: watch::Receiver<u64>,
) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<MonitorInfo>(10);
    let mut action_tx_list = Vec::new();
    let mut handles = vec![];

    // Spawn a task to start and monitor each of our downstairs.
    let rs = dsci.rs.lock().await;
    for ds in rs.ds.iter() {
        println!("start ds: {:?}", ds.port);
        let txc = tx.clone();
        let dsc = ds.clone();
        let (action_tx, action_rx) = mpsc::channel(100);
        let handle = tokio::spawn(async move {
            ds_start_monitor(dsc, txc, action_rx).await;
        });
        action_tx_list.push(action_tx);
        handles.push(handle);
    }
    drop(tx);
    drop(rs);

    // Wait here for all downstairs to start
    let mut running = 0;
    loop {
        let res = rx.recv().await;
        if let Some(mi) = res {
            println!(
                "[{}][{}] initial start wait reports {:?}",
                mi.port, mi.client_id, mi.state
            );
            let mut rs = dsci.rs.lock().await;
            if mi.state == DownstairsState::Running {
                running += 1;
            }

            rs.ds_state[mi.client_id] = mi.state;
            rs.ds_pid[mi.client_id] = mi.pid;

            if running == rs.ds_state.len() {
                println!("All downstairs are running");
                break;
            }
        } else {
            println!("rx.recv got None");
        }
    }

    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();
    let mut timeout_deadline = Instant::now() + Duration::from_secs(5);
    let mut shutdown_sent = false;
    let mut random_restart = false;
    let mut restart_min = 9;
    let mut restart_max = 10;
    loop {
        tokio::select! {
            _ = sleep_until(timeout_deadline) => {

                // If we are trying to shutdown, go and check the state
                // of all downstairs and see if they have stopped.  If so,
                // we can exit this loop and the program.
                let timeout = if shutdown_sent {
                    let mut keep_waiting = false;
                    let rs = dsci.rs.lock().await;

                    for state in rs.ds_state.clone() {
                        match state {
                            DownstairsState::Stopping |
                            DownstairsState::Starting |
                            DownstairsState::Running => {
                                println!("Waiting for all downstairs to exit");
                                keep_waiting = true;
                                break;
                            },
                            _ => {},
                        }
                    }

                    if keep_waiting {
                        Duration::from_secs(1)
                    } else {
                        break;
                    }
                } else if random_restart {
                    println!("Random restart");
                    let cid = rng.gen_range(0..3) as usize;
                    println!("stop rand {}", cid);
                    action_tx_list[cid].send(DownstairsAction::Stop).await.unwrap();
                    Duration::from_secs(rng.gen_range(restart_min..restart_max))
                } else {
                    // Set the timeout to be a random value between our
                    // min and max.
                    Duration::from_secs(rng.gen_range(restart_min..restart_max))
                };
                timeout_deadline = Instant::now() + timeout;
            },
            _ = notify_rx.changed() => {
                println!("Main task has work to do, go find it");
                // We have to walk our job list here, as there are some
                // jobs that require local changes.

                let mut dsc_work = dsci.work.lock().await;
                while let Some(work) = dsc_work.get_cmd() {
                    println!("got dsc {:?}", work);

                    match work {
                        // Commands we handle here
                        DscCmd::EnableRandomStop => {
                            random_restart = true;
                        },
                        DscCmd::DisableRandomStop => {
                            random_restart = false;
                        },
                        DscCmd::RandomStopMin(min) => {
                            restart_min = min;
                        },
                        DscCmd::RandomStopMax(max) => {
                            restart_max = max;
                        },
                        DscCmd::Shutdown => {
                            println!("Shutdown");
                            for action_tx in action_tx_list.clone() {
                                action_tx.send(
                                    DownstairsAction::DisableRestart
                                ).await.unwrap();
                                action_tx.send(DownstairsAction::Stop).await.unwrap();
                            }
                            println!("Shut it down");
                            timeout_deadline =
                                Instant::now() + Duration::from_secs(1);
                            shutdown_sent = true;
                        }

                        // Commands handled in dsc_do_work
                        _ => {
                            do_dsc_work(work, &action_tx_list.clone()).await;
                        }
                    }
                }



            },
            res = rx.recv() => {
                if let Some(mi) = res {
                    println!("[{}][{}] reports {:?}",
                        mi.port, mi.client_id,
                        mi.state);
                    let mut rs = dsci.rs.lock().await;
                    rs.ds_state[mi.client_id] = mi.state;
                    rs.ds_pid[mi.client_id] = mi.pid;
                } else {
                    println!("rx.recv got None");
                }
            }
        }
    }
    Ok(())
}

/// Create a path for a region
///
/// By combining the region directory and the port this downstairs
/// will use, we construct the path for the region directory.
fn port_to_region(region_dir: String, port: u32) -> Result<String> {
    let mut my_region = PathBuf::new();
    my_region.push(region_dir);
    my_region.push(format!("{}", port));

    let mr = my_region.into_os_string().into_string().unwrap();
    Ok(mr)
}

/// Downstairs process information.
///
/// This structure is used to send status from a task responsible for
/// stop/start/status of a downstairs process and the main dsc task.
#[derive(Debug)]
struct MonitorInfo {
    port: u32,
    client_id: usize,
    state: DownstairsState,
    pid: Option<u32>,
}

/// State of a downstairs.
#[derive(Debug, Copy, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DownstairsState {
    Stopped,
    Stopping,
    Starting,
    Running,
    Exit,
    Error,
    Failed,
}

/// Downstairs actions.
///
/// This is used to send actions between the main task and a task that
/// is responsible stopping/starting/restarting a downstairs process.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, PartialEq)]
enum DownstairsAction {
    // Stop the downstairs
    Stop,
    // Start the downstairs
    // TODO, add lossy flag
    Start,
    // Disable auto restart
    DisableRestart,
    // Enable auto restart
    EnableRestart,
}

/// Commands supported by DSC.
///
/// These are the commands the main dsc task can accept from the dropshot
/// control server.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, PartialEq)]
enum DscCmd {
    /// Start the downstairs at the given client index.
    Start(usize),
    /// Start all downstairs.
    StartAll,
    /// Stop the downstairs at the given client index.
    Stop(usize),
    /// Stop a random downstairs.
    StopRand,
    /// Stop all downstairs.
    StopAll,
    /// Disable auto restart of the downstairs at the given client index.
    DisableRestart(usize),
    /// Disable auto restart of all downstairs.
    DisableRestartAll,
    /// Enable auto restart of downstairs at the given client index.
    EnableRestart(usize),
    /// Enable auto restart of all downstairs
    EnableRestartAll,
    /// Stop all downstairs, then stop ourselves (exit).
    Shutdown,
    /// Enable the random stopping of a downstairs at interval (min, max)
    EnableRandomStop,
    /// Disable the random stop of a downstairs
    DisableRandomStop,
    /// Set the minimum wait time between random stops
    RandomStopMin(u64),
    /// Set the maximum wait time between random stops
    RandomStopMax(u64),
}

/// Start a process for a downstairs.
///
/// Returns the <Child> of the spawned process.
async fn start_ds(
    ds: &Arc<DownstairsInfo>,
    tx: &mpsc::Sender<MonitorInfo>,
) -> Child {
    println!("Starting downstairs at port {}", ds.port);
    tx.send(MonitorInfo {
        port: ds.port,
        client_id: ds.client_id,
        state: DownstairsState::Starting,
        pid: None,
    })
    .await
    .unwrap();

    // Create a new process that will run the downstairs
    let cmd = ds.start().unwrap();
    tx.send(MonitorInfo {
        port: ds.port,
        client_id: ds.client_id,
        state: DownstairsState::Running,
        pid: cmd.id(),
    })
    .await
    .unwrap();
    cmd
}

/// Start and monitor a specific downstairs.
///
/// Start up a downstairs. Monitor that downstairs and send a message to
/// the main task if the downstairs exits.
/// Listen for commands from the main task and take action.
async fn ds_start_monitor(
    ds: Arc<DownstairsInfo>,
    tx: mpsc::Sender<MonitorInfo>,
    mut action_rx: mpsc::Receiver<DownstairsAction>,
) {
    // Start by starting.
    let mut cmd = start_ds(&ds, &tx).await;

    let mut keep_running = false;
    let mut start_once = false;
    let mut stop_notify = true;
    loop {
        tokio::select! {
            a = action_rx.recv() => {
                if let Some(act) = a {
                    match act {
                        DownstairsAction::Start => {
                            println!("[{}] got start action", ds.port);
                            // This does nothing if we are already
                            // running. If we are not running, then this
                            // will start the downstairs.
                            start_once = true;
                        },
                        DownstairsAction::Stop => {
                            println!(
                                "[{}] Got stop action so:{} kr:{}",
                                ds.port,
                                start_once,
                                keep_running);
                            start_once = false;
                            if let Err(e) = cmd.start_kill() {
                                println!(
                                    "[{}] Kill attempt returned {:?}",
                                    ds.port, e,
                                );
                            }
                        },
                        DownstairsAction::DisableRestart => {
                            keep_running = false;
                            start_once = false;
                            println!("[{}] Disable keep_running", ds.port);
                        },
                        DownstairsAction::EnableRestart => {
                            keep_running = true;
                            println!("[{}] Enable  keep_running", ds.port);
                        }
                    }
                } else {
                    println!("recv got error {:?}", a);
                }
            }
            c = cmd.wait() => {
                // If the downstairs is not running, this will match
                // every loop.
                match c {
                    Ok(status) => {
                        // Only notify we are down once,
                        if stop_notify {
                            println!("[{}] Exited with: {:?}", ds.port, status);
                                let state = {
                                    if status.code().is_some() {
                                    // There was a problem in the downstairs,
                                    // don't restart even if it was requested.
                                    keep_running = false;
                                    start_once = false;
                                    DownstairsState::Failed
                                } else {
                                    DownstairsState::Exit
                                }
                            };
                            let _ = tx.send(MonitorInfo {
                                port: ds.port,
                                client_id: ds.client_id,
                                state,
                                pid: cmd.id(),
                            }).await;
                            stop_notify = false;
                        }
                    }
                    Err(e) => {
                        // Just exit here?
                        let _ = tx.send(MonitorInfo {
                            port: ds.port,
                            client_id: ds.client_id,
                            state: DownstairsState::Error,
                            pid: cmd.id(),
                        }).await;
                        panic!(
                            "[{}] wait error {} from downstairs",
                            ds.port, e
                        );
                    }
                }
                // restart if desired, otherwise stay down.
                if keep_running || start_once {
                    println!("[{}] I am going to restart", ds.port);
                    cmd = start_ds(&ds, &tx).await;
                    start_once = false;
                    stop_notify = true;
                } else {
                    // Don't loop too fast if we are not required to restart
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
}

/*
 * Create a region with the given values in a loop.  Report the mean,
 * standard deviation, min, and max for the creation.
 * The region is created and deleted each time.
 */
async fn loop_create_test(
    dsci: &DscInfo,
    extent_size: u64,
    extent_count: u32,
    block_size: u64,
) -> Result<()> {
    let mut times = Vec::new();
    for _ in 0..5 {
        let ct = dsci
            .create_ds_region(
                0,
                extent_size,
                extent_count,
                block_size,
                true,
                false,
            )
            .await?;
        times.push(ct);
        dsci.delete_ds_region(0).await?;
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
fn region_si(es: u64, ec: u32, bs: u64) -> String {
    let sz = Byte::from_u64(bs * es * ec as u64);
    format!("{sz:#>11}")
}

/*
 * Return a formatted string of the extent file size in SI units
 */
fn efile_si(es: u64, bs: u64) -> String {
    let sz = Byte::from_u64(bs * es);
    format!("{sz:#>11}")
}

/*
 * Create a single downstairs region with the passed in values.
 * Report the time and stats in the standard format, then delete the region.
 */
async fn single_create_test(
    dsci: &DscInfo,
    extent_size: u64,
    extent_count: u32,
    block_size: u64,
    csv: &mut Option<&mut csv::Writer<File>>,
) -> Result<()> {
    let ct = dsci
        .create_ds_region(0, extent_size, extent_count, block_size, true, false)
        .await?;

    let size = region_si(extent_size, extent_count, block_size);
    let extent_file_size = efile_si(extent_size, block_size);
    println!(
        "{:>9.3} {}  {} {:>6} {:>6} {:>4}",
        ct, size, extent_file_size, extent_size, extent_count, block_size,
    );
    dsci.delete_ds_region(0).await?;

    // If requested, also write out the results to the csv file
    if let Some(csv) = csv {
        csv.serialize((
            ct,
            block_size * extent_size * extent_count as u64,
            block_size * extent_size,
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
async fn region_create_test(
    dsci: &DscInfo,
    long: bool,
    csv_out: Option<PathBuf>,
) -> Result<()> {
    let block_size: u64 = 4096;

    // The total region size we want for the test.  The total region
    // divided by the extent_size will give us the number of extents
    // the creation will require.
    //  XXX I've hard coded some "interesting" values here.  We may
    //  decide to either keep these, or set some different ones, or make
    //  an option to the test to allow it from the command line.
    //  Since the larger sizes can currently take minutes/hours, those
    //  are commented out as well.
    let region_size = [
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
    let extent_size = [4096, 8192, 16384, 32768];

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
            let ec = ((rs / block_size) / es) as u32;
            if long {
                loop_create_test(dsci, *es, ec, block_size).await?;
            } else {
                single_create_test(dsci, *es, ec, block_size, &mut csv_file)
                    .await?;
            }
        }
    }
    Ok(())
}

// Remove any existing dsc files/regions
fn cleanup(output_dir: PathBuf, region_dir: Vec<PathBuf>) -> Result<()> {
    if Path::new(&output_dir).exists() {
        println!("Removing existing dsc directory {:?}", output_dir);
        std::fs::remove_dir_all(&output_dir)?;
    }
    for rd in region_dir.iter() {
        if Path::new(&rd).exists() {
            println!("Removing existing region {:?}", rd);
            std::fs::remove_dir_all(rd)?;
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let runtime = Builder::new_multi_thread()
        .worker_threads(10)
        .thread_name("dsc")
        .enable_all()
        .build()
        .unwrap();

    // This channel is used to communicate between the dropshot server
    // and the dsc main thread.
    let (notify_tx, notify_rx) = watch::channel(0);

    match args.action {
        Action::Cmd {
            client_command,
            server,
        } => {
            client_main(server, client_command)?;
            Ok(())
        }
        Action::Create {
            block_size,
            cleanup,
            ds_bin,
            encrypted,
            extent_size,
            extent_count,
            output_dir,
            port_base,
            region_dir,
            region_count,
        } => {
            if cleanup {
                crate::cleanup(output_dir.clone(), region_dir.clone())?;
            }

            let dsci = DscInfo::new(
                ds_bin,
                output_dir,
                region_dir,
                notify_tx,
                true,
                port_base,
                region_count,
                false,
            )?;

            runtime.block_on(dsci.create_regions(
                extent_size,
                extent_count,
                block_size,
                encrypted,
                region_count,
            ))
        }
        Action::RegionPerf {
            csv_out,
            ds_bin,
            long,
            output_dir,
            region_dir,
        } => {
            let dsci = DscInfo::new(
                ds_bin, output_dir, region_dir, notify_tx, true, 8810, 3, false,
            )?;
            runtime.block_on(region_create_test(&dsci, long, csv_out))
        }
        Action::Start {
            block_size,
            cleanup,
            control,
            create,
            ds_bin,
            encrypted,
            extent_size,
            extent_count,
            output_dir,
            port_base,
            read_only,
            region_dir,
            region_count,
        } => {
            // Delete any existing region if requested

            if cleanup {
                crate::cleanup(output_dir.clone(), region_dir.clone())?;
            } else if create {
                for rd in region_dir.iter() {
                    if Path::new(&rd).exists() {
                        println!("Removing existing region {:?}", rd);
                        std::fs::remove_dir_all(rd)?;
                    }
                }
            }

            let dsci = DscInfo::new(
                ds_bin,
                output_dir,
                region_dir,
                notify_tx,
                create,
                port_base,
                region_count,
                read_only,
            )?;

            if create {
                runtime.block_on(dsci.create_regions(
                    extent_size,
                    extent_count,
                    block_size,
                    encrypted,
                    region_count,
                ))?;
            } else {
                runtime.block_on(dsci.generate_region_set(region_count))?;
            }

            let dsci_c = Arc::clone(&dsci);
            // Start the dropshot control endpoint
            runtime.spawn(control::begin(dsci_c, control));

            runtime.block_on(start_dsc(&dsci, notify_rx))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::{tempdir, NamedTempFile};

    // Create a temporary file.  Close the file but return the path
    // so it won't be deleted.
    fn temp_file_path() -> (String, tempfile::TempPath) {
        let ds_file = NamedTempFile::new().unwrap();
        let ds_path = ds_file.into_temp_path();
        let ds_bin = ds_path.to_str().unwrap().to_string();

        (ds_bin, ds_path)
    }

    #[test]
    fn new_ti() {
        // Test a typical creation
        let (ds_bin, _ds_path) = temp_file_path();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let (tx, _) = watch::channel(0);
        let region_vec = vec![dir.clone()];
        let res = DscInfo::new(
            ds_bin,
            dir.clone(),
            region_vec,
            tx,
            true,
            8810,
            3,
            false,
        );
        assert!(res.is_ok());
        assert!(Path::new(&dir).exists());
    }

    #[test]
    fn new_ti_three() {
        // Test a typical creation but with three region directories.
        let (ds_bin, _ds_path) = temp_file_path();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let r1 = tempdir().unwrap().as_ref().to_path_buf();
        let r2 = tempdir().unwrap().as_ref().to_path_buf();
        let r3 = tempdir().unwrap().as_ref().to_path_buf();
        let region_vec = vec![r1, r2, r3];
        let (tx, _) = watch::channel(0);
        let res = DscInfo::new(
            ds_bin,
            dir.clone(),
            region_vec.clone(),
            tx,
            true,
            8810,
            3,
            false,
        );
        assert!(res.is_ok());
        assert!(Path::new(&dir).exists());
        for rd in region_vec.iter() {
            assert!(Path::new(&rd).exists());
        }
    }

    #[test]
    fn new_ti_two_dirs() {
        // Test a invalid configuration with two region directories
        // but the default count of 3
        let (ds_bin, _ds_path) = temp_file_path();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let r1 = tempdir().unwrap().as_ref().to_path_buf();
        let r2 = tempdir().unwrap().as_ref().to_path_buf();
        let region_vec = vec![r1, r2];
        let (tx, _) = watch::channel(0);
        let res =
            DscInfo::new(ds_bin, dir, region_vec, tx, true, 8810, 3, false);
        assert!(res.is_err());
    }

    #[test]
    fn new_ti_two_region_count() {
        // Test a invalid configuration with three region directories
        // but with a region count of 2
        let (ds_bin, _ds_path) = temp_file_path();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let r1 = tempdir().unwrap().as_ref().to_path_buf();
        let r2 = tempdir().unwrap().as_ref().to_path_buf();
        let r3 = tempdir().unwrap().as_ref().to_path_buf();
        let region_vec = vec![r1, r2, r3];
        let (tx, _) = watch::channel(0);
        let res =
            DscInfo::new(ds_bin, dir, region_vec, tx, true, 8810, 2, false);
        assert!(res.is_err());
    }

    #[test]
    fn new_ti_four() {
        // Test a configuration with four region directories
        let (ds_bin, _ds_path) = temp_file_path();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let r1 = tempdir().unwrap().as_ref().to_path_buf();
        let r2 = tempdir().unwrap().as_ref().to_path_buf();
        let r3 = tempdir().unwrap().as_ref().to_path_buf();
        let r4 = tempdir().unwrap().as_ref().to_path_buf();
        let region_vec = vec![r1, r2, r3, r4];
        let (tx, _) = watch::channel(0);
        let res = DscInfo::new(
            ds_bin,
            dir.clone(),
            region_vec.clone(),
            tx,
            true,
            8810,
            4,
            false,
        );
        assert!(res.is_ok());
        assert!(Path::new(&dir).exists());
        assert_eq!(region_vec.len(), 4);
        for rd in region_vec.iter() {
            assert!(Path::new(&rd).exists());
        }
    }

    #[test]
    fn bad_bin() {
        // Send a directory instead of a file for downstairs_bin, should
        // return error.
        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let region_vec = vec![dir.clone()];
        let (tx, _) = watch::channel(0);
        let res = DscInfo::new(
            "/dev/null".to_string(),
            dir,
            region_vec,
            tx,
            true,
            8810,
            3,
            false,
        );

        assert!(res.is_err());
    }

    #[test]
    fn existing_ti() {
        // Try to create the same directories twice, which should
        // return error on the second try.
        let (ds_bin, _ds_path) = temp_file_path();

        let output_dir = tempdir().unwrap().as_ref().to_path_buf();
        let region_dir = tempdir().unwrap().as_ref().to_path_buf();
        let region_vec = vec![region_dir];
        // First create the new directories.
        let (tx, _) = watch::channel(0);
        DscInfo::new(
            ds_bin.clone(),
            output_dir.clone(),
            region_vec.clone(),
            tx,
            true,
            8810,
            3,
            false,
        )
        .unwrap();

        // Now, create them again and expect an error.
        let (tx, _) = watch::channel(0);
        let res = DscInfo::new(
            ds_bin, output_dir, region_vec, tx, true, 8810, 3, false,
        );
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn delete_bad_region() {
        // Test deletion of a region that does not exist, should report error.
        let (ds_bin, _ds_path) = temp_file_path();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let region_vec = vec![dir.clone()];
        let (tx, _) = watch::channel(0);
        let dsci =
            DscInfo::new(ds_bin, dir, region_vec, tx, true, 8810, 3, false)
                .unwrap();

        let res = dsci.delete_ds_region(0).await;
        println!("res is {:?}", res);
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn delete_bad_second_region() {
        // Test deletion of a region that does not exist, should report error.
        let (ds_bin, _ds_path) = temp_file_path();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let r1 = tempdir().unwrap().as_ref().to_path_buf();
        let r2 = tempdir().unwrap().as_ref().to_path_buf();
        let r3 = tempdir().unwrap().as_ref().to_path_buf();
        let region_vec = vec![r1.clone(), r2, r3];
        let (tx, _) = watch::channel(0);
        let dsci =
            DscInfo::new(ds_bin, dir, region_vec, tx, true, 8810, 3, false)
                .unwrap();

        // Manually create the first region directory.
        let ds_region_dir =
            port_to_region(r1.into_os_string().into_string().unwrap(), 8810)
                .unwrap();
        fs::create_dir_all(&ds_region_dir).unwrap();
        let res = dsci.delete_ds_region(1).await;
        assert!(res.is_err());
    }

    #[test]
    fn port_to_region_generation() {
        let nr = port_to_region("/var/tmp/rr".to_string(), 1234).unwrap();
        assert_eq!(nr, "/var/tmp/rr/1234".to_string());
    }

    #[tokio::test]
    async fn delete_region() {
        // Test creation then deletion of a region
        let (ds_bin, _ds_path) = temp_file_path();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let region_vec = vec![dir.clone()];
        let (tx, _) = watch::channel(0);
        let dsci = DscInfo::new(
            ds_bin,
            dir.clone(),
            region_vec,
            tx,
            true,
            8810,
            3,
            false,
        )
        .unwrap();

        // Manually create the region directory.  We have to convert the
        // PathBuf back into a string.
        let ds_region_dir =
            port_to_region(dir.into_os_string().into_string().unwrap(), 8810)
                .unwrap();
        fs::create_dir_all(&ds_region_dir).unwrap();

        let res = dsci.delete_ds_region(0).await;
        assert!(res.is_ok());
        assert!(!Path::new(&ds_region_dir).exists());
    }

    #[tokio::test]
    async fn restart_three_region() {
        // Test a typical creation
        let (ds_bin, _ds_path) = temp_file_path();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let region_vec = vec![dir.clone()];
        let (tx, _) = watch::channel(0);
        let dsci = DscInfo::new(
            ds_bin,
            dir.clone(),
            region_vec,
            tx,
            true,
            8810,
            3,
            false,
        )
        .unwrap();
        assert!(Path::new(&dir).exists());

        // Manually create the region set.  We have to convert the
        // PathBuf back into a string.
        for port in &[8810, 8820, 8830] {
            let ds_region_dir = port_to_region(
                dir.clone().into_os_string().into_string().unwrap(),
                *port,
            )
            .unwrap();
            fs::create_dir_all(ds_region_dir.clone()).unwrap();

            let cp = config_path::<&Path>(ds_region_dir.as_ref());
            let rd = RegionDefinition::default();
            crucible_common::write_json(&cp, &rd, false).unwrap();
        }

        // Verify that we can find the existing region directories.
        dsci.generate_region_set(3).await.unwrap();
        let rs = dsci.rs.lock().await;
        assert!(rs.region_info.is_some());
    }

    #[tokio::test]
    async fn restart_four_region() {
        // Test a restart with four region directories.
        let (ds_bin, _ds_path) = temp_file_path();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let region_vec = vec![dir.clone()];
        let (tx, _) = watch::channel(0);
        let dsci = DscInfo::new(
            ds_bin,
            dir.clone(),
            region_vec,
            tx,
            true,
            8810,
            4,
            false,
        )
        .unwrap();
        assert!(Path::new(&dir).exists());

        // Manually create the region set.  We have to convert the
        // PathBuf back into a string.
        for port in &[8810, 8820, 8830, 8840] {
            let ds_region_dir = port_to_region(
                dir.clone().into_os_string().into_string().unwrap(),
                *port,
            )
            .unwrap();
            fs::create_dir_all(ds_region_dir.clone()).unwrap();

            let cp = config_path::<&Path>(ds_region_dir.as_ref());
            let rd = RegionDefinition::default();
            crucible_common::write_json(&cp, &rd, false).unwrap();
        }

        // Verify that we can find the existing region directories.
        dsci.generate_region_set(4).await.unwrap();
        let rs = dsci.rs.lock().await;
        assert!(rs.region_info.is_some());
    }

    #[tokio::test]
    async fn restart_region_bad() {
        // Test region restart when a region directory is not present.
        // This should return error.
        let (ds_bin, _ds_path) = temp_file_path();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let region_vec = vec![dir.clone()];
        let (tx, _) = watch::channel(0);
        let dsci = DscInfo::new(
            ds_bin,
            dir.clone(),
            region_vec,
            tx,
            true,
            8810,
            3,
            false,
        )
        .unwrap();
        assert!(Path::new(&dir).exists());

        // Manually create the 2/3 of the region set.
        // We have to convert the PathBuf back into a string.
        for port in &[8810, 8830] {
            let ds_region_dir = port_to_region(
                dir.clone().into_os_string().into_string().unwrap(),
                *port,
            )
            .unwrap();
            fs::create_dir_all(ds_region_dir).unwrap();
        }

        // Verify that the missing region will return error.
        let res = dsci.generate_region_set(3).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn restart_region_four_bad() {
        // Test a restart with four region directories when only three
        // actually exist.
        let (ds_bin, _ds_path) = temp_file_path();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let region_vec = vec![dir.clone()];
        let (tx, _) = watch::channel(0);
        let dsci = DscInfo::new(
            ds_bin,
            dir.clone(),
            region_vec,
            tx,
            true,
            8810,
            4,
            false,
        )
        .unwrap();
        assert!(Path::new(&dir).exists());

        // Manually create the region set.  We have to convert the
        // PathBuf back into a string.
        for port in &[8810, 8820, 8830] {
            let ds_region_dir = port_to_region(
                dir.clone().into_os_string().into_string().unwrap(),
                *port,
            )
            .unwrap();
            fs::create_dir_all(ds_region_dir).unwrap();
        }

        // Verify that we report error.
        let res = dsci.generate_region_set(4).await;
        assert!(res.is_err());
    }
}
