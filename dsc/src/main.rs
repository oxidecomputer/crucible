// Copyright 2022 Oxide Computer Company
#![feature(exit_status_error)]
use std::collections::VecDeque;
use std::fs;
use std::fs::File;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
//use std::process::{Child, Command, Stdio};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
//use std::thread;
// use std::time::Instant;

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
use tokio::sync::{mpsc, watch};
use tokio::time::sleep_until;

pub mod control;

// How far apart the ports are for a default region set.
// Note that this value is sprinkled all over the tests scripts,
// so when you change it here, go forth and search.
const DEFAULT_PORT_STEP: u32 = 10;

/// dsc  DownStairs Controller
#[derive(Debug, Parser)]
#[clap(name = "dsc", term_width = 80)]
#[clap(about = "A downstairs controller", long_about = None)]
struct Cli {
    /// Delete all existing test and region directories
    #[clap(long, global = true, action)]
    cleanup: bool,

    #[clap(subcommand)]
    command: Commands,

    /// Downstairs binary location
    #[clap(
        long,
        global = true,
        default_value = "target/release/crucible-downstairs",
        action
    )]
    ds_bin: String,

    /// default output directory
    #[clap(long, global = true, default_value = "/tmp/dsc", action)]
    output_dir: PathBuf,

    /// default region directory
    #[clap(long, global = true, default_value = "/var/tmp/dsc/region", action)]
    region_dir: PathBuf,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Create a downstairs region then exit.
    Create {
        /// The block size for the region
        #[clap(long, default_value = "4096", action)]
        block_size: u32,

        /// The extent size for the region
        #[clap(long, default_value = "100", action)]
        extent_size: u64,

        /// The extent count for the region
        #[clap(long, default_value = "15", action)]
        extent_count: u64,
    },
    /// Test creation of downstairs regions
    RegionPerf {
        /// Run a longer test, do 10 loops for each region size combo
        /// and report mean min max and stddev.
        #[clap(long, action)]
        long: bool,
        /// If supplied, also write create performance numbers in .csv
        /// format to the provided file name.
        #[clap(long, name = "CSV", action)]
        csv_out: Option<PathBuf>,
    },
    /// Start a downstairs region set
    /// This requires the region is already created, unless you include
    /// the --create option.
    Start {
        /// Delete any existing region and create a new one using the
        /// default or provided block-size, extent-size, and extent-count.
        #[clap(long, action)]
        create: bool,

        /// If creating, the block size for the region
        #[clap(long, default_value = "4096", action)]
        block_size: u32,

        /// If creating, the extent size for the region
        #[clap(long, default_value = "100", action)]
        extent_size: u64,

        /// If creating, the extent count for the region
        #[clap(long, default_value = "15", action)]
        extent_count: u64,

        /// The IP/Port where the control server will listen
        #[clap(long, default_value = "127.0.0.1:9998", action)]
        control: SocketAddr,
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
    client_id: usize,
}

impl DownstairsInfo {
    fn new(
        ds_bin: String,
        region_dir: String,
        port: u32,
        _create_output: String,
        output_file: PathBuf,
        client_id: usize,
    ) -> DownstairsInfo {
        DownstairsInfo {
            ds_bin,
            region_dir,
            port,
            _create_output,
            output_file,
            client_id,
        }
    }

    fn start(&self) -> Result<Child> {
        println!("Make output file at {:?}", self.output_file);
        let outputs = File::create(&self.output_file)
            .context("Failed to create output file")?;
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
    next_id: usize,
    ds_state: Vec<DownstairsState>,
    ds_pid: Vec<Option<u32>>,
}

impl RegionSet {
    // The next unused client ID.
    fn next_client_id(&mut self) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

// This holds the overall info for the regions we have created.
#[derive(Debug)]
pub struct DscInfo {
    /// The directory location where output files are
    output_dir: PathBuf,
    /// The region set that make our downstairs
    rs: Mutex<RegionSet>,
    /// Work for the dsc to do, what downstairs to start/stop/etc
    work: Mutex<DscWork>,
}

impl DscInfo {
    fn new(
        downstairs_bin: String,
        output_dir: PathBuf,
        region_dir: PathBuf,
        notify_tx: watch::Sender<u64>,
        create: bool,
    ) -> Result<Arc<Self>> {
        // Verify the downstairs binary exists as is a file
        if !Path::new(&downstairs_bin).exists() {
            bail!("Can't find downstairs binary at {:?}", downstairs_bin);
        }
        let md = std::fs::metadata(&downstairs_bin).unwrap();
        if !md.is_file() {
            bail!("{} is not a file", downstairs_bin);
        }

        if create {
            // If the caller has requested to create a region, then
            // we expect it not to be present.
            if Path::new(&region_dir).exists() {
                bail!("Remove region {:?} before running", region_dir);
            }
            println!(
                "Creating region directory at: {}",
                region_dir.clone().into_os_string().into_string().unwrap()
            );
            fs::create_dir_all(&region_dir)
                .context("Failed to create region directory")?;

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
            if !Path::new(&region_dir).exists() {
                bail!("Failed to find region {:?}", region_dir);
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

        let ds_state = vec![
            DownstairsState::Stopped,
            DownstairsState::Stopped,
            DownstairsState::Stopped,
        ];
        let ds_pid = vec![None, None, None];

        let rs = RegionSet {
            ds: Vec::new(),
            ds_bin: downstairs_bin,
            region_dir: region_dir.into_os_string().into_string().unwrap(),
            next_id: 0,
            ds_state,
            ds_pid,
        };

        let mrs = Mutex::new(rs);

        let dsc_work = DscWork::new(notify_tx);
        let work = Mutex::new(dsc_work);
        // Received actions from the dropshot server.
        Ok(Arc::new(DscInfo {
            output_dir,
            rs: mrs,
            work,
        }))
    }

    /**
     * Create a region as part of the region set at the given port with
     * the provided extent size and count.
     *
     * TODO: Add encryption option
     */
    async fn create_ds_region(
        &self,
        port: u32,
        extent_size: u64,
        extent_count: u64,
        block_size: u32,
        quiet: bool,
    ) -> Result<f32> {
        // Create the path for this region by combining the region
        // directory and the port this downstairs will use.
        let mut rs = self.rs.lock().unwrap();
        let new_region_dir = port_to_region(rs.region_dir.clone(), port)?;
        let extent_size = format!("{}", extent_size);
        let extent_count = format!("{}", extent_count);
        let block_size = format!("{}", block_size);
        let uuid = format!("12345678-0000-0000-0000-{:012}", port);
        let start = std::time::Instant::now();
        let output = Command::new(rs.ds_bin.clone())
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
            .await
            .unwrap();

        let end = start.elapsed();
        let time_f = end.as_secs() as f32 + (end.subsec_nanos() as f32 / 1e9);

        if !output.status.success() {
            println!(
                "Create failed for {:?} {:?}",
                rs.region_dir, output.status
            );
            println!(
                "dir:{} uuid: {} es:{} ec:{}",
                new_region_dir, uuid, extent_size, extent_count,
            );
            println!("Output:\n{}", String::from_utf8(output.stdout).unwrap());
            println!("Error:\n{}", String::from_utf8(output.stderr).unwrap());
            bail!("Creating region failed");
        }

        let client_id = rs.next_client_id();
        if !quiet {
            println!(
                "Downstairs region {} created at {} in {:04}",
                client_id, new_region_dir, time_f,
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
            String::from_utf8(output.stdout).unwrap(),
            output_path,
            client_id,
        );
        rs.ds.push(Arc::new(dsi));
        Ok(time_f)
    }

    /**
     * Delete a region directory at the given port.
     */
    fn delete_ds_region(&self, port: u32) -> Result<()> {
        // Create the path for this region by combining the region
        // directory and the port this downstairs will use.
        let mut rs = self.rs.lock().unwrap();
        let region_dir = port_to_region(rs.region_dir.clone(), port)?;
        std::fs::remove_dir_all(&region_dir)?;

        // If this region was part of the ds vec, remove it.
        rs.ds.retain(|ds| ds.port != port);
        Ok(())
    }

    /*
     * Generate a region set given the starting port and expected region
     * directories.  Return error if any of them don't already exist.
     * TODO: This is assuming a fair amount of stuff.
     * Make fewer assumptions...
     */
    fn generate_region_set(&self, port_base: u32) -> Result<()> {
        let mut port = port_base;
        let mut rs = self.rs.lock().unwrap();

        for _ in 0..3 {
            let new_region_dir = port_to_region(rs.region_dir.clone(), port)?;
            let output_file = format!("downstairs-{}.txt", port);
            let output_path = {
                let mut t = self.output_dir.clone();
                t.push(output_file);
                t
            };
            if !Path::new(&new_region_dir).exists() {
                bail!("Can't find region dir {:?}", new_region_dir);
            }

            let dsi = DownstairsInfo::new(
                rs.ds_bin.clone(),
                new_region_dir,
                port,
                "/dev/null".to_string(),
                output_path,
                rs.next_client_id(),
            );
            rs.ds.push(Arc::new(dsi));
            port += DEFAULT_PORT_STEP;
        }

        Ok(())
    }

    fn get_ds_state(&self, client_id: usize) -> Result<DownstairsState> {
        let rs = self.rs.lock().unwrap();
        if rs.ds_state.len() <= client_id {
            bail!("Invalid client ID: {}", client_id);
        }
        Ok(rs.ds_state[client_id])
    }

    fn get_ds_pid(&self, client_id: usize) -> Result<Option<u32>> {
        let rs = self.rs.lock().unwrap();
        if rs.ds_state.len() <= client_id {
            bail!("Invalid client ID: {}", client_id);
        }
        Ok(rs.ds_pid[client_id])
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

pub fn deadline_secs(secs: u64) -> tokio::time::Instant {
    tokio::time::Instant::now()
        .checked_add(tokio::time::Duration::from_secs(secs))
        .unwrap()
}

// This is where the main task has received notification that there is new
// work on the work queue.  Take the oldest item on the list and then
// send a message to one (or possibly all) downstairs tasks with the
// piece of work they next need to do.
// TODO: "start random restart with interval", we have to return
// something from this?  Or send it to each DS?
fn do_dsc_work(dsci: &DscInfo, action_tx_list: &[watch::Sender<Action>]) {
    let mut dsc_work = dsci.work.lock().unwrap();
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();
    while let Some(work) = dsc_work.get_cmd() {
        println!("got dsc {:?}", work);
        match work {
            DscCmd::Start(cid) => {
                println!("start {}", cid);
                action_tx_list[cid].send(Action::Start).unwrap();
            }
            DscCmd::StartAll => {
                println!("start all downstairs: {}", action_tx_list.len());
                for action_tx in action_tx_list {
                    action_tx.send(Action::Start).unwrap();
                }
            }
            DscCmd::Stop(cid) => {
                println!("stop {}", cid);
                action_tx_list[cid].send(Action::Stop).unwrap();
            }
            DscCmd::StopRand => {
                let cid = rng.gen_range(0..3) as usize;
                println!("stop rand {}", cid);
                action_tx_list[cid].send(Action::Stop).unwrap();
            }
            DscCmd::StopAll => {
                println!("Stop all downstairs: {}", action_tx_list.len());
                for action_tx in action_tx_list {
                    action_tx.send(Action::Stop).unwrap();
                }
            }
            DscCmd::DisableRestart(cid) => {
                println!("disable restart {}", cid);
                action_tx_list[cid].send(Action::DisableRestart).unwrap();
            }
            DscCmd::DisableRestartAll => {
                println!("disable restart on all: {}", action_tx_list.len());
                for action_tx in action_tx_list {
                    action_tx.send(Action::DisableRestart).unwrap();
                }
            }
            DscCmd::EnableRestart(cid) => {
                println!("enable restart {}", cid);
                action_tx_list[cid].send(Action::EnableRestart).unwrap();
            }
            DscCmd::EnableRestartAll => {
                println!("enable restart on all: {}", action_tx_list.len());
                for action_tx in action_tx_list {
                    action_tx.send(Action::EnableRestart).unwrap();
                }
            }
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
///  Http or API requests from outside -> |  Dropshot   |   Async
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
    let rs = dsci.rs.lock().unwrap();
    for ds in rs.ds.iter() {
        println!("start ds: {:?}", ds.port);
        let txc = tx.clone();
        let dsc = ds.clone();
        let (action_tx, action_rx) = watch::channel(Action::Start);
        let handle = tokio::spawn(async move {
            ds_start_monitor(dsc, txc, action_rx).await;
        });
        action_tx_list.push(action_tx);
        handles.push(handle);
    }
    drop(tx);
    drop(rs);

    let mut timeout_deadline = deadline_secs(5);
    loop {
        tokio::select! {
            _ = sleep_until(timeout_deadline) => {
                // println!("Timeout");
                timeout_deadline = deadline_secs(15);
            },
            _ = notify_rx.changed() => {
                println!("Main task has work to do, go find it");
                do_dsc_work(dsci, &action_tx_list);
            },
            res = rx.recv() => {
                if let Some(mi) = res {
                    println!("[{}][{}] reports {:?}",
                        mi.port, mi.client_id,
                        mi.state);
                    let mut rs = dsci.rs.lock().unwrap();
                    rs.ds_state[mi.client_id] = mi.state;
                    rs.ds_pid[mi.client_id] = mi.pid;
                } else {
                    println!("rx.recv got None");
                }
            }
        }
    }
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
#[derive(Debug, Copy, Clone, Deserialize, Serialize, JsonSchema)]
enum DownstairsState {
    Stopped,
    Stopping,
    Starting,
    Running,
    Exit,
    Error,
}

/// Downstairs action commands.
///
/// This is used to send commands between the main task and a task that
/// is responsible stopping/starting/restarting a downstairs process.
#[derive(Debug, PartialEq)]
enum Action {
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
    mut action_rx: watch::Receiver<Action>,
) {
    // Start by starting.
    let mut cmd = start_ds(&ds, &tx).await;

    let mut keep_running = true;
    let mut start_once = false;
    let mut stop_notify = true;
    loop {
        tokio::select! {
            a = action_rx.changed() => {
                match a {
                    Ok(_) => {
                        let act = action_rx.borrow();
                        match *act {
                            Action::Start => {
                                println!("[{}] got start action", ds.port);
                                // This does nothing if we are already
                                // running. If we are not running, then this
                                // will start the downstairs.
                                start_once = true;
                            },
                            Action::Stop => {
                                println!(
                                    "[{}] got stop action so:{} kr:{}",
                                    ds.port,
                                    start_once,
                                    keep_running);
                                start_once = false;
                                if let Err(e) = cmd.start_kill() {
                                    println!(
                                        "[{}] kill attempt returned {:?}",
                                        ds.port, e,
                                    );
                                }
                            },
                            Action::DisableRestart => {
                                keep_running = false;
                                start_once = false;
                                println!("[{}] Disable keep_running", ds.port);
                            },
                            Action::EnableRestart => {
                                keep_running = true;
                                println!("[{}] Enable  keep_running", ds.port);
                            }
                        }
                    },
                    Err(e) => {
                        println!("recv got error {:?}", e);
                    }
                }
            }
            c = cmd.wait() => {
                // If the downstairs is not running, this will match
                // every loop.
                match c {
                    Ok(status) => {
                        // Only notify we are down once,
                        if stop_notify {
                            println!("[{}] exited with: {}", ds.port, status);
                            let _ = tx.send(MonitorInfo {
                                port: ds.port,
                                client_id: ds.client_id,
                                state: DownstairsState::Exit,
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
                    println!("[{}]I am going to restart", ds.port);
                    cmd = start_ds(&ds, &tx).await;
                    start_once = false;
                    stop_notify = true;
                } else {
                    // Don't loop too fast if we are not required to restart
                    tokio::time::sleep_until(deadline_secs(2)).await;
                }
            }
        }
    }
}

/*
 * Create a default region set.  Attach it to our dsc info struct
 */
async fn create_region_set(
    dsci: &DscInfo,
    extent_size: u64,
    extent_count: u64,
    block_size: u32,
    port_base: u32,
) -> Result<()> {
    let mut port = port_base;

    for _ in 0..3 {
        let _ = dsci
            .create_ds_region(
                port,
                extent_size,
                extent_count,
                block_size,
                false,
            )
            .await
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
async fn loop_create_test(
    dsci: &DscInfo,
    extent_size: u64,
    extent_count: u64,
    block_size: u32,
) -> Result<()> {
    let mut times = Vec::new();
    for _ in 0..5 {
        let ct = dsci
            .create_ds_region(3810, extent_size, extent_count, block_size, true)
            .await?;
        times.push(ct);
        dsci.delete_ds_region(3810)?;
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
async fn single_create_test(
    dsci: &DscInfo,
    extent_size: u64,
    extent_count: u64,
    block_size: u32,
    csv: &mut Option<&mut csv::Writer<File>>,
) -> Result<()> {
    let ct = dsci
        .create_ds_region(3810, extent_size, extent_count, block_size, true)
        .await?;

    let size = region_si(extent_size, extent_count, block_size);
    let extent_file_size = efile_si(extent_size, block_size);
    println!(
        "{:>9.3} {}  {} {:>6} {:>6} {:>4}",
        ct, size, extent_file_size, extent_size, extent_count, block_size,
    );
    dsci.delete_ds_region(3810)?;

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
async fn region_create_test(
    dsci: &DscInfo,
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
                loop_create_test(dsci, *es, ec, block_size).await?;
            } else {
                single_create_test(dsci, *es, ec, block_size, &mut csv_file)
                    .await?;
            }
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Cli::parse();
    // If any of our async tasks in our runtime panic, then we should
    // exit the program right away.
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    // If requested, remove any existing dsc files/regions
    if args.cleanup {
        if Path::new(&args.output_dir).exists() {
            println!("Removing existing dsc directory {:?}", args.output_dir);
            std::fs::remove_dir_all(&args.output_dir)?;
        }
        if Path::new(&args.region_dir).exists() {
            println!("Removing existing region {:?}", args.region_dir);
            std::fs::remove_dir_all(&args.region_dir)?;
        }
    }

    let runtime = Builder::new_multi_thread()
        .worker_threads(10)
        .thread_name("dsc")
        .enable_all()
        .build()
        .unwrap();

    // This channel is used to communicate between the dropshot server
    // and the dsc main thread.
    let (notify_tx, notify_rx) = watch::channel(0);

    match args.command {
        Commands::Create {
            block_size,
            extent_size,
            extent_count,
        } => {
            let dsci = DscInfo::new(
                args.ds_bin,
                args.output_dir,
                args.region_dir.clone(),
                notify_tx,
                true,
            )?;

            runtime.block_on(create_region_set(
                &dsci,
                extent_size,
                extent_count,
                block_size,
                8810,
            ))
        }
        Commands::RegionPerf { long, csv_out } => {
            let dsci = DscInfo::new(
                args.ds_bin,
                args.output_dir,
                args.region_dir.clone(),
                notify_tx,
                true,
            )?;
            runtime.block_on(region_create_test(&dsci, long, csv_out))
        }
        Commands::Start {
            create,
            block_size,
            extent_size,
            extent_count,
            control,
        } => {
            // Delete any existing region if requested
            if create && Path::new(&args.region_dir).exists() {
                println!("Removing existing region {:?}", args.region_dir);
                std::fs::remove_dir_all(&args.region_dir)?;
            }

            let dsci = DscInfo::new(
                args.ds_bin,
                args.output_dir,
                args.region_dir.clone(),
                notify_tx,
                create,
            )?;

            if create {
                runtime.block_on(create_region_set(
                    &dsci,
                    extent_size,
                    extent_count,
                    block_size,
                    8810,
                ))?;
            } else {
                dsci.generate_region_set(8810)?;
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
    use tempfile::tempdir;

    #[test]
    fn new_ti() {
        // Test a typical creation
        // The empty file ds_bin is good enough for our test here.
        let ds_bin = "./dsbin".to_string();
        File::create(ds_bin.clone()).unwrap();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let (tx, _) = watch::channel(0);
        let res = DscInfo::new(ds_bin, dir.clone(), dir.clone(), tx, true);
        assert!(res.is_ok());
        assert!(Path::new(&dir).exists());
    }

    #[test]
    fn bad_bin() {
        // Send a directory instead of a file for downstairs_bin, should
        // return error.
        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let (tx, _) = watch::channel(0);
        let res =
            DscInfo::new("/dev/null".to_string(), dir.clone(), dir, tx, true);

        assert!(res.is_err());
    }

    #[test]
    fn existing_ti() {
        // Try to create the same directories twice, which should
        // return error on the second try.
        let ds_bin = "./dsbin".to_string();
        File::create(ds_bin.clone()).unwrap();

        let output_dir = tempdir().unwrap().as_ref().to_path_buf();
        let region_dir = tempdir().unwrap().as_ref().to_path_buf();
        // First create the new directories.
        let (tx, _) = watch::channel(0);
        DscInfo::new(
            ds_bin.clone(),
            output_dir.clone(),
            region_dir.clone(),
            tx,
            true,
        )
        .unwrap();
        // Now, create them again and expect an error.
        let (tx, _) = watch::channel(0);
        let res = DscInfo::new(ds_bin, output_dir, region_dir, tx, true);
        assert!(res.is_err());
    }

    #[test]
    fn delete_bad_region() {
        // Test deletion of a region that does not exist, should report error.
        let ds_bin = "./dsbin".to_string();
        File::create(ds_bin.clone()).unwrap();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let (tx, _) = watch::channel(0);
        let dsci = DscInfo::new(ds_bin, dir.clone(), dir, tx, true).unwrap();

        let res = dsci.delete_ds_region(8810);
        assert!(res.is_err());
    }

    #[test]
    fn port_to_region_generation() {
        let nr = port_to_region("/var/tmp/rr".to_string(), 1234).unwrap();
        assert_eq!(nr, "/var/tmp/rr/1234".to_string());
    }

    #[test]
    fn delete_region() {
        // Test creation then deletion of a region
        let ds_bin = "./dsbin".to_string();
        File::create(ds_bin.clone()).unwrap();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let (tx, _) = watch::channel(0);
        let dsci =
            DscInfo::new(ds_bin, dir.clone(), dir.clone(), tx, true).unwrap();

        // Manually create the region directory.  We have to convert the
        // PathBuf back into a string.
        let ds_region_dir = port_to_region(
            dir.clone().into_os_string().into_string().unwrap(),
            8810,
        )
        .unwrap();
        fs::create_dir_all(&ds_region_dir).unwrap();

        let res = dsci.delete_ds_region(8810);
        assert!(res.is_ok());
        assert!(!Path::new(&ds_region_dir).exists());
    }

    #[test]
    fn restart_region() {
        // Test a typical creation
        // The empty file ds_bin is good enough for our test here.
        let ds_bin = "./dsbin".to_string();
        File::create(ds_bin.clone()).unwrap();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let (tx, _) = watch::channel(0);
        let dsci =
            DscInfo::new(ds_bin, dir.clone(), dir.clone(), tx, true).unwrap();
        assert!(Path::new(&dir).exists());

        // Manually create the region set.  We have to convert the
        // PathBuf back into a string.
        for port in &[8810, 8820, 8830] {
            let ds_region_dir = port_to_region(
                dir.clone().into_os_string().into_string().unwrap(),
                *port,
            )
            .unwrap();
            fs::create_dir_all(&ds_region_dir).unwrap();
        }

        // Verify that we can find the existing region directories.
        dsci.generate_region_set(8810).unwrap();
        let mut rs = dsci.rs.lock().unwrap();
        assert_eq!(rs.next_client_id(), 3);
    }

    #[test]
    fn restart_region_bad() {
        // Test region restart when a region directory is not present.
        // This should return error.
        let ds_bin = "./dsbin".to_string();
        File::create(ds_bin.clone()).unwrap();

        let dir = tempdir().unwrap().as_ref().to_path_buf();
        let (tx, _) = watch::channel(0);
        let dsci =
            DscInfo::new(ds_bin, dir.clone(), dir.clone(), tx, true).unwrap();
        assert!(Path::new(&dir).exists());

        // Manually create the 2/3 of the region set.
        // We have to convert the PathBuf back into a string.
        for port in &[8810, 8830] {
            let ds_region_dir = port_to_region(
                dir.clone().into_os_string().into_string().unwrap(),
                *port,
            )
            .unwrap();
            fs::create_dir_all(&ds_region_dir).unwrap();
        }

        // Verify that the missing region will return error.
        let res = dsci.generate_region_set(8810);
        assert!(res.is_err());
    }
}
