use anyhow::bail;
use anyhow::Result;
use clap::Parser;
use crucible_common::build_logger;
use futures::StreamExt;
use rand::random;
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use slog::Logger;
use slog::{info, warn};
use std::net::SocketAddr;
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use uuid::Uuid;

use crucible_agent_client::{
    types::{CreateRegion, RegionId, State as RegionState},
    Client as CrucibleAgentClient,
};

#[derive(Debug, Parser)]
#[clap(
    name = "agent-antagonist",
    about = "Stress tester for the crucible agent"
)]
enum Args {
    /// Run a number of antagonist loop tasks that will do the following:
    ///
    /// - Create a 1GB region
    ///
    /// - Randomly:
    ///   Take a snapshot of that region.
    ///   Start the snapshot.
    ///   Create a region using the snapshot as a clone source.
    ///   Delete the region.
    ///   Delete that snapshot if it was made
    ///
    /// - Delete the region
    ///
    /// Additionally, one task is spawned that will:
    ///
    /// - Get a list of regions
    ///
    /// - For each region, get a list of snapshots for that region
    Run {
        /// Address of the crucible agent - leave blank to autodetect if in the
        /// crucible zone
        #[clap(short, long)]
        agent: Option<SocketAddr>,

        /// Dataset for the crucible agent - leave blank to autodetect if in the
        /// crucible zone
        #[clap(short, long)]
        dataset: Option<String>,

        /// How many tasks to spawn performing antagonist loop
        #[clap(short, long)]
        tasks: usize,
    },
}

fn command(log: &Logger, bin: &'static str, args: &[&str]) -> Result<String> {
    let start = std::time::Instant::now();
    let cmd = Command::new(bin).args(args).output()?;
    let elapsed = start.elapsed();
    info!(log, "{} {:?} took {:?}", bin, args, elapsed);

    if !cmd.status.success() {
        bail!("zfs list failed!");
    }

    Ok(String::from_utf8(cmd.stdout)?.trim_end().to_string())
}

fn get_client(agent: &SocketAddr) -> CrucibleAgentClient {
    let dur = std::time::Duration::from_secs(60);

    let client = reqwest::ClientBuilder::new()
        .connect_timeout(dur)
        .build()
        .unwrap();

    CrucibleAgentClient::new_with_client(&format!("http://{}", agent), client)
}

async fn main_thread(
    log: Logger,
    agent: SocketAddr,
    dataset: String,
    stop_flag: Arc<AtomicBool>,
) -> Result<()> {
    let mut count = 0;
    loop {
        if stop_flag.load(Ordering::SeqCst) {
            break;
        }
        count += 1;
        // Create a 1 GB region
        let region_id = Uuid::new_v4();

        let region_request = CreateRegion {
            block_size: 512,
            extent_count: 16,
            extent_size: 131072,
            id: RegionId(region_id.to_string()),
            encrypted: true,
            cert_pem: None,
            key_pem: None,
            root_pem: None,
            source: None,
        };

        info!(
            log,
            "--- New loop begins with {region_id} at count: {count}---"
        );
        if let Err(e) =
            create_a_region(agent, &log, region_request.clone()).await
        {
            bail!("Region create {region_id} failed: {e}");
        }

        // Optionally:
        // - snapshot said region and run the snapshot
        // - Create a region by clone from the snapshot.
        // - Delete the clone region
        // - Stop and delete the snapshot
        if random() {
            let snapshot_id = Uuid::new_v4();
            info!(log, "Create snapshot {snapshot_id}");

            if let Err(e) =
                create_a_snapshot(agent, &log, &dataset, region_id, snapshot_id)
                    .await
            {
                bail!("Snapshot create returned {e}");
            }

            let clone_region_id = Uuid::new_v4();
            if let Err(e) = clone_a_snapshot(
                agent,
                &log,
                region_id,
                region_request,
                snapshot_id,
                clone_region_id,
            )
            .await
            {
                bail!("Snapshot clone returned {e}");
            }

            if let Err(e) = delete_a_region(agent, &log, clone_region_id).await
            {
                bail!("Region clone delete {clone_region_id} failed: {e}");
            }

            if let Err(e) =
                delete_a_snapshot(agent, &log, region_id, snapshot_id).await
            {
                bail!("Snapshot delete returned {e}");
            }
        }

        // Delete region
        if let Err(e) = delete_a_region(agent, &log, region_id).await {
            bail!("Region delete {region_id} failed: {e}");
        }
        info!(log, "--- Completed {:5} loops with {region_id} ---", count);
    }
    Ok(())
}

// Create a region.
// Loop till it is ready.
async fn create_a_region(
    agent: SocketAddr,
    log: &Logger,
    region_request: CreateRegion,
) -> Result<()> {
    loop {
        info!(log, "creating region {:?}", region_request.id);
        let client = get_client(&agent);
        let region = match client.region_create(&region_request).await {
            Ok(region) => {
                info!(log, "creating region {:?} ok", region_request.id,);
                region
            }

            Err(e) => {
                bail!(
                    "creating region {:?} failed: {:?}",
                    region_request.id,
                    e
                );
            }
        };

        match region.state {
            RegionState::Requested => {
                info!(
                    log,
                    "waiting for region {:?}: state {:?}",
                    region_request.id,
                    region.state,
                );
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }

            RegionState::Created => {
                info!(
                    log,
                    "region {:?} state {:?}", region_request.id, region.state,
                );
                break;
            }

            _ => {
                bail!(
                    "region {:?} unknown state {:?}",
                    region_request.id,
                    region.state,
                );
            }
        }
    }
    Ok(())
}

async fn create_a_snapshot(
    agent: SocketAddr,
    log: &Logger,
    dataset: &String,
    region_id: Uuid,
    snapshot_id: Uuid,
) -> Result<()> {
    // Create a snapshot of our region
    let snapshot =
        format!("{}/regions/{:?}@{:?}", dataset, region_id, snapshot_id,);
    command(log, "/usr/sbin/zfs", &["snapshot", &snapshot]).unwrap();

    info!(log, "Get all snapshots for {region_id}");
    let client = get_client(&agent);
    let r = client
        .region_get_snapshots(&RegionId(region_id.to_string()))
        .await;
    drop(client);
    match r {
        Ok(snaps) => {
            info!(log, "get region {:?} snapshots ok", region_id,);
            info!(log, "region {:?} snapshots {:?}", region_id, snaps);
        }

        Err(e) => {
            bail!("get region {:?} snapshots failed with {:?}", region_id, e,);
        }
    }

    info!(log, "Get region snapshot {snapshot_id} we just created");
    let client = get_client(&agent);
    let r = client
        .region_get_snapshot(
            &RegionId(region_id.to_string()),
            &snapshot_id.to_string(),
        )
        .await;
    drop(client);
    match r {
        Ok(rs) => {
            info!(
                log,
                "get region {:?} snapshot {:?} ok {:?}",
                region_id,
                snapshot_id,
                rs,
            );
        }

        Err(e) => {
            bail!(
                "get region {:?} snapshot {:?} failed with {:?}",
                region_id,
                snapshot_id,
                e,
            );
        }
    }

    info!(log, "Now run snapshot {snapshot_id}");
    let client = get_client(&agent);
    let r = client
        .region_run_snapshot(
            &RegionId(region_id.to_string()),
            &snapshot_id.to_string(),
        )
        .await;
    drop(client);
    match r {
        Ok(rs) => {
            info!(
                log,
                "run snapshot region {:?} snapshot {:?} returns: {:?}",
                region_id,
                snapshot_id,
                rs,
            );
        }

        Err(e) => {
            bail!(
                "run snapshot region {:?} snapshot {:?} failed with {:?}",
                region_id,
                snapshot_id,
                e,
            );
        }
    }

    info!(log, "After run, Get all snapshots for {region_id}");
    let client = get_client(&agent);
    let r = client
        .region_get_snapshots(&RegionId(region_id.to_string()))
        .await;
    drop(client);
    match r {
        Ok(snaps) => {
            info!(log, "get region {:?} snapshots ok", region_id,);
            info!(log, "region {:?} snapshots {:?}", region_id, snaps);
        }

        Err(e) => {
            bail!("get region {:?} snapshots failed with {:?}", region_id, e,);
        }
    }
    Ok(())
}

async fn delete_a_snapshot(
    agent: SocketAddr,
    log: &Logger,
    region_id: Uuid,
    snapshot_id: Uuid,
) -> Result<()> {
    info!(log, "Now delete running snapshot {snapshot_id}");
    let client = get_client(&agent);
    let r = client
        .region_delete_running_snapshot(
            &RegionId(region_id.to_string()),
            &snapshot_id.to_string(),
        )
        .await;
    drop(client);
    match r {
        Ok(_) => {
            info!(
                log,
                "delete region {:?} snapshot {:?} ok", region_id, snapshot_id,
            );
        }

        Err(e) => {
            bail!(
                "delete region {:?} snapshot {:?} failed with {:?}",
                region_id,
                snapshot_id,
                e,
            );
        }
    }

    // Loop till the snapshot is deleted
    info!(log, "After delete, Get all snapshots for {region_id}");
    loop {
        let client = get_client(&agent);
        let r = client
            .region_get_snapshots(&RegionId(region_id.to_string()))
            .await;
        drop(client);

        // We just created this region and snapshot, so there should
        // only be one.
        match r {
            Ok(snaps) => {
                info!(log, "region {:?} has snapshots {:?}", region_id, snaps);
                let my_snap =
                    match snaps.running_snapshots.get(&snapshot_id.to_string())
                    {
                        Some(my_snap) => my_snap,
                        None => {
                            bail!("Can't find snapshot {:?}", snapshot_id);
                        }
                    };
                match my_snap.state {
                    RegionState::Tombstoned => {
                        info!(log, "Snapshot Tombstoned, getting closer");
                    }
                    RegionState::Requested => {
                        info!(log, "Snapshot Requested, keep going");
                    }
                    RegionState::Created => {
                        info!(log, "Snapshot Created, Get on with the delete");
                    }
                    RegionState::Destroyed => {
                        info!(log, "Destroyed!! This is what we want");
                        break;
                    }
                    RegionState::Failed => {
                        bail!("Deleted snapshot went to Failed");
                    }
                }
            }

            Err(e) => {
                bail!(
                    "get region {:?} snapshots failed with {:?}",
                    region_id,
                    e,
                );
            }
        }
        // Not destroyed, loop and try again
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    info!(log, "Now delete just snapshot {snapshot_id}");
    let client = get_client(&agent);
    let r = client
        .region_delete_snapshot(
            &RegionId(region_id.to_string()),
            &snapshot_id.to_string(),
        )
        .await;
    drop(client);
    match r {
        Ok(_) => {
            info!(
                log,
                "delete region {:?} snapshot {:?} ok", region_id, snapshot_id,
            );
        }

        Err(e) => {
            bail!(
                "delete region {:?} snapshot {:?} failed with {:?}",
                region_id,
                snapshot_id,
                e,
            );
        }
    }

    info!(
        log,
        "Final Get region snapshot {snapshot_id} we just deleted"
    );
    let client = get_client(&agent);
    let r = client
        .region_get_snapshot(
            &RegionId(region_id.to_string()),
            &snapshot_id.to_string(),
        )
        .await;
    drop(client);

    // We don't expect to find this snapshot.
    match r {
        Ok(_) => {
            bail!(
                "Found region {:?} still has deleted snapshot {:?}",
                region_id,
                snapshot_id,
            );
        }
        Err(_) => {
            info!(
                log,
                "region {:?} no longer has deleted snapshot {:?}",
                region_id,
                snapshot_id,
            );
        }
    }
    Ok(())
}

// Create a clone from this snapshot.
async fn clone_a_snapshot(
    agent: SocketAddr,
    log: &Logger,
    region_id: Uuid,
    mut region_request: CreateRegion,
    snapshot_id: Uuid,
    clone_region_id: Uuid,
) -> Result<()> {
    info!(log, "CLONE for region:{region_id} snapshot:{snapshot_id}");

    // We can't clone from a snapshot till that snapshot has been created.
    // The agents work loop will create all requested regions first before it
    // will create any snapshots.  We need to be sure that the agent work loop
    // has completed the creation source for our clone and that snapshot is
    // running and will respond to requests before we ask the agent to do the
    // region create who will clone from that snapshot.
    let mut source_addr = agent;
    'created: loop {
        let client = get_client(&agent);
        match client
            .region_get_snapshots(&RegionId(region_id.to_string()))
            .await
        {
            Ok(snaps) => {
                info!(log, "region {:?} has snapshots {:?}", region_id, snaps);
                let my_snap =
                    match snaps.running_snapshots.get(&snapshot_id.to_string())
                    {
                        Some(my_snap) => my_snap,
                        None => {
                            bail!("Can't find snapshot {:?}", snapshot_id);
                        }
                    };

                match my_snap.state {
                    RegionState::Created => {
                        info!(
                            log,
                            "Snapshot {snapshot_id} is ready, proceed with clone"
                        );

                        // This +4000 is the repair port, which is determined
                        // by the downstairs itself.
                        source_addr.set_port(my_snap.port_number + 4000);
                        break 'created;
                    }
                    x => {
                        info!(
                            log,
                            "Waiting for snapshot {} to be ready: {:?}",
                            snapshot_id,
                            x,
                        );
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            Err(e) => {
                bail!(
                    "get region {:?} snapshots for clone failed with {:?}",
                    region_id,
                    e,
                );
            }
        }
    }

    // Our snapshot is ready, and we have populated the source_addr with
    // the IP (our agent IP) and the repair port ID of the snapshot.
    region_request.source = Some(source_addr.to_string());
    region_request.id = RegionId(clone_region_id.to_string());

    info!(
        log,
        "Use {:?} for source clone {:?}", source_addr, region_request
    );

    if let Err(e) = create_a_region(agent, log, region_request.clone()).await {
        bail!("Region clone create failed, returned {e}");
    }
    info!(
        log,
        "Region {clone_region_id} cloned from snapshot {snapshot_id}"
    );
    Ok(())
}

async fn delete_a_region(
    agent: SocketAddr,
    log: &Logger,
    region_id: Uuid,
) -> Result<()> {
    loop {
        info!(log, "tombstoning region {:?}", region_id);
        let client = get_client(&agent);
        let r = client.region_delete(&RegionId(region_id.to_string())).await;
        drop(client);
        match r {
            Ok(_) => {
                info!(log, "tombstoning region {:?} ok", region_id);
            }

            Err(e) => {
                bail!("tombstoning region {:?} failed with {:?}", region_id, e);
            }
        }

        let client = get_client(&agent);
        let r = client.region_get(&RegionId(region_id.to_string())).await;
        drop(client);
        let region = match r {
            Ok(region) => {
                info!(log, "get region {:?} ok", region_id);
                region
            }

            Err(e) => {
                bail!("get region {:?} failed with {:?}", region_id, e);
            }
        };

        match region.state {
            RegionState::Tombstoned => {
                info!(
                    log,
                    "waiting for region {:?}: state {:?}",
                    region_id,
                    region.state,
                );
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }

            RegionState::Destroyed => {
                info!(log, "region {:?} state {:?}", region_id, region.state,);
                break;
            }

            _ => {
                bail!(
                    "region {:?} unknown state {:?}",
                    region_id,
                    region.state,
                );
            }
        }
    }
    Ok(())
}

async fn query_thread(
    log: Logger,
    agent: SocketAddr,
    stop_flag: Arc<AtomicBool>,
) -> Result<()> {
    let mut count = 0;
    loop {
        if stop_flag.load(Ordering::SeqCst) {
            break;
        }
        count += 1;
        let client = get_client(&agent);
        let r = client.region_list().await;
        drop(client);

        let regions = match r {
            Ok(regions) => {
                info!(log, "list regions ok");
                regions
            }

            Err(e) => {
                bail!("list regions failed with {:?}", e);
            }
        };

        for region in regions.iter() {
            if matches!(region.state, RegionState::Created) {
                let client = get_client(&agent);
                let r = client.region_get_snapshots(&region.id).await;
                drop(client);

                match r {
                    Ok(result) => {
                        info!(
                            log,
                            "get region {:?} snapshots ok: {}",
                            region.id,
                            result.snapshots.len(),
                        );
                    }

                    Err(e) => {
                        warn!(
                            log,
                            "get region {:?} snapshots failed with {:?}",
                            region.id,
                            e,
                        );

                        // this is ok - the region could have
                        // been deleted in the meantime
                    }
                }
            }
        }
        info!(log, "--- Completed {:5} query loops ---", count);
    }
    Ok(())
}

// Listen for our signal and set the quit flag when received.
async fn signal_thread(log: Logger, stop_flag: Arc<AtomicBool>) {
    let mut signals = Signals::new([SIGUSR1]).unwrap();
    if let Some(signal) = signals.next().await {
        match signal {
            SIGUSR1 => {
                warn!(log, "Stop request received");
                stop_flag.store(true, Ordering::SeqCst);
            }
            x => {
                panic!("Received unsupported signal {}", x);
            }
        }
    } else {
        panic!("Failed to wait in the signal handler");
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::try_parse()?;
    match args {
        Args::Run {
            agent,
            dataset,
            tasks,
        } => {
            let log = build_logger();

            let agent = if let Some(agent) = agent {
                info!(log, "using agent {:?}", agent);
                agent
            } else {
                info!(log, "finding listen_addr from smf");
                let listen_addr = command(
                    &log,
                    "/usr/sbin/svccfg",
                    &["-s", "agent:default", "listprop", "config/listen_addr"],
                )?;
                let listen_addr =
                    listen_addr.split(' ').last().unwrap().to_string();
                info!(log, "using listen_addr {:?}", listen_addr);

                info!(log, "finding listen_port from smf");
                let listen_port = command(
                    &log,
                    "/usr/sbin/svccfg",
                    &["-s", "agent:default", "listprop", "config/listen_port"],
                )?;
                let listen_port =
                    listen_port.split(' ').last().unwrap().to_string();
                info!(log, "using listen_port {:?}", listen_port);

                format!("[{}]:{}", listen_addr, listen_port).parse()?
            };

            let dataset = if let Some(dataset) = dataset {
                info!(log, "using dataset {:?}", dataset);
                dataset
            } else {
                info!(log, "finding dataset from smf");
                let dataset = command(
                    &log,
                    "/usr/sbin/svccfg",
                    &["-s", "agent:default", "listprop", "config/dataset"],
                )?;
                let dataset = dataset.split(' ').last().unwrap().to_string();
                info!(log, "using dataset {:?}", dataset);
                dataset
            };

            info!(
                log,
                "configured for agent {:?} dataset {:?} tasks {:?}",
                agent,
                dataset,
                tasks
            );

            // Setup our stop flag that all tasks will check to see if it
            // is time to stop or not.
            let stop_flag = Arc::new(AtomicBool::new(false));
            let mut jhs: Vec<tokio::task::JoinHandle<Result<()>>> = (0..tasks)
                .map(|i| {
                    let log = log.new(slog::o!("task" => i));
                    let dataset = dataset.clone();
                    let stop_flag_clone = stop_flag.clone();

                    tokio::spawn(async move {
                        main_thread(log, agent, dataset, stop_flag_clone).await
                    })
                })
                .collect();

            // Add another task that grabs all regions, and queries all
            // snapshots for those regions
            let stop_flag_clone = stop_flag.clone();
            let qlog = log.new(slog::o!("query" => 0));
            jhs.push(tokio::spawn(async move {
                query_thread(qlog, agent, stop_flag_clone).await
            }));

            // Spawn a task to listen to signals and order everyone to quit
            // if a signal is received.
            let stop_flag_clone = stop_flag.clone();
            let signal_log = log.new(slog::o!("signal" => 0));
            tokio::spawn(async move {
                signal_thread(signal_log, stop_flag_clone).await
            });

            let mut result_summary = Vec::with_capacity(tasks + 1);

            // Run until SIGUSR1, CTRL-C, or a tasks exits
            // Watch for any of our spawned tasks to exit and report which
            // task it is.  Once we have one task that has exited, we set
            // the flag for everyone else to exit (if it's not already set).
            let (exited_task, index, remaining_futures) =
                futures::future::select_all(jhs).await;
            warn!(log, "Task {index} exited first with {:?}", exited_task);
            result_summary.push(exited_task.unwrap());
            stop_flag.store(true, Ordering::SeqCst);

            for jh in remaining_futures {
                let res = jh.await.unwrap();
                info!(log, "A task has ended with {:?}", res);
                result_summary.push(res);
            }

            // The reason a task ended might be lost in the logging noise, so
            // print out what every task ended with
            info!(log, "All tasks have ended");
            info!(log, "Summary: {:?}", result_summary);
        }
    }
    Ok(())
}
