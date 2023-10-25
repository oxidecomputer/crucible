use anyhow::bail;
use anyhow::Result;
use clap::Parser;
use crucible_common::build_logger;
use rand::random;
use slog::error;
use slog::info;
use slog::Logger;
use std::net::SocketAddr;
use std::process::Command;
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
    /// - create a 1GB region
    ///
    /// - randomly take a snapshot of that region
    ///
    /// - delete that snapshot if it was made
    ///
    /// - delete the region
    ///
    /// Additionally, one task is spawned that will:
    ///
    /// - get a list of regions
    ///
    /// - for each region, get a list of snapshots for that region
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
                info!(log, "using agent at argument {:?}", agent);
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
                info!(log, "using dataset at argument {:?}", dataset);
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

            let mut jhs: Vec<tokio::task::JoinHandle<_>> = (0..tasks).map(|i| {
                let log = log.new(slog::o!("task" => i));
                let dataset = dataset.clone();

                tokio::spawn(async move {
                    'outer: loop {
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
                        };

                        loop {
                            info!(log, "requesting region {:?}", region_id);
                            let client = get_client(&agent);
                            let r = client.region_create(&region_request).await;
                            drop(client);

                            let region = match r {
                                Ok(region) => {
                                    info!(log, "requested region {:?} ok", region_id);
                                    region
                                }

                                Err(e) => {
                                    error!(log, "requesting region {:?} failed with {:?}", region_id, e);
                                    break 'outer;
                                }
                            };

                            match region.state {
                                RegionState::Requested => {
                                    info!(log, "waiting for region {:?}: state {:?}", region_id, region.state);
                                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                }

                                RegionState::Created => {
                                    info!(log, "region {:?} state {:?}", region_id, region.state);
                                    break;
                                }

                                _ => {
                                    info!(log, "region {:?} unknown state {:?}", region_id, region.state);
                                    break 'outer;
                                }
                            }
                        }

                        // Optionally snapshot said region
                        if random() {
                            let snapshot_id = Uuid::new_v4();
                            let snapshot = format!("{}/regions/{:?}@{:?}", dataset, region_id, snapshot_id);
                            command(&log, "/usr/sbin/zfs", &["snapshot", &snapshot]).unwrap();

                            let client = get_client(&agent);
                            let r = client.region_get_snapshots(&RegionId(region_id.to_string())).await;
                            drop(client);
                            match r {
                                Ok(_) => {
                                    info!(log, "get region {:?} snapshots ok", region_id);
                                }

                                Err(e) => {
                                    info!(log, "get region {:?} snapshots failed with {:?}", region_id, e);
                                    break 'outer;
                                }
                            }

                            let client = get_client(&agent);
                            let r = client.region_get_snapshot(&RegionId(region_id.to_string()), &snapshot_id.to_string()).await;
                            drop(client);
                            match r {
                                Ok(_) => {
                                    info!(log, "get region {:?} snapshot {:?} ok", region_id, snapshot_id);
                                }

                                Err(e) => {
                                    info!(log, "get region {:?} snapshot {:?} failed with {:?}", region_id, snapshot_id, e);
                                    break 'outer;
                                }
                            }

                            let client = get_client(&agent);
                            let r = client.region_delete_snapshot(&RegionId(region_id.to_string()), &snapshot_id.to_string()).await;
                            drop(client);
                            match r {
                                Ok(_) => {
                                    info!(log, "delete region {:?} snapshot {:?} ok", region_id, snapshot_id);
                                }

                                Err(e) => {
                                    info!(log, "delete region {:?} snapshot {:?} failed with {:?}", region_id, snapshot_id, e);
                                    break 'outer;
                                }
                            }
                        }

                        // Delete region
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
                                    error!(log, "tombstoning region {:?} failed with {:?}", region_id, e);
                                    break 'outer;
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
                                    error!(log, "get region {:?} failed with {:?}", region_id, e);
                                    break 'outer;
                                }
                            };

                            match region.state {
                                RegionState::Tombstoned => {
                                    info!(log, "waiting for region {:?}: state {:?}", region_id, region.state);
                                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                }

                                RegionState::Destroyed => {
                                    info!(log, "region {:?} state {:?}", region_id, region.state);
                                    break;
                                }

                                _ => {
                                    info!(log, "region {:?} unknown state {:?}", region_id, region.state);
                                    break 'outer;
                                }
                            }
                        }
                    }
                })
            }).collect();

            // Add another task that grabs all regions, and queries all
            // snapshots for those regions
            jhs.push(tokio::spawn(async move {
                'outer: loop {
                    let client = get_client(&agent);
                    let r = client.region_list().await;
                    drop(client);

                    let regions = match r {
                        Ok(regions) => {
                            info!(log, "list regions ok");
                            regions
                        }

                        Err(e) => {
                            error!(log, "list regions failed with {:?}", e);
                            break 'outer;
                        }
                    };

                    for region in regions.iter() {
                        if region.state == RegionState::Created {
                            let client = get_client(&agent);
                            let r = client.region_get_snapshots(&region.id).await;
                            drop(client);

                            match r {
                                Ok(result) => {
                                    info!(log, "get region {:?} snapshots ok: {}", region.id, result.snapshots.len());
                                }

                                Err(e) => {
                                    error!(log, "get region {:?} snapshots failed with {:?}", region.id, e);

                                    // this is ok - the region could have
                                    // been deleted in the meantime
                                }
                            }
                        }
                    }
                }
            }));

            // Run until CTRL-C or all the tasks panic
            for jh in jhs {
                jh.await?;
            }
        }
    }
    Ok(())
}
