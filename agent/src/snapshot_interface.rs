// Copyright 2023 Oxide Computer Company

use anyhow::{bail, Result};
use crucible_agent_types::snapshot::Snapshot;
use slog::{error, info, Logger};
#[cfg(test)]
use std::collections::HashSet;
use std::process::Command;
#[cfg(test)]
use std::sync::Mutex;

use chrono::{TimeZone, Utc};

#[cfg(test)]
use std::path::PathBuf;

/// A interface for an implementation to interact with ZFS snapshots.
pub trait SnapshotInterface: Sync + Send {
    /// Get all snapshots for a dataset.
    fn get_snapshots_for_dataset(
        &self,
        dataset: String,
    ) -> Result<Vec<Snapshot>>;

    /// Delete a snapshot if it exists. This call should be idempotent.
    fn delete_snapshot(&self, snapshot_name: String) -> Result<()>;

    /// Create a snapshot. Only used for testing.
    #[cfg(test)]
    fn create_snapshot(
        &self,
        base_path: PathBuf,
        region_id: String,
        snapshot_name: String,
    );
}

pub struct ZfsSnapshotInterface {
    log: Logger,
}

impl ZfsSnapshotInterface {
    pub fn new(log: Logger) -> ZfsSnapshotInterface {
        ZfsSnapshotInterface { log }
    }
}

impl SnapshotInterface for ZfsSnapshotInterface {
    fn get_snapshots_for_dataset(
        &self,
        dataset: String,
    ) -> Result<Vec<Snapshot>> {
        let cmd = Command::new("zfs")
            .arg("list")
            .arg("-t")
            .arg("snapshot")
            .arg("-H")
            .arg("-o")
            .arg("name")
            .arg("-r")
            .arg(&dataset)
            .output()?;

        let snapshots_stdout = String::from_utf8_lossy(&cmd.stdout);
        if !cmd.status.success() {
            let snapshots_stderr = String::from_utf8_lossy(&cmd.stderr);

            error!(
                self.log,
                "zfs list snapshot for dataset {:?} list failed: out {:?} err {:?}",
                dataset,
                snapshots_stdout,
                snapshots_stderr,
            );

            bail!("zfs list snapshots failed!");
        }

        let mut results = Vec::new();

        let snapshots_list: Vec<&str> = snapshots_stdout
            .trim_end()
            .split('\n')
            .filter(|x| !x.is_empty())
            .collect();

        for snapshot in snapshots_list {
            if snapshot == dataset {
                info!(self.log, "skipping {} matches dataset name", snapshot);
                continue;
            }
            info!(self.log, "snapshot is {}", &snapshot);
            let parts: Vec<&str> = snapshot.split('@').collect();
            info!(self.log, "parts is {:?}", &parts);
            if parts.len() != 2 {
                // If some non-snapshot-name output snuck in here, then don't
                // panic! Continue to the next one in the list.
                error!(self.log, "bad snapshot name {}!", &snapshot);
                continue;
            }
            let snapshot_name = parts[1];

            let cmd = Command::new("zfs")
                .arg("get")
                .arg("-pH")
                .arg("-o")
                .arg("value")
                .arg("creation")
                .arg(snapshot)
                .output()?;

            let cmd_stdout = {
                let cmd_stdout = String::from_utf8_lossy(&cmd.stdout);

                // Remove newline
                let cmd_stdout = cmd_stdout.trim_end().to_string();

                cmd_stdout
            };

            if !cmd.status.success() {
                let err = String::from_utf8_lossy(&cmd.stderr);

                error!(
                    self.log,
                    "zfs get for snapshot {} failed: out {:?} err {:?}",
                    snapshot,
                    cmd_stdout,
                    err,
                );

                bail!("zfs get failed!");
            }

            results.push(Snapshot {
                name: snapshot_name.to_string(),
                created: Utc.timestamp_opt(cmd_stdout.parse()?, 0).unwrap(),
            });
        }

        Ok(results)
    }

    fn delete_snapshot(&self, snapshot_name: String) -> Result<()> {
        // If the snapshot doesn't exist, return Ok - this call should be
        // idempotent
        let cmd = Command::new("zfs")
            .arg("list")
            .arg(snapshot_name.clone())
            .output()?;

        if !cmd.status.success() {
            let out = String::from_utf8_lossy(&cmd.stdout);
            let err = String::from_utf8_lossy(&cmd.stderr);

            if err.trim_end().ends_with("dataset does not exist") {
                return Ok(());
            }

            error!(
                self.log,
                "zfs snapshot {:?} list failed: out {:?} err {:?}",
                snapshot_name,
                out,
                err,
            );

            bail!("zfs snapshot list failure");
        }

        // Delete it if it exists
        let cmd = Command::new("zfs")
            .arg("destroy")
            .arg(snapshot_name.clone())
            .output()?;

        if !cmd.status.success() {
            let err = String::from_utf8_lossy(&cmd.stderr);
            let out = String::from_utf8_lossy(&cmd.stdout);

            error!(
                self.log,
                "zfs snapshot {:?} delete failed: out {:?} err {:?}",
                snapshot_name,
                out,
                err,
            );

            bail!("zfs snapshot delete failure");
        }

        Ok(())
    }

    #[cfg(test)]
    fn create_snapshot(
        &self,
        _base_path: PathBuf,
        _region_id: String,
        _snapshot_name: String,
    ) {
        unimplemented!();
    }
}

#[cfg(test)]
pub struct TestSnapshotInterface {
    log: Logger,
    snapshots: Mutex<HashSet<String>>,
}

#[cfg(test)]
impl TestSnapshotInterface {
    pub fn new(log: Logger) -> TestSnapshotInterface {
        TestSnapshotInterface {
            log,
            snapshots: Mutex::new(HashSet::new()),
        }
    }
}

#[cfg(test)]
impl SnapshotInterface for TestSnapshotInterface {
    fn get_snapshots_for_dataset(
        &self,
        dataset: String,
    ) -> Result<Vec<Snapshot>> {
        let snapshots = self.snapshots.lock().unwrap();

        let snapshots = snapshots
            .iter()
            .filter(|x| x.starts_with(&format!("{}@", dataset)))
            .map(|x| Snapshot {
                name: x.clone(),
                created: Utc::now(),
            })
            .collect();

        info!(
            self.log,
            "dataset {} has snapshots {:?}", dataset, snapshots
        );

        Ok(snapshots)
    }

    fn delete_snapshot(&self, snapshot_name: String) -> Result<()> {
        let mut snapshots = self.snapshots.lock().unwrap();
        info!(self.log, "deleting snapshot {}", snapshot_name);
        snapshots.remove(&snapshot_name);
        Ok(())
    }

    #[cfg(test)]
    fn create_snapshot(
        &self,
        base_path: PathBuf,
        region_id: String,
        snapshot_name: String,
    ) {
        let mut snapshots = self.snapshots.lock().unwrap();

        info!(
            self.log,
            "creating region {} snapshot {}", region_id, snapshot_name
        );
        snapshots.insert(format!("{}@{}", region_id, snapshot_name));

        // Create a directory so that the "wait until snapshot is mounted" code
        // finds what it's looking for.
        let mut snapshot_path = base_path;
        snapshot_path.push("regions");
        snapshot_path.push(region_id.clone());
        snapshot_path.push(".zfs");
        snapshot_path.push("snapshot");
        snapshot_path.push(snapshot_name.clone());

        info!(
            self.log,
            "creating region {} snapshot {} dir {:?}",
            region_id,
            snapshot_name,
            snapshot_path
        );
        std::fs::create_dir_all(&snapshot_path).unwrap();
    }
}
