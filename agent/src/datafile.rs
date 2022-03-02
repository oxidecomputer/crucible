// Copyright 2021 Oxide Computer Company

use super::model::*;
use anyhow::{anyhow, bail, Result};
use crucible_common::write_json;
use serde::{Deserialize, Serialize};
use slog::{crit, error, info, Logger};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Condvar, Mutex, MutexGuard};

use chrono::{TimeZone, Utc};

use crate::ZFSDataset;

pub struct DataFile {
    log: Logger,
    base_path: PathBuf,
    conf_path: PathBuf,
    port_min: u16,
    port_max: u16,
    bell: Condvar,
    inner: Mutex<Inner>,
}

#[derive(Serialize, Deserialize, Default)]
struct Inner {
    regions: BTreeMap<RegionId, Region>,
    // indexed by region id and snapshot name
    running_snapshots: BTreeMap<RegionId, BTreeMap<String, RunningSnapshot>>,
}

impl DataFile {
    pub fn new(
        log: Logger,
        base_path: &Path,
        port_min: u16,
        port_max: u16,
    ) -> Result<DataFile> {
        let mut conf_path = base_path.to_path_buf();
        conf_path.push("crucible.json");

        /*
         * Open data file, load contents.
         */
        let inner = match crucible_common::read_json_maybe(&conf_path) {
            Ok(Some(inner)) => inner,
            Ok(None) => Inner::default(),
            Err(e) => {
                bail!("failed to load data file {:?}: {:?}", conf_path, e)
            }
        };

        Ok(DataFile {
            log,
            base_path: base_path.to_path_buf(),
            conf_path: conf_path.to_path_buf(),
            port_min,
            port_max,
            bell: Condvar::new(),
            inner: Mutex::new(inner),
        })
    }

    pub fn regions(&self) -> Vec<Region> {
        self.inner
            .lock()
            .unwrap()
            .regions
            .values()
            .cloned()
            .collect()
    }

    pub fn running_snapshots(
        &self,
    ) -> BTreeMap<RegionId, BTreeMap<String, RunningSnapshot>> {
        self.inner.lock().unwrap().running_snapshots.clone()
    }

    pub fn get(&self, id: &RegionId) -> Option<Region> {
        self.inner.lock().unwrap().regions.get(id).cloned()
    }

    /**
     * Store the database into the JSON file.
     */
    fn store(&self, inner: MutexGuard<Inner>) {
        loop {
            match write_json(&self.conf_path, &*inner, true) {
                Ok(()) => return,
                Err(e) => {
                    /*
                     * XXX What else could we do here?
                     */
                    crit!(
                        self.log,
                        "could not write data file {:?} (will retry): {:?}",
                        &self.conf_path,
                        e
                    );
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
    }

    fn get_free_port(&self, inner: &MutexGuard<Inner>) -> Result<u16> {
        for port_number in self.port_min..=self.port_max {
            let mut region_uses_port = false;
            let mut running_snapshot_uses_port = false;

            for region in inner.regions.values() {
                /*
                 * We can ignore any completely destroyed region. We choose
                 * not to ignore regions in the failed state
                 * for now, as they may still prevent use of
                 * their assigned port number.
                 */
                if region.state == State::Destroyed {
                    continue;
                }

                if port_number == region.port_number {
                    region_uses_port = true;
                    break;
                }
            }

            if region_uses_port {
                continue;
            }

            'outer: for running_snapshot_regions in
                inner.running_snapshots.values()
            {
                for running_snapshot in running_snapshot_regions.values() {
                    if running_snapshot.state == State::Destroyed {
                        continue;
                    }

                    if port_number == running_snapshot.port_number {
                        running_snapshot_uses_port = true;
                        break 'outer;
                    }
                }
            }

            if running_snapshot_uses_port {
                continue;
            }

            return Ok(port_number);
        }

        bail!("no free port numbers");
    }

    /**
     * Nexus will request that we create a new region by telling us the ID
     * it should have.  To make this idempotent, we will either create
     * the region or return the current state of the region if it was
     * already created in the past.
     *
     * The actual heavy lifting is performed in a worker thread.
     */
    pub fn create_region_request(
        &self,
        create: CreateRegion,
    ) -> Result<Region> {
        let mut inner = self.inner.lock().unwrap();

        /*
         * Look for a region with this ID.
         */
        if let Some(r) = inner.regions.get(&create.id) {
            if let Some(mis) = create.mismatch(r) {
                bail!(
                    "requested region {} already exists, with {}",
                    create.id.0,
                    mis
                );
            }

            /*
             * Return the region we already have, with its current status and
             * allocated port number.
             */
            return Ok(r.clone());
        }

        /*
         * Allocate a port number that is not yet in use.
         */
        let port_number = self.get_free_port(&inner)?;

        let r = Region {
            id: create.id.clone(),
            volume_id: create.volume_id,
            state: State::Requested,

            block_size: create.block_size,
            extent_size: create.extent_size,
            extent_count: create.extent_count,
            encrypted: create.encrypted,

            port_number,
            cert_pem: create.cert_pem,
            key_pem: create.key_pem,
            root_pem: create.root_pem,
        };

        info!(self.log, "region {} state: {:?}", r.id.0, r.state);
        let old = inner.regions.insert(create.id, r.clone());
        assert!(old.is_none());

        /*
         * Wake the worker thread to look at the region we've created.
         */
        self.bell.notify_all();

        self.store(inner);

        Ok(r)
    }

    pub fn create_running_snapshot_request(
        &self,
        request: CreateRunningSnapshotRequest,
    ) -> Result<RunningSnapshot> {
        let mut inner = self.inner.lock().unwrap();

        /*
         * Look for an existing running snapshot.
         */
        if inner
            .running_snapshots
            .entry(request.id.clone())
            .or_insert_with(BTreeMap::default)
            .contains_key(&request.name)
        {
            bail!("already running snapshot {} {}", request.id.0, request.name);
        }

        /*
         * Allocate a port number that is not yet in use.
         */
        let port_number = self.get_free_port(&inner)?;

        let s = RunningSnapshot {
            id: request.id.clone(),
            name: request.name.clone(),
            port_number,
            state: State::Created, // no work to do, just run smf
        };

        info!(
            self.log,
            "requesting running snapshot {}-{} state: {:?}",
            s.id.0,
            s.name,
            s.state,
        );

        inner
            .running_snapshots
            .get_mut(&request.id)
            .unwrap()
            .insert(request.name, s.clone());

        /*
         * Wake the worker thread to look at the snapshot we've created.
         */
        self.bell.notify_all();

        self.store(inner);

        Ok(s)
    }

    pub fn delete_running_snapshot_request(
        &self,
        request: DeleteRunningSnapshotRequest,
    ) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        /*
         * Look for an existing running snapshot.
         */
        if inner.running_snapshots.get(&request.id).is_none() {
            bail!("no running snapshots for region {}", request.id.0);
        }

        let running_snapshots =
            inner.running_snapshots.get_mut(&request.id).unwrap();

        if running_snapshots.get(&request.name).is_none() {
            bail!(
                "not running for region {} snapshot {}",
                request.id.0,
                request.name
            );
        }

        info!(
            self.log,
            "removing snapshot {}-{}", request.id.0, request.name
        );

        running_snapshots.remove(&request.name);

        if running_snapshots.is_empty() {
            inner.running_snapshots.remove(&request.id);
        }

        /*
         * Wake the worker thread to remove the snapshot we've created.
         */
        self.bell.notify_all();

        self.store(inner);

        Ok(())
    }

    pub fn delete_snapshot(
        &self,
        request: DeleteSnapshotRequest,
    ) -> Result<()> {
        let inner = self.inner.lock().unwrap();

        /*
         * Are we running this snapshot? Fail if so.
         */
        if let Some(running_snapshots) =
            inner.running_snapshots.get(&request.id)
        {
            if running_snapshots.get(&request.name).is_some() {
                bail!(
                    "downstairs running for region {} snapshot {}",
                    request.id.0,
                    request.name
                );
            }
        }

        // zfs delete snapshot
        let mut path = self.base_path.to_path_buf();
        path.push("regions");
        path.push(request.id.0.clone());

        let dataset =
            ZFSDataset::new(path.into_os_string().into_string().unwrap())?;

        let snapshot_name = format!("{}@{}", dataset.dataset(), request.name);

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

    /**
     * Mark a particular region as failed to provision.
     */
    pub fn fail(&self, id: &RegionId) {
        let mut inner = self.inner.lock().unwrap();

        let mut r = inner.regions.get_mut(id).unwrap();
        let nstate = State::Failed;
        if r.state == nstate {
            return;
        }

        info!(
            self.log,
            "region {} state: {:?} -> {:?}", r.id.0, r.state, nstate,
        );
        r.state = nstate;

        self.store(inner);
    }

    /**
     * Mark a particular region as provisioned.
     */
    pub fn created(&self, id: &RegionId) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        let mut r = inner.regions.get_mut(id).unwrap();
        let nstate = State::Created;
        match &r.state {
            State::Requested => (),
            State::Tombstoned => {
                /*
                 * Nexus requested that we destroy this region before we
                 * finished provisioning it.
                 */
                return Ok(());
            }
            x => bail!("created region in weird state {:?}", x),
        }

        info!(
            self.log,
            "region {} state: {:?} -> {:?}", r.id.0, r.state, nstate,
        );
        r.state = nstate;

        self.store(inner);
        Ok(())
    }

    /**
     * Mark a particular region as destroyed.
     */
    pub fn destroyed(&self, id: &RegionId) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        let mut r = inner.regions.get_mut(id).unwrap();
        let nstate = State::Destroyed;
        match &r.state {
            State::Tombstoned => (),
            x => bail!("region to destroy in weird state {:?}", x),
        }

        info!(
            self.log,
            "region {} state: {:?} -> {:?}", r.id.0, r.state, nstate,
        );
        r.state = nstate;

        // XXX shouldn't this remove the record?

        self.store(inner);
        Ok(())
    }

    /**
     * Nexus has requested that we destroy this particular region. Returns
     * true if the region is already destroyed, false if not destroyed
     * yet.
     */
    pub fn destroy(&self, id: &RegionId) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();

        let mut r = inner
            .regions
            .get_mut(id)
            .ok_or_else(|| anyhow!("region {} does not exist", id.0))?;

        Ok(match r.state {
            State::Tombstoned => {
                /*
                 * Destroy already scheduled.
                 */
                false
            }
            State::Requested | State::Created => {
                /*
                 * Schedule the destruction of this region.
                 */
                info!(
                    self.log,
                    "region {} state: {:?} -> {:?}",
                    r.id.0,
                    r.state,
                    State::Tombstoned
                );
                r.state = State::Tombstoned;
                self.bell.notify_all();
                self.store(inner);
                false
            }
            State::Destroyed => {
                /*
                 * Already destroyed; no more work to do.
                 */
                true
            }
            State::Failed => {
                /*
                 * For now, this terminal state will preserve evidence for
                 * investigation.
                 */
                bail!(
                    "region {} failed to provision and cannot be \
                    destroyed",
                    r.id.0
                );
            }
        })
    }

    /**
     * The worker thread will request the first region that is in a
     * particular state.
     */
    pub fn first_region_in_states(&self, states: &[State]) -> Option<Region> {
        let inner = self.inner.lock().unwrap();

        /*
         * States are provided in priority order.  We check for regions
         * in the first requested state before we check for
         * regions in the second provided state, etc.  This
         * allows us to focus on destroying tombstoned
         * regions ahead of creating new regions.
         */
        for s in states {
            for r in inner.regions.values() {
                if &r.state == s {
                    return Some(r.clone());
                }
            }
        }

        None
    }

    pub fn wait_on_bell(&self) {
        let inner = self.inner.lock().unwrap();
        let _guard = self.bell.wait(inner).unwrap();
    }

    /**
     * Get snapshots for a region
     */
    pub fn get_snapshots_for_region(
        &self,
        region_id: &RegionId,
    ) -> Result<Vec<Snapshot>> {
        let region = self.get(region_id);

        if region.is_none() {
            bail!("region {:?} does not exist", region_id);
        }

        let region = region.unwrap();

        if region.state != State::Created {
            bail!("region.state is {:?}", region.state);
        }

        let mut path = self.base_path.to_path_buf();
        path.push("regions");
        path.push(region_id.0.clone());
        path.push(".zfs");
        path.push("snapshot");

        info!(self.log, "checking if path {:?} exists", path);

        if !path.exists() {
            // No snapshots directory
            return Ok(vec![]);
        }

        info!(self.log, "checking path {:?}", path);

        let paths = std::fs::read_dir(&path);
        if paths.is_err() {
            bail!(paths.err().unwrap());
        }

        let mut results: Vec<Snapshot> = vec![];
        for path in paths.unwrap() {
            if path.is_err() {
                bail!(path.err().unwrap());
            }

            let path = path.unwrap().path();

            if path.is_dir() {
                let dir_name = path
                    .file_name()
                    .ok_or_else(|| {
                        anyhow!("could not turn {:?} into filename!", path)
                    })?
                    .to_str()
                    .ok_or_else(|| {
                        anyhow!("could not turn {:?} into str!", path)
                    })?
                    .to_string();

                // Creation time of a .zfs/snapshot/<folder> as retrieved by
                // stat doesn't make sense. Use `zfs get`:
                //
                //   # zfs get -pH -o value creation \
                //     data/crucible/regions/
                // 0d052762-10dd-4481-9a1a-1d36b9799f51@before_sync
                //   1644441276

                let dataset_name = {
                    let mut path = self.base_path.clone().to_path_buf();
                    path.push("regions");
                    path.push(region_id.0.clone());

                    let path = path.to_str().unwrap().to_string();

                    let mut chars = path.chars();
                    chars.next();
                    chars.as_str().to_string()
                };

                let snapshot_name = format!("{}@{}", dataset_name, dir_name);

                let mut cmd = Command::new("zfs");
                cmd.arg("get");
                cmd.arg("-pH");
                cmd.arg("-o");
                cmd.arg("value");
                cmd.arg("creation");
                cmd.arg(snapshot_name);

                info!(
                    self.log,
                    "command {:?} {:?}",
                    cmd.get_program(),
                    cmd.get_args()
                );

                let cmd = cmd.output()?;

                if !cmd.status.success() {
                    bail!("stat didn't work!");
                }

                let cmd_stdout = {
                    let cmd_stdout = String::from_utf8_lossy(&cmd.stdout);

                    // Remove newline
                    let cmd_stdout = cmd_stdout.trim_end().to_string();

                    cmd_stdout
                };

                info!(self.log, "stdout is {}", &cmd_stdout);

                results.push(Snapshot {
                    name: dir_name,
                    created: Utc.timestamp(cmd_stdout.parse()?, 0),
                });
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod test {
    use anyhow::{bail, Result};
    use chrono::{DateTime, TimeZone, Utc};
    use std::process::Command;

    #[test]
    fn test_stat_parsing() -> Result<()> {
        // Test round trip

        // $ date --utc -d @"1644356407" --rfc-3339=seconds
        // 2022-02-08 21:40:07+00:00
        let expected: DateTime<Utc> =
            DateTime::parse_from_rfc3339("2022-02-08T21:40:07+00:00")?.into();

        let fake_stdout = "1644356407".as_bytes().to_vec();

        assert_eq!(
            String::from_utf8_lossy(&fake_stdout),
            "1644356407".to_string(),
        );

        let actual =
            Utc.timestamp(String::from_utf8_lossy(&fake_stdout).parse()?, 0);

        assert_eq!(expected, actual);

        // Test parsing from Command output

        let cmd = Command::new("date").arg("+%s").output()?;

        if !cmd.status.success() {
            bail!("date didn't work!");
        }

        let cmd_stdout = {
            let cmd_stdout = String::from_utf8_lossy(&cmd.stdout);

            // Remove newline
            let cmd_stdout = cmd_stdout.trim_end().to_string();

            cmd_stdout
        };

        let _date = Utc.timestamp(cmd_stdout.parse()?, 0);

        Ok(())
    }
}
