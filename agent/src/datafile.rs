// Copyright 2021 Oxide Computer Company

use super::model::*;
use anyhow::{anyhow, bail, Result};
use crucible_common::write_json;
use serde::{Deserialize, Serialize};
use slog::{crit, error, info, Logger};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::snapshot_interface::SnapshotInterface;
use crate::ZFSDataset;

pub struct DataFile {
    log: Logger,
    base_path: PathBuf,
    conf_path: PathBuf,
    listen: SocketAddr,
    port_min: u16,
    port_max: u16,
    bell: Condvar,
    inner: Mutex<Inner>,
    snapshot_interface: Arc<dyn SnapshotInterface>,
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
        listen: SocketAddr,
        port_min: u16,
        port_max: u16,
        snapshot_interface: Arc<dyn SnapshotInterface>,
    ) -> Result<DataFile> {
        let mut conf_path = base_path.to_path_buf();
        conf_path.push("crucible.json");

        info!(log, "Using conf_path:{:?}", conf_path);
        /*
         * Open data file, load contents.
         */
        let inner = match crucible_common::read_json_maybe(&conf_path) {
            Ok(Some(inner)) => inner,
            Ok(None) => Inner::default(),
            Err(e) => {
                bail!("failed to load data file {:?}: {:?}", conf_path, e);
            }
        };

        Ok(DataFile {
            log,
            base_path: base_path.to_path_buf(),
            conf_path: conf_path.to_path_buf(),
            listen,
            port_min,
            port_max,
            bell: Condvar::new(),
            inner: Mutex::new(inner),
            snapshot_interface,
        })
    }

    pub fn get_listen_addr(&self) -> SocketAddr {
        self.listen
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

        let read_only = create.source.is_some();

        let r = Region {
            id: create.id.clone(),
            state: State::Requested,

            block_size: create.block_size,
            extent_size: create.extent_size,
            extent_count: create.extent_count,
            encrypted: create.encrypted,

            port_number,
            cert_pem: create.cert_pem,
            key_pem: create.key_pem,
            root_pem: create.root_pem,
            source: create.source,
            read_only,
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
        if let Some(r) = inner
            .running_snapshots
            .entry(request.id.clone())
            .or_default()
            .get(&request.name)
        {
            return Ok(r.clone());
        }

        /*
         * Wait for ZFS snapshot directory to get mounted before
         * starting a read-only downstairs that points to it. Note
         * `create_running_snapshot_request` is only entered if the
         * snapshot exists for the region so this should eventually
         * be a directory.
         */
        {
            let mut snapshot_path = self.base_path.to_path_buf();
            snapshot_path.push("regions");
            snapshot_path.push(request.id.0.clone());
            snapshot_path.push(".zfs");
            snapshot_path.push("snapshot");
            snapshot_path.push(request.name.clone());

            // Wait a maximum of 5 seconds for the
            // <region>/.zfs/snapshot/<snapshot> directory to appear
            let mut appeared = false;
            for _ in 0..50 {
                if snapshot_path.is_dir() {
                    appeared = true;
                    break;
                }
                info!(self.log, "waiting for path {:?}", snapshot_path);
                std::thread::sleep(std::time::Duration::from_millis(100));
            }

            if !appeared {
                error!(self.log, "{:?} did not appear!", snapshot_path);
                bail!("{:?} did not appear!", snapshot_path);
            }
        }

        /*
         * Allocate a port number that is not yet in use.
         */
        let port_number = self.get_free_port(&inner)?;

        let s = RunningSnapshot {
            id: request.id.clone(),
            name: request.name.clone(),
            port_number,
            state: State::Requested,
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

        match running_snapshots.get_mut(&request.name) {
            None => {
                // A read-only downstairs never existed for this snapshot,
                // return OK.
                info!(
                    self.log,
                    "running snapshot never existed for region {} snapshot {}, returning Ok",
                    request.id.0,
                    request.name
                );

                return Ok(());
            }

            Some(existing) => {
                // It's important *not* to check if the underlying snapshot
                // exists here: it could have been deleted already, and yet the
                // request to delete the running snapshot may have been resent.
                //
                // What's important is that:
                //
                // 1) the running snapshot could not have been created without
                //    an underlying snapshot.
                //
                // 2) the snapshot could not have been deleted unless the
                //    running snapshot was first Destroyed.
                //
                // Because we're in the Some branch, this running snapshot
                // existed at some point. If the underlying snapshot was
                // deleted, then the running snapshot state must be in
                // Destroyed. If the snapshot was not deleted, then the running
                // snapshot state could be anything.

                match existing.state {
                    State::Tombstoned | State::Destroyed => {
                        /*
                         * Either:
                         * - Destroy already scheduled.
                         * - Already destroyed; no more work to do.
                         */
                    }

                    State::Requested | State::Created => {
                        info!(
                            self.log,
                            "removing running snapshot {}-{}",
                            request.id.0,
                            request.name
                        );

                        existing.state = State::Tombstoned;

                        /*
                         * Wake the worker thread to remove the snapshot we've
                         *  created.
                         */
                        self.bell.notify_all();

                        self.store(inner);
                    }

                    State::Failed => {
                        /*
                         * For now, this terminal state will preserve evidence
                         *  for investigation.
                         */
                        bail!(
                            "region {} running snapshot {} failed to provision \
                            and cannot be destroyed",
                            request.id.0,
                            request.name,
                        );
                    }
                }
            }
        }

        Ok(())
    }

    pub fn delete_snapshot(
        &self,
        request: DeleteSnapshotRequest,
    ) -> Result<()> {
        let inner = self.inner.lock().unwrap();

        /*
         * Are we running a read-only downstairs for this snapshot? Fail if so.
         */
        if let Some(running_snapshots) =
            inner.running_snapshots.get(&request.id)
        {
            if let Some(running_snapshot) = running_snapshots.get(&request.name)
            {
                match running_snapshot.state {
                    State::Requested | State::Created | State::Tombstoned => {
                        bail!(
                            "read-only downstairs running for region {} snapshot {}",
                            request.id.0,
                            request.name
                        );
                    }

                    State::Destroyed => {
                        // ok to delete
                    }

                    State::Failed => {
                        // Something has set the running snapshot to state
                        // failed, so we can't delete this snapshot.
                        bail!(
                            "read-only downstairs state set to failed for region {} snapshot {}",
                            request.id.0,
                            request.name
                        );
                    }
                }
            }
        }

        let mut path = self.base_path.to_path_buf();
        path.push("regions");
        path.push(request.id.0.clone());

        let dataset = match ZFSDataset::new(
            path.into_os_string().into_string().unwrap(),
        ) {
            Ok(dataset) => dataset,
            Err(e) => {
                // This branch can only be entered if `zfs list` for that
                // dataset path failed to return anything.

                // Did the region exist in the past, and was it already deleted?
                if let Some(region) = inner.regions.get(&request.id) {
                    match region.state {
                        State::Tombstoned | State::Destroyed => {
                            // If so, any snapshots must have been deleted
                            // before the agent would allow the region to be
                            // deleted.
                            return Ok(());
                        }

                        State::Requested | State::Created => {
                            // This is a bug: according to the agent's datafile,
                            // the region exists, but according to zfs list, it
                            // does not
                            bail!("Agent thinks region {} exists but zfs list does not! {e}", request.id.0);
                        }

                        State::Failed => {
                            // Something has set the region to state failed, so
                            // we can't delete this snapshot.
                            bail!(
                                "Region {} is in state failed! {e}",
                                request.id.0
                            );
                        }
                    }
                } else {
                    // In here, the region never existed!
                    bail!("Inside region {} snapshot {} delete, region never existed! {e}", request.id.0, request.name);
                }
            }
        };

        let snapshot_name = format!("{}@{}", dataset.dataset(), request.name);

        self.snapshot_interface.delete_snapshot(snapshot_name)?;

        Ok(())
    }

    /**
     * Mark a particular region as failed to provision.
     */
    pub fn fail(&self, id: &RegionId) {
        let mut inner = self.inner.lock().unwrap();

        let r = inner.regions.get_mut(id).unwrap();
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
     * Mark a particular running snapshot as failed to provision.
     */
    pub fn fail_rs(&self, region_id: &RegionId, snapshot_name: &str) {
        let mut inner = self.inner.lock().unwrap();

        let rs = inner
            .running_snapshots
            .get_mut(region_id)
            .unwrap()
            .get_mut(snapshot_name)
            .unwrap();

        let nstate = State::Failed;
        if rs.state == nstate {
            return;
        }

        info!(
            self.log,
            "region {} running snapshot {} state: {:?} -> {:?}",
            rs.id.0,
            rs.name,
            rs.state,
            nstate,
        );
        rs.state = nstate;

        self.store(inner);
    }

    /**
     * Mark a particular region as provisioned.
     */
    pub fn created(&self, id: &RegionId) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        let r = inner.regions.get_mut(id).unwrap();
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
     * Mark a particular running snapshot as created.
     */
    pub fn created_rs(
        &self,
        region_id: &RegionId,
        snapshot_name: &str,
    ) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        let rs = inner
            .running_snapshots
            .get_mut(region_id)
            .unwrap()
            .get_mut(snapshot_name)
            .unwrap();

        let nstate = State::Created;

        match &rs.state {
            State::Requested => (),

            State::Tombstoned => {
                /*
                 * Something else set this to Tombstoned between when the SMF
                 * was applied and before the state in the datafile changed!
                 * This means that Nexus requested that we destroy this running
                 * snapshot before we finished creating it. Return Ok(()) here,
                 * something else is working on this running snapshot.
                 */
                return Ok(());
            }

            x => {
                /*
                 * Something else set this to an unexpected state. Bailing here
                 * will cause the RS to be marked as failed, we'll have to
                 * investigate.
                 */
                error!(
                    self.log,
                    "region {} running snapshot {} is currently in unexpected state: {:?}",
                    rs.id.0,
                    rs.name,
                    rs.state,
                );

                bail!(
                    "created region {} running_snapshot {} in weird state {:?}",
                    rs.id.0,
                    rs.name,
                    x
                );
            }
        }

        info!(
            self.log,
            "region {} running snapshot {} state: {:?} -> {:?}",
            rs.id.0,
            rs.name,
            rs.state,
            nstate,
        );
        rs.state = nstate;

        self.store(inner);
        Ok(())
    }

    /**
     * Mark a particular region as destroyed. Do not remove the record: it's
     * important for calls to the agent to be idempotent in order to safely be
     * used in a saga.
     */
    pub fn destroyed(&self, id: &RegionId) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        let r = inner.regions.get_mut(id).unwrap();
        let nstate = State::Destroyed;
        match &r.state {
            State::Requested => (),
            State::Tombstoned => (),
            x => bail!("region to destroy in weird state {:?}", x),
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
     * Mark a particular running snapshot as destroyed. Do not remove the
     * record: it's important for calls to the agent to be idempotent in order
     * to safely be used in a saga.
     */
    pub fn destroyed_rs(
        &self,
        region_id: &RegionId,
        snapshot_name: &str,
    ) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        let rs = inner
            .running_snapshots
            .get_mut(region_id)
            .unwrap()
            .get_mut(snapshot_name)
            .unwrap();

        let nstate = State::Destroyed;

        info!(
            self.log,
            "region {} running snapshot {} state: {:?} -> {:?}",
            rs.id.0,
            rs.name,
            rs.state,
            nstate,
        );
        rs.state = nstate;

        self.store(inner);
        Ok(())
    }

    /**
     * Nexus has requested that we destroy this particular region.
     */
    pub fn destroy(&self, id: &RegionId) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        let r = inner
            .regions
            .get_mut(id)
            .ok_or_else(|| anyhow!("region {} does not exist", id.0))?;

        match r.state {
            State::Tombstoned | State::Destroyed => {
                /*
                 * Either:
                 * - Destroy already scheduled.
                 * - Already destroyed; no more work to do.
                 */
            }
            State::Requested | State::Created | State::Failed => {
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
            }
        }

        Ok(())
    }

    /**
     * The worker thread will request the first resource that is in a
     * particular state. If there are no resources in the provided state,
     * wait on the condition variable.
     */
    pub fn first_in_states(&self, states: &[State]) -> Resource {
        let mut inner = self.inner.lock().unwrap();

        loop {
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
                        return Resource::Region(r.clone());
                    }
                }

                for (rid, r) in &inner.running_snapshots {
                    for (name, rs) in r {
                        if &rs.state == s {
                            return Resource::RunningSnapshot(
                                rid.clone(),
                                name.clone(),
                                rs.clone(),
                            );
                        }
                    }
                }
            }

            /*
             * If we did not find any regions in the specified state, sleep
             * on the condvar.
             */
            inner = self.bell.wait(inner).unwrap();
        }
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

        match region.state {
            State::Requested
            | State::Destroyed
            | State::Tombstoned
            | State::Failed => {
                // Either the region hasn't been created yet, or it has been
                // destroyed or marked to be destroyed (both of which require
                // that no snapshots exist). Return an empty list.
                return Ok(vec![]);
            }

            State::Created => {
                // proceed to next section
            }
        }

        let mut path = self.base_path.to_path_buf();
        path.push("regions");
        path.push(region_id.0.clone());

        info!(self.log, "path is {:?}", &path);

        let dataset =
            ZFSDataset::new(path.into_os_string().into_string().unwrap())?;

        info!(self.log, "dataset is {}", dataset.dataset());

        let results = self
            .snapshot_interface
            .get_snapshots_for_dataset(dataset.dataset())?;

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

        let actual = Utc
            .timestamp_opt(String::from_utf8_lossy(&fake_stdout).parse()?, 0)
            .unwrap();

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

        let _date = Utc.timestamp_opt(cmd_stdout.parse()?, 0).unwrap();

        Ok(())
    }
}
