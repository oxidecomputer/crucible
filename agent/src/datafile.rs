use super::model::{CreateRegion, Region, RegionId, State};
use anyhow::{anyhow, bail, Result};
use crucible_common::write_json;
use slog::{crit, info, Logger};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex, MutexGuard};

pub struct DataFile {
    log: Logger,
    path: PathBuf,
    port_min: u16,
    port_max: u16,
    bell: Condvar,
    inner: Mutex<Inner>,
}

struct Inner {
    regions: BTreeMap<RegionId, Region>,
}

impl DataFile {
    pub fn new(
        log: Logger,
        path: &Path,
        port_min: u16,
        port_max: u16,
    ) -> Result<DataFile> {
        /*
         * Open data file, load contents.
         */
        let regions = match crucible_common::read_json_maybe(path) {
            Ok(Some(regions)) => regions,
            Ok(None) => BTreeMap::new(),
            Err(e) => bail!("failed to load data file {:?}: {:?}", path, e),
        };

        Ok(DataFile {
            log,
            path: path.to_path_buf(),
            port_min,
            port_max,
            bell: Condvar::new(),
            inner: Mutex::new(Inner { regions }),
        })
    }

    pub fn all(&self) -> Vec<Region> {
        self.inner
            .lock()
            .unwrap()
            .regions
            .values()
            .cloned()
            .collect()
    }

    pub fn get(&self, id: &RegionId) -> Option<Region> {
        self.inner.lock().unwrap().regions.get(id).cloned()
    }

    /**
     * Store the database into the JSON file.
     */
    fn store(&self, inner: MutexGuard<Inner>) {
        loop {
            match write_json(&self.path, &inner.regions, true) {
                Ok(()) => return,
                Err(e) => {
                    /*
                     * XXX What else could we do here?
                     */
                    crit!(
                        self.log,
                        "could not write data file {:?} (will retry): {:?}",
                        &self.path,
                        e
                    );
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
    }

    /**
     * Nexus will request that we create a new region by telling us the ID it
     * should have.  To make this idempotent, we will either create the region
     * or return the current state of the region if it was already created in
     * the past.
     *
     * The actual heavy lifting is performed in a worker thread.
     */
    pub fn request(&self, create: CreateRegion) -> Result<Region> {
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
        let port_number = (self.port_min..=self.port_max)
            .find(|n| {
                !inner
                    .regions
                    .values()
                    .filter(|r| {
                        /*
                         * We can ignore any completely destroyed region.  We
                         * choose not to ignore regions in the failed state for
                         * now, as they may still prevent use of their assigned
                         * port number.
                         */
                        r.state != State::Destroyed
                    })
                    .any(|r| r.port_number == *n)
            })
            .ok_or_else(|| anyhow!("no free port numbers"))?;

        let r = Region {
            id: create.id.clone(),
            volume_id: create.volume_id,
            block_size: create.block_size,
            extent_size: create.extent_size,
            extent_count: create.extent_count,
            port_number,
            state: State::Requested,
        };

        info!(self.log, "region {} state: {:?}", r.id.0, r.state);
        let old = inner.regions.insert(create.id, r.clone());
        assert!(old.is_none());

        /*
         * Wake the worker thread to look at the region we've created.
         */
        self.bell.notify_all();

        self.store(inner);

        return Ok(r);
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

        self.store(inner);
        Ok(())
    }

    /**
     * Nexus has requested that we destroy this particular region.
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
     * The worker thread will request the first region that is in a particular
     * state.  If there are no tasks in the provided state, we sleep waiting for
     * work to do.
     */
    pub fn first_in_states(&self, states: &[State]) -> Region {
        let mut inner = self.inner.lock().unwrap();

        loop {
            /*
             * States are provided in priority order.  We check for regions in
             * the first requested state before we check for regions in the
             * second provided state, etc.  This allows us to focus on
             * destroying tombstoned regions ahead of creating new regions.
             */
            for s in states {
                for r in inner.regions.values() {
                    if &r.state == s {
                        return r.clone();
                    }
                }
            }

            /*
             * If we did not find any regions in the specified state, sleep on
             * the condvar.
             */
            inner = self.bell.wait(inner).unwrap();
        }
    }
}
