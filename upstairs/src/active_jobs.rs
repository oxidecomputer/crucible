// Copyright 2023 Oxide Computer Company

use crucible_common::RegionDefinition;

use crate::{AckStatus, DownstairsIO, ImpactedBlocks};
use std::collections::{BTreeMap, BTreeSet};

/// `ActiveJobs` tracks active jobs by ID
///
/// It exposes an API that roughly matches a `BTreeMap<u64, DownstairsIO>`,
/// but leaves open the possibility for further optimization.
///
/// Notably, there is no way to directly modify a `DownstairsIO` contained in
/// `ActiveJobs`.  Bulk modification can be done with `for_each`, and individual
/// modification can be done with `get_mut`, which returns a
/// `DownstairsIOHandle` instead of a raw `&mut DownstairsIO`.  All of this
/// means that we can keep extra metadata in sync, e.g. a list of all ackable
/// jobs.
#[derive(Debug)]
pub(crate) struct ActiveJobs {
    jobs: BTreeMap<u64, DownstairsIO>,
    ackable: BTreeSet<u64>,
}

impl ActiveJobs {
    pub fn new() -> Self {
        Self {
            jobs: BTreeMap::new(),
            ackable: BTreeSet::new(),
        }
    }

    /// Looks up a job by ID, returning a reference
    #[inline]
    pub fn get(&self, job_id: &u64) -> Option<&DownstairsIO> {
        self.jobs.get(job_id)
    }

    /// Looks up a job by ID, returning a mutable reference
    #[inline]
    pub fn get_mut(&mut self, job_id: &u64) -> Option<DownstairsIOHandle> {
        self.jobs
            .get_mut(job_id)
            .map(|job| DownstairsIOHandle::new(job, &mut self.ackable))
    }

    /// Returns the total number of active jobs
    #[inline]
    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    /// Returns `true` if no jobs are active
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    /// Applies a function across all
    #[inline]
    pub fn for_each<F: FnMut(&u64, &mut DownstairsIO)>(&mut self, mut f: F) {
        for (job_id, job) in self.jobs.iter_mut() {
            let handle = DownstairsIOHandle::new(job, &mut self.ackable);
            f(job_id, handle.job);
        }
    }

    /// Inserts a new job ID and its associated IO work
    #[inline]
    pub fn insert(
        &mut self,
        job_id: u64,
        io: DownstairsIO,
    ) -> Option<DownstairsIO> {
        self.jobs.insert(job_id, io)
    }

    /// Removes a job by ID, returning its IO work
    #[inline]
    pub fn remove(&mut self, job_id: &u64) -> Option<DownstairsIO> {
        self.jobs.remove(job_id)
    }

    /// Returns an iterator over job IDs
    #[inline]
    pub fn keys(&self) -> std::collections::btree_map::Keys<u64, DownstairsIO> {
        self.jobs.keys()
    }

    /// Returns an iterator over job values
    #[cfg(test)]
    #[inline]
    pub fn values(
        &self,
    ) -> std::collections::btree_map::Values<u64, DownstairsIO> {
        self.jobs.values()
    }

    #[inline]
    pub fn iter(&self) -> std::collections::btree_map::Iter<u64, DownstairsIO> {
        self.jobs.iter()
    }

    pub fn deps_for_flush(&self) -> Vec<u64> {
        /*
         * To build the dependency list for this flush, iterate from the end of
         * the downstairs work active list in reverse order and check each job
         * in that list to see if this new flush must depend on it.
         *
         * We can safely ignore everything before the last flush, because the
         * last flush will depend on jobs before it. But this flush must depend
         * on the last flush - flush and gen numbers downstairs need to be
         * sequential and the same for each downstairs.
         *
         * The downstairs currently assumes that all jobs previous to the last
         * flush have completed, so the Upstairs must set that flushes depend on
         * all jobs. It's currently important that flushes depend on everything,
         * and everything depends on flushes.
         */
        let num_jobs = self.len();
        let mut dep: Vec<u64> = Vec::with_capacity(num_jobs);

        for (id, job) in self.iter().rev() {
            // Flushes must depend on everything
            dep.push(*id);

            // Depend on the last flush, but then bail out
            if job.work.is_flush() {
                break;
            }
        }
        dep
    }

    pub fn deps_for_write(
        &self,
        impacted_blocks: ImpactedBlocks,
        _ddef: RegionDefinition,
    ) -> Vec<u64> {
        /*
         * To build the dependency list for this write, iterate from the end
         * of the downstairs work active list in reverse order and
         * check each job in that list to see if this new write must
         * depend on it.
         *
         * Construct a list of dependencies for this write based on the
         * following rules:
         *
         * - writes have to depend on the last flush completing (because
         *   currently everything has to depend on flushes)
         * - any overlap of impacted blocks requires a dependency
         *
         * It's important to remember that jobs may arrive at different
         * Downstairs in different orders but they should still complete in job
         * dependency order.
         *
         * TODO: any overlap of impacted blocks will create a dependency.
         * take this an example (this shows three writes, all to the
         * same block, along with the dependency list for each
         * write):
         *
         *       block
         * op# | 0 1 2 | deps
         * ----|-------------
         *   0 | W     |
         *   1 | W     | 0
         *   2 | W     | 0,1
         *
         * op 2 depends on both op 1 and op 0. If dependencies are transitive
         * with an existing job, it would be nice if those were removed from
         * this job's dependencies.
         */
        let num_jobs = self.len();
        let mut dep: Vec<u64> = Vec::with_capacity(num_jobs);

        // Search backwards in the list of active jobs
        for (id, job) in self.iter().rev() {
            // Depend on the last flush - flushes are a barrier for
            // all writes.
            if job.work.is_flush() {
                dep.push(*id);
            }

            // If this job impacts the same blocks as something already active,
            // create a dependency.
            if impacted_blocks.conflicts(&job.impacted_blocks) {
                dep.push(*id);
            }
        }
        dep
    }

    pub fn deps_for_read(
        &self,
        impacted_blocks: ImpactedBlocks,
        _ddef: RegionDefinition,
    ) -> Vec<u64> {
        /*
         * To build the dependency list for this read, iterate from the end
         * of the downstairs work active list in reverse order and
         * check each job in that list to see if this new read must
         * depend on it.
         *
         * Construct a list of dependencies for this read based on the
         * following rules:
         *
         * - reads depend on flushes (because currently everything has to depend
         *   on flushes)
         * - any write with an overlap of impacted blocks requires a
         *   dependency
         */
        let num_jobs = self.len();
        let mut dep: Vec<u64> = Vec::with_capacity(num_jobs);

        // Search backwards in the list of active jobs
        for (id, job) in self.iter().rev() {
            if job.work.is_flush() {
                dep.push(*id);
                break;
            } else if (job.work.is_write() | job.work.is_repair())
                && impacted_blocks.conflicts(&job.impacted_blocks)
            {
                // If this is a write or repair and it impacts the same blocks
                // as something already active, create a dependency.
                dep.push(*id);
            }
        }
        dep
    }

    // Build the list of dependencies for a live repair job.  These are jobs that
    // must finish before this repair job can move begin.  Because we need all
    // three repair jobs to happen lock step, we have to prevent any IO from
    // hitting the same extent, which means any IO going to our ImpactedBlocks
    // (the whole extent) must finish first (or come after) our job.
    pub fn deps_for_live_repair(
        &self,
        impacted_blocks: ImpactedBlocks,
        close_id: u64,
        _ddef: RegionDefinition,
    ) -> Vec<u64> {
        let num_jobs = self.len();
        let mut deps: Vec<u64> = Vec::with_capacity(num_jobs);

        // Search backwards in the list of active jobs, stop at the
        // last flush
        for (id, job) in self.iter().rev() {
            // We are finding dependencies based on impacted blocks.
            // We may have reserved future job IDs for repair, and it's possible
            // that this job we are finding dependencies for now is actually a
            // future repair job we reserved.  If so, don't include ourself in
            // our own list of dependencies.
            if job.ds_id > close_id {
                continue;
            }
            // If this operation impacts the same blocks as something
            // already active, create a dependency.
            if impacted_blocks.conflicts(&job.impacted_blocks) {
                deps.push(*id);
            }

            // A flush job won't show impacted blocks. We can stop looking
            // for dependencies beyond the last flush.
            if job.work.is_flush() {
                deps.push(*id);
                break;
            }
        }

        deps
    }

    pub fn ackable_work(&self) -> Vec<u64> {
        self.ackable.iter().cloned().collect()
    }
}

impl<'a> IntoIterator for &'a ActiveJobs {
    type Item = (&'a u64, &'a DownstairsIO);
    type IntoIter = std::collections::btree_map::Iter<'a, u64, DownstairsIO>;

    fn into_iter(self) -> Self::IntoIter {
        self.jobs.iter()
    }
}

/// Handle for a `DownstairsIO` that keeps secondary data in sync
///
/// Many parts of the code want to modify a `DownstairsIO` by directly poking
/// its fields.  This makes it hard to keep secondary data in sync, e.g.
/// maintaining a separate list of all ackable IOs.
pub(crate) struct DownstairsIOHandle<'a> {
    pub job: &'a mut DownstairsIO,
    initial_status: AckStatus,
    ackable: &'a mut BTreeSet<u64>,
}

impl<'a> std::fmt::Debug for DownstairsIOHandle<'a> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        self.job.fmt(f)
    }
}

impl<'a> DownstairsIOHandle<'a> {
    fn new(job: &'a mut DownstairsIO, ackable: &'a mut BTreeSet<u64>) -> Self {
        let initial_status = job.ack_status;
        Self {
            job,
            initial_status,
            ackable,
        }
    }

    pub fn job(&mut self) -> &mut DownstairsIO {
        self.job
    }
}

impl<'a> std::ops::Drop for DownstairsIOHandle<'a> {
    fn drop(&mut self) {
        match (self.initial_status, self.job.ack_status) {
            (AckStatus::NotAcked, AckStatus::AckReady) => {
                let prev = self.ackable.insert(self.job.ds_id);
                assert!(prev);
            }
            (AckStatus::AckReady, AckStatus::Acked | AckStatus::NotAcked) => {
                let prev = self.ackable.remove(&self.job.ds_id);
                assert!(prev);
            }
            // None transitions
            (AckStatus::AckReady, AckStatus::AckReady)
            | (AckStatus::Acked, AckStatus::Acked)
            | (AckStatus::NotAcked, AckStatus::NotAcked) => (),

            // Invalid transitions!
            (AckStatus::NotAcked, AckStatus::Acked)
            | (AckStatus::Acked, AckStatus::NotAcked)
            | (AckStatus::Acked, AckStatus::AckReady) => {
                panic!(
                    "invalid transition: {:?} => {:?}",
                    self.initial_status, self.job.ack_status
                )
            }
        }
    }
}
