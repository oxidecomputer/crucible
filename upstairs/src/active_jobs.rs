// Copyright 2023 Oxide Computer Company

use crucible_common::RegionDefinition;

use crate::{DownstairsIO, ImpactedBlocks};
use std::collections::BTreeMap;

#[derive(Debug)]
pub(crate) struct ActiveJobs {
    jobs: BTreeMap<u64, DownstairsIO>,
}

impl ActiveJobs {
    pub fn new() -> Self {
        Self {
            jobs: BTreeMap::new(),
        }
    }

    /// Looks up a job by ID, returning a reference
    pub fn get(&self, job_id: &u64) -> Option<&DownstairsIO> {
        self.jobs.get(job_id)
    }

    /// Looks up a job by ID, returning a mutable reference
    pub fn get_mut(&mut self, job_id: &u64) -> Option<&mut DownstairsIO> {
        self.jobs.get_mut(job_id)
    }

    /// Returns the total number of active jobs
    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    /// Returns `true` if no jobs are active
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    /// Returns an iterator over all active jobs
    pub fn iter_mut(
        &mut self,
    ) -> impl Iterator<Item = (&u64, &mut DownstairsIO)> {
        self.jobs.iter_mut()
    }

    /// Inserts a new job ID and its associated IO work
    pub fn insert(
        &mut self,
        job_id: u64,
        io: DownstairsIO,
    ) -> Option<DownstairsIO> {
        self.jobs.insert(job_id, io)
    }

    /// Removes a job by ID, returning its IO work
    pub fn remove(&mut self, job_id: &u64) -> Option<DownstairsIO> {
        self.jobs.remove(job_id)
    }

    /// Returns an iterator over job IDs
    pub fn keys(&self) -> std::collections::btree_map::Keys<u64, DownstairsIO> {
        self.jobs.keys()
    }

    /// Returns an iterator over job values
    #[cfg(test)]
    pub fn values(
        &self,
    ) -> std::collections::btree_map::Values<u64, DownstairsIO> {
        self.jobs.values()
    }

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
                break;
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
}

impl<'a> IntoIterator for &'a ActiveJobs {
    type Item = (&'a u64, &'a DownstairsIO);
    type IntoIter = std::collections::btree_map::Iter<'a, u64, DownstairsIO>;

    fn into_iter(self) -> Self::IntoIter {
        self.jobs.iter()
    }
}

impl Extend<(u64, DownstairsIO)> for ActiveJobs {
    fn extend<T: IntoIterator<Item = (u64, DownstairsIO)>>(&mut self, iter: T) {
        for (job_id, io) in iter {
            self.insert(job_id, io);
        }
    }
}
