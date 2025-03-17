use crate::JobId;
use rangemap::RangeSet;

/// Stores a set of complete jobs
#[derive(Debug)]
pub struct CompletedJobs {
    completed: RangeSet<JobId>,
}

impl CompletedJobs {
    pub fn new(last_flush: Option<JobId>) -> Self {
        Self {
            completed: last_flush
                .into_iter()
                .map(|id| id..JobId(id.0 + 1))
                .collect(),
        }
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.completed.is_empty()
    }

    /// Records a new complete job in the list
    pub fn push(&mut self, id: JobId) {
        self.completed.insert(id..JobId(id.0 + 1));
    }

    /// Resets the data structure, given a new barrier operation
    ///
    /// All older jobs are forgotten, and the provided operation becomes the
    /// oldest complete job.
    pub fn reset(&mut self, id: JobId) {
        self.completed.clear();
        self.completed.insert(id..JobId(id.0 + 1));
    }

    /// Checks whether the given job is complete
    ///
    /// A job is complete if it is listed in the set of complete jobs.
    pub fn is_complete(&self, id: JobId) -> bool {
        self.completed.contains(&id)
    }

    /// Returns the list of completed jobs
    pub fn completed(&self) -> impl Iterator<Item = JobId> + use<'_> {
        self.completed
            .iter()
            .flat_map(|r| (r.start.0..r.end.0).map(JobId))
    }
}
