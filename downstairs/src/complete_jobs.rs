use crate::JobId;

/// Stores a flush operation and a set of complete jobs
///
/// The flush operation is not included in the list of complete jobs, but acts
/// as a lower bound for dependencies; any job older than the flush is assumed
/// to have completed.
#[derive(Debug)]
pub struct CompletedJobs {
    completed: Vec<JobId>,
}

impl CompletedJobs {
    pub fn new(last_flush: Option<JobId>) -> Self {
        Self {
            completed: last_flush.into_iter().collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.completed.is_empty()
    }

    /// Records a new complete job in the list
    pub fn push(&mut self, id: JobId) {
        self.completed.push(id);
    }

    /// Resets the data structure given a new barrier operation
    pub fn reset(&mut self, id: JobId) {
        self.completed.clear();
        self.completed.push(id);
    }

    /// Checks whether the given job is complete
    ///
    /// A job is complete if it is listed in the set of complete jobs.
    pub fn is_complete(&self, id: JobId) -> bool {
        // We deliberately reverse the `completed` list because new jobs are at
        // the back and are more likely to be what we care about
        self.completed.iter().rev().any(|j| *j == id)
    }

    pub fn completed(&self) -> &[JobId] {
        &self.completed
    }
}
