use crate::JobId;

/// Stores a flush operation and a set of complete jobs
///
/// The flush operation is not included in the list of complete jobs, but acts
/// as a lower bound for dependencies; any job older than the flush is assumed
/// to have completed.
#[derive(Debug)]
pub struct CompletedJobs {
    last_flush: Option<JobId>,
    completed: Vec<JobId>,
}

impl CompletedJobs {
    pub fn new(last_flush: Option<JobId>) -> Self {
        Self {
            last_flush,
            completed: vec![],
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
        self.last_flush = Some(id);
        self.completed.clear();
    }

    /// Checks whether the given job is complete
    ///
    /// A job is complete if it precedes our last barrier operation or is listed
    /// in the set of complete jobs.
    pub fn is_complete(&self, id: JobId) -> bool {
        // We deliberately reverse the `completed` list because new jobs are at
        // the back and are more likely to be what we care about
        self.last_flush.map(|b| id <= b).unwrap_or(false)
            || self.completed.iter().rev().any(|j| *j == id)
    }

    pub fn completed(&self) -> &[JobId] {
        &self.completed
    }

    pub fn last_flush(&self) -> Option<JobId> {
        self.last_flush
    }
}
