use crate::JobId;

/// Stores a set of complete jobs
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

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.completed.is_empty()
    }

    /// Records a new complete job in the list
    pub fn push(&mut self, id: JobId) {
        self.completed.push(id);
    }

    /// Resets the data structure, given a new barrier operation
    ///
    /// All older jobs are forgotten, and the provided operation becomes the
    /// oldest complete job.
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

    /// Returns the list of completed jobs
    pub fn completed(&self) -> &[JobId] {
        &self.completed
    }
}
