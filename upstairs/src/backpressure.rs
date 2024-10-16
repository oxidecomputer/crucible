// Copyright 2024 Oxide Computer Company

use crate::IOop;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

/// Helper struct to contain a set of backpressure counters
#[derive(Debug)]
pub struct BackpressureCounters(Arc<BackpressureCountersInner>);

/// Inner data structure for individual backpressure counters
#[derive(Debug)]
struct BackpressureCountersInner {
    /// Number of bytes from `Write` and `WriteUnwritten` operations
    ///
    /// This value is used for global backpressure, to avoid buffering too many
    /// writes (which otherwise return immediately, and are not persistent until
    /// a flush)
    write_bytes: AtomicU64,

    /// Number of jobs in the queue
    ///
    /// This value is also used for global backpressure
    // XXX should we only count write jobs here?  Or should we also count read
    // bytes for global backpressure?  Much to ponder...
    jobs: AtomicU64,

    /// Number of bytes from `Write`, `WriteUnwritten`, and `Read` operations
    ///
    /// This value is used for local backpressure, to keep the 3x Downstairs
    /// roughly in sync.  Otherwise, the fastest Downstairs will answer all read
    /// requests, and the others can get arbitrarily far behind.
    io_bytes: AtomicU64,
}

/// Guard to automatically decrement backpressure bytes when dropped
#[derive(Debug)]
pub struct BackpressureGuard {
    counter: Arc<BackpressureCountersInner>,
    write_bytes: u64,
    io_bytes: u64,
    // There's also an implicit "1 job" here
}

impl Drop for BackpressureGuard {
    fn drop(&mut self) {
        self.counter
            .write_bytes
            .fetch_sub(self.write_bytes, Ordering::Relaxed);
        self.counter
            .io_bytes
            .fetch_sub(self.io_bytes, Ordering::Relaxed);
        self.counter.jobs.fetch_sub(1, Ordering::Relaxed);
    }
}

impl BackpressureGuard {
    #[cfg(test)]
    pub fn dummy() -> Self {
        let counter = Arc::new(BackpressureCountersInner {
            write_bytes: 0.into(),
            io_bytes: 0.into(),
            jobs: 1.into(),
        });
        Self {
            counter,
            write_bytes: 0,
            io_bytes: 0,
        }
    }
}

impl BackpressureCounters {
    pub fn new() -> Self {
        Self(Arc::new(BackpressureCountersInner {
            write_bytes: AtomicU64::new(0),
            io_bytes: AtomicU64::new(0),
            jobs: AtomicU64::new(0),
        }))
    }

    pub fn get_write_bytes(&self) -> u64 {
        self.0.write_bytes.load(Ordering::Relaxed)
    }

    pub fn get_io_bytes(&self) -> u64 {
        self.0.io_bytes.load(Ordering::Relaxed)
    }

    pub fn get_jobs(&self) -> u64 {
        self.0.jobs.load(Ordering::Relaxed)
    }

    /// Stores write / IO bytes (and 1 job) for a pending write
    #[must_use]
    pub fn early_write_increment(&mut self, bytes: u64) -> BackpressureGuard {
        self.0.write_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.0.io_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.0.jobs.fetch_add(1, Ordering::Relaxed);
        BackpressureGuard {
            counter: self.0.clone(),
            write_bytes: bytes,
            io_bytes: bytes,
            // implicit 1 job
        }
    }

    /// Stores write / IO bytes (and 1 job) in the backpressure counters
    #[must_use]
    pub fn increment(&mut self, io: &IOop) -> BackpressureGuard {
        let write_bytes = io.write_bytes();
        let io_bytes = io.job_bytes();
        self.0.write_bytes.fetch_add(write_bytes, Ordering::Relaxed);
        self.0.io_bytes.fetch_add(io_bytes, Ordering::Relaxed);
        self.0.jobs.fetch_add(1, Ordering::Relaxed);
        BackpressureGuard {
            counter: self.0.clone(),
            write_bytes,
            io_bytes,
            // implicit 1 job
        }
    }
}
