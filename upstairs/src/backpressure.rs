// Copyright 2023 Oxide Computer Company
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

#[derive(Debug, Default)]
pub(crate) struct BackpressureCounters {
    /// The number of write bytes that haven't finished yet
    ///
    /// This is used to configure backpressure to the host, because writes
    /// (uniquely) will return before actually being completed by a Downstairs
    /// and can clog up the queues.
    write_bytes_outstanding: AtomicU64,

    /// Number of IO jobs in the downstairs queue
    io_jobs: AtomicU64,

    /// Cached value for backpressure
    ///
    /// This is written whenever we recompute backpressure, and read by the
    /// `GuestIoHandle` when it wants to print updated stats.
    backpressure_us: AtomicU64,
}

impl BackpressureCounters {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn write_bytes_outstanding(&self) -> u64 {
        self.write_bytes_outstanding.load(Ordering::Relaxed)
    }

    pub fn backpressure_us(&self) -> u64 {
        self.backpressure_us.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub(crate) struct BackpressureGuard {
    bp: Arc<BackpressureCounters>,
    write_bytes: u64,
}

impl BackpressureGuard {
    pub fn new(bp: Arc<BackpressureCounters>, write_bytes: u64) -> Self {
        bp.write_bytes_outstanding
            .fetch_add(write_bytes, Ordering::Relaxed);
        bp.io_jobs.fetch_add(1, Ordering::Relaxed);
        Self { bp, write_bytes }
    }
}

impl Drop for BackpressureGuard {
    fn drop(&mut self) {
        self.bp
            .write_bytes_outstanding
            .fetch_sub(self.write_bytes, Ordering::Relaxed);
        self.bp.io_jobs.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Configuration for host-side backpressure
///
/// Backpressure adds an artificial delay to host write messages (which are
/// otherwise acked immediately, before actually being complete).  The delay is
/// varied based on two metrics:
///
/// - number of write bytes outstanding
/// - queue length as a fraction (where 1.0 is full)
///
/// These two metrics are used for quadratic backpressure, picking the larger of
/// the two delays.
#[derive(Copy, Clone, Debug)]
pub(crate) struct BackpressureConfig {
    /// When should backpressure start (in bytes)?
    pub bytes_start: u64,
    /// Scale for byte-based quadratic backpressure
    pub bytes_scale: f64,

    /// When should queue-based backpressure start?
    pub queue_start: f64,
    /// Maximum queue-based delay
    pub queue_max_delay: Duration,
}

impl BackpressureConfig {
    pub fn get_delay(&self, bp: &BackpressureCounters) -> Duration {
        let bytes = bp.write_bytes_outstanding.load(Ordering::Relaxed);
        let jobs = bp.io_jobs.load(Ordering::Relaxed);
        let ratio = jobs as f64 / crate::IO_OUTSTANDING_MAX as f64;

        // Check to see if the number of outstanding write bytes (between
        // the upstairs and downstairs) is particularly high.  If so,
        // apply some backpressure by delaying host operations, with a
        // quadratically-increasing delay.
        let d1 = (bytes.saturating_sub(self.bytes_start) as f64
            * self.bytes_scale)
            .powf(2.0) as u64;

        // Compute an alternate delay based on queue length
        let d2 = self
            .queue_max_delay
            .mul_f64(
                ((ratio - self.queue_start).max(0.0)
                    / (1.0 - self.queue_start))
                    .powf(2.0),
            )
            .as_micros() as u64;

        // Write the current value to the cache in BackpressureCounters
        let bp_us = d1.max(d2);
        bp.backpressure_us.store(bp_us, Ordering::Relaxed);

        Duration::from_micros(bp_us)
    }
}
