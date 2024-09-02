// Copyright 2024 Oxide Computer Company

use crate::{IOop, IO_OUTSTANDING_MAX_BYTES, IO_OUTSTANDING_MAX_JOBS};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
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

/// Configuration for host-side backpressure
///
/// Backpressure adds an artificial delay to host write messages (which are
/// otherwise acked immediately, before actually being complete).  The delay is
/// varied based on two metrics:
///
/// - number of write bytes outstanding
/// - queue length (in jobs)
///
/// We compute backpressure delay based on both metrics, then pick the larger of
/// the two delays.
#[derive(Copy, Clone, Debug)]
pub struct BackpressureConfig {
    pub bytes: BackpressureChannelConfig,
    pub queue: BackpressureChannelConfig,
}

impl Default for BackpressureConfig {
    fn default() -> BackpressureConfig {
        // `max_value` values below must be higher than `IO_OUTSTANDING_MAX_*`;
        // otherwise, replaying jobs to a previously-Offline Downstairs could
        // immediately kick us into the saturated regime, which would be
        // unfortunate.
        BackpressureConfig {
            // Byte-based backpressure
            bytes: BackpressureChannelConfig {
                start_value: 50 * 1024u64.pow(2), // 50 MiB
                max_value: IO_OUTSTANDING_MAX_BYTES * 2,
                delay_scale: Duration::from_millis(100),
                delay_max: Duration::from_millis(30_000),
            },

            // Queue-based backpressure
            queue: BackpressureChannelConfig {
                start_value: 500,
                max_value: IO_OUTSTANDING_MAX_JOBS as u64 * 2,
                delay_scale: Duration::from_millis(5),
                delay_max: Duration::from_millis(30_000),
            },
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct BackpressureChannelConfig {
    /// When should backpressure start
    pub start_value: u64,
    /// Value at which backpressure is saturated
    pub max_value: u64,

    /// Characteristic scale of backpressure
    ///
    /// This scale sets the backpressure delay halfway between `start`_value and
    /// `max_value`
    pub delay_scale: Duration,

    /// Maximum delay (returned at `max_value`)
    pub delay_max: Duration,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BackpressureAmount {
    Duration(Duration),
    Saturated,
}

impl std::cmp::Ord for BackpressureAmount {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (BackpressureAmount::Saturated, BackpressureAmount::Saturated) => {
                std::cmp::Ordering::Equal
            }
            (BackpressureAmount::Saturated, _) => std::cmp::Ordering::Greater,
            (_, BackpressureAmount::Saturated) => std::cmp::Ordering::Less,
            (
                BackpressureAmount::Duration(a),
                BackpressureAmount::Duration(b),
            ) => a.cmp(b),
        }
    }
}

impl std::cmp::PartialOrd for BackpressureAmount {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl BackpressureAmount {
    /// Converts to a delay in microseconds
    ///
    /// The saturated case is marked by `u64::MAX`
    pub fn as_micros(&self) -> u64 {
        match self {
            BackpressureAmount::Duration(t) => t.as_micros() as u64,
            BackpressureAmount::Saturated => u64::MAX,
        }
    }
}

/// Helper `struct` to store a shared backpressure amount
///
/// Under the hood, this stores a value in microseconds in an `AtomicU64`, so it
/// can be read and written atomically.  `BackpressureAmount::Saturated` is
/// indicated by `u64::MAX`.
#[derive(Clone, Debug)]
pub struct SharedBackpressureAmount(Arc<AtomicU64>);

impl SharedBackpressureAmount {
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    pub fn store(&self, b: BackpressureAmount) {
        let v = match b {
            BackpressureAmount::Duration(d) => d.as_micros() as u64,
            BackpressureAmount::Saturated => u64::MAX,
        };
        self.0.store(v, Ordering::Relaxed);
    }

    pub fn load(&self) -> BackpressureAmount {
        match self.0.load(Ordering::Relaxed) {
            u64::MAX => BackpressureAmount::Saturated,
            delay_us => {
                BackpressureAmount::Duration(Duration::from_micros(delay_us))
            }
        }
    }
}

impl BackpressureChannelConfig {
    fn get_backpressure(&self, value: u64) -> BackpressureAmount {
        // Return a special value if we're saturated
        if value >= self.max_value {
            return BackpressureAmount::Saturated;
        }

        // This ratio starts at 0 (at start_value) and hits 1 when backpressure
        // should be maxed out.
        let frac = value.saturating_sub(self.start_value) as f64
            / (self.max_value - self.start_value) as f64;

        let v = if frac < 0.5 {
            frac * 2.0
        } else {
            1.0 / (2.0 * (1.0 - frac))
        };

        BackpressureAmount::Duration(
            self.delay_scale.mul_f64(v.powi(2)).min(self.delay_max),
        )
    }
}

impl BackpressureConfig {
    pub fn get_backpressure(
        &self,
        bytes: u64,
        jobs: u64,
    ) -> BackpressureAmount {
        let bp_bytes = self.bytes.get_backpressure(bytes);
        let bp_queue = self.queue.get_backpressure(jobs);
        bp_bytes.max(bp_queue)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Confirm that replaying the max number of jobs / bytes will not saturate
    #[test]
    fn check_outstanding_backpressure() {
        let cfg = BackpressureConfig::default();
        let BackpressureAmount::Duration(_) =
            cfg.get_backpressure(IO_OUTSTANDING_MAX_BYTES, 0)
        else {
            panic!("backpressure saturated")
        };

        let BackpressureAmount::Duration(_) =
            cfg.get_backpressure(0, IO_OUTSTANDING_MAX_JOBS as u64)
        else {
            panic!("backpressure saturated")
        };
    }

    #[test]
    fn check_max_backpressure() {
        let cfg = BackpressureConfig::default();
        let BackpressureAmount::Duration(timeout) = cfg
            .get_backpressure(IO_OUTSTANDING_MAX_BYTES * 2 - 1024u64.pow(2), 0)
        else {
            panic!("backpressure saturated")
        };
        println!(
            "max byte-based delay: {}",
            humantime::format_duration(timeout)
        );
        assert!(
            timeout >= Duration::from_secs(30),
            "max byte-based backpressure delay is too low;
            expected >= 30 secs, got {}",
            humantime::format_duration(timeout)
        );

        let BackpressureAmount::Duration(timeout) =
            cfg.get_backpressure(0, IO_OUTSTANDING_MAX_JOBS as u64 * 2 - 1)
        else {
            panic!("backpressure saturated")
        };
        println!(
            "max job-based delay: {}",
            humantime::format_duration(timeout)
        );
        assert!(
            timeout >= Duration::from_secs(30),
            "max job-based backpressure delay is too low;
            expected >= 30 secs, got {}",
            humantime::format_duration(timeout)
        );
    }

    #[test]
    fn check_saturated_backpressure() {
        let cfg = BackpressureConfig::default();
        assert!(matches!(
            cfg.get_backpressure(IO_OUTSTANDING_MAX_BYTES * 2 + 1, 0),
            BackpressureAmount::Saturated
        ));
        assert!(matches!(
            cfg.get_backpressure(IO_OUTSTANDING_MAX_BYTES * 2, 0),
            BackpressureAmount::Saturated
        ));

        assert!(matches!(
            cfg.get_backpressure(0, IO_OUTSTANDING_MAX_JOBS as u64 * 2 + 1),
            BackpressureAmount::Saturated
        ));
        assert!(matches!(
            cfg.get_backpressure(0, IO_OUTSTANDING_MAX_JOBS as u64 * 2),
            BackpressureAmount::Saturated
        ));
    }
}
