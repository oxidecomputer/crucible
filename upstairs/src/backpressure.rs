// Copyright 2024 Oxide Computer Company

use crate::{
    ClientId, DownstairsIO, IOop, IO_OUTSTANDING_MAX_BYTES,
    IO_OUTSTANDING_MAX_JOBS,
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

/// Helper struct to contain a count of backpressure bytes
#[derive(Debug)]
pub struct BackpressureBytes(u64);

impl BackpressureBytes {
    pub fn new() -> Self {
        BackpressureBytes(0)
    }

    pub fn get(&self) -> u64 {
        self.0
    }

    /// Ensures that the given `DownstairsIO` is counted for backpressure
    ///
    /// This is idempotent: if the job has already been counted, indicated by
    /// the `DownstairsIO::backpressure_bytes[c]` member being `Some(..)`, it
    /// will not be counted again.
    pub fn increment(&mut self, io: &mut DownstairsIO, c: ClientId) {
        match &io.work {
            IOop::Write { data, .. } | IOop::WriteUnwritten { data, .. } => {
                if !io.backpressure_bytes.contains(&c) {
                    let n = data.len() as u64;
                    io.backpressure_bytes.insert(c, n);
                    self.0 += n;
                }
            }
            _ => (),
        };
    }

    /// Remove the given job's contribution to backpressure
    ///
    /// This is idempotent: `DownstairsIO::backpressure_bytes[c]` is set to
    /// `None` by this function call, so it's harmless to call repeatedly.
    pub fn decrement(&mut self, io: &mut DownstairsIO, c: ClientId) {
        if let Some(n) = io.backpressure_bytes.take(&c) {
            self.0 = self.0.checked_sub(n).unwrap();
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

        // This ratio start at 0 (at start_value) and hits 1 when backpressure
        // should be maxed out.
        let frac = value.saturating_sub(self.start_value) as f64
            / (self.max_value - self.start_value) as f64;

        let v = if frac < 0.5 {
            frac * 2.0
        } else {
            1.0 / (2.0 * (1.0 - frac))
        };

        BackpressureAmount::Duration(
            self.delay_scale.mul_f64(v).min(self.delay_max),
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
