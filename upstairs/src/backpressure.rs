// Copyright 2024 Oxide Computer Company

use crate::{
    ClientId, DownstairsIO, IOop, IO_OUTSTANDING_MAX_BYTES,
    IO_OUTSTANDING_MAX_JOBS,
};
use std::time::Duration;

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
        // `max` values below must be higher than `IO_OUTSTANDING_MAX_*`;
        // otherwise, replaying jobs to a previously-Offline Downstairs could
        // immediately kick us into the infinite-backpressure regine, which
        // would be unfortunate.
        BackpressureConfig {
            // Byte-based backpressure
            bytes: BackpressureChannelConfig {
                start: 4 * 1024u64.pow(2), // 4 MiB
                max: IO_OUTSTANDING_MAX_BYTES * 2,
                scale: Duration::from_millis(50),
            },

            // Queue-based backpressure
            queue: BackpressureChannelConfig {
                start: 50,
                max: IO_OUTSTANDING_MAX_JOBS as u64 * 2,
                scale: Duration::from_millis(5),
            },
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct BackpressureChannelConfig {
    /// When should backpressure start
    pub start: u64,
    /// Value at which backpressure goes to infinity
    pub max: u64,
    /// Scale of backpressure
    pub scale: Duration,
}

impl BackpressureChannelConfig {
    fn get_backpressure(&self, value: u64) -> Duration {
        // Saturate at 1 hour per job, which is basically infinite
        if value >= self.max {
            return Duration::from_secs(60 * 60);
        }

        // These ratios start at 0 (at *_start) and hit 1 when backpressure
        // should be infinite.
        let frac = value.saturating_sub(self.start) as f64
            / (self.max - self.start) as f64;

        // Delay should be 0 at frac = 0, and infinite at frac = 1
        let frac = frac * 2.0;
        let v = if frac < 1.0 {
            frac
        } else {
            1.0 / (1.0 - (frac - 1.0))
        };
        self.scale.mul_f64(v)
    }
}

impl BackpressureConfig {
    pub fn get_backpressure_us(&self, bytes: u64, jobs: u64) -> u64 {
        let bp_bytes = self.bytes.get_backpressure(bytes).as_micros() as u64;
        let bp_queue = self.queue.get_backpressure(jobs).as_micros() as u64;
        bp_bytes.max(bp_queue)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn check_max_backpressure() {
        let cfg = BackpressureConfig::default();
        let t = cfg.get_backpressure_us(
            IO_OUTSTANDING_MAX_BYTES * 2 - 1024u64.pow(2),
            0,
        );
        let timeout = Duration::from_micros(t);
        println!(
            "max byte-based delay: {}",
            humantime::format_duration(timeout)
        );
        assert!(
            timeout > Duration::from_secs(60 * 60),
            "max byte-based backpressure delay is too low;
            expected > 1 hr, got {}",
            humantime::format_duration(timeout)
        );

        let t =
            cfg.get_backpressure_us(0, IO_OUTSTANDING_MAX_JOBS as u64 * 2 - 1);
        let timeout = Duration::from_micros(t);
        println!(
            "max job-based delay: {}",
            humantime::format_duration(timeout)
        );
        assert!(
            timeout > Duration::from_secs(60 * 60),
            "max job-based backpressure delay is too low;
            expected > 1 hr, got {}",
            humantime::format_duration(timeout)
        );
    }
}
