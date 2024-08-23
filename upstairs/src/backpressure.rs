// Copyright 2024 Oxide Computer Company
//! Backpressure control loop
use crate::cdt;
use std::time::Duration;

/// Configuration for host-side backpressure
///
/// Backpressure adds an artificial delay to host write messages (which are
/// otherwise acked immediately, before actually being complete).  The delay is
/// varied based on two metrics:
///
/// - number of write bytes outstanding
/// - queue length (number of jobs)
#[derive(Copy, Clone, Debug)]
pub struct BackpressureConfig {
    /// Backpressure driven by bytes in flight
    bytes: BackpressureChannelConfig,
    /// Backpressure driven by jobs in the queue
    queue: BackpressureChannelConfig,
}

impl Default for BackpressureConfig {
    fn default() -> BackpressureConfig {
        BackpressureConfig {
            // Byte-based backpressure
            bytes: BackpressureChannelConfig {
                target: 50 * 1024u64.pow(2), // 50 MiB
                p_gain: 5e-4,                // manually tuned
                i_gain: 5e-5,
                d_gain: 0.0,
                clamp: 500_000.0,
            },

            // Queue-based backpressure
            queue: BackpressureChannelConfig {
                target: 500,
                p_gain: 40.0, // manually tuned
                i_gain: 4.0,
                d_gain: 0.0,
                clamp: 50_000.0,
            },
        }
    }
}

/// We can disable backpressure in the test suite, where we often want to
/// schedule a bunch of IO operations at once.
#[cfg(test)]
impl BackpressureConfig {
    pub fn disable(&mut self) {
        self.queue.disable();
        self.bytes.disable();
    }

    pub fn is_disabled(&self) -> bool {
        self.queue.p_gain == 0.0
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct BackpressureChannelConfig {
    /// Target value to which we should drive our count
    ///
    /// We behave as a PID controller in the range `[0, 2*target]`, then switch
    /// to a controller which goes to infinity at `3*target`
    target: u64,

    /// Proportional gain
    p_gain: f64,

    /// Integral gain
    i_gain: f64,

    /// Derivative gain
    d_gain: f64,

    /// Clamping value
    clamp: f64,
}

#[cfg(test)]
impl BackpressureChannelConfig {
    fn disable(&mut self) {
        self.p_gain = 0.0;
        self.i_gain = 0.0;
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct BackpressureChannelState {
    /// Controlled value, offset to be centered at zero
    error: f64,

    /// Current integral accumulator value
    integral: f64,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct BackpressureState {
    bytes: BackpressureChannelState,
    queue: BackpressureChannelState,
}

impl BackpressureState {
    /// Updates the backpressure control loop
    ///
    /// Returns the new backpressure value, in microseconds
    pub fn update_control(
        &mut self,
        bytes: u64,
        jobs: u64,
        cfg: &BackpressureConfig,
        dt: Duration,
    ) -> u64 {
        let bp_bytes = self.bytes.update(bytes, &cfg.bytes, dt);
        let bp_queue = self.queue.update(jobs, &cfg.queue, dt);

        // Reset the integral term for the inactive control regime, to avoid
        // integral windup.
        if bp_queue > bp_bytes {
            self.bytes.reset();
        } else {
            self.queue.reset();
        }

        let out = bp_bytes.max(bp_queue);

        cdt::up__pid!(|| (
            u64::from_ne_bytes(self.bytes.error.to_ne_bytes()),
            u64::from_ne_bytes(self.bytes.integral.to_ne_bytes()),
            u64::from_ne_bytes(self.queue.error.to_ne_bytes()),
            u64::from_ne_bytes(self.queue.integral.to_ne_bytes()),
            out
        ));

        out
    }
}

impl BackpressureChannelState {
    /// Resets the integral accumulator
    fn reset(&mut self) {
        self.integral = 0.0;
    }

    /// Updates the backpressure control loop, returning the new value (in us)
    fn update(
        &mut self,
        v: u64,
        cfg: &BackpressureChannelConfig,
        dt: Duration,
    ) -> u64 {
        let prev_error = self.error;
        self.error = v as f64 - cfg.target as f64;

        let mut p = cfg.p_gain * self.error;

        // Avoid integral windup if we're clamping the P term
        if p < cfg.clamp {
            // Accumulate average error over the timestep
            self.integral += (self.error + prev_error) / 2.0 * dt.as_secs_f64();
        } else {
            p = cfg.clamp;
        }

        let i = cfg.i_gain * self.integral;
        let d = (self.error - prev_error) / dt.as_secs_f64() * cfg.d_gain;

        let v = p + i + d;
        v.max(0.0) as u64
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        upstairs::BACKPRESSURE_CONTROL_SECS, IO_OUTSTANDING_MAX_BYTES,
        IO_OUTSTANDING_MAX_JOBS,
    };

    /// Confirm that the offline timeout is reasonable
    #[test]
    fn check_offline_timeout() {
        for job_size in
            [512, 4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024]
        {
            let mut bytes_in_flight = 0;
            let mut jobs_in_flight = 0;
            let mut time_usec: u64 = 0;
            let cfg = BackpressureConfig::default();

            let (t, desc) = loop {
                let mut state = BackpressureState::default();
                let bp_usec = state.update_control(
                    bytes_in_flight,
                    jobs_in_flight,
                    &cfg,
                    BACKPRESSURE_CONTROL_SECS,
                );
                time_usec = time_usec.saturating_add(bp_usec);

                if bytes_in_flight >= IO_OUTSTANDING_MAX_BYTES {
                    break (time_usec, "bytes");
                }

                if jobs_in_flight >= IO_OUTSTANDING_MAX_JOBS as u64 {
                    break (time_usec, "jobs");
                }

                bytes_in_flight += job_size;
                jobs_in_flight += 1;
            };

            let timeout = Duration::from_micros(t);
            assert!(
                timeout > Duration::from_secs(1),
                "offline -> faulted transition happens too quickly \
                 with job size {job_size};  expected > 1 sec, got {}",
                humantime::format_duration(timeout)
            );
            assert!(
                timeout < Duration::from_secs(180),
                "offline -> faulted transition happens too slowly \
                 with job size {job_size};  expected < 3 mins, got {}",
                humantime::format_duration(timeout)
            );

            println!(
                "job size {job_size:>8}:\n    Timeout in {} ({desc})\n",
                humantime::format_duration(timeout)
            );
        }
    }

    #[test]
    fn check_max_backpressure() {
        let cfg = BackpressureConfig::default();

        let mut state = BackpressureState::default();
        let t = state.update_control(
            IO_OUTSTANDING_MAX_BYTES * 2 - 1024u64.pow(2),
            0,
            &cfg,
            BACKPRESSURE_CONTROL_SECS,
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

        let mut state = BackpressureState::default();
        let t = state.update_control(
            0,
            IO_OUTSTANDING_MAX_JOBS as u64 * 2 - 1,
            &cfg,
            BACKPRESSURE_CONTROL_SECS,
        );
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
