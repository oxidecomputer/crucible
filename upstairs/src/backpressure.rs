// Copyright 2023 Oxide Computer Company

use crate::{ClientId, DownstairsIO, IOop};

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
