// Copyright 2025 Oxide Computer Company

//! Common types and utilities shared between cmon and ctop

use crucible::DtraceInfo;
use serde::Deserialize;
use std::fmt;
use strum_macros::EnumIter;

/// Wrapper for DTrace output with PID
#[derive(Debug, Deserialize)]
pub struct DtraceWrapper {
    pub pid: u32,
    pub status: DtraceInfo,
}

/// The possible fields we will display when receiving DTrace output.
#[derive(Debug, Copy, Clone, PartialEq, Eq, EnumIter)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum DtraceDisplay {
    Pid,
    Session,
    UpstairsId,
    State,
    IoCount,
    IoSummary,
    UpCount,
    DsCount,
    Reconcile,
    DsReconciled,
    DsReconcileNeeded,
    LiveRepair,
    Connected,
    Replaced,
    ExtentLiveRepair,
    ExtentLimit,
    NextJobId,
    JobDelta,
    DsDelay,
    WriteBytesOut,
    RoLrSkipped,
    DsIoInProgress,
    DsIoDone,
    DsIoSkipped,
    DsIoError,
}

impl fmt::Display for DtraceDisplay {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DtraceDisplay::Pid => write!(f, "pid"),
            DtraceDisplay::Session => write!(f, "session"),
            DtraceDisplay::UpstairsId => write!(f, "upstairs_id"),
            DtraceDisplay::State => write!(f, "state"),
            DtraceDisplay::IoCount => write!(f, "io_count"),
            DtraceDisplay::IoSummary => write!(f, "io_summary"),
            DtraceDisplay::UpCount => write!(f, "up_count"),
            DtraceDisplay::DsCount => write!(f, "ds_count"),
            DtraceDisplay::Reconcile => write!(f, "reconcile"),
            DtraceDisplay::DsReconciled => write!(f, "ds_reconciled"),
            DtraceDisplay::DsReconcileNeeded => {
                write!(f, "ds_reconcile_needed")
            }
            DtraceDisplay::LiveRepair => write!(f, "live_repair"),
            DtraceDisplay::Connected => write!(f, "connected"),
            DtraceDisplay::Replaced => write!(f, "replaced"),
            DtraceDisplay::ExtentLiveRepair => write!(f, "extent_live_repair"),
            DtraceDisplay::ExtentLimit => write!(f, "extent_under_repair"),
            DtraceDisplay::NextJobId => write!(f, "next_job_id"),
            DtraceDisplay::JobDelta => write!(f, "job_delta"),
            DtraceDisplay::DsDelay => write!(f, "ds_delay"),
            DtraceDisplay::WriteBytesOut => write!(f, "write_bytes_out"),
            DtraceDisplay::RoLrSkipped => write!(f, "ro_lr_skipped"),
            DtraceDisplay::DsIoInProgress => write!(f, "ds_io_in_progress"),
            DtraceDisplay::DsIoDone => write!(f, "ds_io_done"),
            DtraceDisplay::DsIoSkipped => write!(f, "ds_io_skipped"),
            DtraceDisplay::DsIoError => write!(f, "ds_io_error"),
        }
    }
}

/// Translate DsState string into a three letter abbreviation
pub fn short_state(dss: &str) -> String {
    match dss {
        "Active" => "ACT".to_string(),
        "WaitQuorum" => "WQ".to_string(),
        "Reconcile" => "REC".to_string(),
        "LiveRepairReady" => "LRR".to_string(),
        "New" => "NEW".to_string(),
        "Faulted" => "FLT".to_string(),
        "Offline" => "OFL".to_string(),
        "Replaced" => "RPL".to_string(),
        "LiveRepair" => "LR".to_string(),
        "Replacing" => "RPC".to_string(),
        "Disabled" => "DIS".to_string(),
        "Deactivated" => "DAV".to_string(),
        "NegotiationFailed" => "NF".to_string(),
        "Fault" => "FLT".to_string(),
        x => x.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strum::IntoEnumIterator;

    #[test]
    fn test_short_state_all_known_states() {
        // Test all known downstairs states
        assert_eq!(short_state("Active"), "ACT");
        assert_eq!(short_state("WaitQuorum"), "WQ");
        assert_eq!(short_state("Reconcile"), "REC");
        assert_eq!(short_state("LiveRepairReady"), "LRR");
        assert_eq!(short_state("New"), "NEW");
        assert_eq!(short_state("Faulted"), "FLT");
        assert_eq!(short_state("Offline"), "OFL");
        assert_eq!(short_state("Replaced"), "RPL");
        assert_eq!(short_state("LiveRepair"), "LR");
        assert_eq!(short_state("Replacing"), "RPC");
        assert_eq!(short_state("Disabled"), "DIS");
        assert_eq!(short_state("Deactivated"), "DAV");
        assert_eq!(short_state("NegotiationFailed"), "NF");
        assert_eq!(short_state("Fault"), "FLT");
    }

    #[test]
    fn test_short_state_unknown_state() {
        // Unknown states should pass through unchanged
        assert_eq!(short_state("UnknownState"), "UnknownState");
        assert_eq!(short_state(""), "");
        assert_eq!(short_state("XYZ"), "XYZ");
    }

    #[test]
    fn test_short_state_length() {
        // All known states should produce 2-3 character abbreviations
        let known_states = vec![
            "Active",
            "WaitQuorum",
            "Reconcile",
            "LiveRepairReady",
            "New",
            "Faulted",
            "Offline",
            "Replaced",
            "LiveRepair",
            "Replacing",
            "Disabled",
            "Deactivated",
            "NegotiationFailed",
            "Fault",
        ];

        for state in known_states {
            let short = short_state(state);
            assert!(
                short.len() <= 3,
                "State {} abbreviation '{}' is too long",
                state,
                short
            );
        }
    }

    #[test]
    fn test_dtrace_display_to_string() {
        // Test that Display trait works for all variants
        assert_eq!(DtraceDisplay::Pid.to_string(), "pid");
        assert_eq!(DtraceDisplay::Session.to_string(), "session");
        assert_eq!(DtraceDisplay::UpstairsId.to_string(), "upstairs_id");
        assert_eq!(DtraceDisplay::State.to_string(), "state");
        assert_eq!(DtraceDisplay::IoCount.to_string(), "io_count");
        assert_eq!(DtraceDisplay::IoSummary.to_string(), "io_summary");
        assert_eq!(DtraceDisplay::NextJobId.to_string(), "next_job_id");
        assert_eq!(DtraceDisplay::JobDelta.to_string(), "job_delta");
        assert_eq!(
            DtraceDisplay::ExtentLimit.to_string(),
            "extent_under_repair"
        );
    }

    #[test]
    fn test_dtrace_display_all_variants_have_display() {
        // Ensure all enum variants can be displayed without panicking
        for variant in DtraceDisplay::iter() {
            let display = variant.to_string();
            // Should produce a non-empty string
            assert!(
                !display.is_empty(),
                "Variant {:?} has empty display",
                variant
            );
            // Should be lowercase with underscores
            assert!(
                display.chars().all(|c| c.is_lowercase() || c == '_'),
                "Variant {:?} display '{}' should be lowercase with underscores",
                variant,
                display
            );
        }
    }

    #[test]
    fn test_dtrace_display_copy_clone() {
        // Verify Copy and Clone work correctly
        let display = DtraceDisplay::Pid;
        let copied = display;
        let cloned = display.clone();

        assert_eq!(format!("{:?}", display), format!("{:?}", copied));
        assert_eq!(format!("{:?}", display), format!("{:?}", cloned));
    }

    #[test]
    fn test_dtrace_display_enum_count() {
        // Verify we have the expected number of variants
        let count = DtraceDisplay::iter().count();
        assert_eq!(
            count, 25,
            "Expected 25 DtraceDisplay variants, found {}",
            count
        );
    }
}
