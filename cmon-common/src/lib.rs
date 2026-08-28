// Copyright 2026 Oxide Computer Company

//! Common types and utilities shared between cmon and ctop

use crucible::{ClientId, DtraceInfo};
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

/// The fields both cmon and ctop display when none are requested.
pub fn default_display_fields() -> Vec<DtraceDisplay> {
    vec![
        DtraceDisplay::Pid,
        DtraceDisplay::Session,
        DtraceDisplay::State,
        DtraceDisplay::NextJobId,
        DtraceDisplay::JobDelta,
        DtraceDisplay::ExtentLimit,
        DtraceDisplay::DsReconciled,
        DtraceDisplay::DsReconcileNeeded,
    ]
}

/// Build the column header line for the given display fields.
///
/// Every arm here must produce the same width as the matching arm in
/// [`format_row`], or the columns will not line up.  Neither function
/// emits a leading indicator column: a caller that prefixes rows with a
/// selection marker (as ctop does) is responsible for printing the same
/// amount of padding ahead of this header.
pub fn format_header(dd: &[DtraceDisplay]) -> String {
    let mut result = String::new();
    for display_item in dd.iter() {
        match display_item {
            DtraceDisplay::Pid => {
                result.push_str(&format!(" {:>5}", "PID"));
            }
            DtraceDisplay::Session => {
                result.push_str(&format!(" {:>8}", "SESSION"));
            }
            DtraceDisplay::UpstairsId => {
                result.push_str(&format!(" {:>8}", "UPSTAIRS"));
            }
            DtraceDisplay::State => {
                result.push_str(&format!(
                    " {:>3} {:>3} {:>3}",
                    "DS0", "DS1", "DS2"
                ));
            }
            DtraceDisplay::UpCount => {
                result.push_str(&format!(" {:>3}", "UPW"));
            }
            DtraceDisplay::DsCount => {
                result.push_str(&format!(" {:>5}", "DSW"));
            }
            DtraceDisplay::IoCount | DtraceDisplay::IoSummary => {
                result.push_str(&format!(
                    " {:>5} {:>5} {:>5}",
                    "IP0", "IP1", "IP2"
                ));
                result
                    .push_str(&format!(" {:>5} {:>5} {:>5}", "D0", "D1", "D2"));
                result
                    .push_str(&format!(" {:>5} {:>5} {:>5}", "S0", "S1", "S2"));

                if matches!(display_item, DtraceDisplay::IoCount) {
                    result.push_str(&format!(
                        " {:>4} {:>4} {:>4}",
                        "E0", "E1", "E2"
                    ));
                }
            }
            DtraceDisplay::Reconcile => {
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    "REC", "NREC", "AREC"
                ));
            }
            DtraceDisplay::DsReconciled => {
                result.push_str(&format!(" {:>4}", "RECD"));
            }
            DtraceDisplay::DsReconcileNeeded => {
                result.push_str(&format!(" {:>4}", "RECN"));
            }
            DtraceDisplay::LiveRepair => {
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    "LRC0", "LRC1", "LRC2"
                ));
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    "LRA0", "LRA1", "LRA2"
                ));
            }
            DtraceDisplay::Connected => {
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    "CON0", "CON1", "CON2"
                ));
            }
            DtraceDisplay::Replaced => {
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    "RPL0", "RPL1", "RPL2"
                ));
            }
            DtraceDisplay::ExtentLiveRepair => {
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    "EXR0", "EXR1", "EXR2"
                ));
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    "EXC0", "EXC1", "EXC2"
                ));
            }
            DtraceDisplay::ExtentLimit => {
                result.push_str(&format!(" {:>4}", "EXTL"));
            }
            DtraceDisplay::NextJobId => {
                result.push_str(&format!(" {:>10}", "NEXTJOB"));
            }
            DtraceDisplay::JobDelta => {
                result.push_str(&format!(" {:>5}", "DELTA"));
            }
            DtraceDisplay::DsDelay => {
                result.push_str(&format!(
                    " {:>5} {:>5} {:>5}",
                    "DLY0", "DLY1", "DLY2"
                ));
            }
            DtraceDisplay::WriteBytesOut => {
                result.push_str(&format!(" {:>10}", "WRBYTES"));
            }
            DtraceDisplay::RoLrSkipped => {
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    "RLS0", "RLS1", "RLS2"
                ));
            }
            DtraceDisplay::DsIoInProgress => {
                result.push_str(&format!(
                    " {:>5} {:>5} {:>5}",
                    "IP0", "IP1", "IP2"
                ));
            }
            DtraceDisplay::DsIoDone => {
                result
                    .push_str(&format!(" {:>5} {:>5} {:>5}", "D0", "D1", "D2"));
            }
            DtraceDisplay::DsIoSkipped => {
                result
                    .push_str(&format!(" {:>5} {:>5} {:>5}", "S0", "S1", "S2"));
            }
            DtraceDisplay::DsIoError => {
                result
                    .push_str(&format!(" {:>4} {:>4} {:>4}", "E0", "E1", "E2"));
            }
        }
    }
    result
}

/// Build a single data row for the given display fields.
///
/// `delta` is the job delta computed by the caller, which is the only
/// value that cannot be derived from `d_out` alone.  `None` prints a
/// placeholder, which is what the first sample of a session gets.
///
/// Column widths here must match [`format_header`].
pub fn format_row(
    pid: u32,
    d_out: &DtraceInfo,
    delta: Option<u64>,
    dd: &[DtraceDisplay],
) -> String {
    let mut result = String::new();

    for display_item in dd.iter() {
        match display_item {
            DtraceDisplay::Pid => {
                result.push_str(&format!(" {pid:>5}"));
            }
            DtraceDisplay::Session => {
                let session_short =
                    d_out.session_id.chars().take(8).collect::<String>();
                result.push_str(&format!(" {session_short:>8}"));
            }
            DtraceDisplay::UpstairsId => {
                let upstairs_short =
                    d_out.upstairs_id.chars().take(8).collect::<String>();
                result.push_str(&format!(" {upstairs_short:>8}"));
            }
            DtraceDisplay::State => {
                result.push_str(&format!(
                    " {:>3} {:>3} {:>3}",
                    short_state(&d_out.ds_state[0]),
                    short_state(&d_out.ds_state[1]),
                    short_state(&d_out.ds_state[2]),
                ));
            }
            DtraceDisplay::UpCount => {
                result.push_str(&format!(" {:>3}", d_out.up_count));
            }
            DtraceDisplay::DsCount => {
                result.push_str(&format!(" {:>5}", d_out.ds_count));
            }
            DtraceDisplay::IoCount | DtraceDisplay::IoSummary => {
                result.push_str(&format!(
                    " {:>5} {:>5} {:>5}",
                    d_out.ds_io_count.in_progress[ClientId::new(0)],
                    d_out.ds_io_count.in_progress[ClientId::new(1)],
                    d_out.ds_io_count.in_progress[ClientId::new(2)],
                ));
                result.push_str(&format!(
                    " {:>5} {:>5} {:>5}",
                    d_out.ds_io_count.done[ClientId::new(0)],
                    d_out.ds_io_count.done[ClientId::new(1)],
                    d_out.ds_io_count.done[ClientId::new(2)],
                ));
                result.push_str(&format!(
                    " {:>5} {:>5} {:>5}",
                    d_out.ds_io_count.skipped[ClientId::new(0)],
                    d_out.ds_io_count.skipped[ClientId::new(1)],
                    d_out.ds_io_count.skipped[ClientId::new(2)],
                ));
                if matches!(display_item, DtraceDisplay::IoCount) {
                    result.push_str(&format!(
                        " {:>4} {:>4} {:>4}",
                        d_out.ds_io_count.error[ClientId::new(0)],
                        d_out.ds_io_count.error[ClientId::new(1)],
                        d_out.ds_io_count.error[ClientId::new(2)],
                    ));
                }
            }
            DtraceDisplay::Reconcile => {
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    d_out.ds_reconciled,
                    d_out.ds_reconcile_needed,
                    d_out.ds_reconcile_aborted,
                ));
            }
            DtraceDisplay::DsReconciled => {
                result.push_str(&format!(" {:>4}", d_out.ds_reconciled));
            }
            DtraceDisplay::DsReconcileNeeded => {
                result.push_str(&format!(" {:>4}", d_out.ds_reconcile_needed));
            }
            DtraceDisplay::LiveRepair => {
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    d_out.ds_live_repair_completed[0],
                    d_out.ds_live_repair_completed[1],
                    d_out.ds_live_repair_completed[2],
                ));
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    d_out.ds_live_repair_aborted[0],
                    d_out.ds_live_repair_aborted[1],
                    d_out.ds_live_repair_aborted[2],
                ));
            }
            DtraceDisplay::Connected => {
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    d_out.ds_connected[0],
                    d_out.ds_connected[1],
                    d_out.ds_connected[2],
                ));
            }
            DtraceDisplay::Replaced => {
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    d_out.ds_replaced[0],
                    d_out.ds_replaced[1],
                    d_out.ds_replaced[2],
                ));
            }
            DtraceDisplay::ExtentLiveRepair => {
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    d_out.ds_extents_repaired[0],
                    d_out.ds_extents_repaired[1],
                    d_out.ds_extents_repaired[2],
                ));
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    d_out.ds_extents_confirmed[0],
                    d_out.ds_extents_confirmed[1],
                    d_out.ds_extents_confirmed[2],
                ));
            }
            DtraceDisplay::ExtentLimit => {
                result.push_str(&format!(" {:>4}", d_out.ds_extent_limit));
            }
            DtraceDisplay::NextJobId => {
                result.push_str(&format!(" {:>10}", d_out.next_job_id));
            }
            DtraceDisplay::JobDelta => match delta {
                Some(delta) => result.push_str(&format!(" {delta:>5}")),
                None => result.push_str(&format!(" {:>5}", "---")),
            },
            DtraceDisplay::DsDelay => {
                result.push_str(&format!(
                    " {:>5} {:>5} {:>5}",
                    d_out.ds_delay_us[0],
                    d_out.ds_delay_us[1],
                    d_out.ds_delay_us[2],
                ));
            }
            DtraceDisplay::WriteBytesOut => {
                result.push_str(&format!(" {:>10}", d_out.write_bytes_out));
            }
            DtraceDisplay::RoLrSkipped => {
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    d_out.ds_ro_lr_skipped[0],
                    d_out.ds_ro_lr_skipped[1],
                    d_out.ds_ro_lr_skipped[2],
                ));
            }
            DtraceDisplay::DsIoInProgress => {
                result.push_str(&format!(
                    " {:>5} {:>5} {:>5}",
                    d_out.ds_io_count.in_progress[ClientId::new(0)],
                    d_out.ds_io_count.in_progress[ClientId::new(1)],
                    d_out.ds_io_count.in_progress[ClientId::new(2)],
                ));
            }
            DtraceDisplay::DsIoDone => {
                result.push_str(&format!(
                    " {:>5} {:>5} {:>5}",
                    d_out.ds_io_count.done[ClientId::new(0)],
                    d_out.ds_io_count.done[ClientId::new(1)],
                    d_out.ds_io_count.done[ClientId::new(2)],
                ));
            }
            DtraceDisplay::DsIoSkipped => {
                result.push_str(&format!(
                    " {:>5} {:>5} {:>5}",
                    d_out.ds_io_count.skipped[ClientId::new(0)],
                    d_out.ds_io_count.skipped[ClientId::new(1)],
                    d_out.ds_io_count.skipped[ClientId::new(2)],
                ));
            }
            DtraceDisplay::DsIoError => {
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    d_out.ds_io_count.error[ClientId::new(0)],
                    d_out.ds_io_count.error[ClientId::new(1)],
                    d_out.ds_io_count.error[ClientId::new(2)],
                ));
            }
        }
    }
    result
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

    /// The State column is three characters wide, so every
    /// abbreviation has to fit in it.
    #[test]
    fn test_short_state_length() {
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

    /// A DtraceInfo with small values in every field, so that no value
    /// is wider than the column that holds it.
    fn sample_dtrace_info() -> DtraceInfo {
        let json = r#"{
            "upstairs_id": "12345678-1111-2222-3333-444444444444",
            "session_id": "87654321-1111-2222-3333-444444444444",
            "up_count": 1,
            "up_counters": {
                "apply": 1, "action_downstairs": 1, "action_guest": 1,
                "action_deferred_block": 0, "action_deferred_message": 0,
                "action_flush_check": 0, "action_stat_check": 0,
                "action_control_check": 0, "action_noop": 0
            },
            "next_job_id": 1000,
            "ds_count": 3,
            "write_bytes_out": 1,
            "ds_state": ["Active", "Active", "Active"],
            "ds_io_count": {
                "in_progress": [1, 2, 3], "done": [4, 5, 6],
                "skipped": [0, 0, 0], "error": [0, 0, 0]
            },
            "ds_reconciled": 0,
            "ds_reconcile_needed": 0,
            "ds_reconcile_aborted": 0,
            "ds_live_repair_completed": [0, 0, 0],
            "ds_live_repair_aborted": [0, 0, 0],
            "ds_connected": [1, 1, 1],
            "ds_replaced": [0, 0, 0],
            "ds_extents_repaired": [0, 0, 0],
            "ds_extents_confirmed": [0, 0, 0],
            "ds_extent_limit": 0,
            "ds_delay_us": [0, 0, 0],
            "ds_ro_lr_skipped": [0, 0, 0]
        }"#;
        serde_json::from_str(json).unwrap()
    }

    /// format_header and format_row must agree on the width of every
    /// field, or the columns silently stop lining up.  Check each
    /// variant on its own so a failure names the field that drifted.
    #[test]
    fn test_header_and_row_widths_match() {
        let info = sample_dtrace_info();

        for variant in DtraceDisplay::iter() {
            let header = format_header(&[variant]);
            let row = format_row(1234, &info, Some(0), &[variant]);
            assert_eq!(
                header.chars().count(),
                row.chars().count(),
                "{variant:?}: header {header:?} and row {row:?} differ \
                 in width",
            );
        }
    }

    /// The job delta is the one column whose value the caller supplies,
    /// and a session's first sample has no delta to report.
    #[test]
    fn test_format_row_missing_delta_keeps_width() {
        let info = sample_dtrace_info();
        let fields = [DtraceDisplay::JobDelta];

        let with = format_row(1234, &info, Some(42), &fields);
        let without = format_row(1234, &info, None, &fields);

        assert!(with.contains("42"));
        assert!(without.contains("---"));
        assert_eq!(with.chars().count(), without.chars().count());
    }

    /// The default field set is what both cmon and ctop show, and ctop
    /// pads its header by one to leave room for the row indicator.
    #[test]
    fn test_default_display_fields_align() {
        let info = sample_dtrace_info();
        let fields = default_display_fields();

        assert_eq!(
            format_header(&fields).chars().count(),
            format_row(1234, &info, Some(0), &fields).chars().count(),
        );
    }
}
