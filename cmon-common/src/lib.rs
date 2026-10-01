// Copyright 2026 Oxide Computer Company

//! Formatting for the upstairs `up-status` DTrace probe.
//!
//! The probe delivers a [`DtraceInfo`], which has far more in it than
//! fits across a terminal, so a caller picks the fields it wants with
//! [`DtraceDisplay`] and gets back a header line and one line per
//! record.

use clap::ValueEnum;
use crucible::{ClientId, DtraceInfo};
use serde::Deserialize;
use std::fmt;
use strum_macros::EnumIter;

/// One line of output from the raw dtrace script.
///
/// The probe itself only knows about the upstairs that fired it, so the
/// script wraps each record with the pid of the process it came from.
/// The script matches every upstairs on the system, so records from
/// different processes arrive interleaved and the pid, along with the
/// `session_id` inside the status allows us to match prior records.
#[derive(Debug, Deserialize)]
pub struct DtraceWrapper {
    pub pid: u32,
    pub status: DtraceInfo,
}

/// The possible fields we will display when receiving DTrace output.
#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum, EnumIter)]
pub enum DtraceDisplay {
    Pid,
    SessionId,
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

/// The fields shown when none are asked for.
///
/// Enough to see what every upstairs on the system is doing without
/// running off the side of an eighty column terminal: who it is, what
/// its downstairs are up to, how fast work is being issued, and
/// whether it is repairing anything.
pub fn default_display_fields() -> Vec<DtraceDisplay> {
    vec![
        DtraceDisplay::Pid,
        DtraceDisplay::SessionId,
        DtraceDisplay::State,
        DtraceDisplay::NextJobId,
        DtraceDisplay::JobDelta,
        DtraceDisplay::ExtentLimit,
        DtraceDisplay::DsReconciled,
        DtraceDisplay::DsReconcileNeeded,
    ]
}

/// Print the name `-o` accepts for this field.
///
/// Delegating to clap rather than writing the names out means dtrace-decode
/// will always print the correct options.
impl fmt::Display for DtraceDisplay {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // No variant is #[value(skip)], which is the only way this
        // returns None.
        self.to_possible_value()
            .expect("every variant has a clap value")
            .get_name()
            .fmt(f)
    }
}

/// Translate what the default DsState string is (that we are getting from
/// DTrace) into a three letter string for printing.
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

/// Build the column header line for the given display fields.
///
/// Every arm here must produce the same width as the matching arm in
/// [`format_row`], or the columns will not line up.  Neither function
/// emits a trailing newline.
pub fn format_header(dd: &[DtraceDisplay]) -> String {
    let mut result = String::new();
    for display_item in dd.iter() {
        match display_item {
            DtraceDisplay::Pid => {
                result.push_str(&format!(" {:>5}", "PID"));
            }
            DtraceDisplay::SessionId => {
                result.push_str(&format!(" {:>8}", "SESSION"));
            }
            DtraceDisplay::UpstairsId => {
                result.push_str(&format!(" {:>8}", "UPSTAIRS"));
            }
            DtraceDisplay::State => {
                result.push_str(&format!(
                    " {:>3} {:>3} {:>3}",
                    "DS0", "DS1", "DS2",
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
            // Job IDs run to seven digits on a long lived upstairs and
            // eight is not far off, so give the column room rather
            // than letting the number push the rest of the row over.
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
/// `pid` and `delta` are the two values that cannot be derived from
/// `d_out` alone, so the caller supplies them.  The pid arrives
/// alongside the status in a [`DtraceWrapper`]; the delta is how far
/// `next_job_id` moved since this session's previous record, and is
/// `None` for a session's first record.
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
            // The ids are UUIDs, which are far too wide to put in a
            // table.  The leading characters are enough to tell the
            // sessions on one machine apart.
            DtraceDisplay::SessionId => {
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

    /// A state we have no abbreviation for is passed through, so a new
    /// DsState shows up in the output rather than disappearing.
    #[test]
    fn test_short_state_unknown_state() {
        assert_eq!(short_state("UnknownState"), "UnknownState");
        assert_eq!(short_state(""), "");
        assert_eq!(short_state("XYZ"), "XYZ");
    }

    /// The State column is three characters wide, so every
    /// abbreviation has to fit in it.
    #[test]
    fn test_short_state_length() {
        let known_states = [
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
                "State {state} abbreviation '{short}' is too long",
            );
        }
    }

    /// These strings are both what `dtrace-decode` prints and what
    /// `-o` accepts, so they are part of the command line.
    #[test]
    fn test_dtrace_display_to_string() {
        assert_eq!(DtraceDisplay::State.to_string(), "state");
        assert_eq!(DtraceDisplay::IoCount.to_string(), "io-count");
        assert_eq!(DtraceDisplay::IoSummary.to_string(), "io-summary");
        assert_eq!(DtraceDisplay::NextJobId.to_string(), "next-job-id");
        assert_eq!(DtraceDisplay::JobDelta.to_string(), "job-delta");
        assert_eq!(DtraceDisplay::ExtentLimit.to_string(), "extent-limit");
    }

    /// Every label `dtrace-decode` prints has to round trip back
    /// through `-o`, or the subcommand is telling you to type something
    /// that does not work.
    #[test]
    fn test_display_round_trips_through_value_enum() {
        for variant in DtraceDisplay::iter() {
            let name = variant.to_string();
            assert!(
                !name.is_empty(),
                "Variant {variant:?} has an empty display",
            );

            let parsed = DtraceDisplay::from_str(&name, false);
            assert_eq!(
                parsed,
                Ok(variant),
                "-o {name} does not parse back to {variant:?}",
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

    /// Fields are emitted in the order asked for, and asking for
    /// nothing gets nothing rather than a default set.
    #[test]
    fn test_format_field_order_and_empty() {
        let info = sample_dtrace_info();

        assert_eq!(format_header(&[]), "");
        assert_eq!(format_row(1234, &info, Some(0), &[]), "");

        let forward =
            format_header(&[DtraceDisplay::UpCount, DtraceDisplay::DsCount]);
        let reverse =
            format_header(&[DtraceDisplay::DsCount, DtraceDisplay::UpCount]);
        assert_eq!(forward, " UPW   DSW");
        assert_eq!(reverse, "   DSW UPW");
    }

    /// IoSummary is IoCount without the error columns.
    #[test]
    fn test_io_summary_drops_error_columns() {
        let info = sample_dtrace_info();

        let count = format_row(1234, &info, Some(0), &[DtraceDisplay::IoCount]);
        let summary =
            format_row(1234, &info, Some(0), &[DtraceDisplay::IoSummary]);

        assert!(count.starts_with(&summary));
        assert_eq!(count.chars().count(), summary.chars().count() + 15);
    }

    /// The default set is what you get with no `-o`, so it has to line
    /// up and it has to fit on a terminal.
    #[test]
    fn test_default_display_fields_fit_eighty_columns() {
        let info = sample_dtrace_info();
        let fields = default_display_fields();

        let header = format_header(&fields);
        let row = format_row(1234, &info, Some(0), &fields);

        assert_eq!(header.chars().count(), row.chars().count());
        assert!(
            header.chars().count() <= 80,
            "default header is {} columns wide: {header:?}",
            header.chars().count(),
        );
    }

    /// A session's first record has no delta to report, and the
    /// placeholder has to hold the column open so the rest of the row
    /// does not shift left on that one line.
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

    /// The ids are UUIDs and the columns are eight characters, so they
    /// are truncated rather than allowed to push the table apart.
    #[test]
    fn test_id_fields_are_truncated() {
        let info = sample_dtrace_info();
        let fields = [DtraceDisplay::SessionId, DtraceDisplay::UpstairsId];
        let row = format_row(1234, &info, Some(0), &fields);

        assert!(row.contains("87654321"));
        assert!(row.contains("12345678"));
        assert!(!row.contains("-1111-"));
        assert_eq!(format_header(&fields).chars().count(), row.chars().count(),);
    }

    /// What the dtrace script emits has to land in DtraceWrapper.  This
    /// is the shape of one line of `upstairs_raw.d` output.
    #[test]
    fn test_dtrace_wrapper_parses_script_output() {
        let line = format!(
            r#"{{"pid":12345,"status":{}}}"#,
            serde_json::to_string(&sample_dtrace_info()).unwrap()
        );

        let wrapper: DtraceWrapper = serde_json::from_str(&line).unwrap();

        assert_eq!(wrapper.pid, 12345);
        assert_eq!(
            wrapper.status.session_id,
            "87654321-1111-2222-3333-444444444444"
        );
        assert_eq!(wrapper.status.ds_state[0], "Active");
    }
}
