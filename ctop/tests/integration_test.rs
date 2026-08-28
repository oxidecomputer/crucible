// Copyright 2026 Oxide Computer Company

//! Integration tests for ctop
//!
//! These check that the JSON the dtrace scripts emit parses into the
//! types ctop uses.  See TODO.md for the coverage this does not have.

use cmon_common::DtraceWrapper;

/// Test that we can parse valid DTrace JSON output
#[test]
fn test_parse_dtrace_json_format() {
    // This is the JSON format that dtrace scripts output
    // and that ctop needs to parse
    let sample_json = r#"{
        "pid": 12345,
        "status": {
            "upstairs_id": "test-uuid-123",
            "session_id": "session-456",
            "up_count": 10,
            "up_counters": {
                "apply": 100,
                "action_downstairs": 50,
                "action_guest": 30,
                "action_deferred_block": 0,
                "action_deferred_message": 0,
                "action_flush_check": 10,
                "action_stat_check": 5,
                "action_control_check": 3,
                "action_noop": 2
            },
            "next_job_id": 1000,
            "ds_count": 15,
            "write_bytes_out": 10240,
            "ds_state": ["Active", "Active", "Active"],
            "ds_io_count": {
                "in_progress": [5, 5, 5],
                "done": [100, 100, 100],
                "skipped": [0, 0, 0],
                "error": [0, 0, 0]
            },
            "ds_reconciled": 0,
            "ds_reconcile_needed": 0,
            "ds_reconcile_aborted": 0,
            "ds_live_repair_completed": [0, 0, 0],
            "ds_live_repair_aborted": [0, 0, 0],
            "ds_connected": [1, 1, 1],
            "ds_replaced": [0, 0, 0],
            "ds_extents_repaired": [0, 0, 0],
            "ds_extents_confirmed": [100, 100, 100],
            "ds_extent_limit": 0,
            "ds_delay_us": [0, 0, 0],
            "ds_ro_lr_skipped": [0, 0, 0]
        }
    }"#;

    // Parse into the actual DtraceWrapper type used in production
    // This ensures the test catches real compatibility issues
    let result: Result<DtraceWrapper, _> = serde_json::from_str(sample_json);
    assert!(
        result.is_ok(),
        "Sample dtrace JSON should parse into DtraceWrapper: {:?}",
        result.err()
    );

    let parsed = result.unwrap();
    assert_eq!(parsed.pid, 12345);
    assert_eq!(parsed.status.session_id, "session-456");
    assert_eq!(parsed.status.ds_state[0], "Active");
}

/// Test handling of invalid JSON
#[test]
fn test_parse_invalid_dtrace_json() {
    let invalid_json = r#"{ "pid": 12345, "status": invalid }"#;

    let result: Result<serde_json::Value, _> =
        serde_json::from_str(invalid_json);
    assert!(result.is_err(), "Invalid JSON should fail to parse");
}

/// Test handling of partial/incomplete JSON
#[test]
fn test_parse_incomplete_dtrace_json() {
    // Missing required fields
    let incomplete_json = r#"{ "pid": 12345 }"#;

    let result: Result<serde_json::Value, _> =
        serde_json::from_str(incomplete_json);
    assert!(result.is_ok(), "Partial JSON should parse as JSON");

    let parsed = result.unwrap();
    assert_eq!(parsed["pid"], 12345);
    assert!(parsed["status"].is_null());
}
