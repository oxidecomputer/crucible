// Copyright 2025 Oxide Computer Company

use crate::{short_state, DtraceDisplay, DtraceWrapper};
use crossterm::{
    cursor,
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{
        disable_raw_mode, enable_raw_mode, Clear, ClearType,
        EnterAlternateScreen, LeaveAlternateScreen,
    },
};
use crucible::DtraceInfo;
use crucible_protocol::ClientId;
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Notify, RwLock};

const STALE_THRESHOLD_SECS: u64 = 10;

/// Data for a single process
#[derive(Debug, Clone)]
struct ProcessData {
    dtrace_info: DtraceInfo,
    last_job_id: u64,
    last_updated: Instant,
}

/// Shared state between stdin reader and display tasks
#[derive(Debug, Default)]
struct CtopState {
    processes: HashMap<u32, ProcessData>,
}

/// Default display fields (same as dtrace command defaults)
fn default_display_fields() -> Vec<DtraceDisplay> {
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

/// Format header line for the given display fields
fn format_header(dd: &[DtraceDisplay]) -> String {
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
                result.push_str(&format!(" {:>4}", "ERR"));
            }
            DtraceDisplay::DsReconcileNeeded => {
                result.push_str(&format!(" {:>4}", "ERN"));
            }
            DtraceDisplay::LiveRepair => {
                result.push_str(&format!(
                    " {:>4} {:>4} {:>4}",
                    "LRC0", "LRC1", "LRC0"
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
                result.push_str(&format!(" {:>7}", "NEXTJOB"));
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

/// Format a row for a single process
fn format_row(
    pid: u32,
    d_out: &DtraceInfo,
    last_job_id: u64,
    dd: &[DtraceDisplay],
    is_stale: bool,
) -> String {
    let mut result = String::new();
    let mut computed_delta: Option<u64> = None;

    for display_item in dd.iter() {
        match display_item {
            DtraceDisplay::Pid => {
                let pid_str = if is_stale {
                    format!("{pid}*")
                } else {
                    format!("{pid}")
                };
                result.push_str(&format!(" {:>5}", pid_str));
            }
            DtraceDisplay::Session => {
                let session_short =
                    d_out.session_id.chars().take(8).collect::<String>();
                result.push_str(&format!(" {:>8}", session_short));
            }
            DtraceDisplay::UpstairsId => {
                let upstairs_short =
                    d_out.upstairs_id.chars().take(8).collect::<String>();
                result.push_str(&format!(" {:>8}", upstairs_short));
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
                result.push_str(&format!(" {:3}", d_out.up_count));
            }
            DtraceDisplay::DsCount => {
                result.push_str(&format!(" {:5}", d_out.ds_count));
            }
            DtraceDisplay::IoCount | DtraceDisplay::IoSummary => {
                result.push_str(&format!(
                    " {:5} {:5} {:5}",
                    d_out.ds_io_count.in_progress[ClientId::new(0)],
                    d_out.ds_io_count.in_progress[ClientId::new(1)],
                    d_out.ds_io_count.in_progress[ClientId::new(2)],
                ));
                result.push_str(&format!(
                    " {:5} {:5} {:5}",
                    d_out.ds_io_count.done[ClientId::new(0)],
                    d_out.ds_io_count.done[ClientId::new(1)],
                    d_out.ds_io_count.done[ClientId::new(2)],
                ));
                result.push_str(&format!(
                    " {:5} {:5} {:5}",
                    d_out.ds_io_count.skipped[ClientId::new(0)],
                    d_out.ds_io_count.skipped[ClientId::new(1)],
                    d_out.ds_io_count.skipped[ClientId::new(2)],
                ));
                if matches!(display_item, DtraceDisplay::IoCount) {
                    result.push_str(&format!(
                        " {:4} {:4} {:4}",
                        d_out.ds_io_count.error[ClientId::new(0)],
                        d_out.ds_io_count.error[ClientId::new(1)],
                        d_out.ds_io_count.error[ClientId::new(2)],
                    ));
                }
            }
            DtraceDisplay::Reconcile => {
                result.push_str(&format!(
                    " {:4} {:4} {:4}",
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
                    " {:4} {:4} {:4}",
                    d_out.ds_live_repair_completed[0],
                    d_out.ds_live_repair_completed[1],
                    d_out.ds_live_repair_completed[2],
                ));
                result.push_str(&format!(
                    " {:4} {:4} {:4}",
                    d_out.ds_live_repair_aborted[0],
                    d_out.ds_live_repair_aborted[1],
                    d_out.ds_live_repair_aborted[2],
                ));
            }
            DtraceDisplay::Connected => {
                result.push_str(&format!(
                    " {:4} {:4} {:4}",
                    d_out.ds_connected[0],
                    d_out.ds_connected[1],
                    d_out.ds_connected[2],
                ));
            }
            DtraceDisplay::Replaced => {
                result.push_str(&format!(
                    " {:4} {:4} {:4}",
                    d_out.ds_replaced[0],
                    d_out.ds_replaced[1],
                    d_out.ds_replaced[2],
                ));
            }
            DtraceDisplay::ExtentLiveRepair => {
                result.push_str(&format!(
                    " {:4} {:4} {:4}",
                    d_out.ds_extents_repaired[0],
                    d_out.ds_extents_repaired[1],
                    d_out.ds_extents_repaired[2],
                ));
                result.push_str(&format!(
                    " {:4} {:4} {:4}",
                    d_out.ds_extents_confirmed[0],
                    d_out.ds_extents_confirmed[1],
                    d_out.ds_extents_confirmed[2],
                ));
            }
            DtraceDisplay::ExtentLimit => {
                result.push_str(&format!(" {:4}", d_out.ds_extent_limit));
            }
            DtraceDisplay::NextJobId => {
                result.push_str(&format!(" {:>7}", d_out.next_job_id));
            }
            DtraceDisplay::JobDelta => {
                if computed_delta.is_none() {
                    computed_delta = if last_job_id == 0 {
                        None
                    } else {
                        Some(d_out.next_job_id.0 - last_job_id)
                    };
                }
                if let Some(delta) = computed_delta {
                    result.push_str(&format!(" {:5}", delta));
                } else {
                    result.push_str(&format!(" {:>5}", "---"));
                }
            }
            DtraceDisplay::DsDelay => {
                result.push_str(&format!(
                    " {:5} {:5} {:5}",
                    d_out.ds_delay_us[0],
                    d_out.ds_delay_us[1],
                    d_out.ds_delay_us[2],
                ));
            }
            DtraceDisplay::WriteBytesOut => {
                result.push_str(&format!(" {:10}", d_out.write_bytes_out));
            }
            DtraceDisplay::RoLrSkipped => {
                result.push_str(&format!(
                    " {:4} {:4} {:4}",
                    d_out.ds_ro_lr_skipped[0],
                    d_out.ds_ro_lr_skipped[1],
                    d_out.ds_ro_lr_skipped[2],
                ));
            }
            DtraceDisplay::DsIoInProgress => {
                result.push_str(&format!(
                    " {:5} {:5} {:5}",
                    d_out.ds_io_count.in_progress[ClientId::new(0)],
                    d_out.ds_io_count.in_progress[ClientId::new(1)],
                    d_out.ds_io_count.in_progress[ClientId::new(2)],
                ));
            }
            DtraceDisplay::DsIoDone => {
                result.push_str(&format!(
                    " {:5} {:5} {:5}",
                    d_out.ds_io_count.done[ClientId::new(0)],
                    d_out.ds_io_count.done[ClientId::new(1)],
                    d_out.ds_io_count.done[ClientId::new(2)],
                ));
            }
            DtraceDisplay::DsIoSkipped => {
                result.push_str(&format!(
                    " {:5} {:5} {:5}",
                    d_out.ds_io_count.skipped[ClientId::new(0)],
                    d_out.ds_io_count.skipped[ClientId::new(1)],
                    d_out.ds_io_count.skipped[ClientId::new(2)],
                ));
            }
            DtraceDisplay::DsIoError => {
                result.push_str(&format!(
                    " {:4} {:4} {:4}",
                    d_out.ds_io_count.error[ClientId::new(0)],
                    d_out.ds_io_count.error[ClientId::new(1)],
                    d_out.ds_io_count.error[ClientId::new(2)],
                ));
            }
        }
    }
    result
}

/// Subprocess reader task - spawns dtrace command and reads JSON output
async fn subprocess_reader_task(
    dtrace_cmd: String,
    state: Arc<RwLock<CtopState>>,
    notify: Arc<Notify>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse command string into command and args
    let parts: Vec<&str> = dtrace_cmd.split_whitespace().collect();
    if parts.is_empty() {
        return Err("Empty dtrace command".into());
    }

    // Spawn the dtrace subprocess
    let mut child = Command::new(parts[0])
        .args(&parts[1..])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    let stdout = child
        .stdout
        .take()
        .ok_or("Failed to capture subprocess stdout")?;

    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();

    // Read lines from subprocess stdout
    while let Some(line) = lines.next_line().await? {
        // Parse JSON
        let wrapper: DtraceWrapper = match serde_json::from_str(&line) {
            Ok(w) => w,
            Err(_) => continue,
        };

        // Update state
        let mut state_guard = state.write().await;
        let process_data = state_guard
            .processes
            .entry(wrapper.pid)
            .or_insert_with(|| ProcessData {
                dtrace_info: wrapper.status.clone(),
                last_job_id: 0,
                last_updated: Instant::now(),
            });

        process_data.last_job_id = process_data.dtrace_info.next_job_id.0;
        process_data.dtrace_info = wrapper.status;
        process_data.last_updated = Instant::now();

        drop(state_guard);

        // Notify display task
        notify.notify_one();
    }

    // Wait for child to exit
    let _ = child.wait().await;

    Ok(())
}

/// Display task - renders the screen and handles keyboard input
async fn display_task(
    state: Arc<RwLock<CtopState>>,
    notify: Arc<Notify>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Set up terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;

    // Set up panic handler to restore terminal
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        let _ = disable_raw_mode();
        original_hook(panic_info);
    }));

    let display_fields = default_display_fields();

    loop {
        // Wait for notification or timeout
        tokio::select! {
            _ = notify.notified() => {},
            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
        }

        // Clear screen
        execute!(stdout, Clear(ClearType::All), cursor::MoveTo(0, 0))?;

        // Get current time
        let now = Instant::now();
        let system_time = std::time::SystemTime::now();
        let duration = system_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();

        // Display header
        write!(
            stdout,
            "cmon ctop - Unix timestamp: {}\r\n",
            duration.as_secs()
        )?;
        write!(stdout, "\r\n")?;

        // Display column headers
        write!(stdout, "{}\r\n", format_header(&display_fields))?;

        // Read state and display processes sorted by PID
        let state_guard = state.read().await;
        let mut pids: Vec<u32> =
            state_guard.processes.keys().copied().collect();
        pids.sort_unstable();

        for pid in pids {
            if let Some(process_data) = state_guard.processes.get(&pid) {
                let is_stale = now.duration_since(process_data.last_updated)
                    > Duration::from_secs(STALE_THRESHOLD_SECS);

                let row = format_row(
                    pid,
                    &process_data.dtrace_info,
                    process_data.last_job_id,
                    &display_fields,
                    is_stale,
                );
                write!(stdout, "{}\r\n", row)?;
            }
        }
        drop(state_guard);

        // Display footer
        write!(stdout, "\r\n")?;
        write!(
            stdout,
            "Press 'q' or Ctrl+C to quit. * = stale (no update in {}s)\r\n",
            STALE_THRESHOLD_SECS
        )?;

        stdout.flush()?;

        // Check for keyboard input (non-blocking)
        if event::poll(Duration::from_millis(0))? {
            if let Event::Key(key_event) = event::read()? {
                match key_event {
                    KeyEvent {
                        code: KeyCode::Char('q'),
                        modifiers: KeyModifiers::NONE,
                        ..
                    } => break,
                    KeyEvent {
                        code: KeyCode::Char('c'),
                        modifiers: KeyModifiers::CONTROL,
                        ..
                    } => break,
                    _ => {}
                }
            }
        }
    }

    // Clean up terminal
    execute!(stdout, LeaveAlternateScreen)?;
    disable_raw_mode()?;

    Ok(())
}

/// Main entry point for ctop
pub async fn ctop_loop(
    dtrace_cmd: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(RwLock::new(CtopState::default()));
    let notify = Arc::new(Notify::new());

    let state_reader = Arc::clone(&state);
    let notify_reader = Arc::clone(&notify);

    // Spawn subprocess reader task
    let reader_handle = tokio::spawn(async move {
        if let Err(e) =
            subprocess_reader_task(dtrace_cmd, state_reader, notify_reader)
                .await
        {
            eprintln!("Subprocess reader error: {}", e);
        }
    });

    // Run display task (blocks until user quits)
    let display_result = display_task(state, notify).await;

    // Wait for reader task to finish (it should exit quickly)
    let _ =
        tokio::time::timeout(Duration::from_millis(100), reader_handle).await;

    display_result
}
