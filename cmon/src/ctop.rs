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
use std::collections::{HashMap, VecDeque};
use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Notify, RwLock};

const STALE_THRESHOLD_SECS: u64 = 10;
const MAX_DELTA_HISTORY: usize = 100;

/// Data for a single session
#[derive(Debug, Clone)]
struct SessionData {
    pid: u32,
    dtrace_info: DtraceInfo,
    last_job_id: u64,
    last_updated: Instant,
    current_delta: Option<u64>,
    delta_history: VecDeque<u64>,
}

/// Shared state between stdin reader and display tasks
#[derive(Debug, Default)]
struct CtopState {
    sessions: HashMap<String, SessionData>,
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
    precomputed_delta: Option<u64>,
    dd: &[DtraceDisplay],
    is_stale: bool,
) -> String {
    let mut result = String::new();

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
                if let Some(delta) = precomputed_delta {
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

/// Render a sparkline from delta history
/// Uses Unicode block characters to show trend: ▁▂▃▄▅▆▇█
fn render_sparkline(history: &VecDeque<u64>, width: usize) -> String {
    if history.is_empty() || width == 0 {
        return String::new();
    }

    // Unicode block characters from lowest to highest
    const BLOCKS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

    // Take last 'width' samples (most recent)
    let samples: Vec<u64> = history
        .iter()
        .rev()
        .take(width)
        .copied()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();

    if samples.is_empty() {
        return String::new();
    }

    // Find max value for scaling
    let max = *samples.iter().max().unwrap_or(&1);
    if max == 0 {
        return BLOCKS[0].to_string().repeat(samples.len());
    }

    // Map each value to a block character
    samples
        .iter()
        .map(|&val| {
            let normalized = (val as f64 / max as f64 * 7.0) as usize;
            BLOCKS[normalized.min(7)]
        })
        .collect()
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

        let session_data = state_guard
            .sessions
            .entry(wrapper.status.session_id.clone())
            .or_insert_with(|| SessionData {
                pid: wrapper.pid,
                dtrace_info: wrapper.status.clone(),
                last_job_id: 0,
                last_updated: Instant::now(),
                current_delta: None,
                delta_history: VecDeque::new(),
            });

        // Calculate delta (jobs per second)
        let current_job_id = wrapper.status.next_job_id.0;
        let delta = if session_data.last_job_id != 0 {
            let d = current_job_id.saturating_sub(session_data.last_job_id);

            // Add to history ring buffer
            session_data.delta_history.push_back(d);
            if session_data.delta_history.len() > MAX_DELTA_HISTORY {
                session_data.delta_history.pop_front();
            }

            Some(d)
        } else {
            None
        };

        // Store current delta and update state
        session_data.current_delta = delta;
        session_data.last_job_id = current_job_id;
        session_data.dtrace_info = wrapper.status;
        session_data.last_updated = Instant::now();

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

        // Move cursor to top-left (don't clear entire screen)
        execute!(stdout, cursor::MoveTo(0, 0))?;

        // Get current time
        let now = Instant::now();
        let system_time = std::time::SystemTime::now();
        let duration = system_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();

        // Display header (clear line first to remove artifacts)
        write!(stdout, "cmon ctop - Unix timestamp: {}", duration.as_secs())?;
        execute!(stdout, Clear(ClearType::UntilNewLine))?;
        write!(stdout, "\r\n")?;
        execute!(stdout, Clear(ClearType::UntilNewLine))?;
        write!(stdout, "\r\n")?;

        // Display column headers
        write!(stdout, "{}", format_header(&display_fields))?;
        execute!(stdout, Clear(ClearType::UntilNewLine))?;
        write!(stdout, "\r\n")?;

        // Get terminal size for sparkline width calculation
        let (terminal_width, _) = crossterm::terminal::size()?;

        // Read state and display sessions sorted by PID (then session_id)
        let state_guard = state.read().await;
        let mut sessions: Vec<&SessionData> =
            state_guard.sessions.values().collect();
        sessions.sort_by_key(|s| (s.pid, &s.dtrace_info.session_id));

        for session_data in sessions {
            let is_stale = now.duration_since(session_data.last_updated)
                > Duration::from_secs(STALE_THRESHOLD_SECS);

            let row = format_row(
                session_data.pid,
                &session_data.dtrace_info,
                session_data.current_delta,
                &display_fields,
                is_stale,
            );
            write!(stdout, "{}", row)?;

            // Render sparkline in remaining space
            let row_len = row.chars().count();
            if terminal_width > row_len as u16 {
                let sparkline_width =
                    (terminal_width as usize - row_len).saturating_sub(1);
                if sparkline_width > 0 {
                    let sparkline = render_sparkline(
                        &session_data.delta_history,
                        sparkline_width,
                    );
                    write!(stdout, " {}", sparkline)?;
                }
            }

            execute!(stdout, Clear(ClearType::UntilNewLine))?;
            write!(stdout, "\r\n")?;
        }
        drop(state_guard);

        // Display footer
        execute!(stdout, Clear(ClearType::UntilNewLine))?;
        write!(stdout, "\r\n")?;
        write!(
            stdout,
            "Press 'q' or Ctrl+C to quit. * = stale (no update in {}s)",
            STALE_THRESHOLD_SECS
        )?;
        execute!(stdout, Clear(ClearType::UntilNewLine))?;
        write!(stdout, "\r\n")?;

        // Clear from cursor to end of screen (removes any leftover lines)
        execute!(stdout, Clear(ClearType::FromCursorDown))?;

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
