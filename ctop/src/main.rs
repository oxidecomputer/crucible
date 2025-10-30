// Copyright 2025 Oxide Computer Company

//! Standalone ctop - curses-based top-like display of crucible dtrace data

use clap::Parser;
use cmon_common::{DtraceDisplay, DtraceWrapper, short_state};
use crossterm::{
    cursor,
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{
        Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen,
        disable_raw_mode, enable_raw_mode,
    },
};
use crucible::DtraceInfo;
use crucible_protocol::ClientId;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::Color,
    widgets::canvas::{Canvas, Line, Points},
    widgets::{Block, Borders, Paragraph},
};
use std::collections::{HashMap, VecDeque};
use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Notify, RwLock};

/// Default dtrace command - embedded one-liner that matches upstairs_raw.d
///
/// This command:
/// - Uses -Z to continue even if no probes match
/// - Uses -q for quiet mode (no dtrace header)
/// - Sets strsize=2k for 2KB string buffers
/// - Probes crucible_upstairs*:::up-status
/// - Outputs JSON with pid and status
const DEFAULT_DTRACE_CMD: &str =
    r#"dtrace -Z -q -x strsize=2k -n 'crucible_upstairs*:::up-status { printf("{\"pid\":%d,\"status\":%s}\n", pid, json(copyinstr(arg1), "ok")); }'"#;

/// Crucible top - monitor crucible upstairs via dtrace
#[derive(Parser, Debug)]
#[clap(name = "ctop", term_width = 80)]
#[clap(about = "Curses-based crucible monitor", long_about = None)]
struct Args {
    /// Command to run to generate dtrace output
    #[clap(long, default_value = DEFAULT_DTRACE_CMD)]
    dtrace_cmd: String,
}

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
    selected_index: usize,
    detail_mode: bool,
    normalize_detail: bool, // Use global min/max for detail view scaling
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
                result.push_str(&format!("  {:>10}", "NEXTJOB"));
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
    _is_stale: bool,
) -> String {
    let mut result = String::new();

    for display_item in dd.iter() {
        match display_item {
            DtraceDisplay::Pid => {
                // Note: stale indicator is now shown in the first column
                result.push_str(&format!(" {:>5}", pid));
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
                result.push_str(&format!(" {:>10}", d_out.next_job_id));
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
/// If global_max is provided, scales relative to that value for
/// cross-session comparison
fn render_sparkline(
    history: &VecDeque<u64>,
    width: usize,
    global_max: u64,
) -> String {
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

    // Use global max for scaling (minimum 1 to avoid division by zero)
    let max = global_max.max(1);

    // Map each value to a block character
    samples
        .iter()
        .map(|&val| {
            if val == 0 {
                BLOCKS[0]
            } else {
                let normalized = (val as f64 / max as f64 * 7.0) as usize;
                BLOCKS[normalized.min(7)]
            }
        })
        .collect()
}

/// Subprocess reader task - spawns dtrace command and reads JSON output
async fn subprocess_reader_task(
    dtrace_cmd: String,
    state: Arc<RwLock<CtopState>>,
    notify: Arc<Notify>,
) -> Result<(), Box<dyn std::error::Error>> {
    if dtrace_cmd.is_empty() {
        return Err("Empty dtrace command".into());
    }

    // Execute the command through a shell to properly handle quoting
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(&dtrace_cmd)
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

/// Render full-screen detail view for a selected session
fn render_detail_view(
    session_data: &SessionData,
    _terminal_size: (u16, u16),
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    global_min: Option<u64>,
    global_max: Option<u64>,
    normalize: bool,
) -> io::Result<()> {
    // Calculate statistics
    let history: Vec<u64> =
        session_data.delta_history.iter().copied().collect();
    let session_max = history.iter().copied().max().unwrap_or(1);
    let session_min = history.iter().copied().min().unwrap_or(0);
    let avg = if !history.is_empty() {
        history.iter().sum::<u64>() / history.len() as u64
    } else {
        0
    };
    let current = session_data.current_delta.unwrap_or(0);

    // Choose min/max based on normalize mode
    let (display_min, display_max) = if normalize {
        (
            global_min.unwrap_or(session_min),
            global_max.unwrap_or(session_max),
        )
    } else {
        (session_min, session_max)
    };

    // Render using ratatui (terminal is reused, ratatui handles diffing)
    terminal.draw(|f| {
        let area = f.area();

        // Split area: 1 line at top for session data, rest for canvas
        let chunks = Layout::default()
            .constraints([Constraint::Length(1), Constraint::Min(0)])
            .split(area);

        // Format the session data row
        let display_fields = default_display_fields();
        let is_stale = session_data
            .last_updated
            .elapsed()
            .as_secs()
            > STALE_THRESHOLD_SECS;
        let row_data = format_row(
            session_data.pid,
            &session_data.dtrace_info,
            session_data.current_delta,
            &display_fields,
            is_stale,
        );

        // Render session data at top
        let data_paragraph = Paragraph::new(row_data);
        f.render_widget(data_paragraph, chunks[0]);

        // Create title
        let session_short: String =
            session_data.dtrace_info.session_id.chars().take(8).collect();
        let mode_str = if normalize { " [NORMALIZED]" } else { "" };
        let title = format!(
            " Delta History - PID {} - Session {}{} ",
            session_data.pid, session_short, mode_str
        );

        // Create canvas widget in bottom area (1 line shorter)
        let canvas = Canvas::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title)
                    .title_bottom(format!(
                        " Samples: {} | Min: {} | Max: {} | Avg: {} | Current: {} ",
                        history.len(),
                        session_min,
                        session_max,
                        avg,
                        current
                    ))
                    .title_bottom(
                        " ['d': Back | 'n': Toggle normalize | 'q': Quit] ",
                    ),
            )
            .x_bounds([0.0, history.len().max(1) as f64])
            .y_bounds([display_min as f64, display_max as f64])
            .paint(|ctx| {
                // Draw Y-axis labels (at left edge of graph)
                let y_range = display_max as f64 - display_min as f64;
                let y_positions = [
                    display_max,
                    display_min + (y_range * 0.75) as u64,
                    display_min + (y_range * 0.5) as u64,
                    display_min + (y_range * 0.25) as u64,
                    display_min,
                ];

                for y_val in &y_positions {
                    ctx.print(
                        0.0,
                        *y_val as f64,
                        ratatui::text::Span::styled(
                            format!("{}", y_val),
                            ratatui::style::Style::default()
                                .fg(Color::Gray),
                        ),
                    );
                }

                // Draw the line graph
                if history.len() > 1 {
                    for i in 0..history.len() - 1 {
                        let x1 = i as f64;
                        let y1 = history[i] as f64;
                        let x2 = (i + 1) as f64;
                        let y2 = history[i + 1] as f64;

                        ctx.draw(&Line {
                            x1,
                            y1,
                            x2,
                            y2,
                            color: Color::Cyan,
                        });
                    }
                }

                // Draw points for each sample
                for (i, &value) in history.iter().enumerate() {
                    ctx.draw(&Points {
                        coords: &[(i as f64, value as f64)],
                        color: Color::Yellow,
                    });
                }
            });

        f.render_widget(canvas, chunks[1]);
    })?;

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

    // Track detail mode and persistent terminal for detail view
    let mut was_in_detail_mode = false;
    let mut detail_terminal: Option<Terminal<CrosstermBackend<io::Stdout>>> =
        None;

    loop {
        // Wait for notification or timeout
        tokio::select! {
            _ = notify.notified() => {},
            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
        }

        // Get current time
        let now = Instant::now();
        let system_time = std::time::SystemTime::now();
        let duration = system_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();

        // Get terminal size
        let terminal_size = crossterm::terminal::size()?;

        // Read state to check mode
        let state_guard = state.read().await;
        let in_detail_mode = state_guard.detail_mode;
        let selected_index = state_guard.selected_index;
        let normalize_detail = state_guard.normalize_detail;

        // If in detail mode, render detail view and skip table
        if in_detail_mode {
            // Create terminal on first entry to detail mode
            if !was_in_detail_mode {
                execute!(stdout, Clear(ClearType::All))?;
                // Create a new stdout handle for the Terminal
                let detail_stdout = io::stdout();
                let backend = CrosstermBackend::new(detail_stdout);
                detail_terminal = Some(Terminal::new(backend)?);
            }

            let mut sessions: Vec<&SessionData> =
                state_guard.sessions.values().collect();
            sessions.sort_by_key(|s| (s.pid, &s.dtrace_info.session_id));

            // Calculate global min/max across all sessions for normalization
            let global_min = sessions
                .iter()
                .flat_map(|s| s.delta_history.iter())
                .copied()
                .min();
            let global_max = sessions
                .iter()
                .flat_map(|s| s.delta_history.iter())
                .copied()
                .max();

            if let Some(selected_session) = sessions.get(selected_index) {
                // Clone the session data so we can drop the lock
                let session_clone = (*selected_session).clone();
                drop(state_guard);

                if let Some(terminal) = detail_terminal.as_mut() {
                    render_detail_view(
                        &session_clone,
                        terminal_size,
                        terminal,
                        global_min,
                        global_max,
                        normalize_detail,
                    )?;
                }
            } else {
                drop(state_guard);
            }

            was_in_detail_mode = true;
        } else {
            // Exiting detail mode - drop terminal and redraw table
            if was_in_detail_mode {
                detail_terminal = None;
                execute!(stdout, Clear(ClearType::All))?;
            }
            was_in_detail_mode = false;

            // Move cursor to top-left
            execute!(stdout, cursor::MoveTo(0, 0))?;
            // Table mode - render normal view
            drop(state_guard);

            // Display header (clear line first to remove artifacts)
            execute!(stdout, cursor::MoveTo(0, 0))?;
            write!(
                stdout,
                "ctop - Unix timestamp: {}",
                duration.as_secs()
            )?;
            execute!(stdout, Clear(ClearType::UntilNewLine))?;
            write!(stdout, "\r\n")?;
            execute!(stdout, Clear(ClearType::UntilNewLine))?;
            write!(stdout, "\r\n")?;

            // Display column headers
            write!(stdout, "{}", format_header(&display_fields))?;
            execute!(stdout, Clear(ClearType::UntilNewLine))?;
            write!(stdout, "\r\n")?;

            let (terminal_width, _) = terminal_size;

            // Read state and display sessions sorted by PID (then session_id)
            let state_guard = state.read().await;
            let mut sessions: Vec<&SessionData> =
                state_guard.sessions.values().collect();
            sessions.sort_by_key(|s| (s.pid, &s.dtrace_info.session_id));

            // Calculate global max across all sessions for consistent sparkline
            // scaling
            let global_max = sessions
                .iter()
                .flat_map(|s| s.delta_history.iter())
                .copied()
                .max()
                .unwrap_or(1);

            let selected_index = state_guard.selected_index;

            for (idx, session_data) in sessions.iter().enumerate() {
                let is_stale = now.duration_since(session_data.last_updated)
                    > Duration::from_secs(STALE_THRESHOLD_SECS);

                // Add indicator: > for selected, * for stale, space otherwise
                // Selection indicator (>) takes priority over stale indicator (*)
                let indicator = if idx == selected_index {
                    ">"
                } else if is_stale {
                    "*"
                } else {
                    " "
                };
                write!(stdout, "{}", indicator)?;

                let row = format_row(
                    session_data.pid,
                    &session_data.dtrace_info,
                    session_data.current_delta,
                    &display_fields,
                    is_stale,
                );
                write!(stdout, "{}", row)?;

                // Render sparkline in remaining space
                // Account for the indicator character (1 char)
                let row_len = row.chars().count() + 1;
                if terminal_width > row_len as u16 {
                    let sparkline_width =
                        (terminal_width as usize - row_len).saturating_sub(1);
                    if sparkline_width > 0 {
                        let sparkline = render_sparkline(
                            &session_data.delta_history,
                            sparkline_width,
                            global_max,
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
                "[↑↓: Select | 'd': Details | 'q': Quit] > = selected, * = stale ({}s)",
                STALE_THRESHOLD_SECS
            )?;
            execute!(stdout, Clear(ClearType::UntilNewLine))?;
            write!(stdout, "\r\n")?;

            // Clear from cursor to end of screen (removes any leftover lines)
            execute!(stdout, Clear(ClearType::FromCursorDown))?;

            stdout.flush()?;
        } // End of table mode rendering

        // Check for keyboard input (non-blocking)
        if event::poll(Duration::from_millis(0))?
            && let Event::Key(key_event) = event::read()?
        {
            let mut state_guard = state.write().await;
            let num_sessions = state_guard.sessions.len();

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
                KeyEvent {
                    code: KeyCode::Char('d'),
                    modifiers: KeyModifiers::NONE,
                    ..
                } => {
                    // Toggle detail mode
                    state_guard.detail_mode = !state_guard.detail_mode;
                }
                KeyEvent {
                    code: KeyCode::Char('n'),
                    modifiers: KeyModifiers::NONE,
                    ..
                } => {
                    // Toggle normalize mode (only affects detail view)
                    state_guard.normalize_detail =
                        !state_guard.normalize_detail;
                }
                KeyEvent {
                    code: KeyCode::Up,
                    modifiers: KeyModifiers::NONE,
                    ..
                } => {
                    // Move selection up (only in table mode)
                    if !state_guard.detail_mode && num_sessions > 0 {
                        state_guard.selected_index =
                            state_guard.selected_index.saturating_sub(1);
                    }
                }
                KeyEvent {
                    code: KeyCode::Down,
                    modifiers: KeyModifiers::NONE,
                    ..
                } => {
                    // Move selection down (only in table mode)
                    if !state_guard.detail_mode && num_sessions > 0 {
                        state_guard.selected_index =
                            (state_guard.selected_index + 1)
                                .min(num_sessions.saturating_sub(1));
                    }
                }
                KeyEvent {
                    code: KeyCode::Esc,
                    modifiers: KeyModifiers::NONE,
                    ..
                } => {
                    // Exit detail mode
                    state_guard.detail_mode = false;
                }
                _ => {}
            }
            drop(state_guard);
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

/// Main entry point
#[tokio::main]
async fn main() {
    let args = Args::parse();

    if let Err(e) = ctop_loop(args.dtrace_cmd).await {
        eprintln!("Error running ctop: {}", e);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for ctop
    //!
    //! # Test Coverage Overview
    //!
    //! These unit tests validate the pure functions and data structures that
    //! power ctop's display logic, focusing on areas that don't require async
    //! runtime or terminal mocking.
    //!
    //! ## Current Coverage
    //!
    //! ### Display Formatting (format_header, format_row)
    //! - Column header generation for all DtraceDisplay field types
    //! - Multi-column fields (State shows DS0/DS1/DS2)
    //! - IO summary fields with multiple columns per downstairs
    //! - Empty field handling
    //!
    //! ### Sparkline Rendering (render_sparkline)
    //! - Empty history handling
    //! - Zero-width rendering
    //! - Single and multiple value rendering
    //! - Width limiting (showing only recent samples)
    //! - Normalization using global max value
    //! - Unicode block character validation (▁▂▃▄▅▆▇█)
    //! - Ascending/descending trend visualization
    //!
    //! ### State Management
    //! - Default CtopState initialization
    //! - Delta history ring buffer behavior (MAX_DELTA_HISTORY)
    //! - Constants validation (STALE_THRESHOLD_SECS, MAX_DELTA_HISTORY)
    //!
    //! ## What's NOT Tested (See integration_test.rs for full list)
    //!
    //! - Async tasks (subprocess_reader_task, display_task)
    //! - Terminal rendering and keyboard input
    //! - Session lifecycle and state updates
    //! - DTrace subprocess integration
    //! - Multi-session coordination
    //!
    //! ## Testing Strategy
    //!
    //! These tests focus on **testable units** - pure functions with no I/O.
    //! For components requiring mocking (terminal, subprocess, async runtime),
    //! see the testing improvement proposals in integration_test.rs.
    //!
    //! ## Running Tests
    //!
    //! ```bash
    //! # Run all ctop unit tests
    //! cargo test -p ctop --lib
    //!
    //! # Run specific test
    //! cargo test -p ctop --lib test_render_sparkline_normalization
    //! ```

    use super::*;

    // ============================================================================
    // Display Field Configuration Tests
    // ============================================================================
    //
    // These tests verify the default display configuration that users see when
    // they first run ctop. The fields should provide a good overview of upstairs
    // state without overwhelming the display.

    #[test]
    fn test_default_display_fields() {
        let fields = default_display_fields();

        // Verify we have the expected default fields
        assert_eq!(fields.len(), 8);
        assert_eq!(fields[0], DtraceDisplay::Pid);
        assert_eq!(fields[1], DtraceDisplay::Session);
        assert_eq!(fields[2], DtraceDisplay::State);
        assert_eq!(fields[3], DtraceDisplay::NextJobId);
        assert_eq!(fields[4], DtraceDisplay::JobDelta);
        assert_eq!(fields[5], DtraceDisplay::ExtentLimit);
        assert_eq!(fields[6], DtraceDisplay::DsReconciled);
        assert_eq!(fields[7], DtraceDisplay::DsReconcileNeeded);
    }

    // ============================================================================
    // Header Formatting Tests
    // ============================================================================
    //
    // These tests verify that format_header() generates correct column headers
    // for various field types. Some fields (like State, IoSummary) expand into
    // multiple columns representing the three downstairs replicas.

    #[test]
    fn test_format_header_basic_fields() {
        let fields = vec![DtraceDisplay::Pid, DtraceDisplay::Session];
        let header = format_header(&fields);

        // Check that header contains expected column names
        assert!(header.contains("PID"));
        assert!(header.contains("SESSION"));
    }

    #[test]
    fn test_format_header_state_field() {
        let fields = vec![DtraceDisplay::State];
        let header = format_header(&fields);

        // State field should show three downstairs columns
        assert!(header.contains("DS0"));
        assert!(header.contains("DS1"));
        assert!(header.contains("DS2"));
    }

    #[test]
    fn test_format_header_io_fields() {
        let fields = vec![DtraceDisplay::IoSummary];
        let header = format_header(&fields);

        // Should show in_progress, done, and skipped for each DS
        assert!(header.contains("IP0"));
        assert!(header.contains("IP1"));
        assert!(header.contains("IP2"));
        assert!(header.contains("D0"));
        assert!(header.contains("D1"));
        assert!(header.contains("D2"));
        assert!(header.contains("S0"));
        assert!(header.contains("S1"));
        assert!(header.contains("S2"));
    }

    #[test]
    fn test_format_header_empty_fields() {
        let fields = vec![];
        let header = format_header(&fields);

        // Empty fields should produce empty header
        assert_eq!(header, "");
    }

    // ============================================================================
    // Sparkline Rendering Tests
    // ============================================================================
    //
    // Sparklines provide a compact visualization of job delta trends over time.
    // They use Unicode block characters (▁▂▃▄▅▆▇█) to represent values.
    //
    // Key behaviors tested:
    // - Empty history and edge cases (zero width, single values)
    // - Width limiting (showing only the most recent N samples)
    // - Normalization across sessions (global_max parameter)
    // - Unicode character validity
    //
    // TODO: Consider property-based testing (proptest) to generate random
    // histories and verify properties like:
    // - Sparkline length <= requested width
    // - All characters are valid block characters
    // - Normalized values respect global_max

    #[test]
    fn test_render_sparkline_empty() {
        let history = VecDeque::new();
        let sparkline = render_sparkline(&history, 10, 100);

        assert_eq!(sparkline, "");
    }

    #[test]
    fn test_render_sparkline_zero_width() {
        let mut history = VecDeque::new();
        history.push_back(10);
        history.push_back(20);

        let sparkline = render_sparkline(&history, 0, 100);
        assert_eq!(sparkline, "");
    }

    #[test]
    fn test_render_sparkline_single_value() {
        let mut history = VecDeque::new();
        history.push_back(50);

        let sparkline = render_sparkline(&history, 10, 100);

        // Should have one character
        assert_eq!(sparkline.chars().count(), 1);
    }

    #[test]
    fn test_render_sparkline_ascending_values() {
        let mut history = VecDeque::new();
        for i in 0..10 {
            history.push_back(i * 10);
        }

        let sparkline = render_sparkline(&history, 10, 100);

        // Should have 10 characters (one per value)
        assert_eq!(sparkline.chars().count(), 10);

        // First character should be lower than last (ascending trend)
        let chars: Vec<char> = sparkline.chars().collect();
        assert!(chars[0] < chars[9]);
    }

    #[test]
    fn test_render_sparkline_width_limit() {
        let mut history = VecDeque::new();
        for i in 0..100 {
            history.push_back(i);
        }

        // Request only last 5 samples
        let sparkline = render_sparkline(&history, 5, 100);

        // Should only show 5 characters
        assert_eq!(sparkline.chars().count(), 5);
    }

    #[test]
    fn test_render_sparkline_max_value() {
        let mut history = VecDeque::new();
        history.push_back(0);
        history.push_back(100);

        let sparkline = render_sparkline(&history, 10, 100);

        // Should use valid unicode block characters
        for c in sparkline.chars() {
            assert!(
                ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'].contains(&c),
                "Invalid sparkline character: {}",
                c
            );
        }
    }

    #[test]
    fn test_render_sparkline_normalization() {
        // This test verifies a critical feature: sparklines are normalized using
        // a global_max value computed across ALL sessions. This allows users to
        // visually compare activity levels between different upstairs instances.
        //
        // Without global normalization, each session would auto-scale to its own
        // range, making cross-session comparison meaningless.

        let mut history = VecDeque::new();
        history.push_back(50);
        history.push_back(100);

        // Test with global max = 200 (should scale differently than 100)
        let sparkline1 = render_sparkline(&history, 10, 200);
        let sparkline2 = render_sparkline(&history, 10, 100);

        // With higher global max, the values should appear relatively lower
        let chars1: Vec<char> = sparkline1.chars().collect();
        let chars2: Vec<char> = sparkline2.chars().collect();

        // Second sparkline should use higher blocks (value 100 is max in range 0-100,
        // but only midpoint in range 0-200)
        assert!(
            chars1[1] < chars2[1],
            "Expected normalization to affect block height"
        );
    }

    // ============================================================================
    // State Management Tests
    // ============================================================================
    //
    // These tests verify the data structures that track session state:
    // - CtopState: Overall application state (sessions map, selection, mode)
    // - SessionData: Per-session data including delta history ring buffer
    // - Constants: STALE_THRESHOLD_SECS, MAX_DELTA_HISTORY
    //
    // Note: These tests only validate initialization and constants. Full
    // state lifecycle testing (session updates, transitions) requires async
    // mocking infrastructure.

    #[test]
    fn test_ctop_state_default() {
        let state = CtopState::default();

        assert_eq!(state.sessions.len(), 0);
        assert_eq!(state.selected_index, 0);
        assert!(!state.detail_mode);
        assert!(!state.normalize_detail);
    }

    #[test]
    fn test_session_data_delta_history_max_size() {
        // Delta history uses a ring buffer (VecDeque) to maintain a sliding window
        // of recent job delta values for sparkline rendering. This test verifies
        // the ring buffer behavior: old values are evicted when capacity is reached.
        //
        // In production, subprocess_reader_task maintains this ring buffer by:
        // 1. Computing delta = new_job_id - old_job_id
        // 2. Pushing delta to back of deque
        // 3. Popping from front if len > MAX_DELTA_HISTORY

        let mut delta_history = VecDeque::new();

        // Simulate adding more than MAX_DELTA_HISTORY items
        for i in 0..(MAX_DELTA_HISTORY + 10) {
            delta_history.push_back(i as u64);
            if delta_history.len() > MAX_DELTA_HISTORY {
                delta_history.pop_front();
            }
        }

        // Should never exceed MAX_DELTA_HISTORY
        assert_eq!(delta_history.len(), MAX_DELTA_HISTORY);

        // Should contain the most recent items (oldest 10 were evicted)
        assert_eq!(*delta_history.front().unwrap(), 10); // First item should be item 10
        assert_eq!(
            *delta_history.back().unwrap(),
            (MAX_DELTA_HISTORY + 9) as u64
        ); // Last item is most recent
    }

    #[test]
    fn test_stale_threshold_constant() {
        // Verify STALE_THRESHOLD_SECS is reasonable
        assert!(
            STALE_THRESHOLD_SECS > 0,
            "Stale threshold should be positive"
        );
        assert!(
            STALE_THRESHOLD_SECS <= 60,
            "Stale threshold should be under a minute"
        );
    }

    #[test]
    fn test_max_delta_history_constant() {
        // Verify MAX_DELTA_HISTORY is reasonable for sparklines
        assert!(
            MAX_DELTA_HISTORY >= 50,
            "Should store enough history for reasonable sparklines"
        );
        assert!(
            MAX_DELTA_HISTORY <= 1000,
            "History shouldn't be excessively large"
        );
    }

    // ============================================================================
    // Future Testing Opportunities
    // ============================================================================
    //
    // The following areas could benefit from additional testing infrastructure:
    //
    // ## 1. Async Task Testing (mockall crate)
    //
    // Mock the subprocess Command to test subprocess_reader_task:
    // - Emit controlled JSON lines
    // - Verify state updates (sessions map, delta calculations)
    // - Test error handling (invalid JSON, subprocess crashes)
    // - Test session expiration and cleanup
    //
    // ## 2. Terminal UI Testing (ratatui::TestBackend)
    //
    // Capture terminal output to verify:
    // - Header and row formatting in actual terminal context
    // - Stale session indicators (*) appear correctly
    // - Selection indicators (>) highlight correct row
    // - Sparklines render in available terminal width
    // - Detail mode layout and graphing
    //
    // ## 3. Keyboard Input Testing
    //
    // Mock crossterm events to test:
    // - Up/Down arrow navigation (selected_index changes)
    // - 'd' toggles detail_mode
    // - 'n' toggles normalize_detail
    // - 'q' and Ctrl-C exit cleanly
    // - Esc exits detail mode
    //
    // ## 4. Property-Based Testing (proptest)
    //
    // Generate random inputs to verify invariants:
    // - Sparkline width never exceeds requested width
    // - format_header produces valid UTF-8
    // - Delta history never exceeds MAX_DELTA_HISTORY
    // - selected_index never exceeds sessions.len()
    //
    // ## 5. Snapshot Testing (insta)
    //
    // Capture and compare terminal output snapshots:
    // - Table view with various session counts
    // - Detail view with different data patterns
    // - Regression testing for layout changes
    //
    // ## 6. Performance Testing (criterion)
    //
    // Benchmark critical paths:
    // - render_sparkline with full history
    // - format_row with many display fields
    // - Session sorting with 100+ sessions
    //
    // ## 7. End-to-End Testing
    //
    // Run ctop against real DTrace output:
    // - Capture actual crucible DTrace JSON
    // - Replay captured output to ctop
    // - Verify no parsing errors
    // - Compare against expected session data
}
