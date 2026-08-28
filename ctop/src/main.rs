// Copyright 2026 Oxide Computer Company

//! Standalone ctop - curses-based top-like display of crucible dtrace data

use clap::Parser;
use cmon_common::{
    DtraceDisplay, DtraceWrapper, default_display_fields, format_header,
    format_row,
};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{
        EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode,
        enable_raw_mode,
    },
};
use crucible::DtraceInfo;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::Color,
    widgets::canvas::{Canvas, Line, Points},
    widgets::{Block, Borders, Paragraph, Row, Table, TableState},
};
use std::collections::{HashMap, VecDeque};
use std::io;
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
///
/// dtrace needs privileges to run, so ctop itself has to be started with
/// them (`pfexec ctop`).  Without them dtrace exits immediately and the
/// reason is reported on the status line.
const DEFAULT_DTRACE_CMD: &str = r#"dtrace -Z -q -x strsize=2k -n 'crucible_upstairs*:::up-status { printf("{\"pid\":%d,\"status\":%s}\n", pid, json(copyinstr(arg1), "ok")); }'"#;

/// Crucible top - monitor crucible upstairs via dtrace
#[derive(Parser, Debug)]
#[clap(name = "ctop", term_width = 80)]
#[clap(
    about = "Curses-based crucible monitor",
    long_about = "Curses-based crucible monitor.\n\n\
                  Runs a dtrace command and displays the up-status probe \
                  output from every crucible upstairs on the system.  \
                  dtrace requires privileges, so run this as `pfexec ctop`."
)]
struct Args {
    /// Command to run to generate dtrace output
    #[clap(long, default_value = DEFAULT_DTRACE_CMD)]
    dtrace_cmd: String,
}

const STALE_THRESHOLD_SECS: u64 = 5;
const REMOVE_THRESHOLD_SECS: u64 = 30;
const MAX_DELTA_HISTORY: usize = 100;

/// How often we look for keyboard input.  This only wakes the loop to
/// drain the input queue; whether anything is redrawn is a separate
/// decision made below.
const INPUT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Floor between redraws driven by new dtrace records.  Each upstairs
/// reports once a second, so on a sled running dozens of them the
/// records arrive often enough to repaint the screen continuously.  The
/// numbers still only change once a second, so there is nothing to see
/// for the effort.
const REDRAW_MIN_INTERVAL: Duration = Duration::from_millis(250);

/// Redraw at least this often even when nothing has arrived, so the
/// clock and the stale markers stay current.
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// How many trailing lines of the dtrace command's stderr to keep for
/// reporting.  We only need enough to show why it gave up.
const MAX_STDERR_LINES: usize = 5;

/// What the dtrace command is doing.
///
/// An empty table is ambiguous: it means either that no upstairs is
/// running or that dtrace never started.  The latter is easy to hit,
/// because dtrace needs privileges and exits right away without them,
/// so the display reports which one it is.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum ReaderStatus {
    /// Started, but no probe record has arrived yet.
    #[default]
    Waiting,
    /// At least one record has been parsed.
    Running,
    /// The command is no longer running.  The string says why.
    Stopped(String),
}

/// The status line to show under the header, or None once records are
/// arriving and the table speaks for itself.
fn reader_status_line(status: &ReaderStatus) -> Option<String> {
    match status {
        ReaderStatus::Waiting => {
            Some("waiting for dtrace output...".to_string())
        }
        ReaderStatus::Running => None,
        ReaderStatus::Stopped(message) => Some(message.clone()),
    }
}

/// Session ids in the order they are displayed.
fn sorted_session_ids(state: &CtopState) -> Vec<String> {
    let mut sessions: Vec<&SessionData> = state.sessions.values().collect();
    sessions.sort_by_key(|s| (s.pid, &s.dtrace_info.session_id));
    sessions
        .into_iter()
        .map(|s| s.dtrace_info.session_id.clone())
        .collect()
}

/// Move the selection `delta` rows through the displayed order.
fn move_selection(state: &mut CtopState, delta: isize) {
    let ids = sorted_session_ids(state);
    if ids.is_empty() {
        return;
    }

    let current = state
        .selected_session
        .as_ref()
        .and_then(|id| ids.iter().position(|s| s == id))
        .unwrap_or(0);

    let last = ids.len() as isize - 1;
    let next = (current as isize).saturating_add(delta).clamp(0, last);
    state.selected_session = Some(ids[next as usize].clone());
}

/// What the display loop should do after a key.
enum KeyAction {
    Quit,
    /// The key changed something, so redraw without waiting.
    Redraw,
    Ignored,
}

/// Apply one key press to the shared state.
async fn handle_key(
    key_event: KeyEvent,
    state: &Arc<RwLock<CtopState>>,
    page_size: usize,
) -> KeyAction {
    let mut state_guard = state.write().await;
    // Navigation only applies to the table.
    let navigable = !state_guard.detail_mode;

    match key_event {
        KeyEvent {
            code: KeyCode::Char('q'),
            modifiers: KeyModifiers::NONE,
            ..
        } => return KeyAction::Quit,
        KeyEvent {
            code: KeyCode::Char('c'),
            modifiers: KeyModifiers::CONTROL,
            ..
        } => return KeyAction::Quit,
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
            state_guard.normalize_detail = !state_guard.normalize_detail;
        }
        KeyEvent {
            code: KeyCode::Up,
            modifiers: KeyModifiers::NONE,
            ..
        } if navigable => move_selection(&mut state_guard, -1),
        KeyEvent {
            code: KeyCode::Down,
            modifiers: KeyModifiers::NONE,
            ..
        } if navigable => move_selection(&mut state_guard, 1),
        KeyEvent {
            code: KeyCode::PageUp,
            modifiers: KeyModifiers::NONE,
            ..
        } if navigable => {
            move_selection(&mut state_guard, -(page_size as isize))
        }
        KeyEvent {
            code: KeyCode::PageDown,
            modifiers: KeyModifiers::NONE,
            ..
        } if navigable => move_selection(&mut state_guard, page_size as isize),
        KeyEvent {
            code: KeyCode::Esc,
            modifiers: KeyModifiers::NONE,
            ..
        } => {
            // Exit detail mode
            state_guard.detail_mode = false;
        }
        _ => return KeyAction::Ignored,
    }

    KeyAction::Redraw
}

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
    /// Session id of the selected row.  Keyed on the session rather
    /// than its position, so sessions coming and going above the
    /// cursor do not move it onto a different session.
    selected_session: Option<String>,
    detail_mode: bool,
    normalize_detail: bool, // Use global min/max for detail view scaling
    reader_status: ReaderStatus,
}

/// Render a sparkline from delta history
///
/// Uses Unicode block characters to show trend: ▁▂▃▄▅▆▇█
///
/// One column per recorded sample, newest at the right, older values
/// scrolling left and the left padded with spaces when there are fewer
/// samples than columns.  The axis counts samples rather than time: a
/// session that stops reporting records nothing, so its sparkline
/// holds its shape rather than showing a gap.
///
/// Values are scaled against `global_max`, which the caller takes over
/// every session so that activity can be compared between rows.
fn render_sparkline(
    history: &VecDeque<u64>,
    width: usize,
    global_max: u64,
) -> String {
    if history.is_empty() || width == 0 {
        return " ".repeat(width);
    }

    // Unicode block characters from lowest to highest
    const BLOCKS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

    // Take last 'width' samples, reverse to show oldest->newest (left->right)
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
        return " ".repeat(width);
    }

    // Use global max for scaling (minimum 1 to avoid division by zero)
    let max = global_max.max(1);

    // Map each value to a block character
    let sparkline: String = samples
        .iter()
        .map(|&val| {
            if val == 0 {
                BLOCKS[0]
            } else {
                let normalized = (val as f64 / max as f64 * 7.0) as usize;
                BLOCKS[normalized.min(7)]
            }
        })
        .collect();

    // Right-align: pad left with spaces if we have fewer samples than width
    if sparkline.chars().count() < width {
        let padding = width - sparkline.chars().count();
        format!("{}{}", " ".repeat(padding), sparkline)
    } else {
        sparkline
    }
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

    // Execute the command through a shell to properly handle quoting.
    // kill_on_drop so the dtrace child does not outlive us.
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(&dtrace_cmd)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let stdout = child
        .stdout
        .take()
        .ok_or("Failed to capture subprocess stdout")?;
    let stderr = child
        .stderr
        .take()
        .ok_or("Failed to capture subprocess stderr")?;

    // Drain stderr in the background, keeping the last few lines.  This
    // has to be drained rather than ignored, or a chatty command would
    // block once the pipe filled.  dtrace explains itself here, so this
    // is the text worth showing if the command gives up.
    let stderr_task = tokio::spawn(async move {
        let mut stderr_lines = BufReader::new(stderr).lines();
        let mut tail: VecDeque<String> = VecDeque::new();
        while let Ok(Some(line)) = stderr_lines.next_line().await {
            tail.push_back(line);
            if tail.len() > MAX_STDERR_LINES {
                tail.pop_front();
            }
        }
        tail
    });

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
        state_guard.reader_status = ReaderStatus::Running;

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

        // Jobs issued since this session's previous record.  The
        // upstairs fires the probe once a second, so this is a rate
        // per second as long as records keep arriving.
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

    // stdout has closed, so the command is finished one way or another.
    // Report why: an empty display otherwise looks the same whether
    // dtrace is running with nothing to show or never started at all.
    let stderr_tail = stderr_task.await.unwrap_or_default();
    let reason = stderr_tail
        .iter()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join("; ");

    let message = match child.wait().await {
        Ok(status) if reason.is_empty() => {
            format!("dtrace command exited ({status})")
        }
        Ok(status) => format!("dtrace command exited ({status}): {reason}"),
        Err(e) => format!("dtrace command failed: {e}"),
    };

    state.write().await.reader_status = ReaderStatus::Stopped(message);
    notify.notify_one();

    Ok(())
}

/// Render full-screen detail view for a selected session
fn render_detail_view(
    session_data: &SessionData,
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    global_min: Option<u64>,
    global_max: Option<u64>,
    normalize: bool,
) -> io::Result<()> {
    // Calculate statistics (oldest on left, newest on right)
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

    // Normalizing scales the graph to every session rather than just
    // this one, so that two sessions can be compared by eye.
    let (display_min, display_max) = if normalize {
        (
            global_min.unwrap_or(session_min),
            global_max.unwrap_or(session_max),
        )
    } else {
        (session_min, session_max)
    };

    // Labels for the y axis, high to low.  Deduplicated because a
    // session sitting at one value collapses them all onto each other.
    let y_range = display_max as f64 - display_min as f64;
    let mut y_labels: Vec<u64> = vec![
        display_max,
        display_min + (y_range * 0.75) as u64,
        display_min + (y_range * 0.5) as u64,
        display_min + (y_range * 0.25) as u64,
        display_min,
    ];
    y_labels.dedup();

    // Width the labels need, plus a column of gap before the plot.
    let label_width = y_labels
        .iter()
        .map(|v| v.to_string().chars().count())
        .max()
        .unwrap_or(1) as f64
        + 1.0;

    // Render using ratatui (terminal is reused, ratatui handles diffing)
    terminal.draw(|f| {
        let area = f.area();

        // A line of session data on top, the graph in the middle, and
        // the keys on the bottom line where the table view puts them.
        let chunks = Layout::default()
            .constraints([
                Constraint::Length(1),
                Constraint::Min(0),
                Constraint::Length(1),
            ])
            .split(area);

        // Format the session data row
        let display_fields = default_display_fields();
        let row_data = format_row(
            session_data.pid,
            &session_data.dtrace_info,
            session_data.current_delta,
            &display_fields,
        );

        // Render session data at top
        let data_paragraph = Paragraph::new(row_data);
        f.render_widget(data_paragraph, chunks[0]);

        // Create title
        let session_short: String = session_data
            .dtrace_info
            .session_id
            .chars()
            .take(8)
            .collect();
        let mode_str = if normalize { " [NORMALIZED]" } else { "" };
        let title = format!(
            " Delta History - PID {} - Session {}{} ",
            session_data.pid, session_short, mode_str
        );

        // Give the y axis labels a strip of the plot to themselves by
        // extending the x range to the left of the first sample, so
        // they no longer sit on top of the oldest data points.
        let samples = history.len().max(1) as f64;
        let plot_width = chunks[1].width.saturating_sub(2) as f64;
        let x_min = if plot_width > label_width + 1.0 {
            -label_width * (samples / (plot_width - label_width))
        } else {
            0.0
        };

        // Min and max describe this session, which in normalized mode
        // is not what the axis is scaled to, so say what the scale is
        // rather than leaving the two looking like they disagree.
        let stats = format!(
            " Samples: {} | Min: {} | Max: {} | Avg: {} | Current: {} \
             | Scale: {}-{} ",
            history.len(),
            session_min,
            session_max,
            avg,
            current,
            display_min,
            display_max,
        );

        let canvas = Canvas::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title)
                    .title_bottom(stats),
            )
            .x_bounds([x_min, samples])
            .y_bounds([display_min as f64, display_max as f64])
            .paint(|ctx| {
                for y_val in &y_labels {
                    ctx.print(
                        x_min,
                        *y_val as f64,
                        ratatui::text::Span::styled(
                            format!("{y_val}"),
                            ratatui::style::Style::default().fg(Color::Gray),
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

        f.render_widget(
            Paragraph::new(
                "['d'/Esc: Back | 'n': Toggle normalize | 'q': Quit]",
            ),
            chunks[2],
        );
    })?;

    Ok(())
}

/// Draw the session table.
///
/// Returns how many session rows fit, which page up/down use as their
/// page size.
#[allow(clippy::too_many_arguments)]
fn render_table_view(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    sessions: &[&SessionData],
    display_fields: &[DtraceDisplay],
    table_state: &mut TableState,
    reader_status: &ReaderStatus,
    now: Instant,
    timestamp: u64,
    global_max: u64,
) -> io::Result<usize> {
    let selected = table_state.selected();
    let mut visible_rows = 1;

    terminal.draw(|f| {
        let chunks = Layout::default()
            .constraints([
                Constraint::Length(1), // timestamp
                Constraint::Length(1), // dtrace command status
                Constraint::Min(0),    // session table
                Constraint::Length(1), // key help
            ])
            .split(f.area());

        f.render_widget(
            Paragraph::new(format!("ctop - Unix timestamp: {timestamp}")),
            chunks[0],
        );
        f.render_widget(
            Paragraph::new(
                reader_status_line(reader_status).unwrap_or_default(),
            ),
            chunks[1],
        );

        let area = chunks[2];
        // One line of the table area goes to the column header.
        visible_rows = (area.height as usize).saturating_sub(1).max(1);

        // Rows carry a one character selected/stale indicator, so pad
        // the header by the same amount to keep the columns lined up.
        let header = format!(" {}", format_header(display_fields));
        // Whatever width the columns do not use goes to the sparkline.
        let spark_width =
            (area.width as usize).saturating_sub(header.chars().count());

        let rows: Vec<Row> = sessions
            .iter()
            .enumerate()
            .map(|(idx, s)| {
                let stale = now.duration_since(s.last_updated)
                    > Duration::from_secs(STALE_THRESHOLD_SECS);

                // Selection wins over the stale marker.
                let indicator = if Some(idx) == selected {
                    '>'
                } else if stale {
                    '*'
                } else {
                    ' '
                };

                let row = format_row(
                    s.pid,
                    &s.dtrace_info,
                    s.current_delta,
                    display_fields,
                );
                let spark =
                    render_sparkline(&s.delta_history, spark_width, global_max);

                Row::new(vec![format!("{indicator}{row}{spark}")])
            })
            .collect();

        // One full width column: format_row has already laid the row
        // out, and the table clips it to the area instead of letting a
        // row wider than the terminal wrap and push the layout apart.
        let table = Table::new(rows, [Constraint::Min(0)])
            .header(Row::new(vec![header]))
            .column_spacing(0);

        // Rendering with the state lets the table scroll itself to keep
        // the selected row on screen.
        f.render_stateful_widget(table, area, table_state);

        // Key help on the left, position in the list on the right.
        let position = match (selected, sessions.len()) {
            (_, 0) => " no sessions".to_string(),
            (Some(i), n) => format!(" [{}/{}]", i + 1, n),
            (None, n) => format!(" [{n}]"),
        };
        let footer = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Min(0),
                Constraint::Length(position.chars().count() as u16),
            ])
            .split(chunks[3]);

        f.render_widget(
            Paragraph::new(format!(
                "[↑↓/PgUp/PgDn: Navigate | 'd': Details | 'q': Quit] \
                 > = selected, * = stale ({STALE_THRESHOLD_SECS}s)"
            )),
            footer[0],
        );
        f.render_widget(Paragraph::new(position), footer[1]);
    })?;

    Ok(visible_rows)
}

/// Display task - renders the screen and handles keyboard input
///
/// This takes over the terminal, so it is responsible for handing it
/// back.  The loop runs in `display_loop` so that the terminal is
/// restored whether that returns normally or with an error.
async fn display_task(
    state: Arc<RwLock<CtopState>>,
    notify: Arc<Notify>,
) -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    execute!(io::stdout(), EnterAlternateScreen)?;

    // Restore the terminal on the way out of a panic as well.
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        let _ = disable_raw_mode();
        original_hook(panic_info);
    }));

    let result = display_loop(state, notify).await;

    let _ = execute!(io::stdout(), LeaveAlternateScreen);
    let _ = disable_raw_mode();

    result
}

/// Redraw and handle input until the user quits.
///
/// Both views draw through the one terminal, which clips whatever it is
/// given to the area it has and follows terminal resizes on its own.
///
/// Drawing is paced rather than done on every wake-up.  Input is
/// answered immediately so the display keeps up with the keyboard, but
/// arriving records only get a repaint every REDRAW_MIN_INTERVAL, and a
/// quiet screen still refreshes every REFRESH_INTERVAL to keep the
/// clock and the stale markers honest.
async fn display_loop(
    state: Arc<RwLock<CtopState>>,
    notify: Arc<Notify>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let display_fields = default_display_fields();
    let mut table_state = TableState::default();
    let mut page_size = 1;

    let mut records_pending = true;
    let mut last_draw = Instant::now();

    loop {
        tokio::select! {
            _ = notify.notified() => {
                records_pending = true;
            }
            _ = tokio::time::sleep(INPUT_POLL_INTERVAL) => {}
        }

        // Take everything the keyboard has already queued.  Handling
        // one key per pass would let a held down arrow build a backlog
        // that keeps scrolling after the key is released.
        let mut input_pending = false;
        while event::poll(Duration::ZERO)? {
            match event::read()? {
                Event::Key(key_event) => {
                    match handle_key(key_event, &state, page_size).await {
                        KeyAction::Quit => return Ok(()),
                        KeyAction::Redraw => input_pending = true,
                        KeyAction::Ignored => {}
                    }
                }
                // ratatui resizes itself on the next draw, we just have
                // to know that one is needed.
                Event::Resize(..) => input_pending = true,
                _ => {}
            }
        }

        // Input redraws at once; records wait for the floor; and an
        // idle screen still refreshes on its own.
        let since_draw = last_draw.elapsed();
        let draw = input_pending
            || (records_pending && since_draw >= REDRAW_MIN_INTERVAL)
            || since_draw >= REFRESH_INTERVAL;
        if !draw {
            // Leave records_pending set so the next pass can draw them.
            continue;
        }
        records_pending = false;
        last_draw = Instant::now();

        let now = Instant::now();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Drop sessions that stopped reporting a while ago.  The
        // selection names a session rather than a row, so it only has
        // to move when the selected session is the one that went away,
        // and then it takes over whatever slid into its place.
        {
            let mut state_guard = state.write().await;

            let previous_position =
                state_guard.selected_session.as_ref().and_then(|id| {
                    sorted_session_ids(&state_guard)
                        .iter()
                        .position(|s| s == id)
                });

            state_guard.sessions.retain(|_, session_data| {
                now.duration_since(session_data.last_updated)
                    <= Duration::from_secs(REMOVE_THRESHOLD_SECS)
            });

            let selection_gone = state_guard
                .selected_session
                .as_ref()
                .is_some_and(|id| !state_guard.sessions.contains_key(id));

            if selection_gone || state_guard.selected_session.is_none() {
                let remaining = sorted_session_ids(&state_guard);
                let slot = previous_position
                    .unwrap_or(0)
                    .min(remaining.len().saturating_sub(1));
                state_guard.selected_session = remaining.get(slot).cloned();
            }
        }

        {
            let state_guard = state.read().await;
            let mut sessions: Vec<&SessionData> =
                state_guard.sessions.values().collect();
            sessions.sort_by_key(|s| (s.pid, &s.dtrace_info.session_id));

            let selected =
                state_guard.selected_session.as_ref().and_then(|id| {
                    sessions
                        .iter()
                        .position(|s| &s.dtrace_info.session_id == id)
                });

            if state_guard.detail_mode {
                // Min and max over every session, for the normalized
                // view that compares one session against the rest.
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

                if let Some(session) = selected.and_then(|i| sessions.get(i)) {
                    render_detail_view(
                        session,
                        &mut terminal,
                        global_min,
                        global_max,
                        state_guard.normalize_detail,
                    )?;
                }
            } else {
                // Scale every sparkline the same way so activity can be
                // compared between rows.
                let global_max = sessions
                    .iter()
                    .flat_map(|s| s.delta_history.iter())
                    .copied()
                    .max()
                    .unwrap_or(1);

                table_state.select(selected);

                page_size = render_table_view(
                    &mut terminal,
                    &sessions,
                    &display_fields,
                    &mut table_state,
                    &state_guard.reader_status,
                    now,
                    timestamp,
                    global_max,
                )?;
            }
        }
    }
}

/// Main entry point for ctop
pub async fn ctop_loop(
    dtrace_cmd: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(RwLock::new(CtopState::default()));
    let notify = Arc::new(Notify::new());

    let state_reader = Arc::clone(&state);
    let notify_reader = Arc::clone(&notify);
    let state_err = Arc::clone(&state);
    let notify_err = Arc::clone(&notify);

    // Spawn subprocess reader task.  The display owns the screen, so a
    // failure to even start the command is recorded in shared state
    // instead of printed, which would land in the alternate screen
    // buffer and be lost.
    let reader_handle = tokio::spawn(async move {
        // Render the error before awaiting the lock: the boxed error is
        // not Send, so it cannot be held across an await point.
        let failure =
            subprocess_reader_task(dtrace_cmd, state_reader, notify_reader)
                .await
                .err()
                .map(|e| format!("dtrace command failed: {e}"));

        if let Some(message) = failure {
            state_err.write().await.reader_status =
                ReaderStatus::Stopped(message);
            notify_err.notify_one();
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
    use super::*;

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

    #[test]
    fn test_render_sparkline_empty() {
        let history = VecDeque::new();
        let sparkline = render_sparkline(&history, 10, 100);

        // Empty history should return spaces to maintain right alignment
        assert_eq!(sparkline, "          "); // 10 spaces
    }

    #[test]
    fn test_render_sparkline_zero_width() {
        let mut history = VecDeque::new();
        history.push_back(10);
        history.push_back(20);

        let sparkline = render_sparkline(&history, 0, 100);
        assert_eq!(sparkline, ""); // Empty string for 0 width
    }

    #[test]
    fn test_render_sparkline_single_value() {
        let mut history = VecDeque::new();
        history.push_back(50);

        let sparkline = render_sparkline(&history, 10, 100);

        // Should have 10 characters total (9 spaces + 1 block, right-aligned)
        assert_eq!(sparkline.chars().count(), 10);
        // Last character should be the data value
        assert_ne!(sparkline.chars().last().unwrap(), ' ');
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

        // First character should be lower than last (oldest on left, newest on right)
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

        // Use width=2 to match data size (no padding)
        let sparkline = render_sparkline(&history, 2, 100);

        // Should use valid unicode block characters (no spaces since width matches data)
        for c in sparkline.chars() {
            assert!(
                ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'].contains(&c),
                "Invalid sparkline character: {}",
                c
            );
        }

        // Verify correct blocks: 0 maps to lowest, 100 maps to highest
        let chars: Vec<char> = sparkline.chars().collect();
        assert_eq!(chars[0], '▁', "Value 0 should map to lowest block");
        assert_eq!(
            chars[1], '█',
            "Value 100 (max) should map to highest block"
        );
    }

    /// Sparklines scale against a max taken over every session, so
    /// that activity can be compared between rows.  Auto-scaling each
    /// session to its own range would make that comparison meaningless.
    #[test]
    fn test_render_sparkline_normalization() {
        let mut history = VecDeque::new();
        history.push_back(50);
        history.push_back(100);

        // Test with global max = 200 (should scale differently than 100)
        // Use width=2 to avoid padding and test actual data
        let sparkline1 = render_sparkline(&history, 2, 200);
        let sparkline2 = render_sparkline(&history, 2, 100);

        // With higher global max, the values should appear relatively lower
        let chars1: Vec<char> = sparkline1.chars().collect();
        let chars2: Vec<char> = sparkline2.chars().collect();

        // Sparkline1 (max=200): value 100 is only halfway, so should use lower blocks
        // Sparkline2 (max=100): value 100 is the maximum, so should use highest block
        //
        // For value 100:
        //   normalized1 = 100/200 * 7 = 3.5 → index 3 = '▄'
        //   normalized2 = 100/100 * 7 = 7.0 → index 7 = '█'
        //
        // Therefore chars1[1] should be '▄' and chars2[1] should be '█'
        assert!(
            chars1[1] < chars2[1],
            "Expected normalization to affect block height: \
             sparkline1[1]='{}' should be < sparkline2[1]='{}'",
            chars1[1],
            chars2[1]
        );

        // Verify the actual characters are as expected
        assert_eq!(chars1[1], '▄', "Value 100 with max=200 should be ▄");
        assert_eq!(chars2[1], '█', "Value 100 with max=100 should be █");
    }

    #[test]
    fn test_ctop_state_default() {
        let state = CtopState::default();

        assert_eq!(state.sessions.len(), 0);
        assert!(state.selected_session.is_none());
        assert!(!state.detail_mode);
        assert!(!state.normalize_detail);
    }

    /// Build a state holding sessions with the given (pid, session id)
    /// pairs, so selection movement can be exercised.
    fn state_with(sessions: &[(u32, &str)]) -> CtopState {
        let info_json = r#"{
            "upstairs_id": "u", "session_id": "REPLACE", "up_count": 1,
            "up_counters": {
                "apply": 1, "action_downstairs": 1, "action_guest": 1,
                "action_deferred_block": 0, "action_deferred_message": 0,
                "action_flush_check": 0, "action_stat_check": 0,
                "action_control_check": 0, "action_noop": 0
            },
            "next_job_id": 1000, "ds_count": 3, "write_bytes_out": 1,
            "ds_state": ["Active", "Active", "Active"],
            "ds_io_count": {
                "in_progress": [0,0,0], "done": [0,0,0],
                "skipped": [0,0,0], "error": [0,0,0]
            },
            "ds_reconciled": 0, "ds_reconcile_needed": 0,
            "ds_reconcile_aborted": 0,
            "ds_live_repair_completed": [0,0,0],
            "ds_live_repair_aborted": [0,0,0],
            "ds_connected": [1,1,1], "ds_replaced": [0,0,0],
            "ds_extents_repaired": [0,0,0], "ds_extents_confirmed": [0,0,0],
            "ds_extent_limit": 0, "ds_delay_us": [0,0,0],
            "ds_ro_lr_skipped": [0,0,0]
        }"#;

        let mut state = CtopState::default();
        for (pid, id) in sessions {
            let dtrace_info: DtraceInfo =
                serde_json::from_str(&info_json.replace("REPLACE", id))
                    .unwrap();
            state.sessions.insert(
                id.to_string(),
                SessionData {
                    pid: *pid,
                    dtrace_info,
                    last_job_id: 0,
                    last_updated: Instant::now(),
                    current_delta: None,
                    delta_history: VecDeque::new(),
                },
            );
        }
        state
    }

    #[test]
    fn test_sorted_session_ids_orders_by_pid_then_session() {
        let state = state_with(&[(20, "bbb"), (10, "zzz"), (20, "aaa")]);
        assert_eq!(sorted_session_ids(&state), vec!["zzz", "aaa", "bbb"]);
    }

    #[test]
    fn test_move_selection_walks_and_clamps() {
        let mut state = state_with(&[(1, "a"), (2, "b"), (3, "c")]);

        // No selection yet starts from the top of the list.
        move_selection(&mut state, 1);
        assert_eq!(state.selected_session.as_deref(), Some("b"));

        move_selection(&mut state, 1);
        assert_eq!(state.selected_session.as_deref(), Some("c"));

        // Past either end just stops there rather than wrapping.
        move_selection(&mut state, 5);
        assert_eq!(state.selected_session.as_deref(), Some("c"));
        move_selection(&mut state, -99);
        assert_eq!(state.selected_session.as_deref(), Some("a"));
    }

    #[test]
    fn test_move_selection_with_no_sessions() {
        let mut state = state_with(&[]);
        move_selection(&mut state, 1);
        assert!(state.selected_session.is_none());
    }

    /// The point of keying the selection on the session id: a session
    /// appearing or expiring above the cursor must not slide the
    /// cursor onto a different session.
    #[test]
    fn test_selection_survives_sessions_coming_and_going() {
        let mut state = state_with(&[(1, "a"), (2, "b"), (3, "c")]);
        state.selected_session = Some("c".to_string());

        // A new session sorts in above the selected one.
        let added = state_with(&[(2, "a2")]);
        state.sessions.extend(added.sessions);
        assert_eq!(sorted_session_ids(&state), vec!["a", "a2", "b", "c"]);
        assert_eq!(state.selected_session.as_deref(), Some("c"));

        // And one above it goes away.
        state.sessions.remove("a");
        assert_eq!(state.selected_session.as_deref(), Some("c"));

        // Moving up from there lands on its actual neighbour.
        move_selection(&mut state, -1);
        assert_eq!(state.selected_session.as_deref(), Some("b"));
    }

    /// The delta history is a ring buffer: once it is full, the oldest
    /// sample is dropped rather than the buffer growing.
    #[test]
    fn test_session_data_delta_history_max_size() {
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
}
