// Copyright 2026 Oxide Computer Company

//! ctop - watch every crucible upstairs on the system.
//!
//! `cmon dtrace` reads the raw dtrace output from stdin, which means
//! running the dtrace script yourself and piping it in.  ctop runs the
//! equivalent command itself and reads its output.
//!
//! ctop keeps one row per session and updates it in place, so a screen
//! of upstairs can be watched at once.

use anyhow::{Context, Result, bail};
use clap::Parser;
use cmon_common::{
    DtraceDisplay, DtraceWrapper, default_display_fields, format_header,
    format_row, short_state,
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
    style::{Color, Style},
    text::{self, Span},
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

/// The dtrace command we run by default
///
/// This is `tools/dtrace/upstairs_raw.d` as a one liner, so ctop does
/// not have to find a script file on disk.  The differences from the
/// script: -Z so dtrace waits for a matching probe instead of failing
/// when no upstairs is running yet, and -q to keep dtrace from printing
/// a header of its own.
///
/// dtrace needs privileges, so ctop has to be started with them
/// (`pfexec ctop`).  Without them dtrace exits immediately, and ctop
/// reports why on the way out.
const DEFAULT_DTRACE_CMD: &str = r#"dtrace -Z -q -x strsize=2k -n 'crucible_upstairs*:::up-status { printf("{\"pid\":%d,\"status\":%s}\n", pid, json(copyinstr(arg1), "ok")); }'"#;

/// How often the display loop wakes to look for keyboard input.  This
/// only drains the input queue. The redraw is decided separately.
const INPUT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// How often the table is redrawn, showing whatever has arrived since
/// the last time.
///
/// Each upstairs reports once a second, but at whatever point in the
/// second it happens to fire, so redrawing as records land updates a
/// different few rows each time and the table never settles.  Drawing
/// on a tick of our own shows every row that reported, together.
///
/// It is also what keeps the clock and the stale marks honest: a
/// session goes stale by not reporting, so nothing else would fire to
/// mark it.
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// How many trailing lines of the dtrace command's stderr to keep.  We
/// only need enough to say why it gave up.
const MAX_STDERR_LINES: usize = 5;

/// How long a session can go without reporting before its row is
/// marked stale.  The upstairs fires the probe once a second, so a few
/// seconds of silence means something has its attention.
const STALE_THRESHOLD_SECS: u64 = 5;

/// How long a session can go without reporting before its row is
/// dropped.  An upstairs that has exited is never coming back, and its
/// row would otherwise sit there forever.
const REMOVE_THRESHOLD_SECS: u64 = 30;

/// How many job deltas to remember per session.  More than fits across
/// a wide terminal, so the sparkline has samples in hand for whatever
/// width it is given.
const MAX_DELTA_HISTORY: usize = 100;

#[derive(Parser, Debug)]
#[clap(name = "ctop", term_width = 80)]
#[clap(
    about = "Curses based crucible monitor",
    long_about = "Curses based crucible monitor.\n\n\
                  Runs a dtrace command and displays the up-status probe \
                  output from every crucible upstairs on the system.  \
                  dtrace requires privileges, so run this as `pfexec ctop`."
)]
struct Args {
    /// Command to run to generate dtrace output
    #[clap(long, default_value = DEFAULT_DTRACE_CMD)]
    dtrace_cmd: String,

    /// Fields to display from the dtrace output
    #[clap(
        short,
        long,
        value_delimiter = ',',
        default_values_t = default_display_fields()
    )]
    #[arg(value_enum)]
    output: Vec<DtraceDisplay>,
}

/// What the dtrace command is doing.
///
/// An empty table on its own is ambiguous, it means either that no
/// upstairs is running, or that dtrace never started. Say which it is.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
enum ReaderStatus {
    /// Started, but nothing has arrived yet.
    #[default]
    Waiting,

    /// At least one record has been read.
    Running,

    /// The command is no longer running.  The string says why.
    Stopped(String),
}

impl ReaderStatus {
    /// What to show under the clock, or None once records are arriving
    /// and the table speaks for itself.
    fn line(&self) -> Option<&str> {
        match self {
            ReaderStatus::Waiting => Some("waiting for dtrace output..."),
            ReaderStatus::Running => None,
            ReaderStatus::Stopped(why) => Some(why),
        }
    }
}

/// What the sparklines are measured against.
///
/// Both settings start at zero; they differ in what counts as a full
/// height bar.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum SparkScale {
    /// The busiest sample on screen, so the rows can be compared with
    /// each other.  A quiet session next to a busy one reads as flat.
    #[default]
    Global,

    /// Each session's own busiest sample, so every row fills its
    /// height and shows its shape.  Nothing can be read across rows: a
    /// session doing ten jobs a second looks like one doing ten
    /// thousand.
    PerSession,
}

impl SparkScale {
    fn toggled(self) -> Self {
        match self {
            SparkScale::Global => SparkScale::PerSession,
            SparkScale::PerSession => SparkScale::Global,
        }
    }

    /// What to call it on the footer.
    fn label(self) -> &'static str {
        match self {
            SparkScale::Global => "all",
            SparkScale::PerSession => "self",
        }
    }
}

/// Whether the downstairs state columns are drawn in color.
///
/// Allow the user to disable/enable color as they please.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum ColorMode {
    #[default]
    On,
    Off,
}

impl ColorMode {
    fn toggled(self) -> Self {
        match self {
            ColorMode::On => ColorMode::Off,
            ColorMode::Off => ColorMode::On,
        }
    }
}

/// The most recent record for one session and what we recorded from
/// the record before it.
#[derive(Debug)]
struct SessionData {
    pid: u32,
    dtrace_info: DtraceInfo,
    last_job_id: u64,
    current_delta: Option<u64>,

    /// When the last record for this session arrived, which is what
    /// makes a row stale and eventually removes it.
    last_updated: Instant,

    /// The last `MAX_DELTA_HISTORY` job deltas, oldest first, for the
    /// sparkline to draw.
    delta_history: VecDeque<u64>,
}

/// What the reader has collected, for the display to draw.
#[derive(Debug, Default)]
struct CtopState {
    /// Keyed on session id.  A pid can hold more than one session, and
    /// a session outlives no pid, so the session is the identity here.
    sessions: HashMap<String, SessionData>,

    /// The session name the cursor is on.  A row index would move the
    /// cursor onto a different session if one above expire or arrives.
    selected_session: Option<String>,

    /// What the sparklines are measured against.
    spark_scale: SparkScale,

    /// Whether the downstairs states are colored.
    color_mode: ColorMode,

    /// Whether the selected session's history has the screen to
    /// itself, rather than the table of every session.
    detail_mode: bool,

    /// What the dtrace command is doing, reported on screen.
    reader_status: ReaderStatus,
}

/// Run `dtrace_cmd` and record what it produces.
///
/// The command is run through a shell so the quoting in a dtrace one
/// liner survives, and with kill_on_drop so the child does not outlive
/// us.  Its stderr is captured so that a dtrace which gives up can
/// say why, since the display owns the screen by then.
///
/// Records for different sessions arrive interleaved, because the probe
/// matches every upstairs on the system.  A job ID only means anything
/// within its own session, which is why the last one is kept per
/// session rather than globally.
async fn reader_loop(
    dtrace_cmd: &str,
    state: &Arc<RwLock<CtopState>>,
    notify: &Notify,
) -> Result<()> {
    if dtrace_cmd.trim().is_empty() {
        bail!("empty dtrace command");
    }

    let mut child = Command::new("sh")
        .arg("-c")
        .arg(dtrace_cmd)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("failed to start the dtrace command")?;

    let stdout = child
        .stdout
        .take()
        .context("failed to capture the dtrace command's stdout")?;
    let stderr = child
        .stderr
        .take()
        .context("failed to capture the dtrace command's stderr")?;

    // Keep the last few lines of stderr.  This has to be drained rather
    // than ignored, or a chatty command would block once the pipe
    // filled.  dtrace explains itself here, and the display owns the
    // screen, so this text is the only way to say what went wrong.
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

    let mut lines = BufReader::new(stdout).lines();

    while let Some(line) = lines.next_line().await? {
        // dtrace can emit a blank line, and -Z means we may be running
        // before there is anything to report.
        if line.trim().is_empty() {
            continue;
        }

        let wrapper: DtraceWrapper = match serde_json::from_str(&line) {
            Ok(w) => w,
            // There is nowhere good to report this while the display
            // owns the screen, and one bad line is not worth stopping over.
            Err(_) => continue,
        };

        let job_id = wrapper.status.next_job_id.0;

        // This is scoped so the lock is dropped before the notify.
        {
            let mut state = state.write().await;
            state.reader_status = ReaderStatus::Running;

            match state.sessions.get_mut(&wrapper.status.session_id) {
                Some(session) => {
                    let delta = job_id.saturating_sub(session.last_job_id);

                    session.delta_history.push_back(delta);
                    if session.delta_history.len() > MAX_DELTA_HISTORY {
                        session.delta_history.pop_front();
                    }

                    session.current_delta = Some(delta);
                    session.last_job_id = job_id;
                    session.pid = wrapper.pid;
                    session.dtrace_info = wrapper.status;
                    session.last_updated = Instant::now();
                }
                // First record for this session, so there is nothing to
                // take a delta against yet, and nothing to plot.
                None => {
                    state.sessions.insert(
                        wrapper.status.session_id.clone(),
                        SessionData {
                            pid: wrapper.pid,
                            dtrace_info: wrapper.status,
                            last_job_id: job_id,
                            current_delta: None,
                            last_updated: Instant::now(),
                            delta_history: VecDeque::new(),
                        },
                    );
                }
            }
        }

        notify.notify_one();
    }

    // stdout closing means the command is finished one way or another.
    let status = child.wait().await.context("waiting for dtrace")?;
    if status.success() {
        return Ok(());
    }

    let reason = stderr_task
        .await
        .unwrap_or_default()
        .iter()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join("; ");

    if reason.is_empty() {
        bail!("dtrace command exited ({status})");
    }
    bail!("dtrace command exited ({status}): {reason}");
}

/// True if this key means "stop".
fn is_quit(key_event: KeyEvent) -> bool {
    matches!(
        key_event,
        KeyEvent {
            code: KeyCode::Char('q'),
            modifiers: KeyModifiers::NONE,
            ..
        } | KeyEvent {
            code: KeyCode::Char('c'),
            modifiers: KeyModifiers::CONTROL,
            ..
        }
    )
}

/// Apply one key.  Returns true if anything changed.
fn handle_key(key_event: KeyEvent, state: &mut CtopState) -> bool {
    match key_event {
        // 'd' goes both ways, Esc only comes back, so Esc in the table
        // is deliberately nothing rather than a way in.
        KeyEvent {
            code: KeyCode::Char('d'),
            modifiers: KeyModifiers::NONE,
            ..
        } => {
            state.detail_mode = !state.detail_mode;
            true
        }
        KeyEvent {
            code: KeyCode::Esc,
            modifiers: KeyModifiers::NONE,
            ..
        } if state.detail_mode => {
            state.detail_mode = false;
            true
        }
        // Only the table has sparklines, but setting the scale from
        // the detail view is harmless and takes effect on the way back.
        KeyEvent {
            code: KeyCode::Char('s'),
            modifiers: KeyModifiers::NONE,
            ..
        } => {
            state.spark_scale = state.spark_scale.toggled();
            true
        }
        KeyEvent {
            code: KeyCode::Char('c'),
            modifiers: KeyModifiers::NONE,
            ..
        } => {
            state.color_mode = state.color_mode.toggled();
            true
        }
        // The cursor only means anything next to the table it moves
        // through, so the arrows do nothing in the detail view.
        KeyEvent {
            code: KeyCode::Up,
            modifiers: KeyModifiers::NONE,
            ..
        } if !state.detail_mode => {
            move_selection(state, false);
            true
        }
        KeyEvent {
            code: KeyCode::Down,
            modifiers: KeyModifiers::NONE,
            ..
        } if !state.detail_mode => {
            move_selection(state, true);
            true
        }
        _ => false,
    }
}

/// Session ids in the order their rows are drawn.
fn sorted_session_ids(state: &CtopState) -> Vec<String> {
    let mut sessions: Vec<&SessionData> = state.sessions.values().collect();
    sessions.sort_by_key(|s| (s.pid, &s.dtrace_info.session_id));
    sessions
        .into_iter()
        .map(|s| s.dtrace_info.session_id.clone())
        .collect()
}

/// Move the cursor one row, stopping at either end.
fn move_selection(state: &mut CtopState, down: bool) {
    let ids = sorted_session_ids(state);

    let current = state
        .selected_session
        .as_ref()
        .and_then(|id| ids.iter().position(|s| s == id))
        .unwrap_or(0);

    let next = if down {
        current + 1
    } else {
        current.saturating_sub(1)
    };

    // get() rather than indexing: past the last row, or with no rows
    // at all, there is nowhere to go and the cursor stays put.
    if let Some(id) = ids.get(next) {
        state.selected_session = Some(id.clone());
    }
}

/// Put the cursor on a session that exists, if it is not on one.
///
/// Because the cursor names a session rather than a row, it only has
/// to move somewhere when that session goes away.  Going to the top
/// is predictable which is worth more than writing a bunch of code
/// to figure out what is the best place to go.
fn reselect_if_gone(state: &mut CtopState) {
    let still_here = state
        .selected_session
        .as_ref()
        .is_some_and(|id| state.sessions.contains_key(id));

    if !still_here {
        state.selected_session = sorted_session_ids(state).first().cloned();
    }
}

/// Draw a session's delta history as one block character per sample.
///
/// Newest sample at the right, older ones trailing off to the left,
/// and the left padded with spaces when there are fewer samples than
/// columns.  The axis counts samples rather than time: a session that
/// stops reporting adds nothing, so its sparkline holds its shape
/// rather than showing a gap.  This is something to make better in
/// future updates.  It's also.. complicated.. as we would have to decide
/// at what timeout have we given up on a session reporting, 3 seconds? 5?
/// So, for now, I'm punting that decision to later.
///
/// Heights are scaled against `global_max` taken across every session
/// on screen.  A busy row looks busier than a quiet one rather than
/// every row filling its own range.
fn render_sparkline(
    history: &VecDeque<u64>,
    width: usize,
    global_max: u64,
) -> String {
    const BLOCKS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

    // A max of zero means every sample is zero, and dividing by it
    // would not go well.
    let max = global_max.max(1);

    // The newest `width` samples, oldest first.
    let bars: String = history
        .iter()
        .skip(history.len().saturating_sub(width))
        .map(|&value| {
            let step = (value as f64 / max as f64 * 7.0) as usize;
            BLOCKS[step.min(BLOCKS.len() - 1)]
        })
        .collect();

    // Right align, so the newest sample sits in the same column on
    // every row however much history each one has.
    format!("{bars:>width$}")
}

/// Has this session gone quiet long enough to mark its row?
fn is_stale(session: &SessionData, now: Instant) -> bool {
    now.duration_since(session.last_updated)
        > Duration::from_secs(STALE_THRESHOLD_SECS)
}

/// Has this session gone quiet long enough to drop its row?
fn is_expired(session: &SessionData, now: Instant) -> bool {
    now.duration_since(session.last_updated)
        > Duration::from_secs(REMOVE_THRESHOLD_SECS)
}

/// The color for a downstairs state. None to leave it plain.
fn ds_state_color(ds_state: &str) -> Option<Color> {
    match ds_state {
        "Active" => Some(Color::Green),
        "WaitQuorum" | "Reconcile" | "LiveRepairReady" | "LiveRepair"
        | "Replacing" | "Replaced" | "Offline" => Some(Color::Yellow),
        "Faulted" | "Fault" | "NegotiationFailed" | "Disabled" => {
            Some(Color::Red)
        }
        "New" | "Deactivated" => Some(Color::DarkGray),
        _ => None,
    }
}

/// Produce the same text as `format_row`, but broken into pieces so the
/// DS0-DS2 cells can each have their own color.
///
/// In ratatui, a `Span` is a piece of text with one style: a color,
/// bold, and so on.  A screen line (`Line`) is a list of spans drawn
/// one after another.  `format_row` returns one `String`, which can only
/// be drawn in one style.  To color part of a row, the row has to be
/// split into separate spans.
///
/// 1. Color off: return the whole `format_row` output as one span with
///    no color.
/// 2. Color on: go through the fields the user asked for with `-o`, one
///    at a time, and build the row in pieces:
///    - Any field except State: call `format_row` with just that one
///      field and add the result as one plain span.  For example, PID
///      gives `"  2101"`.
///    - The State field: don't call `format_row` for this one.  Loop
///      over the three downstairs states and make one span for each:
///      - The text is `format!(" {:>3}", short_state(state))`, so
///        `"Active"` becomes `" ACT"` and `"LiveRepair"` becomes
///        `"  LR"`.
///      - The color comes from `ds_state_color(state)`, which looks at
///        the full state name.  An unknown state gets no color.
/// 3. Return the list of spans.  The caller adds the cursor and stale
///    marker in front and the sparkline after, then draws them as one
///    line.
fn row_spans(
    pid: u32,
    info: &DtraceInfo,
    delta: Option<u64>,
    fields: &[DtraceDisplay],
    color_mode: ColorMode,
) -> Vec<Span<'static>> {
    if color_mode == ColorMode::Off {
        return vec![Span::raw(format_row(pid, info, delta, fields))];
    }

    let mut spans = Vec::new();
    for field in fields {
        if *field != DtraceDisplay::State {
            spans.push(Span::raw(format_row(
                pid,
                info,
                delta,
                std::slice::from_ref(field),
            )));
            continue;
        }

        for state in &info.ds_state {
            let style = match ds_state_color(state) {
                Some(color) => Style::default().fg(color),
                None => Style::default(),
            };
            spans.push(Span::styled(
                format!(" {:>3}", short_state(state)),
                style,
            ));
        }
    }
    spans
}

/// Give one session's delta history the whole screen.
///
/// The sparkline in the table is a handful of columns; this is the
/// same numbers with room to see them, scaled to this session's own
/// range because there is only one session on screen to compare.
fn render_detail_view(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    session: &SessionData,
    display_fields: &[DtraceDisplay],
    color_mode: ColorMode,
) -> io::Result<()> {
    let history: Vec<u64> = session.delta_history.iter().copied().collect();

    let low = history.iter().copied().min().unwrap_or(0);
    let high = history.iter().copied().max().unwrap_or(1);
    let average = if history.is_empty() {
        0
    } else {
        history.iter().sum::<u64>() / history.len() as u64
    };

    // A session sitting at one value has no range to plot in, and a
    // zero height axis draws nothing at all.
    let (y_low, y_high) = if low == high {
        (low, high + 1)
    } else {
        (low, high)
    };

    // Labels down the y axis, high to low, taken from the data rather
    // than from the axis above.  A flat session's axis was widened to
    // give its line somewhere to sit, and labelling that widened bound
    // would put a number on the axis the session never reached.
    //
    // Deduplicated because a narrow range collapses them onto each
    // other, and a flat one collapses them all onto its single value.
    let span = (high - low) as f64;
    let mut y_labels: Vec<u64> = vec![
        high,
        low + (span * 0.75) as u64,
        low + (span * 0.5) as u64,
        low + (span * 0.25) as u64,
        low,
    ];
    y_labels.dedup();

    let label_width = y_labels
        .iter()
        .map(|v| v.to_string().chars().count())
        .max()
        .unwrap_or(1) as f64
        + 1.0;

    terminal.draw(|f| {
        // The session's own row on top, the graph in the middle, and
        // the keys on the bottom line where the table view puts them.
        let chunks = Layout::default()
            .constraints([
                Constraint::Length(2),
                Constraint::Min(0),
                Constraint::Length(1),
            ])
            .split(f.area());

        // Neither line is padded here.  The table pads both by the
        // width of its indicator columns.  As this view has no
        // indicators we don't need padding.
        f.render_widget(
            Paragraph::new(vec![
                text::Line::from(format_header(display_fields)),
                text::Line::from(row_spans(
                    session.pid,
                    &session.dtrace_info,
                    session.current_delta,
                    display_fields,
                    color_mode,
                )),
            ]),
            chunks[0],
        );

        let session_short: String =
            session.dtrace_info.session_id.chars().take(8).collect();
        let title = format!(
            " Job rate - PID {} - Session {} ",
            session.pid, session_short
        );
        let stats = format!(
            " Samples: {} | Min: {} | Max: {} | Avg: {} | Current: {} ",
            history.len(),
            low,
            high,
            average,
            session.current_delta.unwrap_or(0),
        );

        // The y axis labels are printed inside the canvas, so at x=0
        // they would land on top of the oldest samples.  Start the x
        // range left of sample zero to give them a gutter: solving
        //   -x_min / (samples - x_min) = label_width / plot_width
        // for x_min makes that gutter label_width columns wide.
        let samples = history.len().max(1) as f64;
        let plot_width = chunks[1].width.saturating_sub(2) as f64;
        let x_min = if plot_width > label_width + 1.0 {
            -label_width * samples / (plot_width - label_width)
        } else {
            0.0
        };

        let canvas = Canvas::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title)
                    .title_bottom(stats),
            )
            .x_bounds([x_min, samples])
            .y_bounds([y_low as f64, y_high as f64])
            .paint(|ctx| {
                for label in &y_labels {
                    ctx.print(
                        x_min,
                        *label as f64,
                        Span::styled(
                            format!("{label}"),
                            Style::default().fg(Color::Gray),
                        ),
                    );
                }

                // windows(2) yields nothing for a single sample, which
                // is the right amount of line to draw for one point.
                for (i, pair) in history.windows(2).enumerate() {
                    ctx.draw(&Line {
                        x1: i as f64,
                        y1: pair[0] as f64,
                        x2: (i + 1) as f64,
                        y2: pair[1] as f64,
                        color: Color::Cyan,
                    });
                }

                for (i, &value) in history.iter().enumerate() {
                    ctx.draw(&Points {
                        coords: &[(i as f64, value as f64)],
                        color: Color::Yellow,
                    });
                }
            });

        f.render_widget(canvas, chunks[1]);

        f.render_widget(
            Paragraph::new("['d'/Esc: Back | 'c': Color | 'q': Quit]"),
            chunks[2],
        );
    })?;

    Ok(())
}

/// Draw one frame: a clock, a row per session, and the keys.
///
/// `now` is passed in rather than read here so that every row in a
/// frame is judged stale against the same instant.  The settings and
/// the reader's status come from `state`; the rows come from
/// `sessions`, which is `state.sessions` already sorted.
fn render_table_view(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: &CtopState,
    sessions: &[&SessionData],
    display_fields: &[DtraceDisplay],
    table_state: &mut TableState,
    now: Instant,
) -> io::Result<()> {
    let spark_scale = state.spark_scale;
    let color_mode = state.color_mode;
    let reader_status = &state.reader_status;

    // The busiest sample on screen, which is what the Global setting
    // measures against.  Derived from the sessions being drawn rather
    // than passed in, since nothing else needs it.
    let global_max = sessions
        .iter()
        .flat_map(|s| s.delta_history.iter())
        .copied()
        .max()
        .unwrap_or(1);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    terminal.draw(|f| {
        let chunks = Layout::default()
            .constraints([
                Constraint::Length(1), // timestamp
                Constraint::Length(1), // what dtrace is doing
                Constraint::Min(0),    // session table
                Constraint::Length(1), // key help
            ])
            .split(f.area());

        f.render_widget(
            Paragraph::new(format!("ctop - Unix timestamp: {timestamp}")),
            chunks[0],
        );
        f.render_widget(
            Paragraph::new(reader_status.line().unwrap_or_default()),
            chunks[1],
        );

        let selected = table_state.selected();

        // Rows carry two indicator characters, so the header is padded
        // by the same amount to keep the columns lined up.
        let header = format!("  {}", format_header(display_fields));

        // Whatever width the columns do not use goes to the sparkline.
        let spark_width =
            (chunks[2].width as usize).saturating_sub(header.chars().count());

        let rows: Vec<Row> = sessions
            .iter()
            .enumerate()
            .map(|(idx, s)| {
                // A column for selected row and a column for status
                let cursor = if Some(idx) == selected { '>' } else { ' ' };
                let stale = if is_stale(s, now) { '*' } else { ' ' };

                // Both settings measure up from zero; they differ in
                // what a full height bar means.
                let max = match spark_scale {
                    SparkScale::Global => global_max,
                    SparkScale::PerSession => {
                        s.delta_history.iter().copied().max().unwrap_or(1)
                    }
                };
                let spark =
                    render_sparkline(&s.delta_history, spark_width, max);

                let mut spans = vec![Span::raw(format!("{cursor}{stale}"))];
                spans.extend(row_spans(
                    s.pid,
                    &s.dtrace_info,
                    s.current_delta,
                    display_fields,
                    color_mode,
                ));
                spans.push(Span::raw(spark));
                Row::new(vec![text::Line::from(spans)])
            })
            .collect();

        // One full width column: format_row has already laid the row
        // out, and the table clips it to the area instead of letting a
        // row wider than the terminal wrap and push the layout apart.
        let table = Table::new(rows, [Constraint::Min(0)])
            .header(Row::new(vec![header]))
            .column_spacing(0);

        // Rendering with the state lets the table scroll itself to keep
        // the cursor on screen when there are more sessions than rows.
        f.render_stateful_widget(table, chunks[2], table_state);

        // Keys on the left, where the cursor is on the right.
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

        // Kept short enough to fit eighty columns with the position, so
        // the stale threshold is left to the README.
        f.render_widget(
            Paragraph::new(format!(
                "[up/down: Move | 's': Scale | 'c': Color | 'q': Quit]  \
                 scale: {}  *=stale",
                spark_scale.label(),
            )),
            footer[0],
        );
        f.render_widget(Paragraph::new(position), footer[1]);
    })?;

    Ok(())
}

/// Redraw and handle input until the user quits or the reader stops.
async fn display_loop(
    state: &Arc<RwLock<CtopState>>,
    notify: &Notify,
    display_fields: &[DtraceDisplay],
) -> Result<()> {
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let mut table_state = TableState::default();

    let mut last_draw = Instant::now();

    // The first pass paints, so there is something on screen before
    // anything has happened.
    let mut needs_draw = true;

    loop {
        // One instant for the whole frame, so every row is judged
        // against the same clock.
        let now = Instant::now();

        if needs_draw {
            let mut state = state.write().await;

            // Drop sessions that stopped reporting a while ago.  An
            // upstairs that has gone is not coming back, and its row
            // would otherwise sit here for the life of the program.
            state
                .sessions
                .retain(|_, session| !is_expired(session, now));

            reselect_if_gone(&mut state);

            // HashMap order is arbitrary, so sort or the rows shuffle
            // themselves on every frame.
            let mut sessions: Vec<&SessionData> =
                state.sessions.values().collect();
            sessions.sort_by_key(|s| (s.pid, &s.dtrace_info.session_id));

            // The cursor names a session; the table wants a row.
            table_state.select(state.selected_session.as_ref().and_then(
                |id| {
                    sessions
                        .iter()
                        .position(|s| &s.dtrace_info.session_id == id)
                },
            ));

            // The detail view needs a session to show.  With no rows
            // there is nothing to detail, so fall back to the table
            // rather than an empty graph.
            let detail = state
                .detail_mode
                .then(|| table_state.selected())
                .flatten()
                .and_then(|i| sessions.get(i));

            match detail {
                Some(session) => render_detail_view(
                    &mut terminal,
                    session,
                    display_fields,
                    state.color_mode,
                )?,
                None => render_table_view(
                    &mut terminal,
                    &state,
                    &sessions,
                    display_fields,
                    &mut table_state,
                    now,
                )?,
            }

            last_draw = Instant::now();
        }

        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(INPUT_POLL_INTERVAL) => {}
        }

        // Take everything the keyboard has already queued.  One key
        // per pass would let a held arrow build a backlog that keeps
        // scrolling after the key is released.
        let mut input_pending = false;
        while event::poll(Duration::ZERO)? {
            match event::read()? {
                Event::Key(key_event) => {
                    if is_quit(key_event) {
                        return Ok(());
                    }
                    if handle_key(key_event, &mut *state.write().await) {
                        input_pending = true;
                    }
                }
                // ratatui resizes itself on the next draw; we only
                // have to know that one is needed.
                Event::Resize(..) => input_pending = true,
                _ => {}
            }
        }

        // Input is answered at once so the display keeps up with the
        // keyboard.  Records are not: they wait for the next tick and
        // are drawn together with everything else that arrived.
        needs_draw = input_pending || last_draw.elapsed() >= REFRESH_INTERVAL;
    }
}

/// Take over the terminal, run the display, and give the terminal back.
///
/// The loop is a separate function so the terminal is restored whether
/// it returns normally or with an error.
async fn display_task(
    state: &Arc<RwLock<CtopState>>,
    notify: &Notify,
    display_fields: &[DtraceDisplay],
) -> Result<()> {
    // Raw mode is the first thing that fails when there is no terminal
    // to take over, and "Device not configured" on its own does not
    // explain that ctop cannot be piped.
    enable_raw_mode()
        .context("ctop needs a terminal to draw on, it cannot be piped")?;
    execute!(io::stdout(), EnterAlternateScreen)?;

    // Restore the terminal on the way out of a panic as well, or the
    // backtrace lands in the alternate screen and the shell is left in
    // raw mode.
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        let _ = disable_raw_mode();
        original_hook(panic_info);
    }));

    let result = display_loop(state, notify, display_fields).await;

    let _ = execute!(io::stdout(), LeaveAlternateScreen);
    let _ = disable_raw_mode();

    result
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let state = Arc::new(RwLock::new(CtopState::default()));
    let notify = Arc::new(Notify::new());

    // The display owns the screen, so a reader failure is recorded in
    // shared state rather than printed, and reported once the terminal
    // has been handed back.
    let reader_state = Arc::clone(&state);
    let reader_notify = Arc::clone(&notify);
    let dtrace_cmd = args.dtrace_cmd.clone();
    let reader = tokio::spawn(async move {
        let result =
            reader_loop(&dtrace_cmd, &reader_state, &reader_notify).await;

        let why = match &result {
            Ok(()) => "dtrace command finished".to_string(),
            Err(e) => format!("{e:#}"),
        };
        reader_state.write().await.reader_status = ReaderStatus::Stopped(why);
        reader_notify.notify_one();

        result
    });

    let display_result = display_task(&state, &notify, &args.output).await;

    // The reader is either finished already or about to be dropped
    // along with its child, so do not wait on it for long.
    let reader_result =
        tokio::time::timeout(Duration::from_millis(100), reader).await;

    display_result?;

    // Nested because of the timeout and the join.
    match reader_result {
        Ok(Ok(Err(e))) => Err(e),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A DtraceInfo whose downstairs are in the given states.
    fn info_with_states(states: [&str; 3]) -> DtraceInfo {
        let json = format!(
            r#"{{
            "upstairs_id": "12345678-1111-2222-3333-444444444444",
            "session_id": "87654321-1111-2222-3333-444444444444",
            "up_count": 1,
            "up_counters": {{
                "apply": 1, "action_downstairs": 1, "action_guest": 1,
                "action_deferred_block": 0, "action_deferred_message": 0,
                "action_flush_check": 0, "action_stat_check": 0,
                "action_control_check": 0, "action_noop": 0
            }},
            "next_job_id": 1000,
            "ds_count": 3,
            "write_bytes_out": 1,
            "ds_state": ["{}", "{}", "{}"],
            "ds_io_count": {{
                "in_progress": [1, 2, 3], "done": [4, 5, 6],
                "skipped": [0, 0, 0], "error": [0, 0, 0]
            }},
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
        }}"#,
            states[0], states[1], states[2],
        );
        serde_json::from_str(&json).unwrap()
    }

    fn joined(spans: &[Span]) -> String {
        spans.iter().map(|s| &*s.content).collect()
    }

    /// The spans that carry a color, as (text, color).
    fn colored(spans: &[Span]) -> Vec<(String, Color)> {
        spans
            .iter()
            .filter_map(|s| s.style.fg.map(|c| (s.content.to_string(), c)))
            .collect()
    }

    fn key(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    #[test]
    fn test_ds_state_color_every_known_state() {
        let expected = [
            ("Active", Some(Color::Green)),
            ("WaitQuorum", Some(Color::Yellow)),
            ("Reconcile", Some(Color::Yellow)),
            ("LiveRepairReady", Some(Color::Yellow)),
            ("LiveRepair", Some(Color::Yellow)),
            ("Replacing", Some(Color::Yellow)),
            ("Replaced", Some(Color::Yellow)),
            ("Offline", Some(Color::Yellow)),
            ("Faulted", Some(Color::Red)),
            ("Fault", Some(Color::Red)),
            ("NegotiationFailed", Some(Color::Red)),
            ("Disabled", Some(Color::Red)),
            ("New", Some(Color::DarkGray)),
            ("Deactivated", Some(Color::DarkGray)),
            ("SomethingNew", None),
            ("", None),
        ];
        for (state, color) in expected {
            assert_eq!(ds_state_color(state), color, "{state}");
        }
    }

    /// Wherever the state field sits, the spans read back as exactly
    /// what format_row prints, and the three state cells carry the
    /// colors of their states.
    #[test]
    fn test_row_spans_colors_states_wherever_they_are() {
        let info = info_with_states(["Active", "LiveRepair", "Faulted"]);
        let want = vec![
            (" ACT".to_string(), Color::Green),
            ("  LR".to_string(), Color::Yellow),
            (" FLT".to_string(), Color::Red),
        ];

        let layouts = [
            vec![DtraceDisplay::State],
            vec![DtraceDisplay::State, DtraceDisplay::Pid],
            vec![
                DtraceDisplay::Pid,
                DtraceDisplay::State,
                DtraceDisplay::NextJobId,
            ],
            vec![DtraceDisplay::Pid, DtraceDisplay::State],
            default_display_fields(),
        ];
        for fields in layouts {
            let spans = row_spans(1234, &info, Some(5), &fields, ColorMode::On);
            assert_eq!(
                joined(&spans),
                format_row(1234, &info, Some(5), &fields),
                "{fields:?}",
            );
            assert_eq!(colored(&spans), want, "{fields:?}");
        }
    }

    #[test]
    fn test_row_spans_state_twice() {
        let info = info_with_states(["Active", "New", "Offline"]);
        let fields = [DtraceDisplay::State, DtraceDisplay::State];
        let spans = row_spans(1, &info, None, &fields, ColorMode::On);

        assert_eq!(joined(&spans), format_row(1, &info, None, &fields));
        let colors: Vec<Color> =
            colored(&spans).into_iter().map(|(_, c)| c).collect();
        let once = [Color::Green, Color::DarkGray, Color::Yellow];
        assert_eq!(colors, [once, once].concat());
    }

    #[test]
    fn test_row_spans_without_state_is_plain() {
        let info = info_with_states(["Faulted", "Faulted", "Faulted"]);
        let fields = [DtraceDisplay::Pid, DtraceDisplay::NextJobId];
        let spans = row_spans(1, &info, None, &fields, ColorMode::On);

        assert_eq!(joined(&spans), format_row(1, &info, None, &fields));
        assert!(colored(&spans).is_empty());
    }

    /// An unknown state is drawn plain, and does not take the color
    /// of its neighbors.
    #[test]
    fn test_row_spans_unknown_state_is_plain() {
        let info = info_with_states(["Active", "XYZ", "Faulted"]);
        let fields = [DtraceDisplay::State];
        let spans = row_spans(1, &info, None, &fields, ColorMode::On);

        assert_eq!(joined(&spans), format_row(1, &info, None, &fields));
        assert_eq!(
            colored(&spans),
            [
                (" ACT".to_string(), Color::Green),
                (" FLT".to_string(), Color::Red)
            ],
        );
    }

    /// An unknown state wider than its column pushes the row over,
    /// and the colored row must be pushed over the same way.
    #[test]
    fn test_row_spans_wide_unknown_state_matches_format_row() {
        let info = info_with_states(["Active", "SomethingNew", "Faulted"]);
        let fields = [DtraceDisplay::State, DtraceDisplay::Pid];
        let spans = row_spans(1, &info, None, &fields, ColorMode::On);

        assert_eq!(joined(&spans), format_row(1, &info, None, &fields));
        assert_eq!(
            colored(&spans),
            [
                (" ACT".to_string(), Color::Green),
                (" FLT".to_string(), Color::Red)
            ],
        );
    }

    #[test]
    fn test_row_spans_color_off_is_one_plain_span() {
        let info = info_with_states(["Active", "LiveRepair", "Faulted"]);
        let fields = default_display_fields();
        let spans = row_spans(1, &info, None, &fields, ColorMode::Off);

        assert_eq!(spans.len(), 1);
        assert_eq!(joined(&spans), format_row(1, &info, None, &fields));
        assert!(colored(&spans).is_empty());
    }

    #[test]
    fn test_c_toggles_color_and_ctrl_c_quits() {
        let mut state = CtopState::default();
        assert_eq!(state.color_mode, ColorMode::On);

        let c = key(KeyCode::Char('c'), KeyModifiers::NONE);
        assert!(!is_quit(c));
        assert!(handle_key(c, &mut state));
        assert_eq!(state.color_mode, ColorMode::Off);
        assert!(handle_key(c, &mut state));
        assert_eq!(state.color_mode, ColorMode::On);

        // The same toggle from the detail view.
        state.detail_mode = true;
        assert!(handle_key(c, &mut state));
        assert_eq!(state.color_mode, ColorMode::Off);

        assert!(is_quit(key(KeyCode::Char('c'), KeyModifiers::CONTROL)));
    }
}
