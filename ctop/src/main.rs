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
    widgets::{Paragraph, Row, Table, TableState},
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

/// How often the display loop wakes to look for keyboard input.
const INPUT_POLL_INTERVAL: Duration = Duration::from_millis(50);

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

    /// Set when the dtrace command is no longer running, which tells
    /// the display to stop.  Otherwise a dtrace that never started
    /// would leave an empty screen up with no explanation.
    reader_done: bool,

    /// Why the reader stopped, reported once the terminal is back.
    reader_error: Option<String>,
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

/// Apply one key to the cursor.  Returns true if anything moved.
fn handle_navigation(key_event: KeyEvent, state: &mut CtopState) -> bool {
    let down = match key_event {
        KeyEvent {
            code: KeyCode::Up,
            modifiers: KeyModifiers::NONE,
            ..
        } => false,
        KeyEvent {
            code: KeyCode::Down,
            modifiers: KeyModifiers::NONE,
            ..
        } => true,
        _ => return false,
    };

    move_selection(state, down);
    true
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

/// Draw one frame: a clock, a row per session, and the keys.
///
/// `now` is passed in rather than read here so that every row in a
/// frame is judged stale against the same instant.
fn render_table_view(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    sessions: &[&SessionData],
    display_fields: &[DtraceDisplay],
    table_state: &mut TableState,
    now: Instant,
    timestamp: u64,
    global_max: u64,
) -> io::Result<()> {
    terminal.draw(|f| {
        let chunks = Layout::default()
            .constraints([
                Constraint::Length(1), // timestamp
                Constraint::Min(0),    // session table
                Constraint::Length(1), // key help
            ])
            .split(f.area());

        f.render_widget(
            Paragraph::new(format!("ctop - Unix timestamp: {timestamp}")),
            chunks[0],
        );

        let selected = table_state.selected();

        // Rows carry a one character indicator, so the header is padded
        // by the same amount to keep the columns lined up.
        let header = format!(" {}", format_header(display_fields));

        // Whatever width the columns do not use goes to the sparkline.
        let spark_width =
            (chunks[1].width as usize).saturating_sub(header.chars().count());

        let rows: Vec<Row> = sessions
            .iter()
            .enumerate()
            .map(|(idx, s)| {
                // Cursor location will override the stale flag.
                let indicator = if Some(idx) == selected {
                    '>'
                } else if is_stale(s, now) {
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
        // the cursor on screen when there are more sessions than rows.
        f.render_stateful_widget(table, chunks[1], table_state);

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
            .split(chunks[2]);

        f.render_widget(
            Paragraph::new(format!(
                "[up/down: Move | 'q': Quit]  \
                 > = cursor, * = stale ({STALE_THRESHOLD_SECS}s)"
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

    loop {
        // One instant for the whole frame, so every row is judged
        // against the same clock.
        let now = Instant::now();

        // This is scoped so the lock is dropped before the wait below.
        {
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

            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            // One scale for every sparkline on screen, so a busy row
            // looks busier than a quiet one.
            let global_max = sessions
                .iter()
                .flat_map(|s| s.delta_history.iter())
                .copied()
                .max()
                .unwrap_or(1);

            render_table_view(
                &mut terminal,
                &sessions,
                display_fields,
                &mut table_state,
                now,
                timestamp,
                global_max,
            )?;

            if state.reader_done {
                return Ok(());
            }
        }

        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(INPUT_POLL_INTERVAL) => {}
        }

        while event::poll(Duration::ZERO)? {
            if let Event::Key(key_event) = event::read()? {
                if is_quit(key_event) {
                    return Ok(());
                }
                handle_navigation(key_event, &mut *state.write().await);
            }
        }
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
        let error = reader_loop(&dtrace_cmd, &reader_state, &reader_notify)
            .await
            .err()
            .map(|e| format!("{e:#}"));

        let mut state = reader_state.write().await;
        state.reader_error = error;
        state.reader_done = true;
        drop(state);

        reader_notify.notify_one();
    });

    let display_result = display_task(&state, &notify, &args.output).await;

    // The reader is either finished already or about to be dropped
    // along with its child, so do not wait on it for long.
    let _ = tokio::time::timeout(Duration::from_millis(100), reader).await;

    display_result?;

    if let Some(error) = state.write().await.reader_error.take() {
        bail!("{error}");
    }

    Ok(())
}
