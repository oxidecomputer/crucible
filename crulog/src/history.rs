// Copyright 2026 Oxide Computer Company

//! Trace a crucible panic back through the history of the extent it hit.
use chrono::{DateTime, Utc};
use regex::Captures;
use serde::Serialize;

use crate::catalog::{self, Id};
use crate::record::Record;
use crate::summary::{self, PanicIo, Run, Summary};

#[derive(Debug, Serialize)]
pub struct PanicReport {
    /// The files read, oldest first.
    pub files: Vec<String>,
    /// The run that panicked, with only the disk the panic was on.
    pub run: Run,
    /// The extents the panicked IO covers.
    pub extents: Vec<u64>,
    /// What happened to the disk and those extents, oldest first.
    pub history: Vec<Event>,
}

#[derive(Debug, Serialize)]
pub struct Event {
    pub time: DateTime<Utc>,
    /// Repeated events are counted, and `until` is the time of the last.
    pub count: u32,
    pub until: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub what: What,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum What {
    /// Reconciliation found the extent dirty or different between
    /// clients.  `clients` is in client order.
    ReconcileNeeded {
        extent: u64,
        clients: Vec<Option<ExtentMetadata>>,
    },
    ReconcileStart {
        id: String,
        tasks: u64,
    },
    /// A reconcile task that copies the extent from source to dests.
    ReconcileRepair {
        extent: u64,
        task_id: u64,
        source: u32,
        dests: Vec<u32>,
    },
    ReconcileRepairSent {
        extent: u64,
        task_id: u64,
        clients: Vec<u32>,
    },
    ReconcileDone,
    Transition {
        client: u32,
        from: String,
        to: String,
    },
    ClientStopped {
        client: u32,
        reason: String,
    },
    Faulted {
        client: u32,
        reason: Option<String>,
    },
    LiveRepairReady {
        client: u32,
    },
    /// The source is not logged when live repair starts, so it is taken
    /// from the first extent the repair copies.
    LiveRepairStart {
        id: String,
        clients: Vec<u32>,
        source: Option<u32>,
    },
    LiveRepairExtent {
        extent: u64,
        close_id: u64,
        repair_id: u64,
        noop_id: u64,
        reopen_id: u64,
    },
    /// The source is taken from the live repair, as for LiveRepairStart.
    LiveRepairNotNeeded {
        extent: u64,
        source: Option<u32>,
    },
    LiveRepairCopy {
        extent: u64,
        source: u32,
        dests: Vec<u32>,
    },
    /// The last extent a live repair still running at the panic started.
    LiveRepairProgress {
        extent: u64,
        extent_count: Option<u64>,
    },
    LiveRepairEnd {
        result: String,
    },
    Panic {
        client: u32,
        job_id: u64,
        error: String,
    },
}

#[derive(Debug, PartialEq, Serialize)]
pub struct ExtentMetadata {
    pub generation: u64,
    pub flush: u64,
    pub dirty: bool,
}

/// Find each crucible panic in the logs and trace its extents back.
pub fn analyze(files: &[(String, String)]) -> Vec<PanicReport> {
    let Summary { files: names, runs } = summary::summarize(files);
    let files = summary::ordered(files);
    runs.into_iter()
        .filter_map(|run| report(&files, &names, run))
        .collect()
}

fn report(
    files: &[&(String, String)],
    names: &[String],
    mut run: Run,
) -> Option<PanicReport> {
    let panic = run.panic.as_ref()?;
    let after = panic.after;
    let upstairs_id = panic.upstairs_id.clone();
    let io = panic.io.as_ref();

    run.disks.retain(|d| {
        upstairs_id.is_some()
            && d.crucible.as_ref().map(|c| &c.upstairs_id)
                == upstairs_id.as_ref()
    });
    let crucible = run.disks.first().and_then(|d| d.crucible.as_ref());
    let extents = match (io, crucible.and_then(|c| c.extent_size)) {
        (Some(io), Some(size)) => {
            io.extents(size).into_iter().flatten().collect()
        }
        _ => Vec::new(),
    };

    let mut history =
        History::new(extents.clone(), crucible.and_then(|c| c.extent_count));
    if let Some(upstairs_id) = &upstairs_id {
        for (_, text) in files {
            for record in text.lines().filter_map(Record::parse) {
                if record.pid == run.pid
                    && record.time >= run.first_record
                    && record.time <= after
                    && catalog::captures(Id::CrucibleComponent, &record)
                        .is_some_and(|c| &c["upstairs_id"] == upstairs_id)
                {
                    history.add(&record);
                }
            }
        }
    }
    history.finish(after, io);

    Some(PanicReport {
        files: names.to_vec(),
        run,
        extents,
        history: history.events,
    })
}

struct History {
    extents: Vec<u64>,
    extent_count: Option<u64>,
    events: Vec<Event>,
    /// Reconcile tasks that repair one of the extents: (task, extent).
    tasks: Vec<(u64, u64)>,
    /// Why the next client to fault was faulted.
    fault_reason: Option<String>,
    faulted: Vec<u32>,
    /// Clients ready for the next live repair.
    ready: Vec<u32>,
    /// The extent the running live repair last started, and when.
    progress: Option<(DateTime<Utc>, u64)>,
}

impl History {
    fn new(extents: Vec<u64>, extent_count: Option<u64>) -> History {
        History {
            extents,
            extent_count,
            events: Vec::new(),
            tasks: Vec::new(),
            fault_reason: None,
            faulted: Vec::new(),
            ready: Vec::new(),
            progress: None,
        }
    }

    fn push(&mut self, time: DateTime<Utc>, what: What) {
        self.events.push(Event {
            time,
            count: 1,
            until: None,
            what,
        });
    }

    /// Push an event, or count it if the client's last event was the
    /// same.
    fn push_repeated(&mut self, time: DateTime<Utc>, client: u32, what: What) {
        if let Some(last) = self
            .events
            .iter_mut()
            .rev()
            .find(|e| client_of(&e.what) == Some(client))
            && last.what == what
        {
            last.count += 1;
            last.until = Some(time);
        } else {
            self.push(time, what);
        }
    }

    fn ours(&self, c: &Captures, name: &str) -> Option<u64> {
        let extent = number(c, name)?;
        self.extents.contains(&extent).then_some(extent)
    }

    fn add(&mut self, record: &Record) {
        let time = record.time;
        let client = record.field("client").and_then(|s| s.parse().ok());

        if let Some(c) = catalog::captures(Id::ReconcileNeeded, record) {
            if let Some(extent) = self.ours(&c, "extent") {
                let clients =
                    catalog::captures_iter(Id::ExtentMetadata, &c["clients"])
                        .map(|m| extent_metadata(&m))
                        .collect();
                self.push(time, What::ReconcileNeeded { extent, clients });
            }
        } else if let Some(c) = catalog::captures(Id::ReconcileStart, record) {
            let what = What::ReconcileStart {
                id: c["id"].to_string(),
                tasks: number(&c, "tasks").unwrap_or(0),
            };
            self.push(time, what);
        } else if let Some(c) = catalog::captures(Id::ReconcileTaskList, record)
        {
            for t in
                catalog::captures_iter(Id::ReconcileRepairTask, &c["tasks"])
            {
                let (Some(extent), Some(task_id), Some(source)) = (
                    self.ours(&t, "extent"),
                    number(&t, "id"),
                    number(&t, "source"),
                ) else {
                    continue;
                };
                self.tasks.push((task_id, extent));
                let what = What::ReconcileRepair {
                    extent,
                    task_id,
                    source: source as u32,
                    dests: client_ids(&t["dests"]),
                };
                self.push(time, what);
            }
        } else if let Some(c) = catalog::captures(Id::ReconcileSend, record) {
            let Some(task_id) = number(&c, "id") else {
                return;
            };
            let Some(&(_, extent)) =
                self.tasks.iter().find(|(t, _)| *t == task_id)
            else {
                return;
            };
            // Each client logs its own send, so gather them up.
            if let Some(Event {
                what:
                    What::ReconcileRepairSent {
                        task_id: t,
                        clients,
                        ..
                    },
                ..
            }) = self.events.last_mut()
                && *t == task_id
            {
                clients.extend(client);
            } else {
                let what = What::ReconcileRepairSent {
                    extent,
                    task_id,
                    clients: client.into_iter().collect(),
                };
                self.push(time, what);
            }
        } else if catalog::captures(Id::ReconcileDone, record).is_some() {
            self.push(time, What::ReconcileDone);
        } else if let Some(c) = catalog::captures(Id::Transition, record) {
            let Some(client) = client else {
                return;
            };
            if &c["to"] == "Active" {
                self.faulted.retain(|&f| f != client);
            }
            let what = What::Transition {
                client,
                from: c["from"].to_string(),
                to: c["to"].to_string(),
            };
            self.push(time, what);
        } else if let Some(c) = catalog::captures(Id::ClientStopped, record) {
            let Some(client) = number(&c, "client") else {
                return;
            };
            let client = client as u32;
            let what = What::ClientStopped {
                client,
                reason: c["reason"].to_string(),
            };
            self.push_repeated(time, client, what);
        } else if let Some(c) =
            catalog::captures(Id::TooManyOutstanding, record)
        {
            self.fault_reason = Some(format!(
                "too many outstanding {} {}",
                &c["what"], &c["count"]
            ));
        } else if let Some(c) = catalog::captures(Id::FaultSkip, record) {
            // This is logged every time a faulted client's jobs are
            // skipped, so only the first one is the fault.
            let Some(client) = number(&c, "client") else {
                return;
            };
            let client = client as u32;
            if !self.faulted.contains(&client) {
                self.faulted.push(client);
                let reason = self.fault_reason.take();
                self.push(time, What::Faulted { client, reason });
            }
        } else if let Some(c) = catalog::captures(Id::LiveRepairReady, record) {
            let Some(client) = number(&c, "client") else {
                return;
            };
            let client = client as u32;
            self.faulted.retain(|&f| f != client);
            self.ready.push(client);
            self.push(time, What::LiveRepairReady { client });
        } else if let Some(c) = catalog::captures(Id::LiveRepairStart, record) {
            let what = What::LiveRepairStart {
                id: c["id"].to_string(),
                clients: std::mem::take(&mut self.ready),
                source: None,
            };
            self.progress = None;
            self.push(time, what);
        } else if let Some(c) = catalog::captures(Id::LiveRepairExtent, record)
        {
            let Some(extent) = number(&c, "extent") else {
                return;
            };
            self.progress = Some((time, extent));
            if self.extents.contains(&extent) {
                let what = What::LiveRepairExtent {
                    extent,
                    close_id: number(&c, "close_id").unwrap_or(0),
                    repair_id: number(&c, "repair_id").unwrap_or(0),
                    noop_id: number(&c, "noop_id").unwrap_or(0),
                    reopen_id: number(&c, "reopen_id").unwrap_or(0),
                };
                self.push(time, what);
            }
        } else if let Some(c) =
            catalog::captures(Id::LiveRepairNotNeeded, record)
        {
            if let Some(extent) = self.ours(&c, "extent") {
                let source = self.live_repair_source();
                self.push(time, What::LiveRepairNotNeeded { extent, source });
            }
        } else if let Some(c) = catalog::captures(Id::LiveRepairCopy, record) {
            let Some(source) = number(&c, "source") else {
                return;
            };
            let source = source as u32;
            self.set_live_repair_source(source);
            if let Some(extent) = self.ours(&c, "extent") {
                let what = What::LiveRepairCopy {
                    extent,
                    source,
                    dests: client_ids(&c["dests"]),
                };
                self.push(time, what);
            }
        } else if let Some(c) = catalog::captures(Id::LiveRepairEnd, record) {
            self.progress = None;
            let what = What::LiveRepairEnd {
                result: c["result"].to_string(),
            };
            self.push(time, what);
        }
    }

    /// The source of the running live repair, if it is known yet.
    fn live_repair_source(&self) -> Option<u32> {
        self.events.iter().rev().find_map(|e| match e.what {
            What::LiveRepairStart { source, .. } => Some(source),
            _ => None,
        })?
    }

    /// Fill in the source for the running live repair and the events
    /// logged in it before the source was known.
    fn set_live_repair_source(&mut self, source: u32) {
        for e in self.events.iter_mut().rev() {
            match &mut e.what {
                What::LiveRepairNotNeeded { source: s, .. } => {
                    s.get_or_insert(source);
                }
                What::LiveRepairStart { source: s, .. } => {
                    s.get_or_insert(source);
                    break;
                }
                _ => {}
            }
        }
    }

    fn finish(&mut self, after: DateTime<Utc>, io: Option<&PanicIo>) {
        if let Some((time, extent)) = self.progress {
            let i = self.events.partition_point(|e| e.time <= time);
            self.events.insert(
                i,
                Event {
                    time,
                    count: 1,
                    until: None,
                    what: What::LiveRepairProgress {
                        extent,
                        extent_count: self.extent_count,
                    },
                },
            );
        }
        if let Some(io) = io {
            let what = What::Panic {
                client: io.client,
                job_id: io.job_id,
                error: io.error.clone(),
            };
            self.push(after, what);
        }
    }
}

/// The client an event is about, for events about one client.
fn client_of(what: &What) -> Option<u32> {
    match what {
        What::Transition { client, .. }
        | What::ClientStopped { client, .. }
        | What::Faulted { client, .. }
        | What::LiveRepairReady { client } => Some(*client),
        _ => None,
    }
}

fn number(c: &Captures, name: &str) -> Option<u64> {
    c.name(name)?.as_str().parse().ok()
}

fn client_ids(text: &str) -> Vec<u32> {
    catalog::captures_iter(Id::ClientIds, text)
        .filter_map(|c| c["client"].parse().ok())
        .collect()
}

fn extent_metadata(c: &Captures) -> Option<ExtentMetadata> {
    if c.name("none").is_some() {
        return None;
    }
    Some(ExtentMetadata {
        generation: number(c, "generation")?,
        flush: number(c, "flush")?,
        dirty: &c["dirty"] == "true",
    })
}
