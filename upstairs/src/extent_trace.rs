// Copyright 2026 Oxide Computer Company

//! A bounded record of recent jobs that touch a few traced extents
//!
//! A read hash mismatch tells us that two downstairs returned different data
//! for the same block, but not how they got that way.  For each job that
//! writes to (or live-repairs) a traced extent, we keep the block contexts it
//! wrote, what each downstairs was told to do with it, and how each
//! downstairs finished it.  When a mismatch happens, the contexts each
//! downstairs returned can be matched back to the write that produced them.
//!
//! We keep the last few writes to each traced block, plus any write that was
//! still active when live-repair closed its extent, so the writes behind a
//! mismatch are still here even if it shows up long after the repair.  Each
//! client's completions are numbered in the order they arrive, so we can tell
//! whether a write finished on a client after live-repair closed its extent.
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Write as _;
use std::ops::Range;

use chrono::{DateTime, Utc};
use crucible_common::ExtentId;
use crucible_protocol::ReadBlockContext;

use crate::{
    ClientData, ClientId, DownstairsIO, DsState, IOState, IOop, JobId,
    RegionDefinition, client::EnqueueResult,
};

/// Extents to trace
///
/// Every read hash mismatch we have seen after a live-repair has been in
/// extent 0.
pub(crate) const TRACED_EXTENTS: Range<u32> = 0..1;

/// The most recent writes to keep for each traced block
const WRITES_PER_BLOCK: usize = 4;

/// The most live-repair jobs to keep
const LR_JOBS: usize = 64;

/// The most live-repair close acks to keep
const CLOSE_ACKS: usize = 16;

/// The most jobs to keep, whatever else says to keep them; the oldest are
/// dropped first
const MAX_JOBS: usize = 32768;

/// Where live-repair was when a job was enqueued
#[derive(Debug, Copy, Clone)]
pub(crate) struct RepairPoint {
    pub active_extent: ExtentId,
    pub last_extent: Option<ExtentId>,
    pub min_id: JobId,
}

#[derive(Debug)]
enum TracedWork {
    Write {
        unwritten: bool,
        start_block: u64,
        /// Contexts for the blocks inside the traced extents
        contexts: Vec<ReadBlockContext>,
    },
    Close {
        extent: ExtentId,
        flush_number: u64,
        gen_number: u64,
        repair: Vec<ClientId>,
    },
    Repair {
        extent: ExtentId,
        source: ClientId,
        repair: Vec<ClientId>,
    },
    /// `extent` is the live-repair's active extent when this was enqueued
    NoOp {
        extent: ExtentId,
    },
    Reopen {
        extent: ExtentId,
    },
}

#[derive(Debug)]
struct TracedJob {
    time: DateTime<Utc>,
    work: TracedWork,
    deps: Vec<JobId>,
    /// Each client's state when the job was enqueued
    client_state: ClientData<DsState>,
    /// What enqueue decided for each client
    enqueue: ClientData<Option<EnqueueResult>>,
    /// Dependencies as sent to each client (after pruning), and the number
    /// of times it was sent
    sent: ClientData<Option<(Vec<JobId>, usize)>>,
    repair: Option<RepairPoint>,
    /// The job's replay flag and per-client states when it was retired
    retired: Option<(bool, ClientData<IOState>)>,
    /// How each client completed the job
    done: ClientData<Option<Completion>>,
    /// How many traced blocks list this job among their recent writes
    refs: usize,
    /// Keep this job even when no block lists it: it was active when
    /// live-repair closed its extent
    pinned: bool,
}

/// One client's completion of a traced job
#[derive(Debug, Copy, Clone)]
struct Completion {
    /// Place in that client's completion order (the latest, if the job
    /// completed more than once)
    seq: u64,
    time: DateTime<Utc>,
    ok: bool,
    /// How many times the client completed the job
    count: usize,
}

/// A client's ack of a live-repair close of a traced extent, and the writes
/// to that extent enqueued before the close that the client had not finished
#[derive(Debug)]
struct CloseAck {
    close_id: JobId,
    extent: ExtentId,
    client_id: ClientId,
    time: DateTime<Utc>,
    ok: bool,
    /// Each unfinished write, its state on this client, and whether it is
    /// in the close's dependencies
    unfinished: Vec<(JobId, IOState, bool)>,
}

pub(crate) struct ExtentTrace {
    extents: Range<u32>,
    jobs: BTreeMap<JobId, TracedJob>,
    /// Set once we have had to drop a job to stay under `MAX_JOBS`
    dropped: bool,
    /// The last `WRITES_PER_BLOCK` writes to each traced block, oldest first
    writers: BTreeMap<u64, Vec<JobId>>,
    /// Traced blocks that have had older writes dropped
    evicted: BTreeSet<u64>,
    /// The last `LR_JOBS` live-repair jobs, oldest first
    lr_jobs: VecDeque<JobId>,
    /// The next completion number for each client
    seq: ClientData<u64>,
    /// The last `CLOSE_ACKS` acks of live-repair closes, oldest first
    close_acks: VecDeque<CloseAck>,
}

/// Returns the range of blocks a read or write touches
pub(crate) fn job_blocks(io: &IOop) -> Option<Range<u64>> {
    match io {
        IOop::Write {
            start_block,
            blocks,
            ..
        }
        | IOop::WriteUnwritten {
            start_block,
            blocks,
            ..
        } => Some(start_block.0..start_block.0 + blocks.len() as u64),
        IOop::Read {
            start_block, count, ..
        } => Some(start_block.0..start_block.0 + count),
        _ => None,
    }
}

/// Short form of a block context, for log lines
fn ctx_str(ctx: &ReadBlockContext) -> String {
    match ctx {
        ReadBlockContext::Empty => "empty".to_string(),
        ReadBlockContext::Encrypted { ctx } => {
            let mut s = String::from("tag:");
            for b in ctx.tag {
                write!(s, "{b:02x}").unwrap();
            }
            s
        }
        ReadBlockContext::Unencrypted { hash } => format!("hash:{hash:x}"),
    }
}

fn client_list<T>(d: &ClientData<T>, f: impl Fn(&T) -> String) -> String {
    let v: Vec<String> = d.iter().map(f).collect();
    format!("[{}]", v.join(", "))
}

impl ExtentTrace {
    pub(crate) fn new(extents: Range<u32>) -> Self {
        Self {
            extents,
            jobs: BTreeMap::new(),
            dropped: false,
            writers: BTreeMap::new(),
            evicted: BTreeSet::new(),
            lr_jobs: VecDeque::new(),
            seq: ClientData::new(0),
            close_acks: VecDeque::new(),
        }
    }

    pub(crate) fn covers(&self, extent: ExtentId) -> bool {
        self.extents.contains(&extent.0)
    }

    /// Returns the traced range of blocks
    fn blocks(&self, ddef: &RegionDefinition) -> Range<u64> {
        let es = ddef.extent_size().value;
        self.extents.start as u64 * es..self.extents.end as u64 * es
    }

    /// Starts tracing a job, if it touches a traced extent
    ///
    /// This must be called before the job is sent to any client.
    pub(crate) fn enqueue(
        &mut self,
        ds_id: JobId,
        io: &IOop,
        ddef: Option<&RegionDefinition>,
        client_state: ClientData<DsState>,
        repair: Option<RepairPoint>,
    ) {
        let Some(ddef) = ddef else {
            return;
        };
        let work = match io {
            IOop::Write {
                start_block,
                blocks,
                ..
            }
            | IOop::WriteUnwritten {
                start_block,
                blocks,
                ..
            } => {
                let traced = self.blocks(ddef);
                let start = start_block.0.max(traced.start);
                let end = (start_block.0 + blocks.len() as u64).min(traced.end);
                if start >= end {
                    return;
                }
                let contexts = blocks[(start - start_block.0) as usize
                    ..(end - start_block.0) as usize]
                    .iter()
                    .map(|b| match b.encryption_context {
                        Some(ctx) => ReadBlockContext::Encrypted { ctx },
                        None => ReadBlockContext::Unencrypted { hash: b.hash },
                    })
                    .collect();
                TracedWork::Write {
                    unwritten: matches!(io, IOop::WriteUnwritten { .. }),
                    start_block: start,
                    contexts,
                }
            }
            IOop::ExtentFlushClose {
                extent,
                flush_number,
                gen_number,
                repair_downstairs,
                ..
            } if self.covers(*extent) => TracedWork::Close {
                extent: *extent,
                flush_number: *flush_number,
                gen_number: *gen_number,
                repair: repair_downstairs.clone(),
            },
            IOop::ExtentLiveRepair {
                extent,
                source_downstairs,
                repair_downstairs,
                ..
            } if self.covers(*extent) => TracedWork::Repair {
                extent: *extent,
                source: *source_downstairs,
                repair: repair_downstairs.clone(),
            },
            IOop::ExtentLiveNoOp { .. } => match repair {
                Some(r) if self.covers(r.active_extent) => TracedWork::NoOp {
                    extent: r.active_extent,
                },
                _ => return,
            },
            IOop::ExtentLiveReopen { extent, .. } if self.covers(*extent) => {
                TracedWork::Reopen { extent: *extent }
            }
            _ => return,
        };

        let deps = match io {
            IOop::Write { dependencies, .. }
            | IOop::WriteUnwritten { dependencies, .. }
            | IOop::ExtentFlushClose { dependencies, .. }
            | IOop::ExtentLiveRepair { dependencies, .. }
            | IOop::ExtentLiveNoOp { dependencies }
            | IOop::ExtentLiveReopen { dependencies, .. } => {
                dependencies.clone()
            }
            _ => unreachable!(),
        };

        let written = match &work {
            TracedWork::Write {
                start_block,
                contexts,
                ..
            } => Some(*start_block..start_block + contexts.len() as u64),
            _ => None,
        };
        self.jobs.insert(
            ds_id,
            TracedJob {
                time: Utc::now(),
                work,
                deps,
                client_state,
                enqueue: ClientData::new(None),
                sent: ClientData::new(None),
                repair,
                retired: None,
                done: ClientData::new(None),
                refs: 0,
                pinned: false,
            },
        );

        if let Some(written) = written {
            for block in written {
                let list = self.writers.entry(block).or_default();
                list.push(ds_id);
                let old =
                    (list.len() > WRITES_PER_BLOCK).then(|| list.remove(0));
                self.jobs.get_mut(&ds_id).unwrap().refs += 1;
                if let Some(old) = old {
                    self.evicted.insert(block);
                    self.release(old);
                }
            }
        } else {
            self.lr_jobs.push_back(ds_id);
            if self.lr_jobs.len() > LR_JOBS {
                let old = self.lr_jobs.pop_front().unwrap();
                if self.jobs.get(&old).is_some_and(|j| !j.pinned) {
                    self.jobs.remove(&old);
                }
            }
        }

        while self.jobs.len() > MAX_JOBS {
            self.jobs.pop_first();
            self.dropped = true;
        }
    }

    /// Drops one block's reference to a write, and the write itself if
    /// nothing else keeps it
    fn release(&mut self, ds_id: JobId) {
        if let Some(j) = self.jobs.get_mut(&ds_id) {
            j.refs = j.refs.saturating_sub(1);
            if j.refs == 0 && !j.pinned {
                self.jobs.remove(&ds_id);
            }
        }
    }

    /// Keeps a traced job, even after newer writes replace it
    pub(crate) fn pin(&mut self, ds_id: JobId) {
        if let Some(j) = self.jobs.get_mut(&ds_id) {
            j.pinned = true;
        }
    }

    /// Records that a client completed a job
    ///
    /// This must be called for every completion, traced or not, so that the
    /// numbers give each client's completion order.
    pub(crate) fn completed(
        &mut self,
        ds_id: JobId,
        client_id: ClientId,
        ok: bool,
    ) {
        let seq = self.seq[client_id];
        self.seq[client_id] += 1;
        if let Some(j) = self.jobs.get_mut(&ds_id) {
            let count = j.done[client_id].map(|c| c.count).unwrap_or(0);
            j.done[client_id] = Some(Completion {
                seq,
                time: Utc::now(),
                ok,
                count: count + 1,
            });
        }
    }

    /// Records a client's ack of a live-repair close of a traced extent
    pub(crate) fn close_ack(
        &mut self,
        close_id: JobId,
        extent: ExtentId,
        client_id: ClientId,
        ok: bool,
        unfinished: Vec<(JobId, IOState, bool)>,
    ) {
        self.close_acks.push_back(CloseAck {
            close_id,
            extent,
            client_id,
            time: Utc::now(),
            ok,
            unfinished,
        });
        if self.close_acks.len() > CLOSE_ACKS {
            self.close_acks.pop_front();
        }
    }

    /// Records what enqueue decided for one client
    pub(crate) fn enqueue_result(
        &mut self,
        ds_id: JobId,
        client_id: ClientId,
        r: EnqueueResult,
    ) {
        if let Some(j) = self.jobs.get_mut(&ds_id) {
            j.enqueue[client_id] = Some(r);
        }
    }

    /// Records the (pruned) job as it was sent to one client
    pub(crate) fn sent(
        &mut self,
        ds_id: JobId,
        client_id: ClientId,
        io: &IOop,
    ) {
        let Some(j) = self.jobs.get_mut(&ds_id) else {
            return;
        };
        let deps = match io {
            IOop::Write { dependencies, .. }
            | IOop::WriteUnwritten { dependencies, .. }
            | IOop::ExtentFlushClose { dependencies, .. }
            | IOop::ExtentLiveRepair { dependencies, .. }
            | IOop::ExtentLiveNoOp { dependencies }
            | IOop::ExtentLiveReopen { dependencies, .. } => dependencies,
            _ => return,
        };
        let count = j.sent[client_id].as_ref().map(|s| s.1).unwrap_or(0);
        j.sent[client_id] = Some((deps.clone(), count + 1));
    }

    /// Records each client's final state for a job being retired
    pub(crate) fn retire(&mut self, ds_id: JobId, job: &DownstairsIO) {
        if let Some(j) = self.jobs.get_mut(&ds_id) {
            j.retired = Some((job.replay, job.state.clone()));
        }
    }

    /// Describes one traced job, as seen from `block` (if it is a write)
    ///
    /// `active` is the job's current entry in the active list, if any.
    fn describe(
        &self,
        ds_id: JobId,
        j: &TracedJob,
        block: u64,
        expected: &ReadBlockContext,
        computed: &ReadBlockContext,
        active: Option<&DownstairsIO>,
    ) -> String {
        let mut s = format!("{ds_id} {} ", j.time.format("%H:%M:%S%.6f"));
        match &j.work {
            TracedWork::Write {
                unwritten,
                start_block,
                contexts,
            } => {
                let ctx = &contexts[(block - start_block) as usize];
                let m = match (ctx == expected, ctx == computed) {
                    (true, _) => "EXPECTED",
                    (_, true) => "COMPUTED",
                    _ => "other",
                };
                let name = if *unwritten {
                    "WriteUnwritten"
                } else {
                    "Write"
                };
                write!(s, "{name} {m} {}", ctx_str(ctx)).unwrap();
            }
            TracedWork::Close {
                extent,
                flush_number,
                gen_number,
                repair,
            } => write!(
                s,
                "ExtentFlushClose {extent} f:{flush_number} g:{gen_number} \
                 repair:{repair:?}"
            )
            .unwrap(),
            TracedWork::Repair {
                extent,
                source,
                repair,
            } => write!(
                s,
                "ExtentLiveRepair {extent} source:{source} repair:{repair:?}"
            )
            .unwrap(),
            TracedWork::NoOp { extent } => {
                write!(s, "ExtentLiveNoOp (repairing {extent})").unwrap()
            }
            TracedWork::Reopen { extent } => {
                write!(s, "ExtentLiveReopen {extent}").unwrap()
            }
        }
        write!(
            s,
            " deps:{:?} ds:{} enqueue:{}",
            j.deps,
            client_list(&j.client_state, |c| c.to_string()),
            client_list(&j.enqueue, |e| match e {
                Some(e) => format!("{e:?}"),
                None => "-".to_string(),
            }),
        )
        .unwrap();
        for cid in ClientId::iter() {
            if let Some((deps, count)) = &j.sent[cid]
                && deps != &j.deps
            {
                write!(s, " sent[{cid}]x{count}:{deps:?}").unwrap();
            }
        }
        write!(
            s,
            " done:{}",
            client_list(&j.done, |d| match d {
                Some(d) => format!(
                    "#{}@{}{}{}",
                    d.seq,
                    d.time.format("%H:%M:%S%.6f"),
                    if d.ok { "" } else { "!err" },
                    if d.count > 1 {
                        format!("x{}", d.count)
                    } else {
                        String::new()
                    },
                ),
                None => "-".to_string(),
            })
        )
        .unwrap();
        if let Some(r) = &j.repair {
            write!(
                s,
                " lr:active:{} last:{:?} min:{}",
                r.active_extent, r.last_extent, r.min_id
            )
            .unwrap();
        }
        if let Some((replay, state)) = &j.retired {
            write!(
                s,
                " retired:{} replay:{replay}",
                client_list(state, |c| c.to_string())
            )
            .unwrap();
        } else if let Some(job) = active {
            write!(
                s,
                " active:{} replay:{} acked:{}",
                client_list(&job.state, |c| c.to_string()),
                job.replay,
                job.acked
            )
            .unwrap();
        } else {
            write!(s, " (no longer active)").unwrap();
        }
        s
    }

    /// Returns lines that point out writes to `block` that live-repair's
    /// close of its extent did not wait for
    ///
    /// For each traced close of the extent, and each client that completed
    /// it, a write enqueued before the close should have finished on that
    /// client before the close did.  We list each one that finished later,
    /// or that was sent and had not finished when the close did.
    fn late_writes<'a>(
        &self,
        block: u64,
        extent: ExtentId,
        active: &impl Fn(JobId) -> Option<&'a DownstairsIO>,
    ) -> Vec<String> {
        let mut out = vec![];
        let closes = self.jobs.iter().filter(|(_, j)| {
            matches!(j.work, TracedWork::Close { extent: x, .. } if x == extent)
        });
        for (close_id, close) in closes {
            for cid in ClientId::iter() {
                let Some(cd) = close.done[cid] else {
                    continue;
                };
                let writes = self.jobs.range(..close_id).filter(|(_, j)| {
                    matches!(&j.work, TracedWork::Write { start_block, contexts, .. }
                        if (*start_block..start_block + contexts.len() as u64)
                            .contains(&block))
                });
                for (id, w) in writes {
                    match w.done[cid] {
                        Some(wd) if wd.seq > cd.seq => out.push(format!(
                            "block {block} LATE: write {id} finished on \
                             [{cid}] (#{}) after close {close_id} (#{})",
                            wd.seq, cd.seq
                        )),
                        Some(_) => (),
                        None => {
                            let state = match (&w.retired, active(*id)) {
                                (Some((_, s)), _) => Some(s[cid].clone()),
                                (None, Some(job)) => {
                                    Some(job.state[cid].clone())
                                }
                                (None, None) => None,
                            };
                            if matches!(
                                w.enqueue[cid],
                                Some(EnqueueResult::Send)
                            ) && state != Some(IOState::Skipped)
                            {
                                out.push(format!(
                                    "block {block} LATE: write {id} not \
                                     finished on [{cid}] when close \
                                     {close_id} finished (#{}), state {:?}",
                                    cd.seq, state
                                ));
                            }
                        }
                    }
                }
            }
        }
        out
    }

    /// Returns log lines tracing each mismatched block in a read
    ///
    /// For every mismatched block in a traced extent, this lists each traced
    /// write to that block and each live-repair job for its extent, oldest
    /// first, and marks which write produced the expected and the computed
    /// contexts.  It then points out any write to that block that finished
    /// after live-repair closed its extent, and lists the writes each client
    /// had not finished when it acked the close.  `active` looks up a job
    /// that has not been retired.
    pub(crate) fn mismatch_report<'a>(
        &self,
        ddef: &RegionDefinition,
        start_block: u64,
        expected: &[ReadBlockContext],
        computed: &[ReadBlockContext],
        active: impl Fn(JobId) -> Option<&'a DownstairsIO>,
    ) -> Vec<String> {
        // Keep the report to a manageable size
        const MAX_BLOCKS: usize = 8;

        let mut out = vec![];
        match (self.jobs.first_key_value(), self.jobs.last_key_value()) {
            (Some((first, _)), Some((last, _))) => out.push(format!(
                "trace holds {} jobs {first}..={last} for extents {:?}, \
                 last {WRITES_PER_BLOCK} writes per block{}",
                self.jobs.len(),
                self.extents,
                if self.dropped {
                    ", older jobs dropped at limit"
                } else {
                    ""
                },
            )),
            _ => out.push(format!(
                "trace holds no jobs for extents {:?}",
                self.extents
            )),
        }

        let traced = self.blocks(ddef);
        let es = ddef.extent_size().value;
        let mut extents = BTreeSet::new();
        let mismatched = expected
            .iter()
            .zip(computed)
            .enumerate()
            .filter(|(_, (e, c))| e != c)
            .map(|(i, (e, c))| (start_block + i as u64, e, c));
        for (n, (block, e, c)) in mismatched.enumerate() {
            if n == MAX_BLOCKS {
                out.push(format!("more than {MAX_BLOCKS} blocks differ"));
                break;
            }
            out.push(format!(
                "block {block} expected {} computed {}",
                ctx_str(e),
                ctx_str(c)
            ));
            if !traced.contains(&block) {
                out.push(format!("block {block} is not traced"));
                continue;
            }
            let extent = ExtentId((block / es) as u32);
            extents.insert(extent);
            if self.evicted.contains(&block) {
                out.push(format!("block {block} older writes dropped"));
            }
            for (id, j) in &self.jobs {
                let touches = match &j.work {
                    TracedWork::Write {
                        start_block,
                        contexts,
                        ..
                    } => (*start_block..start_block + contexts.len() as u64)
                        .contains(&block),
                    TracedWork::Close { extent: x, .. }
                    | TracedWork::Repair { extent: x, .. }
                    | TracedWork::NoOp { extent: x }
                    | TracedWork::Reopen { extent: x } => *x == extent,
                };
                if touches {
                    let line = self.describe(*id, j, block, e, c, active(*id));
                    out.push(format!("block {block} {line}"));
                }
            }
            out.extend(self.late_writes(block, extent, &active));
        }

        for a in self
            .close_acks
            .iter()
            .filter(|a| extents.contains(&a.extent))
        {
            out.push(format!(
                "close ack {} extent {} from [{}] {} ok:{} unfinished \
                 writes enqueued before it (id, state, in close deps): {:?}",
                a.close_id,
                a.extent,
                a.client_id,
                a.time.format("%H:%M:%S%.6f"),
                a.ok,
                a.unfinished,
            ));
        }
        out
    }
}
