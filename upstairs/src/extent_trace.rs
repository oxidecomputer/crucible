// Copyright 2026 Oxide Computer Company

//! A bounded record of recent jobs that touch a few traced extents
//!
//! A read hash mismatch tells us that two downstairs returned different data
//! for the same block, but not how they got that way.  For each job that
//! writes to (or live-repairs) a traced extent, we keep the block contexts it
//! wrote, what each downstairs was told to do with it, and how each
//! downstairs finished it.  When a mismatch happens, the contexts each
//! downstairs returned can be matched back to the write that produced them.
use std::collections::BTreeMap;
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

/// The most jobs to keep; the oldest are dropped first
const TRACE_JOBS: usize = 4096;

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
}

pub(crate) struct ExtentTrace {
    extents: Range<u32>,
    jobs: BTreeMap<JobId, TracedJob>,
    /// Set once we have had to drop a job to stay under `TRACE_JOBS`
    dropped: bool,
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
            },
        );
        while self.jobs.len() > TRACE_JOBS {
            self.jobs.pop_first();
            self.dropped = true;
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

    /// Returns log lines tracing each mismatched block in a read
    ///
    /// For every mismatched block in a traced extent, this lists each traced
    /// write to that block and each live-repair job for its extent, oldest
    /// first, and marks which write produced the expected and the computed
    /// contexts.  `active` looks up a job that has not been retired.
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
                "trace holds {} jobs {first}..={last} for extents {:?}{}",
                self.jobs.len(),
                self.extents,
                if self.dropped {
                    ", older jobs dropped"
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
        }
        out
    }
}
