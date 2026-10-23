// Copyright 2026 Oxide Computer Company

//! Render a summary as text.
use chrono::{DateTime, SecondsFormat, Utc};
use std::fmt::Write;

use crate::history::{Event, PanicReport, What};
use crate::summary::{Crucible, Disk, PanicIo, Run, Summary};

pub fn text(summary: &Summary) -> String {
    let mut out = String::new();
    writeln!(out, "Files, oldest first:").unwrap();
    for file in &summary.files {
        writeln!(out, "  {file}").unwrap();
    }
    for run in &summary.runs {
        writeln!(out).unwrap();
        render_run(&mut out, run);
    }
    out
}

fn time(t: &DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn or_unknown(s: &Option<String>) -> &str {
    s.as_deref().unwrap_or("?")
}

fn render_run(out: &mut String, run: &Run) {
    let pid = run.pid.map_or("?".to_string(), |p| p.to_string());
    match &run.started {
        Some(t) => {
            writeln!(out, "propolis-server pid {pid}, started {}", time(t))
        }
        None => writeln!(
            out,
            "propolis-server pid {pid}, already running when the logs \
             begin at {}",
            time(&run.first_record)
        ),
    }
    .unwrap();

    let field = |out: &mut String, name: &str, value: &str| {
        writeln!(out, "  {name:<17}{value}").unwrap();
    };

    if let Some(v) = &run.propolis_version {
        let commit = or_unknown(&run.propolis_commit);
        field(out, "Propolis commit", &format!("{commit} ({v})"));
    }
    if let Some(sha) = &run.crucible_commit {
        field(out, "Crucible commit", sha);
    }
    field(out, "VMM ID", or_unknown(&run.vmm_id));

    match &run.instance {
        Some(i) => {
            field(out, "Instance ID", or_unknown(&i.id));
            field(out, "Name", or_unknown(&i.name));
            let cpus = i.cpus.map_or("?".to_string(), |c| c.to_string());
            let mem = i.memory_mb.map_or("?".to_string(), |m| m.to_string());
            field(out, "Board", &format!("{cpus} vCPUs, {mem} MiB"));
            field(out, "Silo ID", or_unknown(&i.silo_id));
            field(out, "Project ID", or_unknown(&i.project_id));
            field(
                out,
                "Sled ID",
                &format!(
                    "{} ({})",
                    or_unknown(&i.sled_id),
                    or_unknown(&i.sled_serial)
                ),
            );
        }
        None if run.started.is_some() => {
            writeln!(out, "  No VM was initialized by this process.").unwrap()
        }
        None => {}
    }

    for disk in &run.disks {
        writeln!(out).unwrap();
        render_disk(out, disk);
    }

    if let Some(p) = &run.panic {
        writeln!(out).unwrap();
        writeln!(
            out,
            "  Panic in thread '{}' after {}",
            p.thread,
            time(&p.after)
        )
        .unwrap();
        writeln!(out, "    at {}", p.location).unwrap();
        writeln!(out, "    {}", p.message).unwrap();
        if let Some(id) = &p.upstairs_id {
            let name = run
                .disks
                .iter()
                .find(|d| d.backend_id.as_ref() == Some(id))
                .and_then(|d| d.name.as_deref())
                .unwrap_or("?");
            writeln!(out, "    Disk {name}, upstairs {id}").unwrap();
        }
        if let Some(io) = &p.io {
            render_panic_io(out, run, io);
        }
    }
}

fn render_panic_io(out: &mut String, run: &Run, io: &PanicIo) {
    let op = or_unknown(&io.op);
    let blocks = match (io.start_block, io.count) {
        (Some(start), Some(1)) => format!("block {start}"),
        (Some(start), Some(n)) => {
            format!("blocks {start}-{}", start + n.saturating_sub(1))
        }
        _ => "unknown blocks".to_string(),
    };
    let extent_size = run
        .disks
        .iter()
        .filter_map(|d| d.crucible.as_ref())
        .find(|c| c.session_id.as_ref() == Some(&io.session_id))
        .and_then(|c| c.extent_size);
    let extents = match extent_size.and_then(|size| io.extents(size)) {
        Some(range) => format!(" in {}", extents(range)),
        None => String::new(),
    };
    writeln!(
        out,
        "    Client {} job {}: {op} of {blocks}{extents}",
        io.client, io.job_id
    )
    .unwrap();
    if !io.states.is_empty() {
        let states: Vec<String> = io
            .states
            .iter()
            .enumerate()
            .map(|(c, s)| format!("[{c}] {s}"))
            .collect();
        writeln!(out, "    Job state   {}", states.join(", ")).unwrap();
    }
    // The first client to return read data has its data kept in the job,
    // and each later client's data is compared with it.
    let done: Vec<u32> = io
        .states
        .iter()
        .enumerate()
        .filter(|(_, s)| *s == "Done")
        .map(|(c, _)| c as u32)
        .collect();
    if io.error == "read hash mismatch" && !done.is_empty() {
        let from = match done.as_slice() {
            [c] => format!("client {c}"),
            _ => format!("one of {}", clients(&done)),
        };
        writeln!(
            out,
            "    Client {}'s data did not match the data from {from}",
            io.client
        )
        .unwrap();
    }
}

/// Render panic reports as text.
pub fn panic_report(reports: &[PanicReport]) -> String {
    let mut out = String::new();
    let Some(first) = reports.first() else {
        return "No panic found.\n".to_string();
    };
    writeln!(out, "Files, oldest first:").unwrap();
    for file in &first.files {
        writeln!(out, "  {file}").unwrap();
    }
    for report in reports {
        writeln!(out).unwrap();
        render_run(&mut out, &report.run);
        if report.extents.is_empty() {
            continue;
        }
        let blocks = report
            .run
            .disks
            .first()
            .and_then(|d| d.crucible.as_ref())
            .and_then(|c| c.extent_size)
            .map(|size| {
                let first = report.extents[0] * size;
                let last =
                    (report.extents[report.extents.len() - 1] + 1) * size - 1;
                format!(" (blocks {first}-{last})")
            })
            .unwrap_or_default();
        writeln!(out).unwrap();
        writeln!(
            out,
            "  History of {}{blocks}, oldest first:",
            extents(
                report.extents[0]..=report.extents[report.extents.len() - 1]
            )
        )
        .unwrap();
        let mut date = None;
        for event in &report.history {
            let day = event.time.date_naive();
            if date != Some(day) {
                writeln!(out, "  {day}").unwrap();
                date = Some(day);
            }
            render_event(&mut out, event);
        }
    }
    out
}

fn render_event(out: &mut String, event: &Event) {
    let mut text = describe(&event.what);
    if event.count > 1
        && let Some(until) = event.until
    {
        write!(
            text,
            " ({} times, until {})",
            event.count,
            until.format("%H:%M:%S%.3f")
        )
        .unwrap();
    }
    writeln!(out, "    {}  {text}", event.time.format("%H:%M:%S%.3f")).unwrap();
}

fn describe(what: &What) -> String {
    match what {
        What::ReconcileNeeded { extent, clients } => {
            let meta: Vec<String> = clients
                .iter()
                .enumerate()
                .map(|(c, m)| match m {
                    Some(m) => format!(
                        "[{c}] gen {} flush {}{}",
                        m.generation,
                        m.flush,
                        if m.dirty { " dirty" } else { "" }
                    ),
                    None => format!("[{c}] none"),
                })
                .collect();
            format!(
                "reconcile: extent {extent} needs reconciliation: {}",
                meta.join(", ")
            )
        }
        What::ReconcileStart { id, tasks } => {
            format!("reconcile {id} started with {tasks} tasks")
        }
        What::ReconcileRepair {
            extent,
            task_id,
            source,
            dests,
        } => format!(
            "reconcile: task {task_id} will repair extent {extent} from \
             client {source} to {}",
            clients(dests)
        ),
        What::ReconcileRepairSent {
            extent,
            task_id,
            clients: to,
        } => format!(
            "reconcile: task {task_id} (extent {extent}) sent to {}",
            clients(to)
        ),
        What::ReconcileDone => "reconcile done".to_string(),
        What::Transition { client, from, to } => {
            format!("client {client} {from} -> {to}")
        }
        What::ClientStopped { client, reason } => {
            format!("client {client} stopped: {reason}")
        }
        What::Faulted { client, reason } => match reason {
            Some(r) => format!("client {client} faulted: {r}"),
            None => format!("client {client} faulted"),
        },
        What::LiveRepairReady { client } => {
            format!("client {client} ready for live repair")
        }
        What::LiveRepairStart {
            id,
            clients: to,
            source,
        } => {
            let source = source.map_or("an unknown client".to_string(), |s| {
                format!("client {s}")
            });
            format!(
                "live repair {id} started, repairing {} from {source}",
                clients(to)
            )
        }
        What::LiveRepairExtent {
            extent,
            close_id,
            repair_id,
            noop_id,
            reopen_id,
        } => format!(
            "live repair: extent {extent} started, jobs close {close_id} \
             repair {repair_id} noop {noop_id} reopen {reopen_id}"
        ),
        What::LiveRepairNotNeeded { extent, source } => {
            let source = source
                .map_or(String::new(), |s| format!(" (source client {s})"));
            format!("live repair: no repair needed for extent {extent}{source}")
        }
        What::LiveRepairCopy {
            extent,
            source,
            dests,
        } => format!(
            "live repair: extent {extent} repaired from client {source} to {}",
            clients(dests)
        ),
        What::LiveRepairProgress {
            extent,
            extent_count,
        } => {
            let of = extent_count.map_or(String::new(), |n| format!(" of {n}"));
            format!("live repair: last started extent {extent}{of}")
        }
        What::LiveRepairEnd { result } => format!("live repair {result}"),
        What::Panic {
            client,
            job_id,
            error,
        } => format!(
            "panic: client {client} job {job_id} {error} \
             (the panic has no time; this is the last record before it)"
        ),
    }
}

fn clients(list: &[u32]) -> String {
    let ids: Vec<String> = list.iter().map(u32::to_string).collect();
    match list {
        [_] => format!("client {}", ids[0]),
        _ => format!("clients {}", ids.join(", ")),
    }
}

fn extents(range: std::ops::RangeInclusive<u64>) -> String {
    if range.start() == range.end() {
        format!("extent {}", range.start())
    } else {
        format!("extents {}-{}", range.start(), range.end())
    }
}

fn render_disk(out: &mut String, disk: &Disk) {
    let name = disk
        .name
        .as_deref()
        .or(disk.device_id.as_deref())
        .or(disk.backend_id.as_deref())
        .unwrap_or("?");
    let mut header = format!("  Disk {name}");
    if let Some(kind) = &disk.kind {
        write!(header, ", {kind}").unwrap();
    }
    if let Some(pci) = &disk.pci {
        write!(header, " at PCI {pci}").unwrap();
    }
    writeln!(out, "{header}").unwrap();

    match &disk.crucible {
        Some(cru) => render_crucible(out, cru),
        None => writeln!(
            out,
            "    Backend      {} (not Crucible)",
            or_unknown(&disk.backend_id)
        )
        .unwrap(),
    }
}

fn render_crucible(out: &mut String, cru: &Crucible) {
    writeln!(out, "    Upstairs ID  {}", cru.upstairs_id).unwrap();
    writeln!(out, "    Session ID   {}", or_unknown(&cru.session_id)).unwrap();
    if let (Some(size), Some(b), Some(s), Some(n)) = (
        cru.size_bytes,
        cru.block_size,
        cru.extent_size,
        cru.extent_count,
    ) {
        let encrypted = match cru.encrypted {
            Some(true) => ", encrypted",
            Some(false) => ", not encrypted",
            None => "",
        };
        writeln!(
            out,
            "    Region       {}: {n} extents x {s} blocks x {b} bytes\
             {encrypted}",
            bytes(size)
        )
        .unwrap();
    }
    if let Some(ro) = cru.read_only {
        writeln!(out, "    Read only    {ro}").unwrap();
    }

    let targets: Vec<String> = cru
        .downstairs
        .iter()
        .map(|d| {
            if d.ip.contains(':') {
                format!("[{}]:{}", d.ip, d.port)
            } else {
                format!("{}:{}", d.ip, d.port)
            }
        })
        .collect();
    let width = targets.iter().map(String::len).max().unwrap_or(0);
    writeln!(out, "    Client  {:<width$}  Region ID", "Downstairs").unwrap();
    for (d, target) in cru.downstairs.iter().zip(&targets) {
        let client = d.client.map_or("?".to_string(), |c| c.to_string());
        let regions = if d.region_ids.is_empty() {
            "?".to_string()
        } else {
            d.region_ids.join(", ")
        };
        writeln!(out, "    {client:<6}  {target:<width$}  {regions}").unwrap();
    }
}

fn bytes(n: u64) -> String {
    const GIB: u64 = 1 << 30;
    const MIB: u64 = 1 << 20;
    if n.is_multiple_of(GIB) {
        format!("{} GiB", n / GIB)
    } else if n.is_multiple_of(MIB) {
        format!("{} MiB", n / MIB)
    } else {
        format!("{n} bytes")
    }
}
