// Copyright 2026 Oxide Computer Company

//! Render a summary as text.
use chrono::{DateTime, SecondsFormat, Utc};
use std::fmt::Write;

use crate::summary::{Crucible, Disk, Run, Summary};

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
