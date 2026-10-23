// Copyright 2026 Oxide Computer Company

//! Build a summary from the records in one or more log files.
use chrono::{DateTime, Utc};
use regex::Captures;
use serde::Serialize;

use crate::catalog::{self, Id};
use crate::record::Record;

#[derive(Debug, Serialize)]
pub struct Summary {
    /// The files read, oldest first.
    pub files: Vec<String>,
    pub runs: Vec<Run>,
}

/// One propolis-server process.
#[derive(Debug, Serialize)]
pub struct Run {
    pub pid: Option<u64>,
    /// None when the logs begin after this process started.
    pub started: Option<DateTime<Utc>>,
    pub first_record: DateTime<Utc>,
    pub propolis_version: Option<String>,
    pub propolis_commit: Option<String>,
    pub crucible_commit: Option<String>,
    pub vmm_id: Option<String>,
    pub instance: Option<Instance>,
    pub disks: Vec<Disk>,
    pub panic: Option<Panic>,
}

#[derive(Debug, Default, Serialize)]
pub struct Instance {
    pub id: Option<String>,
    pub name: Option<String>,
    pub silo_id: Option<String>,
    pub project_id: Option<String>,
    pub sled_id: Option<String>,
    pub sled_serial: Option<String>,
    pub cpus: Option<u32>,
    pub memory_mb: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
pub struct Disk {
    /// The disk's serial number, which is the disk name.
    pub name: Option<String>,
    pub device_id: Option<String>,
    pub kind: Option<String>,
    /// bus.device.function
    pub pci: Option<String>,
    pub backend_id: Option<String>,
    pub crucible: Option<Crucible>,
    #[serde(skip)]
    pci_order: Option<(u32, u32, u32)>,
}

#[derive(Debug, Serialize)]
pub struct Crucible {
    pub upstairs_id: String,
    pub session_id: Option<String>,
    pub block_size: Option<u64>,
    /// Blocks per extent.
    pub extent_size: Option<u64>,
    pub extent_count: Option<u64>,
    pub size_bytes: Option<u64>,
    pub encrypted: Option<bool>,
    pub read_only: Option<bool>,
    pub downstairs: Vec<Downstairs>,
}

#[derive(Debug, Serialize)]
pub struct Downstairs {
    pub client: Option<u32>,
    pub ip: String,
    pub port: u16,
    /// Every region UUID this downstairs reported, normally just one.
    pub region_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Panic {
    /// Time of the last record before the panic.
    pub after: DateTime<Utc>,
    pub thread: String,
    pub location: String,
    /// The first line of the panic message.
    pub message: String,
    /// The crucible upstairs whose session or UUID the panic names.
    pub upstairs_id: Option<String>,
    /// The IO a crucible upstairs panicked on, if the message names one.
    pub io: Option<PanicIo>,
}

#[derive(Debug, Serialize)]
pub struct PanicIo {
    pub session_id: String,
    /// The client whose reply caused the panic.
    pub client: u32,
    pub job_id: u64,
    pub error: String,
    pub op: Option<String>,
    pub start_block: Option<u64>,
    /// Number of blocks.
    pub count: Option<u64>,
    pub block_size: Option<u64>,
    /// Each client's state for the job, in client order.
    pub states: Vec<String>,
}

impl PanicIo {
    /// The extents the IO covers, given the blocks per extent.
    pub fn extents(
        &self,
        extent_size: u64,
    ) -> Option<std::ops::RangeInclusive<u64>> {
        let start = self.start_block?;
        let end = start + self.count?.max(1) - 1;
        Some(start / extent_size..=end / extent_size)
    }
}

/// Summarize log files, given as (name, contents) pairs in any order.
pub fn summarize(files: &[(String, String)]) -> Summary {
    let files = ordered(files);
    let mut runs: Vec<RunBuilder> = Vec::new();
    // The run of the most recent record.
    let mut current: Option<usize> = None;
    // The run a panic message is being collected for.
    let mut panicking: Option<usize> = None;

    for (_, text) in &files {
        for line in text.lines() {
            if let Some(record) = Record::parse(line) {
                panicking = None;
                let i = run_for(&mut runs, &record);
                runs[i].add(&record);
                current = Some(i);
            } else if let Some(c) = catalog::captures_text(Id::Panic, line) {
                if let Some(i) = current {
                    runs[i].start_panic(&c);
                    panicking = Some(i);
                }
            } else if let Some(i) = panicking {
                // The message runs until the note Rust prints after it,
                // or until SMF says the service exited.
                if line.starts_with("note: ") || line.starts_with("[ ") {
                    panicking = None;
                } else {
                    runs[i].panic_line(line);
                }
            }
        }
    }

    Summary {
        files: files.iter().map(|(name, _)| name.clone()).collect(),
        runs: runs.into_iter().map(RunBuilder::finish).collect(),
    }
}

/// Rotated logs are each in order, so order the files by their first
/// record and read them one after the other.
pub(crate) fn ordered(files: &[(String, String)]) -> Vec<&(String, String)> {
    let mut files: Vec<&(String, String)> = files.iter().collect();
    files.sort_by_key(|(_, text)| {
        text.lines().find_map(Record::parse).map(|r| r.time)
    });
    files
}

/// Find the run a record belongs to, starting a new one if needed.
fn run_for(runs: &mut Vec<RunBuilder>, record: &Record) -> usize {
    let starting = catalog::captures(Id::PropolisVersion, record).is_some();
    if !starting
        && let Some(i) = runs.iter().rposition(|r| r.run.pid == record.pid)
    {
        return i;
    }
    runs.push(RunBuilder::new(record, starting));
    runs.len() - 1
}

struct RunBuilder {
    run: Run,
    last_time: DateTime<Utc>,
    crucibles: Vec<Crucible>,
    panic_text: String,
}

impl RunBuilder {
    fn new(record: &Record, starting: bool) -> RunBuilder {
        RunBuilder {
            run: Run {
                pid: record.pid,
                started: starting.then_some(record.time),
                first_record: record.time,
                propolis_version: None,
                propolis_commit: None,
                crucible_commit: None,
                vmm_id: None,
                instance: None,
                disks: Vec::new(),
                panic: None,
            },
            last_time: record.time,
            crucibles: Vec::new(),
            panic_text: String::new(),
        }
    }

    fn add(&mut self, record: &Record) {
        self.last_time = record.time;
        let run = &mut self.run;

        if run.vmm_id.is_none()
            && let Some(c) = catalog::captures(Id::ZoneName, record)
        {
            run.vmm_id = Some(c["vmm_id"].to_string());
        }

        if let Some(c) = catalog::captures(Id::PropolisVersion, record) {
            run.propolis_version = Some(c["version"].to_string());
            run.propolis_commit = Some(c["commit"].to_string());
        } else if let Some(c) = catalog::captures(Id::VmProperties, record) {
            let instance = run.instance.get_or_insert_default();
            instance.id = Some(c["id"].to_string());
            instance.name = Some(c["name"].to_string());
            instance.silo_id = Some(c["silo_id"].to_string());
            instance.project_id = Some(c["project_id"].to_string());
            instance.sled_id = Some(c["sled_id"].to_string());
            instance.sled_serial = Some(c["sled_serial"].to_string());
        } else if let Some(c) = catalog::captures(Id::StorageDevice, record) {
            run.disks.push(storage_device(&c, record));
        } else if catalog::captures(Id::CrucibleBackend, record).is_some()
            && let Some(id) = record.field("backend_id")
        {
            self.crucible(id);
        }

        // The board is in the same record as the properties.
        if let Some(c) = catalog::captures(Id::VmBoard, record) {
            let instance = self.run.instance.get_or_insert_default();
            instance.cpus = c["cpus"].parse().ok();
            instance.memory_mb = c["memory_mb"].parse().ok();
        }

        if let Some(c) = catalog::captures(Id::CrucibleComponent, record) {
            let upstairs_id = c["upstairs_id"].to_string();
            self.add_crucible(&upstairs_id, record);
        }
    }

    /// Handle a record logged by a crucible upstairs.
    fn add_crucible(&mut self, upstairs_id: &str, record: &Record) {
        if let Some(c) = catalog::captures(Id::CrucibleVersion, record) {
            self.run.crucible_commit = Some(c["git_sha"].to_string());
            return;
        }

        let cru = self.crucible(upstairs_id);
        if let Some(c) = catalog::captures(Id::RegionDefinition, record) {
            cru.block_size = c["block_size"].parse().ok();
            cru.extent_size = c["extent_size"].parse().ok();
            cru.extent_count = c["extent_count"].parse().ok();
            cru.encrypted = c["encrypted"].parse().ok();
            cru.size_bytes =
                match (cru.block_size, cru.extent_size, cru.extent_count) {
                    (Some(b), Some(s), Some(n)) => Some(b * s * n),
                    _ => None,
                };
        } else if let Some(c) = catalog::captures(Id::SessionId, record) {
            cru.session_id = Some(c["session_id"].to_string());
        } else if let Some(c) = catalog::captures(Id::UpstairsOpts, record) {
            cru.read_only = c["read_only"].parse().ok();
            // The opts are logged once per upstairs, but don't repeat the
            // targets if they show up again.
            if cru.downstairs.is_empty() {
                for (client, t) in
                    catalog::captures_iter(Id::Target, &c["targets"])
                        .enumerate()
                {
                    let (ip, port) = target(&t);
                    cru.downstairs.push(Downstairs {
                        client: Some(client as u32),
                        ip,
                        port,
                        region_ids: Vec::new(),
                    });
                }
            }
        } else if let Some(c) = catalog::captures(Id::RegionUuid, record) {
            let client = record.field("client").and_then(|s| s.parse().ok());
            let Some(t) = catalog::captures_text(Id::Target, &c["target"])
            else {
                return;
            };
            let (ip, port) = target(&t);
            let region_id = c["region_id"].to_string();

            // A client at a new address is a replacement downstairs, so
            // give it its own entry.
            match cru
                .downstairs
                .iter_mut()
                .find(|d| d.client == client && d.ip == ip && d.port == port)
            {
                Some(d) => {
                    if !d.region_ids.contains(&region_id) {
                        d.region_ids.push(region_id);
                    }
                }
                None => cru.downstairs.push(Downstairs {
                    client,
                    ip,
                    port,
                    region_ids: vec![region_id],
                }),
            }
        }
    }

    /// Find or create the crucible upstairs with this UUID.
    fn crucible(&mut self, upstairs_id: &str) -> &mut Crucible {
        let i = match self
            .crucibles
            .iter()
            .position(|c| c.upstairs_id == upstairs_id)
        {
            Some(i) => i,
            None => {
                self.crucibles.push(Crucible {
                    upstairs_id: upstairs_id.to_string(),
                    session_id: None,
                    block_size: None,
                    extent_size: None,
                    extent_count: None,
                    size_bytes: None,
                    encrypted: None,
                    read_only: None,
                    downstairs: Vec::new(),
                });
                self.crucibles.len() - 1
            }
        };
        &mut self.crucibles[i]
    }

    fn start_panic(&mut self, c: &Captures) {
        self.run.panic = Some(Panic {
            after: self.last_time,
            thread: c["thread"].to_string(),
            location: c["location"].to_string(),
            message: String::new(),
            upstairs_id: None,
            io: None,
        });
        self.panic_text.clear();
    }

    fn panic_line(&mut self, line: &str) {
        if let Some(panic) = &mut self.run.panic
            && panic.message.is_empty()
        {
            panic.message = line.to_string();
        }
        self.panic_text.push_str(line);
        self.panic_text.push('\n');
    }

    fn finish(mut self) -> Run {
        let run = &mut self.run;

        if let Some(panic) = &mut run.panic {
            panic.upstairs_id = self
                .crucibles
                .iter()
                .find(|c| {
                    self.panic_text.contains(&c.upstairs_id)
                        || c.session_id
                            .as_ref()
                            .is_some_and(|s| self.panic_text.contains(s))
                })
                .map(|c| c.upstairs_id.clone());
            panic.io = panic_io(&panic.message, &self.panic_text);
        }

        // Attach each upstairs to the disk it backs.  An upstairs with no
        // disk (the log began after the disks were created) still gets
        // listed as a disk of its own.
        for cru in self.crucibles {
            match run.disks.iter_mut().find(|d| {
                d.backend_id.as_deref() == Some(cru.upstairs_id.as_str())
            }) {
                Some(disk) => disk.crucible = Some(cru),
                None => run.disks.push(Disk {
                    backend_id: Some(cru.upstairs_id.clone()),
                    crucible: Some(cru),
                    ..Default::default()
                }),
            }
        }
        run.disks
            .sort_by_key(|d| (d.pci_order.is_none(), d.pci_order));

        self.run
    }
}

/// Pull the IO out of a crucible panic message.
fn panic_io(message: &str, text: &str) -> Option<PanicIo> {
    let c = catalog::captures_text(Id::PanicIoError, message)?;
    let job = catalog::captures_text(Id::PanicIo, text);
    let job_field = |name: &str| job.as_ref().map(|j| j[name].to_string());
    let job_number =
        |name: &str| job.as_ref().and_then(|j| j[name].parse::<u64>().ok());
    Some(PanicIo {
        session_id: c["session_id"].to_string(),
        client: c["client"].parse().ok()?,
        job_id: c["job_id"].parse().ok()?,
        error: c["error"].to_string(),
        op: job_field("op"),
        start_block: job_number("start_block"),
        count: job_number("count"),
        block_size: job_number("block_size"),
        states: job_field("states").map_or(Vec::new(), |s| {
            s.split(", ").map(str::to_string).collect()
        }),
    })
}

fn storage_device(c: &Captures, record: &Record) -> Disk {
    let pci_order =
        match (c["bus"].parse(), c["device"].parse(), c["function"].parse()) {
            (Ok(b), Ok(d), Ok(f)) => Some((b, d, f)),
            _ => None,
        };
    Disk {
        name: c.name("serial").map(|m| serial_name(m.as_str())),
        device_id: record.field("device_id").map(str::to_string),
        kind: Some(c["kind"].to_lowercase()),
        pci: pci_order.map(|(b, d, f)| format!("{b}.{d}.{f}")),
        backend_id: c
            .name("backend_uuid")
            .or(c.name("backend_name"))
            .map(|m| m.as_str().to_string()),
        crucible: None,
        pci_order,
    }
}

/// Decode a serial number logged as a list of bytes, NUL padded.
fn serial_name(bytes: &str) -> String {
    let bytes: Vec<u8> = bytes
        .split(',')
        .filter_map(|b| b.trim().parse().ok())
        .take_while(|&b| b != 0)
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

fn target(c: &Captures) -> (String, u16) {
    let ip = c.name("ip6").or(c.name("ip4")).map_or("", |m| m.as_str());
    (ip.to_string(), c["port"].parse().unwrap_or(0))
}
