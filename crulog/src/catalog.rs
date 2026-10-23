// Copyright 2026 Oxide Computer Company

//! The catalog of log lines crulog depends on.
//!
//! Every log message (or structured field) that crulog pulls information
//! out of is listed here, along with where it is produced and a real
//! sample line taken from a propolis-server log.  If crucible or propolis
//! change one of these messages, the matching entry here is the only
//! place that needs updating, and the tests at the bottom check that
//! each pattern still matches its sample.
//!
//! Besides `msg`, crulog uses these bunyan fields directly: `time`,
//! `pid`, `hostname`, `component`, `session_id`, `client` (set by the
//! upstairs on each downstairs client's logger), `backend_id`, `spec`,
//! and `properties` (set by propolis).
use regex::{CaptureMatches, Captures, Regex};
use std::sync::LazyLock;

use crate::record::Record;

macro_rules! uuid {
    () => {
        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Id {
    PropolisVersion,
    ZoneName,
    VmProperties,
    VmBoard,
    StorageDevice,
    CrucibleBackend,
    CrucibleComponent,
    CrucibleVersion,
    RegionDefinition,
    SessionId,
    UpstairsOpts,
    Target,
    RegionUuid,
    Panic,
    PanicIoError,
    PanicIo,
    ReconcileNeeded,
    ExtentMetadata,
    ReconcileStart,
    ReconcileTaskList,
    ReconcileRepairTask,
    ClientIds,
    ReconcileSend,
    ReconcileDone,
    Transition,
    ClientStopped,
    TooManyOutstanding,
    FaultSkip,
    LiveRepairReady,
    LiveRepairStart,
    LiveRepairExtent,
    LiveRepairNotNeeded,
    LiveRepairCopy,
    LiveRepairEnd,
}

/// What text an entry's pattern is matched against.
#[derive(Clone, Copy, Debug)]
pub enum Input {
    /// The record's `msg`.
    Msg,
    /// A string field of the record.
    Field(&'static str),
    /// A piece of text pulled out by another entry.
    Fragment,
    /// A whole line that is not a bunyan record.
    RawLine,
}

/// Where the text an entry matches is produced.
#[derive(Debug)]
pub struct Source {
    pub repo: &'static str,
    /// None when the source file has not been checked.
    pub file: Option<&'static str>,
    /// The format string, or a description of the output.
    pub text: &'static str,
}

#[derive(Debug)]
pub struct Entry {
    pub id: Id,
    pub input: Input,
    /// When set, the record's `msg` must be exactly this.
    pub when_msg: Option<&'static str>,
    pub source: Source,
    pub pattern: &'static str,
    /// A sample, matched the same way a real log line is.
    pub sample: &'static str,
}

// The `spec` field of this sample is trimmed after the board; the rest
// of it (CPUID, every device) is long and not used.
const VM_INIT_SAMPLE: &str = r##"{"msg":"initializing new VM","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:11:49.900639252Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"bootrom":"/opt/oxide/propolis-server/blob/OVMF_CODE.fd","use_reservoir":true,"properties":"InstanceProperties {\n    id: 617281d1-669b-4261-a215-3c32bb78a8af,\n    name: \"newallio-11-inst\",\n    description: \"Omicron-managed VM\",\n    metadata: InstanceMetadata {\n        silo_id: 7bd7623a-68ed-4636-8ecb-b59e3b068787,\n        project_id: 759beaf2-517d-4d24-bc17-1eed69bc8801,\n        sled_id: db183874-65b5-4263-a1c1-ddb2737ae0e9,\n        sled_serial: \"BRM42220009\",\n        sled_revision: 6,\n        sled_model: \"913-0000019\",\n    },\n}","spec":"Spec {\n    board: Board {\n        cpus: 8,\n        memory_mb: 8192,\n        chipset: I440Fx(\n            I440Fx {\n                enable_pcie: false,\n            },\n        ),\n        guest_hv_interface: Bhyve,\n        acpi_variant: V0,\n    },\n}"}"##;

pub static ENTRIES: &[Entry] = &[
    Entry {
        id: Id::PropolisVersion,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "propolis",
            file: None,
            text: "Running {version} ({commit}) {branch}, \
                   bhyve API {v}, viona API {v}",
        },
        pattern: r"^Running (?<version>\S+) \((?<commit>[0-9a-f]+)\) [^,]*, bhyve API v\d+, viona API v\d+$",
        sample: r##"{"msg":"Running v0.1.0-948 (3b60af5) master, bhyve API v18, viona API v7","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:11:49.515744806Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155}"##,
    },
    Entry {
        id: Id::ZoneName,
        input: Input::Field("hostname"),
        when_msg: None,
        source: Source {
            repo: "omicron",
            file: None,
            text: "propolis zone name, oxz_propolis-server_{propolis_id}",
        },
        pattern: concat!("^oxz_propolis-server_(?<vmm_id>", uuid!(), ")$"),
        sample: r##"{"msg":"Starting server...","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:11:49.537488532Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155}"##,
    },
    Entry {
        id: Id::VmProperties,
        input: Input::Field("properties"),
        when_msg: Some("initializing new VM"),
        source: Source {
            repo: "propolis",
            file: None,
            text: "{:#?} of InstanceProperties",
        },
        pattern: concat!(
            r#"(?s)^InstanceProperties \{\s*id: (?<id>"#,
            uuid!(),
            r#"),\s*name: "(?<name>[^"]*)",.*?silo_id: (?<silo_id>"#,
            uuid!(),
            r#"),\s*project_id: (?<project_id>"#,
            uuid!(),
            r#"),\s*sled_id: (?<sled_id>"#,
            uuid!(),
            r#"),\s*sled_serial: "(?<sled_serial>[^"]*)""#,
        ),
        sample: VM_INIT_SAMPLE,
    },
    Entry {
        id: Id::VmBoard,
        input: Input::Field("spec"),
        when_msg: Some("initializing new VM"),
        source: Source {
            repo: "propolis",
            file: None,
            text: "{:#?} of the instance Spec",
        },
        pattern: r"(?s)^Spec \{\s*board: Board \{\s*cpus: (?<cpus>\d+),\s*memory_mb: (?<memory_mb>\d+),",
        sample: VM_INIT_SAMPLE,
    },
    Entry {
        id: Id::StorageDevice,
        input: Input::Field("spec"),
        when_msg: Some("Creating storage device"),
        source: Source {
            repo: "propolis",
            file: None,
            text: "{:?} of the storage device spec, field device_id",
        },
        pattern: concat!(
            r"^(?<kind>Nvme|Virtio)\((?:NvmeDisk|VirtioDisk) \{ ",
            r"backend_id: (?:Uuid\((?<backend_uuid>",
            uuid!(),
            r#")\)|Name\("(?<backend_name>[^"]*)"\)), "#,
            r"pci_path: PciPath \{ bus: (?<bus>\d+), ",
            r"device: (?<device>\d+), function: (?<function>\d+) \}",
            r"(?:, serial_number: \[(?<serial>[0-9, ]*)\])?",
        ),
        sample: r##"{"msg":"Creating storage device","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:11:50.005904149Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"spec":"Nvme(NvmeDisk { backend_id: Uuid(3c432392-26fe-4e12-94b0-a5c0ac6a25f0), pci_path: PciPath { bus: 0, device: 17, function: 0 }, serial_number: [110, 101, 119, 97, 108, 108, 105, 111, 45, 49, 49, 45, 100, 105, 115, 107, 49, 0, 0, 0], has_write_cache: true })","device_id":"3c432392-26fe-4e12-94b0-a5c0ac6a25f0:device"}"##,
    },
    Entry {
        id: Id::CrucibleBackend,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "propolis",
            file: None,
            text: "Creating Crucible disk, field backend_id",
        },
        pattern: r"^Creating Crucible disk$",
        sample: r##"{"msg":"Creating Crucible disk","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:11:50.005939947Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"backend_id":"3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::CrucibleComponent,
        input: Input::Field("component"),
        when_msg: None,
        source: Source {
            repo: "propolis",
            file: None,
            text: "logger component for each crucible backend, \
                   crucible-{backend_id}",
        },
        pattern: concat!("^crucible-(?<upstairs_id>", uuid!(), ")$"),
        sample: r##"{"msg":"Upstairs starts","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:11:50.006054683Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::CrucibleVersion,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/lib.rs"),
            text: "Crucible Version: {:#?}",
        },
        pattern: r#"(?s)^Crucible Version: BuildInfo \{.*?\bgit_sha: "(?<git_sha>[0-9a-f]+)""#,
        sample: r##"{"msg":"Crucible Version: BuildInfo {\n    version: \"0.0.1\",\n    git_sha: \"bd62980710dd3d6d3524aa7f6617127942b75002\",\n    git_commit_timestamp: \"VERGEN_IDEMPOTENT_OUTPUT\",\n    git_branch: \"master\",\n    rustc_semver: \"1.97.1\",\n    rustc_channel: \"stable\",\n    rustc_host_triple: \"x86_64-unknown-illumos\",\n    rustc_commit_sha: \"8bab26f4f68e0e26f0bb7960be334d5b520ea452\",\n    cargo_triple: \"x86_64-unknown-illumos\",\n    debug: false,\n    opt_level: 3,\n}","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:11:50.006104327Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::RegionDefinition,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/lib.rs"),
            text: "Using region definition {:?} \
                   (RegionDefinition is in common/src/region.rs)",
        },
        pattern: concat!(
            r"^Using region definition RegionDefinition \{ ",
            r"block_size: (?<block_size>\d+), ",
            r"extent_size: Block \{ value: (?<extent_size>\d+), shift: \d+ \}, ",
            r"extent_count: (?<extent_count>\d+), ",
            r"uuid: [0-9a-f-]+, encrypted: (?<encrypted>true|false)",
        ),
        sample: r##"{"msg":"Using region definition RegionDefinition { block_size: 4096, extent_size: Block { value: 16384, shift: 12 }, extent_count: 800, uuid: 00000000-0000-0000-0000-000000000000, encrypted: true, database_read_version: 1, database_write_version: 1 }","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:11:50.006141908Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::SessionId,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/upstairs.rs"),
            text: "Crucible {} has session id: {}",
        },
        pattern: concat!(
            "^Crucible (?<upstairs_id>",
            uuid!(),
            ") has session id: (?<session_id>",
            uuid!(),
            ")$",
        ),
        sample: r##"{"msg":"Crucible 3c432392-26fe-4e12-94b0-a5c0ac6a25f0 has session id: 86ceec0d-72e8-499a-a4f0-40d790c60878","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:11:50.007063989Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::UpstairsOpts,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/upstairs.rs"),
            text: "Upstairs opts: {} \
                   (Display for CrucibleOpts is in \
                   crucible-client-types/src/lib.rs)",
        },
        pattern: concat!(
            "^Upstairs opts: Upstairs UUID: (?<upstairs_id>",
            uuid!(),
            r"), Targets: \[(?<targets>.*?)\], lossy: ",
            r".* read_only: (?<read_only>true|false)$",
        ),
        sample: r##"{"msg":"Upstairs opts: Upstairs UUID: 3c432392-26fe-4e12-94b0-a5c0ac6a25f0, Targets: [[fd00:1122:3344:129::23]:19000, [fd00:1122:3344:108::c]:19040, [fd00:1122:3344:104::9]:19037], lossy: false, flush_timeout: None, key populated: true,  cert_pem populated: false,  key_pem populated: false,  root_cert_pem populated: false,  Control: None,  read_only: false","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:11:50.007161863Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::Target,
        input: Input::Fragment,
        when_msg: None,
        source: Source {
            repo: "rust std",
            file: None,
            text: "{:?} of a SocketAddr, found in the UpstairsOpts targets \
                   and RegionUuid target",
        },
        pattern: r"(?:\[(?<ip6>[0-9a-fA-F:.]+)\]|(?<ip4>\d+\.\d+\.\d+\.\d+)):(?<port>\d+)",
        sample: "[fd00:1122:3344:129::23]:19000, [fd00:1122:3344:108::c]:19040, [fd00:1122:3344:104::9]:19037",
    },
    Entry {
        id: Id::RegionUuid,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/client.rs"),
            text: "downstairs client at {:?} has region UUID {}, \
                   field client",
        },
        pattern: concat!(
            r"^downstairs client at Some\((?<target>[^)]*)\) ",
            "has region UUID (?<region_id>",
            uuid!(),
            ")$",
        ),
        sample: r##"{"msg":"downstairs client at Some([fd00:1122:3344:129::23]:19000) has region UUID 76b3116a-10ed-429e-8df9-183a827f9727","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:18:35.202932481Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"client":"0","":"downstairs","session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::Panic,
        input: Input::RawLine,
        when_msg: None,
        source: Source {
            repo: "rust std",
            file: None,
            text: "thread '{name}' ({tid}) panicked at {location}:",
        },
        pattern: r"^thread '(?<thread>[^']*)'(?: \(\d+\))? panicked at (?<location>.+):$",
        sample: "thread 'tokio-rt-vmm' (19) panicked at /home/build/.cargo/git/checkouts/crucible-0a48bd218bc2bbbc/bd62980/upstairs/src/client.rs:1161:37:",
    },
    Entry {
        id: Id::PanicIoError,
        input: Input::RawLine,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/client.rs"),
            text: "{session_id} [{client_id}] {ds_id} read hash mismatch, \
                   the first line of the panic message",
        },
        pattern: concat!(
            "^(?<session_id>",
            uuid!(),
            r") \[(?<client>\d+)\] (?<job_id>\d+) ",
            r"(?<error>read hash mismatch)\b",
        ),
        sample: "86ceec0d-72e8-499a-a4f0-40d790c60878 [0] 852176 read hash mismatch",
    },
    Entry {
        id: Id::PanicIo,
        input: Input::Fragment,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/client.rs"),
            text: "{:?} of the DownstairsIO job, found in the panic message",
        },
        pattern: concat!(
            r"DownstairsIO \{ work: (?<op>\w+) \{ ",
            r"dependencies: \[[^\]]*\], ",
            r"start_block: BlockIndex\((?<start_block>\d+)\), ",
            r"count: (?<count>\d+), block_size: (?<block_size>\d+) \}, ",
            r"state: ClientData\(\[(?<states>[^\]]*)\]\)",
        ),
        // Trimmed after `replay`; the rest is the read data and IO limits.
        sample: "job: DownstairsIO { work: Read { dependencies: [], start_block: BlockIndex(14857), count: 1, block_size: 4096 }, state: ClientData([InProgress, InProgress, Done]), res: None, acked: true, replay: false,",
    },
    Entry {
        id: Id::ReconcileNeeded,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/mend.rs"),
            text: "extent {i} needs reconciliation: {:?}",
        },
        pattern: r"^extent (?<extent>\d+) needs reconciliation: ClientMap\(ClientData\(\[(?<clients>.*)\]\)\)$",
        sample: r##"{"msg":"extent 0 needs reconciliation: ClientMap(ClientData([Some(ExtentMetadata { generation: 9, flush: 8132, dirty: true }), Some(ExtentMetadata { generation: 9, flush: 8132, dirty: true }), Some(ExtentMetadata { generation: 9, flush: 8132, dirty: true })]))","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:18:35.981788368Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"":"mend","":"downstairs","":"downstairs","session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::ExtentMetadata,
        input: Input::Fragment,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/mend.rs"),
            text: "{:?} of each client's Option<ExtentMetadata>, found in \
                   ReconcileNeeded",
        },
        pattern: concat!(
            r"Some\(ExtentMetadata \{ generation: (?<generation>\d+), ",
            r"flush: (?<flush>\d+), dirty: (?<dirty>true|false) \}\)",
            r"|(?<none>None)",
        ),
        sample: "Some(ExtentMetadata { generation: 9, flush: 8132, dirty: true }), Some(ExtentMetadata { generation: 9, flush: 8132, dirty: true }), Some(ExtentMetadata { generation: 9, flush: 8132, dirty: true })",
    },
    Entry {
        id: Id::ReconcileStart,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/downstairs.rs"),
            text: "starting reconciliation {}: found {:?} extents that need \
                   repair (the count is the length of the task list)",
        },
        pattern: concat!(
            "^starting reconciliation (?<id>",
            uuid!(),
            r"): found (?<tasks>\d+) extents that need repair$",
        ),
        sample: r##"{"msg":"starting reconciliation c772275f-53d2-46cd-a424-79fbc67cbb9f: found 3200 extents that need repair","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:18:36.022626107Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"":"downstairs","":"downstairs","session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::ReconcileTaskList,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/downstairs.rs"),
            text: "Task list: {:?}",
        },
        pattern: r"^Task list: \[(?<tasks>.*)\]$",
        // Trimmed to the tasks for extent 0; the real list has 3200.
        sample: r##"{"msg":"Task list: [ReconcileIO { id: ReconciliationId(1224), op: ExtentFlush { repair_id: ReconciliationId(1224), extent_id: ExtentId(0), client_id: ClientId(0), flush_number: 8133, gen_number: 10 }, state: ClientData([New, New, New]) }, ReconcileIO { id: ReconciliationId(1225), op: ExtentClose { repair_id: ReconciliationId(1225), extent_id: ExtentId(0) }, state: ClientData([New, New, New]) }, ReconcileIO { id: ReconciliationId(1226), op: ExtentRepair { repair_id: ReconciliationId(1226), extent_id: ExtentId(0), source_client_id: ClientId(0), source_repair_address: [fd00:1122:3344:129::23]:23000, dest_clients: [ClientId(1), ClientId(2)] }, state: ClientData([New, New, New]) }, ReconcileIO { id: ReconciliationId(1227), op: ExtentReopen { repair_id: ReconciliationId(1227), extent_id: ExtentId(0) }, state: ClientData([New, New, New]) }]","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:18:36.022592935Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"":"downstairs","":"downstairs","session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::ReconcileRepairTask,
        input: Input::Fragment,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/downstairs.rs"),
            text: "{:?} of an ExtentRepair ReconcileIO, found in \
                   ReconcileTaskList",
        },
        pattern: concat!(
            r"ReconcileIO \{ id: ReconciliationId\((?<id>\d+)\), ",
            r"op: ExtentRepair \{ repair_id: ReconciliationId\(\d+\), ",
            r"extent_id: ExtentId\((?<extent>\d+)\), ",
            r"source_client_id: ClientId\((?<source>\d+)\), ",
            r"source_repair_address: (?<address>[^,]+), ",
            r"dest_clients: \[(?<dests>[^\]]*)\] \}",
        ),
        sample: "ReconcileIO { id: ReconciliationId(1226), op: ExtentRepair { repair_id: ReconciliationId(1226), extent_id: ExtentId(0), source_client_id: ClientId(0), source_repair_address: [fd00:1122:3344:129::23]:23000, dest_clients: [ClientId(1), ClientId(2)] }, state: ClientData([New, New, New]) }",
    },
    Entry {
        id: Id::ClientIds,
        input: Input::Fragment,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/client.rs"),
            text: "{:?} of a list of ClientId, found in ReconcileRepairTask \
                   and LiveRepairCopy",
        },
        pattern: r"ClientId\((?<client>\d+)\)",
        sample: "[ClientId(1), ClientId(2)]",
    },
    Entry {
        id: Id::ReconcileSend,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/client.rs"),
            text: "sending reconcile request {repair_id:?}, field client; \
                   only logged for ExtentRepair",
        },
        pattern: r"^sending reconcile request ReconciliationId\((?<id>\d+)\)$",
        sample: r##"{"msg":"sending reconcile request ReconciliationId(1226)","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:21:17.666199294Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"client":"1","":"downstairs","session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::ReconcileDone,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/downstairs.rs"),
            text: "done with reconciliation",
        },
        pattern: r"^done with reconciliation$",
        sample: r##"{"msg":"done with reconciliation","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:25:48.59751211Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"":"downstairs","":"downstairs","session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::Transition,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/client.rs"),
            text: "Transition from {} to Active, and \
                   Transition from {:?} to Reconcile, field client",
        },
        pattern: r"^Transition from (?<from>.+) to (?<to>\w+)$",
        sample: r##"{"msg":"Transition from Offline to Active","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:35:10.443272702Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"client":"0","":"downstairs","session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::ClientStopped,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/upstairs.rs"),
            text: "downstairs task for {client_id} stopped due to {reason:?}",
        },
        pattern: r"^downstairs task for (?<client>\d+) stopped due to (?<reason>.+)$",
        sample: r##"{"msg":"downstairs task for 0 stopped due to Timeout","v":0,"name":"propolis-server","level":40,"time":"2026-10-22T22:28:20.358199941Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::TooManyOutstanding,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/downstairs.rs"),
            text: "downstairs failed, too many outstanding jobs {work_count} \
                   (or bytes {byte_count}); no client is logged, it is \
                   the one in the FaultSkip that follows",
        },
        pattern: r"^downstairs failed, too many outstanding (?<what>jobs|bytes) (?<count>\d+)$",
        sample: r##"{"msg":"downstairs failed, too many outstanding jobs 1001","v":0,"name":"propolis-server","level":40,"time":"2026-10-22T22:43:48.141589847Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"":"downstairs","":"downstairs","session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::FaultSkip,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/downstairs.rs"),
            text: "[{client_id}] client skip {} in process jobs because fault",
        },
        pattern: r"^\[(?<client>\d+)\] client skip (?<jobs>\d+) in process jobs because fault$",
        sample: r##"{"msg":"[0] client skip 1001 in process jobs because fault","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:43:48.141645262Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"":"downstairs","":"downstairs","session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::LiveRepairReady,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/upstairs.rs"),
            text: "client {client_id} is ready for live-repair",
        },
        pattern: r"^client (?<client>\d+) is ready for live-repair$",
        sample: r##"{"msg":"client 0 is ready for live-repair","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:47:00.62712067Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::LiveRepairStart,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/downstairs.rs"),
            text: "started repair {}",
        },
        pattern: concat!("^started repair (?<id>", uuid!(), ")$"),
        sample: r##"{"msg":"started repair 5f314278-4c80-45b0-8298-4527541ff14c","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:47:00.627206392Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"":"downstairs","":"downstairs","session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::LiveRepairExtent,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/downstairs.rs"),
            text: "RE:{} repair extent with ids {},{},{},{} deps:{}",
        },
        pattern: concat!(
            r"^RE:(?<extent>\d+) repair extent with ids ",
            r"(?<close_id>\d+),(?<repair_id>\d+),",
            r"(?<noop_id>\d+),(?<reopen_id>\d+) deps:",
        ),
        sample: r##"{"msg":"RE:0 repair extent with ids 166204,166205,166206,166207 deps:[JobId(161976), ... JobId(164935)] (5 total)","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:47:00.627185091Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"":"downstairs","":"downstairs","session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::LiveRepairNotNeeded,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/downstairs.rs"),
            text: "No repair needed for extent {}",
        },
        pattern: r"^No repair needed for extent (?<extent>\d+)$",
        sample: r##"{"msg":"No repair needed for extent 0","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:47:01.939610684Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"":"downstairs","":"downstairs","session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::LiveRepairCopy,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/downstairs.rs"),
            text: "Repair for extent {} s:{} d:{:?}",
        },
        pattern: r"^Repair for extent (?<extent>\d+) s:(?<source>\d+) d:\[(?<dests>[^\]]*)\]$",
        sample: r##"{"msg":"Repair for extent 1 s:2 d:[ClientId(0)]","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:47:06.058130503Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"":"downstairs","":"downstairs","session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##,
    },
    Entry {
        id: Id::LiveRepairEnd,
        input: Input::Msg,
        when_msg: None,
        source: Source {
            repo: "crucible",
            file: Some("upstairs/src/downstairs.rs"),
            text: "live-repair completed successfully, and live-repair aborted",
        },
        pattern: r"^live-repair (?<result>completed successfully|aborted)$",
        sample: r##"{"msg":"live-repair completed successfully","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:57:19.223905683Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"":"downstairs","":"downstairs","session_id":"2f33a3ea-6e8e-4f34-a569-2e8b5719ce8c","component":"crucible-f451fde2-d7bb-49c3-b912-145400ad114c"}"##,
    },
];

static REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    ENTRIES
        .iter()
        .map(|e| {
            Regex::new(e.pattern).unwrap_or_else(|err| {
                panic!("bad pattern for {:?}: {err}", e.id)
            })
        })
        .collect()
});

fn index(id: Id) -> usize {
    ENTRIES
        .iter()
        .position(|e| e.id == id)
        .unwrap_or_else(|| panic!("no catalog entry for {id:?}"))
}

pub fn entry(id: Id) -> &'static Entry {
    &ENTRIES[index(id)]
}

pub fn regex(id: Id) -> &'static Regex {
    &REGEXES[index(id)]
}

/// Match a record against an entry with a `Msg` or `Field` input.
pub fn captures(id: Id, record: &Record) -> Option<Captures<'_>> {
    let e = entry(id);
    if let Some(msg) = e.when_msg
        && record.msg() != msg
    {
        return None;
    }
    let text = match e.input {
        Input::Msg => record.msg(),
        Input::Field(name) => record.field(name)?,
        Input::Fragment | Input::RawLine => {
            panic!("{id:?} is not matched against a record")
        }
    };
    regex(id).captures(text)
}

/// Match text against an entry with a `Fragment` or `RawLine` input.
pub fn captures_text(id: Id, text: &str) -> Option<Captures<'_>> {
    assert!(matches!(entry(id).input, Input::Fragment | Input::RawLine));
    regex(id).captures(text)
}

/// Find every match of a `Fragment` entry in text.
pub fn captures_iter(id: Id, text: &str) -> CaptureMatches<'static, '_> {
    assert!(matches!(entry(id).input, Input::Fragment));
    regex(id).captures_iter(text)
}

#[cfg(test)]
mod test {
    use super::*;

    fn sample_captures(e: &Entry) -> Option<Captures<'static>> {
        match e.input {
            Input::Msg | Input::Field(_) => {
                // Leak the record so the captures can outlive this call.
                let record = Record::parse(e.sample).unwrap_or_else(|| {
                    panic!("sample for {:?} is not a bunyan record", e.id)
                });
                captures(e.id, Box::leak(Box::new(record)))
            }
            Input::Fragment | Input::RawLine => captures_text(e.id, e.sample),
        }
    }

    #[test]
    fn every_id_has_one_entry() {
        for e in ENTRIES {
            let count = ENTRIES.iter().filter(|o| o.id == e.id).count();
            assert_eq!(count, 1, "{:?} has {count} entries", e.id);
        }
    }

    #[test]
    fn every_pattern_matches_its_sample() {
        for e in ENTRIES {
            assert!(
                sample_captures(e).is_some(),
                "{:?} does not match its sample",
                e.id
            );
        }
    }

    fn get(id: Id, name: &str) -> String {
        let c = sample_captures(entry(id)).unwrap();
        c.name(name)
            .unwrap_or_else(|| panic!("{id:?} has no {name}"))
            .as_str()
            .to_string()
    }

    #[test]
    fn propolis_samples() {
        assert_eq!(get(Id::PropolisVersion, "version"), "v0.1.0-948");
        assert_eq!(get(Id::PropolisVersion, "commit"), "3b60af5");
        assert_eq!(
            get(Id::ZoneName, "vmm_id"),
            "5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6"
        );
        assert_eq!(
            get(Id::VmProperties, "id"),
            "617281d1-669b-4261-a215-3c32bb78a8af"
        );
        assert_eq!(get(Id::VmProperties, "name"), "newallio-11-inst");
        assert_eq!(
            get(Id::VmProperties, "sled_id"),
            "db183874-65b5-4263-a1c1-ddb2737ae0e9"
        );
        assert_eq!(get(Id::VmProperties, "sled_serial"), "BRM42220009");
        assert_eq!(get(Id::VmBoard, "cpus"), "8");
        assert_eq!(get(Id::VmBoard, "memory_mb"), "8192");
        assert_eq!(get(Id::StorageDevice, "kind"), "Nvme");
        assert_eq!(
            get(Id::StorageDevice, "backend_uuid"),
            "3c432392-26fe-4e12-94b0-a5c0ac6a25f0"
        );
        assert_eq!(get(Id::StorageDevice, "device"), "17");
        assert_eq!(
            get(Id::CrucibleComponent, "upstairs_id"),
            "3c432392-26fe-4e12-94b0-a5c0ac6a25f0"
        );
    }

    #[test]
    fn virtio_storage_device() {
        let line = r##"{"msg":"Creating storage device","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:11:50.014327278Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"spec":"Virtio(VirtioDisk { backend_id: Name(\"cloud-init-backend\"), pci_path: PciPath { bus: 0, device: 24, function: 0 } })","device_id":"cloud-init-dev"}"##;
        let record = Record::parse(line).unwrap();
        let c = captures(Id::StorageDevice, &record).unwrap();
        assert_eq!(&c["kind"], "Virtio");
        assert_eq!(&c["backend_name"], "cloud-init-backend");
        assert_eq!(&c["device"], "24");
        assert!(c.name("serial").is_none());
    }

    #[test]
    fn crucible_samples() {
        assert_eq!(
            get(Id::CrucibleVersion, "git_sha"),
            "bd62980710dd3d6d3524aa7f6617127942b75002"
        );
        assert_eq!(get(Id::RegionDefinition, "block_size"), "4096");
        assert_eq!(get(Id::RegionDefinition, "extent_size"), "16384");
        assert_eq!(get(Id::RegionDefinition, "extent_count"), "800");
        assert_eq!(get(Id::RegionDefinition, "encrypted"), "true");
        assert_eq!(
            get(Id::SessionId, "session_id"),
            "86ceec0d-72e8-499a-a4f0-40d790c60878"
        );
        assert_eq!(get(Id::UpstairsOpts, "read_only"), "false");
        assert_eq!(
            get(Id::RegionUuid, "target"),
            "[fd00:1122:3344:129::23]:19000"
        );
        assert_eq!(
            get(Id::RegionUuid, "region_id"),
            "76b3116a-10ed-429e-8df9-183a827f9727"
        );
        assert_eq!(get(Id::Panic, "thread"), "tokio-rt-vmm");
    }

    #[test]
    fn targets() {
        let targets = get(Id::UpstairsOpts, "targets");
        let found: Vec<(String, String)> = captures_iter(Id::Target, &targets)
            .map(|c| (c["ip6"].to_string(), c["port"].to_string()))
            .collect();
        assert_eq!(
            found,
            [
                ("fd00:1122:3344:129::23".into(), "19000".into()),
                ("fd00:1122:3344:108::c".into(), "19040".into()),
                ("fd00:1122:3344:104::9".into(), "19037".into()),
            ]
        );

        // Test setups use IPv4 targets.
        let c = captures_text(Id::Target, "127.0.0.1:44101").unwrap();
        assert_eq!(&c["ip4"], "127.0.0.1");
        assert_eq!(&c["port"], "44101");
    }

    #[test]
    fn panic_samples() {
        assert_eq!(
            get(Id::PanicIoError, "session_id"),
            "86ceec0d-72e8-499a-a4f0-40d790c60878"
        );
        assert_eq!(get(Id::PanicIoError, "client"), "0");
        assert_eq!(get(Id::PanicIoError, "job_id"), "852176");
        assert_eq!(get(Id::PanicIo, "op"), "Read");
        assert_eq!(get(Id::PanicIo, "start_block"), "14857");
        assert_eq!(get(Id::PanicIo, "count"), "1");
        assert_eq!(get(Id::PanicIo, "block_size"), "4096");
        assert_eq!(get(Id::PanicIo, "states"), "InProgress, InProgress, Done");
    }

    #[test]
    fn reconcile_samples() {
        assert_eq!(get(Id::ReconcileNeeded, "extent"), "0");
        let clients = get(Id::ReconcileNeeded, "clients");
        let meta: Vec<(String, String, String)> =
            captures_iter(Id::ExtentMetadata, &clients)
                .map(|c| {
                    (
                        c["generation"].to_string(),
                        c["flush"].to_string(),
                        c["dirty"].to_string(),
                    )
                })
                .collect();
        assert_eq!(meta.len(), 3);
        assert_eq!(meta[0], ("9".into(), "8132".into(), "true".into()));
        let c = captures_text(Id::ExtentMetadata, "None").unwrap();
        assert!(c.name("none").is_some());

        assert_eq!(
            get(Id::ReconcileStart, "id"),
            "c772275f-53d2-46cd-a424-79fbc67cbb9f"
        );
        assert_eq!(get(Id::ReconcileStart, "tasks"), "3200");

        // Only the ExtentRepair task in the list matches.
        let tasks = get(Id::ReconcileTaskList, "tasks");
        let repairs: Vec<(String, String, String, String)> =
            captures_iter(Id::ReconcileRepairTask, &tasks)
                .map(|c| {
                    (
                        c["id"].to_string(),
                        c["extent"].to_string(),
                        c["source"].to_string(),
                        c["address"].to_string(),
                    )
                })
                .collect();
        assert_eq!(
            repairs,
            [(
                "1226".into(),
                "0".into(),
                "0".into(),
                "[fd00:1122:3344:129::23]:23000".into()
            )]
        );
        let dests = get(Id::ReconcileRepairTask, "dests");
        let ids: Vec<String> = captures_iter(Id::ClientIds, &dests)
            .map(|c| c["client"].to_string())
            .collect();
        assert_eq!(ids, ["1", "2"]);
        assert_eq!(get(Id::ReconcileSend, "id"), "1226");
    }

    #[test]
    fn client_state_samples() {
        assert_eq!(get(Id::Transition, "from"), "Offline");
        assert_eq!(get(Id::Transition, "to"), "Active");
        let line = r##"{"msg":"Transition from Connecting { state: WaitQuorum, mode: New } to Reconcile","v":0,"name":"propolis-server","level":30,"time":"2026-10-22T22:18:36.019554358Z","hostname":"oxz_propolis-server_5a1cb26d-2b0b-4162-8ffb-3be5bcc4bab6","pid":16155,"client":"0","":"downstairs","session_id":"86ceec0d-72e8-499a-a4f0-40d790c60878","component":"crucible-3c432392-26fe-4e12-94b0-a5c0ac6a25f0"}"##;
        let record = Record::parse(line).unwrap();
        let c = captures(Id::Transition, &record).unwrap();
        assert_eq!(&c["from"], "Connecting { state: WaitQuorum, mode: New }");
        assert_eq!(&c["to"], "Reconcile");

        assert_eq!(get(Id::ClientStopped, "client"), "0");
        assert_eq!(get(Id::ClientStopped, "reason"), "Timeout");
        assert_eq!(get(Id::TooManyOutstanding, "what"), "jobs");
        assert_eq!(get(Id::TooManyOutstanding, "count"), "1001");
        assert_eq!(get(Id::FaultSkip, "client"), "0");
    }

    #[test]
    fn live_repair_samples() {
        assert_eq!(get(Id::LiveRepairReady, "client"), "0");
        assert_eq!(
            get(Id::LiveRepairStart, "id"),
            "5f314278-4c80-45b0-8298-4527541ff14c"
        );
        assert_eq!(get(Id::LiveRepairExtent, "extent"), "0");
        assert_eq!(get(Id::LiveRepairExtent, "close_id"), "166204");
        assert_eq!(get(Id::LiveRepairExtent, "reopen_id"), "166207");
        assert_eq!(get(Id::LiveRepairNotNeeded, "extent"), "0");
        assert_eq!(get(Id::LiveRepairCopy, "extent"), "1");
        assert_eq!(get(Id::LiveRepairCopy, "source"), "2");
        assert_eq!(get(Id::LiveRepairCopy, "dests"), "ClientId(0)");
        assert_eq!(get(Id::LiveRepairEnd, "result"), "completed successfully");
    }
}
