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
}
