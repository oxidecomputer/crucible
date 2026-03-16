// Copyright 2026 Oxide Computer Company

pub use crucible_agent_types_versions::latest::snapshot::*;

use std::path::Path;

use crucible_smf::scf_type_t::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::region::RegionId;
use crate::smf::SmfProperty;

pub struct CreateRunningSnapshotRequest {
    pub id: RegionId,
    pub name: String,
    pub cert_pem: Option<String>,
    pub key_pem: Option<String>,
    pub root_pem: Option<String>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
pub struct DeleteRunningSnapshotRequest {
    pub id: RegionId,
    pub name: String,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
pub struct DeleteSnapshotRequest {
    pub id: RegionId,
    pub name: String,
}

/// Given a root directory, return a list of SMF properties to ensure for the
/// corresponding running snapshot instance.
pub fn running_snapshot_smf_properties<'a>(
    snapshot: &'a RunningSnapshot,
    dir: &Path,
) -> Vec<SmfProperty<'a>> {
    let mut results = vec![
        SmfProperty {
            name: "directory",
            typ: SCF_TYPE_ASTRING,
            val: dir.to_str().unwrap().to_string(),
        },
        SmfProperty {
            name: "port",
            typ: SCF_TYPE_COUNT,
            val: snapshot.port_number.to_string(),
        },
        SmfProperty {
            name: "mode",
            typ: SCF_TYPE_ASTRING,
            val: "ro".to_string(),
        },
    ];

    // Test for X509 files in snapshot - note this means that running
    // snapshots will use the X509 information in the snapshot, not a new
    // set.
    {
        let mut path = dir.to_path_buf();
        path.push("cert.pem");
        let path = path.into_os_string().into_string().unwrap();

        if Path::new(&path).exists() {
            results.push(SmfProperty {
                name: "cert_pem_path",
                typ: SCF_TYPE_ASTRING,
                val: path,
            });
        }
    }

    {
        let mut path = dir.to_path_buf();
        path.push("key.pem");
        let path = path.into_os_string().into_string().unwrap();

        if Path::new(&path).exists() {
            results.push(SmfProperty {
                name: "key_pem_path",
                typ: SCF_TYPE_ASTRING,
                val: path,
            });
        }
    }

    {
        let mut path = dir.to_path_buf();
        path.push("root.pem");
        let path = path.into_os_string().into_string().unwrap();

        if Path::new(&path).exists() {
            results.push(SmfProperty {
                name: "root_pem_path",
                typ: SCF_TYPE_ASTRING,
                val: path,
            });
        }
    }

    results
}
