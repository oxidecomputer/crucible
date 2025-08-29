// Copyright 2025 Oxide Computer Company

use std::path::Path;

use chrono::{DateTime, Utc};
use crucible_smf::scf_type_t::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    region::{RegionId, State},
    smf::SmfProperty,
};

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
pub struct Snapshot {
    pub name: String,
    pub created: DateTime<Utc>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
pub struct RunningSnapshot {
    pub id: RegionId,
    pub name: String,
    pub port_number: u16,
    pub state: State,
}

impl RunningSnapshot {
    /**
     * Given a root directory, return a list of SMF properties to ensure for
     * the corresponding running instance.
     */
    pub fn get_smf_properties(&self, dir: &Path) -> Vec<SmfProperty> {
        let mut results = vec![
            SmfProperty {
                name: "directory",
                typ: SCF_TYPE_ASTRING,
                val: dir.to_str().unwrap().to_string(),
            },
            SmfProperty {
                name: "port",
                typ: SCF_TYPE_COUNT,
                val: self.port_number.to_string(),
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
}

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
