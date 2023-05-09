// Copyright 2021 Oxide Computer Company

use std::path::Path;

use chrono::prelude::*;
use crucible_smf::scf_type_t::{self, *};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Requested,
    Created,
    Tombstoned,
    Destroyed,
    Failed,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
pub struct Region {
    pub id: RegionId,
    pub state: State,

    // Creation parameters
    pub block_size: u64,
    pub extent_size: u64,
    pub extent_count: u64,
    pub encrypted: bool,

    // Run-time parameters
    pub port_number: u16,
    pub cert_pem: Option<String>,

    // TODO should skip serializing this on list regions response, but this
    // causes crucible.json to not have it
    // #[serde(skip_serializing)]
    pub key_pem: Option<String>,

    pub root_pem: Option<String>,
}

pub struct SmfProperty<'a> {
    pub name: &'a str,
    pub typ: scf_type_t,
    pub val: String,
}

impl Region {
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
        ];

        if self.cert_pem.is_some() {
            let mut path = dir.to_path_buf();
            path.push("cert.pem");
            let path = path.into_os_string().into_string().unwrap();

            results.push(SmfProperty {
                name: "cert_pem_path",
                typ: SCF_TYPE_ASTRING,
                val: path,
            });
        }

        if self.key_pem.is_some() {
            let mut path = dir.to_path_buf();
            path.push("key.pem");
            let path = path.into_os_string().into_string().unwrap();

            results.push(SmfProperty {
                name: "key_pem_path",
                typ: SCF_TYPE_ASTRING,
                val: path,
            });
        }

        if self.root_pem.is_some() {
            let mut path = dir.to_path_buf();
            path.push("root.pem");
            let path = path.into_os_string().into_string().unwrap();

            results.push(SmfProperty {
                name: "root_pem_path",
                typ: SCF_TYPE_ASTRING,
                val: path,
            });
        }

        results
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
pub struct CreateRegion {
    pub id: RegionId,

    pub block_size: u64,
    pub extent_size: u64,
    pub extent_count: u64,
    pub encrypted: bool,

    pub cert_pem: Option<String>,
    pub key_pem: Option<String>,
    pub root_pem: Option<String>,
    // TODO base64 encoded der too?
}

impl CreateRegion {
    pub fn mismatch(&self, r: &Region) -> Option<String> {
        if self.block_size != r.block_size {
            Some(format!(
                "block size {} instead of requested {}",
                self.block_size, r.block_size
            ))
        } else if self.extent_size != r.extent_size {
            Some(format!(
                "extent size {} instead of requested {}",
                self.extent_size, r.extent_size
            ))
        } else if self.extent_count != r.extent_count {
            Some(format!(
                "extent count {} instead of requested {}",
                self.extent_count, r.extent_count
            ))
        } else if self.encrypted != r.encrypted {
            Some(format!(
                "encrypted {} instead of requested {}",
                self.encrypted, r.encrypted
            ))
        } else if self.cert_pem != r.cert_pem {
            Some(format!(
                "cert_pem {:?} instead of requested {:?}",
                self.cert_pem, r.cert_pem
            ))
        } else if self.key_pem != r.key_pem {
            Some(format!(
                "key_pem {:?} instead of requested {:?}",
                self.key_pem, r.key_pem
            ))
        } else if self.root_pem != r.root_pem {
            Some(format!(
                "root_pem {:?} instead of requested {:?}",
                self.root_pem, r.root_pem
            ))
        } else {
            None
        }
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(
    Serialize,
    Deserialize,
    JsonSchema,
    Debug,
    PartialEq,
    Eq,
    Clone,
    PartialOrd,
    Ord,
)]
pub struct RegionId(pub String);

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

// The different types of resources the worker thread monitors for changes. This
// wraps the object that has been added, or changed somehow.
pub enum Resource {
    Region(Region),
    RunningSnapshot(RegionId, String, RunningSnapshot),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() {
        let r = Region {
            id: RegionId("abc".to_string()),
            port_number: 1701,
            state: State::Requested,
            block_size: 4096,
            extent_size: 4096,
            extent_count: 100,
            encrypted: false,
            cert_pem: None,
            key_pem: None,
            root_pem: None,
        };

        let s = serde_json::to_string(&r).expect("serialise");
        println!("{}", s);

        let recons: Region = serde_json::from_str(&s).expect("deserialise");

        assert_eq!(r, recons);
    }
}
