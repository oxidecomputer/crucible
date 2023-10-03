// Copyright 2022 Oxide Computer Company

use base64::{engine, Engine};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use uuid::Uuid;

#[allow(clippy::large_enum_variant)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VolumeConstructionRequest {
    Volume {
        id: Uuid,
        block_size: u64,
        sub_volumes: Vec<VolumeConstructionRequest>,
        read_only_parent: Option<Box<VolumeConstructionRequest>>,
    },
    Url {
        id: Uuid,
        block_size: u64,
        url: String,
    },
    Region {
        block_size: u64,
        blocks_per_extent: u64,
        extent_count: u32,
        opts: CrucibleOpts,
        gen: u64,
    },
    File {
        id: Uuid,
        block_size: u64,
        path: String,
    },
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(
    Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq,
)]
pub struct CrucibleOpts {
    pub id: Uuid,
    pub target: Vec<SocketAddr>,
    pub lossy: bool,
    pub flush_timeout: Option<f32>,
    pub key: Option<String>,
    pub cert_pem: Option<String>,
    pub key_pem: Option<String>,
    pub root_cert_pem: Option<String>,
    pub control: Option<SocketAddr>,
    pub read_only: bool,
}

impl CrucibleOpts {
    pub fn key_bytes(&self) -> Option<Vec<u8>> {
        if let Some(key) = &self.key {
            // For xts, key size must be 32 bytes
            let decoded_key = engine::general_purpose::STANDARD
                .decode(key)
                .expect("could not base64 decode key!");

            if decoded_key.len() != 32 {
                panic!("Key length must be 32 bytes!");
            }

            Some(decoded_key)
        } else {
            None
        }
    }
}

/// Display the contents of CrucibleOpts, Only print if keys are populated,
/// not what the actual contents are.
impl std::fmt::Display for CrucibleOpts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Upstairs UUID: {},", self.id)?;
        write!(f, " Targets: {:?},", self.target)?;
        write!(f, " lossy: {:?},", self.lossy)?;
        write!(f, " flush_timeout: {:?},", self.flush_timeout)?;
        write!(f, " key populated: {}, ", self.key.is_some())?;
        write!(f, " cert_pem populated: {}, ", self.cert_pem.is_some())?;
        write!(f, " key_pem populated: {}, ", self.key_pem.is_some())?;
        write!(
            f,
            " root_cert_pem populated: {}, ",
            self.root_cert_pem.is_some()
        )?;
        write!(f, " Control: {:?}, ", self.control)?;
        write!(f, " read_only: {:?}", self.read_only)?;
        Ok(())
    }
}
