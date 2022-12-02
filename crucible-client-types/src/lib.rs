// Copyright 2022 Oxide Computer Company

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

#[allow(clippy::large_enum_variant)]
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
        opts: CrucibleOpts,
        gen: u64,
    },
    File {
        id: Uuid,
        block_size: u64,
        path: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct RegionExtentInfo {
    pub blocks_per_extent: u64,
    pub extent_count: u32,
}

#[derive(
    Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq,
)]
pub struct CrucibleOpts {
    pub id: Uuid,
    pub target: Vec<SocketAddr>,
    pub lossy: bool,
    pub flush_timeout: Option<u32>,
    pub key: Option<String>,
    pub cert_pem: Option<String>,
    pub key_pem: Option<String>,
    pub root_cert_pem: Option<String>,
    pub control: Option<SocketAddr>,
    pub read_only: bool,
    pub expected_extent_info: Option<RegionExtentInfo>,
}

impl CrucibleOpts {
    pub fn key_bytes(&self) -> Option<Vec<u8>> {
        if let Some(key) = &self.key {
            // For xts, key size must be 32 bytes
            let decoded_key =
                base64::decode(key).expect("could not base64 decode key!");

            if decoded_key.len() != 32 {
                panic!("Key length must be 32 bytes!");
            }

            Some(decoded_key)
        } else {
            None
        }
    }
}
