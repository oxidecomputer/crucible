// Copyright 2022 Oxide Computer Company

use base64::{engine, Engine};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

#[allow(clippy::large_enum_variant)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VolumeConstructionRequest {
    Volume(VcrVolume),
    Url(VcrUrl),
    Region(VcrRegion),
    File(VcrFile),
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(tag = "vcr_type", rename_all = "snake_case")]
pub struct VcrVolume {
    pub id: Uuid,
    pub block_size: u64,
    pub sub_volumes: Vec<VolumeConstructionRequest>,
    pub read_only_parent: Option<Box<VolumeConstructionRequest>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(tag = "vcr_type", rename_all = "snake_case")]
pub struct VcrUrl {
    pub id: Uuid,
    pub block_size: u64,
    pub url: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(tag = "vcr_type", rename_all = "snake_case")]
pub struct VcrRegion {
    pub block_size: u64,
    pub blocks_per_extent: u64,
    pub extent_count: u32,
    pub opts: CrucibleOpts,
    pub gen: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(tag = "vcr_type", rename_all = "snake_case")]
pub struct VcrFile {
    pub id: Uuid,
    pub block_size: u64,
    pub path: String,
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
