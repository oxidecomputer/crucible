// Copyright 2022 Oxide Computer Company

use base64::{Engine, engine};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Debug;
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
        #[serde(rename = "gen")]
        generation: u64,
    },
    File {
        id: Uuid,
        block_size: u64,
        path: String,
    },
}

impl VolumeConstructionRequest {
    pub fn targets(&self) -> Vec<SocketAddr> {
        match self {
            VolumeConstructionRequest::Volume { sub_volumes, .. } => {
                sub_volumes.iter().flat_map(|s| s.targets()).collect()
            }
            VolumeConstructionRequest::Region { opts, .. } => {
                opts.target.clone()
            }
            _ => vec![],
        }
    }
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

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReplaceResult {
    Started,
    StartedAlready,
    CompletedAlready,
    Missing,
    VcrMatches,
}

impl Debug for ReplaceResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplaceResult::Started => {
                write!(f, "Started")
            }
            ReplaceResult::StartedAlready => {
                write!(f, "StartedAlready")
            }
            ReplaceResult::CompletedAlready => {
                write!(f, "CompletedAlready")
            }
            ReplaceResult::Missing => {
                write!(f, "Missing")
            }
            ReplaceResult::VcrMatches => {
                write!(f, "VcrMatches")
            }
        }
    }
}

/// Result of comparing an original volume construction request to a candidate
/// replacement one.
pub enum ReplacementRequestCheck {
    /// The replacement was validated, and this variant holds the old downstairs
    /// target and the new one replacing it.
    Valid { old: SocketAddr, new: SocketAddr },

    /// The replacement is not necessary because the replacement matches the
    /// original.
    ReplacementMatchesOriginal,
}

/// Extent information about a region.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub struct RegionExtentInfo {
    /// Block size in bytes.
    pub block_size: u64,
    /// Number of blocks in a single extent.
    pub blocks_per_extent: u64,
    /// Total number of extents that make up this region.
    pub extent_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_gen_field_serialization_consistency() {
        // Test that the Rust field named "generation" serializes to JSON as "gen"
        // and deserializes from JSON "gen" back to Rust field "generation"
        use std::net::SocketAddr;

        // Create a Region with generation field
        let original_vcr = VolumeConstructionRequest::Region {
            block_size: 512,
            blocks_per_extent: 100,
            extent_count: 10,
            opts: CrucibleOpts {
                id: "12345678-1234-1234-1234-123456789abc".parse().unwrap(),
                target: vec!["127.0.0.1:3810".parse::<SocketAddr>().unwrap()],
                lossy: false,
                flush_timeout: None,
                key: None,
                cert_pem: None,
                key_pem: None,
                root_cert_pem: None,
                control: None,
                read_only: false,
            },
            generation: 42,
        };

        // Serialize to JSON
        let json = serde_json::to_string(&original_vcr).unwrap();

        // Verify JSON contains "gen" not "generation"
        assert!(
            json.contains("\"gen\""),
            "JSON should contain 'gen' field, got: {}",
            json
        );
        assert!(
            !json.contains("\"generation\""),
            "JSON should not contain 'generation' field, got: {}",
            json
        );

        // Deserialize back
        let deserialized_vcr: VolumeConstructionRequest =
            serde_json::from_str(&json).expect("Failed to deserialize");

        // Verify the Rust field is accessible as "generation"
        if let VolumeConstructionRequest::Region { generation, .. } =
            deserialized_vcr
        {
            assert_eq!(generation, 42, "generation field should be 42");
        } else {
            panic!("Expected Region variant");
        }

        // Also test that manually created JSON with "gen" works
        let manual_json = r#"{
            "type": "region",
            "block_size": 512,
            "blocks_per_extent": 100,
            "extent_count": 10,
            "opts": {
                "id": "12345678-1234-1234-1234-123456789abc",
                "target": ["127.0.0.1:3810"],
                "lossy": false,
                "flush_timeout": null,
                "key": null,
                "cert_pem": null,
                "key_pem": null,
                "root_cert_pem": null,
                "control": null,
                "read_only": false
            },
            "gen": 99
        }"#;

        let manual_vcr: VolumeConstructionRequest =
            serde_json::from_str(manual_json)
                .expect("Failed to deserialize manual JSON");

        if let VolumeConstructionRequest::Region { generation, .. } = manual_vcr
        {
            assert_eq!(generation, 99, "generation field should be 99");
        } else {
            panic!("Expected Region variant");
        }
    }
}
