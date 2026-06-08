// Copyright 2026 Oxide Computer Company

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum FileType {
    #[serde(rename = "data")]
    Data,
    #[serde(rename = "db")]
    Database,
    #[serde(rename = "db_shm")]
    DatabaseSharedMemory,
    #[serde(rename = "db_wal")]
    DatabaseLog,
}

#[derive(Deserialize, JsonSchema)]
pub struct FileSpec {
    pub eid: u32,
    pub file_type: FileType,
}

#[derive(Deserialize, JsonSchema)]
pub struct ExtentPath {
    pub eid: u32,
}

#[derive(Deserialize, JsonSchema)]
pub struct ExtentFilePath {
    pub eid: u32,
    pub file_type: FileType,
}

#[derive(Deserialize, JsonSchema)]
pub struct JobPath {
    pub id: String,
}

/// Per-connection memory report
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConnectionMemoryReport {
    pub pending_jobs: usize,
    pub pending_jobs_bytes: usize,
    pub pending_jobs_capacity_hwm: usize,
    pub completed_ranges: usize,
    pub completed_ranges_hwm: usize,
}

/// Summary of downstairs memory usage
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MemoryReport {
    // Region configuration for context
    pub extent_count: u32,
    pub extent_size: u64,
    pub block_size: u64,

    // Region memory
    pub region_bytes: usize,
    pub extent_meta_bytes: usize,
    pub bytes_per_extent: usize,
    pub dirty_extent_count: usize,

    // High-water marks for transient I/O buffers
    pub read_bytes_hwm: usize,
    pub write_bytes_hwm: usize,

    // Thread counts
    pub tokio_worker_threads: usize,
    pub rayon_threads: usize,

    // jemalloc allocator stats
    pub jemalloc_allocated: usize,
    pub jemalloc_active: usize,
    pub jemalloc_resident: usize,
    pub jemalloc_mapped: usize,
    pub jemalloc_retained: usize,

    // Connection memory
    pub active_connections: usize,
    pub connections: Vec<ConnectionMemoryReport>,
}
