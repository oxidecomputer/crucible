// Copyright 2026 Oxide Computer Company

use schemars::JsonSchema;
use serde::Deserialize;

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
