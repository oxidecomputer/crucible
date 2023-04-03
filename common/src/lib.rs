// Copyright 2021 Oxide Computer Company
use std::fs::File;
use std::hash::Hasher;
use std::io::{ErrorKind, Read, Write};
use std::path::Path;

use ErrorKind::NotFound;

use anyhow::{anyhow, bail, Context, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

mod region;
pub use region::{
    Block, RegionDefinition, RegionOptions, MAX_BLOCK_SIZE, MAX_SHIFT,
    MIN_BLOCK_SIZE, MIN_SHIFT,
};

pub mod x509;

pub const REPAIR_PORT_OFFSET: u16 = 4000;

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(
    thiserror::Error,
    Debug,
    PartialEq,
    Clone,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub enum CrucibleError {
    #[error("Error: {0}")]
    GenericError(String),

    #[error("IO Error: {0}")]
    IoError(String),

    #[error("Unsupported: {0}")]
    Unsupported(String),

    #[error("data store disconnected")]
    Disconnect,

    #[error("Error grabbing data lock")]
    DataLockError,

    #[error("Error grabbing reader-writer {0} lock")]
    RwLockError(String),

    #[error("BlockReqWaiter recv channel disconnected")]
    RecvDisconnected,

    #[error("SendError: {0}")]
    SendError(String),

    #[error("Offset not block aligned")]
    OffsetUnaligned,

    #[error("Data length not block size multiple")]
    DataLenUnaligned,

    #[error("Block size mismatch")]
    BlockSizeMismatch,

    #[error("Invalid number of blocks: {0}")]
    InvalidNumberOfBlocks(String),

    #[error("Offset past end of extent")]
    OffsetInvalid,

    #[error("Upstairs is already active")]
    UpstairsAlreadyActive,

    #[error("Upstairs is deactivating")]
    UpstairsDeactivating,

    #[error("Upstairs is not active")]
    UpstairsInactive,

    #[error("Saw a UUID that wasn't ours!")]
    UuidMismatch,

    #[error("Encryption failed! {0}")]
    EncryptionError(String),

    #[error("Decryption failed!")]
    DecryptionError,

    #[error("Integrity hash mismatch!")]
    HashMismatch,

    #[error("LBA range overlap!")]
    LBARangeOverlap,

    #[error("Subvolume size mismatch!")]
    SubvolumeSizeMismatch,

    #[error("Cannot serve blocks: {0}")]
    CannotServeBlocks(String),

    #[error("Cannot receive blocks: {0}")]
    CannotReceiveBlocks(String),

    #[error("Snapshot failed! {0}")]
    SnapshotFailed(String),

    #[error("Snapshot {0} exists already")]
    SnapshotExistsAlready(String),

    #[error("Attempting to modify read-only region!")]
    ModifyingReadOnlyRegion,

    #[error("Invalid extent")]
    InvalidExtent,

    #[error("Repair request error {0}")]
    RepairRequestError(String),

    #[error("Invalid repair file list {0}")]
    RepairFilesInvalid(String),

    #[error("Repair stream error {0}")]
    RepairStreamError(String),

    #[error("Generation number is too low: {0}")]
    GenerationNumberTooLow(String),

    #[error("No longer active")]
    NoLongerActive,

    #[error("Failed reconciliation")]
    RegionAssembleError,

    #[error("Property not available: {0}")]
    PropertyNotAvailable(String),
}

impl From<std::io::Error> for CrucibleError {
    fn from(e: std::io::Error) -> Self {
        CrucibleError::IoError(format!("{:?}", e))
    }
}

// Clippy complains but code won't compile without the Into!
#[allow(clippy::from_over_into)]
impl Into<std::io::Error> for CrucibleError {
    fn into(self) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, self)
    }
}

impl From<anyhow::Error> for CrucibleError {
    fn from(e: anyhow::Error) -> Self {
        CrucibleError::GenericError(format!("{:?}", e))
    }
}

impl From<rusqlite::Error> for CrucibleError {
    fn from(e: rusqlite::Error) -> Self {
        CrucibleError::GenericError(format!("{:?}", e))
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for CrucibleError {
    fn from(e: std::sync::mpsc::SendError<T>) -> Self {
        CrucibleError::GenericError(format!("{:?}", e))
    }
}

#[macro_export]
macro_rules! crucible_bail {
    ($i:ident) => { return Err(CrucibleError::$i) };
    ($i:ident, $str:expr) => {
        return Err(CrucibleError::$i($str.to_string()))
    };
    ($i:ident, $fmt:expr, $($arg:tt)*) => {
        return Err(CrucibleError::$i(format!($fmt, $($arg)*)))
    };
}

pub fn read_json_maybe<P, T>(file: P) -> Result<Option<T>>
where
    P: AsRef<Path>,
    for<'de> T: Deserialize<'de>,
{
    let file = file.as_ref();
    let mut f = match File::open(file) {
        Ok(f) => f,
        Err(e) if e.kind() == NotFound => return Ok(None),
        Err(e) => bail!("open {:?}: {:?}", file, e),
    };
    let mut buf = Vec::<u8>::new();
    f.read_to_end(&mut buf)
        .with_context(|| anyhow!("read {:?}", file))?;
    serde_json::from_slice(buf.as_slice())
        .with_context(|| anyhow!("parse {:?}", file))
}

pub fn read_json<P, T>(file: P) -> Result<T>
where
    P: AsRef<Path>,
    for<'de> T: Deserialize<'de>,
{
    let file = file.as_ref();
    read_json_maybe(file)?
        .ok_or_else(|| anyhow!("open {:?}: file not found", file))
}

pub fn write_json<P, T>(file: P, data: &T, clobber: bool) -> Result<()>
where
    P: AsRef<Path>,
    T: Serialize,
{
    let file = file.as_ref();
    let mut buf = serde_json::to_vec_pretty(data)?;
    buf.push(b'\n');
    let mut tmpf = NamedTempFile::new_in(file.parent().unwrap())?;
    tmpf.write_all(&buf)?;
    tmpf.flush()?;

    if clobber {
        tmpf.persist(file)?;
    } else {
        tmpf.persist_noclobber(file)?;
    }
    Ok(())
}

pub fn mkdir_for_file(file: &Path) -> Result<()> {
    Ok(std::fs::create_dir_all(file.parent().expect("file path"))?)
}

pub fn integrity_hash(args: &[&[u8]]) -> u64 {
    let mut hasher: twox_hash::XxHash64 = Default::default();
    for arg in args {
        hasher.write(arg);
    }
    hasher.finish()
}
