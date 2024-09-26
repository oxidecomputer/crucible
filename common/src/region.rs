// Copyright 2021 Oxide Computer Company
use anyhow::{bail, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use uuid::Uuid;

use super::*;

/*
 * Where the unit is blocks, not bytes, make sure to reflect that in the
 * types used.
 *
 * Consumers of this API should know when to use bytes (rarely), and when to
 * use blocks.
 *
 * Blocks have a shift field to ensure that consumers and the upstairs agree
 * on what a block is. It wouldn't make sense to pass Block { 2, 9 } when the
 * downstairs expects Block { 2, 12 }.
 */
#[derive(
    Deserialize,
    Serialize,
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    JsonSchema,
    PartialOrd,
    Ord,
)]
pub struct Block {
    // Value could mean a size or offset
    pub value: u64,

    // block size as a power of 2
    // shift  9 -> 512
    // shift 12 -> 4096
    pub shift: u32,
}

/// Represents an offset (in blocks) relative to the start of an extent
#[derive(
    Deserialize, Serialize, Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct BlockOffset(pub u64);

/// Represents an absolute block index
#[derive(
    Deserialize, Serialize, Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct BlockIndex(pub u64);

pub const MIN_SHIFT: u32 = 9;
pub const MAX_SHIFT: u32 = 15;

pub const MIN_BLOCK_SIZE: usize = (1 << MIN_SHIFT) as usize;
pub const MAX_BLOCK_SIZE: usize = (1 << MAX_SHIFT) as usize;
pub const MAX_EXTENT_FILE_SIZE: u64 = (1 << 29) as u64; // 512 MiB

/**
 * Crucible Downstairs database format version.
 * This should be updated whenever changes are made to the extent metadata
 * database format.
 *
 * Read Version history
 * 1: Initial version
 *
 * Write Version history
 * 1: Initial version
 */
pub const DATABASE_READ_VERSION: usize = 1;
pub const DATABASE_WRITE_VERSION: usize = 1;

impl Block {
    pub fn new(value: u64, shift: u32) -> Block {
        // are you sure you need blocks that small?
        // are you sure you need blocks that big?
        assert!((MIN_SHIFT..=MAX_SHIFT).contains(&shift));

        Block { value, shift }
    }

    pub fn new_512(value: u64) -> Block {
        Block::new(value, 9)
    }

    pub fn new_4096(value: u64) -> Block {
        Block::new(value, 12)
    }

    pub fn block_size_in_bytes(&self) -> u32 {
        1 << self.shift
    }

    pub fn byte_value(&self) -> u64 {
        self.value * self.block_size_in_bytes() as u64
    }
}

#[derive(Deserialize, Serialize, Copy, Clone, Debug, JsonSchema, PartialEq)]
pub struct RegionDefinition {
    /**
     * The size of each block in bytes. Must be a power of 2, minimum 512.
     */
    block_size: u64,

    /**
     * How many blocks should appear in each extent?
     */
    extent_size: Block,

    /**
     * How many whole extents comprise this region?
     */
    extent_count: u32,

    /**
     * UUID for this region
     */
    uuid: Uuid,

    /**
     * region data will be encrypted
     */
    encrypted: bool,

    /**
     * The database version format for reading an extent database file.
     */
    database_read_version: usize,

    /**
     * The database version format for writing an extent database file.
     */
    database_write_version: usize,
}

impl RegionDefinition {
    pub fn from_options(opts: &RegionOptions) -> Result<Self> {
        opts.validate()?;
        Ok(RegionDefinition {
            block_size: opts.block_size,
            extent_size: opts.extent_size,
            extent_count: 0,
            uuid: opts.uuid,
            encrypted: opts.encrypted,
            database_read_version: DATABASE_READ_VERSION,
            database_write_version: DATABASE_WRITE_VERSION,
        })
    }

    // Compare two RegionDefinitions and verify they are compatible.
    // compatible is valid if all fields are the same, expect for the
    // UUID. The UUID should be different.
    pub fn compatible(
        self,
        other: RegionDefinition,
    ) -> Result<(), CrucibleError> {
        // These fields should be the same.
        if self.block_size != other.block_size {
            return Err(CrucibleError::RegionIncompatible(
                "block_size".to_string(),
            ));
        }
        if self.extent_size != other.extent_size {
            return Err(CrucibleError::RegionIncompatible(
                "extent_size".to_string(),
            ));
        }
        if self.extent_count != other.extent_count {
            return Err(CrucibleError::RegionIncompatible(
                "extent_count".to_string(),
            ));
        }
        if self.encrypted != other.encrypted {
            return Err(CrucibleError::RegionIncompatible(
                "encrypted".to_string(),
            ));
        }
        if self.database_read_version != other.database_read_version {
            return Err(CrucibleError::RegionIncompatible(
                "database_read_version".to_string(),
            ));
        }
        if self.database_write_version != other.database_write_version {
            return Err(CrucibleError::RegionIncompatible(
                "database_write_version".to_string(),
            ));
        }

        // If the UUIDs are the same, this is invalid.
        if self.uuid == other.uuid {
            return Err(CrucibleError::RegionIncompatible(
                "UUIDs are the same".to_string(),
            ));
        }

        Ok(())
    }

    pub fn database_read_version(&self) -> usize {
        self.database_read_version
    }

    pub fn database_write_version(&self) -> usize {
        self.database_write_version
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn set_block_size(&mut self, bs: u64) {
        self.block_size = bs;
    }

    pub fn extent_size(&self) -> Block {
        self.extent_size
    }

    pub fn set_extent_size(&mut self, es: Block) {
        self.extent_size = es;
    }

    pub fn extent_count(&self) -> u32 {
        self.extent_count
    }

    pub fn set_extent_count(&mut self, ec: u32) {
        self.extent_count = ec;
    }

    pub fn total_size(&self) -> u64 {
        self.block_size * self.extent_size.value * (self.extent_count as u64)
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn set_uuid(&mut self, uuid: Uuid) {
        self.uuid = uuid;
    }

    pub fn get_encrypted(&self) -> bool {
        self.encrypted
    }

    /*
     * Validate an IO would fit inside this region
     */
    pub fn validate_io(
        &self,
        offset: BlockIndex,
        length: usize,
    ) -> Result<(), CrucibleError> {
        let final_offset = offset.0 * self.block_size + length as u64;

        if final_offset > self.total_size() {
            return Err(CrucibleError::OffsetInvalid);
        }
        Ok(())
    }

    /// Converts from bytes to blocks
    ///
    /// # Panics
    /// If the byte length is not an even multiple of block size
    pub fn bytes_to_blocks(&self, bytes: usize) -> u64 {
        assert_eq!(
            bytes as u64 % self.block_size,
            0,
            "bytes must be a multiple of block size"
        );
        bytes as u64 / self.block_size
    }

    /// Checks whether the byte length is valid
    pub fn is_valid_byte_size(&self, bytelen: usize) -> bool {
        bytelen % (self.block_size as usize) == 0
    }
}

/**
 * Default for Upstairs to use before it receives the actual values
 * from the downstairs.
 */
impl Default for RegionDefinition {
    fn default() -> RegionDefinition {
        RegionDefinition {
            block_size: 0,
            extent_size: Block::new(0, 9),
            extent_count: 0,
            uuid: Uuid::nil(),
            encrypted: false,
            database_read_version: DATABASE_READ_VERSION,
            database_write_version: DATABASE_WRITE_VERSION,
        }
    }
}

impl RegionDefinition {
    pub fn test_default(
        database_read_version: usize,
        database_write_version: usize,
    ) -> RegionDefinition {
        RegionDefinition {
            block_size: 0,
            extent_size: Block::new(0, 9),
            extent_count: 0,
            uuid: Uuid::nil(),
            encrypted: false,
            database_read_version,
            database_write_version,
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct RegionOptions {
    /**
     * The size of each block in bytes.  Must be a power of 2, minimum 512.
     */
    block_size: u64,

    /**
     * How many blocks should appear in each extent?
     */
    extent_size: Block,

    /**
     * UUID for this region
     */
    uuid: Uuid,

    /**
     * region data will be encrypted
     */
    encrypted: bool,
}

impl RegionOptions {
    pub fn validate(&self) -> Result<()> {
        if !self.block_size.is_power_of_two() {
            bail!("block size must be a power of two, not {}", self.block_size);
        }

        if self.block_size < (MIN_BLOCK_SIZE as u64) {
            bail!(
                "minimum block size is {} bytes, not {}",
                MIN_BLOCK_SIZE,
                self.block_size
            );
        }

        if self.extent_size.value < 1 {
            bail!("extent size must be at least 1 block");
        }

        let es = self.extent_size.value.saturating_mul(self.block_size);
        if es > MAX_EXTENT_FILE_SIZE {
            /*
             * Limit the maximum size of an extent file.
             */
            bail!(
                "extent size {} x {} bytes = {}, bigger than {}",
                self.extent_size.value,
                self.block_size,
                es,
                MAX_EXTENT_FILE_SIZE,
            );
        }

        Ok(())
    }

    pub fn set_block_size(&mut self, bs: u64) {
        self.block_size = bs;
    }

    pub fn set_extent_size(&mut self, es: Block) {
        self.extent_size = es;
    }

    pub fn set_uuid(&mut self, uuid: Uuid) {
        self.uuid = uuid;
    }

    pub fn set_encrypted(&mut self, encrypted: bool) {
        self.encrypted = encrypted;
    }
}

impl Default for RegionOptions {
    fn default() -> Self {
        /* XXX bigger? */
        assert_eq!(MIN_BLOCK_SIZE, 512);
        RegionOptions {
            block_size: MIN_BLOCK_SIZE as u64,
            extent_size: Block::new(100, 9),
            uuid: Uuid::nil(),
            encrypted: false,
        }
    }
}

/// Append the region description file to the end of a provided path.
pub fn config_path<P: AsRef<Path>>(dir: P) -> PathBuf {
    let mut out = dir.as_ref().to_path_buf();
    out.push("region.json");
    out
}

/// Wrapper type for a particular extent
#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    Hash,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct ExtentId(pub u32);

impl std::fmt::Display for ExtentId {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        self.0.fmt(f)
    }
}

impl From<u32> for ExtentId {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl std::ops::Add<u32> for ExtentId {
    type Output = Self;
    fn add(self, rhs: u32) -> Self {
        Self(self.0 + rhs)
    }
}

impl std::ops::Add<ExtentId> for u32 {
    type Output = ExtentId;
    fn add(self, rhs: ExtentId) -> ExtentId {
        ExtentId(self + rhs.0)
    }
}

impl std::ops::AddAssign<u32> for ExtentId {
    fn add_assign(&mut self, rhs: u32) {
        self.0 += rhs;
    }
}

impl std::ops::SubAssign<u32> for ExtentId {
    fn sub_assign(&mut self, rhs: u32) {
        self.0 -= rhs;
    }
}

impl std::ops::Sub<ExtentId> for ExtentId {
    type Output = u32;
    fn sub(self, rhs: ExtentId) -> u32 {
        self.0 - rhs.0
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_basic_region() {
        /*
         * Test basic RegionDefinition methods
         */

        let mut rd = RegionDefinition::default();
        rd.set_block_size(512);
        assert_eq!(rd.block_size(), 512);

        rd.set_extent_size(Block::new(4, 9));
        assert_eq!(rd.extent_size(), Block::new(4, 9));

        rd.set_extent_count(1);
        assert_eq!(rd.extent_count(), 1);

        assert_eq!(rd.total_size(), 2048);
    }

    #[test]
    fn test_region_validate_io() {
        /*
         * Test validate io method of RegionDefinition
         * This is our region, 4 blocks:
         *   |---|---|---|---|
         * So, we test various IO sizes to verify how each pass/fail
         */

        let mut rd = RegionDefinition::default();
        rd.set_block_size(512);
        rd.set_extent_size(Block::new(4, 9));
        rd.set_extent_count(1);

        /*
         *   Region |---|---|---|---|
         *   IO     |---|
         */
        assert_eq!(rd.validate_io(BlockIndex(0), 512), Ok(()));

        /*
         *   Region |---|---|---|---|
         *   IO         |---|
         */
        assert_eq!(rd.validate_io(BlockIndex(1), 512), Ok(()));

        /*
         *   Region |---|---|---|---|
         *   IO             |---|
         */
        assert_eq!(rd.validate_io(BlockIndex(2), 512), Ok(()));

        /*
         *   Region |---|---|---|---|
         *   IO                 |---|
         */
        assert_eq!(rd.validate_io(BlockIndex(3), 512), Ok(()));

        /*
         *   Region |---|---|---|---|
         *   IO                     |---|
         */
        assert!(rd.validate_io(BlockIndex(4), 512).is_err());

        /*
         *   Region |---|---|---|---|
         *   IO     |---|---|
         */
        assert_eq!(rd.validate_io(BlockIndex(0), 1024), Ok(()));

        /*
         *   Region |---|---|---|---|
         *   IO         |---|---|
         */
        assert_eq!(rd.validate_io(BlockIndex(1), 1024), Ok(()));

        /*
         *   Region |---|---|---|---|
         *   IO             |---|---|
         */
        assert_eq!(rd.validate_io(BlockIndex(2), 1024), Ok(()));

        /*
         *   Region |---|---|---|---|
         *   IO                 |---|---|
         */
        assert!(rd.validate_io(BlockIndex(3), 1024).is_err());

        /*
         *   Region |---|---|---|---|
         *   IO                     |---|---|
         */
        assert!(rd.validate_io(BlockIndex(4), 1024).is_err());

        /*
         *   Region |---|---|---|---|
         *   IO     |---|---|---|
         */
        assert_eq!(rd.validate_io(BlockIndex(0), 1536), Ok(()));

        /*
         *   Region |---|---|---|---|
         *   IO         |---|---|---|
         */
        assert_eq!(rd.validate_io(BlockIndex(1), 1536), Ok(()));

        /*
         *   Region |---|---|---|---|
         *   IO             |---|---|---|
         */
        assert!(rd.validate_io(BlockIndex(2), 1536).is_err());

        /*
         *   Region |---|---|---|---|
         *   IO     |---|---|---|---|
         */
        assert_eq!(rd.validate_io(BlockIndex(0), 2048), Ok(()));

        /*
         *   Region |---|---|---|---|
         *   IO         |---|---|---|---|
         */
        assert!(rd.validate_io(BlockIndex(1), 2048).is_err());
    }

    fn test_rd() -> RegionDefinition {
        RegionDefinition {
            block_size: 512,
            extent_size: Block::new(10, 9),
            extent_count: 8,
            uuid: Uuid::new_v4(),
            encrypted: false,
            database_read_version: DATABASE_READ_VERSION,
            database_write_version: DATABASE_WRITE_VERSION,
        }
    }

    #[test]
    fn test_region_compare_block() {
        let mut rd1 = test_rd();
        let rd2 = test_rd();

        // Basic positive test first.
        assert_eq!(rd1.compatible(rd2), Ok(()));

        rd1.block_size = 4096;
        assert!(rd1.compatible(rd2).is_err());

        let rd1 = test_rd();
        let mut rd2 = test_rd();
        rd2.block_size = 4096;
        assert!(rd1.compatible(rd2).is_err());
    }

    #[test]
    fn test_region_compare_extent_size() {
        let mut rd1 = test_rd();
        let rd2 = test_rd();

        rd1.extent_size = Block::new(2, 9);
        assert!(rd1.compatible(rd2).is_err());

        let rd1 = test_rd();
        let mut rd2 = test_rd();
        rd2.extent_size = Block::new(2, 9);
        assert!(rd1.compatible(rd2).is_err());
    }

    #[test]
    fn test_region_compare_extent_count() {
        let mut rd1 = test_rd();
        let rd2 = test_rd();

        rd1.extent_count = 9;
        assert!(rd1.compatible(rd2).is_err());

        let rd1 = test_rd();
        let mut rd2 = test_rd();
        rd2.extent_count = 9;
        assert!(rd1.compatible(rd2).is_err());
    }

    #[test]
    fn test_region_compare_uuid() {
        // Verify region compare, UUIDs must be different
        let mut rd1 = test_rd();
        let rd2 = test_rd();

        rd1.uuid = rd2.uuid;
        assert!(rd1.compatible(rd2).is_err());
    }

    #[test]
    fn test_region_compare_encrypted() {
        let mut rd1 = test_rd();
        let rd2 = test_rd();

        rd1.encrypted = true;
        assert!(rd1.compatible(rd2).is_err());

        let rd1 = test_rd();
        let mut rd2 = test_rd();
        rd2.encrypted = true;
        assert!(rd1.compatible(rd2).is_err());
    }

    #[test]
    fn test_region_compare_db_read_version() {
        let mut rd1 = test_rd();
        let rd2 = test_rd();

        rd1.database_read_version = DATABASE_READ_VERSION + 1;
        assert!(rd1.compatible(rd2).is_err());

        let rd1 = test_rd();
        let mut rd2 = test_rd();
        rd2.database_read_version = DATABASE_READ_VERSION + 1;
        assert!(rd1.compatible(rd2).is_err());
    }

    #[test]
    fn test_region_compare_db_write_version() {
        let mut rd1 = test_rd();
        let rd2 = test_rd();

        rd1.database_write_version = DATABASE_WRITE_VERSION + 1;
        assert!(rd1.compatible(rd2).is_err());

        let rd1 = test_rd();
        let mut rd2 = test_rd();
        rd2.database_write_version = DATABASE_WRITE_VERSION + 1;
        assert!(rd1.compatible(rd2).is_err());
    }
}
