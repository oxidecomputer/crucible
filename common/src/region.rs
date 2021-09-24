// Copyright 2021 Oxide Computer Company
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/*
 * Where the unit is blocks, not bytes, make sure to reflect that in the types used.
 *
 * Consumers of this API should know when to use bytes (rarely), and when to use blocks.
 *
 * Blocks have a shift field to ensure that consumers and the upstairs agree on what a block is. It
 * wouldn't make sense to pass Block { 2, 9 } when the downstairs expects Block { 2, 12 }.
 */
#[derive(Deserialize, Serialize, Copy, Clone, Debug, PartialEq, PartialOrd)]
pub struct Block {
    // Value could mean a size or offset
    pub value: u64,

    // block size as a power of 2
    // shift  9 -> 512
    // shift 12 -> 4096
    pub shift: u32,
}

impl Block {
    pub fn new(value: u64, shift: u32) -> Block {
        // are you sure you need blocks that small?
        // are you sure you need blocks that big?
        assert!((9..16).contains(&shift));

        Block { value, shift }
    }

    pub fn new_512(value: u64) -> Block {
        Block::new(value, 9)
    }

    pub fn new_with_ddef(value: u64, ddef: &RegionDefinition) -> Block {
        Block {
            value,
            shift: ddef.block_size().trailing_zeros(),
        }
    }

    pub fn block_size_in_bytes(&self) -> u32 {
        1 << self.shift
    }

    pub fn byte_value(&self) -> u64 {
        self.value * self.block_size_in_bytes() as u64
    }
}

#[derive(Deserialize, Serialize, Copy, Clone, Debug, PartialEq)]
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
}

impl RegionDefinition {
    pub fn from_options(opts: &RegionOptions) -> Result<Self> {
        opts.validate()?;
        Ok(RegionDefinition {
            block_size: opts.block_size,
            extent_size: opts.extent_size,
            extent_count: 0,
            uuid: opts.uuid,
        })
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
}

/**
 * Default for Upstairs to use before it receives the actual values
 * from the downstairs.  XXX I think I can better do this with an Option.
 */
impl Default for RegionDefinition {
    fn default() -> RegionDefinition {
        RegionDefinition {
            block_size: 0,
            extent_size: Block::new(0, 9),
            extent_count: 0,
            uuid: Uuid::nil(),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
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
}

impl RegionOptions {
    pub fn validate(&self) -> Result<()> {
        if !self.block_size.is_power_of_two() {
            bail!("block size must be a power of two, not {}", self.block_size);
        }

        if self.block_size < 512 {
            bail!("minimum block size is 512 bytes, not {}", self.block_size);
        }

        if self.extent_size.value < 1 {
            bail!("extent size must be at least 1 block");
        }

        let bs = self.extent_size.value.saturating_mul(self.block_size);
        if bs > 10 * 1024 * 1024 {
            /*
             * For now, make sure we don't accidentally try to use a gigantic
             * extent.
             */
            bail!(
                "extent size {:?} x {} bytes = {}MB, bigger than 10MB",
                self.extent_size,
                self.block_size,
                bs / 1024 / 1024
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
}

impl Default for RegionOptions {
    fn default() -> Self {
        /* XXX bigger? */
        RegionOptions {
            block_size: 512,
            extent_size: Block::new(100, 9),
            uuid: Uuid::nil(),
        }
    }
}
