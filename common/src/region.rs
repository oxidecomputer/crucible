use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Copy, Clone, Debug, PartialEq)]
pub struct RegionDefinition {
    /**
     * The size of each block in bytes.  Must be a power of 2, minimum 512.
     */
    block_size: u64,

    /**
     * How many blocks should appear in each extent?
     */
    extent_size: u64,

    /**
     * How many whole extents comprise this region?
     */
    extent_count: u32,
}

impl RegionDefinition {
    pub fn from_options(opts: &RegionOptions) -> Result<Self> {
        opts.validate()?;
        Ok(RegionDefinition {
            block_size: opts.block_size,
            extent_size: opts.extent_size,
            extent_count: 0,
        })
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }
    pub fn set_block_size(&mut self, bs: u64) {
        self.block_size = bs;
    }

    pub fn extent_size(&self) -> u64 {
        self.extent_size
    }
    pub fn set_extent_size(&mut self, es: u64) {
        self.extent_size = es;
    }

    pub fn extent_count(&self) -> u32 {
        self.extent_count
    }

    pub fn set_extent_count(&mut self, ec: u32) {
        self.extent_count = ec;
    }

    pub fn total_size(&self) -> u64 {
        self.block_size * self.extent_size * (self.extent_count as u64)
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
            extent_size: 0,
            extent_count: 0,
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
    extent_size: u64,
}

impl RegionOptions {
    pub fn validate(&self) -> Result<()> {
        if !self.block_size.is_power_of_two() {
            bail!("block size must be a power of two, not {}", self.block_size);
        }

        if self.block_size < 512 {
            bail!("minimum block size is 512 bytes, not {}", self.block_size);
        }

        if self.extent_size < 1 {
            bail!("extent size must be at least 1 block");
        }

        let bs = self.extent_size.saturating_mul(self.block_size);
        if bs > 10 * 1024 * 1024 {
            /*
             * For now, make sure we don't accidentally try to use a gigantic
             * extent.
             */
            bail!(
                "extent size {} x {} bytes = {}MB, bigger than 10MB",
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

    pub fn set_extent_size(&mut self, es: u64) {
        self.extent_size = es;
    }
}

impl Default for RegionOptions {
    fn default() -> Self {
        RegionOptions {
            block_size: 512,  /* XXX bigger? */
            extent_size: 100, /* XXX bigger? */
        }
    }
}
