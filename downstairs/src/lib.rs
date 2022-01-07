// Copyright 2021 Oxide Computer Company

/*
 * Note: This lib.rs was created to facilitate criterion benchmarks, which
 * cannot benchmark binary crates. Including a minimal number of uses here
 * allows us to write those microbenchmarks.
 */

use anyhow::{bail, Result};
use std::collections::HashMap;
use std::path::PathBuf;

use crucible_common::Block;
use crucible_protocol::*;

mod dump;
mod region;
pub use region::Region;

/*
 * This function exists only to prevent the compiler warnings that are
 * otherwise produced when we include the dump.rs file.  It's a big
 * cascade of dependencies that require it and having this exposed here
 * will satisfy the compiler.
 */
pub fn lib_dump_region(
    region_dir: Vec<PathBuf>,
    cmp_extent: Option<u32>,
    block: Option<u64>,
    only_show_differences: bool,
) -> Result<()> {
    dump::dump_region(region_dir, cmp_extent, block, only_show_differences)
}
