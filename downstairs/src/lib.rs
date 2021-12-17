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
