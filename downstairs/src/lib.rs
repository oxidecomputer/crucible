// Copyright 2021 Oxide Computer Company
use anyhow::{bail, Result};
use std::collections::HashMap;
use std::path::PathBuf;

use crucible_common::Block;
use crucible_protocol::*;

mod dump;
mod region;
pub use region::Region;
