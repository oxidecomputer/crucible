// Copyright 2025 Oxide Computer Company

use crate::datafile::Region;
use crate::datafile::RunningSnapshot;
use crucible_agent_types::region::RegionId;

// The different types of resources the worker thread monitors for changes. This
// wraps the object that has been added, or changed somehow.
pub enum Resource {
    Region(Region),
    RunningSnapshot(RegionId, String, RunningSnapshot),
}
