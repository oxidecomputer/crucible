// Copyright 2022 Oxide Computer Company

use progenitor::generate_api;

generate_api!(
    spec = "../openapi/downstairs-repair.json",
    derives = [schemars::JsonSchema],
    replace = {
        RegionDefinition = crucible_common::RegionDefinition,
    }
);
