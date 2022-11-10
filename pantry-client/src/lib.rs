// Copyright 2022 Oxide Computer Company

use progenitor::generate_api;

generate_api!(
    spec = "../openapi/crucible-pantry.json",
    derives = [schemars::JsonSchema],
);
