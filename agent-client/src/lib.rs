// Copyright 2021 Oxide Computer Company

use progenitor::generate_api;

generate_api!(
    spec = "../openapi/crucible-agent.json",
    derives = [schemars::JsonSchema],
);
