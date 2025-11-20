// Copyright 2021 Oxide Computer Company

use progenitor::generate_api;

generate_api!(
    spec = "../openapi/crucible-agent/crucible-agent-latest.json",
    derives = [schemars::JsonSchema],
);
