// Copyright 2022 Oxide Computer Company

use progenitor::generate_api;

generate_api!(
    spec = "../openapi/crucible-control.json",
    derives = [schemars::JsonSchema],
);
