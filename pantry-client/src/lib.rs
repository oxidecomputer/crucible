// Copyright 2022 Oxide Computer Company

use progenitor::generate_api;

generate_api!(
    spec = "../openapi/crucible-pantry/crucible-pantry-latest.json",
    derives = [schemars::JsonSchema],
    patch = {
        DownstairsInfoStatus = { derives = [PartialEq, Eq] },
        UpstairsInfoStatus = { derives = [PartialEq, Eq] },
    },
);
