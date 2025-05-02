// Copyright 2021 Oxide Computer Company

use progenitor::generate_api;

generate_api!(
    spec = "../openapi/crucible-agent.json",
    derives = [schemars::JsonSchema],
);

// `omicron-common` defines some helpful retry operators for operations through
// Progenitor clients.  Re-export them here to avoid a subtle versioning issue
// it is otherwise easy to introduce: all uses of a `ProgenitorOperationRetry`
// (and its Error) must agree on a `progenitor-client` version. In Omicron all
// clients trivially agree due to the shared Cargo.toml, but Crucible's
// Progenitor may reasonably be ahead or behind Omicron.
//
// We re-export `omicron-common` types here so that Omicron (and other
// mixed-client consumers!) may have distinct `ProgenitorOperationRetry` names
// for clients who may have differing Progenitor versions.
pub mod progenitor_operation_retry {
    pub use omicron_common::progenitor_operation_retry::{
        ProgenitorOperationRetry,
        ProgenitorOperationRetryError,
    };
}
