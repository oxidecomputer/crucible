// Copyright 2026 Oxide Computer Company

//! Summarize raw propolis-server logs.
//!
//! Every log message crulog depends on is listed in [`catalog`].  The
//! rest of the crate only looks at log text through that catalog.
pub mod catalog;
pub mod history;
pub mod record;
pub mod render;
pub mod summary;

pub use summary::{Summary, summarize};
