// Copyright 2022 Oxide Computer Company
use anyhow::Result;

/**
 * Dump the crucible admin api
 */
fn main() -> Result<(), String> {
    let _ = crucible::admin::build_api(true)?;
    Ok(())
}
