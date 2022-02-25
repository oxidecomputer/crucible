// Copyright 2022 Oxide Computer Company
use anyhow::Result;

/**
 * Dump the crucible control api
 */
fn main() -> Result<(), String> {
    let _ = crucible::control::build_api(true)?;
    Ok(())
}
