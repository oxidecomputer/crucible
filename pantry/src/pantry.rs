use anyhow::Result;
use slog::Logger;

pub struct Pantry {
    log: Logger,
}

impl Pantry {
    pub fn new(log: Logger) -> Result<Pantry> {
        Ok(Pantry { log })
    }
}
