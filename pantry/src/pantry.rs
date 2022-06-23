use slog::Logger;
use anyhow::Result;

pub struct Pantry {
    log: Logger,
}

impl Pantry {
    pub fn new(log: Logger) -> Result<Pantry> {
        Ok(Pantry { log })
    }

    pub async fn background_task(&self) -> Result<()> {
        Ok(())
    }
}
