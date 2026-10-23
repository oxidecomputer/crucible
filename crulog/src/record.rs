// Copyright 2026 Oxide Computer Company

//! A single bunyan JSON record from a propolis-server log.
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

#[derive(Debug)]
pub struct Record {
    pub time: DateTime<Utc>,
    pub pid: Option<u64>,
    fields: Map<String, Value>,
}

impl Record {
    /// Parse one log line, or return None if it is not a bunyan record.
    ///
    /// SMF lines, the start method's `set -x` output, and panic messages
    /// all end up in the same log file, and are not records.
    pub fn parse(line: &str) -> Option<Record> {
        if !line.starts_with('{') {
            return None;
        }
        // Slog can emit the same key more than once (the "" key shows up
        // that way), so parse into a map and let the last one win.
        let Value::Object(fields) = serde_json::from_str(line).ok()? else {
            return None;
        };
        let time = fields.get("time")?.as_str()?.parse().ok()?;
        let pid = fields.get("pid").and_then(Value::as_u64);
        Some(Record { time, pid, fields })
    }

    pub fn msg(&self) -> &str {
        self.field("msg").unwrap_or("")
    }

    /// Return a string field, or None if it is missing or not a string.
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields.get(name)?.as_str()
    }
}
