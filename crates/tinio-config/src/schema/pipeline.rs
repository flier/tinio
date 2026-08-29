use garde::Validate;
use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use tinio_core::pipeline::{
    CAPACITY_MAX, CAPACITY_MIN, DB_WORKERS_MAX, DB_WORKERS_MIN, DEFAULT_CAPACITY,
    DEFAULT_DB_WORKERS, DEFAULT_IO_WORKERS, IO_WORKERS_MAX, IO_WORKERS_MIN,
};

/// Worker-thread priority of a pipeline (`priority` in `[pipeline.*]`;
/// pipeline-spec.md §3.4, Q7).
///
/// # Examples
///
/// ```rust
/// use std::str::FromStr;
/// use tinio_config::pipeline::Priority;
///
/// assert_eq!(Priority::from_str("low").unwrap(), Priority::Low);
/// assert_eq!(Priority::default().to_string(), "normal");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, Display, FromStr)]
#[serde(rename_all = "lowercase")]
#[display(style = "lowercase")]
pub enum Priority {
    /// Do not set a thread priority — the pipeline runs at the OS default.
    #[default]
    Normal,
    /// The lowest legal thread priority (background work).
    Low,
    /// The highest legal thread priority (opt-in).
    High,
}

/// The task-pipeline sections (`[pipeline]`; presence-gated — an absent
/// section resolves to the defaults, pipeline-spec.md Q8).
///
/// # Examples
///
/// ```rust
/// use tinio_config::pipeline::Config;
///
/// let config = Config::default();
/// assert_eq!(config.io.workers, 2);
/// assert_eq!(config.db.workers, 1);
/// assert_eq!(config.io.capacity, 1024);
/// assert_eq!(config.io.priority, tinio_config::pipeline::Priority::Normal);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Config {
    /// The IO pipeline (ETag computation: bounded file reads + hashing).
    #[serde(default)]
    #[garde(dive)]
    pub io: Io,
    /// The DB write pipeline (batched meta writes).
    #[serde(default)]
    #[garde(dive)]
    pub db: Db,
}

/// The `workers`/`priority`/`capacity` field triple shared by the two
/// pipeline sections (F45 — one definition, two aliases; the worker
/// range and default differ per pipeline).
///
/// # Examples
///
/// ```rust
/// use tinio_config::pipeline::Io;
///
/// let io = Io::default();
/// assert_eq!(io.workers, 2);
/// assert_eq!(io.capacity, 1024);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Queue<const DEFAULT_WORKERS: u8, const WORKERS_MIN: u8, const WORKERS_MAX: u8> {
    /// Worker-thread count (each worker runs one blocking task; the
    /// range is per pipeline — db is 1..=4, redb is single-writer, so
    /// the default 1 is the write-throughput optimum).
    #[serde(default = "default_workers::<DEFAULT_WORKERS>")]
    #[default(_code = "DEFAULT_WORKERS")]
    #[garde(range(min = WORKERS_MIN, max = WORKERS_MAX))]
    pub workers: u8,
    /// Worker-thread priority (`normal` = do not set).
    #[serde(default)]
    #[default(Priority::Normal)]
    pub priority: Priority,
    /// Bounded queue capacity (1..=65536; the backpressure bound).
    #[serde(default = "default_capacity")]
    #[default(_code = "DEFAULT_CAPACITY")]
    #[garde(range(min = CAPACITY_MIN, max = CAPACITY_MAX))]
    pub capacity: u32,
}

/// The IO pipeline keys (`[pipeline.io]`).
pub type Io = Queue<DEFAULT_IO_WORKERS, IO_WORKERS_MIN, IO_WORKERS_MAX>;

/// The DB write pipeline keys (`[pipeline.db]`).
pub type Db = Queue<DEFAULT_DB_WORKERS, DB_WORKERS_MIN, DB_WORKERS_MAX>;

/// The serde default of `workers` — the pipeline's own constant, never a
/// round-trip through `Default` (F46: the serde defaults mirror the
/// constants they exist to mirror).
fn default_workers<const DEFAULT_WORKERS: u8>() -> u8 {
    DEFAULT_WORKERS
}

fn default_capacity() -> u32 {
    DEFAULT_CAPACITY
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;

    #[test]
    fn absent_section_resolves_to_none() {
        // Q8: presence-gated — no `[pipeline]` section, no `pipeline` key.
        let config = crate::Config::parse("version = 1").unwrap();
        assert!(config.pipeline.is_none());
    }

    #[test]
    fn defaults_match_the_contract() {
        let config = Config::default();
        assert_eq!(config.io.workers, DEFAULT_IO_WORKERS);
        assert_eq!(config.db.workers, DEFAULT_DB_WORKERS);
        assert_eq!(config.io.capacity, DEFAULT_CAPACITY);
        assert_eq!(config.db.capacity, DEFAULT_CAPACITY);
        assert_eq!(config.io.priority, Priority::Normal);
        assert_eq!(config.db.priority, Priority::Normal);
    }

    #[test]
    fn workers_range_validated() {
        // io: 1..=64; db: 1..=4 — outside → startup error.
        for (section, bad) in [("io", 0u8), ("io", 65), ("db", 0), ("db", 5)] {
            let text = format!("version = 1\n[pipeline.{section}]\nworkers = {bad}");
            let err = crate::Config::parse(&text).unwrap_err();
            assert!(matches!(err, Error::InvalidValue { .. }), "{text}: {err}");
        }
        let config = crate::Config::parse("version = 1\n[pipeline.io]\nworkers = 64").unwrap();
        assert_eq!(config.pipeline.as_ref().unwrap().io.workers, 64);
        let config = crate::Config::parse("version = 1\n[pipeline.db]\nworkers = 4").unwrap();
        assert_eq!(config.pipeline.as_ref().unwrap().db.workers, 4);
    }

    #[test]
    fn capacity_range_validated() {
        for bad in [0u32, 65537] {
            let text = format!("version = 1\n[pipeline.io]\ncapacity = {bad}");
            let err = crate::Config::parse(&text).unwrap_err();
            assert!(matches!(err, Error::InvalidValue { .. }), "{text}: {err}");
            let text = format!("version = 1\n[pipeline.db]\ncapacity = {bad}");
            let err = crate::Config::parse(&text).unwrap_err();
            assert!(matches!(err, Error::InvalidValue { .. }), "{text}: {err}");
        }
        let config = crate::Config::parse("version = 1\n[pipeline.io]\ncapacity = 65536").unwrap();
        assert_eq!(config.pipeline.as_ref().unwrap().io.capacity, 65536);
    }

    #[test]
    fn unknown_priority_rejected() {
        let err = crate::Config::parse("version = 1\n[pipeline.io]\npriority = \"realtime\"")
            .unwrap_err();
        assert!(matches!(err, Error::Parse { .. }), "{err}");
    }

    #[test]
    fn priority_parses_and_displays() {
        use std::str::FromStr;
        for (text, priority) in [
            ("normal", Priority::Normal),
            ("low", Priority::Low),
            ("high", Priority::High),
        ] {
            assert_eq!(Priority::from_str(text).unwrap(), priority);
            assert_eq!(priority.to_string(), text);
        }
        assert!(Priority::from_str("realtime").is_err());
    }

    #[test]
    fn priority_defaults_to_normal() {
        // `normal` = do not set a thread priority (the tinio-server runtime
        // owns the Q7 low/high mapping; pinned in its own tests).
        assert_eq!(Priority::default(), Priority::Normal);
    }

    #[test]
    fn partial_sections_keep_field_defaults() {
        let config = crate::Config::parse("version = 1\n[pipeline.io]\nworkers = 3").unwrap();
        let io = &config.pipeline.as_ref().unwrap().io;
        assert_eq!(io.workers, 3);
        assert_eq!(io.capacity, DEFAULT_CAPACITY);
        assert_eq!(io.priority, Priority::Normal);
        let config = crate::Config::parse("version = 1\n[pipeline]").unwrap();
        let pipeline = config.pipeline.as_ref().unwrap();
        assert_eq!(pipeline.io, Io::default());
        assert_eq!(pipeline.db, Db::default());
    }
}
