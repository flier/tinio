//! The tier layer's runtime configuration.
//!
//! The CLI owns the `[storage.tier]` schema, its defaults and its document
//! validation (T22); this module carries the runtime value those settings are
//! converted into, the two plane views the engine reads, and the cross-field
//! rules that make a runtime value coherent.

use std::time::Duration;

use garde::{Error as GardeError, Report, Validate};
use serde::{Deserialize, Serialize};

/// How a write reaches the filesystem.
///
/// `Sync` streams every body through the filesystem's staging path before the
/// write is acknowledged. `Batch` lets a plain, small, unversioned write wait in
/// the memory buffer and reach the filesystem on a flush; the buffer is bounded
/// by the configured batch caps.
///
/// # Examples
///
/// ```rust
/// use tinio_tier::FlushMode;
///
/// let mode = FlushMode::Sync;
/// assert_ne!(mode, FlushMode::Batch);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FlushMode {
    /// A write reaches the filesystem before it is acknowledged.
    Sync,
    /// A plain small write is buffered and reaches the filesystem on a flush.
    Batch,
}

/// The tier layer's runtime settings.
///
/// Every value is explicit: the CLI owns the schema and the defaults, so this
/// type carries no `Default` implementation and no defaults of its own. The
/// cross-field rules live in [`validate`](Self::validate).
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_tier::{Config, FlushMode};
///
/// let config = Config {
///     capacity_bytes: 256 * 1024 * 1024,
///     max_object_bytes: 4 * 1024 * 1024,
///     flush_mode: FlushMode::Sync,
///     batch_bytes: 8 * 1024 * 1024,
///     batch_interval: Duration::from_secs(1),
///     heat_window: Duration::from_secs(3600),
///     promote_reads: 3,
///     stats_flush: Duration::from_secs(10),
///     high_water_percent: 90,
///     low_water_percent: 80,
///     demote_idle: Duration::from_secs(3600),
///     dwell: Duration::from_secs(3600),
///     scan_interval: Duration::from_secs(60),
///     move_concurrency: 1,
///     index_compact_percent: 30,
///     gc_grace: Duration::from_secs(300),
/// };
/// assert!(config.validate().is_ok());
/// ```
#[derive(Debug, Clone, PartialEq, Validate)]
#[garde(allow_unvalidated)]
#[garde(custom(validate_limits))]
pub struct Config {
    /// The byte budget the object cache and the write buffer partition; the
    /// mover measures the filesystem's pressure rather than comparing it to a
    /// configured cap.
    pub capacity_bytes: u64,
    /// The largest object the cache and the write buffer admit; a larger object
    /// streams through the filesystem instead.
    pub max_object_bytes: u64,
    /// How a write reaches the filesystem.
    pub flush_mode: FlushMode,
    /// The write buffer's byte threshold.
    pub batch_bytes: u64,
    /// The write buffer's age threshold.
    pub batch_interval: Duration,
    /// How long a read contributes to an object's heat.
    pub heat_window: Duration,
    /// The read count at which an object enters the promotion queue.
    pub promote_reads: u32,
    /// How often the heat counters are flushed to the index.
    pub stats_flush: Duration,
    /// The capacity percentage above which the mover begins demoting.
    pub high_water_percent: u8,
    /// The capacity percentage the mover demotes down to.
    pub low_water_percent: u8,
    /// How long an object may stay unread before idle demotion considers it.
    pub demote_idle: Duration,
    /// How long an object must stay hot before it is promoted.
    pub dwell: Duration,
    /// How often the capacity and idle passes scan for candidates.
    pub scan_interval: Duration,
    /// The number of concurrent moves a pass runs.
    pub move_concurrency: usize,
    /// The fragmentation percentage at which the index compacts.
    pub index_compact_percent: u8,
    /// The grace period before garbage collection resolves a move's leftovers.
    pub gc_grace: Duration,
}

impl Config {
    /// Checks the cross-field invariants a runtime configuration must satisfy.
    ///
    /// A missing `[storage.pg]` section is the CLI's concern (the schema owns
    /// it); the rules here are the ones the runtime value itself can judge.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use tinio_tier::{Config, FlushMode};
    ///
    /// let mut config = Config {
    ///     capacity_bytes: 256 * 1024 * 1024,
    ///     max_object_bytes: 4 * 1024 * 1024,
    ///     flush_mode: FlushMode::Sync,
    ///     batch_bytes: 8 * 1024 * 1024,
    ///     batch_interval: Duration::from_secs(1),
    ///     heat_window: Duration::from_secs(3600),
    ///     promote_reads: 3,
    ///     stats_flush: Duration::from_secs(10),
    ///     high_water_percent: 90,
    ///     low_water_percent: 80,
    ///     demote_idle: Duration::from_secs(3600),
    ///     dwell: Duration::from_secs(3600),
    ///     scan_interval: Duration::from_secs(60),
    ///     move_concurrency: 1,
    ///     index_compact_percent: 30,
    ///     gc_grace: Duration::from_secs(300),
    /// };
    /// assert!(config.validate().is_ok());
    ///
    /// config.low_water_percent = 90;
    /// let report = config.validate().expect_err("the watermarks are inverted");
    /// assert!(report.to_string().contains("low_water_percent"));
    /// ```
    pub fn validate(&self) -> Result<(), Report> {
        <Self as Validate>::validate(self)
    }

    /// The memory plane's runtime view: the cache, the write buffer and heat.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use tinio_tier::{Config, FlushMode};
    ///
    /// let config = Config {
    ///     capacity_bytes: 256 * 1024 * 1024,
    ///     max_object_bytes: 4 * 1024 * 1024,
    ///     flush_mode: FlushMode::Sync,
    ///     batch_bytes: 8 * 1024 * 1024,
    ///     batch_interval: Duration::from_secs(1),
    ///     heat_window: Duration::from_secs(3600),
    ///     promote_reads: 3,
    ///     stats_flush: Duration::from_secs(10),
    ///     high_water_percent: 90,
    ///     low_water_percent: 80,
    ///     demote_idle: Duration::from_secs(3600),
    ///     dwell: Duration::from_secs(3600),
    ///     scan_interval: Duration::from_secs(60),
    ///     move_concurrency: 1,
    ///     index_compact_percent: 30,
    ///     gc_grace: Duration::from_secs(300),
    /// };
    /// let mem = config.mem();
    /// assert_eq!(mem.capacity_bytes, config.capacity_bytes);
    /// assert_eq!(mem.max_object_bytes, config.max_object_bytes);
    /// ```
    pub fn mem(&self) -> Mem {
        Mem::from(self)
    }

    /// The filesystem plane's runtime view: capacity, moves and collection.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use tinio_tier::{Config, FlushMode};
    ///
    /// let config = Config {
    ///     capacity_bytes: 256 * 1024 * 1024,
    ///     max_object_bytes: 4 * 1024 * 1024,
    ///     flush_mode: FlushMode::Sync,
    ///     batch_bytes: 8 * 1024 * 1024,
    ///     batch_interval: Duration::from_secs(1),
    ///     heat_window: Duration::from_secs(3600),
    ///     promote_reads: 3,
    ///     stats_flush: Duration::from_secs(10),
    ///     high_water_percent: 90,
    ///     low_water_percent: 80,
    ///     demote_idle: Duration::from_secs(3600),
    ///     dwell: Duration::from_secs(3600),
    ///     scan_interval: Duration::from_secs(60),
    ///     move_concurrency: 1,
    ///     index_compact_percent: 30,
    ///     gc_grace: Duration::from_secs(300),
    /// };
    /// let fs = config.fs();
    /// assert_eq!(fs.high_water_percent, config.high_water_percent);
    /// assert_eq!(fs.move_concurrency, config.move_concurrency);
    /// ```
    pub fn fs(&self) -> Fs {
        Fs::from(self)
    }
}

/// The memory plane's runtime limits: the object cache, the write buffer and
/// the heat counters share this view of a [`Config`].
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_tier::{FlushMode, Mem};
///
/// let mem = Mem {
///     capacity_bytes: 256 * 1024 * 1024,
///     max_object_bytes: 4 * 1024 * 1024,
///     flush_mode: FlushMode::Sync,
///     batch_bytes: 8 * 1024 * 1024,
///     batch_interval: Duration::from_secs(1),
///     heat_window: Duration::from_secs(3600),
///     promote_reads: 3,
///     stats_flush: Duration::from_secs(10),
/// };
/// assert_eq!(mem.promote_reads, 3);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mem {
    /// The byte budget the cache and the write buffer partition.
    pub capacity_bytes: u64,
    /// The largest object either admits.
    pub max_object_bytes: u64,
    /// How a write reaches the filesystem.
    pub flush_mode: FlushMode,
    /// The write buffer's byte threshold.
    pub batch_bytes: u64,
    /// The write buffer's age threshold.
    pub batch_interval: Duration,
    /// How long a read contributes to an object's heat.
    pub heat_window: Duration,
    /// The read count at which an object enters the promotion queue.
    pub promote_reads: u32,
    /// How often the heat counters are flushed to the index.
    pub stats_flush: Duration,
}

// Derive cannot express a field subset, so this manual conversion stays.
impl From<&Config> for Mem {
    fn from(config: &Config) -> Self {
        Self {
            capacity_bytes: config.capacity_bytes,
            max_object_bytes: config.max_object_bytes,
            flush_mode: config.flush_mode,
            batch_bytes: config.batch_bytes,
            batch_interval: config.batch_interval,
            heat_window: config.heat_window,
            promote_reads: config.promote_reads,
            stats_flush: config.stats_flush,
        }
    }
}

/// The filesystem plane's runtime limits: watermarks, moves, scanning and
/// collection share this view of a [`Config`].
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_tier::Fs;
///
/// let fs = Fs {
///     high_water_percent: 90,
///     low_water_percent: 80,
///     demote_idle: Duration::from_secs(3600),
///     dwell: Duration::from_secs(3600),
///     scan_interval: Duration::from_secs(60),
///     move_concurrency: 1,
///     index_compact_percent: 30,
///     gc_grace: Duration::from_secs(300),
/// };
/// assert_eq!(fs.move_concurrency, 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fs {
    /// The capacity percentage above which the mover begins demoting.
    pub high_water_percent: u8,
    /// The capacity percentage the mover demotes down to.
    pub low_water_percent: u8,
    /// How long an object may stay unread before idle demotion considers it.
    pub demote_idle: Duration,
    /// How long an object must stay hot before it is promoted.
    pub dwell: Duration,
    /// How often the capacity and idle passes scan for candidates.
    pub scan_interval: Duration,
    /// The number of concurrent moves a pass runs.
    pub move_concurrency: usize,
    /// The fragmentation percentage at which the index compacts.
    pub index_compact_percent: u8,
    /// The grace period before garbage collection resolves a move's leftovers.
    pub gc_grace: Duration,
}

// Derive cannot express a field subset, so this manual conversion stays.
impl From<&Config> for Fs {
    fn from(config: &Config) -> Self {
        Self {
            high_water_percent: config.high_water_percent,
            low_water_percent: config.low_water_percent,
            demote_idle: config.demote_idle,
            dwell: config.dwell,
            scan_interval: config.scan_interval,
            move_concurrency: config.move_concurrency,
            index_compact_percent: config.index_compact_percent,
            gc_grace: config.gc_grace,
        }
    }
}

/// The configuration's cross-field rules: one report naming every violated
/// pair, so an operator fixes them in one pass.
fn validate_limits(config: &Config, _context: &()) -> garde::Result {
    let mut problems = Vec::new();
    if config.low_water_percent >= config.high_water_percent {
        problems.push("low_water_percent must be below high_water_percent");
    }
    if config.high_water_percent > 100 {
        problems.push("high_water_percent must be at most 100");
    }
    if config.low_water_percent > 100 {
        problems.push("low_water_percent must be at most 100");
    }
    if config.max_object_bytes > config.capacity_bytes {
        problems.push("max_object_bytes must not exceed capacity_bytes");
    }
    if config.batch_bytes > config.capacity_bytes {
        problems.push("batch_bytes must not exceed capacity_bytes");
    }
    if config.move_concurrency < 1 {
        problems.push("move_concurrency must be at least 1");
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(GardeError::new(problems.join("; ")))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn config() -> Config {
        Config {
            capacity_bytes: 256 * 1024 * 1024,
            max_object_bytes: 4 * 1024 * 1024,
            flush_mode: FlushMode::Sync,
            batch_bytes: 8 * 1024 * 1024,
            batch_interval: Duration::from_secs(1),
            heat_window: Duration::from_secs(3600),
            promote_reads: 3,
            stats_flush: Duration::from_secs(10),
            high_water_percent: 90,
            low_water_percent: 80,
            demote_idle: Duration::from_secs(3600),
            dwell: Duration::from_secs(3600),
            scan_interval: Duration::from_secs(60),
            move_concurrency: 1,
            index_compact_percent: 30,
            gc_grace: Duration::from_secs(300),
        }
    }

    #[test]
    fn low_watermark_below_high() {
        assert!(
            config().validate().is_ok(),
            "80 is below 90: a coherent pair"
        );

        let equal = Config {
            low_water_percent: 90,
            ..config()
        };
        let report = equal.validate().unwrap_err();
        assert!(report.to_string().contains("low_water_percent"), "{report}");

        let inverted = Config {
            low_water_percent: 95,
            ..config()
        };
        assert!(inverted.validate().is_err(), "95 is not below 90");
    }

    #[test]
    fn caps_do_not_exceed_capacity() {
        let at_capacity = Config {
            max_object_bytes: config().capacity_bytes,
            batch_bytes: config().capacity_bytes,
            ..config()
        };
        assert!(
            at_capacity.validate().is_ok(),
            "a cap equal to the capacity is admitted"
        );

        let object_over = Config {
            max_object_bytes: config().capacity_bytes + 1,
            ..config()
        };
        let report = object_over.validate().unwrap_err();
        assert!(report.to_string().contains("max_object_bytes"), "{report}");

        let batch_over = Config {
            batch_bytes: config().capacity_bytes + 1,
            ..config()
        };
        let report = batch_over.validate().unwrap_err();
        assert!(report.to_string().contains("batch_bytes"), "{report}");
    }

    #[test]
    fn move_concurrency_at_least_one() {
        assert_eq!(config().move_concurrency, 1);
        assert!(config().validate().is_ok());

        let idle = Config {
            move_concurrency: 0,
            ..config()
        };
        let report = idle.validate().unwrap_err();
        assert!(report.to_string().contains("move_concurrency"), "{report}");
    }

    #[test]
    fn watermarks_are_percentages() {
        let edge = Config {
            high_water_percent: 100,
            low_water_percent: 0,
            ..config()
        };
        assert!(
            edge.validate().is_ok(),
            "0 and 100 are the percentage's own edges"
        );

        let high_over = Config {
            high_water_percent: 101,
            ..config()
        };
        let report = high_over.validate().unwrap_err();
        assert!(
            report.to_string().contains("high_water_percent"),
            "{report}"
        );

        let low_over = Config {
            high_water_percent: 102,
            low_water_percent: 101,
            ..config()
        };
        let report = low_over.validate().unwrap_err();
        let report = report.to_string();
        assert!(
            report.contains("low_water_percent must be at most 100"),
            "{report}"
        );
        assert!(
            report.contains("high_water_percent must be at most 100"),
            "{report}"
        );
    }
}
