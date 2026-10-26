use std::time::Duration;

use garde::{Error as GardeError, Validate};
use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use crate::{
    _core::storage::{
        COMPACT_THRESHOLD_MAX_PERCENT, COMPACT_THRESHOLD_MIN_PERCENT,
        DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_FOLLOW_SYMLINKS, DEFAULT_META_BATCH_BYTES,
        DEFAULT_META_BATCH_SIZE, META_BATCH_BYTES_MAX, META_BATCH_BYTES_MIN, META_BATCH_SIZE_MAX,
        META_BATCH_SIZE_MIN,
    },
    _tier as tier,
};

pub mod pg;

/// Which backend a run serves (`[storage] type`; the selector PG5 lands).
///
/// The default is [`Backend::Fs`], so a configuration that never writes the key keeps serving a
/// filesystem root. The key parses in every build (§6): a build without the selected backend
/// refuses at startup, naming what it does not carry, rather than ignoring a key it read.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::config::storage::Backend;
///
/// assert_eq!(Backend::default(), Backend::Fs);
/// assert_eq!(Backend::Pg.to_string(), "pg");
/// assert_eq!(Backend::Tier.to_string(), "tier");
/// assert_eq!("fs".parse::<Backend>().unwrap(), Backend::Fs);
/// assert!("s3".parse::<Backend>().is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, Display, FromStr)]
#[serde(rename_all = "lowercase")]
#[display(style = "lowercase")]
pub enum Backend {
    /// The local filesystem backend (`tinio-fs`); the default.
    #[default]
    Fs,
    /// The PostgreSQL backend (`tinio-pg`).
    Pg,
    /// The tiered backend (`tinio-tier`): the memory cache and write buffer, the filesystem as
    /// the warm tier and PostgreSQL as the cold tier. Requires `[storage.pg]`.
    Tier,
}

/// Backend behavior keys (`[storage]`; the sections are per backend — the filesystem-specific
/// keys live in `[storage.fs]`, the in-memory limits in `[storage.mem]`, the PostgreSQL keys in
/// `[storage.pg]` and the tier layer's in `[storage.tier]`).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::config::storage::Config;
///
/// let storage = Config::default();
/// assert!(!storage.fs.follow_symlinks); // secure default: reject symlinks
/// assert_eq!(storage.backend.to_string(), "fs"); // no key written: filesystem
/// assert!(storage.pg.is_none());
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
#[garde(custom(validate_tier))]
pub struct Config {
    /// The backend selector (`[storage] type`; default `"fs"`). `type` is a Rust keyword, so the
    /// field carries the TOML key through a `rename`.
    #[serde(rename = "type", default)]
    pub backend: Backend,
    /// Filesystem backend keys (`[storage.fs]`).
    #[serde(default)]
    #[garde(dive)]
    pub fs: Fs,
    /// In-memory backend keys (`[storage.mem]`; limits apply when a server
    /// is wired to `tinio_mem::MemoryStorage` — the shipped server uses
    /// the filesystem backend, so these default to unlimited).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub mem: Option<Mem>,
    /// PostgreSQL backend keys (`[storage.pg]`; read when `type = "pg"` — and by
    /// `TINIO_PG_DSN`'s resolver, which overrides `dsn` — and required when
    /// `type = "tier"`, whose cold plane it is).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub pg: Option<pg::Config>,
    /// Tiered backend keys (`[storage.tier]`; read when `type = "tier"`, whose keys parse in
    /// every build). Absent = the section's defaults, so generated configs omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub tier: Option<Tier>,
}

/// Filesystem backend keys (`[storage.fs]`; the `follow_symlinks` key moved
/// here from `[storage]`).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::config::storage::Fs;
///
/// let fs = Fs::default();
/// assert!(!fs.follow_symlinks); // secure default: reject symlinks
/// assert_eq!(fs.compact_threshold_percent, 20);
/// assert_eq!(fs.meta_batch_size, 128); // task-2.5 set_batch benchmark knee
/// assert_eq!(fs.meta_batch_bytes, 262144);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
#[serde(default)]
pub struct Fs {
    /// Follow symlinks in the storage root (default `false`: access never
    /// resolves through a link and listings exclude link entries — a link
    /// inside a bucket cannot escape the storage root).
    #[default(_code = "DEFAULT_FOLLOW_SYMLINKS")]
    pub follow_symlinks: bool,
    /// Compact trigger: the fragmentation percentage at which the state
    /// database is compacted at startup (5..=90).
    #[default(_code = "DEFAULT_COMPACT_THRESHOLD_PERCENT")]
    #[garde(
        range(
            min = COMPACT_THRESHOLD_MIN_PERCENT,
            max = COMPACT_THRESHOLD_MAX_PERCENT
        )
    )]
    pub compact_threshold_percent: u8,
    /// Meta-batch entry-count threshold (1..=4096): the cold list/scanner
    /// producers flush one write-pipeline batch once it holds this many
    /// entries (pipeline-spec.md Q5/Q6; default from the task-2.5
    /// `set_batch` benchmark knee).
    #[default(_code = "DEFAULT_META_BATCH_SIZE")]
    #[garde(range(min = META_BATCH_SIZE_MIN, max = META_BATCH_SIZE_MAX))]
    pub meta_batch_size: u16,
    /// Meta-batch byte threshold (1024..=16 MiB): the producers flush once
    /// the estimated batch size (≈ 56 B + key length per entry) reaches
    /// this (pipeline-spec.md Q5).
    #[default(_code = "DEFAULT_META_BATCH_BYTES")]
    #[garde(range(min = META_BATCH_BYTES_MIN, max = META_BATCH_BYTES_MAX))]
    pub meta_batch_bytes: u32,
}

/// In-memory backend resource limits (`[storage.mem]`). Every key is
/// optional and defaults to unlimited, matching the project's documented
/// no-limit posture (CHK028); an operator wiring the in-memory backend
/// should set them explicitly.
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Mem {
    /// Maximum size of a single object or multipart part in bytes
    /// (absent = unlimited).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(custom(validate_positive_option))]
    pub max_object_bytes: Option<u64>,
    /// Maximum total stored bytes across all objects and parts
    /// (absent = unlimited).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(custom(validate_positive_option))]
    pub max_total_bytes: Option<u64>,
}

/// A byte limit must be positive when present (absent = unlimited).
fn validate_positive_option(value: &Option<u64>, _context: &()) -> garde::Result {
    if value.is_some_and(|v| v == 0) {
        return Err(GardeError::new(
            "byte limits must be positive when set (omit the key for unlimited)",
        ));
    }
    Ok(())
}

impl Config {
    /// The tier layer's runtime configuration, when this section names it.
    ///
    /// The parsed `[storage.tier]` section when present (a section beside another backend is
    /// still converted, so its own rules can refuse it); `type = "tier"` with the section
    /// absent is its defaults, so a complete tier document needs no key beyond the selector
    /// and `[storage.pg]`. `None` means no tier configuration is named at all.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_cli::config::Config;
    ///
    /// let config = Config::parse(
    ///     "version = 1\n[storage]\ntype = \"tier\"\n[storage.pg]\ndsn = \"postgres://db/tinio\"",
    /// )
    /// .expect("config");
    /// let runtime = config
    ///     .storage
    ///     .as_ref()
    ///     .expect("section")
    ///     .tier_runtime()
    ///     .expect("the selector names the tier backend");
    /// assert_eq!(runtime.capacity_bytes, 256 * 1024 * 1024);
    /// ```
    pub fn tier_runtime(&self) -> Option<tier::Config> {
        if self.backend == Backend::Tier {
            return Some(self.tier.clone().unwrap_or_default().runtime());
        }
        self.tier.as_ref().map(Tier::runtime)
    }
}

/// The tier layer's keys (`[storage.tier]`; read when `[storage] type = "tier"`).
///
/// The CLI owns this schema and these defaults (T22); `tinio_tier::Config` is the runtime value
/// [`runtime`](Self::runtime) builds, and its own `validate` owns the cross-field rules. The
/// section is presence-gated, so a generated config never carries it.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_cli::config::storage::Tier;
///
/// let tier = Tier::default();
/// assert_eq!(tier.capacity_bytes, 256 * 1024 * 1024);
/// assert_eq!(tier.batch_interval, Duration::from_secs(1));
/// assert_eq!(tier.high_water_percent, 90);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
#[serde(default)]
pub struct Tier {
    /// The object cache and write buffer's shared byte budget. This is the mem + buffer
    /// partition (T9), not the warm filesystem's cap.
    #[default(_code = "DEFAULT_CAPACITY_BYTES")]
    pub capacity_bytes: u64,
    /// The largest object the cache and the write buffer admit; a larger object streams
    /// through the filesystem instead.
    #[default(_code = "DEFAULT_MAX_OBJECT_BYTES")]
    pub max_object_bytes: u64,
    /// How a write reaches the filesystem: `sync` (streamed through the stage before the
    /// acknowledgement) or `batch` (a plain small unversioned write waits for a flush).
    #[default(_code = "DEFAULT_FLUSH_MODE")]
    pub flush_mode: tier::FlushMode,
    /// The write buffer's byte threshold.
    #[default(_code = "DEFAULT_BATCH_BYTES")]
    pub batch_bytes: u64,
    /// The write buffer's age threshold.
    #[serde(with = "humantime_serde")]
    #[default(_code = "DEFAULT_BATCH_INTERVAL")]
    pub batch_interval: Duration,
    /// How long a read contributes to an object's heat.
    #[serde(with = "humantime_serde")]
    #[default(_code = "DEFAULT_HEAT_WINDOW")]
    pub heat_window: Duration,
    /// The read count at which an object enters the promotion queue.
    #[default(_code = "DEFAULT_PROMOTE_READS")]
    pub promote_reads: u32,
    /// How often the heat counters are flushed to the index.
    #[serde(with = "humantime_serde")]
    #[default(_code = "DEFAULT_STATS_FLUSH")]
    pub stats_flush: Duration,
    /// The capacity percentage above which the mover begins demoting.
    #[default(_code = "DEFAULT_HIGH_WATER_PERCENT")]
    pub high_water_percent: u8,
    /// The capacity percentage the mover demotes down to.
    #[default(_code = "DEFAULT_LOW_WATER_PERCENT")]
    pub low_water_percent: u8,
    /// How long an object may stay unread before idle demotion considers it.
    #[serde(with = "humantime_serde")]
    #[default(_code = "DEFAULT_DEMOTE_IDLE")]
    pub demote_idle: Duration,
    /// How long an object must stay hot before it is promoted.
    #[serde(with = "humantime_serde")]
    #[default(_code = "DEFAULT_DWELL")]
    pub dwell: Duration,
    /// How often the capacity and idle passes scan for candidates.
    #[serde(with = "humantime_serde")]
    #[default(_code = "DEFAULT_SCAN_INTERVAL")]
    pub scan_interval: Duration,
    /// The number of concurrent moves a pass runs.
    #[default(_code = "DEFAULT_MOVE_CONCURRENCY")]
    pub move_concurrency: usize,
    /// The fragmentation percentage at which the index compacts.
    #[default(_code = "DEFAULT_INDEX_COMPACT_PERCENT")]
    pub index_compact_percent: u8,
    /// The grace period before garbage collection resolves a move's leftovers.
    #[serde(with = "humantime_serde")]
    #[default(_code = "DEFAULT_GC_GRACE")]
    pub gc_grace: Duration,
}

impl Tier {
    /// The runtime value the tier layer consumes: the parsed keys as `tinio_tier::Config`, for
    /// its `validate` and the engine.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_cli::config::storage::Tier;
    ///
    /// let runtime = Tier::default().runtime();
    /// assert_eq!(runtime.low_water_percent, 80);
    /// assert!(runtime.validate().is_ok());
    /// ```
    pub fn runtime(&self) -> tier::Config {
        tier::Config {
            capacity_bytes: self.capacity_bytes,
            max_object_bytes: self.max_object_bytes,
            flush_mode: self.flush_mode,
            batch_bytes: self.batch_bytes,
            batch_interval: self.batch_interval,
            heat_window: self.heat_window,
            promote_reads: self.promote_reads,
            stats_flush: self.stats_flush,
            high_water_percent: self.high_water_percent,
            low_water_percent: self.low_water_percent,
            demote_idle: self.demote_idle,
            dwell: self.dwell,
            scan_interval: self.scan_interval,
            move_concurrency: self.move_concurrency,
            index_compact_percent: self.index_compact_percent,
            gc_grace: self.gc_grace,
        }
    }
}

/// The `[storage.tier]` defaults (T22: the CLI is the only definer).
const DEFAULT_CAPACITY_BYTES: u64 = 256 * 1024 * 1024;
/// `max_object_bytes`: 4 MiB, the largest object the cache and buffer admit.
const DEFAULT_MAX_OBJECT_BYTES: u64 = 4 * 1024 * 1024;
/// `flush_mode`: every write streams through the filesystem stage before acknowledgement.
const DEFAULT_FLUSH_MODE: tier::FlushMode = tier::FlushMode::Sync;
/// `batch_bytes`: 8 MiB, the write buffer's byte threshold.
const DEFAULT_BATCH_BYTES: u64 = 8 * 1024 * 1024;
/// `batch_interval`: 1s, the write buffer's age threshold.
const DEFAULT_BATCH_INTERVAL: Duration = Duration::from_secs(1);
/// `heat_window`: 1h, how long a read contributes to an object's heat.
const DEFAULT_HEAT_WINDOW: Duration = Duration::from_secs(3600);
/// `promote_reads`: 3 reads between an object's entry into the promotion queue.
const DEFAULT_PROMOTE_READS: u32 = 3;
/// `stats_flush`: 10s, the heat counters' flush cadence.
const DEFAULT_STATS_FLUSH: Duration = Duration::from_secs(10);
/// `high_water_percent`: 90%, where demotion begins.
const DEFAULT_HIGH_WATER_PERCENT: u8 = 90;
/// `low_water_percent`: 80%, where demotion aims.
const DEFAULT_LOW_WATER_PERCENT: u8 = 80;
/// `demote_idle`: 1h unread before idle demotion considers an object.
const DEFAULT_DEMOTE_IDLE: Duration = Duration::from_secs(3600);
/// `dwell`: 1h hot before an object is promoted.
const DEFAULT_DWELL: Duration = Duration::from_secs(3600);
/// `scan_interval`: 60s between capacity and idle scans.
const DEFAULT_SCAN_INTERVAL: Duration = Duration::from_secs(60);
/// `move_concurrency`: one concurrent move per pass.
const DEFAULT_MOVE_CONCURRENCY: usize = 1;
/// `index_compact_percent`: 30% fragmentation compacts the index.
const DEFAULT_INDEX_COMPACT_PERCENT: u8 = 30;
/// `gc_grace`: 5m before garbage collection resolves a move's leftovers.
const DEFAULT_GC_GRACE: Duration = Duration::from_secs(5 * 60);

/// The tier section's cross-field rules: `type = "tier"` names its cold plane, and every
/// present `[storage.tier]` section is handed to the runtime value's own `validate` (T22: the
/// runtime owns the combinations the flat schema cannot see).
fn validate_tier(config: &Config, _context: &()) -> garde::Result {
    if config.backend == Backend::Tier && config.pg.is_none() {
        return Err(GardeError::new(
            "storage.pg: `type = \"tier\"` requires the [storage.pg] section — the tier layer's cold plane",
        ));
    }
    if let Some(runtime) = config.tier_runtime() {
        runtime
            .validate()
            .map_err(|report| GardeError::new(format!("storage.tier: {report}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_contract() {
        // The `[storage.fs]` schema defaults are the tinio-core constants
        // (shared with `FsOptions`, so the two cannot drift).
        let fs = Fs::default();
        assert_eq!(fs.meta_batch_size, DEFAULT_META_BATCH_SIZE);
        assert_eq!(fs.meta_batch_bytes, DEFAULT_META_BATCH_BYTES);
    }

    #[test]
    fn the_backend_spellings_are_the_contracts() {
        assert_eq!(Backend::Fs.to_string(), "fs");
        assert_eq!(Backend::Pg.to_string(), "pg");
        assert_eq!(Backend::Tier.to_string(), "tier");
        assert_eq!("pg".parse::<Backend>().unwrap(), Backend::Pg);
        assert_eq!("tier".parse::<Backend>().unwrap(), Backend::Tier);
        assert!("s3".parse::<Backend>().is_err());
    }
}
