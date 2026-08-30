use garde::{Error as GardeError, Validate};
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;
use tinio_core::storage::{
    COMPACT_THRESHOLD_MAX_PERCENT, COMPACT_THRESHOLD_MIN_PERCENT,
    DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_FOLLOW_SYMLINKS, DEFAULT_META_BATCH_BYTES,
    DEFAULT_META_BATCH_SIZE, META_BATCH_BYTES_MAX, META_BATCH_BYTES_MIN, META_BATCH_SIZE_MAX,
    META_BATCH_SIZE_MIN,
};

/// Backend behavior keys (`[storage]`; filesystem-only in v1 — the
/// filesystem-specific keys live in `[storage.fs]`).
///
/// # Examples
///
/// ```rust
/// use tinio_config::storage::Config;
///
/// let storage = Config::default();
/// assert!(!storage.fs.follow_symlinks); // secure default: reject symlinks
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Config {
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
}

/// Filesystem backend keys (`[storage.fs]`; the `follow_symlinks` key moved
/// here from `[storage]`).
///
/// # Examples
///
/// ```rust
/// use tinio_config::storage::Fs;
///
/// let fs = Fs::default();
/// assert!(!fs.follow_symlinks); // secure default: reject symlinks
/// assert_eq!(fs.compact_threshold_percent, 20);
/// assert_eq!(fs.meta_batch_size, 128); // task-2.5 set_batch benchmark knee
/// assert_eq!(fs.meta_batch_bytes, 262144);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Fs {
    /// Follow symlinks in the storage root (default `false`: access never
    /// resolves through a link and listings exclude link entries — a link
    /// inside a bucket cannot escape the storage root).
    #[serde(default)]
    #[default(_code = "DEFAULT_FOLLOW_SYMLINKS")]
    pub follow_symlinks: bool,
    /// Compact trigger: the fragmentation percentage at which the state
    /// database is compacted at startup (5..=90).
    #[serde(default = "compact_threshold_percent")]
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
    #[serde(default = "meta_batch_size")]
    #[default(_code = "DEFAULT_META_BATCH_SIZE")]
    #[garde(range(min = META_BATCH_SIZE_MIN, max = META_BATCH_SIZE_MAX))]
    pub meta_batch_size: u16,
    /// Meta-batch byte threshold (1024..=16 MiB): the producers flush once
    /// the estimated batch size (≈ 56 B + key length per entry) reaches
    /// this (pipeline-spec.md Q5).
    #[serde(default = "meta_batch_bytes")]
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

fn compact_threshold_percent() -> u8 {
    Fs::default().compact_threshold_percent
}

fn meta_batch_size() -> u16 {
    Fs::default().meta_batch_size
}

fn meta_batch_bytes() -> u32 {
    Fs::default().meta_batch_bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config as RootConfig, Error};

    #[test]
    fn defaults_match_the_contract() {
        // The `[storage.fs]` schema defaults are the tinio-core constants
        // (shared with `FsOptions`, so the two cannot drift).
        let fs = Fs::default();
        assert_eq!(fs.meta_batch_size, DEFAULT_META_BATCH_SIZE);
        assert_eq!(fs.meta_batch_bytes, DEFAULT_META_BATCH_BYTES);
    }

    #[test]
    fn meta_batch_size_range_validated() {
        // 1..=4096; outside → startup error (pipeline-spec.md §3.3).
        for bad in [0u16, 4097] {
            let text = format!("version = 1\n[storage.fs]\nmeta_batch_size = {bad}");
            let err = RootConfig::parse(&text).unwrap_err();
            assert!(matches!(err, Error::InvalidValue { .. }), "{text}: {err}");
        }
        for good in [1u16, 128, 4096] {
            let text = format!("version = 1\n[storage.fs]\nmeta_batch_size = {good}");
            let config = RootConfig::parse(&text).unwrap();
            assert_eq!(config.storage.as_ref().unwrap().fs.meta_batch_size, good);
        }
    }

    #[test]
    fn meta_batch_bytes_range_validated() {
        // 1024..=16 MiB; outside → startup error.
        for bad in [1023u32, 16 * 1024 * 1024 + 1] {
            let text = format!("version = 1\n[storage.fs]\nmeta_batch_bytes = {bad}");
            let err = RootConfig::parse(&text).unwrap_err();
            assert!(matches!(err, Error::InvalidValue { .. }), "{text}: {err}");
        }
        for good in [1024u32, 262144, 16 * 1024 * 1024] {
            let text = format!("version = 1\n[storage.fs]\nmeta_batch_bytes = {good}");
            let config = RootConfig::parse(&text).unwrap();
            assert_eq!(config.storage.as_ref().unwrap().fs.meta_batch_bytes, good);
        }
    }

    #[test]
    fn absent_keys_keep_the_defaults() {
        // Presence-gated (Q8): a section that omits the keys deserializes
        // the defaults — never the field-type default (0 would fail garde).
        let config =
            RootConfig::parse("version = 1\n[storage.fs]\nfollow_symlinks = false").unwrap();
        let fs = &config.storage.as_ref().unwrap().fs;
        assert_eq!(fs.meta_batch_size, DEFAULT_META_BATCH_SIZE);
        assert_eq!(fs.meta_batch_bytes, DEFAULT_META_BATCH_BYTES);
    }

    #[test]
    fn mem_limits_default_to_unlimited() {
        // `[storage.mem]` is absent by default, and the limits are
        // optional — the documented no-limit posture (CHK028).
        let config = RootConfig::parse("version = 1").unwrap();
        assert!(config.storage.is_none());

        let config = RootConfig::parse("version = 1\n[storage.mem]").unwrap();
        let mem = config.storage.as_ref().unwrap().mem.as_ref().unwrap();
        assert_eq!(mem.max_object_bytes, None);
        assert_eq!(mem.max_total_bytes, None);
    }

    #[test]
    fn mem_limits_parse_when_set() {
        let config = RootConfig::parse(
            "version = 1\n[storage.mem]\nmax_object_bytes = 1048576\nmax_total_bytes = 1073741824",
        )
        .unwrap();
        let mem = config.storage.as_ref().unwrap().mem.as_ref().unwrap();
        assert_eq!(mem.max_object_bytes, Some(1048576));
        assert_eq!(mem.max_total_bytes, Some(1073741824));
    }

    #[test]
    fn mem_limits_reject_zero() {
        // A byte limit must be positive when present (absent = unlimited).
        for bad in [
            "version = 1\n[storage.mem]\nmax_object_bytes = 0",
            "version = 1\n[storage.mem]\nmax_total_bytes = 0",
        ] {
            let err = RootConfig::parse(bad).unwrap_err();
            assert!(matches!(err, Error::InvalidValue { .. }), "{bad}: {err}");
        }
    }
}
