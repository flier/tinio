use garde::{Error as GardeError, Validate};
use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use crate::_core::storage::{
    COMPACT_THRESHOLD_MAX_PERCENT, COMPACT_THRESHOLD_MIN_PERCENT,
    DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_FOLLOW_SYMLINKS, DEFAULT_META_BATCH_BYTES,
    DEFAULT_META_BATCH_SIZE, META_BATCH_BYTES_MAX, META_BATCH_BYTES_MIN, META_BATCH_SIZE_MAX,
    META_BATCH_SIZE_MIN,
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
}

/// Backend behavior keys (`[storage]`; the sections are per backend — the filesystem-specific
/// keys live in `[storage.fs]`, the in-memory limits in `[storage.mem]`, the PostgreSQL keys in
/// `[storage.pg]`).
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
    /// `TINIO_PG_DSN`'s resolver, which overrides `dsn`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[garde(dive)]
    pub pg: Option<pg::Config>,
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
        assert_eq!("pg".parse::<Backend>().unwrap(), Backend::Pg);
        assert!("s3".parse::<Backend>().is_err());
    }
}
