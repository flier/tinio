use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;
use tinio_core::storage::{
    COMPACT_THRESHOLD_MAX_PERCENT, COMPACT_THRESHOLD_MIN_PERCENT,
    DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_FOLLOW_SYMLINKS,
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
}

fn compact_threshold_percent() -> u8 {
    Fs::default().compact_threshold_percent
}
