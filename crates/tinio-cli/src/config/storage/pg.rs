//! PostgreSQL backend keys (`[storage.pg]`; selected by `[storage] type = "pg"`).

use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use crate::_util::secret::Secret;

/// The PostgreSQL backend's keys (`[storage.pg]`): the connection string and the pool size.
///
/// **The keys parse in every build** (PG5): no key under `[storage]` is cargo-feature gated, so
/// a configuration written for a pg deployment is readable — and validated — by a `tinio-cli`
/// that is linked into a build carrying no pg backend at all. What such a build does with the
/// section is the consumer's decision; the schema's is that the keys are known.
///
/// `dsn` is **absent by default**: `TINIO_PG_DSN` is the other source PG5 names ("`TINIO_PG_DSN`
/// overrides the dsn"), and the run that reads the variable needs a section that can omit the key.
/// A configuration with neither source is refused where the dsn is resolved, with a message naming
/// both remedies rather than here, because this layer cannot see the environment; an **empty**
/// value from either source is the unset key, and that one reading lives with the resolver.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::config::storage::pg::Config;
///
/// let pg = Config::default();
/// assert_eq!(pg.pool, 8); // PG9's default
/// assert!(pg.dsn.is_none()); // the env-only shape (TINIO_PG_DSN)
/// ```
#[derive(Debug, Clone, PartialEq, Eq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
#[serde(default)]
pub struct Config {
    /// The connection string (`TINIO_PG_DSN` overrides it). Held as [`Secret`], so a rendering
    /// of this section redacts it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dsn: Option<Secret>,
    /// The driver pool's size (PG9; default 8 — `tinio-store-pg`'s own
    /// `DEFAULT_POOL_SIZE`, spelled here because this crate cannot depend on the store).
    #[default(_code = "DEFAULT_POOL_SIZE")]
    #[garde(range(min = 1))]
    pub pool: u32,
}

/// The `[storage.pg] pool` default (PG9): the store's own pool size, so a run that omits the key
/// and one that pins 8 open the same number of connections.
pub const DEFAULT_POOL_SIZE: u32 = 8;
