//! PostgreSQL backend keys (`[storage.pg]`; selected by `[storage] type = "pg"`).

use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use crate::_util::secret::Secret;

/// The PostgreSQL backend's keys (`[storage.pg]`): the connection string and the pool size.
///
/// **The keys parse in every build** (PG5): no key under `[storage]` is cargo-feature gated, so
/// a configuration written for a pg deployment is readable — and validated — by a `tinio-config`
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
/// use tinio_config::Config;
///
/// let config = Config::parse(
///     "version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\ndsn = \"postgres://user:password@db.example:5432/tinio\"",
/// )
/// .unwrap();
/// let pg = config.storage.as_ref().unwrap().pg.as_ref().unwrap();
/// assert_eq!(pg.pool, 8); // PG9's default
/// ```
#[derive(Debug, Clone, PartialEq, Eq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Config {
    /// The connection string (`TINIO_PG_DSN` overrides it). Held as [`Secret`], so a rendering
    /// of this section redacts it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dsn: Option<Secret>,
    /// The driver pool's size (PG9; default 8 — `tinio-store-pg`'s own
    /// `DEFAULT_POOL_SIZE`, spelled here because this crate cannot depend on the store).
    #[serde(default = "pool")]
    #[default(_code = "DEFAULT_POOL_SIZE")]
    #[garde(range(min = 1))]
    pub pool: u32,
}

/// The `[storage.pg] pool` default (PG9): the store's own pool size, so a run that omits the key
/// and one that pins 8 open the same number of connections.
pub const DEFAULT_POOL_SIZE: u32 = 8;

/// The pool-size serde default (see [`DEFAULT_POOL_SIZE`]).
fn pool() -> u32 {
    DEFAULT_POOL_SIZE
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;
    use crate::{Config as RootConfig, Error, storage::Backend};

    /// One fixture dsn, fictional and never connected to: the assertions are about *this* text
    /// reaching — or not reaching — a rendering, which is what PG5's redaction rule is about.
    const DSN: &str = "postgres://user:hunter2@db.example:5432/tinio";

    fn section(config: &str) -> Config {
        let text = format!("version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\n{config}");
        RootConfig::parse(&text)
            .expect("the section parses")
            .storage
            .expect("the section is present")
            .pg
            .expect("the pg section is present")
    }

    #[test]
    fn the_section_parses_and_the_pool_defaults_to_eight() {
        let pg = section(&format!("dsn = \"{DSN}\""));
        assert_eq!(pg.pool, DEFAULT_POOL_SIZE);
        assert_eq!(pg.pool, 8);
        assert_eq!(pg.dsn.expect("dsn").expose_secret().as_ref(), DSN);
    }

    #[test]
    fn the_dsn_is_absent_by_default_and_the_pool_parses_when_written() {
        // The env-only shape (TINIO_PG_DSN, PG5): a section with no dsn parses.
        let pg = section("pool = 32");
        assert!(pg.dsn.is_none());
        assert_eq!(pg.pool, 32);
    }

    #[test]
    fn an_empty_dsn_parses_and_is_the_resolvers_to_read() {
        // `dsn = ""` names nothing, and the one reading that turns it into the unset key lives with
        // the resolver (`resolve::pg::resolve_dsn`), which reads the variable the same way.
        let pg = section("dsn = \"\"");
        assert_eq!(pg.dsn.expect("the key parses").expose_secret().as_ref(), "");
    }

    #[test]
    fn the_pool_must_be_positive() {
        let err = RootConfig::parse("version = 1\n[storage.pg]\npool = 0").unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
    }

    #[test]
    fn a_rendering_of_the_section_masks_the_password() {
        // PG5: the dsn is a `Secret`; a section that reached a log through its `Debug` must not
        // carry the password.
        let pg = section(&format!("dsn = \"{DSN}\""));
        let rendered = format!("{pg:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(rendered.contains("REDACTED"), "{rendered}");
    }

    #[test]
    fn the_section_round_trips_through_toml() {
        // The keys are the contract's shape in both directions: what the parser reads is what
        // the serializer writes (`dsn` included, and `pool` always).
        let text = format!(
            "version = 1\n\n[storage]\ntype = \"pg\"\n\n[storage.pg]\ndsn = \"{DSN}\"\npool = 4"
        );
        let config = RootConfig::parse(&text).expect("parses");
        let again = RootConfig::parse(&config.to_toml().expect("serializes")).expect("re-parses");
        assert_eq!(config, again);
        let toml = config.to_toml().expect("serializes");
        assert!(toml.contains("type = \"pg\""), "{toml}");
        assert!(toml.contains("pool = 4"), "{toml}");
    }

    #[test]
    fn the_keys_parse_in_every_build() {
        // §6: the keys are not feature-gated -- this crate carries no cargo feature at all, so
        // this leg *is* the no-pg-feature leg, and a configuration written for a pg deployment
        // parses and validates wherever the schema is linked.
        let config = RootConfig::parse(&format!(
            "version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\ndsn = \"{DSN}\"\npool = 2"
        ))
        .expect("the pg keys parse with no pg backend in the build");
        assert_eq!(config.storage.as_ref().unwrap().backend, Backend::Pg);
    }
}
