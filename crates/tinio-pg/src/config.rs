//! Runtime configuration for the pg backend: the dsn and the pool size.
//!
//! **This is not the schema section.** `[storage.pg]` (`dsn`, `pool`; the selector, the
//! `TINIO_PG_DSN` override, the synthetic root and every refusal) is the CLI's configuration
//! root's, resolved by the CLI before anything is constructed (PG5). What lands here is the
//! small value the backend's constructor consumes: the connection string, always present, and
//! the pool size the driver is opened with (PG9). Neither type re-exports the other — a schema
//! is parsed, a construction is configured — and the dsn stays a [`Secret`] across the seam, so
//! a rendering of this value redacts the credential.

use crate::_util::secret::Secret;

/// The pg backend's runtime configuration.
///
/// # Examples
///
/// ```rust
/// use tinio_pg::Config;
/// use tinio_util::secret::{Secret, SecretKey};
///
/// let dsn = Secret::from(SecretKey::from(
///     "postgres://user:password@db.example:5432/tinio".to_owned(),
/// ));
/// let config = Config::new(dsn, 8);
/// assert_eq!(config.pool(), 8);
/// // The dsn is the value that was handed over, never a copy of its text.
/// assert_eq!(
///     format!("{config:?}").contains("password"),
///     false,
///     "the rendering redacts the dsn"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    dsn: Secret,
    pool: u32,
}

impl Config {
    /// A configuration for `dsn` whose driver pool holds `pool` connections.
    ///
    /// The two values are the caller's resolution: the dsn it opened (the environment override
    /// applied) and the pool it read from `[storage.pg] pool`. Neither has a default here —
    /// `tinio-cli`'s configuration root owns both — so a backend is never constructed from a
    /// configuration no operator wrote.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_pg::Config;
    /// use tinio_util::secret::{Secret, SecretKey};
    ///
    /// let config = Config::new(
    ///     Secret::from(SecretKey::from("postgres://db.example/tinio".to_owned())),
    ///     32,
    /// );
    /// assert_eq!(config.pool(), 32);
    /// ```
    pub fn new(dsn: Secret, pool: u32) -> Self {
        Self { dsn, pool }
    }

    /// The connection string the backend opens (never rendered by this type's `Debug`).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_pg::Config;
    /// use tinio_util::secret::{Secret, SecretKey};
    ///
    /// let dsn = Secret::from(SecretKey::from("postgres://db.example/tinio".to_owned()));
    /// let config = Config::new(dsn.clone(), 8);
    /// assert_eq!(config.dsn(), &dsn);
    /// ```
    pub fn dsn(&self) -> &Secret {
        &self.dsn
    }

    /// The configured pool size (PG9).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_pg::Config;
    /// use tinio_util::secret::{Secret, SecretKey};
    ///
    /// let config = Config::new(
    ///     Secret::from(SecretKey::from("postgres://db.example/tinio".to_owned())),
    ///     2,
    /// );
    /// assert_eq!(config.pool(), 2);
    /// ```
    pub fn pool(&self) -> u32 {
        self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::_util::secret::SecretKey;

    #[test]
    fn the_rendering_never_carries_the_credential() {
        // The dsn's password is the one thing a log line, a panic message or an event frame
        // must not carry, and this type's `Debug` is what they would render.
        let dsn = Secret::from(SecretKey::from(
            "postgres://user:hunter2@db.example:5432/tinio".to_owned(),
        ));
        let config = Config::new(dsn, 8);
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(rendered.contains("REDACTED"), "{rendered}");
    }
}
