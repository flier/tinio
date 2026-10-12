//! The two `tinio-pg` test targets' shared support.
//!
//! The targets are separate crates to cargo, so a copy per file is what this module exists to
//! prevent: the scratch-database configuration they open their store with is one text each.
//!
//! The dsn the legs read and the skip notice they print when it is unset are
//! `tinio-store-pg`'s own, re-exported rather than re-spelled, so one `.env` newline is read
//! the same way in both crates.

pub use tinio_pg::_store_pg::test_support::require_dsn;
use tinio_pg::{_store_pg::test_support::TestDb, Config};
use tinio_util::secret::{Secret, SecretKey};

/// The pool a test's store is opened with: every leg opens its own store and `cargo test` runs
/// several at once, so the serving default would exhaust the server's connections.
pub const DEFAULT_TEST_POOL: u32 = 4;

/// This scratch database's runtime configuration: the dsn with a pool small enough for a
/// suite that opens several stores at once (the serving default is 8 per store).
pub fn scratch_config(db: &TestDb) -> Config {
    Config::new(
        Secret::from(SecretKey::from(db.dsn.clone())),
        DEFAULT_TEST_POOL,
    )
}
