//! PostgreSQL implementation of the tinio persistence contracts.
//!
//! The crate implements the four persistence contracts — `meta::Metadata`,
//! `meta::VersionMeta`, `content::ObjectContent` and `blob::BlockStore` (all
//! `tinio_core`) — over one connection per transaction from a pooled driver
//! session, with
//! schema versioning, an error type projecting into
//! [`storage::Error`](tinio_core::storage::Error), and TLS-capable pooled
//! connections.
//!
//! # Layout
//!
//! - [`Error`] — the crate error and every redaction rule (PG5).
//! - [`pool`] — the driver pool, the dsn, and the TLS policy (PG9/PG10).
//! - [`migrations`] — the numbered schema migrations (PG11).
//! - [`store`] — the store, its access mode, and the ownership guard (PG12).
//! - [`content`] — the `bytea`-chunked content plane (PG18/PG20/PG23/PG24).
//! - [`blocks`] — the content-addressed block plane (PG14/PG19).
//! - [`diagnostics`] — the read-only probes an operator's diagnosis asks for (PG4/PG6).
//! - [`lock_inspect`] — the doctor's read-only Object Lock scan (OL31).
//!
//! # Ordering
//!
//! Every key column is `text COLLATE "C"` and every ordered read repeats the
//! collation, so a listing orders by the key's UTF-8 byte sequence rather than
//! the server's locale. The connect-time UTF-8 check makes that identity real:
//! `"C"` orders the server encoding's bytes (PG4).
//!
//! The crate *is* the PostgreSQL implementation, so it carries no `pg` feature
//! of its own: a configuration that does not serve PostgreSQL does not depend
//! on it at all (PG15) -- `pg` belongs to the consumer, as an optional
//! dependency. The one feature is `testing`, off by default and absent from
//! every production graph: it publishes the scratch-database harness
//! (`test_support::TestDb`) so other crates' tests share it. The
//! database-backed tests are ordinary `#[cfg(test)]` legs, and
//! they skip when `TINIO_PG_DSN` is unset, so the default build and
//! `cargo test --workspace` stay database-free (PG8).

#[doc(hidden)]
pub extern crate tinio_core as _core;

// The crate's own tests address it by its public name, exactly as an external
// consumer would.
extern crate self as tinio_store_pg;

#[doc(hidden)]
pub extern crate sqlx as _sqlx;

pub mod blocks;
pub mod cleanup;
pub mod content;
pub mod diagnostics;
pub mod lock_inspect;
pub mod meta;
pub mod migrations;
pub mod pool;
pub mod store;
pub mod teardown;

mod error;
mod metrics;
mod timestamp;
mod window;

pub use error::Error;
pub use store::{Access, Ownership, PgStore, SchemaState};

#[cfg(any(test, feature = "testing"))]
#[path = "tests/db.rs"]
pub mod test_support;

#[cfg(test)]
mod tests;

/// The owned transaction a contract method runs in.
pub(crate) type PgTx = _sqlx::Transaction<'static, _sqlx::Postgres>;

/// Acquire one connection and begin a transaction on it.
#[tracing::instrument(target = "tinio::storage", level = "trace", skip_all)]
pub(crate) async fn begin(pool: &_sqlx::PgPool) -> Result<PgTx, Error> {
    Ok(pool.begin().await?)
}
