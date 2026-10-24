//! PostgreSQL backend for tinio.
//!
//! The servable pg backend: the `Storage`, `VersionOps` and `Cleanup` implementations over the
//! shared operation engine (`tinio-ops`) and `tinio-store-pg`'s four persistence contracts,
//! plus the runtime configuration and the constructor they are built from.
//!
//! # Features
//!
//! This crate **is** the pg backend, so it carries no `pg` switch of its own: `tinio-store-pg`
//! is a plain dependency and every module that implements the contracts over it is compiled in
//! every build. A build that does not want pg does not reference this crate — the `pg` switch
//! belongs to the consumers (`tinio-cli`'s and `tinio`'s). `tinio-store-pg`'s only feature is
//! the dev-only `testing` harness, which a production graph leaves off (PG15).
//!
//! - `versioning` adds the version plane (`tinio-version`): the pg backend's `VersionOps` twin
//!   and the enabled half of this crate's version module. Without it, that module's plane-free
//!   bodies leave `PgStorage` on the trait's own defaults — `State::Unversioned` and an
//!   access-denied class for every other operation — exactly as `tinio-fs`'s stripped build
//!   does.
//! - `testing` forwards the store's dev-only scratch-database harness. A production graph
//!   leaves it off; the `@pg` pass turns it on.
//!
//! [`Config`] and [`Error`] are feature-independent: a consumer that resolved `[storage.pg]`
//! builds the one in every graph, and the other is what a resolution failure and a
//! construction refusal are rendered with.
//!
//! # Invocation
//!
//! A pg run has no served root: the CLI resolves `[storage] type = "pg"`'s configuration, the
//! dsn (`TINIO_PG_DSN` over `[storage.pg] dsn`) and the synthetic root the tool state lives
//! under (PG5/PG13), and hands this crate a [`Config`] plus the access mode — read-write, which
//! takes the ownership lock, or read-only, which takes none (PG12).
//!
//! [`doctor`] is the diagnosis half: `tinio doctor` renders its rows for a pg deployment, and
//! every store fact it reads comes from the non-migrating inspection path (PG11).

#[doc(hidden)]
pub extern crate tinio_store_pg as _store_pg;
#[doc(hidden)]
pub extern crate tinio_util as _util;

pub mod config;

mod bucket;

pub mod cleanup;
pub mod doctor;
mod error;
mod hooks;
mod readonly;
mod storage;
mod version;

mod complete;

pub use config::Config;
pub use error::Error;

pub use self::{_store_pg::Access, storage::PgStorage};
