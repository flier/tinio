//! The read-only entry guard: the first of PG6's three layers.
//!
//! PG6 splits read-only mode (FR-023) into three layers, each of which holds where the one above
//! it cannot:
//!
//! 1. **The entry guard** — this module. Every mutating operation of
//!    [`Storage`](tinio_core::storage::Storage) and
//!    [`VersionOps`](tinio_core::storage::VersionOps) answers the contract's
//!    [`AccessDenied`](tinio_core::storage::Error::AccessDenied) **before any SQL**. It holds for
//!    every caller that comes through this backend's trait surface, which is the whole CLI and
//!    HTTP surface.
//! 2. **The session** — a read-only instance serves on a connection opened with
//!    `default_transaction_read_only = on` as a **startup option**
//!    ([`Pool::open_read_only`](tinio_store_pg::pool::Pool::open_read_only)), so the server holds
//!    the setting before the first statement and refuses a write the guard above somehow let
//!    through.
//! 3. **The role** — a read-only deployment's role carries **no `INSERT`/`UPDATE`/`DELETE`
//!    grants**. This is the layer that holds when a *helper* bypasses the entry guard entirely — a
//!    `tinio-store-pg` contract call made straight from this crate, say, where the guard does not
//!    sit — because it is the database's own refusal rather than this crate's. The deployment
//!    requirement is stated in `crates/tinio-pg/README.md`.
//!
//! # The shape of layer 1
//!
//! One function, [`PgStorage::writable`], is the choke point: it is the **first** statement of
//! every mutating method — before the key rules ([`PgStorage`]'s own reserved-segment refusals),
//! before the bucket-row probes, and before any engine or store call. A per-method `if` would put
//! the rule in a dozen places and let the next mutating method be written without it; one
//! function keeps the rule in one place and makes each call site a line whose *presence* is the
//! whole of its correctness.
//!
//! The mode is read from the store ([`PgStore::access`](tinio_store_pg::PgStore::access)) — the
//! mode the instance was constructed with — so there is no second copy of it here to fall out of
//! step with the session the pool opened or the schema gate the store applied.
//!
//! `crates/tinio-pg/tests/conformance.rs` enumerates every mutating method beside its read twin:
//! each mutating method is driven once and must answer `AccessDenied`, each read must succeed.
//! The enumeration is the audit of this choke point — a guard that went missing, or a new
//! mutating method that never called it, fails there.

use tinio_core::{bucket, object, storage};

use crate::{Error, storage::PgStorage};

/// The subject a read-only refusal names: the object key a key-scoped operation asked for, or the
/// bucket a bucket-scoped one did.
///
/// The contract's [`Denied`](tinio_core::storage::Denied) is the same pair and the same reason —
/// a bucket-scoped operation has no key, and fabricating one would make the payload a lie about
/// what was refused — but it owns its payload, while a guard on the hot path needs only a borrow.
/// The two shapes differ by that lifetime alone, so the guard borrows and the contract's own
/// constructors ([`storage::access_denied`], [`storage::access_denied_bucket`]) build the owned
/// payload on the refusal, the way `refuse_reserved` beside this module already does.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Subject<'a> {
    /// A key-scoped operation's subject.
    Key(&'a object::Key),
    /// A bucket-scoped operation's subject.
    Bucket(&'a bucket::Name),
}

impl PgStorage {
    /// The read-only entry guard (PG6, layer 1): a writable instance passes, a read-only one
    /// answers the contract's `AccessDenied` naming `subject`.
    ///
    /// Every mutating operation opens with this call — before the key rules, before any probe,
    /// and before any SQL — so the refusal's class is the contract's read-only class and never
    /// whatever the database happened to answer. Reads never call it: a read-only instance serves
    /// every read unchanged.
    ///
    /// # Errors
    ///
    /// [`storage::Error::AccessDenied`] when the instance was constructed read-only.
    pub(crate) fn writable(&self, subject: Subject<'_>) -> Result<(), Error> {
        if self.store().access().is_writable() {
            return Ok(());
        }
        Err(match subject {
            Subject::Key(key) => storage::access_denied(key),
            Subject::Bucket(name) => storage::access_denied_bucket(name),
        }
        .into())
    }
}
