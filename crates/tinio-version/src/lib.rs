//! The shared version state machine for tinio.
//!
//! One rule every backend shares: **the current version is whatever that
//! backend's object store holds**. The engine in [`Versions`] never invents a
//! second authority for it, and it knows nothing about paths, tables or files:
//! it is written against the persistence contracts
//! ([`Metadata`](tinio_core::meta::Metadata) and
//! [`VersionMeta`](tinio_core::meta::VersionMeta)), the content contract
//! ([`ObjectContent`](tinio_core::content::ObjectContent)) and the shared blob
//! engine over the backend's block store. A backend implements the contracts;
//! the state machine, the crash-safety order and the supersede race are
//! inherited unchanged.
//!
//! Two ports stay. [`CurrentRows`]: the metadata contract's point read
//! deliberately does not gate, while the engine must only ever see the
//! backend's **gated** current (the F01/F04 staleness rule a read applies).
//! [`MarkerWrite`]: a version-creating delete leaves a slot with no version
//! row, which no contract write expresses. The block plane needs no port at
//! all, because the engine holds [`Store`](tinio_blob::Store) directly.
//!
//! The write path's **decision** is a pure function, not a port:
//! [`plan_write`] answers whether to supersede, which identity and sequence
//! the incoming version takes, whether the one-null-row convergence applies
//! and whether the result is a delete marker, from the bucket's state, the
//! key's gated current slot and the key's sequence view. Both backends
//! execute that one decision with their own transactions, writes and I/O, so
//! their write policies cannot drift.
//!
//! This crate carries no cargo feature: it is always compiled, and the
//! compile-time versioning boundary lives in the backends and the server.

#[doc(hidden)]
pub extern crate tinio_blob as _blob;
#[doc(hidden)]
pub extern crate tinio_core as _core;

mod engine;
mod error;
mod plan;
pub mod ports;

// The port double is compiled for the unit tests only: the doctests link the
// library `cargo test --doc` builds for them, and that unit carries neither
// `cfg(test)` nor `cfg(doctest)` in this toolchain (no `--cfg doctest` on it,
// measured), so a doctest cannot name it.
#[cfg(test)]
mod testutil;

pub use self::{
    engine::Versions,
    error::Error,
    plan::{Identity, Intent, Plan, Seqs, converges_null, plan_write},
    ports::{Current, CurrentRows, MarkerWrite, PublishedBody},
};
