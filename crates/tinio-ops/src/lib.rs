//! The S3 operation bodies the tinio backends share.
//!
//! `tinio-ops` writes the S3 storage semantics once, over the persistence
//! contracts: the operation bodies behind the `tinio-core` `Storage`
//! categories, the two-method [`Hooks`] seam a backend
//! implements for the steps only it can take, and the two per-backend policies
//! the bodies read rather than infer. It is deliberately **not** a `Storage`
//! implementation and **not** a framework: every backend keeps its own
//! `Storage`/`VersionOps`/`Cleanup` implementations, its error enum, its
//! `Hooks` implementation and its locks, and calls the bodies from there.
//!
//! The engine knows the four contracts only — [`Metadata`], [`VersionMeta`],
//! [`ObjectContent`] and [`BlockStore`] — and never sees a table, a path or
//! SQL. No backend crate reaches it.
//!
//! [`Metadata`]: tinio_core::meta::Metadata
//! [`VersionMeta`]: tinio_core::meta::VersionMeta
//! [`ObjectContent`]: tinio_core::content::ObjectContent
//! [`BlockStore`]: tinio_core::blob::BlockStore
//!
//! # Modules
//!
//! One module per operation group — [`bucket`], [`object`], [`multipart`],
//! [`version`] — plus the shared pieces the groups build on: [`ops`] (the
//! group receivers' policies), [`hooks`] (the seam), [`listing`] (the listing
//! path, which the object group's `list_objects` lives in) and [`staged`] (the
//! staged body).
//!
//! **What it writes.** The bucket group's twelve bodies, the object group's
//! thirteen (the version-creating put beside the version-free lifecycle, and
//! the read-heal they share), the multipart group's seven, the version
//! group's nine and the version-creating delete the version group's module
//! carries beside them. The inventory those answer (E2's 44 operations) is counted once, per group and per backend, in the design's `Coverage as landed` table (`docs/superpowers/specs/2026-10-03-ops-engine-design.md`) — this is the module map, not a second count. A backend
//! takes the groups its storage can express: `mem` takes the bucket group, the
//! version-free object bodies, five multipart bodies and the listing, and
//! keeps its version group, its version-creating writes and four documented
//! steps mem-local (E16); `fs` takes 11 of the 12 bucket bodies (`list_buckets`
//! stays backend-side, because a bucket the API has never sighted has no row
//! to enumerate), the object group's fourteen implemented bodies (its
//! `put_object` is the core trait's default over the engine's stage and
//! commit), all seven multipart bodies and all nine version bodies.
//!
//! **What it does not write.** Two inventory operations stay backend-side, and
//! each group's module doc carries its reason: the multipart **completion**
//! ([`multipart`]) — each backend's commit half is one state transaction (fs's
//! assembly, the version branch and the consumed upload; mem's in-transaction
//! version decision) — and **copy**, which stays with the backends (fs renames
//! within a bucket; mem reads then writes). The version-addressed **delete** is
//! a body ([`version`]'s `delete_object_version`), whose restore moves the
//! current slot **backwards to a real identity** — a move `advance_slot`
//! refuses — in one transaction with the object row and the row/index drop;
//! the version-creating **delete**'s ordering is the same module's
//! `create_delete_marker`, and only its slot write is the backend's
//! `tinio_version::MarkerWrite` port, because a marker is a recorded slot with
//! **no** version row, a state no contract write expresses.
//! `list_object_versions`, `current_version` and `ensure_rename_allowed` are
//! the trait's backend-side methods by decision (E14) — never bodies the
//! inventory expects here.
//!
//! The engine carries no cargo feature: the version-touching bodies are always
//! compiled, and a backend without versioning refuses through its own `#[cfg]`
//! twin, so the engine never answers for it.

#[doc(hidden)]
pub extern crate tinio_blob as _blob;
#[doc(hidden)]
pub extern crate tinio_core as _core;
#[cfg(test)]
#[doc(hidden)]
pub extern crate tinio_store_redb as _store_redb;
#[doc(hidden)]
pub extern crate tinio_version as _version;

pub mod bucket;
pub mod hooks;
pub mod listing;
pub(crate) mod metrics;
pub mod multipart;
pub mod object;
pub mod ops;
pub mod staged;
#[cfg(test)]
mod testutil;
pub mod version;

pub use self::{
    hooks::{Guard, Hooks, NoGuard, NoHooks},
    multipart::MultipartOps,
    object::ObjectOps,
    ops::{BucketOps, BucketRow, MetadataFailure, VersionPlane},
    staged::StagedBody,
};
