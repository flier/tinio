//! The four metadata contracts over [`crate::RedbMetadata`] — one module per
//! contract (plan 2026-10-03).
//!
//! * [`bucket`] — `BucketMeta` over the `buckets` table (Task 5).
//! * [`object`] — `ObjectMeta` over `object_meta`/`object_parts` (Task 6).
//! * [`upload`] — `UploadMeta` over `uploads`/`parts` (Task 7).
//! * [`version`] — `VersionMeta` over the three version tables (Task 8).
//!
//! The content contract's implementation is [`crate::content`]'s
//! [`RedbContent`](crate::content::RedbContent): the byte plane is a separate
//! contract that no aggregate bound reaches, so it lives beside this module
//! rather than inside it.
//!
//! One file per contract is deliberate: the parallel tasks never touch the
//! same module, and each contract's conformance leg lives beside the
//! implementation it pins (the shared `contract.rs` carries the table-layer
//! rules only — never a per-task leg).

pub mod bucket;
pub mod object;
pub mod upload;
pub mod version;
pub(crate) mod version_lock;
