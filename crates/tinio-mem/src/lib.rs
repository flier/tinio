//! In-memory storage backend for tinio.
//!
//! [`MemoryStorage`] implements the `tinio-core` `Storage` contract over a
//! redb database on the `redb::backends::InMemoryBackend` — no disk, fast
//! test setup. It serves as the shared buffer layer for other backends (e.g.
//! a write/cache tier in front of `tinio-fs`) and as the CLI's backend when
//! no filesystem directory is given.
//!
//! The implementation is split by operation group: `bucket` (the
//! `BucketOps` impl), `object` (the `ObjectOps` impl), `multipart` (the
//! `MultipartOps` impl), with the shared database layout and helpers in
//! `storage`. Backend failures are [`Error`].

mod bucket;
mod cleanup;
mod error;
mod multipart;
mod object;
mod storage;

pub use self::cleanup::MemoryCleanup;
pub use self::error::{DatabaseError, Error};
pub use self::storage::{MemoryOptions, MemoryStorage};
