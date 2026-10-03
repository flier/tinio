//! Content-addressed block engine for tinio.
//!
//! A blob is stored once, addressed by the SHA-256 of its content, and read
//! back as a stream; blocks that no live reference points at are reclaimed by a
//! sweep. Nothing here knows what S3, a bucket or a version is: the contract is
//! [`BlockStore`](tinio_core::blob::BlockStore), the local implementation is
//! [`LocalBlocks`], and the policy — hashing, dedup, blob references,
//! reclamation — is [`Store`].

#[doc(hidden)]
pub extern crate tinio_core as _core;
#[doc(hidden)]
pub extern crate tinio_util as _util;

use std::result;

mod engine;
mod error;
mod local;
mod sweep;

#[cfg(feature = "testing")]
pub mod testing;

pub use self::{
    engine::Store,
    error::Error,
    local::LocalBlocks,
    sweep::{LiveSet, SweepReport},
};

/// The crate's result alias.
///
/// # Examples
///
/// ```rust
/// use std::io;
///
/// use tinio_blob::{Error, Result};
///
/// let failed: Result<u32> = Err(Error::Io(io::Error::other("the disk is gone")));
/// assert!(failed.is_err());
/// ```
pub type Result<T> = result::Result<T, Error>;
