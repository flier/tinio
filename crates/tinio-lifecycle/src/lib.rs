//! The S3 object lifecycle pass for tinio (design LC4): the background sweep
//! that applies a bucket's stored rules to its objects, its noncurrent
//! versions and its abandoned uploads.
//!
//! It depends on `tinio-core` and on `tinio-util`'s shared pacing seam: the
//! rules, their wire and their matcher are the model's, the deletions are the
//! storage contract's, and this crate is the loop between them. It is not a [`Storage`]
//! implementation and not a cleanup pass over a backend's private state.
//!
//! The pass keeps no cursor (LC12): every cycle re-lists, so a restarted run
//! resumes by re-deriving what is due. Each action is the API's own deletion
//! (LC2) — a current version expires through a delete marker in a versioned
//! bucket and an unlink otherwise, a noncurrent version is removed
//! permanently, an incomplete upload is aborted — so the engine owns no
//! version-aware deletion of its own.
//!
//! [`Storage`]: crate::_core::storage::Storage

#[doc(hidden)]
pub extern crate tinio_core as _core;
#[cfg(test)]
extern crate tinio_mem as _mem;
extern crate tinio_util as _util;

mod clock;
mod metrics;
mod runner;

pub use self::{
    clock::{Clock, FixedClock, SystemClock},
    runner::{Options, Runner, Seed, Summary},
};
