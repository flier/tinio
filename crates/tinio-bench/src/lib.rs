//! The measurement harness for the workspace's A/B bench protocol
//! (`docs/benches.md`).
//!
//! Three pieces, one per question a criterion run cannot answer on this host:
//!
//! - [`measure`] runs **one named operation** in a fresh process, times it
//!   sample by sample and prints a single `ab-result` line: the time, its
//!   spread, the run's flush control and the operation's allocation count.
//!   A leg is such a process; the protocol's answer lives in the line.
//! - [`verdict`] turns a series of interleaved rounds into a decision — or
//!   into a refusal, when the interval the rounds produce spans "no
//!   difference".
//! - [`CountingAlloc`] is the allocation channel: `crates/tinio-fs/benches/ab.rs`
//!   installs it as its `#[global_allocator]` and every reading then carries an
//!   allocation count beside its times. That count is the one number on the
//!   line the host's flush regime cannot move.
//!
//! The crate exists because the counting allocator is `unsafe`: a crate whose
//! benches need it may not carry it itself when it sets `unsafe_code =
//! "forbid"` (`crates/tinio-fs` does, and a `forbid` on the command line
//! cannot be lifted by an `#![allow]` in its bench target). Everything here is
//! compiled into bench and test builds only — no production crate depends on
//! it.
//!
//! The benches' data root and the flush control stay where they were:
//! `tinio_util::bench` (the `bench` feature), one copy for every bench target
//! in the workspace. This crate reads them, it does not re-implement them.

#[doc(hidden)]
pub extern crate tinio_util as _util;

pub mod alloc;
pub mod measure;
pub mod verdict;

pub use self::{
    alloc::CountingAlloc,
    measure::{Op, Reading, run},
    verdict::{Decision, Round, Rules, Verdict, decide},
};

/// The counting allocator, installed into this crate's own test binary only:
/// the counters move where the allocator is installed, and a bench target
/// installs it itself (`crates/tinio-fs/benches/ab.rs` is the one that does).
#[cfg(test)]
#[global_allocator]
static TEST_ALLOCATOR: CountingAlloc = CountingAlloc::new();
