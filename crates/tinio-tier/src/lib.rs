//! Tiered storage layer for tinio.
//!
//! One storage root is served from three planes: an in-memory cache and write
//! buffer, the filesystem as the warm durable tier, and PostgreSQL as the cold
//! tier. The layer decorates an [`FsStorage`](tinio_fs::FsStorage), reaches the
//! filesystem's rows through its `tier` seam, and keeps placement and cold
//! metadata in its own index.
//!
//! This module owns the crate's public surface: the runtime [`Config`] and its
//! plane views [`Mem`] and [`Fs`], the [`FlushMode`] a write follows, the
//! [`Error`] type and the cold plane's store alias [`ColdStore`]. The engine
//! modules land with the tasks that own them.

#[doc(hidden)]
pub extern crate tinio_core as _core;
#[doc(hidden)]
pub extern crate tinio_fs as _fs;
#[doc(hidden)]
pub extern crate tinio_pg as _pg;
#[doc(hidden)]
pub extern crate tinio_store_pg as _store_pg;
#[doc(hidden)]
pub extern crate tinio_util as _util;

mod atomic;
mod buffer;
mod cache;
mod capacity;
mod config;
mod error;
mod heat;
mod index;
mod listing;
#[cfg(test)]
mod metrics_tests;
mod mover;
pub mod pg;
mod quarantine;
mod stage;
mod storage;

pub use self::{
    _store_pg::PgStore as ColdStore,
    buffer::{Admission, Buffer, Buffered},
    cache::{Budget, Cache},
    capacity::{Capacity, VolumeCapacity},
    config::{Config, FlushMode, Fs, Mem},
    error::Error,
    heat::Heat,
    index::{ColdInfo, FORMAT_VERSION, HeatEntry, Index, Placement, State, Tier, WarmCandidate},
    mover::{GcSummary, MoveOutcome, Mover, PassSummary, VerifyReport},
    quarantine::Quarantined,
    stage::{MemInfo, TierStaged},
    storage::{TierStorage, index_path, is_tier_root, marker_path, refuse_non_tier_root},
};
