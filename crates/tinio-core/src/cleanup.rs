//! The cleanup contract.
//!
//! Backends own their private state, so repair is a backend contract too
//! (task T012, per failure-handling.md §3): the [`Cleanup`] trait exposes
//! startup repair, doctor diagnostics/fix, and scanner meta-orphan
//! reclamation behind one seam. The start orchestration and `doctor` call it
//! through the trait — never through a backend implementation — so future
//! backends define their own repair semantics.
//!
//! [`Cleanup::repair`] and [`Cleanup::reclaim_meta_orphans`] return a
//! **stream of [`RepairAction`]s** instead of a final report: a repair run is
//! an internal pipeline of stages, and each stage reports its actions as it
//! performs them (progress for logs, `doctor` output, and the JSON report).
//! Every action carries a [`RepairActionLevel`] (lint/info/warn/error) for
//! severity-based reporting.
//!
//! `dry_run` is not a call parameter — it is fixed at construction:
//! implementations take [`CleanupOptions`] (e.g. `FsCleanup::new(root,
//! state_dir, CleanupOptions { dry_run: true })`), so `doctor --dry-run`
//! and `doctor --fix` are two instances of the same pipeline, and a dry-run
//! instance can never accidentally modify anything.
//!
//! The tinio-fs implementation (`FsCleanup`) is described in fs-backend.md
//! §8: it repairs `tmp/`, bucket-orphaned multipart subtrees, stale
//! `buckets.json` entries, stale `state`/socket, meta orphans, and stale
//! home root-state dirs — and never touches user data.

use std::error::Error;

use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::storage;

/// A pinned, `Send` stream of cleanup actions (progress reporting).
///
/// # Examples
///
/// ```rust
/// use futures::stream;
/// use tinio_core::{cleanup::ActionStream, storage};
///
/// let actions: ActionStream<storage::Error> = Box::pin(stream::empty());
/// ```
pub type ActionStream<E> = BoxStream<'static, Result<RepairAction, E>>;

/// The severity of a repair action, for layered reporting.
///
/// Maps onto `doctor`'s per-check severity (`ok`/`warn`/`error`,
/// contracts/cli.md) plus progress levels:
///
/// - [`RepairActionLevel::Lint`] — a check finding with no action needed
///   (e.g. "tmp/ is clean");
/// - [`RepairActionLevel::Info`] — a routine action (applied, or proposed
///   in dry-run mode);
/// - [`RepairActionLevel::Warn`] — an anomaly repaired (or found, in
///   dry-run mode);
/// - [`RepairActionLevel::Error`] — a problem that could not be repaired.
///
/// # Examples
///
/// ```rust
/// use tinio_core::cleanup::RepairActionLevel;
///
/// assert_ne!(RepairActionLevel::Info, RepairActionLevel::Error);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairActionLevel {
    /// A check finding, no action needed.
    Lint,
    /// A routine repair action.
    Info,
    /// An anomaly repaired (or found).
    Warn,
    /// A problem that could not be repaired.
    Error,
}

/// One repair action of the pipeline (performed, or proposed in dry-run
/// mode). Every applied action is logged to the operational log.
///
/// # Examples
///
/// ```rust
/// use tinio_core::cleanup::{RepairAction, RepairActionLevel};
///
/// let action = RepairAction {
///     level: RepairActionLevel::Warn,
///     description: "pruned stale buckets.json entry for `old-bucket`".into(),
/// };
/// assert_eq!(action.level, RepairActionLevel::Warn);
/// assert!(action.description.contains("buckets.json"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairAction {
    /// Severity of the action.
    pub level: RepairActionLevel,
    /// Human-readable description of the action.
    pub description: String,
}

/// The repair scope requested from a [`Cleanup`] implementation.
///
/// The two kinds differ by cost and caller (failure-handling.md §3):
///
/// - [`RepairKind::Startup`] — the fast, deterministic items that run after
///   single-instance binding and before readiness (SC-005): full `tmp/`
///   clear, bucket-orphaned multipart subtrees, stale `buckets.json` entries,
///   stale `state`/socket. Nothing that requires a full-tree walk.
/// - [`RepairKind::Full`] — the `doctor --fix` scope: every `Startup` item
///   plus meta-orphan reclamation and stale home root-state-dir GC.
///
/// # Examples
///
/// ```rust
/// use tinio_core::cleanup::RepairKind;
///
/// assert_ne!(RepairKind::Startup, RepairKind::Full);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairKind {
    /// Fast, deterministic items only (startup repair, T070).
    Startup,
    /// Everything: startup items + meta orphans + home root-state-dir GC
    /// (doctor --fix, T074).
    Full,
}

/// Construction options of a [`Cleanup`] implementation: whether the
/// pipeline runs in dry-run mode (report only, never touch anything).
///
/// # Examples
///
/// ```rust
/// use tinio_core::cleanup::CleanupOptions;
///
/// let options = CleanupOptions::default();
/// assert!(!options.dry_run); // default: repair for real
/// let dry = CleanupOptions {
///     dry_run: true,
///     ..CleanupOptions::default()
/// };
/// assert!(dry.dry_run);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupOptions {
    /// Report exactly what a real run would change without modifying
    /// anything.
    pub dry_run: bool,
}

/// The cleanup contract: startup repair, doctor diagnostics/fix, and scanner
/// meta-orphan reclamation, reported as a stream of actions.
///
/// `dry_run` is fixed at construction ([`CleanupOptions`]); the pipeline
/// internals are implementation-defined (tinio-fs: one code path per stage,
/// fs-backend.md §8). Implementations MUST only ever touch backend-private
/// state (tmp, multipart parts, metadata, state/socket, logs) — never user
/// data (bucket directories and objects) — and MUST log every applied
/// action to the operational log (failure-handling.md §1).
///
/// # Examples
///
/// ```rust
/// use futures::{StreamExt, stream};
/// use tinio_core::{
///     cleanup::{
///         ActionStream, Cleanup, CleanupOptions, RepairAction, RepairActionLevel, RepairKind,
///     },
///     storage,
/// };
/// use tokio::runtime::Runtime;
///
/// struct Noop;
///
/// #[async_trait::async_trait]
/// impl Cleanup for Noop {
///     type Error = storage::Error;
///
///     async fn repair(
///         &self,
///         _kind: RepairKind,
///     ) -> Result<ActionStream<storage::Error>, storage::Error> {
///         let actions: Vec<Result<RepairAction, storage::Error>> = vec![Ok(RepairAction {
///             level: RepairActionLevel::Lint,
///             description: "tmp/ is clean".into(),
///         })];
///         Ok(Box::pin(stream::iter(actions)))
///     }
///
///     async fn reclaim_meta_orphans(
///         &self,
///     ) -> Result<ActionStream<storage::Error>, storage::Error> {
///         Ok(Box::pin(stream::empty()))
///     }
/// }
///
/// let cleanup = Noop;
/// let _options = CleanupOptions {
///     dry_run: true,
///     ..CleanupOptions::default()
/// };
/// let mut stream = Runtime::new()
///     .unwrap()
///     .block_on(cleanup.repair(RepairKind::Startup))
///     .unwrap();
/// let action = Runtime::new()
///     .unwrap()
///     .block_on(stream.next())
///     .unwrap()
///     .unwrap();
/// assert_eq!(action.level, RepairActionLevel::Lint);
/// ```
#[async_trait]
pub trait Cleanup: Send + Sync + 'static {
    /// The backend error type (must convert into [`storage::Error`]).
    type Error: Error + Send + Sync + 'static + Into<storage::Error>;

    /// Run the repair pipeline for the requested kind, yielding each action
    /// as it is performed (or proposed, in dry-run mode).
    async fn repair(&self, kind: RepairKind) -> Result<ActionStream<Self::Error>, Self::Error>;

    /// Reclaim orphaned meta entries (entries whose object file no longer
    /// exists) — the background-scanner path (T045); `doctor --fix` reaches
    /// the same work through [`Self::repair`] with [`RepairKind::Full`].
    async fn reclaim_meta_orphans(&self) -> Result<ActionStream<Self::Error>, Self::Error>;
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, stream};
    use tokio::runtime::Runtime;

    use super::*;

    #[test]
    fn repair_kind_equality() {
        assert_eq!(RepairKind::Startup, RepairKind::Startup);
        assert_eq!(RepairKind::Full, RepairKind::Full);
        assert_ne!(RepairKind::Startup, RepairKind::Full);
    }

    #[test]
    fn repair_action_levels() {
        assert_ne!(RepairActionLevel::Lint, RepairActionLevel::Info);
        assert_ne!(RepairActionLevel::Info, RepairActionLevel::Warn);
        assert_ne!(RepairActionLevel::Warn, RepairActionLevel::Error);
    }

    #[test]
    fn repair_action_construct() {
        let action = RepairAction {
            level: RepairActionLevel::Info,
            description: "clear tmp/".into(),
        };
        assert_eq!(action.level, RepairActionLevel::Info);
        assert_eq!(action.description, "clear tmp/");
    }

    #[test]
    fn cleanup_options_default_is_not_dry_run() {
        let options = CleanupOptions::default();
        assert!(!options.dry_run);
    }

    #[test]
    fn cleanup_is_dyn_compatible() {
        struct DummyCleanup;
        #[async_trait::async_trait]
        impl Cleanup for DummyCleanup {
            type Error = storage::Error;

            async fn repair(
                &self,
                _kind: RepairKind,
            ) -> Result<ActionStream<storage::Error>, storage::Error> {
                Ok(Box::pin(stream::empty()))
            }

            async fn reclaim_meta_orphans(
                &self,
            ) -> Result<ActionStream<storage::Error>, storage::Error> {
                Ok(Box::pin(stream::empty()))
            }
        }
        fn takes_trait_object(_c: &dyn Cleanup<Error = storage::Error>) {}
        fn assert_send_sync<T: Send + Sync + 'static>() {}
        assert_send_sync::<Box<dyn Cleanup<Error = storage::Error>>>();
        takes_trait_object(&DummyCleanup);

        // Exercise the trait-object methods through the dyn reference.
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let c: &dyn Cleanup<Error = storage::Error> = &DummyCleanup;
            let mut actions = c.repair(RepairKind::Full).await.unwrap();
            assert!(StreamExt::next(&mut actions).await.is_none());
            let mut reclaim = c.reclaim_meta_orphans().await.unwrap();
            assert!(StreamExt::next(&mut reclaim).await.is_none());
        });
    }
}
