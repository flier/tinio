//! The generic task-pipeline contract (pipeline-spec.md §3.1, §7).
//!
//! A [`Task`] is a customized [`Future`]: [`Task::run`] returns any
//! [`Send`] value ([`Task::Output`]). A [`Runner`] is the executor —
//! `enqueue` is `spawn`, the returned [`Completion`] is the `JoinHandle`.
//! The runner awaits `run()` and [`Reply::send`]s that value; the producer
//! awaits the handle (list) or drops it (scanner fire-and-forget).
//!
//! [`Reply::send`] returns the value when nobody is listening (dropped
//! receiver or [`Reply::none`]) so the concurrent runtime can still log a
//! [`Result::Err`] (R8). The pipeline itself is **semantic-free** — it
//! never interprets `Ok` payloads.
//!
//! `run()` MUST NOT panic (R6): the concurrent runtime (tinio-server) wraps
//! task execution in `catch_unwind` and keeps its workers alive. The public
//! reference implementation, [`InlineRunner`] (Q1), instead passes panics
//! through to the caller, so failures stay visible in doctor, benches, and
//! unit tests.
//!
//! # `async_trait` and the `&mut self` receiver
//!
//! `async fn` in traits is not dyn-compatible (E0038), so the contract uses
//! the `async_trait` macro — the same mechanism as the [`crate::cleanup`]
//! contract — which type-erases each future into a boxed `Send` future
//! callable through a trait object. [`Task::run`] takes `&mut self` rather
//! than consuming `self` because no consuming receiver is callable on
//! `Box<dyn Task<Output = _>>`: a by-value `self` cannot be moved out of
//! an unsized trait object (E0161). The runner drops the box after `run`
//! completes, so a task runs **at most once**.
//!
//! # Bounds
//!
//! - [`Task`]: `Send + 'static` only — a task is never shared: it lives in
//!   one box, moves between threads inside that box, and runs at most once.
//!   `Sync` would exclude channel payloads (e.g. the spec's `oneshot`
//!   senders are `Send` but not `Sync`) for no benefit. [`Task::Output`]
//!   is `Send + 'static`.
//! - [`Runner`]: `Send + Sync + 'static` — `Arc<dyn Runner<O>>` must be
//!   shareable across server threads (`FsOptions.io_pipeline` /
//!   `db_pipeline`, P4). The default `O` is [`RunOutput`].
//!
//! # Shutdown (Q3) and backpressure
//!
//! `enqueue` is bounded — it waits for queue capacity (backpressure). After
//! `shutdown()`, queued tasks are dropped, in-flight tasks run to
//! completion, and `enqueue` returns `Err`.

use std::{
    error::Error as StdError,
    fmt,
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};

use async_trait::async_trait;
use futures::channel::oneshot;

/// Unit-success [`Task::Output`]: `Ok(())` or a type-erased error.
pub type RunOutput = Result<(), Box<dyn StdError + Send + Sync>>;

/// A runner-level failure: the task was not accepted, or its handle was
/// canceled before [`Task::run`] completed.
///
/// # Examples
///
/// ```rust
/// use tinio_core::pipeline::Error;
///
/// assert_eq!(Error::ShutDown.to_string(), "pipeline is shut down");
/// assert_eq!(Error::Dropped.to_string(), "pipeline task was dropped");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// [`Runner::shutdown`] has run; [`Runner::enqueue`] rejects new work
    /// (Q3).
    #[error("pipeline is shut down")]
    ShutDown,
    /// The task was dropped before its result was sent (panic, or a queued
    /// task discarded at shutdown).
    #[error("pipeline task was dropped")]
    Dropped,
}

/// An optional oneshot reply: the sender half of [`Completion`].
///
/// [`Reply::send`] delivers `value` when the receiver is live (`Ok`); a
/// dropped receiver (or [`Self::none`]) returns `Err(value)` so the
/// concurrent runtime can still log a [`Result::Err`] (R8). `send`
/// consumes the reply.
///
/// # Examples
///
/// ```rust
/// use tinio_core::pipeline::{Completion, Reply};
///
/// let none = Reply::<i32>::none();
/// assert_eq!(none.send(1), Err(1));
///
/// let (reply, _done) = Completion::<i32>::pair();
/// assert_eq!(reply.send(1), Ok(()));
///
/// let (reply, done) = Completion::<i32>::pair();
/// drop(done);
/// assert_eq!(reply.send(1), Err(1));
/// ```
pub struct Reply<T> {
    tx: Option<oneshot::Sender<T>>,
}

impl<T> Reply<T> {
    /// No channel: [`Self::send`] returns `Err(value)`.
    pub fn none() -> Self {
        Self { tx: None }
    }

    /// Send `value` to the waiter. `Ok` if delivered; `Err(value)` if
    /// nobody is listening.
    pub fn send(self, value: T) -> Result<(), T> {
        match self.tx {
            Some(tx) => tx.send(value),
            None => Err(value),
        }
    }
}

/// The awaitable handle returned by [`Runner::enqueue`] — a customized
/// `JoinHandle` for [`Task::Output`].
///
/// `enqueue` `Err` means the task was not accepted (shutdown, Q3). `Ok`
/// is this handle: await it for `run()`'s value (`Ok(output)`), or drop
/// it to fire-and-forget. If the task is dropped before `send` (panic or
/// shutdown), the handle yields [`Error::Dropped`].
pub struct Completion<T> {
    rx: oneshot::Receiver<T>,
}

impl<T> Completion<T> {
    /// Pair the runner's [`Reply`] with the handle `enqueue` returns.
    pub fn pair() -> (Reply<T>, Self) {
        let (tx, rx) = oneshot::channel();
        (Reply { tx: Some(tx) }, Self { rx })
    }
}

impl<T> fmt::Debug for Completion<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Completion")
    }
}

impl<T> Future for Completion<T> {
    type Output = Result<T, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(value)) => Poll::Ready(Ok(value)),
            Poll::Ready(Err(_canceled)) => Poll::Ready(Err(Error::Dropped)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A [`RunOutput`] [`Result::Err`] that the concurrent runtime can log
/// (R7/R8). The blanket impl covers every [`Result`] task output whose
/// error is viewable as a [`StdError`]: [`RunOutput`]'s own
/// `Box<dyn StdError + Send + Sync>` (via std's `AsRef<T> for Box<T>`)
/// and the tinio-fs task errors (`Result<(), Error>` and `etag::Result`,
/// via their `AsRef<dyn StdError + Send + Sync>` impl) — the original
/// error type is kept, never stringified (P7).
pub trait Outcome: Send + 'static {
    /// The error when `self` is a failed `Result`; `None` otherwise.
    fn failure(&self) -> Option<&(dyn StdError + Send + Sync)>;
}

impl<T, E> Outcome for Result<T, E>
where
    T: Send + 'static,
    E: AsRef<dyn StdError + Send + Sync> + Send + Sync + 'static,
{
    fn failure(&self) -> Option<&(dyn StdError + Send + Sync)> {
        match self {
            Ok(_) => None,
            Err(err) => Some(err.as_ref()),
        }
    }
}

/// A unit of pipeline work: a customized [`Future`] labeled by
/// [`Task::kind`]. [`Task::Output`] is any [`Send`] value — the runner
/// [`Reply::send`]s it to [`Completion`]. Concrete kinds are
/// implementation-defined (tinio-fs: `"etag"`, `"meta_write"`); the
/// pipeline treats every task identically.
///
/// # Running a task
///
/// A task runs **at most once**: the runner calls [`Task::run`] (taking
/// `&mut self`), sends the value to [`Completion`], then drops the box —
/// the contract has no re-run path.
///
/// # Panics
///
/// `run()` MUST NOT panic (R6): the concurrent runtime treats a panicking
/// task as failed and keeps its workers alive, and [`InlineRunner`] passes
/// the panic through to the caller.
///
/// # Examples
///
/// ```rust
/// use async_trait::async_trait;
/// use tinio_core::pipeline::Task;
///
/// struct Noop;
///
/// #[async_trait]
/// impl Task for Noop {
///     type Output = ();
///
///     fn kind(&self) -> &'static str {
///         "noop"
///     }
///
///     async fn run(&mut self) {}
/// }
/// ```
#[async_trait]
pub trait Task: Send + 'static {
    /// The value [`Task::run`] produces and [`Completion`] delivers.
    type Output: Send + 'static;

    /// The task-kind label, for diagnostics (e.g. `"etag"`).
    fn kind(&self) -> &'static str;

    /// Execute the task. Called at most once by the runner, which drops the
    /// box afterwards (the module docs explain the `&mut self` receiver).
    /// The return value is sent to [`Completion`].
    async fn run(&mut self) -> Self::Output;
}

/// The task-execution contract: a bounded queue with workers behind it
/// (tinio-server implements the concurrent runtime; [`InlineRunner`] is the
/// synchronous reference implementation, Q1). Parameterized by the
/// [`Task::Output`] this runner accepts (default [`RunOutput`]).
#[async_trait]
pub trait Runner<O: Send + 'static = RunOutput>: Send + Sync + 'static {
    /// Enqueue a task for execution (`spawn`).
    ///
    /// Bounded: waits while the queue is full (backpressure), so a producer
    /// can never outrun the workers. Returns `Err` when the task was *not*
    /// accepted — [`Error::ShutDown`] after [`Self::shutdown`] (Q3). `Ok` is the
    /// [`Completion`] handle: await it for `run()`'s value (list), or
    /// drop it to fire-and-forget (scanner). The runner
    /// [`Reply::send`]s `run()`'s return value; the concurrent runtime
    /// logs a [`Result::Err`] so a dropped handle is still observed.
    async fn enqueue(&self, task: Box<dyn Task<Output = O>>) -> Result<Completion<O>, Error>;

    /// Shut the runner down: queued tasks are dropped, in-flight tasks run
    /// to completion, and subsequent [`Self::enqueue`] calls return `Err`
    /// (Q3). Idempotent.
    fn shutdown(&self);

    /// A snapshot of the runner's counters (pipeline-spec.md §4).
    fn stats(&self) -> Stats;
}

/// A snapshot of a [`Runner`]'s counters (pipeline-spec.md §4): queue depth,
/// in-flight tasks, and busy workers. [`InlineRunner`] returns all zeros —
/// it executes synchronously, so nothing is ever queued or in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Stats {
    /// Tasks currently queued, waiting for a worker.
    pub queue_depth: u64,
    /// Tasks currently executing.
    pub in_flight: u64,
    /// Workers currently busy with a task.
    pub busy_workers: u64,
}

/// The default worker count of the IO pipeline (`[pipeline.io] workers`,
/// pipeline-spec.md §3.3). Shared by the config schema and the tinio-server
/// runtime, so the two defaults cannot drift.
pub const DEFAULT_IO_WORKERS: u8 = 2;

/// The validation bounds of the IO pipeline worker count (1..=64).
pub const IO_WORKERS_MIN: u8 = 1;
pub const IO_WORKERS_MAX: u8 = 64;

/// The default worker count of the removal pipeline (`[pipeline.remove]
/// workers`) — one worker: tombstone tree deletion is background cleanup
/// that must not starve ETag compute (the IO pipeline's job), and the
/// default stays out of the IO workers' way (D-A). The worker range is
/// the IO pipeline's own ([`IO_WORKERS_MIN`]/[`IO_WORKERS_MAX`] — only
/// the default differs).
pub const DEFAULT_REMOVE_WORKERS: u8 = 1;

/// The default worker count of the DB write pipeline (`[pipeline.db]
/// workers`) — redb is a single-writer store, so more than one worker adds
/// no write throughput (pipeline-spec.md §3.1).
pub const DEFAULT_DB_WORKERS: u8 = 1;

/// The validation bounds of the DB pipeline worker count (1..=4).
pub const DB_WORKERS_MIN: u8 = 1;
pub const DB_WORKERS_MAX: u8 = 4;

/// The default bounded-queue capacity of both pipelines (`[pipeline.*]
/// capacity`, pipeline-spec.md Q7).
pub const DEFAULT_CAPACITY: u32 = 1024;

/// The validation bounds of the pipeline queue capacity (1..=65536).
pub const CAPACITY_MIN: u32 = 1;
pub const CAPACITY_MAX: u32 = 65536;

/// The reference [`Runner`] implementation: executes every task inline on
/// the caller's thread (Q1, pipeline-spec.md §7). Public by design — doctor,
/// benches, examples, and unit tests pass it wherever `FsOptions` requires a
/// pipeline (P4); the server runtime implements the same contract with real
/// workers.
///
/// Behavior:
///
/// - `enqueue` runs `task.run().await` before returning (synchronous) and
///   [`Reply::send`]s the result to the returned [`Completion`];
/// - panics pass through uncaught (R6);
/// - after `shutdown()`, `enqueue` returns `Err` (Q3);
/// - `stats()` is always zeros.
///
/// # Examples
///
/// ```rust
/// use async_trait::async_trait;
/// use tinio_core::pipeline::{InlineRunner, Runner, Task};
/// use tokio::runtime::Runtime;
///
/// struct Echo(&'static str);
///
/// #[async_trait]
/// impl Task for Echo {
///     type Output = ();
///
///     fn kind(&self) -> &'static str {
///         "echo"
///     }
///
///     async fn run(&mut self) {}
/// }
///
/// let runner = InlineRunner::default();
/// let rt = Runtime::new().unwrap();
/// rt.block_on(async {
///     runner
///         .enqueue(Box::new(Echo("hi")))
///         .await
///         .unwrap()
///         .await
///         .unwrap();
/// });
/// ```
#[derive(Default)]
pub struct InlineRunner {
    shut_down: AtomicBool,
}

impl InlineRunner {
    /// Shut the runner down (Q3). Inherent so `shutdown` is not ambiguous
    /// under the blanket [`Runner<O>`] impl.
    pub fn shutdown(&self) {
        self.shut_down.store(true, Ordering::Relaxed);
    }

    /// Always zeros — the inline runner never queues.
    pub fn stats(&self) -> Stats {
        Stats::default()
    }
}

#[async_trait]
impl<O> Runner<O> for InlineRunner
where
    O: Send + 'static,
{
    async fn enqueue(&self, mut task: Box<dyn Task<Output = O>>) -> Result<Completion<O>, Error> {
        if self.shut_down.load(Ordering::Relaxed) {
            return Err(Error::ShutDown);
        }
        let (reply, done) = Completion::pair();
        // `run` before returning: the handle is already ready when enqueue
        // returns. The receiver is held here, so `send` cannot miss it.
        let result = task.run().await;
        if reply.send(result).is_err() {
            // F23: the completion was dropped before the result was
            // taken (fire-and-forget) — the task's failure is invisible
            // under the reference runner unless logged. The server
            // runtime reports through its `Outcome` warn (R8); the
            // inline runner matches it here (the value is not logged —
            // `O` has no `Debug` bound), so offline contexts (tests,
            // benches, doctor) at least observe that a task failed.
            tracing::warn!(
                task = task.kind(),
                "inline task result dropped before delivery"
            );
        }
        Ok(done)
    }

    fn shutdown(&self) {
        InlineRunner::shutdown(self);
    }

    fn stats(&self) -> Stats {
        InlineRunner::stats(self)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        mem::replace,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio::{runtime::Runtime, sync::mpsc, time::timeout};

    use super::*;

    /// A stub task that records every `run()` call on a shared counter.
    struct RecordingTask {
        runs: Arc<AtomicUsize>,
    }

    impl RecordingTask {
        fn new() -> (Arc<AtomicUsize>, Self) {
            let runs = Arc::new(AtomicUsize::new(0));
            (runs.clone(), Self { runs })
        }
    }

    #[async_trait]
    impl Task for RecordingTask {
        type Output = RunOutput;

        fn kind(&self) -> &'static str {
            "recording"
        }

        async fn run(&mut self) -> Result<(), Box<dyn StdError + Send + Sync>> {
            self.runs.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    /// A task with a `Send`-but-not-`Sync` payload: `Box<dyn Task<_>>` must
    /// accept it, since a task is never shared.
    struct NotSyncTask(Cell<u32>);

    #[async_trait]
    impl Task for NotSyncTask {
        type Output = RunOutput;

        fn kind(&self) -> &'static str {
            "not_sync"
        }

        async fn run(&mut self) -> Result<(), Box<dyn StdError + Send + Sync>> {
            let _ = self.0.get();
            Ok(())
        }
    }

    /// A task that reports an outcome through the run [`Completion`] (R8).
    struct ReportTask {
        outcome: Result<(), Box<dyn StdError + Send + Sync>>,
    }

    #[async_trait]
    impl Task for ReportTask {
        type Output = RunOutput;

        fn kind(&self) -> &'static str {
            "report"
        }

        async fn run(&mut self) -> Result<(), Box<dyn StdError + Send + Sync>> {
            replace(&mut self.outcome, Ok(()))
        }
    }

    /// A task that panics: `InlineRunner` must pass the panic through (R6).
    struct PanicTask;

    #[async_trait]
    impl Task for PanicTask {
        type Output = RunOutput;

        fn kind(&self) -> &'static str {
            "panic"
        }

        async fn run(&mut self) -> Result<(), Box<dyn StdError + Send + Sync>> {
            panic!("task panicked (R6)");
        }
    }

    /// A bounded stub [`Runner`] over a capacity-1 channel: `enqueue` blocks
    /// while the queue is full — the backpressure half of the contract. The
    /// test "pauses" it by not draining the queue. `shutdown` is a no-op
    /// (shutdown semantics are exercised on [`InlineRunner`]).
    struct PausableStubRunner {
        queue: mpsc::Sender<Box<dyn Task<Output = RunOutput>>>,
    }

    #[async_trait]
    impl Runner for PausableStubRunner {
        async fn enqueue(
            &self,
            task: Box<dyn Task<Output = RunOutput>>,
        ) -> Result<Completion<RunOutput>, Error> {
            self.queue.send(task).await.map_err(|_| Error::ShutDown)?;
            // The stub never runs the task; the handle is canceled if awaited.
            let (_reply, done) = Completion::pair();
            Ok(done)
        }

        fn shutdown(&self) {}

        fn stats(&self) -> Stats {
            Stats::default()
        }
    }

    #[test]
    fn contract_types_are_send_sync() {
        fn assert_send_sync<T: Send + Sync + 'static>() {}
        fn assert_send<T: Send + 'static>() {}

        assert_send_sync::<InlineRunner>();
        assert_send_sync::<Stats>();
        assert_send_sync::<Error>();
        assert_send_sync::<Arc<dyn Runner>>();
        assert_send::<Box<dyn Task<Output = RunOutput>>>();
        assert_send::<Completion<RunOutput>>();

        // `Task` must not require `Sync`: channel senders are `Send` but
        // not `Sync`, and a task is never shared.
        let task: Box<dyn Task<Output = RunOutput>> = Box::new(NotSyncTask(Cell::new(0)));
        assert_eq!(task.kind(), "not_sync");
    }

    #[tokio::test]
    async fn not_sync_task_runs_through_the_inline_runner() {
        // The compile-time `Send`-but-not-`Sync` task is also runnable
        // (its run body is otherwise never executed). The inline runner
        // is runtime-agnostic — a plain `#[tokio::test]` runtime (no
        // `block_on` wrapper, per CLAUDE.md).
        let runner = InlineRunner::default();
        let task: Box<dyn Task<Output = RunOutput>> = Box::new(NotSyncTask(Cell::new(1)));
        assert_eq!(task.kind(), "not_sync");
        runner.enqueue(task).await.unwrap().await.unwrap().unwrap();
    }

    #[test]
    fn enqueue_blocks_while_the_queue_is_full() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (queue, mut rx) = mpsc::channel::<Box<dyn Task<Output = RunOutput>>>(1);
            let runner = Arc::new(PausableStubRunner { queue });

            // Fill the single slot.
            let (_, task) = RecordingTask::new();
            runner.enqueue(Box::new(task)).await.unwrap();

            // A second enqueue must wait for capacity (backpressure).
            let (_, task) = RecordingTask::new();
            let runner2 = Arc::clone(&runner);
            let mut blocked = tokio::spawn(async move { runner2.enqueue(Box::new(task)).await });
            assert!(
                timeout(Duration::from_millis(50), &mut blocked)
                    .await
                    .is_err(),
                "enqueue must block while the queue is full"
            );

            // Drain one slot: the blocked enqueue completes and its task
            // becomes receivable.
            rx.recv().await.unwrap();
            blocked.await.unwrap().unwrap();
            assert!(rx.recv().await.is_some());
            // The stub's contract surface: no-op shutdown + zero stats.
            runner.shutdown();
            assert_eq!(runner.stats(), Stats::default());
        });
    }

    #[test]
    fn inline_runner_executes_tasks_synchronously() {
        let runner = InlineRunner::default();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (runs, task) = RecordingTask::new();
            let task = Box::new(task);
            assert_eq!(task.kind(), "recording");
            runner.enqueue(task).await.unwrap();
            assert_eq!(
                runs.load(Ordering::Relaxed),
                1,
                "run() must complete exactly once before enqueue returns"
            );
            assert_eq!(runner.stats(), Stats::default(), "stats stay at zeros");
        });
    }

    #[test]
    fn inline_runner_rejects_after_shutdown() {
        let runner = InlineRunner::default();
        runner.shutdown();
        runner.shutdown(); // idempotent

        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (_, task) = RecordingTask::new();
            let err = runner.enqueue(Box::new(task)).await.unwrap_err();
            assert_eq!(
                err,
                Error::ShutDown,
                "enqueue after shutdown must error (Q3)"
            );
        });
    }

    #[test]
    #[should_panic(expected = "task panicked (R6)")]
    fn inline_runner_passes_panics_through() {
        let runner = InlineRunner::default();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            assert_eq!(PanicTask.kind(), "panic");
            runner.enqueue(Box::new(PanicTask)).await.unwrap();
        });
    }

    #[test]
    fn enqueue_returns_a_completion_that_carries_run_ok() {
        let runner = InlineRunner::default();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            assert_eq!(ReportTask { outcome: Ok(()) }.kind(), "report");
            runner
                .enqueue(Box::new(ReportTask { outcome: Ok(()) }))
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap();
        });
    }

    #[test]
    fn enqueue_returns_a_completion_that_carries_run_err() {
        // enqueue Ok = accepted; the handle carries run()'s Err (list awaits
        // it; scanner drops it and the concurrent runtime logs instead).
        let runner = InlineRunner::default();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let err = runner
                .enqueue(Box::new(ReportTask {
                    outcome: Err("simulated failure".into()),
                }))
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(err.to_string(), "simulated failure");
        });
    }

    #[test]
    fn enqueue_sends_an_arbitrary_send_output() {
        struct Answer;

        #[async_trait]
        impl Task for Answer {
            type Output = u32;

            fn kind(&self) -> &'static str {
                "answer"
            }

            async fn run(&mut self) -> u32 {
                42
            }
        }

        let runner = InlineRunner::default();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            assert_eq!(Answer.kind(), "answer");
            let n = runner
                .enqueue(Box::new(Answer))
                .await
                .unwrap()
                .await
                .unwrap();
            assert_eq!(n, 42);
        });
    }

    #[test]
    fn dropped_completion_resolves_dropped() {
        let (reply, done) = Completion::<()>::pair();
        drop(reply);
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let err = done.await.unwrap_err();
            assert_eq!(err, Error::Dropped);
            assert_eq!(err.to_string(), "pipeline task was dropped");
        });
    }

    #[test]
    fn reply_none_errors_and_completion_debug_formats() {
        // The no-channel `Reply::none()` form returns the value untouched
        // (a closed channel would also error, but through the oneshot
        // path).
        assert_eq!(Reply::none().send(7), Err(7));

        // The custom `Debug` is a placeholder — the channel payload is
        // never formatted (it has no `Debug` bound).
        let (reply, done) = Completion::<u32>::pair();
        drop(reply);
        assert_eq!(format!("{done:?}"), "Completion");
    }

    #[test]
    fn inline_runner_delegates_shutdown_and_stats_through_the_trait_object() {
        // The contract surface (`Arc<dyn Runner>`) must route shutdown
        // and stats to the inline implementation — the server runtime and
        // the fs backend hold the runner behind the trait object.
        let runner: Arc<dyn Runner> = Arc::new(InlineRunner::default());
        runner.shutdown();
        runner.shutdown(); // idempotent through the delegation
        assert_eq!(runner.stats(), Stats::default());

        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (_, task) = RecordingTask::new();
            let err = runner.enqueue(Box::new(task)).await.unwrap_err();
            assert_eq!(err, Error::ShutDown);
        });
    }
}
