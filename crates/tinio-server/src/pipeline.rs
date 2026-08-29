//! The concurrent pipeline runtime: the real implementation of the
//! `tinio-core` [`pipeline::Runner`] contract (pipeline-spec.md §3.1, §3.5).
//!
//! Each pipeline is a bounded mpsc queue plus worker threads on its **own**
//! tokio runtime (`worker_threads = workers`, `thread_name
//! "tinio-pipeline-io"` / `"tinio-pipeline-db"`). Tasks run directly on the
//! worker threads (blocking-task model, Q4), so the configured thread
//! priority applies exactly to the threads doing the work.
//!
//! Two instances are built from the config section (pipeline-spec.md §3.3):
//! the IO pipeline (`[pipeline.io]`, default 2 workers) for CPU/IO-bound
//! work and the DB write pipeline (`[pipeline.db]`, default 1 worker — redb
//! is single-writer) for batched meta writes. [`Pipelines`] owns both and
//! shuts them down in the required order (R5): **IO first, DB last**, so
//! in-flight list batches queued in the DB pipeline can still drain.
//!
//! # Semantics (contract)
//!
//! - `enqueue` is bounded: it waits while the queue is full (backpressure).
//!   `Ok` is the [`Completion`] handle (`spawn` / `JoinHandle`);
//!   `Err` means the task was not accepted (Q3).
//! - After `shutdown()`, queued tasks are dropped, in-flight tasks run to
//!   completion, and `enqueue` returns `Err` (Q3).
//! - The runner [`Reply::send`]s `run()`'s result to the handle.
//!   `run()`'s `Err` is also logged as a warn so a dropped handle (scanner)
//!   is still observed, and the worker keeps consuming.
//! - `run()` must not panic (R6); the worker wraps it in `catch_unwind`
//!   anyway — a panic is a strong warn and the worker stays alive.
//! - The DB write pipeline tracks consecutive `run()` failures and escalates
//!   to a strong warn at [`CONSECUTIVE_FAILURE_ESCALATION`] ("likely
//!   systemic", R7); it never stops consuming and resets on success.

use std::any::Any;
use std::ops::Deref;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::FutureExt;
use thread_priority::{ThreadPriority, ThreadPriorityValue};
use tinio_config::pipeline as pipeline_config;
use tinio_core::pipeline::{self, Completion, Outcome, Reply, RunOutput, Runner, Stats, Task};
use tokio::sync::{Mutex as TokioMutex, mpsc, watch};

use crate::Error;

/// One queued job: the task plus the reply the worker sends after `run()`
/// (the other end is [`Completion`]).
type Job<O> = (Box<dyn Task<Output = O>>, Reply<O>);

/// The consecutive `run()` failure count at which the DB write pipeline
/// escalates its warning (R7, pipeline-spec.md §7).
pub const CONSECUTIVE_FAILURE_ESCALATION: u32 = 10;

/// Construction parameters for one pipeline runtime. Private: the
/// sanctioned construction path is [`Pipelines::build`], which receives
/// config-validated values (worker counts and capacities are range-checked
/// by the `[pipeline.*]` schema).
#[derive(Debug, Clone, Copy)]
struct PipelineSpec {
    /// The pipeline label (`"io"` / `"db"`), used in thread names and logs.
    pub kind: &'static str,
    /// Worker-thread count (blocking-task model: one task per worker).
    pub workers: u8,
    /// Bounded queue capacity (the backpressure bound).
    pub capacity: usize,
    /// Thread priority applied on each worker thread (`None` = OS default).
    pub priority: Option<ThreadPriority>,
    /// Track consecutive `run()` failures with escalated warnings (R7).
    pub track_consecutive_failures: bool,
}

/// One pipeline runtime (see the module docs). Parameterized by the
/// [`Task::Output`] it accepts (default [`RunOutput`]).
///
/// The `O: Outcome` bound lives on the impl blocks, not the struct: with
/// the blanket `Result` [`Outcome`] impl the struct-level bound would
/// fail well-formedness (the blanket also covers unsized errors).
pub struct Pipeline<O = RunOutput> {
    inner: Arc<PipelineInner<O>>,
    /// The runtime hosting the workers. Owned outside the shared `inner` so
    /// the last handle can move it to a detached thread on drop — tokio
    /// forbids dropping a runtime from an async context (a blocking drop),
    /// and the server shuts the pipelines down from its own runtime.
    runtime: Option<tokio::runtime::Runtime>,
}

struct PipelineInner<O> {
    kind: &'static str,
    /// The bounded queue sender; `None` once shut down (Q3).
    queue: Mutex<Option<mpsc::Sender<Job<O>>>>,
    /// A retained sender clone for the post-shutdown depth read (item 4,
    /// data-path review 2026-08-29): `stats()` derives the queue depth
    /// from the channel itself (`max_capacity() - capacity()`), and this
    /// clone keeps the channel readable after `shutdown()` takes and
    /// drops the enqueue sender.
    sender: mpsc::Sender<Job<O>>,
    /// Signals the workers to stop receiving (queued tasks are dropped).
    shutdown_tx: watch::Sender<bool>,
    /// Fast-path rejection for `enqueue` after shutdown.
    shut_down: AtomicBool,
    counters: Arc<Counters>,
    /// The worker handles, retained for the drain-await (item 6c): taken
    /// once by [`Pipeline::drain`], so the handles outlive the spawn and
    /// the in-flight drain is awaitable instead of being dropped at the
    /// spawn site.
    workers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// The worker counters behind [`Runner::stats`] (pipeline-spec.md §4) —
/// the in-flight and busy-worker groups only. The queue depth is NOT a
/// counter (item 4, data-path review 2026-08-29): it is read from the
/// channel itself in `stats()`, which removes the counted
/// increment/decrement race class entirely. Each counter gets its own
/// 64-byte cache line (item 6b, data-path review 2026-08-27; final fix
/// round): the workers hammer both atomics around `run()` — a shared
/// line would false-share them (the counters live behind an `Arc` on the
/// heap; the per-counter `repr(align(64))` survives the allocation).
#[derive(Default)]
/// F28: `busy_workers` was a second counter incremented/decremented in
/// strict lockstep with `in_flight` (nothing else touched either) — a
/// gauge that always equals `in_flight` by construction, at the price of
/// a second 64-byte-aligned atomic per task. The single counter serves
/// both [`Stats`] fields.
struct Counters {
    in_flight: AlignedCounter,
}

/// An [`AtomicU64`] on its own 64-byte cache line — the [`Counters`]
/// layout unit. Transparent access via [`Deref`], so callers keep using
/// the atomic methods directly.
#[derive(Default)]
#[repr(align(64))]
struct AlignedCounter(AtomicU64);

impl Deref for AlignedCounter {
    type Target = AtomicU64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// The two server pipelines with their construction-time and shutdown
/// wiring (pipeline-spec.md §3.3, R5).
///
/// Parameterized by each pipeline's [`Task::Output`] (default
/// [`RunOutput`]): `FsOptions` types the IO pipeline to the
/// tinio-fs `etag::Result` and the DB pipeline to `Result<(), tinio_fs::Error>`
/// (P4/P7), and the server builds `Pipelines` with those outputs — the
/// two fields are independent, so the pipelines can carry different task
/// types. The `Outcome` bounds live on the impl blocks (see
/// [`Pipeline`]).
pub struct Pipelines<Oio = RunOutput, Odb = RunOutput> {
    io: Arc<Pipeline<Oio>>,
    db: Arc<Pipeline<Odb>>,
}

impl<O> Pipeline<O>
where
    O: Outcome,
{
    /// Build one pipeline runtime: its own tokio runtime with
    /// `spec.workers` worker threads named `tinio-pipeline-<kind>`, each
    /// applying `spec.priority` on start (failure degrades with a warn).
    fn new(spec: PipelineSpec) -> Result<Self, Error> {
        let (queue, rx) = mpsc::channel::<Job<O>>(spec.capacity);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let rx = Arc::new(TokioMutex::new(rx));
        let counters = Arc::new(Counters::default());

        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .worker_threads(usize::from(spec.workers))
            .thread_name(format!("tinio-pipeline-{}", spec.kind));
        // No `enable_all()` (data-path review 2026-08-29, finding 7): the
        // workers only poll the mpsc queue and the watch channel — a
        // timer and IO driver per runtime would be enabled for nothing.
        let priority = spec.priority;
        builder.on_thread_start(move || apply_thread_priority(priority));
        let runtime = builder.build()?;

        // The worker handles are retained (item 6c) — [`Pipeline::drain`]
        // awaits them after shutdown.
        let workers = (0..spec.workers)
            .map(|_| {
                runtime.spawn(worker_loop(
                    spec.kind,
                    spec.track_consecutive_failures,
                    Arc::clone(&counters),
                    Arc::clone(&rx),
                    shutdown_rx.clone(),
                ))
            })
            .collect::<Vec<_>>();

        let inner = Arc::new(PipelineInner {
            kind: spec.kind,
            queue: Mutex::new(Some(queue.clone())),
            sender: queue,
            shutdown_tx,
            shut_down: AtomicBool::new(false),
            counters,
            workers: Mutex::new(workers),
        });
        Ok(Self {
            inner,
            runtime: Some(runtime),
        })
    }

    /// The pipeline label (`"io"` / `"db"`).
    pub fn kind(&self) -> &'static str {
        self.inner.kind
    }
}

impl<O> Pipeline<O> {
    /// The shutdown sequence (Q3), kind-agnostic — the [`Runner`] method
    /// wraps it, and the `Drop` impl calls it without the `O: Outcome`
    /// bound (the struct carries no bounds; see the struct docs).
    fn shutdown_inner(&self) {
        if self.inner.shut_down.swap(true, Ordering::Relaxed) {
            return; // idempotent (contract)
        }
        // Workers stop receiving (queued tasks are dropped with the
        // receiver when they exit, Q3); in-flight tasks run to completion.
        let _ = self.inner.shutdown_tx.send(true);
        // Drop the sender: subsequent enqueues error (Q3). No gauge
        // reset is owed here — `stats()` reads the depth from the
        // channel itself (item 4), and the channel reads 0 once the
        // workers drop the receiver (all permits released).
        self.inner.queue.lock().unwrap().take();
    }

    /// Await the workers' exit — every in-flight task has completed by
    /// the time this returns (Q3: in-flight tasks run to completion at
    /// shutdown; awaiting them is observability, not new semantics —
    /// item 6c). Call after [`Self::shutdown`] (or [`Runner::shutdown`]):
    /// an unshut-down pipeline never signals its workers, so the await
    /// would hang. Idempotent: the retained handles are taken once;
    /// later calls return immediately.
    pub async fn drain(&self) {
        let handles = {
            let mut workers = self.inner.workers.lock().unwrap();
            std::mem::take(&mut *workers)
        };
        for handle in handles {
            // The worker loops are infallible; a JoinError would mean a
            // panicking worker — nothing to propagate to a caller.
            let _ = handle.await;
        }
    }
}

impl<O> Drop for Pipeline<O> {
    fn drop(&mut self) {
        // Shut the workers down so the runtime can drop without waiting on
        // a pipeline that was never explicitly shut down.
        self.shutdown_inner();
        // Drop the runtime on a detached thread: the workers have already
        // exited, so this completes almost immediately — but the drop
        // itself blocks the thread, which tokio forbids from an async
        // context (the server shuts the pipelines down from its own
        // runtime).
        if let Some(runtime) = self.runtime.take() {
            std::thread::spawn(move || drop(runtime));
        }
    }
}

#[async_trait]
impl<O> Runner<O> for Pipeline<O>
where
    O: Outcome,
{
    async fn enqueue(
        &self,
        task: Box<dyn Task<Output = O>>,
    ) -> Result<Completion<O>, pipeline::Error> {
        if self.inner.shut_down.load(Ordering::Relaxed) {
            return Err(pipeline::Error::ShutDown);
        }
        let (reply, done) = Completion::pair();
        // Clone the sender under the lock, then send without holding it.
        let sender = self
            .inner
            .queue
            .lock()
            .unwrap()
            .clone()
            .ok_or(pipeline::Error::ShutDown)?;
        // No depth counting here — `stats()` reads the depth from the
        // channel itself (`max_capacity() - capacity()`, item 4,
        // data-path review 2026-08-29): a task still blocked in `send`
        // is in the CALLER, not the queue (item 6a), and a fast worker's
        // dequeue is exactly reflected by the channel's capacity — the
        // counted increment/decrement race (a decrement consumed before
        // the producer's increment landed, drifting the gauge upward
        // permanently) cannot exist.
        if sender.send((task, reply)).await.is_err() {
            // The channel closed under us (shutdown race) — not accepted.
            return Err(pipeline::Error::ShutDown);
        }
        // F08: a shutdown that fired while the send was in flight would
        // otherwise hand back Ok(done) for a task the workers' biased
        // select then drops unrun (the retained sender keeps the channel
        // open) — a spurious mid-shutdown failure instead of the Q3
        // contract's Err(ShutDown). Re-check after the send: on shutdown
        // the task is either run (harmless — the completion dies with
        // this Err) or dropped unrun (Q3 semantics).
        if self.inner.shut_down.load(Ordering::Relaxed) {
            return Err(pipeline::Error::ShutDown);
        }
        Ok(done)
    }

    fn shutdown(&self) {
        self.shutdown_inner();
    }

    fn stats(&self) -> Stats {
        // The depth is read from the channel itself (item 4, data-path
        // review 2026-08-29) — `max_capacity() - capacity()` is the
        // exact number of tasks in the queue, with no counted
        // increment/decrement to drift. The retained `sender` clone
        // keeps the read valid after shutdown; once the workers exit,
        // dropping the receiver releases all permits, so the depth reads
        // 0.
        let queue_depth = self.inner.sender.max_capacity() - self.inner.sender.capacity();
        let in_flight = self.inner.counters.in_flight.load(Ordering::Relaxed);
        // F28: `busy_workers` equals `in_flight` by construction — both
        // Stats fields come from the single remaining counter.
        Stats {
            queue_depth: queue_depth as u64,
            in_flight,
            busy_workers: in_flight,
        }
    }
}

impl<Oio, Odb> Pipelines<Oio, Odb>
where
    Oio: Outcome,
    Odb: Outcome,
{
    /// Build the IO and DB pipelines from the config section (`[pipeline]`;
    /// an absent top-level section resolves to defaults before this call).
    /// The output types are inferred from the construction site (the
    /// server passes the tinio-fs task outputs when wiring `FsOptions`).
    pub fn build(config: &pipeline_config::Config) -> Result<Self, Error> {
        let io = Pipeline::new(PipelineSpec {
            kind: "io",
            workers: config.io.workers,
            capacity: config.io.capacity as usize,
            priority: thread_priority(config.io.priority),
            track_consecutive_failures: false,
        })?;
        let db = Pipeline::new(PipelineSpec {
            kind: "db",
            workers: config.db.workers,
            capacity: config.db.capacity as usize,
            priority: thread_priority(config.db.priority),
            track_consecutive_failures: true,
        })?;
        Ok(Self {
            io: Arc::new(io),
            db: Arc::new(db),
        })
    }

    /// The IO pipeline (ETag computation: bounded file reads + hashing).
    pub fn io(&self) -> Arc<Pipeline<Oio>> {
        Arc::clone(&self.io)
    }

    /// The DB write pipeline (batched meta writes).
    pub fn db(&self) -> Arc<Pipeline<Odb>> {
        Arc::clone(&self.db)
    }

    /// Shut both pipelines down in the required order (R5): **IO first, DB
    /// last** — in-flight list batches queued in the DB pipeline can still
    /// drain. Idempotent.
    pub fn shutdown(&self) {
        self.io.shutdown_inner();
        self.db.shutdown_inner();
    }

    /// Await both pipelines' workers after [`Self::shutdown`] — the R5
    /// order again (IO first, DB last), so in-flight list batches queued
    /// in the DB pipeline drain first. Observability (item 6c): the Q3
    /// semantics are unchanged — this awaits what shutdown already
    /// guarantees.
    pub async fn drain(&self) {
        self.io.drain().await;
        self.db.drain().await;
    }
}

impl<Oio, Odb> Drop for Pipelines<Oio, Odb> {
    fn drop(&mut self) {
        // Safety net with the same order — the fields drop in declaration
        // order, io before db (R5). Inline: `shutdown` needs the Outcome
        // bounds, which the Drop impl cannot carry.
        self.io.shutdown_inner();
        self.db.shutdown_inner();
    }
}

/// One worker loop: receives tasks off the bounded queue and runs them.
///
/// Shutdown (the watch signal, or a closed channel) exits the loop and
/// drops the shared receiver — any still-queued tasks are dropped with it
/// (Q3). In-flight tasks run to completion: the shutdown signal is only
/// observed between tasks (R5). A task that was *dequeued* before the
/// signal landed is dropped, not run — the biased select wins when the
/// signal and a task are ready at the same poll, and the post-receive
/// flag check closes the window where the signal fired after the receive
/// resolved.
async fn worker_loop<O>(
    kind: &'static str,
    track_consecutive_failures: bool,
    counters: Arc<Counters>,
    rx: Arc<TokioMutex<mpsc::Receiver<Job<O>>>>,
    mut shutdown_rx: watch::Receiver<bool>,
) where
    O: Outcome,
{
    let mut consecutive_failures: u32 = 0;
    loop {
        tokio::select! {
            biased;
            // The shutdown signal wins over a queued task — queued tasks
            // are dropped at shutdown (Q3).
            _ = shutdown_rx.changed() => break,
            job = receive(&rx) => {
                let Some((mut task, reply)) = job else { break }; // channel closed
                // A shutdown that fired while this worker was parked in
                // receive() drops the task instead of running it (Q3).
                if *shutdown_rx.borrow() {
                    break;
                }
                run_one(
                    kind,
                    track_consecutive_failures,
                    &counters,
                    &mut consecutive_failures,
                    &mut task,
                    reply,
                )
                .await;
            }
        }
    }
}

/// Pull one task off the shared receiver. `mpsc::Receiver` is
/// single-consumer, so the workers share it through `Arc<Mutex<…>>` — the
/// canonical no-extra-dependency pattern for a multi-worker queue (a
/// dedicated dispatcher task would add a hop plus its own shutdown
/// plumbing for no real gain: the dequeue is O(1), and dispatch is
/// bounded by worker availability anyway). Tokio's mutex is FIFO-fair,
/// so idle workers take turns; the guard lives only for the
/// `recv().await` in this statement and is released before the job is
/// returned — task execution never holds the lock.
async fn receive<O>(rx: &TokioMutex<mpsc::Receiver<Job<O>>>) -> Option<Job<O>> {
    rx.lock().await.recv().await
}

/// Run one task: counters up, `catch_unwind` around the ENTIRE per-task
/// step (R6 — `kind()`, `run()`, the failure check, and `reply.send` all
/// run inside the catch, F09: a panic in any of them otherwise escapes
/// the worker task and kills it permanently, with no respawn), auto
/// [`Reply::send`] of `run()`'s value to [`Completion`], run() failure
/// and consecutive-failure handling (R7/R8), counters down (a guard —
/// every exit path, panic included, decrements them, F09).
async fn run_one<O>(
    kind: &'static str,
    track_consecutive_failures: bool,
    counters: &Counters,
    consecutive_failures: &mut u32,
    task: &mut Box<dyn Task<Output = O>>,
    reply: Reply<O>,
) where
    O: Outcome,
{
    counters.in_flight.fetch_add(1, Ordering::Relaxed);
    // F09: the counters are decremented on EVERY exit path — a panic in
    // any step of the task must never leave in_flight stuck at its
    // incremented value.
    struct CounterGuard<'a>(&'a Counters);
    impl Drop for CounterGuard<'_> {
        fn drop(&mut self) {
            self.0.in_flight.fetch_sub(1, Ordering::Relaxed);
        }
    }
    let _guard = CounterGuard(counters);
    // `kind()` itself is caught separately (F09): a panicking `kind()` —
    // a task-implementation bug — must not kill the worker; the task
    // cannot even be identified, so the reply is cancelled (Dropped).
    let task_kind = match std::panic::catch_unwind(AssertUnwindSafe(|| task.kind())) {
        Ok(task_kind) => task_kind,
        Err(payload) => {
            *consecutive_failures += 1;
            tracing::warn!(
                pipeline = kind,
                panic = %panic_message(&*payload),
                "pipeline task kind() panicked — the task is dropped, the worker stays alive (R6/F09)"
            );
            return; // the guard decrements the counters
        }
    };
    // R6: `run()` must not panic; even so, catch a panic — the worker
    // stays alive and keeps consuming. The failure check and the send
    // live inside the same catch (F09), so a panic there cannot escape
    // either.
    let outcome = AssertUnwindSafe(async {
        let output = task.run().await;
        match output.failure() {
            Some(err) => {
                *consecutive_failures += 1;
                let escalated = escalation_due(track_consecutive_failures, *consecutive_failures);
                // F48: one warn per failure arm — the escalation rides as
                // an optional `failures` field; the message keeps the
                // operator-facing "likely systemic" signal (R7). Logged
                // even when a waiter holds [`Completion`], so a dropped
                // handle (scanner) cannot lose the failure (R8).
                tracing::warn!(
                    pipeline = kind,
                    kind = task_kind,
                    failures = escalated.then_some(*consecutive_failures),
                    error = %err,
                    "pipeline task failed{}",
                    if escalated {
                        "; the repeated failures are likely systemic (R7)"
                    } else {
                        ""
                    }
                );
            }
            None => *consecutive_failures = 0,
        }
        let _ = reply.send(output);
    })
    .catch_unwind()
    .await;
    if let Err(payload) = outcome {
        // A panic is a failure (R6): it increments the
        // consecutive-failure streak (R7), never resets it. Dropping
        // `reply` cancels [`Completion`] (`Error::Dropped`). One warn per
        // panic arm (F48) — the escalation rides as an optional field.
        *consecutive_failures += 1;
        let escalated = escalation_due(track_consecutive_failures, *consecutive_failures);
        tracing::warn!(
            pipeline = kind,
            kind = task_kind,
            failures = escalated.then_some(*consecutive_failures),
            panic = %panic_message(&*payload),
            "pipeline task panicked{} — the worker stays alive (R6)",
            if escalated {
                "; the repeated failures are likely systemic (R7)"
            } else {
                ""
            }
        );
    }
}

/// Render a panic payload for the logs (R6). The hand-rolled helper
/// stays: `std::panic::panic_message` was claimed stable since Rust 1.81
/// (F29) but does not exist on the 1.98 toolchain — the downcast is
/// still the way.
fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "unknown panic payload".to_string()
}

/// R7: the escalation warn fires once — when the failure streak *crosses*
/// [`CONSECUTIVE_FAILURE_ESCALATION`]. The streak resets on success, so a
/// later streak of the same length escalates again.
fn escalation_due(track_consecutive_failures: bool, consecutive_failures: u32) -> bool {
    track_consecutive_failures && consecutive_failures == CONSECUTIVE_FAILURE_ESCALATION
}

/// Map the config `priority` onto the thread-priority library (Q7).
///
/// `normal` sets nothing — the OS default is kept. `low`/`high` are the
/// lowest/highest legal [`ThreadPriorityValue`]s (`0` / `99`), verified
/// against thread-priority 3.1.1's Windows band mapping (`THREAD_PRIORITY_
/// IDLE` / `THREAD_PRIORITY_TIME_CRITICAL`, Q7 probe).
fn thread_priority(priority: pipeline_config::Priority) -> Option<ThreadPriority> {
    match priority {
        pipeline_config::Priority::Normal => None,
        pipeline_config::Priority::Low => {
            Some(ThreadPriority::Crossplatform(ThreadPriorityValue::MIN))
        }
        pipeline_config::Priority::High => {
            Some(ThreadPriority::Crossplatform(ThreadPriorityValue::MAX))
        }
    }
}

/// Apply a thread priority to the current thread; a failure is warned and
/// degraded — the pipeline keeps running at the OS default (best effort).
fn apply_thread_priority(priority: Option<ThreadPriority>) {
    if let Some(priority) = priority {
        apply_thread_priority_result(priority.set_for_current());
    }
}

/// Warn-and-degrade a `set_for_current` failure (never fatal).
fn apply_thread_priority_result(result: Result<(), thread_priority::Error>) {
    if let Err(err) = result {
        tracing::warn!(
            error = %err,
            "failed to set pipeline thread priority; continuing at the OS default (degraded)"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Duration;

    use tokio::sync::{MutexGuard as TokioMutexGuard, oneshot};
    use tracing_subscriber::prelude::*;

    use tinio_core::pipeline::Task;

    use super::*;

    /// A task that flips a flag on `run()` and on drop — pins the
    /// drop-after-completion contract (the box is dropped by the worker
    /// after `run()` returns).
    struct FlagTask {
        ran: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }

    impl FlagTask {
        fn new() -> (Arc<AtomicBool>, Arc<AtomicBool>, Self) {
            let ran = Arc::new(AtomicBool::new(false));
            let dropped = Arc::new(AtomicBool::new(false));
            (ran.clone(), dropped.clone(), Self { ran, dropped })
        }
    }

    impl Drop for FlagTask {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }

    #[async_trait]
    impl Task for FlagTask {
        type Output = RunOutput;

        fn kind(&self) -> &'static str {
            "flag"
        }

        async fn run(&mut self) -> Result<(), Box<dyn StdError + Send + Sync>> {
            self.ran.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    /// A task whose `run()` signals `started` and then waits for `release`
    /// — parks a worker until the test lets it go. Dropping the `release`
    /// sender unblocks it (the oneshot errors).
    struct GateTask {
        started: Option<oneshot::Sender<()>>,
        release: Option<oneshot::Receiver<()>>,
        ran: Arc<AtomicBool>,
    }

    impl GateTask {
        fn new() -> (
            oneshot::Receiver<()>,
            oneshot::Sender<()>,
            Arc<AtomicBool>,
            Self,
        ) {
            let (started_tx, started_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();
            let ran = Arc::new(AtomicBool::new(false));
            (
                started_rx,
                release_tx,
                ran.clone(),
                Self {
                    started: Some(started_tx),
                    release: Some(release_rx),
                    ran,
                },
            )
        }
    }

    #[async_trait]
    impl Task for GateTask {
        type Output = RunOutput;

        fn kind(&self) -> &'static str {
            "gate"
        }

        async fn run(&mut self) -> Result<(), Box<dyn StdError + Send + Sync>> {
            if let Some(started) = self.started.take() {
                let _ = started.send(());
            }
            if let Some(release) = self.release.take() {
                let _ = release.await;
            }
            self.ran.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    /// A task whose output matches the IO pipeline's type
    /// (`tinio_fs::etag::Result`) — the injected-runtime probe.
    struct EtagTask {
        ran: Arc<AtomicBool>,
    }

    impl EtagTask {
        fn new() -> (Arc<AtomicBool>, Self) {
            let ran = Arc::new(AtomicBool::new(false));
            (ran.clone(), Self { ran })
        }
    }

    #[async_trait]
    impl Task for EtagTask {
        type Output = tinio_fs::etag::Result;

        fn kind(&self) -> &'static str {
            "etag"
        }

        async fn run(&mut self) -> tinio_fs::etag::Result {
            self.ran.store(true, Ordering::Relaxed);
            Ok(tinio_fs::etag::Outcome {
                key: tinio_core::object::key("a.txt").unwrap(),
                etag: tinio_core::ETag::from_content(b"x"),
                size: 1,
                mtime: std::time::UNIX_EPOCH,
                identity: 0,
                kept: false,
            })
        }
    }

    /// A task that always fails `run()` (the R8 fallback report path).
    struct FailingTask {
        runs: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Task for FailingTask {
        type Output = RunOutput;

        fn kind(&self) -> &'static str {
            "failing"
        }

        async fn run(&mut self) -> Result<(), Box<dyn StdError + Send + Sync>> {
            self.runs.fetch_add(1, Ordering::Relaxed);
            Err("simulated run failure".into())
        }
    }

    /// A task that panics inside `run()` (R6).
    struct PanicTask {
        panicked: Arc<AtomicBool>,
    }

    impl PanicTask {
        fn new() -> (Arc<AtomicBool>, Self) {
            let panicked = Arc::new(AtomicBool::new(false));
            (panicked.clone(), Self { panicked })
        }
    }

    #[async_trait]
    impl Task for PanicTask {
        type Output = RunOutput;

        fn kind(&self) -> &'static str {
            "panic"
        }

        async fn run(&mut self) -> Result<(), Box<dyn StdError + Send + Sync>> {
            self.panicked.store(true, Ordering::Relaxed);
            panic!("simulated task panic (R6)");
        }
    }

    /// An owned `Write` sink the fmt layer can hold — the shared
    /// `tinio_util::testing` definition (F32 — log.rs's two copies are
    /// gone too).
    use tinio_util::testing::{SharedBuf, wait_for};

    /// Serializes the log-capturing tests (they share one global buffer).
    /// A tokio mutex: the guard is deliberately held across the whole test.
    static LOG_GUARD: TokioMutex<()> = TokioMutex::const_new(());
    static LOG_BUF: OnceLock<SharedBuf> = OnceLock::new();

    /// Install the warn-capturing global subscriber (once — worker threads
    /// only see the global default) and return the buffer, cleared, under
    /// the capture lock.
    async fn capture_warns() -> (TokioMutexGuard<'static, ()>, Arc<Mutex<Vec<u8>>>) {
        let guard = LOG_GUARD.lock().await;
        let buf = LOG_BUF.get_or_init(|| {
            let buf = SharedBuf::default();
            let writer = buf.clone();
            let subscriber = tracing_subscriber::registry()
                .with(tracing_subscriber::filter::LevelFilter::WARN)
                .with(tracing_subscriber::fmt::layer().with_writer(move || writer.clone()));
            tracing::subscriber::set_global_default(subscriber)
                .expect("only the log-capturing tests set the global default");
            buf
        });
        buf.0.lock().unwrap().clear();
        (guard, buf.0.clone())
    }

    /// Build both pipelines from a config with the given workers/capacity.
    fn pipelines(io_workers: u8, db_workers: u8, capacity: u32) -> Pipelines {
        Pipelines::build(&pipeline_config::Config {
            io: pipeline_config::Io {
                workers: io_workers,
                capacity,
                ..Default::default()
            },
            db: pipeline_config::Db {
                workers: db_workers,
                capacity,
                ..Default::default()
            },
        })
        .expect("pipeline runtime builds")
    }

    #[tokio::test]
    async fn enqueue_executes_a_task() {
        let pipelines = pipelines(1, 1, 16);
        let (ran, _dropped, task) = FlagTask::new();
        pipelines.io().enqueue(Box::new(task)).await.unwrap();
        wait_for(|| ran.load(Ordering::Relaxed)).await;
        assert_eq!(pipelines.io().stats(), Stats::default());
    }

    #[tokio::test]
    async fn task_box_drops_after_run_completes_and_the_worker_survives() {
        // Pins the deferred-minor contract: the task box is dropped after
        // run() completes, and the drop happens while the worker loop is
        // still alive (a follow-up task runs on the same pipeline).
        let pipelines = pipelines(1, 1, 16);
        let (ran, dropped, task) = FlagTask::new();
        pipelines.io().enqueue(Box::new(task)).await.unwrap();
        wait_for(|| ran.load(Ordering::Relaxed)).await;
        wait_for(|| dropped.load(Ordering::Relaxed)).await;
        let (ran, _dropped, task) = FlagTask::new();
        pipelines.io().enqueue(Box::new(task)).await.unwrap();
        wait_for(|| ran.load(Ordering::Relaxed)).await;
    }

    #[tokio::test]
    async fn run_failure_is_logged_and_the_pipeline_keeps_consuming() {
        let (_guard, buf) = capture_warns().await;
        let pipelines = pipelines(1, 1, 16);
        let runs = Arc::new(AtomicUsize::new(0));
        pipelines
            .io()
            .enqueue(Box::new(FailingTask { runs: runs.clone() }))
            .await
            .unwrap();
        wait_for(|| runs.load(Ordering::Relaxed) == 1).await;
        // The failure is logged (R8 fallback), not surfaced and not fatal.
        wait_for(|| {
            String::from_utf8(buf.lock().unwrap().clone())
                .unwrap()
                .contains("task failed")
        })
        .await;
        // A follow-up task still runs.
        let (ran, _dropped, task) = FlagTask::new();
        pipelines.io().enqueue(Box::new(task)).await.unwrap();
        wait_for(|| ran.load(Ordering::Relaxed)).await;
    }

    #[tokio::test]
    async fn shutdown_completes_in_flight_and_drops_queued_tasks() {
        let pipelines = pipelines(1, 1, 8);
        let (started, release, ran, gate) = GateTask::new();
        pipelines.io().enqueue(Box::new(gate)).await.unwrap();
        started.await.unwrap(); // the worker is inside run()

        // Fill the queue behind the in-flight task.
        let mut queued = Vec::new();
        for _ in 0..3 {
            let (ran, dropped, task) = FlagTask::new();
            pipelines.io().enqueue(Box::new(task)).await.unwrap();
            queued.push((ran, dropped));
        }

        // Shutdown: in-flight completes, queued tasks are dropped (Q3).
        pipelines.io().shutdown();
        release.send(()).unwrap();
        wait_for(|| ran.load(Ordering::Relaxed)).await;
        for (ran, dropped) in &queued {
            assert!(
                !ran.load(Ordering::Relaxed),
                "a queued task must not run after shutdown"
            );
            wait_for(|| dropped.load(Ordering::Relaxed)).await;
        }

        // enqueue after shutdown errors (Q3); shutdown is idempotent.
        let (_, _, task) = FlagTask::new();
        let err = pipelines.io().enqueue(Box::new(task)).await.unwrap_err();
        assert_eq!(err, pipeline::Error::ShutDown, "{err}");
        pipelines.io().shutdown();
    }

    #[tokio::test]
    async fn shutdown_orders_io_before_db() {
        // The name overstates what is observable: the IO-before-DB order
        // itself is pinned by code inspection — `Pipelines::shutdown` and
        // `Drop` (io first, db last) plus the serve.rs call site. What
        // this test actually verifies: an in-flight task on EACH pipeline
        // completes after the ordered shutdown, a queued task drops (Q3),
        // and both pipelines reject new work afterwards (R5).
        let pipelines = pipelines(1, 1, 8);

        let (db_started, db_release, db_ran, db_gate) = GateTask::new();
        pipelines.db().enqueue(Box::new(db_gate)).await.unwrap();
        db_started.await.unwrap();
        let (io_started, io_release, io_ran, io_gate) = GateTask::new();
        pipelines.io().enqueue(Box::new(io_gate)).await.unwrap();
        io_started.await.unwrap();
        let (queued_ran, queued_dropped, queued_task) = FlagTask::new();
        pipelines.io().enqueue(Box::new(queued_task)).await.unwrap();

        pipelines.shutdown();

        db_release.send(()).unwrap();
        wait_for(|| db_ran.load(Ordering::Relaxed)).await;
        io_release.send(()).unwrap();
        wait_for(|| io_ran.load(Ordering::Relaxed)).await;
        wait_for(|| queued_dropped.load(Ordering::Relaxed)).await;
        assert!(!queued_ran.load(Ordering::Relaxed));

        for pipeline in [pipelines.io(), pipelines.db()] {
            let (_, _, task) = FlagTask::new();
            assert!(
                pipeline.enqueue(Box::new(task)).await.is_err(),
                "enqueue after the ordered shutdown must error"
            );
        }
    }

    #[tokio::test]
    async fn drain_awaits_the_in_flight_task_after_shutdown() {
        // Item 6c: `drain` awaits the retained worker handles — an
        // in-flight task has completed by the time it returns (the Q3
        // semantics are unchanged; the await is the observability half).
        let pipelines = pipelines(1, 1, 8);
        let (started, release, ran, gate) = GateTask::new();
        let done = pipelines.io().enqueue(Box::new(gate)).await.unwrap();
        started.await.unwrap(); // the worker is inside run()
        pipelines.io().shutdown();

        let mut drained = tokio::spawn(async move {
            pipelines.io().drain().await;
        });
        let still_pending = tokio::time::timeout(Duration::from_millis(100), &mut drained).await;
        assert!(
            still_pending.is_err(),
            "drain must await the in-flight task, not return early"
        );
        release.send(()).unwrap();
        drained.await.unwrap();
        assert!(ran.load(Ordering::Relaxed), "the in-flight task completed");
        assert!(
            done.await.unwrap().is_ok(),
            "the in-flight task's completion resolves normally"
        );
    }

    #[tokio::test]
    async fn drain_is_idempotent_and_returns_when_workers_are_gone() {
        // The handles are taken once — a second drain (or a drain after
        // the workers already exited) returns immediately.
        let pipelines = pipelines(1, 1, 8);
        pipelines.io().shutdown();
        pipelines.io().drain().await;
        pipelines.io().drain().await;
    }

    #[tokio::test]
    async fn idle_worker_never_runs_a_task_racing_shutdown() {
        // Review finding 2: with the worker idle (parked in receive), a
        // task whose enqueue races shutdown must never run. The enqueue
        // observes the shut-down flag (set before the watch fires) and
        // errors — the task box drops without ever reaching the queue.
        // Fresh pipelines per iteration — shutdown is one-shot. On the
        // current-thread test runtime the spawned enqueue cannot run
        // before we yield, so the ordering is deterministic.
        for _ in 0..25 {
            let pipelines = pipelines(1, 1, 8);
            let (ran, dropped, task) = FlagTask::new();
            let pipeline = pipelines.io();
            let enqueue = tokio::spawn(async move { pipeline.enqueue(Box::new(task)).await });
            pipelines.io().shutdown();
            let result = enqueue.await.unwrap();
            assert_eq!(
                result.unwrap_err(),
                pipeline::Error::ShutDown,
                "an enqueue racing shutdown must not be accepted"
            );
            assert!(
                !ran.load(Ordering::Relaxed),
                "a task racing shutdown must not run"
            );
            wait_for(|| dropped.load(Ordering::Relaxed)).await;
        }
    }

    #[tokio::test]
    async fn completion_of_a_task_dropped_at_shutdown_resolves_dropped() {
        // A task still queued at shutdown is dropped (Q3) — its
        // Completion resolves with Error::Dropped instead of hanging, so a
        // list producer awaiting it cannot stall (review finding 2: the
        // task never runs).
        let pipelines = pipelines(1, 1, 8);
        let (started, release, _gate_ran, gate) = GateTask::new();
        let gate_done = pipelines.io().enqueue(Box::new(gate)).await.unwrap();
        started.await.unwrap(); // the worker is inside run()

        // Queue a task behind the in-flight gate and keep its completion.
        let (queued_ran, _queued_dropped, queued_task) = FlagTask::new();
        let queued_done = pipelines.io().enqueue(Box::new(queued_task)).await.unwrap();

        pipelines.io().shutdown();
        release.send(()).unwrap();

        // The in-flight gate completes normally — its reply was sent.
        let gate_outcome = tokio::time::timeout(Duration::from_secs(5), gate_done)
            .await
            .expect("the in-flight completion resolves")
            .expect("the in-flight task's reply was sent");
        assert!(gate_outcome.is_ok());
        // The queued task was dropped — its completion reports Dropped.
        let outcome = tokio::time::timeout(Duration::from_secs(5), queued_done)
            .await
            .expect("the dropped task's completion resolves");
        assert!(
            matches!(outcome, Err(pipeline::Error::Dropped)),
            "{outcome:?}"
        );
        assert!(!queued_ran.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn enqueue_backpressures_when_the_queue_is_full() {
        let pipelines = pipelines(1, 1, 1);
        let (started, release, _ran, gate) = GateTask::new();
        pipelines.io().enqueue(Box::new(gate)).await.unwrap();
        started.await.unwrap(); // the worker is busy; the queue is empty

        // Fill the single queue slot.
        let (_, _, fill) = FlagTask::new();
        pipelines.io().enqueue(Box::new(fill)).await.unwrap();

        // A further enqueue must wait for capacity (backpressure).
        let (blocked_ran, _dropped, blocked) = FlagTask::new();
        let pipeline = pipelines.io();
        let mut blocked_enqueue =
            tokio::spawn(async move { pipeline.enqueue(Box::new(blocked)).await });
        let timed_out =
            tokio::time::timeout(Duration::from_millis(100), &mut blocked_enqueue).await;
        assert!(
            timed_out.is_err(),
            "enqueue must block while the queue is full"
        );

        // Draining the gate lets the blocked enqueue through. The handle's
        // own await adds a JoinError layer, so the join is unwrapped inside
        // the timeout — the timeout then sees exactly two layers (Elapsed
        // and the enqueue result).
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            blocked_enqueue.await.expect("the enqueue task joins")
        })
        .await
        .expect("the blocked enqueue completes after capacity frees")
        .expect("enqueue succeeds");
        wait_for(|| blocked_ran.load(Ordering::Relaxed)).await;
    }

    #[tokio::test]
    async fn panicking_task_warns_and_the_worker_survives() {
        let (_guard, buf) = capture_warns().await;
        let pipelines = pipelines(1, 1, 16);
        let (panicked, task) = PanicTask::new();
        pipelines.io().enqueue(Box::new(task)).await.unwrap();
        wait_for(|| panicked.load(Ordering::Relaxed)).await;
        wait_for(|| {
            String::from_utf8(buf.lock().unwrap().clone())
                .unwrap()
                .contains("task panicked")
        })
        .await;
        // The worker survived the panic: a follow-up task still runs (R6).
        let (ran, _dropped, task) = FlagTask::new();
        pipelines.io().enqueue(Box::new(task)).await.unwrap();
        wait_for(|| ran.load(Ordering::Relaxed)).await;
    }

    #[tokio::test]
    async fn consecutive_run_failures_escalate_then_reset() {
        // R7 (DB write pipeline only): the 10th consecutive run() Err
        // escalates to the "likely systemic" warn; a success resets the
        // streak, so 4 more failures must not escalate again.
        let (_guard, buf) = capture_warns().await;
        let pipelines = pipelines(1, 1, 64);
        let runs = Arc::new(AtomicUsize::new(0));
        for _ in 0..10 {
            pipelines
                .db()
                .enqueue(Box::new(FailingTask {
                    runs: Arc::clone(&runs),
                }))
                .await
                .unwrap();
        }
        wait_for(|| runs.load(Ordering::Relaxed) == 10).await;
        wait_for(|| {
            String::from_utf8(buf.lock().unwrap().clone())
                .unwrap()
                .contains("likely systemic")
        })
        .await;

        let (ran, _dropped, ok_task) = FlagTask::new();
        pipelines.db().enqueue(Box::new(ok_task)).await.unwrap();
        wait_for(|| ran.load(Ordering::Relaxed)).await;
        for _ in 0..4 {
            pipelines
                .db()
                .enqueue(Box::new(FailingTask {
                    runs: Arc::clone(&runs),
                }))
                .await
                .unwrap();
        }
        wait_for(|| runs.load(Ordering::Relaxed) == 14).await;
        let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert_eq!(text.matches("likely systemic").count(), 1, "{text}");
    }

    #[tokio::test]
    async fn escalation_fires_once_per_failure_streak() {
        // R7: the escalation fires once, when the streak crosses the
        // threshold — failures past it log as ordinary warns, and a fresh
        // streak of 10 (after a success reset) escalates again.
        let (_guard, buf) = capture_warns().await;
        let pipelines = pipelines(1, 1, 64);
        let runs = Arc::new(AtomicUsize::new(0));
        for _ in 0..12 {
            pipelines
                .db()
                .enqueue(Box::new(FailingTask {
                    runs: Arc::clone(&runs),
                }))
                .await
                .unwrap();
        }
        wait_for(|| runs.load(Ordering::Relaxed) == 12).await;
        let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert_eq!(
            text.matches("likely systemic").count(),
            1,
            "one escalation per streak, not one per failure: {text}"
        );

        // A success resets the streak; 10 more failures escalate again.
        let (ran, _dropped, ok_task) = FlagTask::new();
        pipelines.db().enqueue(Box::new(ok_task)).await.unwrap();
        wait_for(|| ran.load(Ordering::Relaxed)).await;
        for _ in 0..10 {
            pipelines
                .db()
                .enqueue(Box::new(FailingTask {
                    runs: Arc::clone(&runs),
                }))
                .await
                .unwrap();
        }
        wait_for(|| runs.load(Ordering::Relaxed) == 22).await;
        let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert_eq!(text.matches("likely systemic").count(), 2, "{text}");
    }

    #[tokio::test]
    async fn a_panicking_task_counts_toward_the_failure_streak() {
        // R6 + R7: a panic is a failure — it increments the
        // consecutive-failure counter, so a panic as the 10th consecutive
        // failure escalates (a reset would leave the streak at 1).
        let (_guard, buf) = capture_warns().await;
        let pipelines = pipelines(1, 1, 64);
        let runs = Arc::new(AtomicUsize::new(0));
        let streak = (CONSECUTIVE_FAILURE_ESCALATION - 1) as usize;
        for _ in 0..streak {
            pipelines
                .db()
                .enqueue(Box::new(FailingTask {
                    runs: Arc::clone(&runs),
                }))
                .await
                .unwrap();
        }
        wait_for(|| runs.load(Ordering::Relaxed) == streak).await;
        let (panicked, task) = PanicTask::new();
        pipelines.db().enqueue(Box::new(task)).await.unwrap();
        wait_for(|| panicked.load(Ordering::Relaxed)).await;
        let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert_eq!(
            text.matches("likely systemic").count(),
            1,
            "the panic must be the 10th consecutive failure: {text}"
        );
    }

    #[tokio::test]
    async fn io_pipeline_does_not_escalate_consecutive_failures() {
        // R7 applies to the DB write pipeline only — 12 consecutive IO
        // failures stay ordinary warns.
        let (_guard, buf) = capture_warns().await;
        let pipelines = pipelines(1, 1, 64);
        let runs = Arc::new(AtomicUsize::new(0));
        for _ in 0..12 {
            pipelines
                .io()
                .enqueue(Box::new(FailingTask {
                    runs: Arc::clone(&runs),
                }))
                .await
                .unwrap();
        }
        wait_for(|| runs.load(Ordering::Relaxed) == 12).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(!text.contains("likely systemic"), "{text}");
    }

    #[tokio::test]
    async fn stats_reflect_queue_and_workers() {
        let pipelines = pipelines(1, 1, 8);
        assert_eq!(pipelines.io().stats(), Stats::default());
        let (started, _release, _ran, gate) = GateTask::new();
        pipelines.io().enqueue(Box::new(gate)).await.unwrap();
        started.await.unwrap(); // the worker is inside run()
        let (_, _, fill1) = FlagTask::new();
        let (_, _, fill2) = FlagTask::new();
        pipelines.io().enqueue(Box::new(fill1)).await.unwrap();
        pipelines.io().enqueue(Box::new(fill2)).await.unwrap();
        wait_for(|| {
            let stats = pipelines.io().stats();
            stats.in_flight == 1 && stats.busy_workers == 1 && stats.queue_depth == 2
        })
        .await;
    }

    #[tokio::test]
    async fn queue_depth_counts_only_queued_tasks() {
        // Item 6a: the depth gauge counts tasks IN the queue — a caller
        // blocked in `enqueue` (backpressure) is not queued yet and must
        // not be counted (the old pre-send increment over-stated the
        // depth by every blocked caller).
        let pipelines = pipelines(1, 1, 1);
        let (started, release, _ran, gate) = GateTask::new();
        pipelines.io().enqueue(Box::new(gate)).await.unwrap();
        started.await.unwrap(); // the worker is busy; the queue is empty
        wait_for(|| pipelines.io().stats().queue_depth == 0).await;

        // Fill the single slot.
        let (_, _, fill) = FlagTask::new();
        pipelines.io().enqueue(Box::new(fill)).await.unwrap();
        assert_eq!(pipelines.io().stats().queue_depth, 1);

        // A second enqueue blocks on capacity — the blocked caller must
        // not inflate the gauge.
        let (_, _, blocked) = FlagTask::new();
        let pipeline = pipelines.io();
        let blocked_enqueue =
            tokio::spawn(async move { pipeline.enqueue(Box::new(blocked)).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            pipelines.io().stats().queue_depth,
            1,
            "a task still in the caller's send is not queued (item 6a)"
        );
        release.send(()).unwrap();
        blocked_enqueue.await.unwrap().unwrap();
    }

    #[test]
    fn counters_each_own_a_cache_line() {
        // Item 6b (final fix round): the single `repr(align(64))` on the
        // struct only aligned its start — the atomics shared one line.
        // The remaining counter (in_flight — busy_workers was dropped as
        // redundant, F28) sits on its own 64-byte line, so the
        // worker-written group cannot false-share (a layout pin: the
        // per-counter alignment is the fix).
        assert_eq!(std::mem::size_of::<Counters>(), 64);
        assert_eq!(std::mem::align_of::<Counters>(), 64);
    }

    #[test]
    fn priority_mapping_pins_low_and_high() {
        // Q7: `normal` = do not set; `low`/`high` = the lowest/highest legal
        // `ThreadPriorityValue`s (0 / 99), verified against the crate's
        // Windows band mapping (see the windows probe below).
        assert_eq!(thread_priority(pipeline_config::Priority::Normal), None);
        let low: u8 = ThreadPriorityValue::MIN.into();
        assert_eq!(low, 0);
        assert_eq!(
            thread_priority(pipeline_config::Priority::Low),
            Some(ThreadPriority::Crossplatform(ThreadPriorityValue::MIN))
        );
        let high: u8 = ThreadPriorityValue::MAX.into();
        assert_eq!(high, 99);
        assert_eq!(
            thread_priority(pipeline_config::Priority::High),
            Some(ThreadPriority::Crossplatform(ThreadPriorityValue::MAX))
        );
        // Out-of-range values are rejected by the crate — the Q7 revision
        // retired the original `Crossplatform(255)` ruling.
        assert!(ThreadPriorityValue::try_from(100u8).is_err());
    }

    #[tokio::test]
    async fn priority_set_failure_warns_and_degrades() {
        let (_guard, buf) = capture_warns().await;
        // A failing set_for_current (e.g. missing privileges on unix) must
        // warn and degrade — never fail startup or panic.
        apply_thread_priority_result(Err(thread_priority::Error::OS(1)));
        let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            text.contains("failed to set pipeline thread priority"),
            "{text}"
        );
    }

    /// Q7 probe: the actual Windows mapping of the pinned `Crossplatform`
    /// values, read back through the OS (thread-priority 3.1.1 maps by
    /// bands, not a raw cast): 0 → `Idle`, 50 → `Normal`, 99 →
    /// `TimeCritical`. The crate's own `get_current_thread_priority`
    /// round-trips through `GetThreadPriority`.
    #[cfg(windows)]
    #[test]
    fn windows_thread_priority_mapping_probe() {
        use thread_priority::windows::WinAPIThreadPriority;

        fn roundtrip(value: u8, expected: WinAPIThreadPriority) {
            ThreadPriority::Crossplatform(ThreadPriorityValue::try_from(value).unwrap())
                .set_for_current()
                .unwrap();
            let actual = thread_priority::get_current_thread_priority().unwrap();
            assert_eq!(
                actual,
                ThreadPriority::Os(expected.into()),
                "Crossplatform({value}) must read back as {expected:?}"
            );
        }

        roundtrip(0, WinAPIThreadPriority::Idle);
        roundtrip(50, WinAPIThreadPriority::Normal);
        roundtrip(99, WinAPIThreadPriority::TimeCritical);

        // Restore the OS default for the rest of the suite.
        ThreadPriority::Os(WinAPIThreadPriority::Normal.into())
            .set_for_current()
            .unwrap();
    }

    /// The mandatory `FsOptions.io_pipeline`/`db_pipeline` handles (P4)
    /// accept the real runtimes — typed to the tinio-fs task outputs
    /// (P4/P7) — and a task flows through the injected IO pipeline.
    #[tokio::test]
    async fn pipelines_inject_into_fs_options() {
        // The `Pipelines` output types are inferred from the `FsOptions`
        // fields: IO = the etag task's `etag::Result`, DB = the write
        // task's `Result<(), tinio_fs::Error>`.
        let pipelines = Pipelines::build(&pipeline_config::Config {
            io: pipeline_config::Io {
                workers: 1,
                capacity: 16,
                ..Default::default()
            },
            db: pipeline_config::Db {
                workers: 1,
                capacity: 16,
                ..Default::default()
            },
        })
        .expect("pipeline runtime builds");
        let root = tempfile::tempdir().unwrap();
        let _storage = tinio_fs::FsStorage::new(
            root.path(),
            tinio_fs::FsOptions {
                follow_symlinks: false,
                state_dir: None,
                compact_threshold_percent: tinio_core::storage::DEFAULT_COMPACT_THRESHOLD_PERCENT,
                meta_batch_size: tinio_core::storage::DEFAULT_META_BATCH_SIZE,
                meta_batch_bytes: tinio_core::storage::DEFAULT_META_BATCH_BYTES,
                io_pipeline: pipelines.io(),
                db_pipeline: pipelines.db(),
            },
        )
        .expect("FsStorage accepts the real pipeline runtimes");
        let (ran, task) = EtagTask::new();
        pipelines.io().enqueue(Box::new(task)).await.unwrap();
        wait_for(|| ran.load(Ordering::Relaxed)).await;
    }
}
