//! Shared test helpers (`#[cfg(test)]` only).

#[cfg(unix)]
use std::os::unix::fs::symlink;
#[cfg(windows)]
use std::os::windows::fs::symlink_dir;
#[cfg(windows)]
use std::process::Command;
use std::{
    fs,
    future::Future,
    io::{Error as IoError, ErrorKind},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    fs as tokio_fs,
    sync::{Mutex, mpsc, watch},
    task, time,
};

use crate::{
    _core::{
        bucket,
        pipeline::{self, Completion, Error::ShutDown, Reply, Runner, Stats, Task},
    },
    _util::testing::wait_for as util_wait_for,
    Error, FsOptions, FsStorage, etag, testing,
};

/// Poll `cond` until true or a 10 s deadline passes (the test runners'
/// workers are asynchronous, so assertions must wait). The shared
/// `tinio_util::testing` home (F30).
pub(crate) async fn wait_for(cond: impl FnMut() -> bool) {
    util_wait_for(cond).await
}

/// The standard test `FsOptions` — the shared `tinio_fs::testing` home
/// (F33).
pub(crate) fn fs_options() -> FsOptions {
    testing::fs_options()
}

/// A fresh storage root + backend (default options) — the shared backend
/// test fixture.
pub(crate) fn storage() -> (tempfile::TempDir, FsStorage) {
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    (root, storage)
}

/// A bucket `data` with `n` files `f00.txt..`, each with distinct content
/// (`payload {i}`) — the shared producer fixture of the list and scanner
/// tests (F39; the list tests add their own state store on top).
pub(crate) fn files(root: &Path, n: usize) {
    fs::create_dir(root.join("data")).unwrap();
    for i in 0..n {
        fs::write(
            root.join("data").join(format!("f{i:02}.txt")),
            format!("payload {i}"),
        )
        .unwrap();
    }
}

/// Retarget a followed bucket symlink while a write is blocked between
/// staging/assembly and the rename: hold `bucket`'s mutation lock, spawn
/// `op`, wait until `ready` (phase 1 done), swap `link` to `new_target`,
/// then release the lock and await `op`.
pub(crate) async fn retarget_bucket_during_commit<F, Fut, R, T>(
    storage: &FsStorage,
    bucket: &bucket::Name,
    link: &Path,
    new_target: &Path,
    ready: R,
    op: F,
) -> T
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
    R: Future<Output = ()>,
    T: Send + 'static,
{
    let guard = storage.lock_bucket_mutations(bucket).await;
    let handle = task::spawn(op());
    ready.await;
    replace_dir_link(link, new_target);
    drop(guard);
    handle.await.unwrap()
}

/// Wait until a file appears under `<state-dir>/tmp/` (assembly / first
/// stage has finished).
pub(crate) async fn wait_for_tmp(storage: &FsStorage) {
    let tmp = storage.state_dir().join("tmp");
    let appeared = time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(mut entries) = tokio_fs::read_dir(&tmp).await
                && entries.next_entry().await.ok().flatten().is_some()
            {
                return;
            }
            task::yield_now().await;
        }
    })
    .await;
    assert!(appeared.is_ok(), "phase-1 temp never appeared under tmp/");
}

/// Yield long enough for a spawned commit to reach its wait on the
/// mutation lock (since P5 the commit has no pre-lock resolve — it
/// blocks on the lock first thing).
pub(crate) async fn wait_for_lock_waiter() {
    for _ in 0..64 {
        task::yield_now().await;
    }
    time::sleep(Duration::from_millis(20)).await;
}

/// Create a directory symlink (Unix) or directory symlink (Windows).
pub(crate) fn link_dir(original: &Path, link: &Path) {
    #[cfg(unix)]
    symlink(original, link).unwrap();
    #[cfg(windows)]
    symlink_dir(original, link).unwrap();
}

/// Create a directory link (symlink on unix, **junction** on Windows) —
/// the shared fixture of the follow-policy tests (the listing walk and
/// the bucket-name sweep). The junction form (`mklink /J`) needs no
/// Developer Mode, unlike [`link_dir`]'s `symlink_dir`.
pub(crate) fn link_directory(src: &Path, dst: &Path) {
    #[cfg(unix)]
    symlink(src, dst).unwrap();
    #[cfg(windows)]
    {
        // `mklink` parses the destination with cmd's tokenizer: a
        // forward slash from a `join("a/b")` path reads as a switch
        // ("syntax incorrect at the leaf") — normalize to the native
        // separator first.
        let dst = dst.to_string_lossy().replace('/', "\\");
        let src = src.to_string_lossy().replace('/', "\\");
        let status = Command::new("cmd")
            .args(["/C", "mklink", "/J", &dst, &src])
            .status()
            .expect("spawn mklink");
        assert!(status.success(), "mklink /J failed with {status}");
    }
}

fn replace_dir_link(link: &Path, new_target: &Path) {
    #[cfg(unix)]
    {
        fs::remove_file(link).unwrap();
        symlink(new_target, link).unwrap();
    }
    #[cfg(windows)]
    {
        fs::remove_dir(link).unwrap();
        symlink_dir(new_target, link).unwrap();
    }
}

// --- test task-pipeline runners (Q4/Q3b/R8 probe harnesses) ---
//
// The producers only ever see `InlineRunner` in the other tests; these
// runners exercise the real concurrent/backpressure/failure semantics the
// acceptance criteria call for.

/// One queued job: the task plus the reply the worker sends after `run()`
/// (the other end is [`Completion`]).
type Job<O> = (Box<dyn Task<Output = O>>, Reply<O>);

/// Pull one job off the shared receiver. A separate function so the
/// mutex guard drops before the caller runs the task — holding it across
/// the worker body would serialize the workers (the guard temporary in a
/// `while let` scrutinee lives for the whole statement, sleep included).
async fn recv_job<O>(rx: &Arc<Mutex<mpsc::Receiver<Job<O>>>>) -> Option<Job<O>> {
    rx.lock().await.recv().await
}

/// A tiny **concurrent** [`Runner`] for tests: `workers` async tasks pull
/// jobs off a bounded queue and run them on the test runtime's threads
/// (the blocking-task model, Q4). An optional per-task `delay` parks the
/// worker before running, so overlapping is deterministic no matter how
/// fast the work itself is. Tracks the maximum number of tasks in `run()`
/// simultaneously and the total enqueued count.
pub(crate) struct PacedRunner<O> {
    tx: mpsc::Sender<Job<O>>,
    max_in_run: Arc<AtomicUsize>,
    enqueued: Arc<AtomicUsize>,
}

impl<O> PacedRunner<O>
where
    O: Send + 'static,
{
    /// Build a runner with `workers` concurrent workers over a bounded
    /// queue of `capacity`; each worker sleeps `delay` before running a
    /// task (zero = run immediately). Spawns the workers on the current
    /// runtime.
    pub(crate) fn new(workers: usize, capacity: usize, delay: Duration) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<Job<O>>(capacity);
        let rx = Arc::new(Mutex::new(rx));
        let max_in_run = Arc::new(AtomicUsize::new(0));
        let in_run = Arc::new(AtomicUsize::new(0));
        let enqueued = Arc::new(AtomicUsize::new(0));
        for _ in 0..workers {
            let rx = Arc::clone(&rx);
            let max_in_run = Arc::clone(&max_in_run);
            let in_run = Arc::clone(&in_run);
            task::spawn(async move {
                while let Some((mut task, reply)) = recv_job(&rx).await {
                    let now = in_run.fetch_add(1, Ordering::Relaxed) + 1;
                    max_in_run.fetch_max(now, Ordering::Relaxed);
                    if delay > Duration::ZERO {
                        time::sleep(delay).await;
                    }
                    let _ = reply.send(task.run().await);
                    in_run.fetch_sub(1, Ordering::Relaxed);
                }
            });
        }
        Arc::new(Self {
            tx,
            max_in_run,
            enqueued,
        })
    }

    /// Tasks accepted so far (the producer's enqueue count).
    pub(crate) fn enqueued(&self) -> usize {
        self.enqueued.load(Ordering::Relaxed)
    }

    /// The maximum number of tasks in `run()` simultaneously — the
    /// observed concurrency.
    pub(crate) fn max_in_run(&self) -> usize {
        self.max_in_run.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl<O> Runner<O> for PacedRunner<O>
where
    O: Send + 'static,
{
    async fn enqueue(
        &self,
        task: Box<dyn Task<Output = O>>,
    ) -> Result<Completion<O>, pipeline::Error> {
        let (reply, done) = Completion::pair();
        self.enqueued.fetch_add(1, Ordering::Relaxed);
        self.tx.send((task, reply)).await.map_err(|_| ShutDown)?;
        Ok(done)
    }

    fn shutdown(&self) {}

    fn stats(&self) -> Stats {
        Stats::default()
    }
}

/// A concurrent [`Runner`] whose workers park every task until the test
/// opens the gate — the deterministic walk-to-hash TOCTOU window
/// (pipeline-spec.md R3/R4/Q10 tests swap or delete the files there).
/// Counts accepted tasks so the test knows when the producer's enqueue
/// phase is complete.
pub(crate) struct GatedRunner<O> {
    tx: mpsc::Sender<(Box<dyn Task<Output = O>>, Reply<O>)>,
    gate: watch::Sender<bool>,
    enqueued: Arc<AtomicUsize>,
}

impl<O> GatedRunner<O>
where
    O: Send + 'static,
{
    /// Build a runner with `workers` concurrent workers over a bounded
    /// queue of `capacity`; every task waits for [`Self::open_gate`]
    /// before running.
    pub(crate) fn new(workers: usize, capacity: usize) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<Job<O>>(capacity);
        let rx = Arc::new(Mutex::new(rx));
        let (gate, gate_rx) = watch::channel(false);
        let enqueued = Arc::new(AtomicUsize::new(0));
        for _ in 0..workers {
            let rx = Arc::clone(&rx);
            let mut gate_rx = gate_rx.clone();
            task::spawn(async move {
                while let Some((mut task, reply)) = recv_job(&rx).await {
                    // Park until the gate opens (`changed()` alone would
                    // fire only once per send — later tasks would hang).
                    while !*gate_rx.borrow() {
                        let _ = gate_rx.changed().await;
                    }
                    let _ = reply.send(task.run().await);
                }
            });
        }
        Arc::new(Self { tx, gate, enqueued })
    }

    /// Tasks accepted so far.
    pub(crate) fn enqueued(&self) -> usize {
        self.enqueued.load(Ordering::Relaxed)
    }

    /// Release the parked workers.
    pub(crate) fn open_gate(&self) {
        self.gate.send(true).unwrap();
    }
}

#[async_trait]
impl<O> Runner<O> for GatedRunner<O>
where
    O: Send + 'static,
{
    async fn enqueue(
        &self,
        task: Box<dyn Task<Output = O>>,
    ) -> Result<Completion<O>, pipeline::Error> {
        let (reply, done) = Completion::pair();
        self.enqueued.fetch_add(1, Ordering::Relaxed);
        self.tx.send((task, reply)).await.map_err(|_| ShutDown)?;
        Ok(done)
    }

    fn shutdown(&self) {}

    fn stats(&self) -> Stats {
        Stats::default()
    }
}

/// A [`Runner`] that **loses** every task without running it — the
/// crash-loss simulation (a batch dropped before commit is equivalent to
/// an uncommitted batch; the next pass recomputes). The completion
/// resolves [`Error::Dropped`].
pub(crate) struct LossyRunner;

#[async_trait]
impl<O> Runner<O> for LossyRunner
where
    O: Send + 'static,
{
    async fn enqueue(
        &self,
        _task: Box<dyn Task<Output = O>>,
    ) -> Result<Completion<O>, pipeline::Error> {
        let (reply, done) = Completion::pair();
        drop(reply); // the task never ran — the handle reports Dropped
        Ok(done)
    }

    fn shutdown(&self) {}

    fn stats(&self) -> Stats {
        Stats::default()
    }
}

/// An IO-pipeline runner that **fails** every task immediately (inline,
/// with a non-NotFound IO error) and counts the accepted tasks — the
/// probe for the scanner's streaming drain: an R4 abort must stop the
/// walk, so the enqueue count freezes at the failure threshold instead
/// of running to the bucket size.
pub(crate) struct FailingTaskRunner {
    enqueued: Arc<AtomicUsize>,
}

impl FailingTaskRunner {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            enqueued: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Tasks accepted so far.
    pub(crate) fn enqueued(&self) -> usize {
        self.enqueued.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Runner<etag::Result> for FailingTaskRunner {
    async fn enqueue(
        &self,
        _task: Box<dyn Task<Output = etag::Result>>,
    ) -> Result<Completion<etag::Result>, pipeline::Error> {
        let (reply, done) = Completion::pair();
        self.enqueued.fetch_add(1, Ordering::Relaxed);
        let _ = reply.send(Err(Error::Io(IoError::new(
            ErrorKind::PermissionDenied,
            "simulated task failure",
        ))));
        Ok(done)
    }

    fn shutdown(&self) {}

    fn stats(&self) -> Stats {
        Stats::default()
    }
}

/// A DB-pipeline runner that runs every batch (the real write lands) but
/// reports it as failed — the scanner's fire-and-forget path (Q3b): the
/// failure is observed at the runner (like the concurrent runtime's
/// `Outcome` warn, R8) although the completion is dropped, and the scan
/// continues.
pub(crate) struct FailingBatchRunner {
    batches: Arc<AtomicUsize>,
    failures: Arc<AtomicUsize>,
}

impl FailingBatchRunner {
    pub(crate) fn new() -> (Arc<Self>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let batches = Arc::new(AtomicUsize::new(0));
        let failures = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(Self {
                batches: Arc::clone(&batches),
                failures: Arc::clone(&failures),
            }),
            batches,
            failures,
        )
    }
}

#[async_trait]
impl Runner<Result<(), Error>> for FailingBatchRunner {
    async fn enqueue(
        &self,
        mut task: Box<dyn Task<Output = Result<(), Error>>>,
    ) -> Result<Completion<Result<(), Error>>, pipeline::Error> {
        let (reply, done) = Completion::pair();
        self.batches.fetch_add(1, Ordering::Relaxed);
        let _ = task.run().await; // the real write still lands
        self.failures.fetch_add(1, Ordering::Relaxed);
        let _ = reply.send(Err(Error::Io(IoError::other("simulated batch failure"))));
        Ok(done)
    }

    fn shutdown(&self) {}

    fn stats(&self) -> Stats {
        Stats::default()
    }
}
