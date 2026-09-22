//! T056's matrix: the management plane driven the way the CLI drives it — `Plane::bind(options)`
//! → `publish(port)` → `serve(stop)` → the status/stop client → `shutdown()`. Nothing outside a
//! test starts a plane in this phase, so this suite is the acceptance rather than a companion to
//! a running binary (and T075's phase-A verification, since Task 7 retired the cucumber scrape).
//!
//! The gauges' half uses **one local [`MetricsSource`] double**, and its shape is the reason
//! rather than a shortcut: nothing a real adapter computes crosses the boundary — `refresh`
//! republishes `tinio-server`'s own families, `counts` sets the three statics only that crate can
//! name, and the storage scan cannot run here at all (`Storage` is deliberately dyn-incompatible,
//! so there is no `dyn Storage` to hand a plane, and this crate takes no `tinio-mem` dependency
//! for one).

#[cfg(windows)]
use std::sync::atomic::AtomicU32;
#[cfg(unix)]
use std::{fs, os::unix::fs::PermissionsExt, os::unix::net::UnixListener as StdUnixListener};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use http::Method;
// The Windows legs are the only ones that drive a connection by hand: the pipe dial in
// `local_request` below, and the connection that is open when the stop fires.
#[cfg(windows)]
use hyper::{Request, client::conn::http1};
#[cfg(windows)]
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tinio_api::{
    Error, Options, Plane,
    client::{self, Call, RawResponse, Report},
    metrics::{DEFAULT_TTL, Gauges, MetricsSource},
    router::StatusBody,
    state::{self, State},
    transport::{LocalForm, Selection},
};
#[cfg(windows)]
use tokio::net::windows::named_pipe::ClientOptions;
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{sleep, timeout},
};

/// How long the CLI gives a drain before it stops waiting (design decision 12's bound): the
/// tests use the same number, so a hang is reported rather than absorbed.
const DRAIN_BOUND: Duration = Duration::from_secs(10);

/// How long a leg waits for its plane to answer: `state` is published at `publish`, *before*
/// `serve` reaches its accept loop, and the local channel's backlog can be one connection deep.
const READY_BOUND: Duration = Duration::from_secs(5);

/// The local channel for a root under this state dir: the derived unix socket, whose name is a
/// function of the root (`state::socket_path`), or a pipe name distinct per test.
#[cfg(unix)]
fn local(state_dir: &Path, root: &Path) -> LocalForm {
    LocalForm::Unix(state::socket_path(state_dir, root).expect("socket path"))
}

#[cfg(windows)]
fn local(_state_dir: &Path, _root: &Path) -> LocalForm {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    // The full `\\.\pipe\…` form, which is what a client dials and what `bind_pipe` records:
    // the bare spelling is the config's, and the bind's own normalisation is what turns it into
    // this — the leg hands it the already-normalised one so the name it later dials and reads
    // its DACL back from is byte-for-byte the one the plane bound.
    LocalForm::Pipe(format!(
        r"\\.\pipe\tinio-test-{}-{}",
        process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

/// The CLI's selection for one resolved local channel: the caller resolved the form (the config's
/// `[api.unix]`/`[api.pipe]` section, or the derived default), and nothing downstream re-derives
/// a name from it.
fn local_selection(local: &LocalForm) -> Selection {
    Selection {
        local: Some(local.clone()),
        ..Selection::default()
    }
}

/// The two directories a leg serves from: the state dir the plane anchors on, and the served
/// root — canonical, because it is what the socket name hashes and what `state` records.
struct Dirs {
    state_dir: TempDir,
    /// Held for its lifetime alone: `root` is the canonical path *under* this directory, and
    /// dropping the guard would delete it out from under the plane.
    _root_dir: TempDir,
    root: PathBuf,
}

impl Dirs {
    fn new() -> Self {
        let state_dir = TempDir::new().expect("state dir");
        let root_dir = TempDir::new().expect("root");
        let root = root_dir.path().canonicalize().expect("canonical root");
        Self {
            state_dir,
            _root_dir: root_dir,
            root,
        }
    }

    fn dir(&self) -> &Path {
        self.state_dir.path()
    }

    fn root(&self) -> &Path {
        &self.root
    }

    /// The local channel a run over these two directories binds when the config names neither.
    fn local(&self) -> LocalForm {
        local(self.dir(), self.root())
    }

    /// The bound control socket, unix only.
    #[cfg(unix)]
    fn socket(&self) -> PathBuf {
        let LocalForm::Unix(path) = self.local() else {
            unreachable!("the derived local form is a socket path");
        };
        path
    }
}

/// How a leg waits for its plane to answer. `State` is the CLI's own path — read `state` and
/// dial the channel it names, which for a wildcard bind means the client's own loopback
/// substitution (`client::dialable`, pinned by the wildcard leg below) — while `Loopback` dials
/// `127.0.0.1:<recorded port>` itself, for the leg that drives the listener directly and so must
/// not depend on the code it is not testing to become ready.
enum Ready {
    /// The CLI's own path: read `state`, dial the channel it names.
    State,
    /// The raw address the wildcard bind answers on, rather than what `state` records.
    Loopback,
}

/// A plane that is bound, published and serving, with the shutdown channel the CLI owns.
struct Running {
    plane: Arc<Plane>,
    serving: JoinHandle<Result<(), Error>>,
    stop: watch::Sender<bool>,
    rx: watch::Receiver<bool>,
    report: Report,
}

impl Running {
    /// The CLI's own startup, in order: bind (the single-instance lock) → publish (the readiness
    /// point) → serve, then wait until the channel answers.
    async fn start(
        dirs: &Dirs,
        selection: Selection,
        gauges: Option<Arc<Gauges>>,
        ready: Ready,
    ) -> Self {
        let (stop, rx) = watch::channel(false);
        let plane = Arc::new(
            Plane::bind(Options {
                selection,
                state_dir: dirs.dir().to_path_buf(),
                root: dirs.root().to_path_buf(),
                host: "127.0.0.1".to_string(),
                stop: stop.clone(),
                gauges,
            })
            .expect("bind"),
        );
        plane.publish(9000).expect("publish");
        let serving = tokio::spawn({
            let plane = Arc::clone(&plane);
            let rx = rx.clone();
            async move { plane.serve(rx).await }
        });

        await_ready(dirs.dir(), &ready).await;
        // Read back through the client the CLI uses; the state file is there whatever the
        // recorded control name is, which is what the wildcard leg needs it for.
        let report = client::status(dirs.dir()).await.expect("state reads");
        assert!(report.state.is_some(), "`publish` wrote the state file");
        Self {
            plane,
            serving,
            stop,
            rx,
            report,
        }
    }

    fn token(&self) -> String {
        self.report
            .state
            .as_ref()
            .expect("the state file it read")
            .token
            .clone()
    }

    fn status(&self) -> &StatusBody {
        self.report.status.as_ref().expect("the contract's body")
    }

    /// The CLI's stop path — `client::stop` returns `Ok` only for the contract's `202` — then
    /// the drain and the plane's own deletions.
    async fn stop(self, dirs: &Dirs) {
        client::stop(dirs.dir()).await.expect("stop answers 202");
        self.drain(dirs).await;
    }

    /// The same shutdown from the channel's own end, for the leg that drives the raw address
    /// (`Ready::Loopback`) rather than the client's: `/stop` sends on this very sender.
    async fn end(self, dirs: &Dirs) {
        self.stop.send(true).expect("the plane is listening");
        self.drain(dirs).await;
    }

    /// The bounded drain, the flipped channel, and `shutdown`'s deletion of the state file.
    async fn drain(self, dirs: &Dirs) {
        timeout(DRAIN_BOUND, self.serving)
            .await
            .expect("the drain is bounded")
            .expect("the serve task does not panic")
            .expect("serve returns Ok");
        assert!(
            *self.rx.borrow(),
            "`/stop` flips the channel `serve` awaited"
        );
        self.plane.shutdown().await.expect("shutdown");
        assert!(
            State::read(dirs.dir()).expect("read").is_none(),
            "`shutdown` removes the state file"
        );
    }
}

/// The address the plane bound: `state` records the **bound** one, which is what the token rule
/// followed — a `--port 0` or wildcard bind both make that different from the requested one.
fn bound(dir: &Path) -> SocketAddr {
    State::read(dir)
        .expect("read")
        .expect("published")
        .control_name
        .parse()
        .expect("a TCP control name is a socket address")
}

/// A `GET` call with the token when the leg has one — every direct request this suite makes
/// through [`client::get_http`] is one, and [`Call`] is the shape that client takes.
fn get<'a>(target: &'a str, token: Option<&'a str>) -> Call<'a> {
    Call {
        target,
        method: Method::GET,
        token,
    }
}

/// Whether the plane answers yet: `state` is published at `publish`, *before* `serve` reaches
/// its accept loop, so a probe that races it answers "stopped".
async fn answers(dir: &Path, ready: &Ready) -> bool {
    let token = State::read(dir).expect("read").expect("published").token;
    match ready {
        Ready::State => client::status(dir).await.expect("state reads").running,
        Ready::Loopback => {
            let dial = SocketAddr::from(([127, 0, 0, 1], bound(dir).port()));
            client::get_http(dial, get("/status", Some(&token)))
                .await
                .expect("request")
                .status
                == 200
        }
    }
}

/// Waits, bounded, for [`answers`].
async fn await_ready(dir: &Path, ready: &Ready) {
    let deadline = Instant::now() + READY_BOUND;
    while Instant::now() < deadline {
        if answers(dir, ready).await {
            return;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("the plane did not answer within {READY_BOUND:?}");
}

/// One request over the local channel, with or without the token — the crate's own client covers
/// the CLI's paths, this wrapper the rest: a tokenless `/metrics`, a `POST /stop` whose `202`
/// `client::stop` swallows into `Ok`, and `/openapi.json`. The dial and the exchange are both the
/// crate's ([`client::get_local`]), so there is one definition of each and not two.
#[cfg(unix)]
async fn local_request(
    local: &LocalForm,
    target: &str,
    method: Method,
    token: Option<&str>,
) -> RawResponse {
    let LocalForm::Unix(path) = local else {
        unreachable!("the unix leg dials a socket path");
    };
    client::get_local(
        path,
        Call {
            target,
            method,
            token,
        },
    )
    .await
    .expect("request")
}

/// The named-pipe form of [`local_request`]. No client helper dials a pipe — `get_local` is the
/// unix path and refuses on this platform — so the dial is here and the exchange is the crate's
/// one, [`client::exchange`].
#[cfg(windows)]
async fn local_request(
    local: &LocalForm,
    target: &str,
    method: Method,
    token: Option<&str>,
) -> RawResponse {
    let LocalForm::Pipe(name) = local else {
        unreachable!("the Windows leg dials a pipe name");
    };
    let stream = ClientOptions::new()
        .open(name)
        .expect("dial the control pipe");
    client::exchange(
        TokioIo::new(stream),
        Call {
            target,
            method,
            token,
        },
    )
    .await
    .expect("request")
}

/// The caller's side of the injection point, in miniature (Task 7's Step 1 shape): two counting
/// no-ops. The scan count is the number the reader's cadence exists to keep down, and it is the
/// one the legs below watch — a scrape must never move it.
#[derive(Default)]
struct Counting {
    refreshes: Arc<AtomicUsize>,
    scans: Arc<AtomicUsize>,
}

impl Counting {
    /// The cheap half's count: one per scrape.
    fn refreshes(&self) -> usize {
        self.refreshes.load(Ordering::SeqCst)
    }

    /// The expensive half's count: one per reader-tick, and none per scrape.
    fn scans(&self) -> usize {
        self.scans.load(Ordering::SeqCst)
    }
}

impl MetricsSource for Counting {
    fn refresh(&self) {
        self.refreshes.fetch_add(1, Ordering::SeqCst);
    }

    fn counts(&self) {
        self.scans.fetch_add(1, Ordering::SeqCst);
    }
}

/// The plane's source: the double above, at the ttl the leg needs.
fn gauges(source: &Arc<Counting>, ttl: Duration) -> Arc<Gauges> {
    // The clone keeps the double's concrete type so a leg can read its counters back; the
    // coercion to the trait object happens at the argument, which is where the seam is.
    let source = Arc::clone(source);
    Arc::new(Gauges::new(source, ttl))
}

#[tokio::test]
async fn the_cli_sequence_answers_status_and_leaves_nothing_behind() {
    let dirs = Dirs::new();
    let local = dirs.local();
    let running = Running::start(&dirs, local_selection(&local), None, Ready::State).await;
    assert!(running.report.running, "the plane answers for its own root");

    let status = running.status();
    assert_eq!(status.pid, process::id());
    assert_eq!(status.port, Some(9000));
    assert_eq!(status.host, "127.0.0.1");
    assert_eq!(status.root, dirs.root().to_string_lossy());
    let token = running.token();
    assert_eq!(token.len(), 64, "the token is 32 CSPRNG bytes, hex-encoded");

    // With the token, the contract's `200`; without one, and with a wrong one, `401` — `/status`
    // is guarded on every transport, the local channel included.
    let answer = local_request(&local, "/status", Method::GET, Some(&token)).await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert!(answer.body.contains("\"running\":true"), "{}", answer.body);
    for absent in [None, Some("wrong")] {
        let answer = local_request(&local, "/status", Method::GET, absent).await;
        assert_eq!(answer.status, 401, "`/status` needs the token here too");
    }

    // The CLI's stop path, the drain it starts, and the plane's own deletions.
    running.stop(&dirs).await;
    #[cfg(unix)]
    assert!(
        !dirs.socket().exists(),
        "`shutdown` removes the socket file: {}",
        dirs.socket().display()
    );
}

#[tokio::test]
async fn stop_answers_the_contracts_202_with_the_draining_body() {
    let dirs = Dirs::new();
    let local = dirs.local();
    let running = Running::start(&dirs, local_selection(&local), None, Ready::State).await;
    let token = running.token();

    let answer = local_request(&local, "/stop", Method::POST, Some(&token)).await;
    assert_eq!(answer.status, 202, "{}", answer.body);
    assert!(answer.body.contains("\"draining\""), "{}", answer.body);

    running.drain(&dirs).await;
}

#[tokio::test]
async fn metrics_is_open_on_the_local_channel() {
    let dirs = Dirs::new();
    let local = dirs.local();
    let source = Arc::new(Counting::default());
    let running = Running::start(
        &dirs,
        local_selection(&local),
        Some(gauges(&source, DEFAULT_TTL)),
        Ready::State,
    )
    .await;

    // No token, and the source is attached: the local channel serves the scrape. An unrouted
    // `/metrics` would answer `404` and a guarded one `401`, so the `200` is the rule's.
    let answer = local_request(&local, "/metrics", Method::GET, None).await;
    assert_eq!(
        answer.status, 200,
        "the local channel keeps `/metrics` open: {}",
        answer.body
    );
    // The scrape drove the cheap half: the storage scan is the reader task's, never a request's.
    // `>= 1` rather than `== 1`: the count is this leg's scrape's, but pinning it to exactly one
    // would couple the leg to the readiness probe (above) happening to hit `/status`.
    assert!(
        source.refreshes() >= 1,
        "the scrape refreshed the cheap half"
    );

    running.stop(&dirs).await;
}

#[tokio::test]
async fn metrics_without_a_gauge_source_is_404() {
    let dirs = Dirs::new();
    let local = dirs.local();
    let running = Running::start(&dirs, local_selection(&local), None, Ready::State).await;

    // The absence is served rather than an empty `200` (the contract's `/metrics` row), and it is
    // the handler's answer rather than an unrouted path's: the route is registered on every build.
    let answer = local_request(&local, "/metrics", Method::GET, None).await;
    assert_eq!(answer.status, 404, "{}", answer.body);
    assert!(
        answer.body.contains(r#""error""#) && answer.body.contains("metrics are not attached"),
        "the absence keeps this crate's error shape: {}",
        answer.body
    );

    running.stop(&dirs).await;
}

#[tokio::test]
async fn a_loopback_bind_opens_metrics_and_guards_everything_else() {
    let dirs = Dirs::new();
    let source = Arc::new(Counting::default());
    let selection = Selection {
        http: Some("127.0.0.1:0".parse().expect("addr")),
        ..Selection::default()
    };
    let running = Running::start(
        &dirs,
        selection,
        Some(gauges(&source, DEFAULT_TTL)),
        Ready::State,
    )
    .await;
    let addr = bound(dirs.dir());
    assert!(
        addr.ip().is_loopback(),
        "the token rule follows the bind: {addr}"
    );

    // A scraper cannot send a custom header, which is the whole reason `/metrics` is open here —
    // and that reason does not extend to the control endpoints or to the schema.
    let open = client::get_http(addr, get("/metrics", None))
        .await
        .expect("request");
    assert_eq!(open.status, 200, "a loopback bind keeps `/metrics` open");
    for path in ["/status", "/stop"] {
        let guarded = client::get_http(addr, get(path, None))
            .await
            .expect("request");
        assert_eq!(guarded.status, 401, "{path}");
    }
    #[cfg(feature = "openapi")]
    {
        let schema = client::get_http(addr, get("/openapi.json", None))
            .await
            .expect("request");
        assert_eq!(schema.status, 401, "/openapi.json");
    }
    let token = running.token();
    let allowed = client::get_http(addr, get("/status", Some(&token)))
        .await
        .expect("request");
    assert_eq!(allowed.status, 200, "the token opens the guarded endpoints");

    running.stop(&dirs).await;
}

/// The wildcard bind's recorded name, and the client that makes it dialable. `state.control_name`
/// carries the **bound** address — `0.0.0.0:<port>`, which is truthful and load-bearing, because
/// it is what `Auth::for_bind` and `doctor` reason from — and dialling that verbatim never
/// connects: Windows answers `WSAEADDRNOTAVAIL` for it (measured on this host) while
/// `127.0.0.1:<port>` connects, and `client::status` maps every dial failure to `running: false`.
/// So the leg is the whole CLI path, readiness probe included: `Ready::State` reads the file and
/// dials what it names, and `status()` reporting a **running** server here is the assertion the
/// missing loopback substitution fails. Measured against the pre-fix arm, that failure is the
/// readiness probe's (`the plane did not answer within 5s`), because every dial is refused —
/// which is the defect this leg exists to catch.
#[tokio::test]
async fn a_wildcard_bind_is_reported_running() {
    let dirs = Dirs::new();
    let selection = Selection {
        http: Some("0.0.0.0:0".parse().expect("addr")),
        ..Selection::default()
    };
    let running = Running::start(&dirs, selection, None, Ready::State).await;

    let recorded = bound(dirs.dir());
    assert!(
        recorded.ip().is_unspecified(),
        "`state` records what was bound, the wildcard included: {recorded}"
    );
    assert!(
        running.report.running,
        "the client dials loopback for an unspecified recorded address"
    );
    assert_eq!(running.status().port, Some(9000));

    // And the client's own stop path, over the same substituted address.
    running.stop(&dirs).await;
}

#[tokio::test]
async fn a_non_loopback_bind_guards_every_endpoint() {
    let dirs = Dirs::new();
    let source = Arc::new(Counting::default());
    let selection = Selection {
        http: Some("0.0.0.0:0".parse().expect("addr")),
        ..Selection::default()
    };
    let running = Running::start(
        &dirs,
        selection,
        Some(gauges(&source, DEFAULT_TTL)),
        Ready::Loopback,
    )
    .await;
    let dial = SocketAddr::from(([127, 0, 0, 1], bound(dirs.dir()).port()));
    let token = running.token();

    // Every registered endpoint, `/metrics` included: a reachable bind is the strict rule.
    for path in ["/metrics", "/status", "/stop"] {
        let guarded = client::get_http(dial, get(path, None))
            .await
            .expect("request");
        assert_eq!(guarded.status, 401, "{path}");
    }
    let tokened = client::get_http(dial, get("/metrics", Some(&token)))
        .await
        .expect("request");
    assert_eq!(tokened.status, 200, "the token opens the scrape here");

    running.end(&dirs).await;
}

#[cfg(feature = "openapi")]
#[tokio::test]
async fn the_document_is_served_on_the_local_channel() {
    let dirs = Dirs::new();
    let local = dirs.local();
    let running = Running::start(&dirs, local_selection(&local), None, Ready::State).await;

    let answer = local_request(&local, "/openapi.json", Method::GET, None).await;
    assert_eq!(
        answer.status, 200,
        "the local channel serves the schema openly: {}",
        answer.body
    );
    for path in ["/status", "/stop", "/metrics", "/openapi.json"] {
        assert!(
            answer.body.contains(path),
            "{path} is missing from the document"
        );
    }
    assert!(
        answer.body.contains("X-Tinio-Token"),
        "the token scheme is documented"
    );

    running.stop(&dirs).await;
}

/// The reader's wiring, not just its cadence: `Plane::serve` starts it, asserted through the plane
/// instead of `spawn_reader`. That it *exits* on the plane's own channel rather than being aborted
/// is `metrics.rs`'s unit test's to prove — a frozen counter cannot tell those two apart.
#[tokio::test]
async fn the_reader_starts_with_the_plane_and_stops_with_it() {
    let dirs = Dirs::new();
    let local = dirs.local();
    let source = Arc::new(Counting::default());
    let running = Running::start(
        &dirs,
        local_selection(&local),
        Some(gauges(&source, Duration::from_millis(20))),
        Ready::State,
    )
    .await;

    // Advanced while serving, and with no scrape at all: the reader scans once at start and then
    // once per ttl.
    sleep(Duration::from_millis(120)).await;
    let scanned = source.scans();
    assert!(
        scanned >= 2,
        "the reader scanned with the plane and kept its cadence: {scanned}"
    );

    running.stop(&dirs).await;

    // Frozen after the plane stopped, which is what this leg can show. A grace window first,
    // because the task stops on its own wake-up; *why* it stopped (the shared channel) is
    // `metrics.rs`'s unit test's assertion, not this one's.
    sleep(Duration::from_millis(60)).await;
    let frozen = source.scans();
    sleep(Duration::from_millis(120)).await;
    assert_eq!(
        source.scans(),
        frozen,
        "the reader stops with the plane rather than scanning on"
    );
}

/// The integration form of the no-scrape rule: a burst of `/metrics` requests leaves the scan
/// count where the reader's own cadence put it.
#[tokio::test]
async fn a_scrape_flood_does_not_run_the_storage_scan() {
    let dirs = Dirs::new();
    let local = dirs.local();
    let source = Arc::new(Counting::default());
    // A ttl far longer than the burst, so the reader's only scan is the one at start.
    let running = Running::start(
        &dirs,
        local_selection(&local),
        Some(gauges(&source, DEFAULT_TTL)),
        Ready::State,
    )
    .await;

    sleep(Duration::from_millis(80)).await;
    let scanned = source.scans();
    assert!(scanned >= 1, "the reader scanned once at start: {scanned}");

    for _ in 0..8 {
        let answer = local_request(&local, "/metrics", Method::GET, None).await;
        assert_eq!(answer.status, 200, "{}", answer.body);
    }
    assert_eq!(source.scans(), scanned, "no scrape scans the storage");
    assert!(
        source.refreshes() >= 8,
        "every scrape refreshes the cheap half: {}",
        source.refreshes()
    );

    running.stop(&dirs).await;
}

/// The unix stale-socket probe-then-reclaim, driven through the plane: a file left by a crashed
/// instance must not block a restart, and what the bind then records is the socket it bound.
#[cfg(unix)]
#[tokio::test]
async fn a_stale_socket_file_is_reclaimed_by_the_bind() {
    let dirs = Dirs::new();
    let socket = dirs.socket();
    // What a crashed instance leaves: the socket file with no listener behind it.
    let stale = StdUnixListener::bind(&socket).expect("stale socket");
    drop(stale);
    assert!(socket.exists(), "the residue is there before the bind");

    let local = LocalForm::Unix(socket.clone());
    let running = Running::start(&dirs, local_selection(&local), None, Ready::State).await;
    assert_eq!(
        running.report.state.as_ref().expect("state").control_name,
        socket.to_string_lossy(),
        "`state` records the socket the bind created"
    );

    running.stop(&dirs).await;
    assert!(!socket.exists(), "and `shutdown` removes it");
}

/// Both of the plane's files are private on unix: the socket's mode is set at the bind, the state
/// file's at its create.
#[cfg(unix)]
#[tokio::test]
async fn the_state_file_and_the_socket_are_private() {
    let dirs = Dirs::new();
    let local = dirs.local();
    let running = Running::start(&dirs, local_selection(&local), None, Ready::State).await;

    let state_mode = fs::metadata(state::path(dirs.dir()))
        .expect("metadata")
        .permissions()
        .mode();
    assert_eq!(state_mode & 0o777, 0o600, "the token lives in this file");
    let socket_mode = fs::metadata(dirs.socket())
        .expect("metadata")
        .permissions()
        .mode();
    assert_eq!(
        socket_mode & 0o777,
        0o600,
        "only the owner may talk to the control channel"
    );

    running.stop(&dirs).await;
}

/// The Windows channel's two claims, checked rather than trusted: a second instance cannot bind
/// the same name, and both the pipe and the state file carry a DACL that names only this user.
#[cfg(windows)]
#[tokio::test]
async fn the_pipe_refuses_a_second_instance_and_both_dacls_are_current_user_only() {
    use tinio_api::{_winsec::acl_is_current_user_only, dacl};

    let dirs = Dirs::new();
    let local = dirs.local();
    let running = Running::start(&dirs, local_selection(&local), None, Ready::State).await;
    let LocalForm::Pipe(name) = &local else {
        unreachable!("the Windows leg binds a pipe");
    };

    // The single-instance claim, as the CLI's second start meets it: the same name, refused.
    let (stop, _rx) = watch::channel(false);
    let refused = Plane::bind(Options {
        selection: local_selection(&local),
        state_dir: dirs.dir().to_path_buf(),
        root: dirs.root().to_path_buf(),
        host: "127.0.0.1".to_string(),
        stop,
        gauges: None,
    });
    let err = refused.expect_err("a second instance is refused");
    assert!(
        format!("{err}").contains("already"),
        "names the reason: {err}"
    );

    // The DACL that makes `Auth::Local`'s openness on this channel sound: read back from the pipe
    // the plane is serving, and from the file the token is in.
    assert!(
        dacl::pipe_acl_is_current_user_only(name).expect("read the pipe's DACL back"),
        "the control pipe grants the current user and nobody else"
    );
    assert!(
        acl_is_current_user_only(&state::path(dirs.dir())).expect("read the file's DACL back"),
        "the state file grants the current user and nobody else"
    );

    // Ended through the channel rather than by a second `client::stop` dial, and that is a
    // measurement rather than a convenience: **the loop creates an instance's successor only
    // when the runtime polls it again**, and `ClientOptions::open` does not wait for a free
    // instance — it answers `ERROR_PIPE_BUSY` when every existing one is connected. The
    // read-back above dials synchronously, so this leg is exactly the case with no await between
    // two dials; measured here, the second one is refused. The CLI's own stop path is the story
    // leg's and the `202` leg's, both of which reach it after a full round trip.
    running.end(&dirs).await;
}

/// Task 4's pipe arm, made real: the successor instance keeps the channel dialable for a second
/// connection, and a connection that is open when the plane stops is still answered — the drain,
/// not just the accept loop.
#[cfg(windows)]
#[tokio::test]
async fn the_pipe_serves_sequential_connections_and_answers_one_open_at_the_stop() {
    let dirs = Dirs::new();
    let local = dirs.local();
    let running = Running::start(&dirs, local_selection(&local), None, Ready::State).await;
    let token = running.token();

    // Two sequential connections, both answered: the loop creates the next instance after each
    // accept, so the pipe name never disappears between them.
    for nth in ["one", "two"] {
        let answer = local_request(&local, "/status", Method::GET, Some(&token)).await;
        assert_eq!(answer.status, 200, "connection {nth}: {}", answer.body);
        assert!(answer.body.contains("\"running\":true"), "{}", answer.body);
    }

    // A connection that is open when the stop fires. It is established and silent: the accept loop
    // has handed it to a serving task, and that task is parked on the request head.
    let LocalForm::Pipe(name) = &local else {
        unreachable!("the Windows leg binds a pipe");
    };
    let stream = ClientOptions::new()
        .open(name)
        .expect("dial the control pipe");
    let (mut sender, connection) = http1::handshake(TokioIo::new(stream))
        .await
        .expect("handshake");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    // The loop has observed the connect by now: the client's own connect completed above, and
    // this yields the runtime enough times for the accept to be taken.
    sleep(Duration::from_millis(100)).await;

    running.stop.send(true).expect("the plane is listening");
    // The drain is holding that connection: `serve_local` cannot have returned while a connection
    // it accepted is still open. Without the drain the task would go down with the loop's set.
    assert!(
        !running.serving.is_finished(),
        "the drain abandoned the connection the loop had accepted"
    );

    // ...and the answer arrives even though the accept loop has already stopped.
    let request = Request::builder()
        .method("GET")
        .uri("/status")
        .header("x-tinio-token", &token)
        .body(String::new())
        .expect("request");
    let response = sender
        .send_request(request)
        .await
        .expect("the drain answers the connection that was open when the stop fired");
    assert_eq!(response.status().as_u16(), 200, "answered, not dropped");

    // Only when the client lets go does the drain finish and `serve_local` return.
    drop(sender);
    running.drain(&dirs).await;
}
