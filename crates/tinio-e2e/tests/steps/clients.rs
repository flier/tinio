//! External-client harness and step definitions for the `@external`
//! scenarios (`@interop`/`@boto3`/`@mc`), ported from
//! `tinio-server/tests/e2e/mod.rs` plus the client-leg steps that drive
//! `journey.rs`/`advanced.rs`/`boto3.rs`/`mc.rs`/`edge.rs` and the CI
//! baseline `e2e/interop/*.sh`.
//!
//! The `#[before]` hook (steps/mod.rs) spawns the real `tinio` binary as a
//! subprocess — real redb database, scanner, sweep — bound to `127.0.0.1`
//! on an ephemeral port with no client-side addressing overrides (SC-002),
//! then drives third-party S3 clients (aws cli v2, rclone, mc, boto3)
//! against it. Process lifecycle is native Rust: `Child::kill` + `wait`
//! terminate the server synchronously (TerminateProcess on Windows), so the
//! redb lock is released before a sibling server starts.
//!
//! **A scenario may ask that server for a confinement** ([`Confinement`],
//! from its `@confinement`/`@read-only` tags): the same client journeys then
//! run through the confined server, which is the arm G19–G22 exist for. The
//! spawn site is unchanged — one `job::spawn` call takes every arm — and a
//! host that cannot honour the ask is a failure rather than a skip: the run
//! refuses at start and `spawn_child` reports the refusal it read.
//!
//! Config passthrough (grilling Q4): the spawned binary receives the same
//! tag→config mapping as the in-process hook (`config_from_tags` in
//! steps/mod.rs) — `Capabilities` overrides become the `--config <file>`
//! the old harness used (`[s3] checksum = true`, `[s3] max_buckets = 3`),
//! and the fs-scanner variant becomes `TINIO_SCANNER` (`@cold-listing` →
//! on, plain → off).
use std::{
    env, fs,
    fs::File,
    io::{BufRead, BufReader, Read},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    thread,
    time::Duration,
};

use assert_cmd::{Command as AssertCmd, cargo::cargo_bin};
use cucumber::{given, then, when};
use tinio_sandbox::Event;
use tinio_util::testing::{frame_line, job, kill_and_reap};
#[cfg(feature = "tier")]
use tokio::sync::SemaphorePermit;
use tokio::{task::spawn_blocking, time::sleep};

use super::{
    Backend, FsKind, World,
    cli::scratch_dir,
    common::{contains_ignore_case, deterministic_bytes},
    has_tag,
};
use crate::{
    _cli::config::{Config, s3, storage},
    _server::Capabilities,
    _util::env::{READ_ONLY_VAR, SANDBOX_VAR, STATE_DIR_VAR},
};

/// The fixed MinIO-convention credential pair the harness signs every
/// request with, handed to the child in the two variables the resolution
/// reads (`contracts/config.md:110`): the CLI serves `[auth]`, and a first
/// start *generates* a random pair into the config it creates, so without
/// these every scenario would be refused with `SignatureDoesNotMatch`.
pub const ACCESS_KEY: &str = "minioadmin";
pub const SECRET_KEY: &str = "minioadmin";

/// How long a spawned child gets to report the address it bound before the
/// scenario fails rather than hanging on a pipe that will never carry it.
const START_TIMEOUT: Duration = Duration::from_secs(30);

/// The same bound for a **confined** child — the sibling pair `cli.rs` carries
/// (`RUN_TIMEOUT` 30 s against `CONFINED_RUN_TIMEOUT` 180 s), and the same number for the same
/// reason.
///
/// **The grant is what a plain budget cannot hold.** On the launcher platforms the ACE grant
/// precedes the child's bind, so it sits *inside* this wait: `cli.rs`'s doc records the measured
/// 32.3–37.8 s per grant on NTFS against a 30 s readiness budget, with one host past 120 s. A
/// 30 s bound would therefore read a slow grant as a child that never reported — and the panic
/// below would send the reader after a stale binary instead. A third number would be a fourth
/// home for a fact `docs/tests.md` ("confined timeout separate from normal") already owns, so the
/// confined arms take the sibling's.
const CONFINED_START_TIMEOUT: Duration = Duration::from_secs(180);

/// The workspace target dir (`CARGO_TARGET_DIR` override honored).
fn target_dir() -> PathBuf {
    if let Ok(dir) = env::var("CARGO_TARGET_DIR") {
        return PathBuf::from(dir);
    }
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../../target");
    p
}

/// The confinement an `@external` scenario asks its spawned server for — the CLI's own spelling of
/// the two flags (`contracts/cli.md`), or no flag at all.
///
/// **The ask is a tag, not a step, because the spawn comes first.** The `#[before]` hook starts
/// the server before any scenario step runs, so nothing a step says could change what was
/// spawned. `@confinement` is the ask this suite already owns (`steps/tag_filter.rs` selects the
/// host-bound scenarios off it, and the platform gates opt in), and `@read-only` is FR-023's mode
/// tag — the one the in-process `read_only.feature` scenarios carry. The hook's own `@read-only`
/// branch is never reached for an `@external` scenario (its external branch returns first), which
/// is why that tag's meaning for this arm lives here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confinement {
    /// No flag: the plain server every `@external` scenario without `@confinement` runs against.
    Plain,
    /// `--sandbox` (G19–G21).
    Sandbox,
    /// `--read-only --sandbox` (G22; `contracts/s3-surface.md` §Read-only mode).
    ReadOnly,
}

impl Confinement {
    /// The arm `tags` ask for: `@confinement` alone is `--sandbox`, `@confinement` with
    /// `@read-only` is the read-only pair, and no `@confinement` is the plain server.
    pub fn from_tags(tags: &[String]) -> Self {
        if !has_tag(tags, "confinement") {
            return Self::Plain;
        }
        if has_tag(tags, "read-only") {
            Self::ReadOnly
        } else {
            Self::Sandbox
        }
    }

    /// The flags this arm adds, in the CLI's own spelling — the pair `--read-only --sandbox` in the
    /// order the plan's G22 row and `confinement.feature`'s G9 both write them.
    fn flags(self) -> &'static [&'static str] {
        match self {
            Self::Plain => &[],
            Self::Sandbox => &["--sandbox"],
            Self::ReadOnly => &["--read-only", "--sandbox"],
        }
    }

    /// How long this arm's child may take to report readiness — [`START_TIMEOUT`] for the plain
    /// server, [`CONFINED_START_TIMEOUT`] once the spawn asks for a confinement.
    fn readiness_budget(self) -> Duration {
        match self {
            Self::Plain => START_TIMEOUT,
            Self::Sandbox | Self::ReadOnly => CONFINED_START_TIMEOUT,
        }
    }
}

/// @external scenarios only: a spawned `tinio server` + one client
/// session. The child is killed on drop (synchronously, so the redb lock
/// is released before a sibling server starts).
#[derive(Debug)]
pub struct External {
    /// The `tinio server` subprocess (killed on drop).
    pub child: Child,
    /// `http://127.0.0.1:<port>` — the client-facing base URL.
    pub base_url: String,
    /// The endpoint the aws steps dial (`--endpoint-url`): [`Self::base_url`] until a scenario
    /// points the client at the ephemeral second server (`the client targets the {word} server`),
    /// which is the only observation point that server has.
    target_url: String,
    /// Scratch for client files (uploads, downloads, configs).
    pub workdir: tempfile::TempDir,
    /// The served storage root (`workdir/root`) — out-of-band file drops
    /// (cold listing, pagination truncation) write under it.
    pub root: PathBuf,
    /// The scanner toggle the child was spawned with — replayed on a restart.
    scanner: Option<bool>,
    /// The `--config` file the child was spawned with (`None` when no knob was set) — replayed on
    /// a restart, and the tier server's `[storage]`/`[storage.pg]`/`[storage.tier]` document.
    config: Option<PathBuf>,
    /// The CLI flags the child was spawned with — replayed on a restart.
    confinement: Confinement,
    /// The relocated state dir (the read-only arm alone) — replayed on a restart.
    state: Option<PathBuf>,
    /// The shared database's exclusive permit, held for the scenario's life: a writable tier
    /// server owns the database (one root per database, T18), so the spawned tier processes are
    /// serialized here rather than by a `--concurrency` flag the harness cannot check.
    #[cfg(feature = "tier")]
    _tier_permit: Option<SemaphorePermit<'static>>,
}

impl External {
    /// Spawn the binary per `caps`/`fs_kind`/`confinement` — the tag→config
    /// mapping from `config_from_tags` (steps/mod.rs), translated into the
    /// old harness's mechanisms: `Capabilities` overrides become a
    /// `--config <file>` (`[s3] checksum = true`, `[s3] max_buckets = 3`),
    /// the fs-scanner variant becomes `TINIO_SCANNER` (`@cold-listing` →
    /// on, plain → off), and `confinement` becomes the CLI's own flags.
    ///
    /// **The read-only arm relocates the state dir.** FR-023 refuses an in-root state dir in that
    /// mode (it would write the tree the mode promises to leave alone), so the run resolves
    /// `TINIO_STATE_DIR` instead — the directory [`SpawnedServer`]'s plain arm never needs, which
    /// is why it is created here and handed to the child by [`spawn_child`]. The confinement's
    /// exposure set carries it for the same reason the served root is carried
    /// (`sandbox::product::exposures`), so the confined child can write where an unconfined one
    /// would have written `<root>/.tinio`.
    ///
    /// **The workdir is the harness's [`scratch_dir`]**, not a bare tempdir: the child's control
    /// socket is `<root>/.tinio/tinio-<16 hex>.sock`, and on macOS `$TMPDIR` is too long for
    /// `sun_path` — the server refuses to start (that helper owns the rationale).
    ///
    /// **The tier arm is a spawned *product* start, not the in-process testing constructor**
    /// (T25): `[storage] type = "tier"` with `[storage.pg]` and `[storage.tier]` travels in the
    /// `--config` file, and the cold plane's dsn is the inherited `TINIO_PG_DSN` — the CLI's own
    /// resolution, so the leg exercises the startup an operator runs. A writable tier start owns
    /// its database (T18), so this arm takes the shared pg permit and truncates the database
    /// before the spawn: the permit serializes it with every other pg/tier scenario, and the
    /// truncate gives the fresh root the empty database its first `open_pg` bind requires.
    pub async fn start(
        backend: Backend,
        caps: &Capabilities,
        fs_kind: FsKind,
        confinement: Confinement,
    ) -> Self {
        let workdir = scratch_dir();
        let root = workdir.path().join("root");
        fs::create_dir_all(&root).unwrap();
        let state = (confinement == Confinement::ReadOnly).then(|| {
            let path = workdir.path().join("state");
            fs::create_dir_all(&path).unwrap();
            path
        });
        #[cfg(feature = "tier")]
        let tier = backend == Backend::Tier;
        #[cfg(not(feature = "tier"))]
        let tier = {
            let _ = backend;
            false
        };
        #[cfg(feature = "tier")]
        let tier_permit = if tier {
            let permit = super::common::pg_permit().await;
            super::common::reset_pg_database().await;
            // The dsn is the child's inherited environment; reading it here makes an unset
            // variable this scenario's error rather than a refused child with no cause in its
            // stderr (which `spawn_child` sends to the null device).
            let _ = super::common::pg_dsn();
            Some(permit)
        } else {
            None
        };
        let config = config_for(caps, tier).map(|text| {
            let path = workdir.path().join("config.toml");
            fs::write(&path, text).unwrap();
            path
        });
        let scanner = Some(matches!(fs_kind, FsKind::ColdListing(_)));
        let (child, endpoint) = spawn_child(
            &root,
            scanner,
            config.as_deref(),
            confinement,
            state.as_deref(),
        );
        let base_url = format!("http://{endpoint}");
        Self {
            child,
            base_url: base_url.clone(),
            target_url: base_url,
            workdir,
            root,
            scanner,
            config,
            confinement,
            state,
            #[cfg(feature = "tier")]
            _tier_permit: tier_permit,
        }
    }

    /// Restart the spawned server on the same root and configuration — the tier durability leg
    /// (`the external tier server is restarted`), where the reopened process must find the same
    /// index, the same pg rows and the same root identity.
    ///
    /// **The stop is the product's own, not a kill.** `tinio stop <root>` asks the plane to
    /// cease accepting and drain (FR-018), which is the restart an operator performs; a
    /// terminated process skips the drain the warm plane's write path and the layer's tasks
    /// rely on, so the leg would assert crash tolerance the scenario does not claim. The
    /// process-exit wait is bounded because `stop` confirms the control channel went away, not
    /// necessarily that the process has returned.
    ///
    /// **The short wait after it is the database's.** A writable tier bind takes the store's
    /// ownership advisory lock (T18); the stopped process's session is closed by the server just
    /// after it exits, so the respawn waits out that window rather than reading the still-held
    /// lock as a refusal.
    pub async fn restart(&mut self) {
        let root = self.root.clone();
        let output = spawn_blocking(move || {
            // The stop client resolves the same state the server wrote, and the four variables
            // that would change what it resolves are cleared exactly as [`spawn_child`] clears
            // them for the server itself: a shell exporting one of them must not make the stop
            // probe a different root or mode than the child it is stopping.
            let mut stop = Command::new(cargo_bin("tinio"));
            stop.arg("stop")
                .arg(&root)
                .env_remove("TINIO_SCANNER")
                .env_remove(READ_ONLY_VAR)
                .env_remove(STATE_DIR_VAR)
                .env_remove(SANDBOX_VAR);
            stop.output()
        })
        .await
        .expect("the stop task runs")
        .expect("run `tinio stop`");
        assert!(
            output.status.success(),
            "`tinio stop` failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        for _ in 0..150 {
            if self
                .child
                .try_wait()
                .expect("the spawned server's status")
                .is_some()
            {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        // A server that outlived the stop's own confirmation is ended the hard way, so the respawn
        // cannot race a second owner of the index and the database.
        terminate(&mut self.child);
        sleep(Duration::from_millis(250)).await;
        let (child, endpoint) = spawn_child(
            &self.root,
            self.scanner,
            self.config.as_deref(),
            self.confinement,
            self.state.as_deref(),
        );
        self.child = child;
        self.base_url = format!("http://{endpoint}");
        self.target_url = self.base_url.clone();
    }
}

impl Drop for External {
    fn drop(&mut self) {
        terminate(&mut self.child);
    }
}

/// Synchronously kill + reap a spawned serve child (TerminateProcess on
/// Windows): the redb lock is released before a sibling server can start
/// on the same root. One home for the teardown ordering rationale — the
/// Drop impls of [`External`]/[`SpawnedServer`] and the not-ready panic
/// path all call it.
///
/// The sequence itself is [`kill_and_reap`]'s, spelled once for the whole
/// workspace: this name stays because the ordering rule above is this
/// harness's and not the shared helper's.
fn terminate(child: &mut Child) {
    kill_and_reap(child);
}

/// A running serve subprocess on an ephemeral loopback port (killed on
/// drop — the redb lock is released before a sibling server can start on
/// the same root).
#[derive(Debug)]
pub struct SpawnedServer {
    child: Child,
    endpoint: SocketAddr,
}

/// Spawn `tinio server <root> --port 0` and wait for its readiness event;
/// returns the child and the bound endpoint (`127.0.0.1:PORT`). `scanner`
/// sets `TINIO_SCANNER` per Some (None leaves it unset), `config` is passed
/// through `--config`, `confinement` adds the CLI's own flags, and
/// `state_dir` sets `TINIO_STATE_DIR` (`Some` for the read-only arm alone —
/// see [`External::start`]).
///
/// **The wait is the arm's own budget** ([`Confinement::readiness_budget`]):
/// a confined child's readiness window has to hold the launcher's ACE grant
/// as well as the bind, so the confined arms take the sibling constant
/// ([`CONFINED_START_TIMEOUT`]) rather than the plain one.
///
/// The child inherits this process's environment, so the four variables
/// that would change what it serves are cleared first: a shell exporting
/// `TINIO_SCANNER` (the harness decides the scanner, not the caller
/// shell), `TINIO_READ_ONLY` (FR-023, which would otherwise put every
/// spawned server in read-only mode), `TINIO_STATE_DIR` (which would
/// relocate its state), and `TINIO_SANDBOX` (which would confine every
/// spawned server, while each arm asks for confinement through its own
/// flag). The `scanner` and `state_dir` arguments set their variables back
/// afterwards.
///
/// **The credential pair is one thing the spawned server needs and the
/// retired example did not.** Every client below signs with the fixed
/// `minioadmin` pair, which the example accepted because it hard-coded it —
/// while the CLI reads `[auth]`, and a first start *generates* a random
/// pair into the config it creates, so a scenario would fail with
/// `SignatureDoesNotMatch` the moment the server under test changed.
/// `TINIO_ACCESS_KEY`/`TINIO_SECRET_KEY` are the two names
/// `contracts/config.md:110` lists (not the `MINIO_*` names, which are the
/// data plane's fallback and not this chain's), and the resolution applies
/// them to `[auth]` — env, then `.env`, then the config file.
///
/// **The child is spawned through the job guard** (`tinio_util::testing::job::spawn`): on Windows
/// it is put in this test process's job, so a leg that dies **without unwinding** — the abort or
/// the killed run that no `Drop` reaches — takes the server with it instead of leaving an orphan
/// holding the redb lock and `target\<profile>\tinio.exe`. [`terminate`] and the two `Drop` impls
/// stay what they were: the guard is the backstop for the path that runs no `Drop`, never the
/// reaping.
fn spawn_child(
    root: &Path,
    scanner: Option<bool>,
    config: Option<&Path>,
    confinement: Confinement,
    state_dir: Option<&Path>,
) -> (Child, SocketAddr) {
    let mut cmd = Command::new(cargo_bin("tinio"));
    cmd.arg("server").arg(root).args(["--port", "0"]);
    cmd.args(confinement.flags());
    if let Some(config) = config {
        cmd.arg("--config").arg(config);
    }
    cmd.env_remove("TINIO_SCANNER")
        .env_remove(READ_ONLY_VAR)
        .env_remove(STATE_DIR_VAR)
        .env_remove(SANDBOX_VAR)
        .env("TINIO_ACCESS_KEY", ACCESS_KEY)
        .env("TINIO_SECRET_KEY", SECRET_KEY);
    if let Some(state) = state_dir {
        cmd.env(STATE_DIR_VAR, state);
    }
    if let Some(s) = scanner {
        cmd.env("TINIO_SCANNER", if s { "1" } else { "0" });
    }
    cmd.stdout(Stdio::piped()).stderr(Stdio::null());
    let mut child =
        job::spawn(&mut cmd).unwrap_or_else(|e| panic!("failed to spawn `tinio server`: {e}"));
    let stdout = child.stdout.take().expect("piped stdout");
    let budget = confinement.readiness_budget();
    let endpoint = match wait_for_ready(stdout, budget) {
        // The address travels as the value the child bound: it comes out of the event's typed
        // field and goes to a URL as it stands, so nothing here can disagree with what
        // `local_addr` reported.
        Some(Event::Listening { addr }) => addr,
        // **A refusal is reported as the refusal it is.** Discarding the event and reporting the
        // missing-frame case below made a server that refused to start read as the stale-binary
        // guess, which sends the reader after the wrong thing.
        Some(Event::Error { message }) => {
            terminate(&mut child);
            panic!("the server refused to start: {message}");
        }
        None => {
            terminate(&mut child);
            panic!(
                "the server reported no `listening` event within {budget:?} — is a stale \
                 `tinio.exe` holding this root? ({} server --port 0)",
                cargo_bin("tinio").display()
            );
        }
    };
    (child, endpoint)
}

impl SpawnedServer {
    /// Serve `root` (caller keeps it) on `--port 0`; see [`spawn_child`].
    ///
    /// **Always plain, and that is a decision rather than an omission**: this server exists for
    /// the ephemeral `--port 0` leg (`the client targets the {word} server`), whose subject is the
    /// port a run reports — not one of the external-client legs (G19–G22), so no confined
    /// scenario reaches it. A scenario that confined *both* servers would be a second ask, and
    /// none exists.
    fn start(root: &Path, scanner: Option<bool>, config: Option<&Path>) -> Self {
        let (child, endpoint) = spawn_child(root, scanner, config, Confinement::Plain, None);
        Self { child, endpoint }
    }

    /// The bound address, `127.0.0.1:PORT`.
    fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }
}

impl Drop for SpawnedServer {
    fn drop(&mut self) {
        // See [`terminate`] for the redb-lock ordering rationale.
        terminate(&mut self.child);
    }
}

/// Read the child's stdout until it reports its first event.
///
/// The child speaks the sandbox's event stream (design decision 19): one SSE frame per event —
/// an `event:` line, a `data:` line, closed by a blank line. **A frame is not a line**, and the
/// accumulation this reads with is [`frame_line`]'s — the one home for the rule
/// (`tinio_util::testing`), which `steps/cli.rs`'s whole-stream reader is written on too. Text
/// that is not a frame — a log a dependency wrote before the subscriber was installed — does not
/// decode and is skipped rather than fatal, which is the property the framing buys. A reader
/// thread keeps the pipe drained so the child never blocks on a full buffer; the thread exits
/// when the child dies.
///
/// `budget` is the arm's own ([`Confinement::readiness_budget`]): a confined child's window has to
/// hold the launcher's ACE grant as well as the bind, which is the distinction `cli.rs`'s
/// `RUN_TIMEOUT`/`CONFINED_RUN_TIMEOUT` pair owns and this one follows.
fn wait_for_ready(stdout: impl Read + Send + 'static, budget: Duration) -> Option<Event> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let mut frame = String::new();
        while reader.read_line(&mut line).is_ok_and(|n| n > 0) {
            if let Some(event) = frame_line(&mut frame, &line, Event::decode) {
                let _ = tx.send(event);
                return;
            }
            line.clear();
        }
    });
    rx.recv_timeout(budget).ok()
}

/// The config text for capability overrides and the tier arm (grilling Q4): the spawned server
/// receives the same tag→config mapping as the in-process hook, serialized through the config
/// schema's own serde types — one home for the knob list (a knob the schema gains is a knob the
/// scenarios can set). None when every knob sits at its default (no file).
///
/// `tier` selects the tiered backend: `[storage] type = "tier"` needs `[storage.pg]` (the cold
/// plane; its dsn is the child's inherited `TINIO_PG_DSN`) and takes `[storage.tier]`'s defaults,
/// narrowed so no background pass acts inside a scenario — the IA PUT demotes synchronously
/// (T10), which is the placement the scenarios assert. A non-tier scenario keeps the old
/// shape: no file unless a capability knob is set.
fn config_for(caps: &Capabilities, tier: bool) -> Option<String> {
    if !tier && *caps == Capabilities::default() {
        return None;
    }
    let storage = tier.then(|| storage::Config {
        backend: storage::Backend::Tier,
        pg: Some(storage::pg::Config::default()),
        tier: Some(storage::Tier {
            scan_interval: Duration::from_secs(3600),
            demote_idle: Duration::from_secs(3600),
            stats_flush: Duration::from_secs(3600),
            ..Default::default()
        }),
        ..Default::default()
    });
    let config = Config {
        s3: Some(s3::Config {
            capabilities: *caps,
            ..Default::default()
        }),
        storage,
        ..Default::default()
    };
    Some(toml::to_string(&config).expect("the scenario config serializes to toml"))
}

/// aws cli v2, pre-wired for path-style SigV4 against `base_url`.
fn aws(base_url: &str, args: &[&str]) -> AssertCmd {
    let mut c = AssertCmd::new("aws");
    c.env("AWS_ACCESS_KEY_ID", ACCESS_KEY)
        .env("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .arg("--endpoint-url")
        .arg(base_url)
        .arg("--region")
        .arg("us-east-1")
        .args(args);
    c
}

/// (Re)create the `tinio` rclone remote pointing at `base_url`.
///
/// rclone config is isolated to `config` per scenario: scenarios run in
/// parallel under cargo, and the bash harness used to race the user's
/// `~/.config/rclone/rclone.conf` (and clobber its `tinio` remote).
#[derive(Debug)]
struct Rclone {
    config: PathBuf,
}

impl Rclone {
    /// Use `config` (a path inside the scenario's scratch dir) as the
    /// config file for every rclone invocation.
    fn new(config: PathBuf) -> Self {
        Self { config }
    }

    /// `rclone --config <config> <args>`.
    fn cmd(&self, args: &[&str]) -> AssertCmd {
        let mut c = AssertCmd::new("rclone");
        c.arg("--config").arg(&self.config).args(args);
        c
    }

    /// (Re)create the `tinio` remote pointing at `base_url`.
    fn remote(&self, base_url: &str) -> AssertCmd {
        let mut c = self.cmd(&[
            "config",
            "create",
            "tinio",
            "s3",
            "provider",
            "Minio",
            "access_key_id",
            ACCESS_KEY,
            "secret_access_key",
            SECRET_KEY,
        ]);
        c.arg("endpoint").arg(base_url);
        c
    }
}

/// `mc` with an isolated config dir (same rationale as [`Rclone`]).
#[derive(Debug)]
struct Mc {
    config_dir: PathBuf,
}

impl Mc {
    fn new(config_dir: PathBuf) -> Self {
        Self { config_dir }
    }

    /// `mc --config-dir <dir> <args>`.
    fn cmd(&self, args: &[&str]) -> AssertCmd {
        let mut c = AssertCmd::new("mc");
        c.arg("--config-dir").arg(&self.config_dir).args(args);
        c
    }

    /// (Re)set the `tinio` alias pointing at `base_url`.
    fn alias(&self, base_url: &str) -> AssertCmd {
        let mut c = self.cmd(&["alias", "set", "tinio"]);
        c.arg(base_url).args([ACCESS_KEY, SECRET_KEY]);
        c
    }
}

/// The venv python for the boto3 scenario. boto3 must run inside an
/// isolated venv (never the system python): `TINIO_BOTO3_PYTHON`
/// overrides the conventional `<target>/tinio-e2e-venv` venv.
fn boto3_python() -> PathBuf {
    if let Some(p) = env::var_os("TINIO_BOTO3_PYTHON") {
        return PathBuf::from(p);
    }
    let mut p = target_dir().join("tinio-e2e-venv");
    p.push(if cfg!(windows) { "Scripts" } else { "bin" });
    p.push(if cfg!(windows) {
        "python.exe"
    } else {
        "python3"
    });
    p
}

/// Run a boto3 scenario script against `endpoint` (best-effort client:
/// needs the venv from [`boto3_python`]).
///
/// The harness's fixed server pair is exported to the script explicitly: the
/// spawned server is forced to `ACCESS_KEY`/`SECRET_KEY`, so inheriting a
/// shell's `TINIO_ACCESS_KEY`/`TINIO_SECRET_KEY` could sign with a different
/// pair and fail every call with `SignatureDoesNotMatch`. A manual run of the
/// script keeps its own env/fallback.
fn boto3(endpoint: &str, script: &Path) -> AssertCmd {
    let mut c = AssertCmd::new(boto3_python());
    c.arg(script)
        .arg(endpoint)
        .env("TINIO_ACCESS_KEY", ACCESS_KEY)
        .env("TINIO_SECRET_KEY", SECRET_KEY);
    c
}

/// The checked-in boto3 scenario scripts stay in `tinio-server/tests/`
/// (one copy, shared with the old tests until they are deleted).
fn boto3_script(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../tinio-server/tests");
    p.push(name);
    p
}

/// Assert two files are byte-identical (upload/download round-trips): a
/// size compare first, then a chunked stream compare — a full read of
/// both multi-MB files never happens when the sizes differ.
fn files_equal(a: &Path, b: &Path) {
    let ma = fs::metadata(a).unwrap();
    let mb = fs::metadata(b).unwrap();
    assert_eq!(
        ma.len(),
        mb.len(),
        "files differ: {} vs {} (sizes {} vs {})",
        a.display(),
        b.display(),
        ma.len(),
        mb.len()
    );
    let mut fa = File::open(a).unwrap();
    let mut fb = File::open(b).unwrap();
    let (mut buf_a, mut buf_b) = ([0u8; 64 * 1024], [0u8; 64 * 1024]);
    loop {
        let na = fa.read(&mut buf_a).unwrap();
        let nb = fb.read(&mut buf_b).unwrap();
        if na != nb || buf_a[..na] != buf_b[..nb] {
            panic!("files differ: {} vs {}", a.display(), b.display());
        }
        if na == 0 {
            return;
        }
    }
}

/// Whether `name` (an executable, e.g. `aws`) is on PATH.
fn on_path(name: &str) -> bool {
    let Some(path) = env::var_os("PATH") else {
        return false;
    };
    let names: Vec<String> = if cfg!(windows) {
        vec![
            format!("{name}.exe"),
            format!("{name}.cmd"),
            format!("{name}.bat"),
            name.to_string(),
        ]
    } else {
        vec![name.to_string()]
    };
    env::split_paths(&path).any(|dir| names.iter().any(|n| dir.join(n).is_file()))
}

/// The `#[before]` hook's presence checks for the @external scenario
/// families (a missing client panics with a filter/setup hint, mirroring
/// the old `#[ignore]` semantics: `@interop` is CI-gated, `@boto3`/`@mc`
/// are manual).
pub fn check_presence(tags: &[String]) {
    let tagged = |t: &str| has_tag(tags, t);
    if tagged("interop") {
        for tool in ["aws", "rclone"] {
            assert!(
                on_path(tool),
                "`{tool}` not found on PATH — the @interop scenarios need aws cli v2 and \
                 rclone; run them in WSL2 or filter them out (`--tags 'not @interop'`)"
            );
        }
    }
    if tagged("boto3") {
        let python = boto3_python();
        assert!(
            python.exists(),
            "boto3 venv python not found at {} — create it and install boto3:\n\
             python3 -m venv <target>/tinio-e2e-venv && <venv>/pip install boto3\n\
             (or point TINIO_BOTO3_PYTHON at your own venv python)",
            python.display()
        );
    }
    if tagged("mc") {
        assert!(
            on_path("mc"),
            "`mc` not found on PATH — filter it out (`--tags 'not @mc'`)"
        );
    }
}

/// The scenario's scratch path for `name` (may contain `/` for subdirs).
fn scratch(world: &World, name: &str) -> PathBuf {
    let ext = world.ext.as_ref().expect("external server running");
    ext.workdir.path().join(name)
}

/// Split a client command line into argv: whitespace-separated words;
/// double-quoted segments stay one word (quotes stripped — the shell
/// convention the old bash scripts used). `{work}` expands to the scratch
/// workdir, `{captured}` to the last captured client output, and
/// `{upload-id}` to the captured multipart upload id (the tier journey's
/// initiate-then-part-copy pair needs two values alive at once).
fn tokenize(world: &World, command: &str) -> Vec<String> {
    let work = world
        .ext
        .as_ref()
        .expect("external server running")
        .workdir
        .path()
        .to_string_lossy()
        .into_owned();
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    for c in command.chars() {
        match c {
            '"' => in_quotes = !in_quotes,
            c if c.is_whitespace() && !in_quotes => {
                if !cur.is_empty() {
                    args.push(
                        cur.replace("{work}", &work)
                            .replace("{captured}", &world.ext_captured)
                            .replace("{upload-id}", &world.ext_upload_id),
                    );
                    cur.clear();
                }
            }
            _ => cur.push(c),
        }
    }
    if !cur.is_empty() {
        args.push(
            cur.replace("{work}", &work)
                .replace("{captured}", &world.ext_captured)
                .replace("{upload-id}", &world.ext_upload_id),
        );
    }
    args
}

/// The tokenized command as `&str` args (the wrapper signatures).
fn arg_refs(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}

/// Store the last external client run's stdout/stderr for the Then steps.
fn save_output(world: &mut World, out: &Output) {
    world.ext_output = String::from_utf8_lossy(&out.stdout).into_owned();
    world.ext_error = String::from_utf8_lossy(&out.stderr).into_owned();
}

// --- run steps (registered under every keyword a feature uses) ----------

/// Run `cmd`, assert its exit status matches `expect_success`, and store
/// the output for the Then steps — the tail every client-run step shares
/// (the run steps differ only in the wrapper they build).
fn run_external(world: &mut World, cmd: &mut AssertCmd, expect_success: bool) {
    let assert = cmd.assert();
    let out = if expect_success {
        assert.success()
    } else {
        assert.failure()
    }
    .get_output()
    .clone();
    save_output(world, &out);
}

/// The configured wrapper for a `{work}`-expanded aws command line, dialled at the scenario's
/// **current** target — the first server unless a step switched it.
fn aws_cmd(world: &World, command: &str) -> AssertCmd {
    let ext = world.ext.as_ref().expect("external server running");
    aws(&ext.target_url, &arg_refs(&tokenize(world, command)))
}

#[given(regex = r"I run aws (.*)")]
#[when(regex = r"I run aws (.*)")]
#[then(regex = r"I run aws (.*)")]
async fn run_aws(world: &mut World, command: String) {
    let mut cmd = aws_cmd(world, &command);
    run_external(world, &mut cmd, true);
}

/// A run that must fail (the error-path legs): the client's stderr lands
/// in `world.ext_error` for `the external client error contains …`.
/// Constrained to `s3…` subcommands so the wrong-credentials variant
/// below stays unambiguous (cucumber-rs panics on overlapping matches;
/// the regex crate has no lookahead). Extend the class for a future
/// non-s3 command instead of widening to `.*`.
#[given(regex = r"I try aws (s3[a-z]*(?: .*)?)")]
#[when(regex = r"I try aws (s3[a-z]*(?: .*)?)")]
#[then(regex = r"I try aws (s3[a-z]*(?: .*)?)")]
async fn try_aws(world: &mut World, command: String) {
    let mut cmd = aws_cmd(world, &command);
    run_external(world, &mut cmd, false);
}

/// A failing run signed with wrong credentials (US3-AS2): the server must
/// reject the request with the framework's auth error and perform no
/// operation — aws prints `InvalidAccessKeyId` / `SignatureDoesNotMatch`
/// to stderr for a 403.
#[given(regex = r"I try aws with wrong credentials (.*)")]
#[when(regex = r"I try aws with wrong credentials (.*)")]
#[then(regex = r"I try aws with wrong credentials (.*)")]
async fn try_aws_wrong_credentials(world: &mut World, command: String) {
    let mut cmd = aws_cmd(world, &command);
    cmd.env("AWS_ACCESS_KEY_ID", "wrong-access-key")
        .env("AWS_SECRET_ACCESS_KEY", "wrong-secret-key");
    run_external(world, &mut cmd, false);
}

#[given(regex = r"I run rclone (.*)")]
#[when(regex = r"I run rclone (.*)")]
#[then(regex = r"I run rclone (.*)")]
async fn run_rclone(world: &mut World, command: String) {
    let ext = world.ext.as_ref().expect("external server running");
    let mut cmd = Rclone::new(ext.workdir.path().join("rclone.conf"))
        .cmd(&arg_refs(&tokenize(world, &command)));
    run_external(world, &mut cmd, true);
}

/// A failing `rclone` run — the read-only legs' shape, and the twin of `I try aws …`. Separate
/// from `I run rclone …` rather than a flag on it: the exit status is the assertion (`assert.failure()`
/// vs `assert.success()`), and a step that took an expectation as a parameter would let a scenario
/// spell the wrong one.
#[given(regex = r"I try rclone (.*)")]
#[when(regex = r"I try rclone (.*)")]
#[then(regex = r"I try rclone (.*)")]
async fn try_rclone(world: &mut World, command: String) {
    let ext = world.ext.as_ref().expect("external server running");
    let mut cmd = Rclone::new(ext.workdir.path().join("rclone.conf"))
        .cmd(&arg_refs(&tokenize(world, &command)));
    run_external(world, &mut cmd, false);
}

#[given(expr = "I configure the rclone remote")]
#[when(expr = "I configure the rclone remote")]
#[then(expr = "I configure the rclone remote")]
async fn configure_rclone(world: &mut World) {
    let ext = world.ext.as_ref().expect("external server running");
    let mut cmd = Rclone::new(ext.workdir.path().join("rclone.conf")).remote(&ext.base_url);
    run_external(world, &mut cmd, true);
}

#[given(regex = r"I run mc (.*)")]
#[when(regex = r"I run mc (.*)")]
#[then(regex = r"I run mc (.*)")]
async fn run_mc(world: &mut World, command: String) {
    let ext = world.ext.as_ref().expect("external server running");
    let mut cmd = Mc::new(ext.workdir.path().join("mc")).cmd(&arg_refs(&tokenize(world, &command)));
    run_external(world, &mut cmd, true);
}

/// A failing `mc` run — the twin of `I try rclone …` above.
#[given(regex = r"I try mc (.*)")]
#[when(regex = r"I try mc (.*)")]
#[then(regex = r"I try mc (.*)")]
async fn try_mc(world: &mut World, command: String) {
    let ext = world.ext.as_ref().expect("external server running");
    let mut cmd = Mc::new(ext.workdir.path().join("mc")).cmd(&arg_refs(&tokenize(world, &command)));
    run_external(world, &mut cmd, false);
}

#[given(expr = "I configure the mc alias")]
#[when(expr = "I configure the mc alias")]
#[then(expr = "I configure the mc alias")]
async fn configure_mc(world: &mut World) {
    let ext = world.ext.as_ref().expect("external server running");
    let mut cmd = Mc::new(ext.workdir.path().join("mc")).alias(&ext.base_url);
    run_external(world, &mut cmd, true);
}

/// The venv-python command for a checked-in boto3 script, dialled at the scenario's server (the
/// script takes the endpoint as its first argument) — the plumbing `I run the boto3 script …` and
/// its read-only twin share, so the endpoint and the missing-script report have one home.
fn boto3_cmd(world: &World, name: &str) -> AssertCmd {
    let ext = world.ext.as_ref().expect("external server running");
    let script = boto3_script(name);
    assert!(
        script.exists(),
        "boto3 script not found at {}",
        script.display()
    );
    let endpoint = ext
        .base_url
        .strip_prefix("http://")
        .expect("base url has the http:// prefix");
    boto3(endpoint, &script)
}

/// Run one of the checked-in boto3 scripts against the server (the venv
/// python; the script asserts its own journey, the feature asserts the
/// OK marker in the output).
#[given(expr = "I run the boto3 script {string}")]
#[when(expr = "I run the boto3 script {string}")]
#[then(expr = "I run the boto3 script {string}")]
async fn run_boto3(world: &mut World, name: String) {
    let mut cmd = boto3_cmd(world, &name);
    run_external(world, &mut cmd, true);
}

/// The read-only arm of a checked-in boto3 script: `--read-only` is the script's own second
/// argument (G22), and the script then asserts the mode's refusals in the SDK's own shape
/// (`botocore`'s `ClientError`) instead of running its journey body. A step of its own rather
/// than a flag the harness derives from the scenario's tags: the feature text says which of the
/// script's two contracts it is asserting, and the script's journey body stays untouched.
#[given(expr = "I run the boto3 read-only script {string}")]
#[when(expr = "I run the boto3 read-only script {string}")]
#[then(expr = "I run the boto3 read-only script {string}")]
async fn run_boto3_read_only(world: &mut World, name: String) {
    let mut cmd = boto3_cmd(world, &name);
    cmd.arg("--read-only");
    run_external(world, &mut cmd, true);
}

/// Complete the aws multipart upload whose one part is the captured `UploadPartCopy` ETag.
///
/// The completion needs two captured values — the upload id from the initiate call and the
/// part's ETag — and the one `{captured}` slot holds only the latter, so the id travels through
/// [`World::ext_upload_id`]. The completion document is one inline argv word rather than a
/// `file://` path: aws cli takes inline JSON, and a Windows scratch path inside a URL is the
/// kind of quoting a client command line should not have to survive.
#[given(
    expr = "I complete the aws multipart upload {string} in bucket {string} with the captured part"
)]
#[when(
    expr = "I complete the aws multipart upload {string} in bucket {string} with the captured part"
)]
#[then(
    expr = "I complete the aws multipart upload {string} in bucket {string} with the captured part"
)]
async fn complete_aws_multipart(world: &mut World, key: String, bucket: String) {
    let upload_id = world.ext_upload_id.trim().to_owned();
    assert!(
        !upload_id.is_empty(),
        "no multipart upload id was captured before the completion"
    );
    let etag = world.ext_captured.trim().trim_matches('"').to_owned();
    let document = format!(r#"{{"Parts":[{{"ETag":"{etag}","PartNumber":1}}]}}"#);
    let command = format!("s3api complete-multipart-upload --bucket {bucket} --key {key}");
    let mut cmd = aws_cmd(world, &command);
    cmd.arg("--upload-id")
        .arg(&upload_id)
        .arg("--multipart-upload")
        .arg(&document);
    run_external(world, &mut cmd, true);
}

/// The tier journey's checked-in boto3 script — this crate's own (`tests/scripts/`), because the
/// tier client leg is this suite's artifact; the older scripts stay beside the server crate's
/// retired tests.
fn boto3_tier_script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scripts/boto3_tier.py")
}

/// Run the tier journey script against the spawned tier server. The script asserts the SDK's own
/// view of the class, the copy, the multipart assembly, the lifecycle round trip, the versioned
/// refusal and the batch listing; the feature asserts the OK marker.
#[given(expr = "I run the boto3 tier script")]
#[when(expr = "I run the boto3 tier script")]
#[then(expr = "I run the boto3 tier script")]
async fn run_boto3_tier(world: &mut World) {
    let script = boto3_tier_script();
    assert!(
        script.exists(),
        "boto3 tier script not found at {}",
        script.display()
    );
    let ext = world.ext.as_ref().expect("external server running");
    let endpoint = ext
        .base_url
        .strip_prefix("http://")
        .expect("base url has the http:// prefix");
    let mut cmd = boto3(endpoint, &script);
    run_external(world, &mut cmd, true);
}

/// The ephemeral `--port 0` leg (journey): a second server starts while
/// the first keeps serving.
#[given(expr = "I start a second server")]
#[when(expr = "I start a second server")]
#[then(expr = "I start a second server")]
async fn start_second_server(world: &mut World) {
    let ext = world.ext.as_ref().expect("external server running");
    let root = ext.workdir.path().join("root2");
    fs::create_dir_all(&root).unwrap();
    let spawned = SpawnedServer::start(&root, None, None);
    assert!(
        spawned.endpoint().port() != 0,
        "the second server must report the address it actually bound, not the `:0` it asked for"
    );
    world.ext_second = Some(spawned);
}

/// Restart the spawned server on the same root and configuration (the tier durability leg): the
/// reopened process re-reads the index and the cold rows a scenario's earlier writes left.
#[given(expr = "the external tier server is restarted")]
#[when(expr = "the external tier server is restarted")]
#[then(expr = "the external tier server is restarted")]
async fn restart_external(world: &mut World) {
    world
        .ext
        .as_mut()
        .expect("external server running")
        .restart()
        .await;
}

/// Point the aws steps at one of the scenario's two servers.
///
/// **The second server's only observation point.** `I start a second server` proves it bound an
/// ephemeral port and nothing more, so `journey.feature`'s "serves alongside the first server"
/// needs an op that dials *it* — and the state it then holds is what tells the two apart. An
/// explicit target rather than "the second one from now on", because the same scenario also has
/// to show the first still answering.
#[given(expr = "the client targets the {word} server")]
#[when(expr = "the client targets the {word} server")]
#[then(expr = "the client targets the {word} server")]
async fn client_targets(world: &mut World, which: String) {
    let url = match which.as_str() {
        "first" => world
            .ext
            .as_ref()
            .expect("external server running")
            .base_url
            .clone(),
        "second" => format!(
            "http://{}",
            world
                .ext_second
                .as_ref()
                .expect("the scenario started a second server")
                .endpoint()
        ),
        other => panic!("a client target is `first` or `second`, not {other:?}"),
    };
    let ext = world.ext.as_mut().expect("external server running");
    ext.target_url = url;
}

// --- file-preparation steps ----------------------------------------------

#[given(expr = "I write {string} to the scratch file {string}")]
#[when(expr = "I write {string} to the scratch file {string}")]
#[then(expr = "I write {string} to the scratch file {string}")]
async fn write_text(world: &mut World, text: String, name: String) {
    fs::write(scratch(world, &name), text).unwrap();
}

#[given(expr = "I write {int} deterministic bytes to the scratch file {string}")]
#[when(expr = "I write {int} deterministic bytes to the scratch file {string}")]
#[then(expr = "I write {int} deterministic bytes to the scratch file {string}")]
async fn write_deterministic(world: &mut World, n: u64, name: String) {
    // The ONE byte generator the steps share (F14 — see common.rs).
    fs::write(scratch(world, &name), deterministic_bytes(n)).unwrap();
}

/// Out-of-band files dropped by hand under the served root (cold listing,
/// pagination truncation): `{stem}1.txt` … `{stem}{count}.txt`.
#[given(expr = "the served root contains a bucket {string} with {int} files {string}")]
#[when(expr = "the served root contains a bucket {string} with {int} files {string}")]
#[then(expr = "the served root contains a bucket {string} with {int} files {string}")]
async fn served_root_files(world: &mut World, bucket: String, count: u64, stem: String) {
    let ext = world.ext.as_ref().expect("external server running");
    let dir = ext.root.join(&bucket);
    fs::create_dir_all(&dir).unwrap();
    for i in 1..=count {
        fs::write(dir.join(format!("{stem}{i}.txt")), "x").unwrap();
    }
}

// --- Then steps -----------------------------------------------------------

#[then(expr = "the external client output contains {string}")]
async fn output_contains(world: &mut World, text: String) {
    assert!(
        world.ext_output.contains(&text),
        "client output missing {text:?}: {}",
        world.ext_output
    );
}

/// The mc stat ETag check: the bash baseline greps `etag` case-insensitively
/// (mc prints `ETag` in newer releases, `Etag` in older ones).
#[then(expr = "the external client output contains {string} ignoring case")]
async fn output_contains_ignore_case(world: &mut World, text: String) {
    assert!(
        contains_ignore_case(&world.ext_output, &text),
        "client output missing {text:?} (case-insensitive): {}",
        world.ext_output
    );
}

#[then(expr = "the external client output does not contain {string}")]
async fn output_not_contains(world: &mut World, text: String) {
    assert!(
        !world.ext_output.contains(&text),
        "client output unexpectedly contains {text:?}: {}",
        world.ext_output
    );
}

/// Trimmed equality — for `--output text` values like `KeyCount`.
#[then(expr = "the external client output equals {string}")]
async fn output_equals(world: &mut World, text: String) {
    assert_eq!(
        world.ext_output.trim(),
        text.trim(),
        "client output mismatch"
    );
}

#[then("the external client output is not empty")]
async fn output_not_empty(world: &mut World) {
    assert!(
        !world.ext_output.trim().is_empty(),
        "client output is empty"
    );
}

#[then(expr = "the external client error contains {string}")]
async fn error_contains(world: &mut World, text: String) {
    assert!(
        world.ext_error.contains(&text),
        "client error missing {text:?}: {}",
        world.ext_error
    );
}

/// The `… ignoring case` twin of the step above, for a leg that asserts a **response header**
/// through a client's own rendering: the client prints the header name as it read it, not as the
/// contract spells it — aws cli's debug dump shows `X-Amz-Delete-Marker` where the wire contract
/// writes `x-amz-delete-marker` (measured 2026-10-05) — so a case-sensitive assertion would pin
/// the client's rendering rather than the header's presence. The stdout step's `ignoring case`
/// variant exists for the same reason (`mc`'s `ETag`/`Etag`); this one covers the stderr side.
#[then(expr = "the external client error contains {string} ignoring case")]
async fn error_contains_ignore_case(world: &mut World, text: String) {
    assert!(
        contains_ignore_case(&world.ext_error, &text),
        "client error missing {text:?} (case-insensitive): {}",
        world.ext_error
    );
}

#[then(expr = "I capture the client output")]
async fn capture_output(world: &mut World) {
    world.ext_captured = world.ext_output.trim().to_string();
}

/// Capture the initiate call's output as the multipart upload id — the second value the aws
/// completion step needs alongside the captured part ETag.
#[then(expr = "I capture the client output as the multipart upload id")]
async fn capture_upload_id(world: &mut World) {
    world.ext_upload_id = world.ext_output.trim().to_string();
}

#[then(expr = "the scratch file {string} equals the scratch file {string}")]
async fn scratch_files_equal(world: &mut World, a: String, b: String) {
    files_equal(&scratch(world, &a), &scratch(world, &b));
}

#[then(expr = "the scratch file {string} is {int} bytes")]
async fn scratch_file_len(world: &mut World, name: String, n: u64) {
    let len = fs::metadata(scratch(world, &name)).unwrap().len();
    assert_eq!(len, n, "size mismatch for {name}");
}

#[then(expr = "the scratch file {string} matches the prefix of the scratch file {string}")]
async fn scratch_file_prefix(world: &mut World, part: String, whole: String) {
    let part = fs::read(scratch(world, &part)).unwrap();
    let whole = fs::read(scratch(world, &whole)).unwrap();
    assert!(
        whole.starts_with(&part),
        "{} is not a prefix of {}",
        part.len(),
        whole.len()
    );
}
