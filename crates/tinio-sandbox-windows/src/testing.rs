//! Test devices that **more than one crate's tests** need — the shared ones and
//! the Windows confinement harness those devices serve.
//!
//! **This module exists because a `tests/` directory is not importable across
//! crates.** `crates/tinio-server/tests/sandbox_e2e_windows.rs` drives the same
//! launcher this crate's own suites do, and the harness it needs — reaping a
//! spawned process under a deadline, reading a DACL, planting a junction —
//! lived in `crates/tinio-sandbox-windows/tests/common/mod.rs`, which that crate
//! cannot reach, so it was written a second time. `docs/tests.md` names the
//! convention: shared test support lives on the crate that owns the subject
//! rather than in a copy per suite.
//!
//! **It is a `testing` feature and not a plain module.** A feature is the usual
//! spelling of that convention (`tinio-util`'s, and this crate's `testing` is
//! non-default for the same reason), and it is what keeps the devices out of a
//! build that is not a test build: the module compiles only when the feature is
//! on, and no production edge turns it on. **A `#[cfg(test)]` module could not do
//! this**, because `#[cfg(test)]` is invisible to `tests/` — the harness has to
//! be lib code to cross that boundary at all. So the feature is asked for on this
//! crate's own test builds by a dev-dependency on this crate itself
//! (`tinio-sandbox-windows = { path = ".", features = ["testing"] }` in
//! `Cargo.toml`, which is not the cycle it reads as: Cargo unifies that edge with
//! the lib under test, so both are one build), and `tinio-server`'s Windows e2e
//! targets ask for it the same way from their own dev-dependency. Either way the
//! module is reachable from `tests/` without a per-suite copy. The cost is one
//! small module in a library `publish = false` keeps inside this workspace.
//!
//! **The confinement harness itself moved in here on 2026-10-01**, and the move
//! is the reason this module is no longer "devices" alone
//! (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`, the operator's
//! ruling). The confined-launch legs exercise `process::launch`,
//! `socket::from_value`, the backend's profile window and `launch::adopt`, so by
//! `docs/tests.md`'s placement rule they belong in those `src/` modules — and the
//! harness they compose
//! (`tests/common/`) had to become lib code to reach them, exactly as
//! [`wait_for_exit`] and [`host_cannot`] already had. `tests/common/` and the
//! five suites whose legs moved (`launch.rs`, `listener.rs`, `network.rs`,
//! `payload.rs`, `profile.rs`) are retired; the suites that stay under `tests/`
//! (`confine.rs`, `events.rs`, `signals.rs`) reach this module with
//! `use tinio_sandbox_windows::testing as common;`, which is why so many of the
//! names below read as though they still had a `mod common;` above them.
//!
//! **Nothing here asserts anything about the confinement.** These are devices a
//! test composes: what a test proves is still in the test. The rules they do
//! carry are the ones a suite cannot be trusted to spell for itself, and they are
//! the reason these are shared rather than copied — [`wait_for_exit`] kills the
//! process it gives up on, so a suite that wrote its own copy could assert on the
//! deadline without killing the child first (the `tests/common/` doc recorded the
//! suite where it happened), [`host_cannot`] is the one place a host
//! precondition may be turned into a skip, so a leg cannot opt itself out of an
//! assertion in a second spelling of [`ALLOW_SKIP`], and [`LaunchGuard`] is what
//! keeps `docs/tests.md`'s "never two child-spawning suites concurrent" true now
//! that the confined-launch legs share one libtest binary.
//!
//! **[`wait_for_exit`], [`host_cannot`], the [`ALLOW_SKIP`] pair, the [`capture`]
//! device and the [`Capture`]/[`Recording`]/[`recorder`] names beside it are
//! defined in [`tinio_util::testing`] and re-exported here.** This module used to
//! hold them, and this crate's root is `#![cfg(target_os = "windows")]` — so the
//! macOS and Linux legs that need the same rules, or record their own events,
//! could not import them at all, and each spelled its own copy. The definers moved
//! to the crate every platform builds; the re-exports are what keep every caller
//! of this path (this crate's own suites,
//! `tinio-server/tests/support/windows_e2e_harness.rs`'s wrapper) working
//! unchanged.
//!
//! Behind the crate's own `#![cfg(target_os = "windows")]` for the crate root's
//! reason: `crates/*` has no per-target member form, so this compiles on hosts
//! it does not serve.

use std::{
    env,
    ffi::OsString,
    fs, io,
    io::{Read as _, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    os::windows::process::CommandExt,
    path::{Path, PathBuf},
    process::{self, Child, Command, Stdio},
    sync::{Mutex, MutexGuard, PoisonError},
    thread,
    time::{Duration, Instant},
};

use tinio_sandbox_core::{
    AccessMode, Backend as BackendTrait, Confinement, LaunchPlan, Sandbox, supervise,
};
use tinio_util::job;

use crate::{Backend, Options, acl, launch, sid};

/// Wait for `child` to exit, or kill it and fail the test rather than block on
/// it.
///
/// **The definition is [`tinio_util::testing::wait_for_exit`]'s, re-exported
/// rather than written again**: this crate's root is
/// `#![cfg(target_os = "windows")]`, so the rule's other callers — the server's
/// macOS e2e leg and the macOS crate's own suites — cannot reach a definer that
/// lives here, and each re-spelled the deadline/poll/kill rule for itself. The
/// name stays on this path because that is what this crate's own suites and the
/// server harness's wrapper (`tinio-server/tests/support/windows_e2e_harness.rs`)
/// already import; the rule, its bounds and the kill-before-panic are documented
/// there.
#[doc(inline)]
pub use tinio_util::testing::wait_for_exit;
/// The one way past a precondition this host cannot meet, the deliberate opt-in
/// `TINIO_SANDBOX_ALLOW_SKIP`, and the arbitration a suite takes it through.
///
/// **Defined in [`tinio_util::testing`] and re-exported here**, for
/// [`wait_for_exit`]'s reason: this crate's root is
/// `#![cfg(target_os = "windows")]`, so the Linux jail suite and the server's
/// Linux e2e legs — both of which skip under the same variable — could not import
/// a definer that lives here and each spelled the rule again. The name stays on
/// this path because that is what this crate's own suites
/// (`src/backend/tests.rs`) and the server harness
/// (`tinio-server/tests/support/windows_e2e_harness.rs`) already import; the
/// rule, the assert-before-print order and the remedy's spelling are documented
/// at the definer.
#[doc(inline)]
pub use tinio_util::testing::{ALLOW_SKIP, host_cannot, skip_allowed};

/// `icacls` on `path`, with `args` appended, and its exit status checked.
///
/// The oracle for what is on the operator's directory, and it has to refuse a
/// non-zero exit: an `icacls` that could not read the DACL would otherwise hand
/// back empty text, and "no package ACE is left" would be asserted about a
/// directory nothing had looked at. That is the reason every suite that asserts
/// on the DACL text shares this function rather than spelling the check again.
/// `args` is `icacls`'s own (`&[]` reads the DACL; `"/grant"`, `"/remove:g"` and
/// `"/q"` write it), and the path comes first as its syntax requires.
///
/// An AppContainer package SID does not resolve to an account name, so a
/// package-SID ACE renders as the `S-1-15-2-…` string itself; **a well-known SID
/// does not come back that way** (`icacls` prints `S-1-5-32-545` as
/// `BUILTIN\Users`, localised to the host's display language), which is why the
/// callers compare rendered ACE lines rather than searching the text for a
/// foreign SID.
///
/// **The binary is [`acl::icacls_binary()`] and not `"icacls"`**, for the reason
/// that function exists: the confinement's own enforcement resolves the tool
/// through the system directory rather than the environment, and a test naming
/// it twice would be two ways to spell one tool in one phase — with the second
/// selectable by whoever set `PATH`.
pub fn icacls(path: &Path, args: &[&str]) -> String {
    let output = Command::new(acl::icacls_binary())
        .arg(path)
        .args(args)
        .output()
        .expect("run icacls");
    assert!(
        output.status.success(),
        "icacls {} {}: {}",
        path.display(),
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// `<link>` as a directory junction onto `target`, through `cmd`'s `mklink /J`.
///
/// **A junction and not a symlink.** `mklink /D` needs
/// `SeCreateSymbolicLinkPrivilege`, which an ordinary process does not hold;
/// `/J` needs no privilege at all, and the two are the same thing to every layer
/// the sandbox's tests are about — `std` reports both `is_symlink() == true`, and
/// the canonicalizer resolves both by reading the link (`read_link`) rather than
/// by opening the target.
///
/// **The fixture asserts itself, and that is load-bearing rather than tidy.**
/// Every assertion a caller makes after it is an *absence* — "the grant did not
/// follow the link", "the escaping bucket is refused" — and an `mklink` that
/// silently failed would make each of them pass with nothing planted. The
/// `is_symlink` check is the same argument one step further in: a directory that
/// was created but is not a reparse point would make the leg assert nothing.
///
/// **A host that refuses `mklink /J` fails the test rather than skipping it**,
/// which is the plans' own rule: a run that never made the link proves nothing
/// about the case the link exists for.
pub fn junction(link: &Path, target: &Path) {
    let output = Command::new("cmd")
        .args(["/C", "mklink", "/J"])
        .arg(link)
        .arg(target)
        .output()
        .expect("run mklink");
    assert!(
        output.status.success(),
        "mklink /J {} -> {}: {}{}",
        link.display(),
        target.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata = fs::symlink_metadata(link).expect("the junction exists");
    assert!(
        metadata.file_type().is_symlink(),
        "{} was created but is not a reparse point, so the leg would assert nothing",
        link.display()
    );
}

/// The capture a test binary records through — **defined in [`tinio_util::testing`] and re-exported
/// here**, for [`wait_for_exit`]'s reason: this crate's root is `#![cfg(target_os = "windows")]`, so
/// the macOS and Linux legs that record their own events could not take a definer that lives here,
/// and each of them spelled its own copy of it. The device's doc carries the install, the level it
/// records at, the second-global-installer hazard and which threads each entry point owns.
#[doc(inline)]
pub use tinio_util::testing::{Capture, Recording, capture, recorder};

/// How long any bounded wait in this harness may take before it fails the test.
///
/// The feature's own readiness budget rather than an arbitrary number:
/// `tinio-e2e`'s `START_TIMEOUT` is 30 s, and the launch has to fit inside it.
pub const TIMEOUT: Duration = Duration::from_secs(30);

/// The volumes the suite exercises.
///
/// `TINIO_SANDBOX_TEST_VOLUMES` (semicolon separated) selects them; without it,
/// the volume holding the system temp directory. The design's evidence shows
/// ReFS and NTFS agree on every probed ACL case, so a regression that broke one
/// would be invisible on a single-volume run — this is how both are covered.
///
/// **A selection that names no usable volume fails rather than answering an
/// empty list**, because every caller either loops over this or takes its first
/// entry: an empty answer would make those tests pass by doing nothing, which is
/// exactly the shape of green this phase refuses.
pub fn volumes() -> Vec<PathBuf> {
    let Ok(raw) = env::var("TINIO_SANDBOX_TEST_VOLUMES") else {
        return vec![env::temp_dir()];
    };
    let volumes: Vec<PathBuf> = raw
        .split(';')
        .map(str::trim)
        .filter(|entry| !entry.is_empty() && Path::new(entry).is_dir())
        .map(PathBuf::from)
        .collect();
    assert!(
        !volumes.is_empty(),
        "TINIO_SANDBOX_TEST_VOLUMES={raw:?} names no directory that exists on this host, so \
         every test looping over the volumes would run zero cases and pass"
    );
    volumes
}

/// The first volume, for the tests that exercise one's worth of tree.
pub fn one_volume() -> PathBuf {
    volumes()
        .into_iter()
        .next()
        .expect("volumes() answers with at least one volume or fails")
}

/// One tree's worth of probes: what the child reads and what it writes.
pub struct Tree {
    /// The tree root, on the volume under test.
    pub path: PathBuf,
    /// The served root. Exposed with the caller's access mode.
    pub root: PathBuf,
    /// A sibling with no package-SID ACE, carrying `Users` instead.
    pub denied: PathBuf,
    /// A directory exposed read-write, where the child writes its report.
    pub report: PathBuf,
    /// The address the child should try to reach, when a test sets one. It
    /// travels in the probe file, so the confined child needs no environment
    /// channel to learn it.
    pub target: Option<SocketAddr>,
}

impl Tree {
    /// The tree whose directories sit under `path`, without touching the disk.
    ///
    /// The one rule `tree()` and [`launch_confined_at`] share: `root`, `denied`
    /// and `report` are `path`'s three children, in that order, and nothing about
    /// the layout is carried in a value a second process cannot receive. A
    /// launcher role is a fresh process that got a path in an environment
    /// variable and nothing else, so it rebuilds the same three directories from
    /// it rather than being handed a `Tree`.
    pub fn at(path: &Path) -> Self {
        Self {
            root: path.join("root"),
            denied: path.join("denied"),
            report: path.join("report"),
            path: path.to_path_buf(),
            target: None,
        }
    }

    /// The probe text, without writing it.
    pub fn probe_text(&self) -> String {
        let mut text = format!(
            "root={}\ndenied={}\nreport={}\n",
            self.root.display(),
            self.denied.display(),
            self.report.join("report.txt").display()
        );
        if let Some(target) = self.target {
            text.push_str(&format!("target={target}\n"));
        }
        text
    }

    /// Write the probe file and return its path — what the payload's config
    /// bytes carry.
    ///
    /// **It goes into the report directory and not into `path`**, and that is
    /// the difference between a suite that tests the confinement and one that
    /// tests nothing. `path` is the tree's own directory and is exposed to
    /// nobody; the confined child is an AppContainer, so its reads are checked
    /// against the package SID and the user's own ACEs do not help it — a probe
    /// written as a sibling of `root` and `report` is a file the child cannot
    /// open, and every role that reads it panics on its first line, taking the
    /// confined cases of Tasks 8, 9, 10 and 12 with it. The report directory is
    /// the one path every test here grants read-write, which is what makes it the
    /// only place a probe can live.
    pub fn probe(&self) -> PathBuf {
        let probe = self.report.join("probe.txt");
        fs::write(&probe, self.probe_text()).expect("write the probe file");
        probe
    }

    /// What the child reported, if it got that far.
    pub fn report_text(&self) -> String {
        fs::read_to_string(self.report.join("report.txt")).unwrap_or_default()
    }
}

/// Build the standard tree under `volume`.
///
/// The `denied` sibling carries `Users` and no package-SID ACE, which is the
/// verified row every confinement test reproduces: those ACEs do not help.
pub fn tree(volume: &Path) -> (tempfile::TempDir, Tree) {
    let dir = tempfile::TempDir::new_in(volume).expect("temp dir on the volume");
    let tree = Tree::at(&dir.path().join("tree"));
    fs::create_dir_all(&tree.root).expect("root");
    fs::create_dir_all(&tree.denied).expect("denied");
    fs::create_dir_all(&tree.report).expect("report");
    // Seeded *before* the grant: this is design §6's inheritance question as the
    // confinement suite sees it — Task 4 settled the ACL shape, and this is the
    // functional half of the same answer.
    fs::write(tree.root.join("granted.txt"), b"granted").expect("seed");
    fs::write(tree.denied.join("secret.txt"), b"secret").expect("seed");
    let granted = Command::new(acl::icacls_binary())
        .arg(&tree.denied)
        .args(["/grant", r"*S-1-5-32-545:(OI)(CI)R", "/q"])
        .output()
        .expect("grant Users on the denied tree");
    assert!(
        granted.status.success(),
        "{}",
        String::from_utf8_lossy(&granted.stderr)
    );
    (dir, tree)
}

/// The process-wide slot every confined launch holds — **the lock that makes
/// "never two child-spawning suites concurrent" true under any runner**.
///
/// `docs/tests.md`'s concurrency rule is a property of the suites, and until
/// 2026-10-01 it held by construction: each confined-launch suite was its own
/// `tests/` target, and a runner that ran one test at a time inside it kept two
/// launches apart. The placement cleanup moved those legs into `src/` — one
/// libtest binary, whose tests libtest runs on parallel threads by default — so
/// the rule needed a mechanism of its own. This is it: [`launch_confined`] and
/// [`launch_confined_at`] take the slot before they touch a probe, a DACL or a
/// profile, and a second launch in this process waits for the first.
///
/// **Why the guard is handed back and not dropped at launch time.** The rule is
/// about two *children*, not two `CreateProcessW` calls: an entry that released
/// the slot on return would still leave its child running while the next launch
/// started, which is the interleaving the rule exists to prevent — the profile
/// lock is per root, the ACEs are standing, and the profile legs read one
/// process-wide capture buffer, so a foreign launch inside another leg's window
/// is exactly the noise the rule keeps out. [`launch_confined`] therefore
/// returns the [`LaunchGuard`] and the caller holds it for as long as the child
/// lives: [`run_confined`] does that for the whole run, and a caller that takes
/// the raw entry holds it until it has waited for or dropped the child.
///
/// **It is a lock and not a runner flag** (`--test-threads=1`, which the
/// platform gate script passes) because the flag is not every runner's: nextest
/// runs one test per process and `cargo test` runs threads, so a rule that held
/// under only one of them would be a rule about the runner.
///
/// **What the slot deliberately does not cover.** The legs that re-enter this
/// binary for a *non-confined* reason — `crate::socket`'s listener handoff and
/// `crate::launch`'s payload frame — spawn one short-lived process through
/// `job::spawn` and hold nothing this rule is about: no profile, no ACE, no
/// process-wide recording. They are left out by design rather than by omission,
/// so that the lock's scope states which legs the rule actually speaks for.
static LAUNCH: Mutex<()> = Mutex::new(());

/// Exclusive right to run a confined launch in this process: the `LAUNCH`
/// static's guard, held by the caller for as long as the confined child lives.
///
/// Poisoning is deliberately ignored — [`Mutex::lock`]'s error is taken through
/// [`PoisonError::into_inner`] rather than raised — because the legs this lock
/// serializes *fail* on a host that cannot confine, which is their contract
/// (fail-not-skip): one red test must not turn every later launch in the process
/// red for a reason of its own.
pub struct LaunchGuard {
    _slot: MutexGuard<'static, ()>,
}

impl LaunchGuard {
    /// Take the process-wide slot, waiting for whoever holds it.
    ///
    /// Private on purpose: the only ways into a confined launch are
    /// [`launch_confined`] and [`launch_confined_at`], and both call this before
    /// they do anything else. A caller that could take the slot without
    /// launching, or launch without taking it, would be a second spelling of the
    /// rule this type exists to keep in one place.
    fn acquire() -> Self {
        Self {
            _slot: LAUNCH.lock().unwrap_or_else(PoisonError::into_inner),
        }
    }
}

/// The libtest name of the confined child's role **in the binary that is
/// running** — the name a launch passes as libtest's `--exact` filter.
///
/// **The two arms are a compile-time fact and not a runtime guess.** A confined
/// launch re-enters the *current* executable (`env::current_exe()`) with argv
/// `[…, <this name>, "--exact", "--nocapture"]`, and the name a role answers to
/// depends on which executable that is: this crate's own unit-test binary, where
/// libtest namespaces every test by its module (so the role is
/// `testing::tests::child_role`), or one of the integration binaries under
/// `tests/`, where each suite declares a top-level `child_role` shim (so the name
/// is just `child_role`). `cfg(test)` is the discriminator because it is the one
/// cargo itself applies to that difference: it is on for the lib compiled as this
/// crate's test target, and off for the lib linked into an integration target —
/// including the target of a crate that merely depends on this one.
///
/// **Both arms must name a real test**, which is what the harness's own
/// `every_role_the_harness_spawns_is_a_test_in_this_binary` checks by asking the
/// binary to list itself: a name that matches nothing makes libtest run zero
/// tests and exit `0`, which reads as a child that quietly did nothing.
#[cfg(test)]
pub const CHILD_ROLE: &str = "testing::tests::child_role";
/// See [`CHILD_ROLE`] — the same rule for the same name in an integration binary.
#[cfg(not(test))]
pub const CHILD_ROLE: &str = "child_role";

/// The libtest name of the launcher's role **in the binary that is running**,
/// for [`CHILD_ROLE`]'s reason and with its two arms.
///
/// The lib's arm matters even though no lib test spawns a launcher today: a
/// [`spawn_launcher`] call from a unit test would otherwise ask libtest for a
/// name that does not exist, and the failure would look like a launcher that
/// exited without doing anything.
#[cfg(test)]
pub const LAUNCHER_ROLE: &str = "testing::tests::launcher_role";
/// See [`LAUNCHER_ROLE`] — the same rule for the same name in an integration
/// binary.
#[cfg(not(test))]
pub const LAUNCHER_ROLE: &str = "launcher_role";

/// The environment variable that hands an **unrelayed** child its probe text.
///
/// The harness's one use of an environment channel, and it is deliberate: this
/// child is spawned directly rather than launched, so there is no payload for it
/// to inherit, and the test that owns it is measuring a console control event
/// rather than a payload. Legitimate precisely because this child is **not**
/// confined — design decision 17 keeps the environment out of the confined path,
/// where a marker would be forgeable.
pub const PROBE_ENV: &str = "TINIO_SANDBOX_TEST_PROBE";

/// Start one confined child over `tree` and hand back the supervision and the
/// process-wide launch slot the caller must hold while that child lives.
///
/// The in-process entry: the caller has a [`Tree`] because it built one, and the
/// probe it writes is the one that carries this tree's `target` when a test set
/// one (the network legs do).
///
/// **A launch that fails panics with the backend's own error**, which is the
/// whole of what this harness can do with it: the confinement suite has no
/// subject left after a launch that did not happen, and a `None` a caller could
/// return on would make "the backend refused to confine anything" look green.
///
/// **The third element is [`LaunchGuard`] and not a courtesy**: it is the
/// process-wide slot the `LAUNCH` static is about, and dropping it while the
/// child runs reopens the lane the lock closes. Hold it until the child has been
/// waited for or dropped.
pub fn launch_confined(
    tree: &Tree,
    root_access: AccessMode,
    options: Options,
    bind: Option<SocketAddr>,
) -> (Confinement, SocketAddr, LaunchGuard) {
    // **The slot comes first, before the probe is written**: the exclusive
    // interval is this entry's whole body — the probe, the exposure set, the
    // listener, the launch — and not only the `CreateProcessW` inside it.
    let slot = LaunchGuard::acquire();
    let probe = tree.probe();
    start(tree, &probe, root_access, options, bind, slot)
}

/// Start one confined child from a tree **path**, for a caller that is its own
/// process.
///
/// The launcher roles of the events and signal suites are separate processes,
/// handed a directory in `TINIO_SANDBOX_TEST_TREE`; they cannot be given a
/// [`Tree`], so this entry rebuilds one with [`Tree::at`] and writes the probe
/// under it. What a rebuilt tree cannot carry is a `target` — a launcher role is
/// not the test that set one — and nothing that calls this entry wants one. The
/// third element is [`LaunchGuard`]'s, for [`launch_confined`]'s reason.
pub fn launch_confined_at(
    path: &Path,
    root_access: AccessMode,
    options: Options,
    bind: Option<SocketAddr>,
) -> (Confinement, SocketAddr, LaunchGuard) {
    // [`launch_confined`]'s rule and its reason: the slot first.
    let slot = LaunchGuard::acquire();
    let tree = Tree::at(path);
    let probe = tree.probe();
    start(&tree, &probe, root_access, options, bind, slot)
}

/// The one body both entries above share: the exposure set, the listener, the
/// plan and the launch.
///
/// It takes the probe's path rather than writing it, because *where* the probe
/// goes is [`Tree::probe`]'s rule and *what* it says is the caller's — one entry
/// carries a target and the other cannot — and it takes the [`LaunchGuard`] its
/// callers already hold, so that the exclusive interval is the entry's whole body
/// and not just this function (see [`LAUNCH`]).
fn start(
    tree: &Tree,
    probe: &Path,
    root_access: AccessMode,
    options: Options,
    bind: Option<SocketAddr>,
    slot: LaunchGuard,
) -> (Confinement, SocketAddr, LaunchGuard) {
    let sandbox = Sandbox::new()
        .expose(&tree.root, root_access)
        .expose(&tree.report, AccessMode::ReadWrite);
    let listener = TcpListener::bind(bind.unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0))))
        .expect("bind the listener");
    let address = listener.local_addr().expect("the listener's address");
    // The listener is attached, not carried through the payload: a socket
    // cannot ride on a stream (decision 16).
    let plan = LaunchPlan::for_launch(probe.to_string_lossy().into_owned()).with_listener(listener);
    // The child is one role function of this binary: `Options::args` is the
    // seam that says so, because a test binary's own argv belongs to libtest —
    // and [`CHILD_ROLE`] is the name that role answers to here, which is not one
    // constant across the binaries this harness serves.
    //
    // **`--nocapture` is load-bearing, and what it prevents was measured on this
    // host (2026-09-21).** The child's stdout is the event pipe, and libtest
    // captures a test's `print!`/`println!` into a per-test buffer — discarded for
    // a passing test and written out only for a failing one. Without this flag the
    // child's frames never reach the pipe at all (the events suite failed exactly
    // so), and a child that panicked would have its diagnostics swallowed twice:
    // captured by libtest, and then dropped by the relay as non-frames, leaving
    // the operator a silent non-zero exit.
    let options = Options {
        args: Some(vec![
            OsString::from(CHILD_ROLE),
            OsString::from("--exact"),
            OsString::from("--nocapture"),
        ]),
        ..options
    };
    let mut backend = Backend::with_options(options);
    let confinement = BackendTrait::confine(&mut backend, &sandbox, &plan)
        .expect("the Windows backend must confine the sandbox it was given");
    (confinement, address, slot)
}

/// Start a confined child **with the options the caller needs**, answer one
/// loopback connection through the inherited listener, wait for it, and hand back
/// what it did.
///
/// **The one body, and [`run_confined`] below is its default.** The callers that
/// needed anything but `Options::default()` — the three that exercise a
/// capability — used to open-code this whole sequence, which put the harness's
/// order (launch, destructure, answer, supervise) and the `panic!` that reports a
/// backend which launched nothing in each of them.
///
/// The tree is still the caller's: what it wrote to its report file is read here,
/// and so is what the relay recorded — [`Run`] carries all three, because
/// [`Run::terminal`] is final only once this function's `supervise` has joined
/// the relay (decision 19). **The launch slot is held until this function
/// returns**, i.e. past the wait, which is what keeps the exclusivity over the
/// child's whole life rather than its creation.
pub fn run_confined_with(tree: &Tree, root_access: AccessMode, options: Options) -> Run {
    let (confinement, address, _slot) = launch_confined(tree, root_access, options, None);
    let Confinement::Supervise(mut child) = confinement else {
        panic!("the Windows backend must launch a child");
    };
    answer(address);
    // `supervise` **borrows** the child, which is what leaves the terminal slot
    // readable afterwards — and the wait inside it joins the relay, so that read
    // is final rather than a snapshot (decision 19).
    let status = supervise(&mut child, || ()).expect("supervise the confined child");
    Run {
        status: status.code().unwrap_or(-1),
        report: tree.report_text(),
        terminal: child.terminal().is_some(),
    }
}

/// Start a confined child with the backend's default options, answer one loopback
/// connection through the inherited listener, wait for it, and hand back what it
/// did.
pub fn run_confined(tree: &Tree, root_access: AccessMode) -> Run {
    run_confined_with(tree, root_access, Options::default())
}

/// A confined run's outcome.
pub struct Run {
    /// The child's exit code.
    pub status: i32,
    /// What the child wrote to its report file.
    pub report: String,
    /// **Whether the child reported a terminal event** — `Listening` or
    /// `Error` — on its way up.
    ///
    /// **`false` is a fact and not a missing value**: the child exited without
    /// emitting either frame, which is the state a launcher turns into its own
    /// `Event::Error` (decision 19). A `bool` rather than the `Option<Terminal>`
    /// this field used to carry, because the harness asks only whether the relay
    /// answered at all — the event's own contents are asserted where the frame is
    /// decoded, and a second reader wanting them is what would widen this back.
    /// Read **after** `supervise` — the wait joins the relay, so the answer is
    /// final rather than a snapshot.
    pub terminal: bool,
}

/// The package SID this run derives for `tree`.
pub fn sid_for(tree: &Tree) -> sid::PackageSid {
    sid::PackageSid::derive(&sid::package_name(&tree.root)).expect("derive the package SID")
}

/// Append one line to the report the parent reads.
pub fn append_to_report(report: &Path, line: &str) {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(report)
        .expect("open the report");
    writeln!(file, "{line}").expect("write the report");
}

/// The report directory a probe file's `report=` entry names.
///
/// The probe carries the **report file**, because that is what the ACL probes
/// append to; the files the signal suite's roles agree on are its siblings, so
/// the directory is the part both ends need.
pub fn probe_dir(probe: &str) -> PathBuf {
    probe
        .lines()
        .find_map(|line| line.strip_prefix("report="))
        .map(PathBuf::from)
        .and_then(|report| report.parent().map(Path::to_path_buf))
        .expect("the probe carries a report path")
}

/// The confined child's role, shared by every binary that launches one.
///
/// Inert when there is no payload: that is design decision 17's discriminator,
/// exercised by the tests exactly as production uses it, and it is why the
/// launcher-side test functions can share a binary with this one. The early
/// return is the discriminator rather than a skipped test — a process with no
/// payload is the launcher, and the role it plays there is no role at all.
pub fn child_role() {
    let Some(adopted) =
        launch::adopt(env::args_os(), io::stdin().lock()).expect("adopt the payload")
    else {
        return;
    };
    // Core's rule hands back a descriptor; rebuilding it is this crate's half.
    let listener = launch::listener(&adopted).expect("the harness bound a listener");
    child_role_body(adopted.config(), listener);
}

/// **The dispatch every `child_role` libtest entry point in this crate's
/// binaries spells**, so the two spellings cannot drift.
///
/// A process handed [`PROBE_ENV`] is the signal suite's unrelayed control child:
/// it arms the stop hook and beats, and never returns. Anything else runs the
/// caller's confined-child half — the parameter — because the binaries differ
/// there and nowhere else: this module's own entry runs [`child_role`], while
/// `tests/signals.rs`'s runs [`child_role_with_stop_hook`], the stop that suite
/// delivers being that suite's subject rather than every launch's.
pub fn child_role_entry(confined: fn()) {
    if let Ok(probe) = env::var(PROBE_ENV) {
        let probe = fs::read_to_string(&probe).expect("read the probe file");
        unrelayed_child_role(&probe);
    }
    confined();
}

/// The launcher's role: run the production launch path over the tree the test
/// named and supervise it, so the *launcher* — not the test process — is the
/// process a console control event addresses.
///
/// Inert unless `TINIO_SANDBOX_TEST_LAUNCHER` is set, which is what lets a
/// binary declare it as an ordinary `#[test]` entry point.
///
/// The relay is armed inside `Backend::confine` by the time this returns, and
/// the child cannot have been reached before then: it reads the payload, which
/// is written after the arming (design §3 steps 7 and 9).
pub fn launcher_role(publish_address: bool) {
    if env::var_os("TINIO_SANDBOX_TEST_LAUNCHER").is_none() {
        return;
    }
    let tree = PathBuf::from(env::var("TINIO_SANDBOX_TEST_TREE").expect("the tree path"));
    let (confinement, address, _slot) =
        launch_confined_at(&tree, AccessMode::ReadWrite, Options::default(), None);
    if publish_address {
        fs::write(tree.join("address"), address.to_string()).expect("publish the address");
    }
    let Confinement::Supervise(mut child) = confinement else {
        panic!("the Windows backend must launch a child");
    };
    // The relay is armed inside `Backend::confine` by the time this returns, and
    // the child cannot have been reached before then: it reads the payload, which
    // is written after the arming (design §3 steps 7 and 9).
    let status = supervise(&mut child, || ()).expect("supervise the confined child");
    // The status is the child's, forwarded the way `serve` forwards it: this
    // process exists to be a launcher, and its exit code is what the test reads.
    process::exit(status.code().unwrap_or(1));
}

/// The role's body, so the variant with a stop hook can share it
/// ([`child_role_with_stop_hook`], which arms the hook and beats before calling
/// this).
///
/// The first parameter is the payload's configuration **text** — `Adopted::config`
/// is a `String` in P0's surface, and this harness is the one reader that treats
/// it as a path to the probe file it wrote.
pub fn child_role_body(config: &str, listener: TcpListener) {
    let probe_path = config.to_owned();
    let probe = fs::read_to_string(&probe_path).expect("read the probe file");
    let value = |key: &str| {
        probe
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key}=")))
            .expect("a probe key")
            .to_string()
    };
    let root = PathBuf::from(value("root"));
    let denied = PathBuf::from(value("denied"));
    let report = PathBuf::from(value("report"));

    let mut log = String::new();
    match fs::read_to_string(root.join("granted.txt")) {
        Ok(_) => log.push_str("granted-read: OK\n"),
        Err(err) => log.push_str(&format!("granted-read: refused ({})\n", err.kind())),
    }
    match fs::read_to_string(denied.join("secret.txt")) {
        Ok(_) => log.push_str("denied-read: READ\n"),
        Err(err) => log.push_str(&format!("denied-read: refused ({})\n", err.kind())),
    }
    match fs::write(denied.join("written.txt"), b"nope") {
        Ok(()) => log.push_str("denied-write: ALLOWED\n"),
        Err(err) => log.push_str(&format!("denied-write: refused ({})\n", err.kind())),
    }
    match fs::write(root.join("written.txt"), b"nope") {
        Ok(()) => log.push_str("root-write: ALLOWED\n"),
        Err(err) => log.push_str(&format!("root-write: refused ({})\n", err.kind())),
    }
    append_to_report(&report, log.trim_end());

    // The outbound attempt, only when a test asked for one: a test that only
    // checks "connect failed" proves nothing, so the parent proves the target
    // reachable unconfined first (design §12).
    if let Some(target) = probe
        .lines()
        .find_map(|line| line.strip_prefix("target="))
        .and_then(|raw| raw.parse::<SocketAddr>().ok())
    {
        let outcome = match TcpStream::connect_timeout(&target, Duration::from_secs(5)) {
            Ok(_) => "outbound: SUCCEEDED".to_string(),
            Err(err) => format!("outbound: refused ({})", err.kind()),
        };
        append_to_report(&report, &outcome);
    }

    // The listener this process inherited: answer one connection, which is what
    // the parent is waiting for. **The accept is retried, because the listener the
    // child rebuilt is non-blocking** (see [`accept`] for the mode, the race it
    // used to lose and the bound the retry keeps).
    let mut stream = accept(&listener, "the confined child's listener");
    stream.write_all(b"pong").expect("answer");
    stream.flush().expect("flush the answer");
}

/// The file a child writes once the stop hook a forwarded stop lands on is
/// installed.
///
/// Its existence is the parent's licence to send a stop: before it, the event
/// would arrive with nothing to land on and take the default disposition.
pub const ARMED: &str = "armed";

/// The file the stop hook writes when it fires.
///
/// **This is the evidence the signal relay exists**, and the reason it is a file
/// the hook itself writes rather than a liveness signal the parent samples: a
/// child that was never reached cannot produce it.
pub const STOPPED: &str = "stopped";

/// The file a child appends to while it is alive.
///
/// Scaffolding, not evidence: a heartbeat is the same shape whether the child is
/// working or starved, so what it is for is the *negative* case — a child whose
/// hook stayed silent can only be said to have been missed if it was alive and
/// writing for the whole window.
pub const BEAT: &str = "beat";

/// The status the stop hook leaves behind.
///
/// The child's hook ends the process with it, which is a stopped server's shape
/// (it drains and leaves) and the way the parent learns the hook ran from the
/// event protocol's other side: the launcher forwards this status to its own
/// caller (design §3 step 8).
pub const STOPPED_EXIT: i32 = 41;

/// The flag a child and a launcher are both started with, so each is its own
/// process **group** — the unit a console control event is addressed to, and the
/// flag the launch itself passes (`crate::process`, design §3 step 9).
const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

/// Beat once every 200 ms into `<report>/beat`, for as long as the process lives.
///
/// The child's own liveness, and nothing else — see [`BEAT`] for what it is for
/// and what it is not.
pub fn beat(report: &Path) -> ! {
    let beat = report.join(BEAT);
    loop {
        append_to_report(&beat, ".");
        thread::sleep(Duration::from_millis(200));
    }
}

/// Arm the hook a forwarded stop lands on, and announce it (Task 10).
///
/// The child-side half of the signal relay: without it a console control event
/// takes the default disposition and the process dies where it stands, instead
/// of draining — which is the whole reason the launcher forwards one at all
/// (design §3 step 9). [`ARMED`] is written after the hook is installed, so the
/// parent never sends a stop into a window where nothing would catch it.
///
/// **The hook writes [`STOPPED`] and then ends the process with
/// [`STOPPED_EXIT`].** The file is the evidence; the exit is what unwinds the
/// tree — the launcher's wait returns and it forwards the status — so a
/// test that reached its hook also leaves no process behind. The grants the run
/// wrote are standing, so the launcher's exit takes nothing back.
fn stop_hook(report: &Path) {
    let stopped = report.join(STOPPED);
    launch::on_stop(move || {
        append_to_report(&stopped, "stopped");
        process::exit(STOPPED_EXIT);
    })
    .expect("arm the stop hook");
    append_to_report(&report.join(ARMED), "armed");
}

/// The confined child, with the stop hook armed before it does anything else.
///
/// The payload's configuration is the probe file's path ([`child_role_body`]'s
/// rule), so the report directory the hook writes into is the launcher's own
/// choice and not this process's.
pub fn child_role_with_stop_hook() {
    let Some(adopted) =
        launch::adopt(env::args_os(), io::stdin().lock()).expect("adopt the payload")
    else {
        return;
    };
    let listener = launch::listener(&adopted).expect("the harness bound a listener");
    let probe = fs::read_to_string(adopted.config()).expect("read the probe file");
    let report = probe_dir(&probe);
    stop_hook(&report);
    let beating = report.clone();
    thread::spawn(move || beat(&beating));
    child_role_body(adopted.config(), listener);
}

/// The unrelayed child's role: the same hook and the same beat, with no payload
/// and no listener — never returns, so a stop that never arrives leaves it
/// beating until the test that started it ends it.
///
/// The probe text arrives in the **environment** here ([`PROBE_ENV`]), which is
/// this harness's one use of that channel and is argued at that constant.
pub fn unrelayed_child_role(probe: &str) -> ! {
    let report = probe_dir(probe);
    stop_hook(&report);
    beat(&report)
}

/// A child in its own process group with nothing supervising it: the group a
/// stop can be addressed to directly, which is what the relay's own address
/// space looks like from the outside.
///
/// The caller owns the returned process and must reap it — the signal suite's
/// guard does, on every path. `stdout` is null because a test binary's banner is
/// not this suite's evidence and the event protocol's channel is not a place to
/// put it (design decision 19); `stderr` is inherited because that is the log
/// channel and a child's panic has to be readable.
///
/// The role it re-enters is [`CHILD_ROLE`], and the child dispatches on
/// [`PROBE_ENV`] — see [`child_role_entry`].
pub fn spawn_unrelayed_child_in_own_group(probe: &Path) -> Child {
    // **The process group is this fixture's address and the guard is its lifetime**: the child
    // beats until the test ends it, so a run that dies without unwinding would otherwise leave it
    // beating — and `CREATE_NEW_PROCESS_GROUP` and the job membership the guard hands out are two
    // different attributes, which is why taking the guard changes nothing about the stop this
    // child is spawned to receive.
    let mut command = Command::new(env::current_exe().expect("the test executable"));
    command
        .args([CHILD_ROLE, "--exact"])
        .env(PROBE_ENV, probe)
        .creation_flags(CREATE_NEW_PROCESS_GROUP)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    job::spawn(&mut command).expect("spawn the unrelayed child")
}

/// A launcher of this binary, in its own process group — **which is the
/// address a stop goes to, and the reason this is one function rather than a
/// `Command` at each call site**.
///
/// The launcher role is a separate process because a console control event is
/// addressed at a *process group*: one aimed at the test process would test
/// nothing. `TINIO_SANDBOX_TEST_TREE` is the tree it works in — the role cannot
/// receive a [`Tree`], it is a fresh process.
///
/// The two arguments are the events suite's, and it is the only caller that
/// needs either: `stdout` is the relay it asserts on rather than libtest's
/// banner, so it pipes it where the signals suite, whose launcher is addressed
/// but never read, discards it; `quiet` passes libtest's `--quiet`, which is
/// what narrows the banner that suite has to filter. **A call site that
/// re-spelled this `Command` would drop the creation flag as well as the
/// environment**, and the flag is the whole mechanism: the copy in the events
/// suite did exactly that, which is the defect this signature closes.
pub fn spawn_launcher(tree: &Path, stdout: Stdio, quiet: bool) -> Child {
    // **Through the guard, and the flag is still the whole mechanism**: the launcher is the process
    // a stop is addressed to, and a launcher left behind by a run that died without unwinding holds
    // the listener, the product's job and the grants — the leak `tinio_util::job`'s module doc is
    // written around. The creation flag below is untouched by the guard: it is an attribute of the
    // spawned process, not of the `Command` the guard is handed.
    let mut command = Command::new(env::current_exe().expect("the test executable"));
    command
        .args([LAUNCHER_ROLE, "--exact"])
        .args(quiet.then_some("--quiet"))
        .env("TINIO_SANDBOX_TEST_LAUNCHER", "1")
        .env("TINIO_SANDBOX_TEST_TREE", tree)
        .creation_flags(CREATE_NEW_PROCESS_GROUP)
        .stdout(stdout)
        .stderr(Stdio::inherit());
    job::spawn(&mut command).expect("spawn the launcher")
}

/// How long [`accept`] waits between two `accept` attempts.
///
/// **A poll interval rather than a blocking `accept`, because the listener is
/// non-blocking by design** and there is no readiness primitive in `std` to wait
/// on: 10 ms is short enough that the answer arrives at once for every caller here
/// and long enough that a child waiting on a parent does not spin a core.
const ACCEPT_POLL: Duration = Duration::from_millis(10);

/// The child's half of [`answer`]: accept one connection on the inherited
/// listener, or fail after [`TIMEOUT`].
///
/// **`accept` on this listener answers `WouldBlock` until a client is pending, and
/// that is the product's own mode** — [`crate::launch::listener`] sets the child's
/// rebuilt listener non-blocking, because a `std` socket's mode is inherited by
/// every accepted connection and tokio cannot see it, so the child's copy is the
/// one that serves. A role that called `accept` once therefore only worked when
/// the parent happened to connect first, and it panicked `WouldBlock` whenever the
/// child won that race — which `events` and `signals` did as soon as the launch
/// stopped paying for `icacls` spawns. The retry is bounded by the same
/// [`TIMEOUT`] every other wait in this harness is, so a child nobody connects to
/// fails the test instead of hanging it.
pub fn accept(listener: &TcpListener, what: &str) -> TcpStream {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        match listener.accept() {
            Ok((stream, _)) => return stream,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "{what} was not connected to within {TIMEOUT:?}"
                );
                thread::sleep(ACCEPT_POLL);
            }
            Err(err) => panic!("{what}: accept: {err}"),
        }
    }
}

/// Answer one loopback connection through the inherited listener.
///
/// **The read is bounded, and that is what makes this a test device rather than
/// a way to hang the suite**: a child that died before it reached its `accept`
/// answers nothing at all, and an unbounded read would keep the harness waiting
/// for a failure that has already been reported in full. The unwinding that
/// follows a failure drops the caller's supervision, and the job with it, which
/// is how the child is killed rather than left behind.
pub fn answer(address: SocketAddr) {
    let mut stream =
        TcpStream::connect_timeout(&address, TIMEOUT).expect("connect to the confined child");
    stream
        .set_read_timeout(Some(TIMEOUT))
        .expect("bound the read");
    let mut pong = [0u8; 4];
    stream
        .read_exact(&mut pong)
        .expect("the confined child answered the inherited listener");
    assert_eq!(&pong, b"pong", "the confined child answered something else");
}

#[cfg(test)]
mod tests {
    //! The role entry points the harness spawns, and the two properties of the
    //! harness that only the libtest binary can check: that the names it spawns
    //! exist here ([`CHILD_ROLE`]'s rule), and that a launch waits for the
    //! process-wide slot ([`LAUNCH`]'s).
    //!
    //! The four role functions are not tests in the ordinary sense — they are
    //! this binary's child entry points, inert in the parent's own run, exactly
    //! as the top-level `child_role`/`launcher_role` shims of the suites under
    //! `tests/` are.

    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::mpsc,
        time::Duration,
    };

    use super::*;

    /// The confined child's role, as this binary's libtest entry point.
    ///
    /// The dispatch is [`child_role_entry`]'s; the half it installs here is the
    /// plain one, because this module's launches (the backend's network and
    /// profile legs, `process`'s launch legs) have no stop to catch — the signal
    /// suite's binary has its own shim for that.
    #[test]
    fn child_role() {
        child_role_entry(super::child_role);
    }

    /// The launcher's role, as this binary's libtest entry point.
    ///
    /// `false` is the conservative half: publishing the bound address is the
    /// events suite's need, and its own binary's shim asks for it.
    #[test]
    fn launcher_role() {
        super::launcher_role(false);
    }

    /// **Every name the harness spawns is a real test in this binary**, which is
    /// the half of the re-exec mechanism no assertion about a launch can reach
    /// on a host that cannot confine: a name libtest does not know runs zero
    /// tests and exits `0`, so the child would look like one that quietly did
    /// nothing.
    ///
    /// libtest's `--list` is the oracle, and it is asked of this binary — the
    /// same `env::current_exe()` a launch re-enters.
    #[test]
    fn every_role_the_harness_spawns_is_a_test_in_this_binary() {
        let listed = Command::new(env::current_exe().expect("the test binary"))
            .arg("--list")
            .output()
            .expect("run this binary with --list");
        assert!(
            listed.status.success(),
            "this binary refused --list: {}",
            String::from_utf8_lossy(&listed.stderr)
        );
        let text = String::from_utf8_lossy(&listed.stdout);
        for role in [CHILD_ROLE, LAUNCHER_ROLE] {
            let entry = format!("{role}: test");
            assert!(
                text.lines().any(|line| line == entry),
                "the harness spawns {role:?}, which is not a test in this binary, so libtest \
                 would run nothing and exit 0 — the listing is:\n{text}"
            );
        }
    }

    /// **A launch waits for the slot another launch holds**, which is the whole
    /// of what [`LAUNCH`] promises `docs/tests.md`'s concurrency rule.
    ///
    /// The proof is a wait, not an assertion about the lock's internals: this
    /// thread takes the slot, a second thread *enters a real launch entry*, and
    /// the test requires that the entry has not returned by the time this thread
    /// lets go.
    ///
    /// **The tree is never materialised, so the entry cannot get far even after
    /// it is let in**: it panics on its first step — writing the probe into a
    /// report directory that does not exist — and that panic is caught here,
    /// because a failed launch is the documented shape and not this test's
    /// subject. What the panic buys is that the run is deterministic on every
    /// host (no confinement profile, no child) while the interval stays the whole
    /// of the evidence: a launch that did *not* wait for the slot would reach
    /// that same panic in microseconds, so the message below is what a broken
    /// lock looks like.
    #[test]
    fn a_launch_waits_for_the_slot_another_launch_holds() {
        let held = LaunchGuard::acquire();
        let dir = tempfile::tempdir().expect("temp dir");
        let tree = Tree::at(dir.path());
        let (entered_tx, entered_rx) = mpsc::channel();
        let waiting = thread::spawn(move || {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                launch_confined(&tree, AccessMode::ReadWrite, Options::default(), None)
            }));
            entered_tx.send(()).expect("the launcher reports back");
        });

        assert!(
            entered_rx.recv_timeout(Duration::from_secs(1)).is_err(),
            "a launch ran to completion while another launch held the process-wide slot, so two \
             confined children can be alive at once and docs/tests.md's concurrency rule is a \
             claim no mechanism keeps"
        );
        drop(held);
        entered_rx
            .recv_timeout(TIMEOUT)
            .expect("the launch never got the slot the test released");
        waiting.join().expect("the launching thread");
    }
}
