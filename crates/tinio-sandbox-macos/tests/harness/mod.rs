//! The re-exec harness shared by the macOS confinement tests.
//!
//! A confined child is a copy of this test binary, launched through the crate's
//! real `confine` — so the profile, the listener's descriptor handover and the
//! payload on stdin are all exercised. The child reports through a file inside a
//! writable exposure rather than on stdout, because stdout is the event channel
//! (decision 19) and the report is not an event.
//!
//! **There are three roles in one binary, and the argv filter is what separates
//! them.** `confine` re-invokes *its own* process (shared §3): it derives the
//! child's program from `current_exe()` and its argv from `args_os()`, so the
//! filter the confined child runs under is the filter the process that called
//! `confine` was started with — and libtest reads a positional argument as a
//! filter, which makes the listener-descriptor element the launcher appends one
//! too. The child therefore runs the tests *its launcher's* argv selects, and
//! the only way to select one exact probe — short of a test seam on the
//! production launcher, which the next paragraph rejects — is for the process
//! calling `confine` to be one whose argv already names it. **That is the design,
//! and it is why [`run_confined`] re-executes this binary as a launcher copy**
//! whose own argv names the probe: the copy calls `confine`, the child inherits
//! the filter and runs exactly the probe, and the original process reads the
//! report the probe wrote. Without the copy the child would carry the descriptor
//! element as its only filter, libtest would print `running 0 tests`, exit 0 and
//! write no report, and the parent would fail with a missing file that says
//! nothing about why.
//!
//! **The alternative is a seam this platform does not have.** A macOS analogue of
//! P2's `Options { program, args }` — the test-only override that lets a Windows
//! child be a role function of the test binary instead of the suite — is what
//! shared §3 grants Windows and not macOS, and adding it here would put a
//! test-only field on the production launcher to buy what one re-exec already
//! buys.
//!
//! The three roles, then: the **test runner** (plain `cargo test`), the
//! **launcher copy** (called by [`run_confined`], identified by [`LAUNCH_ENV`],
//! runs [`launch`]), and the **confined child** (identified by the payload on
//! stdin — decision 17 — or, in the pruning experiment, by [`CHILD_ENV`]).
//!
//! Each test target declares `mod harness;` and takes what it uses, so none of
//! them uses everything — `startup.rs` is the pruning experiment's caller and
//! `exposures.rs`/`network.rs` never prune. Hence the module-wide `dead_code`
//! allow, which `crates/tinio-server/tests/common/mod.rs` carries for the same
//! reason.
//!
//! **Every path here is imported rather than spelled out**, because this crate's
//! manifest turns `clippy::absolute_paths` into a warning and the workspace gate
//! is `-D warnings` on the crate's own `--target x86_64-apple-darwin` run
//! (`docs/style.md`'s import rule).

#![allow(dead_code)]
// **The shared harness of the macOS test targets, and a stated exemption because clippy's test
// keys do not reach it** (docs/style.md §Panics, design 2026-10-09 D7): this file is plain
// `pub fn`s with no `#[test]`/`#[cfg(test)]` marker, so `allow-*-in-tests` never applies — and
// each target root that declares `mod harness;` states the same exemption for its own helpers.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a test target's harness helpers (docs/style.md §Panics)"
)]

use std::{
    env,
    ffi::OsString,
    fmt::Display,
    fs,
    io::{self, Error, Write as _},
    net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    os::unix::net::UnixListener,
    path::{Path, PathBuf},
    process::{Child as ProcessChild, Command, Stdio, exit},
    sync::OnceLock,
    time::Duration,
};

use tinio_sandbox_core::{
    AccessMode, Adopted, Confinement, ControlHandle, LaunchPlan, Sandbox, adopt, is_listener_fd_arg,
};
use tinio_sandbox_macos::{
    Mechanism, Options, SANDBOX_EXEC, apply, confine,
    launch::{command_args, listener},
    profile::{Profile, profile, rule_count, with_canonical_paths},
};
use tinio_util::testing::job;

/// Selects the child role in the re-executed test binary.
pub const CHILD_ENV: &str = "TINIO_SANDBOX_MACOS_CHILD";

/// Selects the **mechanism** the harness's launcher copy confines with, so the
/// same suite drives both.
///
/// **`sandbox-exec` is what an unset variable means**, which is the platform's own
/// default: the suites and the pruning experiment have always confined that way,
/// and a leg that wants the in-process route sets this to `sandbox-init` and
/// otherwise changes nothing. It travels through the environment rather than
/// through the parameter text because it is the *launcher's* choice — the child
/// learns it from the launch payload, exactly as the product's child does.
///
/// **The name is [`Mechanism::ENV`]'s and not a second spelling of it**: the
/// variable is the platform's (`tinio-sandbox-macos`'s `mechanism` module owns it,
/// and the CLI reads the same one), so the harness aliases the owner's constant
/// rather than repeating the string — a rename that reached only one of the two
/// would leave this suite driving a variable nothing reads.
pub const MECHANISM_ENV: &str = Mechanism::ENV;

/// The mechanism the launcher copy resolves, from [`MECHANISM_ENV`].
///
/// A harness copy of the product's own resolution, so a leg can select either
/// route without the suite growing a second launcher. An unknown value falls back
/// to the default rather than refusing: this is a test harness, and the platform's
/// refusal for a bad value is its own unit test's subject.
pub fn mechanism() -> Mechanism {
    Mechanism::from_env(env::var(MECHANISM_ENV).ok().as_deref()).unwrap_or_default()
}

/// The options that mechanism asks for, as the product's arm builds them.
pub fn options() -> Options {
    Options::default().with_mechanism(mechanism())
}

/// Whether this run is the **confined child of the in-process mechanism**, which
/// is the one role that has to apply its own confinement.
///
/// The product's child does this in `try_run` before anything else; the harness's
/// child is this test binary, and its `None` arm is the earliest thing in it. The
/// payload decides — not the environment — so the harness reads what the launcher
/// actually handed over.
///
/// **The step itself is the platform's** ([`apply::self_confinement`], the same
/// call the product's child makes), so which mechanisms a child may apply and what
/// a frame naming the `exec`-wrapping one means are one rule rather than two: this
/// wrapper only answers in the harness's own voice — a panic rather than the
/// product's refusal — and a frame the platform refuses fails the probe here.
pub fn apply_self(adopted: &Adopted) {
    if let Err(refused) = apply::self_confinement(adopted) {
        panic!("the child applies its own confinement: {refused}");
    }
}

/// Carries the child's parameters to the launcher copy. Used by
/// [`run_confined`] (whose launcher copy has no payload yet) and by the pruning
/// experiment, which never has one; the product path passes the same text in the
/// payload's configuration bytes.
pub const PARAMS_ENV: &str = "TINIO_SANDBOX_MACOS_PARAMS";

/// Selects the **launcher copy** role — the process [`run_confined`] re-executes
/// so that the confined child inherits a filter.
pub const LAUNCH_ENV: &str = "TINIO_SANDBOX_MACOS_LAUNCH";

/// The pruning experiment's opt-in, and its **value is the rule id to drop** —
/// the one knob `scripts/prune-profile.sh` has.
pub const PRUNE_ENV: &str = "TINIO_SANDBOX_MACOS_PRUNE";

/// The rule id the pruning experiment is running without, or `None` for the
/// product path.
///
/// **`None` is unconditional and cannot be lost by accident, which is the point
/// of reading the value rather than the variable's presence.** A set-but-empty
/// variable — an `export TINIO_SANDBOX_MACOS_PRUNE` with no value, a CI
/// declaration left blank — used to take the pruned path, and `remove_rule`
/// answers the text unchanged for an empty id: the probe then passed with
/// `payload=none`, silently measuring a launch whose payload handover was never
/// exercised. Now the confined path is the default for every unset, empty or
/// whitespace-only value, and the pruned path needs a value that names a rule
/// (the experiment's own caller, `scripts/prune-profile.sh`, is the only
/// intended one). A value naming no rule is **loud**, not silent:
/// [`run_pruned`] refuses it, and the script reads that refusal's own line as
/// "not present" rather than as "required".
pub fn pruned_rule() -> Option<String> {
    env::var(PRUNE_ENV)
        .ok()
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())
}

/// The confined child's report, as `key=value` lines.
pub struct Report(String);

impl Report {
    /// The report as the child wrote it.
    pub fn text(&self) -> &str {
        &self.0
    }

    /// The value of `key`, if the child reported it.
    pub fn value(&self, key: &str) -> Option<&str> {
        let prefix = format!("{key}=");
        self.0.lines().find_map(|line| line.strip_prefix(&prefix))
    }

    /// Assert the child reported exactly this value. The whole report is the
    /// failure message: on a macOS run it is the only evidence there is.
    pub fn expect(&self, key: &str, value: &str) {
        assert_eq!(self.value(key), Some(value), "report:\n{}", self.0);
    }
}

/// One `key=value` line.
pub fn line(key: &str, value: impl Display) -> String {
    format!("{key}={value}\n")
}

/// Everything a confined child needs, adopted **once**.
///
/// This is **not** `tinio_sandbox_core::Child` — that type is the launcher's
/// handle on a running child, and it is never in scope in this module. The name
/// carries that distinction rather than leaving it to a comment: a reader of
/// `ConfinedChild` does not have to hold both crates' vocabularies to know which
/// side of the launch they are looking at.
///
/// The payload arrives once and only once (design §3 step 7), so `adopt`
/// must not be called twice in one process: stdin is consumed by the first read
/// and the second would see an empty stream. This type is how every child-side
/// probe gets its parameters and its listener without a second call.
pub struct ConfinedChild {
    /// The payload, when the real launcher started this process. `None` in the
    /// pruning experiment, which has no payload.
    pub adopted: Option<Adopted>,
    /// This run's parameters.
    pub params: Vec<(String, String)>,
    /// `ok` when the payload validated, `none` in the pruning experiment. A
    /// test asserting `payload=ok` is asserting the whole handover.
    pub payload: &'static str,
    /// The socket, rebuilt from the descriptor **on the first call and never
    /// again** — see [`ConfinedChild::listener`].
    listener: OnceLock<io::Result<TcpListener>>,
}

impl ConfinedChild {
    /// One parameter, or a panic naming the whole set.
    pub fn param(&self, key: &str) -> String {
        self.params
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| panic!("no `{key}` in {:?}", self.params))
    }

    /// The listener the launcher handed over, rebuilt from the descriptor —
    /// **once**, and the same socket on every call.
    ///
    /// The once is not tidiness. `listener` calls `OwnedFd::from_raw_fd`, whose
    /// contract is at most one call per descriptor number (Task 2's `# Safety`):
    /// a repeatable accessor is the easiest way to break it, and a probe that
    /// asked twice would take ownership of one descriptor twice and close it
    /// twice. `OnceLock` also hands a later caller the first caller's error
    /// rather than a second rebuild.
    pub fn listener(&self) -> io::Result<&TcpListener> {
        self.listener
            .get_or_init(|| {
                let adopted = self
                    .adopted
                    .as_ref()
                    .ok_or_else(|| Error::other("the pruning experiment has no payload"))?;
                listener(adopted)
            })
            .as_ref()
            .map_err(|err| Error::new(err.kind(), err.to_string()))
    }
}

/// Whether this process is a confined child or the pruning experiment's,
/// **without consuming the payload**.
///
/// The launcher's descriptor element in argv is the marker on the product path;
/// the pruning experiment, which has no payload by construction, uses
/// [`CHILD_ENV`] instead. Every test — probe and parent alike — checks this
/// first, and the check is also what keeps the **launcher copy** out of the
/// probe: that process runs the probe's own test (it is the filter) but carries
/// no descriptor element, so it takes the other branch and calls
/// [`launch`]. A probe that adopted the payload twice would find the pipe
/// drained, which is why the payload is read in exactly one place — [`child`].
pub fn is_child_process() -> bool {
    env::args_os().any(|arg| is_listener_fd_arg(&arg)) || env::var_os(CHILD_ENV).is_some()
}

/// `Some` when this process is a confined child, `None` when it is the runner.
///
/// Both discriminators are the design's: the payload (decision 17 — never an
/// environment marker) for the product path, and the environmental mark of the
/// pruning experiment, whose child has no payload by construction. Calling this
/// reads the pipe, so it is called **exactly once per process**, by the probe.
pub fn child() -> Option<ConfinedChild> {
    let adopted = adopt(env::args_os(), io::stdin().lock()).ok().flatten();
    if adopted.is_none() && env::var_os(CHILD_ENV).is_none() {
        return None;
    }
    // **The child's own confinement, applied here** — the instant this process
    // knows it is the child, and before the probe runs a single check. That is the
    // product's ordering (`try_run`'s first arm) and it is what makes the probe's
    // assertions assertions *about a confined process*: a probe that ran first
    // would measure an unconfined one and pass.
    if let Some(adopted) = adopted.as_ref() {
        apply_self(adopted);
    }
    let raw = match &adopted {
        // `Adopted::config` is a `&str` (the core's accessor), and the params
        // below are parsed from bytes because the payload path and the
        // environment path carry the same text.
        Some(adopted) => adopted.config().as_bytes().to_vec(),
        None => env::var(PARAMS_ENV).unwrap_or_default().into_bytes(),
    };
    let payload = if adopted.is_some() { "ok" } else { "none" };
    Some(ConfinedChild {
        adopted,
        params: parse_params(&raw),
        payload,
        listener: Default::default(),
    })
}

fn parse_params(bytes: &[u8]) -> Vec<(String, String)> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Write the report where the parent will read it, then end the child process.
///
/// The path is always inside a writable exposure — writing it is itself a test
/// of the write rule. Ending the process rather than returning is deliberate:
/// the report is the whole of what this child is for, and exiting keeps it the
/// last thing the child does (the filter means libtest selected the probe and
/// nothing else, so nothing else was pending).
///
/// **`payload` is written here, by the one function every probe ends through,
/// and never by a probe.** It is the discriminator the product path has no other
/// way to assert — `ok` proves the payload on stdin *and* the descriptor in argv
/// both arrived (decision 17), `none` is the pruning experiment, which has no
/// payload by construction — and a probe that forgot it would leave every
/// `report.expect("payload", …)` in the suite asserting a key nothing writes.
/// Writing it first keeps each probe's own lines in the order it logged them.
pub fn finish(child: &ConfinedChild, log: &str) -> ! {
    let path = child.param("report");
    let body = format!("{}{log}", line("payload", child.payload));
    fs::write(&path, body).unwrap_or_else(|err| panic!("write {path}: {err}"));
    exit(0);
}

/// Run `test_name` confined through the crate's real launcher, and read back its
/// report.
///
/// **The confined child is launched by a launcher copy of this binary, and that
/// is the design rather than a detour** — the module docs carry the argument.
/// `confine` derives the child's argv from the calling process's own, so the
/// process that calls it has to be one whose argv already selects this probe, and
/// this function is that process's spawner: it re-executes `current_exe()` with
/// [`child_argv`] naming the probe and [`LAUNCH_ENV`] marking the launcher role,
/// so the child the copy confines inherits a filter that matches and runs exactly
/// the probe. The descriptor element the launcher appends is then a second,
/// harmless filter — the first one is what selected the test. Were the copy not
/// there, that element would be the child's *only* filter: libtest would run
/// nothing (`running 0 tests`, exit 0), no report would be written, and the
/// missing file would be all the parent could report.
pub fn run_confined(sandbox: &Sandbox, extra: &[(&str, String)], test_name: &str) -> Report {
    let (mut launcher, report) = spawn_confined(sandbox, extra, test_name);
    let status = launcher.wait().expect("wait for the launcher copy");
    assert!(status.success(), "the launcher copy exited with {status:?}");

    let text = fs::read_to_string(&report).unwrap_or_else(|err| {
        panic!("the confined child wrote no report ({err}); the launcher exited with {status:?}")
    });
    Report(text)
}

/// Spawn the launcher copy **without waiting**, so the caller can watch the confined child while
/// it is still running.
///
/// [`run_confined`] is this function plus a wait and a report read, and it cannot serve a caller
/// whose subject is the child's *process* rather than its report: the wait hides the whole
/// lifetime the observation happens in. The descriptor-handover leg is that caller — it reads the
/// confined child's table with `lsof` from the parent while the child is parked, so it needs the
/// launcher copy's handle (its pid is how the child is found) before the launch has ended.
///
/// Everything else is [`run_confined`]'s own shape, and deliberately so: the same parameter text,
/// the same launcher-copy argv, the same stdout discarding (the launcher relays the child's event
/// stream, which is not this process's output to print). The report path comes back beside the
/// handle, so the caller reads the child's report exactly as [`run_confined`] would.
pub fn spawn_confined(
    sandbox: &Sandbox,
    extra: &[(&str, String)],
    test_name: &str,
) -> (ProcessChild, PathBuf) {
    let report = report_path(sandbox);
    let params = launcher_parameters(sandbox, extra, &report);

    let child = Command::new(env::current_exe().expect("current exe"))
        .args(child_argv(test_name))
        .env(LAUNCH_ENV, "1")
        .env(PARAMS_ENV, &params)
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn the launcher copy");
    (child, report)
}

/// The parent's half of the outbound fixture: a listener at an address this
/// process proved reachable **from outside** the confinement, with the proof
/// already made.
///
/// **The proof is the fixture rather than the probe's business** (design §12): a
/// confined `connect` that fails proves nothing on a machine with no route, so
/// every leg that asserts the refusal first asserts the reachability of that exact
/// address here. `network.rs` and `injection.rs` both drive it — the first as the
/// confinement's own subject, the second as the negative control that makes a
/// hostile exposure path's `refused(permission denied)` mean something.
///
/// **On loopback a completed `connect` is a completed handshake**, so no `accept`
/// is needed to prove reachability and this function never touches the backlog.
///
/// **The returned listener must stay alive for as long as the probe runs** — the
/// address is listening only while it is bound, and dropping it would make the
/// confined probe's refusal an accident of a closed port. Bind it to a named
/// `_target` (not a bare `_`, which drops it on the spot).
pub fn reachable_target() -> (TcpListener, SocketAddr) {
    let target = TcpListener::bind("127.0.0.1:0").expect("bind target");
    let addr = target.local_addr().expect("local addr");
    TcpStream::connect(addr).expect("an unconfined process must reach the target");
    (target, addr)
}

/// The child's half: attempt one outbound connection to the `outbound` parameter
/// and answer the report line for it — `CONNECTED`, `refused(<kind>)`, or
/// `bad-address` when the parameter is not a socket address at all.
///
/// **One probe, because the two legs that drive it assert the same vocabulary.**
/// `network.rs` renders it as the confinement's subject and `injection.rs` as the
/// body a hostile exposure path would widen; both assert on the value this
/// function writes (`refused(permission denied)` when the profile grants nothing,
/// `CONNECTED` when a grant reached the compiled text), and both used to carry the
/// five lines below verbatim. What is *not* shared is what the leg makes of them:
/// each file's own comment says which assertion it is driving.
///
/// The refusal is a **render, not an assertion**: the leg compares the string, so
/// a probe that could not reach anything still has to say why.
pub fn probe_outbound(child: &ConfinedChild) -> String {
    let target = child.param("outbound");
    let outbound = match target.to_socket_addrs().ok().and_then(|mut a| a.next()) {
        Some(addr) => match TcpStream::connect_timeout(&addr, Duration::from_secs(5)) {
            Ok(_) => "CONNECTED".to_string(),
            Err(err) => format!("refused({})", err.kind()),
        },
        None => "bad-address".to_string(),
    };
    line("outbound", outbound)
}

/// The launcher role: `confine`, run in the **launcher copy** of this binary.
///
/// A no-op in any process that [`run_confined`] did not spawn — the ordinary
/// test runner and the confined child alike — so every probe's `None` arm can
/// call it unconditionally, and the child (whose filter also selects the probe)
/// returns from it immediately.
pub fn launch() {
    if env::var_os(LAUNCH_ENV).is_none() {
        return;
    }
    let params_text = env::var(PARAMS_ENV).unwrap_or_default();
    let params = parse_params(params_text.as_bytes());
    let sandbox = sandbox_from_params(&params);

    // **The control-channel fixture, bound only when the caller names it** (`control` in the
    // parameter text). It is the production shape: `crates/tinio-cli/src/sandbox/macos.rs` binds
    // the socket, keeps its own copy, and puts one `ControlHandle::duplicate(channel.socket())` on
    // the plan for the platform to clear. **The two copies are
    // what a descriptor-handover observation is about** — the duplicate is the one
    // `make_inheritable` touches, and the launcher's own socket keeps the close-on-exec flag it
    // was bound with, so a test that reads the confined child's table can say which of the two
    // crossed the exec.
    //
    // **The baseline descriptors are held before the socket is bound**, which is the whole point
    // of them: the listener and the control duplicate then land past every descriptor number a
    // process inherits (`scripts/verify-descriptor-handover.py`'s `BASELINE`), so an observation
    // can never mistake a libtest-inherited number for the handover. They are `/dev/null` opens,
    // closed on exec by their own flag, so the child inherits none of them.
    let handover = control_socket(&params).map(|path| {
        let baseline = hold_baseline();
        let channel = UnixListener::bind(path).expect("bind the control socket");
        let handle = ControlHandle::duplicate(&channel).expect("duplicate the control socket");
        (baseline, channel, handle)
    });

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");

    // The launcher owns the port (decision 16) and hands the descriptor over in
    // argv (design §3 step 7). The payload carries the configuration text and
    // nothing else; the launcher neither parses it nor rewrites it.
    let mut plan = LaunchPlan::for_launch(params_text).with_listener(listener);
    // The launcher's own socket and the baseline table live to the end of this function — after
    // the wait — because the descriptor the observation asserts on must still be open then. The
    // binding is underscore-named rather than dropped: only a binding keeps them alive.
    let _launcher_only = match handover {
        Some((baseline, channel, handle)) => {
            plan = plan.with_control(handle);
            Some((baseline, channel))
        }
        None => None,
    };

    let mut child = match confine(&sandbox, &plan, options()).expect("confine") {
        Confinement::Supervise(child) => child,
        Confinement::InPlace => panic!("macOS confines a child, never in place"),
    };

    // One inbound connection, written while the child is running, because the
    // child's `accept` probe would otherwise block until the harness gave up.
    // A child that never accepts pays one connection the backlog absorbs and
    // then closes; a child that does accept reads exactly this line, which is
    // how the network test proves it reached the inherited listener.
    if let Ok(mut stream) = TcpStream::connect(addr) {
        let _ = stream.write_all(b"ping\n");
        let _ = stream.flush();
    }

    // The wait blocks, because the core's `Child` hides the process handle
    // behind its wait closure: the harness cannot poll or kill it. That is why
    // every probe is written never to block indefinitely — the accept probe's
    // connection is the write above — and why Task 9's end-to-end test, which
    // owns a real `std::process::Child`, is where a timeout can actually kill.
    let status = child
        .wait()
        .unwrap_or_else(|err| panic!("wait for the confined child: {err}"));
    assert!(
        status.success(),
        "the confined child exited with {status:?}"
    );
}

/// The parameter that asks the launcher role to bind a **control-channel socket** at this path
/// and put the production duplicate on the plan.
///
/// It is a parameter rather than an unconditional fixture because every other test's launch hands
/// over a listener and no control channel: the channel is what `make_inheritable` clears, and a
/// suite that always cleared one would change the shape each sibling test measures.
pub const CONTROL_PARAM: &str = "control";

/// How many descriptors the handover fixture holds open so the listener and the control socket
/// land past every number a process inherits.
///
/// The trick is `scripts/verify-descriptor-handover.py`'s `BASELINE`: a process starts with the
/// runtime's, libtest's and a shell's descriptors, so a membership test on a low number can be
/// satisfied by something that has nothing to do with this handoff. It is `pub` because the test
/// that observes the spawned process asserts its fixture's numbers against it.
pub const BASELINE: usize = 100;

/// Hold [`BASELINE`] descriptors open until the returned vector is dropped.
///
/// `/dev/null` reads are `O_CLOEXEC` (Rust opens files that way), so the child inherits none of
/// them — they exist only to push the fixture's own numbers up, and the vector's binding is what
/// keeps them open.
fn hold_baseline() -> Vec<fs::File> {
    (0..BASELINE)
        .map(|_| fs::File::open("/dev/null").expect("hold a baseline descriptor"))
        .collect()
}

/// The control socket a caller asked this launcher to bind, from its parameters.
fn control_socket(params: &[(String, String)]) -> Option<PathBuf> {
    params
        .iter()
        .find(|(key, _)| key == CONTROL_PARAM)
        .map(|(_, value)| PathBuf::from(value))
}

/// The parameters the launcher copy needs, which is [`parameters`] plus the
/// **sandbox itself**: the copy is a different process, so the exposures the
/// test built have to travel with it.
///
/// The format is the same `key=value` text, and the reason a path can ride in it
/// at all is that this crate refuses an exposure carrying a control character.
/// **The refusal is a named place**: the `control character` arm of Task 1's
/// `validate_path` in `profile.rs` — the check `profile` runs over every exposure
/// before a rule is rendered for it, held by that task's
/// `control_characters_are_refused` — so no exposure that reaches a profile can
/// contain the newline this format separates its lines with.
///
/// **The refusal runs where the profile is rendered, which on this channel is one
/// step after this function.** That ordering is safe for a reason worth stating
/// rather than assuming: the exposures here are the test's own, built by the
/// process that reads the answer, and the product path never uses this format at
/// all — its configuration travels as the payload's JSON. The remaining values on
/// the channel are the harness's own too: the `report` path, the `listen`
/// address, and each probe's `extra`.
fn launcher_parameters(sandbox: &Sandbox, extra: &[(&str, String)], report: &Path) -> String {
    let mut text = parameters(extra, report);
    for exposure in sandbox.exposures() {
        let access = if exposure.access.is_read_only() {
            "ro"
        } else {
            "rw"
        };
        text.push_str(&line(
            "expose",
            format!("{access}:{}", exposure.host.display()),
        ));
    }
    text
}

/// The launcher copy's [`Sandbox`], rebuilt from its parameters.
///
/// The same set the test built, in the same order, because the profile is
/// rendered from it: an exposure lost in the round trip would be a rule missing
/// from the profile under test, and the probe would be measuring something the
/// product never launches.
fn sandbox_from_params(params: &[(String, String)]) -> Sandbox {
    let mut sandbox = Sandbox::new();
    for (key, value) in params {
        if key == "expose" {
            let (access, host) = value.split_once(':').expect("access:path");
            let access = if access == "rw" {
                AccessMode::ReadWrite
            } else {
                AccessMode::ReadOnly
            };
            sandbox = sandbox.expose(host, access);
        }
    }
    sandbox
}

/// Launch the profile **with one rule removed**, without the launcher.
///
/// This exists only for `scripts/prune-profile.sh`: the necessity of a baseline
/// allowance is measured by rerunning the probe without it, and `confine`
/// renders its own profile so it cannot express "one rule fewer". **The profile
/// rendered here is the one the launch renders** — `profile(&with_canonical_paths(…))`
/// — so the removed rule is the only difference from a real launch, which is the
/// property the whole experiment rests on. The child gets no payload here, which
/// is why this mode passes its parameters through the environment. Never use this
/// to assert product behaviour — that is what [`run_confined`] is for.
///
/// **This leg is `sandbox-exec`'s and stays so**, whatever [`MECHANISM_ENV`] says:
/// it *builds the helper's own invocation* to drop one rule from the text, which is
/// the experiment's whole method — an in-process reap would have to fork a child
/// per rule, and the table it prunes belongs to the render the helper compiles.
/// [`profile`] is therefore asked for `SandboxExec` explicitly rather than through
/// [`mechanism`].
pub fn run_pruned(sandbox: &Sandbox, extra: &[(&str, String)], test_name: &str) -> Report {
    let report = report_path(sandbox);
    let params = parameters(extra, &report);
    let id = pruned_rule().unwrap_or_default();

    let rendered = profile(
        with_canonical_paths(sandbox).sandbox(),
        Mechanism::SandboxExec,
    )
    .expect("profile");
    let pruned = match remove_rule(&rendered.text, &id) {
        Some(pruned) => pruned,
        // **Distinct from "required", and it has to stay distinct.** The id is
        // not in the rendered profile at all — an id the caller's sandbox never
        // produces, for instance — so this run can say
        // nothing about necessity. The script greps for the line below; without
        // it, a bare failure is classified as "required" and a rule is then
        // documented as load-bearing on the strength of a run that never removed
        // it.
        None => {
            eprintln!("prune: rule-not-present id={id}");
            panic!("rule `{id}` is not in the rendered profile — nothing to remove");
        }
    };

    // **The whole argv is the launch's own render**, so a pruned run measures the
    // invocation the launch performs with one rule removed and nothing else changed:
    // `command_args` writes the `-D` pairs (every parameter the pruned text still
    // names — a parameter the profile names but the launch does not define is a
    // compile error, exit 65, not a narrower profile — plus `P`, the one the
    // `process-exec` literal resolves to), the program element, the child's argv,
    // and no descriptor element, because this probe hands nothing over. Building
    // the argv here instead is what let the two spellings drift.
    //
    // **The path is canonicalised the way the launch canonicalises it**
    // (`canonical_program` in `launch.rs`): both the `P` literal and the program
    // element are the canonical `current_exe()`. An un-canonical `P` makes
    // `sandbox-exec` exit 71 with `execvp() … Operation not permitted` before any
    // rule is consulted, because it resolves the target before the check — so a
    // pruned run that skipped this would measure a profile the launch never
    // renders.
    let exe = env::current_exe().expect("current exe");
    let program = fs::canonicalize(&exe).unwrap_or(exe);
    let args = command_args(
        &program,
        &child_argv(test_name),
        &Profile {
            text: pruned.clone(),
            rules: rule_count(&pruned),
            params: rendered.params,
        },
        None,
    );
    let mut command = Command::new(SANDBOX_EXEC);
    command
        .args(&args)
        .env(CHILD_ENV, "1")
        .env(PARAMS_ENV, &params);
    let mut child = job::spawn(&mut command).expect("spawn sandbox-exec");
    let status = child.wait().expect("spawn sandbox-exec");

    let text = fs::read_to_string(&report).unwrap_or_else(|err| {
        panic!("the pruned child wrote no report ({err}); it exited with {status:?}\nprofile:\n{pruned}")
    });
    Report(text)
}

/// Where the child writes its report: inside the first **writable** exposure.
///
/// A confined process can only write where it was granted, so a report outside
/// the exposure set is refused by the very profile under test — and the
/// read-only run has no writable served root at all, which is why this picks
/// the first `ReadWrite` exposure rather than the first exposure. A stale report
/// from an earlier run is removed first, so a missing file always means "the
/// child could not write", never "an old file survived".
fn report_path(sandbox: &Sandbox) -> PathBuf {
    let host = sandbox
        .exposures()
        .iter()
        .find(|exposure| !exposure.access.is_read_only())
        .map(|exposure| exposure.host.clone())
        .expect("a confinement test needs at least one writable exposure");
    let path = host.join(".tinio-sandbox-test-report.txt");
    let _ = fs::remove_file(&path);
    path
}

/// Drop the line whose `; tinio:<id>` marker matches, if `id` is non-empty.
///
/// `None` means the id is **not in the text at all**, which is a different
/// answer from "the rule was removed and nothing broke" and must not be
/// reported as one: an empty `id` returns the text unchanged, and an id whose
/// rule is absent returns `None` so the caller can say so.
pub fn remove_rule(text: &str, id: &str) -> Option<String> {
    if id.is_empty() {
        return Some(text.to_string());
    }
    let marker = format!("; tinio:{id} ");
    let kept: Vec<&str> = text
        .lines()
        .filter(|line| !line.contains(&marker))
        .collect();
    if kept.len() == text.lines().count() {
        return None;
    }
    let mut out = kept.join("\n");
    out.push('\n');
    Some(out)
}

fn child_argv(test_name: &str) -> Vec<OsString> {
    vec![
        OsString::from(test_name),
        OsString::from("--exact"),
        OsString::from("--nocapture"),
    ]
}

/// The address every harness launch asks the server to bind — the kernel picks the port and the
/// child reports what it got. A constant rather than a parsed argument, because it is one value at
/// every call site.
const LISTEN_ADDR: &str = "127.0.0.1:0";

/// The `key=value` text the child reads its parameters from — **every line
/// terminated, including the last**.
///
/// The trailing newline is load-bearing rather than tidy: [`launcher_parameters`]
/// appends its own `expose=` lines to this text, and a `join("\n")`
/// without a final separator welds the last parameter here onto the first one
/// added there — `listen=127.0.0.1:0expose=rw:/srv/root`. The child then reads a
/// `listen` value it cannot parse and **no exposure at all**, which showed up as
/// a confined process with a `(deny default)` profile granting nothing: the
/// report write was refused and the whole probe failed for a reason that looked
/// like the profile generator's. Measured on macOS 15.8, 2026-09-25.
///
/// **It sat above [`LISTEN_ADDR`] until this was moved**, so both paragraphs were
/// that constant's doc and this function had none — the mistake the paragraph above
/// describes from its other side, and the reason the two are now on the items they
/// name.
fn parameters(extra: &[(&str, String)], report: &Path) -> String {
    let mut all: Vec<(String, String)> = extra
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    all.push(("report".to_string(), report.display().to_string()));
    all.push(("listen".to_string(), LISTEN_ADDR.to_string()));
    // Every line through [`line`], which owns the separator and the terminator: a hand-written
    // spelling of this format is what once welded `listen=…expose=…` into one token.
    all.iter().map(|(k, v)| line(k, v)).collect()
}
