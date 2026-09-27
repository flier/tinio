//! Process, DACL and console helpers for the Windows confined-server legs.

#![allow(dead_code)]

use std::{
    env, fs,
    net::SocketAddr,
    os::windows::process::CommandExt as _,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};

use tinio_core::acl::DEFAULT_ROOT_ACCESS_KEY;
use tinio_sandbox::{
    AccessMode,
    windows::{
        acl::{self, Target},
        control, sid, testing,
    },
};

use super::common::{SECRET_KEY, answer as probe, tinio_bin, wait_for_listening};

/// `CREATE_NEW_PROCESS_GROUP` — the launcher is spawned as the leader of its own
/// process group, which is what makes a console control event addressed at *it*
/// reach it and nothing else (Task 10's suite spawns the same way, for the same
/// reason).
pub const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

/// The bound [`exit_timeout`] returns when [`EXIT_TIMEOUT_VAR`] is unset: **15 s**,
/// the number this suite has always asserted on every host.
const DEFAULT_EXIT_TIMEOUT: Duration = Duration::from_secs(15);

/// The operator's override for [`exit_timeout`], in seconds — unset on every host
/// that does not need it.
///
/// Named `TINIO_*` like the project's other test knobs (`TINIO_SANDBOX_TEST_TARGET`,
/// `TINIO_E2E_CONFINEMENT`), and read only for the bound [`exit_timeout`] states
/// the measurement behind.
pub const EXIT_TIMEOUT_VAR: &str = "TINIO_LAUNCHER_EXIT_TIMEOUT_SECS";

/// How long the confined server may take to die once its launcher is gone, and
/// how long the launcher may take to exit once a stop has been forwarded to it:
/// **15 s by default, and the default is unchanged**.
///
/// Both are failures rather than waits: a child that outlives its launcher is a
/// job object that did not do its work, and a launcher that does not exit after a
/// stop is a relay that did not deliver.
///
/// **What the override moves is the clock, never the assertion.** Every host
/// without the pathology below keeps exactly the bound this suite has always had,
/// so CI's behaviour and the strength of the regression it catches — a launcher
/// that hangs — are untouched: a run that does not exit within the configured
/// bound still fails, nothing is skipped, and no leg reads the variable unless the
/// operator set it.
///
/// **Why any host needs one.** Writing the program directory's security descriptor
/// is three thousand times slower on this project's Windows host than on a fresh
/// directory carrying a **byte-identical** descriptor — measured 2026-09-24:
/// `Set-Acl` **9.208 s** on `…\target\debug` against **0.003 s** on a fresh
/// directory, while creating a file there took 0.003 s and writing the
/// directory's attributes 0.023 s. The cost is therefore in writing *that
/// object's* descriptor and not in its ACL, its contents, the volume, the
/// concurrent writers or the launcher's ACE count: the residual package ACEs were
/// removed (Access 68 → 11) and the latency stayed at 19–30 s per call.
/// `docs/tests.md`'s Windows section carries the whole experiment and the
/// controls that exclude the rest. **The launcher's grant is one such write**
/// (`icacls` on the program directory — its DACL *reads* are API calls, its
/// writes are the tool's), so on that host a 15 s bound is below the cost of a
/// single `icacls` call and the leg fails on the clock instead of on its subject.
///
/// **On such a host**: set [`EXIT_TIMEOUT_VAR`] to a larger number of seconds and
/// run the suite with `--test-threads=1`, so two launchers do not write the same
/// directory at once:
///
/// ```text
/// $env:TINIO_LAUNCHER_EXIT_TIMEOUT_SECS = '120'
/// cargo test -p tinio-server --test sandbox_e2e_windows -- --test-threads=1
/// ```
///
/// A value that is not a positive integer **fails here, by name and with the
/// value**, rather than silently falling back to a bound the operator did not
/// choose.
pub fn exit_timeout() -> Duration {
    match env::var(EXIT_TIMEOUT_VAR) {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(secs) if secs > 0 => Duration::from_secs(secs),
            _ => panic!(
                "{EXIT_TIMEOUT_VAR} must be a positive number of seconds (unset it for the \
                 {DEFAULT_EXIT_TIMEOUT:?} default), got {raw:?}"
            ),
        },
        Err(_) => DEFAULT_EXIT_TIMEOUT,
    }
}

/// `tinio server <root> --port <port> --sandbox`, as both of this file's run
/// shapes start it: **the argv, the environment and the creation flags are one
/// body**, so what distinguishes a run is which of them the leg keeps, never how
/// the launcher is invoked.
///
/// **The CLI serves an existing `DIR` or refuses it** ("pass an existing DIR"),
/// where the retired example created the root it was handed: the harness makes
/// it, and the legs that are about a *missing* root make their own absence.
///
/// **The credential pair is injected here rather than left to the operator's
/// environment.** Every request these legs make is signed by `common::signed`
/// with `DEFAULT_ROOT_ACCESS_KEY` and [`SECRET_KEY`], while the CLI reads
/// `[auth]` — and a first start *generates* a random pair into the config it
/// creates, so a leg that did not pin the pair would be answered
/// `AccessDenied`/`SignatureDoesNotMatch` for a request the harness itself
/// signed. Both names are the same two constants the signing side uses (not a
/// third copy of their values), the child inherits the launcher's environment,
/// and `TINIO_ACCESS_KEY`/`TINIO_SECRET_KEY` are the chain
/// `crates/tinio-server/tests/serve_read_only.rs`'s `Serve::spawn` uses for the
/// same reason.
fn launcher_command(root: &Path, port: u16) -> Command {
    fs::create_dir_all(root).expect("the served root");
    let mut command = Command::new(tinio_bin());
    command
        .args([
            "server",
            root.to_str().expect("a UTF-8 root"),
            "--port",
            &port.to_string(),
            "--sandbox",
        ])
        .env("TINIO_ACCESS_KEY", DEFAULT_ROOT_ACCESS_KEY)
        .env("TINIO_SECRET_KEY", SECRET_KEY)
        .creation_flags(CREATE_NEW_PROCESS_GROUP);
    command
}

/// One `tinio server <root> --sandbox` run, from the test's side.
pub struct Run {
    pub(super) launcher: Child,
    /// The launcher's stderr, which the confined child inherits: every line either
    /// process writes, and **the launcher's own `tracing` events among them**.
    /// `crates/tinio-cli/src/commands/start/run.rs` installs the subscriber
    /// (`log::build_subscriber` + `set_global_default`) **before** the
    /// `sandbox::confine` arm, and that order is what makes the events this file
    /// parses reach it at all: `step="grant the ACLs"`,
    /// `CreateAppContainerProfile`'s `taken_over`, `process::launch`'s
    /// `step="CreateProcessW"`. A launcher returns from `confine` and never reaches
    /// a subscriber built after that arm, so building it later would drop every one
    /// of them — which is the state two helpers here ([`Run::package_sid`] and
    /// [`Run::took_the_profile_over`]) were failing on until the order was fixed.
    pub(super) log: PathBuf,
    pub(super) address: SocketAddr,
}

/// The launcher's `grant the ACLs` event, as the one line of `text` that carries
/// it, or `None` when the launcher reported no such event.
///
/// **The event and the field are the contract; the line's shape is the
/// subscriber's.** The launcher writes `tracing`'s text format, whose timestamp,
/// level and module target belong to whatever subscriber the process installed and
/// whose field order is not part of the event, so identifying the event by a prefix
/// of the whole line would pin the wrong thing — and a rewritten banner would match
/// nothing while a reformatted event would stop matching. `text` is the raw log:
/// the ANSI SGR sequences `tracing`'s text formatter writes separate a field's name
/// from its `=`, so [`without_ansi`] runs first.
pub fn grant_event(text: &str) -> Option<String> {
    step_event(text, "grant the ACLs")
}

/// The one line of `text` that carries the `step="<step>"` event named by `step`,
/// or `None` when the process reported no such event.
///
/// **The caller's step name is the contract, and the line is not** — [`grant_event`]
/// states the rule, and this is the one body it and the legs' other step reads share:
/// the launcher platforms write `step="child exit"` with the code the child left
/// (`tinio-sandbox-windows`'s `process.rs`, the other end of its `CreateProcessW`
/// event) exactly as they write `step="grant the ACLs"`, so both are read the same
/// way rather than by a prefix test each leg spells for itself.
pub fn step_event(text: &str, step: &str) -> Option<String> {
    let needle = format!(r#"step="{step}""#);
    without_ansi(text)
        .lines()
        .find(|line| line.contains(&needle))
        .map(str::to_owned)
}

/// One `name=value` field of a line [`grant_event`] returned.
///
/// **A quoted value is trimmed, and that is measured rather than defensive.** The
/// launcher's `grant the ACLs` event renders as
/// `step="grant the ACLs" sid="S-1-15-2-…" exposures=2 granting=…` on this host
/// (2026-09-24): a field recorded from a string arrives with quotes around it,
/// while a numeric or `Display` field arrives bare — so the quotes cannot be
/// assumed away, and a parser that stripped the prefix and kept them would hand
/// `icacls` a SID with a `"` in it. Only the one field is read either way, so the
/// trim cannot make an absent `sid` look present.
fn field(line: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}=");
    line.split_whitespace()
        .find_map(|field| field.strip_prefix(&prefix))
        .map(|value| value.trim_matches('"').to_string())
}

/// **Every spawned process is waited for or killed on every path, red ones
/// included.** A leg that panics between `spawn` and its own kill — [`answer`]
/// does exactly that against a server that accepts and then says nothing — would
/// otherwise leave the launcher *and* the confined child alive for the rest of
/// the run: they hold the listener, the job and the ACEs, and they hold
/// `target\<profile>\tinio.exe` open, which breaks the next build of the
/// binary with `LNK1104` (measured against the retired example, twice, before
/// this impl existed). The path is `common::tinio_bin`'s, resolved beside the
/// test binary rather than named here.
///
/// Cheap when the launcher is already gone: `kill` and `wait` on a reaped child
/// both answer errors, and both are ignored here. Killing the launcher is also
/// what ends the confined child — `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` is the
/// only thing that can (decision 20).
impl Drop for Run {
    fn drop(&mut self) {
        let _ = self.launcher.kill();
        let _ = self.launcher.wait();
    }
}

impl Run {
    /// Spawn `tinio server <root> --sandbox` on `root` and wait for the endpoint.
    ///
    /// **stderr goes to a file and not to a pipe.** Everything the launcher and
    /// the confined child have to say is on stderr, and a pipe nobody drains
    /// becomes a child blocked on a full buffer — which reads as a hung server
    /// rather than as a full pipe. The file is rewritten by a second run on the
    /// same root, so read what you need from a run before starting the next.
    ///
    /// **The file sits beside the served root, never inside it.** The launcher is
    /// what creates the root, so a log written before the spawn has no directory
    /// to live in; and the served tree is the server's, where a diagnostic of the
    /// harness would be an object of the store under test. The confined child still
    /// reaches the file, because its stderr is an inherited handle: the access
    /// decision was made at the open, and no path is involved afterwards.
    ///
    /// What this shape adds to [`launcher_command`] is what it *waits for*: the
    /// endpoint the confined child reports, read off the stdout this run keeps
    /// piped. A leg whose subject is a launch that never reports wants [`Exit`]
    /// instead, which waits for nothing and keeps both streams as files.
    pub(super) fn spawn(root: &Path, port: u16) -> Self {
        let log = root.with_file_name("launcher.log");
        let stderr = fs::File::create(&log).expect("create the launcher's log");
        let mut launcher = launcher_command(root, port)
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawn tinio server");
        // The handshake is the harness's; the failure is this leg's, because a
        // launcher's log survives the kill that a child's stderr pipe does not.
        let address = match wait_for_listening(&mut launcher) {
            Ok(address) => address,
            Err(message) => fail(&mut launcher, &log, &message),
        };
        Self {
            launcher,
            log,
            address,
        }
    }

    /// The package SID this run's launcher reported.
    ///
    /// Read from the launcher's own report rather than re-derived here: a
    /// `PackageSid` built in *this* process would create the very profile the
    /// assertion is about, and its `Drop` would delete that profile while the
    /// launcher still needed it (Task 2).
    ///
    /// **The event is the contract, and the line is not.** [`grant_event`] finds
    /// the launcher's `grant the ACLs` event wherever the subscriber placed it; a
    /// banner that stopped being written fails here, with the log, rather than
    /// matching nothing quietly.
    pub(super) fn package_sid(&self) -> String {
        let log = self.log_text();
        let event = grant_event(&log).unwrap_or_else(|| {
            panic!(
                "the launcher did not report the package SID it was about to grant: no line of \
                 its log carried `step=\"grant the ACLs\"`. The launcher's log:\n{log}"
            )
        });
        field(&event, "sid").unwrap_or_else(|| {
            panic!("the launcher's grant event carried no `sid=` field: {event}\n{log}")
        })
    }

    /// Whether this run found the root's profile already there — design
    /// decision 21's second artifact, read the way an operator reads it.
    ///
    /// **This is the only half of decision 21 an ACE count cannot see.** The
    /// profile is the artifact a clean run takes back — the ACEs are standing and
    /// stay — so a launcher that left the profile behind passes every assertion
    /// on the DACL: that is exactly the state this phase's own
    /// diagnosis found (eight leaked profiles from runs that exited cleanly,
    /// because a `process::exit` had landed above the `Drop` that deletes
    /// them), and the fix for it is the kind a later `process::exit` can
    /// reintroduce. A fresh run on the same root answering `false` says the
    /// profile was not there to take over — the claim in the form a second run
    /// can falsify, rather than in the launcher's own assurance that it called
    /// the deletion API.
    pub(super) fn took_the_profile_over(&self) -> bool {
        let log = without_ansi(&self.log_text());
        let field = log
            .lines()
            .find(|line| line.contains("CreateAppContainerProfile"))
            .and_then(|line| {
                line.split_whitespace()
                    .find_map(|field| field.strip_prefix("taken_over="))
            })
            .expect("the launcher reported its profile creation");
        match field {
            "false" => false,
            "true" => true,
            other => panic!("`taken_over` is not a boolean: {other:?}"),
        }
    }

    pub(super) fn log_text(&self) -> String {
        fs::read_to_string(&self.log).expect("the launcher's log")
    }

    /// Send this run's launcher a stop and wait for it, or fail naming `what`.
    ///
    /// **One spelling for the sequence three legs run**, because a refusal and a
    /// launcher that outlives its stop are the same two failures whoever is
    /// stopping whom, and each leg had its own rendering of them — the third
    /// copy had also lost the `status` its siblings assert on.
    ///
    /// **A refusal is fatal rather than swallowed, and the host's half of the
    /// question was settled before this leg spawned anything.** `send_stop` has
    /// two causes for an `Err` — no console, or a group that does not exist — and
    /// only the first is a property of the machine; [`host_ready`] probed it. A
    /// launcher that announced readiness from *this* run and then refused a stop
    /// died in between, and an earlier revision let the operator's opt-out
    /// swallow exactly that, turning a dead launcher into a pass — the same
    /// defect class as a readiness frame nobody waited for.
    pub(super) fn stop(&mut self, what: &str) -> ExitStatus {
        if let Err(err) = control::send_stop(self.launcher.id()) {
            fail(
                &mut self.launcher,
                &self.log,
                &format!("the stop could not be delivered to {what}'s group ({err})"),
            );
        }
        wait_for_exit(&mut self.launcher, &self.log, what)
    }
}

/// One `tinio server <root> --sandbox` run **whose subject is how it ends**.
///
/// **What [`Run`] cannot serve, and why this is not a second spelling of it.**
/// `Run::spawn` blocks until the confined server reports an endpoint, which is the
/// wrong shape for a leg whose subject *is* a launch that never reports: that
/// handshake would spend the failure on its own "no `listening` event" message, and
/// the frame the launcher writes on its way out — the thing such a leg is here to
/// read — would be quoted from inside a helper's panic instead of asserted. Here
/// nothing is waited for: **both streams are files beside the root**, stdout
/// included, because the assertion reads the frames written there and a pipe nobody
/// drains is a blocked writer rather than a missing event — and the leg waits for
/// the exit itself ([`wait_for_exit`], which is also what kills a launcher that
/// never ends).
///
/// The argv, the environment and the creation flags are [`launcher_command`]'s, so
/// the two shapes differ in nothing but what they wait for.
pub struct Exit {
    pub(super) launcher: Child,
    /// The launcher's **stdout**: the event stream, and the frames a leg asserts on.
    pub(super) stdout: PathBuf,
    /// The launcher's stderr, which the confined child inherits: the log, and the
    /// child's own `tinio: …` refusal among it.
    pub(super) log: PathBuf,
}

/// The rule [`Run`]'s `Drop` states, for the same reason and with the same
/// consequence: a leg that panics between the spawn and its own wait would otherwise
/// leave the launcher *and* the confined child alive for the rest of the run,
/// holding the job, the ACEs and the image the next build of the binary needs.
impl Drop for Exit {
    fn drop(&mut self) {
        let _ = self.launcher.kill();
        let _ = self.launcher.wait();
    }
}

impl Exit {
    /// Spawn `tinio server <root> --sandbox` on `root` and return at once.
    pub(super) fn spawn(root: &Path, port: u16) -> Self {
        let stdout = root.with_file_name("launcher.stdout");
        let log = root.with_file_name("launcher.log");
        let launcher = launcher_command(root, port)
            .stdout(Stdio::from(
                fs::File::create(&stdout).expect("create the launcher's stdout file"),
            ))
            .stderr(Stdio::from(
                fs::File::create(&log).expect("create the launcher's log"),
            ))
            .spawn()
            .expect("spawn tinio server");
        Self {
            launcher,
            stdout,
            log,
        }
    }

    /// The launcher's stdout as it stands — for a launch that never reported, the
    /// `error` frame the parent wrote on its own behalf and nothing else.
    pub(super) fn stdout_text(&self) -> String {
        fs::read_to_string(&self.stdout).expect("the launcher's stdout")
    }

    pub(super) fn log_text(&self) -> String {
        fs::read_to_string(&self.log).expect("the launcher's log")
    }
}

/// `icacls <path> <args…>`, **checked**: a refusal here is the fixture's failure and
/// not the product's, so it fails the leg with the tool's own text rather than
/// leaving an assertion to discover the missing ACE.
pub fn icacls_with(path: &Path, args: &[&str]) {
    let output = Command::new(acl::icacls_binary())
        .arg(path)
        .args(args)
        .output()
        .unwrap_or_else(|err| panic!("run icacls on {}: {err}", path.display()));
    assert!(
        output.status.success(),
        "icacls {} {args:?}: {}",
        path.display(),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The SID of the account this process runs as — `whoami`'s own answer, in the
/// `S-1-5-…` form [`acl::grant_spec`] takes.
///
/// **A SID and not a name, because a name is resolved.** `icacls` takes a
/// `*`-prefixed SID literally and resolves every other spelling against the local
/// (or domain) account database, so a name another principal happens to share would
/// grant the wrong account — the rule [`acl::grant_spec`]'s own doc states from the
/// crate's side, which is why this leg's fixture asks for a SID.
///
/// `/fo csv /nh` is `whoami`'s machine-parsable form: one record, no header, every
/// field quoted and `,`-separated, the SID last. A refusal is the fixture's, so it
/// fails with the tool's text.
pub fn current_user_sid() -> String {
    let output = Command::new("whoami")
        .args(["/user", "/fo", "csv", "/nh"])
        .output()
        .expect("run whoami");
    assert!(
        output.status.success(),
        "whoami: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    let sid = text
        .trim()
        .rsplit("\",\"")
        .next()
        .map(|field| field.trim_matches('"').to_string())
        .unwrap_or_default();
    assert!(
        sid.starts_with("S-1-5-"),
        "whoami answered no SID this leg can grant: {text:?}"
    );
    sid
}

/// **`dir` and everything under it grant this process's own user alone** — the
/// fixture a leg needs when the subject is a path a *confined child* cannot reach.
///
/// **The seal is the whole point, and it is a removal rather than a denial.** A
/// confined child's access check is a restricted-token one: an object it opens must
/// carry an ACE for a SID of its own (the package SID `S-1-15-2-…` or `ALL
/// APPLICATION PACKAGES`, which is what the launcher's grants name) — and a *deny*
/// ACE for such a SID does not refuse it, because deny ACEs are evaluated against a
/// token's normal groups while the restriction is a second, allow-only pass. So an
/// object is hidden from the child by **carrying no container ACE at all**, which is
/// what `/inheritance:r` plus one ACE for the calling user leaves.
///
/// **The inherited ACEs have to go, and that is what makes this deterministic.** A
/// served tree under a scratch directory that grants `ALL APPLICATION PACKAGES` —
/// which some hosts' scratch trees do — would be reachable from inside the container
/// whatever this leg did to one directory of its own; removing the inheritance is
/// what turns "this host's tree happens not to grant it" into a property of the
/// fixture. Every ACE this leaves is the calling user's, which the launcher (the same
/// account, unconfined) and this process both keep.
pub fn seal_to_current_user(dir: &Path) {
    let spec = acl::grant_spec(
        &current_user_sid(),
        Target::Directory,
        AccessMode::ReadWrite,
    );
    icacls_with(dir, &["/inheritance:r"]);
    icacls_with(dir, &["/grant:r", &spec, "/q"]);
    // The precondition itself, read back: a seal that did not take would let the
    // confined child through and turn the leg's subject into its fixture's accident.
    let listing = icacls(dir);
    assert!(
        !listing.contains("ALL APPLICATION PACKAGES")
            && !listing.contains("S-1-15-2-")
            && !listing.contains("S-1-15-3-"),
        "the seal did not take: an ACE a confined child can satisfy is still on {}:\n{listing}",
        dir.display()
    );
}

/// **The host's preconditions, probed before anything is spawned — and the only
/// place this suite's opt-outs are decided.** Returns `false` when this machine
/// cannot do something a leg needs *and* the operator has deliberately said so;
/// the leg then returns without asserting anything about the product.
///
/// Two capabilities of the **machine** are asked about, both before the first
/// spawn, and the reason names whichever answered no:
///
/// - **can it confine at all** — [`host_can_confine`] performs the launcher's own
///   setup acts against a throwaway directory and leaves them standing with it
///   (every grant this crate makes is standing, design decision 15 — the scratch
///   directory's own removal is what takes its ACE away);
/// - **can it deliver a console control event** — [`console_device`] opens
///   `CONOUT$`, which is the console a `GenerateConsoleCtrlEvent` needs to have
///   anywhere to go. The clean-exit half of the second leg depends on that event
///   (the launcher, still alive, deletes the profile it created), and a host
///   without a console cannot exercise that half at all.
///
/// That is the precondition Ruling 28 makes an opt-out, and the shape is P1's
/// suite's: fail by default, name what is missing *and the remedy*, and let
/// [`ALLOW_SKIP`] pass deliberately. P1 has one opt-out site
/// (`crates/tinio-sandbox-linux/tests/jail.rs:118`) and the Linux e2e one
/// (`crates/tinio-server/tests/sandbox_e2e.rs:170`); this file has two, and both
/// go through this function.
///
/// **The assert-before-print order, the variable's name and the remedy in the
/// message are [`testing::host_cannot`]'s** — this harness and
/// `tinio-sandbox-windows`'s own suites used to spell all three for themselves —
/// so the two opt-out sites here are two calls of one rule and not two rules.
///
/// **It is a probe of the host and not a check on the run, and that distinction
/// is the whole of this function.** Earlier drafts asked these same questions of
/// things the product controls — [`wait_for_listening`]'s `Event::Error` frame,
/// which is also how the confined child's own startup failure arrives, and the
/// `send_stop` call made after the second launcher has already been running — so
/// a tree whose server could not start, or a launcher that died mid-run, could
/// report a pass under the variable. Nothing the product does is reachable from
/// here: this runs before the first spawn, and **every failure afterwards is
/// fatal**, with no opt-out left to reach for.
///
/// [`ALLOW_SKIP`]: testing::ALLOW_SKIP
pub fn host_ready() -> bool {
    let Some(reason) = host_missing() else {
        return true;
    };
    testing::host_cannot("the confined server suite", &reason, " (design §12)")
}

/// Which host capability is missing, if either — the one reason [`host_ready`]
/// reports and the one it asserts on.
pub fn host_missing() -> Option<String> {
    host_can_confine().err().or_else(|| console_device().err())
}

/// Whether this process has a console for a control event to be delivered on.
///
/// **`CONOUT$` is the proxy, and it is the honest one for this question.** A
/// console is a property of the process's environment rather than of the
/// product; `GenerateConsoleCtrlEvent` addresses processes attached to the
/// caller's console — `control::send_stop`'s own `# Errors` names "there is no
/// console to deliver on" as one of its two causes — and opening the console
/// device fails exactly when there is none. A CI runner that spawns this suite
/// without a console cannot exercise the clean-exit half of the second leg, which
/// is why that is a precondition here rather than a failure there.
///
/// It is deliberately **not** a probe of the launcher's group: whether a
/// particular group exists is a fact about the run, and the leg reports it.
pub fn console_device() -> Result<(), String> {
    fs::OpenOptions::new()
        .write(true)
        .open("CONOUT$")
        .map(|_| ())
        .map_err(|err| format!("open the console device (CONOUT$): {err}"))
}

/// Whether this machine can do what a confined run's setup needs — the launcher's
/// own acts, in its own order: resolve the exposure set's paths, create the
/// AppContainer profile (`resolve an exposed path` precedes
/// `CreateAppContainerProfile` in every launcher log this suite writes), and
/// write the ACEs with `icacls`.
///
/// A throwaway directory under `tempfile`, so a host that passes leaves nothing
/// behind: **grants are standing** (this crate no longer rolls an ACE back), so
/// the ACE dies with the directory `tempfile` deletes, and the profile is deleted
/// by `PackageSid`'s own `Drop` when the value goes out of scope at the end of the
/// call.
pub fn host_can_confine() -> Result<(), String> {
    let scratch = tempfile::tempdir().map_err(|err| format!("make a scratch directory: {err}"))?;
    let profile = sid::package_name(scratch.path());
    let sid = sid::PackageSid::derive(&profile)
        .map_err(|err| format!("create an AppContainer profile: {err}"))?;
    #[allow(clippy::arc_with_non_send_sync)]
    let mut grants = acl::Grants::new(Arc::new(sid));
    grants
        .apply(scratch.path(), Target::Directory, AccessMode::ReadWrite)
        .map_err(|err| format!("grant an ACE with icacls: {err}"))?;
    // No rollback: every grant this crate makes is standing, which is the contract
    // `acl::Grants`'s own doc states. The scratch directory goes away with
    // `tempfile`, and the ACE goes with it.
    Ok(())
}

/// Kill and reap the launcher, then fail with `message` and the log it left.
///
/// **Killing first is not tidiness.** A confined server left alive holds its
/// listener, its job and its ACEs for the life of the test process, and the leg
/// that failed is the one whose launcher is least likely to exit on its own.
pub fn fail(launcher: &mut Child, log: &Path, message: &str) -> ! {
    let _ = launcher.kill();
    let _ = launcher.wait();
    panic!(
        "{message}; the launcher's log:\n{}",
        fs::read_to_string(log).unwrap_or_default()
    );
}

/// Wait for the launcher to exit, or kill it and fail.
///
/// **Bounded, and that is the whole reason it is not a bare `wait`**: a stop the
/// child never acted on leaves the launcher waiting on a child that is not going
/// to exit, and an unbounded wait would take the leg with it instead of reporting
/// the relay's failure (Task 6 paid for this rule).
///
/// **The wait is [`testing::wait_for_exit`]'s and not a copy of it**, which is
/// what this file and the sandbox crate's suites share: the deadline, the poll,
/// and the kill-before-panic are one rule, and a second spelling of it is a
/// second place it can be given up on without the kill. This wrapper exists for
/// the one argument the shared form does not take — the [`exit_timeout`] bound
/// that is this suite's rather than the shared form's own 30 s — and the log it
/// once rendered into the panic message is read from the same red output either
/// way, since [`fail`] and this file's other diagnostics all name the leg's own
/// files.
pub fn wait_for_exit(launcher: &mut Child, _log: &Path, what: &str) -> ExitStatus {
    testing::wait_for_exit(launcher, exit_timeout(), what)
}

/// `icacls` on `root`, with no extra arguments.
///
/// The oracle for what is on the operator's directory. **The definition is
/// `tinio-sandbox-windows`'s, re-exported through the facade**: this suite reads
/// the same DACLs the crates' own ACL tests do, and its copy was the third
/// spelling of one command — the binary's resolution, the checked exit status
/// and the text all identical. Two arguments are what this suite's callers all
/// want, and the shared function takes the rest as a slice for the callers that
/// pass `["/T"]`.
pub fn icacls(root: &Path) -> String {
    testing::icacls(root, &[])
}

/// How many ACEs on `root` name `sid`.
///
/// The count and not a presence check: the grant is `/grant:r`, so a takeover that
/// added a second entry instead of replacing the first would leave two — and
/// "the residue is reusable" is exactly the claim that no second entry appears.
pub fn ace_count(listing: &str, sid: &str) -> usize {
    listing.matches(sid).count()
}

/// `<link>` as a directory junction onto `target`.
///
/// **The definition is [`testing::junction`]'s**, and this leg is why it is
/// shared: the same fixture is planted by `tinio-sandbox-windows`'s own
/// `backend` tests, and the self-assertion that makes a broken `mklink` fail the
/// leg rather than read as a refusal — every assertion after this one is an
/// *absence* — is the half that must not be re-derived per suite.
pub fn junction(link: &Path, target: &Path) {
    testing::junction(link, target);
}

/// `text` with the ANSI SGR sequences `tracing`'s text formatter writes removed.
///
/// **The launcher's stderr is a file and still carries them.** `serve` builds its
/// subscriber with `fmt::layer()` and does not turn colour off, so the default
/// is `with_ansi(true)` and a *field* arrives with its name and its `=` separated
/// by escapes — `taken_over` is written as
/// `\x1b[3mtaken_over\x1b[0m\x1b[2m=\x1b[0mfalse` in the log a run leaves behind.
/// Every other assertion in this file is on a value (`step="grant the ACLs"`,
/// `CreateProcessW`, `sid=S-1-15-2-…`), and a value survives the interleaving; a field
/// does not, which is why this exists rather than a substring test on the raw
/// text. `tinio-sandbox-core`'s recorder test solves the same problem the other
/// way — `with_ansi(false)` on a subscriber it builds itself — which a test
/// reading another process's log cannot do.
pub fn without_ansi(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        // Everything that is not the start of an escape is copied through.
        if ch != '\u{1b}' {
            plain.push(ch);
            continue;
        }
        // An SGR sequence is `ESC [`, parameter bytes, then one final byte in
        // `@`–`~` — the shape libtest's and `tracing`'s writers both emit. A
        // stray `ESC` with no `[` after it is dropped rather than copied,
        // because copying it would leave the assertion comparing escape text.
        if chars.next() != Some('[') {
            continue;
        }
        for byte in chars.by_ref() {
            if ('@'..='~').contains(&byte) {
                break;
            }
        }
    }
    plain
}

/// Connect to the confined server, ask for the root, and require an HTTP answer.
///
/// A plain TCP exchange proves the listener is reachable from outside the
/// confinement. Anything more would re-test S3, which the other suites cover.
///
/// **The probe is [`common::answer`]'s and this is the suite's face on it**: the
/// request bytes ([`common::PROBE`]), the read to EOF and the bound were the same
/// exchange the Linux leg had also written out, so they live in one place now.
/// What stays here is what this suite's callers rely on — a panic rather than a
/// `Result` (the neighbouring legs' failures are all panics) and the acceptance
/// assertion, which is the leg's expectation and not the helper's.
///
/// **The bound moved with the definition, and it is the same family.** It was
/// [`exit_timeout`]'s 15 s and it is now [`common::wait_timeout`]'s readiness
/// budget (60 s, `TINIO_TEST_WAIT_TIMEOUT_SECS`): a host slow enough to make one
/// too tight is slow for the other, both are the operator's to move, and a leg
/// that hangs still fails rather than passing.
pub fn answer(address: SocketAddr) {
    let response = probe(address).unwrap_or_else(|message| panic!("{message}"));
    assert!(response.starts_with("HTTP/1.1 "), "{response}");
}
