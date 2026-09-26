//! The macOS launch: which mechanism confines the child, and the descriptor
//! handover both of them share.
//!
//! `sandbox-exec -p` confines a process it **execs**, so this module's launch
//! wraps the child's program with the helper, proves the profile with a throwaway
//! `sandbox-exec`, and hands the descriptors over the `exec`.
//!
//! The second mechanism (design decision 6) confines a process that **asks**:
//! `sandbox-init` spawns the child directly, proves the profile by forking an
//! unconfined child that applies it ([`crate::apply::validator`]), and puts the
//! rendered text on the launch payload for the child to apply to itself before it
//! serves. Nothing on that route invokes `sandbox-exec` at all, which is what makes
//! it the answer on a host where the helper is missing or unwelcome.
//!
//! **Both mechanisms share everything after the spawn** — the payload write, the
//! event relay, the readiness channel and the killer — because a change to how a
//! confined child is supervised must not be able to reach one of them and miss the
//! other. What differs is the argv and the check: [`ChildArgv`].
//!
//! `sandbox-exec` is deprecated by Apple and still shipped. It is not a
//! supported or versioned API, and nothing here treats it as one: when Apple
//! removes it the `sandbox-exec` mechanism degrades to "no sandbox available,
//! fail closed", which is the error the backend returns when the binary is
//! missing (design, Risks), while `sandbox-init` keeps working — its one
//! dependency is `libSystem`.

// **The `std` imports, one statement because `imports_granularity = "Crate"` allows
// one per crate** (`rustfmt.toml`), and three of the names are load-bearing rather
// than cosmetic:
//
// - `io`'s two traits are the ones the payload handover and the pre-flight call
//   through — `stdin.write_all`/`flush` need `Write` and `pipe.read_to_string`
//   needs `Read` — and the anonymous form keeps them from colliding with the
//   `AsyncWrite`/`AsyncBufRead` bounds the core's `Observer` carries.
// - `OwnedFd`, `AsFd`, `AsRawFd` and `FromRawFd` must be *in scope* where the descriptor
//   calls are written: `OwnedFd::from_raw_fd` and `raw_fd` resolution come from
//   them, `From<OwnedFd>` is what turns the descriptor into a socket, and the `AsFd`
//   bound is what `make_inheritable` and its test both take — one borrowed descriptor
//   handed to `rustix`, never a number rebuilt into one.
// - `thread` is aliased rather than imported bare, and the reason is the crate's
//   own naming: `Thread` is not a type here, `Builder` and `sleep` are the two
//   members used, and `threads::` at the call site says which of the two the code
//   is doing without also inviting a reader to look for a local.
use std::{
    env::{args_os, current_exe},
    ffi::OsString,
    fs::canonicalize,
    io::{self, Read as _, Write as _},
    net::TcpListener,
    os::{
        fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd},
        unix::net::UnixListener,
    },
    path::{Path, PathBuf},
    process::{Child as ProcessChild, Command, Stdio},
    // The readiness announcement's two halves: the sender rides into the relay
    // thread's `Observer` and the receiver rides back to the caller on the `Child`.
    sync::mpsc,
    thread as threads,
    time::{Duration, Instant},
};

use rustix::{
    io::{Errno, FdFlags, fcntl_setfd},
    // The killer this launcher hands the core, beside the descriptor clear above: the `process`
    // feature is already on (it is what `relay.rs` relays with), and `Pid`/`Signal`/`SIGKILL` are
    // the pair `kill_process` takes and the signal that ends a serving child whose `state` could
    // not be published.
    process::{Pid, Signal, kill_process},
};
use signal_hook::consts::signal::SIGKILL;
use tokio::{
    fs::File,
    io::{BufReader, stdout as tokio_stdout},
    runtime::Builder as RuntimeBuilder,
};
use tracing::{debug, error, info};

// The tests' own copy of the predicate: the launch itself asks the pair's question through
// `is_launcher_fd_arg`, so this one is only reached from `mod tests` below.
#[cfg(test)]
use crate::_core::is_listener_fd_arg;
use crate::{
    _core::{
        Adopted, Child, Confinement, ControlHandle, Error, LaunchPayload, LaunchPlan, Observer,
        Result, Sandbox, SandboxSpec, control_fd_arg, is_launcher_fd_arg, launch, listener_fd_arg,
        refused, step,
    },
    apply::{pairs, validator},
    mechanism::{Mechanism, Options},
    profile::{Profile, profile, with_canonical_paths},
    relay::on_stop,
};

/// Where macOS keeps `sandbox-exec`.
pub const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

/// What the pre-flight asks `/bin/echo` to print.
pub const PREFLIGHT_MARKER: &str = "tinio-sandbox-preflight";

/// The `(param "…")` reference the `process-exec` rule resolves — the one
/// occurrence that makes the `-D P=<program>` pair load-bearing.
///
/// **A binding rather than a literal at the test and the check**, because the two
/// have to agree for the right reason: if the spelling drifted, the argv would stop
/// defining a parameter the text still named, and `sandbox-exec` would answer with
/// a compile error (`invalid data type of path filter; expected pattern, got
/// boolean`) on every launch rather than on the one that mattered. The rule that
/// renders it is `profile::RUNTIME_BASELINE`'s `process-exec` entry.
fn program_reference() -> String {
    String::from("(param \"P\")")
}

/// How long the pre-flight may run before it is killed.
///
/// **The bound is in-process, because `timeout(1)` does not exist on macOS**:
/// there is no external program to lean on, so the wait polls the child's own
/// exit status against this deadline and kills the child when it expires. What is
/// being bounded is one profile compile and one `echo`, so the number is a bound
/// rather than a timeout to tune; what it buys is the difference between a
/// startup error and a launcher that emits no event, exits never and says nothing
/// on stderr while the operator waits for a server that is not coming.
///
/// **`sandbox-init`'s validator waits on the same bound** (`apply::VALIDATOR_TIMEOUT`),
/// because the two mechanisms' checks are the same step of one startup and an
/// operator should not have to learn two numbers for it.
pub(crate) const PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(10);

/// The `step` every pre-flight failure names — **one spelling, because the log's
/// field and the error's field are the same sentence** (shared §7).
///
/// It is a `const` rather than five hand-written literals for the reason the
/// shared builder exists at all: this function used to spell `"sandbox-exec -p"`
/// at every map, event and `Error::Step` it built, so a rename of one of them
/// would have been a log line that no longer matched the error beside it — and
/// nothing would have failed. The five sites now read this name.
const PREFLIGHT_STEP: &str = "sandbox-exec -p";

// Note on the imports that are *absent*: this module names no `tokio::io` trait,
// and that is deliberate rather than an omission. `Observer::new` and `observe`
// are generic over `AsyncWrite` and `AsyncBufRead`, so the bounds are the
// callee's and the caller imports nothing to satisfy them; an earlier draft
// carried `use tokio::io::{AsyncBufRead, AsyncWrite};`, which was dead. The two
// `tokio` names the relay thread does need, `tokio::fs::File` and
// `tokio::io::BufReader`, are imported at the top like every other name
// (`docs/style.md`: top `use`, never inline).
//
// `ControlHandle` is the core's owned control-channel descriptor, and it is imported here rather
// than reached by path for the reason every other name is: `make_inheritable` clears its
// close-on-exec flag and this module's signature spells it. It needs no trait import of its own
// on this target — `rustix::io::fcntl_setfd` takes an `AsFd`, which the core implements for the
// type — so the entry in the `std` block above (`AsRawFd`) is the only descriptor trait imported.

/// This executable's path, resolved the way `sandbox-exec` resolves it.
///
/// **The `process-exec` literal has to be the canonical path or every launch
/// fails, and that is measured rather than deduced.** `sandbox-exec` resolves
/// the exec target *before* it checks the rule, and Seatbelt compares the
/// rule's literal against the resolved path — so a `P` that is a symlinked or
/// otherwise un-resolved spelling never matches the exec the kernel performs.
/// Measured on macOS 15.8, 2026-09-19: a profile whose `(literal (param "P"))`
/// was the un-resolved `/var/folders/…/probe` that `current_exe()` returns for a
/// binary under `$TMPDIR` exits **71** with
/// `sandbox-exec: execvp() of '/private/var/folders/…/probe' failed: Operation
/// not permitted` — the diagnostic names the *canonical* path the check was
/// made against, while the rule named the spelling that never matched it. The
/// same profile with `P` canonicalised exits 0.
///
/// **The fallback is the point of `unwrap_or`, and it fails loudly rather than
/// quietly.** `current_exe` cannot fail for a running process and
/// `canonicalize` can only fail on a filesystem error, so the fallback is
/// unreachable in practice — and if it were reached, the un-canonicalised value
/// produces the `rc=71` refusal above at the next launch rather than a launch
/// that runs outside the rule's narrowing. Refusing is this crate's whole
/// posture (design §11), and the pre-flight is what turns it into a startup
/// error carrying that diagnostic.
///
/// The pre-flight does **not** need this: `/bin/echo` is already canonical on
/// macOS 15.8 (measured), so its literal matches as written.
fn canonical_program() -> Result<PathBuf> {
    let program = current_exe().map_err(step("current_exe"))?;
    Ok(canonicalize(&program).unwrap_or(program))
}

/// Confine through the mechanism `options` selected.
///
/// The calling process is **not** confined — it becomes the supervisor, which is
/// why this returns [`Confinement::Supervise`] and never
/// [`Confinement::InPlace`] (decision 6). **This function is the body of
/// `impl Backend for Backend`** (`lib.rs` carries the impl; the trait's method is
/// the one-line delegation to this function), which is what lets the CLI reach it
/// through the facade's `platform()` without naming this crate.
///
/// **The two mechanisms are two ways to put the same text on the child, and the
/// branch is one `match` on [`Mechanism`].** `sandbox-exec` wraps the child's
/// `exec` with the helper, so its check and its argv are the helper's;
/// `sandbox-init` spawns the child directly, proves the profile with
/// [`crate::apply::validator`] and carries the text on the launch payload for the
/// child to apply to itself. Everything from the spawn on — the payload write, the
/// relay, the readiness channel, the killer — is [`spawn`], shared, so the two
/// mechanisms cannot disagree about how a confined child is supervised.
///
/// **The profile is rendered once per launch and by the resolved mechanism**, and
/// that is why the branch is here rather than inside the renderer: the render's
/// one conditional entry is `process-exec`, whose necessity is the mechanism's
/// (`Mechanism::wraps_the_exec`) — the helper cannot `exec` without it and the
/// in-process route has already exec'd. A launcher that rendered before resolving
/// would have to guess, and the two guesses differ by a rule.
///
/// # Errors
///
/// [`Error::BadPath`] for an exposure the profile cannot express;
/// [`Error::Unsupported`] when `sandbox-exec` is missing under that mechanism (the
/// accepted degradation of the design's Risks); [`Error::Step`] for a refused
/// spawn or a profile the kernel refuses, carrying the refused mechanism's own
/// diagnostic.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_core::{AccessMode, LaunchPlan, Sandbox};
/// use tinio_sandbox_macos::{Mechanism, Options, confine};
///
/// let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
/// let plan = LaunchPlan::for_launch(String::new());
/// let _child = confine(
///     &sandbox,
///     &plan,
///     Options::default().with_mechanism(Mechanism::SandboxExec),
/// )?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn confine(sandbox: &Sandbox, plan: &LaunchPlan, options: Options) -> Result<Confinement> {
    let mechanism = options.mechanism();
    let program = canonical_program()?;
    // **Both launcher-written elements are filtered, and the second is why the filter is not a
    // single predicate.** The listener element is this launch's own and is written below; a
    // control element is *also* a handoff this launch writes (step 3b), so an element either
    // kind in the caller's argv is one the child must not see twice — the CLI filters the same
    // pair out before clap parses (`sandbox::filter_argv`), and a forwarded copy here would be
    // a second descriptor number for the element the child is meant to read once.
    let argv: Vec<OsString> = args_os()
        .skip(1)
        .filter(|arg| !is_launcher_fd_arg(arg))
        .collect();

    // 1. The profile: the exposure set, canonicalised for the kernel's view of
    //    the paths. Nothing is added for the executable's own directory:
    //    decision 22's grant is Windows's alone, and a macOS confined child
    //    runs with no grant of any kind on its own location (`process-exec` is
    //    the only gate), so macOS deliberately does not spend one. The paths
    //    come back as `-D` parameters rather than as text (decision 24), so the
    //    text is a constant with `(param "E<i>")` references in it and the
    //    values are data this launcher only forwards. The text's other reference
    //    is the one path the launcher supplies instead of forwarding:
    //    `process-exec`'s `(literal (param "P"))` is `program` above — already
    //    the **canonical** path, for the reason `canonical_program` states — and
    //    `command_args` resolves it from the same binding it writes as the
    //    program element. **Under `sandbox-init` neither the rule nor that
    //    parameter is rendered at all** (`Mechanism::wraps_the_exec`): the
    //    exec and the loader have already happened by the time the child applies
    //    the text, so both would be dead.
    let profile = profile(&with_canonical_paths(sandbox), mechanism)?;
    info!(
        step = "profile",
        mechanism = %mechanism,
        rules = profile.rules,
        params = profile.params.len(),
        "the profile the child will run under"
    );

    // 2. The payload, exactly as the plan carries it: the configuration **text**
    //    the parent forwarded, which this launcher neither parses nor rewrites
    //    (decisions 17 and 18), **and the token the launcher minted for the
    //    child's management plane** (design decision 17's private channel,
    //    extended to the one secret the two processes share). It is serialized to
    //    one JSON object and written to the child's **stdin**, and the stream is
    //    closed below; the standard streams are inherited anyway, so the payload
    //    costs no pipe and no descriptor.
    //
    //    **The token is the plan's, and pairing the two is this platform's job.**
    //    `LaunchPlan::for_launch` builds its payload with `token: None` and
    //    `with_token` sets the plan's — so the frame is built from the plan's two
    //    halves rather than by serializing the carried payload as it stands, which
    //    is what the Windows backend does too. Serializing the payload alone left
    //    the child with no token, so its plane minted one of its own while this
    //    launcher published the plan's in `state`: the file landed and named the
    //    right socket, and every client that read it was refused `401` — `tinio
    //    status` answered "it is stale" (measured on macOS 15.8, 2026-09-25).
    if plan.payload().is_none() {
        error!(step = "payload", "the launch plan carries no payload");
        return Err(Error::Unsupported {
            detail: "the launch plan carries no payload",
        });
    }

    // 3. **The mechanism's own check, before the real child exists**, so a profile
    //    the kernel refuses is a startup error and not a half-started server
    //    (design §11). Both checks go to the kernel with the real text and the
    //    real parameters — an undefined `(param …)` is a compile error, so a check
    //    that dropped them would refuse every profile that has an exposure — and
    //    both are a second compile of the same text per startup. No document
    //    measures what that costs, and it is not worth a cache here: the
    //    alternative is not proving the profile at all, and the price of that is
    //    the failure design §11 exists to prevent.
    //
    //    **The `sandbox-exec` pre-flight walks the same dyld path the real child
    //    does and is still not the product** — it never sizes a thread pool,
    //    never touches a storage root and never serves — which is why the
    //    end-to-end legs and the A/B in `profile.rs` are what cover those.
    //
    //    **The `sandbox-init` validator has the same gap for a different reason**:
    //    it applies the same text to a short-lived process, so it proves the
    //    profile compiles and the parameters resolve, and it cannot prove that a
    //    rule the product needs was left out of the table. Neither check is a
    //    substitute for a run.
    //
    //    **The listener is still clean at this point and the control channel is
    //    too, and that is the change this ordering exists for.** Both clears
    //    happen *below*, in step 4's single `make_inheritable`: the plan owns the
    //    listener as a socket and the control channel as a [`ControlHandle`], so
    //    this launcher can reach both descriptors after the check and has no
    //    reason to clear either before it. Neither check therefore inherits
    //    anything of this launch's. That is fd hygiene rather than a privilege
    //    boundary — a listened-on socket carries no filesystem authority, and the
    //    processes it spans are either Apple's own or this binary — but the
    //    exemption is the launcher's own instead of an accident of the plan
    //    carrying a bare `i64`, which is what it used to be.
    //    **One conversion of the exposure parameters, for both of this launch's
    //    readers of them** — the validator below and the payload's confinement half
    //    in step 5 — because a launch is one fact about one exposure set and the two
    //    used to convert `profile.params` separately. The `sandbox-exec` path spends
    //    the conversion without reading it (the helper is handed the text and the
    //    `-D` pairs directly), and the price of that is a few string allocations
    //    against one fewer place where the two halves can drift apart. The `?`
    //    cannot fire here for a profile this module rendered: `apply::pairs` refuses
    //    a value that is not UTF-8, and the generator refuses a non-UTF-8 exposure
    //    long before a launch (`profile::validate_path`).
    let params = pairs(&profile.params)?;
    if mechanism.wraps_the_exec() {
        preflight(&profile)?;
    } else {
        // **This is the branch that keeps `sandbox-init` free of `sandbox-exec`**:
        // the validator forks this process, applies the text in the child and
        // reports through a pipe, so no helper is invoked and no profile reaches
        // an argv.
        validator(&profile.text, &params)?;
    }

    // 4. The descriptors that have to survive the exec, cleared together: the
    //    listener, because a socket cannot ride on a stream, and the control
    //    channel, because only this unconfined process may bind it. The child
    //    rebuilds each from the number in its argv. The plan owns both objects and
    //    the descriptors are derived from them here — argv is the single source of
    //    truth the child reads. Both halves go through the objects themselves:
    //    `as_raw_fd` for the number, and `make_inheritable` for the flag, which
    //    takes them as `AsFd`. Nothing in this process reconstructs a descriptor
    //    from an integer, so the crate's descriptor `unsafe` site stays in the
    //    child (decision 14).
    //
    //    **This step sits after the mechanism's check, and the placement is the
    //    handover's whole audience**: the clear is what makes a descriptor visible
    //    to a process this one execs or spawns, so it is deferred until the only
    //    process left to start is the real child. **Both mechanisms clear the
    //    same pair** — the helper passes them across its `exec`, and the direct
    //    spawn across its own.
    let listener_fd = plan.listener().map(|socket| i64::from(socket.as_raw_fd()));
    make_inheritable(plan.listener(), plan.controls())?;

    // 5. The argv, and the payload's confinement half. **The mechanism decides
    //    both**, and they are decided together because they are one fact: a launch
    //    whose payload carries the text is a launch whose child applies it, and
    //    such a child is the one this process spawns directly.
    let (mut child_argv, confinement) = if mechanism.wraps_the_exec() {
        (command_args(&program, &argv, &profile, listener_fd), None)
    } else {
        // The channel's elements are on the launcher's own argv, which the child
        // reads back through `adopt`: the direct spawn cannot wrap anything this
        // launcher already wrote.
        let confinement = SandboxSpec {
            mechanism: mechanism.as_str().to_owned(),
            profile: profile.text.clone(),
            // The pairs converted once above, moved in rather than converted a
            // second time (step 3's binding).
            params,
        };
        (self_args(&argv, listener_fd), Some(confinement))
    };
    // **The control channel's elements are appended once, outside the branch**: the
    // child rebuilds one handle per element, and which argv they extend is the
    // mechanism's business rather than theirs — writing this twice is what let the
    // two arms' element order be maintained separately.
    child_argv.extend(
        plan.controls()
            .iter()
            .map(|handle| control_fd_arg(handle.value())),
    );

    // **The program that is actually spawned, which the mechanism decides.**
    // `sandbox-exec` under the helper, this binary under the in-process mechanism.
    // Both argv shapes name `program` as an element — the helper's names it last —
    // and that element is a fact about the argv, not about what this process
    // executes: conflating the two is how a helper launch came to exec this binary
    // directly, with a `-p <profile>` it does not take.
    let command = if mechanism.wraps_the_exec() {
        PathBuf::from(SANDBOX_EXEC)
    } else {
        program
    };
    spawn(
        child_argv,
        command,
        mechanism,
        listener_fd,
        plan,
        confinement,
    )
}

/// Start the confined child and hand back the supervision, once its argv and its
/// payload's confinement half are decided.
///
/// **This is the shared tail of both mechanisms**, and it is a function rather
/// than two call sites' worth of duplicated steps for the reason `hand_over`'s doc
/// gives about the payload: a second copy of the wiring is how one mechanism comes
/// to relay readiness and the other stops doing it. What the caller decides is the
/// argv, the program to execute and the confinement; what happens to the child
/// either of them produces is the same answer.
///
/// `command` is the **process this one executes** — Apple's helper under
/// `sandbox-exec`, this binary under `sandbox-init` — and it is deliberately not
/// the same value as the program element `confine` writes into a helper's argv:
/// that element is this binary, and the helper is what runs it.
fn spawn(
    child_argv: Vec<OsString>,
    command: PathBuf,
    mechanism: Mechanism,
    listener_fd: Option<i64>,
    plan: &LaunchPlan,
    confinement: Option<SandboxSpec>,
) -> Result<Confinement> {
    // stdout is the child's protocol channel and stderr is its log channel
    // (decision 19): pipe the first so the parent can decode and re-emit, and let
    // the second through untouched — `sandbox-exec`'s own diagnostics go there too
    // on that mechanism, and the parent never mediates it. The stdin pipe car-
    // ries the payload (step 6) and is closed once it has been written.
    let mut child = Command::new(&command)
        .args(child_argv)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|source| spawn_error(source, mechanism, &command))?;
    info!(
        step = "spawn",
        pid = child.id(),
        listener_fd,
        mechanism = %mechanism,
        "the confined child is spawned"
    );

    // The signal relay (design §3 step 9, decision 20) — a different relay from
    // the event one below: that one moves the child's *bytes* out, this one moves
    // the launcher's *signals* in. **Armed after the spawn**, and it has to be:
    // before the child exists there is nothing to relay to, so dying on a signal
    // that arrives then is the correct behaviour rather than a gap in the relay.
    //
    // **The number is derived once, here, because two things below need it**: the
    // signal relay this step arms, and the killer the `Child` is built with at the
    // end of this function, for a launch that fails *after* this launcher has
    // something to end. `std::process::Child::id` is a `u32` and `kill(2)` takes an
    // `i32`, so the conversion is checked rather than `as`-cast: a pid that does
    // not fit is a kernel this launcher cannot relay for, and a refusal at startup
    // beats a wrap that would signal an unrelated process later.
    let pid = i32::try_from(child.id()).map_err(|_| Error::Unsupported {
        detail: "the child's process id does not fit in an i32",
    })?;
    on_stop(pid).map_err(step("install stop-signal handling"))?;

    // 6. Write the frame, then close stdin so the child's read returns. **This
    //    write can block**, and the frame's size is not what bounds it: it is the
    //    whole configuration text, which nothing bounds, so the write returns as
    //    the child drains it — the product child's `main` drains it first thing
    //    (`adopt`), and that is the only reason this is bounded at all, because the
    //    relay thread below is not running yet: while this blocks, the child's
    //    stdout is unattended too. `hand_over`'s doc carries the whole statement.
    hand_over(
        &mut child,
        plan.config(),
        plan.token(),
        confinement.as_ref(),
    )?;
    drop(child.stdin.take());

    // 7. The event relay, on a thread of its own. The parent waits for the child
    //    on the main thread, so draining its stdout has to happen here: one thread
    //    doing both would block on a full pipe while the child blocked writing into
    //    it.
    //
    //    **The thread's handle is kept rather than dropped, and its return value is
    //    the answer's channel**: the handle goes to the core through
    //    `Child::with_relay`, which joins it inside `Child::wait`, and the join's
    //    result becomes the child's terminal record. There is no slot for the
    //    parent to fill and no lock to share — the relay returns what it saw, and
    //    the caller reads it from the `Child` once the child is gone (decision 19's
    //    "the parent owns the printed-exactly-once guarantee"). The child being
    //    reaped and the relay noticing end-of-stream are two events with no
    //    ordering of their own, so a terminal read before the join would be a
    //    snapshot — and on a healthy child it would usually read nothing at all.
    //
    //    **The readiness announcement is the second half of the same relay, and it
    //    is what makes a confined macOS server discoverable at all.** The launcher
    //    publishes `<root>/.tinio/state` the moment the child reports its listener
    //    is up — the token it put on the frame and the control channel's name are
    //    published nowhere else — and it reads that moment from
    //    `Child::wait_for_ready`, which answers `None` at once unless a receiver
    //    was attached. So the channel below is not an optimisation: without it the
    //    child reports `listening` and the launcher publishes nothing, `tinio
    //    status` says "no server has run for this root", and the control socket
    //    answers 401 to every client because the token never reached a reader
    //    (measured on macOS 15.8, 2026-09-25). The Windows launcher wires the same
    //    pair (`Observer::with_notify` + `Child::with_ready`), which is the shape
    //    this mirrors.
    let (ready_tx, ready_rx) = mpsc::channel();
    let mut relay = None;
    if let Some(stdout) = child.stdout.take() {
        let handle = threads::Builder::new()
            .name("tinio-sandbox-events".into())
            .spawn(move || {
                // Decode and re-encode rather than forward bytes (decision 19):
                // anything the child or a dependency prints that is not one of our
                // events must not reach the operator, who could not tell it apart
                // from a real one.
                //
                // Two conversions stand between the child's stdout and the core's
                // relay, and both are load-bearing. The child came from
                // `std::process`, so its stdout is a **blocking** pipe —
                // `ChildStdout` implements `std::io::Read`, not any async reader.
                // `tokio::fs::File` is what turns a descriptor into an `AsyncRead`
                // correctly: it puts the descriptor in non-blocking mode and parks
                // the thread on an epoll/kqueue readiness, which is precisely the
                // work a hand-rolled wrapper would get wrong.
                //
                // Both hops are safe, and neither needs `unsafe`. `std` gives
                // `impl From<ChildStdout> for OwnedFd`, and `tokio::fs::File`
                // accepts an `OwnedFd`; ownership moves into the `File` by value
                // with no second owner to reason about. The route through
                // `as_raw_fd` plus `mem::forget` would move ownership just as well,
                // but it needs an `unsafe` block where this needs none — and this
                // crate's enumerated `#[allow(unsafe_code)]` sites are the
                // inherited-descriptor rebuild in `inherited_fd`, the libSystem FFI
                // in `apply` and the validator's `fork` (decision 14), so an
                // `unsafe` here would be one site more, with no invariant to
                // justify it.
                let file = File::from(OwnedFd::from(stdout));
                let reader = BufReader::new(file);
                let Ok(runtime) = RuntimeBuilder::new_current_thread().build() else {
                    return None;
                };
                // `Observer` rather than `reemit`, and that is the whole point:
                // `reemit` discards what the child reported, and the caller needs
                // it to decide whether this child ever became ready. The sender
                // goes in with it so the *same* record is announced the moment it
                // is seen rather than only at end-of-stream — the record the join
                // below returns is final, and by then the child is gone.
                runtime
                    .block_on(
                        Observer::new(tokio_stdout())
                            .with_notify(ready_tx)
                            .observe(reader),
                    )
                    .ok()
                    .and_then(|observer| observer.terminal().cloned())
            })
            .map_err(step("spawn event relay"))?;
        relay = Some(handle);
    }

    // The relay handle goes in with the child, and `Child::wait` joins it: the
    // terminal read afterwards is a result only because of that join. The receiver
    // goes in beside it, which is what turns a child that reported `listening` into
    // an answer `wait_for_ready` can read — and a relay that never started (the
    // branch below) drops the sender with it, so that caller gets `None` rather
    // than a wait it cannot end.
    //
    // **The killer goes in beside them, because from here on a failure can leave a
    // serving process behind.** The caller's next fallible step is the `state`
    // publish, and it happens *after* the child reported it is listening; with no
    // job object on this platform, returning an error and dropping the `Child`
    // would end nothing — the confined server would keep the port and have no
    // `state` file, so `tinio status` and `tinio stop` could not find it. The core
    // owns that path's shape ([`Child::kill`], called before its `wait`), and this
    // is the platform's one call it hands over: a `SIGKILL` to the pid derived
    // above. **It takes the pid and not the process**, which is what keeps the wait
    // closure — the owner of the `std::process::Child`, and so the only thing that
    // can reap — untouched beside it; a killer that reaped would leave the wait
    // with nothing to return.
    //
    // **Both mechanisms install it**, which is why it lives here rather than under
    // one arm: the direct spawn's child is a plain `std::process` child of this
    // process and is exactly as much this process's to end as the helper's is.
    let killer = move || {
        // The same two conversions the relay's thread makes, for the same reason:
        // the pid came from `std` as a `u32` and the signal is a constant, and
        // neither becomes a platform value without a checked conversion.
        let (Some(pid), Some(signal)) = (Pid::from_raw(pid), Signal::from_named_raw(SIGKILL))
        else {
            return Err(io::Error::other(
                "the child's pid or SIGKILL did not fit the platform's process types",
            ));
        };
        // **`rustix` answers an `Errno`, and the annotated closure is the conversion this crate
        // writes everywhere else** (`make_inheritable`): naming the source once pins the type, and
        // the `io::Result` the core's killer takes is that conversion's result.
        kill_process(pid, signal).map_err(|err: Errno| err.into())
    };
    let supervised = Child::new(move || Ok(child.wait()?)).with_kill(killer);
    Ok(Confinement::Supervise(match relay {
        Some(relay) => supervised.with_relay(relay).with_ready(ready_rx),
        // Not reachable: stdout was piped, so it is either taken above or the
        // child never started. Kept as a real branch rather than an `expect`
        // because this crate forbids panics in library code — and a child with no
        // relay is still a child to wait for.
        None => supervised,
    }))
}

/// The argv `sandbox-init` spawns the child with: **the launcher's own argv, and
/// nothing else.**
///
/// The direct spawn has no helper to be told the profile, so this function writes
/// no `-p` and no `-D` block — the text and its parameters travel on the launch
/// payload, which the child reads from its stdin before it applies them
/// ([`SandboxSpec`]). What is left is the launcher's arguments, filtered by
/// `confine`, plus the one element a launcher alone may write: the inherited
/// descriptor's number.
///
/// **The child's channel elements are written by the caller**, like the other
/// mechanism's: a control element is a handoff the launcher makes, and this
/// function renders no part of the plan.
///
/// **Pure, so the shape is testable without a launch**, exactly as
/// [`command_args`] is.
///
/// # Examples
///
/// ```
/// use std::ffi::OsString;
///
/// use tinio_sandbox_macos::self_args;
///
/// let argv = [OsString::from("serve"), OsString::from("/srv/data")];
/// let args = self_args(&argv, Some(7));
/// // The launcher's own arguments, unchanged, and the descriptor last.
/// assert_eq!(args[..2], argv);
/// assert_eq!(args[2], OsString::from("tinio-sandbox-fd=7"));
/// ```
pub fn self_args(argv: &[OsString], listener_fd: Option<i64>) -> Vec<OsString> {
    // No `P` element: the in-process render carries no `process-exec` rule
    // (`Mechanism::wraps_the_exec`), so there is no parameter for it to
    // resolve and a pair here would name one the text never mentions.
    let mut args = Vec::with_capacity(argv.len() + 1);
    args.extend(argv.iter().cloned());
    if let Some(fd) = listener_fd {
        args.push(listener_fd_arg(fd));
    }
    args
}

/// The argv `sandbox-exec` is invoked with.
///
/// Pure, so the "inline, one element, no file" property of decision 24 is
/// testable without a launch. Shape:
/// `-p <profile text> -D NAME=VALUE … <program> <argv…> <listener-fd>`, with one
/// `-D` flag and one `NAME=VALUE` assignment per [`Profile::params`] entry, in
/// that order — and those entries are the exposures, because the home grant's `H`
/// pair is gone with the grant (design §5's `home` paragraph) — then the program's
/// `P`, all of them between the
/// profile flag and the program path. Measured on macOS 15.8 (2026-09-19): `-D` is
/// accepted on either side of the profile flag, and with `-f <file>` as well as
/// `-p <text>` — the profile flag and the parameters belong to the same
/// invocation, and the assignments are where the text's `(param "E<i>")`
/// references are resolved from.
///
/// **The control channel's elements are not written here.** They are the plan's
/// (`LaunchPlan::controls`), and `confine` appends one `control_fd_arg` per entry
/// after the listener's — the element the child's `Adopted::controls` reads back.
/// This function stays a pure rendering of the profile, the program and the
/// launcher's own arguments.
///
/// **This function writes `P` and nothing else beyond the exposures' pairs.** `P`
/// is written here rather than carried in [`Profile::params`], because it is not
/// an exposure: the value is one this function already holds, and taking it from
/// the same argument as the program element is what makes the rule's filter and
/// the exec `sandbox-exec` performs a single value rather than two spellings that
/// could drift. There is no second launcher-owned parameter: the home grant's `H`
/// is gone with the grant (design §5's `home` paragraph), so [`Profile::params`] is
/// exactly the exposures and the `-D` block below has no conditional in it.
///
/// # Examples
///
/// ```
/// use std::{ffi::OsString, path::Path};
///
/// use tinio_sandbox_core::{AccessMode, Sandbox};
/// use tinio_sandbox_macos::{Mechanism, command_args, profile::profile};
///
/// let rendered = profile(
///     &Sandbox::new().expose("/srv/data", AccessMode::ReadOnly),
///     Mechanism::SandboxExec,
/// )
/// .unwrap();
/// let args = command_args(Path::new("/srv/bin/serve"), &[], &rendered, Some(7));
/// assert_eq!(args[0], OsString::from("-p"));
/// assert_eq!(args[1], OsString::from(rendered.text.clone()));
/// // Every parameter the profile declares reaches argv, in its order — the
/// // exposures' `E<i>` and nothing else, so the assertion is derived from the
/// // profile rather than spelled out.
/// let mut expected = Vec::new();
/// for (name, value) in &rendered.params {
///     let mut assignment = OsString::from(name.as_str());
///     assignment.push("=");
///     assignment.push(value);
///     expected.extend([OsString::from("-D"), assignment]);
/// }
/// assert_eq!(&args[2..2 + expected.len()], &expected[..]);
/// // Then the `process-exec` literal's parameter — the same path the program
/// // element carries — the program itself, and the listener descriptor last.
/// let program_at = 2 + expected.len();
/// assert_eq!(args[program_at], OsString::from("-D"));
/// assert_eq!(args[program_at + 1], OsString::from("P=/srv/bin/serve"));
/// assert_eq!(args[program_at + 2], OsString::from("/srv/bin/serve"));
/// assert_eq!(args[program_at + 3], OsString::from("tinio-sandbox-fd=7"));
/// ```
pub fn command_args(
    program: &Path,
    argv: &[OsString],
    profile: &Profile,
    listener_fd: Option<i64>,
) -> Vec<OsString> {
    // `-p` and the text, one pair per declared parameter, the program's own pair,
    // the program element, the launcher's arguments and the descriptor element.
    let mut args = Vec::with_capacity(argv.len() + 7 + 2 * profile.params.len());
    args.push(OsString::from("-p"));
    args.push(OsString::from(&profile.text));
    // A parameter value is data, and it is why this function does no escaping:
    // the text refers to the value by name, so a quote, a newline or a whole
    // SBPL rule inside a path is passed through as the path it is (measured).
    for (name, value) in &profile.params {
        let mut assignment = OsString::from(name.as_str());
        assignment.push("=");
        assignment.push(value);
        args.push(OsString::from("-D"));
        args.push(assignment);
    }
    // The pairs are the exposures and nothing else: the one other launcher-owned
    // parameter is `P` below, and the home grant that used to append an `H` pair
    // here is gone with the rule (design §5's `home` paragraph), so this loop has no
    // conditional in it and cannot emit a pair the text does not reference.
    // The parameter the `process-exec` rule's literal resolves to. It is `program`
    // itself, so the one exec the rule permits is the one `sandbox-exec` performs
    // — a narrowing that cannot drift from the path it narrows to.
    //
    // **Written only when the text actually references it**, so this function is a
    // rendering of the profile it was handed rather than of a profile this crate
    // expects: a `sandbox-init` render carries no `process-exec` rule
    // (`Mechanism::wraps_the_exec`) and therefore no `P`, and an argv that
    // defined a parameter no rule names would be a pair the launch invented. The
    // check is over `profile.text`, which is what a rule is, rather than over a
    // flag the caller could pass wrongly.
    if profile.text.contains(&program_reference()) {
        let mut program_param = OsString::from("P=");
        program_param.push(program.as_os_str());
        args.push(OsString::from("-D"));
        args.push(program_param);
    }
    args.push(program.as_os_str().to_os_string());
    args.extend(argv.iter().cloned());
    if let Some(fd) = listener_fd {
        args.push(listener_fd_arg(fd));
    }
    args
}

/// An inherited descriptor, as the object that owns it — **one of the crate's
/// enumerated `unsafe` sites** (design decision 14, its Scope and the Bookkeeping
/// bullet).
///
/// Both of this module's rebuilds go through here — [`listener`] for the data
/// plane's socket and [`control`] for the control channel's — because the step
/// that is `unsafe` is the same step in both and the reason is the same reason.
/// One site with two callers is the shape decision 14 asks for: the exception is
/// `#[allow(unsafe_code)]` on the enumerated site alone, and the enumeration
/// counts a site, not a call.
///
/// **The obvious-looking safe route is not one.** `BorrowedFd::try_clone_to_owned`
/// is safe, and `From<OwnedFd> for TcpListener` is safe — but each is safe only
/// *after* a `BorrowedFd` or an `OwnedFd` exists, and obtaining either from the
/// integer that crossed argv is the unsafe step: `BorrowedFd::borrow_raw` and
/// `OwnedFd::from_raw_fd` are both `unsafe fn`, and `rustc` answers `E0133` for
/// each (measured with rustc 1.98.1 on macOS 15.8, 2026-09-19). `rustix` is no
/// escape: its `net::socket` module is free functions over `OwnedFd`
/// (`pub fn socket(...) -> io::Result<OwnedFd>`,
/// `pub fn accept<Fd: AsFd>(sockfd: Fd) -> io::Result<OwnedFd>`) with no
/// raw-descriptor constructor, so a `rustix` route would still begin at
/// `OwnedFd::from_raw_fd`. Duplicating rather than adopting does not help
/// either: the number is all that crossed the exec, so the duplicate would have
/// to be made *from* a raw descriptor, which is the same unsafe call.
///
/// # Safety
///
/// The descriptor is inherited from the launcher, which holds it open for the
/// child's whole lifetime and never closes it. This process is therefore the
/// descriptor's only owner: `OwnedFd::from_raw_fd` takes the only ownership
/// there is, and the `OwnedFd` — and so the socket built from it — closes it
/// exactly once, when that socket is dropped at the end of the run. The
/// launcher's own copy stays open throughout, which is what makes the number
/// valid at the moment it is adopted.
///
/// **`from_raw_fd` may be called at most once per descriptor number**, which is
/// the part of this contract a caller can break without meaning to: the second
/// call claims a second `OwnedFd` over one descriptor, and the two drops close
/// it twice — a double close, or worse, a close of whatever the number was
/// reused for. Each *caller* is therefore **call-once per process**, and the
/// test harness — whose accessor is the one caller that could plausibly be
/// reached twice — takes the listener out of its `Adopted` rather than handing
/// out a fresh rebuild on every call (Task 4's `harness::Child::listener`).
///
/// # Errors
///
/// An I/O error when the number does not fit in a `RawFd` (the core carries it
/// as an `i64`, the kernel wants an `i32`).
#[allow(unsafe_code)]
fn inherited_fd(raw: i64) -> io::Result<OwnedFd> {
    let fd = RawFd::try_from(raw)
        .map_err(|_| io::Error::other("an inherited descriptor is out of range"))?;
    // SAFETY: the invariant in the `# Safety` section above — the descriptor is
    // the launcher's, inherited across the exec, held open there for this
    // process's whole lifetime, and owned by nothing else in this process; and
    // each caller runs once.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Rebuild the listener from the descriptor `adopt` handed back.
///
/// A free function rather than an inherent `Adopted::listener()`, because
/// `Adopted` is the core's type and an inherent impl may only live in its
/// defining crate. The rebuild is the platform's job — it is `fcntl`/`dup` on
/// Unix and handle plumbing on Windows — so core stops at the descriptor and
/// this crate does the rest. What is left here is the *shape*: the inherited
/// number becomes an [`OwnedFd`] ([`inherited_fd`], the crate's descriptor
/// `unsafe` site) and `std`'s own `From` impl turns that into the socket.
///
/// # Errors
///
/// An I/O error when `adopt` carried no descriptor, or when the number does not
/// fit in a `RawFd`.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_macos::launch::listener;
///
/// # let adopted = tinio_sandbox_core::adopt(std::env::args_os(), std::io::stdin()).unwrap().unwrap();
/// let socket = listener(&adopted)?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn listener(adopted: &Adopted) -> io::Result<TcpListener> {
    // `std::io::Error`'s own constructor is written out inside the body rather
    // than shortened to `io::Error`, and the doctest above is why the two
    // spellings differ: a doctest compiles as its own crate against this one's
    // *public* surface, so it cannot see a `use` this module holds, while this
    // body cannot spell a path the lint refuses.
    let raw = adopted
        .listener_fd
        .ok_or_else(|| io::Error::other("no listener descriptor was handed over"))?;
    let owned = inherited_fd(raw)?;
    debug!(
        step = "listener",
        raw, "the child rebuilt its listener from the inherited descriptor"
    );
    Ok(TcpListener::from(owned))
}

/// Rebuild the **control channel's** socket from the descriptor `adopt` handed
/// back — the child's half of the handoff the launcher made when it bound the
/// channel itself.
///
/// **Why the child does not bind this socket.** The profile grants no
/// `network-bind` at all: a unix `bind()` is gated by that rule, and the rule is
/// a grant rather than a filter — it would permit binding anything, which is the
/// opposite of what a `(deny default)` profile is for. So the launcher — which
/// runs unconfined — binds the socket, clears its `FD_CLOEXEC`, and writes the
/// number into the child's argv; this is the rebuild on the other side, and it
/// is the platform's for the same reason [`listener`] is (decision 16 extended
/// to the control channel).
///
/// **Exactly one descriptor.** A unix control channel is one socket file with one
/// listener behind it, so a handoff of any other length is refused rather than
/// served: the launcher and this process would disagree about the channel's
/// shape, and serving one socket out of a launcher's pool would leave the rest
/// of it unserved and unexplained.
///
/// # Errors
///
/// An I/O error when `adopt` carried no control descriptor, when it carried more
/// than one, or when the number does not fit in a `RawFd`.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_macos::launch::control;
///
/// # let adopted = tinio_sandbox_core::Adopted::new(String::new(), Some(7), vec![9], None, None);
/// // The socket the launcher bound for the confined child to serve on.
/// let socket = control(&adopted)?;
/// # let _ = socket;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn control(adopted: &Adopted) -> io::Result<UnixListener> {
    let [raw] = adopted.controls() else {
        return Err(io::Error::other(format!(
            "the launch handed over {} control descriptors; a unix control channel is one socket",
            adopted.controls().len()
        )));
    };
    let owned = inherited_fd(*raw)?;
    debug!(
        step = "control",
        raw = *raw,
        "the child rebuilt its control socket from the inherited descriptor"
    );
    Ok(UnixListener::from(owned))
}

/// Write the payload frame to the child's standard input.
///
/// The caller closes the stdin once this returns, which is what makes the
/// child's `adopt` see the end of the stream.
///
/// **The frame is the plan's two halves, paired here**: `config` is the launcher's
/// **whole configuration text** (decision 17), forwarded unparsed, and `token` is
/// the value this launcher minted and publishes in `state` — the credential the
/// child's plane verifies `/status` and `/stop` against. `LaunchPayload` is the
/// wire shape on both launcher platforms, and building it here rather than
/// serializing the payload the plan carries is what puts the token on it at all:
/// `for_launch` sets `token: None`, `with_token` sets the plan's, and only the
/// platform pairs them (`crates/tinio-sandbox-windows/src/backend/mod.rs` is the
/// other one). A frame without it is a child that mints its own token, which is
/// the run measured on macOS 15.8, 2026-09-25: `state` named the right socket and
/// every client that read it was refused `401`.
///
/// **This is a blocking write, and what bounds it is the child rather than the
/// size of the frame.** Nothing in the design or in this
/// plan bounds how large that is: a pipe's buffer is a few KiB, so a frame past it
/// returns only as the child drains it, and the figure is where the write stops
/// being a copy into the buffer rather than a bound on what may be sent. The child
/// does read: `adopt` is the first thing its `main` does, before the runtime or
/// the storage exists, and the write happens *after* the spawn — so the two are a
/// producer and a consumer that meet, and the write completes as long as the child
/// runs and reads. **What can stall is a child that neither reads nor exits**, and
/// it stalls here, before the launcher has reported anything. No caller imposes a
/// size limit today, and that is recorded rather than assumed away: a caller that
/// wants one has to add it here or where the configuration text is built.
///
/// **The order matters on this platform, and what keeps it safe is the child's
/// side of the arrangement.** The frame is written before the relay thread is
/// spawned (step 6 of `confine` above, then step 7), so while this write blocks
/// the child's stdout is unattended: a child that filled its stdout buffer would
/// leave the launcher blocked on stdin and itself blocked on stdout, with neither
/// side moving. What rules that out is that the child does not write enough on
/// stdout to fill the pipe before its stdin is drained — the product child
/// because `adopt` is the first statement of its `main` (design §10 step 1), and
/// the harness's child, a test binary, because what it opens with is libtest's own
/// preamble, a few hundred bytes against the pipe's buffer. Nothing in the
/// launcher enforces either: it is an invariant the child owns, and a child that
/// broke it would deadlock the launcher here rather than fail.
fn hand_over(
    child: &mut ProcessChild,
    config: &str,
    token: Option<&str>,
    confinement: Option<&SandboxSpec>,
) -> Result<()> {
    let stdin = child
        .stdin
        .as_mut()
        .ok_or_else(|| step("payload stdin")(io::Error::other("the child has no piped stdin")))?;
    // **Serialized from borrows, and the three values are already borrows here.** The frame used
    // to be built as an owned `LaunchPayload` — `config.to_owned()` and `confinement.cloned()` —
    // which deep-copied the whole configuration text and the whole rendered profile (its `params`
    // with it) and then copied them again through `serde_json::to_vec`, on a path that **blocks
    // until the child drains stdin**. The borrowed encoding is the same frame — the same field
    // names, in the same order, with the same two `skip_serializing_if`s, pinned by the payload
    // tests — written straight out of the caller's values, so the only copy left is the frame.
    let frame = LaunchPayload::encode_frame(
        config,
        token,
        // **The confinement half, when the mechanism asks the child to apply one**
        // (see [`SandboxSpec`]): the rendered text and its parameters travel the
        // same stream the configuration does, so the mechanism that spawns the
        // child directly needs no argv of its own for them and no second channel.
        // `None` is the helper's launch, whose confinement was applied by
        // `sandbox-exec` at the `exec` — the field is absent on that wire.
        confinement,
    )
    .map_err(step("serialize payload"))?;
    stdin.write_all(&frame).map_err(step("write payload"))?;
    stdin.flush().map_err(step("flush payload"))?;
    debug!(
        step = "payload",
        bytes = frame.len(),
        token = token.is_some(),
        confinement = confinement.is_some(),
        "the payload is written to the child's stdin"
    );
    Ok(())
}

// Nothing here adds the executable's own directory to the exposure set, and
// that is deliberate: decision 22's grant on the program's own location is
// Windows's alone. A macOS confined child runs with no grant of any kind on its
// own location — `process-exec` is the only gate, `file-read-data` on its own
// image and directory is refused and logged, and the program proceeds
// regardless — so macOS deliberately does not spend one, and this is the note
// that stops a future reader adding one back.

/// Prove `sandbox-exec` accepts the profile before the real child is spawned.
///
/// Design §11 requires a refused profile to surface as "a startup error
/// carrying the binary's own diagnostic". The child's stderr is inherited (it
/// is the server's log), and capturing it would need a pump that deadlocks when
/// the pipe fills — so the profile is proved on a throwaway process instead.
/// The failure this catches is `sandbox-exec`'s own (a syntax error, a refusal),
/// not the confined program's.
///
/// **The probe walks the same dyld path the real child does, and what it proves
/// is correspondingly wider than "the profile compiles" — but it is still not the
/// product.** `/bin/echo` exercises the loader's shared-cache resolution, which is
/// why the `root-traversal` rule's own `deny(1) file-read-data /` kills this probe
/// before it can print anything (measured on the product path 2026-09-25: with
/// that rule removed there is no real child at all, only `sandbox-exec refused the
/// profile … exit=None diagnostic=` and a kernel line naming `echo`). What the
/// probe does **not** do is the work the product's own allowances exist for: it
/// never sizes a thread pool, never touches a storage root, and never binds a
/// control channel, so `sysctl`, `metadata` and an exposure's read rule are each
/// unexercised here. **A profile missing any of those passes this pre-flight and
/// then kills the real child** — the `sysctl` abort is the measured case
/// (`failed to allocate a guard page`, SIGABRT) — and the end-to-end leg (Task 9)
/// plus the product-path A/B in `profile.rs` are the checks for that, not this
/// one.
///
/// The argv comes from [`command_args`], so what is proved is the profile the
/// launch will hand over, parameters included — with the single exception the
/// next paragraph names. That is not a convenience: the text references
/// `(param "E0")` and an undefined parameter is a compile error, so a pre-flight
/// without the `-D` elements would refuse every profile that has an exposure —
/// and a pre-flight that escaped or rewrote a value would prove a profile the
/// launch never passes.
///
/// **One parameter differs from the launch's, and it has to.** `P` is the
/// parameter the `process-exec` rule's literal resolves to, and `command_args`
/// takes its value from the program it is handed — here `/bin/echo`, because that
/// is the exec this probe performs. Binding it to the launcher's own path instead
/// would refuse the probe's own exec, which is the `rc=71` that rule's `evidence`
/// names: correct behaviour, and a useless proof. Every other element is the
/// launch's, so a profile `sandbox-exec` cannot compile is still refused here.
///
/// **The marker below is not an event and is not framed like one.** Nothing but
/// a tinio process may write on the protocol channel (decision 19), and
/// `sandbox-exec` is not one — so this pre-flight does not use it. Its stdout is
/// captured and compared in-process: it is a private convention between this
/// function and `/bin/echo`, and it never reaches the launcher's stdout, which
/// continues to carry `Event` frames and nothing else.
///
/// **The wait is bounded, and that is the difference between a failure and a
/// hang.** This is the last thing `confine` does before the real spawn, so a
/// pre-flight that never returns is a launcher that emits no event, exits never
/// and writes nothing on stderr — which an operator cannot tell from "the server
/// never started". `Command::output()` is exactly that unbounded wait: both pipes
/// and the exit, with no deadline of its own. `timeout(1)` is not the fallback it
/// looks like either, because macOS does not ship one, so the bound is here, in
/// the wait ([`PREFLIGHT_TIMEOUT`]): it polls the child's own exit status and
/// kills the child when the deadline expires. **What the operator sees then** is
/// an `error!` naming the step and the deadline and a startup error carrying the
/// same sentence — `no exit within 10s; the pre-flight was killed` — instead of a
/// silence nothing distinguishes from a server that never came up. The pipes are
/// read *after* the exit, which cannot deadlock here: the only writer is
/// `/bin/echo` and its output is one short line.
fn preflight(profile: &Profile) -> Result<()> {
    // **Skipped unless the profile's compilation is what is being watched.** The probe costs a
    // second `sandbox-exec` + `/bin/echo` pair *and* a second compile of the profile text on every
    // launch, and what it buys is the *typed* refusal: the real spawn fails closed without it,
    // because `sandbox-exec` exits `71` with its diagnostic on the inherited stderr, which is the
    // error the launch carries either way. So it runs in a debug build or under a `debug` log
    // level, and the event still reports which of the two happened — the confined leg asserts the
    // step is present, and a silent skip would make that assertion lie by omission.
    if !(cfg!(debug_assertions) || tracing::enabled!(tracing::Level::DEBUG)) {
        // The step's own name, not the command's: the acceptance below logs `step = "preflight"`,
        // and a skip that logged `sandbox-exec -p` would leave the event stream with two names for
        // one step — and would make the confined leg's `step="preflight"` assertion pass only in a
        // debug build. `verdict` is what tells the two records apart.
        info!(
            step = "preflight",
            verdict = "skipped",
            "the pre-flight runs only in a debug build or under a debug log level"
        );
        return Ok(());
    }
    let probe = command_args(
        Path::new("/bin/echo"),
        &[OsString::from(PREFLIGHT_MARKER)],
        profile,
        None,
    );

    // Spawned rather than `output()`-captured, because `output()` waits for both
    // pipes and the exit without a bound. `Stdio::null()` on stdin is what
    // `output()` did implicitly; neither `/bin/echo` nor `sandbox-exec` reads it.
    let mut child = Command::new(SANDBOX_EXEC)
        .args(probe)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|source| spawn_error(source, Mechanism::SandboxExec, Path::new(SANDBOX_EXEC)))?;

    let deadline = Instant::now() + PREFLIGHT_TIMEOUT;
    let status = loop {
        match child.try_wait().map_err(step(PREFLIGHT_STEP))? {
            Some(status) => break status,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                error!(
                    step = PREFLIGHT_STEP,
                    timeout = ?PREFLIGHT_TIMEOUT,
                    "the pre-flight outlived its deadline and was killed"
                );
                return Err(step(PREFLIGHT_STEP)(io::Error::other(format!(
                    "no exit within {PREFLIGHT_TIMEOUT:?}; the pre-flight was killed"
                ))));
            }
            // A poll rather than a blocking wait, because the wait is the thing
            // being bounded; the interval is smaller than anything this probe
            // spends doing its work.
            None => threads::sleep(Duration::from_millis(10)),
        }
    };

    // Read after the exit rather than beside the wait: a reader racing the wait
    // is the pipe-buffer deadlock shape, and there is nothing here for that
    // machinery to protect.
    let mut stdout = String::new();
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stdout.take() {
        pipe.read_to_string(&mut stdout)
            .map_err(step(PREFLIGHT_STEP))?;
    }
    if let Some(mut pipe) = child.stderr.take() {
        pipe.read_to_string(&mut stderr)
            .map_err(step(PREFLIGHT_STEP))?;
    }

    // `/bin/echo` ends its line with a newline, so the marker is compared with
    // that trimmed: a byte-exact comparison against `PREFLIGHT_MARKER` can never
    // match, and its failure would read as a refused profile.
    if stdout.trim_end_matches('\n') != PREFLIGHT_MARKER {
        // `rc=71` is not the discriminator it looks like: a missing exec target
        // produces the same code, so the stderr text is what says whether the
        // profile was refused or something else went wrong. Both are carried.
        let diagnostic = stderr.trim().to_string();
        error!(
            step = PREFLIGHT_STEP,
            exit = ?status.code(),
            diagnostic = %diagnostic,
            "sandbox-exec refused the profile"
        );
        return Err(step(PREFLIGHT_STEP)(io::Error::other(format!(
            "exit {:?}: {diagnostic}",
            status.code()
        ))));
    }
    info!(
        step = "preflight",
        rules = profile.rules,
        "sandbox-exec accepted the profile"
    );
    Ok(())
}

/// Clear `FD_CLOEXEC` on the descriptors that must survive `sandbox-exec`.
///
/// **Both descriptors that cross the exec are cleared here, together, and the timing is the
/// reason they are one call.** The payload no longer needs this: it travels on stdin, and the
/// standard streams are inherited anyway (decision 17). The listener and the control channel do,
/// because a socket cannot ride on a stream and `sandbox-exec` sits between the launcher and the
/// child on the exec path — so the child can only rebuild each from a descriptor that crossed
/// that exec, and Rust creates sockets close-on-exec. The child cannot clear the flag for itself:
/// with `FD_CLOEXEC` set there is no descriptor left on the other side for it to act on.
///
/// **This function is called after the pre-flight and before the real spawn, and that placement
/// is the handover's whole audience.** An inherited descriptor is visible to every process this
/// one execs while its flag is clear, so clearing early would hand the listening sockets to the
/// throwaway probe (`sandbox-exec` plus the `/bin/echo` it execs) as well as to the child. The
/// plan carries both objects rather than bare numbers — the listener as a socket, the control
/// channel as a [`ControlHandle`] — precisely so this call can sit here: a plan carrying a bare
/// `i64` would force the launcher that owns the socket to clear it before handing the number
/// over, which is where the probe came to inherit it.
///
/// **Measured, macOS 15.8, 2026-09-26**, by `crates/tinio-sandbox-macos/scripts/verify-descriptor-handover.py`
/// — a program of its own rather than a test, because what has to be observed is a *spawned*
/// process's descriptor table and that is a multi-process measurement: with the pre-fix ordering
/// (the socket cleared ahead of the probe, which is what the bare `i64` forced) the plan's
/// duplicate at **fd 4** appeared in the probe's listing — `[0, 1, 2, 3, 4]`, against `[0, 1, 2,
/// 3]` for the same process before that clear. With this ordering every probe reports the same
/// `[0, 1, 2, 3]` until the clear runs, and `[0, 1, 2, 3, 4]` after it.
///
/// The wrapper is safe **because the objects themselves are the arguments**: `TcpListener` and
/// `ControlHandle` implement `AsFd`, so `rustix::io::fcntl_setfd` is handed real borrowed
/// descriptors and nothing here reconstructs one from an integer. That is why this function
/// needs no `#[allow(unsafe_code)]`: the crate's enumerated sites are the child's descriptor
/// rebuild ([`inherited_fd`]) and the FFI in [`crate::apply`] (decision 14).
fn make_inheritable(listener: Option<&TcpListener>, controls: &[ControlHandle]) -> Result<()> {
    // `None` is legal: a caller that bound nothing is a caller whose child will
    // bind its own socket, and Linux has no listener to hand over at all.
    if let Some(socket) = listener {
        // **`rustix` reports an `Errno`, and the conversion is written as an
        // annotated closure rather than `step("…")(err.into())`.** The bound on
        // the core's `step` is generic (`Source: Into<io::Error>`, one helper for
        // `std`'s errors and `rustix`'s alike), so a bare `err.into()` inside a
        // closure leaves `Source` ambiguous and the darwin target answers
        // `E0283: type annotations needed` — measured by cross-checking. Naming
        // the source type once pins it, and the conversion itself is what the
        // parameter's type does.
        fcntl_setfd(socket, FdFlags::empty())
            .map_err(|err: Errno| step::<io::Error>("fcntl(F_SETFD)")(err.into()))?;
        debug!(
            step = "fcntl(F_SETFD)",
            fd = socket.as_raw_fd(),
            "FD_CLOEXEC is cleared on the listener"
        );
    }
    // The control channel: one unix socket on this platform, or none when the run selected no
    // local management channel. The same clear, on the same object kind, at the same moment —
    // which is the whole point of the plan carrying an owned handle instead of a number.
    for handle in controls {
        fcntl_setfd(handle, FdFlags::empty())
            .map_err(|err: Errno| step::<io::Error>("fcntl(F_SETFD)")(err.into()))?;
        debug!(
            step = "fcntl(F_SETFD)",
            fd = handle.value(),
            "FD_CLOEXEC is cleared on the control channel"
        );
    }
    Ok(())
}

/// Map a failed spawn: a missing `sandbox-exec` is the design's accepted
/// degradation, not a generic I/O error.
///
/// **The two halves are the two shapes the core publishes, and this plan
/// defines neither of them.** A `NotFound` is a *refusal* this platform can
/// name but the core cannot — `Error::Unsupported` with operator-facing words —
/// so it goes through [`refused`], which logs it where the `Error` is built;
/// everything else is something done *to a path*
/// (`spawn /usr/bin/sandbox-exec`), which is exactly what [`launch`] builds and
/// logs, with design §11's path and underlying failure named together. Both
/// come from `tinio-sandbox-core` (the phase-basis correction removed the
/// `step` this file used to define for itself).
///
/// **`mechanism` decides which missing program the refusal names, and the program
/// is an argument rather than a constant because the two mechanisms spawn different
/// things.** Under `sandbox-exec` the missing binary is Apple's helper and the
/// sentence is that mechanism's accepted degradation; under `sandbox-init` the
/// program is **this binary**, so a `NotFound` is a launcher that could not find
/// itself — not the helper's story, and reporting it as one would send the operator
/// looking for a binary the run never invokes.
fn spawn_error(source: io::Error, mechanism: Mechanism, program: &Path) -> Error {
    if source.kind() == io::ErrorKind::NotFound {
        if mechanism.wraps_the_exec() {
            return refused(
                "spawn",
                Error::Unsupported {
                    detail: "sandbox-exec is missing; the macOS sandbox cannot be applied",
                },
            );
        }
        return refused(
            "spawn",
            Error::Unsupported {
                detail: "the launcher could not re-execute itself for the confined child",
            },
        );
    }
    // The path is the program this launch actually tried to run — the caller's own
    // resolved command — so the log names exactly what the kernel was handed.
    launch("spawn", program, source)
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::{OsStr, OsString},
        io,
        os::{fd::AsFd, unix::net::UnixListener},
        path::Path,
        slice,
        sync::{Arc, Mutex},
    };

    use rustix::io::fcntl_getfd;
    use tinio_sandbox_core::AccessMode;
    use tracing::subscriber::with_default;
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    fn own_argv() -> Vec<OsString> {
        vec![OsString::from("serve"), OsString::from("/srv/data")]
    }

    /// The control-channel handle a launcher would put on a plan: one socket bound here, and the
    /// **duplicate** production makes of it.
    ///
    /// Nothing here is a fixture value: the socket is real, its descriptor comes from the operating
    /// system, and the handle is the production conversion ([`ControlHandle::duplicate`] — the same
    /// duplicate `crates/tinio-cli/src/sandbox/macos.rs` puts on the plan). The channel keeps
    /// the socket itself, so the two descriptors are distinct and both are live.
    fn bound_control(path: &Path) -> (UnixListener, ControlHandle) {
        let channel = UnixListener::bind(path.join("control.sock")).expect("bind the socket");
        let handle = ControlHandle::duplicate(&channel).expect("duplicate the control socket");
        (channel, handle)
    }

    /// `FD_CLOEXEC` on a descriptor, as the kernel reports it.
    fn cloexec(fd: &impl AsFd) -> FdFlags {
        fcntl_getfd(fd).expect("read the descriptor's flags")
    }

    /// **The clear is the handoff, and it happens on the plan's duplicate alone.**
    ///
    /// A descriptor crosses `execve` exactly when its `FD_CLOEXEC` is clear, so this function's
    /// whole contract is two flag writes: the plan's control handle loses the bit, and the
    /// launcher's own socket keeps it — the launcher holds that socket for the child's whole
    /// lifetime and only the duplicate is the child's. The listener follows the same rule, and the
    /// empty plan is the legal no-op both `None` and an empty control slice are (Linux binds
    /// nothing and hands nothing over).
    ///
    /// **Why the ordering matters is a property of `confine`, and the observation is a spawned
    /// process's** — the pre-flight's inherited table, which is a multi-process measurement rather
    /// than a unit: `crates/tinio-sandbox-macos/scripts/verify-descriptor-handover.py` is that
    /// program, and `sandbox_macos_e2e` is the end-to-end leg that the child really receives both
    /// descriptors. What belongs here is the decision itself, read off the kernel.
    #[test]
    fn the_clear_reaches_the_plans_handles_and_not_the_launchers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (channel, handle) = bound_control(dir.path());
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a listener");

        // Both of the launcher's own objects are close-on-exec before the call, which is the state
        // every clear has to start from.
        assert_eq!(cloexec(&channel), FdFlags::CLOEXEC, "the bound socket");
        assert_eq!(cloexec(&handle), FdFlags::CLOEXEC, "the plan's duplicate");
        assert_eq!(cloexec(&listener), FdFlags::CLOEXEC, "the bound listener");

        make_inheritable(Some(&listener), slice::from_ref(&handle)).expect("clear the plans");

        assert_eq!(
            cloexec(&handle),
            FdFlags::empty(),
            "the plan's control handle is what the child adopts"
        );
        assert_eq!(
            cloexec(&listener),
            FdFlags::empty(),
            "the plan's listener is what the child serves on"
        );
        assert_eq!(
            cloexec(&channel),
            FdFlags::CLOEXEC,
            "the launcher's own socket is not the plan's to clear"
        );

        // The empty plan: a launch with no listener and no control channel clears nothing and
        // fails nothing, which is the shape Linux's in-place arm and an `api`-less run both have.
        make_inheritable(None, &[]).expect("nothing to clear is not a failure");
    }

    /// A rendered profile from the real generator, so the argv tests exercise
    /// the text and the parameters the launch will actually be handed: one
    /// read-only exposure, which is one `(param "E0")` and one `params` entry.
    /// A read-only set used to be the shape that carried the home grant's `H`
    /// pair; the grant is gone, so the pair list is the same in both modes.
    ///
    /// **The helper's mechanism**, because that is the argv shape this module's
    /// tests are about: `command_args` is the helper's render, and the in-process
    /// mechanism's argv has its own leg ([`the_in_process_argv_is_the_launchers_own`]).
    fn rendered_profile() -> Profile {
        profile(
            &Sandbox::new().expose("/srv/data", AccessMode::ReadOnly),
            Mechanism::SandboxExec,
        )
        .expect("profile")
    }

    /// The `-D NAME=VALUE` block [`command_args`] must write for `profile`.
    ///
    /// **The expectation is derived from the profile rather than hard-coded**, and
    /// the property it states is the one that matters: **every** parameter the
    /// profile declares reaches argv, in order, with no gap and no extra. A
    /// hard-coded vector could not say that — it could only say that one
    /// particular exposure set came out right. It was derived this way while the
    /// home grant's pair depended on the host's `$HOME`, and it stays derived now
    /// that there is no conditional pair at all: the assertion reads the same list
    /// the launcher turns into argv, so a future conditional parameter cannot pass
    /// by being hard-coded into the test.
    fn expected_d_pairs(profile: &Profile) -> Vec<OsString> {
        profile
            .params
            .iter()
            .flat_map(|(name, value)| {
                let mut assignment = OsString::from(name.as_str());
                assignment.push("=");
                assignment.push(value);
                [OsString::from("-D"), assignment]
            })
            .collect()
    }

    #[test]
    fn the_profile_is_one_argv_element_and_no_file_is_named() {
        // Design decision 24: `-p <profile>` inline. `sandbox-exec` has no fd
        // or stdin form, and a profile file would need exit-path cleanup — so
        // the profile must appear as exactly one element, and nothing in the
        // argv may look like a profile file. The `-D` parameters ride beside it
        // as further elements (measured: they work with `-p` and with `-f`, on
        // either side of the profile flag; the plan puts them after it).
        let rendered = rendered_profile();
        let args = command_args(Path::new("/srv/bin/serve"), &own_argv(), &rendered, None);
        assert_eq!(args[0], OsString::from("-p"));
        assert_eq!(args[1], OsString::from(rendered.text.clone()));
        // The whole parameter block, from the profile's own list: the exposures'
        // pairs are all of it, which is why the expectation is derived from that
        // same list rather than spelled.
        let pairs = expected_d_pairs(&rendered);
        assert_eq!(&args[2..2 + pairs.len()], &pairs[..]);
        let program_at = 2 + pairs.len();
        // The `process-exec` literal's parameter, whose value is the same path the
        // program element carries — the rule narrows to exactly the exec this
        // launch performs, and there is no second place to spell it.
        assert_eq!(args[program_at], OsString::from("-D"));
        assert_eq!(args[program_at + 1], OsString::from("P=/srv/bin/serve"));
        assert_eq!(args[program_at + 2], OsString::from("/srv/bin/serve"));
        assert_eq!(args[program_at + 3], OsString::from("serve"));
        assert_eq!(args[program_at + 4], OsString::from("/srv/data"));
        assert_eq!(
            args.iter().filter(|a| *a == OsStr::new("-p")).count(),
            1,
            "exactly one -p, carrying the whole profile"
        );
        assert!(
            args.iter().all(|a| !a.to_string_lossy().ends_with(".sb")),
            "no profile file may be named"
        );
    }

    #[test]
    fn every_parameter_is_emitted_after_the_profile_flag_in_exposure_order() {
        // The order is what makes the references resolve: `(param "E0")` in the
        // text is `params[0]`, and `params[0]` is the first `-D` here. Getting
        // it wrong is not silent — a parameter the profile references but the
        // launch never defines is a compile error, and `preflight` turns that
        // into a startup failure.
        let rendered = profile(
            &Sandbox::new()
                .expose("/srv/data", AccessMode::ReadOnly)
                .expose("/home/u/.tinio/roots/h", AccessMode::ReadWrite),
            Mechanism::SandboxExec,
        )
        .expect("profile");
        let args = command_args(Path::new("/srv/bin/serve"), &own_argv(), &rendered, None);
        assert_eq!(args[0], OsString::from("-p"));
        // The exposure pairs come first and in order, whatever else the profile
        // carries after them.
        assert_eq!(
            &args[2..6],
            &[
                OsString::from("-D"),
                OsString::from("E0=/srv/data"),
                OsString::from("-D"),
                OsString::from("E1=/home/u/.tinio/roots/h"),
            ]
        );
        // Then the whole declared block, in the profile's own order — the
        // exposure pairs, which are every parameter the profile declares.
        let pairs = expected_d_pairs(&rendered);
        assert_eq!(&args[2..2 + pairs.len()], &pairs[..]);
        // The program parameter comes after every declared parameter rather than
        // among them: `P` is the launcher's own value, which `command_args` writes
        // from the argument it already holds.
        let program_at = 2 + pairs.len();
        assert_eq!(
            &args[program_at..program_at + 2],
            &[OsString::from("-D"), OsString::from("P=/srv/bin/serve")]
        );
        assert_eq!(args[program_at + 2], OsString::from("/srv/bin/serve"));
        assert_eq!(&args[program_at + 3..program_at + 5], &own_argv()[..]);
    }

    #[test]
    fn the_argv_carries_a_parameter_only_for_a_rule_that_references_one() {
        // **The property the home grant's removal rests on**: a parameter reaches
        // argv exactly when the text references it, so nothing here can hand
        // `sandbox-exec` a `-D` for a rule that is not in the profile, and nothing
        // can reference a parameter the launch never defines (the latter is a
        // compile error, exit 65, which is worse than the former).
        //
        // This leg used to be `the_home_parameter_is_written_exactly_when_the_rule_is`
        // and it kept the home grant's `H` pair in step with its rule. The grant is
        // gone (design §5's `home` paragraph), so the assertion is now the general one:
        // **every** `-D` this function writes resolves a `(param "…")` the text
        // actually names, and every `(param "…")` the text names has a `-D` — with
        // no parameter left over from the deleted rule.
        let rendered = rendered_profile();
        let args = command_args(Path::new("/srv/bin/serve"), &own_argv(), &rendered, None);

        let declared: Vec<&str> = rendered
            .params
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert!(
            !declared.contains(&"H"),
            "the deleted home grant's parameter is still declared: {declared:?}"
        );
        assert!(
            !rendered.text.contains("(param \"H\")"),
            "the deleted home grant's rule is still rendered:\n{}",
            rendered.text
        );

        // Every `(param "…")` the text names — the exposures and the `process-exec`
        // literal's `P`, which `command_args` writes below rather than through
        // `params` — must have an assignment, and every assignment must be one of
        // those names.
        for line in rendered.text.lines() {
            let Some(open) = line.find("(param \"") else {
                continue;
            };
            let rest = &line[open + "(param \"".len()..];
            let name = rest.split('"').next().expect("the parameter name");
            let assignment = format!("{name}=");
            assert!(
                args.iter()
                    .any(|arg| arg.to_string_lossy().starts_with(&assignment)),
                "the text names `{name}` and argv has no assignment for it: {args:?}"
            );
        }
        for pair in args.windows(2) {
            if pair[0] != "-D" {
                continue;
            }
            let assignment = pair[1].to_string_lossy();
            let (name, _) = assignment
                .split_once('=')
                .expect("every `-D` element is a NAME=VALUE assignment");
            assert!(
                rendered.text.contains(&format!("(param \"{name}\")")),
                "argv assigns `{name}` and the text never names it: {args:?}"
            );
        }
    }

    #[test]
    fn the_in_process_argv_is_the_launchers_own() {
        // **The second mechanism's argv, and the property that makes it that
        // mechanism**: the direct spawn has no helper to be told the profile, so
        // nothing here is `-p`, `-D` or a program element — the text and its
        // parameters ride the payload, and the launcher's own arguments are the
        // whole of what the child is executed with.
        let args = self_args(&own_argv(), Some(7));
        assert_eq!(&args[..own_argv().len()], &own_argv()[..]);
        assert_eq!(args.last().expect("last"), &listener_fd_arg(7));
        assert!(
            args.iter().all(|arg| arg != "-p" && arg != "-D"),
            "the in-process argv carries no profile: {args:?}"
        );
        // The helper's own argv for the same launch *does*, which is the pair that
        // keeps this leg from passing on an argv that simply dropped everything.
        let helper = command_args(
            Path::new("/srv/bin/serve"),
            &own_argv(),
            &rendered_profile(),
            Some(7),
        );
        assert!(helper.iter().any(|arg| arg == "-p"), "{helper:?}");
    }

    #[test]
    fn the_program_parameter_is_written_only_when_the_text_references_it() {
        // **The guard that keeps `command_args` a rendering of the profile it was
        // handed.** `P` resolves the `process-exec` rule's literal, and the rule is
        // the one entry the mechanism decides: a `sandbox-init` render has no such
        // rule, so a helper-shaped argv built from one would define a parameter no
        // rule names. Measured the other way, the pair is present for every render
        // that does carry the rule — which is what the rest of this module's legs
        // assert about the exposure pairs.
        let without = profile(
            &Sandbox::new().expose("/srv/data", AccessMode::ReadOnly),
            Mechanism::SandboxInit,
        )
        .expect("profile");
        assert!(
            !without.text.contains(&program_reference()),
            "the in-process render must carry no `process-exec` rule:\n{}",
            without.text
        );
        let args = command_args(Path::new("/srv/bin/serve"), &own_argv(), &without, None);
        assert!(
            !args
                .iter()
                .any(|arg| arg.to_string_lossy().starts_with("P=")),
            "argv defines `P` and the text never names it: {args:?}"
        );

        let with = rendered_profile();
        assert!(
            with.text.contains(&program_reference()),
            "the helper's render must carry the rule the exec needs:\n{}",
            with.text
        );
        let args = command_args(Path::new("/srv/bin/serve"), &own_argv(), &with, None);
        assert!(
            args.iter().any(|arg| arg == "P=/srv/bin/serve"),
            "argv drops the parameter the rule resolves: {args:?}"
        );
    }

    #[test]
    fn the_listener_descriptor_is_the_last_element() {
        // Design §3 step 7: the listener's descriptor number travels in argv,
        // "because a socket cannot ride on a stream". The payload does ride on
        // one — stdin — so this is the listener's own element, and it is this
        // function's last. What this test asserts is the element's *identity*: the
        // number is written by `listener_fd_arg` and recognised by
        // `is_listener_fd_arg`, both the core's. **Decoding it back is not this
        // crate's**: `listener_fd_from_arg` is called on the core's side of the
        // contract, inside `adopt`, so the launcher never imports it and this test
        // does not either — the round trip is core's to test. The control
        // channel's elements are the *plan's* and `confine` appends them after
        // this one, one `control_fd_arg` per entry.
        let args = command_args(
            Path::new("/srv/bin/serve"),
            &own_argv(),
            &rendered_profile(),
            Some(7),
        );
        let last = args.last().expect("last element");
        assert_eq!(last, &listener_fd_arg(7));
        assert!(is_listener_fd_arg(last));
        assert!(
            !is_listener_fd_arg(OsStr::new("serve")),
            "a plain argument is not the launcher's descriptor element"
        );
    }

    #[test]
    fn a_launch_without_a_listener_carries_no_descriptor_element() {
        // Linux confines in place and its plan has no listener; a launcher that
        // bound nothing must not invent one.
        let rendered = rendered_profile();
        let args = command_args(Path::new("/srv/bin/serve"), &own_argv(), &rendered, None);
        // `-p`, the text, the program parameter's two elements and the program
        // itself, plus two elements per declared parameter — every declared
        // parameter being an exposure's pair. (`rendered_profile` is the helper's
        // render, so the `P` pair is one of the two program-parameter elements:
        // `the_program_parameter_is_written_only_when_the_text_references_it` is the
        // leg that pins its presence to the rule.)
        assert_eq!(args.len(), own_argv().len() + 5 + 2 * rendered.params.len());
        assert!(args.iter().all(|arg| !is_listener_fd_arg(arg)));
    }

    #[test]
    fn the_child_is_this_binary_re_invoked() {
        // The launcher has no separate child program: it re-execs itself with
        // its own arguments (design §3 step 6, "`sandbox-exec -p … <self> <args>`").
        let rendered = rendered_profile();
        let args = command_args(Path::new("/srv/bin/serve"), &own_argv(), &rendered, Some(9));
        // The program element is the launcher's own path, which `confine` resolved
        // canonically before calling this, and the launcher's own arguments follow
        // it unchanged — the listener element being the one thing appended, so it
        // is last and the `serve` arguments are not.
        let program_at = 2 + expected_d_pairs(&rendered).len() + 2;
        assert_eq!(args[program_at], OsString::from("/srv/bin/serve"));
        assert_eq!(&args[program_at + 1..program_at + 3], &own_argv()[..]);
        assert!(is_listener_fd_arg(
            args.last().expect("the descriptor element")
        ));
    }

    /// Everything the closure logged, as the subscriber rendered it.
    ///
    /// **This installs a subscriber for the duration of one closure and no
    /// longer**, which is the whole reason it is a test helper: the crate
    /// installs none (shared §7), so a test that wants to read its own events
    /// has to be the one to capture them. `with_default` is thread-scoped, so
    /// the other tests in this binary are unaffected.
    fn record_events(emit: impl FnOnce()) -> String {
        #[derive(Clone, Default)]
        struct Sink(Arc<Mutex<Vec<u8>>>);

        impl io::Write for Sink {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0.lock().expect("sink").extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        impl<'a> MakeWriter<'a> for Sink {
            type Writer = Sink;

            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let sink = Sink::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            // **Load-bearing for a field assertion, and measured rather than
            // assumed**: the default subscriber wraps each field *name* and the
            // equals sign after it in ANSI escapes, so a captured line carries
            // the field only as coloured spans and never as `step=<value>`. The Linux
            // plan's own `traced` helper carries this same line for the same
            // reason, and so does the core's recorder test.
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .without_time()
            .finish();
        with_default(subscriber, emit);
        String::from_utf8_lossy(&sink.0.lock().expect("sink")).into_owned()
    }

    /// A failure logs the `step` it returns, under a recording subscriber.
    ///
    /// Shared §7's checking rule, and the one assertion that makes "it logs" a
    /// property this crate can lose: the closure runs real code and the test
    /// requires the event's `step` field and the `Error::Step`'s `step` to be the
    /// same string, so a step name changed in one place and not the other fails
    /// here rather than in a log nobody reads. **The builder it calls is the
    /// core's** (the phase-basis correction), so what this test pins for this
    /// crate is the call this file actually makes — the curried `step` that the
    /// pre-flight and `make_inheritable` both reach for; the core's own
    /// recorder test covers `launch` and the property in general.
    ///
    /// **The pre-flight's own step is asserted beside it**, because that is the
    /// name five call sites in one function used to spell by hand: the one
    /// [`PREFLIGHT_STEP`] binding is what the log's field and the error's field
    /// now share, and this leg fails if either stops reading it.
    #[test]
    fn a_failure_logs_the_step_it_returns() {
        let mut named = None;
        let recorded = record_events(|| {
            let err = step("payload stdin")(io::Error::other("no piped stdin"));
            named = Some(match err {
                Error::Step { step, .. } => step,
                other => panic!("expected Step, got {other:?}"),
            });
            let err = step(PREFLIGHT_STEP)(io::Error::other("no exit within 10s"));
            let Error::Step { step, .. } = err else {
                panic!("the pre-flight's builder is the core's `step`");
            };
            assert_eq!(step, PREFLIGHT_STEP);
        });
        assert_eq!(named, Some("payload stdin"));
        assert!(recorded.contains("step=\"payload stdin\""), "{recorded}");
        assert!(
            recorded.contains(&format!("step=\"{PREFLIGHT_STEP}\"")),
            "{recorded}"
        );
    }
}
