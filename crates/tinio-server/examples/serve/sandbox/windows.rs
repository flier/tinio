//! Windows launcher confinement: design §10 steps 6 and 7.

use std::{
    env,
    error::Error,
    net::TcpListener as StdTcpListener,
    path::{Path, PathBuf},
    process,
};

use tinio_config::{Config, sources};
use tinio_core::mode::AccessMode;
use tinio_sandbox::{
    Backend, Confinement, Event, LaunchPlan, Sandbox, supervise,
    windows::{self, control},
};
use tokio::{runtime::Builder, sync::watch};

use super::{refuse_in_root_state_dir, sandbox_request};
use crate::{
    args::{Args, own_args, parse_args},
    events::{emit, emit_on_error},
    read_only::resolve_read_only_state_dir,
    serve,
};

/// Windows' step of design §10 step 6 — bind the port, hand the socket over,
/// launch the confined child — returned to `main`, which supervises it.
///
/// **Windows-only, and deliberately so.** The platforms that confine in place
/// have nothing to hand back here: Linux binds *after* the confinement and its
/// own process emits the `Listening` event (decisions 6 and 19), so its arm is
/// [`confine_in_place`] and belongs to `2026-09-18-sandbox-linux.md` Task 7. A
/// shared helper returning `Option<Child>` would have to invent a `None` no
/// launcher platform ever produces — the failure an earlier draft of this step
/// shipped, and it cost a call into an API no phase defines.
///
/// **No `Listening` event is emitted here, even though this is the process that
/// bound the port.** The event means the data plane is serving, and only the
/// confined child can say that; the launcher's part is to relay the child's
/// frame, not to anticipate it (decision 19). An event emitted here would reach
/// the consumer twice — the child's own frame arrives through the relay a
/// moment later — and it would announce a server that had not started.
///
/// **The gate carries `target_os` as well as the feature, and that is not
/// decoration.** The facade publishes this platform's crate under a
/// `#[cfg(target_os = "windows")]` alias (design decision 2) while the `sandbox`
/// feature is on by default everywhere, so on Linux and macOS — which is where
/// P1's legs and CI build — an item gated by the feature alone names an alias
/// that does not exist there and fails to resolve. This is shared §2's rule in
/// its second shape: a crate-root `#![cfg]` does not reach `tests/` or
/// `examples/`, so every separate target repeats the gate it needs, and no item
/// that is configured out may be named by one that is not.
///
/// **The document travels as bytes and this arm never parses it**: the launcher
/// builds no `Config` (decision 18), and the child re-derives the identical
/// answer from the payload's text, the environment and its inherited argv.
#[cfg(all(feature = "sandbox", target_os = "windows"))]
pub fn confine_or_launch(
    args: &Args,
    sandbox: &Sandbox,
    config_bytes: &[u8],
) -> Result<tinio_sandbox::Child, Box<dyn Error + Send + Sync>> {
    // `StdTcpListener` is `std`'s listener under an alias, because this
    // example's `TcpListener` is the `tokio` one everywhere else: the child's
    // socket is a `std` one until the body it is served in converts it inside
    // a runtime.
    let listener = StdTcpListener::bind(args.address)?;
    // The configuration is text on the wire (its `String` type is what makes that
    // a compile-time fact); `config_bytes` is only bytes because the reader in
    // `main` hands them over that way.
    let plan = LaunchPlan::for_launch(String::from_utf8_lossy(config_bytes).into_owned())
        .with_listener(listener);
    // The container's capability list is a Windows setting with no equivalent on
    // the other two platforms, and the `use` above reaches it through the facade's
    // alias for this platform's crate rather than through `platform()` — the
    // facade returns whatever backend the build targets, and an options value only
    // one of them has cannot be handed to it (design decision 2 owns that
    // boundary, and §10's "the difference is configuration rather than dispatch").
    // What stays cfg-free is the dispatch: `Confinement` is the core's, and the
    // match below is the same on every platform.
    //
    // **The environment variable is the only channel an operator has today**,
    // and the list it carries is provisional until `[server] sandbox` and its
    // capability list land with the CLI: an absent or empty value is no
    // capability at all, which is the whole of the outbound guarantee's Windows
    // half (design §7). The parsing is the backend's own
    // (`Capability::parse_list`), so this arm states the channel and not the
    // grammar — and `?` carries its refusal, which names the entry it could not
    // read.
    let options = windows::Options {
        capabilities: windows::Capability::parse_list(
            env::var("TINIO_SANDBOX_CAPABILITIES").ok().as_deref(),
        )?,
        ..Default::default()
    };
    let mut backend = windows::Backend::with_options(options);
    match backend.confine(sandbox, &plan)? {
        Confinement::Supervise(child) => Ok(child),
        Confinement::InPlace => Err("the Windows backend must supervise its child".into()),
    }
}

/// Everything design §10 steps 6 and 7 do on this platform, from the exposure
/// set to the child's exit status.
///
/// **One function rather than the arm `main` used to carry inline**, so the
/// entry point reads as the design's own sequence: this is the step where the
/// two shapes differ, and the difference is here rather than woven through the
/// caller.
///
/// **It diverges: there is no path through it that returns to `main`.** A
/// launcher that failed to launch, or whose child died, reports that as an
/// `Error` frame and an `Err` — never as a fall-through to the unconfined
/// server below it, which is the failure mode this whole design exists to
/// prevent.
#[cfg(all(feature = "sandbox", target_os = "windows"))]
pub fn launch(
    args: &Args,
    config_bytes: &[u8],
    read_only: bool,
    access: AccessMode,
    state_dir: &Option<PathBuf>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // The **resolved** mode and not the configuration file's own field:
    // `TINIO_READ_ONLY` and `--read-only` are part of the chain that produced
    // `access`, so reading a document here would expose the served root
    // `ReadWrite` on a run the operator asked to be read-only — losing the
    // whole of FR-023's confinement half silently. (P1's arm states the same
    // rule; the difference is only that this arm has no document to be
    // tempted by, the launcher having built none — decision 18.) No
    // conversion, and nothing left to keep in step: `access` **is** the type
    // the exposure set takes.
    let root_access = access;
    // **The state directory's path for the exposure set, and on the read-only
    // path it is the value the caller resolved rather than a second resolution
    // of it.** Read-only mode relocates state to `TINIO_STATE_DIR` or
    // `<home>/.tinio/roots/<hash>/`, so a hand-built `<root>/.tinio` would
    // expose the wrong path and leave the confined server unable to write its
    // own state; `state_dir` holds exactly that answer, with its `Some` arm the
    // read-only path's own by construction — so this is the same directory the
    // storage is handed, and the two could only disagree if this arm
    // re-derived it and the environment changed in between. In normal mode
    // `state_dir` is `None` and the answer is `<root>/.tinio`, which the root's
    // exposure already covers, so [`sandbox_request`] adds no second entry. The
    // child re-derives the identical path from its own three sources (design
    // §10).
    //
    // **The spelling stays `args.root`'s, and not the canonicalised path the
    // caller computed for the state directory**, which is what makes the
    // containment check a statement about the paths the operator gave: it is
    // the same string the exposure set carries, so "the state directory is
    // inside the served root" is decided about one pair of paths rather than
    // about a canonical form and a written one. The read-only value is in that
    // spelling too — `TINIO_STATE_DIR` as the operator wrote it, or the derived
    // home layout — which is why the comparison is meaningful on both arms.
    let root = Path::new(&args.root);
    let state = state_dir.clone().unwrap_or_else(|| root.join(".tinio"));
    // The refused combination of design §9, and it is refused **before** the
    // exposure set is built: the entry it would add is the hole the guard
    // exists to close, so the two lines are ordered and not merely adjacent.
    refuse_in_root_state_dir(read_only, root, &state)?;
    // **`sandbox_request` and not a set rebuilt here**, because the exposure
    // rule and the device-file flag are one function's and this example states
    // each of them once: the platform-neutral helper carries the rule the
    // in-place platform's arm calls too, and the executable's own directory is
    // this platform's backend's entry (`backend::exposures`, decision 22)
    // rather than a second copy of it here.
    let sandbox = sandbox_request(root, &state, root_access);
    // A startup failure is an event, not an absence (decision 19): a consumer
    // that decodes the stream hears about it instead of timing out.
    let mut child = match confine_or_launch(args, &sandbox, config_bytes) {
        Ok(child) => child,
        Err(err) => {
            emit(&Event::Error {
                message: err.to_string(),
            });
            return Err(err);
        }
    };
    // §3 step 8: wait, and forward the child's status. §3 step 9's relay is
    // this platform's, armed inside `confine`. `supervise` **borrows** the
    // child, which is what lets the terminal record be read *here* — after
    // the wait, when the relay has been joined and the answer is final. A
    // child that exited without ever reporting is the one thing the parent is
    // the only witness to (decision 19), and `Listening` is not the parent's
    // to emit: only the process serving the data plane can say it is serving.
    // A child that reported in full has already relayed its own frame, so this
    // process writes nothing to stdout on that path at all.
    let status = supervise(&mut child, || ())?;
    if child.terminal().is_none() {
        emit(&Event::Error {
            message: format!("the confined child exited ({status}) without reporting readiness"),
        });
    }
    // The terminal record has been read above and the child waited for and
    // reaped, so the guard is done with its work and the exit consumes it.
    exit_launcher(child, status.code().unwrap_or(1));
}

/// **End the launcher's process, consuming the child guard on the way out.**
///
/// The AppContainer profile's removal is `PackageSid`'s `Drop`, which the guard
/// owns — through the wait closure, whose `Grants` hold the last `Arc` to it once
/// `Grants::rollback` has run. `process::exit` runs no destructors, so a launcher
/// that exits while still holding the guard leaves the profile behind on **every**
/// clean run, where decision 21 promises it is taken over and removed: measured
/// by Task 17, and eight leaked profiles were cleaned across two rounds of it.
///
/// **The guard is an argument rather than a statement, and that is the point.**
/// An earlier shape dropped a borrowed guard three lines above the exit — correct
/// as written, and reintroducible by any later `process::exit` that landed above
/// the drop, silently. What this shape enforces is narrower than "the compiler
/// refuses an exit that skips the deletion", and it is worth stating exactly:
/// **this exit cannot be reached without handing the guard over**, because the
/// guard is its parameter and exiting is all it does. An *unconditional* exit
/// inserted above a call to it fails to compile (`unreachable_code`); a
/// *conditional* one would compile and leak on the path it takes, exactly as the
/// earlier shape would.
///
/// `process::exit` and not a `return` of it: the call diverges, and an expression
/// that can never produce a value for `return` is what the unreachable-code lint
/// names — the same shape P1's arm ends with. The crate's own `Drop` logs
/// `step = "DeleteAppContainerProfile"` (sid.rs) as it goes, so the launcher's
/// stderr states the removal rather than leaving the next diagnosis to infer it
/// from an absent profile.
///
/// **Gated like every other item this platform's arm owns** ([`confine_or_launch`],
/// [`launch`], [`confined`]): its only caller is inside that arm, and on any
/// other target an ungated `fn` here is dead code — which the workspace lints as
/// an error (`-D warnings`), on the very leg that lints Linux.
///
/// **`disarm` is what gives the operator a way out, and it is the second half of
/// a rule the relay states rather than a courtesy.** The handler answers `1` for
/// every console event it recognises (`control::console_handler`), which means
/// Ctrl+C is *consumed*: the forward to the child's group is what design §3 step 9
/// wants, and the cost is that the default disposition never runs. That is the
/// right trade while this process is supervising — the stop is being handled —
/// and the wrong one the moment it is not: a child that never exits leaves
/// `supervise` waiting with the console refusing to terminate this process, so the
/// operator's only remaining move is the task manager. Disarming before the exit
/// restores the default disposition for every signal after the supervision is
/// over, which is exactly the window in which nothing needs consuming. The
/// frequency is one line's worth of doubt: `arm`'s own doc records that a second
/// registration is not recognised as one, so a process that launches more than
/// once disarms before the next arm — and this is the last thing this process
/// does, which makes the same call the honest end of the same contract.
///
/// A failure is ignored rather than reported. There is no caller to report to,
/// the process is about to leave, and the alternative — refusing to exit because
/// the console could not be restored — would be a launcher that hangs on the one
/// path it entered to make hanging impossible.
#[cfg(all(feature = "sandbox", target_os = "windows"))]
fn exit_launcher(child: tinio_sandbox::Child, code: i32) -> ! {
    drop(child);
    let _ = control::disarm();
    process::exit(code);
}

/// The confined child: the configuration is the payload's **text**, never a
/// re-read of the path in `argv` — the file it names is outside every exposure,
/// and re-reading it would be denied by the confinement this process is running
/// under (design §3 step 2). `Adopted::config` is a `String` because `Config`
/// requires UTF-8, so there is no bytes-to-text step at this call site.
///
/// **Everything the parent resolved from a host path is re-derived here, from the
/// same three sources and in the same order** (design §10: "the child re-derives
/// the identical answer from the payload's configuration bytes, the environment
/// and its inherited argv"). That is the mode — `[server] read_only` out of the
/// payload's text, overlaid by `TINIO_READ_ONLY` and then by `--read-only` —
/// the state directory, and the **`FsStorage` that owns it**. What the child
/// must **not** do is re-read a host path: the configuration file is outside
/// every exposure, which is why its bytes travel in the payload in the first
/// place (decision 18).
///
/// **Decision 5's channel refusal is not repeated here, and the reason is that
/// this process cannot be reached with one in effect.** The launcher's arm runs
/// that refusal in the parent, before it launches anything, so a child exists
/// only where the key and the variable were both answered; and the statement
/// would not even hold here — this process *is* confined, having been created
/// inside the container, so "this channel would not confine the run" is false of
/// it whatever the payload says. A second copy of the rule in this process would
/// be a check that cannot fail, with a second chance to drift from the one
/// `main` runs.
///
/// **The storage is built here and the launcher builds none at all** —
/// constructing one opens the state database, and two processes on one state
/// directory is not something the storage layer is written for — so what the
/// parent is left with is a path, and the child builds the storage because it
/// never ran the parent's pre-confinement sequence. The `watch` channel is the
/// same shape of fact: the child is what reacts to a forwarded stop, so the
/// channel is created here and handed to `on_stop`, not carried across the launch
/// (design §3 step 9).
///
/// **This is the process that emits `Listening`** — the only one that can, since
/// it is the one serving — and everything it prints to stdout is relayed by the
/// parent after decoding (decision 19). A refusal on the way here is an `Error`
/// frame for the same reason: the parent's consumer reports the cause instead of
/// waiting out a readiness timeout.
#[cfg(all(feature = "sandbox", target_os = "windows"))]
pub fn confined(adopted: tinio_sandbox::Adopted) -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = parse_args(own_args())?;
    let config = match adopted.config() {
        "" => None,
        text => Some(emit_on_error(Config::parse(text))?),
    };

    // The same chain `main` ran, on this process's own inputs (design §9's
    // precedence, unchanged: the file, then the environment, then the flag).
    let file_read_only = config
        .as_ref()
        .is_some_and(|config| config.server.read_only);
    let read_only =
        emit_on_error(sources::env_read_only())?.unwrap_or(file_read_only) || args.read_only;
    let access = AccessMode::from(read_only);
    let state_dir = resolve_read_only_state_dir(Path::new(&args.root), read_only, |_, _| {})?;
    let (storage, pipelines) = serve::build_storage(&args.root, &config, state_dir, access)?;
    let listener = tinio_sandbox::adopted_listener(&adopted)?;
    // **The inherited listener is blocking, and Windows' tokio cannot tell.**
    // The launcher binds it with `std`'s `TcpListener` (`confine_or_launch`), and
    // a `std` socket is created non-blocking only on the unix side: `mio`'s
    // `from_std`, which tokio's conversion bottoms out in, documents that it
    // "assumes nothing about the underlying listener; it is left up to the user
    // to set it into non-blocking mode", and tokio's own guard —
    // `check_socket_for_blocking` — is a no-op under `cfg(not(unix))`, with the
    // comment "we cannot retrieve the nonblocking status on windows". So a
    // blocking listener is accepted silently, the accepted connection inherits
    // the mode, and hyper's next read **blocks the worker thread inside `recv`**
    // instead of parking the task: the response it has already built is never
    // written and the client sees nothing until it gives up (measured 2026-09-21
    // with a probe that logs every hyper I/O call — the blocking-listener trace
    // reads the whole request, calls the service, then spends 3 s inside one
    // `poll_read` and makes no write of any kind, where the same probe on a
    // listener tokio bound returns `Pending` in 0 µs and writes).
    //
    // **Set on the child's own copy and not on the launcher's**, which is the
    // placement the same probe measured: a mode set by the parent was not visible
    // through the inherited handle, and the rebuilt socket is the one the child
    // serves on. `tinio-sandbox-windows::socket::from_value` would be the other
    // natural home and is the wrong one — it is P0's shared surface, and its
    // blocking round trip is one of the rows that exonerate the handoff.
    listener.set_nonblocking(true)?;
    // A forwarded stop lands here (design §3 step 9). What it does is start the
    // same shutdown the runtime already knows: the server stops accepting, the
    // background tasks are signalled and the pipelines drain. The bounded version
    // of that — ≤10 s, state file removed — is the CLI contract's (T066/T067) and
    // lands on this hook.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tinio_sandbox::on_stop(move || {
        let _ = shutdown_tx.send(true);
    })?;
    let served =
        emit_on_error(Builder::new_multi_thread().enable_all().build())?.block_on(serve::serve(
            args,
            config,
            storage,
            access,
            pipelines,
            Some(listener),
            Some(shutdown_rx),
        ));
    // **The child unarms the handler it armed above: the same call the launcher
    // makes, for the same rule, at the same kind of point — the last one before
    // the process leaves.** `tinio_sandbox::on_stop` is what installed it (the
    // stop hook a forwarded stop lands on), and it is the same
    // `control::console_handler` the launcher's relay registers: the routine that
    // answers `1` for every event it recognises, and therefore one that goes on
    // *consuming* every stop once there is nothing left for it to signal.
    // `exit_launcher` is where the rule is written down; this is its other call
    // site.
    //
    // **What is not the same is the danger, and the difference is stated rather
    // than borrowed.** What the launcher's disarm answers is a supervision it
    // cannot leave: a child that never exits leaves `supervise` waiting while the
    // console refuses to terminate the launcher, and the operator's only move is
    // the task manager. This process has no such window — it reaches the line
    // below only once its own drain has finished (`serve` returned, having
    // stopped accepting, stopped the scanner and the sweeper and drained the
    // pipelines), and everything it waits on is its own — so the stop it consumed
    // on the way there is the hook doing the work it was armed for rather than a
    // stop being ignored. What could still leave it on that line with the handler
    // in place is a shutdown path that never finishes, and that is a wedged drain
    // rather than a supervision it cannot leave. The reading is therefore the
    // honest one: **this is symmetry with the launcher's exit and correctness of
    // teardown — the default disposition restored for the window between the
    // drain and the process actually leaving — and not a fix for a hang.** It
    // should not be read, here or in a later diagnosis, as evidence that it
    // closed one.
    //
    // The one return that does not reach the line is the runtime failing to build
    // above it — `emit_on_error`'s `?` — where nothing was served and no shutdown
    // sequence runs.
    //
    // A failure is ignored rather than returned, for the launcher's reason and
    // for one this call site adds: `confined`'s `Err` is `main`'s `Err` — the
    // child's entry is `return confinement::confined(adopted)`, which is not
    // wrapped in `emit_on_error` as the launcher's arm is — so a console that
    // could not be restored would turn a run that served and drained cleanly into
    // a failure exit. There is no caller to report to, the process is leaving
    // either way, and the handler goes with it.
    //
    // The gate is this function's own above (`feature = "sandbox"` and
    // `target_os = "windows"`), which is what keeps the call out of every build
    // that has no `control` to call — the rule every gated item in this file
    // follows.
    let _ = control::disarm();
    served
}
