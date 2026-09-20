//! Minimal serve binary for the interop harness (task T032).
//!
//! Serves one storage root over the S3 data plane: `serve <root>
//! [--port N] [--address HOST:PORT] [--config <config.toml>] [--read-only]
//! [--sandbox]`. **Stdout carries event frames and nothing else**, and
//! `emit` below is their only writer: one `event: listening` frame whose
//! `data` names the bound address once the plane serves (the harness parses
//! it, needed for `--port 0`), or one `event: error` frame when startup
//! refuses — so a consumer decodes SSE events rather than reading a line,
//! and a stream that ends with no frame at all is a refusal it reports
//! rather than a hang it waits on. Every log, diagnostics included, goes to
//! stderr. **The parser is `clap`'s, and it is configured to have no route to
//! stdout at all**: `--help` and `--version` are disabled, so a request for
//! either stands beside every other unknown argument — clap's error text on
//! stderr, the `error` frame this contract owes, exit 2 ([`RawArgs`]). **That
//! half of the contract lives in a dependency and no scan of this file can
//! reach it** — see [`emit`] for what the source check does and does not
//! prove. The scanner is on by default (config defaults) and toggled by
//! `TINIO_SCANNER` (`0`/`1`); the sweep runs with default TTLs. Superseded by
//! the full CLI in US2.
//!
//! The parser also costs nothing it does not use: `clap` is a **dev**-dependency
//! of this crate, and it is taken without its `help` feature — both flags are
//! disabled outright, so no help text is rendered anywhere and none is compiled
//! in.
//!
//! `--read-only` (FR-023) refuses every mutating operation and moves the
//! tool's state out of the root — `TINIO_STATE_DIR` if set, else
//! `<home>/.tinio/roots/<hash>/` — so the root is only ever read. The
//! resolved mode is the config file's `[server] read_only`, overlaid by
//! `TINIO_READ_ONLY` (`1`/`true`, `0`/`false`) and then by the flag.
//!
//! Auth: s3s rejects signed requests when no auth provider is configured,
//! and real S3 clients always sign — so the harness accepts the fixed
//! MinIO-convention pair `minioadmin` / `minioadmin` (unsigned requests are
//! rejected). With feature `acl` and `[s3] acl = true` the plane runs the
//! identity-backed authn/authz stack instead: the root user is the
//! `[auth]` credential pair presenting the `[owner]` element, with the
//! configured `[[users]]` assembled by
//! `tinio_server::identity::Identity::from_config`.

use std::{
    env,
    error::Error,
    ffi::OsString,
    fs,
    io::{Write as _, stdout},
    net::SocketAddr,
    path::{Path, PathBuf},
    process,
    sync::Arc,
    time::Duration,
};

use clap::Parser;
use futures::StreamExt;
use sweep::{Options, Sweeper};
use tinio_config::{
    Config,
    log::{AccessFormat, Format, Verbosity},
    pipeline, sources, state,
};
use tinio_core::{
    cleanup::{Cleanup, CleanupOptions, RepairKind},
    mode::AccessMode,
    pipeline::Runner,
    storage::{
        DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_META_BATCH_BYTES, DEFAULT_META_BATCH_SIZE,
    },
};
use tinio_fs::{
    Error as FsError, FsCleanup, FsOptions, FsStorage, Scanner, ScannerOptions,
    database::WriteLockSnapshot, etag, state_dir as default_state_dir, sweep,
};
use tinio_sandbox::Event;
// Reading a `Sandbox` is `sandbox_request`'s business and nobody else's, and
// that helper is read by the Linux arm and by the tests below — so this import
// and the helper carry the same gate. `Event` above is unconditional because
// every build writes frames, feature or platform regardless (design decision
// 19).
#[cfg(any(test, all(feature = "sandbox", target_os = "linux")))]
use tinio_sandbox::Sandbox;
// The platform's own vocabulary — `Options`, `Backend`, `Seccomp` — reached
// through the facade rather than by naming the backend crate: a consumer that
// named it would carry one target-gated dependency per platform by the time all
// three arms of `main` exist, and the facade's own rule is that consumers name
// it and nothing else.
#[cfg(all(feature = "sandbox", target_os = "linux"))]
use tinio_sandbox::linux;
// Driving a confinement — the trait, its answer, the plan and the supervision —
// is read only by the arm below, and that arm exists for Linux **with** the
// feature: a target whose arm another plan owns refuses the flag and reads
// nothing else yet. The gate follows that arm rather than the platform alone,
// which is what the example has to compile under either way — it is built by
// every configuration, so an import only a gated arm reads is an unused one in
// the rest.
#[cfg(all(feature = "sandbox", target_os = "linux"))]
use tinio_sandbox::{Backend, Confinement, LaunchPlan, supervise};
use tinio_server::{
    Capabilities, DataPlane, PlaneConfig, identity, log as server_log,
    metrics::{self, WriteLockStats},
    pipeline::Pipelines,
};
#[cfg(feature = "acl")]
use tinio_server::{Identity, identity::IdentityFromConfig};
use tokio::{net::TcpListener, runtime::Builder, sync::watch};
use tracing::subscriber;

/// The three lanes `serve` drives, named once for the signature that carries
/// them.
///
/// The parameters are not decoration and are not this file's choice: the IO lane
/// is typed to `tinio-fs`'s ETag result, the removal and DB lanes to a unit
/// result, and `Pipelines::build` infers exactly this triple from the
/// `FsOptions` the storage is constructed with. Naming it here is what keeps
/// that triple out of a function signature twice over.
type Lanes = Pipelines<etag::Result, Result<(), FsError>, Result<(), FsError>>;

/// The parsed command line.
///
/// Created by this step: the entry point used to parse into four locals of
/// `main` (`root`, `address`, `config_path`, `read_only_flag`), which left
/// nothing for a test to feed a fabricated `argv` to and nothing for `serve`
/// to receive.
///
/// **The address is resolved, not parsed** — [`parse_args`] folds `--address`
/// over `--port` over 9000 into it, so every caller binds `args.address`
/// rather than resolving the pair a second time.
struct Args {
    /// The served root, as the operator spelled it. A `PathBuf` rather than the
    /// `String` it used to be: a path that is not valid UTF-8 is legal on Linux,
    /// and `create_dir_all`, `Config` and `FsStorage::new` all take `Path` anyway.
    root: PathBuf,
    /// The address the listener binds, `--address` over `--port` over 9000.
    address: SocketAddr,
    /// `--config <file>`, when one was given.
    config_path: Option<PathBuf>,
    /// The `--read-only` flag. **FR-023's field, and not the resolved mode**: the
    /// file's `[server] read_only` and `TINIO_READ_ONLY` are the other two
    /// precedence levels, and `main` folds all three together.
    read_only: bool,
    /// `--sandbox` (design decision 5).
    sandbox: bool,
}

/// The command line as `clap` parses it: the flag-for-field shape of
/// `serve <root> [--port N] [--address HOST:PORT] [--config <file>]
/// [--read-only] [--sandbox]`, which [`parse_args`] resolves into [`Args`].
///
/// **Two types rather than one, because clap fills a field from the flag that
/// names it** and `--port`/`--address` are two flags for the one address the
/// server binds. The resolved `Args` is what the rest of the file reads, so
/// this one stays private to the parser.
///
/// **There is no help and no version, and that is a statement about stdout
/// rather than about etiquette** (design decision 19). Text the operator asks
/// for is still tool output, and clap prints it to stdout — the stream this
/// process reserves for event frames — so both flags are disabled and a
/// request for either is an unknown argument like any other typo: refused on
/// stderr with the `error` frame that goes with it, and nothing on stdout but
/// that frame. clap's error text carries the usage line, so the operator still
/// gets the guidance. **The parser is left with no route to stdout at all**,
/// which is cheaper to hold than a rule about which of its outputs go where.
/// The crate also takes clap without its `help` feature, so there is no help
/// *renderer* compiled in either — and `disable_help_flag` earns its place
/// independently of that: clap's `_check_help_and_version` adds `-h`/`--help`
/// unless the setting is set, feature or no feature, so the flag is what stops
/// the argument existing rather than what stops it being answered.
///
/// **`no_binary_name` is what the iterator parameter already means.** The
/// callers hand over arguments and not `argv[0]` — `main` skips it, and each
/// launcher platform filters its own element out of what is left — while clap
/// reads the first element it is given as the program name. Without the
/// setting the served root would be swallowed as that name and every parse
/// would fail on a missing root.
#[derive(Parser)]
#[command(
    name = "serve",
    no_binary_name = true,
    disable_help_flag = true,
    disable_version_flag = true
)]
struct RawArgs {
    /// The served root: the one bare argument, wherever it appears. The
    /// flag/bare split is clap's, so a `--sandbox` before the root is a flag
    /// rather than the directory to serve, and a second bare argument is a
    /// refusal rather than a silently ignored one.
    root: PathBuf,
    /// `--port N`, which means `127.0.0.1:N` once [`parse_args`] resolves it.
    #[arg(long, value_name = "N")]
    port: Option<u16>,
    /// `--address HOST:PORT`, which outranks `--port`.
    #[arg(long, value_name = "HOST:PORT")]
    address: Option<SocketAddr>,
    /// `--config <file>`. A `PathBuf` rather than the `String` a text-only
    /// parser would take: a path that is not valid UTF-8 is legal on Linux, and
    /// `create_dir_all`, `Config` and `FsStorage::new` all take `Path` anyway —
    /// refusing such a path here would be refusing it for a reason the operator
    /// cannot see. clap's parser for it hands the bytes through as they came.
    #[arg(long, value_name = "CONFIG.TOML")]
    config: Option<PathBuf>,
    /// The `--read-only` flag (FR-023).
    #[arg(long)]
    read_only: bool,
    /// `--sandbox` (design decision 5).
    #[arg(long)]
    sandbox: bool,
}

/// Parse the command line, or report why it cannot be parsed.
///
/// **It returns the failure rather than printing and exiting**, which is what
/// lets the tests below assert on the parse at all — and it is the iterator
/// parameter, not a convenience, that lets them: a parser reading
/// `std::env::args_os()` itself could only be tested by mutating the test
/// process's own arguments, and the launcher platforms' `main` parses the
/// arguments left after the listener element is filtered out. clap takes that
/// iterator as it stands in `try_parse_from`; its `parse` would read the
/// process's own arguments and exit on its own terms.
fn parse_args(
    args: impl IntoIterator<Item = OsString>,
) -> Result<Args, Box<dyn Error + Send + Sync>> {
    let args = RawArgs::try_parse_from(args)?;
    Ok(Args {
        root: args.root,
        // The one precedence there is, resolved here rather than at each of
        // the callers: `--address`, else `--port` on the loopback, else 9000.
        address: args
            .address
            .or_else(|| {
                args.port
                    .map(|port| SocketAddr::from(([127, 0, 0, 1], port)))
            })
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 9000))),
        config_path: args.config,
        read_only: args.read_only,
        sandbox: args.sandbox,
    })
}

/// The command line's refusal, on its way out: clap's rendering — the sentence
/// **and its usage line** — to stderr, the same refusal as an `Error` frame on
/// stdout, then exit 2.
///
/// **Both streams are this file's and neither is clap's.** clap would print
/// the same text to stderr and exit with its own code, but the frame is the
/// half it cannot know about: a consumer of stdout waits for a frame, and a
/// startup refusal that arrived as an absence is a timeout where this is a
/// cause (decision 19). Reached for every failure the parse has — a typo, a
/// missing root, `--help` — so the command line has one refusal and not three.
///
/// Exit 2 is what the hand-rolled `usage()` exited with and what the harness
/// reads; the frame carries the same text as stderr, trimmed of the newline
/// the rendering ends with, so the two registers of one refusal agree.
fn refuse_command_line(err: Box<dyn Error + Send + Sync>) -> ! {
    eprint!("{err}");
    emit_error(err.to_string().trim_end());
    process::exit(2)
}

/// The refusal every arm but the command line's makes: one message said twice —
/// a line on stderr, and the `error` frame that goes with it — then exit 2.
///
/// **Both registers, because a consumer reads one and the operator reads the
/// other** (design decision 19): stdout is the protocol channel in every build,
/// feature or no feature, so a consumer that asked for a confinement hears why
/// it did not get one instead of waiting out a timeout. Exit 2 is the code
/// [`refuse_command_line`] exits with and the one the harness reads, so a
/// refusal is one thing however it was reached — the arms below say only *what*
/// was refused, not how to say it.
///
/// **The gate is the complement of the two arms', because they are the whole of
/// what calls it**: on the one configuration that has a backend — Linux, with
/// the feature — a `--sandbox` request is honoured and there is nothing to
/// refuse, so a helper compiled there would be a dead function and the lint
/// that says so is right. Stated as the negation of that configuration rather
/// than as the two arms' predicates, so it cannot drift from them.
#[cfg(not(all(feature = "sandbox", target_os = "linux")))]
fn refuse(message: &str) -> ! {
    eprintln!("{message}");
    emit_error(message);
    process::exit(2)
}

/// The write-lock snapshot of the fs storage into the metric layer's
/// plain-data form — the wiring-point conversion that keeps
/// `tinio-server`'s metrics decoupled from backend snapshot types.
fn write_lock_stats(snapshot: WriteLockSnapshot) -> WriteLockStats {
    WriteLockStats {
        wait_buckets: snapshot.wait_buckets,
        total_buckets: snapshot.total_buckets,
        count: snapshot.count,
        wait_sum_us: snapshot.wait_sum_us,
        wait_max_us: snapshot.wait_max_us,
        total_sum_us: snapshot.total_sum_us,
        total_max_us: snapshot.total_max_us,
    }
}

/// `[s3]` capability toggles and page-size caps from the parsed config
/// (FR-021); without an `[s3]` section the defaults apply. (The bool
/// toggles used to be dropped here — the plane was built with
/// `Capabilities::default()` even when a config set them.)
fn capabilities_for(config: &Option<Config>) -> Capabilities {
    match config.as_ref().and_then(|c| c.s3.as_ref()) {
        Some(s3) => Capabilities::from(s3),
        None => Capabilities::default(),
    }
}

/// The plane constructor selection (same-condition rule, Tasks 10-12):
/// feature `acl` compiled on AND `caps.acl` on → the identity-backed
/// plane (`new_with_acl` — ConfigAuth + AclAccess, every enforced
/// behavior wired together); else today's `new_with_auth` (no
/// identity, no enforced behavior). The identity is the single
/// `Identity::from_config` assembly (spec §6); the credential fallback
/// for the auth-only plane is the same `[auth]` pair that assembly uses.
fn plane_for(storage: FsStorage, plane: PlaneConfig, config: &Option<Config>) -> DataPlane {
    // An absent `--config` serves the default shape (the minioadmin pair
    // and the core owner element).
    #[cfg(feature = "acl")]
    if plane.caps.acl {
        return DataPlane::new_with_acl(
            storage,
            plane,
            Identity::from_config(config.as_ref().unwrap_or(&Config::default())),
        );
    }
    let (access_key, secret) =
        identity::root_auth_pair(config.as_ref().unwrap_or(&Config::default()));
    DataPlane::new_with_auth(storage, plane, &access_key, &secret)
}

/// The confinement request `serve` makes, built from the paths the operator
/// gave and the mode that was resolved for them.
///
/// **The exposure rule lives here and nowhere else** (design §9): the served
/// root carries the access the mode dictates, the state directory is always
/// read-write — the access log, the redb metadata and the staging areas live
/// there — and an in-root state directory needs no entry of its own, because
/// the served root's exposure already covers it. `Sandbox::expose` keeps the
/// most permissive access for a path given twice, so two entries for one path
/// could not fight even if the rule above ever let them meet.
///
/// **One name for the construction, because the device-file flag is an argument
/// rather than something a rebuilt set remembers.** With the call inline in
/// `main` and a test building its own `Sandbox`, flipping the flag to `false`
/// would leave every test green while the jail silently lost `/dev/null` and
/// friends — design decision 12's whole point, and invisible until something
/// reads a device file. The test is about the code that runs only if the test
/// calls the code that runs.
// The gate is the two places that read this: the Linux arm, and the tests
// below — which exercise it on every platform, and are what keeps the exposure
// rule a property this file can lose. A build with neither would otherwise
// carry a dead function, and every build compiles this file now that the
// example has no `required-features`.
#[cfg(any(test, all(feature = "sandbox", target_os = "linux")))]
fn sandbox_request(root: &Path, state_dir: &Path, access: AccessMode) -> Sandbox {
    let sandbox = Sandbox::new().expose(root, access);
    let sandbox = if state_dir.starts_with(root) {
        sandbox
    } else {
        sandbox.expose(state_dir, AccessMode::ReadWrite)
    };
    sandbox.device_files()
}

/// The one name for the construction of this file's confinement: the Linux
/// backend's options, with the environment's model and the two facts `enter`
/// reads carried onto them.
///
/// **Both facts are resolved here rather than at the call site, because
/// `enter` reads them off `Options` and neither default is right for a
/// server.** `enter` takes the exposure set from `served_root_access` and the
/// retained capability set from `(served_root_access, local_uid_configured)`;
/// with the defaults, a `--read-only` run handed the jail a **read-write**
/// served root — the kernel-level half of FR-023, absent with nothing to say
/// so — and `caps::retained` always returned `&[]`, so the single `CAP_CHOWN`
/// design §8 promises for an FR-033 `local_uid` mapping was never retained and
/// the first `chown` of a landed object failed `EPERM` with no capability to
/// fall back on. `access` is the same **resolved** value the exposure set uses
/// — the config file's `[server] read_only`, `TINIO_READ_ONLY` and the flag
/// already folded together — and the mapping is asked of
/// `identity::owner_uids_for`, the same function the storage's chown map comes
/// from, so the capability this keeps and the chowns it performs cannot
/// disagree about whether a mapping exists.
///
/// `base` rather than a fresh `Options` so the caller's `from_env` — the
/// model, and the refusal that names a bad `TINIO_SANDBOX_MODEL` — is not
/// re-derived or lost here.
#[cfg(all(feature = "sandbox", target_os = "linux"))]
fn sandbox_options(
    base: linux::Options,
    config: &Option<Config>,
    access: AccessMode,
) -> linux::Options {
    let mapping_configured =
        !identity::owner_uids_for(config.as_ref().unwrap_or(&Config::default())).is_empty();
    base.with_served_root_access(access)
        .with_local_uid(mapping_configured)
}

/// Write one event frame to stdout and flush it.
///
/// **The only write to stdout in this file** (design decision 19), and one
/// function rather than one per call site: the readiness frame below and the
/// `Error` frames a refusal sends both go through it, so "stdout carries events
/// and nothing else" is a property of one body instead of a rule to re-check.
///
/// **"In this file" is the whole of what a source scan can promise, and that
/// gap is worth stating rather than leaving to the check.** The scan this
/// contract was written against — a `println!`/`print!`/`stdout()` grep with a
/// negative lookbehind for the `e` of `eprintln!` — cannot see a dependency, and
/// the parser is one now: `clap` writes to stdout through its own code when it
/// is allowed to, so a green grep would have kept passing while the guarantee
/// was gone. What holds the parser's half is its configuration and three
/// functions this file never calls — `Error::print`, `Error::exit`,
/// `Command::print_help` — none of which a grep over this source can check.
/// `crates/tinio-server/tests/serve_read_only.rs` counts the child's **stdout
/// lines** after a refused command line and requires exactly the frame's three,
/// which is a fact about the stream rather than about anyone's source, and is
/// what would fail if a clap version ever printed.
///
/// Flushing is not decoration — stdout is line-buffered to a terminal and
/// block-buffered to a pipe, and a harness reads a pipe, so an unflushed frame
/// is a readiness hang rather than a missing event. A failed write is ignored on
/// purpose: it means the reader is gone, and there is nowhere left to report it.
fn emit(event: &Event) {
    let mut out = stdout().lock();
    let _ = out.write_all(event.encode().as_bytes());
    let _ = out.flush();
}

/// A refusal, on stdout, **before** its `Err` is returned — the sandbox arm's
/// own and, through [`emit_on_error`], every other step of `main` that can
/// refuse before the listener exists.
///
/// Decision 19's "a startup failure is an event rather than an absence", which
/// the arm did not honour: a consumer of this stream waits for a frame, and
/// without one it reports a timeout where the launcher platforms report the
/// cause. Both of them send this same frame through a helper of this same name.
///
/// **The frame and the returned `Err` are two registers of one refusal, not one
/// string.** The frame carries the error's `Display` — the operator-facing
/// sentence — while `main`'s `Err` is printed by `Termination` as `Error: {:?}`,
/// its `Debug` (`Error: Io { path: … }`, `Error: InvalidValue(Report { … })`). A
/// consumer reading stdout gets the sentence; the log and the exit path get the
/// structured form.
fn emit_error(message: &str) {
    emit(&Event::Error {
        message: message.to_string(),
    });
}

/// A fallible startup step, with the `Error` frame a consumer of stdout is
/// waiting for.
///
/// **This is the rest of decision 19's "a startup failure is an event rather
/// than an absence".** The sandbox arm's refusals already send one; every other
/// step of `main` on the way to the listener — a config that will not load, a
/// root that cannot be created, a state directory that cannot be resolved, a
/// storage that refuses the mode it was handed — returned through a bare `?`,
/// so the process exited with an empty stdout and a consumer that is waiting for
/// a frame reported a timeout where the launcher platforms report the cause.
///
/// The frame carries the `Error`'s `Display`, the operator-facing sentence; the
/// `Err` handed back is printed by `main`'s `Termination` as its `Debug`. Two
/// registers of one refusal, not the same string — see [`emit_error`].
///
/// **The serving call is deliberately not wrapped.** By then this process has
/// emitted `Listening` itself; an `Error` frame *after* it would be a statement
/// about the serving phase rather than a startup refusal, and that is the
/// shared contract's to make rather than this step's.
fn emit_on_error<T, E>(result: Result<T, E>) -> Result<T, Box<dyn Error + Send + Sync>>
where
    E: Into<Box<dyn Error + Send + Sync>>,
{
    result.map_err(|err| {
        let err = err.into();
        emit_error(&err.to_string());
        err
    })
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = match parse_args(env::args_os().skip(1)) {
        Ok(args) => args,
        // One arm for every bad command line there is: a flag nothing matches
        // (`--help` and `--version` among them), a missing root, a value clap
        // will not read. `refuse_command_line` is where the two streams and the
        // exit code are decided.
        Err(err) => refuse_command_line(err),
    };

    // A build without the feature refuses the request rather than ignoring it
    // (design decision 5): silently running unconfined is the failure this
    // whole design exists to prevent. **This arm is reachable, and it is the
    // example's own build that reaches it**: the example carries no
    // `required-features`, because a `--no-default-features` build must still
    // serve the harness and what the feature gates is only the `--sandbox`
    // arm's body (design decisions 4 and 19) — so `serve <root> --sandbox`
    // built without the feature lands here rather than failing to exist. **The
    // refusal is a frame as well as a line on stderr**: stdout is the protocol
    // channel in every build, feature or no feature (decision 19) — [`refuse`]
    // is where that is written down once.
    #[cfg(not(feature = "sandbox"))]
    if args.sandbox {
        refuse(
            "error: this build was compiled without the `sandbox` feature, so --sandbox cannot \
             be honoured; rebuild with the feature or drop the flag",
        );
    }

    // The other half of decision 5: a platform whose backend belongs to another
    // plan refuses the flag rather than accepting it and confining nothing. P2
    // and P3 replace this arm with theirs and keep the frame — the refusal is
    // the same statement on every platform, and so is the place a consumer reads
    // it.
    #[cfg(all(feature = "sandbox", not(target_os = "linux")))]
    if args.sandbox {
        refuse(
            "error: --sandbox has no backend on this platform yet; the Linux one is the one that \
             has landed",
        );
    }

    // The real dual-pipeline runtime (pipeline-spec.md §3.3): `--config
    // <file>` consumes the parsed `[pipeline]` section (F07 — the values
    // an operator sets are no longer silently dropped); without it the
    // section defaults apply (an absent `[pipeline]` resolves to the
    // defaults, Q8).
    let config = match &args.config_path {
        Some(path) => Some(emit_on_error(Config::load(path))?),
        None => None,
    };
    // FR-023 resolution, lowest precedence first: the config file's
    // `[server] read_only`, then the `TINIO_READ_ONLY` overlay when set,
    // then the flag. An absent `--config` contributes the default
    // (read-write). `.env` joins when the CLI brings the dotenvy wiring
    // (T066/T067).
    let file_read_only = config
        .as_ref()
        .is_some_and(|config| config.server.read_only);
    let read_only =
        emit_on_error(sources::env_read_only())?.unwrap_or(file_read_only) || args.read_only;
    let access = AccessMode::from(read_only);

    // The harness passes a fresh scratch path — create the root if missing
    // (FsStorage itself requires an existing directory). Read-only mode
    // serves a root that must already exist: creating it would write the
    // one thing the mode promises to leave alone. A missing root fails
    // during state-dir resolution — `default_home_state_dir` canonicalizes
    // it — or, with `TINIO_STATE_DIR` set, in `FsStorage::new`'s own
    // canonicalization.
    if !read_only {
        emit_on_error(fs::create_dir_all(&args.root))?;
    }
    // Spec §6 OS-mapping validation: any `[owner]`/`[[users]] local_uid`
    // mapping requires chown privilege at startup (root or CAP_CHOWN) —
    // fail closed with a config error (unix; Windows already rejected the
    // key at parse). Read-only mode skips the probe: it writes
    // `<root>/.tinio-chown-probe-<nonce>/probe` into the served root
    // (decision 14).
    #[cfg(unix)]
    if !read_only {
        emit_on_error(identity::ensure_chown_privilege(
            config.as_ref().unwrap_or(&Config::default()),
            Path::new(&args.root),
        ))?;
    }
    // Read-only mode relocates the tool's state out of the served root
    // (FR-023); normal mode leaves it at the backend's default
    // `<root>/.tinio/`. The diagnostic goes to stderr: stdout carries event
    // frames alone (`emit` is their only writer), and a diagnostic there
    // would be what a consumer reads as an event (design decision 19).
    let state_dir = if read_only {
        // `TINIO_STATE_DIR` wins over the derived layout: the read-only
        // deployments where there is no writable home are exactly the ones
        // that need the escape hatch.
        let dir = match emit_on_error(state::env_state_dir())? {
            Some(dir) => {
                eprintln!(
                    "read-only mode: state dir {} (from {})",
                    dir.display(),
                    state::STATE_DIR_VAR
                );
                dir
            }
            None => {
                let dir = emit_on_error(state::default_home_state_dir(Path::new(&args.root)))?;
                eprintln!("read-only mode: state dir {}", dir.display());
                dir
            }
        };
        Some(dir)
    } else {
        None
    };
    // **The state directory as a path, and it is asked for here rather than of
    // a `FsStorage`.** The two readers below need it before any storage can
    // exist — the exposure set names it, and the access log is opened inside it
    // — and no storage can exist yet for a reason that is not about paths: the
    // storage's lanes are multi-threaded runtimes, and `unshare(CLONE_NEWUSER)`
    // returns `EINVAL` in a multi-threaded process (design §10, verified). The
    // answer is the storage's own, from the function the storage itself calls:
    // `<root>/.tinio` under the canonical root, or the relocation resolved
    // above. The build sources still exist before the door closes — that is
    // what this line and the `create_dir_all` below are.
    let state_dir_path = match &state_dir {
        Some(dir) => dir.clone(),
        None => emit_on_error(default_state_dir(&emit_on_error(fs::canonicalize(
            &args.root,
        ))?))?,
    };
    // Operational logs to stderr (info), access log to `<state
    // dir>/access.log` (T052) — `<root>/.tinio/` in normal mode, the
    // relocated dir in read-only mode. The `[log]` config wiring lands
    // with the US2 CLI.
    emit_on_error(fs::create_dir_all(&state_dir_path))?;
    // **The log subscriber is installed here, one step earlier than the body
    // had it, and that is the whole reason the events this phase adds are
    // visible.** `confine` emits them — the jail's path, the model it resolved,
    // every mount, and an `error!` for whatever refused — and `tracing` drops
    // an event emitted while no subscriber exists. Left where it was, in the
    // moved body below, every one of them would go into a process with nowhere
    // to put them, and the failure an operator most needs a log for (a jail
    // that did not build) would be exactly the one with no line in it. Its two
    // neighbours move with it unchanged: the state directory has to exist
    // before the access log inside it is opened.
    let subscriber = emit_on_error(server_log::build_subscriber(
        Verbosity::Info,
        Format::Text,
        &AccessFormat::Combined,
        &state_dir_path.join("access.log"),
        None,
    ))?;
    emit_on_error(subscriber::set_global_default(subscriber))?;

    #[cfg(all(feature = "sandbox", target_os = "linux"))]
    if args.sandbox {
        // The **resolved** mode and not the configuration file's own field:
        // `TINIO_READ_ONLY` and `--read-only` are part of the chain that produced
        // `access` above, so reading `config.server.read_only` here would expose
        // the served root `ReadWrite` on a run the operator asked to be
        // read-only — losing the kernel-level half of FR-023 that design §9
        // promises, and doing it silently. `access` **is** the type the exposure
        // set takes, so it is handed over as it stands.
        let sandbox = sandbox_request(&args.root, &state_dir_path, access);

        // The model comes from the environment in P1 (`TINIO_SANDBOX_MODEL`);
        // the CLI carries it as a flag when it owns `serve` (design §1). It
        // reaches the backend through `Options`, which is why this arm names the
        // Linux crate directly instead of going through the facade's
        // `platform()` — the facade returns whatever backend the build targets,
        // and an options value only one of them has cannot be handed to it.
        // Each launcher platform's own arm does the same with its own options
        // type.
        // **Every refusal this arm produces on Linux goes to stdout as an
        // `Error` frame, in the same shape both launcher platforms use**
        // (decision 19's "a startup failure is an event rather than an
        // absence"). P1 had none of them until this pass, for a reason that
        // turned out to be wrong: Linux confines **in place**, so there is no
        // child whose frames a parent relays, and the `Error` frame read as the
        // launcher platforms' business. It is not theirs. A consumer of stdout
        // must not have to know which platform it is on — on a launcher a
        // failure arrives as a frame and on Linux it arrived as nothing at all,
        // which a harness reads as a timeout rather than as a cause. The two
        // refusals below are the ones this arm can produce here: the model
        // variable naming something that is not a model, and `confine` refusing
        // — the unavailable-model refusal among its reasons, which is the one an
        // operator is most likely to meet. (The `Supervise` arm's own `?` is the
        // third call that can fail, and it is unreachable on this platform by
        // construction.) Each also logs where its `Error` is built, so stderr
        // carries the same message through the subscriber installed above
        // (shared §7).
        // The two facts `enter` reads that the environment does not carry —
        // the resolved `access` and whether an FR-033 `local_uid` mapping is
        // configured — are folded in by [`sandbox_options`], which is also
        // where they are tested. Left at their defaults they would build a
        // confinement this run did not ask for; see that function.
        let options = match linux::Options::from_env() {
            Ok(options) => sandbox_options(options, &config, access),
            Err(err) => {
                emit_error(&err.to_string());
                return Err(err.into());
            }
        };
        let mut backend = linux::Backend::with_options(options);
        // A refusal here is a startup failure like the launcher platforms'
        // (`confine_or_launch` is the Windows spelling of the same call site):
        // the frame goes out before the `Error` does, because a caller that
        // returns without a frame leaves its consumer waiting for a `Listening`
        // that can never come.
        let confinement = match backend.confine(&sandbox, &LaunchPlan::new()) {
            Ok(confinement) => confinement,
            Err(err) => {
                emit_error(&err.to_string());
                return Err(err.into());
            }
        };
        match confinement {
            Confinement::InPlace => {
                // The one degradable step says so rather than passing quietly
                // (design §11). **Both statements are kept, because they are
                // not the same statement**: the backend emitted a `warn!` event
                // for the degradation through the subscriber installed above,
                // which is what puts it in the log, and this line is `serve`'s
                // own — read by whoever is watching a startup rather than a
                // log, written here because a library cannot assume a
                // subscriber exists. Every log goes to stderr; stdout is the
                // event channel below.
                if backend.seccomp() == Some(linux::Seccomp::SkippedUnsupportedArch) {
                    eprintln!(
                        "warning: this architecture has no seccomp table, so outbound \
                         connections are NOT blocked; the directory confinement still holds"
                    );
                }
            }
            Confinement::Supervise(child) => {
                // Unreachable on Linux by construction, kept so the call site is
                // platform-neutral; the launcher plans own its real behaviour.
                // The shape is theirs too: `supervise` borrows the child, and
                // the terminal read that follows is what tells a child which
                // never reported from one that is merely quiet (decision 19).
                let mut child = child;
                let status = supervise(&mut child, || ())?;
                if child.terminal().is_none() {
                    eprintln!("sandbox: the confined child exited without reporting readiness");
                }
                process::exit(status.code().unwrap_or(1));
            }
        }
    }

    // **No thread may exist above this line, and this is where that stops
    // being true**: `Pipelines::build` starts a multi-threaded runtime per lane
    // (two IO workers, one removal, one DB by default), and
    // `unshare(CLONE_NEWUSER)` refuses a multi-threaded caller with `EINVAL`.
    // So the lanes, the `FsStorage` whose options take them by handle, and the
    // runtime below all sit *after* the confinement rather than beside the
    // config they read — design §10's ordering, and the reason the state
    // directory was resolved as a path above.
    let pipeline_config = match &config {
        Some(config) => config.pipeline.clone().unwrap_or_default(),
        None => pipeline::Config::default(),
    };
    let pipelines = emit_on_error(Pipelines::build(&pipeline_config))?;
    let storage = emit_on_error(FsStorage::new(
        &args.root,
        FsOptions {
            follow_symlinks: false,
            state_dir,
            access,
            compact_threshold_percent: DEFAULT_COMPACT_THRESHOLD_PERCENT,
            meta_batch_size: DEFAULT_META_BATCH_SIZE,
            meta_batch_bytes: DEFAULT_META_BATCH_BYTES,
            io_pipeline: pipelines.io(),
            remove_pipeline: pipelines.remove(),
            db_pipeline: pipelines.db(),
            owner_uids: identity::owner_uids_for(config.as_ref().unwrap_or(&Config::default())),
        },
    ))?;

    // The two `None`s are Linux's half of decision 6 read back out of the
    // signature: this process bound its own listener rather than adopting a
    // child's, and nothing forwards a stop to a process that is not a child.
    // `serve` itself is the one fallible step here that is **not** wrapped: it
    // emits its own `Listening` frame, so a refusal after that point is the
    // serving phase's statement rather than a startup one (`emit_on_error`).
    emit_on_error(Builder::new_multi_thread().enable_all().build())?
        .block_on(serve(args, config, storage, access, pipelines, None, None))
}

/// **One signature across the three plans, and this plan owns it.**
///
/// `storage` is **not** optional, and an earlier version of this signature made
/// it so and had to be corrected: every caller that reaches `serve` serves from
/// a storage, so `None` had no meaning any platform could agree on — this plan
/// read it as a caller bug and the Windows plan read it as "the body builds
/// one", which is not the same function. Linux's `main` builds the storage
/// before the runtime exists (its state directory is one of the jail's
/// mounts), and a launcher's child builds it in `confined`; the launcher itself
/// never reaches `serve` at all.
///
/// **Two parameters carry what `main` has already resolved** rather than
/// letting the body resolve it a second time: `access` is FR-023's chain
/// (`[server] read_only` → `TINIO_READ_ONLY` → `--read-only`) folded into one
/// value, and `pipelines` are the lanes the storage was constructed with — the
/// same instances the metrics hook reads and the shutdown sequence drains, which
/// a second `Pipelines::build` here could not be.
///
/// The two optional ones are optional because the three platforms reach `serve`
/// differently, and each `None` is a fact about a platform rather than a policy
/// of `serve`: Linux serves in the process that confined itself, so it passes
/// `None` for both — there is no adopted listener, because this process binds
/// its own, and no stop channel, because nothing forwards a signal to something
/// that is not a child. A launcher's child passes `Some(listener)` and
/// `Some(stop)` instead.
async fn serve(
    args: Args,
    config: Option<Config>,
    mut storage: FsStorage,
    access: AccessMode,
    pipelines: Lanes,
    inherited: Option<TcpListener>,
    stop: Option<watch::Receiver<bool>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // The stop channel belongs to the launcher platforms: the child is what
    // reacts to a forwarded stop, so their `confined` creates the channel,
    // gives the sender to `on_stop` and hands the receiver here. Nothing
    // forwards a stop to a process that is not a child — Linux passes `None` —
    // so this phase holds it and does nothing with it, and taking it now is
    // what lets P2 and P3 add their accept-loop arm without touching this
    // signature.
    let _ = stop;

    // `[s3] max_concurrent_uploads` caps in-progress multipart uploads
    // (default 1000) — an authenticated client cannot accumulate an
    // unbounded number of uploads.
    if let Some(s3) = config.as_ref().and_then(|c| c.s3.as_ref()) {
        storage.set_max_concurrent_uploads(s3.max_concurrent_uploads);
    }
    // D-B: synchronous Startup repair — the fast, deterministic items
    // (tmp, staging residue, multipart orphans, stale bucket records)
    // run before readiness; the delete-tombstone stage routes through
    // the storage's removal lane (the `remove_pipeline` wired above).
    // Best-effort: a failed stage is warned and readiness proceeds — the
    // scanner covers the residue. The stale-bucket prune runs here,
    // pre-serving, so it stays lock-free (no request can race it yet).
    let cleanup = FsCleanup::new(
        &storage,
        CleanupOptions {
            access,
            ..Default::default()
        },
    );
    match cleanup.repair(RepairKind::Startup).await {
        Ok(mut actions) => {
            while let Some(action) = actions.next().await {
                if let Err(err) = action {
                    tracing::warn!(
                        error = %err,
                        "startup repair step failed; the scanner covers the residue"
                    );
                }
            }
        }
        Err(err) => tracing::warn!(
            error = %err,
            "startup repair failed; the scanner covers the residue"
        ),
    }

    // The launcher platforms' children take the listener their parent had
    // already bound; this process binds its own, which is the only arm that
    // exists in this phase.
    let listener = match inherited {
        Some(listener) => listener,
        None => TcpListener::bind(args.address).await?,
    };
    // stdout is a protocol channel; every log goes to stderr. `emit` above is
    // the only writer, which is what makes the contract cheap to hold (design
    // decision 19) — and this is the serving process's own frame, beside the
    // `Error` frames a refusal sent through the same call before it got here.
    // `local_addr()` is already a `SocketAddr`, so the port travels as a value
    // rather than as something re-parsed from text.
    emit(&Event::Listening {
        addr: listener.local_addr()?,
    });

    // Background scanner (FR-024; `TINIO_SCANNER=0` disables).
    let scanner_options = ScannerOptions {
        enabled: true,
        delay: Duration::from_secs(10),
        max_wait: Duration::from_secs(15),
        cycle: Duration::from_secs(24 * 3600),
        access,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let scanner = Scanner::new(storage.clone(), scanner_options);
    let scanner_task = tokio::spawn(async move {
        scanner.run(shutdown_rx).await;
    });

    // Async sweep (temp 24 h, multipart 7 d).
    let (sweep_tx, sweep_rx) = watch::channel(false);
    let sweeper = Sweeper::new(storage.clone(), Options::default());
    let sweeper_task = tokio::spawn(async move {
        sweeper.run(sweep_rx).await;
    });

    let caps = capabilities_for(&config);

    // The `/metrics` scrape endpoint (F10): the plane's hook refreshes
    // the pipeline gauges and the write-lock histograms on every scrape.
    let metrics_storage = storage.clone();
    let metrics_io = pipelines.io();
    let metrics_db = pipelines.db();
    let plane = plane_for(storage, PlaneConfig { caps, access }, &config).with_metrics(Arc::new(
        move || {
            metrics::refresh(
                metrics_io.stats(),
                metrics_db.stats(),
                write_lock_stats(metrics_storage.write_lock_stats()),
            );
        },
    ));
    plane.serve(listener, shutdown_tx.subscribe()).await?;
    // Stop the scanner and the sweeper BEFORE the pipelines: the watch
    // signals take effect at the next pass boundary, so awaiting the
    // handles guarantees no pass is mid-flight when the pipelines shut
    // down — a running pass would otherwise keep enqueueing after
    // shutdown and hit Err(ShutDown) on enqueue (a spurious "scanner
    // pass failed" warn at stop, data-path review 2026-08-29 finding 7).
    let _ = shutdown_tx.send(true);
    let _ = sweep_tx.send(true);
    let _ = scanner_task.await;
    let _ = sweeper_task.await;
    // R5 (pipeline-spec.md §3.5): the server has stopped accepting new
    // requests and the background tasks are done — shut the pipelines
    // down in order, IO first and the DB write pipeline last, so
    // in-flight list batches can drain.
    pipelines.shutdown();
    // Await the workers' exit — the in-flight tasks shutdown already
    // guaranteed have visibly completed (item 6c — observability, not
    // new shutdown semantics).
    pipelines.drain().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt as _;

    use super::*;

    #[test]
    fn capabilities_for_maps_s3_max_buckets() {
        // A regression back to `Capabilities::default()` here would make
        // this 10,000 and pass CI silently — this pins the wiring.
        let config = Config::parse("version = 1\n[s3]\nmax_buckets = 3").unwrap();
        assert_eq!(capabilities_for(&Some(config)).max_buckets, 3);
    }

    #[test]
    fn capabilities_for_defaults_without_s3_section() {
        assert_eq!(capabilities_for(&None), Capabilities::default());
    }

    #[test]
    fn exposures_carry_the_mode_and_skip_an_in_root_state_dir() {
        let root = Path::new("/srv/data");
        let inside = Path::new("/srv/data/.tinio");
        let outside = Path::new("/home/u/.tinio/roots/h");

        let sandbox = sandbox_request(root, inside, AccessMode::ReadWrite);
        let exposures = sandbox.exposures();
        assert_eq!(
            exposures.len(),
            1,
            "an in-root state dir is already covered by the root"
        );
        assert_eq!(exposures[0].host, root);
        assert_eq!(exposures[0].access, AccessMode::ReadWrite);

        let sandbox = sandbox_request(root, outside, AccessMode::ReadOnly);
        let exposures = sandbox.exposures();
        assert_eq!(
            exposures.len(),
            2,
            "an out-of-root state dir needs its own entry"
        );
        assert_eq!(exposures[0].access, AccessMode::ReadOnly);
        assert_eq!(
            exposures[1].access,
            AccessMode::ReadWrite,
            "the state dir is always writable"
        );
    }

    #[test]
    fn parse_args_recognises_the_sandbox_flag() {
        // The flag and the root path are both bare arguments, so a parser that
        // stops looking at position one would serve `--sandbox` as a directory
        // name — a failure that is invisible until someone reads their filesystem.
        let args = parse_args([OsString::from("/srv/data"), OsString::from("--sandbox")]).unwrap();
        assert_eq!(args.root, Path::new("/srv/data"));
        assert!(args.sandbox);

        let args = parse_args([OsString::from("/srv/data")]).unwrap();
        assert_eq!(args.root, Path::new("/srv/data"));
        assert!(!args.sandbox);

        // The flag is recognised wherever it appears, not only after the root.
        let args = parse_args([OsString::from("--sandbox"), OsString::from("/srv/data")]).unwrap();
        assert_eq!(args.root, Path::new("/srv/data"));
        assert!(args.sandbox);
    }

    #[test]
    fn a_missing_root_is_a_usage_error() {
        assert!(parse_args([OsString::from("--sandbox")]).is_err());
    }

    #[test]
    fn parse_args_resolves_the_address_the_server_binds() {
        // Three plans read `args.address`, so the resolution lives in one place
        // and is pinned there: `--address` over `--port` over 9000.
        let args = parse_args([OsString::from("/srv/data")]).unwrap();
        assert_eq!(args.address, SocketAddr::from(([127, 0, 0, 1], 9000)));

        let args = parse_args([
            OsString::from("/srv/data"),
            OsString::from("--port"),
            OsString::from("0"),
        ])
        .unwrap();
        assert_eq!(args.address, SocketAddr::from(([127, 0, 0, 1], 0)));

        let args = parse_args([
            OsString::from("/srv/data"),
            OsString::from("--port"),
            OsString::from("1234"),
            OsString::from("--address"),
            OsString::from("0.0.0.0:8080"),
        ])
        .unwrap();
        assert_eq!(
            args.address,
            SocketAddr::from(([0, 0, 0, 0], 8080)),
            "--address outranks --port"
        );
    }

    #[test]
    fn help_and_version_are_unknown_arguments() {
        // **The disabled flags are the stdout contract's cheapest half**: clap
        // prints the help and the version it generates to stdout, so this
        // command has none to print and a request for either is refused like
        // any other unknown argument. A parser that answered here would also be
        // one that chose its own stream and exit code.
        for flag in ["--help", "-h", "--version"] {
            assert!(
                parse_args([OsString::from("/srv/data"), OsString::from(flag)]).is_err(),
                "{flag} must be refused rather than answered"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_path_that_is_not_utf8_is_accepted() {
        // The reason `Args.root` is a `PathBuf` and not the `String` it used to
        // be: a path that is not valid UTF-8 is legal on Linux, and `--config`
        // names one just as a root does. Unix-gated because the bytes are how
        // Unix spells a path (Windows spells it in wide units).
        let root = OsString::from_vec(b"/srv/d\xffata".to_vec());
        let args = parse_args([root.clone()]).unwrap();
        assert_eq!(args.root, PathBuf::from(&root));

        let args = parse_args([
            OsString::from("/srv/data"),
            OsString::from("--config"),
            root.clone(),
        ])
        .unwrap();
        assert_eq!(args.config_path, Some(PathBuf::from(&root)));
    }

    #[test]
    fn the_sandbox_asks_for_the_device_files() {
        // **Through `sandbox_request`, which is what `main` calls.** The flag is
        // an argument of the construction rather than something the set
        // remembers, so a test that built its own `Sandbox` would stay green
        // while the shipped one lost its device files: flipping the argument to
        // `false` has to fail *this* test, and it does.
        let sandbox = sandbox_request(
            Path::new("/srv/data"),
            Path::new("/srv/data"),
            AccessMode::ReadWrite,
        );
        assert!(
            sandbox.device_files_enabled(),
            "the Linux jail needs /dev/null and friends (design decision 12)"
        );
        assert_eq!(sandbox.exposures().len(), 1);
    }

    /// **The two `Options` fields `enter` decides the confinement from, and the
    /// test the phase was missing.** Nine reviews passed over the shipped
    /// `linux::Options::from_env()` — which sets neither — because every test
    /// in this file pinned the `Sandbox` and nothing pinned the options: the
    /// device-file flag is a fact about the request, the mode and the
    /// `local_uid` mapping are facts about what the process is allowed to do
    /// once it is inside. Both assertions below are the production call, not a
    /// rebuilt `Options`, for the same reason `sandbox_request` is called
    /// rather than a hand-built `Sandbox`: flipping either argument has to fail
    /// *this* test.
    #[cfg(all(feature = "sandbox", target_os = "linux"))]
    #[test]
    fn the_sandbox_options_carry_the_resolved_mode_and_the_uid_mapping() {
        // Read-only with a mapping: the run that needs both, and the pair whose
        // defaults are wrong in opposite directions — a read-write exposure
        // where FR-023 asked for a read-only one, and no `CAP_CHOWN` where
        // design §8 promises one.
        let config =
            Config::parse("version = 1\n[owner]\nlocal_uid = 1005").expect("a config with a map");
        let options = sandbox_options(
            linux::Options::default(),
            &Some(config),
            AccessMode::ReadOnly,
        );
        assert_eq!(
            options.served_root_access(),
            AccessMode::ReadOnly,
            "a `--read-only` run must expose the served root read-only inside the jail"
        );
        assert!(
            options.local_uid_configured(),
            "a configured `local_uid` mapping must retain CAP_CHOWN (design §8)"
        );

        // And the no-mapping default, so the assertion above is about the
        // config rather than about the setter being stuck on.
        let options = sandbox_options(linux::Options::default(), &None, AccessMode::ReadWrite);
        assert_eq!(options.served_root_access(), AccessMode::ReadWrite);
        assert!(!options.local_uid_configured());
    }

    #[test]
    fn the_executable_directory_is_not_exposed_on_linux() {
        // Linux confines the process in place, so its image is already mapped and
        // needs no exposure (design decision 22). Read off the request `main`
        // builds, for the same reason the flag is: an exposure only the tests
        // add is an exposure the server does not have.
        let exe_dir = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let sandbox = sandbox_request(
            Path::new("/srv/data"),
            Path::new("/srv/data"),
            AccessMode::ReadWrite,
        );
        assert!(!sandbox.exposures().iter().any(|e| e.host == exe_dir));
    }
}
