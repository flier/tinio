//! Minimal serve binary for the interop harness (task T032).
//!
//! Serves one storage root over the S3 data plane: `serve <root>
//! [--port N] [--address HOST:PORT] [--config <config.toml>] [--read-only]
//! [--sandbox]`. **Stdout carries event frames and nothing else**, and
//! [`events::emit`] below is their only writer: one `event: listening` frame
//! whose `data` names the bound address once the plane serves (the harness
//! parses it, needed for `--port 0`), or one `event: error` frame when startup
//! refuses — so a consumer decodes SSE events rather than reading a line,
//! and a stream that ends with no frame at all is a refusal it reports
//! rather than a hang it waits on. Every log, diagnostics included, goes to
//! stderr. **The parser is `clap`'s, and it is configured to have no route to
//! stdout at all**: `--help` and `--version` are disabled, so a request for
//! either stands beside every other unknown argument — clap's error text on
//! stderr, the `error` frame this contract owes, exit 2 (`args::RawArgs`).
//! **That half of the contract lives in a dependency and no scan of this file
//! can reach it** — see [`events::emit`] for what the source check does and
//! does not prove. The scanner is on by default (config defaults) and toggled
//! by `TINIO_SCANNER` (`0`/`1`); the sweep runs with default TTLs. Superseded
//! by the full CLI in US2.
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
//!
//! # Layout
//!
//! The example is a directory target and this file is its entry point: `main`
//! reads as design §10's own step list, and each step's *body* lives in the
//! module that owns it.
//!
//! - [`args`] — design §10 step 2: the command line, and this process's own
//!   argv with the launcher's private element filtered out.
//! - [`events`] — decision 19's stdout contract: the one frame writer, and the
//!   two helpers a refusal goes through.
//! - [`read_only`] — FR-023's state relocation, the one part of the mode that
//!   is not a precedence chain.
//! - [`sandbox`] — design §10 steps 6 and 7: the exposure rule, the
//!   containment guard, both platforms' confinement paths, the confined child,
//!   and decision 5's refusals.
//! - [`serve`] — design §10 step 8: the storage, the lanes and the serving
//!   body, reached by `main` and by the confined child alike.
//!
//! **The split is along the sequence rather than along the types**, because
//! that is what the audit of this file could not read: `main` carried the two
//! platform arms inline, so the step the whole design is about was the one
//! step a reader had to reconstruct from `#[cfg]`s. What stays here is the
//! sequence and the two facts it is written in — where the document is parsed,
//! and which process serves — and no arm.

mod args;
mod events;
mod read_only;
mod sandbox;
mod serve;

// The launch payload arrives on the confined child's **stdin**, and the two
// names the discriminator reads — its argv and that stdin — carry the same gate:
// an unconditional import would be an unused one in every other build, and the
// `--no-default-features` leg builds this example by name (`docs/tests.md`: the
// example deliberately has no `required-features`), so the warning would be a
// gate failure rather than a comment nobody reads.
#[cfg(all(feature = "sandbox", target_os = "windows"))]
use std::{env, io::stdin};
use std::{error::Error, fs, path::Path};

use tinio_config::{
    Config,
    log::{AccessFormat, Format, Verbosity},
    server::probe_server_keys,
    sources,
};
use tinio_core::mode::AccessMode;
use tinio_fs::state_dir as default_state_dir;
// The chown probe is the unix half of spec §6's OS-mapping validation.
#[cfg(unix)]
use tinio_server::identity;
use tinio_server::log as server_log;
use tokio::runtime::Builder;
use tracing::subscriber;

// **The launcher's frame, and the one name this file imports for a single
// configuration.** `emit_error` is called once, in the Windows `--sandbox` arm
// below, so that arm's cfg is this line's: every other configuration leaves the
// import unused, which `-D warnings` fails the build over. Those are exactly the
// legs the arm is not compiled on — a build without the feature, and a target
// that is not Windows — and each of them reaches this same name by another
// route: a build without the feature and a platform with no backend at all
// refuse through `confinement::refuse`, which owns its own import in
// `sandbox.rs`, while Linux, which confines **in place**, returns its refusals
// to `main`'s `emit_on_error` wrap around `confine_in_place`. The `no-default`
// leg is one of them (`docs/tests.md`: the example must build in every feature
// configuration), so this is a gate failure rather than a comment nobody
// reads. Written as the arm's own predicate rather than as its complement, so a
// second caller outside the arm widens this line in the same edit.
//
// **`emit_on_error` deliberately stays in the grouped `use` below, because the
// two names do not share a configuration**: it wraps every startup step of this
// function on every platform and in every feature build, and gating it with
// `emit_error` would break every leg that block serves. `sandbox.rs` splits the
// same pair the other way round — its `refuse` needs `emit_error` on exactly the
// configurations this arm is absent from — which is why neither file imports the
// name unconditionally any more.
#[cfg(all(feature = "sandbox", target_os = "windows"))]
use crate::events::emit_error;
use crate::{
    args::{own_args, parse_args, refuse_command_line},
    events::emit_on_error,
    read_only::resolve_read_only_state_dir,
    sandbox as confinement,
    serve::{build_storage, serve},
};

/// Parse the configuration bytes, or `None` when the operator gave none.
///
/// **The bytes are read in `main` and parsed here, and the split is decision 18's**
/// (design §3 step 3): the launcher reads the document and forwards it unparsed,
/// while the processes that stay in the process — the plain run and the confined
/// child — turn the same bytes into the document they serve from. An empty slice
/// is "no `--config`", which is the absent-section default both callers would
/// otherwise have to spell.
fn parse_config(bytes: &[u8]) -> Result<Option<Config>, Box<dyn Error + Send + Sync>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    Ok(Some(Config::parse(str::from_utf8(bytes)?)?))
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Design §10 step 1, before anything else, so a confined child does no
    // privileged work on its way in — no config read, no root creation, no
    // subscriber. The payload is the only discriminator: an environment marker
    // would be forgeable, and a forged marker is a bypass of the whole feature
    // (decision 17).
    //
    // **`target_os` as well as the feature, because [`confinement::confined`] is
    // this platform's** and the two facade names it calls are `#[cfg(target_os =
    // "windows")]` too. On Linux `adopt` would answer `None` here in any case —
    // that backend confines in place and never re-invokes itself — but the call
    // has to compile, and a feature-only gate would reference a function that does
    // not exist there.
    #[cfg(all(feature = "sandbox", target_os = "windows"))]
    if let Some(adopted) = tinio_sandbox::adopt(env::args_os(), stdin().lock())? {
        return confinement::confined(adopted);
    }

    // Design §10 step 2.
    let args = match parse_args(own_args()) {
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
    // channel in every build, feature or no feature (decision 19) — `refuse`
    // is where that is written down once.
    #[cfg(not(feature = "sandbox"))]
    if args.sandbox {
        confinement::refuse(
            "error: this build was compiled without the `sandbox` feature, so --sandbox cannot \
             be honoured; rebuild with the feature or drop the flag",
        );
    }

    // The other half of decision 5: a platform whose backend belongs to another
    // plan refuses the flag rather than accepting it and confining nothing. P3
    // replaces this arm with its own and keeps the frame — the refusal is the
    // same statement on every platform, and so is the place a consumer reads it.
    // **The pair of platforms below is the same predicate `refuse` and
    // `sandbox_request` carry**, and the three lines have to move together: a
    // platform that has an arm and is still listed here never reaches it, because
    // this refusal exits before the arm does.
    #[cfg(all(
        feature = "sandbox",
        not(any(target_os = "linux", target_os = "windows"))
    ))]
    if args.sandbox {
        confinement::refuse(
            "error: --sandbox has no backend on this platform yet; the Linux and Windows ones are \
             the ones that have landed",
        );
    }

    // The real dual-pipeline runtime (pipeline-spec.md §3.3): `--config
    // <file>` consumes the parsed `[pipeline]` section (F07 — the values
    // an operator sets are no longer silently dropped); without it the
    // section defaults apply (an absent `[pipeline]` resolves to the
    // defaults, Q8).
    //
    // **The file is read as bytes and parsed only where the parse stays in the
    // process** (decision 18). This process is not necessarily the one that
    // serves: a launcher hands the same bytes to its child inside the payload,
    // re-reads nothing, and must not retain a `Config` of its own — the document
    // holds credentials, and "no retained parse" is the half of decision 18 that
    // is about secrets rather than about the confinement.
    let config_bytes = match &args.config_path {
        Some(path) => emit_on_error(fs::read(path))?,
        None => Vec::new(),
    };
    // **The two bits this process resolves before it knows whether it is the
    // process that serves, read by one probe.** FR-023's mode is the first, and
    // decision 5's sandbox channel is the second: no phase implements the key or
    // the environment variable behind it, so a run that sets either must refuse
    // to start rather than proceed unconfined (§11). One parse of one document
    // answers both, which is the whole reason the probe reads two keys — a
    // second function parsing the same bytes is a second mirror of the schema
    // with a second chance to drift from it (`tinio_config::server`).
    //
    // The document's contribution is this **targeted probe and not a parsed
    // `Config`**, for the reason above: this line runs before the process knows
    // whether it is a launcher, so the bits have to be resolvable without a
    // document — and the probe answers exactly them, discarding everything else
    // by serde rather than by a wipe.
    let keys = emit_on_error(probe_server_keys(&config_bytes))?;
    // FR-023 resolution, lowest precedence first: the config file's
    // `[server] read_only`, then the `TINIO_READ_ONLY` overlay when set,
    // then the flag. An absent `--config` contributes the default
    // (read-write). `.env` joins when the CLI brings the dotenvy wiring
    // (T066/T067).
    //
    // **The overlay is tested rather than `unwrap_or`'d, and it stays that way
    // although the probe above now runs unconditionally**: the probe is what
    // answers decision 5's channel below, so it is not the overlay that decides
    // whether the document is read — it is the *precedence* the match spells
    // out, the overlay outranking the file exactly as the flag outranks the
    // overlay.
    //
    // **The price of the second key is paid here, in the fail-closed
    // direction.** A document that will not parse used to be read only when
    // `TINIO_READ_ONLY` had not answered, so a launcher with the overlay set
    // forwarded bytes it never looked at and let its child discover the error —
    // a process, a confinement and an ACL pass later. It is now a startup error
    // in the process that decides, before the root is created or anything is
    // granted, which is where design §11 puts every refusal this step can make.
    let read_only = match emit_on_error(sources::env_read_only())? {
        Some(from_env) => from_env,
        None => keys.read_only,
    } || args.read_only;
    let access = AccessMode::from(read_only);
    // Decision 5's other half, and it is a refusal rather than a resolution: a
    // channel this design names but no phase implements must not be silently
    // ignored, because the operator who wrote it believes they are confined
    // (§11). **Every platform and every feature configuration reaches this
    // line** — the check sits in the sequence rather than behind an arm, so a
    // build whose backends are stubs refuses the channel exactly as a build with
    // both arms does, and the refusal happens before the root is created, the
    // state directory resolved or anything confined. `TINIO_SANDBOX=0` and
    // `[server] sandbox = false` are the operator saying no, and proceed.
    //
    // The variable is read **strictly** (`0`/`1`/`true`/`false`), so a typo is a
    // startup error naming `TINIO_SANDBOX` rather than a value that fell back to
    // "off" — the strictness `TINIO_READ_ONLY` already has, and decision 5's
    // version of the same reason (a typo must not serve unconfined).
    let sandbox_env = emit_on_error(sources::env_sandbox())?.unwrap_or(false);
    emit_on_error(confinement::refuse_unhonoured_channels(
        keys.sandbox,
        sandbox_env,
    ))?;
    // The serving processes parse the bytes into the document they run on: unix
    // here, because the chown probe below and the confinement's own options both
    // need it, and Windows after its launcher's arm — which returns out of `main`
    // before this line, so a launcher never builds one (decision 18).
    #[cfg(unix)]
    let config = emit_on_error(parse_config(&config_bytes))?;

    // Design §10 step 5. The harness passes a fresh scratch path — create the
    // root if missing (FsStorage itself requires an existing directory).
    // Read-only mode serves a root that must already exist: creating it would
    // write the one thing the mode promises to leave alone. A missing root
    // fails during state-dir resolution — `default_home_state_dir`
    // canonicalizes it — or, with `TINIO_STATE_DIR` set, in `FsStorage::new`'s
    // own canonicalization.
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
    let state_dir =
        resolve_read_only_state_dir(Path::new(&args.root), read_only, |dir, from| match from {
            Some(var) => eprintln!("read-only mode: state dir {} (from {var})", dir.display()),
            None => eprintln!("read-only mode: state dir {}", dir.display()),
        })?;
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
    // an event emitted while no subscriber exists. Left where it was, in
    // `serve` below, every one of them would go into a process with nowhere
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

    // ── Design §10 steps 6 and 7: the confinement, or the launch ────────────
    //
    // **One step, two dispatch points, and the reason is where the document is
    // parsed.** A launcher must not hold a `Config` at all (decision 18), so its
    // arm runs *above* the parse and hands the child the bytes; a platform that
    // confines in place builds its confinement *from* the document, so its arm
    // runs *below* it. The order below is therefore Windows first, then the
    // plain run's own parse, then Linux — and exactly one arm per target
    // compiles, so what a reader of either build sees is the sequence with one
    // confinement step in it.
    //
    // Windows: bind, grant, launch, supervise (design §3 steps 3–9). The
    // `Listening` frame is the child's; the only frame this arm emits itself is
    // the one that turns "the child never reported" from an absence into an event
    // (decision 19).
    //
    // **The arm sits below the subscriber, and that placement is load-bearing.**
    // This step's plan installed a `try_init` sink of its own, because it expected
    // the subscriber to be built inside `serve`'s body — which a launcher
    // never reaches. P1 moved that subscriber up into `main` for the Linux arm's
    // benefit, so by the time this arm runs the process *has* a sink: shared §7's
    // events (the profile, the resolved exposures, `CreateProcessW` and its pid,
    // the job) reach the same stderr the plain run logs to, and there is no second
    // subscriber to keep in step. A `try_init` here would be a second answer to
    // "where does this process log", and `INFO` on stderr is already this
    // example's.
    //
    // **The failure is framed here rather than inside the arm, and the reason is
    // that a `return` skips `main`'s own wrapping.** Every other fallible step in
    // this function is called through `emit_on_error`, but this one *is* the
    // return value, so an error from it — design §9's in-root state directory is
    // the one a confined run can reach — arrived on stderr and nowhere else. This
    // is the same rule the plain run's startup failures already follow: a startup
    // failure is an event and not an absence (decision 19), so a consumer that
    // decodes the stream hears about it instead of waiting out a readiness
    // timeout. The `Error` frame and the exit status are the same two signals the
    // `confine_or_launch` arm below emits at its own call site.
    #[cfg(all(feature = "sandbox", target_os = "windows"))]
    if args.sandbox {
        return match confinement::launch(&args, &config_bytes, read_only, access, &state_dir) {
            Ok(()) => Ok(()),
            Err(err) => {
                emit_error(&err.to_string());
                Err(err)
            }
        };
    }
    // The document, for the process that is left once a launcher's arm has had
    // its chance to divert: Windows' plain run parses here, and the child parses
    // the payload's text in `confined` (decision 18).
    #[cfg(windows)]
    let config = emit_on_error(parse_config(&config_bytes))?;

    // **Linux's arm confines *this* process and comes back** — it binds its own
    // listener afterwards, serves on it, and emits its own `Listening` event
    // (design decisions 6 and 19) — so it is a call rather than a `return`, and
    // the step below is the same storage the plain run builds. What reaches
    // here with the flag unset is the plain run, on every platform.
    //
    // **The document is the confinement's input here**, which is why this arm is
    // below the parse and the launcher's is above it: the Linux backend's
    // options carry the FR-033 `local_uid` mapping, and that is a fact about the
    // document (design §10 step 6).
    // The same framing the launcher's arm does at its call site, and for the same
    // reason: `confine_in_place`'s failures are startup failures, so they are
    // events rather than a line on stderr that a stream consumer would wait out.
    #[cfg(all(feature = "sandbox", target_os = "linux"))]
    if args.sandbox {
        emit_on_error(confinement::confine_in_place(
            &args,
            &config,
            &state_dir_path,
            access,
        ))?;
    }

    // ── Design §10 step 8: the storage, and the body that serves from it ────
    //
    // **No thread may exist above this line, and this is where that stops
    // being true**: `Pipelines::build` starts a multi-threaded runtime per lane
    // (two IO workers, one removal, one DB by default), and
    // `unshare(CLONE_NEWUSER)` refuses a multi-threaded caller with `EINVAL`.
    // So the lanes, the `FsStorage` whose options take them by handle, and the
    // runtime below all sit *after* the confinement rather than beside the
    // config they read — design §10's ordering, and the reason the state
    // directory was resolved as a path above.
    let (storage, pipelines) = build_storage(&args.root, &config, state_dir, access)?;

    // The two `None`s are Linux's half of decision 6 read back out of the
    // signature: this process bound its own listener rather than adopting a
    // child's, and nothing forwards a stop to a process that is not a child.
    // `serve` itself is the one fallible step here that is **not** wrapped: it
    // emits its own `Listening` frame, so a refusal after that point is the
    // serving phase's statement rather than a startup one (`emit_on_error`).
    emit_on_error(Builder::new_multi_thread().enable_all().build())?
        .block_on(serve(args, config, storage, access, pipelines, None, None))
}
