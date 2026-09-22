//! The `--sandbox` seam: three channels, one decision, every refusal named (design §7,
//! decision 10).
//!
//! What this module does *not* do is confine anything: `tinio-sandbox-linux` and its two
//! siblings own that, and until they land every platform refuses here rather than serving
//! without the confinement the operator asked for (sandbox design decision 5). The three
//! channels are the `--sandbox` flag, `TINIO_SANDBOX` and `[server] sandbox`, and they collapse
//! to one answer in [`plan`], which is also where every *static* refusal is raised — before the
//! fork, so the operator's shell sees it.
//!
//! **This module is compiled in every build**, because the feature gates behaviour and not
//! linkage: `TINIO_SANDBOX` and `[server] sandbox` cannot be compiled away, and code that does
//! not exist cannot refuse. The feature takes the `--sandbox` *flag* and nothing else: the
//! variable keeps an argument of its own in that build (hidden, `start::Args::sandbox`'s
//! feature-off declaration), so the channel still reaches this module and still refuses.

use std::{
    env::consts::OS,
    ffi::OsString,
    io::{self, Read},
};

// Tokio's listener is the one this module's public surface names (`adopted_listener` hands the
// child's socket back to the serving half), and the standard library's is the one `confine`
// binds — a launcher binds before any runtime exists, which is §1's step i. Two types, one
// name each, and the alias is the honest spelling of "the std one".
use clap::parser::ValueSource;
use tokio::net::TcpListener;

use crate::{
    _sandbox::{Adopted, Child, adopt, is_listener_fd_arg},
    Error,
    cli::Provenance,
    commands::start::Args,
    resolve::{Resolution, pick},
};

/// Whether this platform's backend is still a stub.
///
/// **A table here rather than a call into the backend**, because the backend cannot answer it
/// where the answer is needed: `Backend` has one method, `confine`, and every platform answers
/// `Error::Unsupported` from inside it — while [`plan`] runs *before* `confine` by design (§1
/// step f before step i) and on Linux no `confine` call happens at all. P1 (`tinio-sandbox-linux`),
/// P2 (`-macos`) and P3 (`-windows`) are **still stubs**, so this is `true` for every target and
/// a confinement request is refused with the platform named. A plan that lands a real backend
/// flips its own arm — the mixed state is the reason this is a per-target match rather than one
/// boolean.
///
/// **Linux is the one target whose flip is also guarded by a unit leg**
/// (`the_linux_arm_is_a_stub_while_no_confine_call_exists_on_this_target`, below): it is the only
/// target with no `confine` call to reach — `commands::start::run`'s arm is macOS/Windows — so
/// flipping that arm alone would let a Linux run start normally and serve unconfined. The other
/// two targets have the call and are covered by the refusal legs instead.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::sandbox::BACKEND_IS_STUB;
///
/// // Every target's backend is a stub today, so a confinement request is refused with the
/// // platform named. This assertion is what makes the flip visible when P1, P2 or P3 lands: a
/// // per-target arm that says `false` fails it, and the failure names the constant to update.
/// assert!(BACKEND_IS_STUB);
/// ```
pub const BACKEND_IS_STUB: bool = {
    #[cfg(target_os = "linux")]
    {
        true
    }
    #[cfg(target_os = "macos")]
    {
        true
    }
    #[cfg(target_os = "windows")]
    {
        true
    }
};

/// Which of the three channels asked for the confinement (design decision 10).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::sandbox::Channel;
///
/// // A refusal names the channel it came from, which is why the channel is a value and not a
/// // boolean.
/// assert!(matches!(Channel::Env, Channel::Env));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    /// `--sandbox`.
    Flag,
    /// `TINIO_SANDBOX`.
    Env,
    /// `TINIO_SANDBOX` as an `.env` file supplied it.
    DotEnv,
    /// `[server] sandbox`.
    Config,
}

impl Channel {
    /// The channel as a refusal names it — the spelling the operator used, which is the first
    /// half of every message this module raises.
    ///
    /// **`DotEnv` cannot be produced today, and that is a fact about the environment rather than
    /// an omission**: `dotenvy` writes the file's values into the process environment, so by the
    /// time [`plan`] reads `TINIO_SANDBOX` a value from `.env` and one exported by the operator
    /// are the same answer, and the refusal names the variable — which is what the operator has
    /// to go and look at either way. The variant carries the distinction for a caller that can
    /// still tell the two apart (one that reads the file itself), so no such caller has to invent
    /// a second spelling of "the variable".
    fn name(self) -> &'static str {
        match self {
            Self::Flag => "`--sandbox`",
            Self::Env => "`TINIO_SANDBOX`",
            Self::DotEnv => "`TINIO_SANDBOX` (from `.env`)",
            Self::Config => "`[server] sandbox`",
        }
    }
}

/// What the three channels resolved to.
///
/// The exposure set, device-file grant and OTLP trade land with the platform backend. Every
/// backend is still a stub, and [`plan`] refuses a request before any of that is built, so a
/// `Request` that this build returns is a request that was not made.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::sandbox::Request;
///
/// // No channel set: nothing is asked for.
/// let request = Request {
///     enabled: false,
///     channel: None,
/// };
/// assert!(!request.enabled);
/// ```
#[derive(Debug)]
pub struct Request {
    /// Whether a confinement was asked for at all.
    pub enabled: bool,
    /// The channel that asked, when one did.
    pub channel: Option<Channel>,
}

/// Reads the launch payload out of `argv`, or decides this process is not a confined child.
///
/// The element's spelling, its predicate and the adoption are all the **facade's**:
/// `LISTENER_FD_PREFIX` and `is_listener_fd_arg` are what name and recognise it, and
/// `adopt(args, stdin)` is what reads the payload off the reader — there is no second
/// `LISTENER_PREFIX` constant here and no hand-rolled scan of the prefix, because a second
/// spelling is exactly how a launcher and its child drift apart.
///
/// # Examples
///
/// ```rust
/// use std::ffi::OsString;
///
/// use tinio_cli::sandbox::filter_argv;
///
/// // No listener element: this process is the launcher, and its stdin is never touched.
/// let argv = vec![OsString::from("server"), OsString::from("/srv/data")];
/// let (argv, adopted) = filter_argv(argv).expect("argv");
/// assert_eq!(argv.len(), 2);
/// assert!(adopted.is_none(), "no launcher put an element in this argv");
/// ```
pub fn filter_argv(argv: Vec<OsString>) -> Result<(Vec<OsString>, Option<Adopted>), Error> {
    adopt_argv(argv, io::stdin())
}

/// [`filter_argv`]'s body with the reader as a parameter — the seam a unit test drives.
///
/// **The reader is a parameter rather than the process's stdin, and that is a rule rather than
/// a convenience.** `adopt` reads `stdin` to end-of-stream whenever the listener element is in
/// argv, so a test that drove [`filter_argv`] with the element present would read whatever this
/// test binary inherited — a terminal, where it blocks until someone types an EOF. Nothing else
/// changes: the public entry point passes `io::stdin()`, which `adopt` leaves alone whenever the
/// element is absent (the launcher's ordinary case).
fn adopt_argv(
    argv: Vec<OsString>,
    stdin: impl Read,
) -> Result<(Vec<OsString>, Option<Adopted>), Error> {
    // `adopt` reads the element out of the original list — it applies `is_listener_fd_arg`
    // itself, and touches stdin only when the element is present — so it is called **before**
    // the element is dropped from the argv clap will see.
    let adopted = adopt(argv.iter().cloned(), stdin)?;
    let filtered = argv
        .into_iter()
        .filter(|arg| !is_listener_fd_arg(arg))
        .collect();
    Ok((filtered, adopted))
}

/// The sandbox channels, collapsed with FR-016's precedence: the flag and the variable (one
/// clap answer, `.env` included — the parse is two-pass), then the config file.
///
/// `source` is that answer's [`ValueSource`], and it is load-bearing rather than decorative:
/// `TINIO_SANDBOX=0` with `[server] sandbox = true` resolves **off**, which a plain `||` over a
/// `bool` could not express. Only clap's own default leaves the file's key in place.
///
/// # Examples
///
/// ```rust
/// use clap::parser::ValueSource;
/// use tinio_cli::sandbox::resolve_flag;
///
/// assert!(
///     resolve_flag(ValueSource::CommandLine, true, false),
///     "the flag wins"
/// );
/// assert!(
///     resolve_flag(ValueSource::EnvVariable, true, false),
///     "the variable answers next"
/// );
/// assert!(
///     resolve_flag(ValueSource::DefaultValue, false, true),
///     "the config file is the last level"
/// );
/// assert!(
///     !resolve_flag(ValueSource::EnvVariable, false, true),
///     "an explicit `false` wins over the file"
/// );
/// ```
pub fn resolve_flag(source: ValueSource, requested: bool, file: bool) -> bool {
    pick(source, Some(requested), Some(file)).unwrap_or(false)
}

/// The three **static** determinations, as §1 step f runs them — before the daemon fork, so
/// every refusal reaches the operator's shell.
///
/// No channel set answers "disabled" with an empty exposure set, and a channel that **is** set
/// is resolved in one place and then measured against the two facts this build cannot change:
///
/// - **the feature.** With `sandbox` off, `--sandbox` is not in the parser at all — so the
///   answer can only have come from `TINIO_SANDBOX` or `[server] sandbox`, and either is a
///   **usage** error (exit 2) naming the build: a stripped capability refuses the request rather
///   than silently ignoring it, because silently running unconfined is the failure the whole
///   feature exists to prevent (design decision 5).
/// - **the platform's backend.** With the feature on, [`BACKEND_IS_STUB`] is the only place that
///   records whether the backend for *this* target can confine anything, and while it can not,
///   the answer is a **startup** error (exit 1) naming `std::env::consts::OS`. It is raised
///   here rather than read off `Backend::confine`'s answer because that call cannot be asked:
///   `Backend` has one method and every platform says `Error::Unsupported` from inside it, while
///   on Linux the launcher arm is `#[cfg(any(target_os = "macos", target_os = "windows"))]` and
///   no `confine` call happens at all. The determination this runs *before* the fork is the same
///   one on every target, which is what makes it reachable and observable in every build.
///
/// Only a build that has both is allowed to build the [`Request`] — the exposure set at the
/// mode's access, plus the platform's own extras.
///
/// # Errors
///
/// A usage error (2) for a channel this build cannot honour, a startup error (1) for a platform
/// whose backend is still a stub, and whatever reading the platform's own extras needs.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::start::Args,
///     resolve::{Resolution, locate, server},
///     sandbox::plan,
/// };
///
/// # fn main() -> Result<(), tinio_cli::Error> {
/// let discovery = Discovery {
///     dir: Some("/srv/data".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args {
///     dir: Some("/srv/data".into()),
///     ..Args::default()
/// };
/// let resolution: Resolution = server(located, &args, &Provenance::default())?;
/// let request = plan(&resolution, &args, &Provenance::default())?;
/// assert!(!request.enabled);
/// # Ok(())
/// # }
/// ```
pub fn plan(
    resolution: &Resolution,
    args: &Args,
    provenance: &Provenance,
) -> Result<Request, Error> {
    // FR-016's precedence, and nothing else rides on it. The flag and `TINIO_SANDBOX` are one
    // clap answer — the switch carries its variable through `env`, and a value `.env` supplied
    // arrives on the authoritative parse as an ordinary `EnvVariable` answer, which is what
    // makes `.env` the same channel one level lower — so the `ValueSource` is what names the
    // channel a refusal reports, and `[server] sandbox` is read only when clap answered nothing.
    //
    // `Channel::DotEnv` is still not producible, and `Channel::name`'s own doc says why: the
    // two upper channels are one answer by the time anything here reads it, so the variable is
    // what the operator has to go and look at either way.
    let requested = resolve_flag(
        provenance.sandbox,
        args.sandbox_flag(),
        resolution.config.server.sandbox,
    );
    let channel = requested.then_some(match provenance.sandbox {
        ValueSource::CommandLine => Channel::Flag,
        ValueSource::EnvVariable => Channel::Env,
        _ => Channel::Config,
    });
    let Some(channel) = channel else {
        return Ok(Request {
            enabled: false,
            channel: None,
        });
    };
    // A request is refused here, before any exposure set exists. The graph that would
    // honour one — exposures, the OTLP trade, `confine`'s launch — lands with the backend
    // that flips [`BACKEND_IS_STUB`].
    refuse_unhonourable(channel)?;
    let _ = resolution;
    Err(Error::Operational(format!(
        "{} asked for a confinement, but no platform backend is wired to apply it on `{}`: nothing would be confined, so the run is refused rather than served unconfined",
        channel.name(),
        OS
    )))
}

/// The two facts this build cannot change, refused in one place so neither can be answered by
/// accident — the fail-closed half of [`plan`], and the only reader of [`BACKEND_IS_STUB`].
///
/// The messages are a matched pair and are written to be told apart by a test: the build's names
/// the build and never the platform, the platform's names `std::env::consts::OS` and never the
/// build. A leg that accepted either would pass just as well if the two were swapped, which is
/// why `cli_lifecycle.feature`'s variable scenario asserts each one's own words and the absence
/// of the other's (`crates/tinio-e2e/tests/features/cli_lifecycle.feature`).
fn refuse_unhonourable(channel: Channel) -> Result<(), Error> {
    // A channel this build has no feature for is a **usage** error: the request was well formed
    // and the answer is "not in this build" (design decision 5). This arm and the next are the
    // function's whole body, one cfg'd away at compile time — the same shape
    // `Args::sandbox_flag` uses, so no build has an unreachable tail.
    #[cfg(not(feature = "sandbox"))]
    {
        Err(Error::Usage(format!(
            "{} asked for a confinement, but this build has no `sandbox` feature: nothing would be confined, so the run is refused rather than served unconfined",
            channel.name()
        )))
    }
    // The platform's backend is a startup error: the argument was fine, the target cannot honour
    // it, and the message names which platform so the operator knows what the remedy is.
    #[cfg(feature = "sandbox")]
    {
        // A block-local import, so the name exists exactly where the cfg does and no build has
        // an unused one.
        use std::env::consts::OS;

        if BACKEND_IS_STUB {
            Err(Error::Operational(format!(
                "{} asked for a confinement, but this platform's `sandbox` backend (`{}`) is still a stub: nothing would be confined, so the run is refused rather than served unconfined",
                channel.name(),
                OS
            )))
        } else {
            // The stub flipped without the confinement graph. Still refuse: serving
            // unconfined is the failure the feature exists to prevent.
            Err(Error::Operational(format!(
                "{} asked for a confinement, but no platform backend is wired to apply it on `{OS}`: nothing would be confined, so the run is refused rather than served unconfined",
                channel.name(),
            )))
        }
    }
}

/// Runs the confinement — §1 step i, the **dynamic** half, after the fork.
///
/// It contains no determination of its own: [`plan`] has already refused every request this
/// build cannot honour, so what is left is to rebuild what the [`Request`] carries and hand it to
/// the platform. Three values are assembled:
///
/// - the `Sandbox`, rebuilt through `Sandbox::from_exposures(exposures, device_files)` — the flag
///   travels as an argument rather than a re-derivation, which is the whole reason that
///   constructor takes two.
/// - the configuration's **text**, read from the file the resolution named and forwarded
///   unparsed. The launcher never builds a `Config`: it holds credential-bearing bytes for exactly
///   as long as it takes to hand them over (sandbox design decision 18), and the child re-derives
///   its answers from them.
/// - the **listener**, bound here so the plan owns it (sandbox design decision 16). The child
///   inherits a socket instead of the right to bind, which is what lets the platform's profile
///   or capability set drop that rule; the descriptor reaches the child in argv, spelled by the
///   facade's `listener_fd_arg`.
///
/// The answer is the platform's: `Confinement::InPlace` is "this process is confined, carry on"
/// — Linux, whose `serve` then binds nothing and serves on what it has — and
/// `Confinement::Supervise(child)` is "a confined child is running", which the caller drives with
/// `tinio_sandbox::supervise`.
///
/// # Errors
///
/// A configuration the payload cannot be read from, a bind that fails, and every failure the
/// platform's own call reports — each fatal, because a confinement failure must not leave the
/// process running unconfined (design §11).
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
///
/// use tinio_cli::{
///     resolve::Resolution,
///     sandbox::{Request, confine},
/// };
/// use tinio_config::Config;
///
/// # fn main() -> Result<(), tinio_cli::Error> {
/// let resolution = Resolution {
///     root: PathBuf::from("/srv/data"),
///     config: Config::default(),
///     config_path: PathBuf::from("/srv/data/.tinio/.tinio.toml"),
///     read_only: false,
///     state_dir: PathBuf::from("/srv/data/.tinio"),
///     config_error: None,
/// };
/// // Nothing asked for the confinement: there is nothing to run.
/// let request = Request {
///     enabled: false,
///     channel: None,
/// };
/// let child = confine(&resolution, &request)?;
/// assert!(child.is_none());
/// # Ok(())
/// # }
/// ```
pub fn confine(resolution: &Resolution, request: &Request) -> Result<Option<Child>, Error> {
    let _ = resolution;
    if !request.enabled {
        return Ok(None);
    }
    // [`plan`] refuses every request this build cannot honour, so an enabled request is a
    // backend that flipped [`BACKEND_IS_STUB`] without landing the launch graph.
    Err(Error::Operational(
        "a confinement was requested, but no platform backend is wired to apply it: refusing rather than serving unconfined"
            .into(),
    ))
}

/// The listener a confined child was handed by its launcher.
///
/// **A descriptor is not a socket, and turning one back into a `TcpListener` is the platform
/// crate's conversion** — `from_raw_socket` on Windows, `OwnedFd`/`from_raw_fd` on unix — which
/// is the one place that work may live, because it is the crate's enumerated `unsafe` site and
/// this crate adds none. No backend has landed, so nothing can have handed a descriptor over
/// legitimately, and the only way to reach this call is an argv that carries the listener element
/// and a stream that decodes as a payload — a forged element, since [`plan`] refuses every
/// request this build cannot honour before a launcher could ever exist.
///
/// So the answer is a refusal rather than a fabricated listener: a process that *was* handed a
/// descriptor must not silently bind its own socket on the configured address instead, because
/// that listener is the one the confinement's caller bound and the one the platform's rules were
/// written against (sandbox design decision 16).
///
/// # Errors
///
/// Always, until a platform backend can perform the rebuild.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::sandbox::adopted_listener;
/// use tinio_sandbox::Adopted;
///
/// # fn listener(adopted: &Adopted) -> Result<(), tinio_cli::Error> {
/// let listener = adopted_listener(adopted)?;
/// # let _ = listener;
/// # Ok(())
/// # }
/// ```
pub fn adopted_listener(adopted: &Adopted) -> Result<TcpListener, Error> {
    Err(Error::Operational(format!(
        "this process inherited listener descriptor {:?}, but no platform backend can rebuild a listener from one yet: refusing rather than serving on a socket this process did not bind",
        adopted.listener()
    )))
}

#[cfg(test)]
mod tests {
    use std::{env::consts::OS, ffi::OsString, path::Path};

    use super::*;
    use crate::_config::Config;

    /// A resolution whose config asked for the confinement, with `root` and `state_dir` as given.
    fn requested(root: &Path, state_dir: &Path) -> Resolution {
        let mut config = Config::parse("version = 1\n[server]\nsandbox = true\n").expect("config");
        config.server.sandbox = true;
        Resolution {
            root: root.to_path_buf(),
            config,
            config_path: state_dir.join(".tinio.toml"),
            read_only: false,
            state_dir: state_dir.to_path_buf(),
            config_error: None,
        }
    }

    #[test]
    fn the_listener_element_is_taken_out_of_argv() {
        let argv = vec![
            OsString::from("server"),
            OsString::from("/srv/data"),
            OsString::from("tinio-sandbox-fd=7"),
        ];
        // The reader is `adopt_argv`'s parameter rather than this binary's stdin: with the
        // element present, `adopt` reads the stream to its end, and a unit test that read the
        // inherited stdin would block on a terminal. The public signature is unchanged —
        // `filter_argv` passes `io::stdin()` — and this is the same body.
        let (filtered, _adopted) = adopt_argv(argv, io::empty()).expect("filter");
        assert_eq!(
            filtered,
            vec![OsString::from("server"), OsString::from("/srv/data")],
            "clap must never see the element"
        );
    }

    #[test]
    fn a_payload_on_the_element_is_adopted_and_the_element_is_gone() {
        // The other half of the same call: `adopt` is the facade's, so a decodable payload on
        // the stream is what travels out of here — never a hand-rolled scan of the prefix.
        let argv = vec![
            OsString::from("server"),
            OsString::from("/srv/data"),
            OsString::from("tinio-sandbox-fd=7"),
        ];
        let (filtered, adopted) =
            adopt_argv(argv, io::Cursor::new(r#"{"config":"port = 9000"}"#)).expect("filter");
        assert_eq!(filtered.len(), 2, "the element is out of argv");
        let adopted = adopted.expect("the payload decoded");
        assert_eq!(adopted.config(), "port = 9000");
        assert_eq!(adopted.listener(), Some(7));
    }

    #[test]
    fn every_channel_collapses_to_one_answer() {
        // The first argument is clap's answer's *source* — the flag, the variable (`.env`'s
        // value arrives as this one too: dotenvy puts it in the process environment before the
        // authoritative parse) or nothing at all — the second is that answer's value, and the
        // third is `config.server.sandbox`.
        assert!(
            !resolve_flag(ValueSource::DefaultValue, false, false),
            "nothing asked for it"
        );
        assert!(
            resolve_flag(ValueSource::CommandLine, true, false),
            "the flag"
        );
        assert!(
            resolve_flag(ValueSource::EnvVariable, true, false),
            "the variable"
        );
        assert!(
            resolve_flag(ValueSource::DefaultValue, false, true),
            "the config key"
        );
        assert!(
            !resolve_flag(ValueSource::EnvVariable, false, true),
            "the variable beats the config file"
        );
        assert!(
            !resolve_flag(ValueSource::CommandLine, false, true),
            "and so does a typed `false`"
        );
    }

    #[test]
    fn no_channel_is_a_disabled_request() {
        // The answer half: nothing asked for the confinement, so nothing is refused and
        // nothing is exposed.
        let resolution = Resolution {
            root: Path::new("/srv/data").to_path_buf(),
            config: Config::default(),
            config_path: Path::new("/srv/data/.tinio/.tinio.toml").to_path_buf(),
            read_only: false,
            state_dir: Path::new("/srv/data/.tinio").to_path_buf(),
            config_error: None,
        };
        let request = plan(
            &resolution,
            &Args::for_test(Path::new("/srv/data")),
            &Provenance::default(),
        )
        .expect("nothing to refuse");
        assert!(!request.enabled);
        assert!(request.channel.is_none());
    }

    /// The value half of the ruling is the leg above; this is the *naming* half, and it is
    /// what decision 10 means by "the refusal names the channel it came from". Every request is
    /// refused by this build (the backends are stubs), so the refusal's first words are where
    /// the channel shows: a typed flag names itself, the variable names itself, and a request
    /// nothing above the file made names the config key.
    #[test]
    fn the_refusal_names_the_channel_the_request_came_from() {
        let root = Path::new("/srv/data");
        let state_dir = Path::new("/srv/data/.tinio");
        let bare = Args::for_test(root);
        let typed = Args {
            sandbox: true,
            ..Args::for_test(root)
        };
        for (args, source, named) in [
            (&typed, ValueSource::CommandLine, "`--sandbox`"),
            (&typed, ValueSource::EnvVariable, "`TINIO_SANDBOX`"),
            (&bare, ValueSource::DefaultValue, "`[server] sandbox`"),
        ] {
            let err = plan(
                &requested(root, state_dir),
                args,
                &Provenance {
                    sandbox: source,
                    ..Provenance::default()
                },
            )
            .expect_err("a confinement request is never silently ignored");
            assert!(
                format!("{err}").starts_with(named),
                "the refusal names `{named}`: {err}"
            );
        }

        // An explicit `false` from either clap channel is a request **not made**, so the
        // config key's `true` is never reached and nothing is refused.
        let request = plan(
            &requested(root, state_dir),
            &bare,
            &Provenance {
                sandbox: ValueSource::EnvVariable,
                ..Provenance::default()
            },
        )
        .expect("an explicit `false` is not a request for a confinement");
        assert!(!request.enabled);
        assert!(request.channel.is_none());
    }

    /// Linux's in-place arm has no `confine` call to reach it, and this leg is what keeps the two
    /// facts from drifting apart.
    ///
    /// `commands::start::run` compiles the `sandbox::confine` call for macOS and Windows only
    /// (`#[cfg(any(target_os = "macos", target_os = "windows"))]`), because `confine` binds a
    /// `std::net::TcpListener` to hand to a child — meaningless on the in-place path, whose shape
    /// `tinio-sandbox-linux`'s own plan settles. While [`BACKEND_IS_STUB`] is `true`, `plan`
    /// refuses first and the absent call cannot be reached; the moment the Linux arm is flipped to
    /// `false`, a Linux `--sandbox` run would sail past that refusal and serve **unconfined** — and
    /// `cli_lifecycle.feature`'s refusal scenarios would stop catching it at the very same
    /// instant, because the refusal this constant produces is exactly what they assert. Only
    /// Linux carries the leg: the other two targets do reach a `confine` call.
    ///
    /// `clippy::assertions_on_constants` is allowed **deliberately, and this is the one place the
    /// lint's premise does not hold**: it reads a constant assertion as one that can never fail,
    /// while here the constant is the subject and a future edit is exactly what makes this leg
    /// fail. Nothing else is allowed for it.
    #[cfg(target_os = "linux")]
    #[allow(clippy::assertions_on_constants)]
    #[test]
    fn the_linux_arm_is_a_stub_while_no_confine_call_exists_on_this_target() {
        assert!(
            BACKEND_IS_STUB,
            "the Linux arm of `BACKEND_IS_STUB` was flipped to `false`, but \
             `commands::start::run` still has no `sandbox::confine` call on this target — its arm \
             is `#[cfg(any(target_os = \"macos\", target_os = \"windows\"))]` — so every Linux run \
             that asks for a confinement would start normally and serve unconfined. Wire the \
             in-place confinement (or whatever shape `tinio-sandbox-linux`'s plan settles, which \
             is why the call is not here yet) in the same change, then delete this leg"
        );
    }

    #[test]
    fn a_requested_confinement_is_refused_by_name() {
        // Fail-closed, and the discrimination is the point: a channel this build cannot honour
        // is refused rather than ignored, and the refusal says *which* reason it is. The two
        // messages are the ones `plan` produces; a build that answered with the wrong one would
        // be either silently permissive or misleading about what to fix.
        // The channel here is the config key (`requested`'s fixture says `sandbox = true`, and
        // the provenance is every key at clap's own default), which is the arm a refusal names
        // in a build that *has* the feature; a feature-off build refuses it just the same, at
        // its own arm of `refuse_unhonourable`.
        let resolution = requested(Path::new("/srv/data"), Path::new("/srv/data/.tinio"));
        let err = plan(
            &resolution,
            &Args::for_test(Path::new("/srv/data")),
            &Provenance::default(),
        )
        .expect_err("a requested confinement is never silently ignored");
        let text = format!("{err}");
        assert!(text.contains("sandbox"), "names the feature: {text}");
        if cfg!(feature = "sandbox") {
            assert!(text.contains("platform"), "the stub backend: {text}");
            assert!(
                text.contains(OS),
                "names the platform this build is on: {text}"
            );
            assert!(
                !text.contains("build"),
                "the feature is compiled in: {text}"
            );
            assert_eq!(err.exit_code(), 1, "a startup error, not a bad argument");
        } else {
            assert!(
                text.contains("build"),
                "the feature-off refusal names the build: {text}"
            );
            assert!(
                !text.contains("platform"),
                "without the feature the platform is moot: {text}"
            );
            assert_eq!(err.exit_code(), 2, "a usage error naming the build");
        }
    }
}
