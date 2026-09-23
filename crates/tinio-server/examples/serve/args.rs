//! The command line, and this process's own argv: design §10 step 2.
//!
//! `serve <root> [--port N] [--address HOST:PORT] [--config <file>]
//! [--read-only] [--sandbox]`, parsed by `clap` and resolved into [`Args`],
//! plus the two facts about argv that the launcher platforms make load-bearing
//! ([`own_args`]).

use std::{env, error::Error, ffi::OsString, net::SocketAddr, path::PathBuf, process};

use clap::Parser;

use crate::events::emit_error;

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
pub struct Args {
    /// The served root, as the operator spelled it. A `PathBuf` rather than the
    /// `String` it used to be: a path that is not valid UTF-8 is legal on Linux,
    /// and `create_dir_all`, `Config` and `FsStorage::new` all take `Path` anyway.
    pub root: PathBuf,
    /// The address the listener binds, `--address` over `--port` over 9000.
    pub address: SocketAddr,
    /// `--config <file>`, when one was given.
    pub config_path: Option<PathBuf>,
    /// The `--read-only` flag. **FR-023's field, and not the resolved mode**: the
    /// file's `[server] read_only` and `TINIO_READ_ONLY` are the other two
    /// precedence levels, and `main` folds all three together.
    pub read_only: bool,
    /// `--sandbox` (design decision 5).
    pub sandbox: bool,
}

/// The command line as `clap` parses it: the flag-for-field shape of
/// `serve <root> [--port N] [--address HOST:PORT] [--config <file>]
/// [--read-only] [--sandbox]`, which [`parse_args`] resolves into [`Args`].
///
/// **Two types rather than one, because clap fills a field from the flag that
/// names it** and `--port`/`--address` are two flags for the one address the
/// server binds. The resolved `Args` is what the rest of the example reads, so
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
pub fn parse_args(
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

/// This process's arguments: `argv[0]` dropped, and the launcher's private
/// element filtered out (design §3: the child's argv carries the payload handle
/// and nothing else that is security-relevant).
///
/// **The element and `argv[0]` are two different things and both matter.**
/// [`parse_args`] is handed arguments rather than a program name — [`RawArgs`] is
/// `no_binary_name` for exactly that reason — and the launcher puts its element
/// **first** in the child's line (`launch::command_args`), so a parser that saw it
/// would read `tinio-sandbox-fd=…` as the served root. The predicate is the
/// launcher's own (`is_listener_fd_arg`), not a prefix spelled here: one
/// definition of what the element looks like is what stops the two sides of the
/// launch from disagreeing about which argument to drop.
///
/// A build without the feature drops nothing, because a build without the feature
/// has no launcher to have set an element — the `let _ = arg` arm exists so the
/// closure's parameter is not an unused binding in that configuration.
pub fn own_args() -> Vec<OsString> {
    env::args_os()
        .skip(1)
        .filter(|arg| {
            #[cfg(feature = "sandbox")]
            {
                !tinio_sandbox::is_listener_fd_arg(arg)
            }
            #[cfg(not(feature = "sandbox"))]
            {
                let _ = arg;
                true
            }
        })
        .collect()
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
pub fn refuse_command_line(err: Box<dyn Error + Send + Sync>) -> ! {
    eprint!("{err}");
    emit_error(err.to_string().trim_end());
    process::exit(2)
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt as _;

    use super::*;

    #[test]
    fn parse_args_recognises_the_sandbox_flag() {
        // The flag and the root path are both bare arguments, so a parser that
        // stops looking at position one would serve `--sandbox` as a directory
        // name — a failure that is invisible until someone reads their filesystem.
        let args = parse_args([OsString::from("/srv/data"), OsString::from("--sandbox")]).unwrap();
        assert_eq!(args.root, PathBuf::from("/srv/data"));
        assert!(args.sandbox);

        let args = parse_args([OsString::from("/srv/data")]).unwrap();
        assert_eq!(args.root, PathBuf::from("/srv/data"));
        assert!(!args.sandbox);

        // The flag is recognised wherever it appears, not only after the root.
        let args = parse_args([OsString::from("--sandbox"), OsString::from("/srv/data")]).unwrap();
        assert_eq!(args.root, PathBuf::from("/srv/data"));
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
}
