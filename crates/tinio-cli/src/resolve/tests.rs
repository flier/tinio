use super::*;
#[cfg(feature = "api")]
use crate::_api::{state as api_state, transport::LocalForm};
// V1: both items live behind the `api` feature (`commands/start/mod.rs` gates the module), so the
// import is gated with them — the `--no-default-features` graph compiles without it.
#[cfg(feature = "api")]
use crate::commands::start::{api::local_form, api_options};
use crate::{cli::Discovery, commands::doctor};

fn cli_matches<const N: usize>(argv: [&str; N]) -> Result<clap::ArgMatches, crate::Error> {
    let argv: Vec<OsString> = argv.into_iter().map(OsString::from).collect();
    let (_cli, matches) = cli::parse(&argv)?;
    matches
        .subcommand_matches("server")
        .cloned()
        .ok_or_else(|| Error::Usage("no `server` subcommand in this argv".into()))
}

/// The discovery parse's answers for `root`, as `locate` needs them: a root and nothing else.
/// The mode, the state dir and the config path are all left at their defaults, so a leg adds
/// exactly the one it is about.
fn discovery_for(root: &Path) -> Discovery {
    Discovery {
        dir: Some(root.to_path_buf()),
        config: None,
        read_only: false,
        state_dir: None,
        pg_dsn: None,
    }
}

/// A `Resolution` for `config_text`, as `server`'s chain would have produced it.
fn resolution_with(root: &Path, state_dir: &Path, config_text: &str) -> Resolution {
    let text = platform_config(config_text);
    let config = Config::parse(&text).expect("config");
    // The mode is read **from** the parsed config rather than assumed: the two are the same
    // value in a `Resolution` `resume` built (it pins them together), so a fixture that
    // hard-coded `false` beside a config saying `read_only = true` would be a `Resolution` no
    // production path can produce. `config_text` is the text the config was parsed from — the
    // chain keeps both, because the launch payload carries the text.
    Resolution {
        pg_root: None,
        root: root.to_path_buf(),
        read_only: config.server.read_only,
        config,
        identity: Default::default(),
        config_text: text,
        config_path: state_dir.join(discover::CONFIG_FILE),
        state_dir: state_dir.to_path_buf(),
        state_dir_override: None,
        config_error: None,
    }
}

/// A path as a TOML basic string: Windows' separators are escape sequences, so the leg that
/// writes an absolute path into a config fixture escapes them (the second platform difference
/// between this file and the brief's unix-written block).
#[cfg(feature = "api")]
fn toml_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "\\\\")
}

/// `config_text` with the local channel's section spelled the way this platform parses it: the
/// other platform's section is an unknown key at parse (the config root's own platform tests),
/// and the legs above are written once, in the spelling `contracts/config.md` documents. What
/// is under test is the channel's *path* rule, not the section's name.
fn platform_config(config_text: &str) -> String {
    if cfg!(windows) {
        config_text.replace("[api.unix]", "[api.pipe]")
    } else {
        config_text.to_string()
    }
}

/// `LocalForm` carries no `PartialEq` (`tinio-api`'s type is an enum of two payloads, and the
/// value is only ever read by the transport), so the leg compares the platform's **own** arm —
/// built by [`local_form`], the production rule itself — with the one `api_options` produced.
#[cfg(feature = "api")]
fn assert_local(local: Option<LocalForm>, expected: impl AsRef<Path>) {
    match (local, local_form(expected.as_ref())) {
        (Some(LocalForm::Unix(actual)), LocalForm::Unix(want)) => assert_eq!(actual, want),
        (Some(LocalForm::Pipe(actual)), LocalForm::Pipe(want)) => assert_eq!(actual, want),
        (actual, want) => panic!("the local channel is {actual:?}, expected {want:?}"),
    }
}

/// `start::Args` as these legs build it: a root and nothing else.
fn args_for(root: &Path) -> start::Args {
    start::Args::for_test(root)
}

/// The same, with `--log-format` typed.
fn args_for_log_format(root: &Path, format: Format) -> start::Args {
    start::Args {
        log_format: Some(format),
        ..start::Args::for_test(root)
    }
}

/// The same, with `--log-file` typed.
fn args_for_log_file(root: &Path, path: &str) -> start::Args {
    start::Args {
        log_file: Some(PathBuf::from(path)),
        ..start::Args::for_test(root)
    }
}

#[test]
fn an_explicit_directory_is_the_root() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = resolve_root(Some(dir.path()), dir.path()).expect("root");
    assert_eq!(root, dir.path().canonicalize().expect("canonical"));
}

#[test]
fn a_missing_directory_and_no_ancestor_is_a_usage_error() {
    let dir = tempfile::TempDir::new().expect("temp");
    let err = resolve_root(None, dir.path()).expect_err("no root anywhere");
    assert!(matches!(err, Error::Usage(_)), "exit 2, not exit 1: {err}");
    assert!(format!("{err}").contains("DIR"), "names the remedy: {err}");
}

#[test]
fn the_walk_up_finds_the_nearest_ancestor_with_a_reserved_directory() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    let nested = root.join("a/b/c");
    fs::create_dir_all(root.join(".tinio")).expect("reserved dir");
    fs::create_dir_all(&nested).expect("nested");
    assert_eq!(
        resolve_root(None, &nested).expect("root"),
        root.canonicalize().expect("c")
    );
}

/// The discovery pass's three answers, which is what the second parse is possible at all
/// because of: a root, a mode and a state dir, resolved with no `.env` in sight.
#[test]
fn locate_resolves_the_root_the_mode_and_the_state_dir() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    fs::create_dir_all(&root).expect("root");
    let canonical = root.canonicalize().expect("canonical");

    // Nothing asked: read-write, and the reserved directory under the root.
    let located = locate(discovery_for(&root), &Provenance::default()).expect("located");
    assert_eq!(located.root, canonical);
    assert!(!located.read_only);
    assert_eq!(located.state_dir, canonical.join(".tinio"));

    // The flag: read-only, and the state dir relocated — to the override when the arm's
    // `TINIO_STATE_DIR` answered, which is the only source that keeps a unit test out of
    // the developer's real home.
    let relocated = dir.path().join("state");
    let discovery = Discovery {
        dir: Some(root.clone()),
        config: None,
        read_only: true,
        state_dir: Some(relocated.clone()),
        pg_dsn: None,
    };
    let located = locate(
        discovery,
        &Provenance {
            read_only: ValueSource::CommandLine,
            ..Provenance::default()
        },
    )
    .expect("located");
    assert!(located.read_only);
    assert_eq!(located.state_dir, relocated);
    assert_eq!(
        located.state_dir_override.as_deref(),
        Some(relocated.as_path())
    );
}

#[test]
fn the_four_levels_resolve_in_order() {
    // Levels 1–3 are clap's, and `ValueSource` (read off the `ArgMatches` the parse hands
    // back — see `cli::provenance`) says whether it answered at all: a flag beats the
    // process environment, and `.env` reaches the same parser on the second pass, so both
    // of the levels clap answers arrive as `CommandLine`/`EnvVariable` values here. The
    // config file is the fourth, and `pick` is where it is read — so this leg drives every
    // case directly, with no parser in the way.
    assert_eq!(
        pick(ValueSource::CommandLine, Some(true), Some(false)),
        Some(true),
        "the flag"
    );
    assert_eq!(
        pick(ValueSource::EnvVariable, Some(true), Some(false)),
        Some(true),
        "the process environment, and `.env` with it — the same answer, one level apart"
    );
    assert_eq!(
        pick(ValueSource::DefaultValue, Some(false), Some(true)),
        Some(true),
        "nothing above the file: the config file is level 4"
    );
    // Nothing anywhere: clap's own default stands — `None` for a key that has none.
    assert_eq!(
        pick(ValueSource::DefaultValue, Some(false), None),
        Some(false)
    );
    assert_eq!(pick::<bool>(ValueSource::DefaultValue, None, None), None);
}

#[test]
fn a_config_path_that_does_not_exist_is_a_usage_error() {
    // `--config` names a file that must be there; auto-create is discovery's behaviour,
    // and a mistyped path must not silently become a new config in a new place.
    let dir = tempfile::TempDir::new().expect("temp");
    let missing = dir.path().join("root/elsewhere.toml");
    let err = config_path(dir.path(), dir.path(), Some(&missing)).expect_err("missing file");
    assert!(matches!(err, Error::Usage(_)), "exit 2, not exit 1: {err}");
    assert!(
        format!("{err}").contains("elsewhere.toml"),
        "names the path: {err}"
    );
}

/// **A confined child resolves from the payload and does not re-validate the path its launcher
/// read** (design decision 18), and this leg is the fix's keeper: a `--config` path the
/// confinement cannot see is what every out-of-root configuration looks like from inside, while
/// the unconfined run's "must be there" contract stays exactly as it was.
#[test]
fn an_adopted_run_reads_the_payload_not_the_explicit_config_path() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    fs::create_dir_all(&root).expect("root");
    // Never created: the path `--config` names, as the container sees it.
    let outside = dir.path().join("outside/sandbox.toml");
    let discovery = || Discovery {
        dir: Some(root.clone()),
        config: Some(outside.clone()),
        read_only: false,
        // Read-only relocates the state dir, and the override is what keeps this leg out of the
        // developer's real home (`locate_resolves_the_root_the_mode_and_the_state_dir`).
        state_dir: Some(dir.path().join("state")),
        pg_dsn: None,
    };

    // The unconfined contract is untouched: a path that is not a file is a usage error.
    let err = locate(discovery(), &Provenance::default()).expect_err("missing file");
    assert!(matches!(err, Error::Usage(_)), "exit 2, not exit 1: {err}");
    assert!(
        format!("{err}").contains("sandbox.toml"),
        "still names the path: {err}"
    );

    // The adopted run: the text *is* the document that path named, so the mode is the text's and
    // the path is only carried, never asked for.
    let located = locate_confined(
        discovery(),
        &Provenance::default(),
        "version = 1\n[server]\nread_only = true\n",
        None,
    )
    .expect("the payload's text stands in for the path");
    assert!(located.read_only, "the mode is the payload text's");
    assert_eq!(
        located.config.as_deref(),
        Some(outside.as_path()),
        "the path is still what the resolution names"
    );
    assert_eq!(located.state_dir, dir.path().join("state"));
}

/// **A broken file's refusal names the file it came from.** `document` loads through
/// `Config::load`, whose refusal labels its source with the path, so a broken document is named
/// rather than left for the operator to hunt down. **`locate` itself refuses it**, at the anchor
/// read: swallowing that failure here is what made a broken `--config` with no `DIR` answer the
/// storage-root usage error instead of naming the file. This is the file arm; the
/// launcher-forwarded text is the design's one text parse (`resume`'s adopted arm, design
/// decision 18), not a file.
#[test]
fn a_broken_file_refusal_names_the_file() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    fs::create_dir_all(&root).expect("root");
    let config_path = dir.path().join("outside/sandbox.toml");
    fs::create_dir_all(config_path.parent().expect("outside dir")).expect("outside dir");
    // Malformed TOML: an unterminated basic string, refused before any key is read.
    fs::write(
        &config_path,
        "version = 1\n[server]\nhost = \"unterminated\n",
    )
    .expect("write the broken document");

    let err = locate(
        Discovery {
            dir: Some(root.clone()),
            config: Some(config_path.clone()),
            read_only: false,
            state_dir: Some(dir.path().join("state")),
            pg_dsn: None,
        },
        &Provenance::default(),
    )
    .expect_err("the file's document refuses at the anchoring read");
    assert!(
        err.to_string().contains("sandbox.toml"),
        "names the file: {err}"
    );
}

#[test]
fn the_candidates_lead_with_the_state_dir_the_chain_resolved() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    let relocated = dir.path().join("home/.tinio/roots/h");

    // Two inputs arrive as parameters rather than being read here, and both have a
    // reason. `override_dir` is `TINIO_STATE_DIR` as the *caller* read it — this function
    // never touches the environment, because the crate's tests may not (`set_var` is
    // unsafe under edition 2024 and a unit test mutating one process-global table across
    // parallel threads is the rule's whole point). `derived` is the home-derived
    // candidate, *lazily*: the real one is `default_home_state_dir`, which **creates**
    // `<home>/.tinio/roots` while it computes (`crates/tinio-util/src/state.rs:84-87`),
    // so a unit test that derived it would seed the developer's real home — the rule
    // `home_state_dir`'s own doc states ("home is an explicit parameter, never
    // `dirs::home_dir()` inline: tests must resolve a scratch home instead of the real
    // one"). Laziness is load-bearing rather than tidy: when the override answers, the
    // derived closure is never called, which is what keeps a test on Windows out of
    // `%USERPROFILE%`.

    // Read-only: the resolved dir is the relocated one and the other candidate is the
    // root's own `.tinio` — which is the case the fallback exists for. Nothing derives a
    // home here at all: the other candidate is a join.
    let derived_ran = Cell::new(false);
    let candidates = state_dir_candidates(&root, &relocated, true, None, || {
        derived_ran.set(true);
        Ok(dir.path().join("never"))
    })
    .expect("candidates");
    assert_eq!(candidates, vec![relocated.clone(), root.join(".tinio")]);
    assert!(!derived_ran.get(), "a read-only pair derives nothing");

    // Read-write: the root's `.tinio` leads, and the other candidate is the derived
    // home dir of the same root.
    let candidates = state_dir_candidates(&root, &root.join(".tinio"), false, None, || {
        Ok(relocated.clone())
    })
    .expect("candidates");
    assert_eq!(candidates[0], root.join(".tinio"));
    assert_eq!(candidates.len(), 2, "two candidates, never more");
    assert_ne!(candidates[0], candidates[1]);

    // Read-write with the variable set: it replaces the **home-derived** candidate and
    // nothing derives.
    let derived_ran = Cell::new(false);
    let candidates = state_dir_candidates(
        &root,
        &root.join(".tinio"),
        false,
        Some(relocated.clone()),
        || {
            derived_ran.set(true);
            Ok(dir.path().join("never"))
        },
    )
    .expect("candidates");
    assert_eq!(candidates, vec![root.join(".tinio"), relocated.clone()]);
    assert!(
        !derived_ran.get(),
        "an exported TINIO_STATE_DIR means no home is derived"
    );

    // Read-only with the variable set: the resolved dir already *is* the variable, and the
    // pair must not collapse onto it — `<root>/.tinio` is what a reader probes second, or
    // the read-write server a read-only client is looking for is unreachable.
    let candidates = state_dir_candidates(&root, &relocated, true, Some(relocated.clone()), || {
        Ok(dir.path().join("never"))
    })
    .expect("candidates");
    assert_eq!(candidates, vec![relocated.clone(), root.join(".tinio")]);
    assert_ne!(
        candidates[0], candidates[1],
        "the pair never collapses onto one directory"
    );
}

#[test]
fn provenance_reads_the_parsers_own_answer() {
    // The one leg that proves the matches are kept: a flag reaches `CommandLine`, and a
    // key nothing supplied reaches `DefaultValue` with clap's own default as the value.
    let argv = ["tinio", "server", "/srv/data", "--port", "9001"];
    let matches = cli_matches(argv).expect("parse");
    let provenance = cli::provenance(&matches);
    assert_eq!(
        provenance.port,
        ValueSource::CommandLine,
        "--port was typed"
    );
    assert_eq!(
        pick(
            provenance.port,
            matches.get_one::<u16>("port").copied(),
            Some(9000)
        ),
        Some(9001)
    );

    let matches = cli_matches(["tinio", "server", "/srv/data"]).expect("parse");
    let provenance = cli::provenance(&matches);
    assert_eq!(
        provenance.port,
        ValueSource::DefaultValue,
        "nothing supplied a port"
    );
    assert_eq!(
        pick(
            provenance.port,
            matches.get_one::<u16>("port").copied(),
            Some(9000)
        ),
        Some(9000)
    );

    // The `EnvVariable` arm cannot be driven from here: it needs `TINIO_PORT` in the
    // process environment, and no test in this workspace may set one (a `set_var` in a
    // test is unsafe under edition 2024 and races every sibling test). It is covered where
    // the variable can be set on a *child* — Task 3's `spawn_with_env` leg, which starts
    // the server with `TINIO_PORT=0` and no `--port` flag and asserts the readiness line
    // reports a bound port, i.e. that level 2 reached the same flag.
}

#[test]
fn doctor_records_a_config_that_does_not_parse() {
    // The field plan C consumes (`config_error`) and the reason it is not a `Result`:
    // `doctor` exists for exactly this root, and a `for_doctor` that propagated the
    // failure would put the "configuration valid" check out of reach.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    fs::create_dir_all(root.join(".tinio")).expect("reserved dir");
    fs::write(
        root.join(".tinio/.tinio.toml"),
        "version = \"not a number\"\n",
    )
    .expect("seed");

    let args = doctor::Args::for_test(&root);
    let located = locate(discovery_for(&root), &Provenance::default()).expect("located");
    let resolution =
        for_doctor(located, &args.dir).expect("a broken config is a finding, not an error");
    assert!(
        resolution.config_error.is_some(),
        "the failure is recorded: {resolution:?}"
    );
    assert_eq!(
        resolution.config.server.port, 9000,
        "the parsed config falls back to the default"
    );

    // And the server's chain is the other half of the rule: the same root fails there.
    let server_args = start::Args::for_test(&root);
    let located = locate(discovery_for(&root), &Provenance::default()).expect("located");
    assert!(
        server(located, &server_args, &Provenance::default()).is_err(),
        "a server must not run on a config it could not read"
    );
}

#[test]
fn the_log_file_follows_flag_config_then_format() {
    // The one ordering of `--log-file`, `[log] server_log_file` and the format-derived
    // default (Task 5's daemon redirects to this answer and declares no rule of its own).
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    let state = dir.path().join("state");
    fs::create_dir_all(&root).expect("root");

    // The default: no flag, no `[log]` section — and it lands in the state dir, because a
    // daemon has no console to write to (an empty key means "stderr" for a foreground run).
    let resolution = resolution_with(&root, &state, "version = 1\n");
    let default = log_file(&resolution, &args_for(&root)).expect("log file");
    assert!(default.starts_with(&resolution.state_dir), "{default:?}");
    assert!(
        default.ends_with("server.log"),
        "text is the default format: {default:?}"
    );
    // The JSON default, through the **folded** chain: `resolve::server` is what runs the four
    // levels and writes the answer into `config.log.server_log_format` — the one place
    // `log_file` reads the format from. A hand-built `Resolution` never ran that chain, so a
    // flag alone would not move this answer, which is the property the collapse rests on.
    // `resolve::server`'s chain creates the config it does not find, which is what this leg
    // lets it do: what is under test is the fold, not the auto-create.
    let located = locate(discovery_for(&root), &Provenance::default()).expect("located");
    let folded = server(
        located,
        &args_for_log_format(&root, Format::Json),
        &Provenance {
            log_format: ValueSource::CommandLine,
            ..Provenance::default()
        },
    )
    .expect("the folded chain");
    assert!(
        log_file(&folded, &args_for_log_format(&root, Format::Json))
            .expect("json")
            .ends_with("server.json"),
        "the JSON format's default is `server.json`"
    );

    // The config key answers when the flag does not, and the flag answers over it.
    let resolution = resolution_with(
        &root,
        &state,
        "version = 1\n[log]\nserver_log_file = \"/var/log/tinio.log\"\n",
    );
    assert_eq!(
        log_file(&resolution, &args_for(&root)).expect("config"),
        PathBuf::from("/var/log/tinio.log")
    );
    assert_eq!(
        log_file(&resolution, &args_for_log_file(&root, "/tmp/asked.log")).expect("flag"),
        PathBuf::from("/tmp/asked.log"),
        "the flag wins over the key"
    );
}

#[test]
fn an_ipv6_literal_keeps_its_colons() {
    let v6 = authority_addr("[::1]:8080", 9000).expect("bracketed ipv6");
    assert_eq!(v6.ip().to_string(), "::1");
    assert_eq!(v6.port(), 8080);
    let defaulted = authority_addr("[::1]", 9000).expect("default port");
    assert_eq!(defaulted.port(), 9000);
    let bare = authority_addr("::1", 9000).expect("bare ipv6");
    assert_eq!(bare.ip().to_string(), "::1");
    assert_eq!(bare.port(), 9000);
    let v4 = authority_addr("127.0.0.1:8080", 9000).expect("ipv4");
    assert_eq!(v4.port(), 8080);
    let port_only = authority_addr(":9001", 9000).expect("port only");
    assert_eq!(port_only.ip().to_string(), "127.0.0.1");
    assert_eq!(port_only.port(), 9001);
    assert!(authority_addr("[::1]:nope", 9000).is_err());
}

/// `api` only: `api_options` returns plan A's `Selection`, and `tinio-api` is an optional
/// dependency of this crate, so this leg does not exist in a feature-off build.
#[cfg(feature = "api")]
#[test]
fn api_options_resolves_the_local_channel() {
    // Four assertions on the returned `Selection`: a relative `[api.unix] path` joins the
    // state dir, an absolute one is verbatim, an empty one is the derived
    // `state::socket_path`, and a config with **no `[api]` section at all** is that same
    // derived answer (Task 10's harness fixtures depend on the last one).
    let dir = tempfile::TempDir::new().expect("temp");
    let state = dir.path().join("state");
    let root = dir.path().join("root");
    fs::create_dir_all(&root).expect("root");

    let selection = api_options(
        &resolution_with(&root, &state, "[api.unix]\npath = \"ctrl.sock\"\n"),
        &args_for(&root),
    )
    .expect("selection");
    // The configured value travels as written — the join to the state dir is `Plane::bind`'s.
    assert_local(selection.local, "ctrl.sock");

    let absolute = dir.path().join("elsewhere/ctrl.sock");
    let selection = api_options(
        &resolution_with(
            &root,
            &state,
            &format!("[api.unix]\npath = \"{}\"\n", toml_path(&absolute)),
        ),
        &args_for(&root),
    )
    .expect("selection");
    assert_local(selection.local, absolute);

    // The derived channel, per platform: `api_options` answers with the socket path on unix and
    // with the contract's bare pipe name on Windows (`contracts/management-api.md:5`) — a name,
    // because a path in the pipe namespace would publish the state dir to every local user.
    #[cfg(windows)]
    let derived = PathBuf::from(api_state::pipe_name(&root).expect("derived pipe name"));
    #[cfg(not(windows))]
    let derived = api_state::socket_path(&state, &root).expect("derived");
    let selection = api_options(
        &resolution_with(&root, &state, "[api.unix]\n"),
        &args_for(&root),
    )
    .expect("selection");
    assert_local(selection.local, derived.clone());

    let selection =
        api_options(&resolution_with(&root, &state, ""), &args_for(&root)).expect("selection");
    assert_local(selection.local, derived);

    // The `http`/`https` arms come from `--api`-shaped inputs in the second selection;
    // exclusivity is `select`'s and is asserted in plan A, not here.
}

/// **The credential policy is one decision with three answers, and both sentences render the
/// fallback from its one definer** (audit run-2, P2): the pair's source is
/// `tinio_server::identity::uses_builtin_fallback` and the bind is `Resolution::address`'s, so the
/// only inputs are the config and `[server] host` — which is what this leg varies, while the
/// static half (`commands::start::run`) and the serving half (`commands::start::serve`) read the
/// same value.
#[test]
fn the_credential_policy_reads_the_bind_and_the_pair() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    let state = dir.path().join("state");

    // The schema's default host is loopback, so no `[auth]` is the warning arm.
    let loopback = resolution_with(&root, &state, "");
    assert_eq!(
        loopback
            .credential_policy()
            .expect("the default bind parses"),
        CredentialPolicy::BuiltinLoopback
    );
    let warning = loopback
        .credential_policy()
        .expect("the default bind parses")
        .warning()
        .expect("the loopback arm warns");
    assert!(
        warning.contains(identity::DEFAULT_ROOT_ACCESS_KEY),
        "the warning names the fallback through its one definer: {warning}"
    );
    assert_eq!(
        loopback
            .credential_policy()
            .expect("the default bind parses")
            .refusal(),
        None
    );

    // A bind another host can reach is the refusal arm, and its sentence names both escapes.
    let exposed = resolution_with(&root, &state, "[server]\nhost = \"0.0.0.0\"\n");
    let refusal = exposed
        .credential_policy()
        .expect("the bind parses")
        .refusal()
        .expect("the exposed arm refuses");
    assert!(
        refusal.contains("[auth]")
            && refusal.contains("TINIO_ACCESS_KEY")
            && refusal.contains("TINIO_SECRET_KEY"),
        "the refusal names the section and the variables: {refusal}"
    );
    assert!(
        refusal.contains(identity::DEFAULT_ROOT_ACCESS_KEY),
        "and it names the fallback through its one definer: {refusal}"
    );
    assert_eq!(
        exposed
            .credential_policy()
            .expect("the bind parses")
            .warning(),
        None
    );

    // A configured pair is the operator's own: neither arm.
    let configured = resolution_with(
        &root,
        &state,
        "[auth]\naccess_key = \"AK\"\nsecret_key = \"SK\"\n",
    );
    assert_eq!(
        configured.credential_policy().expect("the bind parses"),
        CredentialPolicy::Configured
    );
}

/// **A tier configuration is not a dsn-addressed pg run** (T1, ruling R8): the document names
/// `[storage.pg]` because that is the tier layer's cold plane, so a `TINIO_PG_DSN` beside it must
/// not buy the synthetic root a *pg* run lives under. Without a `DIR` and without a discovered
/// root the run has no served root, and the resolution says so (the usage error names `DIR`)
/// instead of handing the serving half a tool-state leaf as if it were the root to serve.
#[test]
fn resolve_keeps_a_tier_run_out_of_the_pg_arm() {
    use crate::{
        _util::secret::{Secret, SecretKey},
        config::storage::Backend,
    };

    const DSN: &str = "postgres://tinio:tinio@127.0.0.1:55432/tinio?sslmode=disable";
    let scratch = tempfile::TempDir::new().expect("a scratch working directory");
    let config = scratch.path().join("tier.toml");
    fs::write(
        &config,
        format!("version = 1\n[storage]\ntype = \"tier\"\n[storage.pg]\ndsn = \"{DSN}\"\n"),
    )
    .expect("write the tier configuration");
    let discovery = Discovery {
        dir: None,
        config: Some(config),
        read_only: false,
        state_dir: None,
        // The cold plane's dsn is also exported, which is the shape that used to make every
        // dsn-carrying run pg-addressed.
        pg_dsn: Some(Secret::from(SecretKey::from(DSN.to_owned()))),
    };

    let err = locate_from(discovery, &Provenance::default(), scratch.path())
        .expect_err("a tier run with no DIR has no served root to synthesize");
    let rendered = err.to_string();
    assert!(
        rendered.contains("DIR"),
        "the refusal is the missing-root usage error: {rendered}"
    );

    let parsed = Config::parse(&format!(
        "version = 1\n[storage]\ntype = \"tier\"\n[storage.pg]\ndsn = \"{DSN}\"\n"
    ))
    .expect("the document parses");
    assert_eq!(
        parsed.backend(),
        Backend::Tier,
        "the selector is what the pg arm must read, not the section's presence"
    );
}
