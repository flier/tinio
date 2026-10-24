//! The free-form argument and parse tests: the contract's command lines, the scope flags and
//! their refusals, `Nd`/`Ny`, and the stopped-root refusals that precede the plane.

use std::path::PathBuf;

use super::fixtures::{
    Fake, legalhold_verb, no_server_root, object_lock_verb, parse, retention_verb,
};
use crate::{
    _core::object_lock as model,
    commands::{
        legalhold, object_lock,
        object_lock::DirArgs,
        plane::Connection,
        retention,
        retention::{Validity, parse_validity},
    },
};

/// The contract's seven command lines parse into the arms and fields their rows list.
#[test]
fn the_seven_commands_parse() {
    let object_lock::Verb::Enable(enable) =
        object_lock_verb(&["tinio", "object-lock", "enable", "photos"]);
    assert_eq!(enable.bucket, "photos");
    assert!(!enable.output.json);

    let retention::Verb::Set(set) = retention_verb(&[
        "tinio",
        "retention",
        "set",
        "photos/report.txt",
        "governance",
        "30d",
        "--version-id",
        "abc",
        "--bypass",
    ]) else {
        panic!("the set verb");
    };
    assert_eq!(set.target, "photos/report.txt");
    assert_eq!(set.mode, model::Mode::Governance);
    assert_eq!(set.validity, Validity::Days(30));
    assert_eq!(set.version_id.as_deref(), Some("abc"));
    assert!(set.bypass);
    assert!(!set.default);

    let retention::Verb::Set(default) = retention_verb(&[
        "tinio",
        "retention",
        "set",
        "photos",
        "--default",
        "compliance",
        "3y",
    ]) else {
        panic!("the set --default verb");
    };
    assert_eq!(default.target, "photos");
    assert_eq!(default.mode, model::Mode::Compliance);
    assert_eq!(default.validity, Validity::Years(3));
    assert!(default.default);
    assert!(default.version_id.is_none());
    assert!(!default.bypass);

    let retention::Verb::Info(info) = retention_verb(&[
        "tinio",
        "retention",
        "info",
        "photos/report.txt",
        "--version-id",
        "abc",
        "--json",
    ]) else {
        panic!("the info verb");
    };
    assert_eq!(info.version_id.as_deref(), Some("abc"));
    assert!(info.output.json);

    let retention::Verb::Clear(clear) =
        retention_verb(&["tinio", "retention", "clear", "photos", "--default"])
    else {
        panic!("the clear verb");
    };
    assert!(clear.default);

    let legalhold::Verb::Set(set) =
        legalhold_verb(&["tinio", "legalhold", "set", "photos/report.txt"])
    else {
        panic!("the legalhold set verb");
    };
    assert_eq!(set.target, "photos/report.txt");

    let legalhold::Verb::Info(info) = legalhold_verb(&[
        "tinio",
        "legalhold",
        "info",
        "photos/report.txt",
        "--version-id",
        "abc",
    ]) else {
        panic!("the legalhold info verb");
    };
    assert_eq!(info.version_id.as_deref(), Some("abc"));

    let legalhold::Verb::Clear(clear) =
        legalhold_verb(&["tinio", "legalhold", "clear", "photos/report.txt"])
    else {
        panic!("the legalhold clear verb");
    };
    assert_eq!(clear.target, "photos/report.txt");
}

/// The three groups take the root and the mode exactly as `status` does (`[DIR]`,
/// `--read-only`), beside their own positionals.
#[test]
fn the_root_and_mode_parse_beside_the_verb_arguments() {
    let retention::Verb::Set(set) = retention_verb(&[
        "tinio",
        "retention",
        "set",
        "photos/a",
        "governance",
        "30d",
        "/srv/data",
        "--read-only",
    ]) else {
        panic!("the set verb");
    };
    assert_eq!(set.dir.dir, Some(PathBuf::from("/srv/data")));
    assert!(set.dir.read_only);

    let object_lock::Verb::Enable(enable) =
        object_lock_verb(&["tinio", "object-lock", "enable", "photos", "/srv/data"]);
    assert_eq!(enable.dir.dir, Some(PathBuf::from("/srv/data")));
    assert!(!enable.dir.read_only);
}

/// `--bypass` belongs to `retention set` alone, and `--default` to `set`/`clear`: the parser,
/// not the plane, refuses the other spellings.
#[test]
fn the_scope_flags_belong_to_their_own_verbs() {
    assert!(parse(&["tinio", "retention", "clear", "photos/a", "--bypass"]).is_err());
    assert!(parse(&["tinio", "retention", "info", "photos/a", "--bypass"]).is_err());
    assert!(parse(&["tinio", "legalhold", "set", "photos/a", "--bypass"]).is_err());
    assert!(parse(&["tinio", "legalhold", "clear", "photos/a", "--bypass"]).is_err());
    assert!(parse(&["tinio", "retention", "info", "photos", "--default"]).is_err());
    assert!(parse(&["tinio", "legalhold", "set", "photos/a", "--default"]).is_err());
    assert!(parse(&["tinio", "object-lock", "enable", "photos", "--default"]).is_err());
}

/// `Nd`/`Ny`: the two units, the model's bounds, and a refusal for every other spelling.
#[test]
fn nd_and_ny_parse_and_refuse() {
    for (raw, expected) in [
        ("1d", Validity::Days(1)),
        ("30d", Validity::Days(30)),
        ("36500d", Validity::Days(36_500)),
        ("1y", Validity::Years(1)),
        ("100y", Validity::Years(100)),
    ] {
        assert_eq!(parse_validity(raw).expect(raw), expected, "{raw}");
    }
    for raw in [
        "", "d", "y", "0d", "36501d", "0y", "101y", "30", "-1d", "+1d", "1.5d", "30D", "3Y", "abc",
        "1dd",
    ] {
        assert!(parse_validity(raw).is_err(), "{raw:?} must be refused");
    }

    // The parser is clap's: an invalid validity is a usage error (exit 2), not a plane call.
    for raw in ["0d", "101y", "30", "30D"] {
        assert!(
            parse(&["tinio", "retention", "set", "photos/a", "governance", raw]).is_err(),
            "{raw:?}"
        );
    }
    assert!(parse(&["tinio", "retention", "set", "photos/a", "invalid", "30d"]).is_err());
    // The mode is case-insensitive, as `mc` upper-cases it.
    assert!(parse(&["tinio", "retention", "set", "photos/a", "GOVERNANCE", "30d"]).is_ok());
}

/// `--default` is a bucket scope: a key, a version or a bypass beside it is the CLI's usage
/// error, and the keyed verbs need a key. Nothing reaches the plane on a refusal.
#[tokio::test]
async fn the_scope_combinations_are_refused_before_the_plane() {
    let fake = Fake::default();
    let set = |argv: &[&str]| {
        let retention::Verb::Set(args) = retention_verb(argv) else {
            panic!("the set verb");
        };
        args
    };
    let clear = |argv: &[&str]| {
        let retention::Verb::Clear(args) = retention_verb(argv) else {
            panic!("the clear verb");
        };
        args
    };

    for argv in [
        &[
            "tinio",
            "retention",
            "set",
            "photos/a",
            "--default",
            "governance",
            "30d",
        ][..],
        &[
            "tinio",
            "retention",
            "set",
            "photos",
            "--default",
            "governance",
            "30d",
            "--version-id",
            "abc",
        ][..],
        &[
            "tinio",
            "retention",
            "set",
            "photos",
            "--default",
            "governance",
            "30d",
            "--bypass",
        ][..],
    ] {
        let error = retention::set::execute(&set(argv), &fake)
            .await
            .expect_err(argv.join(" ").as_str());
        assert_eq!(error.exit_code(), 2, "{argv:?}: {error}");
    }

    for argv in [
        &["tinio", "retention", "set", "photos", "governance", "30d"][..],
        &["tinio", "retention", "clear", "photos"][..],
        &[
            "tinio",
            "retention",
            "info",
            "photos",
            "--version-id",
            "abc",
        ][..],
        &["tinio", "retention", "clear", "photos/a", "--default"][..],
        &[
            "tinio",
            "retention",
            "clear",
            "photos",
            "--default",
            "--version-id",
            "abc",
        ][..],
    ] {
        let error = match *argv {
            ["tinio", "retention", "set", ..] => retention::set::execute(&set(argv), &fake).await,
            ["tinio", "retention", "clear", ..] => {
                retention::clear::execute(&clear(argv), &fake).await
            }
            _ => {
                let retention::Verb::Info(args) = retention_verb(argv) else {
                    panic!("the info verb");
                };
                retention::info::execute(&args, &fake).await
            }
        }
        .expect_err(argv.join(" ").as_str());
        assert_eq!(error.exit_code(), 2, "{argv:?}: {error}");
    }

    for argv in [
        &["tinio", "legalhold", "set", "photos"][..],
        &["tinio", "legalhold", "info", "photos"][..],
        &["tinio", "legalhold", "clear", "photos"][..],
    ] {
        let error = match *argv {
            ["tinio", "legalhold", "set", ..] => {
                let legalhold::Verb::Set(args) = legalhold_verb(argv) else {
                    panic!("the set verb");
                };
                legalhold::set::execute(&args, &fake).await
            }
            ["tinio", "legalhold", "info", ..] => {
                let legalhold::Verb::Info(args) = legalhold_verb(argv) else {
                    panic!("the info verb");
                };
                legalhold::info::execute(&args, &fake).await
            }
            _ => {
                let legalhold::Verb::Clear(args) = legalhold_verb(argv) else {
                    panic!("the clear verb");
                };
                legalhold::clear::execute(&args, &fake).await
            }
        }
        .expect_err(argv.join(" ").as_str());
        assert_eq!(error.exit_code(), 2, "{argv:?}: {error}");
    }

    let object_lock::Verb::Enable(enable) =
        object_lock_verb(&["tinio", "object-lock", "enable", "photos/report.txt"]);
    let error = object_lock::enable::execute(&enable, &fake)
        .await
        .expect_err("an object target");
    assert_eq!(error.exit_code(), 2);

    assert!(fake.calls().is_empty(), "no refusal reached the plane");
}

/// A root with no server is a clean refusal naming the root, not a panic and not an empty
/// configuration.
#[tokio::test]
async fn a_root_with_no_server_refuses_cleanly() {
    let (_scratch, located) = no_server_root();
    let error = Connection::connect(DirArgs::default(), located)
        .await
        .expect_err("no server");
    assert_eq!(error.exit_code(), 1);
    assert!(
        error.to_string().starts_with("no running server for "),
        "{error}"
    );
}

/// A usage error precedes the server lookup: the target and scope checks live in each verb's
/// `run` before `Connection::connect`, so a stopped root answers exit 2 rather than the
/// no-server refusal. One representative misuse per group.
#[tokio::test]
async fn a_usage_error_precedes_the_server_lookup() {
    // retention: `--default` with an object target.
    let (_scratch, located) = no_server_root();
    let retention::Verb::Set(args) = retention_verb(&[
        "tinio",
        "retention",
        "set",
        "photos/a.txt",
        "--default",
        "governance",
        "30d",
    ]) else {
        panic!("the set verb");
    };
    let error = retention::run(
        retention::Args {
            verb: retention::Verb::Set(args),
        },
        located,
    )
    .await
    .expect_err("the misuse");
    assert_eq!(error.exit_code(), 2, "{error}");

    // legalhold: a bucket-only target.
    let (_scratch, located) = no_server_root();
    let legalhold::Verb::Set(args) = legalhold_verb(&["tinio", "legalhold", "set", "photos"])
    else {
        panic!("the set verb");
    };
    let error = legalhold::run(
        legalhold::Args {
            verb: legalhold::Verb::Set(args),
        },
        located,
    )
    .await
    .expect_err("the misuse");
    assert_eq!(error.exit_code(), 2, "{error}");

    // object-lock: an object target.
    let (_scratch, located) = no_server_root();
    let object_lock::Verb::Enable(args) =
        object_lock_verb(&["tinio", "object-lock", "enable", "photos/a.txt"]);
    let error = object_lock::run(
        object_lock::Args {
            verb: object_lock::Verb::Enable(args),
        },
        located,
    )
    .await
    .expect_err("the misuse");
    assert_eq!(error.exit_code(), 2, "{error}");
}
