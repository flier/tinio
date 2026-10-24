//! The legal-hold tests: `legalhold set`, `legalhold clear`, and `legalhold info`.

use super::fixtures::{Call, Fake, body, legalhold_verb, object_put_call};
use crate::commands::legalhold;

/// `legalhold set` and `clear` write `ON` and `OFF`, leaving the retention member absent.
#[tokio::test]
async fn legalhold_set_and_clear_write_on_and_off() {
    for (argv, expected, shape) in [
        (
            &["tinio", "legalhold", "set", "photos/report.txt", "--json"][..],
            "ON",
            r#"{"legalhold":"ON","urlpath":"photos/report.txt","key":"report.txt","versionID":""}"#,
        ),
        (
            &["tinio", "legalhold", "clear", "photos/report.txt", "--json"][..],
            "OFF",
            r#"{"legalhold":"OFF","urlpath":"photos/report.txt","key":"report.txt","versionID":""}"#,
        ),
    ] {
        let fake = Fake::default();
        fake.object_put_answers(204, "");
        let outcome = match *argv {
            ["tinio", "legalhold", "set", ..] => {
                let legalhold::Verb::Set(args) = legalhold_verb(argv) else {
                    panic!("the set verb");
                };
                legalhold::set::execute(&args, &fake).await
            }
            _ => {
                let legalhold::Verb::Clear(args) = legalhold_verb(argv) else {
                    panic!("the clear verb");
                };
                legalhold::clear::execute(&args, &fake).await
            }
        }
        .expect("the hold write");
        assert!(outcome.succeeded());
        assert_eq!(outcome.text(), shape);
        let calls = fake.calls();
        let (_, body) = object_put_call(&calls);
        assert_eq!(body["key"], "report.txt");
        assert_eq!(body["version"], "current");
        assert_eq!(body["legal_hold"], expected);
        assert!(body.get("retention").is_none(), "{body}");
        assert!(body.get("bypass").is_none(), "{body}");
    }
}

/// `legalhold info` renders the hold, and the plane's `no retention` absence means `OFF`: the
/// version exists and holds no hold.
#[tokio::test]
async fn legalhold_info_reads_the_hold_and_treats_no_facts_as_off() {
    let fake = Fake::default();
    fake.object_get_answers(200, r#"{"retention":null,"legal_hold":"ON"}"#);
    let legalhold::Verb::Info(args) =
        legalhold_verb(&["tinio", "legalhold", "info", "photos/report.txt", "--json"])
    else {
        panic!("the info verb");
    };
    let outcome = legalhold::info::execute(&args, &fake)
        .await
        .expect("the info");
    assert_eq!(
        outcome.text(),
        r#"{"legalhold":"ON","urlpath":"photos/report.txt","key":"report.txt","versionID":""}"#
    );

    let fake = Fake::default();
    fake.object_get_answers(404, r#"{"error":"no retention"}"#);
    let legalhold::Verb::Info(args) =
        legalhold_verb(&["tinio", "legalhold", "info", "photos/report.txt"])
    else {
        panic!("the info verb");
    };
    let outcome = legalhold::info::execute(&args, &fake)
        .await
        .expect("the absence is OFF");
    assert!(outcome.succeeded());
    assert_eq!(outcome.text(), "legal hold for photos/report.txt: OFF");

    // Any other 404 names a subject this verb cannot report on.
    let fake = Fake::default();
    fake.object_get_answers(404, r#"{"error":"no such version"}"#);
    let legalhold::Verb::Info(args) =
        legalhold_verb(&["tinio", "legalhold", "info", "photos/report.txt"])
    else {
        panic!("the info verb");
    };
    let error = legalhold::info::execute(&args, &fake)
        .await
        .expect_err("a missing version");
    assert!(error.to_string().contains("no such version"), "{error}");
}

/// The addressed version travels to `legalhold info` and back into the document: the JSON
/// `versionID` and the request body's `version` are the same id, not the current address.
#[tokio::test]
async fn legalhold_info_addresses_the_named_version_in_json() {
    let fake = Fake::default();
    fake.object_get_answers(200, r#"{"retention":null,"legal_hold":"ON"}"#);
    let legalhold::Verb::Info(args) = legalhold_verb(&[
        "tinio",
        "legalhold",
        "info",
        "photos/report.txt",
        "--version-id",
        "abc",
        "--json",
    ]) else {
        panic!("the info verb");
    };
    let outcome = legalhold::info::execute(&args, &fake)
        .await
        .expect("the info");
    assert!(outcome.succeeded());
    assert_eq!(
        outcome.text(),
        r#"{"legalhold":"ON","urlpath":"photos/report.txt","key":"report.txt","versionID":"abc"}"#
    );
    let calls = fake.calls();
    let [Call::ObjectGet(bucket, raw)] = &calls[..] else {
        panic!("expected one object GET, got {calls:?}");
    };
    assert_eq!(bucket, "photos");
    let sent = body(raw);
    assert_eq!(sent["key"], "report.txt");
    assert_eq!(sent["version"], "abc");
}
