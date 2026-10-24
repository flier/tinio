//! The keyed retention tests: `retention set`, `retention clear`, and `retention info
//! <bucket/key>`, plus the keyed refusal documents.

use super::fixtures::{Call, Fake, legalhold_verb, object_put_call, retention_verb};
use crate::commands::{legalhold, retention};

/// A keyed `set` addresses the version in the body and carries `--bypass`; without the flag the
/// member is omitted (the plane reads absent as `false`).
#[tokio::test]
async fn set_writes_the_retention_address_and_bypass() {
    let fake = Fake::default();
    fake.object_put_answers(204, "");
    let retention::Verb::Set(args) = retention_verb(&[
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
    let outcome = retention::set::execute(&args, &fake)
        .await
        .expect("the set");
    assert!(outcome.succeeded());
    let calls = fake.calls();
    let (bucket, body) = object_put_call(&calls);
    assert_eq!(bucket, "photos");
    assert_eq!(body["key"], "report.txt");
    assert_eq!(body["version"], "abc");
    assert_eq!(body["retention"]["mode"], "GOVERNANCE");
    assert!(
        body["retention"]["retain_until"]
            .as_str()
            .expect("an RFC3339 instant")
            .ends_with('Z')
    );
    assert_eq!(body["bypass"], true);

    let fake = Fake::default();
    fake.object_put_answers(204, "");
    let retention::Verb::Set(args) = retention_verb(&[
        "tinio",
        "retention",
        "set",
        "photos/report.txt",
        "governance",
        "1d",
    ]) else {
        panic!("the set verb");
    };
    retention::set::execute(&args, &fake)
        .await
        .expect("the set");
    let (_, body) = object_put_call(&fake.calls());
    assert_eq!(body["version"], "current");
    assert!(
        body.get("bypass").is_none(),
        "an absent --bypass is omitted, not false: {body}"
    );
}

/// A keyed `clear` sends an explicit `null` retention and the implicit governance bypass, and
/// does not touch the legal hold.
#[tokio::test]
async fn clear_writes_an_explicit_null_with_the_implicit_bypass() {
    let fake = Fake::default();
    fake.object_put_answers(204, "");
    let retention::Verb::Clear(args) =
        retention_verb(&["tinio", "retention", "clear", "photos/report.txt", "--json"])
    else {
        panic!("the clear verb");
    };
    let outcome = retention::clear::execute(&args, &fake)
        .await
        .expect("the clear");
    assert_eq!(
        outcome.text(),
        r#"{"mode":"","until":"","urlpath":"photos/report.txt","versionID":"","status":"success","error":""}"#
    );
    let calls = fake.calls();
    let (_, body) = object_put_call(&calls);
    assert_eq!(body["key"], "report.txt");
    assert_eq!(body["version"], "current");
    assert!(body["retention"].is_null(), "{body}");
    assert_eq!(body["bypass"], true);
    assert!(body.get("legal_hold").is_none(), "{body}");
}

/// `retention info <bucket/key>` addresses the version in the body and renders `mc`'s retention
/// shape with camel-case `versionID`.
#[tokio::test]
async fn retention_info_reads_a_version() {
    let fake = Fake::default();
    fake.object_get_answers(
        200,
        r#"{"retention":{"mode":"GOVERNANCE","retain_until":"2030-01-01T00:00:00Z"},"legal_hold":"OFF"}"#,
    );
    let retention::Verb::Info(args) = retention_verb(&[
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
    let outcome = retention::info::execute(&args, &fake)
        .await
        .expect("the info");
    assert!(outcome.succeeded());
    assert_eq!(
        outcome.text(),
        r#"{"mode":"GOVERNANCE","until":"2030-01-01T00:00:00Z","urlpath":"photos/report.txt","versionID":"abc","status":"success","error":""}"#
    );
    assert_eq!(
        fake.calls(),
        vec![Call::ObjectGet(
            "photos".to_owned(),
            r#"{"key":"report.txt","version":"abc"}"#.to_owned()
        )]
    );
}

/// A version with no retention reports the `status:"error"` retention shape under `--json` and
/// an operational error otherwise.
#[tokio::test]
async fn retention_info_without_a_retention_reports_the_error_shape() {
    let fake = Fake::default();
    fake.object_get_answers(200, r#"{"retention":null,"legal_hold":"ON"}"#);
    let retention::Verb::Info(args) =
        retention_verb(&["tinio", "retention", "info", "photos/report.txt", "--json"])
    else {
        panic!("the info verb");
    };
    let outcome = retention::info::execute(&args, &fake)
        .await
        .expect("the error shape");
    assert!(!outcome.succeeded());
    assert_eq!(
        outcome.text(),
        r#"{"mode":"","until":"","urlpath":"photos/report.txt","versionID":"","status":"error","error":"no retention"}"#
    );

    let fake = Fake::default();
    fake.object_get_answers(200, r#"{"retention":null,"legal_hold":"ON"}"#);
    let retention::Verb::Info(args) =
        retention_verb(&["tinio", "retention", "info", "photos/report.txt"])
    else {
        panic!("the info verb");
    };
    let error = retention::info::execute(&args, &fake)
        .await
        .expect_err("no retention");
    assert_eq!(error.exit_code(), 1);
}

/// A plane refusal is the plane's sentence, an operational error rather than a panic.
#[tokio::test]
async fn a_plane_refusal_is_an_operational_error() {
    let fake = Fake::default();
    fake.object_put_answers(403, r#"{"error":"access denied"}"#);
    let retention::Verb::Set(args) = retention_verb(&[
        "tinio",
        "retention",
        "set",
        "photos/report.txt",
        "compliance",
        "1y",
    ]) else {
        panic!("the set verb");
    };
    let error = retention::set::execute(&args, &fake)
        .await
        .expect_err("the refusal");
    assert_eq!(error.exit_code(), 1);
    assert!(error.to_string().contains("access denied"), "{error}");
}

/// A keyed mutation's refusal under `--json` renders the retention `mc` shape (`status:"error"`,
/// `error`), printed and exit 1; the legal hold's shape has no `status`/`error` field, so it uses
/// the one-field `{"error": …}` document.
#[tokio::test]
async fn keyed_mutation_refusals_under_json_render_an_error_document() {
    let fake = Fake::default();
    fake.object_put_answers(403, r#"{"error":"access denied"}"#);
    let retention::Verb::Set(args) = retention_verb(&[
        "tinio",
        "retention",
        "set",
        "photos/report.txt",
        "compliance",
        "1y",
        "--json",
    ]) else {
        panic!("the set verb");
    };
    let outcome = retention::set::execute(&args, &fake)
        .await
        .expect("the refusal document");
    assert!(!outcome.succeeded());
    assert_eq!(
        outcome.text(),
        r#"{"mode":"","until":"","urlpath":"photos/report.txt","versionID":"","status":"error","error":"access denied"}"#
    );

    let fake = Fake::default();
    fake.object_put_answers(403, r#"{"error":"access denied"}"#);
    let retention::Verb::Clear(args) =
        retention_verb(&["tinio", "retention", "clear", "photos/report.txt", "--json"])
    else {
        panic!("the clear verb");
    };
    let outcome = retention::clear::execute(&args, &fake)
        .await
        .expect("the refusal document");
    assert!(!outcome.succeeded());
    assert_eq!(
        outcome.text(),
        r#"{"mode":"","until":"","urlpath":"photos/report.txt","versionID":"","status":"error","error":"access denied"}"#
    );

    let fake = Fake::default();
    fake.object_put_answers(403, r#"{"error":"access denied"}"#);
    let legalhold::Verb::Set(args) =
        legalhold_verb(&["tinio", "legalhold", "set", "photos/report.txt", "--json"])
    else {
        panic!("the set verb");
    };
    let outcome = legalhold::set::execute(&args, &fake)
        .await
        .expect("the refusal document");
    assert!(!outcome.succeeded());
    assert_eq!(outcome.text(), r#"{"error":"access denied"}"#);
}
