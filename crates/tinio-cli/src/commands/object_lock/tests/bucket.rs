//! The bucket-configuration tests: `object-lock enable`'s enable path, `retention set|clear
//! --default`'s bucket scope, `retention info <bucket>`, and the bucket shape's rendering.

use super::fixtures::{Call, Fake, object_lock_verb, retention_verb};
use crate::{
    _core::object_lock as model,
    commands::{legalhold::hold_label, object_lock, retention},
};

/// `object-lock enable` writes through the plane's enable path — the bucket `PUT` whose admin
/// enables versioning first when the bucket is unversioned. The CLI makes no versioning call of
/// its own because it has no store handle (OL3, OL24).
#[tokio::test]
async fn enable_writes_the_plane_enable_configuration() {
    let fake = Fake::default();
    fake.bucket_get_answers(404, r#"{"error":"no object lock configuration"}"#);
    fake.bucket_put_answers(204, "");
    let object_lock::Verb::Enable(args) =
        object_lock_verb(&["tinio", "object-lock", "enable", "photos"]);
    let outcome = object_lock::enable::execute(&args, &fake)
        .await
        .expect("the enable path");
    assert!(outcome.succeeded());
    assert_eq!(
        fake.calls(),
        vec![
            Call::BucketGet("photos".to_owned()),
            Call::BucketPut(
                "photos".to_owned(),
                r#"{"enabled":true,"rule":null}"#.to_owned()
            )
        ]
    );

    // The bucket-configuration shape for the enable verb: `mc`'s `{op, enabled, mode, validity}`.
    let fake = Fake::default();
    fake.bucket_get_answers(404, r#"{"error":"no object lock configuration"}"#);
    fake.bucket_put_answers(204, "");
    let object_lock::Verb::Enable(args) =
        object_lock_verb(&["tinio", "object-lock", "enable", "photos", "--json"]);
    let outcome = object_lock::enable::execute(&args, &fake)
        .await
        .expect("the enable path");
    assert_eq!(
        outcome.text(),
        r#"{"op":"enable","enabled":true,"mode":"","validity":""}"#
    );
}

/// Minor 8: enabling an already-configured bucket must not clear its default
/// rule — the first read carries the stored rule into the replace-all `PUT`.
#[tokio::test]
async fn enable_preserves_an_existing_default_rule() {
    let fake = Fake::default();
    fake.bucket_get_answers(
        200,
        r#"{"enabled":true,"rule":{"default_retention":{"mode":"COMPLIANCE","period":{"days":30}}}}"#,
    );
    fake.bucket_put_answers(204, "");
    let object_lock::Verb::Enable(args) =
        object_lock_verb(&["tinio", "object-lock", "enable", "photos", "--json"]);
    let outcome = object_lock::enable::execute(&args, &fake)
        .await
        .expect("the enable path");
    assert_eq!(
        fake.calls(),
        vec![
            Call::BucketGet("photos".to_owned()),
            Call::BucketPut(
                "photos".to_owned(),
                r#"{"enabled":true,"rule":{"default_retention":{"mode":"COMPLIANCE","period":{"days":30}}}}"#
                    .to_owned()
            )
        ]
    );
    assert_eq!(
        outcome.text(),
        r#"{"op":"enable","enabled":true,"mode":"COMPLIANCE","validity":"30DAYS"}"#
    );
}

/// `retention set --default` also uses the enable path: one bucket `PUT` carrying the rule, which
/// is how the versioning step reaches the admin (OL24).
#[tokio::test]
async fn set_default_writes_the_plane_enable_configuration() {
    let fake = Fake::default();
    fake.bucket_put_answers(204, "");
    let retention::Verb::Set(args) = retention_verb(&[
        "tinio",
        "retention",
        "set",
        "photos",
        "--default",
        "governance",
        "30d",
        "--json",
    ]) else {
        panic!("the set verb");
    };
    let outcome = retention::set::execute(&args, &fake)
        .await
        .expect("the enable path");
    assert_eq!(
        outcome.text(),
        r#"{"op":"set","enabled":true,"mode":"GOVERNANCE","validity":"30DAYS"}"#
    );
    assert_eq!(
        fake.calls(),
        vec![Call::BucketPut(
            "photos".to_owned(),
            r#"{"enabled":true,"rule":{"default_retention":{"mode":"GOVERNANCE","period":{"days":30}}}}"#
                .to_owned()
        )]
    );
}

/// `clear --default` on a bucket that is not enabled reports "not enabled" and writes nothing —
/// it must not enable the bucket on the way past (OL24).
#[tokio::test]
async fn clear_default_is_a_no_op_on_a_disabled_bucket() {
    let fake = Fake::default();
    fake.bucket_get_answers(404, r#"{"error":"no object lock configuration"}"#);
    let retention::Verb::Clear(args) =
        retention_verb(&["tinio", "retention", "clear", "photos", "--default"])
    else {
        panic!("the clear verb");
    };
    let outcome = retention::clear::execute(&args, &fake)
        .await
        .expect("the no-op");
    assert!(outcome.succeeded());
    assert!(outcome.text().contains("not enabled"), "{}", outcome.text());
    assert_eq!(fake.calls(), vec![Call::BucketGet("photos".to_owned())]);

    // The same no-op in the bucket-configuration shape: `enabled:false`, still exit 0.
    let fake = Fake::default();
    fake.bucket_get_answers(404, r#"{"error":"no object lock configuration"}"#);
    let retention::Verb::Clear(args) = retention_verb(&[
        "tinio",
        "retention",
        "clear",
        "photos",
        "--default",
        "--json",
    ]) else {
        panic!("the clear verb");
    };
    let outcome = retention::clear::execute(&args, &fake)
        .await
        .expect("the no-op");
    assert!(outcome.succeeded());
    assert_eq!(
        outcome.text(),
        r#"{"op":"clear","enabled":false,"mode":"","validity":""}"#
    );
    assert_eq!(fake.calls(), vec![Call::BucketGet("photos".to_owned())]);
}

/// Only the "not enabled" absence is the no-op: a missing bucket or a refused read is the
/// plane's refusal.
#[tokio::test]
async fn clear_default_refuses_a_bucket_the_plane_cannot_report() {
    let fake = Fake::default();
    fake.bucket_get_answers(404, r#"{"error":"no such bucket"}"#);
    let retention::Verb::Clear(args) =
        retention_verb(&["tinio", "retention", "clear", "photos", "--default"])
    else {
        panic!("the clear verb");
    };
    let error = retention::clear::execute(&args, &fake)
        .await
        .expect_err("a missing bucket");
    assert!(error.to_string().contains("no such bucket"), "{error}");
    assert_eq!(fake.calls(), vec![Call::BucketGet("photos".to_owned())]);
}

/// A stored configuration is read, the rule is dropped and `enabled` stays true.
#[tokio::test]
async fn clear_default_keeps_the_bucket_enabled() {
    let fake = Fake::default();
    fake.bucket_get_answers(
        200,
        r#"{"enabled":true,"rule":{"default_retention":{"mode":"COMPLIANCE","period":{"years":3}}}}"#,
    );
    fake.bucket_put_answers(204, "");
    let retention::Verb::Clear(args) = retention_verb(&[
        "tinio",
        "retention",
        "clear",
        "photos",
        "--default",
        "--json",
    ]) else {
        panic!("the clear verb");
    };
    let outcome = retention::clear::execute(&args, &fake)
        .await
        .expect("the clear");
    assert_eq!(
        outcome.text(),
        r#"{"op":"clear","enabled":true,"mode":"","validity":""}"#
    );
    assert_eq!(
        fake.calls(),
        vec![
            Call::BucketGet("photos".to_owned()),
            Call::BucketPut(
                "photos".to_owned(),
                r#"{"enabled":true,"rule":null}"#.to_owned()
            ),
        ]
    );
}

/// `retention info <bucket>` reads the bucket configuration and renders `mc`'s config shape.
#[tokio::test]
async fn retention_info_reads_the_bucket_configuration() {
    let fake = Fake::default();
    fake.bucket_get_answers(
        200,
        r#"{"enabled":true,"rule":{"default_retention":{"mode":"COMPLIANCE","period":{"years":3}}}}"#,
    );
    let retention::Verb::Info(args) =
        retention_verb(&["tinio", "retention", "info", "photos", "--json"])
    else {
        panic!("the info verb");
    };
    let outcome = retention::info::execute(&args, &fake)
        .await
        .expect("the info");
    assert!(outcome.succeeded());
    assert_eq!(
        outcome.text(),
        r#"{"op":"info","enabled":true,"mode":"COMPLIANCE","validity":"3YEARS"}"#
    );
    assert_eq!(fake.calls(), vec![Call::BucketGet("photos".to_owned())]);

    // The same read without a rule renders empty mode and validity, the `mc` bare-enable shape.
    let fake = Fake::default();
    fake.bucket_get_answers(200, r#"{"enabled":true,"rule":null}"#);
    let retention::Verb::Info(args) =
        retention_verb(&["tinio", "retention", "info", "photos", "--json"])
    else {
        panic!("the info verb");
    };
    let outcome = retention::info::execute(&args, &fake)
        .await
        .expect("the info");
    assert_eq!(
        outcome.text(),
        r#"{"op":"info","enabled":true,"mode":"","validity":""}"#
    );
}

/// A disabled bucket is a successful `info <bucket>` read: the plane's "no configuration" 404
/// renders the same `enabled:false` shape `clear --default` uses, in both output modes, and exits
/// 0. Other refusals remain errors.
#[tokio::test]
async fn retention_info_on_a_disabled_bucket_reports_the_disabled_shape() {
    let fake = Fake::default();
    fake.bucket_get_answers(404, r#"{"error":"no object lock configuration"}"#);
    let retention::Verb::Info(args) =
        retention_verb(&["tinio", "retention", "info", "cold", "--json"])
    else {
        panic!("the info verb");
    };
    let outcome = retention::info::execute(&args, &fake)
        .await
        .expect("a disabled bucket is a successful read");
    assert!(outcome.succeeded());
    assert_eq!(
        outcome.text(),
        r#"{"op":"info","enabled":false,"mode":"","validity":""}"#
    );

    let fake = Fake::default();
    fake.bucket_get_answers(404, r#"{"error":"no object lock configuration"}"#);
    let retention::Verb::Info(args) = retention_verb(&["tinio", "retention", "info", "cold"])
    else {
        panic!("the info verb");
    };
    let outcome = retention::info::execute(&args, &fake)
        .await
        .expect("a disabled bucket is a successful read");
    assert!(outcome.succeeded());
    assert_eq!(outcome.text(), "object lock is not enabled for cold");

    // A missing bucket is not "not enabled": it stays the plane's refusal.
    let fake = Fake::default();
    fake.bucket_get_answers(404, r#"{"error":"no such bucket"}"#);
    let retention::Verb::Info(args) =
        retention_verb(&["tinio", "retention", "info", "cold", "--json"])
    else {
        panic!("the info verb");
    };
    let error = retention::info::execute(&args, &fake)
        .await
        .expect_err("a missing bucket");
    assert_eq!(error.exit_code(), 1);
}

/// A bucket-scope mutation's refusal under `--json` uses the one-field `mc` error document: the
/// bucket configuration's success shape (`op`/`enabled`/`mode`/`validity`) has no error member.
#[tokio::test]
async fn bucket_mutation_refusals_under_json_render_the_error_document() {
    let fake = Fake::default();
    fake.bucket_put_answers(403, r#"{"error":"access denied"}"#);
    let retention::Verb::Set(args) = retention_verb(&[
        "tinio",
        "retention",
        "set",
        "photos",
        "--default",
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
    assert_eq!(outcome.text(), r#"{"error":"access denied"}"#);

    let fake = Fake::default();
    fake.bucket_get_answers(403, r#"{"error":"access denied"}"#);
    let retention::Verb::Clear(args) = retention_verb(&[
        "tinio",
        "retention",
        "clear",
        "photos",
        "--default",
        "--json",
    ]) else {
        panic!("the clear verb");
    };
    let outcome = retention::clear::execute(&args, &fake)
        .await
        .expect("the refusal document");
    assert!(!outcome.succeeded());
    assert_eq!(outcome.text(), r#"{"error":"access denied"}"#);

    // `object-lock enable`'s configuration read is the enabling path's first call; its refusal
    // is the same document.
    let fake = Fake::default();
    fake.bucket_get_answers(500, r#"{"error":"internal server error"}"#);
    // `Verb` has one variant, so the pattern needs no let-else (clippy rejects the dead arm).
    let object_lock::Verb::Enable(args) =
        object_lock_verb(&["tinio", "object-lock", "enable", "photos", "--json"]);
    let outcome = object_lock::enable::execute(&args, &fake)
        .await
        .expect("the refusal document");
    assert!(!outcome.succeeded());
    assert_eq!(outcome.text(), r#"{"error":"internal server error"}"#);
}

/// The `mc` validity spelling: days and years are words in the bucket shape.
#[test]
fn the_bucket_shape_spells_validity_in_words() {
    use crate::commands::plane::validity_label;
    assert_eq!(validity_label(model::Period::Days(30)), "30DAYS");
    assert_eq!(validity_label(model::Period::Years(3)), "3YEARS");
    assert_eq!(model::Mode::Governance.to_string(), "GOVERNANCE");
    assert_eq!(
        hold_label(model::LegalHold::On),
        "ON",
        "the legal-hold shape's spelling"
    );
}
