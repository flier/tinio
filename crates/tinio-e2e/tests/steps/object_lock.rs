//! The Object Lock steps (FR-043): the bucket configuration and the two
//! per-object XML writes, plus the `CreateBucket` header.
//!
//! **The raw requests are the shared ones** (`errors.rs`): `?object-lock`,
//! `?retention` and `?legal-hold` are ordinary bucket/object paths, so the
//! `GET`s and the header-carrying writes need no step of their own. What the
//! corpus cannot spell is here — the three XML documents as docstrings with
//! their own checksum, and the one-step `CreateBucket` header.
//!
//! **Why the documents carry a computed `Content-MD5`**: the contract requires
//! a checksum on the bucket configuration (and Object Lock's XML writes are
//! presence-only — s3s's deserializer consumes the body before the handler
//! runs, the deviation `backend/object_lock.rs` records). Computing the real
//! digest keeps the step honest against a future that does compare it.

use cucumber::{gherkin::Step, given, when};

use super::{common::put_xml, errors::subst};

/// `PUT /<bucket>` with `x-amz-bucket-object-lock-enabled: true` — the
/// one-step enablement (OL1/OL13). The write is a fixture for the scenario
/// that follows, so a refusal panics here, naming the answer: a scenario whose
/// bucket stayed unlocked would otherwise fail in a later assertion with a
/// misleading message.
#[given(expr = "I create bucket {string} with the object lock header")]
#[when(expr = "I create bucket {string} with the object lock header")]
async fn create_bucket_with_lock(world: &mut super::World, name: String) {
    world.last = world
        .client
        .request(
            "PUT",
            &format!("/{name}"),
            &[("x-amz-bucket-object-lock-enabled", "true")],
            &[],
        )
        .await;
    assert_eq!(
        world.last.status,
        200,
        "the CreateBucket Object Lock header was not accepted: {} {}",
        world.last.status,
        String::from_utf8_lossy(&world.last.body)
    );
}

/// `PUT /<bucket>?object-lock` with the document the step carries, replacing
/// the bucket's whole configuration.
#[given(expr = "I put this object lock configuration on bucket {string}")]
#[when(expr = "I put this object lock configuration on bucket {string}")]
async fn put_object_lock_configuration(world: &mut super::World, bucket: String, step: &Step) {
    put_xml(world, format!("/{bucket}?object-lock"), step).await;
}

/// `PUT /<object>?retention` with the `Retention` document.
#[given(expr = "I put this retention on object {string}")]
#[when(expr = "I put this retention on object {string}")]
async fn put_retention(world: &mut super::World, object: String, step: &Step) {
    put_xml(world, format!("/{object}?retention"), step).await;
}

/// The version-addressed form; `version` may be the literal `null` (the
/// identity wire OL18 keys on) or a stored header value (`{notes}`).
#[given(expr = "I put this retention on object {string} at version {string}")]
#[when(expr = "I put this retention on object {string} at version {string}")]
async fn put_retention_at(world: &mut super::World, object: String, version: String, step: &Step) {
    let version = subst(world, &version);
    put_xml(
        world,
        format!("/{object}?retention&versionId={version}"),
        step,
    )
    .await;
}

/// `PUT /<object>?legal-hold` with the `LegalHold` document.
#[given(expr = "I put this legal hold on object {string}")]
#[when(expr = "I put this legal hold on object {string}")]
async fn put_legal_hold(world: &mut super::World, object: String, step: &Step) {
    put_xml(world, format!("/{object}?legal-hold"), step).await;
}

/// The version-addressed legal-hold form.
#[given(expr = "I put this legal hold on object {string} at version {string}")]
#[when(expr = "I put this legal hold on object {string} at version {string}")]
async fn put_legal_hold_at(world: &mut super::World, object: String, version: String, step: &Step) {
    let version = subst(world, &version);
    put_xml(
        world,
        format!("/{object}?legal-hold&versionId={version}"),
        step,
    )
    .await;
}

/// The multipart boundary and file bytes of the form upload (fixed, so the
/// body is deterministic and the object assertion can name the bytes).
const FORM_BOUNDARY: &str = "tinio-object-lock-form-boundary";
const FORM_FILE_BODY: &str = "post-body";

/// `POST /<bucket>` as a browser form (`multipart/form-data`): every table row
/// is one form field, and the `file` part is appended last — the shape s3s's
/// POST parser requires. This is OL1's `PostObject` path, where the lock
/// arrives as mapped form fields (`x-amz-object-lock-*`, `content-md5`) rather
/// than headers; the bucket is the path, the key is the `key` field.
#[given(expr = "I post an object form to bucket {string}")]
#[when(expr = "I post an object form to bucket {string}")]
async fn post_object_form(world: &mut super::World, bucket: String, step: &Step) {
    let table = step.table().expect("the form step carries a field table");
    let mut body = String::new();
    for row in &table.rows {
        let value = subst(world, &row[1]);
        body.push_str(&format!(
            "--{FORM_BOUNDARY}\r\nContent-Disposition: form-data; name=\"{}\"\r\n\r\n{value}\r\n",
            row[0]
        ));
    }
    body.push_str(&format!(
        "--{FORM_BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"post.txt\"\r\nContent-Type: text/plain\r\n\r\n{FORM_FILE_BODY}\r\n--{FORM_BOUNDARY}--\r\n"
    ));
    let content_type = format!("multipart/form-data; boundary={FORM_BOUNDARY}");
    world.last = world
        .client
        .request(
            "POST",
            &format!("/{bucket}"),
            &[("Content-Type", content_type.as_str())],
            body.as_bytes(),
        )
        .await;
}
