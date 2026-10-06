//! Versioning steps (spec 2026-10-03-s3-versioning, plan 3's Task 8): the two
//! `?versioning` writes and the two version-listing assertions the versioning
//! journeys need, plus the named header-capture those scenarios use and the
//! generic header-presence assertion the harness keeps as the fallback for a
//! literal that cannot be predicted (every current caller pins its value with
//! `the response header "…" is "…"` instead).
//!
//! The listing carries metadata, never an object's bytes, which is why the
//! scenarios assert on element counts and the `IsLatest` position and never on
//! content: `the version listing has …` counts the `<Version>` and
//! `<DeleteMarker>` elements, and `the first version in the listing is the
//! latest` pins "newest first" — the first `<IsLatest>` element in document
//! order is `true` — without parsing version ids.
//!
//! The writes go through the same raw client every other step uses: the
//! in-process plane has no identity, so these steps serve the versioned
//! scenarios that run against it; the `@cli` leg drives the spawned binary's
//! identity-enforced endpoint with the existing `signed as "root"` steps.

use cucumber::{given, then, when};

/// The `VersioningConfiguration` body of one state — the wire XML s3s's
/// deserializer takes (no namespace is required, the tagging bodies'
/// precedent).
fn versioning_body(status: &str) -> String {
    format!("<VersioningConfiguration><Status>{status}</Status></VersioningConfiguration>")
}

/// `PUT /<bucket>?versioning` with the given status. The write is a fixture
/// for the scenario that follows, so a refusal panics here, naming the status
/// answer: a scenario whose bucket stayed unversioned would otherwise fail in
/// a later assertion with a misleading message.
async fn set_versioning(world: &mut super::World, bucket: String, status: &str) {
    let body = versioning_body(status);
    world.last = world
        .client
        .request(
            "PUT",
            &format!("/{bucket}?versioning"),
            &[("Content-Type", "application/xml")],
            body.as_bytes(),
        )
        .await;
    assert_eq!(
        world.last.status,
        200,
        "the `{status}` versioning write was not accepted: {} {}",
        world.last.status,
        String::from_utf8_lossy(&world.last.body)
    );
}

#[given(expr = "I enable versioning on {string}")]
#[when(expr = "I enable versioning on {string}")]
async fn enable_versioning(world: &mut super::World, bucket: String) {
    set_versioning(world, bucket, "Enabled").await;
}

#[given(expr = "I suspend versioning on {string}")]
#[when(expr = "I suspend versioning on {string}")]
async fn suspend_versioning(world: &mut super::World, bucket: String) {
    set_versioning(world, bucket, "Suspended").await;
}

/// The listing's two element counts (the last response must be a
/// `ListObjectVersions` page).
#[then(expr = "the version listing has {int} versions and {int} delete markers")]
async fn version_listing_counts(world: &mut super::World, versions: usize, markers: usize) {
    let body = String::from_utf8_lossy(&world.last.body).into_owned();
    assert_eq!(
        super::common::count_tag(&world.last.body, "<Version>"),
        versions,
        "the listing's <Version> count; body: {body}"
    );
    assert_eq!(
        super::common::count_tag(&world.last.body, "<DeleteMarker>"),
        markers,
        "the listing's <DeleteMarker> count; body: {body}"
    );
}

/// The listing's first entry (in document order) is the latest — the
/// "newest first" pin.
#[then("the first version in the listing is the latest")]
async fn first_version_is_latest(world: &mut super::World) {
    let body = String::from_utf8_lossy(&world.last.body).into_owned();
    assert_eq!(
        super::common::extract(&body, "<IsLatest>", "</IsLatest>"),
        "true",
        "the first <IsLatest> in the listing; body: {body}"
    );
}

/// The response carries `name` — the presence form of the header assertions
/// (values with an interesting literal are pinned by the steps that name
/// them, e.g. `the response header "…" is "…"`).
#[then(expr = "the response has a {string} header")]
async fn response_has_header(world: &mut super::World, name: String) {
    assert!(
        world.last.header(&name).is_some(),
        "header {name} must be present; headers: {:?}",
        world.last.headers
    );
}

/// Store a response header's value under a chosen name — the named sibling
/// of `the response header … is stored` (errors.rs), which keys by the
/// header's own name. The version-addressed delete journey captures three
/// `x-amz-version-id` answers in one scenario (two content versions and the
/// delete marker), so each needs a name of its own; `subst` (errors.rs)
/// substitutes the stored names, case-insensitively, into later steps'
/// paths, bodies and header tables.
#[then(expr = "the response header {string} is stored as {string}")]
async fn response_header_stored_as(world: &mut super::World, name: String, key: String) {
    let value = world
        .last
        .header(&name)
        .unwrap_or_else(|| {
            panic!(
                "header {name} must be present; headers: {:?}",
                world.last.headers
            )
        })
        .to_string();
    world.stored_headers.insert(key, value);
}
