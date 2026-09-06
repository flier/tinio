//! Step definitions of the acl.feature scenarios (spec 2026-09-05,
//! Task 15): the signed-request wrappers over the SigV4 signer —
//! authenticate-requiring requests sign with the fixture identity's
//! users (`signed as "root"` / `signed as "user-b"`), while every
//! anonymous leg stays driver-level unsigned (the shared raw steps).
//! The response-status / error-code / body assertions are the shared
//! steps in common.rs + errors.rs.

use cucumber::{gherkin::Step, given, then, when};

use super::common::{ACL_ROOT, ACL_USER_B};
use super::errors::table_headers;

/// The fixture user whose credentials answer a `signed as "…"` step:
/// `root` = the configured `[auth]` pair presenting the `[owner]`
/// element, `user-b` = the `[[users]]` member (the same access-key
/// names the @acl plane's config-driven identity mounts).
fn credentials(user: &str) -> (&'static str, &'static str) {
    match user {
        "root" => ACL_ROOT,
        "user-b" => ACL_USER_B,
        other => panic!("unknown fixture user: {other}"),
    }
}

/// A signed raw request whose headers come from a data table (the
/// single-row tables the features use) — the signed twin of the shared
/// `I send a … with headers` step.
#[given(regex = r#"I send a "(\w+)" request to "([^"]+)" signed as "([^"]+)" with headers$"#)]
#[when(regex = r#"I send a "(\w+)" request to "([^"]+)" signed as "([^"]+)" with headers$"#)]
#[then(regex = r#"I send a "(\w+)" request to "([^"]+)" signed as "([^"]+)" with headers$"#)]
async fn signed_request_with_headers(
    world: &mut super::World,
    method: String,
    path: String,
    user: String,
    step: &Step,
) {
    let headers = table_headers(world, step);
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    world.last = world
        .client
        .signed_request(&method, &path, &refs, &[], credentials(&user))
        .await;
}

/// A signed raw request with both a header table and a body (the
/// wire-XML bodies of the delete/leg requests).
#[given(
    regex = r#"I send a "(\w+)" request to "([^"]+)" signed as "([^"]+)" with headers and body "([^"]*)""#
)]
#[when(
    regex = r#"I send a "(\w+)" request to "([^"]+)" signed as "([^"]+)" with headers and body "([^"]*)""#
)]
#[then(
    regex = r#"I send a "(\w+)" request to "([^"]+)" signed as "([^"]+)" with headers and body "([^"]*)""#
)]
async fn signed_request_with_headers_and_body(
    world: &mut super::World,
    method: String,
    path: String,
    user: String,
    body: String,
    step: &Step,
) {
    let headers = table_headers(world, step);
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    world.last = world
        .client
        .signed_request(&method, &path, &refs, body.as_bytes(), credentials(&user))
        .await;
}

/// A signed raw request without a body or extra headers (the
/// list_buckets legs).
#[given(regex = r#"I send a "(\w+)" request to "([^"]+)" signed as "([^"]+)"$"#)]
#[when(regex = r#"I send a "(\w+)" request to "([^"]+)" signed as "([^"]+)"$"#)]
#[then(regex = r#"I send a "(\w+)" request to "([^"]+)" signed as "([^"]+)"$"#)]
async fn signed_request(world: &mut super::World, method: String, path: String, user: String) {
    world.last = world
        .client
        .signed_request(&method, &path, &[], &[], credentials(&user))
        .await;
}
