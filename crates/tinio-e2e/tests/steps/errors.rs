//! Raw wire-level requests and error-response assertions (SC-004), ported
//! from `tinio-server/tests/error_codes.rs`.
//!
//! Cucumber 0.23 matches steps per keyword (given/when/then maps), so the
//! raw-request steps are registered under every keyword a feature uses
//! them with (`Given I send a "PUT" request …`, `When I send a "PUT" request
//! … with body`, `Then I send a "GET" request …`).

use cucumber::{gherkin::Step, given, then, when};

/// A raw request without a body. The feature quotes the method
/// (`I send a "PUT" request to …`), so the `{word}` capture includes the
/// surrounding quotes; strip them before sending.
/// Substitute the world's captured values into a step string: `{upload_id}`
/// the scenario's multipart upload id, and any `{name}` a header captured
/// by `the response header … is stored` — `{etag}` is just such a header
/// (matched case-insensitively against the stored names, so a stored
/// `ETag` substitutes `{etag}`). Applied to the raw steps' paths, bodies,
/// and header-table values.
fn subst(world: &super::World, text: &str) -> String {
    let mut out = text.to_string();
    if out.contains("{upload_id}") {
        out = out.replace("{upload_id}", &world.mp.upload_id);
    }
    for (name, value) in &world.stored_headers {
        // The raw client lowercases response header names; the step text
        // may spell them either way.
        let key = format!("{{{}}}", name.to_lowercase());
        if out.contains(&key) {
            out = out.replace(&key, value);
        }
    }
    out
}

/// The with-headers steps' shared preamble: the data-table rows, each
/// value with the world's captured substitutions applied, as the header
/// slice the raw client takes.
fn table_headers(world: &super::World, step: &Step) -> Vec<(String, String)> {
    let table = step.table().expect("the with-headers step carries a table");
    table
        .rows
        .iter()
        .map(|row| (row[0].clone(), subst(world, &row[1])))
        .collect()
}

#[given(expr = "I send a {word} request to {string}")]
#[when(expr = "I send a {word} request to {string}")]
#[then(expr = "I send a {word} request to {string}")]
async fn raw_request(world: &mut super::World, method: String, path: String) {
    let method = method.trim_matches('"');
    let path = subst(world, &path);
    world.last = world.client.request(method, &path, &[], &[]).await;
}

/// A raw request with a body.
#[given(regex = r#"I send a "(\w+)" request to "([^"]+)" with body "([^"]*)""#)]
#[when(regex = r#"I send a "(\w+)" request to "([^"]+)" with body "([^"]*)""#)]
#[then(regex = r#"I send a "(\w+)" request to "([^"]+)" with body "([^"]*)""#)]
async fn raw_request_with_body(
    world: &mut super::World,
    method: String,
    path: String,
    body: String,
) {
    let path = subst(world, &path);
    let body = subst(world, &body);
    world.last = world
        .client
        .request(&method, &path, &[], body.as_bytes())
        .await;
}

/// A raw request whose headers come from a data table (the single-row
/// tables the features use: the header row doubles as the data row).
/// Captured values substitute into the path, body and table values:
/// `{etag}` (grilling Q3), `{upload_id}` and any `{name}` stored by
/// `the response header … is stored`.
///
/// Cucumber 0.23 has no `Table` step-arg wiring, so the table rides on
/// the `step` context (the codegen special-cases an argument named
/// `step`). The regex is end-anchored so it never prefix-matches the
/// `with headers and body` variant below (cucumber matches raw regexes
/// unanchored, and two matches for one step text are ambiguous).
#[given(regex = r#"I send a "(\w+)" request to "([^"]+)" with headers$"#)]
#[when(regex = r#"I send a "(\w+)" request to "([^"]+)" with headers$"#)]
#[then(regex = r#"I send a "(\w+)" request to "([^"]+)" with headers$"#)]
async fn raw_request_with_headers(
    world: &mut super::World,
    method: String,
    path: String,
    step: &Step,
) {
    let headers = table_headers(world, step);
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let path = subst(world, &path);
    world.last = world.client.request(&method, &path, &refs, &[]).await;
}

/// A raw request with both headers (a data table) and a body — the
/// wire-XML requests (e.g. the DeleteObjects quiet-mode body) that carry
/// a `Content-Type`. Same table semantics as the with-headers step above.
#[given(regex = r#"I send a "(\w+)" request to "([^"]+)" with headers and body "([^"]*)""#)]
#[when(regex = r#"I send a "(\w+)" request to "([^"]+)" with headers and body "([^"]*)""#)]
#[then(regex = r#"I send a "(\w+)" request to "([^"]+)" with headers and body "([^"]*)""#)]
async fn raw_request_with_headers_and_body(
    world: &mut super::World,
    method: String,
    path: String,
    body: String,
    step: &Step,
) {
    let headers = table_headers(world, step);
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let path = subst(world, &path);
    let body = subst(world, &body);
    world.last = world
        .client
        .request(&method, &path, &refs, body.as_bytes())
        .await;
}

#[then(expr = "the error code is {string}")]
async fn error_code_is(world: &mut super::World, code: String) {
    let text = String::from_utf8_lossy(&world.last.body);
    let found = super::common::extract(&text, "<Code>", "</Code>");
    assert_eq!(found, code, "S3 <Code> mismatch in body: {text}");
}

#[then(expr = "the response header {string} is {string}")]
async fn header_is(world: &mut super::World, name: String, value: String) {
    let found = world.last.header(&name).map(str::to_string);
    assert_eq!(found.as_deref(), Some(value.as_str()), "header {name}");
}

#[then(expr = "the response header {string} is stored")]
async fn header_stored(world: &mut super::World, name: String) {
    // Saves the header value for later steps (the conditional-request
    // scenarios' `{etag}` substitution — the stored names are matched
    // case-insensitively, so an `ETag` store answers `{etag}`).
    let value = world
        .last
        .header(&name)
        .expect("header must be present")
        .to_string();
    world.stored_headers.insert(name, value);
}

/// The header must not appear on the response — e.g. user `x-amz-meta-*`
/// headers are accepted on upload but dropped (never echoed, never
/// stored): the contract behavior is observable as an absent header.
#[then(expr = "the response header {string} is absent")]
async fn header_absent(world: &mut super::World, name: String) {
    assert!(
        world.last.header(&name).is_none(),
        "header {name} must be absent"
    );
}

#[then("the error code is not empty")]
async fn error_code_present(world: &mut super::World) {
    let text = String::from_utf8_lossy(&world.last.body);
    // `extract` yields "" for a missing `<Code>` element (or an empty
    // one) — a body without a code fails this step.
    assert!(
        !super::common::extract(&text, "<Code>", "</Code>").is_empty(),
        "no <Code> element in body: {text}"
    );
}

/// The traversal-proof assertion: the parent dir of the served root holds
/// only the root itself — no rejected key escaped the root.
///
/// Assumes the `@nested-root` server shape ([`Server::fs_nested`]): the
/// parent of the served root is a controlled tempdir, so "nothing outside"
/// is observable as "the parent holds exactly the root dir".
#[then("no file was written outside the served root")]
async fn nothing_outside_root(world: &mut super::World) {
    let root = world
        .server
        .as_ref()
        .expect("server running")
        .root()
        .expect("fs-backed server root");
    let parent = root.parent().expect("served root has a parent dir");
    let entries = super::common::sorted_entries(parent).await;
    let root_name = root.file_name().unwrap().to_string_lossy().into_owned();
    assert_eq!(
        entries,
        [root_name],
        "nothing may be written next to the served root"
    );
}
