//! ListObjectsV2 steps (SC-001), ported from the listing legs of
//! `tinio-server/tests/data_plane.rs`: prefix + delimiter grouping and the
//! continuation-token pagination loop.

use cucumber::{given, then, when};

/// The last list request's parameters, for the pagination step to resume.
#[derive(Debug, Clone, Default)]
pub struct ListingState {
    pub bucket: String,
    pub prefix: String,
    pub delimiter: Option<String>,
    pub max_keys: u64,
}

#[given(expr = "I list objects under {string}")]
#[when(expr = "I list objects under {string}")]
#[then(expr = "I list objects under {string}")]
async fn list_objects(world: &mut super::World, key: String) {
    do_list(world, &key, None, 1000).await;
}

#[given(expr = "I list objects under {string} with delimiter {string}")]
#[when(expr = "I list objects under {string} with delimiter {string}")]
#[then(expr = "I list objects under {string} with delimiter {string}")]
async fn list_objects_delim(world: &mut super::World, key: String, delimiter: String) {
    do_list(world, &key, Some(delimiter), 1000).await;
}

#[given(expr = "I list objects under {string} with max-keys {int}")]
#[when(expr = "I list objects under {string} with max-keys {int}")]
#[then(expr = "I list objects under {string} with max-keys {int}")]
async fn list_objects_max(world: &mut super::World, key: String, max_keys: u64) {
    do_list(world, &key, None, max_keys).await;
}

#[given(expr = "I list objects under {string} with delimiter {string} and max-keys {int}")]
#[when(expr = "I list objects under {string} with delimiter {string} and max-keys {int}")]
#[then(expr = "I list objects under {string} with delimiter {string} and max-keys {int}")]
async fn list_objects_delim_max(
    world: &mut super::World,
    key: String,
    delimiter: String,
    max_keys: u64,
) {
    do_list(world, &key, Some(delimiter), max_keys).await;
}

/// One ListObjectsV2 request; `key` is "bucket" or "bucket/prefix".
async fn do_list(world: &mut super::World, key: &str, delimiter: Option<String>, max_keys: u64) {
    let (bucket, prefix) = split_key(key);
    let path = list_v2_path(&bucket, &prefix, delimiter.as_deref(), Some(max_keys));
    world.last = world.client.request("GET", &path, &[], &[]).await;
    world.last_listing = ListingState {
        bucket,
        prefix,
        delimiter,
        max_keys,
    };
}

/// The ListObjectsV2 request path of `bucket`/`prefix` with an optional
/// `delimiter` and `max-keys` — one builder for every list step (the
/// plain lists, the pagination walk, and its full re-list).
fn list_v2_path(
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: Option<u64>,
) -> String {
    let mut path = format!("/{bucket}?list-type=2&prefix={}", url_encode(prefix));
    if let Some(d) = delimiter {
        path += &format!("&delimiter={}", url_encode(d));
    }
    if let Some(m) = max_keys {
        path += &format!("&max-keys={m}");
    }
    path
}

/// Split a "bucket" / "bucket/prefix" step key into its two halves.
fn split_key(key: &str) -> (String, String) {
    key.split_once('/')
        .map(|(b, p)| (b.to_string(), p.to_string()))
        .unwrap_or_else(|| (key.to_string(), String::new()))
}

/// ListObjects (V1): the path-style GET without `list-type` — the surface
/// only reachable with the list-v1 capability. Ported from the v1 leg of
/// `tinio-server/tests/coverage_gaps.rs`; `key` is "bucket" or
/// "bucket/prefix" (the v2 split convention), the marker and delimiter
/// are the wire query values (empty string omits nothing — the empty
/// params are sent, matching `prefix=&marker=&delimiter=`).
#[given(expr = "I list v1 objects under {string} with marker {string} and delimiter {string}")]
#[when(expr = "I list v1 objects under {string} with marker {string} and delimiter {string}")]
#[then(expr = "I list v1 objects under {string} with marker {string} and delimiter {string}")]
async fn list_objects_v1(world: &mut super::World, key: String, marker: String, delimiter: String) {
    do_list_v1(world, &key, &marker, &delimiter).await;
}

/// One ListObjects (V1) request; the response lands in `world.last` like
/// the v2 list steps.
async fn do_list_v1(world: &mut super::World, key: &str, marker: &str, delimiter: &str) {
    let (bucket, prefix) = split_key(key);
    let path = format!(
        "/{bucket}?prefix={}&marker={}&delimiter={}",
        url_encode(&prefix),
        url_encode(marker),
        url_encode(delimiter)
    );
    world.last = world.client.request("GET", &path, &[], &[]).await;
    world.last_listing = ListingState {
        bucket,
        prefix,
        delimiter: (!delimiter.is_empty()).then(|| delimiter.to_string()),
        max_keys: 1000,
    };
}

/// The last response was a listing containing `key` as a `<Key>` entry.
#[then(expr = "the listing contains {string}")]
async fn listing_contains_key(world: &mut super::World, key: String) {
    let text = String::from_utf8_lossy(&world.last.body).into_owned();
    assert!(
        text.contains(&format!("<Key>{key}</Key>")),
        "key {key} missing from listing: {text}"
    );
}

/// The last response was a listing with exactly `n` `<Key>` entries (a
/// folder marker is never a key). The optional plural matches both the
/// "1 key" and "2 keys" step texts.
#[then(expr = "the listing shows {int} key(s)")]
async fn listing_shows_keys(world: &mut super::World, n: u64) {
    let text = String::from_utf8_lossy(&world.last.body);
    let count = super::common::count_tag(&world.last.body, "<Key>") as u64;
    assert_eq!(count, n, "key count mismatch in listing: {text}");
}

/// The last response was a listing whose `<Key>` entries equal the data
/// table exactly, in listing order (the lexicographic full-listing
/// assertion).
#[then("the listing keys in order are")]
async fn listing_keys_in_order(world: &mut super::World, step: &cucumber::gherkin::Step) {
    let table = step.table().expect("the in-order step carries a table");
    let expected: Vec<String> = table.rows.iter().map(|row| row[0].clone()).collect();
    let text = String::from_utf8_lossy(&world.last.body).into_owned();
    let found = collect_between(&text, "<Key>", "</Key>");
    assert_eq!(
        found, expected,
        "keys not in the expected order in listing: {text}"
    );
}

/// The last response was a delimiter listing whose common prefixes are
/// exactly `p1` and `p2`, in listing order (the response's empty
/// `<Prefix></Prefix>` param echo is skipped).
#[then(expr = "the listing prefixes are {string} and {string}")]
async fn listing_prefixes(world: &mut super::World, p1: String, p2: String) {
    let text = String::from_utf8_lossy(&world.last.body).into_owned();
    let found: Vec<String> = collect_between(&text, "<Prefix>", "</Prefix>")
        .into_iter()
        .filter(|p| !p.is_empty())
        .collect();
    assert_eq!(
        found,
        [p1, p2],
        "common prefixes mismatch in listing: {text}"
    );
}

/// The last response was a truncated first page; walk the
/// `NextContinuationToken` chain until the listing ends, then prove the
/// paged union equals the full listing (the old test's three-page walk:
/// truncated pages carry a token, the final page does not, and every key
/// appears exactly once, in order).
#[then("a truncated listing resumes with the next page")]
async fn truncated_resumes(world: &mut super::World) {
    let st = &world.last_listing;
    let base = list_v2_path(
        &st.bucket,
        &st.prefix,
        st.delimiter.as_deref(),
        Some(st.max_keys),
    );
    let mut pages = vec![String::from_utf8_lossy(&world.last.body).into_owned()];
    assert!(
        pages[0].contains("<IsTruncated>true</IsTruncated>"),
        "expected a truncated first page: {}",
        pages[0]
    );
    for _ in 0..100 {
        let text = pages.last().expect("page");
        if !text.contains("<IsTruncated>true</IsTruncated>") {
            break;
        }
        let token =
            super::common::extract(text, "<NextContinuationToken>", "</NextContinuationToken>");
        assert!(
            !token.is_empty(),
            "truncated page without a continuation token"
        );
        let path = format!("{base}&continuation-token={}", url_encode(&token));
        let resp = world.client.request("GET", &path, &[], &[]).await;
        pages.push(String::from_utf8_lossy(&resp.body).into_owned());
    }
    let last = pages.last().expect("page");
    assert!(
        !last.contains("<IsTruncated>true</IsTruncated>"),
        "pagination did not terminate: {last}"
    );
    // Page boundaries resume exactly: the concatenated keys are strictly
    // increasing (no repeats, no gaps) and equal the full, unpaged listing.
    let paged_keys: Vec<String> = pages
        .iter()
        .flat_map(|p| collect_between(p, "<Key>", "</Key>"))
        .collect();
    assert!(
        paged_keys.windows(2).all(|w| w[0] < w[1]),
        "keys not strictly increasing across pages: {paged_keys:?}"
    );
    // The full re-list omits max-keys (the default page cap covers the
    // whole corpus the paged walk gathered).
    let full_path = list_v2_path(&st.bucket, &st.prefix, st.delimiter.as_deref(), None);
    let full = world.client.request("GET", &full_path, &[], &[]).await;
    let full_keys = collect_between(&String::from_utf8_lossy(&full.body), "<Key>", "</Key>");
    assert_eq!(
        paged_keys, full_keys,
        "paged union must equal the full listing"
    );
}

/// Every text between `open`/`close`, in order (empty when absent).
fn collect_between(text: &str, open: &str, close: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find(open) {
        let from = start + open.len();
        let Some(end_rel) = rest[from..].find(close) else {
            break;
        };
        out.push(rest[from..from + end_rel].to_string());
        rest = &rest[from + end_rel + close.len()..];
    }
    out
}

/// Percent-encode a query-string value (unreserved + `/` pass through) —
/// ported verbatim from the old test.
fn url_encode(value: &str) -> String {
    let mut out = String::new();
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => out += &format!("%{b:02X}"),
        }
    }
    out
}
