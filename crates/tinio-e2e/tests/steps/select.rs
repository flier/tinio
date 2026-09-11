//! Select step definitions (FR-034): SelectObjectContent over objects —
//! fixtures (plain and gzip-compressed docstrings) built by the steps, the
//! request driven through the shared raw client like every other surface.
//! Each scenario gets its own in-process server, so the fixed bucket name
//! never collides; the select answer is the AWS binary event stream, whose
//! `Records` frames carry the record bytes — [`record_payloads`] joins
//! them for the `contains` assertions.

use std::io::Write as _;

use cucumber::{given, then, when};

/// The bucket every select scenario uses (one per scenario's server).
/// Creation is the shared `I create bucket {string}` step (buckets.rs) —
/// the select module keeps only the object-docstring fixtures, which no
/// other step module carries (2026-09-06 review S10).
const BUCKET: &str = "select";

/// An object whose body is the step's `"""`-delimited docstring.
#[given(regex = r#"an object "([^"]+)" with content"#)]
async fn object_with_content(
    world: &mut super::World,
    key: String,
    step: &cucumber::gherkin::Step,
) {
    let content = fixture_bytes(step);
    put_object(world, &key, &content).await;
}

/// Same, but the object's body is the docstring gzip-compressed (the GZIP
/// input leg of FR-034).
#[given(regex = r#"a gzip-compressed object "([^"]+)" with content"#)]
async fn gzip_object_with_content(
    world: &mut super::World,
    key: String,
    step: &cucumber::gherkin::Step,
) {
    let content = fixture_bytes(step);
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(&content).expect("gzip: write");
    let gz = enc.finish().expect("gzip: finish");
    put_object(world, &key, &gz).await;
}

/// Same, but the object's body is the docstring bzip2-compressed.
#[given(regex = r#"a bzip2-compressed object "([^"]+)" with content"#)]
async fn bzip2_object_with_content(
    world: &mut super::World,
    key: String,
    step: &cucumber::gherkin::Step,
) {
    let content = fixture_bytes(step);
    let mut enc = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::best());
    enc.write_all(&content).expect("bzip2: write");
    let bz = enc.finish().expect("bzip2: finish");
    put_object(world, &key, &bz).await;
}

/// A parquet object built from the committed fixture — the same bytes
/// `tinio-select`'s integration test asserts and `tinio-server`'s parquet
/// test selects (regenerate with the ignored generator in
/// `crates/tinio-select/tests/parquet.rs`). No docstring: parquet is binary,
/// and a `"""` block cannot carry it.
///
/// The scenario is tagged `@parquet` because the reader lives behind
/// `tinio-server/select-parquet`: with the feature off the request answers
/// 501 before the body is even read. The runner's default filter excludes
/// the tag; the parquet CI leg selects it with `--features parquet`.
#[given(regex = r#"^a parquet object "([^"]+)"$"#)]
async fn parquet_object(world: &mut super::World, key: String) {
    put_object(world, &key, PARQUET_FIXTURE).await;
}

/// The shared fixture bytes (`include_bytes!` crosses into the owning crate
/// on purpose — one file, three layers).
const PARQUET_FIXTURE: &[u8] =
    include_bytes!("../../../tinio-select/tests/fixtures/select.parquet");

/// The docstring value, its delimiter newlines trimmed: the gherkin parser
/// keeps the newline that closes the opening `"""` and the one before the
/// closing delimiter, so a fixture's first content byte would be `\n`
/// (harmless for CSV — an empty record — but a JSON LINES reader errors on
/// the empty first line). `trim_matches('\n')` strips every leading and
/// trailing newline — the two delimiter ones plus any intentional blank
/// edge lines; a fixture whose content itself begins or ends with a
/// meaningful newline cannot express it.
fn fixture_bytes(step: &cucumber::gherkin::Step) -> Vec<u8> {
    step.docstring()
        .map(|s| s.trim_matches('\n').as_bytes().to_vec())
        .unwrap_or_default()
}

async fn put_object(world: &mut super::World, key: &str, content: &[u8]) {
    world.last = world
        .client
        .request("PUT", &format!("/{BUCKET}/{key}"), &[], content)
        .await;
    assert_eq!(
        world.last.status,
        200,
        "object put failed: {}",
        String::from_utf8_lossy(&world.last.body)
    );
}

#[when(regex = r#"^I select over object "([^"]+)" with query "([^"]+)"$"#)]
async fn select_over_object(world: &mut super::World, key: String, query: String) {
    let body = select_body(&key, &query, false, None);
    world.last = world
        .client
        .request(
            "POST",
            &format!("/{BUCKET}/{key}?select&select-type=2"),
            &[("Content-Type", "application/xml")],
            body.as_bytes(),
        )
        .await;
    assert_eq!(
        world.last.status,
        200,
        "select failed: {}",
        String::from_utf8_lossy(&world.last.body)
    );
}

#[when(regex = r#"^I select over object "([^"]+)" with query "([^"]+)" as JSON output$"#)]
async fn select_as_json(world: &mut super::World, key: String, query: String) {
    let body = select_body(&key, &query, true, None);
    world.last = world
        .client
        .request(
            "POST",
            &format!("/{BUCKET}/{key}?select&select-type=2"),
            &[("Content-Type", "application/xml")],
            body.as_bytes(),
        )
        .await;
    assert_eq!(
        world.last.status,
        200,
        "select failed: {}",
        String::from_utf8_lossy(&world.last.body)
    );
}

#[when(
    regex = r#"^I select over object "([^"]+)" with query "([^"]+)" within scan range "(\d+)" to "(\d+)"$"#
)]
async fn select_in_scan_range(
    world: &mut super::World,
    key: String,
    query: String,
    start: String,
    end: String,
) {
    let body = select_body(&key, &query, false, Some((Some(start), Some(end))));
    world.last = world
        .client
        .request(
            "POST",
            &format!("/{BUCKET}/{key}?select&select-type=2"),
            &[("Content-Type", "application/xml")],
            body.as_bytes(),
        )
        .await;
    assert_eq!(
        world.last.status,
        200,
        "select failed: {}",
        String::from_utf8_lossy(&world.last.body)
    );
}

/// Start-only ScanRange: from the byte offset to the end of the object.
#[when(regex = r#"^I select over object "([^"]+)" with query "([^"]+)" from scan range "(\d+)"$"#)]
async fn select_in_scan_start(world: &mut super::World, key: String, query: String, start: String) {
    let body = select_body(&key, &query, false, Some((Some(start), None)));
    world.last = world
        .client
        .request(
            "POST",
            &format!("/{BUCKET}/{key}?select&select-type=2"),
            &[("Content-Type", "application/xml")],
            body.as_bytes(),
        )
        .await;
    assert_eq!(
        world.last.status,
        200,
        "select failed: {}",
        String::from_utf8_lossy(&world.last.body)
    );
}

/// End-only ScanRange: the last N bytes of the object (AWS "last N").
#[when(
    regex = r#"^I select over object "([^"]+)" with query "([^"]+)" over the last "(\d+)" bytes$"#
)]
async fn select_over_last_bytes(world: &mut super::World, key: String, query: String, end: String) {
    let body = select_body(&key, &query, false, Some((None, Some(end))));
    world.last = world
        .client
        .request(
            "POST",
            &format!("/{BUCKET}/{key}?select&select-type=2"),
            &[("Content-Type", "application/xml")],
            body.as_bytes(),
        )
        .await;
    assert_eq!(
        world.last.status,
        200,
        "select failed: {}",
        String::from_utf8_lossy(&world.last.body)
    );
}

/// The failure-leg of the select step: records the response without the
/// 200 assertion, so a request-level error scenario can pin its status.
#[when(regex = r#"^I try select over object "([^"]+)" with query "([^"]+)"$"#)]
async fn try_select_over_object(world: &mut super::World, key: String, query: String) {
    let body = select_body(&key, &query, false, None);
    world.last = world
        .client
        .request(
            "POST",
            &format!("/{BUCKET}/{key}?select&select-type=2"),
            &[("Content-Type", "application/xml")],
            body.as_bytes(),
        )
        .await;
}

#[then(regex = r#"^the select fails with HTTP (\d+) and code "([^"]+)""#)]
async fn select_fails_with(world: &mut super::World, status: String, code: String) {
    assert_eq!(
        world.last.status,
        status.parse::<u16>().unwrap(),
        "{world:?}"
    );
    let text = String::from_utf8_lossy(&world.last.body).into_owned();
    assert!(
        text.contains(&code),
        "expected code {code:?} in response: {text:?}"
    );
}

/// The zero-record assertion (empty input, nothing matching).
#[then("the select results are empty")]
async fn results_empty(world: &mut super::World) {
    let body = record_payloads(&world.last.body);
    assert!(
        body.is_empty(),
        "expected no record payloads, got {:?}",
        String::from_utf8_lossy(&body)
    );
}

/// The one request builder: the input serialization follows the fixture
/// suffix, the step phrases pick output mode and ScanRange.
fn select_body(
    key: &str,
    query: &str,
    json_out: bool,
    scan: Option<(Option<String>, Option<String>)>,
) -> String {
    let scan = scan
        .map(|(start, end)| {
            let start = start
                .map(|s| format!("<Start>{s}</Start>"))
                .unwrap_or_default();
            let end = end.map(|e| format!("<End>{e}</End>")).unwrap_or_default();
            format!("<ScanRange>{start}{end}</ScanRange>")
        })
        .unwrap_or_default();
    let output = if json_out { "<JSON/>" } else { "<CSV/>" };
    format!(
        "<SelectObjectContentRequest>\
         <Expression>{}</Expression>\
         <ExpressionType>SQL</ExpressionType>\
         <InputSerialization>{}</InputSerialization>\
         <OutputSerialization>{output}</OutputSerialization>\
         {scan}\
         </SelectObjectContentRequest>",
        xml_escape(query),
        input_serialization(key),
    )
}

/// The input serialization chosen from the fixture's suffix, layered:
/// `.gz`/`.bz2` strip to the compression type, the stem's `.parquet` ⇒
/// Parquet, `.jsonl` ⇒ JSON LINES, `.jsond` ⇒ JSON DOCUMENT, `.csvh` ⇒ CSV
/// with `FileHeaderInfo USE` (the AWS doc sample's named columns), anything
/// else ⇒ CSV. Every combination is expressible: `data.csv.gz`,
/// `people.jsonl.bz2`, …
fn input_serialization(key: &str) -> String {
    let (stem, compression) = if let Some(stem) = key.strip_suffix(".bz2") {
        (stem, Some("BZIP2"))
    } else if let Some(stem) = key.strip_suffix(".gz") {
        (stem, Some("GZIP"))
    } else {
        (key, None)
    };
    let format = if stem.ends_with(".parquet") {
        // Parquet takes no delimiter/header options, and the server refuses
        // `CompressionType` on it — the format alone.
        "<Parquet/>"
    } else if stem.ends_with(".jsonl") {
        "<JSON><Type>LINES</Type></JSON>"
    } else if stem.ends_with(".jsond") {
        "<JSON><Type>DOCUMENT</Type></JSON>"
    } else if stem.ends_with(".csvh") {
        "<CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV>"
    } else if stem.ends_with(".csvi") {
        "<CSV><FileHeaderInfo>IGNORE</FileHeaderInfo></CSV>"
    } else {
        "<CSV/>"
    };
    match compression {
        None => format.to_string(),
        Some(c) => format!("{format}<CompressionType>{c}</CompressionType>"),
    }
}

/// The select-step form of the generic `the response body contains`:
/// the record payloads of the event-stream answer, joined and searched.
/// The anchor is the unanchored-convention exception: without `^…$` the
/// one-phrase pattern would also match the compound sentence below.
#[then(regex = r#"^the select results contain "([^"]+)"$"#)]
async fn results_contain(world: &mut super::World, needle: String) {
    let text = record_text(&world.last.body);
    assert!(
        text.contains(&needle),
        "results missing {needle:?}: {text:?} (raw: {})",
        String::from_utf8_lossy(&world.last.body)
    );
}

#[then(regex = r#"^the select results contain "([^"]+)" and "([^"]+)"$"#)]
async fn results_contain_and(world: &mut super::World, a: String, b: String) {
    let text = record_text(&world.last.body);
    for needle in [&a, &b] {
        assert!(
            text.contains(needle),
            "results missing {needle:?}: {text:?}"
        );
    }
}

#[then(regex = r#"^the select results contain "([^"]+)" and "([^"]+)" but not "([^"]+)"$"#)]
async fn results_contain_but_not(world: &mut super::World, a: String, b: String, c: String) {
    let text = record_text(&world.last.body);
    for needle in [&a, &b] {
        assert!(
            text.contains(needle),
            "results missing {needle:?}: {text:?}"
        );
    }
    assert!(
        !text.contains(&c),
        "results unexpectedly contain {c:?}: {text:?}"
    );
}

#[then(regex = r#"^the select results do not contain "([^"]+)"$"#)]
async fn results_not_contain(world: &mut super::World, needle: String) {
    let text = record_text(&world.last.body);
    assert!(
        !text.contains(&needle),
        "results unexpectedly contain {needle:?}: {text:?}"
    );
}

/// The joined record bytes, as lossy text (for the assertion message and
/// the search alike).
fn record_text(body: &[u8]) -> String {
    String::from_utf8_lossy(&record_payloads(body)).into_owned()
}

/// The record bytes carried by an S3 Select event-stream answer, joined
/// across the `Records` frames. The frame layout is the AWS binary event
/// stream s3s emits (tests/xml.rs-driven): a 12-byte prelude (total length,
/// header length, CRC — all big-endian), the header block, the payload,
/// and a trailing 4-byte message CRC. The `Records` payload is the raw
/// record bytes, so a join+`contains` is the select-output equivalent of
/// the wire-XML `contains` phrase. `Progress`/`Stats` frames carry XML
/// payloads and are skipped — a digit inside `<BytesScanned>` must never
/// satisfy a record assertion.
fn record_payloads(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut rest = body;
    // Prelude: 4-byte total length, 4-byte header length, 4-byte CRC —
    // big-endian. `total` spans the whole frame (prelude + headers +
    // payload + the trailing 4-byte message CRC), so a frame's length IS
    // `total` and a payload-less frame's payload is 0. A malformed layout
    // stops the walk.
    while rest.len() >= 16 {
        let total = u32::from_be_bytes(rest[0..4].try_into().unwrap()) as usize;
        let headers_len = u32::from_be_bytes(rest[4..8].try_into().unwrap()) as usize;
        if total < 16 || total > rest.len() || headers_len > total - 16 {
            break;
        }
        let payload_len = total - 16 - headers_len;
        let mut headers = &rest[12..12 + headers_len];
        let mut is_records = false;
        while headers.len() >= 4 {
            let name_len = headers[0] as usize;
            if 4 + name_len > headers.len() {
                break;
            }
            let value_len =
                u16::from_be_bytes(headers[2 + name_len..4 + name_len].try_into().unwrap())
                    as usize;
            if 4 + name_len + value_len > headers.len() {
                break;
            }
            if headers[1..1 + name_len] == b":event-type"[..]
                && headers[4 + name_len..4 + name_len + value_len] == b"Records"[..]
            {
                is_records = true;
                break;
            }
            headers = &headers[4 + name_len + value_len..];
        }
        if is_records {
            out.extend_from_slice(&rest[12 + headers_len..12 + headers_len + payload_len]);
        }
        rest = &rest[total..];
    }
    out
}

/// Minimal XML text escape for the expression (the feature's queries carry
/// nothing but `'`, `*`, `=`, spaces — only `&`, `<`, `>` need care).
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
