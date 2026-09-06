//! Select step definitions (FR-033): SelectObjectContent over objects —
//! fixtures (plain and gzip-compressed docstrings) built by the steps, the
//! request driven through the shared raw client like every other surface.
//! Each scenario gets its own in-process server, so the fixed bucket name
//! never collides; the select answer is the AWS binary event stream, whose
//! `Records` frames carry the record bytes — [`record_payloads`] joins
//! them for the `contains` assertions.

use std::io::Write as _;

use cucumber::{given, then, when};

/// The bucket every select scenario uses (one per scenario's server).
const BUCKET: &str = "select";

#[given("a bucket")]
async fn given_bucket(world: &mut super::World) {
    world.last = world
        .client
        .request("PUT", &format!("/{BUCKET}"), &[], &[])
        .await;
    assert_eq!(world.last.status, 200, "bucket create failed");
}

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
/// input leg of FR-033).
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

/// The docstring value, minus its delimiter newlines: the gherkin parser
/// keeps the newline that closes the opening `"""` and the one before the
/// closing delimiter, so a fixture's first content byte would be `\n`
/// (harmless for CSV — an empty record — but a JSON LINES reader errors on
/// the empty first line). The closing `"""` dedent is preserved by the
/// parser, so only the two delimiter newlines are trimmed.
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
        world.last.status, 200,
        "object put failed: {}",
        String::from_utf8_lossy(&world.last.body)
    );
}

#[when(regex = r#"I select over object "([^"]+)" with query "([^"]+)""#)]
async fn select_over_object(world: &mut super::World, key: String, query: String) {
    let body = format!(
        "<SelectObjectContentRequest>\
         <Expression>{}</Expression>\
         <ExpressionType>SQL</ExpressionType>\
         <InputSerialization>{}</InputSerialization>\
         <OutputSerialization><CSV/></OutputSerialization>\
         </SelectObjectContentRequest>",
        xml_escape(&query),
        input_serialization(&key),
    );
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
        world.last.status, 200,
        "select failed: {}",
        String::from_utf8_lossy(&world.last.body)
    );
}

/// The input serialization chosen from the fixture's suffix: `.jsonl` ⇒
/// JSON LINES, `.gz` ⇒ CSV with GZIP compression, anything else ⇒ CSV.
/// Output is always CSV (positional — the projections the feature uses
/// never need the JSON alias gate).
fn input_serialization(key: &str) -> &'static str {
    if key.ends_with(".jsonl") {
        "<JSON><Type>LINES</Type></JSON>"
    } else if key.ends_with(".gz") {
        "<CSV/><CompressionType>GZIP</CompressionType>"
    } else {
        "<CSV/>"
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

#[then(regex = r#"^the select results contain "([^"]+)" and "([^"]+)" but not "([^"]+)"$"#)]
async fn results_contain_but_not(world: &mut super::World, a: String, b: String, c: String) {
    let text = record_text(&world.last.body);
    for needle in [&a, &b] {
        assert!(text.contains(needle), "results missing {needle:?}: {text:?}");
    }
    assert!(
        !text.contains(&c),
        "results unexpectedly contain {c:?}: {text:?}"
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
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}
