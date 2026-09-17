//! Streaming a large DOCUMENT end to end (spec 2026-09-12): row count,
//! completion, strictly advancing element-start offsets. The fixture is the
//! gzip-compressed synthetic document in git LFS, 11 577 783 bytes decompressed
//! (~11 MB); the read path is the production one (`decompressed(Gzip)`).

use std::{
    fs::File,
    io::{Cursor, Read, Write},
};

use flate2::write::GzEncoder;
use tinio_select::{
    Error, json,
    record::{Compression, RecordReader, decompressed},
    row::Record,
    sql::parse,
};

#[path = "support/json_fixture.rs"]
mod json_fixture;

/// Fixture size, shared with `generate_large_fixture` below.
const ELEMENTS: u64 = 100_000;

fn fixture_reader() -> json::Reader<Box<dyn Read + Send>> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/large/document.json.gz"
    );
    let file = File::open(path).expect("LFS fixture missing — run `git lfs pull`");
    let input = decompressed(Some(Compression::Gzip), Box::new(file));
    let plan = parse("SELECT * FROM S3Object").unwrap();
    json::Reader::new(
        input,
        json::Params {
            ty: json::Type::Document,
        },
        &plan.from,
    )
}

#[test]
fn large_document_streams_to_completion() {
    let mut r = fixture_reader();
    let mut count = 0u64;
    let mut last = 0u64;
    while let Some(rec) = r.next().unwrap() {
        assert!(matches!(rec, Record::Json(Some(_))));
        let start = r.last_record_start();
        // Strict, not `>=`: `last` starts at 0, so this also requires the very
        // first record to start past the `[`, and a reader returning a constant
        // offset (which `start >= last` accepted) now fails. This is the only
        // CI-level check of spec deviation ⑤ (element-level real offsets).
        assert!(
            start > last,
            "record starts must strictly advance (spec deviation ⑤): record \
             {count} starts at {start}, previous at {last}"
        );
        last = start;
        count += 1;
    }
    assert_eq!(count, ELEMENTS);
}

#[test]
fn over_cap_element_errors_after_prior_records() {
    // The spec's cap-behaviour pin on the CI path (inline data, not the LFS
    // fixture). The public accessor classifies the failure mode; the literal is
    // the `CAP_MESSAGE` rendering, pinned here because the constant is
    // `pub(crate)`.
    let big = "a".repeat(1024 * 1024);
    let doc = format!("[{{\"id\": 1}}, {{\"x\": \"{big}\"}}]");
    let plan = parse("SELECT * FROM S3Object").unwrap();
    let mut r = json::Reader::new(
        Box::new(Cursor::new(doc.into_bytes())) as Box<dyn Read + Send>,
        json::Params {
            ty: json::Type::Document,
        },
        &plan.from,
    );
    assert!(matches!(r.next().unwrap(), Some(Record::Json(Some(_)))));
    match r.next() {
        // A separate crate can name and match the variant now: the public
        // `JsonInputError` payload carries the classifiers, so telling the cap
        // from another JSON failure needs no Display string.
        Err(Error::Json(json)) => {
            assert!(json.is_cap(), "{json:?}");
            let e = Error::Json(json);
            assert!(e.is_cap(), "{e:?}");
            assert!(e.is_json_input(), "{e:?}");
            // The crate-level Display — byte-identical to the old
            // `Error::Format(CAP_MESSAGE)` rendering — is what the S3 Select
            // layer surfaces to the client.
            assert_eq!(
                e.to_string(),
                "S3 select: input error: input record exceeds 1 MB"
            );
        }
        other => panic!("expected the reader cap error, got {other:?}"),
    }
}

/// Regenerate the fixture:
/// `cargo test -p tinio-select --test stream_large -- --ignored`.
/// Same provenance shape as `tests/parquet.rs`'s ignored generator.
#[test]
#[ignore = "regenerates the LFS fixture"]
fn generate_large_fixture() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/large/document.json.gz"
    );
    let file = File::create(path).expect("create fixture");
    let mut gz = GzEncoder::new(file, flate2::Compression::default());
    gz.write_all(b"[\n").unwrap();
    for i in 0..ELEMENTS {
        writeln!(
            gz,
            "{}{}",
            json_fixture::element_pretty(i),
            if i + 1 < ELEMENTS { "," } else { "" }
        )
        .unwrap();
    }
    gz.write_all(b"]\n").unwrap();
    gz.finish().unwrap();
}
