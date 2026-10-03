//! The block-store conformance suite.
//!
//! Behind the non-default `testing` feature (`docs/tests.md`): a `tests/`
//! target cannot cross into `src/`, so a suite a target must share has to be
//! library code behind a feature.

use std::{
    io,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use futures::StreamExt;

use crate::_core::{
    ByteRange,
    blob::{BlockHash, BlockStore, Entry, Published, StagedBlock},
    storage,
};

/// Assert the rules every [`BlockStore`] must satisfy.
///
/// The store must be **empty** when this is called: the first rule asserted is
/// that it holds nothing, and the suite publishes the blocks it pages over
/// itself, removing them again before it returns so the store is left empty.
///
/// # Panics
///
/// Panics with a rule-naming message when the store breaks one.
///
/// # Examples
///
/// ```rust
/// use tinio_blob::{LocalBlocks, testing};
///
/// # async fn example() {
/// let dir = tempfile::tempdir().unwrap();
/// testing::assert_block_store(&LocalBlocks::new(dir.path())).await;
/// # }
/// # tokio::runtime::Runtime::new().unwrap().block_on(example());
/// ```
pub async fn assert_block_store<S: BlockStore>(store: &S) {
    let hash = BlockHash::of(b"conformance");
    assert!(
        !store.contains(&hash).await.expect("contains"),
        "a fresh store holds nothing"
    );

    let mut staged = store.stage().await.expect("stage");
    for chunk in [b"conf".as_slice(), b"orm", b"ance"] {
        staged
            .write(Bytes::copy_from_slice(chunk))
            .await
            .expect("write");
    }
    // A stage is invisible: the bytes live at the stage's own name until
    // `publish` makes the block visible, so a backend whose `write` lands
    // bytes at the final name would serve an interrupted write — the trait's
    // headline guarantee. Nothing has been published yet, so both reads below
    // see the empty store the suite was handed.
    assert!(
        !store.contains(&hash).await.expect("contains"),
        "a staged block is not visible under its address before it is published"
    );
    assert!(
        store.list(None, 10).await.expect("list").is_empty(),
        "a staged block is not listed before it is published"
    );
    assert!(
        walk_entries(store, None).await.is_empty(),
        "a staged block is not walked before it is published"
    );
    assert_eq!(
        store.publish(staged, &hash).await.expect("publish"),
        Published::Stored,
        "the first publish stores"
    );
    assert!(
        store.contains(&hash).await.expect("contains"),
        "a published block is present"
    );

    let mut staged = store.stage().await.expect("stage");
    staged
        .write(Bytes::from_static(b"conformance"))
        .await
        .expect("write");
    assert_eq!(
        store.publish(staged, &hash).await.expect("publish"),
        Published::Existing,
        "a repeated publish is a dedup hit"
    );

    let body = storage::collect_body(store.open(&hash, None).await.expect("open"))
        .await
        .expect("read");
    assert_eq!(body, b"conformance", "a block reads back byte for byte");

    let range = Some(ByteRange::Inclusive(0, 3));
    let body = storage::collect_body(store.open(&hash, range).await.expect("open"))
        .await
        .expect("read");
    assert_eq!(body, b"conf", "a range is resolved within the block");

    // A range entirely past the block: `conformance` is 11 bytes, so a start
    // at 11 is one past its last byte. The answer is an error, never an empty
    // body and never a clamp back into the block.
    assert!(
        store.open(&hash, Some(ByteRange::From(11))).await.is_err(),
        "a range that starts past the stored block is an error"
    );

    let listed = store.list(None, 10).await.expect("list");
    assert_eq!(listed.len(), 1, "the store lists exactly what it holds");
    assert_eq!(
        listed[0].hash, hash,
        "the listed address is the block's own"
    );
    assert_eq!(listed[0].len, 11, "a listed block reports its own length");

    store.remove(&hash).await.expect("remove");
    assert!(
        !store.contains(&hash).await.expect("contains"),
        "a removed block is gone"
    );
    store.remove(&hash).await.expect("remove is idempotent");

    // A block that is not there. The kind is the contract, not the error type:
    // the engine and the S3 layer classify on `NotFound`, and an empty body
    // would be indistinguishable from a stored empty block.
    let missing = BlockHash::of(b"never published");
    assert!(
        !store.contains(&missing).await.expect("contains"),
        "the missing-block probe is not in the store"
    );
    let err = match store.open(&missing, None).await {
        Ok(_) => panic!("opening a missing block is an error, not an empty body"),
        Err(err) => err,
    };
    let projected: storage::Error = err.into();
    let storage::Error::Io(io) = projected else {
        panic!("a missing block must project onto an I/O error, got {projected:?}");
    };
    assert_eq!(
        io.kind(),
        io::ErrorKind::NotFound,
        "a missing block is `NotFound`"
    );

    // A zero-byte block — the empty blob the engine stores as one block of no
    // bytes, so a backend that cannot hold zero-length content would break it.
    // Such a block reads back as an empty body, is listed as a block of zero
    // bytes, and refuses a range: an empty body would be indistinguishable
    // from a stored empty block, which is why the contract answers 416.
    let zero = BlockHash::of(b"");
    let mut staged = store.stage().await.expect("stage");
    staged
        .write(Bytes::new())
        .await
        .expect("a zero-byte write is a legal write");
    assert_eq!(
        store.publish(staged, &zero).await.expect("publish"),
        Published::Stored,
        "a zero-byte block publishes like any other"
    );
    assert!(
        store.contains(&zero).await.expect("contains"),
        "a published zero-byte block is present"
    );
    assert!(
        storage::collect_body(store.open(&zero, None).await.expect("open"))
            .await
            .expect("read")
            .is_empty(),
        "a zero-byte block reads back as an empty body"
    );
    let listed = store.list(None, 10).await.expect("list");
    assert_eq!(listed.len(), 1, "the zero-byte block is the only block");
    assert_eq!(listed[0].len, 0, "it lists as a block of zero bytes");
    let err = match store.open(&zero, Some(ByteRange::Inclusive(0, 0))).await {
        Ok(_) => panic!("a range against a zero-byte block is an error, not an empty body"),
        Err(err) => err,
    };
    let projected: storage::Error = err.into();
    assert!(
        matches!(projected, storage::Error::InvalidRange { .. }),
        "a range against a zero-byte block is InvalidRange, got {projected:?}"
    );
    store.remove(&zero).await.expect("remove");
    assert!(
        !store.contains(&zero).await.expect("contains"),
        "the zero-byte block is removed again"
    );

    // The page walk — `list`'s one risky step. A backend's listing promises
    // no order (`read_dir` does not, and neither does WebDAV's PROPFIND nor
    // FTP's LIST), so entries may arrive in any order; the walk must still
    // ascend within a page, resume strictly after the previous page's last
    // address, and lose nothing between pages.
    // The publication time must be a reading of the caller's clock, not a
    // constant: the sweep compares it against its own `now`, so a fixed
    // instant either disables reclamation or reclaims blocks that were
    // published moments ago. A second of slack on each side absorbs a
    // filesystem's timestamp granularity.
    let before = SystemTime::now() - Duration::from_secs(1);
    let mut published = Vec::new();
    for payload in [
        b"page-0".as_slice(),
        b"page-1",
        b"page-2",
        b"page-3",
        b"page-4",
    ] {
        let hash = BlockHash::of(payload);
        let mut staged = store.stage().await.expect("stage");
        staged
            .write(Bytes::copy_from_slice(payload))
            .await
            .expect("write");
        assert_eq!(
            store.publish(staged, &hash).await.expect("publish"),
            Published::Stored,
            "each paged payload is new, so its publish stores"
        );
        published.push(hash);
    }
    published.sort();

    let listed = page_entries(store, 2).await.expect("list");
    assert_eq!(
        hashes(&listed),
        published,
        "every published block is listed exactly once, in ascending address order"
    );
    assert!(
        listed.iter().all(|entry| entry.len == 6),
        "a listed block reports its own length"
    );

    // The streaming walk — the same sequence in one pass: ascending, every
    // block exactly once, resuming strictly after the cursor. The layout is the
    // backend's business, so the walk is compared against the published set,
    // never against a directory count.
    let walked = walk_entries(store, None).await;
    assert_eq!(
        hashes(&walked),
        published,
        "one walk yields every block exactly once, in ascending address order"
    );
    assert!(
        walked.iter().all(|entry| entry.len == 6),
        "a walked block reports its own length"
    );
    let after = SystemTime::now() + Duration::from_secs(1);
    assert!(
        listed
            .iter()
            .chain(&walked)
            .all(|entry| before <= entry.published && entry.published <= after),
        "every listed and walked block is stamped with a clock reading from this run, not a constant"
    );

    let resumed = walk_entries(store, Some(&published[2])).await;
    assert_eq!(
        hashes(&resumed),
        published[3..].to_vec(),
        "a walk from a cursor resumes strictly after it, losing nothing"
    );
    assert!(
        walk_entries(store, published.last()).await.is_empty(),
        "a walk from the last address yields nothing"
    );

    // Leave the store as it was found.
    for hash in published {
        store.remove(&hash).await.expect("remove");
    }
    assert!(
        walk_entries(store, None).await.is_empty(),
        "a walk of a store that holds nothing ends immediately"
    );
}

/// Walk `store`'s listing a page at a time, asserting the page contract as it
/// goes, and return every entry the walk yielded in the order it yielded them.
///
/// The contract: a page never exceeds `limit` entries, one page's addresses
/// ascend, and a page resumes strictly after the previous page's last address;
/// the walk ends on the first empty page. Shared with the implementation's own
/// test module, so the two suites walk the listing through one copy of the rule.
///
/// # Panics
///
/// Panics with a rule-naming message when the store breaks one.
pub(crate) async fn page_entries<S: BlockStore>(
    store: &S,
    limit: usize,
) -> Result<Vec<Entry>, S::Error> {
    let mut seen: Vec<Entry> = Vec::new();
    let mut cursor: Option<BlockHash> = None;
    loop {
        let page = store.list(cursor.as_ref(), limit).await?;
        if page.is_empty() {
            break;
        }
        assert!(
            page.len() <= limit,
            "a page never exceeds the limit it was asked for"
        );
        assert!(
            page.windows(2).all(|pair| pair[0].hash < pair[1].hash),
            "one page's addresses are in ascending order"
        );
        if let Some(previous) = seen.last() {
            assert!(
                previous.hash < page[0].hash,
                "a page resumes strictly after the previous page's last address"
            );
        }
        cursor = Some(page.last().expect("a non-empty page has a last entry").hash);
        seen.extend(page);
    }
    Ok(seen)
}

/// Drain a walk into the entries it yields, ending on the first item a backend
/// reports as an error.
async fn walk_entries<S: BlockStore>(store: &S, after: Option<&BlockHash>) -> Vec<Entry> {
    let mut walk = store.walk(after).await.expect("walk");
    let mut out = Vec::new();
    while let Some(entry) = walk.next().await {
        out.push(entry.expect("walk entry"));
    }
    out
}

/// A run of entries' addresses, in the order the run holds them.
fn hashes(entries: &[Entry]) -> Vec<BlockHash> {
    entries.iter().map(|entry| entry.hash).collect()
}
