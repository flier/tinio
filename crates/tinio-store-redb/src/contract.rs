//! The shared-layer contract tests: the row self-heal rule and the state
//! version/marker protocol — pinned here over a plain in-memory redb
//! database (no tempdir, no `Handle`, no tokio), the home of the rules
//! both backends share.

use std::time::{Duration, SystemTime};

use redb::{Database, ReadableDatabase, backends::InMemoryBackend};
use tinio_core::{
    acl::{self, Acl, Grant, Grantee, OwnerId},
    etag::ETag,
    object,
    object::OBJECT_TAGS_MAX,
    to_nanos, version,
};

use crate::{bucket, bucket::BucketRow, object_meta, state, upload};

fn mem_db() -> Database {
    Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap()
}

const OWNER: &str = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";
const ETAG: &str = "d41d8cd98f00b204e9800998ecf8427e";
const GRANTS: &str =
    "id=aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899,FULL_CONTROL";

#[test]
fn meta_validate_self_heals_every_element_independently() {
    // The 11-tuple: (etag, size, mtime, file_identity, tags wire, checksum
    // wire, owner wire, acl wire, version id wire, version sequence,
    // delete-marker flag).
    let valid = object_meta::validate((
        "d41d8cd98f00b204e9800998ecf8427e",
        1,
        2,
        0,
        "",
        "",
        "",
        "",
        "null",
        0,
        false,
    ));
    let row = valid.expect("an empty tags/checksum/owner/acl wire is valid");
    assert_eq!(row.size, 1);
    assert!(row.tags.is_empty());
    assert!(row.checksum.is_none());
    assert_eq!(row.owner, None);
    assert_eq!(row.acl, Acl::default_private(None));
    assert_eq!(row.version_id, None, "the literal null is the null version");
    assert_eq!(row.version_seq, 0, "0 is no sequence");
    assert!(
        !row.is_delete_marker,
        "an object row clears the marker flag"
    );

    // A garbage etag is the only element that fails the whole row.
    assert!(
        object_meta::validate(("not-an-etag", 1, 2, 0, "", "", "", "", "null", 0, false)).is_none()
    );

    // Garbage tags/checksum/owner/acl wires self-heal individually; the
    // row survives (the owner element is its own wire — an ACL grant
    // wire that names no owner must not conjure owner data).
    let healed = object_meta::validate((
        "d41d8cd98f00b204e9800998ecf8427e",
        1,
        2,
        0,
        "team=%zz&",
        "CRC32:@@:NOPE",
        "not-a-canonical-id",
        "id=short,READ",
        "v1",
        9,
        true,
    ))
    .expect("the etag is valid, the row is served");
    assert!(healed.tags.is_empty());
    assert!(healed.checksum.is_none());
    assert_eq!(healed.owner, None);
    assert_eq!(healed.acl, Acl::default_private(None));
    assert_eq!(
        healed.version_id.unwrap().as_str(),
        "v1",
        "a real identifier rides the row untouched"
    );
    assert_eq!(healed.version_seq, 9);
    assert!(
        healed.is_delete_marker,
        "the recorded marker flag rides the row untouched"
    );
}

#[test]
fn state_round_trips_version_and_compact_marker() {
    let db = mem_db();
    // The first-open write: the version row is written; the marker is not
    // (absent marker => false, the compact protocol's clean state).
    {
        let mut txn = db.begin_write().unwrap();
        let mut state = state::Table::open(&mut txn).unwrap();
        state.write_version(state::FORMAT_VERSION).unwrap();
        drop(state);
        txn.commit().unwrap();
    }
    {
        let txn = db.begin_read().unwrap();
        let state = state::Table::open_readonly(&txn).unwrap();
        assert_eq!(state.version().unwrap(), Some(state::FORMAT_VERSION));
        assert!(!state.compact_marker().unwrap(), "absent marker => false");
    }
    // The marker flip, in one transaction.
    {
        let mut txn = db.begin_write().unwrap();
        let mut state = state::Table::open(&mut txn).unwrap();
        state.set_compact_marker(true).unwrap();
        drop(state);
        txn.commit().unwrap();
    }
    let txn = db.begin_read().unwrap();
    let state = state::Table::open_readonly(&txn).unwrap();
    assert!(state.compact_marker().unwrap());
}

// The row-shape pins (buckets 4, object_meta 11, uploads 5): redb's
// `TableDefinition` exposes no value-arity getter in 4.2, so each shape
// is pinned by writing and reading back a full row through the public
// handle — the destructure below fails to compile on a narrower tuple,
// and the equality assertions would fail on a stray element.

#[test]
fn bucket_row_round_trips_the_five_element_shape() {
    let db = mem_db();
    let created_at = SystemTime::UNIX_EPOCH + Duration::from_nanos(7);
    {
        let mut txn = db.begin_write().unwrap();
        let mut table = bucket::Table::open(&mut txn).unwrap();
        table
            .put_full(
                "data",
                &BucketRow {
                    tags: "a=b".into(),
                    owner: OWNER.into(),
                    acl: GRANTS.into(),
                    ..BucketRow::at(created_at)
                },
            )
            .unwrap();
        drop(table);
        txn.commit().unwrap();
    }
    let txn = db.begin_read().unwrap();
    let table = bucket::Table::open_readonly(&txn).unwrap();
    let row = table.row("data").unwrap().unwrap();
    assert_eq!(row.created, created_at);
    assert_eq!(row.tags, "a=b");
    assert_eq!(row.owner, OWNER);
    assert_eq!(row.acl, GRANTS);
}

#[test]
fn meta_row_round_trips_the_eleven_element_shape() {
    let db = mem_db();
    let owner = OwnerId::new(OWNER).unwrap();
    let stored = object_meta::Stored {
        version_id: None,
        version_seq: 0,
        is_delete_marker: false,
        etag: ETAG.parse::<ETag>().unwrap(),
        size: 1,
        mtime: 2,
        file_identity: 0,
        tags: object::Tags::from_wire_limited("a=b", OBJECT_TAGS_MAX),
        checksum: None,
        owner: Some(owner.clone()),
        acl: Acl {
            owner: None,
            grants: vec![Grant {
                grantee: Grantee::Canonical(owner),
                permission: acl::Permission::FullControl,
            }],
        },
    };
    {
        let mut txn = db.begin_write().unwrap();
        let mut table = object_meta::Table::open(&mut txn).unwrap();
        table.put("data", "k", &stored).unwrap();
        drop(table);
        txn.commit().unwrap();
    }
    let txn = db.begin_read().unwrap();
    let table = object_meta::Table::open_readonly(&txn).unwrap();
    assert_eq!(table.get("data", "k").unwrap(), Some(stored.clone()));

    // The version elements round-trip too: a real identifier (the null
    // version is the empty row's `None`), its sequence, and the recorded
    // delete-marker flag.
    let versioned = object_meta::Stored {
        version_id: Some(version::id("v1").unwrap()),
        version_seq: 3,
        is_delete_marker: true,
        ..stored
    };
    {
        let mut txn = db.begin_write().unwrap();
        let mut table = object_meta::Table::open(&mut txn).unwrap();
        table.put("data", "versioned", &versioned).unwrap();
        drop(table);
        txn.commit().unwrap();
    }
    let txn = db.begin_read().unwrap();
    let table = object_meta::Table::open_readonly(&txn).unwrap();
    let row = table.get("data", "versioned").unwrap().unwrap();
    assert_eq!(row.version_id.unwrap().as_str(), "v1");
    assert_eq!(row.version_seq, 3);
    assert!(row.is_delete_marker, "the recorded marker flag round-trips");
    assert_eq!(row.etag, versioned.etag, "the other elements ride");
}

#[test]
fn upload_row_round_trips_the_five_element_shape() {
    let db = mem_db();
    let initiated = SystemTime::UNIX_EPOCH + Duration::from_nanos(42);
    {
        let mut txn = db.begin_write().unwrap();
        let mut table = upload::Table::open(&mut txn).unwrap();
        table
            .put("data", "u1", "dir/key", initiated, "t=1", OWNER, GRANTS)
            .unwrap();
        drop(table);
        txn.commit().unwrap();
    }
    let txn = db.begin_read().unwrap();
    let table = upload::Table::open_readonly(&txn).unwrap();
    let (key, initiated_at, tags_wire, owner_wire, acl_wire) = table
        .get_matching("data", "dir/key", "u1")
        .unwrap()
        .unwrap();
    assert_eq!(key, "dir/key");
    assert_eq!(initiated_at, to_nanos(initiated));
    assert_eq!(tags_wire, "t=1");
    assert_eq!(owner_wire, OWNER);
    assert_eq!(acl_wire, GRANTS);
}

#[test]
fn decode_owner_wire_is_none_on_empty_or_domain_invalid() {
    assert_eq!(crate::decode_owner_wire(""), None);
    assert_eq!(crate::decode_owner_wire("not-a-canonical-id"), None);
    assert_eq!(
        crate::decode_owner_wire(OWNER),
        Some(OwnerId::new(OWNER).unwrap())
    );
}

#[test]
fn decode_acl_wire_is_private_default_on_empty_or_domain_invalid() {
    assert_eq!(crate::decode_acl_wire(""), Acl::default_private(None));
    assert_eq!(
        crate::decode_acl_wire("garbage![nope"),
        Acl::default_private(None)
    );
    let acl = crate::decode_acl_wire(GRANTS);
    assert_eq!(acl.owner, None);
    assert_eq!(acl.grants.len(), 1);
    assert_eq!(Acl::from_grants_wire(GRANTS), acl);
}
