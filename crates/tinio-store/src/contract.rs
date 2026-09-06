//! The shared-layer contract tests: the row self-heal rule and the state
//! version/marker protocol — pinned here over a plain in-memory redb
//! database (no tempdir, no `Handle`, no tokio), the home of the rules
//! both backends share.

use redb::{Database, ReadableDatabase};

use crate::{meta, state};

fn mem_db() -> Database {
    Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .unwrap()
}

#[test]
fn meta_validate_self_heals_every_element_independently() {
    // The 6-tuple: (etag, size, mtime, file_identity, tags wire, checksum wire).
    let valid = meta::validate(("d41d8cd98f00b204e9800998ecf8427e", 1, 2, 0, "", ""));
    let row = valid.expect("an empty tags/checksum wire is valid");
    assert_eq!(row.size, 1);
    assert!(row.tags.is_empty());
    assert!(row.checksum.is_none());

    // A garbage etag is the only element that fails the whole row.
    assert!(meta::validate(("not-an-etag", 1, 2, 0, "", "")).is_none());

    // Garbage tags/checksum wires self-heal individually; the row survives.
    let healed = meta::validate((
        "d41d8cd98f00b204e9800998ecf8427e",
        1,
        2,
        0,
        "team=%zz&",
        "CRC32:@@:NOPE",
    ))
    .expect("the etag is valid, the row is served");
    assert!(healed.tags.is_empty());
    assert!(healed.checksum.is_none());
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
