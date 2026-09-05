//! Row-level tests for the shared store — the single home of the row
//! semantics both backends share (spec 2026-09-03 §4). These were
//! extracted from the tinio-mem/tinio-fs backend test suites and live by
//! the tables they exercise: a row round-trip, the self-heal boundary
//! (corrupt rows neither fail the walk nor leak a sibling bucket), and
//! the per-bucket scan/drain (a prefix stops before a longer key —
//! redb-notes pit 14, the no-exclusive-upper-bound ruling).

use std::time::{Duration, SystemTime};

use redb::{Database, TableDefinition};
use tinio_core::{
    checksum::{Algorithm, Part, Recorded, Type as ChecksumType, Value},
    etag::ETag,
    object::{self, Tags},
};
use tinio_store::{
    bucket, ensure_all, meta, object_part, objects, part, part_checksum, part_data, part_meta,
    state, store::Handle, upload, upload_checksum,
};

/// A ready store handle over a fresh in-memory redb database — the
/// byte-format guard's home: no tempdir, no fs `Handle`, no tokio.
fn handle() -> Handle {
    let db = Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .unwrap();
    let handle = Handle::new(db);
    handle.write(ensure_all).unwrap();
    handle
}

/// A short single-upload ETag.
fn etag(hex: &str) -> ETag {
    ETag::new(hex).unwrap()
}

#[test]
fn bucket_put_get_put_full_and_get_or_insert() {
    let h = handle();
    let now = SystemTime::UNIX_EPOCH + Duration::from_nanos(42);
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = bucket::Table::open(txn)?;
        // Absent bucket -> no row.
        assert!(!t.exists("data")?);
        assert!(t.get("data")?.is_none());
        // Record: put writes (created, empty tags/owner/acl/cors).
        t.put("data", now)?;
        assert!(t.exists("data")?);
        assert_eq!(t.get("data")?, Some(now));
        let (created, tags, owner, acl, cors) = t.row("data")?.expect("present");
        assert_eq!((created, tags), (now, "".to_string()));
        assert_eq!(
            (owner, acl, cors),
            ("".to_string(), "".to_string(), "".to_string())
        );
        // The tagging write upserts the whole row — every wire is
        // preserved at its own position (the same-typed `&str` wires
        // must not swap positions).
        t.put_full("data", now, "env=prod", "owner:w", "acl:w", "cors:w")?;
        let (_, tags, owner, acl, cors) = t.row("data")?.unwrap();
        assert_eq!(tags, "env=prod");
        assert_eq!(owner, "owner:w");
        assert_eq!(acl, "acl:w");
        assert_eq!(cors, "cors:w");
        // The list/head first-sight upsert must keep the first time AND
        // the stored wires (never clear them).
        let recorded = t.get_or_insert("data", now + Duration::from_secs(1))?;
        assert_eq!(recorded, now);
        let (created, tags, owner, acl, cors) = t.row("data")?.unwrap();
        assert_eq!(
            (created, tags, owner, acl, cors),
            (
                now,
                "env=prod".to_string(),
                "owner:w".to_string(),
                "acl:w".to_string(),
                "cors:w".to_string(),
            )
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn bucket_iterates_in_name_order_and_remove_is_idempotent() {
    let h = handle();
    let now = SystemTime::UNIX_EPOCH;
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = bucket::Table::open(txn)?;
        t.put("zeta", now)?;
        t.put("alpha", now)?;
        t.put("mid", now)?;
        let mut names = Vec::new();
        t.for_each(|name, _| {
            names.push(name.to_string());
            Ok(())
        })?;
        assert_eq!(names, ["alpha", "mid", "zeta"]);
        t.remove("mid")?;
        t.remove("mid")?; // idempotent
        Ok(())
    })
    .unwrap();
    let names = h
        .read(|txn| -> Result<Vec<String>, tinio_store::Error> {
            let t = bucket::Table::open_readonly(txn)?;
            let mut out = Vec::new();
            t.for_each(|n, _| {
                out.push(n.to_string());
                Ok(())
            })?;
            Ok(out)
        })
        .unwrap();
    assert_eq!(names, ["alpha", "zeta"]);
}

/// The on-disk format guard (final-review F2, no-migration ruling): a
/// `buckets` table written under a LEGACY tuple arity must NOT open under
/// the current 5-tuple definition. redb binds the key/value type names at
/// the `TableDefinition`, so `check_match` answers `TableTypeMismatch`
/// at open — an old state dir fails loudly (never silently misreads a
/// row); there is no migration, and the documented recovery is deleting
/// the state dir.
#[test]
fn legacy_buckets_arity_fails_loudly_on_open() {
    // The pre-tagging row shape: (created_at_nanos, tags_wire).
    const LEGACY: TableDefinition<'static, &'static str, (u64, &'static str)> =
        TableDefinition::new("buckets");

    let db = Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .unwrap();
    {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(LEGACY).unwrap();
            table.insert("legacy-bucket", (42u64, "tag=wire")).unwrap();
        }
        txn.commit().unwrap();
    }
    // Opening the SAME table under the CURRENT definition (the store's
    // 5-tuple) must fail at open with the table-type mismatch.
    let mut txn = db.begin_write().unwrap();
    let err = bucket::Table::open(&mut txn)
        .err()
        .expect("legacy arity must not open");
    assert!(
        matches!(
            err,
            tinio_store::Error::Table(redb::TableError::TableTypeMismatch { .. })
        ),
        "{err:?}"
    );
}

#[test]
fn object_meta_put_round_trips_all_elements() {
    let h = handle();
    let key = object::key("dir/a.txt").unwrap();
    let hex = "5eb63bbbe01eeed093cb22bb8f5acdc3";
    let written = meta::Stored {
        etag: etag(hex),
        size: 11,
        mtime: 0,
        file_identity: 7,
        tags: Tags::from_pairs([("env".into(), "prod".into())]).unwrap(),
        checksum: Some(Recorded {
            part: Part {
                algorithm: Algorithm::Crc32,
                value: Value("NhCmhg==".into()),
            },
            kind: ChecksumType::FullObject,
        }),
    };
    h.write(|txn| -> Result<(), tinio_store::Error> {
        meta::Table::open(txn)?.put("data", &key, &written)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let t = meta::Table::open_readonly(txn)?;
        let got = t.get("data", &key)?.expect("the put row must be readable");
        assert_eq!(got, written.clone());
        // Missing bucket/key -> None.
        assert!(t.get("nope", &key)?.is_none());
        assert!(t.get("data", &object::key("nope.txt").unwrap())?.is_none());
        Ok(())
    })
    .unwrap();
    // Idempotent remove (its own txn), then a separate drain txn —
    // draining in the same txn as a write trips a redb page-manager
    // assertion (the drain-and-insert caveat).
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = meta::Table::open(txn)?;
        t.remove("data", &key)?;
        assert!(t.get("data", &key)?.is_none());
        Ok(())
    })
    .unwrap();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = meta::Table::open(txn)?;
        t.put("data", &key, &written)?;
        Ok(())
    })
    .unwrap();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = meta::Table::open(txn)?;
        t.drain_bucket("data")?;
        assert!(t.get("data", &key)?.is_none());
        Ok(())
    })
    .unwrap();
}

#[test]
fn object_meta_walk_self_heals_a_corrupt_etag_row() {
    let h = handle();
    let valid = meta::Stored {
        etag: etag("d41d8cd98f00b204e9800998ecf8427e"),
        size: 1,
        mtime: 0,
        file_identity: 0,
        tags: Tags::empty(),
        checksum: None,
    };
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = meta::Table::open(txn)?;
        t.put("data", &object::key("ok.txt").unwrap(), &valid)?;
        // A corrupt-etag row (written by a stale writer) self-heals on
        // the walk rather than failing it — the gating load's discipline.
        t.insert(
            ("data", "bad-etag"),
            ("not-an-etag", 1u64, 0u64, 0u64, "", ""),
        )
        .map_err(tinio_store::Error::from)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let t = meta::Table::open_readonly(txn)?;
        let mut saw_valid = false;
        let mut saw_corrupt = false;
        t.for_bucket_gated("data", |key, stored| {
            if &*key == "ok.txt" {
                saw_valid = true;
                assert!(stored.is_some());
            } else if &*key == "bad-etag" {
                saw_corrupt = true;
                assert!(stored.is_none(), "corrupt etag row self-heals");
            }
            Ok(())
        })?;
        assert!(saw_valid && saw_corrupt);
        Ok(())
    })
    .unwrap();
}

#[test]
fn objects_put_get_remove_and_has_bucket() {
    let h = handle();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = objects::Table::open(txn)?;
        assert!(!t.has_bucket("data")?);
        t.put("data", "a.txt", b"hello")?;
        t.put("data", "b.txt", b"world")?;
        assert!(t.has_bucket("data")?);
        assert!(!t.has_bucket("other")?);
        t.remove("data", "a.txt")?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let t = objects::Table::open_readonly(txn)?;
        assert!(t.get("data", "a.txt")?.is_none());
        let guard = t.get("data", "b.txt")?.expect("present");
        assert_eq!(guard.value(), b"world");
        Ok(())
    })
    .unwrap();
}

#[test]
fn upload_rows_key_match_bucket_scan_and_for_each() {
    let h = handle();
    let key = object::key("big.bin").unwrap();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = upload::Table::open(txn)?;
        assert!(!t.has_bucket("data")?);
        t.put("data", "u1", &key, SystemTime::UNIX_EPOCH, "k=v")?;
        t.put("data", "u2", &key, SystemTime::UNIX_EPOCH, "")?;
        t.put("other", "u9", &key, SystemTime::UNIX_EPOCH, "")?;
        assert!(t.has_bucket("data")?);
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let t = upload::Table::open_readonly(txn)?;
        // key_matches / get_matching bind the S3 identity (bucket,key,id).
        assert!(t.key_matches("data", &key, "u1")?);
        assert!(!t.key_matches("data", "wrong-key", "u1")?);
        assert!(!t.key_matches("data", &key, "u5")?);
        let (got_key, initiated, tags) = t.get_matching("data", &key, "u1")?.unwrap();
        assert_eq!(got_key, &*key);
        assert_eq!(initiated, 0);
        assert_eq!(tags, "k=v");
        assert!(t.get_matching("data", &key, "u5")?.is_none());
        // The bucket scan visits only this bucket's uploads, in key order.
        let mut ids = Vec::new();
        t.for_bucket("data", |id, _| {
            ids.push(id.to_string());
            Ok(())
        })?;
        assert_eq!(ids, ["u1", "u2"]);
        // The whole-table walk.
        let mut all = Vec::new();
        t.for_each(|b, id, _, _, _| {
            all.push((b.to_string(), id.to_string()));
            Ok(())
        })?;
        assert_eq!(
            all,
            vec![
                ("data".into(), "u1".into()),
                ("data".into(), "u2".into()),
                ("other".into(), "u9".into()),
            ]
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn upload_drain_bucket_removes_only_the_bucket() {
    let h = handle();
    let key = object::key("k").unwrap();
    // Insert into a write txn, then drain in a separate one (drain and
    // insert in the same txn trips a redb page-manager assertion).
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = upload::Table::open(txn)?;
        t.put("data", "u1", &key, SystemTime::UNIX_EPOCH, "")?;
        t.put("other", "u9", &key, SystemTime::UNIX_EPOCH, "")?;
        Ok(())
    })
    .unwrap();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = upload::Table::open(txn)?;
        t.drain_bucket("data")?;
        assert!(!t.has_bucket("data")?);
        Ok(())
    })
    .unwrap();
    assert!(
        !h.read(|txn| upload::Table::open_readonly(txn)?.key_matches("data", &key, "u1"))
            .unwrap()
    );
}

#[test]
fn part_rows_list_from_pagination_and_boundary() {
    let h = handle();
    let e1 = etag("d41d8cd98f00b204e9800998ecf8427e");
    let e2 = etag("900150983cd24fb0d6963f7d28e17f72");
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = part::Table::open(txn)?;
        t.put("data", "u1", 1, &e1)?;
        t.put("data", "u1", 2, &e2)?;
        t.put("data", "u2", 1, &e1)?;
        t.put("other", "u9", 1, &e1)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let t = part::Table::open_readonly(txn)?;
        // A full page, no truncation; it stops before the sibling upload.
        let (page, truncated) = t.list_from("data", "u1", 0, 10)?;
        assert_eq!(page, [(1, e1.to_string()), (2, e2.to_string())]);
        assert!(!truncated);
        // A page max of 1 truncates when the second part is present.
        let (page, truncated) = t.list_from("data", "u1", 0, 1)?;
        assert_eq!(page, [(1, e1.to_string())]);
        assert!(truncated);
        // A start-into page resumes from the offset.
        let (page, _) = t.list_from("data", "u1", 2, 10)?;
        assert_eq!(page, [(2, e2.to_string())]);
        // A missing upload -> empty, no truncation.
        let (page, truncated) = t.list_from("data", "nope", 0, 10)?;
        assert!(page.is_empty() && !truncated);
        Ok(())
    })
    .unwrap();
}

#[test]
fn object_part_rows_list_in_order_and_remove_key() {
    let h = handle();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = object_part::Table::open(txn)?;
        t.put("data", "big.bin", 1, 100, "CRC32", "AA==")?;
        t.put("data", "big.bin", 2, 200, "", "")?;
        t.put("data", "big.bin", 3, 50, "SHA256", "BB==")?;
        t.put("data", "other.bin", 1, 1, "", "")?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let t = object_part::Table::open_readonly(txn)?;
        let rows = t.list("data", "big.bin")?;
        assert_eq!(
            rows,
            vec![
                (1, 100, "CRC32".to_string(), "AA==".to_string()),
                (2, 200, "".to_string(), "".to_string()),
                (3, 50, "SHA256".to_string(), "BB==".to_string()),
            ]
        );
        // A different key's rows do not bleed in.
        assert_eq!(t.list("data", "other.bin")?.len(), 1);
        Ok(())
    })
    .unwrap();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        object_part::Table::open(txn)?.remove_key("data", "big.bin")?;
        Ok(())
    })
    .unwrap();
    assert!(
        h.read(|txn| object_part::Table::open_readonly(txn)?.list("data", "big.bin"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn upload_checksum_and_part_checksum_rows() {
    let h = handle();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        {
            let mut uc = upload_checksum::Table::open(txn)?;
            assert!(uc.get("data", "u1")?.is_none());
            uc.put("data", "u1", "CRC32", "FULL_OBJECT")?;
            uc.put("data", "u1", "CRC32", "")?; // upsert replaces the type
        }
        let mut pc = part_checksum::Table::open(txn)?;
        assert!(!pc.has_upload("data", "u1")?);
        pc.put("data", "u1", 1, "CRC32", "NhCmhg==")?;
        pc.put("data", "u1", 2, "SHA256", "BB==")?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let uc = upload_checksum::Table::open_readonly(txn)?;
        assert_eq!(uc.get("data", "u1")?, Some(("CRC32".into(), "".into())));
        let pc = part_checksum::Table::open_readonly(txn)?;
        assert!(pc.has_upload("data", "u1")?);
        assert!(!pc.has_upload("data", "u2")?);
        assert_eq!(
            pc.get("data", "u1", 1)?,
            Some(("CRC32".into(), "NhCmhg==".into()))
        );
        assert!(pc.get("data", "u1", 3)?.is_none());
        Ok(())
    })
    .unwrap();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut pc = part_checksum::Table::open(txn)?;
        pc.remove("data", "u1", 1)?;
        pc.drain_upload("data", "u1")?;
        Ok(())
    })
    .unwrap();
    assert!(
        !h.read(|txn| part_checksum::Table::open_readonly(txn)?.has_upload("data", "u1"))
            .unwrap()
    );
}

#[test]
fn part_data_and_part_meta_rows_round_trip_and_total_len() {
    let h = handle();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        {
            let mut pd = part_data::Table::open(txn)?;
            pd.put("data", "u1", 1, b"ab")?;
            pd.put("data", "u1", 2, b"cdef")?;
            pd.put("data", "u2", 1, b"zz")?;
        }
        let mut pm = part_meta::Table::open(txn)?;
        pm.put("data", "u1", 1, 2, 100)?;
        pm.put("data", "u1", 2, 4, 200)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let pd = part_data::Table::open_readonly(txn)?;
        assert_eq!(pd.total_len("data", "u1")?, 6);
        assert_eq!(pd.total_len("data", "u2")?, 2);
        // A sibling upload's rows do not leak into the scan.
        assert_eq!(pd.total_len("data", "u3")?, 0);
        let pm = part_meta::Table::open_readonly(txn)?;
        assert_eq!(pm.get("data", "u1", 1)?, Some((2, 100)));
        assert!(pm.get("data", "u1", 9)?.is_none());
        Ok(())
    })
    .unwrap();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        part_data::Table::open(txn)?.drain_upload("data", "u1")?;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        h.read(|txn| part_data::Table::open_readonly(txn)?.total_len("data", "u1"))
            .unwrap(),
        0
    );
}

#[test]
fn state_version_and_compact_marker_round_trip() {
    let h = handle();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        state::Table::open(txn)?.write_version(state::FORMAT_VERSION)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let t = state::Table::open_readonly(txn)?;
        assert_eq!(t.version()?, Some(state::FORMAT_VERSION));
        assert!(!t.compact_marker()?, "absent marker => false");
        Ok(())
    })
    .unwrap();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        state::Table::open(txn)?.set_compact_marker(true)?;
        Ok(())
    })
    .unwrap();
    assert!(
        h.read(|txn| state::Table::open_readonly(txn)?.compact_marker())
            .unwrap()
    );
}

#[test]
fn handle_write_commits_on_success_and_aborts_on_error() {
    let h = handle();
    // A failing write closure aborts the transaction — nothing is visible.
    let err = h.write(|txn| -> Result<(), tinio_store::Error> {
        bucket::Table::open(txn)?.put("rolled-back", SystemTime::UNIX_EPOCH)?;
        Err(tinio_store::Error::Storage(redb::StorageError::Corrupted(
            "boom".into(),
        )))
    });
    assert!(err.is_err());
    assert!(
        !h.read(|txn| bucket::Table::open_readonly(txn)?.exists("rolled-back"))
            .unwrap()
    );
    // A succeeding write commits.
    h.write(|txn| -> Result<(), tinio_store::Error> {
        bucket::Table::open(txn)?.put("committed", SystemTime::UNIX_EPOCH)?;
        Ok(())
    })
    .unwrap();
    assert!(
        h.read(|txn| bucket::Table::open_readonly(txn)?.exists("committed"))
            .unwrap()
    );
}
