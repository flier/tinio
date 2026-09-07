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
    acl::Acl,
    checksum::{Algorithm, Part, Recorded, Type as ChecksumType, Upload, Value},
    cors,
    etag::ETag,
    object::{self, Tags},
};
use tinio_store::{
    bucket::{self, BucketRow},
    ensure_all, meta, object_part, objects, part, part_checksum, part_data, part_meta, state,
    store::Handle,
    upload, upload_checksum,
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
        let row = t.row("data")?.expect("present");
        assert_eq!(row.created, now);
        assert_eq!(row.tags, "");
        assert_eq!(row.owner, "");
        assert_eq!(row.acl, "");
        assert_eq!(row.cors, "");
        // The tagging write upserts the whole row — every wire is
        // preserved at its own slot (the same-typed `&str` wires must
        // not swap elements).
        t.put_full(
            "data",
            &BucketRow {
                tags: "env=prod".into(),
                owner: "owner:w".into(),
                acl: "acl:w".into(),
                cors: "cors:w".into(),
                ..BucketRow::at(now)
            },
        )?;
        let row = t.row("data")?.unwrap();
        assert_eq!(row.tags, "env=prod");
        assert_eq!(row.owner, "owner:w");
        assert_eq!(row.acl, "acl:w");
        assert_eq!(row.cors, "cors:w");
        // The list/head first-sight upsert must keep the first time AND
        // the stored wires (never clear them).
        let recorded = t.get_or_insert("data", now + Duration::from_secs(1))?;
        assert_eq!(recorded, now);
        let row = t.row("data")?.unwrap();
        assert_eq!(
            (row.created, row.tags, row.owner, row.acl, row.cors),
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

#[test]
fn bucket_cors_accessors_round_trip_and_self_heal() {
    let h = handle();
    let now = SystemTime::UNIX_EPOCH;
    let cfg = cors::Config {
        rules: vec![cors::Rule {
            id: Some("one".into()),
            allowed_methods: vec!["GET".into()],
            allowed_origins: vec!["*".into()],
            allowed_headers: None,
            expose_headers: None,
            max_age_seconds: None,
        }],
    };
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = bucket::Table::open(txn)?;
        // Missing row: outer None; writes are no-ops.
        assert!(t.cors("data")?.is_none());
        assert!(t.put_cors("data", &cfg)?.is_none());
        assert!(t.clear_cors("data")?.is_none());
        t.put("data", now)?;
        // Fresh row: present, no configuration.
        assert_eq!(t.cors("data")?, Some(None));
        assert_eq!(t.put_cors("data", &cfg)?, Some(true));
        assert_eq!(t.cors("data")?, Some(Some(cfg.clone())));
        // Identical put skips the write.
        assert_eq!(t.put_cors("data", &cfg)?, Some(false));
        // Empty rules normalize to "no configuration".
        assert_eq!(t.put_cors("data", &cors::Config::default())?, Some(true));
        assert_eq!(t.cors("data")?, Some(None));
        t.put_cors("data", &cfg)?;
        assert_eq!(t.clear_cors("data")?, Some(true));
        assert_eq!(t.cors("data")?, Some(None));
        assert_eq!(t.clear_cors("data")?, Some(false));
        Ok(())
    })
    .unwrap();
    // A garbage CORS wire self-heals on the table accessor.
    h.write(|txn| -> Result<(), tinio_store::Error> {
        bucket::Table::open(txn)?
            .insert("data", (0u64, "", "", "", "a,b,c"))
            .map_err(tinio_store::Error::from)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        assert_eq!(
            bucket::Table::open_readonly(txn)?.cors("data")?,
            Some(None),
            "a corrupt CORS wire is no configuration"
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn bucket_tags_accessors_round_trip_and_self_heal() {
    let h = handle();
    let now = SystemTime::UNIX_EPOCH;
    let tags = Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = bucket::Table::open(txn)?;
        // Missing row: None; writes are no-ops.
        assert!(t.tags("data")?.is_none());
        assert!(t.put_tags("data", &tags)?.is_none());
        assert!(t.clear_tags("data")?.is_none());
        t.put("data", now)?;
        // Fresh row: present, empty set.
        assert_eq!(t.tags("data")?, Some(Tags::empty()));
        assert_eq!(t.put_tags("data", &tags)?, Some(true));
        assert_eq!(t.tags("data")?, Some(tags.clone()));
        // Identical put skips the write.
        assert_eq!(t.put_tags("data", &tags)?, Some(false));
        // Empty set normalizes to the '' wire.
        assert_eq!(t.put_tags("data", &Tags::empty())?, Some(true));
        assert_eq!(t.tags("data")?, Some(Tags::empty()));
        t.put_tags("data", &tags)?;
        assert_eq!(t.clear_tags("data")?, Some(true));
        assert_eq!(t.tags("data")?, Some(Tags::empty()));
        assert_eq!(t.clear_tags("data")?, Some(false));
        Ok(())
    })
    .unwrap();
    // A garbage tags wire self-heals on the table accessor.
    h.write(|txn| -> Result<(), tinio_store::Error> {
        bucket::Table::open(txn)?
            .insert("data", (0u64, "garbage", "", "", ""))
            .map_err(tinio_store::Error::from)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        assert_eq!(
            bucket::Table::open_readonly(txn)?.tags("data")?,
            Some(Tags::empty()),
            "a corrupt tags wire is the empty set"
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn bucket_put_tags_or_create_records_the_first_sight_row() {
    let h = handle();
    let tags = Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
    let zero = SystemTime::UNIX_EPOCH;
    let later = SystemTime::UNIX_EPOCH + Duration::from_secs(9);
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = bucket::Table::open(txn)?;
        // Row-miss: ONE upsert records the creation time AND the tags
        // (the fs first-sight policy, replacing the old put + re-rewrite).
        assert!(t.put_tags_or_create("data", &tags, zero)?);
        assert_eq!(t.get("data")?, Some(zero));
        assert_eq!(t.tags("data")?, Some(tags.clone()));
        // Existing row: tags replaced, the recorded creation time kept.
        let new_tags = Tags::from_pairs([("team".into(), "core".into())]).unwrap();
        assert!(t.put_tags_or_create("data", &new_tags, later)?);
        assert_eq!(t.get("data")?, Some(zero));
        assert_eq!(t.tags("data")?, Some(new_tags.clone()));
        // Identical set: no write.
        assert!(!t.put_tags_or_create("data", &new_tags, later)?);
        Ok(())
    })
    .unwrap();
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
        owner: None,
        acl: Acl::default_private(None),
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
fn object_meta_get_self_heals_garbage_tags_and_checksum() {
    let h = handle();
    let key = object::key("g.txt").unwrap();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        meta::Table::open(txn)?
            .insert(
                ("data", key.as_ref().as_str()),
                (
                    "d41d8cd98f00b204e9800998ecf8427e",
                    1u64,
                    0u64,
                    0u64,
                    "env=%zz",
                    "CRC32:AA==:NOPE",
                    "",
                    "",
                ),
            )
            .map_err(tinio_store::Error::from)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let got = meta::Table::open_readonly(txn)?
            .get("data", &key)?
            .expect("a valid etag keeps the row");
        assert!(got.tags.is_empty(), "a corrupt tags wire is the empty set");
        assert!(
            got.checksum.is_none(),
            "a corrupt checksum wire is no checksum"
        );
        assert_eq!(got.size, 1);
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
        owner: None,
        acl: Acl::default_private(None),
    };
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = meta::Table::open(txn)?;
        t.put("data", &object::key("ok.txt").unwrap(), &valid)?;
        // A corrupt-etag row (written by a stale writer) self-heals on
        // the walk rather than failing it — the gating load's discipline.
        t.insert(
            ("data", "bad-etag"),
            ("not-an-etag", 1u64, 0u64, 0u64, "", "", "", ""),
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
fn object_meta_tag_accessors_rewrite_only_the_element() {
    let h = handle();
    let key = object::key("a.txt").unwrap();
    let tags = Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
    let stored = meta::Stored {
        etag: etag("d41d8cd98f00b204e9800998ecf8427e"),
        size: 3,
        mtime: 1,
        file_identity: 2,
        tags: Tags::empty(),
        checksum: None,
        owner: None,
        acl: Acl::default_private(None),
    };
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = meta::Table::open(txn)?;
        // Missing row: no presence outcome, nothing created.
        assert!(t.put_tags("data", &key, &tags)?.is_none());
        assert!(t.clear_tags("data", &key)?.is_none());
        t.put("data", &key, &stored)?;
        // Set rewrites the tags element, the row's other elements ride.
        assert_eq!(t.put_tags("data", &key, &tags)?, Some(true));
        let got = t.get("data", &key)?.unwrap();
        assert_eq!(got.tags, tags);
        assert_eq!(got.size, 3);
        assert_eq!(got.etag, stored.etag);
        // Identical set: no write.
        assert_eq!(t.put_tags("data", &key, &tags)?, Some(false));
        // Clear: true once, then the already-empty no-write.
        assert_eq!(t.clear_tags("data", &key)?, Some(true));
        assert_eq!(t.clear_tags("data", &key)?, Some(false));
        assert!(t.get("data", &key)?.unwrap().tags.is_empty());
        Ok(())
    })
    .unwrap();
    // A corrupt tags wire self-heals on the read AND the put heals the
    // row back to a clean wire (the row's etag stays valid throughout).
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = meta::Table::open(txn)?;
        t.insert(
            ("data", "a.txt"),
            (
                "d41d8cd98f00b204e9800998ecf8427e",
                3u64,
                1u64,
                2u64,
                "garbage",
                "",
                "",
                "",
            ),
        )
        .map_err(tinio_store::Error::from)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        assert!(
            meta::Table::open_readonly(txn)?
                .get("data", &key)?
                .unwrap()
                .tags
                .is_empty()
        );
        Ok(())
    })
    .unwrap();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        assert_eq!(
            meta::Table::open(txn)?.put_tags("data", &key, &tags)?,
            Some(true)
        );
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let got = meta::Table::open_readonly(txn)?.get("data", &key)?.unwrap();
        assert_eq!(got.tags, tags, "the rewrite normalized the corrupt wire");
        assert_eq!(got.size, 3, "the other elements survived the heal");
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
    let tags = Tags::from_pairs([("k".into(), "v".into())]).unwrap();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = upload::Table::open(txn)?;
        assert!(!t.has_bucket("data")?);
        t.put(
            "data",
            "u1",
            &key,
            SystemTime::UNIX_EPOCH,
            &tags.to_wire(),
            "",
            "",
        )?;
        t.put("data", "u2", &key, SystemTime::UNIX_EPOCH, "", "", "")?;
        t.put("other", "u9", &key, SystemTime::UNIX_EPOCH, "", "", "")?;
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
        let (got_key, initiated, tags_wire, _, _) = t.get_matching("data", &key, "u1")?.unwrap();
        assert_eq!(got_key, &*key);
        assert_eq!(initiated, 0);
        assert_eq!(t.tags("data", &key, "u1")?.unwrap(), tags);
        assert!(!tags_wire.is_empty());
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
        t.for_each(|b, id, _, _, _, _, _| {
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
fn upload_tags_accessors_round_trip_and_self_heal() {
    let h = handle();
    let key = object::key("big.bin").unwrap();
    let tags = Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = upload::Table::open(txn)?;
        t.put("data", "u1", &key, SystemTime::UNIX_EPOCH, &tags.to_wire(), "", "")?;
        assert_eq!(t.tags("data", &key, "u1")?.unwrap(), tags);
        t.put("data", "u1", &key, SystemTime::UNIX_EPOCH, "", "", "")?;
        assert!(t.tags("data", &key, "u1")?.unwrap().is_empty());
        Ok(())
    })
    .unwrap();
    // A garbage tags wire self-heals on the table accessor.
    h.write(|txn| -> Result<(), tinio_store::Error> {
        upload::Table::open(txn)?
            .insert(("data", "u1"), ("big.bin", 0u64, "garbage", "", ""))
            .map_err(tinio_store::Error::from)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let t = upload::Table::open_readonly(txn)?;
        assert!(
            t.tags("data", &key, "u1")?.unwrap().is_empty(),
            "a corrupt upload tags wire is the empty set"
        );
        t.for_bucket("data", |_, (_, _, tags_wire, _, _)| {
            assert!(
                object::Tags::from_wire_limited(tags_wire, object::OBJECT_TAGS_MAX).is_empty(),
                "the bucket scan self-heals the same way"
            );
            Ok(())
        })?;
        t.for_each(|_, _, _, _, tags_wire, _, _| {
            assert!(
                object::Tags::from_wire_limited(tags_wire, object::OBJECT_TAGS_MAX).is_empty(),
                "the whole-table walk self-heals the same way"
            );
            Ok(())
        })?;
        Ok(())
    })
    .unwrap();
}

#[test]
fn upload_tags_accessor_answers_the_identity_check() {
    let h = handle();
    let key = object::key("big.bin").unwrap();
    let tags = Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = upload::Table::open(txn)?;
        // Missing row: None.
        assert!(t.tags("data", &key, "u1")?.is_none());
        t.put("data", "u1", &key, SystemTime::UNIX_EPOCH, &tags.to_wire(), "", "")?;
        // S3 identity is (bucket, key, uploadId): a matching key answers
        // the tags, a mismatched key or upload id answers None (the
        // caller's NoSuchUpload arm).
        assert_eq!(t.tags("data", &key, "u1")?, Some(tags.clone()));
        assert!(
            t.tags("data", &object::key("other.bin").unwrap(), "u1")?
                .is_none()
        );
        assert!(t.tags("data", &key, "u2")?.is_none());
        Ok(())
    })
    .unwrap();
    // A corrupt tags wire still answers Some (the upload exists) with the
    // self-healed empty set.
    h.write(|txn| -> Result<(), tinio_store::Error> {
        upload::Table::open(txn)?
            .insert(("data", "u1"), ("big.bin", 0u64, "garbage", "", ""))
            .map_err(tinio_store::Error::from)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        assert_eq!(
            upload::Table::open_readonly(txn)?.tags("data", &key, "u1")?,
            Some(Tags::empty()),
            "a corrupt upload tags wire is the empty set"
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
        t.put("data", "u1", &key, SystemTime::UNIX_EPOCH, "", "", "")?;
        t.put("other", "u9", &key, SystemTime::UNIX_EPOCH, "", "", "")?;
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
        t.put(
            "data",
            "big.bin",
            &object_part::Stored {
                part_number: 1,
                size: 100,
                checksum: Some(Part {
                    algorithm: Algorithm::Crc32,
                    value: Value("AA==".into()),
                }),
            },
        )?;
        t.put(
            "data",
            "big.bin",
            &object_part::Stored {
                part_number: 2,
                size: 200,
                checksum: None,
            },
        )?;
        t.put(
            "data",
            "big.bin",
            &object_part::Stored {
                part_number: 3,
                size: 50,
                checksum: Some(Part {
                    algorithm: Algorithm::Sha256,
                    value: Value("BB==".into()),
                }),
            },
        )?;
        t.put(
            "data",
            "other.bin",
            &object_part::Stored {
                part_number: 1,
                size: 1,
                checksum: None,
            },
        )?;
        // A garbage checksum wire self-heals: the part is listed without
        // a checksum (F07).
        t.insert(("data", "big.bin", 4), (10u64, "BLAKE3", "AA=="))
            .map_err(tinio_store::Error::from)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let t = object_part::Table::open_readonly(txn)?;
        let rows = t.list("data", "big.bin")?;
        assert_eq!(
            rows,
            vec![
                object_part::Stored {
                    part_number: 1,
                    size: 100,
                    checksum: Some(Part {
                        algorithm: Algorithm::Crc32,
                        value: Value("AA==".into()),
                    }),
                },
                object_part::Stored {
                    part_number: 2,
                    size: 200,
                    checksum: None,
                },
                object_part::Stored {
                    part_number: 3,
                    size: 50,
                    checksum: Some(Part {
                        algorithm: Algorithm::Sha256,
                        value: Value("BB==".into()),
                    }),
                },
                object_part::Stored {
                    part_number: 4,
                    size: 10,
                    checksum: None,
                },
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
            uc.put(
                "data",
                "u1",
                &Upload {
                    algorithm: Algorithm::Crc32,
                    r#type: Some(ChecksumType::FullObject),
                },
            )?;
            uc.put(
                "data",
                "u1",
                &Upload {
                    algorithm: Algorithm::Crc32,
                    r#type: None,
                },
            )?; // upsert replaces the type
        }
        let mut pc = part_checksum::Table::open(txn)?;
        assert!(!pc.has_upload("data", "u1")?);
        pc.put(
            "data",
            "u1",
            1,
            &Part {
                algorithm: Algorithm::Crc32,
                value: Value("NhCmhg==".into()),
            },
        )?;
        pc.put(
            "data",
            "u1",
            2,
            &Part {
                algorithm: Algorithm::Sha256,
                value: Value("BB==".into()),
            },
        )?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let uc = upload_checksum::Table::open_readonly(txn)?;
        assert_eq!(
            uc.get("data", "u1")?,
            Some(Upload {
                algorithm: Algorithm::Crc32,
                r#type: None,
            })
        );
        let pc = part_checksum::Table::open_readonly(txn)?;
        assert!(pc.has_upload("data", "u1")?);
        assert!(!pc.has_upload("data", "u2")?);
        assert_eq!(
            pc.get("data", "u1", 1)?,
            Some(Part {
                algorithm: Algorithm::Crc32,
                value: Value("NhCmhg==".into()),
            })
        );
        assert!(pc.get("data", "u1", 3)?.is_none());
        Ok(())
    })
    .unwrap();
    // A garbage part-checksum wire self-heals on the table accessor.
    h.write(|txn| -> Result<(), tinio_store::Error> {
        part_checksum::Table::open(txn)?
            .insert(("data", "u1", 1), ("BLAKE3", "AAAA"))
            .map_err(tinio_store::Error::from)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let pc = part_checksum::Table::open_readonly(txn)?;
        assert!(
            pc.get("data", "u1", 1)?.is_none(),
            "a corrupt part checksum wire is no checksum"
        );
        assert!(pc.has_upload("data", "u1")?, "the row itself remains");
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
fn part_checksum_set_replaces_and_clears_the_slot() {
    let h = handle();
    let part = Part {
        algorithm: Algorithm::Crc32,
        value: Value("NhCmhg==".into()),
    };
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut pc = part_checksum::Table::open(txn)?;
        // None on a missing row: a no-op (the part carried no checksum).
        pc.set("data", "u1", 1, None)?;
        // Some writes the digest.
        pc.set("data", "u1", 1, Some(&part))?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        assert_eq!(
            part_checksum::Table::open_readonly(txn)?.get("data", "u1", 1)?,
            Some(part.clone())
        );
        Ok(())
    })
    .unwrap();
    // The None arm clears the stale row from a previous upload of this
    // part number (the digest-slot discipline, F-spec 2026-08-31).
    h.write(|txn| -> Result<(), tinio_store::Error> {
        part_checksum::Table::open(txn)?.set("data", "u1", 1, None)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        let pc = part_checksum::Table::open_readonly(txn)?;
        assert!(pc.get("data", "u1", 1)?.is_none());
        assert!(!pc.has_upload("data", "u1")?);
        Ok(())
    })
    .unwrap();
    // And the Some arm re-writes after the clear.
    h.write(|txn| -> Result<(), tinio_store::Error> {
        part_checksum::Table::open(txn)?.set("data", "u1", 1, Some(&part))?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        assert_eq!(
            part_checksum::Table::open_readonly(txn)?.get("data", "u1", 1)?,
            Some(part)
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn upload_checksum_accessors_round_trip_and_self_heal() {
    let h = handle();
    let spec = Upload {
        algorithm: Algorithm::Sha256,
        r#type: Some(ChecksumType::FullObject),
    };
    h.write(|txn| -> Result<(), tinio_store::Error> {
        let mut t = upload_checksum::Table::open(txn)?;
        assert!(t.get("data", "u1")?.is_none());
        t.put("data", "u1", &spec)?;
        assert_eq!(t.get("data", "u1")?, Some(spec.clone()));
        t.put(
            "data",
            "u1",
            &Upload {
                algorithm: Algorithm::Sha256,
                r#type: None,
            },
        )?;
        assert_eq!(
            t.get("data", "u1")?,
            Some(Upload {
                algorithm: Algorithm::Sha256,
                r#type: None,
            })
        );
        Ok(())
    })
    .unwrap();
    // A garbage spec wire self-heals on the table accessor.
    h.write(|txn| -> Result<(), tinio_store::Error> {
        upload_checksum::Table::open(txn)?
            .insert(("data", "u1"), ("BLAKE3", ""))
            .map_err(tinio_store::Error::from)?;
        Ok(())
    })
    .unwrap();
    h.read(|txn| -> Result<(), tinio_store::Error> {
        assert!(
            upload_checksum::Table::open_readonly(txn)?
                .get("data", "u1")?
                .is_none(),
            "a corrupt upload checksum wire is no spec"
        );
        Ok(())
    })
    .unwrap();
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
