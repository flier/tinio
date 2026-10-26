//! The pg cold plane: ownership, the root identity and lazy bucket namespaces.
//!
//! Every leg runs against the live PostgreSQL fixture through
//! `tinio-store-pg`'s scratch-database harness, so each test has a database of
//! its own and leaves nothing behind. A test that reopens a root after its
//! first instance has to let that instance go completely — the ownership lock
//! lives on the dedicated connection the store and its re-verify task share —
//! so the first instance runs on a runtime of its own and the reopen happens
//! once that runtime (and with it the spawned re-verify) has ended.

// This target's file-level helpers are not `#[test]` functions, and clippy's
// `allow-*-in-tests` reach `#[test]` functions only (docs/style.md §Panics).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: an integration-test target's file-level helpers (docs/style.md §Panics)"
)]

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    thread,
    time::{Duration, SystemTime},
};

use tinio_core::{
    AccessMode,
    acl::Acl,
    bucket,
    content::{ContentKey, ObjectContent, stage_body},
    meta::BucketMeta,
    object,
    storage::{self, BucketOps, ListBucketsParams, ListObjectsParams, ObjectOps, Storage},
};
use tinio_fs::{FsStorage, testing};
use tinio_pg::Config as PgConfig;
use tinio_store_pg::test_support::{TestDb, require_dsn};
use tinio_tier::{
    ColdInfo, Config, Error, FlushMode, Index, MoveOutcome, Placement, State, Tier, TierStorage,
    marker_path, pg,
};
use tinio_util::{
    secret::{Secret, SecretKey},
    testing::{body, read_body},
};
use tokio::runtime::Builder;

/// The runtime configuration every leg uses: small limits and synchronous
/// writes, matching the other tier integration targets.
fn config() -> Config {
    Config {
        capacity_bytes: 1024 * 1024,
        max_object_bytes: 64 * 1024,
        flush_mode: FlushMode::Sync,
        batch_bytes: 8 * 1024,
        batch_interval: Duration::from_secs(1),
        heat_window: Duration::from_secs(60),
        promote_reads: 2,
        stats_flush: Duration::from_secs(10),
        high_water_percent: 90,
        low_water_percent: 80,
        demote_idle: Duration::from_secs(3600),
        dwell: Duration::from_secs(3600),
        scan_interval: Duration::from_secs(60),
        move_concurrency: 1,
        index_compact_percent: 30,
        gc_grace: Duration::from_secs(300),
    }
}

/// The scratch database's cold-plane configuration (a small pool: the suite
/// opens several stores at once).
fn pg_config(db: &TestDb) -> PgConfig {
    PgConfig::new(Secret::from(SecretKey::from(db.dsn.clone())), 4)
}

/// Run `f` on a runtime of its own and join it: the first instance of a
/// restart leg must be gone, re-verify task included, before the second opens
/// the same database as its writer.
fn on_its_own_runtime<T, F>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    thread::spawn(f)
        .join()
        .expect("the first instance's runtime ends")
}

/// Whether `err` is the reserved-namespace refusal naming `bucket`.
fn is_reserved_refusal(err: &Error, name: &bucket::Name) -> bool {
    matches!(
        err,
        Error::Storage(storage::Error::AccessDenied(storage::Denied::Bucket(named)))
            if named == name
    )
}

/// Every file under `root` with its bytes, keyed by path: a read-only run must
/// leave this map unchanged.
fn tree(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).expect("the directory is readable") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.insert(path.clone(), fs::read(&path).expect("the file is readable"));
            }
        }
    }
    out
}

/// The cold plane's visible shape around one object: its bucket names and the
/// object's recorded length. A read-only run must leave it unchanged.
async fn cold_snapshot(
    cold: &PgConfig,
    name: &bucket::Name,
    key: &object::Key,
) -> (Vec<String>, Option<u64>) {
    let store = pg::connect(cold, AccessMode::ReadOnly)
        .await
        .expect("the cold plane opens read-only");
    let mut names: Vec<String> = store
        .load_all()
        .await
        .expect("the bucket rows read")
        .into_iter()
        .map(|(bucket, _)| bucket)
        .collect();
    names.sort();
    let len = store
        .stat(name, key)
        .await
        .expect("the cold stat runs")
        .map(|entry| entry.len);
    (names, len)
}

/// A cold round trip survives the first instance's disappearance: the object
/// bytes live in pg, the warm file is gone, and the restarted tier serves it.
#[tokio::test]
async fn a_cold_round_trip_survives_restart() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let cold = pg_config(&db);
    let root = tempfile::tempdir().expect("a temporary root");
    let first_root = root.path().to_path_buf();
    let first_cold = cold.clone();

    let root_id = on_its_own_runtime(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a test runtime");
        runtime.block_on(async move {
            let tier = TierStorage::open_pg(
                &first_root,
                testing::fs_options(),
                config(),
                &first_cold,
                AccessMode::ReadWrite,
                false,
            )
            .await
            .expect("the first instance opens");
            let name = bucket::name("data").expect("a valid bucket name");
            tier.create_bucket(&name, None, &Acl::default_private(None))
                .await
                .expect("the bucket is created");
            let key = object::key("cold.bin").expect("a valid key");
            tier.put_object(&name, &key, body(b"cold bytes"))
                .await
                .expect("the object is written");
            let info = tier
                .head_object(&name, &key)
                .await
                .expect("the row is readable");

            // The settled shape of a demoted object (T4): the bucket namespace
            // exists, the bytes are in pg, the placement is cold and the warm
            // file is gone.
            tier.ensure_cold_bucket(&name)
                .await
                .expect("the first demote creates the namespace");
            let content = ContentKey::Object(key.clone());
            let mut staged = tier
                .cold()
                .stage(&name, &content)
                .await
                .expect("the cold stage opens");
            stage_body(&mut staged, body(b"cold bytes"))
                .await
                .expect("the bytes land");
            tier.cold()
                .publish(&name, staged, &content)
                .await
                .expect("the cold copy is published");
            tier.index()
                .set_placement(
                    &name,
                    &key,
                    &Placement {
                        tier: Tier::Pg,
                        state: State::Live,
                        cold: Some(ColdInfo::from_info(&info)),
                        mtime: info.last_modified,
                    },
                )
                .expect("the cold placement is recorded");
            fs::remove_file(first_root.join("data").join("cold.bin"))
                .expect("the warm file is removed");

            tier.index()
                .root_id()
                .expect("the root id is read")
                .expect("the root is bound")
        })
    });

    let tier = TierStorage::open_pg(
        root.path(),
        testing::fs_options(),
        config(),
        &cold,
        AccessMode::ReadWrite,
        false,
    )
    .await
    .expect("the restarted instance opens");
    assert_eq!(
        tier.index().root_id().expect("the root id is read"),
        Some(root_id),
        "a restart keeps the root identity"
    );
    let name = bucket::name("data").expect("a valid bucket name");
    let key = object::key("cold.bin").expect("a valid key");
    let got = tier
        .get_object(&name, &key, None)
        .await
        .expect("the cold object is served");
    assert_eq!(
        read_body(got.body).await.expect("the body reads"),
        b"cold bytes"
    );
}

/// A bucket's cold namespace is created by the first demote into it, never at
/// construction, and the reserved namespace is never a demote target.
#[tokio::test]
async fn a_bucket_namespace_is_created_on_first_demote() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let root = tempfile::tempdir().expect("a temporary root");
    let tier = TierStorage::open_pg(
        root.path(),
        testing::fs_options(),
        config(),
        &pg_config(&db),
        AccessMode::ReadWrite,
        false,
    )
    .await
    .expect("the tier opens");

    let name = bucket::name("data").expect("a valid bucket name");
    assert!(
        tier.cold()
            .row(&name)
            .await
            .expect("the bucket row is read")
            .is_none(),
        "no namespace exists before the first demote"
    );
    tier.ensure_cold_bucket(&name)
        .await
        .expect("the first demote creates the namespace");
    assert!(
        tier.cold()
            .row(&name)
            .await
            .expect("the bucket row is read")
            .is_some(),
        "the first demote creates the namespace"
    );
    tier.ensure_cold_bucket(&name)
        .await
        .expect("a later demote keeps the namespace");

    let reserved = bucket::name(pg::ROOT_NAMESPACE).expect("the reserved name is valid");
    assert!(
        tier.cold()
            .row(&reserved)
            .await
            .expect("the reserved row is read")
            .is_some(),
        "opening the tier creates the reserved namespace"
    );
    let refused = tier
        .ensure_cold_bucket(&reserved)
        .await
        .expect_err("the reserved namespace is never a demote target");
    assert!(is_reserved_refusal(&refused, &reserved), "{refused}");
}

/// One writable instance per database: the second construction is refused and
/// the refusal names the ownership lock, while a read-only instance starts
/// beside the owner.
#[tokio::test]
async fn ownership_refuses_a_second_instance() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let cold = pg_config(&db);
    let first = pg::connect(&cold, AccessMode::ReadWrite)
        .await
        .expect("the first writable instance owns the database");
    let refused = pg::connect(&cold, AccessMode::ReadWrite)
        .await
        .expect_err("a second writable instance is refused");
    assert!(matches!(refused, Error::Cold(_)), "{refused}");
    assert!(
        refused.to_string().contains("advisory lock"),
        "the refusal names the ownership lock: {refused}"
    );
    let reader = pg::connect(&cold, AccessMode::ReadOnly)
        .await
        .expect("a read-only instance starts beside the owner");
    drop(reader);
    drop(first);
}

/// A fresh index over a database that already holds a root refuses until the
/// operator adopts it.
#[tokio::test]
async fn a_fresh_index_over_non_empty_pg_refuses() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let cold = pg_config(&db);
    let owner = tempfile::tempdir().expect("the owning root");
    let owner_path = owner.path().to_path_buf();
    let owner_cold = cold.clone();
    on_its_own_runtime(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a test runtime");
        runtime.block_on(async move {
            TierStorage::open_pg(
                &owner_path,
                testing::fs_options(),
                config(),
                &owner_cold,
                AccessMode::ReadWrite,
                false,
            )
            .await
            .expect("the owning instance opens");
        })
    });

    let fresh = tempfile::tempdir().expect("a fresh root");
    let refused = TierStorage::open_pg(
        fresh.path(),
        testing::fs_options(),
        config(),
        &cold,
        AccessMode::ReadWrite,
        false,
    )
    .await
    .err()
    .expect("a fresh index over a bound database refuses");
    assert!(matches!(refused, Error::AdoptRequired), "{refused}");
}

/// `--adopt` binds a fresh index to the root identity the database already
/// records.
#[tokio::test]
async fn adopt_matches_the_root_id() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let cold = pg_config(&db);
    let owner = tempfile::tempdir().expect("the owning root");
    let owner_path = owner.path().to_path_buf();
    let owner_cold = cold.clone();
    let root_id = on_its_own_runtime(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a test runtime");
        runtime.block_on(async move {
            let tier = TierStorage::open_pg(
                &owner_path,
                testing::fs_options(),
                config(),
                &owner_cold,
                AccessMode::ReadWrite,
                false,
            )
            .await
            .expect("the owning instance opens");
            tier.index()
                .root_id()
                .expect("the root id is read")
                .expect("the root is bound")
        })
    });

    let fresh = tempfile::tempdir().expect("a fresh root");
    let tier = TierStorage::open_pg(
        fresh.path(),
        testing::fs_options(),
        config(),
        &cold,
        AccessMode::ReadWrite,
        true,
    )
    .await
    .expect("--adopt binds a matching root");
    assert_eq!(
        tier.index().root_id().expect("the root id is read"),
        Some(root_id),
        "--adopt copies the database's root identity into the fresh index"
    );
}

/// `--adopt` refuses an index whose root identity is not the database's.
#[tokio::test]
async fn adopt_refuses_a_foreign_root_id() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let cold = pg_config(&db);
    let owner = tempfile::tempdir().expect("the owning root");
    let owner_path = owner.path().to_path_buf();
    let owner_cold = cold.clone();
    let cold_root = on_its_own_runtime(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a test runtime");
        runtime.block_on(async move {
            let tier = TierStorage::open_pg(
                &owner_path,
                testing::fs_options(),
                config(),
                &owner_cold,
                AccessMode::ReadWrite,
                false,
            )
            .await
            .expect("the owning instance opens");
            tier.index()
                .root_id()
                .expect("the root id is read")
                .expect("the root is bound")
        })
    });

    let foreign = tempfile::tempdir().expect("a foreign root");
    let foreign_id = [0xab; 16];
    {
        let fs = FsStorage::new(foreign.path(), testing::fs_options())
            .expect("the foreign fs store opens");
        let index = Index::open(fs.state_dir().join("tier.redb")).expect("the foreign index opens");
        index
            .set_root_id(foreign_id)
            .expect("the foreign root id is recorded");
    }
    assert_ne!(cold_root, foreign_id);

    let refused = TierStorage::open_pg(
        foreign.path(),
        testing::fs_options(),
        config(),
        &cold,
        AccessMode::ReadWrite,
        true,
    )
    .await
    .err()
    .expect("--adopt refuses a foreign root");
    assert!(matches!(refused, Error::RootMismatch { .. }), "{refused}");
}

/// The reserved namespace is invisible to listings, refused by every
/// operation, and a root that already holds it cannot enter tier mode.
#[tokio::test]
async fn the_reserved_namespace_is_hidden_and_protected() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let root = tempfile::tempdir().expect("a temporary root");
    let tier = TierStorage::open_pg(
        root.path(),
        testing::fs_options(),
        config(),
        &pg_config(&db),
        AccessMode::ReadWrite,
        false,
    )
    .await
    .expect("the tier opens");
    let reserved = bucket::name(pg::ROOT_NAMESPACE).expect("the reserved name is valid");
    assert!(pg::is_reserved(&reserved));
    let normal = bucket::name("data").expect("a valid bucket name");
    assert!(!pg::is_reserved(&normal));
    tier.create_bucket(&normal, None, &Acl::default_private(None))
        .await
        .expect("the copy source's bucket is created");

    let listed = tier
        .list_buckets(
            ListBucketsParams {
                prefix: String::new(),
                start_after: None,
                max_buckets: 100,
            },
            None,
            None,
        )
        .await
        .expect("the buckets list");
    assert!(
        listed.buckets.iter().all(|entry| entry.name != reserved),
        "the reserved namespace is never listed"
    );

    let refused = tier
        .create_bucket(&reserved, None, &Acl::default_private(None))
        .await
        .expect_err("the reserved namespace cannot be created");
    assert!(is_reserved_refusal(&refused, &reserved), "{refused}");
    let refused = tier
        .head_bucket(&reserved)
        .await
        .expect_err("the reserved namespace cannot be read");
    assert!(is_reserved_refusal(&refused, &reserved), "{refused}");
    let key = object::key("planted.bin").expect("a valid key");
    let refused = tier
        .put_object(&reserved, &key, body(b"planted"))
        .await
        .expect_err("no object can be written there");
    assert!(is_reserved_refusal(&refused, &reserved), "{refused}");
    let refused = tier
        .get_object(&reserved, &key, None)
        .await
        .expect_err("no object can be read there");
    assert!(is_reserved_refusal(&refused, &reserved), "{refused}");
    let refused = tier
        .list_objects(ListObjectsParams {
            bucket: reserved.clone(),
            prefix: String::new(),
            delimiter: None,
            start_after: None,
            max_keys: 100,
        })
        .await
        .expect_err("the reserved namespace cannot be listed");
    assert!(is_reserved_refusal(&refused, &reserved), "{refused}");

    // The DELETE and the COPY destination are refused too, not only the PUT.
    let refused = tier
        .delete_object(&reserved, &key)
        .await
        .expect_err("the reserved namespace cannot be deleted from");
    assert!(is_reserved_refusal(&refused, &reserved), "{refused}");
    let source = object::key("source.bin").expect("a valid key");
    tier.put_object(&normal, &source, body(b"source"))
        .await
        .expect("the copy source is written");
    let destination = object::key("copied.bin").expect("a valid key");
    let refused = tier
        .copy_object(
            &normal,
            &source,
            &reserved,
            &destination,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
            None,
        )
        .await
        .expect_err("the reserved namespace is never a copy destination");
    assert!(is_reserved_refusal(&refused, &reserved), "{refused}");
    let refused = tier
        .copy_object(
            &reserved,
            &key,
            &normal,
            &destination,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
            None,
        )
        .await
        .expect_err("the reserved namespace is never a copy source");
    assert!(is_reserved_refusal(&refused, &reserved), "{refused}");
    let refused = tier
        .delete_bucket(&reserved)
        .await
        .expect_err("the reserved namespace cannot be torn down");
    assert!(is_reserved_refusal(&refused, &reserved), "{refused}");

    // A root that already holds the reserved name cannot be converted.
    let taken = TestDb::create().await.expect("a second scratch database");
    let taken_root = tempfile::tempdir().expect("a taken root");
    {
        let fs =
            FsStorage::new(taken_root.path(), testing::fs_options()).expect("the fs store opens");
        fs.create_bucket(&reserved, None, &Acl::default_private(None))
            .await
            .expect("the reserved bucket exists before conversion");
    }
    let refused = TierStorage::open_pg(
        taken_root.path(),
        testing::fs_options(),
        config(),
        &pg_config(&taken),
        AccessMode::ReadWrite,
        false,
    )
    .await
    .err()
    .expect("a root already holding the reserved name refuses conversion");
    assert!(matches!(refused, Error::ReservedRoot { .. }), "{refused}");
}

/// A read-only run over the production cold plane opens the index read-only,
/// serves a cold GET, HEAD and listing, and writes nothing: the marker is not
/// refreshed, the planes are not repaired, and no ownership task starts (T27).
#[tokio::test]
async fn a_read_only_pg_run_serves_cold_and_writes_nothing() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let cold = pg_config(&db);
    let root = tempfile::tempdir().expect("a temporary root");
    let writer_root = root.path().to_path_buf();
    let writer_cold = cold.clone();
    let (root_id, written) = on_its_own_runtime(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a test runtime");
        runtime.block_on(async move {
            let tier = TierStorage::open_pg(
                &writer_root,
                testing::fs_options(),
                config(),
                &writer_cold,
                AccessMode::ReadWrite,
                false,
            )
            .await
            .expect("the writer opens");
            let name = bucket::name("data").expect("a valid bucket name");
            tier.create_bucket(&name, None, &Acl::default_private(None))
                .await
                .expect("the bucket is created");
            let key = object::key("cold.bin").expect("a valid key");
            tier.put_object(&name, &key, body(b"read only bytes"))
                .await
                .expect("the object is written");
            let moved = tier
                .demote(&name, &key, SystemTime::now())
                .await
                .expect("the demote runs");
            assert!(matches!(moved, MoveOutcome::Moved { .. }));
            fs::remove_file(writer_root.join("data").join("cold.bin"))
                .expect("the leftover file is removed");
            let info = tier
                .head_object(&name, &key)
                .await
                .expect("the cold row is readable");
            let id = tier
                .index()
                .root_id()
                .expect("the root id is read")
                .expect("the root is bound");
            (id, info)
        })
    });

    let name = bucket::name("data").expect("a valid bucket name");
    let key = object::key("cold.bin").expect("a valid key");
    let marker = marker_path(root.path());
    let marker_before = fs::read(&marker).expect("the marker reads");
    let marker_mtime = fs::metadata(&marker)
        .expect("the marker stats")
        .modified()
        .expect("the marker records a modification time");
    let tree_before = tree(root.path());
    let cold_before = cold_snapshot(&cold, &name, &key).await;

    let tier = TierStorage::open_pg(
        root.path(),
        testing::fs_options(),
        config(),
        &cold,
        AccessMode::ReadOnly,
        false,
    )
    .await
    .expect("the read-only run opens");
    assert!(tier.supports_tiers());
    assert_eq!(
        tier.index().root_id().expect("the root id reads"),
        Some(root_id)
    );

    let got = tier
        .get_object(&name, &key, None)
        .await
        .expect("the cold object is served");
    assert_eq!(
        read_body(got.body).await.expect("the body reads"),
        b"read only bytes"
    );
    let head = tier
        .head_object(&name, &key)
        .await
        .expect("the cold head answers");
    assert_eq!(head.etag, written.etag);
    assert_eq!(head.size, written.size);
    let listing = tier
        .list_objects(ListObjectsParams {
            bucket: name.clone(),
            prefix: String::new(),
            delimiter: None,
            start_after: None,
            max_keys: 100,
        })
        .await
        .expect("the bucket lists");
    assert!(
        listing.objects.iter().any(|info| info.key == key),
        "the cold object is listed"
    );

    // The index is read-only: a metadata write is refused, not applied.
    let refused = tier
        .put_object_tags(
            &name,
            &key,
            &object::Tags::from_pairs([("env".to_owned(), "prod".to_owned())])
                .expect("a valid tag set"),
        )
        .await;
    assert!(
        matches!(refused, Err(Error::ReadOnly)),
        "the read-only index refuses a write: {refused:?}"
    );

    // No ownership lock and no re-verify task: a writable connection opens
    // beside the read-only run.
    let owner = pg::connect(&cold, AccessMode::ReadWrite)
        .await
        .expect("the read-only run holds no ownership lock");
    drop(owner);
    drop(tier);

    assert_eq!(
        fs::read(&marker).expect("the marker reads"),
        marker_before,
        "the read-only run does not refresh the marker"
    );
    assert_eq!(
        fs::metadata(&marker)
            .expect("the marker stats")
            .modified()
            .expect("the marker records a modification time"),
        marker_mtime,
        "the marker is not rewritten"
    );
    assert!(
        !marker_path(root.path())
            .with_file_name("tier.mode.tmp")
            .exists(),
        "the read-only run leaves no marker temp"
    );
    assert_eq!(
        tree(root.path()),
        tree_before,
        "the read-only run writes nothing into the root"
    );
    assert_eq!(
        cold_snapshot(&cold, &name, &key).await,
        cold_before,
        "the read-only run repairs nothing on the cold plane"
    );
}

/// A root whose marker names a different root than the cold plane refuses the
/// pg bind, and the refusal rewrites neither record (T18).
#[tokio::test]
async fn a_pg_marker_naming_a_foreign_root_refuses() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let cold = pg_config(&db);
    let owner = tempfile::tempdir().expect("the owning root");
    let owner_path = owner.path().to_path_buf();
    let owner_cold = cold.clone();
    let owner_id = on_its_own_runtime(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a test runtime");
        runtime.block_on(async move {
            let tier = TierStorage::open_pg(
                &owner_path,
                testing::fs_options(),
                config(),
                &owner_cold,
                AccessMode::ReadWrite,
                false,
            )
            .await
            .expect("the owning instance opens");
            tier.index()
                .root_id()
                .expect("the root id is read")
                .expect("the root is bound")
        })
    });

    let foreign = tempfile::tempdir().expect("a foreign root");
    let foreign_id = [0xcd; 16];
    {
        let fs = FsStorage::new(foreign.path(), testing::fs_options())
            .expect("the foreign fs store opens");
        let index = Index::open(fs.state_dir().join("tier.redb")).expect("the foreign index opens");
        index
            .set_root_id(foreign_id)
            .expect("the foreign identity is recorded");
    }
    fs::write(marker_path(foreign.path()), foreign_id).expect("the foreign marker is written");

    let foreign_path = foreign.path().to_path_buf();
    let refused_cold = cold.clone();
    // The refused construction still opens a writable cold connection, and its
    // re-verify task holds the database for its runtime's life: run it on a
    // runtime of its own, as every other writable leg does.
    let refused = on_its_own_runtime(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a test runtime");
        runtime.block_on(async move {
            TierStorage::open_pg(
                &foreign_path,
                testing::fs_options(),
                config(),
                &refused_cold,
                AccessMode::ReadWrite,
                false,
            )
            .await
            .err()
            .expect("a marker naming a foreign root refuses the pg bind")
        })
    });
    assert!(matches!(refused, Error::RootMismatch { .. }), "{refused}");
    assert_eq!(
        fs::read(marker_path(foreign.path())).expect("the marker survives"),
        foreign_id.to_vec(),
        "the refusal does not rewrite the marker"
    );

    // The database still belongs to the owner: its root opens unchanged.
    let owner_reopen = owner.path().to_path_buf();
    let reopen_cold = cold.clone();
    let reopened_id = on_its_own_runtime(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a test runtime");
        runtime.block_on(async move {
            let reopened = TierStorage::open_pg(
                &owner_reopen,
                testing::fs_options(),
                config(),
                &reopen_cold,
                AccessMode::ReadWrite,
                false,
            )
            .await
            .expect("the owning root still binds");
            reopened
                .index()
                .root_id()
                .expect("the root id reads")
                .expect("the root is bound")
        })
    });
    assert_eq!(
        Some(reopened_id),
        Some(owner_id),
        "the refusal changed neither record"
    );
}

/// An unreachable server is an error at both entry points, never a panic or a
/// silently empty plane.
#[tokio::test]
async fn pg_unreachable_errors() {
    let unreachable = PgConfig::new(
        Secret::from(SecretKey::from(
            "postgres://tinio:tinio@127.0.0.1:1/tinio?sslmode=disable".to_owned(),
        )),
        1,
    );
    let refused = pg::connect(&unreachable, AccessMode::ReadWrite)
        .await
        .expect_err("no server listens there");
    assert!(matches!(refused, Error::Cold(_)), "{refused}");

    let root = tempfile::tempdir().expect("a temporary root");
    let refused = TierStorage::open_pg(
        root.path(),
        testing::fs_options(),
        config(),
        &unreachable,
        AccessMode::ReadWrite,
        false,
    )
    .await
    .err()
    .expect("the tier refuses an unreachable cold plane");
    assert!(matches!(refused, Error::Cold(_)), "{refused}");
}
