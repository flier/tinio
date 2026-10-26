//! The tier layer's behavioural conformance over the real filesystem plane and
//! a real PostgreSQL cold plane, plus the crash matrix.
//!
//! Every leg opens a scratch database through `tinio-store-pg`'s test harness
//! and a temporary warm root, so the suites run against the contract's planes
//! rather than a fake. The shared `tinio_util::testing` aggregate suite (the
//! whole `Storage` surface: buckets, objects, copy, listing and multipart,
//! including tags, ACLs, checksums and the retained parts) and its versioning
//! leg run first; the legs after them pin the behaviour a tier backend adds: a
//! cold read answered from `ColdInfo`, cold tags/ACL, the null-version address,
//! the batch buffer reaching the cold plane, a read-only run and a restart.
//!
//! **The harness drains metadata-only bodies.** `assert_conformance` checks a
//! GET's metadata without reading its body, and T14 holds a read's shared lock
//! through the body stream; the suite now drains each such body
//! (`read_body(get.body)`) before it writes or deletes the same key, so the
//! aggregate suite runs over the tier exactly as it runs over the fs, mem and
//! pg backends. The tier-specific legs below are still where the cold plane,
//! the crash states and the read-only/restart behaviour are pinned.
//!
//! The crash matrix plants the exact rows, placements and files a crashed move
//! or delete leaves behind (T12, T13) — the states `moves.rs` already covers
//! over a fake cold plane — and holds an in-flight read across the collection
//! pass, so the pass's `busy` answer and the reader's bytes are both asserted.
//! A crash state is never mocked: the state is the state.

// This target's file-level helpers are not `#[test]` functions, and clippy's
// `allow-*-in-tests` reach `#[test]` functions only (docs/style.md §Panics).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: an integration-test target's file-level helpers (docs/style.md §Panics)"
)]

use std::{
    fs,
    path::PathBuf,
    thread,
    time::{Duration, SystemTime},
};

use tinio_core::{
    AccessMode, Class, ETag,
    acl::Acl,
    bucket,
    content::{ContentKey, ObjectContent, stage_body},
    object, object_lock,
    storage::{
        self, BucketOps, ByteRange, GetObjectResult, ListBucketsParams, ListObjectsParams,
        ObjectOps, Storage, VersionOps,
    },
    version,
};
use tinio_fs::testing;
use tinio_pg::Config as PgConfig;
use tinio_store_pg::test_support::{TestDb, require_dsn};
use tinio_tier::{
    ColdInfo, ColdStore, Config, Error as TierError, FlushMode, Index, MoveOutcome, Placement,
    State, Tier, TierStorage, index_path,
};
use tinio_util::{
    secret::{Secret, SecretKey},
    testing::{
        assert_conformance, body, conform_group_read_grant, conform_owner, conformance_versioning,
        read_body,
    },
};
use tokio::runtime::{Builder, Runtime};

/// The bucket every leg writes to.
const DATA: &str = "data";

/// The collection grace the fixture records; a crash state is aged past it.
const GRACE_SECS: u64 = 300;

/// The runtime configuration every leg uses: synchronous writes, small limits
/// and a long grace. `max_object_bytes` admits the fixtures' whole bodies, so
/// a full cold GET also fills the object cache — a read path, not a move.
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
        gc_grace: Duration::from_secs(GRACE_SECS),
    }
}

/// The same limits with the batch write buffer in front of the warm plane.
fn batch_config() -> Config {
    Config {
        flush_mode: FlushMode::Batch,
        ..config()
    }
}

/// The scratch database's cold-plane configuration (a small pool: the suite
/// opens several stores at once).
fn pg_config(db: &TestDb) -> PgConfig {
    PgConfig::new(Secret::from(SecretKey::from(db.dsn.clone())), 4)
}

/// A real warm root, a real cold database and the tier over both.
struct Fixture {
    /// The scratch database, dropped with the fixture.
    _db: TestDb,
    /// The warm root.
    root: tempfile::TempDir,
    /// The tier under test.
    tier: TierStorage<ColdStore>,
    /// The one bucket every leg uses.
    bucket: bucket::Name,
}

impl Fixture {
    /// Open a fresh root and scratch database with the default configuration.
    async fn new() -> Self {
        Self::with_config(config()).await
    }

    /// Open a fresh root and scratch database with `config`.
    async fn with_config(config: Config) -> Self {
        let db = TestDb::create().await.expect("a scratch database");
        let root = tempfile::tempdir().expect("a temporary root");
        let tier = TierStorage::open_pg(
            root.path(),
            testing::fs_options(),
            config,
            &pg_config(&db),
            AccessMode::ReadWrite,
            false,
        )
        .await
        .expect("the tier opens");
        let bucket = bucket::name(DATA).expect("a valid bucket name");
        tier.create_bucket(&bucket, None, &Acl::default_private(None))
            .await
            .expect("the bucket is created");
        Self {
            _db: db,
            root,
            tier,
            bucket,
        }
    }

    /// The warm file a plain key maps to.
    fn path(&self, name: &str) -> PathBuf {
        self.root.path().join(DATA).join(name)
    }

    /// Write one object through the tier's write path and answer its row.
    async fn put(&self, name: &str, bytes: &[u8]) -> object::Info {
        let key = key(name);
        self.tier
            .put_object(&self.bucket, &key, body(bytes.to_vec()))
            .await
            .expect("the object is written");
        self.tier
            .head_object(&self.bucket, &key)
            .await
            .expect("the object is readable")
    }

    /// Stage and commit `bytes` into `bucket` (`name`), answering the
    /// committed row — the version identity rides this answer.
    async fn commit(&self, bucket: &bucket::Name, name: &str, bytes: &[u8]) -> object::Info {
        let key = key(name);
        let staged = self
            .tier
            .stage_body(bucket, &key, body(bytes.to_vec()), None, None)
            .await
            .expect("the body stages");
        self.tier
            .commit_object(
                bucket,
                &key,
                staged,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
                None,
            )
            .await
            .expect("the commit lands")
    }

    /// Write one object and leave it settled in the cold plane: demoted, with
    /// the leftover warm file removed.
    async fn make_cold(&self, name: &str, bytes: &[u8]) -> object::Info {
        let info = self.put(name, bytes).await;
        let moved = self
            .tier
            .demote(&self.bucket, &info.key, SystemTime::now())
            .await
            .expect("the demote runs");
        assert!(
            matches!(moved, MoveOutcome::Moved { .. }),
            "the fixture object reaches the cold plane"
        );
        fs::remove_file(self.path(name)).expect("the leftover warm file is removed");
        info
    }

    /// Publish `bytes` into the cold plane without recording a placement — the
    /// copy a crashed demote or a foreign writer left.
    async fn publish_cold(&self, name: &str, bytes: &[u8]) {
        let key = key(name);
        self.tier
            .ensure_cold_bucket(&self.bucket)
            .await
            .expect("the cold namespace exists");
        let content = ContentKey::Object(key.clone());
        let mut staged = self
            .tier
            .cold()
            .stage(&self.bucket, &content)
            .await
            .expect("the cold stage opens");
        stage_body(&mut staged, body(bytes.to_vec()))
            .await
            .expect("the cold bytes land");
        self.tier
            .cold()
            .publish(&self.bucket, staged, &content)
            .await
            .expect("the cold copy is published");
    }

    /// Record `placement` for `name`.
    fn place(&self, name: &str, placement: Placement) {
        self.tier
            .index()
            .set_placement(&self.bucket, &key(name), &placement)
            .expect("the placement is written");
    }

    /// The placement `name` records, if any.
    fn placement(&self, name: &str) -> Option<Placement> {
        self.tier
            .index()
            .placement(&self.bucket, &key(name))
            .expect("the placement is read")
    }

    /// The warm row `name` records, if any.
    async fn row(&self, name: &str) -> Option<object::Info> {
        self.tier
            .fs()
            .tier_rows()
            .info(&self.bucket, &key(name))
            .await
            .expect("the row is read")
    }

    /// Whether the cold plane holds content for `name`.
    async fn cold_has(&self, name: &str) -> bool {
        self.tier
            .cold()
            .stat(&self.bucket, &key(name))
            .await
            .expect("the cold stat runs")
            .is_some()
    }

    /// Read `name` whole through the tier.
    async fn get(&self, name: &str) -> Vec<u8> {
        let got = self
            .tier
            .get_object(&self.bucket, &key(name), None)
            .await
            .expect("the object is served");
        read_body(got.body).await.expect("the body reads")
    }

    /// Start a read and hand back the result without consuming its body: the
    /// per-key read guard stays held until the body is dropped (T14).
    async fn in_flight(&self, name: &str) -> GetObjectResult {
        self.tier
            .get_object(&self.bucket, &key(name), None)
            .await
            .expect("the in-flight read serves")
    }

    /// A time older than the collection grace.
    fn aged(&self, now: SystemTime) -> SystemTime {
        now - Duration::from_secs(GRACE_SECS + 1)
    }
}

/// `name` as a validated object key.
fn key(name: &str) -> object::Key {
    object::key(name).expect("a valid object key")
}

/// One cold GET's answered body.
async fn answered(result: GetObjectResult) -> Vec<u8> {
    read_body(result.body).await.expect("the body reads")
}

/// The contract error a tier error projects onto.
fn contract(err: TierError) -> storage::Error {
    err.into()
}

/// Run `f` on a runtime of its own and join it: a writable tier must be gone,
/// re-verify task included, before the same database is opened again.
fn on_its_own_runtime<T, F>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    thread::spawn(f).join().expect("the leg's runtime ends")
}

/// The runtime one reopen leg drives.
fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a test runtime")
}

/// The shared aggregate suite, then its versioning leg: the whole `Storage`
/// surface against the real warm and cold planes.
#[tokio::test]
async fn the_shared_conformance_suite_holds_over_fs_and_pg() {
    if !require_dsn() {
        return;
    }
    let fixture = Fixture::new().await;
    assert_conformance(&fixture.tier).await;

    let versioned = bucket::name("conform-versioned").expect("a valid bucket name");
    fixture
        .tier
        .create_bucket(&versioned, None, &Acl::default_private(None))
        .await
        .expect("the versioned leg's bucket");
    conformance_versioning(&fixture.tier, &versioned).await;
}

/// The warm surface over the real planes: buckets, object CRUD, ranges,
/// tags/ACL, copy/rename, listing (prefix, delimiter, pagination), folder
/// markers and the idempotent delete.
#[tokio::test]
async fn the_warm_surface_conforms() {
    if !require_dsn() {
        return;
    }
    let fixture = Fixture::new().await;
    let bucket = fixture.bucket.clone();

    fixture
        .tier
        .head_bucket(&bucket)
        .await
        .expect("the bucket heads");
    let buckets = fixture
        .tier
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
    assert!(buckets.buckets.iter().any(|entry| entry.name == bucket));

    let alpha = b"alpha";
    for (name, bytes) in [
        ("a.txt", &alpha[..]),
        ("dir/b.txt", &b"bravo"[..]),
        ("dir/c.txt", &b"charlie"[..]),
    ] {
        fixture
            .tier
            .put_object(&bucket, &key(name), body(bytes.to_vec()))
            .await
            .expect("the object is written");
    }
    let head = fixture
        .tier
        .head_object(&bucket, &key("a.txt"))
        .await
        .expect("the object heads");
    assert_eq!(head.size, alpha.len() as u64);
    assert_eq!(head.etag, ETag::from_content(alpha));
    assert_eq!(fixture.get("a.txt").await, &alpha[..]);

    for (range, expected) in [
        (ByteRange::From(1), &b"lpha"[..]),
        (ByteRange::Suffix(3), &b"pha"[..]),
        (ByteRange::Inclusive(0, 2), &b"alp"[..]),
    ] {
        let got = fixture
            .tier
            .get_object(&bucket, &key("a.txt"), Some(range))
            .await
            .expect("the range serves");
        assert_eq!(
            read_body(got.body).await.expect("the range reads"),
            expected
        );
    }

    let tags =
        object::Tags::from_pairs([("env".to_owned(), "prod".to_owned())]).expect("a valid tag set");
    fixture
        .tier
        .put_object_tags(&bucket, &key("a.txt"), &tags)
        .await
        .expect("the tags are written");
    assert_eq!(
        fixture
            .tier
            .get_object_tags(&bucket, &key("a.txt"))
            .await
            .expect("the tags read"),
        tags
    );
    fixture
        .tier
        .delete_object_tags(&bucket, &key("a.txt"))
        .await
        .expect("the tags are removed");
    assert_eq!(
        fixture
            .tier
            .get_object_tags(&bucket, &key("a.txt"))
            .await
            .expect("the tags read"),
        object::Tags::empty()
    );

    let owner = conform_owner();
    let staged = fixture
        .tier
        .stage_body(&bucket, &key("acl.txt"), body(b"acl".to_vec()), None, None)
        .await
        .expect("the ACL fixture stages");
    fixture
        .tier
        .commit_object(
            &bucket,
            &key("acl.txt"),
            staged,
            object::Tags::empty(),
            Some(&owner),
            &Acl::default_private(Some(owner.clone())),
            None,
        )
        .await
        .expect("the owned write commits");
    let grants = vec![conform_group_read_grant()];
    fixture
        .tier
        .put_object_acl(&bucket, &key("acl.txt"), &grants)
        .await
        .expect("the ACL is replaced");
    let acl = fixture
        .tier
        .get_object_acl(&bucket, &key("acl.txt"))
        .await
        .expect("the ACL reads");
    assert_eq!(acl.owner, Some(owner), "the owner is preserved");
    assert_eq!(acl.grants, grants, "the grants are replaced");

    let copied = fixture
        .tier
        .copy_object(
            &bucket,
            &key("a.txt"),
            &bucket,
            &key("copy.txt"),
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
            None,
        )
        .await
        .expect("the copy commits");
    assert_eq!(copied.etag, ETag::from_content(alpha));
    let renamed = fixture
        .tier
        .rename_object(&bucket, &key("copy.txt"), &key("renamed.txt"))
        .await
        .expect("the rename commits");
    assert_eq!(renamed.key, key("renamed.txt"));
    let missing = fixture
        .tier
        .get_object(&bucket, &key("copy.txt"), None)
        .await
        .expect_err("the rename consumed the source");
    assert!(matches!(contract(missing), storage::Error::NoSuchKey(_)));
    assert_eq!(fixture.get("renamed.txt").await, &alpha[..]);

    let listing = |prefix: &str, delimiter: Option<&str>, start_after: Option<String>, max_keys| {
        let params = ListObjectsParams {
            bucket: bucket.clone(),
            prefix: prefix.to_owned(),
            delimiter: delimiter.map(str::to_owned),
            start_after,
            max_keys,
        };
        fixture.tier.list_objects(params)
    };
    let prefixed = listing("dir/", None, None, 100)
        .await
        .expect("the prefix lists");
    assert_eq!(prefixed.objects.len(), 2);
    assert!(
        prefixed
            .objects
            .iter()
            .all(|info| info.key.starts_with("dir/"))
    );
    let grouped = listing("", Some("/"), None, 100)
        .await
        .expect("the delimiter lists");
    assert!(
        grouped
            .common_prefixes
            .iter()
            .any(|prefix| prefix == "dir/"),
        "the delimiter rolls the keys into one prefix: {:?}",
        grouped.common_prefixes
    );

    let mut paged = Vec::new();
    let mut after = None;
    loop {
        let page = listing("", None, after, 2).await.expect("the page lists");
        assert!(page.objects.len() <= 2);
        paged.extend(page.objects.into_iter().map(|info| info.key));
        match page.next_start_after {
            Some(next) if page.truncated => after = Some(next),
            _ => break,
        }
    }
    assert_eq!(paged.len(), 5, "every object is paged exactly once");

    fixture
        .tier
        .put_object(&bucket, &key("folder/"), body(Vec::<u8>::new()))
        .await
        .expect("the folder marker is written");
    let marker = fixture
        .tier
        .get_object(&bucket, &key("folder/"), None)
        .await
        .expect_err("a folder marker is not an object");
    assert!(matches!(contract(marker), storage::Error::NoSuchKey(_)));

    fixture
        .tier
        .delete_object(&bucket, &key("a.txt"))
        .await
        .expect("the object is deleted");
    let gone = fixture
        .tier
        .get_object(&bucket, &key("a.txt"), None)
        .await
        .expect_err("the deleted object is gone");
    assert!(matches!(contract(gone), storage::Error::NoSuchKey(_)));
    fixture
        .tier
        .delete_object(&bucket, &key("a.txt"))
        .await
        .expect("a second delete is idempotent");

    let other = bucket::name("other").expect("a valid bucket name");
    fixture
        .tier
        .create_bucket(&other, None, &Acl::default_private(None))
        .await
        .expect("the second bucket is created");
    fixture
        .tier
        .put_object(&other, &key("only.bin"), body(b"x".to_vec()))
        .await
        .expect("the object is written");
    let not_empty = fixture
        .tier
        .delete_bucket(&other)
        .await
        .expect_err("a bucket holding an object is not empty");
    assert!(matches!(contract(not_empty), storage::Error::NotEmpty(_)));
    fixture
        .tier
        .delete_object(&other, &key("only.bin"))
        .await
        .expect("the object is deleted");
    fixture
        .tier
        .delete_bucket(&other)
        .await
        .expect("the empty bucket is removed");
    let gone = fixture
        .tier
        .head_bucket(&other)
        .await
        .expect_err("the removed bucket is gone");
    assert!(matches!(contract(gone), storage::Error::NoSuchBucket(_)));
}

/// The versioning surface over the tier: identity-minted writes, the
/// superseded read, the enabled-bucket marker and its removal, a suspended
/// bucket's null marker with the real versions still addressable, and the
/// null-version delete that removes the null marker (T6, Task 10's fix).
///
/// `the_shared_conformance_suite_holds_over_fs_and_pg` runs
/// `tinio_util::testing::conformance_versioning` as well; this leg pins the
/// same contract at the tier boundary the shared leg does not name.
#[tokio::test]
async fn the_versioning_surface_conforms() {
    if !require_dsn() {
        return;
    }
    let fixture = Fixture::new().await;
    let bucket = bucket::name("versions").expect("a valid bucket name");
    fixture
        .tier
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .expect("the versioned bucket is created");
    assert_eq!(
        fixture
            .tier
            .bucket_versioning(&bucket)
            .await
            .expect("the state reads"),
        version::State::Unversioned
    );
    fixture
        .tier
        .set_bucket_versioning(&bucket, version::State::Enabled)
        .await
        .expect("versioning is enabled");

    let first = fixture.commit(&bucket, "v.bin", b"first").await;
    let first_id = first
        .version_id
        .clone()
        .expect("the first write mints an id");
    let second = fixture.commit(&bucket, "v.bin", b"second").await;
    let second_id = second
        .version_id
        .clone()
        .expect("the second write mints an id");
    assert_ne!(first_id, second_id);

    let old = fixture
        .tier
        .get_object_version(&bucket, &key("v.bin"), Some(&first_id), None)
        .await
        .expect("the superseded version reads");
    let version::ReadResult::Object(old) = old else {
        panic!("a content version is not a marker");
    };
    assert_eq!(old.info.version_id, Some(first_id.clone()));
    assert_eq!(read_body(old.body).await.expect("the body reads"), b"first");
    let current = fixture
        .tier
        .get_object(&bucket, &key("v.bin"), None)
        .await
        .expect("the current version reads");
    assert_eq!(
        read_body(current.body).await.expect("the body reads"),
        b"second"
    );

    let listing = fixture
        .tier
        .list_object_versions(
            &bucket,
            version::ListParams {
                prefix: Some("v.bin".to_owned()),
                max_keys: 100,
                ..Default::default()
            },
        )
        .await
        .expect("the versions list");
    assert!(
        listing
            .entries
            .iter()
            .any(|entry| entry.id.as_ref() == Some(&first_id)),
        "the superseded version is listed"
    );
    assert!(
        listing
            .entries
            .iter()
            .any(|entry| entry.id.as_ref() == Some(&second_id))
    );

    let marker = fixture
        .tier
        .delete_object(&bucket, &key("v.bin"))
        .await
        .expect("the enabled delete writes a marker");
    assert!(marker.is_delete_marker);
    let marker_id = marker.id.clone().expect("the marker has an identity");
    let hidden = fixture
        .tier
        .get_object(&bucket, &key("v.bin"), None)
        .await
        .expect_err("the marker hides the key");
    assert!(matches!(contract(hidden), storage::Error::NoSuchKey(_)));
    fixture
        .tier
        .delete_object_version(
            &bucket,
            &key("v.bin"),
            Some(&marker_id),
            object_lock::Bypass::None,
        )
        .await
        .expect("the marker is removed");
    assert_eq!(
        fixture
            .tier
            .get_object(&bucket, &key("v.bin"), None)
            .await
            .map(|got| got.info.etag.clone())
            .expect("the second version is current again"),
        second.etag
    );

    // A suspended DELETE makes a null-identity marker current; the real
    // versions stay addressable by their ids.
    fixture
        .tier
        .set_bucket_versioning(&bucket, version::State::Suspended)
        .await
        .expect("versioning is suspended");
    let null_marker = fixture
        .tier
        .delete_object(&bucket, &key("v.bin"))
        .await
        .expect("the suspended delete writes a null marker");
    assert!(null_marker.is_delete_marker);
    assert!(null_marker.id.is_none(), "the null marker has no identity");
    let by_id = fixture
        .tier
        .get_object_version(&bucket, &key("v.bin"), Some(&second_id), None)
        .await
        .expect("a real version survives the null marker");
    let version::ReadResult::Object(by_id) = by_id else {
        panic!("a content version is not a marker");
    };
    assert_eq!(
        read_body(by_id.body).await.expect("the body reads"),
        b"second"
    );

    // The null address removes the null-identity marker instead of writing
    // another one (T6, Task 10's fix): the latest real version is current
    // again, and no null version remains to name.
    let unmarked = fixture
        .tier
        .delete_object_version(&bucket, &key("v.bin"), None, object_lock::Bypass::None)
        .await
        .expect("the null version is removed");
    assert!(unmarked.is_delete_marker);
    assert!(unmarked.id.is_none(), "the null version had no identity");
    let current = fixture
        .tier
        .get_object(&bucket, &key("v.bin"), None)
        .await
        .expect("the latest real version is current again");
    assert_eq!(
        read_body(current.body).await.expect("the body reads"),
        b"second"
    );
    let gone = fixture
        .tier
        .get_object_version(&bucket, &key("v.bin"), None, None)
        .await
        .expect_err("no null version remains");
    assert!(
        matches!(contract(gone), storage::Error::NoSuchVersion(_)),
        "the removed null version leaves the null address empty"
    );
}

/// A settled cold object answers bytes, metadata, a range, its class and its
/// listing row from the cold authority without its warm file.
#[tokio::test]
async fn a_cold_object_answers_every_read_from_pg() {
    if !require_dsn() {
        return;
    }
    let fixture = Fixture::new().await;
    let bytes = vec![b'c'; 1024];
    let written = fixture.make_cold("cold.bin", &bytes).await;
    assert!(!fixture.path("cold.bin").exists(), "the warm file is gone");

    let placement = fixture.placement("cold.bin").expect("the cold placement");
    assert_eq!(placement.tier, Tier::Pg);
    assert_eq!(placement.state, State::Live);
    assert_eq!(
        placement.cold.as_ref().map(|cold| cold.etag.as_str()),
        Some(written.etag.to_string().as_str())
    );

    let head = fixture
        .tier
        .head_object(&fixture.bucket, &key("cold.bin"))
        .await
        .expect("the cold head answers");
    assert_eq!(head.etag, written.etag);
    assert_eq!(head.size, bytes.len() as u64);
    assert_eq!(head.last_modified, written.last_modified);

    assert_eq!(fixture.get("cold.bin").await, bytes);

    let ranged = fixture
        .tier
        .get_object(
            &fixture.bucket,
            &key("cold.bin"),
            Some(ByteRange::Inclusive(1, 4)),
        )
        .await
        .expect("the cold range serves");
    assert_eq!(
        read_body(ranged.body).await.expect("the range reads"),
        &bytes[1..=4]
    );

    assert_eq!(
        fixture
            .tier
            .storage_class(&fixture.bucket, &key("cold.bin"))
            .await
            .expect("the class reads"),
        Class::StandardIa
    );

    let listing = fixture
        .tier
        .list_objects(ListObjectsParams {
            bucket: fixture.bucket.clone(),
            prefix: String::new(),
            delimiter: None,
            start_after: None,
            max_keys: 100,
        })
        .await
        .expect("the bucket lists");
    let row = listing
        .objects
        .iter()
        .find(|info| info.key == key("cold.bin"))
        .expect("the cold object is listed");
    assert_eq!(row.etag, written.etag);
    assert_eq!(row.size, bytes.len() as u64);
}

/// A cold object's tags and ACL are written to the placement authority and
/// answered from it, while the bytes stay in the cold plane.
#[tokio::test]
async fn cold_tags_and_acl_are_written_to_the_placement() {
    if !require_dsn() {
        return;
    }
    let fixture = Fixture::new().await;
    let bytes = b"cold metadata";
    let owner = conform_owner();
    let staged = fixture
        .tier
        .stage_body(
            &fixture.bucket,
            &key("tagged.bin"),
            body(bytes.to_vec()),
            None,
            None,
        )
        .await
        .expect("the owned fixture stages");
    fixture
        .tier
        .commit_object(
            &fixture.bucket,
            &key("tagged.bin"),
            staged,
            object::Tags::empty(),
            Some(&owner),
            &Acl::default_private(Some(owner.clone())),
            None,
        )
        .await
        .expect("the owned fixture commits");
    let moved = fixture
        .tier
        .demote(&fixture.bucket, &key("tagged.bin"), SystemTime::now())
        .await
        .expect("the demote runs");
    assert!(matches!(moved, MoveOutcome::Moved { .. }));
    fs::remove_file(fixture.path("tagged.bin")).expect("the leftover file is removed");

    let tags = object::Tags::from_pairs([("tier".to_owned(), "cold".to_owned())])
        .expect("a valid tag set");
    fixture
        .tier
        .put_object_tags(&fixture.bucket, &key("tagged.bin"), &tags)
        .await
        .expect("the cold tags are written");
    assert_eq!(
        fixture
            .tier
            .get_object_tags(&fixture.bucket, &key("tagged.bin"))
            .await
            .expect("the cold tags read"),
        tags
    );

    let grants = vec![conform_group_read_grant()];
    fixture
        .tier
        .put_object_acl(&fixture.bucket, &key("tagged.bin"), &grants)
        .await
        .expect("the cold ACL is written");
    let acl = fixture
        .tier
        .get_object_acl(&fixture.bucket, &key("tagged.bin"))
        .await
        .expect("the cold ACL reads");
    assert_eq!(acl.owner, Some(owner.clone()), "the owner is preserved");
    assert_eq!(acl.grants, grants);

    let placement = fixture.placement("tagged.bin").expect("the placement");
    let cold = placement.cold.expect("the cold authority");
    assert_eq!(cold.tags, tags.to_wire());
    assert_eq!(cold.owner, owner.as_str(), "the owner is preserved");
    assert_eq!(fixture.get("tagged.bin").await, bytes.to_vec());
    assert!(!fixture.path("tagged.bin").exists());
}

/// The null-version address serves a cold object on an unversioned bucket and
/// refuses one whose placement names a real version (T6).
#[tokio::test]
async fn the_null_version_address_is_reconciled_against_the_placement() {
    if !require_dsn() {
        return;
    }
    let fixture = Fixture::new().await;
    let bytes = b"the null version";
    let written = fixture.make_cold("null.bin", bytes).await;

    let read = fixture
        .tier
        .get_object_version(&fixture.bucket, &key("null.bin"), None, None)
        .await
        .expect("the null address serves an unversioned cold object");
    let version::ReadResult::Object(got) = read else {
        panic!("the null address names an object, not a marker");
    };
    assert_eq!(got.info.etag, written.etag);
    assert_eq!(answered(got).await, bytes);
    assert_eq!(
        fixture
            .tier
            .bucket_versioning(&fixture.bucket)
            .await
            .expect("the state reads"),
        version::State::Unversioned
    );

    // A placement that records a real identity cannot answer the null address:
    // the cold authority would silently serve another version's bytes.
    let versioned = fixture.put("named.bin", b"a named version").await;
    fixture.publish_cold("named.bin", b"a named version").await;
    let mut info = versioned.clone();
    info.version_id = Some(version::id("v1").expect("a valid version id"));
    fixture.place(
        "named.bin",
        Placement {
            tier: Tier::Pg,
            state: State::Live,
            cold: Some(ColdInfo::from_info(&info)),
            mtime: info.last_modified,
        },
    );
    fs::remove_file(fixture.path("named.bin")).expect("the warm file is removed");

    let refused = fixture
        .tier
        .get_object_version(&fixture.bucket, &key("named.bin"), None, None)
        .await
        .expect_err("the null address refuses a named placement");
    assert!(
        matches!(contract(refused), storage::Error::NoSuchVersion(_)),
        "the refusal is the contract's NoSuchVersion"
    );
}

/// A batch write waits in the buffer, a client listing flushes it, and a
/// transition demotes it into the cold plane.
#[tokio::test]
async fn batch_writes_reach_the_cold_plane() {
    if !require_dsn() {
        return;
    }
    let fixture = Fixture::with_config(batch_config()).await;
    let bytes = vec![b'b'; 1024];

    fixture
        .tier
        .put_object(&fixture.bucket, &key("buffered.bin"), body(bytes.clone()))
        .await
        .expect("the write is acknowledged");
    assert!(
        fixture.row("buffered.bin").await.is_none(),
        "the acknowledged write waits in the buffer"
    );
    assert!(!fixture.path("buffered.bin").exists());

    let listing = fixture
        .tier
        .list_objects(ListObjectsParams {
            bucket: fixture.bucket.clone(),
            prefix: String::new(),
            delimiter: None,
            start_after: None,
            max_keys: 100,
        })
        .await
        .expect("the listing flushes the buffer");
    assert!(
        listing
            .objects
            .iter()
            .any(|info| info.key == key("buffered.bin")),
        "the flushed write is listed"
    );
    assert!(fixture.row("buffered.bin").await.is_some());

    assert!(
        fixture
            .tier
            .transition(
                &fixture.bucket,
                &key("buffered.bin"),
                Class::StandardIa,
                SystemTime::now(),
            )
            .await
            .is_ok()
    );
    fs::remove_file(fixture.path("buffered.bin")).expect("the leftover file is removed");
    assert_eq!(fixture.get("buffered.bin").await, bytes);
}

/// A read-only run opens beside the closed writer, serves the cold placement
/// from its index, and refuses every mutation.
#[tokio::test]
async fn a_read_only_run_serves_the_cold_plane() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let cold = pg_config(&db);
    let root = tempfile::tempdir().expect("a temporary root");
    let first_root = root.path().to_path_buf();
    let first_cold = cold.clone();
    let expected = on_its_own_runtime(move || {
        runtime().block_on(async move {
            let tier = TierStorage::open_pg(
                &first_root,
                testing::fs_options(),
                config(),
                &first_cold,
                AccessMode::ReadWrite,
                false,
            )
            .await
            .expect("the writer opens");
            let name = bucket::name(DATA).expect("a valid bucket name");
            tier.create_bucket(&name, None, &Acl::default_private(None))
                .await
                .expect("the bucket is created");
            let key = object::key("readonly.bin").expect("a valid key");
            tier.put_object(&name, &key, body(b"read only"))
                .await
                .expect("the object is written");
            let moved = tier
                .demote(&name, &key, SystemTime::now())
                .await
                .expect("the demote runs");
            assert!(matches!(moved, MoveOutcome::Moved { .. }));
            fs::remove_file(first_root.join(DATA).join("readonly.bin"))
                .expect("the leftover file is removed");
            tier.head_object(&name, &key)
                .await
                .expect("the cold row is readable")
        })
    });

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
    let name = bucket::name(DATA).expect("a valid bucket name");
    let key = object::key("readonly.bin").expect("a valid key");
    assert_eq!(
        tier.index()
            .placement(&name, &key)
            .expect("the index reads")
            .map(|placement| placement.tier),
        Some(Tier::Pg),
        "the read-only index serves the placement"
    );
    let got = tier
        .get_object(&name, &key, None)
        .await
        .expect("the cold object is served");
    assert_eq!(answered(got).await, b"read only");
    let head = tier
        .head_object(&name, &key)
        .await
        .expect("the cold head answers");
    assert_eq!(head.etag, expected.etag);
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
    assert_eq!(listing.objects.len(), 1);

    let write = tier
        .put_object(
            &name,
            &object::key("new.bin").expect("a valid key"),
            body(b"no"),
        )
        .await;
    assert!(write.is_err(), "a read-only run refuses a write");
    let bucket_create = tier
        .create_bucket(
            &bucket::name("other").expect("a valid bucket name"),
            None,
            &Acl::default_private(None),
        )
        .await;
    assert!(bucket_create.is_err(), "a read-only run refuses a bucket");
}

/// A restart serves the cold object and keeps the root identity (T2, T18).
#[tokio::test]
async fn a_restart_serves_the_cold_plane() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let cold = pg_config(&db);
    let root = tempfile::tempdir().expect("a temporary root");
    let first_root = root.path().to_path_buf();
    let first_cold = cold.clone();
    let (root_id, written) = on_its_own_runtime(move || {
        runtime().block_on(async move {
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
            let name = bucket::name(DATA).expect("a valid bucket name");
            tier.create_bucket(&name, None, &Acl::default_private(None))
                .await
                .expect("the bucket is created");
            let key = object::key("restart.bin").expect("a valid key");
            tier.put_object(&name, &key, body(b"survives"))
                .await
                .expect("the object is written");
            let moved = tier
                .demote(&name, &key, SystemTime::now())
                .await
                .expect("the demote runs");
            assert!(matches!(moved, MoveOutcome::Moved { .. }));
            fs::remove_file(first_root.join(DATA).join("restart.bin"))
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
        tier.index().root_id().expect("the root id reads"),
        Some(root_id),
        "a restart keeps the root identity"
    );
    let name = bucket::name(DATA).expect("a valid bucket name");
    let key = object::key("restart.bin").expect("a valid key");
    let got = tier
        .get_object(&name, &key, None)
        .await
        .expect("the restarted tier serves");
    assert_eq!(answered(got).await, b"survives");
    let head = tier
        .head_object(&name, &key)
        .await
        .expect("the cold head answers");
    assert_eq!(head.etag, written.etag);
    assert_eq!(
        tier.storage_class(&name, &key)
            .await
            .expect("the class reads"),
        Class::StandardIa
    );
}

/// **Crash at copy.** A demote recorded its `copying` placement and crashed
/// before any cold byte landed: collection reverts to the warm plane, and the
/// in-flight reader never sees a torn object.
#[tokio::test]
async fn crash_at_copy_reverts_to_warm() {
    if !require_dsn() {
        return;
    }
    let fixture = Fixture::new().await;
    let now = SystemTime::now();
    let info = fixture.put("copy.bin", b"the warm bytes").await;
    fixture.place(
        "copy.bin",
        Placement {
            tier: Tier::Fs,
            state: State::Copying,
            cold: Some(ColdInfo::from_info(&info)),
            mtime: fixture.aged(now),
        },
    );
    assert!(!fixture.cold_has("copy.bin").await);

    let reading = fixture.in_flight("copy.bin").await;
    let busy = fixture.tier.gc(now).await.expect("the collection runs");
    assert_eq!(busy.busy, 1, "the in-flight reader's key is skipped");
    assert_eq!(answered(reading).await, b"the warm bytes");

    let summary = fixture.tier.gc(now).await.expect("the second pass runs");
    assert_eq!(summary.reverted, 1, "the partial copy reverts to warm");
    let placement = fixture.placement("copy.bin").expect("the placement stays");
    assert_eq!(placement.tier, Tier::Fs);
    assert_eq!(placement.state, State::Live);
    assert_eq!(placement.cold, None);
    assert!(!fixture.cold_has("copy.bin").await);
    assert_eq!(fixture.get("copy.bin").await, b"the warm bytes");
}

/// **Crash at `copying`.** The demote published the complete cold copy and
/// crashed before settling the placement: collection finishes the demote.
#[tokio::test]
async fn crash_at_copying_completes_the_demote() {
    if !require_dsn() {
        return;
    }
    let fixture = Fixture::new().await;
    let now = SystemTime::now();
    let info = fixture.put("copying.bin", b"half demoted").await;
    fixture.publish_cold("copying.bin", b"half demoted").await;
    fixture.place(
        "copying.bin",
        Placement {
            tier: Tier::Fs,
            state: State::Copying,
            cold: Some(ColdInfo::from_info(&info)),
            mtime: fixture.aged(now),
        },
    );

    let reading = fixture.in_flight("copying.bin").await;
    let busy = fixture.tier.gc(now).await.expect("the collection runs");
    assert_eq!(busy.busy, 1);
    assert_eq!(answered(reading).await, b"half demoted");

    let summary = fixture.tier.gc(now).await.expect("the second pass runs");
    assert_eq!(
        summary.completed, 1,
        "the complete copy finishes the demote"
    );
    let placement = fixture
        .placement("copying.bin")
        .expect("the demote settles");
    assert_eq!(placement.tier, Tier::Pg);
    assert_eq!(placement.state, State::Live);
    assert!(
        fixture.row("copying.bin").await.is_some(),
        "the row is kept"
    );
    assert_eq!(fixture.get("copying.bin").await, b"half demoted");
}

/// **Crash at placement.** The demote settled `pg/live` and crashed before the
/// warm file was collected: the reader serves cold, then the file goes after
/// the grace.
#[tokio::test]
async fn crash_at_placement_leaves_the_file_for_gc() {
    if !require_dsn() {
        return;
    }
    let fixture = Fixture::new().await;
    let now = SystemTime::now();
    let info = fixture.put("placed.bin", b"cold bytes").await;
    fixture.publish_cold("placed.bin", b"cold bytes").await;
    fixture.place(
        "placed.bin",
        Placement {
            tier: Tier::Pg,
            state: State::Live,
            cold: Some(ColdInfo::from_info(&info)),
            mtime: fixture.aged(now),
        },
    );
    assert!(
        fixture.path("placed.bin").exists(),
        "the leftover file is planted"
    );

    let reading = fixture.in_flight("placed.bin").await;
    let busy = fixture.tier.gc(now).await.expect("the collection runs");
    assert_eq!(busy.busy, 1, "the reader's file is not removed");
    assert_eq!(answered(reading).await, b"cold bytes");
    assert!(fixture.path("placed.bin").exists());

    let summary = fixture.tier.gc(now).await.expect("the second pass runs");
    assert_eq!(summary.files, 1, "the leftover warm file is removed");
    assert!(!fixture.path("placed.bin").exists());
    assert!(fixture.row("placed.bin").await.is_some());
    assert_eq!(fixture.get("placed.bin").await, b"cold bytes");
}

/// **Crash at file removal.** The warm file is already gone and the
/// `pg/live` placement is the settled cold object: collection is a no-op and
/// the reader is served from the cold authority.
#[tokio::test]
async fn crash_at_file_removal_is_settled() {
    if !require_dsn() {
        return;
    }
    let fixture = Fixture::new().await;
    let now = SystemTime::now();
    let info = fixture.make_cold("removed.bin", b"settled cold").await;
    fixture.place(
        "removed.bin",
        Placement {
            tier: Tier::Pg,
            state: State::Live,
            cold: Some(ColdInfo::from_info(&info)),
            mtime: fixture.aged(now),
        },
    );
    assert!(!fixture.path("removed.bin").exists());

    let reading = fixture.in_flight("removed.bin").await;
    let busy = fixture.tier.gc(now).await.expect("the collection runs");
    assert_eq!(busy.busy, 1);
    assert_eq!(answered(reading).await, b"settled cold");

    let summary = fixture.tier.gc(now).await.expect("the second pass runs");
    assert_eq!(summary, Default::default(), "nothing is left to collect");
    let placement = fixture
        .placement("removed.bin")
        .expect("the placement stays");
    assert_eq!(placement.tier, Tier::Pg);
    assert_eq!(placement.state, State::Live);
    assert_eq!(fixture.get("removed.bin").await, b"settled cold");
}

/// **Crash at publish.** A promote published the warm file and crashed before
/// the row and placement reset: the read serves the cold authority (T5's
/// ETag match), and collection removes the file after the grace.
#[tokio::test]
async fn crash_at_publish_serves_cold_and_drops_the_file() {
    if !require_dsn() {
        return;
    }
    let fixture = Fixture::new().await;
    let now = SystemTime::now();
    fixture.make_cold("published.bin", b"promoted bytes").await;
    fixture.place(
        "published.bin",
        Placement {
            tier: Tier::Pg,
            state: State::Live,
            cold: Some(ColdInfo::from_info(
                &fixture.row("published.bin").await.expect("the cold row"),
            )),
            mtime: fixture.aged(now),
        },
    );
    // The crash state: the promotion's publish recreated the file with the
    // cold ETag and the placement was never reset.
    fs::write(fixture.path("published.bin"), b"promoted bytes")
        .expect("the published file is planted");

    let reading = fixture.in_flight("published.bin").await;
    let busy = fixture.tier.gc(now).await.expect("the collection runs");
    assert_eq!(busy.busy, 1, "the reader keeps the published file");
    assert_eq!(answered(reading).await, b"promoted bytes");

    let summary = fixture.tier.gc(now).await.expect("the second pass runs");
    assert_eq!(summary.files, 1, "the published file is collected");
    assert!(!fixture.path("published.bin").exists());
    assert!(fixture.cold_has("published.bin").await);
    assert_eq!(fixture.get("published.bin").await, b"promoted bytes");
}

/// **Crash at index clear.** A delete removed the row and the file and cleared
/// the placement, but the cold copy survived: a new read is `NoSuchKey`, the
/// in-flight read keeps its bytes, and collection never deletes unplaced
/// content.
#[tokio::test]
async fn crash_at_index_clear_leaves_an_orphan() {
    if !require_dsn() {
        return;
    }
    let fixture = Fixture::new().await;
    let now = SystemTime::now();
    fixture.make_cold("cleared.bin", b"orphaned bytes").await;

    let reading = fixture.in_flight("cleared.bin").await;
    // The crash: the row and file are gone and the index was cleared, while
    // the cold copy is still there.
    fixture
        .tier
        .fs()
        .delete_object(&fixture.bucket, &key("cleared.bin"))
        .await
        .expect("the crash's delete lands");
    fixture
        .tier
        .index()
        .clear(&fixture.bucket, &key("cleared.bin"))
        .expect("the placement is cleared");
    // The crash loses the memory plane, so the cached bytes go with it: the
    // cold copy and the cleared index are what a restart finds.
    fixture
        .tier
        .cache()
        .remove(&fixture.bucket, &key("cleared.bin"));

    let summary = fixture.tier.gc(now).await.expect("the collection runs");
    assert_eq!(
        summary,
        Default::default(),
        "an unplaced cold copy is never collected automatically"
    );
    assert!(
        fixture.cold_has("cleared.bin").await,
        "the orphan stays for the explicit repair action"
    );
    assert_eq!(answered(reading).await, b"orphaned bytes");

    let missing = fixture
        .tier
        .get_object(&fixture.bucket, &key("cleared.bin"), None)
        .await
        .expect_err("the key is deleted");
    assert!(
        matches!(contract(missing), storage::Error::NoSuchKey(_)),
        "the deleted key is NoSuchKey"
    );
}

/// **Crash at tombstone.** A delete wrote the tombstone, removed the row and
/// crashed before finishing: collection completes it after the grace, and the
/// in-flight reader is skipped until its body ends.
#[tokio::test]
async fn crash_at_tombstone_is_finished_after_the_grace() {
    if !require_dsn() {
        return;
    }
    let fixture = Fixture::new().await;
    let now = SystemTime::now();
    let info = fixture.make_cold("tombstoned.bin", b"delete me").await;

    let reading = fixture.in_flight("tombstoned.bin").await;
    fixture
        .tier
        .fs()
        .delete_object(&fixture.bucket, &key("tombstoned.bin"))
        .await
        .expect("the crash's delete lands");
    fixture.place(
        "tombstoned.bin",
        Placement {
            tier: Tier::Pg,
            state: State::Deleting,
            cold: Some(ColdInfo::from_info(&info)),
            mtime: fixture.aged(now),
        },
    );

    let busy = fixture.tier.gc(now).await.expect("the collection runs");
    assert_eq!(busy.busy, 1, "the in-flight reader's key is skipped");
    assert!(fixture.cold_has("tombstoned.bin").await);
    assert_eq!(answered(reading).await, b"delete me");

    let summary = fixture.tier.gc(now).await.expect("the second pass runs");
    assert_eq!(summary.tombstones, 1, "the aged tombstone is finished");
    assert!(fixture.placement("tombstoned.bin").is_none());
    assert!(!fixture.cold_has("tombstoned.bin").await);
    assert!(fixture.row("tombstoned.bin").await.is_none());
    let missing = fixture
        .tier
        .get_object(&fixture.bucket, &key("tombstoned.bin"), None)
        .await
        .expect_err("the object stays deleted");
    assert!(matches!(contract(missing), storage::Error::NoSuchKey(_)));
}

/// The index a read-only run opens records the writer's placements (T27): a
/// guard that the read-only leg is not answering from an empty index.
#[tokio::test]
async fn a_written_index_survives_a_write_close() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let root = tempfile::tempdir().expect("a temporary root");
    let root_path = root.path().to_path_buf();
    let cold = pg_config(&db);
    on_its_own_runtime(move || {
        runtime().block_on(async move {
            let tier = TierStorage::open_pg(
                &root_path,
                testing::fs_options(),
                config(),
                &cold,
                AccessMode::ReadWrite,
                false,
            )
            .await
            .expect("the writer opens");
            let name = bucket::name(DATA).expect("a valid bucket name");
            tier.create_bucket(&name, None, &Acl::default_private(None))
                .await
                .expect("the bucket is created");
            let key = object::key("kept.bin").expect("a valid key");
            tier.put_object(&name, &key, body(b"kept"))
                .await
                .expect("the object is written");
            let moved = tier
                .demote(&name, &key, SystemTime::now())
                .await
                .expect("the demote runs");
            assert!(matches!(moved, MoveOutcome::Moved { .. }));
        })
    });

    let index = Index::open_read_only(index_path(root.path())).expect("the index opens read-only");
    let placement = index
        .placement(
            &bucket::name(DATA).expect("a valid bucket name"),
            &key("kept.bin"),
        )
        .expect("the placement reads")
        .expect("the placement survived the writer");
    assert_eq!(placement.tier, Tier::Pg);
    assert_eq!(
        placement.cold.as_ref().map(|cold| cold.etag.as_str()),
        Some(ETag::from_content(b"kept").to_string().as_str())
    );
}
