//! The pg backend's conformance leg: `assert_conformance` and the versioning leg, against a
//! **live** PostgreSQL server, plus the two construction acceptances (PG12's ownership and the
//! read-only mode's coexistence).
//!
//! The database is read from `TINIO_PG_DSN`; with the variable unset every leg prints the skip
//! notice and returns, which is what keeps `cargo test --workspace` database-free (PG8). No
//! credential is ever printed (PG5).
//!
//! Every leg creates its **own scratch database** through `tinio-store-pg`'s `testing`
//! harness (`TestDb`), so the legs are safe under `cargo test`'s parallelism and a run leaves
//! no state behind.
//!
//! The store's pool and its own connection type are reached through the crate's hidden
//! re-exports (`tinio_pg::_store_pg::_sqlx`), which is what a test target may use without
//! taking a second dependency on the driver.
// The legs address the crate, the standard library and the driver's re-exports by absolute
// path on purpose (the store's own suite states the same rule): a test that used a shortened
// path would not prove the path an external caller needs. `expect` and `expect_err` here are
// assertions — the harness asserts by panicking, and a refusal that did not arrive is the
// failure this task's acceptance is about.
#![expect(
    clippy::absolute_paths,
    reason = "test support: a test proves the public paths"
)]
#![expect(
    clippy::expect_used,
    reason = "test support: a leg asserts its preconditions by panicking"
)]

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

use secrecy::ExposeSecret;
use tinio_core::{
    ETag,
    acl::{Acl, AclGrants},
    bucket, cors,
    meta::{ObjectMeta, object::Row},
    multipart, object,
    storage::{
        self, BucketOps, ListBucketsParams, ListObjectsParams, ListPartsParams, ListUploadsParams,
        MultipartOps, ObjectOps, VersionOps,
    },
    version,
};
use tinio_pg::{
    _store_pg::{
        _sqlx::{AssertSqlSafe, PgPool, query_scalar, raw_sql},
        Error as StoreError, PgStore,
        pool::Pool,
        test_support::TestDb,
    },
    Access, Error, PgStorage,
};
use tinio_util::testing::{assert_conformance, body, conformance_versioning};

/// The `TINIO_PG_DSN` gate and the scratch configuration both `tinio-pg` test targets share.
mod support;

use support::{require_dsn, scratch_config};

/// A generated suffix so two legs in one process never collide on a role name.
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// The whole `Storage` surface, then the versioning leg — the acceptance this task's Step 3
/// names first.
#[tokio::test]
async fn the_shared_conformance_suite_holds_over_postgres() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let storage = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("a writable store over the scratch database");

    assert_conformance(&storage).await;

    // The versioning leg runs on a bucket of its own (the suite's rule): it enables
    // versioning on the bucket it is handed and leaves it suspended.
    let versioned = bucket::name("conform-versioned-pg").expect("a valid bucket name");
    storage
        .create_bucket(&versioned, None, &Acl::default_private(None))
        .await
        .expect("the versioned leg's bucket");
    conformance_versioning(&storage, &versioned).await;

    db.drop_db().await;
}

/// PG12: one writable instance per database, and read-only instances beside it.
///
/// The refusal is the **owner's**: a second writable construction fails with a message naming
/// the session that holds the lock. A read-only construction takes no lock, so it starts beside
/// the writer and serves reads.
#[tokio::test]
async fn a_second_writer_refuses_naming_the_owner_and_a_reader_starts_beside_it() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let config = scratch_config(&db);
    let writer = PgStorage::connect(&config, Access::ReadWrite)
        .await
        .expect("the first writable instance owns the database");

    // The second writable instance: refused, and the refusal names the holder.
    let refused = PgStorage::connect(&config, Access::ReadWrite)
        .await
        .expect_err("a second writable instance is refused");
    let rendered = refused.to_string();
    assert!(
        rendered.contains("advisory lock") && rendered.contains("held by"),
        "the refusal names the ownership lock and its holder: {rendered}"
    );
    // The dsn is never part of a rendering (PG5).
    assert!(
        !rendered.contains(config.dsn().expose_secret().as_ref()),
        "a refusal never renders the dsn: {rendered}"
    );

    // The read-only instance: no lock, so it starts beside the writer and serves reads.
    let reader = PgStorage::connect(&config, Access::ReadOnly)
        .await
        .expect("a read-only instance starts beside the writer");
    let listed = reader
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
        .expect("a read-only instance serves reads");
    assert!(
        listed.buckets.is_empty(),
        "a fresh database holds no bucket: {:?}",
        listed.buckets
    );

    // The writer still holds its lock: the guard lives for the process's life, not for the
    // storage value's.
    drop(writer);
    PgStorage::connect(&config, Access::ReadWrite)
        .await
        .expect_err("the lock outlives the storage value that took it");

    drop(reader);
    db.drop_db().await;
}

/// The scratch dsn with another role's credentials: the connection string a restricted role
/// reaches the database with.
fn with_role(dsn: &str, user: &str, password: &str) -> String {
    let (scheme, rest) = dsn.split_once("://").expect("a URL dsn");
    let after = rest
        .split_once('@')
        .map(|(_, rest)| rest)
        .expect("userinfo");
    format!("{scheme}://{user}:{password}@{after}")
}

/// Assert an operation answered the contract's read-only refusal: `AccessDenied` — the class a
/// read-only instance answers **every** write with (PG6, layer 1) — naming its own subject.
#[track_caller]
fn assert_denied(outcome: Option<Error>) {
    let err = outcome.expect("a mutating operation must be refused on a read-only instance");
    let rendered = err.to_string();
    assert!(
        matches!(storage::Error::from(err), storage::Error::AccessDenied(_)),
        "the refusal is the contract's AccessDenied class: {rendered}"
    );
    assert!(
        rendered.contains("access denied"),
        "the rendering spells the refusal: {rendered}"
    );
}

/// [`assert_denied`]'s twin for an entry whose error type is the **contract's** own
/// ([`tinio_core::storage::Error`]) rather than the backend's: `tinio_version::MarkerWrite`
/// fixes its method's error to the contract's type, so the refusal cannot go through the
/// backend's `Error`.
#[track_caller]
fn assert_contract_denied(outcome: Option<storage::Error>) {
    let err = outcome.expect("a mutating operation must be refused on a read-only instance");
    assert!(
        matches!(err, storage::Error::AccessDenied(_)),
        "the refusal is the contract's AccessDenied class: {err}"
    );
}

/// PG6, layer 1 (FR-023): the entry guard. **Every** mutating operation of `BucketOps`,
/// `ObjectOps`, `MultipartOps` and `VersionOps` answers the contract's `AccessDenied` before any
/// SQL, and every read is served unchanged.
///
/// The enumeration below is the audit of `readonly.rs`'s single choke point: **every** mutating
/// method of the four contract traits is driven once — the two defaulted bodies
/// (`ObjectOps::put_object`, `MultipartOps::copy_part`) included, since a default body is a
/// mutating operation like any other — with its read twin beside it, so a guard that went missing
/// — or a new mutating method that never called the guard — fails here rather than in production.
#[tokio::test]
async fn a_read_only_instance_refuses_every_write_and_serves_every_read() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let writer = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("the writable instance");
    let reader = PgStorage::connect(&scratch_config(&db), Access::ReadOnly)
        .await
        .expect("a read-only instance starts beside the writer");

    let bucket = bucket::name("readonly-bucket").expect("a valid bucket name");
    let key = object::key("ro/object.txt").expect("a valid key");
    let part_key = object::key("ro/part.bin").expect("a valid key");
    let acl = Acl::default_private(None);

    // ---- the writer seeds what the reads below serve: a versioned bucket, an object and an
    // in-progress upload with one part ----
    writer
        .create_bucket(&bucket, None, &acl)
        .await
        .expect("the writer creates the bucket");
    writer
        .set_bucket_versioning(&bucket, version::State::Enabled)
        .await
        .expect("the writer enables versioning");
    let staged = writer
        .stage_body(&bucket, &key, body("seed content"), None, None)
        .await
        .expect("the writer stages the object");
    let committed = writer
        .commit_object(&bucket, &key, staged, object::Tags::empty(), None, &acl)
        .await
        .expect("the writer commits the object");
    // The versioned bucket's commit mints the current version's identity: the reads below
    // address it, because `None` names the *null* version and this bucket has none.
    let version = committed.version_id.clone();
    let upload = writer
        .create_multipart_upload(&bucket, &part_key, None, object::Tags::empty(), None, &acl)
        .await
        .expect("the writer opens an upload");
    writer
        .upload_part(
            &bucket,
            &part_key,
            &upload.upload_id,
            multipart::part_number(1).expect("a valid part number"),
            body("part"),
            None,
        )
        .await
        .expect("the writer uploads a part");

    // ---- every read succeeds: the read-only refusal is for writes alone ----
    reader
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
        .expect("list_buckets");
    reader.head_bucket(&bucket).await.expect("head_bucket");
    reader
        .get_bucket_tags(&bucket)
        .await
        .expect("get_bucket_tags");
    reader
        .get_bucket_cors(&bucket)
        .await
        .expect("get_bucket_cors");
    reader
        .get_bucket_acl(&bucket)
        .await
        .expect("get_bucket_acl");
    reader
        .get_object(&bucket, &key, None)
        .await
        .expect("get_object");
    reader
        .head_object(&bucket, &key)
        .await
        .expect("head_object");
    reader
        .get_object_tags(&bucket, &key)
        .await
        .expect("get_object_tags");
    reader
        .get_object_acl(&bucket, &key)
        .await
        .expect("get_object_acl");
    reader
        .list_objects(ListObjectsParams {
            bucket: bucket.clone(),
            prefix: String::new(),
            delimiter: None,
            start_after: None,
            max_keys: 100,
        })
        .await
        .expect("list_objects");
    reader
        .list_object_parts(&bucket, &key)
        .await
        .expect("list_object_parts");
    reader
        .get_multipart_upload(&bucket, &part_key, &upload.upload_id)
        .await
        .expect("get_multipart_upload");
    reader
        .list_parts(ListPartsParams {
            bucket: bucket.clone(),
            key: part_key.clone(),
            upload_id: upload.upload_id.clone(),
            max_parts: 100,
            part_number_marker: None,
        })
        .await
        .expect("list_parts");
    reader
        .list_multipart_uploads(ListUploadsParams {
            bucket: bucket.clone(),
            prefix: String::new(),
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 100,
        })
        .await
        .expect("list_multipart_uploads");
    reader
        .bucket_versioning(&bucket)
        .await
        .expect("bucket_versioning");
    reader
        .current_version(&bucket, &key)
        .await
        .expect("current_version");
    reader
        .get_object_version(&bucket, &key, version.as_ref(), None)
        .await
        .expect("get_object_version");
    reader
        .head_object_version(&bucket, &key, version.as_ref())
        .await
        .expect("head_object_version");
    reader
        .list_object_versions(&bucket, version::ListParams::default())
        .await
        .expect("list_object_versions");
    reader
        .version_tags(&bucket, &key, version.as_ref())
        .await
        .expect("version_tags");
    reader
        .version_acl(&bucket, &key, version.as_ref())
        .await
        .expect("version_acl");

    // ---- every write answers the contract's read-only refusal, before any SQL ----
    assert_denied(reader.create_bucket(&bucket, None, &acl).await.err());
    assert_denied(reader.delete_bucket(&bucket).await.err());
    assert_denied(
        reader
            .put_bucket_tags(&bucket, &object::Tags::empty())
            .await
            .err(),
    );
    assert_denied(reader.delete_bucket_tags(&bucket).await.err());
    assert_denied(
        reader
            .put_bucket_cors(&bucket, &cors::Config::default())
            .await
            .err(),
    );
    assert_denied(reader.delete_bucket_cors(&bucket).await.err());
    assert_denied(
        reader
            .put_bucket_acl(&bucket, &AclGrants::new())
            .await
            .err(),
    );
    assert_denied(
        reader
            .stage_body(&bucket, &key, body("guard probe"), None, None)
            .await
            .err(),
    );
    // `commit_object` needs a staged body, which only a writable instance can mint — and the
    // guard fires before that body is ever read, so the writer supplies the argument and the
    // reader still answers the refusal.
    let staged = writer
        .stage_body(&bucket, &key, body("guard probe"), None, None)
        .await
        .expect("the writer stages the argument");
    assert_denied(
        reader
            .commit_object(&bucket, &key, staged, object::Tags::empty(), None, &acl)
            .await
            .err(),
    );
    assert_denied(
        reader
            .copy_object(
                &bucket,
                &key,
                &bucket,
                &part_key,
                object::Tags::empty(),
                None,
                &acl,
                None,
            )
            .await
            .err(),
    );
    // `put_object` is a **defaulted body** (the trait's `stage_body` + `commit_object`), driven
    // here in its own right: a default body is a mutating operation like any other, and the guard
    // it reaches through `stage_body` is the whole of its refusal.
    assert_denied(
        reader
            .put_object(&bucket, &key, body("defaulted body"))
            .await
            .err(),
    );
    assert_denied(reader.rename_object(&bucket, &key, &part_key).await.err());
    // The degenerate move is still a write: the guard precedes the head the arm would serve.
    assert_denied(reader.rename_object(&bucket, &key, &key).await.err());
    assert_denied(reader.delete_object(&bucket, &key).await.err());
    assert_denied(
        reader
            .put_object_tags(&bucket, &key, &object::Tags::empty())
            .await
            .err(),
    );
    assert_denied(reader.delete_object_tags(&bucket, &key).await.err());
    assert_denied(
        reader
            .put_object_acl(&bucket, &key, &AclGrants::new())
            .await
            .err(),
    );
    assert_denied(
        reader
            .create_multipart_upload(&bucket, &key, None, object::Tags::empty(), None, &acl)
            .await
            .err(),
    );
    assert_denied(
        reader
            .upload_part(
                &bucket,
                &part_key,
                &upload.upload_id,
                multipart::part_number(1).expect("a valid part number"),
                body("part"),
                None,
            )
            .await
            .err(),
    );
    // `copy_part` is the other **defaulted body**: its source read is a read — served, which is
    // why the source is a key that exists — and the write half it reaches (`upload_part`) is what
    // answers the refusal. It is the one mutating entry whose guard is not literally first,
    // because the entry is the trait's, not this backend's.
    assert_denied(
        reader
            .copy_part(
                &bucket,
                &key,
                &bucket,
                &part_key,
                &upload.upload_id,
                multipart::part_number(2).expect("a valid part number"),
                None,
            )
            .await
            .err(),
    );
    assert_denied(
        reader
            .complete_multipart_upload(&bucket, &part_key, &upload.upload_id, &[], None)
            .await
            .err(),
    );
    assert_denied(
        reader
            .abort_multipart_upload(&bucket, &part_key, &upload.upload_id)
            .await
            .err(),
    );
    assert_denied(
        reader
            .set_bucket_versioning(&bucket, version::State::Suspended)
            .await
            .err(),
    );
    assert_denied(
        reader
            .delete_object_version(&bucket, &key, None)
            .await
            .err(),
    );
    assert_denied(
        reader
            .set_version_tags(&bucket, &key, None, object::Tags::empty())
            .await
            .err(),
    );
    assert_denied(
        reader
            .set_version_acl(&bucket, &key, None, &acl)
            .await
            .err(),
    );
    // The marker-write port is the versioned delete's slot step. The engine reaches it through
    // `delete_object`, whose own guard fires first, but the port is a **separate public entry**
    // (`tinio_version::MarkerWrite`), so a direct call must answer the same refusal rather than
    // reaching SQL on a read-only instance.
    #[cfg(feature = "versioning")]
    assert_contract_denied(
        tinio_version::MarkerWrite::mark_current_marker(&reader, &bucket, &key, None, 1, None)
            .await
            .err(),
    );

    // The refusal is the guard's and not an argument that failed later: a bucket that does not
    // exist still answers the read-only class, never `NoSuchBucket` — the guard precedes every
    // probe, which is the "before any SQL" half of PG6 layer 1.
    let absent = bucket::name("readonly-absent").expect("a valid bucket name");
    assert_denied(reader.delete_bucket(&absent).await.err());
    // The read twin of that same call answers the contract's **own** class, not the read-only
    // one: the guard sits on the writes alone and never leaked onto the reads.
    let missing = reader
        .head_bucket(&absent)
        .await
        .expect_err("a missing bucket is a `NoSuchBucket`, not a read-only refusal");
    let rendered = missing.to_string();
    assert!(
        matches!(
            storage::Error::from(missing),
            storage::Error::NoSuchBucket(_)
        ),
        "a read keeps the contract's own class: {rendered}"
    );
    // And the refusals left nothing behind: the seeded object still reads.
    reader
        .head_object(&bucket, &key)
        .await
        .expect("the object survived every refusal");

    db.drop_db().await;
}

/// PG6, layer 2 (PG26): the session. A read-only instance's connections are opened with
/// `default_transaction_read_only = on` as a **startup option**, so the server holds the setting
/// before the first statement — a statement that reaches it by some path the entry guard did not
/// cover still cannot write.
#[tokio::test]
async fn a_read_only_instance_opens_its_session_read_only_before_any_statement() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    // The schema exists because a writable instance migrated it; the reader below never does.
    let writer = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("the writable instance migrates the schema");

    // The setting, read back from a real connection: `current_setting` reports what the server
    // applied at session start.
    const SETTING: &str = "SELECT current_setting('default_transaction_read_only')";
    let read_only_pool = Pool::open_read_only(&db.dsn, 2).expect("the dsn parses");
    let on: String = query_scalar(SETTING)
        .fetch_one(&read_only_pool)
        .await
        .expect("the read-only session answers");
    assert_eq!(
        on, "on",
        "a read-only pool's session is read-only before its first statement"
    );
    assert!(read_only_pool.is_read_only(), "the pool reports its mode");

    let writable_pool = Pool::open(&db.dsn, 2).expect("the dsn parses");
    let off: String = query_scalar(SETTING)
        .fetch_one(&writable_pool)
        .await
        .expect("the writable session answers");
    assert_eq!(off, "off", "a writable session carries no such setting");
    assert!(!writable_pool.is_read_only());

    // The instance built on that pool reports the mode through the store and serves reads.
    let reader = PgStorage::connect_with_pool(read_only_pool.clone(), Access::ReadOnly)
        .await
        .expect("a read-only instance over the read-only session");
    assert!(
        !reader.store().access().is_writable(),
        "a read-only instance is read-only end to end"
    );
    reader
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
        .expect("a read-only session still serves reads");

    drop(reader);
    read_only_pool.close().await;
    writable_pool.close().await;
    drop(writer);
    db.drop_db().await;
}

/// PG6, layer 3: the role's privileges, the layer that holds when a **helper** bypasses the entry
/// guard. A write driven straight at `PgStore`'s contract — never through `PgStorage`, so never
/// past `readonly.rs` — is refused by the **database**, because the role the test creates carries
/// no `INSERT`/`UPDATE`/`DELETE` grants. The same write through the privileged role succeeds, so
/// the refusal is the privilege and not the statement.
///
/// **The store is built `Access::ReadWrite` on purpose.** A store opened `Access::ReadOnly` gets
/// its pool re-opened read-only ([`Pool::reopened_for`]), so the *session* layer (PG6, layer 2)
/// would refuse first and the role's grant would never be exercised — the leg would then prove
/// nothing about layer 3. A writable session is what this leg needs: the entry guard does not sit
/// over a direct `PgStore` call, and with the session layer absent the role's missing DML grant is
/// the one layer still standing. That is exactly the deployment requirement
/// `crates/tinio-pg/README.md` states.
#[tokio::test]
async fn a_write_that_bypasses_the_entry_guard_is_refused_by_the_role() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let writer = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("the writable instance migrates the schema");

    let bucket = bucket::name("ro-role").expect("a valid bucket name");
    let key = object::key("probe.txt").expect("a valid key");
    let acl = Acl::default_private(None);
    writer
        .create_bucket(&bucket, None, &acl)
        .await
        .expect("the writer creates the bucket the probe writes into");

    // A role with `SELECT` and nothing else, created by the fixture's own superuser — the
    // deployment requirement `crates/tinio-pg/README.md` states. The name and the password are
    // generated, never literal: no credential in the source (PG25).
    let role = format!(
        "tinio_ro_{}_{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let password = format!("{role}-pw");
    assert!(
        role.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
        "a scratch role name is generated, never arbitrary: {role:?}"
    );
    let admin = PgPool::connect(&db.dsn)
        .await
        .expect("a superuser connection to the scratch database");
    for statement in [
        format!("CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'"),
        format!("GRANT USAGE ON SCHEMA public TO \"{role}\""),
        format!("GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"{role}\""),
    ] {
        raw_sql(AssertSqlSafe(statement))
            .execute(&admin)
            .await
            .expect("the restricted role's setup");
    }
    admin.close().await;

    // A store over the restricted role, reached **without** `PgStorage`: this is the path a
    // helper that bypassed the entry guard would take. Built writable so the session layer is
    // absent and the role's grant is the only layer left to refuse the write.
    let role_dsn = with_role(&db.dsn, &role, &password);
    let pool = Pool::open(&role_dsn, 2).expect("the role's dsn parses");
    let store = PgStore::connect_with_pool(pool, Access::ReadWrite)
        .await
        .expect("the store verifies the schema through the role's SELECT grant");

    let row = Row {
        etag: ETag::EMPTY,
        size: 0,
        mtime: SystemTime::UNIX_EPOCH,
        identity: 0,
        tags: object::Tags::empty(),
        checksum: None,
        owner: None,
        acl: acl.clone(),
    };
    // The write, straight at the store's contract: the entry guard does not sit here, so only the
    // role can refuse it — and it does, with PostgreSQL's insufficient-privilege error.
    let refused = ObjectMeta::put(&store, &bucket, &key, &row)
        .await
        .expect_err("a role with no DML grants may not write");
    let rendered = refused.to_string();
    assert!(
        rendered.contains("permission denied"),
        "the database refuses the write: {rendered}"
    );
    assert!(
        rendered.contains("42501"),
        "the SQLSTATE is insufficient_privilege: {rendered}"
    );
    assert!(
        matches!(refused, StoreError::Database(_)),
        "the refusal is the store's statement failure: {rendered}"
    );

    // The same contract call through the privileged role succeeds: the refusal is the privilege,
    // not the statement or the row.
    ObjectMeta::put(writer.store(), &bucket, &key, &row)
        .await
        .expect("the writer's role may write the same row");

    drop(store);
    let admin = PgPool::connect(&db.dsn)
        .await
        .expect("a superuser connection to drop the role");
    // `DROP OWNED BY` first: it revokes the grants this database gave the role, and only then
    // can the role itself leave (PostgreSQL refuses to drop a role that still holds privileges).
    for statement in [
        format!("DROP OWNED BY \"{role}\""),
        format!("DROP ROLE \"{role}\""),
    ] {
        raw_sql(AssertSqlSafe(statement))
            .execute(&admin)
            .await
            .expect("the scratch role leaves with the fixture");
    }
    admin.close().await;
    drop(writer);
    db.drop_db().await;
}

/// PG6/PG11: a read-only instance never migrates and refuses a schema it does not match. Pointed
/// at a fresh database — one no writable instance has migrated — it refuses with the pg remedy,
/// and it leaves the database exactly as it found it.
#[tokio::test]
async fn a_read_only_instance_never_migrates_and_refuses_a_schema_it_does_not_match() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let refused = PgStorage::connect(&scratch_config(&db), Access::ReadOnly)
        .await
        .expect_err("a read-only instance refuses a schema it does not match");
    let rendered = refused.to_string();
    assert!(
        rendered.contains("does not match"),
        "the refusal names the mismatch: {rendered}"
    );
    assert!(
        rendered.contains("never migrates"),
        "and the remedy: {rendered}"
    );

    // It migrated nothing on the way out: the schema table is still absent, so the *next*
    // writable instance is the one that will migrate the database.
    let admin = PgPool::connect(&db.dsn)
        .await
        .expect("a superuser connection to the scratch database");
    let present: bool = query_scalar("SELECT to_regclass('schema_version') IS NOT NULL")
        .fetch_one(&admin)
        .await
        .expect("the schema probe");
    assert!(!present, "a read-only instance never migrates");
    admin.close().await;
    db.drop_db().await;
}
