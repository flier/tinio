//! The PostgreSQL implementation of the persistence contracts -- the P1 Task 2
//! spike (D21/D24/D25).
//!
//! This crate implements [tinio_core::meta::Metadata] (its three category
//! traits), [tinio_core::meta::VersionMeta] and the whole D23
//! [tinio_core::content::ObjectContent] surface over tokio-postgres, and its
//! gate driver (src/main.rs) runs the conformance suite against a real
//! postgres:18-alpine.
//!
//! Three rules from the brief are structural here:
//!
//! 1. **One statement per point operation.** A point read, a presence-
//!    conditioned setter and remove_if_empty's probe-and-delete are one SQL
//!    statement each; put_guarded's refusal and its overwrite-part clear are
//!    one statement too (a data-modifying CTE), so no pre-read can race it.
//! 2. **One transaction per mutating method.** The multi-statement mutating
//!    methods open an explicit transaction; a single-statement mutation is
//!    its own transaction.
//! 3. **COLLATE "C" on every ordered read.** Every key column is declared
//!    text COLLATE "C" in schema.sql and every ORDER BY repeats the
//!    collation: the database's own default collation is en_US.utf8, which
//!    does NOT order the contract's UTF-8 byte sequence. The connect-time
//!    UTF-8 check (PG4) is the other half.
//!
//! All values are bound parameters; every table and column identifier is a
//! fixed literal. No statement is assembled by formatting a value into SQL.

// The helper `p` is `fn p<T: ToSql + Sync>(value: &T)` with an implicit `T:
// Sized`, so `&str` (what `&*bucket` yields) does not satisfy it: the newtypes
// must be passed as an owned wire. clippy::unnecessary_to_owned does not model
// that bound and would have the code stop compiling.
#![allow(clippy::unnecessary_to_owned)]

use std::{collections::HashMap, io, time::SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use tinio_core::{
    BodyStream, ByteRange, ETag,
    acl::{Acl, AclGrants, OwnerId},
    bucket, checksum,
    content::{ContentEntry, ContentKey, ObjectContent, StagedObject},
    cors,
    meta::{
        BucketMeta, Metadata, ObjectMeta, UploadMeta, bucket as bucket_row,
        object::{Identity, PartRow, Preserved, Row as ObjectRow},
        upload,
        version::{Current, Kind, Stored, VersionMeta},
    },
    multipart::{self, PartNumber},
    object,
    storage::{self, from_nanos, to_nanos},
    version::{self, State},
};
use tokio::sync::{Mutex, MutexGuard};
use tokio_postgres::{Client, NoTls, Row as PgRow, types::ToSql};

/// The environment variable this crate reads its dsn from.
///
/// No dsn is compiled into the crate: the value lives only in
/// `docker-compose.yml` (host port 55432) and the README's run command, so the
/// P3 promotion into `crates/tinio-store-pg` (D25/D28) cannot trip the repo's
/// credential scan (pg-design PG25).
pub const DSN_VAR: &str = "TINIO_PG_DSN";

/// The dsn the gate connects with, read from [`DSN_VAR`].
///
/// An unset variable is an error naming the variable and the compose file that
/// carries the value -- never a hardcoded fallback, which is exactly the
/// compiled-in credential this read removes. The environment is no new
/// rendering channel: [`PgStore::connect`] wraps every failure in
/// [`redact_dsn`] (and [`redact_userinfo`]) the same way for an env-derived dsn
/// as for a literal one.
pub fn dsn_from_env() -> Result<String, String> {
    std::env::var(DSN_VAR).map_err(|_| {
        format!(
            "{var} is not set: this crate compiles in no dsn, so set it to the dsn \
             spike/pg-meta/docker-compose.yml pins (spike/pg-meta/README.md, section 2, \
             carries the PowerShell and POSIX forms) before running the gate",
            var = DSN_VAR
        )
    })
}

/// The content plane's chunk size: 1 MiB (PG2).
pub const CONTENT_CHUNK_BYTES: usize = 1024 * 1024;

/// The publish batch bound: one INSERT statement (one transaction) per 4 MiB
/// of chunks, so a publish's transaction boundary is bounded (PG22).
pub const PUBLISH_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// Chunks per publish batch.
pub const PUBLISH_BATCH_CHUNKS: usize = PUBLISH_BATCH_BYTES / CONTENT_CHUNK_BYTES;

/// The chunk/statement plan a publish of `len` bytes uses -- the one source of
/// truth the gate prints as PG22's measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishPlan {
    /// The number of chunk rows written for the generation.
    pub chunks: usize,
    /// The number of batch INSERT statements (implicit transactions).
    pub batches: usize,
}

/// The plan a publish of `len` bytes executes.
pub fn publish_plan(len: u64) -> PublishPlan {
    let chunks = (len as usize).div_ceil(CONTENT_CHUNK_BYTES);
    PublishPlan {
        chunks,
        batches: chunks.div_ceil(PUBLISH_BATCH_CHUNKS),
    }
}

/// The spike crate's error; every rendering is redacted (no dsn, no password).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A connection attempt failed (the message is redacted).
    #[error("postgres connect: {0}")]
    Connect(String),
    /// A statement failed (the message is redacted).
    #[error("postgres: {0}")]
    Database(String),
    /// The server encoding is not UTF-8 (PG4 refuses to serve).
    #[error("postgres server encoding is not UTF-8: {0}")]
    Encoding(String),
    /// The bucket row is absent.
    #[error("no such bucket: {0}")]
    NoSuchBucket(bucket::Name),
    /// The object row -- or the content the call named -- is absent.
    #[error("no such key: {0}")]
    NoSuchKey(object::Key),
    /// The upload row is absent, or names another key.
    #[error("no such multipart upload: {0}")]
    NoSuchUpload(String),
    /// The bucket still holds an object or an upload row.
    #[error("bucket is not empty: {0}")]
    NotEmpty(bucket::Name),
    /// The bucket row already exists.
    #[error("already exists: {0}")]
    AlreadyExists(bucket::Name),
    /// A stored row cannot be decoded: an object row whose etag is
    /// domain-invalid (the strict traversal fails on it; the point read
    /// self-heals it to absent), or a content generation whose chunk rows are
    /// not a contiguous run.
    #[error("corrupt stored row at {bucket}/{key}")]
    Corrupt {
        /// The bucket the row was read from.
        bucket: bucket::Name,
        /// The key the corrupt row is stored at.
        key: String,
    },
    /// The requested window cannot be satisfied for the object's length.
    #[error("invalid byte range: requested {range:?} on {size} bytes")]
    InvalidRange {
        /// The requested range.
        range: ByteRange,
        /// The content's length in bytes.
        size: u64,
    },
    /// The content key holds no live generation (post-fix open).
    #[error("no content for {0:?}")]
    MissingContent(ContentKey),
    /// Another publish moved the key's live generation between this publish's
    /// allocation and its visibility flip.
    #[error("a concurrent publish moved the live generation")]
    PublishRaced,
    /// A version row already stored at the row's sequence names a **different**
    /// version: the contract's `put_version` refuses the write rather than
    /// overwriting another version's committed content (which would drop that
    /// version's blob out of the live set for the sweep). The typed-refusal
    /// shape this crate reports its other refusals with; the context is the
    /// stored fact, never a value formatted into SQL.
    #[error("version row conflicts at {bucket}/{key} seq {seq}")]
    VersionConflict {
        /// The bucket the conflicting row is stored in.
        bucket: bucket::Name,
        /// The key the conflicting row is stored at.
        key: String,
        /// The sequence both rows name.
        seq: u64,
    },
    /// A version slot was asked to move **backwards**: the sequence the caller
    /// named is **lower** than the one the key's slot already records. The
    /// contract's `advance_slot` refuses that move - a slot never moves
    /// backwards (an **equal** sequence is the crash window's own re-run and is
    /// allowed) - so a lower-sequence advance committing after a
    /// higher-sequence one can never leave the slot below the newest version it
    /// already named. The typed-refusal twin of `VersionConflict`, projected
    /// into `storage::Error` the same way (redb refuses the same case, and its
    /// transaction rolls back on the `tx` drop with the slot untouched); the
    /// context is the refused sequence itself, never a value formatted into
    /// SQL.
    #[error("version slot would move backwards at {bucket}/{key} seq {seq}")]
    SlotRewind {
        /// The bucket the slot belongs to.
        bucket: bucket::Name,
        /// The key the slot belongs to.
        key: String,
        /// The refused (lower) sequence the caller asked to advance to.
        seq: u64,
    },
}

impl From<tokio_postgres::Error> for Error {
    fn from(err: tokio_postgres::Error) -> Self {
        // A driver message can embed the dsn; strip any URL userinfo before
        // it reaches a log or an S3 mapping. The server's own diagnosis is
        // kept: the bare "db error" rendering hides every constraint the gate
        // can trip.
        let raw = match err.as_db_error() {
            Some(db) => format!(
                "{db} (code {}, detail {:?}, constraint {:?}, table {:?})",
                db.code().code(),
                db.detail(),
                db.constraint(),
                db.table()
            ),
            None => err.to_string(),
        };
        Self::Database(redact_userinfo(&raw))
    }
}

impl From<Error> for storage::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::NoSuchBucket(name) => storage::Error::NoSuchBucket(name),
            Error::NoSuchKey(key) => storage::Error::NoSuchKey(key),
            Error::NoSuchUpload(id) => storage::Error::NoSuchUpload(id),
            Error::NotEmpty(name) => storage::Error::NotEmpty(name),
            Error::AlreadyExists(name) => storage::Error::AlreadyExists(name),
            Error::InvalidRange { range, size } => storage::Error::InvalidRange { range, size },
            Error::MissingContent(ContentKey::Object(key)) => storage::Error::NoSuchKey(key),
            Error::MissingContent(ContentKey::Part { upload_id, .. }) => {
                storage::Error::NoSuchUpload(upload_id)
            }
            other => storage::Error::Io(io::Error::other(other.to_string())),
        }
    }
}

/// Redact `raw` for `dsn`: the dsn itself, the dsn's password, and any URL
/// userinfo password are replaced.
pub fn redact_dsn(dsn: &str, raw: &str) -> String {
    let mut out = raw.replace(dsn, "<dsn>");
    if let Some(password) = dsn_password(dsn)
        && !password.is_empty()
    {
        out = out.replace(&password, "***");
    }
    redact_userinfo(&out)
}

/// The password a dsn carries, in URL (scheme://user:pass@host) or keyword
/// (password=...) form.
fn dsn_password(dsn: &str) -> Option<String> {
    if let Some((_, rest)) = dsn.split_once("://") {
        let authority = rest.split(['/', '?']).next().unwrap_or(rest);
        if let Some((userinfo, _host)) = authority.rsplit_once('@')
            && let Some((_user, password)) = userinfo.split_once(':')
        {
            return Some(password.to_owned());
        }
    }
    for token in dsn.split_whitespace() {
        if let Some(value) = token.strip_prefix("password=") {
            return Some(value.trim_matches('\'').to_owned());
        }
    }
    None
}

/// Strip every URL's userinfo password from a message.
fn redact_userinfo(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(index) = rest.find("://") {
        let (head, tail) = rest.split_at(index + 3);
        out.push_str(head);
        let end = tail
            .find(|c: char| c.is_whitespace() || c == '/' || c == '\'')
            .unwrap_or(tail.len());
        let authority = &tail[..end];
        match authority.rsplit_once('@') {
            Some((userinfo, host)) => match userinfo.split_once(':') {
                Some((user, _password)) => {
                    out.push_str(user);
                    out.push_str(":***@");
                    out.push_str(host);
                }
                None => out.push_str(authority),
            },
            None => out.push_str(authority),
        }
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}

/// Borrow a value as a statement parameter.
///
/// The string newtypes deref to `str`, which is not itself `ToSql` (only
/// `&str` is), so their call sites pass an owned wire (`to_string()`) and the
/// referent stays a sized `ToSql` type.
fn p<T: ToSql + Sync>(value: &T) -> &(dyn ToSql + Sync) {
    value
}

fn nanos(time: SystemTime) -> i64 {
    to_nanos(time) as i64
}

fn stamp(value: i64) -> SystemTime {
    from_nanos(value as u64)
}

/// The content relation's collision-proof key namespace (PG20): an object key
/// and a part key can never share a row, because the kind discriminator is
/// part of the relation's primary key and the encoded key is prefix-disjoint
/// (o: for objects, p:<hex(upload_id)>:<number> for parts).
fn object_ckey(key: &object::Key) -> String {
    format!("o:{key}")
}

fn part_ckey(upload_id: &str, number: u32) -> String {
    let mut hex = String::with_capacity(upload_id.len() * 2);
    for byte in upload_id.as_bytes() {
        hex.push_str(&format!("{byte:02x}"));
    }
    format!("p:{hex}:{number:04}")
}

fn content_key(key: &ContentKey) -> (&'static str, String) {
    match key {
        ContentKey::Object(key) => ("object", object_ckey(key)),
        ContentKey::Part { upload_id, number } => {
            ("part", part_ckey(upload_id, u32::from(*number)))
        }
    }
}

fn scan_key(ckey: &str) -> object::Key {
    object::key(&ckey[2..]).expect("a stored content key is a valid object key")
}

fn body(bytes: Vec<u8>) -> BodyStream {
    Box::pin(stream::iter(vec![Ok(Bytes::from(bytes))]))
}

/// The spike's store: one serialized tokio-postgres connection (PG12's single
/// writer). The mutex is what makes the explicit BEGIN/COMMIT transactions
/// safe on a shared client.
pub struct PgStore {
    client: Mutex<Client>,
    dsn: String,
}

impl PgStore {
    /// The spike's relation drop, run on **every** connect. Blast radius: it
    /// removes all 15 relations (`DROP TABLE ... CASCADE`), so every
    /// connection starts from an empty schema - including tables an earlier
    /// connection in the same process created. That is what makes the gate
    /// reproducible against a database left by an earlier revision: the schema
    /// file is `CREATE TABLE IF NOT EXISTS`, so without the reset a stale
    /// column set would survive and the gate would pass against yesterday's DDL.
    ///
    /// **Spike-only, and gone at promotion.** P3 promotes this directory into
    /// `crates/tinio-store-pg` (README), where the schema becomes migrations:
    /// `PgStore::connect` must then migrate and never drop, and this constant
    /// must not be carried over.
    const RESET_SQL: &str = "DROP TABLE IF EXISTS part_checksums, parts, \
        upload_checksums, uploads, object_parts, object_meta, version_index, \
        version_rows, bucket_versioning, current_slots, content, content_heads, \
        blocks, buckets, schema_version CASCADE;";

    /// A content generation that cannot be read as a contiguous run of chunks.
    fn corrupt_content(bucket: &bucket::Name, ckey: &str) -> Error {
        Error::Corrupt {
            bucket: bucket.clone(),
            key: ckey.to_owned(),
        }
    }

    /// Connect, verify the server encoding is UTF-8 (PG4) and apply
    /// `schema.sql` (the spike's stand-in for P3's migrations), resetting the
    /// spike's relations first (see `RESET_SQL`).
    pub async fn connect(dsn: &str) -> Result<Self, Error> {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls)
            .await
            .map_err(|err| Error::Connect(redact_dsn(dsn, &err.to_string())))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let store = Self {
            client: Mutex::new(client),
            dsn: dsn.to_owned(),
        };
        store.verify_encoding().await?;
        store.apply_schema().await?;
        Ok(store)
    }

    /// The dsn this store was opened with.
    pub fn dsn(&self) -> &str {
        &self.dsn
    }

    async fn conn(&self) -> MutexGuard<'_, Client> {
        self.client.lock().await
    }

    async fn verify_encoding(&self) -> Result<(), Error> {
        let client = self.conn().await;
        let encoding: String = client
            .query_one("SELECT current_setting('server_encoding')", &[])
            .await?
            .get(0);
        if !encoding.eq_ignore_ascii_case("UTF8") {
            return Err(Error::Encoding(encoding));
        }
        Ok(())
    }

    async fn apply_schema(&self) -> Result<(), Error> {
        let client = self.conn().await;
        client.batch_execute(Self::RESET_SQL).await?;
        client.batch_execute(include_str!("../schema.sql")).await?;
        let applied = nanos(SystemTime::now());
        client
            .execute(
                "INSERT INTO schema_version (id, applied_at, note) VALUES (1, $1, $2) \
                 ON CONFLICT (id) DO NOTHING",
                &[p(&applied), p(&"pg-meta spike schema (P1 Task 2)")],
            )
            .await?;
        Ok(())
    }

    /// A gate-only raw read: the driver plants and inspects rows the scripts
    /// never write (the raw-SQL self-heal leg).
    pub async fn raw_query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<PgRow>, Error> {
        let client = self.conn().await;
        Ok(client.query(sql, params).await?)
    }

    /// A gate-only raw statement.
    pub async fn raw_execute(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let client = self.conn().await;
        Ok(client.execute(sql, params).await?)
    }
}

impl Metadata for PgStore {
    type Error = Error;
}

#[async_trait]
impl BucketMeta for PgStore {
    async fn created_at(&self, bucket: &bucket::Name) -> Result<Option<SystemTime>, Error> {
        let client = self.conn().await;
        let row = client
            .query_opt(
                r#"SELECT created FROM buckets WHERE name = $1"#,
                &[p(&bucket.to_string())],
            )
            .await?;
        Ok(row.map(|row| stamp(row.get::<_, i64>(0))))
    }

    async fn load_many(&self, buckets: &[bucket::Name]) -> Result<Vec<Option<SystemTime>>, Error> {
        let names: Vec<String> = buckets.iter().map(ToString::to_string).collect();
        let client = self.conn().await;
        let rows = client
            .query(
                r#"SELECT name, created FROM buckets WHERE name = ANY($1)"#,
                &[p(&names)],
            )
            .await?;
        let mut found: HashMap<String, SystemTime> = HashMap::new();
        for row in rows {
            found.insert(row.get::<_, String>(0), stamp(row.get::<_, i64>(1)));
        }
        Ok(buckets
            .iter()
            .map(|bucket| found.get(&bucket.to_string()).copied())
            .collect())
    }

    async fn load_all(&self) -> Result<Vec<(String, SystemTime)>, Error> {
        let client = self.conn().await;
        let rows = client
            .query(
                r#"SELECT name, created FROM buckets ORDER BY name COLLATE "C""#,
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get::<_, String>(0), stamp(row.get::<_, i64>(1))))
            .collect())
    }

    async fn row(&self, bucket: &bucket::Name) -> Result<Option<bucket_row::Row>, Error> {
        let client = self.conn().await;
        let row = client
            .query_opt(
                r#"SELECT created, tags, owner, acl, cors FROM buckets WHERE name = $1"#,
                &[p(&bucket.to_string())],
            )
            .await?;
        Ok(row.map(|row| {
            bucket_row::Row::from_raw(
                row.get::<_, i64>(0) as u64,
                &row.get::<_, String>(1),
                &row.get::<_, String>(2),
                &row.get::<_, String>(3),
                &row.get::<_, String>(4),
            )
        }))
    }

    async fn owners(&self, buckets: &[bucket::Name]) -> Result<Vec<Option<OwnerId>>, Error> {
        let names: Vec<String> = buckets.iter().map(ToString::to_string).collect();
        let client = self.conn().await;
        let rows = client
            .query(
                r#"SELECT name, owner FROM buckets WHERE name = ANY($1)"#,
                &[p(&names)],
            )
            .await?;
        let mut found: HashMap<String, Option<OwnerId>> = HashMap::new();
        for row in rows {
            let owner: String = row.get(1);
            found.insert(row.get::<_, String>(0), bucket_row::decode_owner(&owner));
        }
        Ok(buckets
            .iter()
            .map(|bucket| found.get(&bucket.to_string()).cloned().flatten())
            .collect())
    }

    async fn record(&self, bucket: &bucket::Name, created: SystemTime) -> Result<(), Error> {
        let created = nanos(created);
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        tx.execute(
            r#"INSERT INTO buckets (name, created, tags, owner, acl, cors)
               VALUES ($1, $2, '', '', '', '')
               ON CONFLICT (name) DO UPDATE
                 SET created = EXCLUDED.created, tags = '', owner = '', acl = '', cors = ''"#,
            &[p(&bucket.to_string()), p(&created)],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn create(&self, bucket: &bucket::Name, row: &bucket_row::Row) -> Result<(), Error> {
        let wires = row.to_raw();
        let created = wires.created as i64;
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let inserted = tx
            .execute(
                r#"INSERT INTO buckets (name, created, tags, owner, acl, cors)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   ON CONFLICT (name) DO NOTHING"#,
                &[
                    p(&bucket.to_string()),
                    p(&created),
                    p(&wires.tags),
                    p(&wires.owner),
                    p(&wires.grants),
                    p(&wires.cors),
                ],
            )
            .await?;
        tx.commit().await?;
        if inserted == 0 {
            return Err(Error::AlreadyExists(bucket.clone()));
        }
        Ok(())
    }

    async fn record_if_absent(
        &self,
        bucket: &bucket::Name,
        now: SystemTime,
    ) -> Result<SystemTime, Error> {
        let now = nanos(now);
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        tx.execute(
            r#"INSERT INTO buckets (name, created, tags, owner, acl, cors)
               VALUES ($1, $2, '', '', '', '')
               ON CONFLICT (name) DO NOTHING"#,
            &[p(&bucket.to_string()), p(&now)],
        )
        .await?;
        let created: i64 = tx
            .query_one(
                r#"SELECT created FROM buckets WHERE name = $1"#,
                &[p(&bucket.to_string())],
            )
            .await?
            .get(0);
        tx.commit().await?;
        Ok(stamp(created))
    }

    async fn record_full(&self, bucket: &bucket::Name, row: &bucket_row::Row) -> Result<(), Error> {
        let wires = row.to_raw();
        let created = wires.created as i64;
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        tx.execute(
            r#"INSERT INTO buckets (name, created, tags, owner, acl, cors)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (name) DO UPDATE
                 SET created = EXCLUDED.created, tags = EXCLUDED.tags, owner = EXCLUDED.owner,
                     acl = EXCLUDED.acl, cors = EXCLUDED.cors"#,
            &[
                p(&bucket.to_string()),
                p(&created),
                p(&wires.tags),
                p(&wires.owner),
                p(&wires.grants),
                p(&wires.cors),
            ],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_tags(&self, bucket: &bucket::Name, tags: &object::Tags) -> Result<bool, Error> {
        let wire = tags.to_wire();
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let written = tx
            .execute(
                r#"UPDATE buckets SET tags = $2 WHERE name = $1"#,
                &[p(&bucket.to_string()), p(&wire)],
            )
            .await?;
        tx.commit().await?;
        Ok(written > 0)
    }

    async fn set_tags_or_create(
        &self,
        bucket: &bucket::Name,
        tags: &object::Tags,
        now: SystemTime,
    ) -> Result<(), Error> {
        // ONE statement, its own transaction. The insert arm records the
        // first-sight row at `now` carrying the tag set and no other element
        // (the lazy first-sight policy); the conflict arm replaces the tags
        // alone, so the stored creation time, owner, ACL and CORS ride
        // untouched. The `WHERE` is the no-write arm redb's table layer
        // documents: an identical wire updates nothing.
        let wire = tags.to_wire();
        let created = nanos(now);
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        tx.execute(
            r#"INSERT INTO buckets (name, created, tags, owner, acl, cors)
               VALUES ($1, $2, $3, '', '', '')
               ON CONFLICT (name) DO UPDATE SET tags = EXCLUDED.tags
               WHERE buckets.tags IS DISTINCT FROM EXCLUDED.tags"#,
            &[p(&bucket.to_string()), p(&created), p(&wire)],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn clear_tags(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let written = tx
            .execute(
                r#"UPDATE buckets SET tags = '' WHERE name = $1"#,
                &[p(&bucket.to_string())],
            )
            .await?;
        tx.commit().await?;
        Ok(written > 0)
    }

    async fn set_acl(
        &self,
        bucket: &bucket::Name,
        owner: Option<&OwnerId>,
        grants: &AclGrants,
    ) -> Result<bool, Error> {
        let owner_wire = owner.map(OwnerId::as_str).unwrap_or_default();
        let grants_wire = Acl {
            owner: None,
            grants: grants.clone(),
        }
        .to_grants_wire();
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let written = tx
            .execute(
                r#"UPDATE buckets SET owner = $2, acl = $3 WHERE name = $1"#,
                &[p(&bucket.to_string()), p(&owner_wire), p(&grants_wire)],
            )
            .await?;
        tx.commit().await?;
        Ok(written > 0)
    }

    async fn set_grants_or_create(
        &self,
        bucket: &bucket::Name,
        grants: &AclGrants,
        now: SystemTime,
    ) -> Result<(), Error> {
        // ONE statement, its own transaction - and the owner element needs no
        // read to carry over: only the ACL column is in the conflict arm's
        // `SET`, so whatever owner (or creation time, tags, CORS) the row
        // already carries is preserved. The insert arm is the first-sight
        // row: the grant set, no owner (a bucket the API never sighted has
        // none), seeded at `now`. The `WHERE` is the documented no-write arm.
        let wire = Acl {
            owner: None,
            grants: grants.clone(),
        }
        .to_grants_wire();
        let created = nanos(now);
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        tx.execute(
            r#"INSERT INTO buckets (name, created, tags, owner, acl, cors)
               VALUES ($1, $2, '', '', $3, '')
               ON CONFLICT (name) DO UPDATE SET acl = EXCLUDED.acl
               WHERE buckets.acl IS DISTINCT FROM EXCLUDED.acl"#,
            &[p(&bucket.to_string()), p(&created), p(&wire)],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_cors(&self, bucket: &bucket::Name, config: &cors::Config) -> Result<bool, Error> {
        let wire = config.to_wire();
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let written = tx
            .execute(
                r#"UPDATE buckets SET cors = $2 WHERE name = $1"#,
                &[p(&bucket.to_string()), p(&wire)],
            )
            .await?;
        tx.commit().await?;
        Ok(written > 0)
    }

    async fn clear_cors(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let written = tx
            .execute(
                r#"UPDATE buckets SET cors = '' WHERE name = $1"#,
                &[p(&bucket.to_string())],
            )
            .await?;
        tx.commit().await?;
        Ok(written > 0)
    }

    async fn remove(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        tx.execute(
            r#"DELETE FROM buckets WHERE name = $1"#,
            &[p(&bucket.to_string())],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn remove_if_empty(&self, bucket: &bucket::Name) -> Result<(), Error> {
        // ONE statement: the probe and the delete share a snapshot and a
        // statement boundary, so no pre-read can be raced.
        let client = self.conn().await;
        let row = client
            .query_one(
                r#"WITH removed AS (
                       DELETE FROM buckets b
                       WHERE b.name = $1
                         AND NOT EXISTS (SELECT 1 FROM object_meta o WHERE o.bucket = b.name)
                         AND NOT EXISTS (SELECT 1 FROM uploads u WHERE u.bucket = b.name)
                       RETURNING b.name
                   )
                   SELECT EXISTS (SELECT 1 FROM buckets WHERE name = $1),
                          EXISTS (SELECT 1 FROM removed)"#,
                &[p(&bucket.to_string())],
            )
            .await?;
        let present: bool = row.get(0);
        let removed: bool = row.get(1);
        if !present {
            return Err(Error::NoSuchBucket(bucket.clone()));
        }
        if !removed {
            return Err(Error::NotEmpty(bucket.clone()));
        }
        Ok(())
    }
}

/// Decode one object row from a result row whose first column is index
/// `offset` (the four derived elements, then tags, checksum, owner, acl).
fn decode_object(row: &PgRow, offset: usize) -> Option<ObjectRow> {
    ObjectRow::from_raw(
        &row.get::<_, String>(offset),
        row.get::<_, i64>(offset + 1) as u64,
        row.get::<_, i64>(offset + 2) as u64,
        row.get::<_, i64>(offset + 3) as u64,
        &row.get::<_, String>(offset + 4),
        &row.get::<_, String>(offset + 5),
        &row.get::<_, String>(offset + 6),
        &row.get::<_, String>(offset + 7),
    )
}

#[async_trait]
impl ObjectMeta for PgStore {
    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<ObjectRow>, Error> {
        let client = self.conn().await;
        let row = client
            .query_opt(
                r#"SELECT etag, size, mtime, identity, tags, checksum, owner, acl
                   FROM object_meta WHERE bucket = $1 AND key = $2"#,
                &[p(&bucket.to_string()), p(&key.to_string())],
            )
            .await?;
        Ok(row.and_then(|row| decode_object(&row, 0)))
    }

    async fn put(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &ObjectRow,
    ) -> Result<(), Error> {
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        upsert_object(&tx, bucket, key, row).await?;
        tx.execute(
            r#"DELETE FROM object_parts WHERE bucket = $1 AND key = $2"#,
            &[p(&bucket.to_string()), p(&key.to_string())],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn put_guarded(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &ObjectRow,
    ) -> Result<(), Error> {
        // ONE statement: the bucket probe, the upsert and the overwrite's
        // retained-part clear share a statement boundary, so no pre-read can
        // be raced.
        let wires = row.to_raw();
        let size = wires.size as i64;
        let mtime = wires.mtime as i64;
        let identity = wires.identity as i64;
        let client = self.conn().await;
        let answer = client
            .query_one(
                r#"WITH guarded AS (
                       INSERT INTO object_meta
                           (bucket, key, etag, size, mtime, identity, tags, checksum, owner, acl)
                       SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                       WHERE EXISTS (SELECT 1 FROM buckets WHERE name = $1)
                       ON CONFLICT (bucket, key) DO UPDATE SET
                           etag = EXCLUDED.etag, size = EXCLUDED.size, mtime = EXCLUDED.mtime,
                           identity = EXCLUDED.identity, tags = EXCLUDED.tags,
                           checksum = EXCLUDED.checksum, owner = EXCLUDED.owner, acl = EXCLUDED.acl
                       RETURNING 1
                   ), cleared AS (
                       DELETE FROM object_parts
                       WHERE bucket = $1 AND key = $2 AND EXISTS (SELECT 1 FROM guarded)
                   )
                   SELECT EXISTS (SELECT 1 FROM guarded)"#,
                &[
                    p(&bucket.to_string()),
                    p(&key.to_string()),
                    p(&wires.etag),
                    p(&size),
                    p(&mtime),
                    p(&identity),
                    p(&wires.tags),
                    p(&wires.checksum),
                    p(&wires.owner),
                    p(&wires.grants),
                ],
            )
            .await?;
        let written: bool = answer.get(0);
        if !written {
            return Err(Error::NoSuchBucket(bucket.clone()));
        }
        Ok(())
    }

    async fn put_with_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &ObjectRow,
        parts: &[PartRow],
    ) -> Result<(), Error> {
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        upsert_object(&tx, bucket, key, row).await?;
        tx.execute(
            r#"DELETE FROM object_parts WHERE bucket = $1 AND key = $2"#,
            &[p(&bucket.to_string()), p(&key.to_string())],
        )
        .await?;
        if !parts.is_empty() {
            let numbers: Vec<i64> = parts.iter().map(|part| part.number as i64).collect();
            let sizes: Vec<i64> = parts.iter().map(|part| part.to_raw().0 as i64).collect();
            let algorithms: Vec<String> = parts.iter().map(|part| part.to_raw().1).collect();
            let values: Vec<String> = parts.iter().map(|part| part.to_raw().2).collect();
            tx.execute(
                r#"INSERT INTO object_parts (bucket, key, number, size, algorithm, value)
                   SELECT $1, $2, u.n, u.s, u.a, u.v
                   FROM unnest($3::bigint[], $4::bigint[], $5::text[], $6::text[])
                       AS u(n, s, a, v)"#,
                &[
                    p(&bucket.to_string()),
                    p(&key.to_string()),
                    p(&numbers),
                    p(&sizes),
                    p(&algorithms),
                    p(&values),
                ],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn parts(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Vec<PartRow>, Error> {
        let client = self.conn().await;
        let rows = client
            .query(
                r#"SELECT number, size, algorithm, value FROM object_parts
                   WHERE bucket = $1 AND key = $2 ORDER BY number"#,
                &[p(&bucket.to_string()), p(&key.to_string())],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|row| {
                PartRow::from_raw(
                    row.get::<_, i64>(0) as u32,
                    row.get::<_, i64>(1) as u64,
                    &row.get::<_, String>(2),
                    &row.get::<_, String>(3),
                )
            })
            .collect())
    }

    async fn put_preserving(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        identity: &Identity,
    ) -> Result<Preserved, Error> {
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let existing = tx
            .query_opt(
                r#"SELECT etag, size, mtime, identity, tags, checksum, owner, acl
                   FROM object_meta WHERE bucket = $1 AND key = $2 FOR UPDATE"#,
                &[p(&bucket.to_string()), p(&key.to_string())],
            )
            .await?;
        let had_row = existing.is_some();
        let preserved = existing
            .as_ref()
            .and_then(|row| decode_object(row, 0))
            .map(|row| row.preserved())
            .unwrap_or_else(|| Preserved {
                tags: object::Tags::empty(),
                checksum: None,
                owner: None,
                acl: Acl::default_private(None),
            });
        let etag = identity.etag.as_str();
        let size = identity.size as i64;
        let mtime = nanos(identity.mtime);
        let file_identity = identity.file_identity as i64;
        if had_row {
            tx.execute(
                r#"UPDATE object_meta SET etag = $3, size = $4, mtime = $5, identity = $6
                   WHERE bucket = $1 AND key = $2"#,
                &[
                    p(&bucket.to_string()),
                    p(&key.to_string()),
                    p(&etag),
                    p(&size),
                    p(&mtime),
                    p(&file_identity),
                ],
            )
            .await?;
        } else {
            let tags = preserved.tags.to_wire();
            let checksum = preserved
                .checksum
                .as_ref()
                .map(checksum::Recorded::to_wire)
                .unwrap_or_default();
            let owner = preserved
                .owner
                .as_ref()
                .map(OwnerId::as_str)
                .unwrap_or_default()
                .to_owned();
            let grants = Acl {
                owner: None,
                grants: preserved.acl.grants.clone(),
            }
            .to_grants_wire();
            tx.execute(
                r#"INSERT INTO object_meta
                       (bucket, key, etag, size, mtime, identity, tags, checksum, owner, acl)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#,
                &[
                    p(&bucket.to_string()),
                    p(&key.to_string()),
                    p(&etag),
                    p(&size),
                    p(&mtime),
                    p(&file_identity),
                    p(&tags),
                    p(&checksum),
                    p(&owner),
                    p(&grants),
                ],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(preserved)
    }

    async fn move_(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Error> {
        // A same-key move is a no-op. The destination delete below would
        // otherwise remove the very row and parts being moved, the UPDATE would
        // match nothing, and the call would report success having destroyed the
        // key. The content plane's rename/copy guard the identical case.
        if from == to {
            return Ok(());
        }
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let source = tx
            .query_opt(
                r#"SELECT 1 FROM object_meta WHERE bucket = $1 AND key = $2"#,
                &[p(&bucket.to_string()), p(&from.to_string())],
            )
            .await?;
        if source.is_none() {
            tx.rollback().await?;
            return Ok(());
        }
        // Each statement binds exactly the placeholders it mentions: an unused
        // parameter leaves PostgreSQL unable to infer its type (42P18).
        tx.execute(
            r#"DELETE FROM object_meta WHERE bucket = $1 AND key = $2"#,
            &[p(&bucket.to_string()), p(&to.to_string())],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM object_parts WHERE bucket = $1 AND key = $2"#,
            &[p(&bucket.to_string()), p(&to.to_string())],
        )
        .await?;
        tx.execute(
            r#"UPDATE object_meta SET key = $3 WHERE bucket = $1 AND key = $2"#,
            &[
                p(&bucket.to_string()),
                p(&from.to_string()),
                p(&to.to_string()),
            ],
        )
        .await?;
        tx.execute(
            r#"UPDATE object_parts SET key = $3 WHERE bucket = $1 AND key = $2"#,
            &[
                p(&bucket.to_string()),
                p(&from.to_string()),
                p(&to.to_string()),
            ],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<bool, Error> {
        let wire = tags.to_wire();
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let written = tx
            .execute(
                r#"UPDATE object_meta SET tags = $3 WHERE bucket = $1 AND key = $2"#,
                &[p(&bucket.to_string()), p(&key.to_string()), p(&wire)],
            )
            .await?;
        tx.commit().await?;
        Ok(written > 0)
    }

    async fn clear_tags(&self, bucket: &bucket::Name, key: &object::Key) -> Result<bool, Error> {
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let written = tx
            .execute(
                r#"UPDATE object_meta SET tags = '' WHERE bucket = $1 AND key = $2"#,
                &[p(&bucket.to_string()), p(&key.to_string())],
            )
            .await?;
        tx.commit().await?;
        Ok(written > 0)
    }

    async fn ensure_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<(), Error> {
        // The contract's `ensure_tags` rule: replacing an **identical** set
        // happens without a write, the no-write arm redb's table carries as
        // `write_if(changed)`. The `WHERE` makes the update conditional on the
        // stored wire actually differing - `IS DISTINCT FROM` is the same
        // no-write arm the bucket twins `set_tags_or_create` and
        // `set_grants_or_create` use - so an identical set rewrites no row and
        // commits nothing. The answer is `Ok(())` either way: the contract
        // gives this method no change count to report.
        let wire = tags.to_wire();
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        tx.execute(
            r#"UPDATE object_meta SET tags = $3
               WHERE bucket = $1 AND key = $2 AND tags IS DISTINCT FROM $3"#,
            &[p(&bucket.to_string()), p(&key.to_string()), p(&wire)],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        owner: Option<&OwnerId>,
        grants: &AclGrants,
    ) -> Result<bool, Error> {
        let owner_wire = owner.map(OwnerId::as_str).unwrap_or_default();
        let grants_wire = Acl {
            owner: None,
            grants: grants.clone(),
        }
        .to_grants_wire();
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let written = tx
            .execute(
                r#"UPDATE object_meta SET owner = $3, acl = $4 WHERE bucket = $1 AND key = $2"#,
                &[
                    p(&bucket.to_string()),
                    p(&key.to_string()),
                    p(&owner_wire),
                    p(&grants_wire),
                ],
            )
            .await?;
        tx.commit().await?;
        Ok(written > 0)
    }

    async fn put_batch(
        &self,
        bucket: &bucket::Name,
        rows: &[(object::Key, ObjectRow)],
    ) -> Result<(), Error> {
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        for (key, row) in rows {
            upsert_object(&tx, bucket, key, row).await?;
            tx.execute(
                r#"DELETE FROM object_parts WHERE bucket = $1 AND key = $2"#,
                &[p(&bucket.to_string()), p(&key.to_string())],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn remove(&self, bucket: &bucket::Name, key: &object::Key) -> Result<bool, Error> {
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let removed = tx
            .execute(
                r#"DELETE FROM object_meta WHERE bucket = $1 AND key = $2"#,
                &[p(&bucket.to_string()), p(&key.to_string())],
            )
            .await?;
        tx.execute(
            r#"DELETE FROM object_parts WHERE bucket = $1 AND key = $2"#,
            &[p(&bucket.to_string()), p(&key.to_string())],
        )
        .await?;
        tx.commit().await?;
        Ok(removed > 0)
    }

    async fn remove_many(&self, bucket: &bucket::Name, keys: &[object::Key]) -> Result<(), Error> {
        let wires: Vec<String> = keys.iter().map(ToString::to_string).collect();
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        tx.execute(
            r#"DELETE FROM object_meta WHERE bucket = $1 AND key = ANY($2)"#,
            &[p(&bucket.to_string()), p(&wires)],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM object_parts WHERE bucket = $1 AND key = ANY($2)"#,
            &[p(&bucket.to_string()), p(&wires)],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn load(
        &self,
        bucket: &bucket::Name,
        keys: &[object::Key],
    ) -> Result<Vec<Option<ObjectRow>>, Error> {
        let wires: Vec<String> = keys.iter().map(ToString::to_string).collect();
        let client = self.conn().await;
        let rows = client
            .query(
                r#"SELECT key, etag, size, mtime, identity, tags, checksum, owner, acl
                   FROM object_meta WHERE bucket = $1 AND key = ANY($2)"#,
                &[p(&bucket.to_string()), p(&wires)],
            )
            .await?;
        let mut found: HashMap<String, Option<ObjectRow>> = HashMap::new();
        for row in rows {
            let stored: String = row.get(0);
            found.insert(stored, decode_object(&row, 1));
        }
        Ok(keys
            .iter()
            .map(|key| found.get(&key.to_string()).cloned().flatten())
            .collect())
    }

    async fn walk(&self, bucket: &bucket::Name) -> Result<Vec<(object::Key, ObjectRow)>, Error> {
        let client = self.conn().await;
        let rows = client
            .query(
                r#"SELECT key, etag, size, mtime, identity, tags, checksum, owner, acl
                   FROM object_meta WHERE bucket = $1 ORDER BY key COLLATE "C""#,
                &[p(&bucket.to_string())],
            )
            .await?;
        let mut walked = Vec::with_capacity(rows.len());
        for row in &rows {
            let key = object::key(row.get::<_, String>(0)).expect("a stored key is valid");
            let decoded = decode_object(row, 1).ok_or_else(|| Error::Corrupt {
                bucket: bucket.clone(),
                key: key.to_string(),
            })?;
            walked.push((key, decoded));
        }
        Ok(walked)
    }

    async fn walk_healing(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<(object::Key, Option<ObjectRow>)>, Error> {
        let client = self.conn().await;
        let rows = client
            .query(
                r#"SELECT key, etag, size, mtime, identity, tags, checksum, owner, acl
                   FROM object_meta WHERE bucket = $1 ORDER BY key COLLATE "C""#,
                &[p(&bucket.to_string())],
            )
            .await?;
        let mut walked = Vec::with_capacity(rows.len());
        for row in &rows {
            let key = object::key(row.get::<_, String>(0)).expect("a stored key is valid");
            match decode_object(row, 1) {
                Some(decoded) => walked.push((key, Some(decoded))),
                None => {
                    // The heal write: a corrupt row is not served. A write that
                    // fails is logged and the read still succeeds.
                    if let Err(err) = client
                        .execute(
                            r#"DELETE FROM object_meta WHERE bucket = $1 AND key = $2"#,
                            &[p(&bucket.to_string()), p(&key.to_string())],
                        )
                        .await
                    {
                        eprintln!("pg-meta: heal write for {bucket}/{key} failed: {err}");
                    }
                    walked.push((key, None));
                }
            }
        }
        Ok(walked)
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        tx.execute(
            r#"DELETE FROM object_meta WHERE bucket = $1"#,
            &[p(&bucket.to_string())],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM object_parts WHERE bucket = $1"#,
            &[p(&bucket.to_string())],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

/// The one upsert shape every object-row write shares.
async fn upsert_object(
    tx: &tokio_postgres::Transaction<'_>,
    bucket: &bucket::Name,
    key: &object::Key,
    row: &ObjectRow,
) -> Result<(), Error> {
    let wires = row.to_raw();
    let size = wires.size as i64;
    let mtime = wires.mtime as i64;
    let identity = wires.identity as i64;
    tx.execute(
        r#"INSERT INTO object_meta
               (bucket, key, etag, size, mtime, identity, tags, checksum, owner, acl)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
           ON CONFLICT (bucket, key) DO UPDATE SET
               etag = EXCLUDED.etag, size = EXCLUDED.size, mtime = EXCLUDED.mtime,
               identity = EXCLUDED.identity, tags = EXCLUDED.tags,
               checksum = EXCLUDED.checksum, owner = EXCLUDED.owner, acl = EXCLUDED.acl"#,
        &[
            p(&bucket.to_string()),
            p(&key.to_string()),
            p(&wires.etag),
            p(&size),
            p(&mtime),
            p(&identity),
            p(&wires.tags),
            p(&wires.checksum),
            p(&wires.owner),
            p(&wires.grants),
        ],
    )
    .await?;
    Ok(())
}

/// Decode one upload (its state row joined to its optional checksum row) from
/// a result row: eight columns starting at `offset`.
fn decode_upload(
    row: &PgRow,
    offset: usize,
    bucket: &bucket::Name,
) -> Option<multipart::MultipartUpload> {
    let mut decoded = upload::Row::from_raw(
        &row.get::<_, String>(offset),
        &row.get::<_, String>(offset + 1),
        row.get::<_, i64>(offset + 2) as u64,
        &row.get::<_, String>(offset + 3),
        &row.get::<_, String>(offset + 4),
        &row.get::<_, String>(offset + 5),
    )?;
    let algorithm: Option<String> = row.get(offset + 6);
    let kind: Option<String> = row.get(offset + 7);
    decoded.checksum = algorithm
        .zip(kind)
        .and_then(|(algorithm, kind)| checksum::Upload::from_wire_opt(&algorithm, &kind));
    Some(decoded.to_upload(bucket))
}

#[async_trait]
impl UploadMeta for PgStore {
    async fn create(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        row: &upload::Row,
    ) -> Result<(), Error> {
        let wires = row.to_raw();
        let initiated_at = wires.initiated_at as i64;
        let checksum_wires = row.checksum.as_ref().map(checksum::Upload::to_wire);
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        tx.execute(
            r#"INSERT INTO uploads (bucket, upload_id, key, initiated_at, tags, owner, acl)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (bucket, upload_id) DO UPDATE SET
                   key = EXCLUDED.key, initiated_at = EXCLUDED.initiated_at,
                   tags = EXCLUDED.tags, owner = EXCLUDED.owner, acl = EXCLUDED.acl"#,
            &[
                p(&bucket.to_string()),
                p(&upload_id),
                p(&wires.key.to_string()),
                p(&initiated_at),
                p(&wires.tags),
                p(&wires.owner),
                p(&wires.grants),
            ],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM upload_checksums WHERE bucket = $1 AND upload_id = $2"#,
            &[p(&bucket.to_string()), p(&upload_id)],
        )
        .await?;
        if let Some((algorithm, kind)) = checksum_wires {
            tx.execute(
                r#"INSERT INTO upload_checksums (bucket, upload_id, algorithm, kind)
                   VALUES ($1, $2, $3, $4)"#,
                &[
                    p(&bucket.to_string()),
                    p(&upload_id),
                    p(&algorithm),
                    p(&kind),
                ],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<Option<upload::Row>, Error> {
        let client = self.conn().await;
        let row = client
            .query_opt(
                r#"SELECT u.key, u.initiated_at, u.tags, u.owner, u.acl, c.algorithm, c.kind
                   FROM uploads u
                   LEFT JOIN upload_checksums c
                       ON c.bucket = u.bucket AND c.upload_id = u.upload_id
                   WHERE u.bucket = $1 AND u.upload_id = $2"#,
                &[p(&bucket.to_string()), p(&upload_id)],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let stored_key: String = row.get(0);
        if stored_key != key.to_string() {
            return Ok(None);
        }
        let mut decoded = upload::Row::from_raw(
            upload_id,
            &stored_key,
            row.get::<_, i64>(1) as u64,
            &row.get::<_, String>(2),
            &row.get::<_, String>(3),
            &row.get::<_, String>(4),
        );
        if let Some(decoded) = decoded.as_mut() {
            let algorithm: Option<String> = row.get(5);
            let kind: Option<String> = row.get(6);
            decoded.checksum = algorithm
                .zip(kind)
                .and_then(|(algorithm, kind)| checksum::Upload::from_wire_opt(&algorithm, &kind));
        }
        Ok(decoded)
    }

    async fn put_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        number: PartNumber,
        etag: &ETag,
        checksum: Option<&checksum::PartChecksum>,
    ) -> Result<(), Error> {
        let number = u32::from(number) as i64;
        let etag_wire = etag.as_str();
        let recorded = checksum.and_then(|tee| tee.digest.get()).cloned();
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let found = tx
            .query_opt(
                r#"SELECT key FROM uploads WHERE bucket = $1 AND upload_id = $2"#,
                &[p(&bucket.to_string()), p(&upload_id)],
            )
            .await?;
        let matches = found
            .map(|row| row.get::<_, String>(0) == key.to_string())
            .unwrap_or(false);
        if !matches {
            // The upload is absent, or it names another key: no part is
            // written for a foreign upload.
            tx.rollback().await?;
            return Err(Error::NoSuchUpload(upload_id.to_owned()));
        }
        tx.execute(
            r#"INSERT INTO parts (bucket, upload_id, number, etag) VALUES ($1, $2, $3, $4)
               ON CONFLICT (bucket, upload_id, number) DO UPDATE SET etag = EXCLUDED.etag"#,
            &[
                p(&bucket.to_string()),
                p(&upload_id),
                p(&number),
                p(&etag_wire),
            ],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM part_checksums WHERE bucket = $1 AND upload_id = $2 AND number = $3"#,
            &[p(&bucket.to_string()), p(&upload_id), p(&number)],
        )
        .await?;
        if let Some(part) = recorded {
            let (algorithm, value) = part.to_wire();
            tx.execute(
                r#"INSERT INTO part_checksums (bucket, upload_id, number, algorithm, value)
                   VALUES ($1, $2, $3, $4, $5)"#,
                &[
                    p(&bucket.to_string()),
                    p(&upload_id),
                    p(&number),
                    p(&algorithm),
                    p(&value),
                ],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn list_parts(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        from: PartNumber,
    ) -> Result<Vec<upload::PartRow>, Error> {
        let from = u32::from(from) as i64;
        let client = self.conn().await;
        let rows = client
            .query(
                r#"SELECT p.number, p.etag, c.algorithm, c.value
                   FROM parts p
                   LEFT JOIN part_checksums c
                       ON c.bucket = p.bucket AND c.upload_id = p.upload_id
                      AND c.number = p.number
                   WHERE p.bucket = $1 AND p.upload_id = $2 AND p.number >= $3
                   ORDER BY p.number"#,
                &[p(&bucket.to_string()), p(&upload_id), p(&from)],
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(|row| {
                upload::PartRow::from_raw(
                    row.get::<_, i64>(0) as u32,
                    &row.get::<_, String>(1),
                    &row.get::<_, Option<String>>(2).unwrap_or_default(),
                    &row.get::<_, Option<String>>(3).unwrap_or_default(),
                )
            })
            .collect())
    }

    async fn list_parts_page(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        from: u32,
        max: usize,
    ) -> Result<upload::PartsPage, Error> {
        // `max == 0` asks for nothing and must not read as truncated: the
        // caller's resume marker would otherwise advance past a page it never
        // received.
        if max == 0 {
            return Ok(upload::PartsPage {
                rows: Vec::new(),
                more: false,
                raw_last: None,
            });
        }
        let from = from as i64;
        let limit = max as i64 + 1;
        let client = self.conn().await;
        // One statement: the `parts` rows (`from` inclusive, the page's `max`
        // rows plus the one-lookahead flag) and the `part_checksums` join --
        // the same join `list_parts` performs, capped at the page.
        let rows = client
            .query(
                r#"SELECT p.number, p.etag, c.algorithm, c.value
                   FROM parts p
                   LEFT JOIN part_checksums c
                       ON c.bucket = p.bucket AND c.upload_id = p.upload_id
                      AND c.number = p.number
                   WHERE p.bucket = $1 AND p.upload_id = $2 AND p.number >= $3
                   ORDER BY p.number
                   LIMIT $4"#,
                &[
                    p(&bucket.to_string()),
                    p(&upload_id),
                    p(&from),
                    p(&limit),
                ],
            )
            .await?;
        // The truncation flag and the raw read's own last part number, both
        // taken before the join drops any domain-invalid row: a caller
        // resuming from the served rows alone would stall on a truncated page
        // the self-heal emptied.
        let more = rows.len() > max;
        let page = &rows[..rows.len().min(max)];
        let raw_last = page.last().map(|row| row.get::<_, i64>(0) as u32);
        let mut parts = Vec::with_capacity(page.len());
        for row in page {
            // A domain-invalid stored ETag reads as absent (the `PartRow`
            // self-heal); a domain-invalid checksum self-heals to `None` and
            // the part is still listed.
            if let Some(part) = upload::PartRow::from_raw(
                row.get::<_, i64>(0) as u32,
                &row.get::<_, String>(1),
                &row.get::<_, Option<String>>(2).unwrap_or_default(),
                &row.get::<_, Option<String>>(3).unwrap_or_default(),
            ) {
                parts.push(part);
            }
        }
        Ok(upload::PartsPage {
            rows: parts,
            more,
            raw_last,
        })
    }

    async fn part_records(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        numbers: &[PartNumber],
    ) -> Result<Option<Vec<Option<upload::PartRecord>>>, Error> {
        let client = self.conn().await;
        let upload_key = client
            .query_opt(
                r#"SELECT key FROM uploads WHERE bucket = $1 AND upload_id = $2"#,
                &[p(&bucket.to_string()), p(&upload_id)],
            )
            .await?;
        match upload_key {
            Some(row) if row.get::<_, String>(0) == key.to_string() => {}
            _ => return Ok(None),
        }
        let wanted: Vec<i64> = numbers
            .iter()
            .map(|number| u32::from(*number) as i64)
            .collect();
        let rows = client
            .query(
                r#"SELECT p.number, p.etag, c.algorithm, c.value
                   FROM parts p
                   LEFT JOIN part_checksums c
                       ON c.bucket = p.bucket AND c.upload_id = p.upload_id
                      AND c.number = p.number
                   WHERE p.bucket = $1 AND p.upload_id = $2 AND p.number = ANY($3)"#,
                &[p(&bucket.to_string()), p(&upload_id), p(&wanted)],
            )
            .await?;
        let mut found: HashMap<i64, upload::PartRecord> = HashMap::new();
        for row in rows {
            let number: i64 = row.get(0);
            if let Some(record) = upload::PartRecord::from_raw(
                number as u32,
                &row.get::<_, String>(1),
                &row.get::<_, Option<String>>(2).unwrap_or_default(),
                &row.get::<_, Option<String>>(3).unwrap_or_default(),
            ) {
                found.insert(number, record);
            }
        }
        Ok(Some(
            numbers
                .iter()
                .map(|number| found.get(&(u32::from(*number) as i64)).cloned())
                .collect(),
        ))
    }

    async fn list_uploads(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<multipart::MultipartUpload>, Error> {
        let client = self.conn().await;
        let rows = client
            .query(
                r#"SELECT u.upload_id, u.key, u.initiated_at, u.tags, u.owner, u.acl,
                          c.algorithm, c.kind
                   FROM uploads u
                   LEFT JOIN upload_checksums c
                       ON c.bucket = u.bucket AND c.upload_id = u.upload_id
                   WHERE u.bucket = $1
                   ORDER BY u.key COLLATE "C", u.upload_id COLLATE "C""#,
                &[p(&bucket.to_string())],
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(|row| decode_upload(row, 0, bucket))
            .collect())
    }

    async fn list_uploads_page(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        max_uploads: usize,
    ) -> Result<
        (
            Vec<multipart::MultipartUpload>,
            Vec<String>,
            bool,
            Option<String>,
        ),
        Error,
    > {
        // The whole-bucket materialization (the deferred performance item,
        // finding F17: bounded at the promotion to `crates/tinio-store-pg`):
        // one SELECT narrows the scan to the prefix bound and the rows feed
        // the shared unordered pager, which owns the marker, rollup,
        // truncation and resume rules -- the one home, shared with the mem
        // and fs backends. The SQL carries no marker predicate and no ORDER
        // BY: the exclusive-after marker is the engine's, applied in memory
        // over the materialized rows, and the engine is order-independent.
        let client = self.conn().await;
        let rows = client
            .query(
                r#"SELECT u.upload_id, u.key, u.initiated_at, u.tags, u.owner, u.acl,
                          c.algorithm, c.kind
                   FROM uploads u
                   LEFT JOIN upload_checksums c
                       ON c.bucket = u.bucket AND c.upload_id = u.upload_id
                   WHERE u.bucket = $1
                     AND starts_with(u.key, $2)"#,
                &[p(&bucket.to_string()), p(&prefix)],
            )
            .await?;
        let uploads: Vec<multipart::MultipartUpload> = rows
            .iter()
            .filter_map(|row| decode_upload(row, 0, bucket))
            .collect();
        Ok(storage::group_and_paginate_unordered(
            uploads,
            prefix,
            delimiter,
            marker,
            max_uploads,
            |upload| upload.key.as_ref(),
            |upload| storage::uploads_order(&upload.key, &upload.upload_id),
        ))
    }

    async fn has_uploads(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let client = self.conn().await;
        let row = client
            .query_one(
                r#"SELECT EXISTS (SELECT 1 FROM uploads WHERE bucket = $1)"#,
                &[p(&bucket.to_string())],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn walk_uploads(&self) -> Result<Vec<multipart::MultipartUpload>, Error> {
        let client = self.conn().await;
        let rows = client
            .query(
                r#"SELECT u.bucket, u.upload_id, u.key, u.initiated_at, u.tags, u.owner, u.acl,
                          c.algorithm, c.kind
                   FROM uploads u
                   LEFT JOIN upload_checksums c
                       ON c.bucket = u.bucket AND c.upload_id = u.upload_id
                   ORDER BY u.bucket COLLATE "C", u.key COLLATE "C",
                            u.upload_id COLLATE "C""#,
                &[],
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(|row| {
                let bucket = bucket::name(row.get::<_, String>(0)).ok()?;
                decode_upload(row, 1, &bucket)
            })
            .collect())
    }

    async fn drain_upload(&self, bucket: &bucket::Name, upload_id: &str) -> Result<(), Error> {
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        tx.execute(
            r#"DELETE FROM parts WHERE bucket = $1 AND upload_id = $2"#,
            &[p(&bucket.to_string()), p(&upload_id)],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM part_checksums WHERE bucket = $1 AND upload_id = $2"#,
            &[p(&bucket.to_string()), p(&upload_id)],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM upload_checksums WHERE bucket = $1 AND upload_id = $2"#,
            &[p(&bucket.to_string()), p(&upload_id)],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM uploads WHERE bucket = $1 AND upload_id = $2"#,
            &[p(&bucket.to_string()), p(&upload_id)],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn drain_upload_checked(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), Error> {
        // ONE statement: the identity check and the drain share a statement
        // boundary and a snapshot, so a racing complete cannot slip between
        // them - the check IS the gate every delete reads (the `put_guarded`
        // and `remove_if_empty` idiom). A missing row, or one recorded under
        // another key, drains nothing and answers `NoSuchUpload`, never an
        // empty success (D22: the abort path's refusal).
        let client = self.conn().await;
        let row = client
            .query_one(
                r#"WITH matched AS (
                       SELECT 1 FROM uploads
                       WHERE bucket = $1 AND upload_id = $2 AND key = $3
                   ),
                   drained_parts AS (
                       DELETE FROM parts
                       WHERE bucket = $1 AND upload_id = $2
                         AND EXISTS (SELECT 1 FROM matched)
                       RETURNING 1
                   ),
                   drained_part_checksums AS (
                       DELETE FROM part_checksums
                       WHERE bucket = $1 AND upload_id = $2
                         AND EXISTS (SELECT 1 FROM matched)
                       RETURNING 1
                   ),
                   drained_upload_checksums AS (
                       DELETE FROM upload_checksums
                       WHERE bucket = $1 AND upload_id = $2
                         AND EXISTS (SELECT 1 FROM matched)
                       RETURNING 1
                   ),
                   drained_upload AS (
                       DELETE FROM uploads
                       WHERE bucket = $1 AND upload_id = $2
                         AND EXISTS (SELECT 1 FROM matched)
                       RETURNING 1
                   )
                   SELECT EXISTS (SELECT 1 FROM matched)"#,
                &[p(&bucket.to_string()), p(&upload_id), p(&key.to_string())],
            )
            .await?;
        let matched: bool = row.get(0);
        if !matched {
            return Err(Error::NoSuchUpload(upload_id.to_owned()));
        }
        Ok(())
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        tx.execute(
            r#"DELETE FROM parts WHERE bucket = $1"#,
            &[p(&bucket.to_string())],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM part_checksums WHERE bucket = $1"#,
            &[p(&bucket.to_string())],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM upload_checksums WHERE bucket = $1"#,
            &[p(&bucket.to_string())],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM uploads WHERE bucket = $1"#,
            &[p(&bucket.to_string())],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

/// The generation counter: first publish 1, then 2, ...
const ALLOC_HEAD_SQL: &str = r#"INSERT INTO content_heads
        (bucket, kind, ckey, next_seq, live_seq, live_len)
    VALUES ($1, $2, $3, 1, NULL, 0)
    ON CONFLICT (bucket, kind, ckey) DO UPDATE
        SET next_seq = content_heads.next_seq + 1
    RETURNING next_seq"#;

/// The visibility flip: one statement, guarded so only the publish that
/// allocated the sequence can make it live.
const PUBLISH_HEAD_SQL: &str = r#"UPDATE content_heads SET live_seq = $4, live_len = $5
    WHERE bucket = $1 AND kind = $2 AND ckey = $3 AND next_seq = $4"#;

/// Decode one version row whose eleven value columns start at `offset`.
fn decode_version(row: &PgRow, offset: usize, seq: u64) -> Option<Stored> {
    Stored::from_raw(
        seq,
        &row.get::<_, String>(offset),
        &row.get::<_, String>(offset + 1),
        row.get::<_, i64>(offset + 2) as u64,
        &row.get::<_, String>(offset + 3),
        row.get::<_, i64>(offset + 4) as u64,
        row.get::<_, i64>(offset + 5) as u64,
        &row.get::<_, String>(offset + 6),
        &row.get::<_, String>(offset + 7),
        &row.get::<_, String>(offset + 8),
        &row.get::<_, String>(offset + 9),
        &row.get::<_, String>(offset + 10),
    )
}

#[async_trait]
impl VersionMeta for PgStore {
    type Error = Error;

    async fn bucket_state(&self, bucket: &bucket::Name) -> Result<State, Error> {
        let client = self.conn().await;
        let row = client
            .query_opt(
                r#"SELECT state FROM bucket_versioning WHERE bucket = $1"#,
                &[p(&bucket.to_string())],
            )
            .await?;
        Ok(row
            .map(|row| row.get::<_, String>(0))
            .and_then(|state| state.parse::<State>().ok())
            .unwrap_or(State::Unversioned))
    }

    async fn set_bucket_state(&self, bucket: &bucket::Name, state: State) -> Result<(), Error> {
        let wire = state.to_string();
        let client = self.conn().await;
        client
            .execute(
                r#"INSERT INTO bucket_versioning (bucket, state) VALUES ($1, $2)
                   ON CONFLICT (bucket) DO UPDATE SET state = EXCLUDED.state"#,
                &[p(&bucket.to_string()), p(&wire)],
            )
            .await?;
        Ok(())
    }

    async fn current_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<Current>, Error> {
        let client = self.conn().await;
        let row = client
            .query_opt(
                r#"SELECT s.version_id, r.kind
                   FROM current_slots s
                   LEFT JOIN version_rows r
                       ON r.bucket = s.bucket AND r.key = s.key AND r.seq = s.seq
                   WHERE s.bucket = $1 AND s.key = $2"#,
                &[p(&bucket.to_string()), p(&key.to_string())],
            )
            .await?;
        Ok(row.map(|row| {
            let id: Option<String> = row.get(0);
            let kind: Option<String> = row.get(1);
            Current {
                exists: true,
                version: id.as_deref().and_then(version::Id::from_wire),
                is_delete_marker: kind.as_deref() == Some(Kind::Marker.as_wire()),
            }
        }))
    }

    async fn advance_slot(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<version::Id>,
        seq: u64,
    ) -> Result<(), Error> {
        let id_wire = id.as_ref().map(version::Id::as_str);
        let seq = seq as i64;
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        // The monotonic guard (the contract's `advance_slot` rule): a slot
        // never moves backwards. The write transaction serialises commits but
        // does not order them by sequence, so a lower-sequence advance
        // committing after a higher-sequence one would leave the slot below the
        // newest version it already named - the state the slot exists to
        // prevent, since `current_slot` would then resolve the marker bit off
        // the stale row. The read is `FOR UPDATE` (the `put_preserving` /
        // `put_version` idiom), inside this transaction, so no other advance can
        // slip between the read and the write. A move to the **same** sequence
        // is the crash window's own re-run and is allowed; a move to a
        // **lower** one is refused, the way `put_version` refuses a row another
        // version occupies. The `tx` drop rolls the read back, leaving the slot
        // untouched.
        let stored = tx
            .query_opt(
                r#"SELECT seq FROM current_slots
                   WHERE bucket = $1 AND key = $2
                   FOR UPDATE"#,
                &[p(&bucket.to_string()), p(&key.to_string())],
            )
            .await?;
        if let Some(current) = stored.map(|row| row.get::<_, i64>(0))
            && seq < current
        {
            return Err(Error::SlotRewind {
                bucket: bucket.clone(),
                key: key.to_string(),
                seq: seq as u64,
            });
        }
        // The slot is the version-row fact `current_slot` resolves the marker
        // bit from: the slot row names only `(version_id, seq)`, and
        // `current_slot` LEFT JOINs the version row at that sequence, so the bit
        // is derived from the row at read time and is never carried or written
        // by this call (there is no column a caller's flag could land in).
        tx.execute(
            r#"INSERT INTO current_slots (bucket, key, version_id, seq)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (bucket, key) DO UPDATE
                   SET version_id = EXCLUDED.version_id, seq = EXCLUDED.seq"#,
            &[
                p(&bucket.to_string()),
                p(&key.to_string()),
                p(&id_wire),
                p(&seq),
            ],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn put_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &Stored,
    ) -> Result<(), Error> {
        let (id, blob, size, etag, created, became, kind, tags, checksum, owner, grants) =
            row.to_raw();
        let seq = row.seq as i64;
        let size = size as i64;
        let created = created as i64;
        let became = became as i64;
        // The one identity two rows can share: the null wire. Committing it
        // triggers the one-null-version displacement below.
        let converges_null = row.id.is_none();
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        // The row-overwrite guard: the row already at this sequence may only be
        // rewritten by the **same** version - the crash-window adoption path
        // re-runs a supersede whose row is already committed. A different
        // identity or kind, or the same identity and kind naming different
        // content (the null-versus-null shape, where only the content address
        // separates the two), is another version's committed row: overwriting
        // it would replace that version's content and drop its blob out of the
        // live set for the sweep. The read is `FOR UPDATE`, inside this
        // transaction (the `put_preserving` idiom), so no other commit can slip
        // a row in between the read and the write.
        let existing = tx
            .query_opt(
                r#"SELECT id, blob, size, kind, became_noncurrent
                   FROM version_rows
                   WHERE bucket = $1 AND key = $2 AND seq = $3
                   FOR UPDATE"#,
                &[p(&bucket.to_string()), p(&key.to_string()), p(&seq)],
            )
            .await?;
        let stamp = match &existing {
            Some(stored) => {
                let stored_id: String = stored.get(0);
                let stored_blob: String = stored.get(1);
                let stored_size: i64 = stored.get(2);
                let stored_kind: String = stored.get(3);
                if stored_id != id
                    || stored_kind != kind
                    || stored_blob != blob
                    || stored_size != size
                {
                    // The `tx` drop rolls the read back; the refusal carries
                    // the stored fact, never a value formatted into SQL.
                    return Err(Error::VersionConflict {
                        bucket: bucket.clone(),
                        key: key.to_string(),
                        seq: row.seq,
                    });
                }
                // The stored stamp is the authority: a row already at this
                // sequence was stamped by the supersede that wrote it, and the
                // adoption rewrite must never un-stamp it. Read here, in the
                // writing transaction, so no other commit can slip a stamp in
                // between a read and this write.
                stored.get::<_, i64>(4)
            }
            // A sequence with no row stores the caller's stamp.
            None => became,
        };
        // The one-null-version rule: the index can name only one of two rows
        // sharing the `null` wire, so committing a null-identity row first
        // displaces any other null-id row of the key - the row and the index
        // entry that pointed at it - in this same transaction. The
        // `DELETE ... RETURNING seq` and the index delete run in one statement,
        // so the entry goes only when it still named the displaced row.
        if converges_null {
            tx.execute(
                r#"WITH displaced AS (
                       DELETE FROM version_rows
                       WHERE bucket = $1 AND key = $2 AND id = 'null' AND seq <> $3
                       RETURNING seq
                   )
                   DELETE FROM version_index
                   WHERE bucket = $1 AND key = $2 AND id = 'null'
                     AND seq IN (SELECT seq FROM displaced)"#,
                &[p(&bucket.to_string()), p(&key.to_string()), p(&seq)],
            )
            .await?;
        }
        tx.execute(
            r#"INSERT INTO version_rows
                   (bucket, key, seq, id, blob, size, etag, created, became_noncurrent,
                    kind, tags, checksum, owner, acl)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
               ON CONFLICT (bucket, key, seq) DO UPDATE SET
                   id = EXCLUDED.id, blob = EXCLUDED.blob, size = EXCLUDED.size,
                   etag = EXCLUDED.etag, created = EXCLUDED.created,
                   became_noncurrent = EXCLUDED.became_noncurrent, kind = EXCLUDED.kind,
                   tags = EXCLUDED.tags, checksum = EXCLUDED.checksum,
                   owner = EXCLUDED.owner, acl = EXCLUDED.acl"#,
            &[
                p(&bucket.to_string()),
                p(&key.to_string()),
                p(&seq),
                p(&id),
                p(&blob),
                p(&size),
                p(&etag),
                p(&created),
                p(&stamp),
                p(&kind),
                p(&tags),
                p(&checksum),
                p(&owner),
                p(&grants),
            ],
        )
        .await?;
        // A re-put can change the id stored at a sequence; the old id's index
        // row would otherwise keep answering a sequence whose row now carries
        // another id (the row and its id index must not disagree).
        tx.execute(
            r#"DELETE FROM version_index
               WHERE bucket = $1 AND key = $2 AND seq = $3 AND id <> $4"#,
            &[p(&bucket.to_string()), p(&key.to_string()), p(&seq), p(&id)],
        )
        .await?;
        tx.execute(
            r#"INSERT INTO version_index (bucket, key, id, seq) VALUES ($1, $2, $3, $4)
               ON CONFLICT (bucket, key, id) DO UPDATE SET seq = EXCLUDED.seq"#,
            &[p(&bucket.to_string()), p(&key.to_string()), p(&id), p(&seq)],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn get_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<Option<Stored>, Error> {
        let seq = seq as i64;
        let client = self.conn().await;
        let row = client
            .query_opt(
                r#"SELECT id, blob, size, etag, created, became_noncurrent, kind,
                          tags, checksum, owner, acl
                   FROM version_rows WHERE bucket = $1 AND key = $2 AND seq = $3"#,
                &[p(&bucket.to_string()), p(&key.to_string()), p(&seq)],
            )
            .await?;
        Ok(row.and_then(|row| decode_version(&row, 0, seq as u64)))
    }

    async fn latest(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<(u64, Stored)>, Error> {
        let client = self.conn().await;
        let row = client
            .query_opt(
                r#"SELECT seq, id, blob, size, etag, created, became_noncurrent, kind,
                          tags, checksum, owner, acl
                   FROM version_rows WHERE bucket = $1 AND key = $2
                   ORDER BY seq DESC LIMIT 1"#,
                &[p(&bucket.to_string()), p(&key.to_string())],
            )
            .await?;
        // The newest row's decode is the read: a corrupt newest row answers
        // absent, never an older version.
        Ok(row.and_then(|row| {
            let seq = row.get::<_, i64>(0) as u64;
            decode_version(&row, 1, seq).map(|stored| (seq, stored))
        }))
    }

    async fn bucket_rows(&self, bucket: &bucket::Name) -> Result<Vec<Stored>, Error> {
        let client = self.conn().await;
        let rows = client
            .query(
                r#"SELECT seq, id, blob, size, etag, created, became_noncurrent, kind,
                          tags, checksum, owner, acl
                   FROM version_rows WHERE bucket = $1 ORDER BY seq"#,
                &[p(&bucket.to_string())],
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(|row| {
                let seq = row.get::<_, i64>(0) as u64;
                decode_version(row, 1, seq)
            })
            .collect())
    }

    async fn list_versions(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<Stored>, Error> {
        let client = self.conn().await;
        let rows = client
            .query(
                r#"SELECT seq, id, blob, size, etag, created, became_noncurrent, kind,
                          tags, checksum, owner, acl
                   FROM version_rows WHERE bucket = $1 AND key = $2 ORDER BY seq DESC"#,
                &[p(&bucket.to_string()), p(&key.to_string())],
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(|row| {
                let seq = row.get::<_, i64>(0) as u64;
                decode_version(row, 1, seq)
            })
            .collect())
    }

    async fn expire(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), Error> {
        let seq = seq as i64;
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let removed = tx
            .query_opt(
                r#"DELETE FROM version_rows WHERE bucket = $1 AND key = $2 AND seq = $3
                   RETURNING id"#,
                &[p(&bucket.to_string()), p(&key.to_string()), p(&seq)],
            )
            .await?;
        if let Some(row) = removed {
            let id: String = row.get(0);
            // The index entry goes only when it still points at **this**
            // sequence: two rows can share one identity wire (the null version),
            // and the index names only the newest of them, so expiring an older
            // row must not drop the entry a newer row installed and leave that
            // version unaddressable. Mirrors redb's `remove_pointing_at`.
            tx.execute(
                r#"DELETE FROM version_index
                   WHERE bucket = $1 AND key = $2 AND id = $3 AND seq = $4"#,
                &[
                    p(&bucket.to_string()),
                    p(&key.to_string()),
                    p(&id),
                    p(&seq),
                ],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn expire_many(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        seqs: &[u64],
    ) -> Result<(), Error> {
        if seqs.is_empty() {
            return Ok(());
        }
        let seqs: Vec<i64> = seqs.iter().map(|seq| *seq as i64).collect();
        let mut client = self.conn().await;
        // One transaction for the batch: the sweep's crash point stays one per
        // sweep, never one per version.
        let tx = client.transaction().await?;
        let removed = tx
            .query(
                r#"DELETE FROM version_rows
                   WHERE bucket = $1 AND key = $2 AND seq = ANY($3)
                   RETURNING id, seq"#,
                &[p(&bucket.to_string()), p(&key.to_string()), p(&seqs)],
            )
            .await?;
        for row in &removed {
            let id: String = row.get(0);
            let expired: i64 = row.get(1);
            // The index entry goes only when it still points at the sequence
            // being expired (the `remove_pointing_at` rule): the row's own
            // returned sequence is the guard, so an older row's expiry never
            // drops the entry a newer row installed.
            tx.execute(
                r#"DELETE FROM version_index
                   WHERE bucket = $1 AND key = $2 AND id = $3 AND seq = $4"#,
                &[
                    p(&bucket.to_string()),
                    p(&key.to_string()),
                    p(&id),
                    p(&expired),
                ],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let mut client = self.conn().await;
        // One transaction for the whole bucket: the version rows and their
        // index entries retire together, so the index can never be left naming
        // a row the drain removed (a crash leaves every row or none).
        let tx = client.transaction().await?;
        tx.execute(
            r#"DELETE FROM version_rows WHERE bucket = $1"#,
            &[p(&bucket.to_string())],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM version_index WHERE bucket = $1"#,
            &[p(&bucket.to_string())],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn version_seq_of(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
    ) -> Result<Option<u64>, Error> {
        let wire = version::wire_of(id);
        let client = self.conn().await;
        let row = client
            .query_opt(
                r#"SELECT seq FROM version_index WHERE bucket = $1 AND key = $2 AND id = $3"#,
                &[p(&bucket.to_string()), p(&key.to_string()), p(&wire)],
            )
            .await?;
        Ok(row.map(|row| row.get::<_, i64>(0) as u64))
    }
}

/// A staged write: the chunks buffered in memory until publish (the staging
/// area is the implementation's own, D38).
#[derive(Default)]
pub struct PgStaged {
    chunks: Vec<Bytes>,
}

impl PgStaged {
    fn into_bytes(self) -> Vec<u8> {
        let mut out = Vec::new();
        for chunk in self.chunks {
            out.extend_from_slice(&chunk);
        }
        out
    }
}

#[async_trait]
impl StagedObject for PgStaged {
    type Error = Error;

    async fn write(&mut self, chunk: Bytes) -> Result<(), Error> {
        self.chunks.push(chunk);
        Ok(())
    }
}

#[async_trait]
impl ObjectContent for PgStore {
    type Error = Error;
    type Staged = PgStaged;

    async fn stage(&self, _bucket: &bucket::Name, _key: &ContentKey) -> Result<PgStaged, Error> {
        Ok(PgStaged::default())
    }

    async fn publish(
        &self,
        bucket: &bucket::Name,
        staged: PgStaged,
        key: &ContentKey,
    ) -> Result<(), Error> {
        let (kind, ckey) = content_key(key);
        let data = staged.into_bytes();
        let len = data.len() as i64;
        let plan = publish_plan(data.len() as u64);
        let client = self.conn().await;
        // The generation is allocated before any chunk is written; until the
        // visibility flip it is invisible (the incompleteness marker).
        let seq: i64 = client
            .query_one(
                ALLOC_HEAD_SQL,
                &[p(&bucket.to_string()), p(&kind), p(&ckey)],
            )
            .await?
            .get(0);
        for batch_start in (0..plan.chunks).step_by(PUBLISH_BATCH_CHUNKS) {
            let batch_end = (batch_start + PUBLISH_BATCH_CHUNKS).min(plan.chunks);
            let mut numbers: Vec<i32> = Vec::with_capacity(batch_end - batch_start);
            let mut chunks: Vec<Vec<u8>> = Vec::with_capacity(batch_end - batch_start);
            for index in batch_start..batch_end {
                let start = index * CONTENT_CHUNK_BYTES;
                let end = (start + CONTENT_CHUNK_BYTES).min(data.len());
                numbers.push(index as i32);
                chunks.push(data[start..end].to_vec());
            }
            client
                .execute(
                    r#"INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk)
                       SELECT $1, $2, $3, $4, u.n, u.c
                       FROM unnest($5::int[], $6::bytea[]) AS u(n, c)
                       ON CONFLICT (bucket, kind, ckey, seq, chunk_no) DO UPDATE
                           SET chunk = EXCLUDED.chunk"#,
                    &[
                        p(&bucket.to_string()),
                        p(&kind),
                        p(&ckey),
                        p(&seq),
                        p(&numbers),
                        p(&chunks),
                    ],
                )
                .await?;
        }
        let published = client
            .execute(
                PUBLISH_HEAD_SQL,
                &[p(&bucket.to_string()), p(&kind), p(&ckey), p(&seq), p(&len)],
            )
            .await?;
        if published == 0 {
            return Err(Error::PublishRaced);
        }
        Ok(())
    }

    async fn open(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Error> {
        let (kind, ckey) = content_key(key);
        let client = self.conn().await;
        let head = client
            .query_opt(
                r#"SELECT live_seq, live_len FROM content_heads
                   WHERE bucket = $1 AND kind = $2 AND ckey = $3 AND live_seq IS NOT NULL"#,
                &[p(&bucket.to_string()), p(&kind), p(&ckey)],
            )
            .await?;
        let Some(head) = head else {
            return Err(Error::MissingContent(key.clone()));
        };
        let seq: i64 = head.get(0);
        let len = head.get::<_, i64>(1) as u64;
        if len == 0 {
            return Ok(body(Vec::new()));
        }
        let (start, end) = match range {
            None => (0, len - 1),
            Some(range) => range
                .resolve(len)
                .map_err(|_| Error::InvalidRange { range, size: len })?,
        };
        let first = (start / CONTENT_CHUNK_BYTES as u64) as i32;
        let last = (end / CONTENT_CHUNK_BYTES as u64) as i32;
        let rows = client
            .query(
                r#"SELECT chunk_no, chunk FROM content
                   WHERE bucket = $1 AND kind = $2 AND ckey = $3 AND seq = $4
                     AND chunk_no BETWEEN $5 AND $6
                   ORDER BY chunk_no"#,
                &[
                    p(&bucket.to_string()),
                    p(&kind),
                    p(&ckey),
                    p(&seq),
                    p(&first),
                    p(&last),
                ],
            )
            .await?;
        let mut buffer: Vec<u8> = Vec::with_capacity((end - start + 1) as usize);
        let mut expected = first;
        for row in rows {
            let chunk_no: i32 = row.get(0);
            // A gap or a foreign row would desynchronize every later byte: a
            // generation that cannot be read as a contiguous run must surface as
            // a typed error, never as an index panic.
            if chunk_no != expected {
                return Err(Self::corrupt_content(bucket, &ckey));
            }
            expected += 1;
            buffer.extend_from_slice(&row.get::<_, Vec<u8>>(1));
        }
        let base = first as u64 * CONTENT_CHUNK_BYTES as u64;
        let lo = (start - base) as usize;
        let hi = (end - base) as usize;
        if expected != last + 1 || hi >= buffer.len() {
            return Err(Self::corrupt_content(bucket, &ckey));
        }
        Ok(body(buffer[lo..=hi].to_vec()))
    }

    async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Error> {
        let (kind, ckey) = content_key(key);
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        tx.execute(
            r#"DELETE FROM content WHERE bucket = $1 AND kind = $2 AND ckey = $3"#,
            &[p(&bucket.to_string()), p(&kind), p(&ckey)],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM content_heads WHERE bucket = $1 AND kind = $2 AND ckey = $3"#,
            &[p(&bucket.to_string()), p(&kind), p(&ckey)],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn rename(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Error> {
        if from == to {
            return Ok(());
        }
        let from_ckey = object_ckey(from);
        let to_ckey = object_ckey(to);
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let source = tx
            .query_opt(
                r#"SELECT live_seq FROM content_heads
                   WHERE bucket = $1 AND kind = 'object' AND ckey = $2 AND live_seq IS NOT NULL"#,
                &[p(&bucket.to_string()), p(&from_ckey)],
            )
            .await?;
        if source.is_none() {
            tx.rollback().await?;
            return Err(Error::NoSuchKey(from.clone()));
        }
        tx.execute(
            r#"DELETE FROM content WHERE bucket = $1 AND kind = 'object' AND ckey = $2"#,
            &[p(&bucket.to_string()), p(&to_ckey)],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM content_heads WHERE bucket = $1 AND kind = 'object' AND ckey = $2"#,
            &[p(&bucket.to_string()), p(&to_ckey)],
        )
        .await?;
        tx.execute(
            r#"UPDATE content SET ckey = $3
               WHERE bucket = $1 AND kind = 'object' AND ckey = $2"#,
            &[p(&bucket.to_string()), p(&from_ckey), p(&to_ckey)],
        )
        .await?;
        tx.execute(
            r#"UPDATE content_heads SET ckey = $3
               WHERE bucket = $1 AND kind = 'object' AND ckey = $2"#,
            &[p(&bucket.to_string()), p(&from_ckey), p(&to_ckey)],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn copy(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Error> {
        if from == to {
            return Ok(());
        }
        let from_ckey = object_ckey(from);
        let to_ckey = object_ckey(to);
        let mut client = self.conn().await;
        let tx = client.transaction().await?;
        let source = tx
            .query_opt(
                r#"SELECT live_seq, live_len FROM content_heads
                   WHERE bucket = $1 AND kind = 'object' AND ckey = $2 AND live_seq IS NOT NULL"#,
                &[p(&bucket.to_string()), p(&from_ckey)],
            )
            .await?;
        let Some(source) = source else {
            tx.rollback().await?;
            return Err(Error::NoSuchKey(from.clone()));
        };
        let source_seq: i64 = source.get(0);
        let len: i64 = source.get(1);
        tx.execute(
            r#"DELETE FROM content WHERE bucket = $1 AND kind = 'object' AND ckey = $2"#,
            &[p(&bucket.to_string()), p(&to_ckey)],
        )
        .await?;
        tx.execute(
            r#"DELETE FROM content_heads WHERE bucket = $1 AND kind = 'object' AND ckey = $2"#,
            &[p(&bucket.to_string()), p(&to_ckey)],
        )
        .await?;
        let seq: i64 = tx
            .query_one(
                ALLOC_HEAD_SQL,
                &[p(&bucket.to_string()), p(&"object"), p(&to_ckey)],
            )
            .await?
            .get(0);
        tx.execute(
            r#"INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk)
               SELECT bucket, kind, $1, $2, chunk_no, chunk FROM content
               WHERE bucket = $3 AND kind = $4 AND ckey = $5 AND seq = $6"#,
            &[
                p(&to_ckey),
                p(&seq),
                p(&bucket.to_string()),
                p(&"object"),
                p(&from_ckey),
                p(&source_seq),
            ],
        )
        .await?;
        tx.execute(
            PUBLISH_HEAD_SQL,
            &[
                p(&bucket.to_string()),
                p(&"object"),
                p(&to_ckey),
                p(&seq),
                p(&len),
            ],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn scan(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&object::Key>,
        limit: usize,
    ) -> Result<Vec<ContentEntry>, Error> {
        let lower = format!("o:{prefix}");
        let upper = after.map(|key| format!("o:{key}"));
        let limit = limit as i64;
        let client = self.conn().await;
        let rows = client
            .query(
                r#"SELECT ckey, live_len FROM content_heads
                   WHERE bucket = $1 AND kind = 'object' AND live_seq IS NOT NULL
                     AND starts_with(ckey, $2)
                     AND ($3::text IS NULL OR ckey > $3)
                   ORDER BY ckey COLLATE "C" LIMIT $4"#,
                &[p(&bucket.to_string()), p(&lower), p(&upper), p(&limit)],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|row| {
                let ckey: String = row.get(0);
                ContentEntry {
                    key: scan_key(&ckey),
                    len: row.get::<_, i64>(1) as u64,
                    identity: 0,
                    mtime: None,
                }
            })
            .collect())
    }

    async fn stat(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<ContentEntry>, Error> {
        let ckey = object_ckey(key);
        let client = self.conn().await;
        let row = client
            .query_opt(
                r#"SELECT live_len FROM content_heads
                   WHERE bucket = $1 AND kind = 'object' AND ckey = $2 AND live_seq IS NOT NULL"#,
                &[p(&bucket.to_string()), p(&ckey)],
            )
            .await?;
        Ok(row.map(|row| ContentEntry {
            key: key.clone(),
            len: row.get::<_, i64>(0) as u64,
            identity: 0,
            mtime: None,
        }))
    }

    async fn create_bucket(&self, _bucket: &bucket::Name) -> Result<(), Error> {
        // A content namespace with no directory: nothing to create. The
        // metadata plane's bucket row is BucketMeta::create.
        Ok(())
    }

    async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        // One statement: the emptiness decision and both deletes read the same
        // snapshot, so a writer landing between a check and the delete cannot
        // lose bytes. The data-modifying CTEs run even though only `empty` is
        // referenced (as in `put_guarded`).
        let client = self.conn().await;
        let row = client
            .query_one(
                r#"WITH empty AS (
                       SELECT NOT EXISTS (
                           SELECT 1 FROM content_heads
                           WHERE bucket = $1 AND live_seq IS NOT NULL
                       ) AS ok
                   ),
                   del_heads AS (
                       DELETE FROM content_heads
                       WHERE bucket = $1 AND (SELECT ok FROM empty)
                       RETURNING 1
                   ),
                   del_chunks AS (
                       DELETE FROM content
                       WHERE bucket = $1 AND (SELECT ok FROM empty)
                       RETURNING 1
                   )
                   SELECT ok FROM empty"#,
                &[p(&bucket.to_string())],
            )
            .await?;
        let empty: bool = row.get(0);
        if !empty {
            return Err(Error::NotEmpty(bucket.clone()));
        }
        Ok(())
    }

    async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let client = self.conn().await;
        let row = client
            .query_one(
                r#"SELECT NOT EXISTS (
                       SELECT 1 FROM content_heads WHERE bucket = $1 AND live_seq IS NOT NULL
                   )"#,
                &[p(&bucket.to_string())],
            )
            .await?;
        Ok(row.get(0))
    }
}
