//! The PostgreSQL cold plane: its connection, its ownership and the root
//! identity the database records (T18).
//!
//! [`connect`] mirrors [`PgStorage::connect`](tinio_pg::PgStorage::connect):
//! the access mode picks the pool's session mode, and a writable connection
//! takes the store's ownership lock and keeps its re-verify alive for the
//! process's life, so a second writable instance is refused before it can
//! serve. The database is **dedicated** to one tier root; sharing it across
//! roots is unsupported.
//!
//! The root identity lives in the dedicated reserved namespace
//! [`ROOT_NAMESPACE`], which no S3 operation can name: [`is_reserved`] is the
//! predicate every operation, GC pass and repair pass uses to exclude it. The
//! warm root that already holds a bucket of that name cannot be converted to
//! tier mode at all.
use std::io;

use futures::stream;
use secrecy::ExposeSecret;

use crate::{
    _core::{
        AccessMode, bucket,
        content::{ContentKey, ObjectContent, stage_body},
        meta::BucketMeta,
        object, storage,
    },
    _pg,
    _store_pg::{Access, pool::Pool, store::OWNERSHIP_RECHECK},
    ColdStore, Error,
};

/// The reserved namespace the root identity lives under.
///
/// Every S3 operation on this bucket name is refused; GC and repair exclude
/// it; and a warm root that already holds a bucket by this name refuses the
/// conversion to tier mode.
///
/// # Examples
///
/// ```rust
/// use tinio_tier::pg::ROOT_NAMESPACE;
///
/// assert_eq!(ROOT_NAMESPACE, "tinio-tier-root");
/// ```
pub const ROOT_NAMESPACE: &str = "tinio-tier-root";

/// The reserved namespace's object key holding the 16-byte root identity.
const ROOT_KEY: &str = "root-id";

/// The length of the recorded root identity: a UUID's 16 bytes.
const ROOT_ID_BYTES: u64 = 16;

/// Open the cold plane for `config` in `mode`.
///
/// A writable connection owns the database: it takes the ownership lock
/// before the store is returned and spawns the periodic re-verify, exactly as
/// [`PgStorage::connect`](tinio_pg::PgStorage::connect) does, so a second
/// writable instance is refused. A read-only connection takes no lock, never
/// migrates, and may serve beside the owner.
///
/// # Errors
///
/// [`Error::Cold`] when the pool cannot be opened, the schema gate refuses
/// the database, or the ownership lock is held by another session.
///
/// # Examples
///
/// An unreachable server is an error, never a panic or a silently empty
/// plane:
///
/// ```rust
/// use tinio_core::AccessMode;
/// use tinio_pg::Config;
/// use tinio_tier::{Error, pg};
/// use tinio_util::secret::{Secret, SecretKey};
/// use tokio::runtime::Runtime;
///
/// let config = Config::new(
///     Secret::from(SecretKey::from(
///         "postgres://tinio:tinio@127.0.0.1:1/tinio?sslmode=disable".to_owned(),
///     )),
///     1,
/// );
/// let refused = Runtime::new()
///     .expect("a runtime")
///     .block_on(pg::connect(&config, AccessMode::ReadWrite))
///     .expect_err("no server listens there");
/// assert!(matches!(refused, Error::Cold(_)));
/// ```
pub async fn connect(config: &_pg::Config, mode: AccessMode) -> Result<ColdStore, Error> {
    let access = match mode {
        AccessMode::ReadWrite => Access::ReadWrite,
        AccessMode::ReadOnly => Access::ReadOnly,
    };
    let dsn = config.dsn().expose_secret();
    let pool = if access.is_writable() {
        Pool::open(dsn.as_ref(), config.pool())
    } else {
        Pool::open_read_only(dsn.as_ref(), config.pool())
    }
    .map_err(Error::Cold)?;
    let store = ColdStore::connect_with_pool(pool, access)
        .await
        .map_err(Error::Cold)?;
    if access.is_writable() {
        // The lock is taken before the store is handed out, so a second
        // writable instance's refusal is this construction's answer. The
        // re-verify consumes the guard and never returns while the lock holds;
        // the store keeps a handle too, so the lock lives as long as anything
        // is left to serve.
        let guard = store.hold_ownership().await.map_err(Error::Cold)?;
        tokio::spawn(guard.verify_forever(OWNERSHIP_RECHECK));
    }
    Ok(store)
}

/// Whether `name` is the reserved namespace (T18).
///
/// The predicate every S3 operation, GC pass and repair pass reads: nothing
/// in the reserved namespace is ever an S3-visible bucket or object.
///
/// # Examples
///
/// ```rust
/// use tinio_core::bucket;
/// use tinio_tier::pg;
///
/// let reserved = bucket::name(pg::ROOT_NAMESPACE).expect("a valid bucket name");
/// assert!(pg::is_reserved(&reserved));
/// assert!(!pg::is_reserved(
///     &bucket::name("data").expect("a valid bucket name")
/// ));
/// ```
pub fn is_reserved(name: &bucket::Name) -> bool {
    name.as_ref() == ROOT_NAMESPACE
}

/// The root identity `store` records, or `None` when the reserved namespace
/// holds no marker.
pub(crate) async fn root_id(store: &ColdStore) -> Result<Option<[u8; 16]>, Error> {
    let name = reserved_name()?;
    let key = root_key()?;
    let Some(entry) = store.stat(&name, &key).await.map_err(Error::Cold)? else {
        return Ok(None);
    };
    if entry.len != ROOT_ID_BYTES {
        return Err(Error::CorruptRootId { len: entry.len });
    }
    let body = store
        .open(&name, &ContentKey::Object(key), None)
        .await
        .map_err(Error::Cold)?;
    let bytes = storage::collect_body(body).await?;
    let raw: &[u8] = bytes.as_ref();
    let id = raw.try_into().map_err(|_| Error::CorruptRootId {
        len: bytes.len() as u64,
    })?;
    Ok(Some(id))
}

/// Record `id` in `store`'s reserved namespace.
///
/// The namespace is created when absent; a later record replaces the marker
/// (the adopt path writes an identity only once per binding).
pub(crate) async fn record_root(store: &ColdStore, id: [u8; 16]) -> Result<(), Error> {
    let name = reserved_name()?;
    let key = root_key()?;
    store.create_bucket(&name).await.map_err(Error::Cold)?;
    let content = ContentKey::Object(key);
    let mut staged = store.stage(&name, &content).await.map_err(Error::Cold)?;
    let body: storage::BodyStream = Box::pin(stream::once(async move {
        Ok::<_, io::Error>(bytes::Bytes::copy_from_slice(&id))
    }));
    stage_body(&mut staged, body).await?;
    store
        .publish(&name, staged, &content)
        .await
        .map_err(Error::Cold)?;
    Ok(())
}

/// Whether `store` holds no bucket at all — the state a fresh index may bind
/// without `--adopt`.
pub(crate) async fn is_empty(store: &ColdStore) -> Result<bool, Error> {
    let buckets = store.load_all().await.map_err(Error::Cold)?;
    Ok(buckets.is_empty())
}

/// The reserved namespace as a validated bucket name.
fn reserved_name() -> Result<bucket::Name, Error> {
    bucket::name(ROOT_NAMESPACE).map_err(Error::Storage)
}

/// The root marker's validated object key.
fn root_key() -> Result<object::Key, Error> {
    object::key(ROOT_KEY).map_err(Error::Storage)
}
