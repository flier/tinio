//! The redb `ObjectContent` ([`RedbContent`]) — Task 8's implementation.
//!
//! The byte plane is the two shared content tables — `objects`
//! (`(bucket, key)` → bytes) and `part_data` (`(bucket, upload_id,
//! part_number)` → bytes) — the tables tinio-mem's content plane also
//! writes. The bucket "namespace" is implicit in the key prefix, so
//! [`ObjectContent::create_bucket`] records nothing and
//! [`ObjectContent::remove_bucket`] counts those two tables' rows before it
//! removes anything (the contract's `NotEmpty` refusal: this plane *can*
//! count, so it must not lose bytes).
//!
//! A write is staged in the handle's own buffer and
//! [`ObjectContent::publish`] inserts the whole value in one transaction —
//! the visibility point, and the reason a staged write is invisible by
//! construction. A walk yields object keys only (`objects` holds nothing
//! else), ascending, **inside the prefix**, with `after` a filter over that
//! range rather than a walk start: a value sorting below the prefix cannot
//! shorten the page (S3's `start-after` is any client key). An entry's
//! `identity` is `0` and its `mtime` is `None`: a table row knows neither
//! (D42 — a store without a timestamp must not fabricate one), and the two
//! reads answer the same pair.
//!
//! Staging buffers in memory and never refuses a size (D38's staging area is
//! the implementation's own; this plane stores one value per key), and a read
//! copies the whole value before it windows it — the caller's size discipline
//! is this plane's only bound.

use std::{ops::Bound, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use redb::{ReadTransaction, ReadableTable, WriteTransaction};
use tracing::{Span, field, instrument};

use crate::{
    _core::{
        bucket,
        content::{ContentEntry, ContentKey, ObjectContent, StagedObject},
        object,
        storage::{self, BodyStream, ByteRange},
    },
    Driver, Error,
    objects::{self, Table as ObjectsTable},
    part_data::{self, Table as PartDataTable},
    scan, store,
    table::{self, TableDef},
};

/// The staged write of the redb content plane: the body chunks buffered
/// until [`ObjectContent::publish`] inserts them as one value.
///
/// redb's own value ceiling is the only size rule on this plane, and it
/// answers at publish (a redb failure), never at `write`.
#[derive(Debug, Default)]
pub struct RedbStaged {
    bytes: Vec<u8>,
}

#[async_trait]
impl StagedObject for RedbStaged {
    type Error = Error;

    async fn write(&mut self, chunk: Bytes) -> Result<(), Error> {
        self.bytes.extend_from_slice(&chunk);
        Ok(())
    }
}

/// The redb content plane: the whole D23 surface over one [`Driver`].
///
/// It is a type of its own rather than a method set on
/// [`crate::RedbMetadata`]: the bytes side carries none of the metadata
/// categories, and its two tables are the backend's own (they are not among
/// the shared schema's ten). A backend or the engine composes the plane with
/// wrappers of its own — `CountingContent<RedbContent>` — so the name is
/// part of this crate's surface.
///
/// # Examples
///
/// ```rust
/// use tinio_store_redb::RedbContent;
///
/// let content = RedbContent::in_memory().unwrap();
/// // The handle a backend shares with its metadata implementation.
/// let driver = content.driver();
/// assert!(std::sync::Arc::strong_count(&driver) >= 2);
/// ```
#[derive(Debug, Clone)]
pub struct RedbContent {
    /// The driver this plane runs its transactions through.
    driver: Arc<Driver>,
}

impl RedbContent {
    /// Wrap a driver the caller already owns — the backend path, where one
    /// state database serves the metadata rows and the bytes.
    pub fn from_driver(driver: Arc<Driver>) -> Self {
        Self { driver }
    }

    /// A fresh in-memory plane. Nothing is ensured up front: a database with
    /// no content table holds no content, and the two tables are created by
    /// the first write.
    pub fn in_memory() -> Result<Self, Error> {
        Ok(Self::from_driver(Driver::new(store::open_in_memory()?)))
    }

    /// The driver this plane runs its transactions through.
    pub fn driver(&self) -> Arc<Driver> {
        Arc::clone(&self.driver)
    }
}

/// The range window of `bytes`, resolved to an owned slice —
/// [`ByteRange::resolve`] is the contract's own rule, and its refusal
/// projects verbatim (D22).
fn window(bytes: Vec<u8>, range: Option<ByteRange>) -> Result<Vec<u8>, Error> {
    let Some(range) = range else {
        return Ok(bytes);
    };
    let (start, end) = range.resolve(bytes.len() as u64).map_err(Error::Refused)?;
    Ok(bytes[start as usize..=end as usize].to_vec())
}

/// The one-chunk body a published value answers with.
fn body(bytes: Vec<u8>) -> BodyStream {
    Box::pin(stream::iter([Ok(Bytes::from(bytes))]))
}

/// The move/copy write body: one transaction that reads `from`'s value out of
/// the `objects` table and writes it at `to` — the whole of
/// [`ObjectContent::copy`] and all of [`ObjectContent::rename`] but the source
/// removal (`rename` adds the remove of `from` in the same transaction, so the
/// move stays one unit). A source with no bytes is refused (`NoSuchKey`,
/// D22) before anything is written, so a move or copy that moved nothing is
/// recorded as nothing.
fn move_value(
    txn: &mut WriteTransaction,
    bucket: &str,
    from: &str,
    to: &str,
    named: &object::Key,
    remove_source: bool,
) -> Result<(), Error> {
    let mut table = ObjectsTable::open(txn)?;
    let bytes = {
        let Some(guard) = table.get(bucket, from)? else {
            return Err(Error::Refused(storage::no_such_key(named)));
        };
        guard.value().to_vec()
    };
    table.put(bucket, to, &bytes)?;
    if remove_source {
        table.remove(bucket, from)?;
    }
    Ok(())
}

/// Open a content table for reading, answering a database that has none as
/// "no content": the two content tables are the backend's own (they are not
/// among the shared schema's ten), so a root that never stored object or part
/// bytes legitimately has neither — the table layer's "a base table's absence
/// is the redb error it is" rule is the *metadata* tables'.
type ReadTable<'txn, D> =
    table::Table<'txn, D, redb::ReadOnlyTable<<D as TableDef>::Key, <D as TableDef>::Value>>;

fn read_table<D: TableDef>(txn: &ReadTransaction) -> Result<Option<ReadTable<'_, D>>, Error> {
    match txn.open_table(D::DEF) {
        Ok(table) => Ok(Some(table::Table::from_table(table))),
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Whether the objects table holds a row of `bucket` — one key-range seek,
/// never a walk.
fn objects_present<T>(table: &T, bucket: &str) -> Result<bool, Error>
where
    T: ReadableTable<(&'static str, &'static str), &'static [u8]>,
{
    scan::has_prefix_pair::<_, _, Error>(table, (bucket, ""), |name, _| name == bucket)
}

/// Whether the part-data table holds a row of `bucket` — the triple-key
/// counterpart of [`objects_present`].
fn parts_present<T>(table: &T, bucket: &str) -> Result<bool, Error>
where
    T: ReadableTable<(&'static str, &'static str, u32), &'static [u8]>,
{
    scan::has_prefix_triple::<_, _, Error>(table, (bucket, "", 0), |name, _, _| name == bucket)
}

/// Whether the bucket holds any content this plane counts — an object row or
/// a part row.
fn has_bytes(txn: &ReadTransaction, bucket: &str) -> Result<bool, Error> {
    let Some(objects) = read_table::<objects::Def>(txn)? else {
        return Ok(false);
    };
    if objects_present(&objects.0, bucket)? {
        return Ok(true);
    }
    let Some(parts) = read_table::<part_data::Def>(txn)? else {
        return Ok(false);
    };
    parts_present(&parts.0, bucket)
}

/// [`has_bytes`] inside a write transaction — the removal's own, so no
/// concurrent write can slip between the probe and the decision.
fn has_bytes_writable(txn: &mut WriteTransaction, bucket: &str) -> Result<bool, Error> {
    // One table handle per statement: the write transaction lends each table
    // mutably, so the first probe's handle must be gone before the second
    // opens.
    if objects_present(&ObjectsTable::open(txn)?.0, bucket)? {
        return Ok(true);
    }
    parts_present(&PartDataTable::open(txn)?.0, bucket)
}

#[async_trait]
impl ObjectContent for RedbContent {
    type Error = Error;
    type Staged = RedbStaged;

    async fn stage(&self, _bucket: &bucket::Name, _key: &ContentKey) -> Result<RedbStaged, Error> {
        // The staging area is this handle's buffer: nothing reaches a table
        // before publish, so a half-written object cannot be observed.
        Ok(RedbStaged::default())
    }

    #[instrument(
        target = "tinio::storage",
        level = "info",
        skip(self, staged),
        fields(
            bucket = %bucket,
            kind = key.kind_label(),
            bytes = staged.bytes.len(),
            key = field::Empty,
        )
    )]
    async fn publish(
        &self,
        bucket: &bucket::Name,
        staged: RedbStaged,
        key: &ContentKey,
    ) -> Result<(), Error> {
        // The visibility point of a staged write: one info span per publish,
        // with the kind and the byte count the commit carries.
        if let Some(object) = key.object() {
            Span::current().record("key", field::display(object));
        }
        let bucket = bucket.to_string();
        let bytes = staged.bytes;
        match key {
            ContentKey::Object(key) => {
                let key = key.to_string();
                self.driver
                    .write(move |txn| ObjectsTable::open(txn)?.put(&bucket, &key, &bytes))
                    .await
            }
            ContentKey::Part { upload_id, number } => {
                let upload_id = upload_id.clone();
                let number = u32::from(*number);
                self.driver
                    .write(move |txn| {
                        PartDataTable::open(txn)?.put(&bucket, &upload_id, number, &bytes)
                    })
                    .await
            }
        }
    }

    #[instrument(
        target = "tinio::storage",
        level = "debug",
        skip(self),
        fields(
            bucket = %bucket,
            kind = key.kind_label(),
            key = field::Empty,
        )
    )]
    async fn open(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Error> {
        // A read is a T1 unit inside the request's T0 span: it never commits,
        // so it stays out of the default info filter.
        if let Some(object) = key.object() {
            Span::current().record("key", field::display(object));
        }
        let bucket = bucket.to_string();
        let bytes = match key {
            ContentKey::Object(key) => {
                let named = key.clone();
                let stored = key.to_string();
                self.driver.read(move |txn| {
                    let Some(table) = read_table::<objects::Def>(txn)? else {
                        return Err(Error::Refused(storage::no_such_key(&named)));
                    };
                    let Some(guard) = table.get(&bucket, &stored)? else {
                        // A missing key is an error: D23's pinned return has
                        // no `None` arm, and the projection is `NoSuchKey`.
                        return Err(Error::Refused(storage::no_such_key(&named)));
                    };
                    Ok(guard.value().to_vec())
                })?
            }
            ContentKey::Part { upload_id, number } => {
                let upload_id = upload_id.clone();
                let number = u32::from(*number);
                self.driver.read(move |txn| {
                    let Some(table) = read_table::<part_data::Def>(txn)? else {
                        return Err(Error::Refused(storage::invalid_part(number)));
                    };
                    let Some(guard) = table.get(&bucket, &upload_id, number)? else {
                        return Err(Error::Refused(storage::invalid_part(number)));
                    };
                    Ok(guard.value().to_vec())
                })?
            }
        };
        Ok(body(window(bytes, range)?))
    }

    async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Error> {
        let bucket = bucket.to_string();
        match key {
            ContentKey::Object(key) => {
                let key = key.to_string();
                self.driver
                    .write(move |txn| ObjectsTable::open(txn)?.remove(&bucket, &key))
                    .await
            }
            ContentKey::Part { upload_id, number } => {
                let upload_id = upload_id.clone();
                let number = u32::from(*number);
                self.driver
                    .write(move |txn| PartDataTable::open(txn)?.remove(&bucket, &upload_id, number))
                    .await
            }
        }
    }

    async fn remove_many(&self, bucket: &bucket::Name, keys: &[ContentKey]) -> Result<(), Error> {
        // One commit for the batch: the rows that named these bytes are
        // already gone, so a batch that stopped half-way would leave bytes
        // nothing can reach.
        let bucket = bucket.to_string();
        let keys = keys.to_vec();
        self.driver
            .write(move |txn| {
                for key in &keys {
                    match key {
                        ContentKey::Object(key) => {
                            let key = key.to_string();
                            ObjectsTable::open(txn)?.remove(&bucket, &key)?;
                        }
                        ContentKey::Part { upload_id, number } => {
                            PartDataTable::open(txn)?.remove(
                                &bucket,
                                upload_id,
                                u32::from(*number),
                            )?;
                        }
                    }
                }
                Ok::<(), Error>(())
            })
            .await
    }

    async fn remove_if_matches(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        expected: &[u8],
    ) -> Result<bool, Error> {
        // The read, the comparison and the removal share one commit: the
        // writer this guard exists for publishes between them, and a
        // compare outside the write transaction would let it slip into the
        // window and lose the bytes it just published.
        let bucket = bucket.to_string();
        let key = key.clone();
        let expected = expected.to_vec();
        self.driver
            .write(move |txn| {
                let matches = match &key {
                    ContentKey::Object(key) => {
                        let key = key.to_string();
                        let table = ObjectsTable::open(txn)?;
                        let stored = table.get(&bucket, &key)?;
                        stored.is_some_and(|guard| guard.value() == expected.as_slice())
                    }
                    ContentKey::Part { upload_id, number } => {
                        let table = PartDataTable::open(txn)?;
                        let stored = table.get(&bucket, upload_id, u32::from(*number))?;
                        stored.is_some_and(|guard| guard.value() == expected.as_slice())
                    }
                };
                if matches {
                    match &key {
                        ContentKey::Object(key) => {
                            let key = key.to_string();
                            ObjectsTable::open(txn)?.remove(&bucket, &key)?;
                        }
                        ContentKey::Part { upload_id, number } => {
                            PartDataTable::open(txn)?.remove(
                                &bucket,
                                upload_id,
                                u32::from(*number),
                            )?;
                        }
                    }
                }
                Ok::<bool, Error>(matches)
            })
            .await
    }

    async fn rename(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Error> {
        // A self-move is a no-op (R14): the bytes are already where they are
        // asked to go, and the read-then-put-then-remove would delete the
        // value it just wrote.
        if from == to {
            return Ok(());
        }
        let named = from.clone();
        let bucket = bucket.to_string();
        let from = from.to_string();
        let to = to.to_string();
        self.driver
            .write(move |txn| move_value(txn, &bucket, &from, &to, &named, true))
            .await
    }

    async fn copy(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Error> {
        // The pair stays uniform with [`Self::rename`]: a self-copy has
        // nothing to duplicate, and answering before the source probe keeps
        // the two self-moves the same no-op.
        if from == to {
            return Ok(());
        }
        let named = from.clone();
        let bucket = bucket.to_string();
        let from = from.to_string();
        let to = to.to_string();
        self.driver
            .write(move |txn| move_value(txn, &bucket, &from, &to, &named, false))
            .await
    }

    async fn scan(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&object::Key>,
        limit: usize,
    ) -> Result<Vec<ContentEntry>, Error> {
        let bucket = bucket.to_string();
        let prefix = prefix.to_owned();
        let after = after.map(ToString::to_string);
        // A page of a bucket is a bucket-width walk: it runs on the blocking
        // pool like every other traversal.
        self.driver
            .read_blocking(move |txn| {
                let mut entries = Vec::new();
                if limit == 0 {
                    return Ok(entries);
                }
                let Some(table) = read_table::<objects::Def>(txn)? else {
                    return Ok(entries);
                };
                // `after` is a **filter**, not a walk start: the walk
                // begins at the greater of the two bounds, so a value sorting
                // below the prefix cannot hide the prefix's page (S3's
                // `start-after` is any client key). Within the prefix the
                // filter is exactly the exclusive start.
                //
                // The prefix is the bound the walk must not leave: the keys
                // sharing it are contiguous, so the first key past the prefix
                // ends the walk (the mismatch-break rule the shared scans
                // use). That break is what the greater bound preserves — from
                // `included(prefix)` on, a non-matching key precedes no
                // matching one.
                let lower = match &after {
                    Some(after) if after.as_str() >= prefix.as_str() => {
                        Bound::Excluded((bucket.as_str(), after.as_str()))
                    }
                    _ => Bound::Included((bucket.as_str(), prefix.as_str())),
                };
                for item in table.0.range((lower, Bound::Unbounded))? {
                    let (key, data) = item?;
                    let (name, raw) = key.value();
                    if name != bucket.as_str() || !raw.starts_with(prefix.as_str()) {
                        break;
                    }
                    let Ok(key) = object::key(raw) else {
                        // A key the domain rejects is not an object: skipped,
                        // like every other unservable shape.
                        continue;
                    };
                    entries.push(ContentEntry {
                        key,
                        len: data.value().len() as u64,
                        identity: 0,
                        mtime: None,
                    });
                    if entries.len() >= limit {
                        break;
                    }
                }
                Ok(entries)
            })
            .await
    }

    async fn stat(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<ContentEntry>, Error> {
        let named = key.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        self.driver.read(move |txn| {
            let Some(table) = read_table::<objects::Def>(txn)? else {
                return Ok(None);
            };
            let Some(guard) = table.get(&bucket, &key)? else {
                return Ok(None);
            };
            Ok(Some(ContentEntry {
                key: named,
                len: guard.value().len() as u64,
                identity: 0,
                mtime: None,
            }))
        })
    }

    async fn create_bucket(&self, _bucket: &bucket::Name) -> Result<(), Error> {
        // No per-bucket object exists on this plane: a row's bucket element
        // is its whole namespace, so there is nothing to create.
        Ok(())
    }

    async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let named = bucket.clone();
        let bucket = bucket.to_string();
        // The contract's refusal: this plane can count its bytes, so a
        // non-empty bucket is refused, never silently emptied. An empty one
        // has nothing to remove — the no-op-aware write commits nothing.
        self.driver
            .write_if(move |txn| -> Result<Option<()>, Error> {
                if has_bytes_writable(txn, &bucket)? {
                    return Err(Error::Refused(storage::not_empty(&named)));
                }
                Ok(None)
            })
            .await
            .map(|_| ())
    }

    async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let bucket = bucket.to_string();
        self.driver.read(move |txn| Ok(!has_bytes(txn, &bucket)?))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tinio_util::testing::{metadata::assert_content_conformance, spans::one_named};
    use tracing::Level;

    use crate::{
        _core::{
            bucket,
            content::{ContentKey, ObjectContent, StagedObject},
            object, storage,
        },
        RedbContent,
        testutil::capture_spans,
    };

    /// The suite's content leg — the staged write and its visibility point,
    /// the ranged read, the move/copy/remove primitives, the walk and its
    /// prefix bound, the bucket lifecycle, and the 8 MiB staged leg (D43).
    #[tokio::test]
    async fn content_conforms() {
        let store = RedbContent::in_memory().unwrap();
        assert_content_conformance(&store).await;
    }

    /// The refusals the suite cannot reach: a staged write publishes
    /// nothing, a move/copy source with no bytes is `NoSuchKey`, and a
    /// bucket that still holds bytes refuses `remove_bucket` with the
    /// contract's `NotEmpty` projection instead of losing them.
    #[tokio::test]
    async fn the_content_refusals_project_onto_the_contract_error() {
        let store = RedbContent::in_memory().unwrap();
        let bucket = bucket::name("content-refusals").unwrap();
        let key = object::key("staged.txt").unwrap();
        let content_key = ContentKey::Object(key.clone());

        // Creating a namespace records nothing on this plane, and removing
        // an empty one is a no-op, twice.
        store.create_bucket(&bucket).await.unwrap();
        store.create_bucket(&bucket).await.unwrap();
        assert!(store.is_empty(&bucket).await.unwrap());
        store.remove_bucket(&bucket).await.unwrap();
        store.remove_bucket(&bucket).await.unwrap();

        // A staged write that is never published leaves nothing behind.
        let mut staged = store.stage(&bucket, &content_key).await.unwrap();
        staged
            .write(Bytes::from_static(b"unpublished"))
            .await
            .unwrap();
        assert!(
            store.stat(&bucket, &key).await.unwrap().is_none(),
            "a staged write is invisible to stat"
        );
        assert!(
            store.is_empty(&bucket).await.unwrap(),
            "and it is not content"
        );

        // A move or copy of bytes that are not there is refused.
        let target = object::key("target.txt").unwrap();
        let refused: storage::Error = store
            .rename(&bucket, &key, &target)
            .await
            .unwrap_err()
            .into();
        assert!(
            matches!(refused, storage::Error::NoSuchKey(_)),
            "{refused:?}"
        );
        let refused: storage::Error = store.copy(&bucket, &key, &target).await.unwrap_err().into();
        assert!(
            matches!(refused, storage::Error::NoSuchKey(_)),
            "{refused:?}"
        );

        // A bucket holding bytes is refused, never emptied.
        let mut held = store.stage(&bucket, &content_key).await.unwrap();
        held.write(Bytes::from_static(b"held")).await.unwrap();
        store.publish(&bucket, held, &content_key).await.unwrap();
        let refused: storage::Error = store.remove_bucket(&bucket).await.unwrap_err().into();
        assert!(
            matches!(refused, storage::Error::NotEmpty(_)),
            "{refused:?}"
        );

        store.remove(&bucket, &content_key).await.unwrap();
        store.remove_bucket(&bucket).await.unwrap();
    }

    /// The publish commit is one info span and the read one debug span; both
    /// carry the object key recorded after creation, so a dynamic field lands
    /// on the span the caller sees.
    #[tokio::test]
    async fn publish_and_open_open_one_span_at_their_tier() {
        let capture = capture_spans();
        let store = RedbContent::in_memory().unwrap();
        let bucket = bucket::name("content-spans").unwrap();
        let key = object::key("traced.txt").unwrap();
        let content_key = ContentKey::Object(key.clone());
        let mut staged = store.stage(&bucket, &content_key).await.unwrap();
        staged.write(Bytes::from_static(b"traced!")).await.unwrap();

        let watermark = capture.watermark();
        store.publish(&bucket, staged, &content_key).await.unwrap();
        drop(store.open(&bucket, &content_key, None).await.unwrap());

        let spans = capture.spans_since(watermark);
        let one = |name: &str| one_named(&spans, name);

        let publish = one("publish");
        assert_eq!(publish.target, "tinio::storage");
        assert_eq!(publish.level, Level::INFO);
        assert_eq!(publish.field("bucket"), Some("content-spans"));
        assert_eq!(publish.field("key"), Some("traced.txt"));
        assert_eq!(publish.field("kind"), Some("object"));
        assert_eq!(publish.field("bytes"), Some("7"));

        let open = one("open");
        assert_eq!(open.target, "tinio::storage");
        assert_eq!(open.level, Level::DEBUG);
        assert_eq!(open.field("key"), Some("traced.txt"));
        assert_eq!(open.field("kind"), Some("object"));
    }
}
