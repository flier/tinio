//! The bounded listing merge of the warm row stream and the cold index range.
//!
//! A listing is one key-ordered page over two planes: the filesystem's
//! metadata rows (warm and cold rows alike — a demoted object keeps its row
//! as the mirror, T2) and the layer's placements (the cold authority). Both
//! sides are read with the same `prefix`/`after`/`limit` window and merged by
//! key; a key both sides name is listed once, from its [`ColdInfo`], because
//! the index is the cold metadata's authority (T2, T20). The merged stream is
//! then handed to the core listing engine, so delimiter grouping, the page
//! cut and the resume markers stay the shared implementation's, not a second
//! spelling here.
//!
//! The merge is bounded: each round fetches one window from each side and
//! advances that side's exclusive cursor, so a page costs a page's worth of
//! rows plus the one probe entry the cut needs — never a whole bucket. A
//! delimiter group is the one case that reads past the page: every member has
//! to be seen to know the group is complete, exactly as a single-plane
//! delimiter listing does.
//!
//! [`ColdInfo`]: crate::ColdInfo

use std::collections::VecDeque;

use crate::{
    _core::{
        bucket,
        content::ObjectContent,
        object,
        storage::{self, ListObjectsParams, ObjectListing, Storage, VersionOps},
        version,
    },
    Error, State, Tier, TierStorage,
};

/// One merged listing row and the plane whose metadata it carries.
#[derive(Debug, Clone)]
struct Merged {
    /// The key's metadata — the cold authority's for a pg-placed key.
    info: object::Info,
    /// Whether the cold index supplied the metadata.
    cold: bool,
}

/// One plane's fetched window, ascending by key.
struct Stream {
    /// The rows fetched but not merged yet.
    items: VecDeque<Merged>,
    /// The last raw key the plane was read through (the fetch cursor and the
    /// safe frontier).
    last: Option<String>,
    /// Whether the plane returned a short page or an empty one.
    done: bool,
}

impl Stream {
    /// A plane nothing has been fetched from yet, resuming after `after`
    /// (exclusive) when the listing names a marker.
    fn new(after: Option<&str>) -> Self {
        Self {
            items: VecDeque::new(),
            last: after.map(str::to_owned),
            done: false,
        }
    }

    /// The front row's key, if any.
    fn front_key(&self) -> Option<&str> {
        self.items.front().map(|row| &row.info.key[..])
    }

    /// Whether this plane has been read through `key`: every row it holds at
    /// or below `key` is in hand, so the other plane's row at `key` is safe to
    /// emit (a duplicate would have been seen).
    fn read_through(&self, key: &str) -> bool {
        self.done || self.last.as_deref().is_some_and(|last| last >= key)
    }
}

/// Merge every row the two planes hold up to the point `decided` marks the
/// page complete.
///
/// A row is emitted only once both planes have been read through its key, so
/// a duplicate always meets its partner before the pair is cut: the warm
/// mirror is dropped and the cold authority's row takes the key once.
async fn merged_prefix<C: ObjectContent>(
    storage: &TierStorage<C>,
    bucket: &bucket::Name,
    prefix: &str,
    after: Option<&str>,
    batch: usize,
    decided: impl Fn(&[Merged]) -> bool,
) -> Result<Vec<Merged>, Error> {
    let mut warm = Stream::new(after);
    let mut cold = Stream::new(after);
    let mut merged = Vec::new();
    loop {
        if !warm.done {
            let cursor = warm.last.clone();
            let (items, done) = storage
                .warm_window(bucket, prefix, cursor.as_deref(), batch)
                .await?;
            if let Some(last) = items.last() {
                warm.last = Some(last.info.key.as_ref().to_owned());
            }
            warm.done = done;
            warm.items.extend(items);
        }
        if !cold.done {
            let cursor = cold.last.clone();
            let (items, done, through) =
                storage.cold_window(bucket, prefix, cursor.as_deref(), batch)?;
            if through.is_some() {
                cold.last = through;
            }
            cold.done = done;
            cold.items.extend(items);
        }
        emit(&mut warm, &mut cold, &mut merged);
        if decided(&merged) || (warm.done && cold.done) {
            return Ok(merged);
        }
    }
}

/// Move every row both planes are known to have passed into `merged`, in key
/// order, with the cold row winning a key both planes name.
///
/// The decision reads both front keys through borrows; only the chosen stream
/// is then moved, so no key is cloned to compare it.
fn emit(warm: &mut Stream, cold: &mut Stream, merged: &mut Vec<Merged>) {
    loop {
        let advance = match (warm.front_key(), cold.front_key()) {
            (None, None) => break,
            (Some(key), None) => {
                if cold.read_through(key) {
                    Advance::Warm
                } else {
                    Advance::Wait
                }
            }
            (None, Some(key)) => {
                if warm.read_through(key) {
                    Advance::Cold
                } else {
                    Advance::Wait
                }
            }
            (Some(warm_key), Some(cold_key)) if warm_key == cold_key => Advance::Duplicate,
            (Some(warm_key), Some(cold_key)) if warm_key < cold_key => {
                if cold.read_through(warm_key) {
                    Advance::Warm
                } else {
                    Advance::Wait
                }
            }
            (Some(_), Some(cold_key)) => {
                if warm.read_through(cold_key) {
                    Advance::Cold
                } else {
                    Advance::Wait
                }
            }
        };
        match advance {
            Advance::Wait => break,
            Advance::Warm => emitter(warm, merged),
            Advance::Cold => emitter(cold, merged),
            Advance::Duplicate => {
                // The index is the authority for a pg-placed key (T2): drop
                // the warm mirror and emit the cold row once (T20).
                warm.items.pop_front();
                emitter(cold, merged);
            }
        }
    }
}

/// Which stream the merge advances next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Advance {
    /// The warm plane's front row is safe to emit.
    Warm,
    /// The cold plane's front row is safe to emit.
    Cold,
    /// Both planes name the key: drop the warm mirror, emit the cold row.
    Duplicate,
    /// The other plane has not been read through the front key yet.
    Wait,
}

/// Move one plane's front row into the merged stream.
fn emitter(stream: &mut Stream, merged: &mut Vec<Merged>) {
    if let Some(row) = stream.items.pop_front() {
        merged.push(row);
    }
}

impl<C: ObjectContent> TierStorage<C> {
    /// The client `ListObjects` page: flush the write buffer first (T8/T11),
    /// then cut the merged page.
    ///
    /// This is the deliberate seam Task 11's buffer flush fills in; the
    /// metrics scan below deliberately does not take it.
    pub(crate) async fn list_objects_merged(
        &self,
        params: ListObjectsParams,
    ) -> Result<ObjectListing, <Self as Storage>::Error> {
        self.before_client_listing().await?;
        self.scan_objects_merged(params).await
    }

    /// The merged `ListObjects` page without the client listing's pre-flight
    /// hook — the metrics reader's counting scan (T20, T26).
    pub(crate) async fn scan_objects_merged(
        &self,
        params: ListObjectsParams,
    ) -> Result<ObjectListing, <Self as Storage>::Error> {
        let _gate = self.gate_bucket_read(&params.bucket).await;
        // The bucket-existence probe the row scans cannot answer themselves
        // (a missing bucket's row range is simply empty).
        self.fs().bucket_versioning(&params.bucket).await?;
        if params.max_keys == 0 {
            return Ok(ObjectListing {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                truncated: false,
                next_start_after: None,
            });
        }
        let merged = merged_prefix(
            self,
            &params.bucket,
            &params.prefix,
            params.start_after.as_deref(),
            params.max_keys,
            |items| past_page(items, &params),
        )
        .await?;
        let (objects, common_prefixes, truncated, next_start_after) =
            group_and_paginate(merged, &params);
        Ok(ObjectListing {
            objects,
            common_prefixes,
            truncated,
            next_start_after,
        })
    }

    /// The client `ListObjectVersions` page: flush first (T8/T11), then merge
    /// the planes.
    pub(crate) async fn list_object_versions_merged(
        &self,
        bucket: &bucket::Name,
        params: version::ListParams,
    ) -> Result<version::Listing, <Self as Storage>::Error> {
        self.before_client_listing().await?;
        let _gate = self.gate_bucket_read(bucket).await;
        let state = self.fs().bucket_versioning(bucket).await?;
        if state.records_versions() {
            // Cold placements only exist for unversioned buckets: a classed
            // write (the only demotion path) is refused on a versioned or
            // suspended bucket, and enabling or suspending versioning
            // promotes every cold object first (T7, T10). The filesystem's
            // own bounded version walk is therefore the whole answer.
            return Ok(self.fs().list_object_versions(bucket, params).await?);
        }
        if params.max_keys == 0 {
            return Ok(version::paginate(&[], &params));
        }
        let prefix = params.prefix.clone().unwrap_or_default();
        let merged = merged_prefix(
            self,
            bucket,
            &prefix,
            params.key_marker.as_deref(),
            params.max_keys,
            |items| version::paginate(&entries(items), &params).is_truncated,
        )
        .await?;
        Ok(version::paginate(&entries(&merged), &params))
    }

    /// One warm row window.
    async fn warm_window(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<Merged>, bool), Error> {
        let page = self
            .fs()
            .tier_rows()
            .scan(bucket, prefix, after, limit)
            .await?;
        let done = page.len() < limit;
        Ok((
            page.into_iter()
                .map(|info| Merged { info, cold: false })
                .collect(),
            done,
        ))
    }

    /// One cold placement window, with its raw read-through key: the page's
    /// last key even when every placement on it is a warm-tier row the merge
    /// skips, so the next window never re-reads the same page.
    fn cold_window(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<Merged>, bool, Option<String>), Error> {
        let page = self.index().scan_placement(bucket, prefix, after, limit)?;
        let done = page.len() < limit;
        let through = page.last().map(|(key, _)| key.as_ref().to_owned());
        let mut items = Vec::with_capacity(page.len());
        for (_, placement) in page {
            if placement.tier != Tier::Pg || placement.state != State::Live {
                continue;
            }
            let Some(cold) = placement.cold else {
                return Err(Error::Storage(storage::corrupt(
                    "a pg placement carries no cold metadata",
                )));
            };
            items.push(Merged {
                info: cold.to_info()?,
                cold: true,
            });
        }
        Ok((items, done, through))
    }
}

/// Whether the merged stream holds more than `max_keys` listing entries.
///
/// This is the cut's truncation flag alone, answered by the core engine's
/// shared predicate: the marker filter and delimiter grouping run exactly as
/// [`storage::group_and_paginate`] runs them, but the scan stops at the first
/// entry past the page instead of building a page it would throw away. Each
/// merge round asks this of the accumulated stream; a settled page stops the
/// merge.
fn past_page(merged: &[Merged], params: &ListObjectsParams) -> bool {
    storage::past_page(
        merged,
        &params.prefix,
        params.delimiter.as_deref(),
        params.start_after.as_deref(),
        params.max_keys,
        |row| row.info.key.as_ref(),
    )
}

/// The core engine's cut over the merged stream: grouping happens before the
/// page is cut, and the resume marker is the last emitted entry.
///
/// The stream is consumed, so a listed object's metadata moves into the page
/// instead of being cloned.
fn group_and_paginate(
    merged: Vec<Merged>,
    params: &ListObjectsParams,
) -> (Vec<object::Info>, Vec<String>, bool, Option<String>) {
    let (rows, common_prefixes, truncated, next) = storage::group_and_paginate(
        merged,
        &params.prefix,
        params.delimiter.as_deref(),
        params.start_after.as_deref(),
        params.max_keys,
        |row| row.info.key.as_ref(),
    );
    (
        rows.into_iter().map(|row| row.info).collect(),
        common_prefixes,
        truncated,
        next,
    )
}

/// The version entries of a merged key stream: one null version per key.
///
/// A cold key is listed as its null version built from [`ColdInfo`] (T20);
/// a warm row on an unversioned bucket carries the same null identity, so the
/// merge keeps the row's own when it has one.
///
/// [`ColdInfo`]: crate::ColdInfo
fn entries(merged: &[Merged]) -> Vec<version::Entry> {
    merged
        .iter()
        .map(|row| version::Entry {
            key: row.info.key.clone(),
            id: if row.cold {
                None
            } else {
                row.info.version_id.clone()
            },
            seq: 0,
            is_delete_marker: false,
            size: row.info.size,
            last_modified: row.info.last_modified,
            etag: row.info.etag.clone(),
        })
        .collect()
}
