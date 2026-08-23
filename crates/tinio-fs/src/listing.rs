//! Object listing over the storage root (task T043).
//!
//! A directory-tree walk (buckets → files, keys in lexicographic order)
//! with prefix filtering, delimiter-based grouping (common-prefix roll-up),
//! and pagination per S3 semantics (FR-004) — via the shared engine
//! `tinio_core::storage::group_and_paginate`.
//!
//! ETags are included: missing/stale entries of the emitted page are
//! recomputed synchronously (the documented one-time full-content pass
//! over externally-added files, mitigated by the background scanner) —
//! pagination happens on the walked keys first, so the recompute is
//! bounded to the page. `.tinio` entries are always skipped (FR-020);
//! symlink entries are excluded when `follow_symlinks` is disabled. No
//! listing latency bound is promised; listings remain correct and
//! complete at all times.

use std::{
    collections::HashSet,
    io,
    path::{Path, PathBuf},
    time::SystemTime,
};

use futures::StreamExt;
use tinio_core::{
    bucket,
    object::{self, Info, Key},
    storage::{self, ListObjectsParams, ObjectListing, group_and_paginate},
};

use crate::{Error, meta::MetaStore, path::STATE_DIR_NAME};

/// Bounded parallelism for the per-object ETag resolution of one listing
/// page (the recompute-on-stale pass can hash large files).
const ETAG_CONCURRENCY: usize = 16;

/// Listing over one storage root (bucket dirs at the top level).
///
/// # Examples
///
/// ```rust
/// use tinio_core::{bucket, storage::ListObjectsParams};
/// use tinio_fs::{FsListing, MetaStore};
///
/// let root = tempfile::tempdir().unwrap();
/// let state = tempfile::tempdir().unwrap();
/// let meta = MetaStore::new(state.path());
/// let listing = FsListing::new(root.path(), meta, true);
/// tokio::runtime::Runtime::new().unwrap().block_on(async {
///     let b = bucket::name("data").unwrap();
///     std::fs::create_dir(root.path().join("data")).unwrap();
///     std::fs::write(root.path().join("data/a.txt"), b"a").unwrap();
///     std::fs::create_dir(root.path().join("data/dir")).unwrap();
///     std::fs::write(root.path().join("data/dir/b.txt"), b"b").unwrap();
///     let page = listing
///         .list(&ListObjectsParams {
///             bucket: b,
///             prefix: String::new(),
///             delimiter: Some("/".into()),
///             start_after: None,
///             max_keys: 1000,
///         })
///         .await
///         .unwrap();
///     assert_eq!(page.objects.len(), 1);
///     assert_eq!(page.common_prefixes, ["dir/"]);
/// });
/// ```
#[derive(Debug, Clone)]
pub struct FsListing {
    /// Storage root (bucket dirs at the top level).
    root: PathBuf,
    /// The meta store (ETags; recompute-on-stale during the walk).
    meta: MetaStore,
    /// Exclude symlink entries (and do not descend symlink dirs) when
    /// `follow_symlinks` is disabled.
    follow_symlinks: bool,
}

impl FsListing {
    /// Create a listing over `root`.
    pub fn new(root: &Path, meta: MetaStore, follow_symlinks: bool) -> Self {
        Self {
            root: root.to_path_buf(),
            meta,
            follow_symlinks,
        }
    }

    /// List one page of objects per S3 semantics (prefix, delimiter,
    /// pagination). `NoSuchBucket` when the bucket does not exist.
    pub async fn list(&self, params: &ListObjectsParams) -> Result<ObjectListing, Error> {
        // Paginate on the walked keys first — grouping, the marker skip,
        // and the truncation probe need no per-object I/O. ETags are then
        // resolved for the emitted page only: a `max_keys=1` request
        // costs one meta read, not one per object in the bucket.
        let walked = self
            .walk_files(&params.bucket, &params.prefix)
            .await?
            .into_iter()
            .filter(|(key, ..)| key.starts_with(&params.prefix));
        let (page, common_prefixes, truncated, next) = group_and_paginate(
            walked,
            &params.prefix,
            params.delimiter.as_deref(),
            params.start_after.as_deref(),
            params.max_keys,
            |(key, ..)| key.as_ref(),
        );
        // Resolve the page's ETags with bounded concurrency: each is a
        // small meta read, a full content hash when stale (the
        // recompute-on-stale pass is bounded to the page).
        let mut objects: Vec<Info> = Vec::with_capacity(page.len());
        let mut tasks = futures::stream::iter(page.into_iter().map(
            |(key, path, size, mtime)| async move {
                let etag = match self
                    .meta
                    .etag_for_file(&params.bucket, &key, &path, size, mtime)
                    .await
                {
                    Ok(etag) => etag,
                    // The file vanished between the walk and the hash
                    // (concurrent delete): skip the entry instead of
                    // failing the whole page.
                    Err(Error::Io(err)) if err.kind() == io::ErrorKind::NotFound => {
                        return Ok(None);
                    }
                    Err(err) => return Err(err),
                };
                Ok::<_, Error>(Some(Info {
                    key,
                    size,
                    last_modified: mtime,
                    etag,
                }))
            },
        ))
        .buffer_unordered(ETAG_CONCURRENCY);
        while let Some(item) = tasks.next().await {
            if let Some(info) = item? {
                objects.push(info);
            }
        }
        // `buffer_unordered` does not preserve order; the page is
        // re-sorted by key.
        objects.sort_by(|a, b| a.key.as_ref().cmp(b.key.as_ref()));
        Ok(ObjectListing {
            objects,
            common_prefixes,
            truncated,
            next_start_after: next,
        })
    }

    /// Walk every object file of a bucket, in key order, with the size and
    /// mtime from the same stat (listings and the scanner resolve ETags
    /// against these — one syscall per entry). Directories are never
    /// objects, `.tinio` entries are skipped at any depth (FR-020),
    /// symlink entries are excluded when `follow_symlinks` is disabled.
    /// `key_prefix` prunes directories that cannot contain matching keys
    /// (a listing passes its prefix); the scanner passes `""` for the
    /// full walk. Used by the scanner too (same walk, one source of truth
    /// for what an object is). `NoSuchBucket` when the bucket does not
    /// exist.
    pub async fn walk_files(
        &self,
        bucket: &bucket::Name,
        key_prefix: &str,
    ) -> Result<Vec<(Key, PathBuf, u64, SystemTime)>, Error> {
        let bucket_dir = self.root.join(&**bucket);
        let mut out = Vec::new();
        let bucket_meta = match tokio::fs::symlink_metadata(&bucket_dir).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(storage::no_such_bucket(bucket).into());
            }
            Err(err) => return Err(err.into()),
        };
        // A bucket dir that is itself a symlink/junction must not be
        // walked when following is disabled — `read_dir`/`canonicalize`
        // would otherwise list the target (outside the storage root).
        if crate::fsutil::is_symlink_or_reparse(&bucket_meta) && !self.follow_symlinks {
            return Ok(Vec::new());
        }
        // Resolved directory targets already descended into — a symlink
        // pointing at an ancestor would otherwise loop forever.
        let mut visited: HashSet<PathBuf> = HashSet::new();
        if self.follow_symlinks {
            visited.insert(match tokio::fs::canonicalize(&bucket_dir).await {
                Ok(canonical) => canonical,
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    return Err(storage::no_such_bucket(bucket).into());
                }
                Err(err) => return Err(err.into()),
            });
        } else {
            visited.insert(bucket_dir.clone());
        }
        // Iterative tree walk (no async recursion): worklist of
        // `(directory, relative-prefix)` pairs.
        let mut stack: Vec<(PathBuf, PathBuf)> = vec![(bucket_dir, PathBuf::new())];
        while let Some((dir, prefix)) = stack.pop() {
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    if prefix.as_os_str().is_empty() {
                        // The bucket directory itself is gone.
                        return Err(storage::no_such_bucket(bucket).into());
                    }
                    continue; // a nested dir vanished mid-walk — skip it
                }
                Err(err) => return Err(err.into()),
            };
            while let Some(entry) = entries.next_entry().await? {
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    continue; // non-UTF8 names cannot be object keys
                };
                if name == STATE_DIR_NAME {
                    continue; // reserved at any depth (FR-020)
                }
                let lmeta = match tokio::fs::symlink_metadata(entry.path()).await {
                    Ok(metadata) => metadata,
                    Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                    Err(err) => return Err(err.into()),
                };
                let is_symlink = crate::fsutil::is_symlink_or_reparse(&lmeta);
                if is_symlink && !self.follow_symlinks {
                    continue;
                }
                let file_type = lmeta.file_type();
                let rel = prefix.join(name);
                let key = rel.to_string_lossy().into_owned();
                // Windows renders joined path separators as `\` (the only
                // place a backslash can appear on Windows); on Unix a
                // literal `\` in a file name is a legal key character and
                // must survive intact.
                #[cfg(windows)]
                let key = key.replace('\\', "/");
                if file_type.is_dir()
                    || (is_symlink
                        && tokio::fs::metadata(entry.path())
                            .await
                            .map(|m| m.is_dir())
                            .unwrap_or(false))
                {
                    // A symlinked directory may point at an ancestor (a
                    // cycle): never descend into a resolved target twice.
                    if is_symlink {
                        let target = tokio::fs::canonicalize(entry.path()).await?;
                        if !visited.insert(target) {
                            continue;
                        }
                    }
                    // Prefix pruning: a directory neither inside the
                    // requested prefix nor an ancestor of it can never
                    // hold a matching key — skip the whole subtree (a
                    // `max_keys=1` listing of a huge bucket walks only
                    // the prefix's directories).
                    if !key_prefix.is_empty() {
                        let inside = key == key_prefix || key.starts_with(key_prefix);
                        let ancestor = key_prefix.starts_with(&format!("{key}/"));
                        if !inside && !ancestor {
                            continue;
                        }
                    }
                    stack.push((entry.path(), rel));
                    continue;
                }
                let Ok(key) = object::key(key) else {
                    continue; // unrepresentable as an object key
                };
                if key.is_reserved() || key.is_folder_marker() {
                    continue;
                }
                // One stat per entry. Symlinks are followed to the target
                // (the served object's own size/mtime); a dangling link
                // (target gone) is skipped — one broken link must not
                // fail the whole bucket listing.
                let metadata = if is_symlink {
                    match tokio::fs::metadata(entry.path()).await {
                        Ok(metadata) => metadata,
                        Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                        Err(err) => return Err(err.into()),
                    }
                } else {
                    entry.metadata().await?
                };
                out.push((key, entry.path(), metadata.len(), metadata.modified()?));
            }
        }
        out.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::rt;
    use std::fs;
    use tinio_core::storage::Error::NoSuchBucket;
    use tinio_core::storage::ListObjectsParams;
    use tinio_core::testing::etag;

    fn fixture() -> (
        tempfile::TempDir,
        tempfile::TempDir,
        FsListing,
        bucket::Name,
    ) {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let b = bucket::name("data").unwrap();
        fs::create_dir(root.path().join("data")).unwrap();
        for key in ["a.txt", "b.txt", "dir/c.txt", "dir/sub/d.txt", "dir/e.txt"] {
            let path = root.path().join("data").join(key);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, format!("{key}!")).unwrap();
        }
        let listing = FsListing::new(root.path(), MetaStore::new(state.path()), true);
        (root, state, listing, b)
    }

    fn md5_hex(data: &[u8]) -> String {
        use md5::Digest;
        hex::encode(md5::Md5::digest(data))
    }

    fn params(
        b: &bucket::Name,
        prefix: &str,
        delimiter: Option<&str>,
        start: Option<&str>,
        max: usize,
    ) -> ListObjectsParams {
        ListObjectsParams {
            bucket: b.clone(),
            prefix: prefix.into(),
            delimiter: delimiter.map(str::to_string),
            start_after: start.map(str::to_string),
            max_keys: max,
        }
    }

    #[test]
    fn full_listing_is_lexicographic() {
        rt(async {
            let (_root, _state, listing, b) = fixture();
            let page = listing
                .list(&params(&b, "", None, None, 1000))
                .await
                .unwrap();
            let keys: Vec<&str> = page
                .objects
                .iter()
                .map(|o| o.key.as_ref().as_str())
                .collect();
            assert_eq!(
                keys,
                ["a.txt", "b.txt", "dir/c.txt", "dir/e.txt", "dir/sub/d.txt"]
            );
            // ETags were computed (sync recompute pass) and persisted.
            for info in &page.objects {
                assert_eq!(
                    info.etag,
                    etag(&md5_hex(format!("{}!", info.key).as_bytes()))
                );
            }
        });
    }

    #[test]
    fn prefix_and_delimiter_grouping() {
        rt(async {
            let (_root, _state, listing, b) = fixture();
            let page = listing
                .list(&params(&b, "dir/", None, None, 1000))
                .await
                .unwrap();
            assert!(page.objects.iter().all(|o| o.key.starts_with("dir/")));
            assert_eq!(page.objects.len(), 3);

            let page = listing
                .list(&params(&b, "", Some("/"), None, 1000))
                .await
                .unwrap();
            let keys: Vec<&str> = page
                .objects
                .iter()
                .map(|o| o.key.as_ref().as_str())
                .collect();
            assert_eq!(keys, ["a.txt", "b.txt"]);
            assert_eq!(page.common_prefixes, ["dir/"]);
        });
    }

    #[test]
    fn pagination_rolls_over() {
        rt(async {
            let (_root, _state, listing, b) = fixture();
            let page = listing.list(&params(&b, "", None, None, 2)).await.unwrap();
            assert_eq!(page.objects.len(), 2);
            assert!(page.truncated);
            let resume = page.next_start_after.clone().unwrap();
            let page2 = listing
                .list(&params(&b, "", None, Some(&resume), 1000))
                .await
                .unwrap();
            assert_eq!(page.objects.len() + page2.objects.len(), 5);
            assert!(!page2.truncated);
        });
    }

    #[test]
    fn missing_bucket_is_no_such_bucket() {
        rt(async {
            let (_, _, listing, _) = fixture();
            let missing = bucket::name("ghost").unwrap();
            let err = listing
                .list(&params(&missing, "", None, None, 1000))
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Storage(NoSuchBucket(_))), "{err:?}");
        });
    }

    #[test]
    fn tinio_entries_skipped_at_any_depth() {
        rt(async {
            let (_root, _, listing, b) = fixture();
            let root = _root;
            fs::create_dir_all(root.path().join("data/dir/.tinio")).unwrap();
            fs::write(root.path().join("data/dir/.tinio/state"), b"x").unwrap();
            // A file literally named `.tinio` at the bucket root is
            // reserved too (FR-020, exact segment).
            fs::write(root.path().join("data/.tinio"), b"x").unwrap();
            let page = listing
                .list(&params(&b, "", None, None, 1000))
                .await
                .unwrap();
            let keys: Vec<&str> = page
                .objects
                .iter()
                .map(|o| o.key.as_ref().as_str())
                .collect();
            assert!(!keys.iter().any(|k| k.contains(".tinio")));
            assert_eq!(keys.len(), 5);
        });
    }

    #[cfg(unix)]
    #[test]
    fn dangling_symlink_is_skipped_not_fatal() {
        rt(async {
            let (root, _state, listing, b) = fixture();
            // A link whose target does not exist must not fail the whole
            // bucket listing (or the scanner pass).
            std::os::unix::fs::symlink(
                root.path().join("nope.txt"),
                root.path().join("data/broken"),
            )
            .unwrap();
            let page = listing
                .list(&params(&b, "", None, None, 1000))
                .await
                .unwrap();
            let keys: Vec<&str> = page
                .objects
                .iter()
                .map(|o| o.key.as_ref().as_str())
                .collect();
            assert_eq!(keys.len(), 5, "{keys:?}");
            assert!(!keys.contains(&"broken"));
        });
    }

    #[cfg(unix)]
    #[test]
    fn symlink_cycles_terminate() {
        rt(async {
            let (root, _state, listing, b) = fixture();
            // `loop` points at the bucket itself: without cycle detection
            // the walk would descend forever.
            std::os::unix::fs::symlink(".", root.path().join("data/loop")).unwrap();
            let page = listing
                .list(&params(&b, "", None, None, 1000))
                .await
                .unwrap();
            let keys: Vec<&str> = page
                .objects
                .iter()
                .map(|o| o.key.as_ref().as_str())
                .collect();
            assert!(keys.contains(&"a.txt"), "{keys:?}");
        });
    }

    #[test]
    fn symlink_entries_excluded_when_disabled() {
        rt(async {
            let (root, _state, _, _b) = fixture();
            fs::write(root.path().join("outside.txt"), b"out").unwrap();
            #[cfg(unix)]
            {
                let state = _state;
                let b = _b;
                std::os::unix::fs::symlink(
                    root.path().join("outside.txt"),
                    root.path().join("data/link.txt"),
                )
                .unwrap();
                let listing = FsListing::new(root.path(), MetaStore::new(state.path()), false);
                let page = listing
                    .list(&params(&b, "", None, None, 1000))
                    .await
                    .unwrap();
                assert!(!page.objects.iter().any(|o| o.key.as_ref() == "link.txt"));
                let listing = FsListing::new(root.path(), MetaStore::new(state.path()), true);
                let page = listing
                    .list(&params(&b, "", None, None, 1000))
                    .await
                    .unwrap();
                assert!(page.objects.iter().any(|o| o.key.as_ref() == "link.txt"));
            }
        });
    }

    fn link_directory(src: &std::path::Path, dst: &std::path::Path) {
        #[cfg(unix)]
        std::os::unix::fs::symlink(src, dst).unwrap();
        #[cfg(windows)]
        {
            let status = std::process::Command::new("cmd")
                .args([
                    "/C",
                    "mklink",
                    "/J",
                    &dst.to_string_lossy(),
                    &src.to_string_lossy(),
                ])
                .status()
                .expect("spawn mklink");
            assert!(status.success(), "mklink /J failed with {status}");
        }
    }

    #[test]
    fn bucket_dir_symlink_not_walked_when_disabled() {
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let state = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            fs::write(outside.path().join("secret.txt"), b"secret").unwrap();
            link_directory(outside.path(), &root.path().join("data"));
            let listing = FsListing::new(root.path(), MetaStore::new(state.path()), false);
            let b = bucket::name("data").unwrap();
            let page = listing
                .list(&params(&b, "", None, None, 1000))
                .await
                .unwrap();
            let keys: Vec<&str> = page
                .objects
                .iter()
                .map(|o| o.key.as_ref().as_str())
                .collect();
            assert!(
                keys.is_empty(),
                "must not list through a bucket-dir symlink: {keys:?}"
            );
        });
    }

    #[test]
    fn out_of_band_edit_recomputes_etag() {
        rt(async {
            let (root, _, listing, b) = fixture();
            // First listing persists the entries.
            listing
                .list(&params(&b, "", None, None, 1000))
                .await
                .unwrap();
            // Out-of-band modification (new size).
            fs::write(root.path().join("data/a.txt"), b"changed content").unwrap();
            let page = listing
                .list(&params(&b, "", None, None, 1000))
                .await
                .unwrap();
            let a = page
                .objects
                .iter()
                .find(|o| o.key.as_ref() == "a.txt")
                .unwrap();
            assert_eq!(a.etag, etag(&md5_hex(b"changed content")));
        });
    }

    #[test]
    fn directories_never_objects() {
        rt(async {
            let (root, _, listing, b) = fixture();
            fs::create_dir_all(root.path().join("data/empty-dir")).unwrap();
            let page = listing
                .list(&params(&b, "", None, None, 1000))
                .await
                .unwrap();
            assert!(!page.objects.iter().any(|o| o.key.as_ref() == "empty-dir"));
        });
    }
}
