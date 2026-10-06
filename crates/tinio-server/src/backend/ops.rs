//! The S3 operation inventory.
//!
//! One row per operation tinio implements, grouped by whether it mutates.
//! The rows are facts about the operation — its s3s dto pair, its mutating
//! kind, the cargo feature that strips it, the `[s3]` capability that
//! refuses it, and any post-success side effect — and five consumers read
//! them: the three decorators (`MetricS3` in `metrics.rs`, `CapabilityS3`
//! in `capability.rs`, `S3ReadonlyBackend` in `readonly.rs`), this
//! module's own `op_names` (the `include_str!` cross-check against
//! `backend/s3.rs`, the read/write group pins, and the group lists
//! `readonly.rs`'s table-driven tests drive), and `readonly.rs`'s
//! test-only `readonly_probe`.
//!
//! # Row grammar
//!
//! ```text
//! $(#[gated(<field>[: "<feature>"], …)] )?   every listed `Capabilities`
//!     field must be on (the list is a conjunction), and each entry's cargo
//!     feature — when given — strips the row from builds without it.
//! $(#[on_ok(<expr>)]   )?     run after a successful call
//! fn <method> (<InputIdent>) -> <OutputIdent>;
//! ```
//!
//! The compile-time strip and the runtime refusal are one fact at two layers,
//! and the strip is a function of the capability — verified across all 39
//! operations against `backend/s3.rs`'s 22 `#[cfg]` attributes. So the row
//! states the feature once, and the consumers derive the attribute. A
//! capability with no cargo feature (`tagging`, `delete_objects`) is written
//! bare: `#[gated(tagging)]`.
//!
//! **The feature literal cannot be omitted.** `#[cfg]` takes a literal,
//! `macro_rules!` cannot mint one from an ident, and the lookup-macro
//! workaround hits the `delegate!` wall — `E0195` under `#[async_trait]`,
//! which passes an in-impl macro invocation through untouched (design §4,
//! decision 2). Hence `copy_object: "copy"`, never `copy: "copy"`: the
//! literal is mandatory, so the field name informs instead of tautologizing.
//!
//! Two more constraints, each verified by compiling the alternative:
//!
//! - **A consumer cannot use `$(#[$m:meta])*`.** `#[gated(...)]` and
//!   `#[on_ok(...)]` are themselves `meta`: a catch-all beside a named clause
//!   is a local ambiguity the parser refuses (design §4).
//! - **The dto pair is captured as `ident`, not `ty`.** A `ty` fragment
//!   cannot be used as a path segment (`dto::$in`), and `stringify!` of a
//!   multi-token path would break the derived wire name.
//!
//! Every consumer must spell out both clauses in order — `macro_rules!` has
//! no partial matching — so adding a clause here edits five matchers.

macro_rules! tinio_s3_ops {
    ($consumer:ident) => {
        $consumer! {
            read {
                fn head_bucket (HeadBucketInput) -> HeadBucketOutput;
                fn list_buckets (ListBucketsInput) -> ListBucketsOutput;
                fn get_bucket_location (GetBucketLocationInput) -> GetBucketLocationOutput;
                #[gated(tagging)]
                fn get_bucket_tagging (GetBucketTaggingInput) -> GetBucketTaggingOutput;
                #[gated(cors: "cors")]
                fn get_bucket_cors (GetBucketCorsInput) -> GetBucketCorsOutput;
                #[gated(acl: "acl")]
                fn get_bucket_acl (GetBucketAclInput) -> GetBucketAclOutput;
                fn get_object (GetObjectInput) -> GetObjectOutput;
                fn head_object (HeadObjectInput) -> HeadObjectOutput;
                fn get_object_attributes (GetObjectAttributesInput) -> GetObjectAttributesOutput;
                #[gated(versioning: "versioning")]
                fn get_bucket_versioning (GetBucketVersioningInput) -> GetBucketVersioningOutput;
                #[gated(versioning: "versioning")]
                fn list_object_versions (ListObjectVersionsInput) -> ListObjectVersionsOutput;
                #[gated(tagging)]
                fn get_object_tagging (GetObjectTaggingInput) -> GetObjectTaggingOutput;
                #[gated(acl: "acl")]
                fn get_object_acl (GetObjectAclInput) -> GetObjectAclOutput;
                #[gated(list_objects_v1: "list-v1")]
                fn list_objects (ListObjectsInput) -> ListObjectsOutput;
                #[gated(list_objects_v2: "list-v2")]
                fn list_objects_v2 (ListObjectsV2Input) -> ListObjectsV2Output;
                #[gated(multipart: "multipart")]
                fn list_parts (ListPartsInput) -> ListPartsOutput;
                #[gated(multipart: "multipart")]
                fn list_multipart_uploads (ListMultipartUploadsInput) -> ListMultipartUploadsOutput;
                #[gated(select: "select")]
                fn select_object_content (SelectObjectContentInput) -> SelectObjectContentOutput;
            }
            write {
                fn create_bucket (CreateBucketInput) -> CreateBucketOutput;
                fn delete_bucket (DeleteBucketInput) -> DeleteBucketOutput;
                #[gated(tagging)]
                fn put_bucket_tagging (PutBucketTaggingInput) -> PutBucketTaggingOutput;
                #[gated(tagging)]
                fn delete_bucket_tagging (DeleteBucketTaggingInput) -> DeleteBucketTaggingOutput;
                #[gated(cors: "cors")]
                fn put_bucket_cors (PutBucketCorsInput) -> PutBucketCorsOutput;
                #[gated(cors: "cors")]
                fn delete_bucket_cors (DeleteBucketCorsInput) -> DeleteBucketCorsOutput;
                #[gated(acl: "acl")]
                fn put_bucket_acl (PutBucketAclInput) -> PutBucketAclOutput;
                #[gated(acl: "acl")]
                fn put_object_acl (PutObjectAclInput) -> PutObjectAclOutput;
                fn put_object (PutObjectInput) -> PutObjectOutput;
                fn delete_object (DeleteObjectInput) -> DeleteObjectOutput;
                #[gated(delete_objects)]
                fn delete_objects (DeleteObjectsInput) -> DeleteObjectsOutput;
                #[gated(versioning: "versioning")]
                fn put_bucket_versioning (PutBucketVersioningInput) -> PutBucketVersioningOutput;
                #[gated(tagging)]
                fn put_object_tagging (PutObjectTaggingInput) -> PutObjectTaggingOutput;
                #[gated(tagging)]
                fn delete_object_tagging (DeleteObjectTaggingInput) -> DeleteObjectTaggingOutput;
                #[gated(copy_object: "copy")]
                fn copy_object (CopyObjectInput) -> CopyObjectOutput;
                #[gated(copy_object: "copy")]
                fn rename_object (RenameObjectInput) -> RenameObjectOutput;
                #[gated(multipart: "multipart")]
                #[on_ok(crate::metrics::multipart_in_progress_inc())]
                fn create_multipart_upload (CreateMultipartUploadInput) -> CreateMultipartUploadOutput;
                #[gated(multipart: "multipart")]
                fn upload_part (UploadPartInput) -> UploadPartOutput;
                #[gated(multipart: "multipart", copy_object: "copy")]
                fn upload_part_copy (UploadPartCopyInput) -> UploadPartCopyOutput;
                #[gated(multipart: "multipart")]
                #[on_ok(crate::metrics::multipart_in_progress_dec())]
                fn complete_multipart_upload (CompleteMultipartUploadInput) -> CompleteMultipartUploadOutput;
                #[gated(multipart: "multipart")]
                #[on_ok(crate::metrics::multipart_in_progress_dec())]
                fn abort_multipart_upload (AbortMultipartUploadInput) -> AbortMultipartUploadOutput;
            }
        }
    };
}
pub(crate) use tinio_s3_ops;

/// The table read as data, for the cross-check test.
///
/// Two properties, deliberately built two ways:
///
/// - [`table_features`] pushes **unconditionally**. It is the table's facts —
///   name plus stripping features — and must be comparable with `s3.rs`'s raw
///   text, which `include_str!` never filters by feature;
/// - [`table_ops`] carries the derived `#[cfg]`, so it is what *this* build
///   compiles (measured on this table: 39 names under `--all-features`, 23
///   under `--no-default-features --features multipart`, 17 under
///   `--no-default-features`). It is a subset check against the table, never
///   an equality with the file.
/// - [`read_ops`] and [`write_ops`] push unconditionally too, one group each:
///   the grouping pin has to hold in every build, not only an all-features
///   one, so neither may carry the derived `#[cfg]`.
///
/// The `allow`s cover this shape, not its content: a `Vec::new()` followed by
/// `push` trips `clippy::vec_init_then_push`, and a row without `#[gated]`
/// never pushes into `feats`, which makes that `mut` unused. Clippy **does**
/// lint local `macro_rules!` output (measured), and the repo gates on
/// `-D warnings`.
macro_rules! op_names {
    (
        read { $( $(#[gated($($rg:ident $(: $rf:literal)?),+)])? $(#[on_ok($rh:expr)])? fn $r:ident ( $ri:ident ) -> $ro:ident; )* }
        write { $( $(#[gated($($wg:ident $(: $wf:literal)?),+)])? $(#[on_ok($wh:expr)])? fn $w:ident ( $wi:ident ) -> $wo:ident; )* }
    ) => {
        /// Every row this feature set compiles.
        #[cfg(test)]
        #[allow(clippy::vec_init_then_push)]
        pub(crate) fn table_ops() -> Vec<&'static str> {
            let mut ops = Vec::new();
            $(
                $( $( $(#[cfg(feature = $rf)])? )+ )?
                ops.push(stringify!($r));
            )*
            $(
                $( $( $(#[cfg(feature = $wf)])? )+ )?
                ops.push(stringify!($w));
            )*
            ops
        }

        /// Every row with the cargo features that strip it — not filtered by
        /// the build, so it can be compared with `s3.rs`'s text.
        #[cfg(test)]
        #[allow(clippy::vec_init_then_push)]
        pub(crate) fn table_features() -> Vec<(&'static str, Vec<&'static str>)> {
            let mut rows = Vec::new();
            $(
                #[allow(unused_mut)]
                let mut feats: Vec<&'static str> = Vec::new();
                $( $( $( feats.push($rf); )? )+ )?
                rows.push((stringify!($r), feats));
            )*
            $(
                #[allow(unused_mut)]
                let mut feats: Vec<&'static str> = Vec::new();
                $( $( $( feats.push($wf); )? )+ )?
                rows.push((stringify!($w), feats));
            )*
            rows
        }

        /// The read group alone, unfiltered by the build: the decorators
        /// forward these.
        #[cfg(test)]
        #[allow(clippy::vec_init_then_push)]
        pub(crate) fn read_ops() -> Vec<&'static str> {
            let mut ops = Vec::new();
            $(
                ops.push(stringify!($r));
            )*
            ops
        }

        /// The write group alone, unfiltered by the build: these are the
        /// operations the read-only decorator refuses.
        #[cfg(test)]
        #[allow(clippy::vec_init_then_push)]
        pub(crate) fn write_ops() -> Vec<&'static str> {
            let mut ops = Vec::new();
            $(
                ops.push(stringify!($w));
            )*
            ops
        }
    };
}

tinio_s3_ops!(op_names);

#[cfg(test)]
mod tests {
    use std::mem::take;

    use super::*;

    /// `(async fn name, cfg features)` for each method of `S3Backend`'s
    /// dispatch, in file order.
    ///
    /// The scan starts at the `impl … S3 for S3Backend` header and runs to
    /// the first `#[cfg(test)]` line or the end of the file. There is no
    /// brace matching, so those two bounds coincide with the impl block only
    /// because the impl is `s3.rs`'s last item and the file has no test
    /// module — a later test module, or any `async fn` after the block, is
    /// what the `#[cfg(test)]` stop is there to catch. Comment lines are
    /// skipped without clearing the pending features: a comment between an
    /// operation's `#[cfg]` and its `fn` must not be mistaken for another
    /// operation or silently drop the features.
    fn dispatch_rows(source: &str) -> Vec<(String, Vec<String>)> {
        let mut rows: Vec<(String, Vec<String>)> = Vec::new();
        let mut pending: Vec<String> = Vec::new();
        let mut in_impl = false;
        for line in source.lines() {
            let trimmed = line.trim();
            if !in_impl {
                in_impl = trimmed.starts_with("impl<") && trimmed.contains("S3 for S3Backend");
                continue;
            }
            if trimmed.starts_with("#[cfg(test)]") {
                break;
            }
            if trimmed.starts_with("//") {
                continue;
            }
            if trimmed.starts_with("#[cfg(") {
                // A `not(...)` would read as a plain feature and silently
                // invert the set instead of failing, so refuse it loudly.
                assert!(
                    !trimmed.contains("not("),
                    "unsupported `not(...)` in a dispatch cfg: {trimmed}"
                );
                // Every other cfg in the dispatch is either one
                // `feature = "x"` or an `all(...)` of them.
                pending.extend(
                    trimmed
                        .split("feature = \"")
                        .skip(1)
                        .filter_map(|rest| rest.split('"').next())
                        .map(String::from),
                );
                continue;
            }
            if let Some(rest) = trimmed.strip_prefix("async fn ") {
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                let mut features = take(&mut pending);
                features.sort();
                rows.push((name, features));
                continue;
            }
            if !trimmed.is_empty() {
                pending.clear();
            }
        }
        rows
    }

    #[test]
    fn table_matches_the_s3_dispatch() {
        // The file side is raw text, so it is feature-independent: all 36
        // methods with their `#[cfg]` attributes, in file order.
        let mut expected = dispatch_rows(include_str!("s3.rs"));
        // The table side must not be filtered either — hence
        // `table_features`, which pushes unconditionally. Comparing the
        // `#[cfg]`-filtered `table_ops()` against the file could only ever
        // hold in an all-features build.
        let mut actual: Vec<(String, Vec<String>)> = table_features()
            .into_iter()
            .map(|(name, feats)| {
                let mut feats: Vec<String> = feats.into_iter().map(String::from).collect();
                feats.sort();
                (name.to_string(), feats)
            })
            .collect();
        expected.sort();
        actual.sort();
        // Names AND the cargo features that strip them, sorted on both
        // sides: the table's row order is free (reads first, then writes)
        // and is not `s3.rs`'s order.
        assert_eq!(actual, expected);
    }

    /// The twenty-one mutating operations, written out from the design's
    /// `Kind` column and the plan's table, in sorted order so it compares
    /// directly with the sorted group. The independent side of
    /// [`table_groups_operations_by_whether_they_mutate`], which is why it
    /// is spelled out rather than read back from the table it pins.
    const WRITE_OPS: [&str; 21] = [
        "abort_multipart_upload",
        "complete_multipart_upload",
        "copy_object",
        "create_bucket",
        "create_multipart_upload",
        "delete_bucket",
        "delete_bucket_cors",
        "delete_bucket_tagging",
        "delete_object",
        "delete_object_tagging",
        "delete_objects",
        "put_bucket_acl",
        "put_bucket_cors",
        "put_bucket_tagging",
        "put_bucket_versioning",
        "put_object",
        "put_object_acl",
        "put_object_tagging",
        "rename_object",
        "upload_part",
        "upload_part_copy",
    ];

    /// The table's read/write split, pinned against [`WRITE_OPS`].
    ///
    /// [`table_matches_the_s3_dispatch`] sorts both sides and compares names
    /// and stripping features, so a row moved between the two groups is
    /// invisible to it — and the read-only decorator forwards the `read`
    /// group while refusing the `write` group, so a mutating operation
    /// misplaced into `read` would be *forwarded* in read-only mode with
    /// every other test still green. `s3.rs` is the independent side for the
    /// names and the features; the duplicated list above is the independent
    /// side for the classification.
    #[test]
    fn table_groups_operations_by_whether_they_mutate() {
        let mut writes = write_ops();
        writes.sort();
        assert_eq!(writes, WRITE_OPS);
        // The two groups partition the table, so the read group is exactly
        // the other eighteen rows: no row in both, none in neither. A row
        // listed twice inside one group, or in both groups, breaks the
        // sorted comparison below; a row duplicated in the table itself is
        // `table_has_no_duplicates`'s to catch.
        let mut read = read_ops();
        read.sort();
        let mut union = read.clone();
        union.extend(writes);
        union.sort();
        let mut all: Vec<&str> = table_features().into_iter().map(|(name, _)| name).collect();
        all.sort();
        assert_eq!(union, all);
    }

    /// What this build compiles comes from the table — the decorators' own
    /// operation set, checked under every feature combination.
    #[test]
    fn compiled_ops_come_from_the_table() {
        let names: Vec<&str> = table_features().into_iter().map(|(name, _)| name).collect();
        for op in table_ops() {
            assert!(names.contains(&op), "{op} is not a table row");
        }
    }

    #[test]
    fn table_has_no_duplicates() {
        let ops = table_ops();
        let mut sorted = ops.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), ops.len(), "{ops:?}");
    }
}
