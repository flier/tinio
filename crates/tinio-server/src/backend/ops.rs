//! The S3 operation inventory.
//!
//! One row per operation tinio implements, grouped by whether it mutates.
//! The rows are facts about the operation — its s3s dto pair, its inherent
//! handler, its mutating kind, the cargo feature that strips it, the `[s3]`
//! capability that refuses it, and any post-success side effect — and six
//! consumers read them: the four generated `impl S3` blocks (`S3Backend`'s
//! dispatch in `backend/s3.rs`, `MetricS3` in `metrics.rs`, `CapabilityS3`
//! in `capability.rs`, `S3ReadonlyBackend` in `readonly.rs`), this
//! module's own `op_names` (the read/write group pins, and the group lists
//! `readonly.rs`'s table-driven tests drive), and `readonly.rs`'s
//! test-only `readonly_probe`.
//!
//! The base dispatch — `impl S3 for S3Backend` — is generated here too, so
//! the table is the single place an operation is declared: a row's name
//! answers its s3s trait method, and its `#[handler(...)]` names the
//! inherent method that does the work. A row whose handler does not exist,
//! whose dto pair does not match that handler, or whose stripping features
//! disagree with the handler's own `#[cfg]` is a **compile error** in the
//! generated dispatch — a name no dispatch arm answers (a 501 waiting to
//! happen) cannot be written down.
//!
//! # Row grammar
//!
//! ```text
//! #[handler(<ident>)]                    the inherent method on `S3Backend`
//!     that answers this s3s operation; the
//!     generated dispatch calls `self.<ident>`
//! $(#[gated(<field>[: "<feature>"], …)] )?   every listed `Capabilities`
//!     field must be on (the list is a conjunction), and each entry's cargo
//!     feature — when given — strips the row from builds without it.
//! $(#[on_ok(<expr>)]   )?     run after a successful call
//! fn <method> (<InputIdent>) -> <OutputIdent>;
//! ```
//!
//! The handler name is captured as an `ident` because `macro_rules!` cannot
//! mint `op_<method>` from the row name, so the spelling that the dispatch
//! calls must be written out. It is not free to diverge: the generated arm
//! is `self.<handler>(req)`, and the handler's signature is the row's dto
//! pair, so a handler that is missing (`E0599`) or that answers a different
//! operation's dtos (`E0308`) fails to compile.
//!
//! The compile-time strip and the runtime refusal are one fact at two layers,
//! and the strip is a function of the capability — verified across all 42
//! operations against the handlers' own `#[cfg]` attributes. So the row
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
//! - **A consumer cannot use `$(#[$m:meta])*`.** `#[handler(...)]`,
//!   `#[gated(...)]` and `#[on_ok(...)]` are themselves `meta`: a catch-all
//!   beside a named clause is a local ambiguity the parser refuses (design §4).
//! - **The dto pair is captured as `ident`, not `ty`.** A `ty` fragment
//!   cannot be used as a path segment (`dto::$in`), and `stringify!` of a
//!   multi-token path would break the derived wire name.
//!
//! Every consumer must spell out all three clauses in order — `macro_rules!`
//! has no partial matching — so adding a clause here edits six matchers.

macro_rules! tinio_s3_ops {
    ($consumer:ident) => {
        $consumer! {
            read {
                #[handler(op_head_bucket)]
                fn head_bucket (HeadBucketInput) -> HeadBucketOutput;
                #[handler(op_list_buckets)]
                fn list_buckets (ListBucketsInput) -> ListBucketsOutput;
                #[handler(op_get_bucket_location)]
                fn get_bucket_location (GetBucketLocationInput) -> GetBucketLocationOutput;
                #[handler(op_get_bucket_tagging)]
                #[gated(tagging)]
                fn get_bucket_tagging (GetBucketTaggingInput) -> GetBucketTaggingOutput;
                #[handler(op_get_bucket_cors)]
                #[gated(cors: "cors")]
                fn get_bucket_cors (GetBucketCorsInput) -> GetBucketCorsOutput;
                #[handler(op_get_bucket_lifecycle)]
                #[gated(lifecycle: "lifecycle")]
                fn get_bucket_lifecycle_configuration (GetBucketLifecycleConfigurationInput) -> GetBucketLifecycleConfigurationOutput;
                #[handler(op_get_bucket_acl)]
                #[gated(acl: "acl")]
                fn get_bucket_acl (GetBucketAclInput) -> GetBucketAclOutput;
                #[handler(op_get_object)]
                fn get_object (GetObjectInput) -> GetObjectOutput;
                #[handler(op_head_object)]
                fn head_object (HeadObjectInput) -> HeadObjectOutput;
                #[handler(op_get_object_attributes)]
                fn get_object_attributes (GetObjectAttributesInput) -> GetObjectAttributesOutput;
                #[handler(op_get_bucket_versioning)]
                #[gated(versioning: "versioning")]
                fn get_bucket_versioning (GetBucketVersioningInput) -> GetBucketVersioningOutput;
                #[handler(op_list_object_versions)]
                #[gated(versioning: "versioning")]
                fn list_object_versions (ListObjectVersionsInput) -> ListObjectVersionsOutput;
                #[handler(op_get_object_tagging)]
                #[gated(tagging)]
                fn get_object_tagging (GetObjectTaggingInput) -> GetObjectTaggingOutput;
                #[handler(op_get_object_acl)]
                #[gated(acl: "acl")]
                fn get_object_acl (GetObjectAclInput) -> GetObjectAclOutput;
                #[handler(op_list_objects)]
                #[gated(list_objects_v1: "list-v1")]
                fn list_objects (ListObjectsInput) -> ListObjectsOutput;
                #[handler(op_list_objects_v2)]
                #[gated(list_objects_v2: "list-v2")]
                fn list_objects_v2 (ListObjectsV2Input) -> ListObjectsV2Output;
                #[handler(op_list_parts)]
                #[gated(multipart: "multipart")]
                fn list_parts (ListPartsInput) -> ListPartsOutput;
                #[handler(op_list_multipart_uploads)]
                #[gated(multipart: "multipart")]
                fn list_multipart_uploads (ListMultipartUploadsInput) -> ListMultipartUploadsOutput;
                #[handler(op_select_object_content)]
                #[gated(select: "select")]
                fn select_object_content (SelectObjectContentInput) -> SelectObjectContentOutput;
            }
            write {
                #[handler(op_create_bucket)]
                fn create_bucket (CreateBucketInput) -> CreateBucketOutput;
                #[handler(op_delete_bucket)]
                fn delete_bucket (DeleteBucketInput) -> DeleteBucketOutput;
                #[handler(op_put_bucket_tagging)]
                #[gated(tagging)]
                fn put_bucket_tagging (PutBucketTaggingInput) -> PutBucketTaggingOutput;
                #[handler(op_delete_bucket_tagging)]
                #[gated(tagging)]
                fn delete_bucket_tagging (DeleteBucketTaggingInput) -> DeleteBucketTaggingOutput;
                #[handler(op_put_bucket_cors)]
                #[gated(cors: "cors")]
                fn put_bucket_cors (PutBucketCorsInput) -> PutBucketCorsOutput;
                #[handler(op_delete_bucket_cors)]
                #[gated(cors: "cors")]
                fn delete_bucket_cors (DeleteBucketCorsInput) -> DeleteBucketCorsOutput;
                #[handler(op_put_bucket_lifecycle)]
                #[gated(lifecycle: "lifecycle")]
                fn put_bucket_lifecycle_configuration (PutBucketLifecycleConfigurationInput) -> PutBucketLifecycleConfigurationOutput;
                #[handler(op_delete_bucket_lifecycle)]
                #[gated(lifecycle: "lifecycle")]
                fn delete_bucket_lifecycle (DeleteBucketLifecycleInput) -> DeleteBucketLifecycleOutput;
                #[handler(op_put_bucket_acl)]
                #[gated(acl: "acl")]
                fn put_bucket_acl (PutBucketAclInput) -> PutBucketAclOutput;
                #[handler(op_put_object_acl)]
                #[gated(acl: "acl")]
                fn put_object_acl (PutObjectAclInput) -> PutObjectAclOutput;
                #[handler(op_put_object)]
                #[on_ok(crate::metrics::object_uploaded("PutObject"))]
                fn put_object (PutObjectInput) -> PutObjectOutput;
                #[handler(op_delete_object)]
                #[on_ok(crate::metrics::object_deleted())]
                fn delete_object (DeleteObjectInput) -> DeleteObjectOutput;
                #[handler(op_delete_objects)]
                #[gated(delete_objects)]
                fn delete_objects (DeleteObjectsInput) -> DeleteObjectsOutput;
                #[handler(op_put_bucket_versioning)]
                #[gated(versioning: "versioning")]
                fn put_bucket_versioning (PutBucketVersioningInput) -> PutBucketVersioningOutput;
                #[handler(op_put_object_tagging)]
                #[gated(tagging)]
                fn put_object_tagging (PutObjectTaggingInput) -> PutObjectTaggingOutput;
                #[handler(op_delete_object_tagging)]
                #[gated(tagging)]
                fn delete_object_tagging (DeleteObjectTaggingInput) -> DeleteObjectTaggingOutput;
                #[handler(op_copy_object)]
                #[gated(copy_object: "copy")]
                #[on_ok(crate::metrics::object_uploaded("CopyObject"))]
                fn copy_object (CopyObjectInput) -> CopyObjectOutput;
                #[handler(op_rename_object)]
                #[gated(copy_object: "copy")]
                fn rename_object (RenameObjectInput) -> RenameObjectOutput;
                #[handler(op_create_multipart_upload)]
                #[gated(multipart: "multipart")]
                #[on_ok(crate::metrics::multipart_in_progress_inc())]
                fn create_multipart_upload (CreateMultipartUploadInput) -> CreateMultipartUploadOutput;
                #[handler(op_upload_part)]
                #[gated(multipart: "multipart")]
                fn upload_part (UploadPartInput) -> UploadPartOutput;
                #[handler(op_upload_part_copy)]
                #[gated(multipart: "multipart", copy_object: "copy")]
                fn upload_part_copy (UploadPartCopyInput) -> UploadPartCopyOutput;
                #[handler(op_complete_multipart_upload)]
                #[gated(multipart: "multipart")]
                #[on_ok({
                    crate::metrics::multipart_in_progress_dec();
                    crate::metrics::object_uploaded("CompleteMultipartUpload");
                })]
                fn complete_multipart_upload (CompleteMultipartUploadInput) -> CompleteMultipartUploadOutput;
                #[handler(op_abort_multipart_upload)]
                #[gated(multipart: "multipart")]
                #[on_ok(crate::metrics::multipart_in_progress_dec())]
                fn abort_multipart_upload (AbortMultipartUploadInput) -> AbortMultipartUploadOutput;
            }
        }
    };
}
pub(crate) use tinio_s3_ops;

/// The table read as data, for the group pins.
///
/// Two properties, deliberately built two ways:
///
/// - [`table_ops`] carries the derived `#[cfg]`, so it is what *this* build
///   compiles (measured on this table: 42 names under `--all-features`, 23
///   under `--no-default-features --features multipart`, 17 under
///   `--no-default-features`). It is a subset check against the table, never
///   an equality with a file.
/// - [`read_ops`] and [`write_ops`] carry their group unconditionally, one group
///   each: the grouping pin has to hold in every build, not only an all-features
///   one, so neither may carry the derived `#[cfg]`.
///
/// **The four are `vec![…]` literals rather than `Vec::new()` followed by `push`,
/// and the reason is that clippy reads this expansion**: `clippy::vec_init_then_push`
/// fires on the accumulate shape, and whether it does depends on the toolchain —
/// the shape was observed to redden one toolchain's `-D warnings` while every run
/// on this host stayed green, so the shape is not worth keeping. A literal with the
/// `#[cfg]` on each element builds the same table and gives the lint no shape to
/// read, on any toolchain. The element-level `#[cfg]` is long stable (measured on
/// 1.75), so the fix trusts nothing newer than the workspace already builds with.
macro_rules! op_names {
    (
        read { $( $(#[handler($rh:ident)])? $(#[gated($($rg:ident $(: $rf:literal)?),+)])? $(#[on_ok($rhh:expr)])? fn $r:ident ( $ri:ident ) -> $ro:ident; )* }
        write { $( $(#[handler($wh:ident)])? $(#[gated($($wg:ident $(: $wf:literal)?),+)])? $(#[on_ok($whh:expr)])? fn $w:ident ( $wi:ident ) -> $wo:ident; )* }
    ) => {
        /// Every row this feature set compiles.
        #[cfg(test)]
        pub(crate) fn table_ops() -> Vec<&'static str> {
            vec![
                $(
                    $( $( $(#[cfg(feature = $rf)])? )+ )?
                    stringify!($r),
                )*
                $(
                    $( $( $(#[cfg(feature = $wf)])? )+ )?
                    stringify!($w),
                )*
            ]
        }

        /// Every row, unfiltered by the build — the independent side of the
        /// read/write partition pin, which has to hold in every build.
        #[cfg(test)]
        pub(crate) fn table_rows() -> Vec<&'static str> {
            vec![
                $(
                    stringify!($r),
                )*
                $(
                    stringify!($w),
                )*
            ]
        }

        /// The read group alone, unfiltered by the build: the decorators
        /// forward these.
        #[cfg(test)]
        pub(crate) fn read_ops() -> Vec<&'static str> {
            vec![
                $(
                    stringify!($r),
                )*
            ]
        }

        /// The write group alone, unfiltered by the build: these are the
        /// operations the read-only decorator refuses.
        #[cfg(test)]
        pub(crate) fn write_ops() -> Vec<&'static str> {
            vec![
                $(
                    stringify!($w),
                )*
            ]
        }
    };
}

tinio_s3_ops!(op_names);

#[cfg(test)]
mod tests {
    use super::*;

    /// The twenty-three mutating operations, written out from the design's
    /// `Kind` column and the plan's table, in sorted order so it compares
    /// directly with the sorted group. The independent side of
    /// [`table_groups_operations_by_whether_they_mutate`], which is why it
    /// is spelled out rather than read back from the table it pins.
    const WRITE_OPS: [&str; 23] = [
        "abort_multipart_upload",
        "complete_multipart_upload",
        "copy_object",
        "create_bucket",
        "create_multipart_upload",
        "delete_bucket",
        "delete_bucket_cors",
        "delete_bucket_lifecycle",
        "delete_bucket_tagging",
        "delete_object",
        "delete_object_tagging",
        "delete_objects",
        "put_bucket_acl",
        "put_bucket_cors",
        "put_bucket_lifecycle_configuration",
        "put_bucket_tagging",
        "put_bucket_versioning",
        "put_object",
        "put_object_acl",
        "put_object_tagging",
        "rename_object",
        "upload_part",
        "upload_part_copy",
    ];

    /// The three lifecycle rows, in table order — the one group whose cargo
    /// feature is the whole fact: present with it, stripped without it. The
    /// `#[gated(…)]` feature becomes the `#[cfg]` of the generated dispatch
    /// arms, so a stripped build has no arm to answer with and s3s's trait
    /// default answers instead.
    const LIFECYCLE_OPS: [&str; 3] = [
        "delete_bucket_lifecycle",
        "get_bucket_lifecycle_configuration",
        "put_bucket_lifecycle_configuration",
    ];

    #[cfg(feature = "lifecycle")]
    #[test]
    fn the_lifecycle_rows_come_with_their_feature() {
        for op in LIFECYCLE_OPS {
            assert!(table_ops().contains(&op), "{op} is not compiled");
        }
    }

    /// The other side: the feature off is three rows fewer, not three rows
    /// answering.
    #[cfg(not(feature = "lifecycle"))]
    #[test]
    fn the_lifecycle_rows_are_stripped_without_their_feature() {
        for op in LIFECYCLE_OPS {
            assert!(!table_ops().contains(&op), "{op} is compiled");
        }
    }

    /// The table's read/write split, pinned against [`WRITE_OPS`].
    ///
    /// The generated dispatch sorts nothing: a row moved between the two
    /// groups is invisible to it (both groups generate the same forwarding
    /// arm), and the read-only decorator forwards the `read` group while
    /// refusing the `write` group — so a mutating operation misplaced into
    /// `read` would be *forwarded* in read-only mode with every other test
    /// still green. The table rows are the independent side for the names;
    /// the duplicated list above is the independent side for the
    /// classification.
    #[test]
    fn table_groups_operations_by_whether_they_mutate() {
        let mut writes = write_ops();
        writes.sort();
        assert_eq!(writes, WRITE_OPS);
        // The two groups partition the table, so the read group is exactly
        // the other nineteen rows: no row in both, none in neither. A row
        // listed twice inside one group, or in both groups, breaks the
        // sorted comparison below; a row duplicated in the table itself is
        // `table_has_no_duplicates`'s to catch.
        let mut read = read_ops();
        read.sort();
        let mut union = read.clone();
        union.extend(writes);
        union.sort();
        // Unfiltered on both sides, so the partition holds in every build —
        // not only an all-features one.
        let mut all = table_rows();
        all.sort();
        assert_eq!(union, all);
    }

    /// What this build compiles comes from the table — the decorators' own
    /// operation set, checked under every feature combination.
    #[test]
    fn compiled_ops_come_from_the_table() {
        let names = table_rows();
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
