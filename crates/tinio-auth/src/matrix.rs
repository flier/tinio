//! The operation→requirement truth table (spec §4): the pure mapping
//! from an s3s operation name to the non-owner authorization rule the
//! access checker evaluates. An op with no §4 row resolves to the
//! owner-only default (`OpRule { base: OwnerOnly, .. }`).

/// The non-owner authorization gate an operation requires: which
/// resource permission the requester must hold, or the owner-parity
/// condition that no grant can satisfy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requirement {
    /// Bucket data READ (also the `HeadBucket` gate and the lazy-owner
    /// bucket class).
    BucketRead,
    /// Bucket data WRITE.
    BucketWrite,
    /// Bucket READ_ACP.
    BucketReadAcp,
    /// Bucket WRITE_ACP.
    BucketWriteAcp,
    /// Object data READ.
    ObjectRead,
    /// Object READ_ACP.
    ObjectReadAcp,
    /// Object WRITE_ACP.
    ObjectWriteAcp,
    /// Owner parity only — no grant can satisfy the gate; also the
    /// resolved-but-unmapped default.
    OwnerOnly,
    /// Any signed principal (anonymous 403, no grant evaluation).
    Authenticated,
    /// `P == O(object) or P == O(bucket)` — creator-or-bucket-owner
    /// semantics; no ACL grant satisfies this gate.
    ObjectOrBucketOwner,
}

/// The full non-owner authorization rule for one operation: the base
/// requirement plus the overlays a bare `Requirement` cannot express.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpRule {
    /// The resource's base gate.
    pub base: Requirement,
    /// The op reads a source object: an extra source-object READ beyond
    /// the base (CopyObject, UploadPartCopy, RenameObject).
    pub reads_source: bool,
    /// The op writes into a destination bucket: an extra
    /// destination-bucket WRITE (CopyObject, RenameObject).
    pub writes_destination_bucket: bool,
    /// The op dispatches on destination-key existence: a missing key
    /// runs on `base` alone; an existing key further requires
    /// `P == O(destination object) or P == O(destination bucket)`
    /// (PutObject, PostObject, CopyObject, CompleteMultipartUpload,
    /// RenameObject).
    pub existence_dispatch: bool,
}

/// The rule for an s3s operation name (spec §4), resolved-but-unmapped
/// ops → the owner-only default.
pub fn rule_for(op: &str) -> OpRule {
    fn rule(
        base: Requirement,
        reads_source: bool,
        writes_destination_bucket: bool,
        existence_dispatch: bool,
    ) -> OpRule {
        OpRule {
            base,
            reads_source,
            writes_destination_bucket,
            existence_dispatch,
        }
    }
    use Requirement::*;
    match op {
        // Bucket-level data reads.
        "ListObjects" | "ListObjectsV2" | "ListMultipartUploads" | "HeadBucket" => {
            rule(BucketRead, false, false, false)
        }
        // Bucket-level data writes; the new-key writes dispatch on
        // destination-key existence (overwrite = delete parity).
        "PutObject" | "PostObject" => rule(BucketWrite, false, false, true),
        "CreateMultipartUpload" => rule(BucketWrite, false, false, false),
        // Bucket ACL reads/writes.
        "GetBucketAcl" => rule(BucketReadAcp, false, false, false),
        "PutBucketAcl" => rule(BucketWriteAcp, false, false, false),
        // Object data reads.
        "GetObject" | "HeadObject" => rule(ObjectRead, false, false, false),
        // Object ACL reads/writes.
        "GetObjectAcl" => rule(ObjectReadAcp, false, false, false),
        "PutObjectAcl" => rule(ObjectWriteAcp, false, false, false),
        // Root-level: any signed principal.
        "ListBuckets" | "CreateBucket" => rule(Authenticated, false, false, false),
        // Policy-only bucket permissions: owner only (tinio's
        // equivalent of the AWS "policy-only" class — the resolved
        // owner-only rows the `_` fallback also returns, listed here for
        // the record).
        //
        // Creator-or-bucket-owner / object-or-bucket-owner rows:
        // upload-scoped (creator of the upload row), delete parity
        // (DeleteObject), and the default-A tagging/attributes class.
        "ListParts" | "UploadPart" | "AbortMultipartUpload" | "DeleteObject"
        | "GetObjectTagging" | "PutObjectTagging" | "DeleteObjectTagging" | "GetObjectAttributes" => {
            rule(ObjectOrBucketOwner, false, false, false)
        }
        "UploadPartCopy" => rule(ObjectOrBucketOwner, true, false, false),
        "CompleteMultipartUpload" => rule(ObjectOrBucketOwner, false, false, true),
        // CopyObject: source READ base, source READ + destination bucket
        // WRITE overlays, destination-key existence dispatch.
        "CopyObject" => rule(ObjectRead, true, true, true),
        // RenameObject: delete-parity source, source READ + destination
        // bucket WRITE overlays, destination-key existence dispatch.
        "RenameObject" => rule(ObjectOrBucketOwner, true, true, true),
        // Resolved-but-unmapped → owner-only (default B).
        _ => rule(OwnerOnly, false, false, false),
    }
}

/// The base requirement of an operation (convenience over [`rule_for`]).
pub fn requirement_for(op: &str) -> Requirement {
    rule_for(op).base
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix_covers_the_spec_table() {
        use Requirement::*;
        // base requirement per row of spec §4 (asserting only `.base`, where compound rows
        // are asserted separately below so nothing passes for the wrong reason):
        assert_eq!(requirement_for("ListObjects"), BucketRead);
        assert_eq!(requirement_for("ListObjectsV2"), BucketRead);
        assert_eq!(requirement_for("ListMultipartUploads"), BucketRead);
        assert_eq!(requirement_for("ListParts"), ObjectOrBucketOwner);
        assert_eq!(requirement_for("PutObject"), BucketWrite);
        assert_eq!(requirement_for("PostObject"), BucketWrite);
        assert_eq!(requirement_for("CreateMultipartUpload"), BucketWrite);
        assert_eq!(requirement_for("UploadPart"), ObjectOrBucketOwner);
        assert_eq!(requirement_for("UploadPartCopy"), ObjectOrBucketOwner); // base only; source-READ asserted by rule_for below
        assert_eq!(
            requirement_for("CompleteMultipartUpload"),
            ObjectOrBucketOwner
        );
        assert_eq!(requirement_for("AbortMultipartUpload"), ObjectOrBucketOwner);
        assert_eq!(requirement_for("GetObject"), ObjectRead);
        assert_eq!(requirement_for("HeadObject"), ObjectRead);
        assert_eq!(requirement_for("CopyObject"), ObjectRead); // base only; source+dest asserted by rule_for below
        assert_eq!(requirement_for("RenameObject"), ObjectOrBucketOwner); // base only; asserted by rule_for below
        assert_eq!(requirement_for("DeleteObject"), ObjectOrBucketOwner);
        assert_eq!(requirement_for("GetBucketAcl"), BucketReadAcp);
        assert_eq!(requirement_for("PutBucketAcl"), BucketWriteAcp);
        assert_eq!(requirement_for("GetObjectAcl"), ObjectReadAcp);
        assert_eq!(requirement_for("PutObjectAcl"), ObjectWriteAcp);
        assert_eq!(requirement_for("ListBuckets"), Authenticated);
        assert_eq!(requirement_for("CreateBucket"), Authenticated);
        assert_eq!(requirement_for("HeadBucket"), BucketRead);
        assert_eq!(requirement_for("GetBucketLocation"), OwnerOnly);
        assert_eq!(requirement_for("GetBucketTagging"), OwnerOnly);
        assert_eq!(requirement_for("PutBucketTagging"), OwnerOnly);
        assert_eq!(requirement_for("DeleteBucketTagging"), OwnerOnly);
        assert_eq!(requirement_for("DeleteBucket"), OwnerOnly);
        assert_eq!(requirement_for("GetObjectTagging"), ObjectOrBucketOwner);
        assert_eq!(requirement_for("PutObjectTagging"), ObjectOrBucketOwner);
        assert_eq!(requirement_for("DeleteObjectTagging"), ObjectOrBucketOwner);
        assert_eq!(requirement_for("GetObjectAttributes"), ObjectOrBucketOwner);
        assert_eq!(requirement_for("GetBucketPolicy"), OwnerOnly); // resolved, unmapped → owner-only default B
    }

    #[test]
    fn overlay_rules_express_the_compound_and_existence_dispatch_rows() {
        // spec §4 rows that a bare Requirement cannot express — the truth table asserts these
        // so no row "passes for the wrong reason":
        let upc = rule_for("UploadPartCopy");
        assert!(upc.reads_source && upc.base == Requirement::ObjectOrBucketOwner); // source READ + initiator-only
        assert!(!upc.writes_destination_bucket && !upc.existence_dispatch);

        let copy = rule_for("CopyObject"); // source READ + destination bucket WRITE
        assert!(copy.reads_source && copy.writes_destination_bucket && copy.existence_dispatch);

        let rename = rule_for("RenameObject"); // delete-parity source + dest WRITE + dest overwrite
        assert!(
            rename.reads_source && rename.writes_destination_bucket && rename.existence_dispatch
        );

        for op in [
            "PutObject",
            "PostObject",
            "CopyObject",
            "CompleteMultipartUpload",
            "RenameObject",
        ] {
            assert!(
                rule_for(op).existence_dispatch,
                "{op} dispatches on destination-key existence"
            );
        }
        // The brief's shorthand claims the full s3s names; "Complete" names the
        // dispatching CompleteMultipartUpload (asserted above), so the non-dispatch
        // upload-scoped rows are the upload-part and abort/list rows only.
        for op in ["UploadPart", "AbortMultipartUpload", "ListParts"] {
            assert!(
                !rule_for(op).existence_dispatch && !rule_for(op).reads_source,
                "{op}"
            );
        }
    }

    #[test]
    fn unmapped_op_resolves_to_the_owner_only_default_b() {
        // A resolved-but-unmapped op (GetBucketPolicy has no §4 row) resolves to the
        // default-B rule: owner-only base and no overlays.
        assert_eq!(
            rule_for("GetBucketPolicy"),
            OpRule {
                base: Requirement::OwnerOnly,
                reads_source: false,
                writes_destination_bucket: false,
                existence_dispatch: false,
            }
        );
    }
}
