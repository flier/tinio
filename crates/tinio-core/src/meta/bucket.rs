//! The decoded bucket row: the creation time, the tag set, the owner and ACL
//! elements and the CORS configuration.
//!
//! The stored value is the five-tuple `(created_at unix nanos, tags wire, owner
//! wire, acl grants wire, cors wire)` with an element empty when the bucket has
//! none. [`Row`] is the decoded form every backend's contract implementation
//! answers; [`Wires`] is the tuple's element spelling, and the two conversions
//! here are the single home of the encode/decode rules.

use std::time::SystemTime;

use async_trait::async_trait;

use super::Metadata;
use crate::{
    acl::{self, Acl, Grant, Grantee, GroupUri, OwnerId, Permission},
    bucket, cors, object, percent,
    storage::{from_nanos, to_nanos},
};

/// A decoded bucket row.
///
/// `created` is the recorded creation time; the four optional elements are the
/// bucket's tag set, its recorded owner, its grant set and its CORS
/// configuration. `acl` carries the grant set with no owner — the owner rides
/// its own element, exactly as the store splits the two.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The recorded creation time.
    pub created: SystemTime,
    /// The bucket's tag set (empty when none).
    pub tags: object::Tags,
    /// The recorded owner element (`None` = the empty owner wire — the lazy
    /// default owner at the auth layer).
    pub owner: Option<OwnerId>,
    /// The bucket's grant set (the owner's own element rides [`Row::owner`]).
    pub acl: Acl,
    /// The CORS configuration (`None` = no configuration).
    pub cors: Option<cors::Config>,
}

impl Row {
    /// A fresh row at `created` with every optional element absent.
    pub fn at(created: SystemTime) -> Self {
        Self {
            created,
            tags: object::Tags::empty(),
            owner: None,
            acl: Acl::default_private(None),
            cors: None,
        }
    }

    /// Decode one stored value tuple.
    ///
    /// The tags, owner, grants and CORS elements self-heal individually — an
    /// empty or domain-invalid wire is "the bucket has none", the read-path
    /// discipline every stored row shares. Only the creator's own decoding can
    /// fail, and the creator never writes a malformed element.
    pub fn from_raw(created: u64, tags: &str, owner: &str, grants: &str, cors: &str) -> Self {
        Self {
            created: from_nanos(created),
            tags: decode_tags(tags),
            owner: decode_owner(owner),
            acl: decode_acl(grants),
            cors: decode_cors(cors),
        }
    }

    /// Decode a row from the contract's own values.
    ///
    /// The stored form is one ACL composite carrying the owner, so the caller
    /// passes the whole [`Acl`] and the row splits it ([`Acl::into_parts`]) the
    /// way every backend serves it: `owner` is the composite's owner and `acl`
    /// the grant set with no owner. The tags and CORS arrive as the contract
    /// values.
    pub fn from_values(
        created: SystemTime,
        tags: object::Tags,
        acl: Acl,
        cors: Option<cors::Config>,
    ) -> Self {
        let (owner, grants) = acl.into_parts();
        Self {
            created,
            tags,
            owner,
            acl: Acl::from_parts(None, grants),
            cors,
        }
    }

    /// Encode the row back into its value tuple's elements.
    pub fn to_raw(&self) -> Wires {
        Wires {
            created: to_nanos(self.created),
            tags: self.tags.to_wire(),
            owner: encode_owner(self.owner.as_ref()),
            grants: encode_grants(&self.acl),
            cors: match &self.cors {
                Some(config) => config.to_wire(),
                None => String::new(),
            },
        }
    }
}

/// The stored bucket tuple's five elements, owned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wires {
    /// The creation time in unix nanoseconds.
    pub created: u64,
    /// The tag-set wire.
    pub tags: String,
    /// The owner wire.
    pub owner: String,
    /// The ACL grants wire (the owner element excluded).
    pub grants: String,
    /// The CORS wire.
    pub cors: String,
}

/// Decode the owner element: empty or domain-invalid is `None`.
pub fn decode_owner(wire: &str) -> Option<OwnerId> {
    OwnerId::new(wire).ok()
}

/// Encode an owner element: the empty wire for no owner.
pub fn encode_owner(owner: Option<&OwnerId>) -> String {
    owner.map(OwnerId::as_str).unwrap_or_default().to_owned()
}

/// Decode the tag-set element: an empty or corrupt wire self-heals to the
/// empty set, capped at [`object::BUCKET_TAGS_MAX`]. The **one home** of the
/// bucket-tag decode rule, shared by [`Row::from_raw`] and the table crate's
/// tag accessor — the bucket-cap sibling of the object row's
/// `OBJECT_TAGS_MAX` decode.
pub fn decode_tags(wire: &str) -> object::Tags {
    object::Tags::from_wire_limited(wire, object::BUCKET_TAGS_MAX)
}

/// Decode the row's grants element into the [`Acl`] the row serves: the owner
/// rides its own element ([`decode_owner`]), so the decoded ACL carries the
/// grants only — the absent owner beside them is [`Acl::from_parts`], the one
/// home of the row's split. Every row module calls this rather than
/// re-deriving it.
pub fn decode_acl(grants: &str) -> Acl {
    Acl::from_parts(None, decode_grants(grants).unwrap_or_default())
}

/// Encode an [`Acl`]'s grants element. The owner is not part of this wire —
/// it rides its own element ([`encode_owner`]) — so this is the grants half of
/// the row's ACL encoding.
pub fn encode_grants(acl: &Acl) -> String {
    acl.to_grants_wire()
}

/// Decode the grants element. `None` is a domain-invalid wire; the caller
/// self-heals it to the empty set. The decode is strict — unlike
/// [`Acl::from_grants_wire`], which collapses an invalid wire into the private
/// default — so a caller can tell "no grants" from "a corrupt element".
pub fn decode_grants(wire: &str) -> Option<acl::AclGrants> {
    let mut grants = Vec::new();
    for part in wire.split('&').filter(|part| !part.is_empty()) {
        let (grantee, permission) = part.split_once(',')?;
        let grantee = if let Some(id) = grantee.strip_prefix("id=") {
            Grantee::Canonical(OwnerId::new(id).ok()?)
        } else {
            let raw = grantee.strip_prefix("uri=")?;
            let uri = GroupUri(percent::decode(raw));
            Grantee::Group(uri.is_valid().then_some(uri)?)
        };
        grants.push(Grant {
            grantee,
            permission: Permission::from_wire(permission).ok()?,
        });
    }
    if grants.len() > acl::ACL_GRANTS_MAX {
        return None;
    }
    Some(grants)
}

/// Decode the CORS element: empty, corrupt or a configuration with no rules is
/// "no configuration" (`None`).
pub fn decode_cors(wire: &str) -> Option<cors::Config> {
    let config = cors::Config::from_wire(wire);
    (!config.rules.is_empty()).then_some(config)
}

/// Bucket rows: the creation time, the tag set, the owner and ACL elements and
/// the CORS configuration.
///
/// # Examples
///
/// ```ignore
/// use std::time::SystemTime;
///
/// use tinio_core::{acl::Acl, bucket, meta::{BucketMeta, Metadata}};
///
/// async fn record<M: BucketMeta + Metadata>(store: &M, bucket: &bucket::Name) {
///     store.record(bucket, SystemTime::now()).await.unwrap();
///     store
///         .set_acl(bucket, None, &Acl::default_private(None).grants)
///         .await
///         .unwrap();
///     assert!(store.row(bucket).await.unwrap().is_some());
/// }
/// ```
#[async_trait]
pub trait BucketMeta: Send + Sync + 'static {
    /// The bucket's creation time as recorded, or `None` when the bucket row
    /// does not exist.
    async fn created_at(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Option<SystemTime>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// The creation times of `buckets`, in the input's order; a `None` entry
    /// is a bucket with no row.
    async fn load_many(
        &self,
        buckets: &[bucket::Name],
    ) -> Result<Vec<Option<SystemTime>>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Every recorded bucket and its creation time, in ascending name order.
    async fn load_all(&self) -> Result<Vec<(String, SystemTime)>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// The bucket's whole row, or `None` when it has none.
    async fn row(&self, bucket: &bucket::Name) -> Result<Option<Row>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// The recorded owner elements of `buckets`, in the input's order.
    async fn owners(
        &self,
        buckets: &[bucket::Name],
    ) -> Result<Vec<Option<acl::OwnerId>>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Record the bucket's creation time, clearing the row's other elements.
    async fn record(
        &self,
        bucket: &bucket::Name,
        created: SystemTime,
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Insert `row` **or refuse**: a bucket that already has a row is
    /// [`crate::storage::Error::AlreadyExists`], decided inside the transaction so a
    /// concurrent create cannot be raced (review item 10 — `create_bucket`'s
    /// already-exists outcome had no atomic home). `record_full` is the
    /// unconditional upsert.
    async fn create(
        &self,
        bucket: &bucket::Name,
        row: &Row,
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Insert the row at `now` when absent and answer the stored creation
    /// time; an existing row's other elements ride untouched.
    async fn record_if_absent(
        &self,
        bucket: &bucket::Name,
        now: SystemTime,
    ) -> Result<SystemTime, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Upsert the whole row.
    async fn record_full(
        &self,
        bucket: &bucket::Name,
        row: &Row,
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Replace the tag set. `Ok(false)` is a bucket with no row (nothing was
    /// written); `Ok(true)` a recorded set. The presence decision happens
    /// inside the transaction, never as a pre-read (review item 9, D8).
    async fn set_tags(
        &self,
        bucket: &bucket::Name,
        tags: &object::Tags,
    ) -> Result<bool, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Replace the tag set, inserting a first-sight row at `now` when the
    /// bucket has none — the create-or-update twin of
    /// [`BucketMeta::set_tags`], the read (or the insert) and the write
    /// decided and committed in **one transaction**, so a reader never
    /// observes the row without its tags. An existing row's other elements
    /// (including its creation time) ride untouched.
    ///
    /// The lazy first-sight policy of a backend whose buckets outlive their
    /// rows (`tinio-fs`'s pre-existing directory): there a missing row is
    /// not an error, so the element write also records the bucket.
    ///
    /// An unchanged write is a no-op: `Ok(())` means the bucket records
    /// `tags`, and an implementation whose stored set already equals the
    /// argument may commit nothing (no write, no fsync) — the observable
    /// state is the same either way, so a caller reads `Ok(())` as "the
    /// stored tag set is `tags`" and never as a change signal.
    async fn set_tags_or_create(
        &self,
        bucket: &bucket::Name,
        tags: &object::Tags,
        now: SystemTime,
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Clear the tag set (idempotent). Same presence outcome as
    /// [`BucketMeta::set_tags`] — including `Ok(true)` for a row that had no
    /// tags and needed no change.
    async fn clear_tags(&self, bucket: &bucket::Name) -> Result<bool, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Replace the owner and the grant set. Same presence outcome as
    /// [`BucketMeta::set_tags`].
    async fn set_acl(
        &self,
        bucket: &bucket::Name,
        owner: Option<&acl::OwnerId>,
        grants: &acl::AclGrants,
    ) -> Result<bool, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Replace the grant set **preserving the recorded owner element**,
    /// inserting a first-sight row at `now` when the bucket has none — the
    /// create-or-update twin of [`BucketMeta::set_acl`] for a caller that
    /// changes the grants only, the row read (or the insert) and the write
    /// decided and committed in **one transaction**.
    ///
    /// The owner element is read and carried over inside that transaction,
    /// so the owner's own writer cannot slip between a read and a write.
    /// A first-sight row carries the grant set and **no owner** — a bucket
    /// the API never sighted has none to carry (the lazy first-sight
    /// policy); an existing row's creation time and other elements ride
    /// untouched.
    ///
    /// An unchanged write is a no-op, as for
    /// [`BucketMeta::set_tags_or_create`]: `Ok(())` means the bucket records
    /// `grants`, and an implementation whose stored set already equals the
    /// argument may commit nothing — the observable state is the same
    /// either way.
    ///
    /// An **empty** `grants` set is recorded like any other: a first-sight
    /// row is still inserted at `now`, so the bucket becomes recorded (a
    /// later [`BucketMeta::created_at`] answers `Some`), but its ACL element
    /// is the empty wire — [`BucketMeta::row`] then cannot tell that bucket
    /// apart from one that never had grants. A caller that must distinguish
    /// the two writes a non-empty set, or reads
    /// [`BucketMeta::row`]/[`BucketMeta::created_at`] before deciding.
    async fn set_grants_or_create(
        &self,
        bucket: &bucket::Name,
        grants: &acl::AclGrants,
        now: SystemTime,
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Replace the CORS configuration. Same presence outcome as
    /// [`BucketMeta::set_tags`].
    async fn set_cors(
        &self,
        bucket: &bucket::Name,
        config: &cors::Config,
    ) -> Result<bool, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Clear the CORS configuration (idempotent). Same presence outcome as
    /// [`BucketMeta::set_tags`].
    async fn clear_cors(&self, bucket: &bucket::Name) -> Result<bool, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Remove the bucket row (idempotent).
    async fn remove(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Remove the bucket row only when no object row and no upload row names
    /// it: the TOCTOU-free delete. A missing row projects to
    /// [`crate::storage::Error::NoSuchBucket`], a non-empty bucket to
    /// [`crate::storage::Error::NotEmpty`].
    async fn remove_if_empty(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_bucket_row_round_trips_through_its_wires() {
        let owner =
            OwnerId::new("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
                .unwrap();
        let row = Row {
            // A 100 ns tick: the Windows `SystemTime` resolution, so the
            // stored nanosecond value round-trips on every platform.
            created: SystemTime::UNIX_EPOCH + Duration::from_nanos(700),
            tags: object::Tags::from_pairs_limited(
                [("env".to_owned(), "prod".to_owned())],
                object::BUCKET_TAGS_MAX,
            )
            .unwrap(),
            owner: Some(owner.clone()),
            acl: Acl {
                owner: None,
                grants: vec![Grant {
                    grantee: Grantee::Canonical(owner.clone()),
                    permission: Permission::FullControl,
                }],
            },
            cors: Some(cors::Config {
                rules: vec![cors::Rule {
                    id: Some("one".into()),
                    allowed_methods: vec!["GET".into()],
                    allowed_origins: vec!["*".into()],
                    allowed_headers: None,
                    expose_headers: None,
                    max_age_seconds: None,
                }],
            }),
        };
        let wires = row.to_raw();
        assert_eq!(wires.created, 700);
        assert_eq!(
            Row::from_raw(
                wires.created,
                &wires.tags,
                &wires.owner,
                &wires.grants,
                &wires.cors
            ),
            row
        );
    }

    #[test]
    fn every_corrupt_element_self_heals_independently() {
        let row = Row::from_raw(
            3,
            "team=%zz&",
            "not-a-canonical-id",
            "id=short,READ",
            "\\bad",
        );
        assert_eq!(row.created, from_nanos(3));
        assert!(row.tags.is_empty(), "a corrupt tag wire is no tag set");
        assert_eq!(row.owner, None, "a corrupt owner wire is no owner");
        assert!(
            row.acl.grants.is_empty(),
            "a corrupt grants wire is no grant set"
        );
        assert_eq!(row.cors, None, "a corrupt CORS wire is no configuration");
    }

    #[test]
    fn an_empty_bucket_row_carries_no_optional_element() {
        let row = Row::from_raw(0, "", "", "", "");
        assert_eq!(row, Row::at(SystemTime::UNIX_EPOCH));
        assert_eq!(row.to_raw().created, 0);
    }

    #[test]
    fn the_grants_decode_is_strict_about_what_it_cannot_read() {
        assert_eq!(decode_grants(""), Some(Vec::new()));
        assert_eq!(decode_grants("nope,READ"), None);
        assert_eq!(decode_grants("id=aabb,READ"), None, "not a canonical id");
        assert_eq!(decode_grants("uri=http%3A%2F%2Fevildomain%2F,READ"), None);
        assert_eq!(
            decode_grants(
                "id=aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899,READ"
            )
            .unwrap()
            .len(),
            1
        );
    }

    #[test]
    fn the_named_tags_decode_is_the_row_rule() {
        // `decode_tags` is the rule `Row::from_raw` inlined before this
        // function existed (plan Task 1 Step 4, "move the decode helpers
        // into `meta`"): the table crate's tag accessor calls it so the
        // bucket-tag rule has one home. The declared equality is the pin.
        for wire in ["", "env=prod", "team=core&env=prod", "novalue"] {
            assert_eq!(
                decode_tags(wire),
                object::Tags::from_wire_limited(wire, object::BUCKET_TAGS_MAX),
                "{wire:?}"
            );
        }
        // An empty or malformed wire self-heals to the empty set.
        assert!(decode_tags("").is_empty());
        assert!(decode_tags("no-equals-sign").is_empty());
        // And the row decode answers exactly the named function's set.
        assert_eq!(
            Row::from_raw(0, "env=prod", "", "", "").tags,
            decode_tags("env=prod")
        );
    }
}
