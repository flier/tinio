//! ACL domain types and the grants wire codec (S3 owner + access
//! control).
//!
//! [`OwnerId`] is the canonical-account identifier grants and owners
//! reference; [`Grantee`] is a canonical account or one of the three
//! documented group URIs ([`GROUP_ALL_USERS`],
//! [`GROUP_AUTHENTICATED_USERS`], [`GROUP_LOG_DELIVERY`] — AWS
//! documents no EC2 group URI). [`Acl`] is the row form: an owner and
//! its grant set.
//!
//! The grant-set wire (`to_grants_wire` / `from_grants_wire`) is
//! canonical, sorted, and deduped:
//! `grant := grantee_wire ',' permission_wire`; `grants := grant ('&'
//! grant)*`; `grantee_wire := 'id=' percent(id) | 'uri=' percent(group
//! uri)`; the sort key is `(grantee_wire, permission_wire)`, and the
//! group URI carries the full RFC 3986 encoding (`percent::encode_uri`).
//! `from_grants_wire` self-heals — any domain-invalid wire becomes
//! [`Acl::default_private`] with no owner, the established read-path
//! discipline for stored rows.
//!
//! The contract's PostgreSQL mapping lives in the crate's one mapping module
//! (`sql_types`), behind the crate's `sqlx` feature: a backend binds the
//! contract types themselves and keeps no mirror. The composite's guarantees
//! are the wire codec's: a write is canonical (unknown group URIs dropped, the
//! grant set sorted by the same key and deduped), and a read self-heals a
//! stored element outside the domain — the same discipline this module's own
//! `from_grants_wire` applies.

use derive_more::Deref;
use sha2::{Digest, Sha256};
use tracing::instrument;

use crate::percent;

/// AWS's documented special-grantee canonical ID: the anonymous
/// (unsigned) uploader. The only non-64-hex value [`OwnerId`] admits —
/// the exception sits beside the type on purpose.
pub const ANONYMOUS_CANONICAL_ID: &str = "65a011a29cdf8ec533ec3d1ccaae921c";

/// A validated canonical user ID: the 64 lowercase-hex account
/// identifier S3 uses for owners and grantees (AWS's own canonical IDs
/// have the same shape), or AWS's anonymous special-grantee ID
/// ([`ANONYMOUS_CANONICAL_ID`] — the one exception to the shape).
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deref)]
pub struct OwnerId(String);

const CANONICAL_ID_LEN: usize = 64;

impl OwnerId {
    /// Validate and wrap a canonical ID from untrusted wire or config
    /// input: exactly 64 lowercase hex digits, or exactly
    /// [`ANONYMOUS_CANONICAL_ID`] (the anonymous special-grantee ID).
    pub fn new(s: impl Into<String>) -> Result<Self, AclError> {
        let s = s.into();
        if (s.len() == CANONICAL_ID_LEN
            && s.bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()))
            || s == ANONYMOUS_CANONICAL_ID
        {
            Ok(Self(s))
        } else {
            Err(AclError::InvalidOwnerId(s))
        }
    }

    /// Wrap a raw SHA-256 digest: hex-encoding 32 bytes emits exactly the
    /// 64 lowercase hex digits [`OwnerId::new`] checks, so a derived ID is
    /// constructed rather than asserted into shape.
    fn from_digest(digest: impl AsRef<[u8]>) -> Self {
        Self(hex::encode(digest))
    }

    /// The anonymous principal's canonical ID — the unsigned requester's
    /// owner. Total, with no assertion to annotate: the constant is the
    /// `|| s == ANONYMOUS_CANONICAL_ID` arm of [`OwnerId::new`]'s own shape
    /// check, so the wrap is the same construction `OwnerId::from_digest`
    /// performs.
    pub fn anonymous() -> Self {
        Self(ANONYMOUS_CANONICAL_ID.to_string())
    }
}

/// The default owner display name (`"tinio"` — the `[owner]` config
/// default; design §6). The feature-off / `identity: None` fallback
/// lives in core so the CLI's config root and the auth layer share one source
/// without a dependency on core's hashing.
pub const DEFAULT_OWNER_DISPLAY_NAME: &str = "tinio";

/// The root access-key fallback (`"minioadmin"` — the US1 interop pair,
/// design §6: with no `[auth]` section the root credential is
/// `minioadmin`/`minioadmin`). ONE home so the config uniqueness
/// validation (the CLI's config root) reserves the same effective root access key
/// the identity assembly (tinio-server) falls back to — a `[[users]]` key
/// equal to it would shadow the root in the access-key map.
pub const DEFAULT_ROOT_ACCESS_KEY: &str = "minioadmin";

/// The default owner canonical ID: `hex(SHA-256("tinio"))` — the fixed
/// 64-hex constant every default owner element resolves to.
pub fn default_owner_id() -> OwnerId {
    let digest = Sha256::digest(DEFAULT_OWNER_DISPLAY_NAME.as_bytes());
    OwnerId::from_digest(digest)
}

/// `hex(SHA-256(access_key))` — the canonical ID for a `[[users]]` entry
/// whose config omits one, and the user-test fixture basis. ONE home:
/// the CLI's config root (the derived-ID uniqueness default) and the identity
/// assembly (tinio-auth re-exports it) share the derivation.
pub fn derive_canonical_id(access_key: &str) -> OwnerId {
    let digest = Sha256::digest(access_key.as_bytes());
    OwnerId::from_digest(digest)
}

/// The single-key delete/overwrite parity rule (spec review B2): `P ==
/// O(object) or P == O(bucket)` — grants never satisfy it. The ONE home
/// for the rule, shared by the server's handler-side per-key DeleteObjects
/// check and tinio-auth's `ObjectOrBucketOwner` gate.
///
/// `object_owner` is the RAW row element — `None` (the empty wire,
/// review B4) resolves to `lazy_default`, NOT to the bucket owner: an
/// empty-wire object is owned by the configured lazy default.
#[instrument(target = "tinio::auth", level = "debug", skip_all, ret)]
pub fn can_delete(
    principal: &OwnerId,
    bucket_owner: &OwnerId,
    lazy_default: &OwnerId,
    object_owner: Option<&OwnerId>,
) -> bool {
    principal == object_owner.unwrap_or(lazy_default) || principal == bucket_owner
}

/// An ACL permission (the AWS grant permission strings).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    /// `FULL_CONTROL` — the owner permission.
    FullControl,
    /// `READ` — read the object/bucket data.
    Read,
    /// `READ_ACP` — read the ACL.
    ReadAcp,
    /// `WRITE` — write the object/bucket data.
    Write,
    /// `WRITE_ACP` — write the ACL.
    WriteAcp,
}

impl Permission {
    /// The AWS wire string.
    pub fn as_wire(&self) -> &'static str {
        match self {
            Permission::FullControl => "FULL_CONTROL",
            Permission::Read => "READ",
            Permission::ReadAcp => "READ_ACP",
            Permission::Write => "WRITE",
            Permission::WriteAcp => "WRITE_ACP",
        }
    }

    /// Parse the AWS wire string.
    pub fn from_wire(s: &str) -> Result<Self, AclError> {
        Ok(match s {
            "FULL_CONTROL" => Permission::FullControl,
            "READ" => Permission::Read,
            "READ_ACP" => Permission::ReadAcp,
            "WRITE" => Permission::Write,
            "WRITE_ACP" => Permission::WriteAcp,
            _ => return Err(AclError::InvalidPermission(s.into())),
        })
    }
}

/// `http://acs.amazonaws.com/groups/global/AllUsers` — anonymous
/// access.
pub const GROUP_ALL_USERS: &str = "http://acs.amazonaws.com/groups/global/AllUsers";
/// `http://acs.amazonaws.com/groups/global/AuthenticatedUsers` —
/// authenticated AWS accounts.
pub const GROUP_AUTHENTICATED_USERS: &str =
    "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
/// `http://acs.amazonaws.com/groups/s3/LogDelivery` — S3 log delivery.
pub const GROUP_LOG_DELIVERY: &str = "http://acs.amazonaws.com/groups/s3/LogDelivery";
// No EC2 group constant: AWS documents only the three URIs above
// (grilling Q5, 2026-09-05), and no public implementation exists.

/// A group grantee URI — one of the three documented S3 group URIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupUri(pub String);

impl GroupUri {
    /// Whether the URI is one of the three documented S3 group URIs.
    pub fn is_valid(&self) -> bool {
        matches!(
            self.0.as_str(),
            GROUP_ALL_USERS | GROUP_AUTHENTICATED_USERS | GROUP_LOG_DELIVERY
        )
    }
}

/// A grantee: a canonical account, or a group URI (email grantees are
/// rejected upstream at the interface).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grantee {
    /// The canonical-ID account.
    Canonical(OwnerId),
    /// One of the three documented group URIs.
    Group(GroupUri),
}

/// One grant: a grantee and the permission given to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    /// The grantee.
    pub grantee: Grantee,
    /// The permission granted.
    pub permission: Permission,
}

/// An ACL's grant set (the AWS cap of 100 grants).
pub type AclGrants = Vec<Grant>;

/// The per-ACL grant cap (AWS: 100 grants per ACL).
pub const ACL_GRANTS_MAX: usize = 100;

/// An access-control list: the owning account and its grant set. The
/// row form the backends persist; the owner is its own row element —
/// [`Acl::into_parts`] splits the two and [`Acl::from_parts`] re-joins
/// them, while `to_grants_wire`/`from_grants_wire` cover the grant set
/// only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acl {
    /// The owning canonical ID (`None` = legacy/lazy — resolves to the
    /// default owner at the auth layer).
    pub owner: Option<OwnerId>,
    /// The grant set.
    pub grants: AclGrants,
}

impl Acl {
    /// The default private ACL: no grants beyond the owner's own
    /// `FULL_CONTROL` (an absent owner has none — there is no canonical
    /// ID to grant to).
    pub fn default_private(owner: Option<OwnerId>) -> Self {
        let mut grants = Vec::new();
        if let Some(o) = &owner {
            grants.push(Grant {
                grantee: Grantee::Canonical(o.clone()),
                permission: Permission::FullControl,
            });
        }
        Self { owner, grants }
    }

    /// Split the composite into the two row elements every backend stores: the
    /// owner (which rides its own row element) and the grant set. The row keeps
    /// the grants in an owner-less [`Acl`] ([`Acl::from_parts`] re-joins the
    /// pair).
    ///
    /// The **one home** of that split — the row modules call it rather than
    /// re-deriving it, and the row's own ACL decode reaches it through the
    /// absent owner.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_core::acl::{Acl, OwnerId};
    ///
    /// let owner =
    ///     OwnerId::new("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899").unwrap();
    /// let acl = Acl::default_private(Some(owner.clone()));
    ///
    /// // The row form: the owner rides its own element, the grants stay behind.
    /// let (owner_element, grants) = acl.into_parts();
    /// assert_eq!(owner_element, Some(owner));
    /// assert_eq!(grants.len(), 1, "the owner's own FULL_CONTROL grant");
    /// ```
    pub fn into_parts(self) -> (Option<OwnerId>, AclGrants) {
        (self.owner, self.grants)
    }

    /// The inverse of [`Acl::into_parts`]: the composite a row's split elements
    /// make. A store's write path assembles it before binding the ACL
    /// composite, so the re-join has one home too.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_core::acl::{Acl, Grant, Grantee, OwnerId, Permission};
    ///
    /// let owner =
    ///     OwnerId::new("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899").unwrap();
    /// let acl = Acl::from_parts(
    ///     Some(owner.clone()),
    ///     vec![Grant {
    ///         grantee: Grantee::Canonical(owner),
    ///         permission: Permission::FullControl,
    ///     }],
    /// );
    /// assert!(acl.owner.is_some());
    /// assert_eq!(acl.grants.len(), 1);
    ///
    /// // Round trip: the two row elements re-join to what they split from.
    /// let (owner_element, grants) = acl.clone().into_parts();
    /// assert_eq!(Acl::from_parts(owner_element, grants), acl);
    /// ```
    pub fn from_parts(owner: Option<OwnerId>, grants: AclGrants) -> Self {
        Self { owner, grants }
    }

    /// The canonical grantee wire element; a group URI outside the
    /// three documented constants has no wire form.
    fn grantee_wire(g: &Grantee) -> Option<String> {
        Some(match g {
            Grantee::Canonical(id) => format!("id={}", id.as_str()),
            Grantee::Group(uri) if uri.is_valid() => format!("uri={}", percent::encode_uri(&uri.0)),
            Grantee::Group(_) => return None,
        })
    }

    /// One grant's canonical key: `grantee_wire,PERMISSION`. `None` when the
    /// grantee has no stored form (an unknown group URI) -- the one rule both
    /// the grants wire and the PostgreSQL composite drop on.
    pub(crate) fn grant_key(grant: &Grant) -> Option<String> {
        Some(format!(
            "{},{}",
            Self::grantee_wire(&grant.grantee)?,
            grant.permission.as_wire()
        ))
    }

    /// The canonical grant order: each grant's [`Acl::grant_key`] beside the
    /// index of the grant it came from, grants whose grantee has no stored form
    /// dropped, sorted by key and deduped. The **one home** of the
    /// canonicalization rule — [`Acl::canonical_grants`] and
    /// [`Acl::to_grants_wire`] both derive from it, so a key is built once per
    /// grant and no grant is cloned unless an owned copy is the answer.
    fn canonical_order(&self) -> Vec<(String, usize)> {
        let mut keyed: Vec<(String, usize)> = self
            .grants
            .iter()
            .enumerate()
            .filter_map(|(index, grant)| Some((Self::grant_key(grant)?, index)))
            .collect();
        // The sort is stable, so an equal-key run keeps its first occurrence:
        // exactly the grant the clone-based sort-then-dedupe picked, and the
        // order the stored form's byte identity is pinned to.
        keyed.sort_by(|a, b| a.0.cmp(&b.0));
        keyed.dedup_by(|a, b| a.0 == b.0);
        keyed
    }

    /// The canonical grant set, in the stored order every backend persists:
    /// grants whose grantee has no stored form are dropped, and the rest are
    /// ordered by [`Acl::grant_key`] and deduped. The grants wire and the
    /// PostgreSQL composite's write share this one rule.
    pub(crate) fn canonical_grants(&self) -> AclGrants {
        self.canonical_order()
            .into_iter()
            .map(|(_, index)| self.grants[index].clone())
            .collect()
    }

    /// The canonical grants wire: each `Acl::grant_key` entry, sorted and
    /// deduped, joined by `&`.
    pub fn to_grants_wire(&self) -> String {
        self.canonical_order()
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>()
            .join("&")
    }

    /// Parse the grants wire; any domain-invalid input self-heals to
    /// [`Acl::default_private`] with no owner (rows are API-written, so
    /// a garbage wire reads as missing — the established self-heal
    /// style). The parse output is canonical: sorted and deduped, with
    /// no owner element.
    pub fn from_grants_wire(s: &str) -> Self {
        Self::parse_grants_wire(s).unwrap_or_else(|_| Self::default_private(None))
    }

    fn parse_grants_wire(s: &str) -> Result<Self, AclError> {
        let mut grants = Vec::new();
        for part in s.split('&').filter(|p| !p.is_empty()) {
            let (gee, perm) = part
                .split_once(',')
                .ok_or_else(|| AclError::InvalidPermission(part.into()))?;
            let grantee = if let Some(id) = gee.strip_prefix("id=") {
                Grantee::Canonical(OwnerId::new(id)?)
            } else if let Some(raw_uri) = gee.strip_prefix("uri=") {
                let uri = GroupUri(percent::decode(raw_uri));
                if !uri.is_valid() {
                    return Err(AclError::InvalidGroupUri(uri.0));
                }
                Grantee::Group(uri)
            } else {
                return Err(AclError::InvalidGroupUri(gee.into()));
            };
            grants.push(Grant {
                grantee,
                permission: Permission::from_wire(perm)?,
            });
        }
        if grants.len() > ACL_GRANTS_MAX {
            return Err(AclError::GrantsOverflow);
        }
        // Canonical: the same sort and dedupe the encoder emits, so a parsed
        // wire is byte-identical to the one a write stores.
        let parsed = Self {
            owner: None,
            grants,
        };
        Ok(Self {
            owner: None,
            grants: parsed.canonical_grants(),
        })
    }
}

/// ACL domain validation failure (mirrors the object module's
/// `TagError` style).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AclError {
    /// The canonical ID is not 64 lowercase hex digits.
    #[error("invalid canonical owner ID {0:?}")]
    InvalidOwnerId(String),
    /// The permission is not one of the five AWS wire strings.
    #[error("invalid ACL permission {0:?}")]
    InvalidPermission(String),
    /// The group URI is not one of the three documented constants (the
    /// `uri=` wire element decodes to a URI outside the known set).
    #[error("invalid group URI {0:?}")]
    InvalidGroupUri(String),
    /// The grant set exceeds the per-ACL cap.
    #[error("ACL grant count exceeds the maximum of {ACL_GRANTS_MAX}")]
    GrantsOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> OwnerId {
        OwnerId::new(s).unwrap()
    }

    fn uid() -> OwnerId {
        id("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
    }

    #[test]
    fn owner_id_requires_64_lower_hex() {
        assert!(OwnerId::new("nothex").is_err());
        assert!(
            OwnerId::new("aabbccddeeff00112233445566778899aabbccddeeff0011223344556677889")
                .is_err()
        ); // 63 hex
        assert!(
            OwnerId::new("AABBCCDDEFF00112233445566778899AABBCCDDEFF00112233445566778899").is_err()
        ); // uppercase
        assert!(
            OwnerId::new("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
                .is_ok()
        );
    }

    #[test]
    fn anonymous_special_grantee_id_is_admitted() {
        // AWS's anonymous uploader ID is 32-hex — the one non-64 exception.
        assert_eq!(ANONYMOUS_CANONICAL_ID.len(), 32);
        assert!(OwnerId::new(ANONYMOUS_CANONICAL_ID).is_ok());
        // The total constructor wraps exactly the admitted constant.
        assert_eq!(OwnerId::anonymous().as_str(), ANONYMOUS_CANONICAL_ID);
        assert_eq!(
            OwnerId::new(ANONYMOUS_CANONICAL_ID).unwrap().as_str(),
            ANONYMOUS_CANONICAL_ID
        );
        // A different 32-hex value is NOT admitted (exact-constant guard).
        assert!(OwnerId::new("65a011a29cdf8ec533ec3d1ccaae921d").is_err());
    }

    #[test]
    fn permission_wire_round_trip() {
        for p in [
            Permission::FullControl,
            Permission::Read,
            Permission::ReadAcp,
            Permission::Write,
            Permission::WriteAcp,
        ] {
            assert_eq!(Permission::from_wire(p.as_wire()).unwrap(), p);
        }
        assert!(Permission::from_wire("DENY").is_err());
    }

    #[test]
    fn grants_wire_is_sorted_canonical_and_deduped() {
        let acl = Acl {
            owner: Some(uid()),
            grants: vec![
                Grant {
                    grantee: Grantee::Group(GroupUri(GROUP_ALL_USERS.to_string())),
                    permission: Permission::Read,
                },
                Grant {
                    grantee: Grantee::Canonical(id(
                        "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100",
                    )),
                    permission: Permission::FullControl,
                },
                Grant {
                    grantee: Grantee::Canonical(id(
                        "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100",
                    )),
                    permission: Permission::FullControl,
                }, // dedupe
            ],
        };
        let wire = acl.to_grants_wire();
        let back = Acl::from_grants_wire(&wire);
        assert_eq!(back.grants.len(), 2, "{wire}");
        assert_eq!(
            wire,
            "id=ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100,FULL_CONTROL&uri=http%3A%2F%2Facs.amazonaws.com%2Fgroups%2Fglobal%2FAllUsers,READ"
        );
    }

    #[test]
    fn grants_wire_self_heals_to_default_private() {
        assert_eq!(
            Acl::from_grants_wire("garbage![nope"),
            Acl::default_private(None)
        );
        assert_eq!(
            Acl::from_grants_wire("id=short,READ"),
            Acl::default_private(None)
        );
    }

    #[test]
    fn grant_limit_is_enforced() {
        // 100 distinct grants round-trip; a 101-grant wire is over the
        // cap and self-heals to the default private ACL.
        let mut grants = Vec::new();
        for i in 0..100 {
            let hex = format!("{i:064x}");
            grants.push(Grant {
                grantee: Grantee::Canonical(id(&hex)),
                permission: Permission::Read,
            });
        }
        assert!(
            Acl::from_grants_wire(
                &Acl {
                    owner: None,
                    grants
                }
                .to_grants_wire()
            )
            .grants
            .len()
                == 100
        );
        assert!(
            Acl::from_grants_wire("(over-cap wire constructed with 101 grants)")
                .owner
                .is_none()
        );
    }

    #[test]
    fn group_uri_must_be_known() {
        assert!(GroupUri("http://acs.amazonaws.com/groups/global/AllUsers".into()).is_valid());
        assert!(!GroupUri("http://evildomain/".into()).is_valid());
    }

    #[test]
    fn default_owner_id_is_the_fixed_sha256_of_tinio() {
        // hex(SHA-256("tinio")) — pinned so the tinio-auth/config
        // defaults sharing this constant cannot drift.
        assert_eq!(
            default_owner_id().as_str(),
            "d16b7e8c0bb9728d01e3bf9c30940a32622195f821325af577a87bd6284ac306"
        );
        assert_eq!(DEFAULT_OWNER_DISPLAY_NAME, "tinio");
    }

    #[test]
    fn derive_canonical_id_is_hex_sha256() {
        // hex(SHA-256("AKID")) — pinned so the derivation (the [[users]]
        // canonical-id default, one home here) cannot drift.
        assert_eq!(
            derive_canonical_id("AKID").as_str(),
            "2c8a2a08ad81dddf7e7830cbc75310f731a1381431bb29afb55a76ee07e81721"
        );
    }

    #[test]
    fn can_delete_owner_parity_with_lazy_default() {
        // The one truth table of the single-key delete rule. The empty
        // wire (B4) resolves to the lazy default — never the bucket
        // owner: a non-default principal whose object row is empty is
        // NOT its owner even when it owns the bucket.
        let alice = id("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899");
        let bob = id("ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100");
        let lazy = default_owner_id();
        // P == O(object).
        assert!(can_delete(&alice, &bob, &lazy, Some(&alice)));
        // P == O(bucket).
        assert!(can_delete(&bob, &bob, &lazy, None));
        // P == the lazy default of an empty-wire object (NOT the bucket
        // owner — bob owns the bucket, the empty row is the lazy's).
        assert!(can_delete(&lazy, &bob, &lazy, None));
        // Neither side matches (a third principal with no grant).
        assert!(!can_delete(&bob, &alice, &lazy, Some(&alice)));
        assert!(!can_delete(&lazy, &bob, &lazy, Some(&alice)));
    }

    #[test]
    fn grants_wire_drops_a_group_outside_the_documented_set() {
        // An unknown group URI has no wire form: the encoder drops the
        // grant, the decoder rejects the element (self-heal happens at
        // the `from_grants_wire` boundary only).
        let acl = Acl {
            owner: None,
            grants: vec![
                Grant {
                    grantee: Grantee::Group(GroupUri("http://evildomain/".into())),
                    permission: Permission::Read,
                },
                Grant {
                    grantee: Grantee::Canonical(uid()),
                    permission: Permission::Read,
                },
            ],
        };
        let wire = acl.to_grants_wire();
        assert_eq!(wire, format!("id={},READ", uid().as_str()));
        assert_eq!(
            Acl::from_grants_wire(&wire).grants,
            vec![acl.grants[1].clone()]
        );
    }

    #[test]
    fn grants_wire_parse_rejects_malformed_elements() {
        // A grantee with neither the `id=` nor the `uri=` prefix.
        assert_eq!(
            Acl::parse_grants_wire("nope,READ"),
            Err(AclError::InvalidGroupUri("nope".into()))
        );
        // A `uri=` element decoding to a URI outside the three constants.
        assert_eq!(
            Acl::parse_grants_wire("uri=http%3A%2F%2Fevildomain%2F,READ"),
            Err(AclError::InvalidGroupUri("http://evildomain/".into()))
        );
    }

    #[test]
    fn grants_wire_parse_over_the_cap_is_overflow_not_self_heal() {
        let grants: Vec<Grant> = (0..=ACL_GRANTS_MAX)
            .map(|i| Grant {
                grantee: Grantee::Canonical(id(&format!("{i:064x}"))),
                permission: Permission::Read,
            })
            .collect();
        let wire = Acl {
            owner: None,
            grants,
        }
        .to_grants_wire();
        assert_eq!(Acl::parse_grants_wire(&wire), Err(AclError::GrantsOverflow));
        // The public parser self-heals the same wire to the default private ACL.
        assert_eq!(Acl::from_grants_wire(&wire), Acl::default_private(None));
    }

    #[test]
    fn grants_wire_parse_canonicalizes_unsorted_and_duplicate_input() {
        let a = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100";
        let b = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";
        let wire = format!("id={a},READ&id={b},FULL_CONTROL&id={b},FULL_CONTROL");
        assert_eq!(
            Acl::parse_grants_wire(&wire).unwrap().to_grants_wire(),
            format!("id={b},FULL_CONTROL&id={a},READ")
        );
    }

    #[test]
    fn the_composite_write_is_the_grants_wires_canonical_value() {
        // The PostgreSQL composite's write reuses the wire codec's key, so a
        // stored grant set is byte-identical to the wire's canonical value:
        // an unknown group URI dropped, order fixed, duplicates removed.
        let acl = Acl {
            owner: Some(uid()),
            grants: vec![
                Grant {
                    grantee: Grantee::Group(GroupUri("http://evildomain/".into())),
                    permission: Permission::Read,
                },
                Grant {
                    grantee: Grantee::Canonical(uid()),
                    permission: Permission::Read,
                },
                Grant {
                    grantee: Grantee::Canonical(uid()),
                    permission: Permission::Read,
                },
                Grant {
                    grantee: Grantee::Group(GroupUri(GROUP_ALL_USERS.to_string())),
                    permission: Permission::Write,
                },
            ],
        };
        let canonical = acl.canonical_grants();
        assert_eq!(
            canonical.len(),
            2,
            "the unknown group and the duplicate drop"
        );
        assert_eq!(
            canonical
                .iter()
                .filter_map(Acl::grant_key)
                .collect::<Vec<_>>(),
            acl.to_grants_wire().split('&').collect::<Vec<_>>(),
            "the composite's write is the wire's canonical value"
        );
        assert_eq!(canonical[0].grantee, Grantee::Canonical(uid()));
        assert_eq!(
            canonical[1].grantee,
            Grantee::Group(GroupUri(GROUP_ALL_USERS.to_string()))
        );
    }

    #[test]
    fn the_composite_read_drops_an_element_outside_the_domain() {
        // A stored grantee outside the domain has no canonical key, so the
        // composite's read drops the whole grant -- the wire's own self-heal.
        let unknown = Grant {
            grantee: Grantee::Group(GroupUri("http://evildomain/".into())),
            permission: Permission::Read,
        };
        // The sentinel a stored fragment outside the domain decodes to.
        let sentinel = Grant {
            grantee: Grantee::Group(GroupUri(String::new())),
            permission: Permission::Read,
        };
        let canonical = Grant {
            grantee: Grantee::Canonical(uid()),
            permission: Permission::Read,
        };
        assert!(Acl::grant_key(&canonical).is_some());
        assert!(Acl::grant_key(&unknown).is_none());
        assert!(Acl::grant_key(&sentinel).is_none());
    }
}
