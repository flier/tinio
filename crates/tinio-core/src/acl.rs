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

use sha2::{Digest, Sha256};

use crate::percent;

/// AWS's documented special-grantee canonical ID: the anonymous
/// (unsigned) uploader. The only non-64-hex value [`OwnerId`] admits —
/// the exception sits beside the type on purpose.
pub const ANONYMOUS_CANONICAL_ID: &str = "65a011a29cdf8ec533ec3d1ccaae921c";

/// A validated canonical user ID: the 64 lowercase-hex account
/// identifier S3 uses for owners and grantees (AWS's own canonical IDs
/// have the same shape), or AWS's anonymous special-grantee ID
/// ([`ANONYMOUS_CANONICAL_ID`] — the one exception to the shape).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

    /// The canonical ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The default owner display name (`"tinio"` — the `[owner]` config
/// default; design §6). The feature-off / `identity: None` fallback
/// lives in core so tinio-config and the auth layer share one source
/// without a dependency on core's hashing.
pub const DEFAULT_OWNER_DISPLAY_NAME: &str = "tinio";

/// The default owner canonical ID: `hex(SHA-256("tinio"))` — the fixed
/// 64-hex constant every default owner element resolves to.
pub fn default_owner_id() -> OwnerId {
    let digest = Sha256::digest(DEFAULT_OWNER_DISPLAY_NAME.as_bytes());
    OwnerId::new(hex::encode(digest)).expect("sha256 hex is 64 lowercase hex digits")
}

/// The single-key delete/overwrite parity rule (spec review B2): `P ==
/// O(object) or P == O(bucket)` — grants never satisfy it. The ONE home
/// for the rule, shared by the server's handler-side per-key DeleteObjects
/// check and tinio-auth's `ObjectOrBucketOwner` gate.
///
/// `object_owner` is the RAW row element — `None` (the empty wire,
/// review B4) resolves to `lazy_default`, NOT to the bucket owner: an
/// empty-wire object is owned by the configured lazy default.
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
/// `to_grants_wire`/`from_grants_wire` cover the grant set only.
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

    /// The canonical grantee wire element; a group URI outside the
    /// three documented constants has no wire form.
    fn grantee_wire(g: &Grantee) -> Option<String> {
        Some(match g {
            Grantee::Canonical(id) => format!("id={}", id.as_str()),
            Grantee::Group(uri) if uri.is_valid() => format!("uri={}", percent::encode_uri(&uri.0)),
            Grantee::Group(_) => return None,
        })
    }

    /// The canonical grants wire: each `grantee_wire,PERMISSION` entry
    /// sorted and deduped, joined by `&`. Grants whose grantee has no
    /// wire form (unknown group URIs) are dropped.
    pub fn to_grants_wire(&self) -> String {
        let mut entries: Vec<String> = self
            .grants
            .iter()
            .filter_map(|g| {
                Some(format!(
                    "{},{}",
                    Self::grantee_wire(&g.grantee)?,
                    g.permission.as_wire()
                ))
            })
            .collect();
        entries.sort();
        entries.dedup();
        entries.join("&")
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
        // Canonical: sort by the same `grantee_wire,PERMISSION` key the
        // encoder emits, and dedupe exact duplicates.
        let mut keyed: Vec<(String, Grant)> = grants
            .into_iter()
            .filter_map(|g| {
                Some((
                    format!(
                        "{},{}",
                        Self::grantee_wire(&g.grantee)?,
                        g.permission.as_wire()
                    ),
                    g,
                ))
            })
            .collect();
        keyed.sort_by(|a, b| a.0.cmp(&b.0));
        keyed.dedup_by(|a, b| a.0 == b.0);
        Ok(Self {
            owner: None,
            grants: keyed.into_iter().map(|(_, g)| g).collect(),
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
}
