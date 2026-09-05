//! The owner/ACL wire-element decode helpers — the single home of the
//! read-path rule for the two row elements the ACL feature added
//! (bucket/meta/upload rows all carry `owner_wire` + `acl_wire`).
//! Re-exported from `lib.rs` as `tinio_store::{decode_owner_wire,
//! decode_acl_wire}` so the backends reach them without a store-internal
//! path.

use tinio_core::acl::{Acl, OwnerId};

/// Decode one stored owner wire element — `None` when empty or
/// domain-invalid, matching the self-heal style of the other row
/// elements (the row is then served without an owner; the auth layer
/// resolves the default owner).
pub fn decode_owner_wire(wire: &str) -> Option<OwnerId> {
    if wire.is_empty() {
        None
    } else {
        OwnerId::new(wire).ok()
    }
}

/// Decode one stored ACL wire element — an empty or domain-invalid wire
/// self-heals to [`Acl::default_private(None)`] (a row with no ACL is
/// private with no owner, the `from_grants_wire` discipline).
pub fn decode_acl_wire(wire: &str) -> Acl {
    if wire.is_empty() {
        Acl::default_private(None)
    } else {
        Acl::from_grants_wire(wire)
    }
}
