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
/// resolves the default owner). The core codecs already own the
/// empty-wire behavior (`OwnerId::new("")` is invalid, so the empty
/// branch the `acl` module used to special-case was dead).
pub fn decode_owner_wire(wire: &str) -> Option<OwnerId> {
    OwnerId::new(wire).ok()
}

/// Decode one stored ACL wire element — an empty or domain-invalid wire
/// self-heals to [`Acl::default_private(None)`] (a row with no ACL is
/// private with no owner, the `from_grants_wire` discipline; an empty
/// wire parses to exactly that, so no special case).
pub fn decode_acl_wire(wire: &str) -> Acl {
    Acl::from_grants_wire(wire)
}
