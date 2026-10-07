//! The store-redb owner/ACL wire decode helpers.
//!
//! Both rules have one home in `tinio_core`'s `meta::bucket` (plan 2026-10-03
//! Task 1 Step 4) — `decode_owner` and `decode_acl` — re-exported here under
//! the table crate's historical names. `decode_acl` is the faithful decode:
//! it preserves the stored wire's grant order and duplicates, where the local
//! copy this file used to carry canonicalized through `Acl::from_grants_wire`.
//! Every row the store writes goes through the canonical encoder, so no
//! stored row distinguishes the two — only a valid non-canonical wire (one
//! the store cannot produce) does, and there the faithful rule is the
//! contract's.
//!
//! Re-exported from `lib.rs` as `tinio_store_redb::{decode_owner_wire,
//! decode_acl_wire}` so the backends reach them without a store-internal
//! path.

/// Decode one stored ACL wire element — an empty or domain-invalid wire
/// self-heals to the private default with no owner and no grants (a row with
/// no ACL is private with no owner). Re-exported from `tinio_core`'s row
/// codec, the rule's one home.
pub use crate::_core::meta::bucket::decode_acl as decode_acl_wire;
/// Decode one stored owner wire element — `None` when empty or
/// domain-invalid, matching the self-heal style of the other row elements.
/// Re-exported from `tinio_core`'s row codec, the rule's one home.
pub use crate::_core::meta::bucket::decode_owner as decode_owner_wire;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::_core::acl::{Acl, Grant, Grantee, OwnerId, Permission};

    fn id(s: &str) -> OwnerId {
        OwnerId::new(s).unwrap()
    }

    /// The converged decoder is the FAITHFUL one: a valid but non-canonical
    /// wire keeps its grant order and its duplicates, where the table crate's
    /// previous copy (`Acl::from_grants_wire`) sorted and deduped. This is
    /// the behaviour change the convergence pinned; the store's own writers
    /// emit the canonical wire, so no row it stores differs.
    #[test]
    fn the_acl_decode_preserves_a_non_canonical_wire_faithfully() {
        let a = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100";
        let b = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";
        // Canonical order is `id=b` before `id=a`; this wire reverses it and
        // repeats the `id=a` grant.
        let wire = format!("id={a},READ&id={b},FULL_CONTROL&id={a},READ");
        let acl = decode_acl_wire(&wire);
        assert_eq!(acl.owner, None, "the owner rides its own element");
        assert_eq!(
            acl.grants,
            vec![
                Grant {
                    grantee: Grantee::Canonical(id(a)),
                    permission: Permission::Read,
                },
                Grant {
                    grantee: Grantee::Canonical(id(b)),
                    permission: Permission::FullControl,
                },
                Grant {
                    grantee: Grantee::Canonical(id(a)),
                    permission: Permission::Read,
                },
            ],
            "the faithful decode keeps the wire's order and duplicates"
        );
        // The canonicalizing rule the table crate used to call would have
        // collapsed this to two sorted grants.
        assert_eq!(
            Acl::from_grants_wire(&wire).grants.len(),
            2,
            "the old canonicalizing decode"
        );
    }

    /// A canonical wire (what the store writes) and an empty/invalid wire
    /// read identically under either rule — the convergence changes nothing
    /// the store can observe.
    #[test]
    fn the_acl_decode_agrees_on_every_wire_the_store_can_produce() {
        let canonical = "id=aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899,READ";
        assert_eq!(decode_acl_wire(canonical), Acl::from_grants_wire(canonical));
        for wire in ["", "garbage![nope", "id=short,READ"] {
            assert_eq!(
                decode_acl_wire(wire),
                Acl::from_grants_wire(wire),
                "{wire:?}"
            );
            assert_eq!(
                decode_acl_wire(wire),
                Acl::default_private(None),
                "{wire:?}"
            );
        }
    }
}
