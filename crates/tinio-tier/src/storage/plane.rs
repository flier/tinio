//! The request metadata a staged write carries and the settled plane a read
//! resolves.

use crate::_core::{
    acl::{self, Acl},
    object, object_lock,
};

/// The request-shaped metadata a memory-staged write carries: the fields the
/// buffer's plain-commit rule examines and the filesystem write-through
/// records.
pub(super) struct WriteMeta<'a> {
    /// The request's tags.
    pub(super) tags: &'a object::Tags,
    /// The request's owner element.
    pub(super) owner: Option<&'a acl::OwnerId>,
    /// The request's ACL.
    pub(super) acl: &'a Acl,
    /// The write's effective Object Lock.
    pub(super) lock: Option<&'a object_lock::Lock>,
}

/// The settled plane a read resolves to.
#[expect(
    clippy::large_enum_variant,
    reason = "argued: the cold metadata rides the resolution instead of a per-read allocation"
)]
pub(super) enum Plane {
    /// The warm filesystem plane (the default for every key the index does
    /// not place in the cold plane).
    Warm,
    /// The cold plane, with the metadata authority its placement carries.
    Cold(object::Info),
    /// The object's row is gone; the placement and cold copy are retired.
    Deleted,
}
