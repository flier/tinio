//! Identity model: configured principals and owner resolution.

use std::collections::HashMap;

use s3s::{
    auth::{Credentials, SecretKey},
    dto::Owner,
};

pub use crate::_core::acl::{ANONYMOUS_CANONICAL_ID, derive_canonical_id};
use crate::_core::acl::{DEFAULT_OWNER_DISPLAY_NAME, OwnerId, default_owner_id};

/// One configured SigV4 principal: the access key and secret, the
/// canonical grant/owner ID, and the presentation display name.
#[derive(Debug)]
pub struct User {
    pub access_key: String,
    pub canonical_id: OwnerId,
    pub display_name: String,
    pub secret: SecretKey,
}

impl User {
    /// A test user: canonical ID derived from the access key.
    pub fn test(access_key: &str, secret: &str, name: &str) -> Self {
        Self {
            access_key: access_key.into(),
            canonical_id: derive_canonical_id(access_key),
            display_name: name.into(),
            secret: SecretKey::from(secret),
        }
    }
}

/// The identity map: access key → user, the default owner element (the
/// config defaults; the lazy owner for rows without one), and the
/// canonical-ID → display-name inverse (the O(1) owner resolution of
/// listing pages and the ACL ops — built once at construction).
#[derive(Debug)]
pub struct Identity {
    pub users: HashMap<String, User>,
    pub default_owner: OwnerId,
    pub default_display_name: String,
    /// canonical ID → display name (the inverse of `users`).
    display_names: HashMap<OwnerId, String>,
}

impl Identity {
    /// The identity over the given user map and the default owner
    /// element.
    pub fn new(
        users: HashMap<String, User>,
        default_owner: OwnerId,
        default_display_name: String,
    ) -> Self {
        let display_names = users
            .values()
            .map(|u| (u.canonical_id.clone(), u.display_name.clone()))
            .collect();
        Self {
            users,
            default_owner,
            default_display_name,
            display_names,
        }
    }

    /// A default-shaped identity over the given users: the default owner
    /// element is the built-in `hex(SHA-256("tinio"))` / `"tinio"` pair.
    pub fn test(users: Vec<User>) -> Self {
        Self::new(
            users
                .into_iter()
                .map(|u| (u.access_key.clone(), u))
                .collect(),
            default_owner_id(),
            DEFAULT_OWNER_DISPLAY_NAME.into(),
        )
    }

    /// The requester's canonical ID: the authenticated user's, or
    /// [`ANONYMOUS_CANONICAL_ID`] when no credentials are present.
    ///
    /// A signed request with an unknown access key is impossible post-auth
    /// (`ConfigAuth` rejects it) — an invariant violation panics.
    pub fn principal(&self, credentials: Option<&Credentials>) -> OwnerId {
        match credentials {
            Some(creds) => self
                .users
                .get(creds.access_key.as_str())
                .expect("authenticated access key must resolve to a configured user")
                .canonical_id
                .clone(),
            None => OwnerId::new(ANONYMOUS_CANONICAL_ID)
                .expect("the anonymous canonical ID is a valid OwnerId"),
        }
    }

    /// The s3s `Owner` element for a row owner: the display name from the
    /// identity map, or the default-owner element; an unknown ID — the
    /// anonymous ID included — is ID-only, AWS's unknown-account shape.
    pub fn owner(&self, id: Option<&OwnerId>) -> Owner {
        let (id, display_name) = match id {
            Some(id) => {
                let display_name = self
                    .display_names
                    .get(id)
                    .cloned()
                    .or_else(|| (id == &self.default_owner).then(|| self.default_display_name.clone()));
                (Some(id.clone()), display_name)
            }
            None => (
                Some(self.default_owner.clone()),
                Some(self.default_display_name.clone()),
            ),
        };
        Owner {
            display_name,
            id: id.map(|i| i.as_str().to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user1_canonical_id() -> OwnerId {
        derive_canonical_id("AKID")
    }

    #[test]
    fn principal_resolution_and_anonymous_id() {
        let id = Identity::test(vec![User::test("AKID", "secret", "user1")]);
        let cred = Credentials {
            access_key: "AKID".into(),
            secret_key: SecretKey::from("secret"),
        };
        assert_eq!(
            id.principal(Some(&cred)).as_str(),
            user1_canonical_id().as_str()
        );
        assert_eq!(id.principal(None).as_str(), ANONYMOUS_CANONICAL_ID);
    }

    #[test]
    fn owner_resolves_display_name_from_the_map() {
        let id = Identity::test(vec![User::test("AKID", "secret", "user1")]);
        let o = id.owner(Some(&user1_canonical_id()));
        assert_eq!(o.display_name.as_deref(), Some("user1"));
        assert_eq!(o.id.as_deref(), Some(user1_canonical_id().as_str()));
    }

    #[test]
    fn owner_of_unknown_ids_is_id_only() {
        let id = Identity::test(vec![User::test("AKID", "secret", "user1")]);
        let unknown =
            OwnerId::new("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
                .unwrap();
        let o = id.owner(Some(&unknown));
        assert_eq!(o.display_name, None);
        assert_eq!(
            o.id.as_deref(),
            Some("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
        );
        // The anonymous ID resolves ID-only too (AWS emits no DisplayName).
        let anon = OwnerId::new(ANONYMOUS_CANONICAL_ID).unwrap();
        let o = id.owner(Some(&anon));
        assert_eq!(o.display_name, None);
        assert_eq!(o.id.as_deref(), Some(ANONYMOUS_CANONICAL_ID));
    }

    #[test]
    fn owner_none_is_the_default_owner_element() {
        let id = Identity::test(vec![User::test("AKID", "secret", "user1")]);
        let o = id.owner(None);
        assert_eq!(o.display_name.as_deref(), Some(DEFAULT_OWNER_DISPLAY_NAME));
        assert_eq!(o.id.as_deref(), Some(default_owner_id().as_str()));
    }
}
