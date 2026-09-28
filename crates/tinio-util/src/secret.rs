//! Zeroizing secret strings for config credentials and management-plane tokens.
//!
//! [`SecretKey`] is the material; [`Secret`] is the `SecretBox` layer that redacts
//! `Debug` and zeroizes on drop. Callers that need a domain name add one more
//! newtype (or a field) on top — they do not reinvent the box.

use derive_more::{Deref, From};
use secrecy::{CloneableSecret, ExposeSecret, SecretBox, SerializableSecret, zeroize::Zeroize};
use serde::{Deserialize, Serialize};

/// Secret string material: zeroized on drop; [`SerializableSecret`] opts into serde
/// serialization for config and `state` file round-trips.
///
/// [`Deref`] reaches the inner [`String`]; `as_str` / `&str` follow via `String`'s own `Deref`.
#[derive(Clone, PartialEq, Eq, Deref, From, Serialize, Deserialize)]
#[from(forward)]
#[serde(transparent)]
pub struct SecretKey(String);

impl Zeroize for SecretKey {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

impl SerializableSecret for SecretKey {}

impl CloneableSecret for SecretKey {}

impl AsRef<str> for SecretKey {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<SecretKey> for SecretBox<SecretKey> {
    fn from(key: SecretKey) -> Self {
        SecretBox::new(Box::new(key))
    }
}

/// Zeroizing secret box. [`PartialEq`] compares the exposed material —
/// `SecretBox` itself is not comparable.
#[derive(Clone, Debug, Deref, From, Serialize, Deserialize)]
#[from(forward)]
#[serde(transparent)]
pub struct Secret(SecretBox<SecretKey>);

impl PartialEq for Secret {
    fn eq(&self, other: &Self) -> bool {
        self.expose_secret() == other.expose_secret()
    }
}

impl Eq for Secret {}

#[cfg(test)]
mod tests {
    use super::*;

    /// The material a test hands the box. A constant rather than a generated token for the
    /// reason every fixture here is: the assertions are about *this* string reaching — or not
    /// reaching — a rendering.
    const MATERIAL: &str = "00112233445566778899aabbccddeeff";

    fn key() -> SecretKey {
        SecretKey::from(MATERIAL.to_owned())
    }

    /// **The property the box exists for.** [`SecretKey`] carries no `Debug` at all, so the only
    /// rendering that could leak is the box's — and a `#[derive(Debug)]` that reached the inner
    /// `String` would be a token in a log rather than a compile error.
    #[test]
    fn debug_renders_the_box_and_never_the_material() {
        let printed = format!("{:?}", Secret::from(key()));
        assert!(!printed.contains(MATERIAL), "{printed}");
        assert!(printed.contains("REDACTED"), "{printed}");
    }

    /// **The custom half of the type** — `SecretBox` is not comparable, so `PartialEq` is this
    /// module's own comparison of the exposed material, and nothing else pins what it compares.
    #[test]
    fn equality_is_the_material_and_not_the_box() {
        assert_eq!(Secret::from(key()), Secret::from(key()));
        assert_ne!(
            Secret::from(key()),
            Secret::from(SecretKey::from(
                "ffeeddccbbaa99887766554433221100".to_owned()
            ))
        );
    }

    /// The forwarding the callers read the material through: `AsRef<str>`, `Deref` and the
    /// `From<String>` the config's generated credentials arrive as.
    #[test]
    fn the_key_reaches_its_material() {
        let key = key();
        assert_eq!(key.as_ref(), MATERIAL);
        // `Deref` reaches the inner `String` — annotated so the test pins *which* type the
        // deref lands on rather than passing through whichever comparison happens to resolve.
        let inner: &String = &key;
        assert_eq!(inner.as_str(), MATERIAL);
        // **`assert!` rather than `assert_eq!`, and that is the type's own doing**: `assert_eq!`
        // needs `Debug` to render a failure, and `SecretKey` carries none — a key that cannot be
        // printed, rather than one that is printed redacted.
        assert!(key == SecretKey::from(MATERIAL.to_owned()));
    }

    /// The box's two construction routes: the `From<SecretKey>` impl written by hand, and the
    /// `#[from(forward)]` that reaches it. Both must expose the same material.
    #[test]
    fn the_box_exposes_what_it_wrapped() {
        let forwarded = Secret::from(key());
        let boxed = Secret::from(SecretBox::<SecretKey>::from(key()));
        assert_eq!(forwarded.expose_secret().as_ref(), MATERIAL);
        assert_eq!(boxed.expose_secret().as_ref(), MATERIAL);
    }

    /// **`Zeroize` is what the drop does, and this is the only way to observe it**: the material
    /// is gone from the `String` after the call, so the bytes the buffer held are not readable
    /// through the value that held them.
    #[test]
    fn zeroizing_the_key_empties_its_material() {
        let mut key = key();
        key.zeroize();
        assert!(key.as_ref().is_empty());
    }
}
