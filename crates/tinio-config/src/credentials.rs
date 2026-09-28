//! Generated credentials for a first start (`config.md:141`, FR-007).

use crate::{
    Error,
    auth::{Secret, SecretKey},
};

/// The generated pair.
///
/// **The secret key is [`Secret`]** — redacted from `Debug` by construction (`SecretBox`),
/// matching every other secret this crate hands to callers. An unredacted `String` would be
/// one `tracing::debug!("{credentials:?}")` away from a secret in a log line.
///
/// # Examples
///
/// ```rust
/// use secrecy::ExposeSecret;
/// use tinio_config::credentials::{Credentials, generate};
///
/// let pair: Credentials = generate()?;
/// assert_eq!(pair.access_key.len(), 32); // 16 bytes, hex-encoded
/// assert_eq!(pair.secret_key.expose_secret().len(), 64); // 32 bytes, hex-encoded
/// // The secret is redacted whenever the pair is formatted.
/// let secret = pair.secret_key.expose_secret();
/// assert!(!format!("{pair:?}").contains(&**secret));
/// # Ok::<(), tinio_config::Error>(())
/// ```
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Credentials {
    /// At least 16 bytes of CSPRNG output, hex-encoded.
    pub access_key: String,
    /// At least 32 bytes, hex-encoded.
    pub secret_key: Secret,
}

/// The contract's floors, in bytes before hex encoding.
const ACCESS_BYTES: usize = 16;
const SECRET_BYTES: usize = 32;

/// Generates a pair from the CSPRNG.
///
/// # Examples
///
/// ```rust
/// use secrecy::ExposeSecret;
/// use tinio_config::credentials::generate;
///
/// let pair = generate()?;
/// assert_ne!(pair.access_key.as_str(), &**pair.secret_key.expose_secret());
/// # Ok::<(), tinio_config::Error>(())
/// ```
pub fn generate() -> Result<Credentials, Error> {
    Ok(Credentials {
        access_key: hex(ACCESS_BYTES)?,
        secret_key: SecretKey::from(hex(SECRET_BYTES)?).into(),
    })
}

fn hex(bytes: usize) -> Result<String, Error> {
    let mut buffer = vec![0u8; bytes];
    // There is no `Error::Utf8` variant in this crate: a CSPRNG failure is an
    // "environment fact the configuration cannot proceed without", which is
    // `Error::Missing`'s own doc — so it maps there rather than to a new variant
    // invented for one call site.
    getrandom::fill(&mut buffer).map_err(|err| {
        Error::Missing(format!(
            "generated credentials: the CSPRNG is unavailable: {err}"
        ))
    })?;
    Ok(hex::encode(buffer))
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    #[test]
    fn the_pair_meets_the_contracts_floors() {
        // 16/32 bytes before hex, so 32/64 characters after — and hex, so a
        // credential is a string no parser has to escape.
        let pair = generate().unwrap();
        assert_eq!(pair.access_key.len(), ACCESS_BYTES * 2);
        assert_eq!(pair.secret_key.expose_secret().len(), SECRET_BYTES * 2);
        assert!(pair.access_key.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(
            pair.secret_key
                .expose_secret()
                .chars()
                .all(|c| c.is_ascii_hexdigit())
        );
    }

    #[test]
    fn the_pair_redacts_its_secret_when_debugged() {
        // `Secret` redacts by construction: the access key identifies the pair, the
        // secret never reaches a log line, and neither `{:?}` nor `{:#?}` can undo that.
        let pair = generate().unwrap();
        let secret = pair.secret_key.expose_secret().to_string();
        for printed in [format!("{pair:?}"), format!("{pair:#?}")] {
            assert!(printed.contains(&pair.access_key), "{printed}");
            assert!(printed.contains("REDACTED"), "{printed}");
            assert!(
                !printed.contains(&secret),
                "the secret key must not be printed: {printed}"
            );
        }
    }

    #[test]
    fn two_generations_differ() {
        // A CSPRNG that answered the same pair twice would be a constant,
        // and a first start that writes a constant is a first start that
        // ships a public credential.
        let first = generate().unwrap();
        let second = generate().unwrap();
        assert_ne!(first.access_key, second.access_key);
        assert_ne!(first.secret_key, second.secret_key);
    }
}
