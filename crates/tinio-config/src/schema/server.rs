use std::path::Path;

use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use crate::error;

/// The S3 data-plane listener (`[server]`).
///
/// # Examples
///
/// ```rust
/// use tinio_config::server::Config;
///
/// let s = Config::default();
/// assert_eq!(s.port, 9000); // Minio-compatible default; 0 = ephemeral
/// assert!(!s.read_only);
/// assert!(!s.sandbox); // confinement is opt-in, on every channel
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(allow_unvalidated)]
pub struct Config {
    /// Bind host (default `127.0.0.1`).
    #[serde(default = "host")]
    #[default = r#"127.0.0.1"#]
    pub host: String,
    /// Bind port (default 9000; `0` = OS-assigned ephemeral).
    #[serde(default = "port")]
    #[default = 9000]
    pub port: u16,
    /// Read-only mode (FR-023): reject all S3 writes, state relocates to
    /// `~/.tinio/roots/<hash>/`.
    #[serde(default)]
    pub read_only: bool,
    /// Process confinement (`--sandbox`, `TINIO_SANDBOX`; FR-036, design
    /// decision 10).
    #[serde(default)]
    pub sandbox: bool,
}

fn host() -> String {
    Config::default().host
}

fn port() -> u16 {
    Config::default().port
}

/// The `[server]` keys a process resolves **without** parsing the rest of the
/// document: FR-023's mode, and design decision 5's sandbox channel.
///
/// Both are read by the launcher platforms, which must not retain a parsed
/// [`crate::Config`] — it holds credentials (design decision 18: "no retained
/// parse, not no contact") — and both are read by the same *call*, because one
/// parse of one document is what keeps the two answers from drifting apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ServerKeys {
    /// `[server] read_only` (FR-023).
    pub read_only: bool,
    /// `[server] sandbox` (design decision 5). Setting it is a refusal, not a
    /// mode, until a phase implements the channel.
    pub sandbox: bool,
}

/// Whether a configuration document sets `[server] read_only` (`ServerKeys::read_only`)
/// or `[server] sandbox` (`ServerKeys::sandbox`), without parsing the rest of it.
///
/// Two fields are deserialized, everything else is discarded by serde, and an
/// empty document means "neither", which is the default [`Config::default`]
/// carries.
///
/// **One probe for two keys, and that is the anti-drift rule rather than a
/// convenience.** The mechanism's cost is the mirror below: a struct that
/// restates two field names and types outside the schema that defines them. A
/// second function parsing the same document for its one key would double that
/// mirror and give the two copies two places to fall out of step — the failure
/// this probe's strictness already guards one half of. One parse, both bits,
/// one place to change when the schema changes.
///
/// **The keys are spelled twice and the type is what keeps the two spellings
/// honest**: the mirror below answers an error for a `read_only` that is not a
/// boolean rather than falling back to `false`, because the fallback is the
/// failure that loses FR-023's kernel half with nothing logged — and a
/// `sandbox` that is not a boolean is refused by the same rule, for decision
/// 5's version of the same reason (a run the operator believes is confined).
/// A change to the fields above is a change to the mirror, and the test below
/// is where a divergence lands: it drives one document through
/// [`crate::Config::parse`] and through this probe and requires the two to
/// agree key by key.
///
/// # Errors
///
/// The document's own parse error, for bytes that are not TOML or that set
/// either key to something other than a boolean: the caller reports what it
/// could not read rather than guessing at an access mode or a confinement.
///
/// # Examples
///
/// ```
/// use tinio_config::server::probe_server_keys;
///
/// let keys = probe_server_keys(b"").unwrap();
/// assert!(!keys.read_only);
/// assert!(!keys.sandbox);
///
/// let keys = probe_server_keys(b"version = 1\n[server]\nread_only = true\nsandbox = true\n");
/// let keys = keys.unwrap();
/// assert!(keys.read_only && keys.sandbox);
/// ```
pub fn probe_server_keys(bytes: &[u8]) -> Result<ServerKeys, error::Error> {
    #[derive(Deserialize)]
    struct Server {
        #[serde(default)]
        read_only: bool,
        #[serde(default)]
        sandbox: bool,
    }
    #[derive(Deserialize)]
    struct Document {
        #[serde(default)]
        server: Option<Server>,
    }

    if bytes.is_empty() {
        return Ok(ServerKeys::default());
    }
    let text =
        str::from_utf8(bytes).map_err(|err| error::parse(Path::new("(bytes)"), err.to_string()))?;
    let document: Document =
        toml::from_str(text).map_err(|err| error::parse(Path::new("(bytes)"), err.to_string()))?;
    Ok(document
        .server
        .map_or_else(ServerKeys::default, |server| ServerKeys {
            read_only: server.read_only,
            sandbox: server.sandbox,
        }))
}

/// [`probe_server_keys`]'s read-only bit alone, for a caller whose only
/// question is FR-023's.
///
/// The platforms that launch a confined child must know this bit in the
/// unconfined launcher, where a parsed [`crate::Config`] must not be retained —
/// it holds credentials (design decision 18: "no retained parse, not no
/// contact").
///
/// # Errors
///
/// The document's own parse error, as [`probe_server_keys`] answers it.
///
/// # Examples
///
/// ```
/// use tinio_config::server::probe_read_only;
///
/// assert!(!probe_read_only(b"").unwrap());
/// assert!(probe_read_only(b"version = 1\n[server]\nread_only = true\n").unwrap());
/// ```
pub fn probe_read_only(bytes: &[u8]) -> Result<bool, error::Error> {
    probe_server_keys(bytes).map(|keys| keys.read_only)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_reads_only_the_server_keys() {
        assert_eq!(probe_server_keys(b"").unwrap(), ServerKeys::default());
        assert_eq!(
            probe_server_keys(b"version = 1\n").unwrap(),
            ServerKeys::default()
        );
        assert_eq!(
            probe_server_keys(b"version = 1\n[server]\nread_only = true\n").unwrap(),
            ServerKeys {
                read_only: true,
                ..ServerKeys::default()
            }
        );
        assert_eq!(
            probe_server_keys(b"version = 1\n[server]\nsandbox = true\n").unwrap(),
            ServerKeys {
                sandbox: true,
                ..ServerKeys::default()
            }
        );
        assert_eq!(
            probe_server_keys(b"version = 1\n[server]\nread_only = false\nsandbox = false\n")
                .unwrap(),
            ServerKeys::default()
        );
        // A key that is not a boolean is refused, never defaulted — for both
        // keys, because the two are one parse.
        assert!(probe_server_keys(b"version = 1\n[server]\nread_only = \"yes\"\n").is_err());
        assert!(probe_server_keys(b"version = 1\n[server]\nsandbox = \"yes\"\n").is_err());
        // Credentials and every other key are ignored, and nothing of them is
        // retained: this is the launcher's whole view of the document.
        let with_secrets = b"version = 1\n[server]\nread_only = true\nsandbox = true\n[auth]\n\
                             access_key = \"a\"\nsecret_key = \"b\"\n";
        assert_eq!(
            probe_server_keys(with_secrets).unwrap(),
            ServerKeys {
                read_only: true,
                sandbox: true,
            }
        );
        // The narrower reader is the wider one's projection, so it cannot
        // answer a different FR-023 bit than the probe two callers share.
        assert!(probe_read_only(with_secrets).unwrap());
    }

    /// The probe is a **mirror** of the schema's `[server]` section, and this
    /// is what keeps the two from drifting: one document through both readers,
    /// key by key. A field renamed in one place and not the other fails here —
    /// the schema's reader would reject the key as unknown, or the mirror
    /// would silently answer the default.
    #[test]
    fn probe_agrees_with_the_parsed_schema() {
        for text in [
            "version = 1\n",
            "version = 1\n[server]\nread_only = true\n",
            "version = 1\n[server]\nsandbox = true\n",
            "version = 1\n[server]\nread_only = true\nsandbox = true\n",
        ] {
            let config = crate::Config::parse(text).expect("a document the schema accepts");
            let keys = probe_server_keys(text.as_bytes()).expect("the same document, probed");
            assert_eq!(
                keys.read_only, config.server.read_only,
                "the probe and the schema disagree about `[server] read_only` for {text:?}"
            );
            assert_eq!(
                keys.sandbox, config.server.sandbox,
                "the probe and the schema disagree about `[server] sandbox` for {text:?}"
            );
        }
    }
}
