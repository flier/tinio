//! Configuration schema: the `Config` struct and its sections.
//!
//! Pure data types with serde attributes (TOML shape), `SmartDefault` field
//! defaults, and garde validation attributes. Unknown keys are not rejected
//! by serde — the internal `parse_at` collects them via `serde_ignored` and
//! reports [`crate::Error::UnknownKey`] (FR-016, fail-fast).
//! Sections are presence-gated: absent optional sections
//! parse as `None` and are skipped when the config is re-serialized.
//!
//! One public module per TOML section (`api`, `auth`, `log`, …); section
//! types drop the section prefix (`log::Config`, `api::Http`). The root
//! document is [`Config`].

use garde::Error as GardeError;

pub mod api;
pub mod auth;
mod config;
pub mod log;
pub mod owner;
pub mod pipeline;
pub mod s3;
pub mod scanner;
pub mod server;
pub mod storage;
pub mod telemetry;
pub mod users;

/// The shared "must not be empty" garde rule body: the api `cert`/`key`
/// path fields and the auth secret key reject empty values with their own
/// messages (one home for the boilerplate, so the rule cannot drift).
pub(super) fn reject_empty(message: &str, is_empty: bool) -> garde::Result {
    if is_empty {
        Err(GardeError::new(message))
    } else {
        Ok(())
    }
}

/// The canonical-ID garde rule (spec §6, shared by `[owner]` and
/// `[[users]]`): 64 lowercase hex digits per [`OwnerId::new`] — and never
/// the anonymous special-grantee ID, whose holder would BE the anonymous
/// uploader (Task 7 ruling).
pub(super) fn validate_canonical_id(value: &str, _context: &()) -> garde::Result {
    if value == crate::_core::acl::ANONYMOUS_CANONICAL_ID {
        return Err(GardeError::new(
            "must not be the anonymous canonical ID (it would make the principal the anonymous uploader)",
        ));
    }
    match crate::_core::acl::OwnerId::new(value) {
        Ok(_) => Ok(()),
        Err(_) => Err(GardeError::new(
            "must be 64 lowercase hex digits (the canonical-account ID shape)",
        )),
    }
}

/// The optional-canonical-ID variant of [`validate_canonical_id`] (the
/// `[[users]]` field; `None` = the derived default).
pub(super) fn validate_canonical_id_opt(value: &Option<String>, context: &()) -> garde::Result {
    match value {
        Some(id) => validate_canonical_id(id, context),
        None => Ok(()),
    }
}

/// The Windows `local_uid` rejection (spec §6, fail fast): the type stays
/// portable, the key parses, and validation errors instead — a unix-only
/// key in a Windows config is an invalid value, not a silent ignore.
#[cfg(windows)]
pub(super) fn validate_local_uid(value: &Option<u32>, _context: &()) -> garde::Result {
    match value {
        Some(uid) => Err(GardeError::new(format!(
            "local_uid ({uid}) is unsupported on Windows — remove the key or run on a unix system"
        ))),
        None => Ok(()),
    }
}

pub use config::{Config, Version};
