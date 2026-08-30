//! Configuration schema: the `Config` struct and its sections.
//!
//! Pure data types with serde attributes (TOML shape), `SmartDefault` field
//! defaults, and garde validation attributes. Unknown keys are not rejected
//! by serde — the internal `parse_at` collects them via `serde_ignored` and
//! reports [`Error::UnknownKey`] (FR-016, fail-fast).
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
pub mod pipeline;
pub mod s3;
pub mod scanner;
pub mod server;
pub mod storage;
pub mod telemetry;

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

pub use config::{Config, Version};
