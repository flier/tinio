//! Configuration for tinio.
//!
//! Single source of truth for the server's configuration (tasks T016/T017):
//! the TOML config file schema (`version = 1` with `[server]`, `[scanner]`,
//! `[auth]`, `[log]`, `[s3]`, `[storage]`, `[api]`, `[telemetry]` sections
//! per contracts/config.md), fail-fast validation (unknown keys collected by
//! `serde_ignored`, value rules by `garde`, presence-gated sections, port
//! rules, the
//! closed access-log variable set), and source loading (`.env` via
//! `dotenvy`; the env overlays are declared as clap `env` attributes in the
//! CLI — FR-016, in [`sources`]).
//!
//! Module layout: [`schema`] (structs/enums + serde/garde/smart-default
//! attributes), [`sources`] (`.env` loading), [`Error`]. Validation is derive-driven
//! ([`garde::Validate`] on every section); [`schema::Config::parse`] runs it
//! after deserialization and maps the report onto [`Error`].
//!
//! Section types live under their module path (`log::Config`, `api::Http`);
//! the root document is [`Config`].

mod error;
pub mod schema;
pub mod sources;

pub use self::error::Error;

pub use schema::{Config, Version, api, auth, log, s3, scanner, server, storage, telemetry};
