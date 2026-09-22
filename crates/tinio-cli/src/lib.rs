//! Command-line interface for tinio.
//!
//! The lifecycle commands — `server`/`start`, `status`, `stop`, `doctor` and `service` — with
//! a Minio-style invocation (positional directory argument, default ports 9000/9001), the
//! FR-016 four-level precedence chain, and the exit-code mapping 0/1/2.
//!
//! [`run()`] is the process entry point and the design's §1 sequence: the confinement payload is
//! read before any parser sees argv, the command line is parsed **twice** (design decision 3:
//! the discovery pass answers `DIR`, `--config`, the mode and `TINIO_STATE_DIR`; `<state dir>`
//! `.env` is loaded; the second pass is the authoritative one, and the `ArgMatches` is what
//! carries FR-016's provenance), the command is dispatched, and the process's **one** runtime is
//! built last — after the fork and the confinement, because forking a multi-threaded process is
//! the hazard that order exists to avoid.

#[cfg(feature = "api")]
#[doc(hidden)]
pub extern crate tinio_api as _api;
#[doc(hidden)]
pub extern crate tinio_config as _config;
#[doc(hidden)]
pub extern crate tinio_core as _core;
#[doc(hidden)]
pub extern crate tinio_fs as _fs;
#[cfg(feature = "mem")]
#[doc(hidden)]
pub extern crate tinio_mem as _mem;
#[doc(hidden)]
pub extern crate tinio_sandbox as _sandbox;
#[doc(hidden)]
pub extern crate tinio_server as _server;
#[cfg(test)]
extern crate tinio_util as _util;

#[doc(inline)]
pub use self::{error::Error, run::run};

pub mod cli;
pub mod commands;
/// **`api`-only, like the flag it serves.** A build without the feature has no plane, so it has
/// no `state` file to prove readiness on and no parent-side wait that could; `--daemon` is
/// therefore not in that build's surface at all (clap answers an unknown argument), the same
/// shape `--sandbox` takes when its own feature is off.
#[cfg(feature = "api")]
mod daemon;
mod error;
/// systemd's notification protocol and the unit text that asks for it. `sd-notify` cannot
/// link on a non-unix target (`std::os::unix::*`), so the whole module is the gate — there is
/// no Windows stub.
#[cfg(unix)]
mod notify;
pub mod resolve;
mod run;
pub mod sandbox;
pub mod service;
mod signal;
