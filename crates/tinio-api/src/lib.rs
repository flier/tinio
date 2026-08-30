//! Management plane for tinio.
//!
//! The optional management surface (feature `api`, default on at the facade):
//! axum router with `/status`, `/stop`, `/metrics`, `/openapi.json`, token
//! authentication, control-channel transports (unix socket on Linux/macOS,
//! Windows named pipe, optional TCP HTTP/HTTPS), the state file with
//! single-instance enforcement, and the status/stop client used by the CLI.
//!
//! Module layout is populated by the US2 tasks (router, transport, state,
//! openapi, client, error); nothing is public yet.

#[doc(hidden)]
pub extern crate tinio_core as _core;
#[cfg(test)]
extern crate tinio_util as _util;

mod error;

pub use self::error::{Error, ErrorBody};
