//! Management plane for tinio.
//!
//! The optional management surface (feature `api`, default on at the facade):
//! axum router with `/status`, `/stop`, `/metrics`, `/openapi.json`, token
//! authentication, control-channel transports (unix socket on Linux/macOS,
//! Windows named pipe, optional TCP HTTP/HTTPS), the state file with
//! single-instance enforcement, and the status/stop client used by the CLI.
//!
//! Module layout is populated by the US2 tasks (router, transport, state,
//! openapi, client, error).

#[doc(hidden)]
pub extern crate tinio_core as _core;
#[cfg(test)]
extern crate tinio_util as _util;
#[cfg(windows)]
#[doc(hidden)]
pub extern crate tinio_winsec as _winsec;

pub mod bind;
pub mod client;
#[cfg(windows)]
pub mod dacl;
mod error;
pub mod metrics;
#[cfg(feature = "openapi")]
pub mod openapi;
pub mod router;
pub mod state;
#[cfg(test)]
pub(crate) mod test_support;
pub mod transport;

#[cfg(feature = "openapi")]
pub use self::openapi::document;
pub use self::{
    error::{Error, ErrorBody},
    metrics::{DEFAULT_TTL, Gauges, MetricsSource},
    state::State,
    transport::{Options, Plane},
};
