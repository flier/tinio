//! Management plane for tinio.
//!
//! The optional management surface (feature `api`, default on at the facade):
//! axum router with `/status`, `/stop`, `/metrics`, `/openapi.json` (and the
//! three `/buckets/{bucket}/lifecycle` routes under the `lifecycle` feature),
//! token authentication, control-channel transports (unix socket on Linux/macOS,
//! Windows named pipe, optional TCP HTTP/HTTPS), the state file with
//! single-instance enforcement, and the status/stop client used by the CLI.
//!
//! Module layout is populated by the US2 tasks (router, transport, state,
//! openapi, client, error).

#[doc(hidden)]
pub extern crate tinio_core as _core;
extern crate tinio_util as _util;

pub mod bind;
pub mod client;
mod error;
#[cfg(feature = "lifecycle")]
pub mod lifecycle;
pub mod metrics;
#[cfg(feature = "openapi")]
pub mod openapi;
#[cfg(windows)]
pub mod pipe;
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
