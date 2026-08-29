//! S3 compatibility layer for tinio.
//!
//! Hosts the s3s protocol layer (routing, XML, error codes, SigV4/SigV2
//! verification) over the `tinio-core` storage contract: the `backend/`
//! modules map the ~30 S3 operations onto the contract, `data.rs` wires the
//! hyper data plane, and `log.rs`/`metrics.rs` provide observability. The
//! capability groups `multipart`, `copy`, `list-v1`, `list-v2` are strippable
//! cargo features (default on); `otel` enables the opt-in OpenTelemetry
//! export layer.

pub mod backend;

mod data;
mod error;
pub mod log;
pub mod metrics;
pub mod pipeline;

pub use self::backend::{Capabilities, S3Backend};
pub use self::data::DataPlane;
pub use self::error::Error;
