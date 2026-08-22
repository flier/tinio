//! S3 compatibility layer for tinio.
//!
//! Hosts the s3s protocol layer (routing, XML, error codes, SigV4/SigV2
//! verification) over the `tinio-core` storage contract: the `backend/`
//! modules map the ~30 S3 operations onto the contract, `data.rs` wires the
//! hyper data plane, and `log.rs`/`metrics.rs` provide observability. The
//! capability groups `multipart`, `copy`, `list-v1`, `list-v2` are strippable
//! cargo features (default on); `otel` enables the opt-in OpenTelemetry
//! export layer.
//!
//! Module layout is populated by the Phase 2 foundational tasks and US1;
//! nothing is public yet.

mod error;
pub mod metrics;

pub use self::error::Error;
