//! S3 authn/authz engine for tinio.
//!
//! [`identity`] is the configured-principal model (access key → user, the
//! default owner element, the anonymous special ID); [`auth`] is the s3s
//! `S3Auth` provider over it; [`error`] is the access-decision vocabulary
//! the authorization pipeline consumes.

#[doc(hidden)]
pub extern crate tinio_core as _core;

pub mod auth;
pub mod error;
pub mod identity;

pub use self::{
    auth::ConfigAuth,
    error::{AccessDecision, classify},
    identity::{ANONYMOUS_CANONICAL_ID, Identity, User, derive_canonical_id},
};
