//! S3 authn/authz engine for tinio.
//!
//! [`identity`] is the configured-principal model (access key → user, the
//! default owner element, the anonymous special ID); [`auth`] is the s3s
//! `S3Auth` provider over it; [`error`] is the access-decision vocabulary
//! the authorization pipeline consumes; [`matrix`] is the
//! operation→requirement truth table, [`canned`] the canned-ACL
//! expansion and request-level grant parsing, [`access`] the
//! pre-route `S3Access` authorization pipeline (spec §4), [`bypass`] the
//! governance-bypass authority, [`source`] the one parse authority for the
//! copy/rename source headers, and [`metrics`] the two decision families.

#[doc(hidden)]
pub extern crate tinio_core as _core;

pub mod access;
pub mod auth;
pub mod bypass;
pub mod canned;
pub mod error;
pub mod identity;
pub mod matrix;
pub mod metrics;
pub mod source;

pub use self::{
    access::AclAccess,
    auth::ConfigAuth,
    bypass::{MayBypass, may_bypass_authority},
    canned::{
        GrantHeaders, canned_bucket_grants, canned_object_grants, expand_acl, grants_from_headers,
    },
    error::{AccessDecision, classify},
    identity::{ANONYMOUS_CANONICAL_ID, Identity, User, derive_canonical_id},
    matrix::{OpRule, Requirement, requirement_for, rule_for},
};
