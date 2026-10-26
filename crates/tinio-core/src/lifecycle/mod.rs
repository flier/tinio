//! Bucket lifecycle configuration: the stored model, its JSON wire, and the
//! request-path validation (design LC3, LC8, LC17).
//!
//! **One wire per surface** (LC17). The S3 subresource speaks the framework's
//! XML, and this module never does: the row the store keeps and the JSON the
//! management plane and the CLI speak are this type's own `serde` shape, so a
//! rule is spelled once and there is no third spelling. [`Config::to_wire`]
//! and [`Config::from_wire`] are that shape and nothing else.
//!
//! **A corrupt row self-heals on read** (LC8). [`Config::from_wire`] answers
//! `None` and logs one warning — never an error, and never a configuration
//! read as "retain everything" (the RustFS post-mortem: a corrupt count
//! silently stopped expiry). The wire carries no `deny_unknown_fields`, so a
//! field a later binary adds is tolerated by an earlier one rather than
//! corrupting the row.
//!
//! **Validation is the request path's, never the read path's** (the CORS
//! rule). [`Config::validate`] answers two classes, carried by
//! [`InvalidKind`] so the S3 surface maps one field instead of re-deriving
//! the class from a message: a document whose filter does not match the
//! published schema is a [`Shape`](InvalidKind::Shape) violation
//! (`MalformedXML`), every semantic violation is
//! [`Semantic`](InvalidKind::Semantic) (`InvalidRequest`).
//!
//! This is the stored shape, not `s3s::dto`: a transition is modelled as
//! [`Transition`] (T11), `NoncurrentVersionTransition` is not modelled at all,
//! and no member AWS documents but this build does not act on is mirrored
//! here — the S3 surface maps the dto onto these types and refuses the rest.
//!
//! **The matcher is pure** (LC10): [`Facts`] is what a listing already carries,
//! so evaluating a rule costs no read, and the day arithmetic lives in one
//! place — [`midnight_utc`].

mod eval;
mod model;

pub use self::{eval::*, model::*};

#[cfg(test)]
mod tests;
