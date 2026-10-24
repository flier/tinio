//! The S3 Object Lock handler tests (OL1–OL20, OL29).
//!
//! Split by concern: [`common`] carries the shared fixtures, [`configuration`]
//! the bucket configuration / echo / cache tests, [`retention`] the retention
//! set, clear and bypass tests, [`legal_hold`] the legal-hold tests, and
//! [`delete`] the delete, delete-marker, multipart and checksum tests.

mod common;
mod configuration;
mod delete;
mod legal_hold;
mod retention;
