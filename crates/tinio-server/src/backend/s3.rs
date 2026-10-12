//! The s3s `S3` dispatch over [`S3Backend`] — **generated from the ops table**
//! ([`tinio_s3_ops`]), so an operation is declared in exactly one place.
//!
//! Each row of `backend/ops.rs` names its s3s trait method (the row's own
//! name), its dto pair, and — through `#[handler(...)]` — the inherent method
//! on `S3Backend` that answers it. This module expands the table into the
//! forwarding `impl`: one arm per row, under the row's own `#[cfg]`, calling
//! `self.<handler>(req)`. The body of every arm is the same three lines, so
//! there is nothing here to keep in step with the table by hand.
//!
//! **What the compiler holds.** The arm's name comes from the row and the
//! trait's own signature fixes its dto pair, so a row that names a handler
//! that does not exist (`E0599`), or one whose dtos do not match (`E0308`),
//! keeps the crate from building — the "operation no dispatch arm answers, a
//! 501 waiting to happen" failure the source-parsing cross-check used to
//! catch. A dispatch arm the table does not describe cannot be written at
//! all: the whole `impl` is generated, and a hand-added second `impl S3 for
//! S3Backend` would be a conflicting implementation (`E0119`).
//!
//! The handlers themselves live beside their operation groups —
//! `buckets.rs`, `objects.rs`, `listing.rs`, `multipart.rs`, `cors.rs`,
//! `select.rs`, `versioning.rs` — as `pub(crate) async fn op_<name>`.

use async_trait::async_trait;
use s3s::{S3, S3Request, S3Response, S3Result, dto};

use super::{S3Backend, ops::tinio_s3_ops};
use crate::_core::storage::Storage;

/// The base dispatch, expanded from [`tinio_s3_ops`]. See this module's doc
/// for what the compiler holds.
macro_rules! s3_dispatch_impl {
    (
        read { $(#[handler($rh:ident)] $(#[gated($($rg:ident $(: $rf:literal)?),+)])? $(#[on_ok($rok:expr)])? fn $r:ident ( $ri:ident ) -> $ro:ident; )* }
        write { $(#[handler($wh:ident)] $(#[gated($($wg:ident $(: $wf:literal)?),+)])? $(#[on_ok($wok:expr)])? fn $w:ident ( $wi:ident ) -> $wo:ident; )* }
    ) => {
        #[async_trait]
        impl<S: Storage> S3 for S3Backend<S> {
            $(
                $( $( $(#[cfg(feature = $rf)])? )+ )?
                async fn $r(&self, req: S3Request<dto::$ri>) -> S3Result<S3Response<dto::$ro>> {
                    self.$rh(req).await
                }
            )*
            $(
                $( $( $(#[cfg(feature = $wf)])? )+ )?
                async fn $w(&self, req: S3Request<dto::$wi>) -> S3Result<S3Response<dto::$wo>> {
                    self.$wh(req).await
                }
            )*
        }
    };
}

tinio_s3_ops!(s3_dispatch_impl);
