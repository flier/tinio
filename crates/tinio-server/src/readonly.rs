//! Read-only mode as a decorator (FR-023).
//!
//! [`S3ReadonlyBackend`] forwards every read to the wrapped implementation
//! and refuses every write with `AccessDenied`, without touching it. The
//! write set is the table's `write` group, so it cannot drift from the
//! protocol mapping.
//!
//! The refusal message is the same literal `tinio-auth`'s `denied()` uses
//! (AWS's default), so a read-only refusal is indistinguishable on the wire
//! from an ACL denial — see `contracts/s3-surface.md`'s 403-ambiguity note.

use async_trait::async_trait;
use s3s::{S3, S3Request, S3Response, S3Result, dto, s3_error};

use crate::backend::ops::tinio_s3_ops;

/// Wraps an `S3` implementation, refusing every mutating operation.
#[derive(Debug, Clone)]
pub struct S3ReadonlyBackend<T> {
    inner: T,
}

impl<T> S3ReadonlyBackend<T> {
    /// Wrap `inner` in read-only mode.
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

macro_rules! readonly_impl {
    (
        read { $( $(#[gated($($rg:ident $(: $rf:literal)?),+)])? $(#[on_ok($rh:expr)])? fn $r:ident ( $ri:ident ) -> $ro:ident; )* }
        write { $( $(#[gated($($wg:ident $(: $wf:literal)?),+)])? $(#[on_ok($wh:expr)])? fn $w:ident ( $wi:ident ) -> $wo:ident; )* }
    ) => {
        #[async_trait]
        impl<T: S3 + Send + Sync> S3 for S3ReadonlyBackend<T> {
            // reads: forward
            $(
                $( $( $(#[cfg(feature = $rf)])? )+ )?
                async fn $r(&self, req: S3Request<dto::$ri>) -> S3Result<S3Response<dto::$ro>> {
                    self.inner.$r(req).await
                }
            )*
            // writes: refuse, never touching the inner backend
            $(
                $( $( $(#[cfg(feature = $wf)])? )+ )?
                async fn $w(&self, _req: S3Request<dto::$wi>) -> S3Result<S3Response<dto::$wo>> {
                    Err(s3_error!(AccessDenied, "Access Denied"))
                }
            )*
        }
    };
}

tinio_s3_ops!(readonly_impl);

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use http::Method;

    use super::*;
    use crate::backend::{
        ops::{read_ops, table_ops, write_ops},
        testutil::s3_request,
    };

    /// The fake inner backend: it records every call it receives and
    /// answers `Ok`, so a forwarded operation is observable twice — as the
    /// result the caller sees and as the record. Every method is an
    /// override: s3s's trait defaults answer `NotImplemented` without
    /// recording, so a fake that inherited them could not tell "the
    /// decorator forwarded and the inner answered" from "the decorator
    /// answered by itself".
    ///
    /// The record is shared with the test through an `Arc`, because the
    /// decorator consumes the backend by value.
    #[derive(Clone, Default)]
    struct RecordingBackend {
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl RecordingBackend {
        /// Record one call.
        fn record(&self, op: &'static str) {
            self.calls.lock().unwrap().push(op);
        }

        /// The operations received so far, in call order.
        fn recorded(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }
    }

    /// A probe input for one table row.
    ///
    /// Most inputs derive `Default`; the seven that do not — their required
    /// fields are non-optional — are built through s3s's own builders with
    /// empty required values. Nothing reads the value: a read forwards it
    /// untouched to the fake, and a write is refused before it is looked
    /// at, so only the type matters. Any other input without `Default`
    /// fails to compile here instead of going quietly undriven.
    macro_rules! probe_input {
        (CopyObjectInput) => {
            dto::CopyObjectInput::builder()
                .bucket(String::new())
                .key(String::new())
                .copy_source(dto::CopySource::parse("src/key").expect("a copy source"))
                .build()
                .expect("a probe input")
        };
        (UploadPartCopyInput) => {
            dto::UploadPartCopyInput::builder()
                .bucket(String::new())
                .key(String::new())
                .copy_source(dto::CopySource::parse("src/key").expect("a copy source"))
                .part_number(1)
                .upload_id(String::new())
                .build()
                .expect("a probe input")
        };
        (SelectObjectContentInput) => {
            dto::SelectObjectContentInput::builder()
                .bucket(String::new())
                .key(String::new())
                .request(dto::SelectObjectContentRequest {
                    expression: String::new(),
                    expression_type: dto::ExpressionType::from(String::new()),
                    input_serialization: Default::default(),
                    output_serialization: Default::default(),
                    request_progress: None,
                    scan_range: None,
                })
                .build()
                .expect("a probe input")
        };
        (PutBucketTaggingInput) => {
            dto::PutBucketTaggingInput::builder()
                .bucket(String::new())
                .tagging(Default::default())
                .build()
                .expect("a probe input")
        };
        (PutBucketCorsInput) => {
            dto::PutBucketCorsInput::builder()
                .bucket(String::new())
                .cors_configuration(Default::default())
                .build()
                .expect("a probe input")
        };
        (DeleteObjectsInput) => {
            dto::DeleteObjectsInput::builder()
                .bucket(String::new())
                .delete(Default::default())
                .build()
                .expect("a probe input")
        };
        (PutObjectTaggingInput) => {
            dto::PutObjectTaggingInput::builder()
                .bucket(String::new())
                .key(String::new())
                .tagging(Default::default())
                .build()
                .expect("a probe input")
        };
        ($other:ident) => {
            dto::$other::default()
        };
    }

    /// Test-only consumer of the shared table ([`tinio_s3_ops`]): the
    /// recording fake's `S3` impl and the two call-by-name dispatches the
    /// tests drive the decorator with, so neither can drift from the
    /// decorator's own operation set — same rows, same `#[cfg]`s.
    macro_rules! readonly_probe {
        (
            read { $( $(#[gated($($rg:ident $(: $rf:literal)?),+)])? $(#[on_ok($rh:expr)])? fn $r:ident ( $ri:ident ) -> $ro:ident; )* }
            write { $( $(#[gated($($wg:ident $(: $wf:literal)?),+)])? $(#[on_ok($wh:expr)])? fn $w:ident ( $wi:ident ) -> $wo:ident; )* }
        ) => {
            #[async_trait]
            impl S3 for RecordingBackend {
                $(
                    $( $( $(#[cfg(feature = $rf)])? )+ )?
                    async fn $r(&self, _req: S3Request<dto::$ri>) -> S3Result<S3Response<dto::$ro>> {
                        self.record(stringify!($r));
                        Ok(S3Response::new(Default::default()))
                    }
                )*
                $(
                    $( $( $(#[cfg(feature = $wf)])? )+ )?
                    async fn $w(&self, _req: S3Request<dto::$wi>) -> S3Result<S3Response<dto::$wo>> {
                        self.record(stringify!($w));
                        Ok(S3Response::new(Default::default()))
                    }
                )*
            }

            /// Drive one read row through `backend`, by its table name.
            async fn call_read<T: S3>(backend: &T, op: &str) -> S3Result<()> {
                match op {
                    $(
                        $( $( $(#[cfg(feature = $rf)])? )+ )?
                        stringify!($r) => backend.$r(s3_request(probe_input!($ri))).await.map(|_| ()),
                    )*
                    other => panic!("no read row named {other}"),
                }
            }

            /// Drive one write row through `backend`, by its table name.
            async fn call_write<T: S3>(backend: &T, op: &str) -> S3Result<()> {
                match op {
                    $(
                        $( $( $(#[cfg(feature = $wf)])? )+ )?
                        stringify!($w) => backend.$w(s3_request(probe_input!($wi))).await.map(|_| ()),
                    )*
                    other => panic!("no write row named {other}"),
                }
            }
        };
    }

    tinio_s3_ops!(readonly_probe);

    /// The rows of `names` this build compiles. [`read_ops`] and
    /// [`write_ops`] are deliberately unfiltered (they pin the table's
    /// groups in every build), while [`call_read`]/[`call_write`] carry
    /// only the arms this feature set compiles — so the tests intersect
    /// the two through [`table_ops`], the same filtering the decorator's
    /// own `#[cfg]`s apply.
    fn compiled(names: Vec<&'static str>) -> Vec<&'static str> {
        let ops = table_ops();
        names
            .into_iter()
            .filter(|name| ops.contains(name))
            .collect()
    }

    /// Every read row reaches the inner backend and the inner really
    /// answered it: a read the decorator did not forward would fall back on
    /// s3s's trait default, `NotImplemented`. The record is the second
    /// witness — the forwarded set is the table's read group, no more and
    /// no less.
    #[tokio::test]
    async fn every_read_reaches_the_inner_backend() {
        let inner = RecordingBackend::default();
        let wrapped = S3ReadonlyBackend::new(inner.clone());
        let mut ops = compiled(read_ops());
        for op in &ops {
            call_read(&wrapped, op)
                .await
                .unwrap_or_else(|err| panic!("{op} was not forwarded: {err:?}"));
        }
        ops.sort_unstable();
        let mut recorded = inner.recorded();
        recorded.sort_unstable();
        assert_eq!(recorded, ops, "the reads the inner backend answered");
    }

    /// Every write row answers `AccessDenied` with the ACL denial's own
    /// literal, and the inner backend never sees the call — a forwarded
    /// write would land on the fake's `Ok` and record itself.
    #[tokio::test]
    async fn every_write_is_refused_without_reaching_the_inner_backend() {
        let inner = RecordingBackend::default();
        let wrapped = S3ReadonlyBackend::new(inner.clone());
        for op in compiled(write_ops()) {
            let err = match call_write(&wrapped, op).await {
                Ok(_) => panic!("{op} was not refused"),
                Err(err) => err,
            };
            assert_eq!(err.code().as_str(), "AccessDenied", "{op}: {err:?}");
            assert_eq!(err.message(), Some("Access Denied"), "{op}: {err:?}");
        }
        let recorded = inner.recorded();
        assert!(
            recorded.is_empty(),
            "the refused writes reached the inner backend: {recorded:?}"
        );
    }

    /// `post_object` is not a table row: s3s's trait default maps the
    /// browser form upload onto `put_object` and calls it on `self` — the
    /// decorator — so the POST path lands on the refusal without the inner
    /// backend seeing it.
    #[tokio::test]
    async fn post_object_is_refused() {
        let inner = RecordingBackend::default();
        let wrapped = S3ReadonlyBackend::new(inner.clone());
        let mut req = s3_request(dto::PostObjectInput::default());
        req.method = Method::POST;
        let err = match wrapped.post_object(req).await {
            Ok(_) => panic!("the form upload was not refused"),
            Err(err) => err,
        };
        assert_eq!(err.code().as_str(), "AccessDenied", "{err:?}");
        assert_eq!(err.message(), Some("Access Denied"), "{err:?}");
        let recorded = inner.recorded();
        assert!(
            recorded.is_empty(),
            "the form upload reached the inner backend: {recorded:?}"
        );
    }
}
