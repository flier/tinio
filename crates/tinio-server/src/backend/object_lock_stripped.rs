//! The stripped-build runtime refusal test (OL2, OL20).
//!
//! Compiled only when the `object-lock` cargo feature is **off**
//! (`#[cfg(all(test, not(feature = "object-lock")))]` in `backend/mod.rs`),
//! so the six rows are absent from the operation table by construction: the
//! generated `impl S3 for S3Backend` carries no arm for them and s3s's trait
//! defaults answer `NotImplemented` — even though the runtime capability
//! (`Capabilities::object_lock`, whose default is on) says the group is
//! wanted. The strip is the capability-off shape at compile time.
//!
//! The second half is OL20's invariant: enforcement is not a gated surface.
//! A lock-enabled bucket created through the **storage API** and a
//! default-retention version written through the same API still refuse a
//! permanent delete with `403 AccessDenied`. The write surface is enforcement
//! too: in a stripped build a PUT/COPY/multipart carry no lock headers, but
//! the bucket's stored default retention must still ride the version they
//! create, exactly as it does with the feature compiled in. Nothing about the
//! stripped server may turn a stored lock off.

#[cfg(test)]
mod tests {
    #[cfg(feature = "versioning")]
    use std::io::Error as IoError;

    #[cfg(feature = "versioning")]
    use bytes::Bytes;
    #[cfg(feature = "versioning")]
    use futures::stream;
    #[cfg(feature = "versioning")]
    use http::HeaderValue;
    use s3s::{S3, S3Error, dto};

    #[cfg(feature = "versioning")]
    use crate::_core::{
        acl::Acl,
        bucket, object,
        object_lock::{Config, DefaultRetention, Mode, Period, Rule},
        storage::{BucketOps, ObjectOps, VersionOps},
        version,
    };
    #[cfg(feature = "versioning")]
    use crate::backend::testutil::VALID_MD5;
    // The stripped-dispatch test runs under every feature set this module is
    // built under, so its helpers are ungated (they are available whenever
    // `testutil` is compiled). The versioning fixtures below need the
    // versioning storage API.
    use crate::{
        _mem::MemoryStorage,
        backend::{S3Backend, testutil::s3_request},
    };

    /// The framework's own refusal sentence — distinct from the capability
    /// decorator's `"<Operation> is disabled"`, so the test proves the arm is
    /// absent rather than runtime-gated.
    fn assert_stripped(op: &str, err: &S3Error) {
        assert_eq!(err.code().as_str(), "NotImplemented", "{op}: {err:?}");
        let expected = format!("{op} is not implemented yet");
        assert_eq!(
            err.message(),
            Some(expected.as_str()),
            "{op} must answer s3s's trait default, not a handler: {err:?}"
        );
    }

    /// A `Compliance`/30-day default rule — the lock every storage-created
    /// fixture bucket carries.
    #[cfg(feature = "versioning")]
    fn lock_config() -> Config {
        Config {
            enabled: true,
            rule: Some(Rule {
                default_retention: DefaultRetention {
                    mode: Mode::Compliance,
                    period: Period::Days(30),
                },
            }),
        }
    }

    /// A backend over `MemoryStorage` whose `locked` bucket was created
    /// lock-enabled **through the storage API** — the fixture path no
    /// stripped handler can reach.
    #[cfg(feature = "versioning")]
    async fn lock_backend() -> (S3Backend<MemoryStorage>, bucket::Name) {
        let storage = MemoryStorage::new().unwrap();
        let name = bucket::name("locked").unwrap();
        storage
            .create_bucket_with_lock(&name, None, &Acl::default_private(None), &lock_config())
            .await
            .expect("the storage API creates the lock-enabled bucket");
        (S3Backend::new(storage, Default::default()), name)
    }

    /// A request body for the write surface.
    #[cfg(feature = "versioning")]
    fn body(payload: &'static [u8]) -> dto::StreamingBlob {
        dto::StreamingBlob::wrap(stream::once(async move {
            Ok::<_, IoError>(Bytes::from_static(payload))
        }))
    }

    /// Attach the well-formed `Content-MD5` header the OL16 gate reads on a
    /// multipart create.
    #[cfg(feature = "versioning")]
    fn send_md5<T>(mut req: s3s::S3Request<T>) -> s3s::S3Request<T> {
        req.headers
            .insert("content-md5", HeaderValue::from_static(VALID_MD5));
        req
    }

    /// The version a write answered, from its `x-amz-version-id` header.
    #[cfg(feature = "versioning")]
    fn version_of(headers: &http::HeaderMap) -> version::Id {
        let wire = headers
            .get("x-amz-version-id")
            .and_then(|value| value.to_str().ok())
            .expect("a versioned write answers its identity");
        version::id(wire).expect("the header is a valid version id")
    }

    /// A version-addressed permanent delete of `key`'s `version`, through the
    /// server's S3 dispatch.
    #[cfg(feature = "versioning")]
    async fn permanent_delete(
        backend: &S3Backend<MemoryStorage>,
        key: &str,
        version: &version::Id,
    ) -> S3Error {
        backend
            .delete_object(s3_request(dto::DeleteObjectInput {
                bucket: "locked".into(),
                key: key.into(),
                version_id: Some(version::wire_of(Some(version))),
                ..Default::default()
            }))
            .await
            .unwrap_err()
    }

    /// The six Object Lock operations are absent from the dispatch in a stripped
    /// build, whatever the `[s3] object_lock` capability says.
    #[tokio::test]
    async fn the_six_object_lock_operations_are_stripped_from_the_dispatch() {
        let backend = S3Backend::new(MemoryStorage::new().unwrap(), Default::default());
        assert!(
            backend.capabilities().object_lock,
            "the runtime capability is on: the refusal below is the compile-time strip"
        );
        let bucket = "data".to_owned();

        let err = backend
            .get_object_lock_configuration(s3_request(dto::GetObjectLockConfigurationInput {
                bucket: bucket.clone(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_stripped("GetObjectLockConfiguration", &err);

        let err = backend
            .put_object_lock_configuration(s3_request(dto::PutObjectLockConfigurationInput {
                bucket: bucket.clone(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_stripped("PutObjectLockConfiguration", &err);

        let err = backend
            .get_object_retention(s3_request(dto::GetObjectRetentionInput {
                bucket: bucket.clone(),
                key: "k".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_stripped("GetObjectRetention", &err);

        let err = backend
            .put_object_retention(s3_request(dto::PutObjectRetentionInput {
                bucket: bucket.clone(),
                key: "k".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_stripped("PutObjectRetention", &err);

        let err = backend
            .get_object_legal_hold(s3_request(dto::GetObjectLegalHoldInput {
                bucket: bucket.clone(),
                key: "k".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_stripped("GetObjectLegalHold", &err);

        let err = backend
            .put_object_legal_hold(s3_request(dto::PutObjectLegalHoldInput {
                bucket,
                key: "k".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_stripped("PutObjectLegalHold", &err);
    }

    /// OL20 on the PUT surface: a stripped build has no lock headers, but the
    /// bucket's stored default retention still rides the version the PUT
    /// creates — a version-addressed delete answers `403 AccessDenied`.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_stripped_put_still_applies_the_bucket_default_retention() {
        let (backend, name) = lock_backend().await;
        let put = backend
            .put_object(s3_request(dto::PutObjectInput {
                bucket: "locked".into(),
                key: "written".into(),
                body: Some(body(b"payload")),
                content_md5: Some(VALID_MD5.to_owned()),
                ..Default::default()
            }))
            .await
            .expect("the stripped PUT writes the object");
        let protected = version_of(&put.headers);

        let key = object::key("written").unwrap();
        let retention = backend
            .storage()
            .get_object_retention(&name, &key, Some(&protected))
            .await
            .expect("the storage read is the enforcement authority");
        assert!(
            retention.is_some(),
            "the stripped PUT stored the bucket's default retention"
        );
        let err = permanent_delete(&backend, "written", &protected).await;
        assert_eq!(
            err.code().as_str(),
            "AccessDenied",
            "the default-retention version the stripped PUT created is protected: {err:?}"
        );
    }

    /// OL16/OL20 on the stripped PUT surface: the stored default's retention
    /// still requires a checksum. The strip omits the lock *headers*, never
    /// the enforcement they would have needed.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_stripped_put_still_requires_the_lock_checksum() {
        let (backend, _) = lock_backend().await;
        let err = backend
            .op_put_object(s3_request(dto::PutObjectInput {
                bucket: "locked".into(),
                key: "no-digest".into(),
                body: Some(body(b"payload")),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(
            err.code().as_str(),
            "InvalidRequest",
            "the stored default retention requires a checksum: {err:?}"
        );
        assert!(
            err.message()
                .is_some_and(|message| message.contains("Content-MD5")),
            "{err:?}"
        );
    }

    /// OL20 on the COPY surface: the destination bucket's stored default
    /// retention rides the copy, so the copy cannot create an unprotected
    /// version.
    #[cfg(all(feature = "versioning", feature = "copy"))]
    #[tokio::test]
    async fn a_stripped_copy_still_applies_the_destination_bucket_default_retention() {
        let (backend, name) = lock_backend().await;
        backend
            .put_object(s3_request(dto::PutObjectInput {
                bucket: "locked".into(),
                key: "src".into(),
                body: Some(body(b"source")),
                content_md5: Some(VALID_MD5.to_owned()),
                ..Default::default()
            }))
            .await
            .expect("the source write");
        let mut copy = s3_request(
            dto::CopyObjectInput::builder()
                .bucket("locked".to_owned())
                .key("dst".to_owned())
                .copy_source(dto::CopySource::parse("locked/src").unwrap())
                .build()
                .expect("a copy input"),
        );
        copy.headers
            .insert("content-md5", HeaderValue::from_static(VALID_MD5));
        let copied = backend
            .copy_object(copy)
            .await
            .expect("the stripped COPY writes the destination");
        let protected = version_of(&copied.headers);

        let key = object::key("dst").unwrap();
        assert!(
            backend
                .storage()
                .get_object_retention(&name, &key, Some(&protected))
                .await
                .expect("the storage read is the enforcement authority")
                .is_some(),
            "the stripped COPY stored the destination bucket's default retention"
        );
        let err = permanent_delete(&backend, "dst", &protected).await;
        assert_eq!(
            err.code().as_str(),
            "AccessDenied",
            "the default-retention version the stripped COPY created is protected: {err:?}"
        );
    }

    /// OL16/OL20 on the stripped COPY surface: without a checksum the
    /// destination's stored default refuses the copy before it writes.
    #[cfg(all(feature = "versioning", feature = "copy"))]
    #[tokio::test]
    async fn a_stripped_copy_still_requires_the_lock_checksum() {
        let (backend, _) = lock_backend().await;
        backend
            .put_object(s3_request(dto::PutObjectInput {
                bucket: "locked".into(),
                key: "src".into(),
                body: Some(body(b"source")),
                content_md5: Some(VALID_MD5.to_owned()),
                ..Default::default()
            }))
            .await
            .expect("the source write");
        let err = backend
            .copy_object(s3_request(
                dto::CopyObjectInput::builder()
                    .bucket("locked".to_owned())
                    .key("dst".to_owned())
                    .copy_source(dto::CopySource::parse("locked/src").unwrap())
                    .build()
                    .expect("a copy input"),
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest", "{err:?}");
    }

    /// OL20 on the multipart-create surface: the lock resolved at create
    /// rides the completion, so a completed upload is protected like a PUT.
    #[cfg(all(feature = "versioning", feature = "multipart"))]
    #[tokio::test]
    async fn a_stripped_multipart_upload_still_applies_the_bucket_default_retention() {
        let (backend, name) = lock_backend().await;
        let created = backend
            .create_multipart_upload(send_md5(s3_request(dto::CreateMultipartUploadInput {
                bucket: "locked".into(),
                key: "upload".into(),
                ..Default::default()
            })))
            .await
            .expect("the stripped create");
        let upload_id = created.output.upload_id.unwrap();
        let part = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: "locked".into(),
                key: "upload".into(),
                upload_id: upload_id.clone(),
                part_number: 1,
                body: Some(body(b"data")),
                ..Default::default()
            }))
            .await
            .expect("the part write");
        let etag = part.output.e_tag.unwrap();
        backend
            .complete_multipart_upload(s3_request(dto::CompleteMultipartUploadInput {
                bucket: "locked".into(),
                key: "upload".into(),
                upload_id,
                multipart_upload: Some(dto::CompletedMultipartUpload {
                    parts: Some(vec![dto::CompletedPart {
                        part_number: Some(1),
                        e_tag: Some(etag),
                        ..Default::default()
                    }]),
                }),
                ..Default::default()
            }))
            .await
            .expect("the completion");

        let key = object::key("upload").unwrap();
        let protected = backend
            .storage()
            .head_object(&name, &key)
            .await
            .unwrap()
            .version_id
            .expect("a versioned bucket answers the version the completion created");
        assert!(
            backend
                .storage()
                .get_object_retention(&name, &key, Some(&protected))
                .await
                .expect("the storage read is the enforcement authority")
                .is_some(),
            "the completion stored the bucket's default retention"
        );
        let err = permanent_delete(&backend, "upload", &protected).await;
        assert_eq!(
            err.code().as_str(),
            "AccessDenied",
            "the default-retention version the stripped upload completed is protected: {err:?}"
        );
    }

    /// OL16/OL20 on the stripped multipart-create surface: the resolved
    /// create-time lock's retention requires a checksum before the upload row
    /// exists.
    #[cfg(all(feature = "versioning", feature = "multipart"))]
    #[tokio::test]
    async fn a_stripped_multipart_create_still_requires_the_lock_checksum() {
        let (backend, _) = lock_backend().await;
        let err = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: "locked".into(),
                key: "no-digest".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest", "{err:?}");
    }

    /// OL6/OL20: the Complete-header rejection is enforcement, not a gated
    /// surface — a stripped build refuses the same lock and bypass headers.
    #[cfg(all(feature = "versioning", feature = "multipart"))]
    #[tokio::test]
    async fn a_stripped_complete_rejects_lock_headers() {
        let (backend, _) = lock_backend().await;
        for header in [
            "x-amz-object-lock-mode",
            "x-amz-object-lock-retain-until-date",
            "x-amz-object-lock-legal-hold",
            "x-amz-bypass-governance-retention",
        ] {
            let mut req = s3_request(dto::CompleteMultipartUploadInput {
                bucket: "locked".into(),
                key: "k".into(),
                upload_id: "u".into(),
                ..Default::default()
            });
            req.headers
                .insert(header, HeaderValue::from_static("GOVERNANCE"));
            let err = backend.complete_multipart_upload(req).await.unwrap_err();
            assert_eq!(err.code().as_str(), "InvalidRequest", "{header}: {err:?}");
            assert!(
                err.message()
                    .is_some_and(|message| message.contains(header)),
                "the refusal names {header}: {err:?}"
            );
        }
    }

    /// OL20's storage-API half in the stripped build: the bucket is created
    /// lock-enabled through the storage API, the default compliance retention
    /// rides a storage write, and the server's permanent delete still answers
    /// `403 AccessDenied` — then the protected version is still there.
    ///
    /// Requires `versioning` (the stripped feature set this proves: versioning
    /// on, `object-lock` off — `tinio-server`'s default). Without versioning a
    /// version-addressed delete is refused before the store, so there is no
    /// enforcement path to prove.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_stripped_build_still_refuses_a_protected_delete() {
        let (backend, name) = lock_backend().await;
        let key = object::key("k").unwrap();
        let body = Box::pin(stream::iter([Ok::<_, IoError>(Bytes::from_static(
            b"payload",
        ))]));
        backend
            .storage()
            .put_object(&name, &key, body)
            .await
            .expect("the storage write applies the bucket's default retention");
        let protected = backend
            .storage()
            .head_object(&name, &key)
            .await
            .unwrap()
            .version_id
            .expect("a versioned bucket answers the version the write created");

        let err = permanent_delete(&backend, "k", &protected).await;
        assert_eq!(
            err.code().as_str(),
            "AccessDenied",
            "the default retention still protects the version in a stripped build: {err:?}"
        );
        let survivor = backend
            .storage()
            .head_object(&name, &key)
            .await
            .expect("the protected version survives");
        assert_eq!(
            survivor.version_id.as_ref(),
            Some(&protected),
            "the protected version is still the current one"
        );
    }
}
