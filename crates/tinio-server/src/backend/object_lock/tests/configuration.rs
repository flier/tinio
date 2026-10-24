//! Bucket configuration, echo and cache tests: the create-bucket header, the
//! configuration round trip, the GET/HEAD lock echo, the write-path shape and
//! ownership gates, and the capability, read-only and `ObjectLockCache`
//! behavior.

use std::io;

use bytes::Bytes;
use futures::stream;
use s3s::{S3, dto};

use super::common::*;
use crate::{
    _core::{
        bucket,
        meta::VersionMeta,
        object,
        object_lock::{Config, LegalHold, Lock, Mode, Retention},
        storage::{BucketOps, VersionOps},
        version,
    },
    _fs::{FsStorage, testing::fs_options},
    backend::{
        Capabilities, S3Backend,
        testutil::{VALID_MD5, s3_request, setup, setup_gated_with_caps, setup_with_caps},
    },
    readonly::S3ReadonlyBackend,
};

#[tokio::test]
async fn the_create_bucket_header_enables_versioning_and_stores_the_config() {
    let (backend, b) = lock_backend().await;
    let name = bucket::name(&b).unwrap();
    assert_eq!(
        backend.storage().bucket_versioning(&name).await.unwrap(),
        version::State::Enabled,
        "the create header enables versioning"
    );
    let out = backend
        .op_get_object_lock_configuration(get_config(&b))
        .await
        .unwrap();
    let config = out.output.object_lock_configuration.unwrap();
    assert_eq!(
        config.object_lock_enabled.as_ref().map(|e| e.as_str()),
        Some(dto::ObjectLockEnabled::ENABLED)
    );
    assert!(config.rule.is_none(), "no default retention was asked for");
}

#[tokio::test]
async fn the_create_bucket_header_is_honoured_with_the_capability_off() {
    // R13: the header is enforcement, not one of the six gated surfaces.
    let caps = Capabilities {
        object_lock: false,
        ..Default::default()
    };
    let (backend, _) = setup_gated_with_caps(caps).await;
    backend
        .create_bucket(s3_request(dto::CreateBucketInput {
            bucket: "kept".into(),
            object_lock_enabled_for_bucket: Some(true),
            ..Default::default()
        }))
        .await
        .unwrap();
    let out = backend
        .inner()
        .op_get_object_lock_configuration(get_config("kept"))
        .await
        .unwrap();
    assert!(out.output.object_lock_configuration.is_some());
}

#[tokio::test]
async fn a_configuration_put_then_get_round_trips() {
    let (backend, b) = lock_backend().await;
    backend
        .op_put_object_lock_configuration(put_config(&b, Some(rule())))
        .await
        .unwrap();
    let out = backend
        .op_get_object_lock_configuration(get_config(&b))
        .await
        .unwrap();
    let stored = out.output.object_lock_configuration.unwrap();
    let default = stored.rule.unwrap().default_retention.unwrap();
    assert_eq!(default.days, Some(30));
    assert_eq!(default.years, None);
    assert_eq!(
        default.mode.as_ref().map(|m| m.as_str()),
        Some(dto::ObjectLockRetentionMode::COMPLIANCE)
    );
}

#[tokio::test]
async fn the_get_and_head_echo_the_lock_headers() {
    let (backend, b) = lock_backend().await;
    backend
        .op_put_object(put(
            &b,
            "k",
            Some(Mode::Compliance),
            Some(LegalHold::On),
            Some(VALID_MD5),
        ))
        .await
        .unwrap();

    let got = backend
        .op_get_object(s3_request(dto::GetObjectInput {
            bucket: b.clone(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        got.output.object_lock_mode.as_ref().map(|m| m.as_str()),
        Some(dto::ObjectLockMode::COMPLIANCE)
    );
    assert!(got.output.object_lock_retain_until_date.is_some());
    assert_eq!(
        got.output
            .object_lock_legal_hold_status
            .as_ref()
            .map(|h| h.as_str()),
        Some(dto::ObjectLockLegalHoldStatus::ON)
    );

    let head = backend
        .op_head_object(s3_request(dto::HeadObjectInput {
            bucket: b,
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        head.output.object_lock_mode.as_ref().map(|m| m.as_str()),
        Some(dto::ObjectLockMode::COMPLIANCE)
    );
    assert_eq!(
        head.output
            .object_lock_legal_hold_status
            .as_ref()
            .map(|h| h.as_str()),
        Some(dto::ObjectLockLegalHoldStatus::ON)
    );
}

#[tokio::test]
async fn the_null_version_echo_is_not_skipped() {
    // A lock row on the null identity: the echo keys the lookup by
    // `wire_of(version_id)` (`"null"` for `None`), never by the Option.
    let (backend, b) = setup().await;
    let name = bucket::name(&b).unwrap();
    let key = object::key("legacy").unwrap();
    // The object predates versioning, so its identity is the null one.
    backend
        .op_put_object(put(&b, "legacy", None, None, None))
        .await
        .unwrap();
    backend
        .op_put_bucket_versioning(s3_request(dto::PutBucketVersioningInput {
            bucket: b.clone(),
            versioning_configuration: dto::VersioningConfiguration {
                status: Some(dto::BucketVersioningStatus::from_static(
                    dto::BucketVersioningStatus::ENABLED,
                )),
                mfa_delete: None,
            },
            checksum_algorithm: None,
            content_md5: None,
            expected_bucket_owner: None,
            mfa: None,
        }))
        .await
        .unwrap();
    backend
        .op_put_object_lock_configuration(put_config(&b, None))
        .await
        .unwrap();
    // The test-only lock writer plants the null identity's lock (the S3
    // surface cannot reach it on an always-enabled lock bucket).
    VersionMeta::set_version_lock(
        backend.storage().metadata().as_ref(),
        &name,
        &key,
        None,
        Some(&Lock {
            retention: Some(Retention {
                mode: Mode::Compliance,
                retain_until: future(),
            }),
            legal_hold: LegalHold::On,
        }),
    )
    .await
    .unwrap();

    let got = backend
        .op_get_object(s3_request(dto::GetObjectInput {
            bucket: b.clone(),
            key: "legacy".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        got.output.object_lock_mode.as_ref().map(|m| m.as_str()),
        Some(dto::ObjectLockMode::COMPLIANCE),
        "the null version's lock is echoed"
    );
    let head = backend
        .op_head_object(s3_request(dto::HeadObjectInput {
            bucket: b,
            key: "legacy".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        head.output
            .object_lock_legal_hold_status
            .as_ref()
            .map(|h| h.as_str()),
        Some(dto::ObjectLockLegalHoldStatus::ON)
    );
}

#[tokio::test]
async fn a_copy_takes_the_destination_lock_not_the_sources() {
    let (backend, b) = lock_backend().await;
    backend
        .op_put_object(put(
            &b,
            "src",
            Some(Mode::Compliance),
            None,
            Some(VALID_MD5),
        ))
        .await
        .unwrap();
    let source = format!("{b}/src");
    backend
        .op_copy_object(s3_request(
            dto::CopyObjectInput::builder()
                .bucket(b.clone())
                .key("dst".to_owned())
                .copy_source(dto::CopySource::parse(source.as_str()).unwrap())
                .build()
                .expect("a copy input"),
        ))
        .await
        .unwrap();
    // The destination has no lock: the copy did not inherit the source's.
    let err = backend
        .op_get_object_retention(get_retention(&b, "dst", None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "NoSuchObjectLockConfiguration", "{err:?}");
    // And the source's lock is untouched by the copy.
    assert!(
        backend
            .op_get_object_retention(get_retention(&b, "src", None))
            .await
            .unwrap()
            .output
            .retention
            .is_some()
    );
}

// --- the write-path gates ---

#[tokio::test]
async fn a_lock_header_on_an_unconfigured_bucket_is_invalid_request() {
    let (backend, b) = setup().await;
    let err = backend
        .op_put_object(put(&b, "k", Some(Mode::Governance), None, Some(VALID_MD5)))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidRequest", "{err:?}");
}

#[tokio::test]
async fn a_lock_configuration_put_requires_enabled_versioning() {
    let (backend, b) = setup().await;
    let err = backend
        .op_put_object_lock_configuration(put_config(&b, None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidBucketState", "{err:?}");
}

#[tokio::test]
async fn suspending_a_lock_enabled_bucket_is_invalid_bucket_state() {
    let (backend, b) = lock_backend().await;
    let err = backend
        .op_put_bucket_versioning(s3_request(dto::PutBucketVersioningInput {
            bucket: b.clone(),
            versioning_configuration: dto::VersioningConfiguration {
                status: Some(dto::BucketVersioningStatus::from_static(
                    dto::BucketVersioningStatus::SUSPENDED,
                )),
                mfa_delete: None,
            },
            checksum_algorithm: None,
            content_md5: None,
            expected_bucket_owner: None,
            mfa: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidBucketState", "{err:?}");
    assert_eq!(
        backend
            .storage()
            .bucket_versioning(&bucket::name(&b).unwrap())
            .await
            .unwrap(),
        version::State::Enabled
    );
}

// --- delete protection ---

#[tokio::test]
async fn the_error_classes_follow_the_design() {
    let (backend, b) = setup().await;
    // Missing bucket configuration.
    let err = backend
        .op_get_object_lock_configuration(get_config(&b))
        .await
        .unwrap_err();
    assert_eq!(
        code(&err),
        "ObjectLockConfigurationNotFoundError",
        "{err:?}"
    );
    // A per-object operation on an unconfigured bucket is 400.
    backend
        .op_put_object(put(&b, "k", None, None, None))
        .await
        .unwrap();
    let err = backend
        .op_get_object_retention(get_retention(&b, "k", None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidRequest", "{err:?}");
    let err = backend
        .op_put_object_retention(put_retention(&b, "k", Mode::Governance, None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidRequest", "{err:?}");
    let err = backend
        .op_get_object_legal_hold(get_hold(&b, "k", None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidRequest", "{err:?}");
}

#[tokio::test]
async fn a_locked_bucket_without_a_per_object_lock_answers_no_such_object_lock_configuration() {
    let (backend, b) = lock_backend().await;
    backend
        .op_put_object(put(&b, "k", None, None, None))
        .await
        .unwrap();
    let err = backend
        .op_get_object_retention(get_retention(&b, "k", None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "NoSuchObjectLockConfiguration", "{err:?}");
    let err = backend
        .op_get_object_legal_hold(get_hold(&b, "k", None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "NoSuchObjectLockConfiguration", "{err:?}");
}

#[tokio::test]
async fn the_capability_off_answers_not_implemented_on_the_six_operations() {
    let caps = Capabilities {
        object_lock: false,
        ..Default::default()
    };
    let (backend, b) = setup_gated_with_caps(caps).await;
    let err = backend
        .get_object_lock_configuration(get_config(&b))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "NotImplemented", "{err:?}");
    let err = backend
        .put_object_lock_configuration(put_config(&b, None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "NotImplemented", "{err:?}");
    let err = backend
        .get_object_retention(get_retention(&b, "k", None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "NotImplemented", "{err:?}");
    let err = backend
        .put_object_retention(put_retention(&b, "k", Mode::Governance, None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "NotImplemented", "{err:?}");
    let err = backend
        .get_object_legal_hold(get_hold(&b, "k", None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "NotImplemented", "{err:?}");
    let err = backend
        .put_object_legal_hold(put_hold(&b, "k", LegalHold::On, None))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "NotImplemented", "{err:?}");
}

#[tokio::test]
async fn the_capability_off_refuses_the_six_while_enforcement_stays() {
    // OL20's second half: the toggle owns the six surfaces, never the stored
    // facts — the default retention still applies, a protected delete still
    // 403s, and GET/HEAD skip the lock headers.
    let caps = Capabilities {
        object_lock: false,
        ..Default::default()
    };
    let (backend, _) = setup_gated_with_caps(caps).await;
    backend
        .create_bucket(s3_request(dto::CreateBucketInput {
            bucket: "kept".into(),
            object_lock_enabled_for_bucket: Some(true),
            ..Default::default()
        }))
        .await
        .unwrap();
    // The configuration is stored through the handler directly: the
    // decorator owns the capability gate, the store path is enforcement.
    backend
        .inner()
        .op_put_object_lock_configuration(put_config("kept", Some(rule())))
        .await
        .unwrap();

    // The six surfaces refuse.
    for (op, err) in [
        (
            "GetObjectLockConfiguration",
            backend
                .get_object_lock_configuration(get_config("kept"))
                .await
                .unwrap_err(),
        ),
        (
            "PutObjectLockConfiguration",
            backend
                .put_object_lock_configuration(put_config("kept", None))
                .await
                .unwrap_err(),
        ),
        (
            "GetObjectRetention",
            backend
                .get_object_retention(get_retention("kept", "k", None))
                .await
                .unwrap_err(),
        ),
        (
            "PutObjectRetention",
            backend
                .put_object_retention(put_retention("kept", "k", Mode::Governance, None))
                .await
                .unwrap_err(),
        ),
        (
            "GetObjectLegalHold",
            backend
                .get_object_legal_hold(get_hold("kept", "k", None))
                .await
                .unwrap_err(),
        ),
        (
            "PutObjectLegalHold",
            backend
                .put_object_legal_hold(put_hold("kept", "k", LegalHold::On, None))
                .await
                .unwrap_err(),
        ),
    ] {
        assert_eq!(code(&err), "NotImplemented", "{op}: {err:?}");
    }

    // Enforcement: a plain PUT (an ungated surface) still receives the
    // bucket's default compliance retention. The read goes through the
    // contract — the six handlers are the surfaces the toggle refuses.
    let written = backend
        .put_object(put("kept", "k", None, None, Some(VALID_MD5)))
        .await
        .unwrap();
    let version = written
        .headers
        .get("x-amz-version-id")
        .and_then(|value| value.to_str().ok())
        .expect("the write answers its version")
        .to_owned();
    let name = bucket::name("kept").unwrap();
    let key = object::key("k").unwrap();
    let id = version::id(&version).unwrap();
    assert!(
        backend
            .inner()
            .storage()
            .get_object_retention(&name, &key, Some(&id))
            .await
            .unwrap()
            .is_some(),
        "the default retention still applied"
    );

    // GET/HEAD skip the three lock headers (the echo's own capability gate).
    let got = backend
        .get_object(s3_request(dto::GetObjectInput {
            bucket: "kept".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(got.output.object_lock_mode.is_none());
    assert!(got.output.object_lock_retain_until_date.is_none());
    assert!(got.output.object_lock_legal_hold_status.is_none());
    let head = backend
        .head_object(s3_request(dto::HeadObjectInput {
            bucket: "kept".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(head.output.object_lock_mode.is_none());
    assert!(head.output.object_lock_legal_hold_status.is_none());

    // Delete protection still refuses the protected version.
    let err = backend
        .delete_object(delete_version("kept", "k", &version))
        .await
        .unwrap_err();
    assert_eq!(code(&err), "AccessDenied", "{err:?}");
}

#[tokio::test]
async fn read_only_serves_the_three_reads_and_refuses_the_three_writes() {
    let (backend, b) = lock_backend().await;
    backend
        .op_put_object(put(&b, "k", Some(Mode::Governance), None, Some(VALID_MD5)))
        .await
        .unwrap();
    let read_only = S3ReadonlyBackend::new(backend);
    read_only
        .get_object_lock_configuration(get_config(&b))
        .await
        .expect("the configuration read is served");
    read_only
        .get_object_retention(get_retention(&b, "k", None))
        .await
        .expect("the retention read is served");
    read_only
        .get_object_legal_hold(get_hold(&b, "k", None))
        .await
        .expect("the legal-hold read is served (OFF is a lock fact here)");
    for err in [
        read_only
            .put_object_lock_configuration(put_config(&b, None))
            .await
            .unwrap_err(),
        read_only
            .put_object_retention(put_retention(&b, "k", Mode::Governance, None))
            .await
            .unwrap_err(),
        read_only
            .put_object_legal_hold(put_hold(&b, "k", LegalHold::On, None))
            .await
            .unwrap_err(),
    ] {
        assert_eq!(code(&err), "AccessDenied", "{err:?}");
    }
}

#[tokio::test]
async fn the_cache_answers_first_and_is_written_by_the_three_paths() {
    let (backend, b) = setup().await;
    let name = bucket::name(&b).unwrap();
    // The first probe reads the store and caches `false`.
    assert!(!backend.has_lock_config(&name).await.unwrap());
    // A configuration written behind the cache's back is not seen: the
    // entry is what answers.
    backend
        .storage()
        .set_bucket_versioning(&name, version::State::Enabled)
        .await
        .unwrap();
    backend
        .storage()
        .put_bucket_object_lock(
            &name,
            &Config {
                enabled: true,
                rule: None,
            },
        )
        .await
        .unwrap();
    assert!(
        !backend.has_lock_config(&name).await.unwrap(),
        "the cached false answers without touching the lock table"
    );
    // The configuration put (the handler) flips it.
    backend
        .op_put_object_lock_configuration(put_config(&b, None))
        .await
        .unwrap();
    assert!(backend.has_lock_config(&name).await.unwrap());

    // Bucket create records the truth (`false` for an ordinary create).
    backend
        .op_create_bucket(s3_request(dto::CreateBucketInput {
            bucket: "fresh".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    let fresh = bucket::name("fresh").unwrap();
    assert!(!backend.has_lock_config(&fresh).await.unwrap());

    // Bucket delete writes the fact it leaves behind: `false`.
    backend
        .op_delete_bucket(s3_request(dto::DeleteBucketInput {
            bucket: "fresh".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        backend.object_lock_cache().cached(&fresh),
        Some(false),
        "the deleted bucket's entry is the fact the delete produced"
    );
}

#[tokio::test]
async fn a_late_cache_merge_never_overwrites_a_newer_answer() {
    // The OL18 race, without a generation counter: writers insert the fact
    // they produced and a late reader only fills a missing entry with
    // `or_insert`. A writer's fact that lands while a reader's store read is
    // in flight therefore survives that read, and a delete's `false` is not
    // undone by a reader that read `true` before it.
    let (backend, b) = setup_with_caps(Default::default()).await;
    let name = bucket::name(&b).unwrap();
    let entry = || backend.object_lock_cache().cached(&name);

    // A writer's `true` wins over a reader that read `false`.
    backend.object_lock_cache().insert(&name, true);
    assert!(
        backend.object_lock_cache().merge(&name, false),
        "the reader answers the newer true"
    );
    assert!(backend.has_lock_config(&name).await.unwrap());
    assert_eq!(entry(), Some(true), "the writer's true survives");

    // A delete's `false` wins over a reader that read `true` before it.
    backend.object_lock_cache().insert(&name, false);
    assert!(
        !backend.object_lock_cache().merge(&name, true),
        "the reader answers the delete's false"
    );
    assert_eq!(entry(), Some(false), "the delete's false survives");

    // With no entry, the reader's own read is inserted and answered.
    let fresh = bucket::name("merge-fresh").unwrap();
    let cache = backend.object_lock_cache();
    assert!(
        !cache.merge(&fresh, false),
        "the read is inserted and answered"
    );
    assert_eq!(cache.cached(&fresh), Some(false));
}

#[tokio::test]
async fn the_capability_off_skips_the_cache_entirely() {
    let caps = Capabilities {
        object_lock: false,
        ..Default::default()
    };
    let (backend, b) = setup_with_caps(caps).await;
    assert!(
        !backend
            .has_lock_config(&bucket::name(&b).unwrap())
            .await
            .unwrap()
    );
    assert!(
        backend.object_lock_cache().is_empty(),
        "the capability off neither reads the store nor caches"
    );
}

/// **The management plane's cache hook, without a request**: the admin shares the backend's
/// handle ([`S3Backend::object_lock_cache`]) and inserts the fact after a write through the
/// store. The stale `false` a GET/HEAD cached must stop answering, or the plane's new
/// configuration would be invisible to the echo until the process restarted.

#[tokio::test]
async fn an_external_fact_write_stops_the_stale_answer() {
    let (backend, b) = setup_with_caps(Default::default()).await;
    let name = bucket::name(&b).unwrap();
    backend
        .storage
        .set_bucket_versioning(&name, version::State::Enabled)
        .await
        .unwrap();

    // The first read caches `false`: the bucket stores no configuration.
    assert!(!backend.has_lock_config(&name).await.unwrap());

    // The plane's write goes through the storage handle, not this backend.
    backend
        .storage
        .put_bucket_object_lock(
            &name,
            &Config {
                enabled: true,
                rule: None,
            },
        )
        .await
        .unwrap();

    // Without the hook the cached `false` would answer the echo indefinitely.
    assert!(
        !backend.has_lock_config(&name).await.unwrap(),
        "the cache is the writer's to update"
    );

    // The shared handle is that update: the admin inserts the fact it wrote.
    backend.object_lock_cache().insert(&name, true);
    assert!(
        backend.has_lock_config(&name).await.unwrap(),
        "the next read answers the plane's configured fact"
    );
}

/// Minor 4: a lock read on a missing bucket answers `404 NoSuchBucket` — the
/// row-backed stores' answer and AWS's. The fs plane is the regression: its
/// lazy engine can report "no configuration" for a bucket it has never seen,
/// so the handler probes the bucket before the configuration.
///
/// This pins the end-to-end answer. It does **not** by itself prove the
/// handler's `head_bucket` probe: `FsStorage::get_bucket_object_lock` also
/// fences on the bucket directory, so the test would pass without the probe.
/// The discriminating fixture would be a stub `Storage` whose config read is
/// lazily `Ok(None)`; `Storage` requires 39 methods across four `async_trait`
/// traits with no forwarding blanket impl, so that fixture is not practical
/// here (the cleanup report records the choice).

#[tokio::test]
async fn lock_reads_on_a_missing_bucket_answer_no_such_bucket() {
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).expect("an fs storage root");
    let backend = S3Backend::new(storage, Default::default());

    let err = backend
        .op_get_object_retention(get_retention("missing", "k", None))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "NoSuchBucket", "{err:?}");

    let err = backend
        .op_get_object_legal_hold(get_hold("missing", "k", None))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "NoSuchBucket", "{err:?}");
}

/// Minor 5 (R28): a corrupt stored lock row fails closed on the GET path too
/// — `500 InternalError`, never an omitted header that would read as
/// "unlocked".

#[tokio::test]
async fn a_corrupt_lock_row_fails_the_retention_and_hold_reads_closed() {
    let (backend, b) = lock_backend().await;
    let written = backend
        .op_put_object(s3_request(dto::PutObjectInput {
            bucket: b.clone(),
            key: "corrupt.txt".into(),
            body: Some(dto::StreamingBlob::wrap(stream::once(async {
                Ok::<_, io::Error>(Bytes::from_static(b"payload"))
            }))),
            ..Default::default()
        }))
        .await
        .expect("an unlocked write on a lock-enabled bucket");
    let version = version_of(&written);

    // The raw writer is the test's own: no contract write produces a corrupt
    // mode.
    let data = backend.storage().metadata();
    let (bucket_wire, key_wire, version_wire) =
        (b.clone(), "corrupt.txt".to_owned(), version.clone());
    data.driver()
        .write(move |txn| -> Result<(), tinio_store_redb::Error> {
            let mut table = tinio_store_redb::version_object_lock::Table::open(txn)?;
            table
                .insert(
                    (
                        bucket_wire.as_str(),
                        key_wire.as_str(),
                        version_wire.as_str(),
                    ),
                    (Some("NOT-A-MODE"), Some(1), Some("OFF")),
                )
                .map_err(tinio_store_redb::Error::from)?;
            Ok(())
        })
        .await
        .expect("the corrupt row is planted");

    let err = backend
        .op_get_object_retention(get_retention(&b, "corrupt.txt", Some(&version)))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "InternalError", "{err:?}");

    let err = backend
        .op_get_object_legal_hold(get_hold(&b, "corrupt.txt", Some(&version)))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "InternalError", "{err:?}");
}
