//! The pg Object Lock behaviour the store's own suite cannot reach through the backend:
//! the versioning path's R21 guard (a lock-enabled bucket is never suspended).

#![cfg(feature = "versioning")]
#![expect(
    clippy::absolute_paths,
    reason = "test support: a test proves the public paths"
)]

mod support;

use std::time::{Duration, SystemTime};

use support::{require_dsn, scratch_config};
use tinio_core::{
    CompletedPart,
    acl::Acl,
    meta::{BucketMeta, ObjectMeta, UploadMeta, VersionMeta},
    multipart, object, object_lock,
    storage::{self, BucketOps, MultipartOps, ObjectOps, VersionOps},
    version::State,
};
use tinio_pg::{
    _store_pg::{_sqlx, pool::Pool},
    Access, PgStorage,
};
use tinio_util::testing::{body, bucket_name};

/// The number of stored per-version lock rows for one key: the raw read the
/// contract does not expose by key, used to pin the compensation's cleanup.
#[expect(
    clippy::expect_used,
    reason = "test helper: a failed count query must fail the leg, and clippy's allow-expect-in-tests does not reach a free function in an integration-test crate"
)]
async fn version_lock_rows(raw: &Pool, bucket: &str, key: &str) -> i64 {
    _sqlx::query_scalar("SELECT count(*) FROM version_object_lock WHERE bucket = $1 AND key = $2")
        .bind(bucket)
        .bind(key)
        .fetch_one(&**raw)
        .await
        .expect("the lock count")
}

/// R21: the backend's versioning write routes through the store's guarded
/// statement. A suspend over an enabled Object Lock configuration is
/// `InvalidBucketState` (the S3 409 is Task 6's), and the state is unchanged;
/// the unguarded engine path would have accepted it and reopened the OL13
/// write-skew window.
#[tokio::test]
async fn suspending_a_lock_enabled_bucket_is_invalid_bucket_state() {
    if !require_dsn() {
        return;
    }
    let db = tinio_pg::_store_pg::test_support::TestDb::create()
        .await
        .expect("a scratch database");
    let storage = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("a writable store over the scratch database");

    let bucket = bucket_name("suspend-lock-bucket");
    let acl = Acl::default_private(None);
    storage
        .create_bucket(&bucket, None, &acl)
        .await
        .expect("the bucket");
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .expect("versioning enabled");
    BucketMeta::set_object_lock(
        storage.store(),
        &bucket,
        &object_lock::Config {
            enabled: true,
            rule: None,
        },
    )
    .await
    .expect("the configuration");

    let err = storage
        .set_bucket_versioning(&bucket, State::Suspended)
        .await
        .expect_err("a lock-enabled bucket is never suspended");
    assert!(
        matches!(
            storage::Error::from(err),
            storage::Error::InvalidBucketState(_)
        ),
        "the suspend is InvalidBucketState"
    );
    assert_eq!(
        storage
            .bucket_versioning(&bucket)
            .await
            .expect("the state read"),
        State::Enabled,
        "the refused suspend left the state unchanged"
    );

    // The negative case is an **absent** configuration, not only a disabled
    // one: the guard keys on an enabled row, and a bucket that never stored a
    // configuration suspends.
    let plain = bucket_name("suspend-absent-bucket");
    storage
        .create_bucket(&plain, None, &acl)
        .await
        .expect("the plain bucket");
    storage
        .set_bucket_versioning(&plain, State::Enabled)
        .await
        .expect("versioning enabled");
    assert_eq!(
        BucketMeta::object_lock(storage.store(), &plain)
            .await
            .expect("the configuration read"),
        None,
        "the plain bucket stores no configuration at all"
    );
    storage
        .set_bucket_versioning(&plain, State::Suspended)
        .await
        .expect("an absent configuration does not refuse the suspend");
    assert_eq!(
        storage
            .bucket_versioning(&plain)
            .await
            .expect("the state read"),
        State::Suspended
    );

    db.drop_db().await;
}

/// OL6/OL21: aborting an upload retires its create-time lock intent with the
/// upload row — the abort's drain is what consumes it.
#[tokio::test]
async fn aborting_an_upload_clears_the_create_time_lock_intent() {
    if !require_dsn() {
        return;
    }
    let db = tinio_pg::_store_pg::test_support::TestDb::create()
        .await
        .expect("a scratch database");
    let storage = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("a writable store over the scratch database");

    let bucket = bucket_name("abort-intent-bucket");
    storage
        .create_bucket_with_lock(
            &bucket,
            None,
            &Acl::default_private(None),
            &object_lock::Config {
                enabled: true,
                rule: Some(object_lock::Rule {
                    default_retention: object_lock::DefaultRetention {
                        mode: object_lock::Mode::Governance,
                        period: object_lock::Period::Days(1),
                    },
                }),
            },
        )
        .await
        .expect("the lock-enabled create");
    let key = object::key("aborted.bin").unwrap();
    let intent = storage
        .resolve_write_lock(
            &bucket,
            None,
            object_lock::LegalHold::Off,
            SystemTime::now(),
        )
        .await
        .expect("the resolve")
        .expect("the default retention is a lock");
    let upload = storage
        .create_multipart_upload(
            &bucket,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            Some(&intent),
        )
        .await
        .expect("the create");
    assert_eq!(
        UploadMeta::upload_lock(storage.store(), &bucket, &upload.upload_id)
            .await
            .expect("the intent read"),
        Some(intent),
        "the intent is stored with the upload row"
    );

    storage
        .abort_multipart_upload(&bucket, &key, &upload.upload_id)
        .await
        .expect("the abort");
    assert_eq!(
        UploadMeta::upload_lock(storage.store(), &bucket, &upload.upload_id)
            .await
            .expect("the intent read"),
        None,
        "the abort drains the intent with the upload"
    );

    db.drop_db().await;
}

/// OL10/OL16: the effective lock is resolved once from the stored
/// configuration, and the same fact drives the checksum requirement and the
/// write.
#[tokio::test]
async fn the_write_path_resolves_the_default_and_the_override() {
    if !require_dsn() {
        return;
    }
    let db = tinio_pg::_store_pg::test_support::TestDb::create()
        .await
        .expect("a scratch database");
    let storage = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("a writable store over the scratch database");

    let bucket = bucket_name("resolve-lock-bucket");
    let config = object_lock::Config {
        enabled: true,
        rule: Some(object_lock::Rule {
            default_retention: object_lock::DefaultRetention {
                mode: object_lock::Mode::Governance,
                period: object_lock::Period::Days(7),
            },
        }),
    };
    storage
        .create_bucket_with_lock(&bucket, None, &Acl::default_private(None), &config)
        .await
        .expect("the lock-enabled create");
    assert_eq!(
        storage
            .bucket_versioning(&bucket)
            .await
            .expect("the state read"),
        State::Enabled
    );
    assert_eq!(
        BucketMeta::object_lock(storage.store(), &bucket)
            .await
            .expect("the configuration read"),
        Some(config)
    );

    let now = SystemTime::now();
    let default = storage
        .resolve_write_lock(&bucket, None, object_lock::LegalHold::Off, now)
        .await
        .expect("the resolve")
        .expect("the default retention is a lock");
    assert_eq!(
        default.retention.expect("the default").retain_until,
        now + Duration::from_secs(7 * 86_400)
    );

    let explicit = object_lock::Retention {
        mode: object_lock::Mode::Compliance,
        retain_until: now + Duration::from_secs(60),
    };
    let resolved = storage
        .resolve_write_lock(&bucket, Some(&explicit), object_lock::LegalHold::Off, now)
        .await
        .expect("the resolve")
        .expect("the explicit retention is a lock");
    assert_eq!(resolved.retention, Some(explicit));

    let held = storage
        .resolve_write_lock(&bucket, None, object_lock::LegalHold::On, now)
        .await
        .expect("the resolve")
        .expect("the hold alone is a lock");
    assert_eq!(held.legal_hold, object_lock::LegalHold::On);
    assert!(
        held.retention.is_some(),
        "a legal-hold-only write still receives the default retention"
    );

    let plain = bucket_name("resolve-plain-bucket");
    storage
        .create_bucket(&plain, None, &Acl::default_private(None))
        .await
        .expect("the plain bucket");
    assert!(
        storage
            .resolve_write_lock(&plain, None, object_lock::LegalHold::Off, now)
            .await
            .expect("the resolve")
            .is_none(),
        "a bucket with no configuration resolves no lock"
    );

    db.drop_db().await;
}

/// OL7/OL12: the lock rides the versioned commit, and a copy takes the
/// destination's lock and never the source's.
#[tokio::test]
async fn the_lock_rides_the_commit_and_a_copy_takes_the_destination_lock() {
    if !require_dsn() {
        return;
    }
    let db = tinio_pg::_store_pg::test_support::TestDb::create()
        .await
        .expect("a scratch database");
    let storage = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("a writable store over the scratch database");

    let bucket = bucket_name("write-lock-bucket");
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .expect("the bucket");
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .expect("versioning enabled");

    let src = object::key("src.bin").unwrap();
    let dst = object::key("dst.bin").unwrap();
    let lock = object_lock::Lock {
        retention: Some(object_lock::Retention {
            mode: object_lock::Mode::Governance,
            retain_until: SystemTime::now() + Duration::from_secs(3_600),
        }),
        legal_hold: object_lock::LegalHold::Off,
    };
    let staged = storage
        .stage_body(&bucket, &src, body(b"source"), None, None)
        .await
        .expect("the staged body");
    let source = storage
        .commit_object(
            &bucket,
            &src,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            Some(&lock),
        )
        .await
        .expect("the locked write");
    let source_id = source.version_id.expect("an identity");
    assert_eq!(
        storage
            .get_object_retention(&bucket, &src, Some(&source_id))
            .await
            .expect("the retention read"),
        lock.retention,
        "the lock commits with the current version"
    );

    let copied = storage
        .copy_object(
            &bucket,
            &src,
            &bucket,
            &dst,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
            None,
        )
        .await
        .expect("the copy");
    assert!(
        storage
            .get_object_retention(&bucket, &dst, copied.version_id.as_ref())
            .await
            .expect("the destination read")
            .is_none(),
        "a copy never inherits the source's lock"
    );

    let destination = object_lock::Lock {
        retention: Some(object_lock::Retention {
            mode: object_lock::Mode::Compliance,
            retain_until: SystemTime::now() + Duration::from_secs(7_200),
        }),
        legal_hold: object_lock::LegalHold::On,
    };
    let locked = storage
        .copy_object(
            &bucket,
            &src,
            &bucket,
            &dst,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
            Some(&destination),
        )
        .await
        .expect("the locked copy");
    assert_eq!(
        VersionMeta::version_lock(storage.store(), &bucket, &dst, locked.version_id.as_ref())
            .await
            .expect("the lock read"),
        Some(destination),
        "the destination's own lock applies"
    );

    db.drop_db().await;
}

/// OL13: the atomic create is all-or-none — a taken name rolls back and
/// leaves the original facts untouched.
#[tokio::test]
async fn create_bucket_with_lock_is_all_or_none() {
    if !require_dsn() {
        return;
    }
    let db = tinio_pg::_store_pg::test_support::TestDb::create()
        .await
        .expect("a scratch database");
    let storage = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("a writable store over the scratch database");

    let bucket = bucket_name("create-lock-bucket");
    let config = object_lock::Config {
        enabled: true,
        rule: Some(object_lock::Rule {
            default_retention: object_lock::DefaultRetention {
                mode: object_lock::Mode::Compliance,
                period: object_lock::Period::Days(1),
            },
        }),
    };
    storage
        .create_bucket_with_lock(&bucket, None, &Acl::default_private(None), &config)
        .await
        .expect("the lock-enabled create");
    assert_eq!(
        BucketMeta::object_lock(storage.store(), &bucket)
            .await
            .expect("the configuration read"),
        Some(config.clone())
    );

    let err = storage
        .create_bucket_with_lock(
            &bucket,
            None,
            &Acl::default_private(None),
            &object_lock::Config {
                enabled: true,
                rule: None,
            },
        )
        .await
        .expect_err("the name is taken");
    assert!(
        matches!(storage::Error::from(err), storage::Error::AlreadyExists(_)),
        "the taken name refuses"
    );
    assert_eq!(
        BucketMeta::object_lock(storage.store(), &bucket)
            .await
            .expect("the configuration read"),
        Some(config),
        "the refused create leaves the existing configuration untouched"
    );

    db.drop_db().await;
}

/// OL8/OL11: the delete forwards the request's bypass to the store's atomic
/// removal. Governance needs it; COMPLIANCE refuses it.
#[tokio::test]
async fn the_delete_forwards_the_governance_bypass() {
    if !require_dsn() {
        return;
    }
    let db = tinio_pg::_store_pg::test_support::TestDb::create()
        .await
        .expect("a scratch database");
    let storage = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("a writable store over the scratch database");

    let bucket = bucket_name("delete-bypass-bucket");
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .expect("the bucket");
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .expect("versioning enabled");
    BucketMeta::set_object_lock(
        storage.store(),
        &bucket,
        &object_lock::Config {
            enabled: true,
            rule: None,
        },
    )
    .await
    .expect("the configuration");

    let key = object::key("locked.bin").unwrap();
    let now = SystemTime::now();
    let governance = object_lock::Lock {
        retention: Some(object_lock::Retention {
            mode: object_lock::Mode::Governance,
            retain_until: now + Duration::from_secs(3_600),
        }),
        legal_hold: object_lock::LegalHold::Off,
    };
    let committed = storage
        .commit_object(
            &bucket,
            &key,
            storage
                .stage_body(&bucket, &key, body(b"governed"), None, None)
                .await
                .expect("the staged body"),
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            Some(&governance),
        )
        .await
        .expect("the locked write");
    let id = committed.version_id.expect("an identity");

    let refused = storage
        .delete_object_version(&bucket, &key, Some(&id), object_lock::Bypass::None)
        .await
        .expect_err("the governance lock refuses without the bypass");
    assert!(
        matches!(
            storage::Error::from(refused),
            storage::Error::Locked(object_lock::RefusalReason::Governance)
        ),
        "the refused delete removes nothing"
    );
    assert!(
        storage.get_object(&bucket, &key, None).await.is_ok(),
        "the object survives the refusal"
    );

    storage
        .delete_object_version(&bucket, &key, Some(&id), object_lock::Bypass::Governance)
        .await
        .expect("the bypass deletes the version");
    assert_eq!(
        VersionMeta::version_lock(storage.store(), &bucket, &key, Some(&id))
            .await
            .expect("the lock read"),
        None,
        "the lock row leaves with the version"
    );

    // COMPLIANCE refuses even with the bypass.
    let compliance = object_lock::Lock {
        retention: Some(object_lock::Retention {
            mode: object_lock::Mode::Compliance,
            retain_until: now + Duration::from_secs(3_600),
        }),
        legal_hold: object_lock::LegalHold::Off,
    };
    let committed = storage
        .commit_object(
            &bucket,
            &key,
            storage
                .stage_body(&bucket, &key, body(b"compliance"), None, None)
                .await
                .expect("the staged body"),
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            Some(&compliance),
        )
        .await
        .expect("the locked write");
    let id = committed.version_id.expect("an identity");
    let refused = storage
        .delete_object_version(&bucket, &key, Some(&id), object_lock::Bypass::Governance)
        .await
        .expect_err("compliance ignores the bypass");
    assert!(
        matches!(
            storage::Error::from(refused),
            storage::Error::Locked(object_lock::RefusalReason::Compliance)
        ),
        "compliance refuses in either mode"
    );

    db.drop_db().await;
}

/// OL6: the create-time intent is stored with the upload row, a mid-upload
/// configuration change cannot alter it, and the completion applies it to the
/// final version.
#[tokio::test]
async fn the_create_time_lock_intent_rides_the_completion() {
    if !require_dsn() {
        return;
    }
    let db = tinio_pg::_store_pg::test_support::TestDb::create()
        .await
        .expect("a scratch database");
    let storage = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("a writable store over the scratch database");

    let bucket = bucket_name("multipart-lock-bucket");
    storage
        .create_bucket_with_lock(
            &bucket,
            None,
            &Acl::default_private(None),
            &object_lock::Config {
                enabled: true,
                rule: Some(object_lock::Rule {
                    default_retention: object_lock::DefaultRetention {
                        mode: object_lock::Mode::Governance,
                        period: object_lock::Period::Days(7),
                    },
                }),
            },
        )
        .await
        .expect("the lock-enabled create");

    let key = object::key("assembled.bin").unwrap();
    let intent = storage
        .resolve_write_lock(
            &bucket,
            None,
            object_lock::LegalHold::Off,
            SystemTime::now(),
        )
        .await
        .expect("the resolve")
        .expect("the default retention is a lock");
    let upload = storage
        .create_multipart_upload(
            &bucket,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            Some(&intent),
        )
        .await
        .expect("the create");
    assert_eq!(
        UploadMeta::upload_lock(storage.store(), &bucket, &upload.upload_id)
            .await
            .expect("the intent read"),
        Some(intent.clone()),
        "the intent is stored with the upload row"
    );

    // A mid-upload configuration change must not alter the fixed intent.
    BucketMeta::set_object_lock(
        storage.store(),
        &bucket,
        &object_lock::Config {
            enabled: true,
            rule: Some(object_lock::Rule {
                default_retention: object_lock::DefaultRetention {
                    mode: object_lock::Mode::Compliance,
                    period: object_lock::Period::Days(30),
                },
            }),
        },
    )
    .await
    .expect("the changed configuration");

    let part = storage
        .upload_part(
            &bucket,
            &key,
            &upload.upload_id,
            multipart::part_number(1).expect("a valid part number"),
            body(b"assembled"),
            None,
        )
        .await
        .expect("the part");
    let info = storage
        .complete_multipart_upload(
            &bucket,
            &key,
            &upload.upload_id,
            &[CompletedPart {
                part_number: part.part_number,
                etag: part.etag.clone(),
            }],
            None,
        )
        .await
        .expect("the completion");

    assert_eq!(
        VersionMeta::version_lock(storage.store(), &bucket, &key, info.version_id.as_ref())
            .await
            .expect("the lock read"),
        Some(intent),
        "the completed version carries the create-time intent, not the mid-upload config"
    );
    assert_eq!(
        UploadMeta::upload_lock(storage.store(), &bucket, &upload.upload_id)
            .await
            .expect("the intent read"),
        None,
        "the completion consumes the intent"
    );

    db.drop_db().await;
}

/// **The completion's compensation arm, forced deterministically.** A trigger
/// on `uploads` makes the final drain fail — the one step that runs after the
/// object row and the slot have landed. The completion must take the publish,
/// the row and the slot back, and it must remove the version lock row the
/// advance stored beside the slot; the upload must survive so the same client
/// can retry the same completion.
///
/// The fault is a **test-only database object** (a `BEFORE DELETE` trigger in
/// the scratch database), not a production seam: the completion code carries no
/// hook, and the trigger is dropped as soon as the failure has been observed.
#[tokio::test]
async fn a_failed_drain_compensates_the_completion_and_the_upload_retries() {
    if !require_dsn() {
        return;
    }
    let db = tinio_pg::_store_pg::test_support::TestDb::create()
        .await
        .expect("a scratch database");
    let storage = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("a writable store over the scratch database");
    // A second, raw pool for the fault injection and the row count the
    // contract does not expose by key.
    let raw = Pool::open(&db.dsn, 2).expect("the dsn parses");

    let bucket = bucket_name("multipart-compensation");
    storage
        .create_bucket_with_lock(
            &bucket,
            None,
            &Acl::default_private(None),
            &object_lock::Config {
                enabled: true,
                rule: Some(object_lock::Rule {
                    default_retention: object_lock::DefaultRetention {
                        mode: object_lock::Mode::Governance,
                        period: object_lock::Period::Days(7),
                    },
                }),
            },
        )
        .await
        .expect("the lock-enabled bucket");
    let intent = storage
        .resolve_write_lock(
            &bucket,
            None,
            object_lock::LegalHold::Off,
            SystemTime::now(),
        )
        .await
        .expect("the resolve")
        .expect("the bucket's default retention is a lock");

    let key = object::key("compensated.bin").unwrap();
    let upload = storage
        .create_multipart_upload(
            &bucket,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            Some(&intent),
        )
        .await
        .expect("the create");
    let part = storage
        .upload_part(
            &bucket,
            &key,
            &upload.upload_id,
            multipart::part_number(1).expect("a valid part number"),
            body(b"compensated"),
            None,
        )
        .await
        .expect("the part");

    // The injected fault: every `uploads` delete raises. The drain is one
    // statement, and it is the step that runs after the row and slot landed.
    _sqlx::query(
        "CREATE FUNCTION tinio_fail_upload_drain() RETURNS trigger \
         LANGUAGE plpgsql AS $$ BEGIN \
             RAISE EXCEPTION 'injected: the upload drain fails'; \
         END $$",
    )
    .execute(&*raw)
    .await
    .expect("the fault function");
    _sqlx::query(
        "CREATE TRIGGER tinio_fail_upload_drain BEFORE DELETE ON uploads \
         FOR EACH ROW EXECUTE FUNCTION tinio_fail_upload_drain()",
    )
    .execute(&*raw)
    .await
    .expect("the fault trigger");

    let _failure = storage
        .complete_multipart_upload(
            &bucket,
            &key,
            &upload.upload_id,
            &[CompletedPart {
                part_number: part.part_number,
                etag: part.etag.clone(),
            }],
            None,
        )
        .await
        .expect_err("the drain fails on the injected trigger");

    // The compensation's documented outcome: the key is the object it was
    // (nothing for a fresh key), the upload and its create-time intent are
    // still there for a retry, and the lock row the advance stored was removed
    // with the slot it protected.
    assert_eq!(
        ObjectMeta::get(storage.store(), &bucket, &key)
            .await
            .expect("the object read"),
        None,
        "the row the failed completion wrote was taken back"
    );
    assert!(
        UploadMeta::get(storage.store(), &bucket, &key, &upload.upload_id)
            .await
            .expect("the upload read")
            .is_some(),
        "the upload survives, so the client can retry the same upload"
    );
    assert_eq!(
        UploadMeta::upload_lock(storage.store(), &bucket, &upload.upload_id)
            .await
            .expect("the intent read"),
        Some(intent.clone()),
        "the failed drain rolled back, so the create-time intent rides"
    );
    assert_eq!(
        version_lock_rows(&raw, bucket.as_ref(), key.as_ref()).await,
        0,
        "the compensation removed the version lock row the advance stored"
    );

    // Drop the fault and retry the same completion: the upload is consistent,
    // and the completed version carries the create-time intent.
    _sqlx::query("DROP TRIGGER tinio_fail_upload_drain ON uploads")
        .execute(&*raw)
        .await
        .expect("the trigger drops");
    _sqlx::query("DROP FUNCTION tinio_fail_upload_drain()")
        .execute(&*raw)
        .await
        .expect("the function drops");
    let info = storage
        .complete_multipart_upload(
            &bucket,
            &key,
            &upload.upload_id,
            &[CompletedPart {
                part_number: part.part_number,
                etag: part.etag.clone(),
            }],
            None,
        )
        .await
        .expect("the retry completes the same upload");
    assert_eq!(
        VersionMeta::version_lock(storage.store(), &bucket, &key, info.version_id.as_ref())
            .await
            .expect("the lock read"),
        Some(intent),
        "the retry's completed version carries the create-time intent"
    );
    assert_eq!(
        version_lock_rows(&raw, bucket.as_ref(), key.as_ref()).await,
        1,
        "and the compensation's removal did not touch the retry's row"
    );
    assert_eq!(
        UploadMeta::upload_lock(storage.store(), &bucket, &upload.upload_id)
            .await
            .expect("the intent read"),
        None,
        "the successful drain consumed the intent"
    );

    raw.close().await;
    db.drop_db().await;
}
