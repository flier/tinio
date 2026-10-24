//! Bucket Object Lock operations of the S3 mapping layer (design
//! 2026-10-20-s3-object-lock, OL1–OL20, OL29).
//!
//! The six operations — Get/PutObjectLockConfiguration,
//! Get/PutObjectRetention, Get/PutObjectLegalHold — over the storage
//! contract, runtime-gated on `caps.object_lock` by
//! [`CapabilityS3`](crate::capability::CapabilityS3) and compile-gated on the
//! `object-lock` cargo feature (feature-off builds omit the forwards
//! entirely; s3s's trait defaults answer "is not implemented yet"). The three
//! reads are read rows of the operation table, so read-only mode serves them;
//! the three writes are write rows, so it refuses them.
//!
//! **The writes are the request path's, the enforcement is the store's.** The
//! XML is mapped onto the core model and validated here
//! ([`Config::validate`](crate::_core::object_lock::Config::validate)); the
//! store's atomic methods assert the identity, the configuration and the
//! versioning state inside one transaction (Task 3/4), so no request can
//! place a lock the stored facts do not allow.
//!
//! **The marker rules are OL9's**, and they are the handler's: an explicitly
//! addressed delete marker answers `405 MethodNotAllowed`; the unaddressed
//! address reads the current object, so a marker there (or an absent key) is
//! the plain read's `404 NoSuchKey`; a non-marker version without a lock
//! answers `404 NoSuchObjectLockConfiguration`.
//!
//! **The write path shares one resolution** ([`S3Backend::write_lock`],
//! OL7/OL16): the effective lock is resolved once from the bucket
//! configuration and the request's three headers, the checksum requirement is
//! decided from that same value, and the same `Option<Lock>` is passed into
//! the write seam. A lock header on a bucket with no configuration is the
//! `400 InvalidRequest` OL17 pins.
//!
//! **The wire maps are the model's.** [`Mode`] and [`LegalHold`] parse and
//! render themselves once (GL/Task 2), so the headers, the XML bodies and the
//! echoes cannot spell a mode or a hold differently.

use std::time::SystemTime;

use s3s::{S3Error, S3Request, S3Response, S3Result, dto, s3_error};

use crate::{
    _core::{
        bucket, object,
        object_lock::{Config, DefaultRetention, LegalHold, Mode, Period, Retention, Rule},
        storage::Storage,
        version,
    },
    backend::{S3Backend, map_backend_error},
};

impl<S: Storage> S3Backend<S> {
    pub(crate) async fn op_get_object_lock_configuration(
        &self,
        req: S3Request<dto::GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<dto::GetObjectLockConfigurationOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        match self.object_lock_config(&bucket).await? {
            Some(config) => Ok(S3Response::new(dto::GetObjectLockConfigurationOutput {
                object_lock_configuration: Some(config_to_dto(&config)),
            })),
            // OL17: a bucket that stores no configuration is the 404 AWS
            // names `ObjectLockConfigurationNotFoundError`.
            None => Err(s3_error!(ObjectLockConfigurationNotFoundError)),
        }
    }

    pub(crate) async fn op_put_object_lock_configuration(
        &self,
        req: S3Request<dto::PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<dto::PutObjectLockConfigurationOutput>> {
        let dto::PutObjectLockConfigurationInput {
            bucket,
            object_lock_configuration,
            content_md5,
            ..
        } = req.input;
        let bucket = self.bucket(bucket)?;
        // The body's digest is presence-only (the XML deserializer consumed
        // the body; the mapped field is the claim).
        super::checksum::require_content_md5(content_md5.as_deref())?;
        let Some(xml) = object_lock_configuration else {
            return Err(malformed(
                "the request carries no Object Lock configuration",
            ));
        };
        let config = config_from_dto(xml)?;
        // OL13: the first configuration requires versioning `Enabled`; the
        // S3 surface never enables it implicitly (the CLI's convenience is
        // not the API's).
        let state = self
            .storage
            .bucket_versioning(&bucket)
            .await
            .map_err(map_backend_error)?;
        if state != version::State::Enabled {
            return Err(s3_error!(
                InvalidBucketState,
                "Object Lock requires versioning `Enabled` on the bucket"
            ));
        }
        self.storage
            .put_bucket_object_lock(&bucket, &config)
            .await
            .map_err(map_backend_error)?;
        // OL18: a `false` cache entry must not outlive the enablement.
        self.object_lock_cache().insert(&bucket, true);
        Ok(S3Response::new(
            dto::PutObjectLockConfigurationOutput::default(),
        ))
    }

    pub(crate) async fn op_get_object_retention(
        &self,
        req: S3Request<dto::GetObjectRetentionInput>,
    ) -> S3Result<S3Response<dto::GetObjectRetentionOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        Self::record_operation_identity(&bucket, Some(&key));
        let address = self.version_address(req.input.version_id.as_deref())?;
        self.require_lock_config(&bucket).await?;
        let identity = self.lock_target(&bucket, &key, &address).await?;
        let retention = self
            .storage
            .get_object_retention(&bucket, &key, identity.as_ref())
            .await
            .map_err(map_backend_error)?;
        match retention {
            Some(retention) => Ok(S3Response::new(dto::GetObjectRetentionOutput {
                retention: Some(retention_to_dto(&retention)),
            })),
            // OL9: a non-marker version with no lock is this 404.
            None => Err(s3_error!(NoSuchObjectLockConfiguration)),
        }
    }

    pub(crate) async fn op_put_object_retention(
        &self,
        req: S3Request<dto::PutObjectRetentionInput>,
    ) -> S3Result<S3Response<dto::PutObjectRetentionOutput>> {
        let request_may_bypass = super::extension_may_bypass(&req.extensions);
        let dto::PutObjectRetentionInput {
            bucket,
            key,
            version_id,
            retention,
            content_md5,
            bypass_governance_retention,
            ..
        } = req.input;
        let bucket = self.bucket(bucket)?;
        let key = self.key(key)?;
        Self::record_operation_identity(&bucket, Some(&key));
        let address = self.version_address(version_id.as_deref())?;
        super::checksum::require_content_md5(content_md5.as_deref())?;
        self.require_lock_config(&bucket).await?;
        // R29: an absent `Retention` element clears the version's retention
        // (AWS's clear shape). A document that carries the element with only
        // one half is still a malformed document, not a clear.
        let now = SystemTime::now();
        let requested = retention
            .map(|xml| retention_from_dto(xml, now))
            .transpose()?;
        // OL15: the authority is the addressed version's owner/FullControl
        // (or the bucket owner), resolved by the ACL pipeline and carried in
        // the request extensions. The header alone has none (fail closed).
        let bypass = super::bypass_of(
            bypass_governance_retention.unwrap_or(false),
            request_may_bypass,
        );
        self.lock_target(&bucket, &key, &address).await?;
        self.storage
            .set_retention(&bucket, &key, &address, requested.as_ref(), bypass, now)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::PutObjectRetentionOutput::default()))
    }

    pub(crate) async fn op_get_object_legal_hold(
        &self,
        req: S3Request<dto::GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<dto::GetObjectLegalHoldOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        Self::record_operation_identity(&bucket, Some(&key));
        let address = self.version_address(req.input.version_id.as_deref())?;
        self.require_lock_config(&bucket).await?;
        let identity = self.lock_target(&bucket, &key, &address).await?;
        let lock = self
            .storage
            .object_lock(&bucket, &key, identity.as_ref())
            .await
            .map_err(map_backend_error)?;
        // OL9: a version whose lock row carries neither fact has no
        // per-object lock configuration.
        let Some(lock) = lock else {
            return Err(s3_error!(NoSuchObjectLockConfiguration));
        };
        Ok(S3Response::new(dto::GetObjectLegalHoldOutput {
            legal_hold: Some(dto::ObjectLockLegalHold {
                status: Some(hold_status_to_dto(lock.legal_hold)),
            }),
        }))
    }

    pub(crate) async fn op_put_object_legal_hold(
        &self,
        req: S3Request<dto::PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<dto::PutObjectLegalHoldOutput>> {
        let dto::PutObjectLegalHoldInput {
            bucket,
            key,
            version_id,
            legal_hold,
            content_md5,
            ..
        } = req.input;
        let bucket = self.bucket(bucket)?;
        let key = self.key(key)?;
        Self::record_operation_identity(&bucket, Some(&key));
        let address = self.version_address(version_id.as_deref())?;
        super::checksum::require_content_md5(content_md5.as_deref())?;
        self.require_lock_config(&bucket).await?;
        let Some(legal_hold) = legal_hold else {
            return Err(malformed("the request carries no LegalHold"));
        };
        let hold = hold_status_from_dto(legal_hold.status.as_ref())?;
        self.lock_target(&bucket, &key, &address).await?;
        self.storage
            .set_legal_hold(&bucket, &key, &address, hold)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::PutObjectLegalHoldOutput::default()))
    }
}

// --- the read paths' locks ---

impl<S: Storage> S3Backend<S> {
    /// The GET/HEAD lock-header echo (OL18): keyed by
    /// `(bucket_records_versions, wire_of(version_id))`, so the null version —
    /// whose wire is the literal `null` and whose identity is `None` — is not
    /// skipped. The `has_lock_config` cache answers first, skipping the
    /// version-lock read entirely for a bucket with no configuration; the
    /// capability off and a bucket that records no versions skip both. A
    /// corrupt stored row is the store's `Corrupt` → 500, never an omitted
    /// header that would read as "unlocked".
    pub(crate) async fn echo_lock_headers<O: LockEchoFields>(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version_id: Option<&version::Id>,
        records_versions: bool,
        out: &mut O,
    ) -> S3Result<()> {
        if !self.caps.object_lock || !records_versions || !self.has_lock_config(bucket).await? {
            return Ok(());
        }
        let lock = self
            .storage
            .object_lock(bucket, key, version_id)
            .await
            .map_err(map_backend_error)?;
        // `None` is the contract's "neither fact": no retention, hold off.
        let (retention, hold) = match lock {
            Some(lock) => (lock.retention, lock.legal_hold),
            None => (None, LegalHold::Off),
        };
        out.set_lock(retention.as_ref(), hold);
        Ok(())
    }
}

impl<S: Storage> S3Backend<S> {
    /// The retention/legal-hold handlers' configuration gate: a bucket with
    /// no stored configuration answers `400 InvalidRequest` (OL17). The
    /// capability is on by the time this runs (the six rows are gated).
    ///
    /// This reads the store through [`S3Backend::object_lock_config`], never
    /// the `has_lock_config` cache: that cache is the GET/HEAD echo's alone
    /// (OL18), and a stale `false` left by its eviction/generation policy
    /// would turn a configured bucket's lock operation into this `400`.
    ///
    /// The bucket presence probe runs first, as a store read: a lazy row
    /// plane can answer "no configuration" for a bucket it has never seen, so
    /// without this probe a missing bucket would answer
    /// `InvalidRequest`/`ObjectLockConfigurationNotFoundError` where AWS (and
    /// the row-backed stores) answer `404 NoSuchBucket`.
    async fn require_lock_config(&self, bucket: &bucket::Name) -> S3Result<()> {
        self.storage
            .head_bucket(bucket)
            .await
            .map_err(map_backend_error)?;
        if self.object_lock_config(bucket).await?.is_some() {
            Ok(())
        } else {
            Err(s3_error!(
                InvalidRequest,
                "Bucket is missing Object Lock Configuration"
            ))
        }
    }

    /// Resolve the version a retention/legal-hold request addresses into the
    /// store's identity spelling, enforcing OL9's marker rules: an explicitly
    /// addressed marker is `405`; the unaddressed address reads the current
    /// object (a marker there — or an absent key — is the plain read's
    /// `404 NoSuchKey`).
    async fn lock_target(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &version::Address,
    ) -> S3Result<Option<version::Id>> {
        if address.is_addressed() {
            let info = super::versioning::object_of(
                self.storage
                    .head_object_version(bucket, key, address.id())
                    .await
                    .map_err(map_backend_error)?,
            )?;
            Ok(info.version_id)
        } else {
            let info = self
                .storage
                .head_object(bucket, key)
                .await
                .map_err(map_backend_error)?;
            Ok(info.version_id)
        }
    }
}

// --- the wire and the stored model ---

/// The retention/legal-hold echo fields both read DTOs carry — one setter so
/// GET and HEAD cannot describe a lock differently.
pub(crate) trait LockEchoFields {
    fn set_lock(&mut self, retention: Option<&Retention>, hold: LegalHold);
}

macro_rules! impl_lock_echo_fields {
    ($ty:ty) => {
        impl LockEchoFields for $ty {
            fn set_lock(&mut self, retention: Option<&Retention>, hold: LegalHold) {
                if retention.is_none() && hold == LegalHold::Off {
                    // No lock fact on the version: no lock header (OL18's
                    // point read is what proves it).
                    return;
                }
                if let Some(retention) = retention {
                    self.object_lock_mode = Some(mode_to_dto(retention.mode));
                    self.object_lock_retain_until_date =
                        Some(dto::ObjectLockRetainUntilDate::from(retention.retain_until));
                }
                self.object_lock_legal_hold_status = Some(hold_status_to_dto(hold));
            }
        }
    };
}

impl_lock_echo_fields!(dto::GetObjectOutput);
impl_lock_echo_fields!(dto::HeadObjectOutput);

/// The wire configuration of a stored one (OL1): always `Enabled`, with the
/// default retention when the bucket has a rule.
fn config_to_dto(config: &Config) -> dto::ObjectLockConfiguration {
    dto::ObjectLockConfiguration {
        object_lock_enabled: Some(dto::ObjectLockEnabled::from_static(
            dto::ObjectLockEnabled::ENABLED,
        )),
        rule: config.rule.as_ref().map(|rule| dto::ObjectLockRule {
            default_retention: Some(default_retention_to_dto(&rule.default_retention)),
        }),
    }
}

fn default_retention_to_dto(retention: &DefaultRetention) -> dto::DefaultRetention {
    let (days, years) = match retention.period {
        Period::Days(days) => (i32::try_from(days).ok(), None),
        Period::Years(years) => (None, i32::try_from(years).ok()),
    };
    dto::DefaultRetention {
        days,
        mode: Some(retention_mode_to_dto(retention.mode)),
        years,
    }
}

/// The request's configuration as the stored model (OL13: only `Enabled` is
/// accepted; the rule carries one period and one mode).
fn config_from_dto(xml: dto::ObjectLockConfiguration) -> S3Result<Config> {
    match xml.object_lock_enabled.as_ref() {
        Some(enabled) if enabled.as_str() == dto::ObjectLockEnabled::ENABLED => {}
        // The wire enum is a string wrapper; an absent or different value is
        // the request's schema failure.
        _ => {
            return Err(malformed("ObjectLockEnabled must be present and `Enabled`"));
        }
    }
    let rule = xml
        .rule
        .and_then(|rule| rule.default_retention)
        .map(default_retention_from_dto)
        .transpose()?
        .map(|default_retention| Rule { default_retention });
    let config = Config {
        enabled: true,
        rule,
    };
    config
        .validate()
        .map_err(|invalid| super::errors::invalid_error(invalid.kind, invalid.reason, None))?;
    Ok(config)
}

fn default_retention_from_dto(xml: dto::DefaultRetention) -> S3Result<DefaultRetention> {
    let Some(mode) = xml.mode.as_ref() else {
        return Err(malformed("DefaultRetention carries a Mode"));
    };
    let mode = mode_from_str(mode.as_str())?;
    let period = match (xml.days, xml.years) {
        (Some(days), None) => Period::Days(
            u32::try_from(days)
                .map_err(|_| malformed("DefaultRetention's Days is a non-negative count"))?,
        ),
        (None, Some(years)) => Period::Years(
            u32::try_from(years)
                .map_err(|_| malformed("DefaultRetention's Years is a non-negative count"))?,
        ),
        (Some(_), Some(_)) => {
            return Err(malformed(
                "DefaultRetention carries Days or Years, not both",
            ));
        }
        (None, None) => {
            return Err(malformed("DefaultRetention carries Days or Years"));
        }
    };
    Ok(DefaultRetention { mode, period })
}

/// The stored retention back to the wire.
fn retention_to_dto(retention: &Retention) -> dto::ObjectLockRetention {
    dto::ObjectLockRetention {
        mode: Some(retention_mode_to_dto(retention.mode)),
        retain_until_date: Some(dto::Date::from(retention.retain_until)),
    }
}

/// A `PutObjectRetention` body as the model (OL17/R29: an absent document is
/// the clear, handled by the caller; a present element missing one half of
/// its pair is `MalformedXML`; a past date is `InvalidRequest`).
fn retention_from_dto(xml: dto::ObjectLockRetention, now: SystemTime) -> S3Result<Retention> {
    let Some(mode) = xml.mode.as_ref() else {
        return Err(malformed("Retention carries a Mode"));
    };
    let Some(date) = xml.retain_until_date else {
        return Err(malformed("Retention carries a RetainUntilDate"));
    };
    let retention = Retention {
        mode: mode_from_str(mode.as_str())?,
        retain_until: time::OffsetDateTime::from(date).into(),
    };
    retention
        .validate(now)
        .map_err(|invalid| super::errors::invalid_error(invalid.kind, invalid.reason, None))?;
    Ok(retention)
}

fn mode_from_str(value: &str) -> S3Result<Mode> {
    value.parse().map_err(|_| malformed_enum("Mode", value))
}

/// The model's one spelling of a mode, shared by every wire surface.
fn mode_wire(mode: Mode) -> &'static str {
    match mode {
        Mode::Governance => dto::ObjectLockMode::GOVERNANCE,
        Mode::Compliance => dto::ObjectLockMode::COMPLIANCE,
    }
}

fn mode_to_dto(mode: Mode) -> dto::ObjectLockMode {
    dto::ObjectLockMode::from_static(mode_wire(mode))
}

fn retention_mode_to_dto(mode: Mode) -> dto::ObjectLockRetentionMode {
    dto::ObjectLockRetentionMode::from_static(mode_wire(mode))
}

fn hold_status_to_dto(hold: LegalHold) -> dto::ObjectLockLegalHoldStatus {
    dto::ObjectLockLegalHoldStatus::from_static(match hold {
        LegalHold::On => dto::ObjectLockLegalHoldStatus::ON,
        LegalHold::Off => dto::ObjectLockLegalHoldStatus::OFF,
    })
}

fn hold_status_from_dto(status: Option<&dto::ObjectLockLegalHoldStatus>) -> S3Result<LegalHold> {
    let Some(status) = status else {
        return Err(malformed("LegalHold carries a Status"));
    };
    LegalHold::parse(status.as_str())
        .map_err(|_| malformed_enum("LegalHold status", status.as_str()))
}

fn malformed(reason: &str) -> S3Error {
    s3_error!(MalformedXML, "{reason}")
}

fn malformed_enum(field: &str, value: &str) -> S3Error {
    s3_error!(MalformedXML, "invalid {field}: {value}")
}

#[cfg(test)]
mod tests;
