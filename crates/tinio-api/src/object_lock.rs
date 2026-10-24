//! The management plane's Object Lock admin (OL3, OL23): the trait the two
//! `/buckets/{bucket}/object-lock` routes are served from, so this crate never opens a store.
//!
//! The edge is the lifecycle plane's (LC14): `tinio-server` owns the `Storage`, and this crate
//! must not depend on the crate that owns one, so the admin is **injected** — the CLI implements
//! [`ObjectLockAdmin`] over its storage handle and attaches it with
//! [`Context::with_object_lock`](crate::router::Context::with_object_lock).
//!
//! **Two routes, one stored fact each** (OL3):
//!
//! - `GET/PUT /buckets/{bucket}/object-lock` — the bucket's stored configuration.
//! - `GET/PUT /buckets/{bucket}/object-lock/objects` — the addressed version's lock. The key and
//!   the version address travel in the **JSON body on both methods**, never as a path segment.
//!
//! **The version address is explicit** (OL29): [`VersionAddress`] spells the three states
//! `current`, `null` and an id, so an absent address and the null version can never collapse into
//! one. The bodies are the stored model's own fields in `snake_case` (OL30); there is no S3
//! element spelling and no fourth wire.
//!
//! **`enabled:false` is refused** (OL23): the bucket setter only enables a bucket or replaces its
//! default-retention rule; a configuration is never disabled. The admin's `put_bucket_config`
//! performs the CLI's OL24 convenience — it enables versioning first when the bucket is
//! unversioned — and refuses an update on a suspended bucket.
//!
//! **The object route's update is set / clear / leave per member.** A `retention` member that is
//! absent leaves the retention alone, `null` clears it and an object sets it; a `legal_hold`
//! member that is absent leaves the hold alone and a value sets it. Clearing one member never
//! clears the other.
//!
//! **The present members are applied atomically** (OL23): the admin performs the retention and
//! the legal hold in one store transaction, so a member that refuses leaves **neither** applied.
//!
//! **No admin, no routes** (OL23): a build without the `object-lock` feature registers no path,
//! and a run whose `[s3] object_lock` is off injects no admin — the handlers answer the same
//! contract's `404`. A read-only run serves both `GET`s and answers `403` for both `PUT`s: the
//! plane's own access mode is the admission, checked before the body.

use std::time::SystemTime;

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Path, State as AxumState, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    _core::{bucket, object, object_lock as model, storage, version},
    router::{
        Shared,
        admin::{admit, failure as admin_failure, refusal},
    },
};

/// The `404` body a bucket route answers when the bucket stores no Object Lock configuration.
pub const NO_CONFIGURATION: &str = "no object lock configuration";
/// The `404` body the object route answers when the bucket is not Object Lock enabled.
pub const NOT_ENABLED: &str = "object lock is not enabled";
/// The `404` body the object route answers when the addressed version carries neither fact.
pub const NO_RETENTION: &str = "no retention";

/// The plane's only door to a bucket's stored Object Lock configuration and a version's lock
/// (OL23). The methods are the two routes and nothing else crosses: the plane never names a
/// backend, a table or a transaction, so the CLI is free to implement this over whatever handle
/// its server owns.
///
/// The models are `tinio_core::object_lock`'s, handed over typed, so no JSON is parsed on this
/// side. The S3 contract's error classes keep their meaning: `Locked` is a refused change,
/// `InvalidBucketState` a bucket whose versioning or configuration refuses the write,
/// `NoSuchBucket`/`NoSuchVersion` an absent subject, and a read-only store's `AccessDenied` (or
/// its read-only `Io`) the `403`.
///
/// # Examples
///
/// ```rust
/// use std::{
///     sync::Mutex,
///     time::{Duration, SystemTime},
/// };
///
/// use async_trait::async_trait;
/// use tinio_api::{
///     _core::{
///         bucket, object,
///         object_lock::{self, Bypass, Config, LegalHold, Lock, Retention},
///         storage, version,
///     },
///     object_lock::ObjectLockAdmin,
/// };
///
/// /// The CLI's implementation in miniature: one configuration and one lock fact.
/// #[derive(Default)]
/// struct Admin(Mutex<Option<Config>>, Mutex<Option<Lock>>);
///
/// #[async_trait]
/// impl ObjectLockAdmin for Admin {
///     async fn bucket_config(
///         &self,
///         _bucket: &bucket::Name,
///     ) -> Result<Option<Config>, storage::Error> {
///         Ok(self.0.lock().expect("the fixture is not poisoned").clone())
///     }
///
///     async fn put_bucket_config(
///         &self,
///         _bucket: &bucket::Name,
///         config: &Config,
///     ) -> Result<(), storage::Error> {
///         *self.0.lock().expect("the fixture is not poisoned") = Some(config.clone());
///         Ok(())
///     }
///
///     async fn object_lock(
///         &self,
///         _bucket: &bucket::Name,
///         _key: &object::Key,
///         _address: &version::Address,
///     ) -> Result<Option<Lock>, storage::Error> {
///         Ok(self.1.lock().expect("the fixture is not poisoned").clone())
///     }
///
///     async fn put_retention(
///         &self,
///         _bucket: &bucket::Name,
///         _key: &object::Key,
///         _address: &version::Address,
///         _retention: Option<&Retention>,
///         _bypass: Bypass,
///     ) -> Result<(), storage::Error> {
///         Ok(())
///     }
///
///     async fn put_legal_hold(
///         &self,
///         _bucket: &bucket::Name,
///         _key: &object::Key,
///         _address: &version::Address,
///         _hold: LegalHold,
///     ) -> Result<(), storage::Error> {
///         Ok(())
///     }
///
///     async fn update_lock(
///         &self,
///         _bucket: &bucket::Name,
///         _key: &object::Key,
///         _address: &version::Address,
///         _retention: Option<Option<&Retention>>,
///         _legal_hold: Option<LegalHold>,
///         _bypass: Bypass,
///     ) -> Result<(), storage::Error> {
///         Ok(())
///     }
/// }
///
/// # #[tokio::main]
/// # async fn main() {
/// let admin = Admin::default();
/// let name = bucket::name("logs").expect("a bucket name");
/// assert!(admin.bucket_config(&name).await.expect("a read").is_none());
/// admin
///     .put_bucket_config(
///         &name,
///         &Config {
///             enabled: true,
///             rule: None,
///         },
///     )
///     .await
///     .expect("the enable path");
/// assert_eq!(
///     admin.bucket_config(&name).await.expect("a read"),
///     Some(Config {
///         enabled: true,
///         rule: None
///     })
/// );
/// # }
/// ```
#[async_trait]
pub trait ObjectLockAdmin: Send + Sync {
    /// The bucket's stored configuration, or `None` when the bucket stores none. `NoSuchBucket`
    /// when the bucket is missing, which the plane answers `404`.
    async fn bucket_config(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Option<model::Config>, storage::Error>;

    /// Enable the bucket or replace its default-retention rule — replace-all, never a merge.
    ///
    /// The plane refuses `enabled:false` and validates the rule **before** it calls this, so an
    /// implementation may treat `config` as one the model accepted (OL23). The implementation
    /// enables versioning first when the bucket is unversioned (the CLI's OL24 convenience) and
    /// refuses an update on a bucket whose versioning is `Suspended`. A store that refuses a
    /// write — a run whose mode changed after admission, or a read-only backend — answers its
    /// `AccessDenied` (or read-only `Io`), which the plane maps to `403`.
    async fn put_bucket_config(
        &self,
        bucket: &bucket::Name,
        config: &model::Config,
    ) -> Result<(), storage::Error>;

    /// The addressed version's lock facts, or `None` when it carries neither a retention nor a
    /// legal hold (`LegalHold::Off`). The plane answers that absence `404`.
    ///
    /// `NoSuchBucket` when the bucket is missing, `NoSuchVersion` (or `NoSuchKey`) when the key or
    /// version does not exist; the plane maps both to `404` with their own body.
    async fn object_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &version::Address,
    ) -> Result<Option<model::Lock>, storage::Error>;

    /// Set (`Some`) or clear (`None`) the addressed version's retention. **Not calling this is
    /// "leave it alone"** — the plane makes that distinction before it reaches the trait.
    ///
    /// `bypass` is the plane token's governance authority (OL15, OL24): the CLI's clear carries
    /// it implicitly, and `retention set --bypass` asks for it explicitly. A compliance retention
    /// and a legal hold refuse regardless (OL11).
    async fn put_retention(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &version::Address,
        retention: Option<&model::Retention>,
        bypass: model::Bypass,
    ) -> Result<(), storage::Error>;

    /// Set (`LegalHold::On`) or clear (`LegalHold::Off`) the addressed version's legal hold. The
    /// retention is preserved; a legal hold has no bypass.
    async fn put_legal_hold(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &version::Address,
        hold: model::LegalHold,
    ) -> Result<(), storage::Error>;

    /// Apply the retention member and the legal-hold member in **one** store transaction (OL23).
    ///
    /// `retention` is three-state — `None` leaves the retention alone, `Some(None)` clears it and
    /// `Some(Some(retention))` sets it — and `legal_hold` is two-state (`None` leaves the hold
    /// alone, a value sets it). A refused retention (a COMPLIANCE change, or GOVERNANCE without
    /// [`model::Bypass::Governance`]) applies **neither** member and answers `Locked`; the legal
    /// hold itself never refuses and has no bypass.
    ///
    /// # Errors
    ///
    /// The same classes as [`ObjectLockAdmin::put_retention`].
    async fn update_lock(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &version::Address,
        retention: Option<Option<&model::Retention>>,
        legal_hold: Option<model::LegalHold>,
        bypass: model::Bypass,
    ) -> Result<(), storage::Error>;
}

/// The version address as the plane's JSON spells it (OL29): `"current"`, `"null"`, or a version
/// id. Never an `Option` where "absent" and "the null version" would collapse.
///
/// # Examples
///
/// ```rust
/// use tinio_api::object_lock::VersionAddress;
///
/// assert_eq!(
///     serde_json::to_string(&VersionAddress::Current).expect("json"),
///     r#""current""#
/// );
/// assert_eq!(
///     serde_json::to_string(&VersionAddress::Null).expect("json"),
///     r#""null""#
/// );
/// let id = VersionAddress::Id("abc".to_string());
/// assert_eq!(serde_json::to_string(&id).expect("json"), r#""abc""#);
/// assert_eq!(
///     serde_json::from_str::<VersionAddress>(r#""null""#).expect("json"),
///     VersionAddress::Null
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionAddress {
    /// The current version — resolved to the current slot's identity, which may be the null one.
    Current,
    /// The null version.
    Null,
    /// A specific version id.
    Id(String),
}

impl VersionAddress {
    /// The S3 contract's one address model (OL29).
    fn to_address(&self) -> Result<version::Address, storage::Error> {
        match self {
            Self::Current => Ok(version::Address::Current),
            Self::Null => Ok(version::Address::Null),
            Self::Id(raw) => version::id(raw.clone()).map(version::Address::Id),
        }
    }
}

impl Serialize for VersionAddress {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(match self {
            Self::Current => "current",
            Self::Null => "null",
            Self::Id(id) => id,
        })
    }
}

impl<'de> Deserialize<'de> for VersionAddress {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ok(match raw.as_str() {
            "current" => Self::Current,
            "null" => Self::Null,
            _ => Self::Id(raw),
        })
    }
}

/// The object route's `GET` request body: the key and the version address, never a path segment
/// (OL3, OL29).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectAddress {
    /// The object's key.
    pub key: String,
    /// `current`, `null`, or a version id.
    pub version: VersionAddress,
}

/// The object route's `PUT` request body: the address plus the per-member update.
///
/// A `retention` member that is **absent** leaves the retention alone, `null` clears it and an
/// object sets it; a `legal_hold` member that is absent leaves the hold alone and a value sets
/// it. The two members serialize the same way they deserialize: "leave" omits the member and
/// `Some(None)` is an explicit `null`. `bypass` is the governance authority for a retention
/// change (OL15, OL24).
///
/// **The present members are applied atomically** (OL23): one store transaction applies the
/// retention and the legal hold, so a refused retention writes neither member. An absent
/// retention cannot be refused, and a legal hold itself never refuses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateObjectLock {
    /// The object's key.
    pub key: String,
    /// `current`, `null`, or a version id.
    pub version: VersionAddress,
    /// Absent = leave; `null` = clear; an object = set.
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub retention: Option<Option<model::Retention>>,
    /// Absent = leave; a value = set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legal_hold: Option<model::LegalHold>,
    /// Ask for the governance bypass on a retention change (`false` when absent).
    #[serde(default, skip_serializing_if = "is_false")]
    pub bypass: bool,
}

/// Whether a `bool` is the default `false`, for `skip_serializing_if`: "leave" omits `bypass`
/// exactly as an absent member does.
fn is_false(value: &bool) -> bool {
    !*value
}

/// Distinguish an absent member from an explicit `null`: `#[serde(default)]` supplies the outer
/// `None` for a missing member, and this deserializer wraps every present value — `null`
/// included — in `Some`.
fn present<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

/// Registers the four Object Lock routes. The caller keeps them above the token layer, with
/// every other route (OL23).
pub(crate) fn mount(routes: Router<Shared>) -> Router<Shared> {
    routes
        .route(
            "/buckets/{bucket}/object-lock",
            get(bucket_get).put(bucket_put),
        )
        .route(
            "/buckets/{bucket}/object-lock/objects",
            get(object_get).put(object_put),
        )
}

/// `GET /buckets/{bucket}/object-lock`: the stored configuration in the model's own JSON, or
/// `404 {"error": "no object lock configuration"}` when the bucket stores none (OL23).
async fn bucket_get(AxumState(shared): AxumState<Shared>, Path(wire): Path<String>) -> Response {
    let (admin, bucket) = match admit(shared.object_lock(), wire, "object lock is not served here")
    {
        Ok(found) => found,
        Err(refused) => return refused.response(),
    };
    match admin.bucket_config(&bucket).await {
        Ok(Some(config)) => Json(config).into_response(),
        Ok(None) => refusal(StatusCode::NOT_FOUND, NO_CONFIGURATION),
        Err(err) => failure(err),
    }
}

/// `PUT /buckets/{bucket}/object-lock`: enable the bucket or replace its default-retention rule
/// (replace-all; `204`). `enabled:false` is refused as `400` — the setter only enables or
/// updates, never disables. The admin taken first means a plane carrying none answers `404`
/// whatever the body looks like; the plane's access mode is checked before the body is parsed, so
/// a read-only run answers `403` even for a body it would otherwise refuse.
async fn bucket_put(
    AxumState(shared): AxumState<Shared>,
    Path(wire): Path<String>,
    body: Result<Json<model::Config>, JsonRejection>,
) -> Response {
    let (admin, bucket) = match admit(shared.object_lock(), wire, "object lock is not served here")
    {
        Ok(found) => found,
        Err(refused) => return refused.response(),
    };
    if shared.access.is_read_only() {
        return refusal(StatusCode::FORBIDDEN, "access denied");
    }
    let Json(config) = match body {
        Ok(body) => body,
        Err(rejection) => {
            tracing::debug!(error = %rejection, "the object-lock body is not the model");
            return refusal(
                rejection.status(),
                "the body is not an object lock configuration",
            );
        }
    };
    if !config.enabled {
        return refusal(
            StatusCode::BAD_REQUEST,
            "the setter only enables or updates; it does not disable",
        );
    }
    if let Err(invalid) = config.validate() {
        return refusal(StatusCode::BAD_REQUEST, invalid.reason);
    }
    match admin.put_bucket_config(&bucket, &config).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => failure(err),
    }
}

/// `GET /buckets/{bucket}/object-lock/objects`: the addressed version's lock state. The key and
/// version address ride in the JSON body (OL3, OL29).
///
/// Two absences are distinguished in the body (OL23): a bucket with no configuration is
/// `404 {"error": "object lock is not enabled"}`, and a configured bucket whose addressed
/// version carries neither fact is `404 {"error": "no retention"}`.
async fn object_get(
    AxumState(shared): AxumState<Shared>,
    Path(wire): Path<String>,
    body: Result<Json<ObjectAddress>, JsonRejection>,
) -> Response {
    let (admin, bucket) = match admit(shared.object_lock(), wire, "object lock is not served here")
    {
        Ok(found) => found,
        Err(refused) => return refused.response(),
    };
    let Json(body) = match body {
        Ok(body) => body,
        Err(rejection) => {
            tracing::debug!(error = %rejection, "the object-lock body is not an address");
            return refusal(rejection.status(), "the body is not an object lock request");
        }
    };
    let key = match key_of(&body.key) {
        Ok(key) => key,
        Err(message) => return refusal(StatusCode::BAD_REQUEST, &message),
    };
    let address = match body.version.to_address() {
        Ok(address) => address,
        Err(err) => return refusal(StatusCode::BAD_REQUEST, &err.to_string()),
    };
    match admin.bucket_config(&bucket).await {
        Ok(None) => return refusal(StatusCode::NOT_FOUND, NOT_ENABLED),
        Ok(Some(_)) => {}
        Err(err) => return failure(err),
    }
    match admin.object_lock(&bucket, &key, &address).await {
        Ok(Some(lock)) => Json(lock).into_response(),
        Ok(None) => refusal(StatusCode::NOT_FOUND, NO_RETENTION),
        Err(err) => failure(err),
    }
}

/// `PUT /buckets/{bucket}/object-lock/objects`: set or clear the addressed version's retention
/// and/or legal hold (`204`). The two members are independent: an absent one is left alone, so
/// clearing the retention never clears the hold and setting the hold never touches the
/// retention (OL23).
///
/// **The present members are applied atomically**: the admin performs both in one store
/// transaction, and a refused retention answers that member's status with nothing written. **The
/// plane's access mode is checked before the body is parsed**, so a read-only run answers `403`
/// even for a body carrying neither member, which a writable run answers `204` as a no-op.
async fn object_put(
    AxumState(shared): AxumState<Shared>,
    Path(wire): Path<String>,
    body: Result<Json<UpdateObjectLock>, JsonRejection>,
) -> Response {
    let (admin, bucket) = match admit(shared.object_lock(), wire, "object lock is not served here")
    {
        Ok(found) => found,
        Err(refused) => return refused.response(),
    };
    if shared.access.is_read_only() {
        return refusal(StatusCode::FORBIDDEN, "access denied");
    }
    let Json(body) = match body {
        Ok(body) => body,
        Err(rejection) => {
            tracing::debug!(error = %rejection, "the object-lock body is not an update");
            return refusal(rejection.status(), "the body is not an object lock update");
        }
    };
    let key = match key_of(&body.key) {
        Ok(key) => key,
        Err(message) => return refusal(StatusCode::BAD_REQUEST, &message),
    };
    let address = match body.version.to_address() {
        Ok(address) => address,
        Err(err) => return refusal(StatusCode::BAD_REQUEST, &err.to_string()),
    };
    // The retention member is validated before the admin is called; `None` (absent) is "leave"
    // and never reaches the store as a refusal.
    let retention = match body.retention {
        Some(Some(retention)) => {
            if let Err(invalid) = retention.validate(SystemTime::now()) {
                return refusal(StatusCode::BAD_REQUEST, invalid.reason);
            }
            Some(Some(retention))
        }
        Some(None) => Some(None),
        None => None,
    };
    // Neither member is present: the route stays a no-op, as it always has.
    if retention.is_none() && body.legal_hold.is_none() {
        return StatusCode::NO_CONTENT.into_response();
    }
    let bypass = if body.bypass {
        model::Bypass::Governance
    } else {
        model::Bypass::None
    };
    match admin
        .update_lock(
            &bucket,
            &key,
            &address,
            retention.as_ref().map(Option::as_ref),
            body.legal_hold,
            bypass,
        )
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => failure(err),
    }
}

/// The key from the body, or the universal key rules' refusal (the plane answers `400`).
fn key_of(raw: &str) -> Result<object::Key, String> {
    object::key(raw).map_err(|err| err.to_string())
}

/// A storage failure as this plane's status (OL23, the S3 table's meaning): the shared
/// read-only/404/500 mapping, plus this group's own refusals — a protected change is `403`, an
/// invalid bucket state is `409`, and an absent key or version is `404 "no such version"`.
fn failure(err: storage::Error) -> Response {
    admin_failure("an object lock request failed", err, |err| match err {
        storage::Error::Locked(_) => Some(refusal(StatusCode::FORBIDDEN, "access denied")),
        storage::Error::InvalidBucketState(_) => {
            Some(refusal(StatusCode::CONFLICT, "invalid bucket state"))
        }
        storage::Error::NoSuchVersion(_) | storage::Error::NoSuchKey(_) => {
            Some(refusal(StatusCode::NOT_FOUND, "no such version"))
        }
        _ => None,
    })
}

// Never called — `status_doc` carries why.

/// The bucket's stored Object Lock configuration, the model's own JSON.
#[cfg(feature = "openapi")]
#[expect(
    dead_code,
    reason = "argued: the `#[utoipa::path]` stub has no caller by design; the attribute needs an item to attach to, so nothing reaches it"
)]
#[utoipa::path(
    get,
    path = "/buckets/{bucket}/object-lock",
    tag = "management",
    params(("bucket" = String, Path, description = "The bucket's name")),
    responses(
        (status = 200, description = "The stored configuration; its members are the model's"),
        (status = 401, description = "Missing or wrong token"),
        (status = 404, description = "No such bucket, no stored configuration, or object lock is not served here")
    ),
    security(("token" = []))
)]
pub(crate) fn object_lock_bucket_get_doc() {}

// Never called — `status_doc` carries why.

/// Enable the bucket or replace its default-retention rule; `enabled:false` is refused.
#[cfg(feature = "openapi")]
#[expect(
    dead_code,
    reason = "argued: the `#[utoipa::path]` stub has no caller by design; the attribute needs an item to attach to, so nothing reaches it"
)]
#[utoipa::path(
    put,
    path = "/buckets/{bucket}/object-lock",
    tag = "management",
    params(("bucket" = String, Path, description = "The bucket's name")),
    responses(
        (status = 204, description = "Enabled or updated; the request body is the model's own JSON"),
        (status = 400, description = "JSON syntax error, `enabled:false`, or a rule the model refuses"),
        (status = 401, description = "Missing or wrong token"),
        (status = 403, description = "Refused: the run is read-only"),
        (status = 404, description = "No such bucket, or object lock is not served here"),
        (status = 409, description = "The bucket's versioning refuses the enable path"),
        (status = 413, description = "The body is past the plane's body limit"),
        (status = 415, description = "Missing `Content-Type: application/json`"),
        (status = 422, description = "The JSON is not the model: a member of the wrong type")
    ),
    security(("token" = []))
)]
pub(crate) fn object_lock_bucket_put_doc() {}

// Never called — `status_doc` carries why.

/// The addressed version's lock state; the key and version address are in the JSON body.
#[cfg(feature = "openapi")]
#[expect(
    dead_code,
    reason = "argued: the `#[utoipa::path]` stub has no caller by design; the attribute needs an item to attach to, so nothing reaches it"
)]
#[utoipa::path(
    get,
    path = "/buckets/{bucket}/object-lock/objects",
    tag = "management",
    params(("bucket" = String, Path, description = "The bucket's name")),
    responses(
        (status = 200, description = "The addressed version's retention and legal hold"),
        (status = 400, description = "The body is not the address model"),
        (status = 401, description = "Missing or wrong token"),
        (status = 404, description = "No such bucket, object lock is not enabled, no retention, or no such version"),
        (status = 413, description = "The body is past the plane's body limit"),
        (status = 415, description = "Missing `Content-Type: application/json`"),
        (status = 422, description = "The JSON is not the address model")
    ),
    security(("token" = []))
)]
pub(crate) fn object_lock_object_get_doc() {}

// Never called — `status_doc` carries why.

/// Set or clear the addressed version's retention and/or legal hold.
#[cfg(feature = "openapi")]
#[expect(
    dead_code,
    reason = "argued: the `#[utoipa::path]` stub has no caller by design; the attribute needs an item to attach to, so nothing reaches it"
)]
#[utoipa::path(
    put,
    path = "/buckets/{bucket}/object-lock/objects",
    tag = "management",
    params(("bucket" = String, Path, description = "The bucket's name")),
    responses(
        (status = 204, description = "The members that were present were applied"),
        (status = 400, description = "The body, key, version or retention the plane refuses"),
        (status = 401, description = "Missing or wrong token"),
        (status = 403, description = "Refused: the run is read-only, or the lock refuses the change"),
        (status = 404, description = "No such bucket, no such version, or object lock is not served here"),
        (status = 409, description = "The bucket's versioning or configuration refuses the write"),
        (status = 413, description = "The body is past the plane's body limit"),
        (status = 415, description = "Missing `Content-Type: application/json`"),
        (status = 422, description = "The JSON is not the update model")
    ),
    security(("token" = []))
)]
pub(crate) fn object_lock_object_put_doc() {}

#[cfg(test)]
mod tests;
