//! The management plane's lifecycle admin (LC14): the trait the three
//! `/buckets/{bucket}/lifecycle` routes are served from, so this crate never opens a store.
//!
//! The edge that keeps `/metrics` decoupled keeps these routes decoupled too: `tinio-server` owns
//! the `Storage`, and depending on it from here would be the reverse of its own optional
//! `tinio-api` edge, which cargo rejects as a cycle. So the admin is **injected** — the CLI
//! implements [`LifecycleAdmin`] over its storage handle and attaches it with
//! [`Context::with_lifecycle`](crate::router::Context::with_lifecycle) — and the plane carries what
//! it was built with, exactly as it carries its gauge source. That trait object is the plane's only
//! door to a stored configuration.
//!
//! **No admin, no routes** (LC14, LC15, LC23). A build without the `lifecycle` feature registers
//! none of the three paths; a run whose capability is off injects no admin, and the handlers answer
//! the contract's plain `404`. Both causes are one mechanism, and what it protects is the stored
//! rules: a plane that cannot serve a configuration must never be able to replace it.

use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Path, State as AxumState, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};

use crate::{
    _core::{bucket, lifecycle, storage},
    ErrorBody,
    router::{InvalidBody, Shared},
};

/// The plane's only door to a bucket's stored configuration (LC14).
///
/// The three methods are the three routes and nothing else crosses: the plane never names a
/// backend, a table or a transaction, so the CLI is free to implement this over whatever handle its
/// server owns. The model is `tinio_core::lifecycle::Config`, whose own `serde` shape is the
/// route's body (LC17); the trait hands it over typed, so no JSON is parsed on this side.
///
/// # Examples
///
/// ```rust
/// use std::sync::Mutex;
///
/// use async_trait::async_trait;
/// use tinio_api::{
///     _core::{bucket, lifecycle, storage},
///     lifecycle::LifecycleAdmin,
/// };
///
/// /// The CLI's implementation in miniature: the configuration it holds, and nothing else.
/// #[derive(Default)]
/// struct Admin(Mutex<Option<lifecycle::Config>>);
///
/// #[async_trait]
/// impl LifecycleAdmin for Admin {
///     async fn get(
///         &self,
///         _bucket: &bucket::Name,
///     ) -> Result<Option<lifecycle::Config>, storage::Error> {
///         Ok(self.0.lock().expect("the fixture is not poisoned").clone())
///     }
///
///     async fn put(
///         &self,
///         _bucket: &bucket::Name,
///         config: &lifecycle::Config,
///     ) -> Result<(), storage::Error> {
///         *self.0.lock().expect("the fixture is not poisoned") = Some(config.clone());
///         Ok(())
///     }
///
///     async fn delete(&self, _bucket: &bucket::Name) -> Result<(), storage::Error> {
///         *self.0.lock().expect("the fixture is not poisoned") = None;
///         Ok(())
///     }
/// }
///
/// # #[tokio::main]
/// # async fn main() {
/// let admin = Admin::default();
/// let name = bucket::name("logs").expect("a bucket name");
/// assert_eq!(admin.get(&name).await.expect("a read"), None);
/// // A configuration the model accepts: the plane validates before it calls this (LC8).
/// let config = lifecycle::Config {
///     rules: vec![lifecycle::Rule {
///         id: Some("expire-logs".to_owned()),
///         status: lifecycle::Status::Enabled,
///         filter: lifecycle::Filter::default(),
///         expiration: Some(lifecycle::Expiration {
///             days: Some(30),
///             date: None,
///             expired_object_delete_marker: None,
///         }),
///         noncurrent_version_expiration: None,
///         abort_incomplete_multipart_upload: None,
///     }],
/// };
/// config
///     .validate()
///     .expect("a configuration the model accepts");
/// admin.put(&name, &config).await.expect("a write");
/// assert_eq!(admin.get(&name).await.expect("a read"), Some(config));
/// admin.delete(&name).await.expect("a delete");
/// assert_eq!(admin.get(&name).await.expect("a read"), None);
/// # }
/// ```
#[async_trait]
pub trait LifecycleAdmin: Send + Sync {
    /// The bucket's stored configuration, or `None` when it has none. `NoSuchBucket` when the
    /// bucket is missing, which the plane answers `404`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Mutex;
    ///
    /// use async_trait::async_trait;
    /// use tinio_api::{
    ///     _core::{bucket, lifecycle, storage},
    ///     lifecycle::LifecycleAdmin,
    /// };
    ///
    /// #[derive(Default)]
    /// struct Admin(Mutex<Option<lifecycle::Config>>);
    ///
    /// #[async_trait]
    /// impl LifecycleAdmin for Admin {
    ///     async fn get(
    ///         &self,
    ///         _bucket: &bucket::Name,
    ///     ) -> Result<Option<lifecycle::Config>, storage::Error> {
    ///         Ok(self.0.lock().expect("the fixture is not poisoned").clone())
    ///     }
    ///
    ///     async fn put(
    ///         &self,
    ///         _bucket: &bucket::Name,
    ///         config: &lifecycle::Config,
    ///     ) -> Result<(), storage::Error> {
    ///         *self.0.lock().expect("the fixture is not poisoned") = Some(config.clone());
    ///         Ok(())
    ///     }
    ///
    ///     async fn delete(&self, _bucket: &bucket::Name) -> Result<(), storage::Error> {
    ///         *self.0.lock().expect("the fixture is not poisoned") = None;
    ///         Ok(())
    ///     }
    /// }
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let admin = Admin::default();
    /// let name = bucket::name("logs").expect("a bucket name");
    /// assert_eq!(admin.get(&name).await.expect("a read"), None);
    /// # }
    /// ```
    async fn get(&self, bucket: &bucket::Name)
    -> Result<Option<lifecycle::Config>, storage::Error>;

    /// Replace the bucket's configuration — replace-all, never a merge.
    ///
    /// A read-only run answers the store's `AccessDenied` (LC18), which the plane maps to `403`.
    /// **The plane validates before it calls this** (LC8): the route refuses a configuration the
    /// model rejects and stores nothing, so an implementation may treat `config` as one the model
    /// accepted.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Mutex;
    ///
    /// use async_trait::async_trait;
    /// use tinio_api::{
    ///     _core::{bucket, lifecycle, storage},
    ///     lifecycle::LifecycleAdmin,
    /// };
    ///
    /// #[derive(Default)]
    /// struct Admin(Mutex<Option<lifecycle::Config>>);
    ///
    /// #[async_trait]
    /// impl LifecycleAdmin for Admin {
    ///     async fn get(
    ///         &self,
    ///         _bucket: &bucket::Name,
    ///     ) -> Result<Option<lifecycle::Config>, storage::Error> {
    ///         Ok(self.0.lock().expect("the fixture is not poisoned").clone())
    ///     }
    ///
    ///     async fn put(
    ///         &self,
    ///         _bucket: &bucket::Name,
    ///         config: &lifecycle::Config,
    ///     ) -> Result<(), storage::Error> {
    ///         *self.0.lock().expect("the fixture is not poisoned") = Some(config.clone());
    ///         Ok(())
    ///     }
    ///
    ///     async fn delete(&self, _bucket: &bucket::Name) -> Result<(), storage::Error> {
    ///         *self.0.lock().expect("the fixture is not poisoned") = None;
    ///         Ok(())
    ///     }
    /// }
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let admin = Admin::default();
    /// let name = bucket::name("logs").expect("a bucket name");
    /// let config = lifecycle::Config {
    ///     rules: vec![lifecycle::Rule {
    ///         id: Some("expire-logs".to_owned()),
    ///         status: lifecycle::Status::Enabled,
    ///         filter: lifecycle::Filter::default(),
    ///         expiration: Some(lifecycle::Expiration {
    ///             days: Some(30),
    ///             date: None,
    ///             expired_object_delete_marker: None,
    ///         }),
    ///         noncurrent_version_expiration: None,
    ///         abort_incomplete_multipart_upload: None,
    ///     }],
    /// };
    /// admin.put(&name, &config).await.expect("a write");
    /// assert_eq!(admin.get(&name).await.expect("a read"), Some(config));
    /// # }
    /// ```
    async fn put(
        &self,
        bucket: &bucket::Name,
        config: &lifecycle::Config,
    ) -> Result<(), storage::Error>;

    /// Remove the bucket's configuration; removing one that was never stored is `Ok`, so the
    /// route is idempotent.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::Mutex;
    ///
    /// use async_trait::async_trait;
    /// use tinio_api::{
    ///     _core::{bucket, lifecycle, storage},
    ///     lifecycle::LifecycleAdmin,
    /// };
    ///
    /// struct Admin(Mutex<Option<lifecycle::Config>>);
    ///
    /// #[async_trait]
    /// impl LifecycleAdmin for Admin {
    ///     async fn get(
    ///         &self,
    ///         _bucket: &bucket::Name,
    ///     ) -> Result<Option<lifecycle::Config>, storage::Error> {
    ///         Ok(self.0.lock().expect("the fixture is not poisoned").clone())
    ///     }
    ///
    ///     async fn put(
    ///         &self,
    ///         _bucket: &bucket::Name,
    ///         config: &lifecycle::Config,
    ///     ) -> Result<(), storage::Error> {
    ///         *self.0.lock().expect("the fixture is not poisoned") = Some(config.clone());
    ///         Ok(())
    ///     }
    ///
    ///     async fn delete(&self, _bucket: &bucket::Name) -> Result<(), storage::Error> {
    ///         *self.0.lock().expect("the fixture is not poisoned") = None;
    ///         Ok(())
    ///     }
    /// }
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let admin = Admin(Mutex::new(Some(lifecycle::Config { rules: Vec::new() })));
    /// let name = bucket::name("logs").expect("a bucket name");
    /// admin.delete(&name).await.expect("a delete");
    /// assert_eq!(admin.get(&name).await.expect("a read"), None);
    /// // Removing it again is the same answer: the route is idempotent.
    /// admin.delete(&name).await.expect("a second delete");
    /// # }
    /// ```
    async fn delete(&self, bucket: &bucket::Name) -> Result<(), storage::Error>;
}

/// Registers the three lifecycle routes. The caller keeps them above the token
/// layer, with every other route (LC14, LC15).
pub(crate) fn mount(routes: Router<Shared>) -> Router<Shared> {
    routes.route(
        "/buckets/{bucket}/lifecycle",
        get(lifecycle_get)
            .put(lifecycle_put)
            .delete(lifecycle_delete),
    )
}

/// `GET /buckets/{bucket}/lifecycle`: the stored configuration in the model's own JSON (LC17).
///
/// `404` covers two absences — no configuration stored for the bucket, and a plane carrying no
/// admin at all (LC15, LC23) — because they are the same answer to this endpoint: there is
/// nothing to serve here. Neither touches the stored rules.
async fn lifecycle_get(AxumState(shared): AxumState<Shared>, Path(wire): Path<String>) -> Response {
    let (admin, bucket) = match admit(&shared, wire) {
        Ok(found) => found,
        Err(refused) => return refused.response(),
    };
    match admin.get(&bucket).await {
        Ok(Some(config)) => Json(config).into_response(),
        Ok(None) => lifecycle_refusal(StatusCode::NOT_FOUND, "no lifecycle configuration"),
        Err(err) => lifecycle_failure(err),
    }
}

/// `PUT /buckets/{bucket}/lifecycle`: replace the stored configuration with the body — the model
/// itself, so the JSON a caller sends is the row the store keeps and there is no second shape
/// (LC17). A member the model does not have is ignored, as it is on every read of the wire.
///
/// **The admin is taken before the body is parsed.** A plane carrying none answers `404` whatever
/// the body looks like, so a disabled capability (LC23) or a stripped build (LC15) never
/// advertises itself with a parser's status. With an admin in hand the body is parsed and
/// **validated before it is written** — the plane is a write path, so LC8's rule applies here as
/// it does on the S3 surface: an invalid rule must not be stored and then enforced by the pass.
async fn lifecycle_put(
    AxumState(shared): AxumState<Shared>,
    Path(wire): Path<String>,
    body: Result<Json<lifecycle::Config>, JsonRejection>,
) -> Response {
    let (admin, bucket) = match admit(&shared, wire) {
        Ok(found) => found,
        Err(refused) => return refused.response(),
    };
    let Json(config) = match body {
        Ok(body) => body,
        // The extractor's own verdict once this plane serves the route: the body is not the
        // model, so the status says what is wrong with it (a missing content type, a syntax
        // error, a member of the wrong type).
        Err(rejection) => {
            tracing::debug!(error = %rejection, "the lifecycle body is not the model");
            return lifecycle_refusal(
                rejection.status(),
                "the body is not a lifecycle configuration",
            );
        }
    };
    if let Err(invalid) = config.validate() {
        return lifecycle_invalid(invalid);
    }
    match admin.put(&bucket, &config).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => lifecycle_failure(err),
    }
}

/// `DELETE /buckets/{bucket}/lifecycle`: remove the stored configuration. Idempotent — removing
/// one that was never stored is `204` like any other, since the answer the caller needs is that
/// nothing is stored now.
async fn lifecycle_delete(
    AxumState(shared): AxumState<Shared>,
    Path(wire): Path<String>,
) -> Response {
    let (admin, bucket) = match admit(&shared, wire) {
        Ok(found) => found,
        Err(refused) => return refused.response(),
    };
    match admin.delete(&bucket).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => lifecycle_failure(err),
    }
}

/// Why [`admit`] refused a request.
enum Admission {
    /// No admin was injected (LC15, LC23).
    Absent,
    /// The route segment is not a bucket name.
    InvalidName,
}

impl Admission {
    /// The plane's answer for this refusal.
    fn response(self) -> Response {
        match self {
            Self::Absent => lifecycle_absent(),
            Self::InvalidName => lifecycle_refusal(StatusCode::NOT_FOUND, "no such bucket"),
        }
    }
}

/// The admin and the bucket every route's handler needs. **The admin is taken
/// before anything else**, so a plane carrying none answers `404` whatever the
/// request looks like.
fn admit(
    shared: &Shared,
    wire: String,
) -> Result<(Arc<dyn LifecycleAdmin>, bucket::Name), Admission> {
    let Some(admin) = shared.lifecycle() else {
        return Err(Admission::Absent);
    };
    let Ok(bucket) = bucket::name(wire) else {
        return Err(Admission::InvalidName);
    };
    Ok((Arc::clone(admin), bucket))
}

/// The plane's answer when no admin was injected: the contract's `404`, in this crate's error
/// shape so a caller reads one format. **One mechanism for both causes** — LC15's stripped build
/// and LC23's disabled capability — and what it protects is the stored rules: a plane that cannot
/// serve a configuration must never be able to replace it.
fn lifecycle_absent() -> Response {
    lifecycle_refusal(StatusCode::NOT_FOUND, "lifecycle is not served here")
}

/// A refused configuration: `400` with the model's own verdict carrying the offending rule's ID
/// (`None` when the rule has none) and the reason. No S3 code is added — the plane is not the S3
/// operation table — and nothing was written, which is the point of refusing here (LC8).
fn lifecycle_invalid(invalid: lifecycle::Invalid) -> Response {
    let body = InvalidBody {
        rule: invalid.rule,
        reason: invalid.reason.to_string(),
    };
    (StatusCode::BAD_REQUEST, Json(body)).into_response()
}

/// This crate's error body with `status`: the shape every failure this plane answers carries.
fn lifecycle_refusal(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(ErrorBody {
            error: message.to_string(),
        }),
    )
        .into_response()
}

/// A storage failure as the plane's status: a read-only run's refusal is `403` — the one status the
/// plane maps itself, because the runs are not S3 operations (LC18) — a missing bucket is `404`, and
/// everything else is a `500` whose detail stays in the log, as it does for every other internal
/// failure this crate answers.
///
/// **The read-only refusal has two shapes**, and both are `403`: the pg handle answers
/// `AccessDenied`, while the filesystem handle is refused at its state database and keeps the
/// contract's read-only marker ([`storage::Error::is_read_only`]). Nothing was written on either.
fn lifecycle_failure(err: storage::Error) -> Response {
    tracing::warn!(error = %err, "a lifecycle request failed");
    if err.is_read_only() || matches!(&err, storage::Error::AccessDenied(_)) {
        return lifecycle_refusal(StatusCode::FORBIDDEN, "access denied");
    }
    match err {
        storage::Error::NoSuchBucket(_) => {
            lifecycle_refusal(StatusCode::NOT_FOUND, "no such bucket")
        }
        _ => lifecycle_refusal(StatusCode::INTERNAL_SERVER_ERROR, "internal server error"),
    }
}

// Never called — `status_doc` carries why.

/// The bucket's stored lifecycle configuration, in the model's own JSON.
#[cfg(feature = "openapi")]
#[expect(
    dead_code,
    reason = "argued: the `#[utoipa::path]` stub has no caller by design; the attribute needs an item to attach to, so nothing reaches it"
)]
#[utoipa::path(
    get,
    path = "/buckets/{bucket}/lifecycle",
    tag = "management",
    params(("bucket" = String, Path, description = "The bucket's name")),
    responses(
        (status = 200, description = "The stored configuration; its members are the model's"),
        (status = 401, description = "Missing or wrong token"),
        (status = 404, description = "No such bucket, no stored configuration, or lifecycle is not served here")
    ),
    security(("token" = []))
)]
pub(crate) fn lifecycle_get_doc() {}

// Never called — `status_doc` carries why.

/// Replace the bucket's stored lifecycle configuration with the body, the model's own JSON.
#[cfg(feature = "openapi")]
#[expect(
    dead_code,
    reason = "argued: the `#[utoipa::path]` stub has no caller by design; the attribute needs an item to attach to, so nothing reaches it"
)]
#[utoipa::path(
    put,
    path = "/buckets/{bucket}/lifecycle",
    tag = "management",
    params(("bucket" = String, Path, description = "The bucket's name")),
    responses(
        (status = 204, description = "Replaced; the request body is the model's own JSON"),
        (status = 400, description = "JSON syntax error (`{\"error\": …}`), or a configuration the model refuses: `rule` and `reason`"),
        (status = 401, description = "Missing or wrong token"),
        (status = 403, description = "Refused: the run is read-only"),
        (status = 404, description = "No such bucket, or lifecycle is not served here"),
        (status = 413, description = "The body is past the plane's body limit"),
        (status = 415, description = "Missing `Content-Type: application/json`"),
        (status = 422, description = "The JSON is not the model: a member of the wrong type")
    ),
    security(("token" = []))
)]
pub(crate) fn lifecycle_put_doc() {}

// Never called — `status_doc` carries why.

/// Remove the bucket's stored lifecycle configuration; idempotent.
#[cfg(feature = "openapi")]
#[expect(
    dead_code,
    reason = "argued: the `#[utoipa::path]` stub has no caller by design; the attribute needs an item to attach to, so nothing reaches it"
)]
#[utoipa::path(
    delete,
    path = "/buckets/{bucket}/lifecycle",
    tag = "management",
    params(("bucket" = String, Path, description = "The bucket's name")),
    responses(
        (status = 204, description = "Removed, or nothing was stored"),
        (status = 401, description = "Missing or wrong token"),
        (status = 403, description = "Refused: the run is read-only"),
        (status = 404, description = "No such bucket, or lifecycle is not served here")
    ),
    security(("token" = []))
)]
pub(crate) fn lifecycle_delete_doc() {}

#[cfg(test)]
mod tests {
    use std::{
        io,
        path::Path,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        response::Response,
    };
    use http_body_util::BodyExt;
    use secrecy::ExposeSecret;
    use tokio::sync::watch;
    use tower::ServiceExt;

    use super::LifecycleAdmin;
    use crate::{
        _core::{bucket, lifecycle, storage},
        _util::testing::metadata::lifecycle_config,
        ErrorBody, State,
        router::{Auth, Context, InvalidBody, Shared, router},
    };

    fn state() -> State {
        State::new(
            "tinio-0123456789abcdef.sock",
            Some(9000),
            Path::new("/srv/data"),
        )
        .expect("state")
    }

    const PATH: &str = "/buckets/logs/lifecycle";

    fn config() -> lifecycle::Config {
        lifecycle_config("expire-logs")
    }

    /// What the admin answers instead of `Ok`.
    #[derive(Clone, Copy, Default)]
    enum Failure {
        #[default]
        None,
        /// A read-only run on the filesystem backend: the store refuses the write at its state
        /// database, which the storage contract spells as an I/O failure whose kind is
        /// `ReadOnlyFilesystem` (and the read is served, LC18).
        ReadOnlyIo,
        /// A read-only run on the pg backend: the same refusal as the store's `AccessDenied`
        /// (and the read is served, LC18).
        ReadOnly,
        /// A failure the mapping has no code for: `500`, and the detail stays in the log.
        Unexpected,
    }

    impl Failure {
        fn refusal(self, write: bool, bucket: &bucket::Name) -> Option<storage::Error> {
            match self {
                Self::None => None,
                Self::ReadOnlyIo if !write => None,
                Self::ReadOnlyIo => Some(storage::io(io::Error::from(
                    io::ErrorKind::ReadOnlyFilesystem,
                ))),
                Self::ReadOnly if !write => None,
                Self::ReadOnly => Some(storage::access_denied_bucket(bucket)),
                Self::Unexpected => Some(storage::io(io::Error::other(
                    "/srv/data/.tinio/meta.redb is unreadable",
                ))),
            }
        }
    }

    /// The CLI's admin in miniature: one stored configuration, and the refusal every call
    /// answers with when it is not `Ok`.
    #[derive(Default)]
    struct Admin {
        stored: Mutex<Option<lifecycle::Config>>,
        failure: Failure,
    }

    impl Admin {
        fn storing(config: lifecycle::Config) -> Self {
            Self {
                stored: Mutex::new(Some(config)),
                ..Self::default()
            }
        }

        fn failing(failure: Failure) -> Self {
            Self {
                failure,
                ..Self::default()
            }
        }
    }

    #[async_trait]
    impl LifecycleAdmin for Admin {
        async fn get(
            &self,
            bucket: &bucket::Name,
        ) -> Result<Option<lifecycle::Config>, storage::Error> {
            match self.failure.refusal(false, bucket) {
                Some(err) => Err(err),
                None => Ok(self
                    .stored
                    .lock()
                    .expect("the fixture is not poisoned")
                    .clone()),
            }
        }

        async fn put(
            &self,
            bucket: &bucket::Name,
            config: &lifecycle::Config,
        ) -> Result<(), storage::Error> {
            match self.failure.refusal(true, bucket) {
                Some(err) => Err(err),
                None => {
                    *self.stored.lock().expect("the fixture is not poisoned") =
                        Some(config.clone());
                    Ok(())
                }
            }
        }

        async fn delete(&self, bucket: &bucket::Name) -> Result<(), storage::Error> {
            match self.failure.refusal(true, bucket) {
                Some(err) => Err(err),
                None => {
                    *self.stored.lock().expect("the fixture is not poisoned") = None;
                    Ok(())
                }
            }
        }
    }

    /// A plane whose context carries `admin` (or none) against a fresh state, and its token.
    fn plane(auth: Auth, admin: Option<Arc<dyn LifecycleAdmin>>) -> (Shared, String) {
        let (tx, _rx) = watch::channel(false);
        let shared = Arc::new(Context::new(state(), "127.0.0.1", auth, tx).with_lifecycle(admin));
        let token = shared.state.token.expose_secret().as_str().to_owned();
        (shared, token)
    }

    fn request(method: &str, token: Option<&str>, body: Option<String>) -> Request<Body> {
        let mut builder = Request::builder().uri(PATH).method(method);
        if let Some(token) = token {
            builder = builder.header("x-tinio-token", token);
        }
        // The body is the model's JSON (LC17), so the plane's extractor is told exactly that,
        // as any caller of a `PUT` is (a request without the header is `415`).
        if body.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        builder
            .body(body.map_or_else(Body::empty, Body::from))
            .expect("request")
    }

    /// The three methods, with the body only `PUT` carries — for the tests that assert one
    /// answer for all of them.
    fn calls() -> [(&'static str, Option<String>); 3] {
        [
            ("GET", None),
            ("PUT", Some(config().to_wire())),
            ("DELETE", None),
        ]
    }

    async fn body_text(response: Response) -> String {
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        String::from_utf8(body.to_vec()).expect("utf8")
    }

    #[tokio::test]
    async fn the_lifecycle_routes_need_the_token() {
        // Registered above the token layer like every other route, so the path rule the
        // middleware already carries covers them: guarded on every transport, because a
        // configuration is neither a scrape nor the schema. That these are the guard's `401`s
        // and not an unregistered path's `404`s is what the tokened read below answers.
        let admin: Arc<dyn LifecycleAdmin> = Arc::new(Admin::storing(config()));
        for auth in [Auth::Local, Auth::Loopback, Auth::Tcp] {
            let (shared, token) = plane(auth, Some(Arc::clone(&admin)));
            for (method, body) in calls() {
                let response = router(shared.clone())
                    .oneshot(request(method, None, body))
                    .await
                    .expect("response");
                assert_eq!(
                    response.status(),
                    StatusCode::UNAUTHORIZED,
                    "{auth:?} {method}"
                );
            }
            let response = router(shared)
                .oneshot(request("GET", Some(&token), None))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK, "{auth:?}");
        }
    }

    #[tokio::test]
    async fn a_get_without_a_configuration_is_404() {
        // A bucket with no stored configuration answers `404`; a stored one answers the
        // model's own JSON (LC17). The pair is what says the first `404` is the handler's
        // answer rather than the fallback's.
        let (empty, token) = plane(Auth::Local, Some(Arc::new(Admin::default())));
        let response = router(empty)
            .oneshot(request("GET", Some(&token), None))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let stored_admin: Arc<dyn LifecycleAdmin> = Arc::new(Admin::storing(config()));
        let (stored, token) = plane(Auth::Local, Some(stored_admin));
        let response = router(stored)
            .oneshot(request("GET", Some(&token), None))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let text = body_text(response).await;
        assert_eq!(
            serde_json::from_str::<lifecycle::Config>(&text).expect("the model's own JSON"),
            config(),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_put_and_a_delete_replace_and_clear_the_stored_configuration() {
        let (shared, token) = plane(Auth::Local, Some(Arc::new(Admin::default())));
        let response = router(shared.clone())
            .oneshot(request("PUT", Some(&token), Some(config().to_wire())))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let response = router(shared.clone())
            .oneshot(request("GET", Some(&token), None))
            .await
            .expect("response");
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "the put replaced the row"
        );

        let response = router(shared.clone())
            .oneshot(request("DELETE", Some(&token), None))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let response = router(shared)
            .oneshot(request("GET", Some(&token), None))
            .await
            .expect("response");
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "the delete cleared it"
        );
    }

    #[tokio::test]
    async fn a_read_only_refusal_is_403() {
        // LC18: the plane's writes are not S3 operations, so a read-only run's refusal arrives
        // as the store's answer and the plane is what maps it — while the read a read-only run
        // can serve it serves. Both backends' shapes are driven, because they differ: pg
        // refuses with `AccessDenied`, fs at its state database.
        for failure in [Failure::ReadOnlyIo, Failure::ReadOnly] {
            let read_only: Arc<dyn LifecycleAdmin> = Arc::new(Admin::failing(failure));
            let (shared, token) = plane(Auth::Local, Some(read_only));
            for (method, body) in [("PUT", Some(config().to_wire())), ("DELETE", None)] {
                let response = router(shared.clone())
                    .oneshot(request(method, Some(&token), body))
                    .await
                    .expect("response");
                assert_eq!(response.status(), StatusCode::FORBIDDEN, "{method}");
                let text = body_text(response).await;
                assert!(text.contains("access denied"), "{method}: {text}");
            }
            let response = router(shared)
                .oneshot(request("GET", Some(&token), None))
                .await
                .expect("response");
            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "the read is served: the admin's own answer came through"
            );
        }
    }

    /// A rule that violates the schema class (LC8): two top-level filter predicates, which no
    /// S3 client and no command line produces.
    fn two_predicates() -> lifecycle::Config {
        lifecycle::Config {
            rules: vec![lifecycle::Rule {
                id: Some("two-predicates".to_owned()),
                filter: lifecycle::Filter {
                    prefix: Some("logs/".to_owned()),
                    size_greater_than: Some(500),
                    ..lifecycle::Filter::default()
                },
                ..config().rules[0].clone()
            }],
        }
    }

    /// A `PUT` whose body is not the model at all: valid JSON of the wrong type, sent under an
    /// explicit content type (`None` is the request the extractor refuses for the header alone).
    fn malformed(token: &str, content_type: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder()
            .uri(PATH)
            .method("PUT")
            .header("x-tinio-token", token);
        if let Some(content_type) = content_type {
            builder = builder.header("content-type", content_type);
        }
        builder
            .body(Body::from(r#"{"rules": "none"}"#))
            .expect("request")
    }

    #[tokio::test]
    async fn an_invalid_rule_is_400() {
        // LC8: the request path validates, and the plane is a write path — so an invalid rule
        // is refused before it is stored, with the model's own verdict (rule id and reason)
        // and no S3 error code. The `GET` after it is what proves nothing was written.
        let (shared, token) = plane(Auth::Local, Some(Arc::new(Admin::default())));
        let invalid = two_predicates();
        let expected = invalid.validate().expect_err("the fixture is refused");

        let response = router(shared.clone())
            .oneshot(request("PUT", Some(&token), Some(invalid.to_wire())))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let refused: InvalidBody =
            serde_json::from_str(&body_text(response).await).expect("the refusal body");
        assert_eq!(refused.rule, expected.rule);
        assert_eq!(refused.rule.as_deref(), Some("two-predicates"));
        assert_eq!(refused.reason, expected.reason);

        let response = router(shared)
            .oneshot(request("GET", Some(&token), None))
            .await
            .expect("response");
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "the refused configuration was never stored"
        );
    }

    #[tokio::test]
    async fn no_admin_answers_404_even_with_a_malformed_body() {
        // The admin is taken before the body is parsed, so a plane carrying none answers its
        // `404` whatever the body is — a disabled capability never advertises itself with a
        // status the parser produced (LC15, LC23). The same body with an admin present is the
        // parser's to refuse, which is what makes this ordering rather than a blanket.
        let (absent, token) = plane(Auth::Local, None);
        for content_type in [Some("application/json"), None] {
            let response = router(absent.clone())
                .oneshot(malformed(&token, content_type))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{content_type:?}");
            let text = body_text(response).await;
            assert!(
                text.contains("lifecycle is not served here"),
                "{content_type:?}: {text}"
            );
        }

        let (present, token) = plane(Auth::Local, Some(Arc::new(Admin::default())));
        let response = router(present)
            .oneshot(malformed(&token, Some("application/json")))
            .await
            .expect("response");
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "the parser's own status, once the route is served"
        );
    }

    #[tokio::test]
    async fn no_injected_admin_answers_404() {
        // LC15's stripped build and LC23's disabled capability are one mechanism and one
        // answer: no admin in the context. The two legs are the two ways a context ends up
        // without one — `Context::new`'s own default, never calling `with_lifecycle` at all,
        // and an explicit `with_lifecycle(None)` — so every method answers `404`, with a body,
        // which is what makes it the handler's answer rather than the fallback's. Nothing can
        // replace a stored configuration on a plane that cannot serve it.
        let (tx, _rx) = watch::channel(false);
        let bare = Arc::new(Context::new(state(), "127.0.0.1", Auth::Local, tx));
        let bare_token = bare.state.token.expose_secret().as_str().to_owned();
        let (explicit, explicit_token) = plane(Auth::Local, None);
        for (shared, token) in [(bare, bare_token), (explicit, explicit_token)] {
            for (method, body) in calls() {
                let response = router(shared.clone())
                    .oneshot(request(method, Some(&token), body))
                    .await
                    .expect("response");
                assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method}");
                let text = body_text(response).await;
                assert!(
                    text.contains("lifecycle is not served here"),
                    "{method}: {text}"
                );
            }
        }
    }

    #[tokio::test]
    async fn an_unexpected_storage_failure_is_500_and_keeps_its_detail_out_of_the_body() {
        let broken: Arc<dyn LifecycleAdmin> = Arc::new(Admin::failing(Failure::Unexpected));
        let (shared, token) = plane(Auth::Local, Some(broken));
        for (method, body) in calls() {
            let response = router(shared.clone())
                .oneshot(request(method, Some(&token), body))
                .await
                .expect("response");
            assert_eq!(
                response.status(),
                StatusCode::INTERNAL_SERVER_ERROR,
                "{method}"
            );
            let text = body_text(response).await;
            assert_eq!(
                serde_json::from_str::<ErrorBody>(&text)
                    .expect("the crate's error shape")
                    .error,
                "internal server error",
                "{method}"
            );
            assert!(
                !text.contains("meta.redb"),
                "the detail stays in the log: {text}"
            );
        }
    }
}
