//! The management-plane client's Object Lock calls (`OL23`), both routes through [`send`].
//!
//! The key and the version address travel in the JSON body, never in the path (OL3, OL29).

use http::Method;

use super::{RawResponse, send};
use crate::{Error, state::State};

/// The bucket Object Lock route's target (`OL23`'s `GET`/`PUT` pair).
///
/// # Examples
///
/// ```rust
/// use tinio_api::client::object_lock_target;
///
/// assert_eq!(object_lock_target("photos"), "/buckets/photos/object-lock");
/// ```
#[cfg(feature = "object-lock")]
pub fn object_lock_target(bucket: &str) -> String {
    format!("/buckets/{bucket}/object-lock")
}

/// The per-version Object Lock route's target. The key and the version address travel in the
/// JSON body, never in this path (OL3, OL29).
///
/// # Examples
///
/// ```rust
/// use tinio_api::client::object_lock_objects_target;
///
/// assert_eq!(
///     object_lock_objects_target("photos"),
///     "/buckets/photos/object-lock/objects"
/// );
/// ```
#[cfg(feature = "object-lock")]
pub fn object_lock_objects_target(bucket: &str) -> String {
    format!("/buckets/{bucket}/object-lock/objects")
}

/// `GET /buckets/{bucket}/object-lock` (OL23): the stored configuration, or a `404` whose body
/// names the absence.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_api::{client::object_lock_get, state::State};
///
/// # async fn run() -> Result<(), tinio_api::Error> {
/// // Port 0 names no listener: the dial is the failure path, so no server is
/// // needed and no port has to be reserved first (nothing can occupy port 0).
/// let state = State::new("127.0.0.1:0".to_owned(), Some(9000), Path::new("/srv/data"))?;
/// let answer = object_lock_get(&state, "photos").await;
/// assert!(matches!(answer, Err(tinio_api::Error::Io(_))));
/// # Ok(())
/// # }
/// #
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// #     run().await.expect("the closed-port dial is an error");
/// # }
/// ```
///
/// # Errors
///
/// The dial failures [`send`] answers.
#[cfg(feature = "object-lock")]
pub async fn object_lock_get(state: &State, bucket: &str) -> Result<RawResponse, Error> {
    send(state, &object_lock_target(bucket), Method::GET, None).await
}

/// `PUT /buckets/{bucket}/object-lock` (OL23): enable the bucket or replace its rule.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_api::{client::object_lock_put, state::State};
///
/// # async fn run() -> Result<(), tinio_api::Error> {
/// // Port 0 names no listener: the dial is the failure path, so no server is
/// // needed and no port has to be reserved first (nothing can occupy port 0).
/// let state = State::new("127.0.0.1:0".to_owned(), Some(9000), Path::new("/srv/data"))?;
/// let answer = object_lock_put(&state, "photos", r#"{"enabled":true,"rule":null}"#).await;
/// assert!(matches!(answer, Err(tinio_api::Error::Io(_))));
/// # Ok(())
/// # }
/// #
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// #     run().await.expect("the closed-port dial is an error");
/// # }
/// ```
///
/// # Errors
///
/// The dial failures [`send`] answers.
#[cfg(feature = "object-lock")]
pub async fn object_lock_put(
    state: &State,
    bucket: &str,
    body: &str,
) -> Result<RawResponse, Error> {
    send(state, &object_lock_target(bucket), Method::PUT, Some(body)).await
}

/// `GET /buckets/{bucket}/object-lock/objects` (OL23): the addressed version's lock state.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_api::{client::object_lock_object_get, state::State};
///
/// # async fn run() -> Result<(), tinio_api::Error> {
/// // Port 0 names no listener: the dial is the failure path, so no server is
/// // needed and no port has to be reserved first (nothing can occupy port 0).
/// let state = State::new("127.0.0.1:0".to_owned(), Some(9000), Path::new("/srv/data"))?;
/// let body = r#"{"key":"report.txt","version":"current"}"#;
/// let answer = object_lock_object_get(&state, "photos", body).await;
/// assert!(matches!(answer, Err(tinio_api::Error::Io(_))));
/// # Ok(())
/// # }
/// #
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// #     run().await.expect("the closed-port dial is an error");
/// # }
/// ```
///
/// # Errors
///
/// The dial failures [`send`] answers.
#[cfg(feature = "object-lock")]
pub async fn object_lock_object_get(
    state: &State,
    bucket: &str,
    body: &str,
) -> Result<RawResponse, Error> {
    send(
        state,
        &object_lock_objects_target(bucket),
        Method::GET,
        Some(body),
    )
    .await
}

/// `PUT /buckets/{bucket}/object-lock/objects` (OL23): set or clear the addressed version's
/// retention and/or legal hold.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_api::{client::object_lock_object_put, state::State};
///
/// # async fn run() -> Result<(), tinio_api::Error> {
/// // Port 0 names no listener: the dial is the failure path, so no server is
/// // needed and no port has to be reserved first (nothing can occupy port 0).
/// let state = State::new("127.0.0.1:0".to_owned(), Some(9000), Path::new("/srv/data"))?;
/// let body = r#"{"key":"report.txt","version":"null","legal_hold":"ON"}"#;
/// let answer = object_lock_object_put(&state, "photos", body).await;
/// assert!(matches!(answer, Err(tinio_api::Error::Io(_))));
/// # Ok(())
/// # }
/// #
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// #     run().await.expect("the closed-port dial is an error");
/// # }
/// ```
///
/// # Errors
///
/// The dial failures [`send`] answers.
#[cfg(feature = "object-lock")]
pub async fn object_lock_object_put(
    state: &State,
    bucket: &str,
    body: &str,
) -> Result<RawResponse, Error> {
    send(
        state,
        &object_lock_objects_target(bucket),
        Method::PUT,
        Some(body),
    )
    .await
}
