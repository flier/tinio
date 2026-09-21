//! Management-plane errors (task T021).
//!
//! Maps onto the HTTP statuses and JSON bodies of the management API
//! contract: `401 {"error": "unauthorized"}` for token failures, `404` for
//! unknown paths, `500 {"error": "..."}` for internal failures.

use std::{io, path::PathBuf};

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::_core::storage;

/// Wire body for management-plane errors: `{"error": "<message>"}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: String,
}

/// A management-plane failure with its HTTP mapping.
///
/// # Examples
///
/// ```rust
/// use tinio_api::Error;
///
/// let err = Error::Unauthorized;
/// assert_eq!(err.status(), 401);
/// assert_eq!(err.body().error, "unauthorized");
/// ```
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Missing or wrong management token.
    #[error("unauthorized")]
    Unauthorized,
    /// Unknown management path.
    #[error("not found")]
    NotFound,
    /// An internal failure.
    #[error("{0}")]
    Internal(String),
    /// An I/O failure (control-channel bind, state file).
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// A storage-contract failure surfaced by the management plane.
    #[error("storage error: {0}")]
    Storage(#[from] storage::Error),
    /// The state file could not be read, or its contents are not usable.
    #[error("state file {path}: {message}")]
    State { path: PathBuf, message: String },
    /// A management transport could not be set up or resolved.
    #[error("transport: {0}")]
    Transport(String),
}

impl Error {
    /// The wire contract for this error: the status and the body's message, mapped together so
    /// the two cannot drift. `401`/`404` are the contract's; every internal failure is one
    /// `500` whose message is deliberately generic (the detail stays in [`std::fmt::Display`]
    /// for the server log).
    fn contract(&self) -> (u16, &'static str) {
        match self {
            Self::Unauthorized => (401, "unauthorized"),
            Self::NotFound => (404, "not found"),
            Self::Internal(_)
            | Self::Io(_)
            | Self::Storage(_)
            | Self::State { .. }
            | Self::Transport(_) => (500, "internal server error"),
        }
    }

    /// The HTTP status of this error (401/404/500).
    pub fn status(&self) -> u16 {
        self.contract().0
    }

    /// The typed JSON error body.
    ///
    /// Internal failures expose a generic message on the wire — the detailed
    /// message stays in [`std::fmt::Display`] for the server log.
    pub fn body(&self) -> ErrorBody {
        ErrorBody {
            error: self.contract().1.into(),
        }
    }

    /// The JSON error body (`{"error":"<message>"}`).
    pub fn json_body(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.body())
    }
}

impl IntoResponse for Error {
    /// The contract's status and body (`401 {"error":"unauthorized"}`, `404`,
    /// `500 {"error":"internal server error"}`); the detailed message is logged, not sent.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use axum::response::IntoResponse;
    /// use tinio_api::Error;
    ///
    /// let response = Error::Unauthorized.into_response();
    /// assert_eq!(response.status(), 401);
    /// ```
    fn into_response(self) -> Response {
        tracing::debug!(error = %self, "management request failed");
        let status =
            StatusCode::from_u16(self.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, Json(self.body())).into_response()
    }
}

#[cfg(test)]
mod tests {
    use io::Error as IoError;

    use super::*;
    use crate::{_core::storage::Error::*, _util::testing::assert_send_sync};

    #[test]
    fn maps_to_status_codes() {
        assert_eq!(Error::Unauthorized.status(), 401);
        assert_eq!(Error::NotFound.status(), 404);
        assert_eq!(Error::Internal("boom".into()).status(), 500);
        assert_eq!(Error::Io(IoError::other("x")).status(), 500);
        assert_eq!(Error::Storage(NoSuchKey("k".into())).status(), 500);
    }

    #[test]
    fn json_bodies_follow_contract() {
        let unauthorized: ErrorBody =
            serde_json::from_str(&Error::Unauthorized.json_body().unwrap()).unwrap();
        assert_eq!(
            unauthorized,
            ErrorBody {
                error: "unauthorized".into()
            }
        );
        let internal: ErrorBody =
            serde_json::from_str(&Error::Internal("disk full".into()).json_body().unwrap())
                .unwrap();
        assert_eq!(
            internal,
            ErrorBody {
                error: "internal server error".into()
            }
        );
    }

    #[test]
    fn internal_detail_stays_off_the_wire() {
        // OS paths / internal strings must not leak into JSON bodies (the
        // token-gated local channel is trusted, but TCP exposure is not).
        let err = Error::Internal(r"config at C:\Users\alice\.tinio\secrets".into());
        let body: ErrorBody = serde_json::from_str(&err.json_body().unwrap()).unwrap();
        assert_eq!(body.error, "internal server error");
        // The detail is still available via Display for the server log.
        assert!(err.to_string().contains("config at"));
    }

    #[test]
    fn errors_are_send_sync_and_static() {
        assert_send_sync::<Error>();
        assert_send_sync::<ErrorBody>();
    }

    #[test]
    fn body_messages_follow_contract() {
        assert_eq!(Error::Unauthorized.body().error, "unauthorized");
        assert_eq!(Error::NotFound.body().error, "not found");
        assert_eq!(
            Error::Internal("boom".into()).body().error,
            "internal server error"
        );
        assert_eq!(
            Error::Io(IoError::other("x")).body().error,
            "internal server error"
        );
        assert_eq!(
            Error::Storage(NoSuchKey("k".into())).body().error,
            "internal server error"
        );
    }
}
