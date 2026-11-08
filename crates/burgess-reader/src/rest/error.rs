//! Requests the API cannot serve.

use axum::Json;
use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use burgess_api::{ErrorBody, TimeParseError};
use burgess_core::flow::IdParseError;
use burgess_core::report::error_chain;

use crate::queries::PageRequestError;

/// Why a request cannot be served. Each answers with an
/// [`ErrorBody`] saying what to change, so a client reads every error the
/// same way.
#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    /// The id in the path is not one the route takes.
    #[error("invalid id")]
    Id(#[from] IdParseError),
    /// `until` is not a time.
    #[error("invalid until")]
    Until(#[from] TimeParseError),
    /// `limit` is not a number.
    #[error("invalid limit {found:?}, expected a number of rows")]
    LimitNotANumber {
        /// The value given.
        found: String,
    },
    /// `until` or `limit` is a value that the service does not serve.
    #[error(transparent)]
    Page(#[from] PageRequestError),
    /// The path or the query string cannot be read. That includes a query
    /// naming an option the route does not take, since a misspelled option
    /// would otherwise be silently ignored.
    #[error("{message}")]
    Unreadable {
        /// The status the reading part chose: the caller's mistake, or a
        /// route that does not fit its handler.
        status: StatusCode,
        /// What went wrong, already naming its cause.
        message: String,
    },
    /// No route serves the path.
    #[error("no such route; the API serves /v1/trees/{{id}} and /v1/flows/{{id}}")]
    NoRoute,
    /// The route serves only reads.
    #[error("the route serves only GET")]
    MethodNotAllowed,
}

impl RequestError {
    fn status(&self) -> StatusCode {
        match self {
            RequestError::Unreadable { status, .. } => *status,
            RequestError::NoRoute => StatusCode::NOT_FOUND,
            RequestError::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            _ => StatusCode::BAD_REQUEST,
        }
    }
}

impl From<PathRejection> for RequestError {
    fn from(rejection: PathRejection) -> Self {
        RequestError::Unreadable {
            status: rejection.status(),
            message: rejection.body_text(),
        }
    }
}

impl From<QueryRejection> for RequestError {
    fn from(rejection: QueryRejection) -> Self {
        RequestError::Unreadable {
            status: rejection.status(),
            message: rejection.body_text(),
        }
    }
}

impl IntoResponse for RequestError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            error: error_chain(&self),
        };
        (self.status(), Json(body)).into_response()
    }
}
