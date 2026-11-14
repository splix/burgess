//! Requests the API cannot serve.

use axum::Json;
use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use burgess_api::{ErrorBody, TimeParseError};
use burgess_core::flow::IdParseError;
use burgess_core::report::error_chain;

use crate::queries::{self, TimeField};

/// Why a request cannot be served. Each answers with an
/// [`ErrorBody`] saying what to change, so a client reads every error the
/// same way.
#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    /// The id in the path is not a tree id or a flow id.
    #[error("invalid id")]
    Id(#[from] IdParseError),
    /// `since` or `until` is not a time.
    #[error("invalid {field}")]
    Time {
        /// The option given.
        field: TimeField,
        /// Why it is not a time.
        #[source]
        source: TimeParseError,
    },
    /// `limit` is not a number.
    #[error("invalid limit {found:?}, expected a number of rows")]
    LimitNotANumber {
        /// The value given.
        found: String,
    },
    /// `order` is not an order that REST names.
    #[error("invalid order {found:?}, expected asc or desc")]
    Order {
        /// The value given.
        found: String,
    },
    /// An option is given two times. The API would take one of them, and
    /// the caller could not tell which.
    #[error("{name} is given more than one time")]
    GivenTwice {
        /// The option given.
        name: String,
    },
    /// The options cannot be read: an option that the route does not take,
    /// since a misspelled one would otherwise be silently ignored, or a
    /// value of a wrong type.
    #[error("invalid options: {0}")]
    Options(String),
    /// The lookup refused the request, as it refuses it over gRPC.
    #[error(transparent)]
    Lookup(#[from] queries::RequestError),
    /// The path, the query string or the body cannot be read. That includes
    /// an option that the route does not take, since a misspelled option
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
    #[error(
        "no such route; the API serves /v1/trees/{{id}}, /v1/flows/{{id}}, /v1/states and /v1/refs"
    )]
    NoRoute,
    /// The route does not serve the method. The response tells the methods
    /// that it serves.
    #[error("the route does not serve this method")]
    MethodNotAllowed,
}

impl RequestError {
    fn status(&self) -> StatusCode {
        match self {
            RequestError::Unreadable { status, .. } => *status,
            RequestError::NoRoute => StatusCode::NOT_FOUND,
            RequestError::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            // The page that the cursor continues is gone for good.
            RequestError::Lookup(queries::RequestError::Expired) => StatusCode::GONE,
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

impl From<JsonRejection> for RequestError {
    fn from(rejection: JsonRejection) -> Self {
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
        let where_column = match &self {
            RequestError::Lookup(queries::RequestError::Where(filter)) => Some(filter.column),
            _ => None,
        };
        let body = ErrorBody {
            error: error_chain(&self),
            where_column,
        };
        (self.status(), Json(body)).into_response()
    }
}
