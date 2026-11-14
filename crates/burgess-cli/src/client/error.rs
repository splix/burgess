//! Why no page came back.

use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

use burgess_api::grpc::WHERE_COLUMN;
use burgess_core::report::error_chain;

use tonic::{Code, Status};

use crate::server::ServerAddress;

/// Why no page came back.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// TLS for an https address cannot be set up.
    #[error("setting up TLS for {server} failed")]
    Tls {
        /// The address given.
        server: ServerAddress,
        /// What went wrong.
        #[source]
        source: tonic::transport::Error,
    },
    /// The service is down, the address is wrong, or the service is
    /// temporarily unavailable. gRPC tells all of these the same way.
    #[error("the query service at {server} is unavailable")]
    Unavailable {
        /// The address asked.
        server: ServerAddress,
        /// What gRPC said, and its cause, such as a refused connection.
        #[source]
        cause: StatusCause,
    },
    /// The service refused the request as it was written.
    #[error("the query service refused the request")]
    Refused {
        /// The column of the `where` filter that breaks the grammar, when
        /// that is why.
        where_column: Option<NonZeroUsize>,
        /// The words of the service.
        #[source]
        cause: StatusCause,
    },
    /// Retention deleted rows of the answer that the cursor continues,
    /// since the answer started. Only an answer from its start can tell
    /// what is still stored; the words of the service say so.
    #[error("the query service cannot continue the answer")]
    Expired {
        /// The words of the service.
        #[source]
        cause: StatusCause,
    },
    /// The service, or a different program at its address, failed.
    #[error("the query service at {server} failed with {code:?}")]
    Failed {
        /// The address asked.
        server: ServerAddress,
        /// The gRPC status code.
        code: Code,
        /// The words of the service, and their cause.
        #[source]
        cause: StatusCause,
    },
}

impl ClientError {
    /// The error that `status` tells, from the service at `server`, for a
    /// request that `continued` an answer with a cursor or not.
    pub(super) fn from_status(status: Status, server: &ServerAddress, continued: bool) -> Self {
        match status.code() {
            Code::InvalidArgument => ClientError::Refused {
                where_column: where_column(&status),
                cause: StatusCause(status),
            },
            // The service tells an expired cursor so. To a request without
            // a cursor, it is the answer of another program.
            Code::FailedPrecondition if continued => ClientError::Expired {
                cause: StatusCause(status),
            },
            Code::Unavailable => ClientError::Unavailable {
                server: server.clone(),
                cause: StatusCause(status),
            },
            code => ClientError::Failed {
                server: server.clone(),
                code,
                cause: StatusCause(status),
            },
        }
    }

    /// Whether the command line is at fault, thus only a different command
    /// line can succeed.
    pub fn is_usage(&self) -> bool {
        matches!(
            self,
            ClientError::Refused { .. } | ClientError::Expired { .. }
        )
    }

    /// The problem of an answer that stopped for this error after some
    /// rows.
    pub fn stopped_answer(&self) -> String {
        format!("the answer stopped before its end: {}", error_chain(self))
    }
}

/// The column that the service tells for a `where` that breaks the
/// grammar. A service that does not tell it, or tells one that cannot be
/// read, gives `None`, and the message is then shown alone.
fn where_column(status: &Status) -> Option<NonZeroUsize> {
    status
        .metadata()
        .get(WHERE_COLUMN)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

/// A gRPC status as the cause of an error. Its text is only the message,
/// because the code is in the error above it, and the `Display` of a
/// status also prints its metadata.
#[derive(Debug)]
pub struct StatusCause(Status);

impl fmt::Display for StatusCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.message() {
            "" => f.write_str("no message"),
            message => f.write_str(message),
        }
    }
}

impl Error for StatusCause {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.source()
    }
}
