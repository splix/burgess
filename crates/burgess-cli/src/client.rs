//! Asking the query service for a page.

use std::time::Duration;

use burgess_api::{ErrorBody, Page};
use burgess_core::flow::{FlowId, TreeId};
use reqwest::{StatusCode, Url};

use crate::args::PageArgs;

/// How long to wait for the service to accept a connection. A lookup itself
/// may take long, reading many files, so only connecting is bounded: a
/// wrong address should fail fast rather than hang.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// What to look up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookup {
    /// Every row of a tree.
    Tree(TreeId),
    /// Every row of a flow and the flows below it.
    Flow(FlowId),
}

/// Why no page came back.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The address cannot have paths below it, like `mailto:` addresses.
    #[error("{server} cannot be the address of the query service")]
    NotABase {
        /// The address given.
        server: Url,
    },
    /// No answer came: the service is down, the address is wrong, or the
    /// connection broke.
    #[error("asking the query service at {server} failed")]
    Unreachable {
        /// The address asked.
        server: Url,
        /// What went wrong.
        #[source]
        source: reqwest::Error,
    },
    /// The service answered with an error.
    #[error("the query service answered {status}: {message}")]
    Refused {
        /// The status of the answer.
        status: StatusCode,
        /// The service's own words, or the body when it sent no error body.
        message: String,
    },
    /// The service answered with something other than a page, as a service
    /// of another version, or something else on its address, might.
    #[error("the query service answered {status} with a body that is not a page")]
    NotAPage {
        /// The status of the answer.
        status: StatusCode,
        /// Why the body is not a page.
        #[source]
        source: serde_json::Error,
    },
}

impl ClientError {
    /// Whether the command line itself is at fault, so only a different one
    /// can succeed: the service refused the request as written, or the
    /// server address cannot be one.
    pub fn is_usage(&self) -> bool {
        match self {
            ClientError::NotABase { .. } => true,
            ClientError::Refused { status, .. } => *status == StatusCode::BAD_REQUEST,
            ClientError::Unreachable { .. } | ClientError::NotAPage { .. } => false,
        }
    }
}

/// A client of one query service.
#[derive(Debug, Clone)]
pub struct QueryClient {
    http: reqwest::Client,
    server: Url,
}

impl QueryClient {
    /// A client of the service at `server`.
    pub fn new(server: Url) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .expect("a client without custom TLS settings builds");
        QueryClient { http, server }
    }

    /// The page the service answers `lookup` with.
    pub async fn page(&self, lookup: Lookup, page: PageArgs) -> Result<Page, ClientError> {
        let url = self.url(lookup, page)?;
        tracing::debug!(%url, "asking the query service");
        let unreachable = |source| ClientError::Unreachable {
            server: self.server.clone(),
            source,
        };
        let response = self.http.get(url).send().await.map_err(unreachable)?;
        let status = response.status();
        let body = response.bytes().await.map_err(unreachable)?;
        if !status.is_success() {
            return Err(ClientError::Refused {
                status,
                message: message(&body),
            });
        }
        serde_json::from_slice(&body).map_err(|source| ClientError::NotAPage { status, source })
    }

    /// The address of `lookup`, below the server's own path, so a service
    /// behind a proxy at `https://host/burgess` is asked at
    /// `https://host/burgess/v1/...`.
    fn url(&self, lookup: Lookup, page: PageArgs) -> Result<Url, ClientError> {
        let (route, id) = match lookup {
            Lookup::Tree(tree) => ("trees", tree.to_string()),
            Lookup::Flow(flow) => ("flows", flow.to_string()),
        };
        let mut url = self.server.clone();
        url.path_segments_mut()
            .map_err(|()| ClientError::NotABase {
                server: self.server.clone(),
            })?
            .pop_if_empty()
            .extend(["v1", route, &id]);
        url.set_query(None);
        {
            let mut query = url.query_pairs_mut();
            // Sent as unix milliseconds, which need no escaping, whichever
            // form the investigator typed.
            if let Some(until) = page.until {
                query.append_pair("until", &until.as_millisecond().to_string());
            }
            if let Some(limit) = page.limit {
                query.append_pair("limit", &limit.to_string());
            }
        }
        if url.query() == Some("") {
            url.set_query(None);
        }
        Ok(url)
    }
}

/// The service's message from an error body, or the body itself when it is
/// not one, as from a proxy in front of the service.
fn message(body: &[u8]) -> String {
    match serde_json::from_slice::<ErrorBody>(body) {
        Ok(error) => error.error,
        Err(_) => {
            let text = String::from_utf8_lossy(body);
            match text.trim() {
                "" => "no message".to_string(),
                text => text.to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests;
