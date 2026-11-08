//! Asking the query service for a page, over gRPC.

mod error;

pub use error::{ClientError, StatusCause};

use std::time::Duration;

use burgess_api::grpc::query_service_client::QueryServiceClient;
use burgess_api::grpc::{
    AnswerMessage, GetFlowRequest, GetTreeRequest, ListRefsRequest, ListStatesRequest,
};
use burgess_api::{Page, RefRow};
use burgess_core::report::error_chain;
use tonic::codec::Streaming;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Response, Status};

use crate::server::ServerAddress;

/// A lookup can take long when it reads many files, thus only the connect
/// and the TLS handshake have a timeout: a wrong address fails fast and does
/// not hang.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// One message holds one row. The writer sets the payload limit, which the
/// client does not know, thus the cap is far above any usual payload. A
/// cap is necessary: the decoder reserves the size that a message header
/// gives before the message comes.
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// A client of one query service.
#[derive(Debug, Clone)]
pub struct QueryClient {
    server: ServerAddress,
    grpc: QueryServiceClient<Channel>,
}

impl QueryClient {
    /// A client of the service at `server`. It connects on the first
    /// request, thus it must be made in the async runtime.
    pub fn new(server: ServerAddress) -> Result<Self, ClientError> {
        let mut endpoint = Endpoint::from(server.uri().clone()).connect_timeout(CONNECT_TIMEOUT);
        if server.is_https() {
            let tls = ClientTlsConfig::new()
                .with_native_roots()
                .domain_name(server.tls_name())
                .timeout(CONNECT_TIMEOUT);
            endpoint = endpoint
                .tls_config(tls)
                .map_err(|source| ClientError::Tls {
                    server: server.clone(),
                    source,
                })?;
        }
        let grpc = QueryServiceClient::new(endpoint.connect_lazy())
            .max_decoding_message_size(MAX_MESSAGE_BYTES);
        Ok(QueryClient { server, grpc })
    }

    /// The page of rows that the service answers `request` with.
    pub async fn tree(&self, request: GetTreeRequest) -> Result<Page, ClientError> {
        tracing::debug!(
            server = %self.server,
            tree = ?request.tree,
            until_ms = ?request.until_ms,
            limit = ?request.limit,
            "asking the query service for a tree"
        );
        let answer = self.grpc.clone().get_tree(request).await;
        self.collect(answer).await
    }

    /// The page of rows that the service answers `request` with.
    pub async fn flow(&self, request: GetFlowRequest) -> Result<Page, ClientError> {
        tracing::debug!(
            server = %self.server,
            flow = %request.flow_id,
            until_ms = ?request.until_ms,
            limit = ?request.limit,
            "asking the query service for a flow"
        );
        let answer = self.grpc.clone().get_flow(request).await;
        self.collect(answer).await
    }

    /// The page of states that the service answers `request` with.
    pub async fn states(&self, request: ListStatesRequest) -> Result<Page, ClientError> {
        // The window is logged, since a window of a default size is the
        // usual cause of an empty answer. The filter and the labels can
        // hold data about a person, thus they are not.
        tracing::debug!(
            server = %self.server,
            service = %request.service,
            category = %request.category,
            since_ms = ?request.since_ms,
            until_ms = ?request.until_ms,
            limit = ?request.limit,
            order = request.order,
            "asking the query service for states"
        );
        let answer = self.grpc.clone().list_states(request).await;
        self.collect(answer).await
    }

    /// The page of references that the service answers `request` with.
    pub async fn refs(&self, request: ListRefsRequest) -> Result<Page<RefRow>, ClientError> {
        // The outside id can be data about a person, thus it is not logged.
        tracing::debug!(
            server = %self.server,
            ref_type = %request.ref_type,
            since_ms = ?request.since_ms,
            until_ms = ?request.until_ms,
            limit = ?request.limit,
            "asking the query service for references"
        );
        let answer = self.grpc.clone().list_refs(request).await;
        self.collect(answer).await
    }

    /// The page of all messages of the answer.
    ///
    /// If the stream stops with an error after some messages, the page keeps
    /// them and tells why it is not complete. An error before the first
    /// message is an error of the lookup, as if no stream had started.
    async fn collect<M: AnswerMessage>(
        &self,
        answer: Result<Response<Streaming<M>>, Status>,
    ) -> Result<Page<M::Row>, ClientError> {
        let mut stream = answer.map_err(|status| self.error(status))?.into_inner();
        let mut responses = Vec::new();
        let stopped = loop {
            match stream.message().await {
                Ok(Some(response)) => responses.push(response),
                Ok(None) => break None,
                Err(status) => break Some(self.error(status)),
            }
        };
        match stopped {
            Some(error) if responses.is_empty() => Err(error),
            stopped => {
                let mut page = Page::from_responses(responses);
                if let Some(error) = stopped {
                    page.problems.push(format!(
                        "the answer stopped before its end: {}",
                        error_chain(&error)
                    ));
                }
                Ok(page)
            }
        }
    }

    fn error(&self, status: Status) -> ClientError {
        ClientError::from_status(status, &self.server)
    }
}

#[cfg(test)]
mod tests;
