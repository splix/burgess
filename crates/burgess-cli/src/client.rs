//! Asking the query service for a page, over gRPC.

mod error;

pub use error::{ClientError, StatusCause};

use std::fmt;
use std::time::Duration;

use burgess_api::Page;
use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::query_service_client::QueryServiceClient;
use burgess_api::grpc::{GetFlowRequest, GetTreeRequest, TreeResponse};
use burgess_core::flow::{FlowId, TreeId};
use burgess_core::report::error_chain;
use tonic::codec::Streaming;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Response, Status};

use crate::args::PageArgs;
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

/// What to look up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookup {
    /// All rows of a tree.
    Tree(TreeId),
    /// All rows of a flow and of the flows below it.
    Flow(FlowId),
}

impl fmt::Display for Lookup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Lookup::Tree(tree) => write!(f, "tree {tree}"),
            Lookup::Flow(flow) => write!(f, "flow {flow}"),
        }
    }
}

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

    /// The page that the service answers `lookup` with.
    pub async fn page(&self, lookup: Lookup, page: PageArgs) -> Result<Page, ClientError> {
        let mut grpc = self.grpc.clone();
        let until_ms = page.until.map(|until| until.as_millisecond());
        let limit = page.limit.map(|limit| limit.get());
        tracing::debug!(server = %self.server, %lookup, ?until_ms, ?limit, "asking the query service");
        match lookup {
            Lookup::Tree(tree) => {
                let request = GetTreeRequest {
                    tree: Some(Tree::TreeId(tree.to_string())),
                    until_ms,
                    limit,
                    ..Default::default()
                };
                self.collect(grpc.get_tree(request).await).await
            }
            Lookup::Flow(flow) => {
                let request = GetFlowRequest {
                    flow_id: flow.to_string(),
                    until_ms,
                    limit,
                    ..Default::default()
                };
                self.collect(grpc.get_flow(request).await).await
            }
        }
    }

    /// The page of all messages of the answer.
    ///
    /// If the stream stops with an error after some messages, the page keeps
    /// them and tells why it is not complete. An error before the first
    /// message is an error of the lookup, as if no stream had started.
    async fn collect<R: TreeResponse>(
        &self,
        answer: Result<Response<Streaming<R>>, Status>,
    ) -> Result<Page, ClientError> {
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
