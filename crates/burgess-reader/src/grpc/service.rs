//! The methods of the gRPC service.

use std::sync::Arc;

use burgess_api::grpc::WHERE_COLUMN;
use burgess_api::grpc::query_service_server::QueryService;
use burgess_api::grpc::{
    GetFlowRequest, GetFlowResponse, GetTreeRequest, GetTreeResponse, ListRefsRequest,
    ListRefsResponse, ListStatesRequest, ListStatesResponse,
};
use burgess_core::report::error_chain;
use futures::stream;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::queries::{Queries, RequestError};

/// The messages of one answer. The lookup reads the rows of the page before
/// it cuts it, thus the full answer is known before the first message.
pub type Answer<T> = stream::Iter<std::vec::IntoIter<Result<T, Status>>>;

/// The gRPC service. It sends the answers of the queries as they are.
pub struct QueryApi {
    queries: Arc<Queries>,
}

impl QueryApi {
    /// A service that answers from `queries`.
    pub fn new(queries: Arc<Queries>) -> Self {
        QueryApi { queries }
    }
}

#[tonic::async_trait]
impl QueryService for QueryApi {
    type GetTreeStream = Answer<GetTreeResponse>;
    type GetFlowStream = Answer<GetFlowResponse>;
    type ListStatesStream = Answer<ListStatesResponse>;
    type ListRefsStream = Answer<ListRefsResponse>;

    async fn get_tree(
        &self,
        request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        answer(self.queries.get_tree(request.into_inner()).await)
    }

    async fn get_flow(
        &self,
        request: Request<GetFlowRequest>,
    ) -> Result<Response<Self::GetFlowStream>, Status> {
        answer(self.queries.get_flow(request.into_inner()).await)
    }

    async fn list_states(
        &self,
        request: Request<ListStatesRequest>,
    ) -> Result<Response<Self::ListStatesStream>, Status> {
        answer(self.queries.list_states(request.into_inner()).await)
    }

    async fn list_refs(
        &self,
        request: Request<ListRefsRequest>,
    ) -> Result<Response<Self::ListRefsStream>, Status> {
        answer(self.queries.list_refs(request.into_inner()).await)
    }
}

/// A request error refuses the request before the first message.
fn answer<R>(responses: Result<Vec<R>, RequestError>) -> Result<Response<Answer<R>>, Status> {
    let responses = responses.map_err(|error| refusal(&error))?;
    let messages: Vec<_> = responses.into_iter().map(Ok).collect();
    Ok(Response::new(stream::iter(messages)))
}

/// The status of a refused request.
///
/// An expired cursor is not a fault of how the request is written, and a
/// retry gives the same answer. Thus it has a status of its own, and not
/// UNAVAILABLE, which clients retry and show as a service that is down.
fn refusal(error: &RequestError) -> Status {
    if let RequestError::Expired = error {
        return Status::failed_precondition(error_chain(error));
    }
    let mut status = Status::invalid_argument(error_chain(error));
    if let RequestError::Where(filter) = error {
        status
            .metadata_mut()
            .insert(WHERE_COLUMN, MetadataValue::from(filter.column));
    }
    status
}
