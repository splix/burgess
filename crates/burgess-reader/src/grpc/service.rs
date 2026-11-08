//! The methods of the gRPC service.

use std::sync::Arc;

use burgess_api::grpc::query_service_server::QueryService;
use burgess_api::grpc::{
    GetFlowRequest, GetFlowResponse, GetTreeRequest, GetTreeResponse, ListRefsRequest,
    ListRefsResponse, ListStatesRequest, ListStatesResponse,
};
use burgess_core::report::error_chain;
use futures::stream;
use tonic::{Request, Response, Status};

use crate::queries::{Queries, RequestError};

/// The messages of one answer. The lookup reads all rows before it cuts the
/// page, thus the full answer is known before the first message.
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
        _request: Request<ListStatesRequest>,
    ) -> Result<Response<Self::ListStatesStream>, Status> {
        Err(Status::unimplemented("ListStates is not served yet"))
    }

    async fn list_refs(
        &self,
        _request: Request<ListRefsRequest>,
    ) -> Result<Response<Self::ListRefsStream>, Status> {
        Err(Status::unimplemented("ListRefs is not served yet"))
    }
}

/// Each request error is a fault of the request.
fn answer<R>(responses: Result<Vec<R>, RequestError>) -> Result<Response<Answer<R>>, Status> {
    let responses = responses.map_err(|error| Status::invalid_argument(error_chain(&error)))?;
    let messages: Vec<_> = responses.into_iter().map(Ok).collect();
    Ok(Response::new(stream::iter(messages)))
}
