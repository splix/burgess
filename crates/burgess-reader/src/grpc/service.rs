//! The methods of the gRPC service.

use std::str::FromStr;
use std::sync::Arc;

use burgess_api::Page;
use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::query_service_server::QueryService;
use burgess_api::grpc::{
    GetFlowRequest, GetFlowResponse, GetTreeRequest, GetTreeResponse, ListRefsRequest,
    ListRefsResponse, ListStatesRequest, ListStatesResponse, TreeResponse,
};
use burgess_core::flow::{FlowId, IdParseError, TreeId};
use futures::stream;
use tonic::{Request, Response, Status};

use super::error::RequestError;
use super::options::PageOptions;
use crate::queries::Queries;

/// The messages of one answer. The lookup reads all rows before it cuts the
/// page, thus the full answer is known before the first message.
pub type Answer<T> = stream::Iter<std::vec::IntoIter<Result<T, Status>>>;

/// The gRPC service. It answers from the same queries as REST.
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
        let request = request.into_inner();
        let tree = match &request.tree {
            Some(Tree::TreeId(tree)) => parse::<TreeId>(tree)?,
            Some(Tree::FlowId(flow)) => parse::<FlowId>(flow)?.tree(),
            None => return Err(RequestError::NoTree.into()),
        };
        let page = PageOptions::from(&request).check(&self.queries.limits)?;
        Ok(answer(self.queries.tree(tree, page).await))
    }

    async fn get_flow(
        &self,
        request: Request<GetFlowRequest>,
    ) -> Result<Response<Self::GetFlowStream>, Status> {
        let request = request.into_inner();
        let flow = parse::<FlowId>(&request.flow_id)?;
        let page = PageOptions::from(&request).check(&self.queries.limits)?;
        Ok(answer(self.queries.flow(flow, page).await))
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

fn parse<T: FromStr<Err = IdParseError>>(id: &str) -> Result<T, RequestError> {
    Ok(id.parse()?)
}

fn answer<R: TreeResponse>(page: Page) -> Response<Answer<R>> {
    let responses: Vec<_> = page.into_responses().map(Ok).collect();
    Response::new(stream::iter(responses))
}
