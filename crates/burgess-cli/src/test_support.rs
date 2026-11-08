//! A gRPC query service in the test process, with fixed answers.

use std::sync::{Arc, Mutex};

use burgess_api::grpc::query_service_server::{QueryService, QueryServiceServer};
use burgess_api::grpc::{
    GetFlowRequest, GetFlowResponse, GetTreeRequest, GetTreeResponse, ListRefsRequest,
    ListRefsResponse, ListStatesRequest, ListStatesResponse, TreeItem, TreeResponse, TreeRow,
};
use tokio::net::TcpListener;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

use crate::server::ServerAddress;

/// How the service answers each lookup.
#[derive(Debug, Clone)]
pub(crate) enum Answer {
    /// The service refuses the request.
    Refuse(Status),
    /// The service sends these messages, and an error in the list stops
    /// the stream there.
    Stream(Vec<Result<TreeItem, Status>>),
}

impl Answer {
    /// An answer of these items, without an error.
    pub(crate) fn items(items: Vec<TreeItem>) -> Self {
        Answer::Stream(items.into_iter().map(Ok).collect())
    }

    /// An answer of these rows only.
    pub(crate) fn rows(rows: Vec<TreeRow>) -> Self {
        Answer::items(rows.into_iter().map(TreeItem::Row).collect())
    }
}

/// A request that the service received.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Received {
    Tree(GetTreeRequest),
    Flow(GetFlowRequest),
}

/// A running service, which stops when the test ends.
pub(crate) struct MockService {
    /// Its address.
    pub(crate) server: ServerAddress,
    received: Arc<Mutex<Vec<Received>>>,
}

impl MockService {
    /// A service that answers each lookup with `answer`.
    pub(crate) async fn start(answer: Answer) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let received = Arc::new(Mutex::new(Vec::new()));
        let service = Fixed {
            answer,
            received: received.clone(),
        };
        tokio::spawn(
            Server::builder()
                .add_service(QueryServiceServer::new(service))
                .serve_with_incoming(TcpIncoming::from(listener)),
        );
        MockService { server, received }
    }

    /// The requests received, in order.
    pub(crate) fn received(&self) -> Vec<Received> {
        self.received.lock().unwrap().clone()
    }
}

struct Fixed {
    answer: Answer,
    received: Arc<Mutex<Vec<Received>>>,
}

type Messages<R> = tokio_stream::Iter<std::vec::IntoIter<Result<R, Status>>>;

impl Fixed {
    fn answer<R: TreeResponse>(&self, received: Received) -> Result<Response<Messages<R>>, Status> {
        self.received.lock().unwrap().push(received);
        match &self.answer {
            Answer::Refuse(status) => Err(status.clone()),
            Answer::Stream(items) => {
                let messages: Vec<_> = items
                    .iter()
                    .map(|item| item.clone().map(R::from_item))
                    .collect();
                Ok(Response::new(tokio_stream::iter(messages)))
            }
        }
    }
}

#[tonic::async_trait]
impl QueryService for Fixed {
    type GetTreeStream = Messages<GetTreeResponse>;
    type GetFlowStream = Messages<GetFlowResponse>;
    type ListStatesStream = Messages<ListStatesResponse>;
    type ListRefsStream = Messages<ListRefsResponse>;

    async fn get_tree(
        &self,
        request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        self.answer(Received::Tree(request.into_inner()))
    }

    async fn get_flow(
        &self,
        request: Request<GetFlowRequest>,
    ) -> Result<Response<Self::GetFlowStream>, Status> {
        self.answer(Received::Flow(request.into_inner()))
    }

    async fn list_states(
        &self,
        _request: Request<ListStatesRequest>,
    ) -> Result<Response<Self::ListStatesStream>, Status> {
        Err(Status::unimplemented("not in the mock"))
    }

    async fn list_refs(
        &self,
        _request: Request<ListRefsRequest>,
    ) -> Result<Response<Self::ListRefsStream>, Status> {
        Err(Status::unimplemented("not in the mock"))
    }
}
