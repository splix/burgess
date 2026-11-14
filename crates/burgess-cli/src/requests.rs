//! The gRPC requests that a command line asks for. Each request is made
//! here, before a client exists, thus a command line that cannot make one
//! is refused before anything else can fail.

use std::collections::BTreeMap;

use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::{
    GetFlowRequest, GetTreeRequest, ListRefsRequest, ListStatesRequest, Order,
};
use burgess_core::flow::{FlowId, TreeId};

use crate::args::{DaysArgs, StatesArgs, TsOrder, WindowArgs};

/// Why the command line cannot make a request.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArgsError {
    /// A label is given with two values. The service would take one of
    /// them, and the investigator could not tell which.
    #[error("--meta {key} is given with two values")]
    LabelTwice {
        /// The key of the label.
        key: String,
    },
}

/// A request of an answer in pages.
pub trait PagedRequest: Clone {
    /// The request for the page of at most `limit` rows after `cursor`, or
    /// for the first page. All pages of an answer have the same other
    /// fields, or the service refuses the cursor.
    fn page(self, limit: Option<u32>, cursor: Option<String>) -> Self;
}

/// The GetTree request for all rows of `tree`, without its page.
pub fn tree_request(tree: TreeId, days: DaysArgs) -> GetTreeRequest {
    GetTreeRequest {
        tree: Some(Tree::TreeId(tree.to_string())),
        until_ms: days.until_ms(),
        limit: None,
        order: Order::Unspecified as i32,
        cursor: None,
    }
}

/// The GetFlow request for all rows of `flow` and of the flows below it,
/// without its page.
pub fn flow_request(flow: FlowId, days: DaysArgs) -> GetFlowRequest {
    GetFlowRequest {
        flow_id: flow.to_string(),
        until_ms: days.until_ms(),
        limit: None,
        order: Order::Unspecified as i32,
        cursor: None,
    }
}

/// The ListStates request for the states of `service` and `category`,
/// without its page.
pub fn states_request(
    service: String,
    category: String,
    states: StatesArgs,
    window: WindowArgs,
) -> Result<ListStatesRequest, ArgsError> {
    let mut meta = BTreeMap::new();
    for (key, value) in states.meta {
        if let Some(earlier) = meta.insert(key.clone(), value.clone())
            && earlier != value
        {
            return Err(ArgsError::LabelTwice { key });
        }
    }
    let order = match states.order {
        None => Order::Unspecified,
        Some(TsOrder::Asc) => Order::TsAscending,
        Some(TsOrder::Desc) => Order::TsDescending,
    };
    Ok(ListStatesRequest {
        service,
        category,
        r#where: states.filter,
        meta,
        since_ms: window.since_ms(),
        until_ms: window.until_ms(),
        limit: None,
        order: order as i32,
        cursor: None,
    })
}

/// The ListRefs request for the references to `domain_id`, without its
/// page.
pub fn refs_request(
    ref_type: String,
    domain_id: String,
    service: Option<String>,
    category: Option<String>,
    window: WindowArgs,
) -> ListRefsRequest {
    ListRefsRequest {
        ref_type,
        domain_id,
        service,
        category,
        since_ms: window.since_ms(),
        until_ms: window.until_ms(),
        limit: None,
        order: Order::Unspecified as i32,
        cursor: None,
    }
}

impl DaysArgs {
    fn until_ms(&self) -> Option<i64> {
        self.until.map(|until| until.as_millisecond())
    }
}

impl WindowArgs {
    fn since_ms(&self) -> Option<i64> {
        self.since.map(|since| since.as_millisecond())
    }

    fn until_ms(&self) -> Option<i64> {
        self.until.map(|until| until.as_millisecond())
    }
}

impl PagedRequest for GetTreeRequest {
    fn page(self, limit: Option<u32>, cursor: Option<String>) -> Self {
        GetTreeRequest {
            limit,
            cursor,
            ..self
        }
    }
}

impl PagedRequest for GetFlowRequest {
    fn page(self, limit: Option<u32>, cursor: Option<String>) -> Self {
        GetFlowRequest {
            limit,
            cursor,
            ..self
        }
    }
}

impl PagedRequest for ListStatesRequest {
    fn page(self, limit: Option<u32>, cursor: Option<String>) -> Self {
        ListStatesRequest {
            limit,
            cursor,
            ..self
        }
    }
}

impl PagedRequest for ListRefsRequest {
    fn page(self, limit: Option<u32>, cursor: Option<String>) -> Self {
        ListRefsRequest {
            limit,
            cursor,
            ..self
        }
    }
}
