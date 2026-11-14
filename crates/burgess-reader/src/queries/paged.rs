//! The requests as the hash of a cursor sees them.
//!
//! Here, next to the checks of the requests, because the hash must read
//! two values the same exactly when the checks do.

use burgess_api::grpc::{GetFlowRequest, GetTreeRequest, ListRefsRequest, ListStatesRequest};

use super::{non_blank, non_empty};
use crate::cursor::{PagedMethod, PagedRequest};
use crate::refs_file::RefKey;

impl PagedRequest for GetTreeRequest {
    const METHOD: PagedMethod = PagedMethod::GetTree;

    fn hashed_form(&self) -> Self {
        GetTreeRequest {
            limit: None,
            cursor: None,
            ..self.clone()
        }
    }
}

impl PagedRequest for GetFlowRequest {
    const METHOD: PagedMethod = PagedMethod::GetFlow;

    fn hashed_form(&self) -> Self {
        GetFlowRequest {
            limit: None,
            cursor: None,
            ..self.clone()
        }
    }
}

impl PagedRequest for ListStatesRequest {
    const METHOD: PagedMethod = PagedMethod::ListStates;

    fn hashed_form(&self) -> Self {
        ListStatesRequest {
            r#where: non_blank(self.r#where.clone()),
            limit: None,
            cursor: None,
            ..self.clone()
        }
    }
}

impl PagedRequest for ListRefsRequest {
    const METHOD: PagedMethod = PagedMethod::ListRefs;

    /// The reference as the key reads it, thus also with the spaces that
    /// the key trims. A request whose key is refused has no page, and is
    /// hashed as sent.
    fn hashed_form(&self) -> Self {
        let (ref_type, domain_id) = match RefKey::new(self.ref_type.clone(), self.domain_id.clone())
        {
            Ok(key) => (key.ref_type().to_owned(), key.domain_id().to_owned()),
            Err(_) => (self.ref_type.clone(), self.domain_id.clone()),
        };
        ListRefsRequest {
            ref_type,
            domain_id,
            service: non_empty(self.service.clone()),
            category: non_empty(self.category.clone()),
            limit: None,
            cursor: None,
            ..self.clone()
        }
    }
}
