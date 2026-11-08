//! The ListRefs lookup.

use std::mem;

use burgess_api::grpc::{ListRefsRequest, ListRefsResponse};

use super::error::{RequestError, TimeField};
use super::options::{PageOptions, time};
use super::{Queries, log_problems, non_empty};
use crate::refs_file::RefKey;
use crate::refs_lookup::RefsQuery;

impl Queries {
    /// The messages of the answer to a ListRefs request.
    pub async fn list_refs(
        &self,
        mut request: ListRefsRequest,
    ) -> Result<Vec<ListRefsResponse>, RequestError> {
        let key = RefKey::new(
            mem::take(&mut request.ref_type),
            mem::take(&mut request.domain_id),
        )?;
        let page = PageOptions::from(&request).check(&self.limits)?;
        let since = request
            .since_ms
            .map(|since_ms| time(TimeField::Since, since_ms))
            .transpose()?;
        if let (Some(since), Some(until)) = (since, page.until)
            && since > until
        {
            return Err(RequestError::SinceAfterUntil { since, until });
        }
        let query = RefsQuery {
            key,
            service: non_empty(request.service),
            category: non_empty(request.category),
            since,
            until: page.until,
        };
        // One row more than the page holds tells that the page is cut.
        let found = self.refs.refs(&query, page.limit.saturating_add(1)).await?;
        // The outside id can be data about a person, thus it is not logged.
        log_problems(
            &found.problems,
            format_args!("references of type {}", query.key.ref_type()),
        );
        Ok(found.into_responses(page.limit))
    }
}
