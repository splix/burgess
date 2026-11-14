//! The hash that ties a cursor to its request.

use std::hash::Hasher;

use prost::Message;
use twox_hash::XxHash3_64;

/// The methods with paged answers.
///
/// The method is in the hash: the requests of two methods can have the
/// same bytes, for example ListStates and ListRefs with the same two
/// strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PagedMethod {
    /// GetTree.
    GetTree,
    /// GetFlow.
    GetFlow,
    /// ListStates.
    ListStates,
    /// ListRefs.
    ListRefs,
}

impl PagedMethod {
    /// The name in the hash. A change refuses each cursor given before it.
    fn name(self) -> &'static str {
        match self {
            PagedMethod::GetTree => "GetTree",
            PagedMethod::GetFlow => "GetFlow",
            PagedMethod::ListStates => "ListStates",
            PagedMethod::ListRefs => "ListRefs",
        }
    }
}

/// A request of a method with paged answers.
pub trait PagedRequest: Message + Clone {
    /// The method of the request.
    const METHOD: PagedMethod;

    /// The request as the hash sees it.
    ///
    /// Without the options that can change from page to page: the cursor,
    /// and the limit, as a client can ask for a different number of rows on
    /// each page. Values that the service reads the same are made the same,
    /// for example "" and no value, thus a client that changes one for the
    /// other between pages keeps its cursor.
    ///
    /// Each other field stays as sent, thus a field added to a request
    /// later is in the hash without a change here.
    fn hashed_form(&self) -> Self;
}

/// The hash of a request, the same on each reader and each build of the
/// same query API.
///
/// It is of the request as this reader decoded it. Protobuf drops the
/// fields that the reader does not know, thus a reader of an older API can
/// give another hash for the same request.
///
/// It is of the request as sent, not as the service resolved it: the next
/// page sends the same request, but a "now" in it has moved.
///
/// The hash only finds a mistake. It is not a protection: a client can
/// make a valid cursor for each request it sends. Thus a fast hash that is
/// not cryptographic is sufficient.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryHash(u64);

impl QueryHash {
    /// The hash of `request`.
    ///
    /// The protobuf bytes of a request are the same each time, because its
    /// maps are sorted maps.
    pub fn of<R: PagedRequest>(request: &R) -> Self {
        let mut hasher = XxHash3_64::new();
        hasher.write(R::METHOD.name().as_bytes());
        // Ends the name, thus a name and the bytes after it are never the
        // same input as another name.
        hasher.write(&[0]);
        hasher.write(&request.hashed_form().encode_to_vec());
        QueryHash(hasher.finish())
    }
}

impl From<QueryHash> for u64 {
    fn from(hash: QueryHash) -> Self {
        hash.0
    }
}
