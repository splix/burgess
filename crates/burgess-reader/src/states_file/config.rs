//! How much of a file's tail a read fetches up front.

use std::num::NonZeroUsize;

/// Settings of a [`StatesFileReader`](super::StatesFileReader).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatesFileConfig {
    /// Bytes fetched from the end of a file before anything else. When the
    /// footer and the page index fit in them, the read needs no further
    /// request to decide which pages to fetch; when they do not, it costs one
    /// or two more requests and still works.
    pub tail_bytes: NonZeroUsize,
}

impl Default for StatesFileConfig {
    /// 256 KiB, provisional.
    ///
    /// The footer length is not fixed by the file layout and has not been
    /// measured on real traffic yet. It grows with the pages: the offset
    /// index has an entry for every page of every column, payload included,
    /// and the writer closes a page at 20,000 rows at the latest, so each
    /// row group of the default million rows adds some tens of KiB of index.
    /// The store takes about as long to send 256 KiB as 64 KiB, while a
    /// second request doubles the wait, so the tail errs on the long side.
    fn default() -> Self {
        StatesFileConfig {
            tail_bytes: NonZeroUsize::new(256 * 1024).expect("not zero"),
        }
    }
}
