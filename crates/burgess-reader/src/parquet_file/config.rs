//! How much of a file's tail a read fetches up front.

use std::num::NonZeroUsize;

/// How the readers of data files read a file.
///
/// The first request of a read gets the tail of the file. When the footer
/// and the page index fit in it, the read needs no further request to
/// decide which pages to fetch. When they do not fit, the read costs one or
/// two more requests and still works.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileReadConfig {
    /// Bytes of the tail of a states file.
    pub tail_bytes: NonZeroUsize,
    /// Bytes of the tail of a refs file.
    pub refs_tail_bytes: NonZeroUsize,
}

impl Default for FileReadConfig {
    /// 256 KiB for states files and 64 KiB for refs files, provisional.
    ///
    /// The footer length is not fixed by the file layout and has not been
    /// measured on real traffic yet. It grows with the pages: the offset
    /// index has an entry for every page of every column, and the writer
    /// closes a page at 20,000 rows at the latest. The store takes about as
    /// long to send 256 KiB as 64 KiB, while a second request doubles the
    /// wait, so the states tail errs on the long side.
    ///
    /// A refs file has fewer and smaller columns, thus a smaller footer. A
    /// refs lookup also opens every refs file of the days it walks, thus a
    /// large tail costs more there.
    fn default() -> Self {
        FileReadConfig {
            tail_bytes: NonZeroUsize::new(256 * 1024).expect("not zero"),
            refs_tail_bytes: NonZeroUsize::new(64 * 1024).expect("not zero"),
        }
    }
}
