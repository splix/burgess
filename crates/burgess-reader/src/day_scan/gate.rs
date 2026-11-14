//! What a walk knows of a day before it reads the day's manifest, as a
//! filter of the day tells it.

/// What a walk knows of a day before it reads the day's manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DayGate<P> {
    /// No file of the day can hold a row of the lookup, thus the walk does
    /// not read its manifest.
    Skip,
    /// The walk reads the day, and `P` tells it more of the day's files.
    Read(P),
}
