//! How the producer compresses its batches.

/// The codec Kafka batches are compressed with.
///
/// Records are not compressed one by one: state payloads are small and
/// similar, so they compress far better together. Kafka compresses whole
/// batches, and the writer reads every codec listed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// Batches are sent as they are.
    None,
    /// gzip: small output, but slow for the application's CPU.
    Gzip,
    /// Snappy: fast, compresses less.
    Snappy,
    /// LZ4: fast, compresses less.
    Lz4,
    /// Zstandard: compresses about as well as gzip, at a fraction of the
    /// CPU. The default, because JSON states compress well and the
    /// application should not pay much for it.
    #[default]
    Zstd,
}

impl Compression {
    /// The value of librdkafka's `compression.type`.
    pub(super) fn property(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Gzip => "gzip",
            Compression::Snappy => "snappy",
            Compression::Lz4 => "lz4",
            Compression::Zstd => "zstd",
        }
    }
}
