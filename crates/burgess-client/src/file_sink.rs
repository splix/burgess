//! A sink that keeps messages in a file.

use std::fs::File;
use std::io::{self, BufWriter};
use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use burgess_core::wire::DumpWriter;
use jiff::Timestamp;

use crate::message::Message;
use crate::sink::Sink;
use crate::throttle::{Throttle, Verdict};

/// Writes every message to a file, each after its length, in the format
/// [`DumpReader`] reads.
///
/// For tests and local runs: an application emits through it without a
/// Kafka broker, and the tables are built from the file afterwards. Messages
/// are buffered, so call [`Emitter::flush`] before reading the file.
///
/// [`DumpReader`]: burgess_core::wire::DumpReader
/// [`Emitter::flush`]: crate::Emitter::flush
#[derive(Debug)]
pub struct FileSink {
    writer: Mutex<DumpWriter<BufWriter<File>>>,
    throttle: Throttle,
}

impl FileSink {
    /// Creates the file, replacing one already there.
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::create(path)?;
        Ok(FileSink {
            writer: Mutex::new(DumpWriter::new(BufWriter::new(file))),
            throttle: Throttle::default(),
        })
    }

    /// A thread that panicked while holding the lock may have left a message
    /// half written; the file is a test aid, and reading it reports the cut,
    /// so writing carries on rather than failing every later message.
    fn lock(&self) -> MutexGuard<'_, DumpWriter<BufWriter<File>>> {
        self.writer.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn log_failure(&self, error: &io::Error) {
        match self.throttle.admit(Timestamp::now()) {
            Verdict::Warn { suppressed } => {
                tracing::warn!(suppressed, "cannot write records to the file: {error}")
            }
            Verdict::Quiet => tracing::debug!("cannot write records to the file: {error}"),
        }
    }
}

impl Sink for FileSink {
    fn send(&self, message: Message) {
        if let Err(error) = self.lock().write(message.value()) {
            self.log_failure(&error);
        }
    }

    /// Writes out what is buffered. A local file does not need the timeout.
    fn flush(&self, _timeout: Duration) {
        if let Err(error) = self.lock().flush() {
            self.log_failure(&error);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::BufReader;

    use burgess_core::flow::FlowId;
    use burgess_core::wire::{Body, DumpReader, Record};
    use serde_json::json;

    use super::*;
    use crate::emitter::Emitter;

    const PATIENCE: Duration = Duration::from_secs(10);

    fn read_records(path: &Path) -> Vec<Record> {
        DumpReader::new(BufReader::new(File::open(path).unwrap()))
            .map(|message| Record::decode(&message.unwrap()).unwrap())
            .collect()
    }

    #[test]
    fn records_read_back_from_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("records.bin");
        let emitter = Emitter::new("checkout", FileSink::create(&path).unwrap());
        let root = emitter.start();
        let child = emitter.split(root, "fan-out");
        emitter.capture(child, "payment", json!({"amount": 42}));
        emitter.reference(root, "order_id", "ORD-1");
        emitter.flush(PATIENCE);

        let kinds: Vec<(FlowId, &str)> = read_records(&path)
            .iter()
            .map(|record| {
                let kind = match record.body {
                    Body::Split(_) => "split",
                    Body::State(_) => "state",
                    Body::Reference(_) => "reference",
                };
                (record.flow_id, kind)
            })
            .collect();
        assert_eq!(
            kinds,
            [(root, "split"), (child, "state"), (root, "reference")]
        );
    }

    #[test]
    fn nothing_emitted_leaves_an_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("records.bin");
        let emitter = Emitter::new("checkout", FileSink::create(&path).unwrap());
        emitter.flush(PATIENCE);
        assert!(read_records(&path).is_empty());
    }

    #[test]
    fn replaces_an_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("records.bin");
        fs::write(&path, b"left over from an earlier run").unwrap();
        let sink = FileSink::create(&path).unwrap();
        sink.flush(PATIENCE);
        assert!(read_records(&path).is_empty());
    }

    #[test]
    fn dropping_the_emitter_writes_out_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("records.bin");
        let emitter = Emitter::new("checkout", FileSink::create(&path).unwrap());
        emitter.capture(emitter.start(), "payment", json!({}));
        drop(emitter);
        assert_eq!(read_records(&path).len(), 1);
    }
}
