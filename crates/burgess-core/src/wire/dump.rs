//! Kafka messages kept in a file instead of a topic.
//!
//! `u32 big-endian length | message | u32 big-endian length | message | …`
//!
//! Lets tests produce records without a broker and build tables from them
//! later. Each message is stored exactly as it would travel in Kafka, so
//! reading the file goes through the same decoding as reading the topic. The
//! partition key is not stored: it is the tree id, which every record
//! carries.

use std::io::{self, Read, Write};

/// Writes messages one after another, each after its length.
#[derive(Debug)]
pub struct DumpWriter<W> {
    out: W,
}

impl<W: Write> DumpWriter<W> {
    /// Writes to `out`. Wrap a file in a `BufWriter`: every message is two
    /// writes.
    pub fn new(out: W) -> Self {
        DumpWriter { out }
    }

    /// Appends one message.
    pub fn write(&mut self, message: &[u8]) -> io::Result<()> {
        let length = u32::try_from(message.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("a message of {} bytes does not fit a dump", message.len()),
            )
        })?;
        self.out.write_all(&length.to_be_bytes())?;
        self.out.write_all(message)
    }

    /// Flushes the underlying writer.
    pub fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }
}

/// Reads the messages back, in the order they were written.
///
/// Yields an error for a file that ends inside a message, which is what a
/// writer interrupted mid-message leaves behind. Ending between messages is
/// the normal end.
#[derive(Debug)]
pub struct DumpReader<R> {
    input: R,
}

impl<R: Read> DumpReader<R> {
    /// Reads from `input`. Wrap a file in a `BufReader`: every message is
    /// two reads.
    pub fn new(input: R) -> Self {
        DumpReader { input }
    }

    fn read_message(&mut self) -> io::Result<Option<Vec<u8>>> {
        let mut length = [0; 4];
        if !self.read_or_end(&mut length)? {
            return Ok(None);
        }
        let mut message = vec![0; u32::from_be_bytes(length) as usize];
        self.input.read_exact(&mut message)?;
        Ok(Some(message))
    }

    /// Fills `buffer`, or returns `false` if the input ended before its
    /// first byte. Ending part way is an error.
    fn read_or_end(&mut self, buffer: &mut [u8]) -> io::Result<bool> {
        let mut filled = 0;
        while filled < buffer.len() {
            match self.input.read(&mut buffer[filled..]) {
                Ok(0) if filled == 0 => return Ok(false),
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(read) => filled += read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(true)
    }
}

impl<R: Read> Iterator for DumpReader<R> {
    type Item = io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_message().transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dump(messages: &[&[u8]]) -> Vec<u8> {
        let mut writer = DumpWriter::new(Vec::new());
        for message in messages {
            writer.write(message).unwrap();
        }
        writer.out
    }

    fn read_all(bytes: &[u8]) -> io::Result<Vec<Vec<u8>>> {
        DumpReader::new(bytes).collect()
    }

    #[test]
    fn each_message_follows_its_big_endian_length() {
        assert_eq!(dump(&[b"abc"]), [0, 0, 0, 3, b'a', b'b', b'c']);
    }

    #[test]
    fn messages_read_back_in_order() {
        let bytes = dump(&[b"first", b"", b"third"]);
        assert_eq!(
            read_all(&bytes).unwrap(),
            [b"first".to_vec(), Vec::new(), b"third".to_vec()]
        );
    }

    #[test]
    fn empty_input_has_no_messages() {
        assert!(read_all(&[]).unwrap().is_empty());
    }

    #[test]
    fn input_ending_inside_a_length_is_an_error() {
        let bytes = dump(&[b"whole"]);
        let error = read_all(&[&bytes[..], &[0, 0]].concat()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn input_ending_inside_a_message_is_an_error() {
        let bytes = dump(&[b"whole", b"cut short"]);
        let error = read_all(&bytes[..bytes.len() - 1]).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn messages_before_a_cut_are_still_read() {
        let bytes = dump(&[b"whole", b"cut short"]);
        let mut reader = DumpReader::new(&bytes[..bytes.len() - 1]);
        assert_eq!(reader.next().unwrap().unwrap(), b"whole");
        assert!(reader.next().unwrap().is_err());
    }
}
