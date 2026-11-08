//! How errors read where people see them: in log lines and in the problems
//! a query reports.

use std::error::Error;

/// An error with all its sources, `outer: inner: innermost`: the outermost
/// message alone rarely says what actually went wrong.
///
/// Some errors, such as the object store's, already end their message with
/// their source's. Such a source is not repeated: only a whole message, not
/// a word that happens to end the chain, counts as a repeat.
pub fn error_chain(error: &(dyn Error + 'static)) -> String {
    let mut chain = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !chain.ends_with(&format!(": {cause_text}")) {
            chain.push_str(": ");
            chain.push_str(&cause_text);
        }
        source = cause.source();
    }
    chain
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use super::*;

    #[derive(Debug)]
    struct Failed(&'static str, Option<Box<Failed>>);

    impl fmt::Display for Failed {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    impl Error for Failed {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.1
                .as_deref()
                .map(|cause| cause as &(dyn Error + 'static))
        }
    }

    #[test]
    fn lists_every_source_outermost_first() {
        let error = Failed(
            "reading failed",
            Some(Box::new(Failed(
                "the store failed",
                Some(Box::new(Failed("connection reset", None))),
            ))),
        );
        assert_eq!(
            error_chain(&error),
            "reading failed: the store failed: connection reset"
        );
    }

    #[test]
    fn a_source_the_message_already_ends_with_is_not_repeated() {
        let error = Failed(
            "the store failed",
            Some(Box::new(Failed(
                "object not found: connection reset",
                Some(Box::new(Failed("connection reset", None))),
            ))),
        );
        assert_eq!(
            error_chain(&error),
            "the store failed: object not found: connection reset"
        );
    }

    #[test]
    fn a_source_that_only_ends_with_the_same_word_is_kept() {
        let error = Failed(
            "reading the file failed",
            Some(Box::new(Failed("failed", None))),
        );
        assert_eq!(error_chain(&error), "reading the file failed: failed");
    }

    #[test]
    fn an_error_without_sources_is_its_message() {
        assert_eq!(error_chain(&Failed("gone", None)), "gone");
    }
}
