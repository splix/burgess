//! Reading an expression into a filter.
//!
//! The grammar has no nesting, thus the parser reads the text one time from
//! left to right, and looks at most one token ahead.

use std::num::NonZeroUsize;
use std::str::FromStr;

use serde_json::Number;

use super::error::{FilterError, FilterErrorKind, ScalarType};
use super::expression::{
    Comparison, Filter, Index, Path, RangeBounds, RangeEnd, Scalar, Step, Test,
};

impl FromStr for Filter {
    type Err = FilterError;

    /// Reads `path cmp scalar` or `path in scalar..scalar`. Space before and
    /// after the expression is ignored, since a shell or a form often adds
    /// it.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut parser = Parser { text, at: 0 };
        parser.spaces();
        let filter = parser.filter()?;
        parser.spaces();
        if parser.at < text.len() {
            return Err(parser.error(FilterErrorKind::TrailingText));
        }
        Ok(filter)
    }
}

/// The text of an expression, and how much of it is read.
struct Parser<'a> {
    text: &'a str,
    /// The byte offset of the next character to read. Each token of the
    /// grammar is ASCII, thus a token never starts inside a character.
    at: usize,
}

impl<'a> Parser<'a> {
    fn filter(&mut self) -> Result<Filter, FilterError> {
        let path = self.path()?;
        let spaced = self.spaces();
        let test = if let Some(comparison) = self.comparison() {
            self.spaces();
            Test::Compare(comparison, self.scalar()?)
        } else if spaced && self.keyword_in() {
            if !self.spaces() {
                return Err(self.error(FilterErrorKind::ExpectedSpace));
            }
            self.range()?
        } else {
            return Err(self.error(FilterErrorKind::ExpectedOperator));
        };
        Ok(Filter { path, test })
    }

    /// Reads `in` if it is a full word, thus `inx` is a missing operator,
    /// and not `in` without a space.
    fn keyword_in(&mut self) -> bool {
        let word = self.text[self.at..].starts_with("in") && !self.peek_at(2).is_some_and(is_key);
        if word {
            self.at += 2;
        }
        word
    }

    fn path(&mut self) -> Result<Path, FilterError> {
        if self.eat(".") {
            if self.peek().is_some_and(|next| is_key(next) || next == b'[') {
                return Err(self.error(FilterErrorKind::StepAfterRoot));
            }
            return Ok(Path::default());
        }
        let head = match self.peek() {
            Some(b'[') => self.subscript()?,
            Some(next) if is_key(next) => Step::Key(self.key()),
            _ => return Err(self.error(FilterErrorKind::ExpectedPath)),
        };
        let mut steps = vec![head];
        loop {
            match self.peek() {
                Some(b'.') => {
                    self.at += 1;
                    if !self.peek().is_some_and(is_key) {
                        return Err(self.error(FilterErrorKind::ExpectedKey));
                    }
                    steps.push(Step::Key(self.key()));
                }
                Some(b'[') => steps.push(self.subscript()?),
                _ => return Ok(Path(steps)),
            }
        }
    }

    /// A key, which must start at the next character.
    fn key(&mut self) -> String {
        self.take_while(is_key).to_string()
    }

    /// `[]`, `[n]`, `[-n]` or `["key"]`, which must start at the next
    /// character.
    fn subscript(&mut self) -> Result<Step, FilterError> {
        let start = self.at;
        self.at += 1;
        let step = match self.peek() {
            Some(b']') => Step::Any,
            Some(b'"') => Step::Key(self.string()?),
            Some(b'-' | b'0'..=b'9') => Step::Index(self.index(start)?),
            _ => return Err(self.error(FilterErrorKind::ExpectedSubscript)),
        };
        if !self.eat("]") {
            return Err(self.error(FilterErrorKind::ExpectedSubscript));
        }
        Ok(step)
    }

    /// `n` or `-n` of the subscript that starts at `start`.
    fn index(&mut self, start: usize) -> Result<Index, FilterError> {
        let from_end = self.eat("-");
        let digits = self.take_while(|byte| byte.is_ascii_digit());
        if digits.is_empty() {
            return Err(self.error(FilterErrorKind::ExpectedSubscript));
        }
        let index: usize = digits
            .parse()
            .map_err(|_| FilterError::at(self.text, start, FilterErrorKind::IndexTooLarge))?;
        if !from_end {
            return Ok(Index::FromStart(index));
        }
        NonZeroUsize::new(index)
            .map(Index::FromEnd)
            .ok_or_else(|| FilterError::at(self.text, start, FilterErrorKind::NegativeZero))
    }

    /// The comparison operator at the next character, if any. A two
    /// character operator is read before the one character operator that it
    /// starts with.
    fn comparison(&mut self) -> Option<Comparison> {
        let (comparison, length) = match (self.peek()?, self.peek_at(1)) {
            (b'<', Some(b'=')) => (Comparison::Le, 2),
            (b'>', Some(b'=')) => (Comparison::Ge, 2),
            (b'!', Some(b'=')) => (Comparison::Ne, 2),
            (b'<', _) => (Comparison::Lt, 1),
            (b'>', _) => (Comparison::Gt, 1),
            (b'=', _) => (Comparison::Eq, 1),
            _ => return None,
        };
        self.at += length;
        Some(comparison)
    }

    /// `scalar .. scalar` or `scalar ..= scalar`.
    fn range(&mut self) -> Result<Test, FilterError> {
        let start_at = self.at;
        let start = self.scalar()?;
        self.spaces();
        let end_kind = if self.eat("..=") {
            RangeEnd::Inclusive
        } else if self.eat("..") {
            RangeEnd::Exclusive
        } else {
            return Err(self.error(FilterErrorKind::ExpectedRange));
        };
        self.spaces();
        let end = self.scalar()?;
        let bounds = match (start, end) {
            (Scalar::Number(start), Scalar::Number(end)) => RangeBounds::Numbers(start, end),
            (Scalar::String(start), Scalar::String(end)) => RangeBounds::Strings(start, end),
            (start, end) => {
                let (start, end) = (ScalarType::from(&start), ScalarType::from(&end));
                let kind = if start != end {
                    FilterErrorKind::MixedRange { start, end }
                } else {
                    FilterErrorKind::UnorderedRange(start)
                };
                return Err(FilterError::at(self.text, start_at, kind));
            }
        };
        if bounds.is_empty(end_kind) {
            return Err(FilterError::at(
                self.text,
                start_at,
                FilterErrorKind::EmptyRange,
            ));
        }
        Ok(Test::Range {
            bounds,
            end: end_kind,
        })
    }

    fn scalar(&mut self) -> Result<Scalar, FilterError> {
        match self.peek() {
            Some(b'"') => Ok(Scalar::String(self.string()?)),
            Some(b'-' | b'0'..=b'9') => Ok(Scalar::Number(self.number()?)),
            _ if self.eat("true") => Ok(Scalar::Bool(true)),
            _ if self.eat("false") => Ok(Scalar::Bool(false)),
            _ if self.eat("null") => Ok(Scalar::Null),
            _ => Err(self.error(FilterErrorKind::ExpectedScalar)),
        }
    }

    /// A JSON number, which must start at the next character.
    ///
    /// The parser finds where the number ends, and JSON decides if it is
    /// valid, thus the rules are those of JSON. A `.` is part of the number
    /// only before a digit, thus `10..20` is a range.
    fn number(&mut self) -> Result<Number, FilterError> {
        let start = self.at;
        self.eat("-");
        self.take_while(|byte| byte.is_ascii_digit());
        if self.peek() == Some(b'.') && self.peek_at(1).is_some_and(|byte| byte.is_ascii_digit()) {
            self.at += 1;
            self.take_while(|byte| byte.is_ascii_digit());
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.at += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.at += 1;
            }
            self.take_while(|byte| byte.is_ascii_digit());
        }
        serde_json::from_str(&self.text[start..self.at]).map_err(|error| {
            FilterError::at(
                self.text,
                start,
                FilterErrorKind::InvalidNumber(reason(&error)),
            )
        })
    }

    /// A JSON string, which must start at the next character.
    fn string(&mut self) -> Result<String, FilterError> {
        let start = self.at;
        let bytes = self.text.as_bytes();
        let mut at = start + 1;
        // `"` and `\` are ASCII, thus they are never a byte inside a
        // character of more bytes.
        loop {
            match bytes.get(at) {
                None => {
                    return Err(FilterError::at(
                        self.text,
                        start,
                        FilterErrorKind::UnclosedString,
                    ));
                }
                Some(b'\\') => at += 2,
                Some(b'"') => break,
                Some(_) => at += 1,
            }
        }
        self.at = at + 1;
        serde_json::from_str(&self.text[start..self.at]).map_err(|error| {
            FilterError::at(
                self.text,
                start,
                FilterErrorKind::InvalidString(reason(&error)),
            )
        })
    }

    /// Reads the spaces at the next character, and tells if there were any.
    fn spaces(&mut self) -> bool {
        !self
            .take_while(|byte| byte.is_ascii_whitespace())
            .is_empty()
    }

    /// Reads `token` if the text continues with it. Keywords are lower case
    /// only, thus the comparison is exact.
    fn eat(&mut self, token: &str) -> bool {
        let found = self.text[self.at..].starts_with(token);
        if found {
            self.at += token.len();
        }
        found
    }

    fn take_while(&mut self, wanted: impl Fn(u8) -> bool) -> &'a str {
        let start = self.at;
        while self.peek().is_some_and(&wanted) {
            self.at += 1;
        }
        &self.text[start..self.at]
    }

    fn peek(&self) -> Option<u8> {
        self.peek_at(0)
    }

    fn peek_at(&self, ahead: usize) -> Option<u8> {
        self.text.as_bytes().get(self.at + ahead).copied()
    }

    fn error(&self, kind: FilterErrorKind) -> FilterError {
        FilterError::at(self.text, self.at, kind)
    }
}

impl FilterError {
    /// The error `kind` at the byte offset `at` of `text`.
    fn at(text: &str, at: usize, kind: FilterErrorKind) -> Self {
        let column = text
            .get(..at)
            .map_or(text.chars().count(), |before| before.chars().count())
            + 1;
        FilterError { kind, column }
    }
}

/// Whether `byte` can be in a key that is not in quotes.
fn is_key(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
}

/// Why JSON refused a token, without the position in the token that JSON
/// adds, since the filter error tells its own position.
fn reason(error: &serde_json::Error) -> String {
    let message = error.to_string();
    match message.rfind(" at line ") {
        Some(position) => message[..position].to_string(),
        None => message,
    }
}
