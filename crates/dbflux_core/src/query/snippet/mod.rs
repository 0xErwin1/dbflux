//! Parser for snippet bodies.
//!
//! Syntax: `$N`, `${N}`, `${N:default}` (the default is literal text and may
//! be empty) and `$0` for the final cursor. `\$`, `\}` and `\\` escape the
//! character after the backslash; any other backslash is literal, and so is a
//! `$` that starts neither a placeholder nor a variable. A repeated index
//! mirrors: every occurrence shares the text, and the first occurrence that
//! has a default supplies it. Nested placeholders, `${N|a,b|}` choices and
//! `$NAME` variables are rejected with a positioned error.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;

/// One tabstop of an expanded snippet: every occurrence of an index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tabstop {
    pub index: u32,
    /// Byte ranges in the expanded text, in text order. The first one is the
    /// field the cursor visits; the others mirror it.
    pub ranges: Vec<Range<usize>>,
}

/// A parsed snippet body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSnippet {
    text: String,
    tabstops: Vec<Tabstop>,
}

impl ParsedSnippet {
    /// The body with placeholders replaced by their default text.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Tabstops in navigation order: 1, 2, …, then the final stop (`$0`).
    /// When the body has no `$0`, the end of the text is the final stop.
    pub fn tabstops(&self) -> &[Tabstop] {
        &self.tabstops
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnippetParseErrorKind {
    UnclosedPlaceholder,
    NestedPlaceholder,
    UnsupportedChoice,
    UnsupportedVariable,
    InvalidPlaceholder,
}

impl SnippetParseErrorKind {
    fn label(self) -> &'static str {
        match self {
            Self::UnclosedPlaceholder => "Unclosed placeholder",
            Self::NestedPlaceholder => "Nested placeholder",
            Self::UnsupportedChoice => "Choices are not supported",
            Self::UnsupportedVariable => "Variables are not supported",
            Self::InvalidPlaceholder => "Invalid placeholder",
        }
    }
}

/// A rejected body. `line` and `column` are 1-based and count characters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnippetParseError {
    pub kind: SnippetParseErrorKind,
    pub line: usize,
    pub column: usize,
    /// The offending source, from the error position to the end of its line.
    pub excerpt: String,
}

impl fmt::Display for SnippetParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at line {}, column {}: {}",
            self.kind.label(),
            self.line,
            self.column,
            self.excerpt
        )
    }
}

impl std::error::Error for SnippetParseError {}

enum Piece {
    Text(String),
    Stop { index: u32, default: Option<String> },
}

pub fn parse_snippet(body: &str) -> Result<ParsedSnippet, SnippetParseError> {
    let pieces = Scanner::new(body).scan()?;
    Ok(expand(pieces))
}

fn expand(pieces: Vec<Piece>) -> ParsedSnippet {
    let mut defaults: HashMap<u32, String> = HashMap::new();
    for piece in &pieces {
        if let Piece::Stop {
            index,
            default: Some(default),
        } = piece
        {
            defaults.entry(*index).or_insert_with(|| default.clone());
        }
    }

    let mut text = String::new();
    let mut tabstops: Vec<Tabstop> = Vec::new();
    for piece in pieces {
        match piece {
            Piece::Text(literal) => text.push_str(&literal),
            Piece::Stop { index, .. } => {
                let start = text.len();
                if let Some(default) = defaults.get(&index) {
                    text.push_str(default);
                }
                let range = start..text.len();
                match tabstops.iter_mut().find(|stop| stop.index == index) {
                    Some(stop) => stop.ranges.push(range),
                    None => tabstops.push(Tabstop {
                        index,
                        ranges: vec![range],
                    }),
                }
            }
        }
    }

    // Navigation order: 1, 2, … then 0.
    tabstops.sort_by_key(|stop| (stop.index == 0, stop.index));
    if tabstops.last().is_none_or(|stop| stop.index != 0) {
        let end = text.len();
        tabstops.push(Tabstop {
            index: 0,
            ranges: std::iter::once(end..end).collect(),
        });
    }

    ParsedSnippet { text, tabstops }
}

struct Scanner<'a> {
    source: &'a str,
    chars: Vec<char>,
    position: usize,
}

impl<'a> Scanner<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            source,
            chars: source.chars().collect(),
            position: 0,
        }
    }

    fn scan(mut self) -> Result<Vec<Piece>, SnippetParseError> {
        let mut pieces = Vec::new();
        let mut literal = String::new();

        while let Some(&current) = self.chars.get(self.position) {
            match current {
                '\\' => literal.push(self.escaped()),
                '$' => match self.dollar()? {
                    Some(stop) => {
                        if !literal.is_empty() {
                            pieces.push(Piece::Text(std::mem::take(&mut literal)));
                        }
                        pieces.push(stop);
                    }
                    None => {
                        literal.push('$');
                        self.position += 1;
                    }
                },
                other => {
                    literal.push(other);
                    self.position += 1;
                }
            }
        }

        if !literal.is_empty() {
            pieces.push(Piece::Text(literal));
        }
        Ok(pieces)
    }

    /// Consumes a backslash: an escape yields the escaped character, any
    /// other backslash stays literal.
    fn escaped(&mut self) -> char {
        match self.chars.get(self.position + 1) {
            Some(&next) if matches!(next, '$' | '}' | '\\') => {
                self.position += 2;
                next
            }
            _ => {
                self.position += 1;
                '\\'
            }
        }
    }

    /// Parses a placeholder at a `$`. `None` means the `$` is literal.
    fn dollar(&mut self) -> Result<Option<Piece>, SnippetParseError> {
        let start = self.position;
        match self.chars.get(start + 1).copied() {
            Some(digit) if digit.is_ascii_digit() => {
                self.position += 1;
                let index = self.index(start)?;
                Ok(Some(Piece::Stop {
                    index,
                    default: None,
                }))
            }
            Some('{') => {
                self.position += 2;
                self.braced(start).map(Some)
            }
            Some(first) if is_name_start(first) => {
                Err(self.error(SnippetParseErrorKind::UnsupportedVariable, start))
            }
            _ => Ok(None),
        }
    }

    fn braced(&mut self, start: usize) -> Result<Piece, SnippetParseError> {
        match self.chars.get(self.position).copied() {
            None => Err(self.error(SnippetParseErrorKind::UnclosedPlaceholder, start)),
            Some(digit) if digit.is_ascii_digit() => {
                let index = self.index(start)?;
                match self.chars.get(self.position).copied() {
                    Some('}') => {
                        self.position += 1;
                        Ok(Piece::Stop {
                            index,
                            default: None,
                        })
                    }
                    Some(':') => {
                        self.position += 1;
                        let default = self.default_text(start)?;
                        Ok(Piece::Stop {
                            index,
                            default: Some(default),
                        })
                    }
                    Some('|') => Err(self.error(SnippetParseErrorKind::UnsupportedChoice, start)),
                    None => Err(self.error(SnippetParseErrorKind::UnclosedPlaceholder, start)),
                    Some(_) => Err(self.error(SnippetParseErrorKind::InvalidPlaceholder, start)),
                }
            }
            Some(first) if is_name_start(first) => {
                Err(self.error(SnippetParseErrorKind::UnsupportedVariable, start))
            }
            Some(_) => Err(self.error(SnippetParseErrorKind::InvalidPlaceholder, start)),
        }
    }

    /// Reads the digits at the cursor as an index.
    fn index(&mut self, start: usize) -> Result<u32, SnippetParseError> {
        let mut value: u32 = 0;
        while let Some(digit) = self
            .chars
            .get(self.position)
            .and_then(|character| character.to_digit(10))
        {
            value = value
                .checked_mul(10)
                .and_then(|scaled| scaled.checked_add(digit))
                .ok_or_else(|| self.error(SnippetParseErrorKind::InvalidPlaceholder, start))?;
            self.position += 1;
        }
        Ok(value)
    }

    /// Reads default text up to the closing brace.
    fn default_text(&mut self, start: usize) -> Result<String, SnippetParseError> {
        let mut text = String::new();
        loop {
            match self.chars.get(self.position).copied() {
                None => return Err(self.error(SnippetParseErrorKind::UnclosedPlaceholder, start)),
                Some('}') => {
                    self.position += 1;
                    return Ok(text);
                }
                Some('\\') => text.push(self.escaped()),
                Some('$') => {
                    let inner = self.position;
                    match self.chars.get(inner + 1).copied() {
                        Some(next) if next.is_ascii_digit() || next == '{' => {
                            return Err(self.error(SnippetParseErrorKind::NestedPlaceholder, inner));
                        }
                        Some(next) if is_name_start(next) => {
                            return Err(
                                self.error(SnippetParseErrorKind::UnsupportedVariable, inner)
                            );
                        }
                        _ => {
                            text.push('$');
                            self.position += 1;
                        }
                    }
                }
                Some(other) => {
                    text.push(other);
                    self.position += 1;
                }
            }
        }
    }

    fn error(&self, kind: SnippetParseErrorKind, at: usize) -> SnippetParseError {
        let mut line = 1;
        let mut column = 1;
        for &character in self.chars.iter().take(at) {
            if character == '\n' {
                line += 1;
                column = 1;
            } else {
                column += 1;
            }
        }

        let excerpt = self.source.lines().nth(line - 1).unwrap_or_default();
        let excerpt: String = excerpt
            .trim_end_matches('\r')
            .chars()
            .skip(column - 1)
            .collect();

        SnippetParseError {
            kind,
            line,
            column,
            excerpt,
        }
    }
}

fn is_name_start(character: char) -> bool {
    character.is_ascii_alphabetic() || character == '_'
}

#[cfg(test)]
mod tests;
