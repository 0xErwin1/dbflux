//! Reads one key from a user-owned dotenv file at the moment a value is used.
//!
//! The file is opened on every read, so a file replaced by an atomic rename or
//! a symlink swap is picked up on the next connection without restarting
//! DBFlux. Only the requested key's value leaves this module. Every error
//! names the file and the key but never a value or the text of a line.

use secrecy::{ExposeSecret, SecretString};

use crate::DbError;
use crate::values::source_file;

const LABEL: &str = "Dotenv file";

/// Largest dotenv file DBFlux reads.
pub const MAX_FILE_BYTES: u64 = source_file::MAX_FILE_BYTES;

/// Largest number of entries a dotenv file may define.
pub const MAX_ENTRIES: usize = 1024;

/// Reads `key` from the dotenv file at `path`.
///
/// `path` must be absolute or start with `~/`. The file must be a regular
/// file, at most [`MAX_FILE_BYTES`] long, valid UTF-8, and on Unix readable
/// and writable by its owner only. The
/// whole file is parsed, so a malformed entry fails the read even when the
/// requested key is well formed.
pub fn read_dotenv_value(path: &str, key: &str) -> Result<SecretString, DbError> {
    if !is_valid_key(key) {
        return Err(error(format!(
            "Dotenv key '{key}' is not a valid variable name"
        )));
    }

    let content = source_file::read_owner_only_file(LABEL, path)?;

    match find_value(content.expose_secret(), key)
        .map_err(|failure| error(failure.describe(path)))?
    {
        None => Err(error(format!(
            "Key '{key}' is not defined in dotenv file '{path}'"
        ))),
        Some(value) if value.expose_secret().is_empty() => Err(error(format!(
            "Key '{key}' in dotenv file '{path}' is empty"
        ))),
        Some(value) => Ok(value),
    }
}

fn error(message: String) -> DbError {
    DbError::value_resolution_failed(message)
}

/// Why a dotenv file could not be parsed. Carries line numbers only, never
/// the text of a line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParseFailure {
    MalformedEntry { line: usize },
    TooManyEntries,
}

impl ParseFailure {
    fn describe(&self, display_path: &str) -> String {
        match self {
            Self::MalformedEntry { line } => {
                format!("Dotenv file '{display_path}' has a malformed entry on line {line}")
            }
            Self::TooManyEntries => {
                format!("Dotenv file '{display_path}' defines more than {MAX_ENTRIES} entries")
            }
        }
    }
}

/// Parses every entry of `content` and returns the value of the last
/// definition of `key`.
///
/// Supported syntax: blank lines, `#` comments, an optional `export` prefix,
/// `KEY=value` with an unquoted value (trailing ` # comment` removed),
/// a single-quoted value taken literally, or a double-quoted value with the
/// escapes `\n`, `\r`, `\t`, `\"`, `\\` and `\$`. Values span one line and
/// are never interpolated.
pub(crate) fn find_value(content: &str, key: &str) -> Result<Option<SecretString>, ParseFailure> {
    let mut found = None;
    let mut entries = 0usize;

    for (index, raw_line) in content.lines().enumerate() {
        let line_number = index + 1;
        let line = raw_line.trim();

        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let (entry_key, raw_value) =
            split_entry(line).ok_or(ParseFailure::MalformedEntry { line: line_number })?;

        entries += 1;
        if entries > MAX_ENTRIES {
            return Err(ParseFailure::TooManyEntries);
        }

        let value =
            parse_value(raw_value).ok_or(ParseFailure::MalformedEntry { line: line_number })?;

        if entry_key == key {
            found = Some(SecretString::from(value));
        }
    }

    Ok(found)
}

fn split_entry(line: &str) -> Option<(&str, &str)> {
    let line = match line.strip_prefix("export") {
        Some(rest) if rest.starts_with(char::is_whitespace) => rest.trim_start(),
        _ => line,
    };

    let (key, value) = line.split_once('=')?;
    let key = key.trim_end();

    if !is_valid_key(key) {
        return None;
    }

    Some((key, value.trim_start()))
}

fn parse_value(raw: &str) -> Option<String> {
    if let Some(rest) = raw.strip_prefix('\'') {
        let end = rest.find('\'')?;
        return only_comment_after(&rest[end + 1..]).then(|| rest[..end].to_string());
    }

    if let Some(rest) = raw.strip_prefix('"') {
        return parse_double_quoted(rest);
    }

    let value = match raw.find(" #").or_else(|| raw.find("\t#")) {
        Some(comment_start) => &raw[..comment_start],
        None => raw,
    };

    Some(value.trim_end().to_string())
}

fn parse_double_quoted(rest: &str) -> Option<String> {
    let mut value = String::new();
    let mut characters = rest.char_indices();

    while let Some((position, character)) = characters.next() {
        match character {
            '"' => {
                return only_comment_after(&rest[position + 1..]).then_some(value);
            }
            '\\' => {
                let (_, escaped) = characters.next()?;
                match escaped {
                    'n' => value.push('\n'),
                    'r' => value.push('\r'),
                    't' => value.push('\t'),
                    '"' | '\\' | '$' => value.push(escaped),
                    other => {
                        value.push('\\');
                        value.push(other);
                    }
                }
            }
            other => value.push(other),
        }
    }

    None
}

fn only_comment_after(rest: &str) -> bool {
    let rest = rest.trim_start();
    rest.is_empty() || rest.starts_with('#')
}

/// A variable name: a letter or `_`, then letters, digits, `_` or `.`.
pub(crate) fn is_valid_key(key: &str) -> bool {
    let mut characters = key.chars();

    let Some(first) = characters.next() else {
        return false;
    };

    (first.is_ascii_alphabetic() || first == '_')
        && characters.all(|character| {
            character.is_ascii_alphanumeric() || character == '_' || character == '.'
        })
}

#[cfg(test)]
mod tests;
