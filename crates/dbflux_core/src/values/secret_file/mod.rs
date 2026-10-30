//! Reads a secret from a user-owned file at the moment a value is used.
//!
//! Tools such as sops-nix, secretspec or systemd credentials own and decrypt
//! the file; DBFlux only reads it, on every use, so rotation is picked up
//! without restarting. Every error names the file and the key but never its
//! content.

use std::collections::HashMap;

use secrecy::{ExposeSecret, SecretString};
use serde_json::value::RawValue;

use crate::DbError;
use crate::values::source_file;

/// Largest secret file DBFlux reads.
pub const MAX_FILE_BYTES: u64 = source_file::MAX_FILE_BYTES;

const LABEL: &str = "Secret file";

/// Reads the secret stored in the file at `path`.
///
/// `path` must be absolute or start with `~/`. The file must be a regular
/// file, at most [`MAX_FILE_BYTES`] long, valid UTF-8, and on Unix readable
/// and writable by its owner only.
///
/// Without `json_key` the value is the whole file with exactly one trailing
/// `\n` or `\r\n` removed. With `json_key` the file must be a JSON object and
/// the value is that top-level field: a string as is, a number or boolean as
/// its exact text in the file.
pub fn read_secret_file(path: &str, json_key: Option<&str>) -> Result<SecretString, DbError> {
    if json_key.is_some_and(str::is_empty) {
        return Err(error(
            "JSON key in secret file reference is empty".to_string(),
        ));
    }

    let content = source_file::read_owner_only_file(LABEL, path)?;

    match json_key {
        None => whole_file_value(path, content.expose_secret()),
        Some(key) => json_field_value(path, content.expose_secret(), key),
    }
}

fn error(message: String) -> DbError {
    DbError::value_resolution_failed(message)
}

fn whole_file_value(path: &str, content: &str) -> Result<SecretString, DbError> {
    let value = content
        .strip_suffix("\r\n")
        .or_else(|| content.strip_suffix('\n'))
        .unwrap_or(content);

    if value.is_empty() {
        return Err(error(format!("{LABEL} '{path}' is empty")));
    }

    Ok(SecretString::from(value.to_string()))
}

fn json_field_value(path: &str, content: &str, key: &str) -> Result<SecretString, DbError> {
    // serde_json's error text can quote the content, so it is never surfaced.
    let not_an_object = || error(format!("{LABEL} '{path}' is not a JSON object"));

    // Raw values keep a number's text from the file; parsing it into a
    // `serde_json::Number` would rewrite long or exponent forms.
    let mut fields: HashMap<String, Box<RawValue>> =
        serde_json::from_str(content).map_err(|_| not_an_object())?;

    let Some(raw) = fields.remove(key) else {
        return Err(error(format!(
            "Key '{key}' is not defined in secret file '{path}'"
        )));
    };

    let value = scalar_text(&raw).ok_or_else(|| {
        error(format!(
            "Key '{key}' in secret file '{path}' is not a string, number or boolean"
        ))
    })?;

    if value.is_empty() {
        return Err(error(format!(
            "Key '{key}' in secret file '{path}' is empty"
        )));
    }

    Ok(SecretString::from(value))
}

/// The text of a string, number or boolean JSON value; `None` for null,
/// arrays and objects. Numbers and booleans keep their exact source text.
fn scalar_text(raw: &RawValue) -> Option<String> {
    let text = raw.get();

    match text.as_bytes().first()? {
        b'"' => serde_json::from_str::<String>(text).ok(),
        b't' | b'f' | b'-' | b'0'..=b'9' => Some(text.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
