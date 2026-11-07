//! Scrubbing of a submitted value out of a store error.

use dbflux_core::secrecy::ExposeSecret;
use dbflux_core::secrecy::zeroize::Zeroize;
use dbflux_core::{DbError, FormattedError, SecretPayload};

/// Marker a scrubbed value leaves behind, so the message still shows the
/// reader that something was removed.
const REDACTED: &str = "[redacted]";

/// The JSON-escaped form of a value: what a serializer writes for it inside
/// a JSON body, without the surrounding quotes — newlines as `\n`, quotes
/// as `\"`, backslashes as `\\`, tabs as `\t`, other control characters as
/// `\uXXXX`. A service or a client echoing a request body leaks the value
/// in this form too. Returns an empty string when the escape cannot be
/// produced or adds nothing over the verbatim value; an empty form scrubs
/// nothing.
fn json_escaped(value: &str) -> String {
    let Ok(quoted) = serde_json::to_string(value) else {
        return String::new();
    };

    if quoted.len() >= 2 && quoted.starts_with('"') && quoted.ends_with('"') {
        quoted[1..quoted.len() - 1].to_string()
    } else {
        String::new()
    }
}

/// Removes a submitted value from a put error before it reaches the caller
/// or the audit trail. A service message that echoes the submitted value
/// would print the very secret the write was carrying, so every field an
/// error renders — the message, its detail and hint, the code and every
/// location field — is scrubbed of every form the value may be echoed in:
/// the text value verbatim and the JSON-escaped form a serializer writes
/// inside a request body; a binary value as its lossy UTF-8 rendering, the
/// form a service echoing received bytes would produce, and its standard
/// base64, the form a blob formatter would produce, each also escaped. An
/// empty form scrubs nothing, so `replace` cannot mangle the message.
///
/// The verbatim text needle is borrowed straight from the `secrecy` type, so
/// no second plaintext copy is created; every form that had to be built is
/// zeroized as soon as the scrub is done.
///
/// The AWS drivers scrub their own put errors the same way; the seam scrubs
/// again because a driver that forgets must not leak a value through a
/// failure path this seam owns.
pub(super) fn scrub_submitted_value(error: DbError, payload: &SecretPayload) -> DbError {
    match payload {
        SecretPayload::Text(text) => {
            let verbatim = text.expose_secret();
            let mut escaped = json_escaped(verbatim);
            let error = scrub_value_from_message(error, &[verbatim, &escaped]);
            escaped.zeroize();
            error
        }
        SecretPayload::Binary(bytes) => {
            let bytes = bytes.expose_secret();
            let mut lossy = String::from_utf8_lossy(bytes).into_owned();
            let mut base64 = super::super::value::encode_base64(bytes);
            let mut lossy_escaped = json_escaped(&lossy);
            let mut base64_escaped = json_escaped(&base64);
            let error = scrub_value_from_message(
                error,
                &[&lossy, &lossy_escaped, &base64, &base64_escaped],
            );
            for buffer in [
                &mut lossy,
                &mut base64,
                &mut lossy_escaped,
                &mut base64_escaped,
            ] {
                buffer.zeroize();
            }
            error
        }
    }
}

/// Scrubs each needle out of a message-carrying field, skipping a needle
/// that is empty — it would match everything `replace` sees — or equal to
/// the marker itself. A needle that equals an earlier one finds nothing
/// new and is harmless.
fn scrub_string_value(field: &mut String, needles: &[&str]) {
    for needle in needles {
        if needle.is_empty() || *needle == REDACTED {
            continue;
        }

        *field = field.replace(needle, REDACTED);
    }
}

/// Scrubs every field of a formatted error that a rendered message reaches:
/// the message, its detail and hint, the code and every location field.
fn scrub_formatted_error(formatted: &mut FormattedError, needles: &[&str]) {
    scrub_string_value(&mut formatted.message, needles);

    if let Some(detail) = &mut formatted.detail {
        scrub_string_value(detail, needles);
    }
    if let Some(hint) = &mut formatted.hint {
        scrub_string_value(hint, needles);
    }
    if let Some(code) = &mut formatted.code {
        scrub_string_value(code, needles);
    }
    if let Some(location) = &mut formatted.location {
        if let Some(schema) = &mut location.schema {
            scrub_string_value(schema, needles);
        }
        if let Some(table) = &mut location.table {
            scrub_string_value(table, needles);
        }
        if let Some(column) = &mut location.column {
            scrub_string_value(column, needles);
        }
        if let Some(constraint) = &mut location.constraint {
            scrub_string_value(constraint, needles);
        }
    }
}

/// Scrubs the submitted value, in every form it may be echoed in, out of
/// the error the store returned. Every `DbError` variant is named here on
/// purpose: a new message-carrying variant fails to compile until this
/// scrub covers it, instead of leaking through a wildcard until a reviewer
/// notices.
fn scrub_value_from_message(error: DbError, needles: &[&str]) -> DbError {
    match error {
        DbError::ConnectionFailed(mut formatted) => {
            scrub_formatted_error(&mut formatted, needles);
            DbError::ConnectionFailed(formatted)
        }
        DbError::QueryFailed(mut formatted) => {
            scrub_formatted_error(&mut formatted, needles);
            DbError::QueryFailed(formatted)
        }
        DbError::AuthFailed(mut formatted) => {
            scrub_formatted_error(&mut formatted, needles);
            DbError::AuthFailed(formatted)
        }
        DbError::ConstraintViolation(mut formatted) => {
            scrub_formatted_error(&mut formatted, needles);
            DbError::ConstraintViolation(formatted)
        }
        DbError::SyntaxError(mut formatted) => {
            scrub_formatted_error(&mut formatted, needles);
            DbError::SyntaxError(formatted)
        }
        DbError::PermissionDenied(mut formatted) => {
            scrub_formatted_error(&mut formatted, needles);
            DbError::PermissionDenied(formatted)
        }
        DbError::ObjectNotFound(mut formatted) => {
            scrub_formatted_error(&mut formatted, needles);
            DbError::ObjectNotFound(formatted)
        }
        DbError::NotSupported(mut message) => {
            scrub_string_value(&mut message, needles);
            DbError::NotSupported(message)
        }
        DbError::InvalidProfile(mut message) => {
            scrub_string_value(&mut message, needles);
            DbError::InvalidProfile(message)
        }
        DbError::ValueResolutionFailed(mut message) => {
            scrub_string_value(&mut message, needles);
            DbError::ValueResolutionFailed(message)
        }
        DbError::Parse(mut message) => {
            scrub_string_value(&mut message, needles);
            DbError::Parse(message)
        }
        DbError::Unsupported(mut message) => {
            scrub_string_value(&mut message, needles);
            DbError::Unsupported(message)
        }
        // An `io::Error` may carry its message inside a custom error or a
        // formatted string; the message is rebuilt with the value removed
        // and the error's kind kept.
        DbError::IoError(error) => {
            let kind = error.kind();
            let mut message = error.to_string();
            scrub_string_value(&mut message, needles);
            DbError::IoError(std::io::Error::new(kind, message))
        }
        // Message-less variants cannot carry any form of the value.
        DbError::Timeout => DbError::Timeout,
        DbError::Cancelled => DbError::Cancelled,
    }
}
