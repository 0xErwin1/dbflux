//! Removing a submitted value from the text of an error.
//!
//! A service message that echoes a request would print the very secret the
//! write carried, and a client or a proxy may echo it wrapped in JSON, once
//! or several times over. Both the AWS drivers and the governed write seam
//! scrub their errors through this module, so the two cannot drift apart.

use secrecy::{ExposeSecret, zeroize::Zeroize};

use super::SecretPayload;
use crate::{DbError, FormattedError};

/// Marker a scrubbed value leaves behind, so the message still shows the
/// reader that something was removed.
const REDACTED: &str = "[redacted]";

/// Most levels of JSON escaping looked through. A value echoed inside a
/// body that was itself echoed inside another body is escaped once per level.
const MAX_ESCAPE_DEPTH: usize = 4;

/// Removes every occurrence of each value from every text an error renders,
/// in its verbatim form and in the form a JSON serializer writes it in —
/// with short escapes (`\n`, `\b`), `\uXXXX` escapes, or a mix of both, at
/// any nesting depth up to [`MAX_ESCAPE_DEPTH`]. Occurrences that overlap
/// are removed as one region, so no fragment of one form survives next to
/// another. An empty value scrubs nothing, so it cannot mangle the message.
///
/// Every `DbError` variant is named on purpose: a new message-carrying
/// variant fails to compile until this scrub covers it, instead of leaking
/// through a wildcard.
pub fn scrub_echoed_values(error: DbError, values: &[&str]) -> DbError {
    let values: Vec<&str> = values
        .iter()
        .copied()
        .filter(|value| !value.is_empty() && *value != REDACTED)
        .collect();

    if values.is_empty() {
        return error;
    }

    match error {
        DbError::ConnectionFailed(formatted) => {
            DbError::ConnectionFailed(scrub_formatted(formatted, &values))
        }
        DbError::QueryFailed(formatted) => {
            DbError::QueryFailed(scrub_formatted(formatted, &values))
        }
        DbError::AuthFailed(formatted) => DbError::AuthFailed(scrub_formatted(formatted, &values)),
        DbError::ConstraintViolation(formatted) => {
            DbError::ConstraintViolation(scrub_formatted(formatted, &values))
        }
        DbError::SyntaxError(formatted) => {
            DbError::SyntaxError(scrub_formatted(formatted, &values))
        }
        DbError::PermissionDenied(formatted) => {
            DbError::PermissionDenied(scrub_formatted(formatted, &values))
        }
        DbError::ObjectNotFound(formatted) => {
            DbError::ObjectNotFound(scrub_formatted(formatted, &values))
        }
        DbError::NotSupported(message) => DbError::NotSupported(redact(&message, &values)),
        DbError::InvalidProfile(message) => DbError::InvalidProfile(redact(&message, &values)),
        DbError::ValueResolutionFailed(message) => {
            DbError::ValueResolutionFailed(redact(&message, &values))
        }
        DbError::Parse(message) => DbError::Parse(redact(&message, &values)),
        DbError::Unsupported(message) => DbError::Unsupported(redact(&message, &values)),
        // An `io::Error` may carry its message inside a custom error or a
        // formatted string; the message is rebuilt with the value removed
        // and the error's kind kept.
        DbError::IoError(error) => {
            let kind = error.kind();
            DbError::IoError(std::io::Error::new(
                kind,
                redact(&error.to_string(), &values),
            ))
        }
        // Message-less variants cannot carry any form of the value.
        DbError::Timeout => DbError::Timeout,
        DbError::Cancelled => DbError::Cancelled,
    }
}

/// Removes a submitted payload from an error: a text value as it is, a
/// binary value as the lossy UTF-8 rendering a service echoing received
/// bytes would produce and as the standard base64 a blob formatter would
/// produce. The forms that had to be built are zeroized before returning.
pub fn scrub_submitted_payload(error: DbError, payload: &SecretPayload) -> DbError {
    match payload {
        SecretPayload::Text(text) => scrub_echoed_values(error, &[text.expose_secret()]),
        SecretPayload::Binary(bytes) => {
            let bytes = bytes.expose_secret();
            let mut lossy = String::from_utf8_lossy(bytes).into_owned();
            let mut base64 = encode_base64(bytes);
            let error = scrub_echoed_values(error, &[&lossy, &base64]);
            lossy.zeroize();
            base64.zeroize();
            error
        }
    }
}

fn scrub_formatted(mut formatted: FormattedError, values: &[&str]) -> FormattedError {
    let scrub = |field: &mut String| *field = redact(field, values);

    scrub(&mut formatted.message);
    for field in [
        &mut formatted.detail,
        &mut formatted.hint,
        &mut formatted.code,
    ]
    .into_iter()
    .flatten()
    {
        scrub(field);
    }
    if let Some(location) = &mut formatted.location {
        for field in [
            &mut location.schema,
            &mut location.table,
            &mut location.column,
            &mut location.constraint,
        ]
        .into_iter()
        .flatten()
        {
            scrub(field);
        }
    }

    formatted
}

/// One character of a text and the bytes of the original text it stands
/// for: a character as written, or the escape sequence that decoded to it.
struct Unit {
    character: char,
    start: usize,
    end: usize,
}

/// Replaces every occurrence of a value in `text`, however deeply
/// JSON-escaped, with the marker. The text is read at each escaping depth —
/// as written, then with its escape sequences decoded, then those decoded
/// again — and every occurrence found at any depth marks the bytes of the
/// original text it covers. Marked regions that touch or overlap merge.
fn redact(text: &str, values: &[&str]) -> String {
    let mut units: Vec<Unit> = text
        .char_indices()
        .map(|(start, character)| Unit {
            character,
            start,
            end: start + character.len_utf8(),
        })
        .collect();
    let mut regions: Vec<(usize, usize)> = Vec::new();

    for _ in 0..=MAX_ESCAPE_DEPTH {
        for value in values {
            mark_occurrences(&units, value, &mut regions);
        }

        match decode_escapes(&units) {
            Some(decoded) => units = decoded,
            None => break,
        }
    }

    if regions.is_empty() {
        return text.to_string();
    }

    regions.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(regions.len());
    for (start, end) in regions {
        match merged.last_mut() {
            Some((_, last_end)) if start <= *last_end => *last_end = (*last_end).max(end),
            _ => merged.push((start, end)),
        }
    }

    let mut scrubbed = String::with_capacity(text.len());
    let mut cursor = 0;
    for (start, end) in merged {
        scrubbed.push_str(&text[cursor..start]);
        scrubbed.push_str(REDACTED);
        cursor = end;
    }
    scrubbed.push_str(&text[cursor..]);

    scrubbed
}

fn mark_occurrences(units: &[Unit], value: &str, regions: &mut Vec<(usize, usize)>) {
    let needle: Vec<char> = value.chars().collect();
    let Some(last_start) = units.len().checked_sub(needle.len()) else {
        return;
    };

    for start in 0..=last_start {
        let Some(window) = units.get(start..start + needle.len()) else {
            continue;
        };
        if window
            .iter()
            .zip(&needle)
            .all(|(unit, expected)| unit.character == *expected)
            && let (Some(first), Some(last)) = (window.first(), window.last())
        {
            regions.push((first.start, last.end));
        }
    }
}

/// One level of JSON unescaping: `\"`, `\\`, `\/`, `\b`, `\f`, `\n`, `\r`,
/// `\t` and `\uXXXX` (a surrogate pair as one character). `None` when the
/// text holds no escape sequence, so the caller knows to stop.
fn decode_escapes(units: &[Unit]) -> Option<Vec<Unit>> {
    let mut decoded = Vec::with_capacity(units.len());
    let mut changed = false;
    let mut index = 0;

    while index < units.len() {
        let Some(unit) = units.get(index) else {
            break;
        };

        if unit.character == '\\'
            && let Some((character, consumed)) = decode_escape_at(units, index)
        {
            decoded.push(Unit {
                character,
                start: unit.start,
                end: units
                    .get(index + consumed - 1)
                    .map_or(unit.end, |last| last.end),
            });
            index += consumed;
            changed = true;
            continue;
        }

        decoded.push(Unit {
            character: unit.character,
            start: unit.start,
            end: unit.end,
        });
        index += 1;
    }

    changed.then_some(decoded)
}

/// The character the escape sequence starting at `index` stands for and how
/// many units it spans, or `None` when it is not an escape sequence.
fn decode_escape_at(units: &[Unit], index: usize) -> Option<(char, usize)> {
    let character = match units.get(index + 1)?.character {
        '"' => '"',
        '\\' => '\\',
        '/' => '/',
        'b' => '\u{8}',
        'f' => '\u{c}',
        'n' => '\n',
        'r' => '\r',
        't' => '\t',
        'u' => return decode_unicode_escape(units, index),
        _ => return None,
    };

    Some((character, 2))
}

fn decode_unicode_escape(units: &[Unit], index: usize) -> Option<(char, usize)> {
    let high = hex_quad(units, index + 2)?;

    if (0xD800..0xDC00).contains(&high) {
        let continues = units
            .get(index + 6)
            .is_some_and(|unit| unit.character == '\\')
            && units
                .get(index + 7)
                .is_some_and(|unit| unit.character == 'u');
        let low = continues.then(|| hex_quad(units, index + 8)).flatten()?;
        if !(0xDC00..0xE000).contains(&low) {
            return None;
        }
        let code = 0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00);

        return char::from_u32(code).map(|character| (character, 12));
    }

    char::from_u32(high).map(|character| (character, 6))
}

fn hex_quad(units: &[Unit], start: usize) -> Option<u32> {
    let digits = units.get(start..start + 4)?;

    digits.iter().try_fold(0u32, |accumulated, unit| {
        unit.character
            .to_digit(16)
            .map(|digit| accumulated * 16 + digit)
    })
}

/// Standard base64 with padding (RFC 4648).
pub fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);

    for chunk in bytes.chunks(3) {
        let first = chunk.first().copied().unwrap_or(0);
        let second = chunk.get(1).copied();
        let third = chunk.get(2).copied();

        let triple = (u32::from(first) << 16)
            | (u32::from(second.unwrap_or(0)) << 8)
            | u32::from(third.unwrap_or(0));

        let symbol = |shift: u32| {
            ALPHABET
                .get(((triple >> shift) & 0x3f) as usize)
                .map_or('=', |symbol| char::from(*symbol))
        };

        encoded.push(symbol(18));
        encoded.push(symbol(12));
        encoded.push(if second.is_some() { symbol(6) } else { '=' });
        encoded.push(if third.is_some() { symbol(0) } else { '=' });
    }

    encoded
}
