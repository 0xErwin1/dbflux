//! Relaxed JSON as typed in document query bars and shells: unquoted keys and
//! single-quoted strings are accepted and normalized to strict JSON.

/// Normalize relaxed JSON to strict JSON
// All indexing is guarded by explicit `i < chars.len()` checks in the loop body.
#[allow(clippy::indexing_slicing)]
pub fn normalize_relaxed_json(input: &str) -> String {
    let mut result = String::with_capacity(input.len() * 2);
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];

        // Handle strings (preserve as-is, just convert single to double quotes)
        if ch == '"' || ch == '\'' {
            let quote = ch;
            let target_quote = '"';
            result.push(target_quote);
            i += 1;

            while i < chars.len() {
                let inner = chars[i];
                if inner == '\\' && i + 1 < chars.len() {
                    result.push(inner);
                    result.push(chars[i + 1]);
                    i += 2;
                } else if inner == quote {
                    result.push(target_quote);
                    i += 1;
                    break;
                } else {
                    result.push(inner);
                    i += 1;
                }
            }
            continue;
        }

        // Check for unquoted key after { or ,
        if ch == '{' || ch == ',' {
            result.push(ch);
            i += 1;

            // Skip whitespace
            while i < chars.len() && chars[i].is_whitespace() {
                result.push(chars[i]);
                i += 1;
            }

            // Check if this looks like an unquoted key
            if i < chars.len() && is_key_start_char(chars[i]) {
                // Collect the key
                let key_start = i;
                while i < chars.len() && is_key_char(chars[i]) {
                    i += 1;
                }
                let key = &chars[key_start..i];

                // Skip whitespace after key
                while i < chars.len() && chars[i].is_whitespace() {
                    i += 1;
                }

                // Check if followed by colon (confirming it's a key)
                if i < chars.len() && chars[i] == ':' {
                    result.push('"');
                    for &c in key {
                        result.push(c);
                    }
                    result.push('"');
                } else {
                    // Not a key, output as-is
                    for &c in key {
                        result.push(c);
                    }
                }
            }
            continue;
        }

        result.push(ch);
        i += 1;
    }

    result
}

fn is_key_start_char(ch: char) -> bool {
    ch.is_alphabetic() || ch == '_' || ch == '$'
}

fn is_key_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_' || ch == '$'
}

/// Parses relaxed JSON (`{ status: 'paid' }`) into a JSON value. Strict JSON is
/// accepted as is.
pub fn parse_relaxed_json(input: &str) -> Result<serde_json::Value, serde_json::Error> {
    let trimmed = input.trim();

    match serde_json::from_str::<serde_json::Value>(trimmed) {
        Ok(json) => Ok(json),
        Err(_) => serde_json::from_str(&normalize_relaxed_json(trimmed)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unquoted_keys_and_single_quotes_become_strict_json() {
        let parsed = parse_relaxed_json("{ status: 'failed', total: { $gt: 100 } }")
            .expect("relaxed JSON parses");

        assert_eq!(
            parsed,
            serde_json::json!({ "status": "failed", "total": { "$gt": 100 } })
        );
    }

    #[test]
    fn quoted_dotted_keys_are_kept() {
        let parsed = parse_relaxed_json(r#"{ "price.amount": -1 }"#).expect("parses");

        assert_eq!(parsed, serde_json::json!({ "price.amount": -1 }));
    }

    #[test]
    fn invalid_input_is_an_error() {
        assert!(parse_relaxed_json("{ status: }").is_err());
    }
}
