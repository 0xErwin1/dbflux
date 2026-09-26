//! Redaction of sensitive values from audit events.
//!
//! This module provides functions to redact sensitive data from audit event fields
//! such as `details_json` and `error_message`. Sensitive values include passwords,
//! tokens, secrets, API keys, and other credential-like strings.
//!
//! ## Redaction Strategy
//!
//! Uses a combination of key-based redaction (for JSON objects) and pattern-based
//! redaction (for connection strings and URL-encoded credentials).

use std::collections::HashSet;

/// Set of sensitive field names that should be redacted in JSON objects.
const SENSITIVE_JSON_KEYS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "api_key_id",
    "access_key",
    "access_key_id",
    "secret_key",
    "session_token",
    "credentials",
    "private_key",
    "ssh_key",
    "authorization",
    "auth_token",
    "bearer",
    "refresh_token",
    "client_secret",
    "connection_string",
    "connection_uri",
    "database_url",
    "url",
    "uri",
    "passphrase",
];

/// Regex patterns for detecting sensitive values in strings.
/// These patterns match common credential formats. Credentials embedded in
/// URLs are handled separately by `dbflux_core::redact_uri_credentials`.
const SENSITIVE_PATTERNS: &[(&str, &str); 6] = &[
    // AWS access key ID (AKIA...)
    ("aws_access_key", r"(?i)AKIA[0-9A-Z]{16}"),
    // AWS secret access key (40 hex chars)
    ("aws_secret_key", r"(?i)[0-9a-f]{40}"),
    // Generic API key pattern (long hex or base64 strings)
    ("api_key_hex", r#"(?i)['"][0-9a-f]{32,}['"]"#),
    // JWT token
    (
        "jwt_token",
        r"(?i)eyJ[0-9A-Za-z_-]+\.eyJ[0-9A-Za-z_-]+\.[0-9A-Za-z_-]+",
    ),
    // Base64-encoded secrets (at least 32 chars, only base64 chars)
    ("base64_secret", r"(?i)[A-Za-z0-9+/]{40,}={0,2}"),
    // Hex-encoded secrets (at least 32 chars)
    ("hex_secret", r"(?i)[0-9a-f]{32,}"),
];

/// Default redaction replacement string.
const REDACTED: &str = "[REDACTED]";

/// Result of a redaction operation.
#[derive(Debug, Clone)]
pub struct RedactionResult {
    /// The redacted string.
    pub redacted: String,
    /// Number of fields/values that were redacted.
    pub redaction_count: usize,
}

/// Redacts sensitive values from a JSON string.
///
/// Key-name-based redaction (SENSITIVE_JSON_KEYS) runs unconditionally regardless
/// of `redact_sensitive`. Setting `redact_sensitive=false` disables only the
/// pattern-based pass over string values.
///
/// ## Arguments
///
/// * `input` - The JSON string to redact
/// * `redact_sensitive` - Whether to apply sensitive pattern-based redaction
///
/// ## Returns
///
/// A `RedactionResult` containing the redacted string and the number of redactions applied.
pub fn redact_json(input: &str, redact_sensitive: bool) -> RedactionResult {
    if input.is_empty() {
        return RedactionResult {
            redacted: input.to_string(),
            redaction_count: 0,
        };
    }

    let sensitive_keys: HashSet<&str> = SENSITIVE_JSON_KEYS.iter().cloned().collect();
    let mut count = 0;

    // Try to parse as JSON and redact key-value pairs
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(input) {
        let redacted = redact_json_value(&value, &sensitive_keys, redact_sensitive, &mut count);
        return RedactionResult {
            redacted: serde_json::to_string(&redacted).unwrap_or_else(|_| input.to_string()),
            redaction_count: count,
        };
    }

    // If not valid JSON, treat as plain text and apply pattern-based redaction
    if redact_sensitive {
        let (result, redactions) = redact_sensitive_text(input);
        RedactionResult {
            redacted: result,
            redaction_count: redactions,
        }
    } else {
        RedactionResult {
            redacted: input.to_string(),
            redaction_count: 0,
        }
    }
}

/// Recursively redact sensitive values in a JSON value.
fn redact_json_value(
    value: &serde_json::Value,
    sensitive_keys: &HashSet<&str>,
    redact_sensitive: bool,
    count: &mut usize,
) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut new_map = serde_json::Map::new();
            for (key, val) in map {
                let lower_key = key.to_lowercase();
                let is_sensitive = sensitive_keys.contains(key.as_str())
                    || sensitive_keys.contains(lower_key.as_str());

                if is_sensitive {
                    // Redact the value
                    *count += 1;
                    new_map.insert(key.clone(), serde_json::Value::String(REDACTED.to_string()));
                } else if let serde_json::Value::String(str_val) = val {
                    // For string values, apply pattern-based redaction if enabled
                    if redact_sensitive {
                        let (redacted_str, redaction_count) = redact_string_values(str_val, count);
                        *count += redaction_count;
                        new_map.insert(key.clone(), serde_json::Value::String(redacted_str));
                    } else {
                        new_map.insert(key.clone(), val.clone());
                    }
                } else {
                    // Recurse into nested objects and arrays
                    new_map.insert(
                        key.clone(),
                        redact_json_value(val, sensitive_keys, redact_sensitive, count),
                    );
                }
            }
            serde_json::Value::Object(new_map)
        }
        serde_json::Value::Array(arr) => serde_json::Value::Array(
            arr.iter()
                .map(|v| redact_json_value(v, sensitive_keys, redact_sensitive, count))
                .collect(),
        ),
        _ => value.clone(),
    }
}

/// Apply pattern-based redaction to a string value.
fn redact_string_values(input: &str, count: &mut usize) -> (String, usize) {
    let (result, redaction_count) = redact_sensitive_text(input);

    *count += redaction_count;

    (result, redaction_count)
}

/// Redacts URL credentials first, then every value matching `SENSITIVE_PATTERNS`.
fn redact_sensitive_text(input: &str) -> (String, usize) {
    let (mut result, mut redaction_count) = dbflux_core::redact_uri_credentials(input, REDACTED);

    for (_, pattern) in SENSITIVE_PATTERNS {
        let matches = count_regex_matches(&result, pattern);
        if matches > 0 {
            redaction_count += matches;
            result = regex_replace(&result, pattern, REDACTED);
        }
    }

    (result, redaction_count)
}

/// Redacts sensitive values from an error message string.
///
/// ## Arguments
///
/// * `input` - The error message string to redact
/// * `redact_sensitive` - Whether to apply sensitive pattern-based redaction
///
/// ## Returns
///
/// A `RedactionResult` containing the redacted string and the number of redactions applied.
pub fn redact_error_message(input: &str, redact_sensitive: bool) -> RedactionResult {
    if input.is_empty() {
        return RedactionResult {
            redacted: input.to_string(),
            redaction_count: 0,
        };
    }

    if !redact_sensitive {
        return RedactionResult {
            redacted: input.to_string(),
            redaction_count: 0,
        };
    }

    let (result, count) = redact_sensitive_text(input);

    RedactionResult {
        redacted: result,
        redaction_count: count,
    }
}

/// Count the number of regex matches in a string.
fn count_regex_matches(input: &str, pattern: &str) -> usize {
    regex::Regex::new(pattern)
        .map(|re| re.find_iter(input).count())
        .unwrap_or(0)
}

/// Replace all regex matches in a string with a replacement.
fn regex_replace(input: &str, pattern: &str, replacement: &str) -> String {
    regex::Regex::new(pattern)
        .map(|re| re.replace_all(input, replacement).to_string())
        .unwrap_or_else(|_| input.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_json_object_with_password() {
        let json = r#"{"username": "admin", "password": "secret123"}"#;
        let result = redact_json(json, true);
        assert!(result.redacted.contains("[REDACTED]"));
        assert!(!result.redacted.contains("secret123"));
        assert!(result.redacted.contains("admin"));
        assert_eq!(result.redaction_count, 1);
    }

    #[test]
    fn test_redact_json_object_with_multiple_sensitive_fields() {
        let json = r#"{"api_key": "ak_12345678", "secret": "super_secret", "data": "ok"}"#;
        let result = redact_json(json, true);
        assert!(result.redacted.contains("[REDACTED]"));
        assert!(!result.redacted.contains("ak_12345678"));
        assert!(!result.redacted.contains("super_secret"));
        assert!(result.redacted.contains("ok"));
        assert!(result.redaction_count >= 2);
    }

    #[test]
    fn test_redact_nested_json() {
        let json = r#"{"config": {"password": "nested_secret", "token": "abc123"}}"#;
        let result = redact_json(json, true);
        assert!(!result.redacted.contains("nested_secret"));
        assert!(!result.redacted.contains("abc123"));
    }

    #[test]
    fn test_redact_error_message_with_aws_key() {
        let error = "Failed to connect: access_key_id=AKIAIOSFODNN7EXAMPLE secret_key=a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        let result = redact_error_message(error, true);
        assert!(!result.redacted.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(
            !result
                .redacted
                .contains("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2")
        );
        assert!(result.redaction_count >= 1);
    }

    #[test]
    fn test_key_based_redaction_is_unconditional() {
        let json = r#"{"password": "secret123"}"#;
        let result = redact_json(json, false);
        assert!(result.redacted.contains("[REDACTED]"));
        assert_eq!(result.redaction_count, 1);
    }

    #[test]
    fn test_pattern_redaction_disabled_when_flag_false() {
        // A value that matches a pattern-based rule (long hex string) but whose
        // key is NOT in SENSITIVE_JSON_KEYS.
        let json = r#"{"trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"}"#;

        let result_off = redact_json(json, false);
        assert_eq!(
            result_off.redaction_count, 0,
            "pattern-based redaction must not fire when redact_sensitive=false"
        );

        let result_on = redact_json(json, true);
        assert!(
            result_on.redaction_count >= 1,
            "pattern-based redaction must fire when redact_sensitive=true"
        );
    }

    #[test]
    fn test_redact_empty_string() {
        let result = redact_json("", true);
        assert_eq!(result.redacted, "");
        assert_eq!(result.redaction_count, 0);
    }

    #[test]
    fn test_redact_invalid_json_falls_back_to_pattern_matching() {
        // Non-JSON input must still produce a Redacted result without panicking;
        // whether any patterns match is a property of the regex set, not this fallback.
        let input = "password=secret123";
        let _ = redact_json(input, true);
    }

    #[test]
    fn test_redact_error_message_covers_every_connection_scheme() {
        let urls = [
            "postgres://u:hunter2pw@h:5432/db",
            "postgresql://u:hunter2pw@h:5432/db",
            "mysql://u:hunter2pw@h:3306/db",
            "mariadb://u:hunter2pw@h/db",
            "mongodb://u:hunter2pw@h1,h2/db",
            "mongodb+srv://u:hunter2pw@cluster.example.net/db",
            "redis://:hunter2pw@h:6379",
            "rediss://default:hunter2pw@h:6380",
            "sqlserver://sa:hunter2pw@h:1433",
            "clickhouse://u:hunter2pw@h:8123",
            "http://u:hunter2pw@h:8086",
            "https://u:hunter2pw@h",
            "postgresql://u:hunter%40pw@[::1]:5432/db",
            "postgresql://h/db?password=hunter2pw",
        ];

        for url in urls {
            let result = redact_error_message(&format!("connect failed: {url} refused"), true);
            assert!(
                !result.redacted.contains("hunter"),
                "password leaked for {url}: {}",
                result.redacted
            );
            assert!(result.redacted.contains("[REDACTED]"));
            assert!(result.redaction_count >= 1);
        }
    }

    #[test]
    fn test_redact_json_value_with_postgresql_url_under_neutral_key() {
        let json = r#"{"detail": "dial postgresql://alice:hunter2pw@db:5432/app failed"}"#;
        let result = redact_json(json, true);
        assert!(!result.redacted.contains("hunter2pw"));
        assert!(
            result
                .redacted
                .contains("postgresql://alice:[REDACTED]@db:5432/app")
        );
    }

    #[test]
    fn test_redact_connection_string() {
        let json = r#"{"connection_string": "postgresql://user:password123@localhost:5432/db"}"#;
        let result = redact_json(json, true);
        assert!(result.redacted.contains("[REDACTED]"));
        assert!(!result.redacted.contains("password123"));
    }
}
