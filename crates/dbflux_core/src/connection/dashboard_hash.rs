//! Canonical JSON hashing for dashboard bodies and individual widgets.
//!
//! Used by the dashboard sync model to detect upstream drift without
//! depending on insignificant variations such as key ordering or whitespace.
//!
//! Hashes are stored with a `"v1:"` prefix. If the canonicalization rule
//! changes in the future, the prefix can be bumped and stored hashes whose
//! prefix does not match the current version are treated as unknown
//! (forces a fresh fetch + compare).

use std::collections::BTreeMap;

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::DbError;

/// Prefix for hash format v1. Any stored hash whose prefix does not match
/// MUST be treated as `Unknown` by callers.
pub const HASH_PREFIX_V1: &str = "v1:";

/// Returns the canonical byte representation of a JSON value:
/// - object keys are sorted lexicographically at every level
/// - allowlisted optional-empty fields (see [`is_dropped`]) are dropped
/// - serialized in compact form (no whitespace) UTF-8 without trailing newline.
///
/// Equivalent inputs that differ only in key order or insignificant
/// whitespace produce identical output.
pub fn canonicalize(value: &Value) -> Vec<u8> {
    let normalized = normalize(value);
    serde_json::to_vec(&normalized).expect("BTreeMap-backed Value always serializes")
}

/// Computes the canonical content hash of a dashboard body JSON string.
///
/// Returns a `"v1:" + hex(sha256(canonical_bytes))` string.
///
/// # Errors
///
/// Returns `DbError::Parse` when `body_json` is not valid JSON.
pub fn content_hash(body_json: &str) -> Result<String, DbError> {
    let value: Value = serde_json::from_str(body_json)
        .map_err(|e| DbError::Parse(format!("dashboard body is not valid JSON: {e}")))?;
    Ok(hash_value(&value))
}

/// Computes the canonical hash of a single widget object.
///
/// Operates on the widget object only — the widget's array position is NOT
/// part of the hash. Two widgets at different indices but with identical
/// content produce identical hashes.
pub fn widget_hash(widget: &Value) -> String {
    hash_value(widget)
}

fn hash_value(value: &Value) -> String {
    let bytes = canonicalize(value);
    let digest = Sha256::digest(&bytes);
    format!("{HASH_PREFIX_V1}{}", hex::encode(digest))
}

/// Recursively normalizes a `Value`:
/// - object keys sorted via `BTreeMap`
/// - allowlisted optional-empty fields removed
/// - arrays and primitives pass through (arrays recurse into their elements)
fn normalize(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted: BTreeMap<String, Value> = BTreeMap::new();

            for (key, val) in map {
                if is_dropped(key, val) {
                    continue;
                }
                sorted.insert(key.clone(), normalize(val));
            }

            let mut out = Map::with_capacity(sorted.len());
            for (key, val) in sorted {
                out.insert(key, val);
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(normalize).collect()),
        other => other.clone(),
    }
}

/// Returns `true` when `(key, value)` is an allowlisted optional-empty field
/// that should be dropped from the canonical form.
///
/// Allowlist (per design §A):
/// - `annotations` when empty (empty object or empty array)
/// - `legend` when default (empty object or `{"position": "bottom"}`)
fn is_dropped(key: &str, value: &Value) -> bool {
    match key {
        "annotations" => is_empty_container(value),
        "legend" => is_default_legend(value),
        _ => false,
    }
}

fn is_empty_container(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.is_empty(),
        Value::Array(arr) => arr.is_empty(),
        Value::Null => true,
        _ => false,
    }
}

fn is_default_legend(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            if map.is_empty() {
                return true;
            }
            map.len() == 1 && map.get("position").map(|v| v.as_str()) == Some(Some("bottom"))
        }
        Value::Null => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn content_hash_always_starts_with_v1_prefix() {
        let h = content_hash("{}").expect("hash");
        assert!(h.starts_with(HASH_PREFIX_V1));
    }

    #[test]
    fn widget_hash_always_starts_with_v1_prefix() {
        let h = widget_hash(&json!({}));
        assert!(h.starts_with(HASH_PREFIX_V1));
    }

    #[test]
    fn content_hash_invalid_json_errors() {
        let err = content_hash("{not json").unwrap_err();
        matches!(err, DbError::Parse(_));
    }

    #[test]
    fn key_reorder_invariance() {
        let a = r#"{"a":1,"b":{"x":1,"y":2}}"#;
        let b = r#"{"b":{"y":2,"x":1},"a":1}"#;
        assert_eq!(content_hash(a).unwrap(), content_hash(b).unwrap());
    }

    #[test]
    fn whitespace_invariance() {
        let a = r#"{"a":1,"b":2}"#;
        let b = "  {\n  \"a\" :  1 , \n  \"b\":\t2\n}\n";
        assert_eq!(content_hash(a).unwrap(), content_hash(b).unwrap());
    }

    #[test]
    fn empty_annotations_dropped() {
        let with_empty = r#"{"widgets":[{"properties":{"annotations":{}}}]}"#;
        let without = r#"{"widgets":[{"properties":{}}]}"#;
        assert_eq!(
            content_hash(with_empty).unwrap(),
            content_hash(without).unwrap()
        );
    }

    #[test]
    fn empty_annotations_array_dropped() {
        let with_empty = r#"{"properties":{"annotations":[]}}"#;
        let without = r#"{"properties":{}}"#;
        assert_eq!(
            content_hash(with_empty).unwrap(),
            content_hash(without).unwrap()
        );
    }

    #[test]
    fn default_legend_dropped() {
        let with_default = r#"{"properties":{"legend":{"position":"bottom"}}}"#;
        let without = r#"{"properties":{}}"#;
        assert_eq!(
            content_hash(with_default).unwrap(),
            content_hash(without).unwrap()
        );
    }

    #[test]
    fn non_default_legend_kept() {
        let custom = r#"{"properties":{"legend":{"position":"right"}}}"#;
        let without = r#"{"properties":{}}"#;
        assert_ne!(
            content_hash(custom).unwrap(),
            content_hash(without).unwrap()
        );
    }

    #[test]
    fn nonempty_annotations_kept() {
        let with_data = r#"{"properties":{"annotations":{"horizontal":[{"value":42}]}}}"#;
        let without = r#"{"properties":{}}"#;
        assert_ne!(
            content_hash(with_data).unwrap(),
            content_hash(without).unwrap()
        );
    }

    #[test]
    fn widget_array_order_affects_content_hash() {
        let a = r#"{"widgets":[{"id":1},{"id":2}]}"#;
        let b = r#"{"widgets":[{"id":2},{"id":1}]}"#;
        assert_ne!(content_hash(a).unwrap(), content_hash(b).unwrap());
    }

    #[test]
    fn widget_hash_ignores_position() {
        let w1 = json!({"id": 7, "props": {"a": 1, "b": 2}});
        let w2 = json!({"props": {"b": 2, "a": 1}, "id": 7});
        assert_eq!(widget_hash(&w1), widget_hash(&w2));
    }

    #[test]
    fn deterministic_across_runs() {
        let body = r#"{"widgets":[{"type":"metric","properties":{"stat":"Avg"}}]}"#;
        let h1 = content_hash(body).unwrap();
        let h2 = content_hash(body).unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn hex_payload_is_64_chars() {
        let h = content_hash("{}").unwrap();
        let payload = h.strip_prefix(HASH_PREFIX_V1).unwrap();
        assert_eq!(payload.len(), 64);
        assert!(payload.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
