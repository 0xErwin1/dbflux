use crate::ui::icons::AppIcon;
use dbflux_core::{KeyGetResult, KeyType, Value, ValueRepr};
use gpui::Hsla;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub(super) struct MemberEntry {
    pub display: String,
    pub field: Option<String>,
    pub score: Option<f64>,
    /// Stream entry ID, used for XDEL targeting.
    pub entry_id: Option<String>,
}

pub(super) fn key_type_icon(key_type: Option<KeyType>) -> (AppIcon, Hsla) {
    match key_type {
        Some(KeyType::String) | Some(KeyType::Bytes) => {
            (AppIcon::CaseSensitive, gpui::hsla(0.5, 0.6, 0.6, 1.0))
        }
        Some(KeyType::Hash) => (AppIcon::Hash, gpui::hsla(0.75, 0.6, 0.6, 1.0)),
        Some(KeyType::List) => (AppIcon::Rows3, gpui::hsla(0.6, 0.6, 0.6, 1.0)),
        Some(KeyType::Set) => (AppIcon::Box, gpui::hsla(0.08, 0.7, 0.6, 1.0)),
        Some(KeyType::SortedSet) => (AppIcon::ArrowUp, gpui::hsla(0.08, 0.7, 0.6, 1.0)),
        Some(KeyType::Json) => (AppIcon::Braces, gpui::hsla(0.35, 0.6, 0.6, 1.0)),
        Some(KeyType::Stream) => (AppIcon::Zap, gpui::hsla(0.15, 0.7, 0.6, 1.0)),
        _ => (AppIcon::KeyRound, gpui::hsla(0.0, 0.0, 0.5, 1.0)),
    }
}

pub(super) fn key_type_label(key_type: KeyType) -> &'static str {
    match key_type {
        KeyType::String => "String",
        KeyType::Bytes => "Bytes",
        KeyType::Hash => "Hash",
        KeyType::List => "List",
        KeyType::Set => "Set",
        KeyType::SortedSet => "ZSet",
        KeyType::Json => "JSON",
        KeyType::Stream => "Stream",
        KeyType::Unknown => "?",
    }
}

pub(super) fn render_value_preview(value: &KeyGetResult) -> String {
    match value.repr {
        ValueRepr::Text | ValueRepr::Json | ValueRepr::Structured | ValueRepr::Stream => {
            let text = String::from_utf8_lossy(&value.value);
            let max_chars = 4000;

            if text.chars().count() > max_chars {
                let truncated: String = text.chars().take(max_chars).collect();
                format!("{}\n... (truncated)", truncated)
            } else {
                text.to_string()
            }
        }
        ValueRepr::Binary => format!("{} bytes (binary)", value.value.len()),
    }
}

pub(super) fn parse_database_name(name: &str) -> Option<u32> {
    let trimmed = name.trim();
    let digits = trimmed.strip_prefix("db").unwrap_or(trimmed);
    digits.parse::<u32>().ok()
}

pub(super) fn parse_members(value: &KeyGetResult) -> Vec<MemberEntry> {
    if value.repr == ValueRepr::Stream {
        return parse_stream_entries(&value.value);
    }

    if value.repr != ValueRepr::Structured {
        return vec![MemberEntry {
            display: String::from_utf8_lossy(&value.value).to_string(),
            field: None,
            score: None,
            entry_id: None,
        }];
    }

    let Ok(json) = serde_json::from_slice::<serde_json::Value>(&value.value) else {
        return vec![MemberEntry {
            display: String::from_utf8_lossy(&value.value).to_string(),
            field: None,
            score: None,
            entry_id: None,
        }];
    };

    match json {
        serde_json::Value::Object(map) => map
            .into_iter()
            .map(|(k, v)| {
                let display = match &v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                MemberEntry {
                    display,
                    field: Some(k),
                    score: None,
                    entry_id: None,
                }
            })
            .collect(),
        serde_json::Value::Array(items) => items
            .into_iter()
            .map(|item| match item {
                serde_json::Value::Object(map) => {
                    let member = map
                        .get("member")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let score = map.get("score").and_then(|v| v.as_f64());

                    if score.is_some() {
                        MemberEntry {
                            display: member,
                            field: None,
                            score,
                            entry_id: None,
                        }
                    } else {
                        MemberEntry {
                            display: match map.values().next() {
                                Some(serde_json::Value::String(s)) => s.clone(),
                                Some(v) => v.to_string(),
                                None => String::new(),
                            },
                            field: None,
                            score: None,
                            entry_id: None,
                        }
                    }
                }
                serde_json::Value::String(s) => MemberEntry {
                    display: s,
                    field: None,
                    score: None,
                    entry_id: None,
                },
                other => MemberEntry {
                    display: other.to_string(),
                    field: None,
                    score: None,
                    entry_id: None,
                },
            })
            .collect(),
        _ => vec![MemberEntry {
            display: String::from_utf8_lossy(&value.value).to_string(),
            field: None,
            score: None,
            entry_id: None,
        }],
    }
}

/// Parses stream entries from `[{"id":"...","fields":{...}}]` JSON into member rows.
/// Each entry becomes a `MemberEntry` with `display = id` and `field = compact JSON of fields`.
pub(super) fn parse_stream_entries(raw: &[u8]) -> Vec<MemberEntry> {
    let Ok(entries) = serde_json::from_slice::<Vec<serde_json::Value>>(raw) else {
        return vec![MemberEntry {
            display: String::from_utf8_lossy(raw).to_string(),
            field: None,
            score: None,
            entry_id: None,
        }];
    };

    entries
        .into_iter()
        .filter_map(|entry| {
            let id = entry.get("id")?.as_str()?.to_string();
            let fields = entry.get("fields")?;
            let fields_str = serde_json::to_string(fields).ok()?;

            Some(MemberEntry {
                display: id.clone(),
                field: Some(fields_str),
                score: None,
                entry_id: Some(id),
            })
        })
        .collect()
}

/// Parse a JSON string into a `Value` tree. Falls back to `Value::Text` on error.
pub(super) fn parse_json_to_value(json_str: &str) -> Value {
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(json_str) else {
        return Value::Text(json_str.to_string());
    };
    serde_json_to_value(&parsed)
}

pub(super) fn serde_json_to_value(jv: &serde_json::Value) -> Value {
    match jv {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::Text(s.clone()),
        serde_json::Value::Array(arr) => {
            Value::Array(arr.iter().map(serde_json_to_value).collect())
        }
        serde_json::Value::Object(map) => {
            let fields: BTreeMap<String, Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), serde_json_to_value(v)))
                .collect();
            Value::Document(fields)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MemberEntry, key_type_icon, key_type_label, parse_database_name, parse_json_to_value,
        parse_members, parse_stream_entries, render_value_preview, serde_json_to_value,
    };
    use crate::ui::icons::AppIcon;
    use dbflux_core::{KeyEntry, KeyGetResult, KeyType, Value, ValueRepr};

    fn make_result(value: Vec<u8>, repr: ValueRepr) -> KeyGetResult {
        KeyGetResult {
            entry: KeyEntry::new("test-key"),
            value,
            repr,
        }
    }

    // --- parse_database_name ---

    #[test]
    fn parse_database_name_with_db_prefix() {
        assert_eq!(parse_database_name("db0"), Some(0));
        assert_eq!(parse_database_name("db15"), Some(15));
    }

    #[test]
    fn parse_database_name_numeric_only() {
        assert_eq!(parse_database_name("3"), Some(3));
    }

    #[test]
    fn parse_database_name_with_whitespace() {
        assert_eq!(parse_database_name("  db7  "), Some(7));
    }

    #[test]
    fn parse_database_name_invalid() {
        assert_eq!(parse_database_name("abc"), None);
        assert_eq!(parse_database_name(""), None);
        assert_eq!(parse_database_name("db"), None);
    }

    // --- key_type_label ---

    #[test]
    fn key_type_label_covers_all_variants() {
        assert_eq!(key_type_label(KeyType::String), "String");
        assert_eq!(key_type_label(KeyType::Hash), "Hash");
        assert_eq!(key_type_label(KeyType::List), "List");
        assert_eq!(key_type_label(KeyType::Set), "Set");
        assert_eq!(key_type_label(KeyType::SortedSet), "ZSet");
        assert_eq!(key_type_label(KeyType::Json), "JSON");
        assert_eq!(key_type_label(KeyType::Stream), "Stream");
        assert_eq!(key_type_label(KeyType::Bytes), "Bytes");
        assert_eq!(key_type_label(KeyType::Unknown), "?");
    }

    // --- key_type_icon ---

    #[test]
    fn key_type_icon_none_returns_default() {
        let (icon, _) = key_type_icon(None);
        assert!(matches!(icon, AppIcon::KeyRound));
    }

    #[test]
    fn key_type_icon_string_returns_case_sensitive() {
        let (icon, _) = key_type_icon(Some(KeyType::String));
        assert!(matches!(icon, AppIcon::CaseSensitive));
    }

    #[test]
    fn key_type_icon_hash() {
        let (icon, _) = key_type_icon(Some(KeyType::Hash));
        assert!(matches!(icon, AppIcon::Hash));
    }

    #[test]
    fn key_type_icon_list() {
        let (icon, _) = key_type_icon(Some(KeyType::List));
        assert!(matches!(icon, AppIcon::Rows3));
    }

    #[test]
    fn key_type_icon_set() {
        let (icon, _) = key_type_icon(Some(KeyType::Set));
        assert!(matches!(icon, AppIcon::Box));
    }

    #[test]
    fn key_type_icon_sorted_set() {
        let (icon, _) = key_type_icon(Some(KeyType::SortedSet));
        assert!(matches!(icon, AppIcon::ArrowUp));
    }

    #[test]
    fn key_type_icon_json() {
        let (icon, _) = key_type_icon(Some(KeyType::Json));
        assert!(matches!(icon, AppIcon::Braces));
    }

    #[test]
    fn key_type_icon_stream() {
        let (icon, _) = key_type_icon(Some(KeyType::Stream));
        assert!(matches!(icon, AppIcon::Zap));
    }

    // --- render_value_preview ---

    #[test]
    fn render_value_preview_text_short() {
        let result = make_result(b"hello world".to_vec(), ValueRepr::Text);
        assert_eq!(render_value_preview(&result), "hello world");
    }

    #[test]
    fn render_value_preview_text_truncates_at_4000_chars() {
        let long_text = "x".repeat(5000);
        let result = make_result(long_text.into_bytes(), ValueRepr::Text);
        let preview = render_value_preview(&result);
        assert!(preview.ends_with("... (truncated)"));
        assert!(preview.len() < 4100);
    }

    #[test]
    fn render_value_preview_binary() {
        let result = make_result(vec![0xFF; 42], ValueRepr::Binary);
        assert_eq!(render_value_preview(&result), "42 bytes (binary)");
    }

    #[test]
    fn render_value_preview_json() {
        let result = make_result(br#"{"key":"value"}"#.to_vec(), ValueRepr::Json);
        assert_eq!(render_value_preview(&result), r#"{"key":"value"}"#);
    }

    #[test]
    fn render_value_preview_structured() {
        let result = make_result(b"structured data".to_vec(), ValueRepr::Structured);
        assert_eq!(render_value_preview(&result), "structured data");
    }

    #[test]
    fn render_value_preview_stream() {
        let result = make_result(b"stream data".to_vec(), ValueRepr::Stream);
        assert_eq!(render_value_preview(&result), "stream data");
    }

    // --- parse_members ---

    #[test]
    fn parse_members_hash_object() {
        let result = make_result(
            br#"{"field1":"value1","field2":"value2"}"#.to_vec(),
            ValueRepr::Structured,
        );
        let members = parse_members(&result);
        assert_eq!(members.len(), 2);
        assert!(
            members
                .iter()
                .any(|m| m.field.as_deref() == Some("field1") && m.display == "value1")
        );
    }

    #[test]
    fn parse_members_sorted_set_array() {
        let result = make_result(
            br#"[{"member":"alice","score":1.5},{"member":"bob","score":2.0}]"#.to_vec(),
            ValueRepr::Structured,
        );
        let members = parse_members(&result);
        assert_eq!(members.len(), 2);
        assert_eq!(members[0].display, "alice");
        assert_eq!(members[0].score, Some(1.5));
    }

    #[test]
    fn parse_members_set_string_array() {
        let result = make_result(br#"["a","b","c"]"#.to_vec(), ValueRepr::Structured);
        let members = parse_members(&result);
        assert_eq!(members.len(), 3);
        assert_eq!(members[0].display, "a");
    }

    #[test]
    fn parse_members_non_structured_returns_raw() {
        let result = make_result(b"raw text".to_vec(), ValueRepr::Text);
        let members = parse_members(&result);
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].display, "raw text");
    }

    #[test]
    fn parse_members_invalid_json_falls_back() {
        let result = make_result(b"not json".to_vec(), ValueRepr::Structured);
        let members = parse_members(&result);
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].display, "not json");
    }

    #[test]
    fn parse_members_stream_delegates_to_stream_parser() {
        let result = make_result(
            br#"[{"id":"1-0","fields":{"key":"val"}}]"#.to_vec(),
            ValueRepr::Stream,
        );
        let members = parse_members(&result);
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].display, "1-0");
        assert_eq!(members[0].entry_id, Some("1-0".to_string()));
    }

    // --- parse_stream_entries ---

    #[test]
    fn parse_stream_entries_valid() {
        let json = br#"[{"id":"1-0","fields":{"key":"val"}}]"#;
        let entries = parse_stream_entries(json);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].display, "1-0");
        assert_eq!(entries[0].entry_id, Some("1-0".to_string()));
        assert!(entries[0].field.as_ref().unwrap().contains("key"));
    }

    #[test]
    fn parse_stream_entries_missing_id_skips() {
        let json = br#"[{"fields":{"key":"val"}}]"#;
        let entries = parse_stream_entries(json);
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn parse_stream_entries_invalid_json() {
        let entries = parse_stream_entries(b"not json");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].display, "not json");
    }

    #[test]
    fn parse_stream_entries_multiple() {
        let json = br#"[{"id":"1-0","fields":{"a":"1"}},{"id":"2-0","fields":{"b":"2"}}]"#;
        let entries = parse_stream_entries(json);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].display, "1-0");
        assert_eq!(entries[1].display, "2-0");
    }

    // --- serde_json_to_value ---

    #[test]
    fn serde_json_to_value_null() {
        let v = serde_json_to_value(&serde_json::Value::Null);
        assert!(matches!(v, Value::Null));
    }

    #[test]
    fn serde_json_to_value_bool() {
        let v = serde_json_to_value(&serde_json::json!(true));
        assert!(matches!(v, Value::Bool(true)));
    }

    #[test]
    fn serde_json_to_value_integer() {
        let v = serde_json_to_value(&serde_json::json!(42));
        assert!(matches!(v, Value::Int(42)));
    }

    #[test]
    fn serde_json_to_value_float() {
        let v = serde_json_to_value(&serde_json::json!(3.14));
        assert!(matches!(v, Value::Float(f) if (f - 3.14).abs() < f64::EPSILON));
    }

    #[test]
    fn serde_json_to_value_string() {
        let v = serde_json_to_value(&serde_json::json!("hello"));
        assert!(matches!(v, Value::Text(s) if s == "hello"));
    }

    #[test]
    fn serde_json_to_value_array() {
        let v = serde_json_to_value(&serde_json::json!([1, 2, 3]));
        match v {
            Value::Array(arr) => assert_eq!(arr.len(), 3),
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn serde_json_to_value_nested_object() {
        let v = serde_json_to_value(&serde_json::json!({"a": [1, "b"]}));
        match v {
            Value::Document(map) => {
                assert!(map.contains_key("a"));
                assert!(matches!(map["a"], Value::Array(_)));
            }
            _ => panic!("expected Document"),
        }
    }

    // --- parse_json_to_value ---

    #[test]
    fn parse_json_to_value_valid() {
        let v = parse_json_to_value(r#"{"x":1}"#);
        assert!(matches!(v, Value::Document(_)));
    }

    #[test]
    fn parse_json_to_value_invalid_falls_back_to_text() {
        let v = parse_json_to_value("not json");
        assert!(matches!(v, Value::Text(s) if s == "not json"));
    }

    #[test]
    fn parse_json_to_value_array() {
        let v = parse_json_to_value("[1, 2, 3]");
        assert!(matches!(v, Value::Array(_)));
    }

    #[test]
    fn parse_json_to_value_scalar() {
        let v = parse_json_to_value("42");
        assert!(matches!(v, Value::Int(42)));
    }
}
