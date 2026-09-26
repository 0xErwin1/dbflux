//! Driver-agnostic editing model for documents.
//!
//! A document edit is described by field paths rather than by native update
//! syntax: [`DocumentPatch`] sets or removes individual paths, and a full
//! replacement carries the whole document. Drivers translate both into their
//! own mutation (for example `$set` / `$unset` or `replaceOne`). The server
//! change check compares the document as the page loaded it with the document
//! the server holds now, and reports which paths moved underneath the edit.

use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::{CollectionRef, Value};

/// Path from the document root to a field, one segment per level. Array
/// elements are addressed by their decimal index.
pub type FieldPath = Vec<String>;

/// Joins a field path with dots, the notation document stores use for nested
/// fields (`price.amount`, `items.0.qty`).
pub fn field_path_to_dotted(path: &[String]) -> String {
    path.join(".")
}

/// Splits a dotted field path into its segments. Empty segments are dropped,
/// so `a..b` and `.a` never produce an empty key.
pub fn parse_field_path(dotted: &str) -> FieldPath {
    dotted
        .split('.')
        .filter(|segment| !segment.is_empty())
        .map(str::to_string)
        .collect()
}

/// Whether one path contains the other: equal paths, or one is an ancestor of
/// the other on segment boundaries (`price` and `price.amount`).
pub fn field_paths_overlap(first: &[String], second: &[String]) -> bool {
    let shared = first.len().min(second.len());
    first.iter().take(shared).eq(second.iter().take(shared))
}

/// The value stored at `path`, or `None` when any segment is absent.
pub fn value_at_path<'a>(value: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut cursor = value;

    for segment in path {
        cursor = match cursor {
            Value::Document(fields) => fields.get(segment)?,
            Value::Array(items) => items.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }

    Some(cursor)
}

/// Stores `new_value` at `path`, creating missing intermediate documents.
/// Returns `false` when the path crosses a scalar or an array index that is out
/// of range, in which case `value` is left unchanged.
pub fn set_value_at_path(value: &mut Value, path: &[String], new_value: Value) -> bool {
    let Some((last, parents)) = path.split_last() else {
        *value = new_value;
        return true;
    };

    let mut cursor = value;

    for segment in parents {
        cursor = match cursor {
            Value::Document(fields) => fields
                .entry(segment.clone())
                .or_insert_with(|| Value::Document(BTreeMap::new())),
            Value::Array(items) => {
                let Some(item) = segment
                    .parse::<usize>()
                    .ok()
                    .and_then(|index| items.get_mut(index))
                else {
                    return false;
                };
                item
            }
            _ => return false,
        };
    }

    match cursor {
        Value::Document(fields) => {
            fields.insert(last.clone(), new_value);
            true
        }
        Value::Array(items) => {
            match last
                .parse::<usize>()
                .ok()
                .and_then(|index| items.get_mut(index))
            {
                Some(item) => {
                    *item = new_value;
                    true
                }
                None => false,
            }
        }
        _ => false,
    }
}

/// Removes the field at `path`. Array elements cannot be removed by path (a
/// document store would leave a hole), so an index target returns `false`.
pub fn remove_value_at_path(value: &mut Value, path: &[String]) -> bool {
    let Some((last, parents)) = path.split_last() else {
        return false;
    };

    let mut cursor = value;

    for segment in parents {
        cursor = match cursor {
            Value::Document(fields) => match fields.get_mut(segment) {
                Some(next) => next,
                None => return false,
            },
            Value::Array(items) => {
                let Some(item) = segment
                    .parse::<usize>()
                    .ok()
                    .and_then(|index| items.get_mut(index))
                else {
                    return false;
                };
                item
            }
            _ => return false,
        };
    }

    match cursor {
        Value::Document(fields) => fields.remove(last).is_some(),
        _ => false,
    }
}

/// One staged change to a single field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldChange {
    Set(Value),
    Unset,
}

/// A partial document update expressed as field paths: the paths to set and
/// the paths to remove. Paths never overlap one another, so a driver can send
/// the whole patch as one update.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct DocumentPatch {
    pub set: Vec<(FieldPath, Value)>,
    pub unset: Vec<FieldPath>,
}

impl DocumentPatch {
    /// Builds a patch from per-field changes.
    ///
    /// A later change to the same path replaces an earlier one. A change below
    /// a path that is itself being set is folded into that value, and a change
    /// below a removed path is dropped, so the result never holds two
    /// overlapping paths (document stores reject such updates).
    pub fn from_changes(changes: impl IntoIterator<Item = (FieldPath, FieldChange)>) -> Self {
        let mut latest: Vec<(FieldPath, FieldChange)> = Vec::new();

        for (path, change) in changes {
            if path.is_empty() {
                continue;
            }

            match latest.iter_mut().find(|(existing, _)| *existing == path) {
                Some(entry) => entry.1 = change,
                None => latest.push((path, change)),
            }
        }

        latest.sort_by_key(|(path, _)| path.len());

        let mut patch = DocumentPatch::default();

        for (path, change) in latest {
            if patch.unset.iter().any(|removed| path.starts_with(removed)) {
                continue;
            }

            let ancestor = patch
                .set
                .iter_mut()
                .find(|(existing, _)| path.len() > existing.len() && path.starts_with(existing));

            if let Some((ancestor_path, ancestor_value)) = ancestor {
                let relative = path.get(ancestor_path.len()..).unwrap_or_default();
                match change {
                    FieldChange::Set(value) => {
                        set_value_at_path(ancestor_value, relative, value);
                    }
                    FieldChange::Unset => {
                        remove_value_at_path(ancestor_value, relative);
                    }
                }
                continue;
            }

            match change {
                FieldChange::Set(value) => patch.set.push((path, value)),
                FieldChange::Unset => patch.unset.push(path),
            }
        }

        patch
    }

    /// The smallest patch that turns `original` into `edited`.
    ///
    /// Nested documents are compared field by field and arrays of equal length
    /// element by element; any other difference sets the whole value. Fields
    /// present only in `original` are removed.
    pub fn diff(original: &Value, edited: &Value) -> Self {
        let mut patch = DocumentPatch::default();
        let mut path = Vec::new();
        diff_into(&mut patch, &mut path, original, edited);
        patch
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty() && self.unset.is_empty()
    }

    /// Number of paths the patch touches.
    pub fn len(&self) -> usize {
        self.set.len() + self.unset.len()
    }

    /// Every path the patch sets or removes, sets first.
    pub fn touched_paths(&self) -> Vec<FieldPath> {
        self.set
            .iter()
            .map(|(path, _)| path.clone())
            .chain(self.unset.iter().cloned())
            .collect()
    }

    /// Applies the patch to a local copy of the document.
    pub fn apply_to(&self, document: &mut Value) {
        for (path, value) in &self.set {
            set_value_at_path(document, path, value.clone());
        }

        for path in &self.unset {
            remove_value_at_path(document, path);
        }
    }
}

fn diff_into(patch: &mut DocumentPatch, path: &mut FieldPath, original: &Value, edited: &Value) {
    if original == edited {
        return;
    }

    match (original, edited) {
        (Value::Document(before), Value::Document(after)) => {
            for key in before.keys() {
                if !after.contains_key(key) {
                    path.push(key.clone());
                    patch.unset.push(path.clone());
                    path.pop();
                }
            }

            for (key, after_value) in after {
                path.push(key.clone());
                match before.get(key) {
                    Some(before_value) => diff_into(patch, path, before_value, after_value),
                    None => patch.set.push((path.clone(), after_value.clone())),
                }
                path.pop();
            }
        }
        (Value::Array(before), Value::Array(after))
            if before.len() == after.len() && !path.is_empty() =>
        {
            for (index, (before_item, after_item)) in before.iter().zip(after).enumerate() {
                path.push(index.to_string());
                diff_into(patch, path, before_item, after_item);
                path.pop();
            }
        }
        _ if path.is_empty() => {
            // A root that is not a document cannot be expressed as field paths.
        }
        _ => patch.set.push((path.clone(), edited.clone())),
    }
}

/// How the server copy of a document compares with the copy the page loaded.
#[derive(Debug, Clone, PartialEq)]
pub enum DocumentServerState {
    /// The server copy matches what the page loaded.
    Unchanged,
    /// The document no longer exists on the server.
    Deleted,
    /// Someone changed the document after the page loaded.
    Changed(ServerChange),
}

/// Paths changed on the server since the page loaded, and the ones among them
/// that the pending edit also writes.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerChange {
    pub changed_paths: Vec<FieldPath>,
    pub overlapping_paths: Vec<FieldPath>,
}

impl ServerChange {
    /// Whether the pending edit touches none of the paths the server changed,
    /// so applying it keeps the other change.
    pub fn can_apply_on_top(&self) -> bool {
        self.overlapping_paths.is_empty()
    }
}

/// Compares the document the page loaded with the document the server holds
/// now, against the paths a pending edit writes.
///
/// `edit_paths` are the paths of a field patch. A full replacement writes every
/// field, so pass `replaces_whole_document = true` and every server change
/// counts as overlapping.
pub fn assess_server_change(
    loaded: &Value,
    current: Option<&Value>,
    edit_paths: &[FieldPath],
    replaces_whole_document: bool,
) -> DocumentServerState {
    let Some(current) = current else {
        return DocumentServerState::Deleted;
    };

    let server_patch = DocumentPatch::diff(loaded, current);
    if server_patch.is_empty() {
        return DocumentServerState::Unchanged;
    }

    let changed_paths = server_patch.touched_paths();

    let overlapping_paths = if replaces_whole_document {
        changed_paths.clone()
    } else {
        changed_paths
            .iter()
            .filter(|changed| {
                edit_paths
                    .iter()
                    .any(|edited| field_paths_overlap(changed, edited))
            })
            .cloned()
            .collect()
    };

    DocumentServerState::Changed(ServerChange {
        changed_paths,
        overlapping_paths,
    })
}

/// Fields that identify one document, usually its primary key (`_id`).
pub type DocumentIdentity = Vec<(String, Value)>;

/// Applies a field patch to the document with `identity`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentPatchRequest {
    pub collection: CollectionRef,
    pub identity: DocumentIdentity,
    pub patch: DocumentPatch,
}

/// Replaces the whole document with `identity` by `document`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentReplaceRequest {
    pub collection: CollectionRef,
    pub identity: DocumentIdentity,
    pub document: Value,
}

/// Reads the current server copy of the document with `identity`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentFetchRequest {
    pub collection: CollectionRef,
    pub identity: DocumentIdentity,
}

/// Turns the text typed into a cell into a value of the type the field held.
///
/// Editing must not change a field's type by accident: a number stays a
/// number, a decimal stays a decimal, a date stays a date. `original` is the
/// value before the edit (`None` for a field the document did not have), and
/// a value that does not parse as that type is an error rather than a silent
/// switch to text.
pub fn coerce_edited_value(original: Option<&Value>, input: &str) -> Result<Value, String> {
    let trimmed = input.trim();

    match original {
        None | Some(Value::Null) => Ok(infer_value(input)),
        Some(Value::Text(_)) => Ok(Value::Text(input.to_string())),
        Some(Value::Bool(_)) => match trimmed.to_ascii_lowercase().as_str() {
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            "null" => Ok(Value::Null),
            _ => Err(format!("\"{trimmed}\" is not true or false")),
        },
        Some(Value::Int(_)) => {
            if trimmed.eq_ignore_ascii_case("null") {
                return Ok(Value::Null);
            }
            trimmed
                .parse::<i64>()
                .map(Value::Int)
                .map_err(|_| format!("\"{trimmed}\" is not a whole number"))
        }
        Some(Value::Float(_)) => {
            if trimmed.eq_ignore_ascii_case("null") {
                return Ok(Value::Null);
            }
            trimmed
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| format!("\"{trimmed}\" is not a number"))
        }
        Some(Value::Decimal(_)) => {
            if trimmed.eq_ignore_ascii_case("null") {
                return Ok(Value::Null);
            }
            if is_decimal_literal(trimmed) {
                Ok(Value::Decimal(trimmed.to_string()))
            } else {
                Err(format!("\"{trimmed}\" is not a decimal number"))
            }
        }
        Some(Value::DateTime(_)) => {
            if trimmed.eq_ignore_ascii_case("null") {
                return Ok(Value::Null);
            }
            parse_date_time(trimmed)
                .map(Value::DateTime)
                .ok_or_else(|| format!("\"{trimmed}\" is not a date and time"))
        }
        Some(Value::Date(_)) => NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
            .map(Value::Date)
            .map_err(|_| format!("\"{trimmed}\" is not a date (YYYY-MM-DD)")),
        Some(Value::Time(_)) => NaiveTime::parse_from_str(trimmed, "%H:%M:%S")
            .map(Value::Time)
            .map_err(|_| format!("\"{trimmed}\" is not a time (HH:MM:SS)")),
        Some(Value::ObjectId(_)) => {
            if trimmed.len() == 24 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
                Ok(Value::ObjectId(trimmed.to_ascii_lowercase()))
            } else {
                Err(format!("\"{trimmed}\" is not a 24-character ObjectId"))
            }
        }
        Some(Value::Document(_)) | Some(Value::Array(_)) | Some(Value::Json(_)) => {
            serde_json::from_str::<serde_json::Value>(trimmed)
                .map(|json| document_json_to_value(&json))
                .map_err(|error| format!("invalid JSON: {error}"))
        }
        Some(Value::Bytes(_)) | Some(Value::Unsupported(_)) => {
            Err("this value cannot be edited as text".to_string())
        }
    }
}

fn infer_value(input: &str) -> Value {
    let trimmed = input.trim();

    if trimmed.eq_ignore_ascii_case("null") {
        return Value::Null;
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }
    if let Ok(integer) = trimmed.parse::<i64>() {
        return Value::Int(integer);
    }
    if let Ok(float) = trimmed.parse::<f64>()
        && float.is_finite()
    {
        return Value::Float(float);
    }
    if (trimmed.starts_with('{') || trimmed.starts_with('[') || trimmed.starts_with('"'))
        && let Ok(json) = serde_json::from_str::<serde_json::Value>(trimmed)
    {
        return document_json_to_value(&json);
    }

    Value::Text(input.to_string())
}

fn is_decimal_literal(text: &str) -> bool {
    let digits = text.strip_prefix(['-', '+']).unwrap_or(text);
    let mut parts = digits.splitn(2, '.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next();

    let whole_ok = whole.chars().all(|c| c.is_ascii_digit());
    let fraction_ok = fraction.is_none_or(|part| part.chars().all(|c| c.is_ascii_digit()));
    let has_digit = whole.chars().any(|c| c.is_ascii_digit())
        || fraction.is_some_and(|part| part.chars().any(|c| c.is_ascii_digit()));

    whole_ok && fraction_ok && has_digit
}

fn parse_date_time(text: &str) -> Option<DateTime<Utc>> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(text) {
        return Some(parsed.with_timezone(&Utc));
    }

    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(text, format) {
            return Some(Utc.from_utc_datetime(&naive));
        }
    }

    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .ok()
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|naive| Utc.from_utc_datetime(&naive))
}

/// Serializes a value as document JSON: plain JSON where the type survives a
/// round trip, and the extended forms (`$oid`, `$date`, `$numberDecimal`,
/// `$binary`) where it would not. [`document_json_to_value`] reads it back to
/// the same value, which is what lets an edited JSON document be compared with
/// the loaded one field by field.
pub fn value_to_document_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(boolean) => serde_json::Value::Bool(*boolean),
        Value::Int(integer) => serde_json::Value::from(*integer),
        Value::Float(float) => serde_json::Number::from_f64(*float)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::json!({ "$numberDouble": float.to_string() })),
        Value::Text(text) => serde_json::Value::String(text.clone()),
        Value::Bytes(bytes) => {
            let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            serde_json::json!({ "$binary": { "hex": hex } })
        }
        Value::Json(text) => {
            serde_json::from_str(text).unwrap_or_else(|_| serde_json::Value::String(text.clone()))
        }
        Value::Decimal(decimal) => serde_json::json!({ "$numberDecimal": decimal }),
        Value::DateTime(date_time) => serde_json::json!({ "$date": date_time.to_rfc3339() }),
        Value::Date(date) => serde_json::Value::String(date.to_string()),
        Value::Time(time) => serde_json::Value::String(time.to_string()),
        Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(value_to_document_json).collect())
        }
        Value::Document(fields) => serde_json::Value::Object(
            fields
                .iter()
                .map(|(key, field)| (key.clone(), value_to_document_json(field)))
                .collect(),
        ),
        Value::ObjectId(object_id) => serde_json::json!({ "$oid": object_id }),
        Value::Unsupported(type_name) => serde_json::json!({ "$unsupported": type_name }),
    }
}

/// Reads document JSON written by [`value_to_document_json`] (or typed by
/// hand in the same notation) back into a value.
pub fn document_json_to_value(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(boolean) => Value::Bool(*boolean),
        serde_json::Value::Number(number) => match number.as_i64() {
            Some(integer) => Value::Int(integer),
            None => number
                .as_f64()
                .map(Value::Float)
                .unwrap_or_else(|| Value::Decimal(number.to_string())),
        },
        serde_json::Value::String(text) => Value::Text(text.clone()),
        serde_json::Value::Array(items) => {
            Value::Array(items.iter().map(document_json_to_value).collect())
        }
        serde_json::Value::Object(object) => {
            if let Some(value) = extended_json_scalar(object) {
                return value;
            }

            Value::Document(
                object
                    .iter()
                    .map(|(key, field)| (key.clone(), document_json_to_value(field)))
                    .collect(),
            )
        }
    }
}

fn extended_json_scalar(object: &serde_json::Map<String, serde_json::Value>) -> Option<Value> {
    if object.len() != 1 {
        return None;
    }

    let (key, inner) = object.iter().next()?;

    match (key.as_str(), inner) {
        ("$oid", serde_json::Value::String(object_id)) => Some(Value::ObjectId(object_id.clone())),
        ("$numberDecimal", serde_json::Value::String(decimal)) => {
            Some(Value::Decimal(decimal.clone()))
        }
        ("$numberLong" | "$numberInt", serde_json::Value::String(integer)) => {
            integer.parse::<i64>().ok().map(Value::Int)
        }
        ("$numberDouble", serde_json::Value::String(float)) => {
            float.parse::<f64>().ok().map(Value::Float)
        }
        ("$date", serde_json::Value::String(text)) => parse_date_time(text).map(Value::DateTime),
        ("$date", serde_json::Value::Number(millis)) => millis
            .as_i64()
            .and_then(DateTime::from_timestamp_millis)
            .map(Value::DateTime),
        ("$date", serde_json::Value::Object(long)) => long
            .get("$numberLong")
            .and_then(serde_json::Value::as_str)
            .and_then(|text| text.parse::<i64>().ok())
            .and_then(DateTime::from_timestamp_millis)
            .map(Value::DateTime),
        ("$binary", serde_json::Value::Object(binary)) => binary
            .get("hex")
            .and_then(serde_json::Value::as_str)
            .and_then(decode_hex)
            .map(Value::Bytes),
        ("$unsupported", serde_json::Value::String(type_name)) => {
            Some(Value::Unsupported(type_name.clone()))
        }
        _ => None,
    }
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }

    (0..text.len())
        .step_by(2)
        .map(|start| {
            text.get(start..start + 2)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
        })
        .collect()
}

/// Distinct field paths of a set of documents, in the order they first appear,
/// with nested document fields expanded (`customer`, `customer.email`). Array
/// elements that are documents contribute their fields under the array path
/// (`items.sku`), the notation document stores accept in filters.
pub fn collect_field_paths(documents: &[Value]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut ordered = Vec::new();

    for document in documents {
        if let Value::Document(fields) = document {
            collect_paths_into(fields, "", &mut seen, &mut ordered);
        }
    }

    ordered
}

fn collect_paths_into(
    fields: &BTreeMap<String, Value>,
    prefix: &str,
    seen: &mut HashSet<String>,
    ordered: &mut Vec<String>,
) {
    for (key, value) in fields {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };

        if seen.insert(path.clone()) {
            ordered.push(path.clone());
        }

        match value {
            Value::Document(nested) => collect_paths_into(nested, &path, seen, ordered),
            Value::Array(items) => {
                for item in items {
                    if let Value::Document(nested) = item {
                        collect_paths_into(nested, &path, seen, ordered);
                    }
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document(entries: &[(&str, Value)]) -> Value {
        Value::Document(
            entries
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
        )
    }

    fn path(dotted: &str) -> FieldPath {
        parse_field_path(dotted)
    }

    #[test]
    fn diff_sets_only_the_changed_nested_field() {
        let original = document(&[
            ("_id", Value::ObjectId("a".repeat(24))),
            (
                "price",
                document(&[
                    ("amount", Value::Decimal("405.00".into())),
                    ("currency", Value::Text("USD".into())),
                ]),
            ),
        ]);

        let mut edited = original.clone();
        set_value_at_path(
            &mut edited,
            &path("price.amount"),
            Value::Decimal("119.00".into()),
        );

        let patch = DocumentPatch::diff(&original, &edited);

        assert_eq!(
            patch.set,
            vec![(path("price.amount"), Value::Decimal("119.00".into()))]
        );
        assert!(patch.unset.is_empty());
    }

    #[test]
    fn diff_unsets_removed_fields_and_sets_added_ones() {
        let original = document(&[
            ("sku", Value::Text("CAT-1".into())),
            ("legacy", Value::Int(1)),
        ]);
        let edited = document(&[
            ("sku", Value::Text("CAT-1".into())),
            ("active", Value::Bool(true)),
        ]);

        let patch = DocumentPatch::diff(&original, &edited);

        assert_eq!(patch.unset, vec![path("legacy")]);
        assert_eq!(patch.set, vec![(path("active"), Value::Bool(true))]);
    }

    #[test]
    fn diff_addresses_array_elements_when_the_length_is_unchanged() {
        let original = document(&[(
            "items",
            Value::Array(vec![
                document(&[("qty", Value::Int(1))]),
                document(&[("qty", Value::Int(2))]),
            ]),
        )]);
        let mut edited = original.clone();
        set_value_at_path(&mut edited, &path("items.1.qty"), Value::Int(5));

        let patch = DocumentPatch::diff(&original, &edited);

        assert_eq!(patch.set, vec![(path("items.1.qty"), Value::Int(5))]);
    }

    #[test]
    fn diff_replaces_an_array_whose_length_changed() {
        let original = document(&[("tags", Value::Array(vec![Value::Text("a".into())]))]);
        let edited = document(&[(
            "tags",
            Value::Array(vec![Value::Text("a".into()), Value::Text("b".into())]),
        )]);

        let patch = DocumentPatch::diff(&original, &edited);

        assert_eq!(patch.set.len(), 1);
        assert_eq!(
            patch.set.first().map(|(p, _)| p.clone()),
            Some(path("tags"))
        );
    }

    #[test]
    fn diff_of_identical_documents_is_empty() {
        let original = document(&[("a", Value::Int(1))]);
        assert!(DocumentPatch::diff(&original, &original.clone()).is_empty());
    }

    #[test]
    fn from_changes_keeps_the_last_change_per_path() {
        let patch = DocumentPatch::from_changes(vec![
            (path("status"), FieldChange::Set(Value::Text("paid".into()))),
            (path("status"), FieldChange::Unset),
        ]);

        assert!(patch.set.is_empty());
        assert_eq!(patch.unset, vec![path("status")]);
    }

    #[test]
    fn from_changes_folds_a_child_change_into_a_set_ancestor() {
        let patch = DocumentPatch::from_changes(vec![
            (
                path("price"),
                FieldChange::Set(document(&[("amount", Value::Int(1))])),
            ),
            (
                path("price.currency"),
                FieldChange::Set(Value::Text("EUR".into())),
            ),
        ]);

        assert_eq!(patch.len(), 1);
        assert_eq!(
            patch.set,
            vec![(
                path("price"),
                document(&[
                    ("amount", Value::Int(1)),
                    ("currency", Value::Text("EUR".into())),
                ]),
            )]
        );
    }

    #[test]
    fn from_changes_drops_a_child_of_a_removed_path() {
        let patch = DocumentPatch::from_changes(vec![
            (path("failure"), FieldChange::Unset),
            (
                path("failure.code"),
                FieldChange::Set(Value::Text("x".into())),
            ),
        ]);

        assert!(patch.set.is_empty());
        assert_eq!(patch.unset, vec![path("failure")]);
    }

    #[test]
    fn apply_to_sets_and_removes_paths() {
        let mut target = document(&[
            ("a", Value::Int(1)),
            ("b", document(&[("c", Value::Int(2))])),
        ]);
        let patch = DocumentPatch {
            set: vec![(path("b.d"), Value::Int(3))],
            unset: vec![path("a")],
        };

        patch.apply_to(&mut target);

        assert_eq!(value_at_path(&target, &path("a")), None);
        assert_eq!(value_at_path(&target, &path("b.d")), Some(&Value::Int(3)));
        assert_eq!(value_at_path(&target, &path("b.c")), Some(&Value::Int(2)));
    }

    #[test]
    fn server_change_on_other_paths_can_apply_on_top() {
        let loaded = document(&[
            ("price", document(&[("amount", Value::Int(405))])),
            ("stock", Value::Int(3)),
        ]);
        let current = document(&[
            ("price", document(&[("amount", Value::Int(405))])),
            ("stock", Value::Int(2)),
        ]);

        let state = assess_server_change(&loaded, Some(&current), &[path("price.amount")], false);

        let DocumentServerState::Changed(change) = state else {
            panic!("expected a server change, got {state:?}");
        };
        assert_eq!(change.changed_paths, vec![path("stock")]);
        assert!(change.can_apply_on_top());
    }

    #[test]
    fn server_change_on_the_edited_path_overlaps() {
        let loaded = document(&[("price", document(&[("amount", Value::Int(405))]))]);
        let current = document(&[("price", document(&[("amount", Value::Int(400))]))]);

        let state = assess_server_change(&loaded, Some(&current), &[path("price")], false);

        let DocumentServerState::Changed(change) = state else {
            panic!("expected a server change, got {state:?}");
        };
        assert_eq!(change.overlapping_paths, vec![path("price.amount")]);
        assert!(!change.can_apply_on_top());
    }

    #[test]
    fn a_whole_document_replacement_overlaps_every_server_change() {
        let loaded = document(&[("stock", Value::Int(3))]);
        let current = document(&[("stock", Value::Int(2))]);

        let state = assess_server_change(&loaded, Some(&current), &[], true);

        let DocumentServerState::Changed(change) = state else {
            panic!("expected a server change, got {state:?}");
        };
        assert!(!change.can_apply_on_top());
    }

    #[test]
    fn server_state_reports_unchanged_and_deleted_documents() {
        let loaded = document(&[("a", Value::Int(1))]);

        assert_eq!(
            assess_server_change(&loaded, Some(&loaded.clone()), &[path("a")], false),
            DocumentServerState::Unchanged
        );
        assert_eq!(
            assess_server_change(&loaded, None, &[path("a")], false),
            DocumentServerState::Deleted
        );
    }

    #[test]
    fn coerce_keeps_the_original_type() {
        assert_eq!(
            coerce_edited_value(Some(&Value::Decimal("405.00".into())), "119.00"),
            Ok(Value::Decimal("119.00".into()))
        );
        assert_eq!(
            coerce_edited_value(Some(&Value::Int(3)), "7"),
            Ok(Value::Int(7))
        );
        assert_eq!(
            coerce_edited_value(Some(&Value::Bool(false)), "TRUE"),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            coerce_edited_value(Some(&Value::Text("team".into())), "42"),
            Ok(Value::Text("42".into()))
        );
        assert_eq!(
            coerce_edited_value(Some(&Value::Float(1.5)), "2"),
            Ok(Value::Float(2.0))
        );
        assert!(
            coerce_edited_value(Some(&Value::Int(3)), "7.5").is_err(),
            "an integer field must not become a double"
        );
        assert!(coerce_edited_value(Some(&Value::Decimal("1".into())), "abc").is_err());
        assert!(coerce_edited_value(Some(&Value::ObjectId("a".repeat(24))), "xyz").is_err());
    }

    #[test]
    fn coerce_parses_dates_in_the_grid_format() {
        let parsed = coerce_edited_value(Some(&Value::DateTime(Utc::now())), "2026-09-22 14:02:00");

        let Ok(Value::DateTime(date_time)) = parsed else {
            panic!("expected a date time, got {parsed:?}");
        };
        assert_eq!(date_time.to_rfc3339(), "2026-09-22T14:02:00+00:00");
    }

    #[test]
    fn coerce_infers_a_type_for_a_new_field() {
        assert_eq!(coerce_edited_value(None, "12"), Ok(Value::Int(12)));
        assert_eq!(coerce_edited_value(None, "1.5"), Ok(Value::Float(1.5)));
        assert_eq!(
            coerce_edited_value(None, "hello"),
            Ok(Value::Text("hello".into()))
        );
        assert_eq!(coerce_edited_value(None, "null"), Ok(Value::Null));
    }

    #[test]
    fn document_json_round_trips_typed_values() {
        let original = document(&[
            ("_id", Value::ObjectId("66f0c2a1e4b0c2a1e4b0c2a1".into())),
            ("total", Value::Decimal("1284.00".into())),
            ("rating", Value::Float(3.0)),
            ("qty", Value::Int(2)),
            (
                "created_at",
                Value::DateTime(Utc.with_ymd_and_hms(2026, 9, 22, 14, 2, 11).unwrap()),
            ),
            ("tags", Value::Array(vec![Value::Text("a".into())])),
            ("blob", Value::Bytes(vec![0, 255])),
        ]);

        let json = value_to_document_json(&original);
        let text = serde_json::to_string(&json).expect("serializes");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parses");

        assert_eq!(document_json_to_value(&parsed), original);
    }

    #[test]
    fn collect_field_paths_follows_first_appearance_and_nesting() {
        let first = document(&[
            ("_id", Value::Int(1)),
            ("customer", document(&[("email", Value::Text("a".into()))])),
            (
                "items",
                Value::Array(vec![document(&[("sku", Value::Text("x".into()))])]),
            ),
        ]);
        let second = document(&[("status", Value::Text("paid".into()))]);

        let paths = collect_field_paths(&[first, second]);

        assert_eq!(
            paths,
            vec![
                "_id",
                "customer",
                "customer.email",
                "items",
                "items.sku",
                "status"
            ]
        );
    }

    #[test]
    fn overlapping_paths_match_on_segment_boundaries() {
        assert!(field_paths_overlap(&path("price"), &path("price.amount")));
        assert!(field_paths_overlap(
            &path("price.amount"),
            &path("price.amount")
        ));
        assert!(!field_paths_overlap(&path("price"), &path("prices")));
        assert!(!field_paths_overlap(
            &path("price.amount"),
            &path("price.currency")
        ));
    }
}
