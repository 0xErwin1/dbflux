//! Flattens a page of documents into grid columns.
//!
//! Top-level fields become columns in document order with `_id` first. An
//! object column the user expanded is replaced by one column per child field
//! under a shared group header; other objects and arrays stay one column that
//! shows a summary. Stepping into a nested value shows its contents as rows:
//! the elements of an array, or the single object.

use std::collections::{BTreeSet, HashSet};

use dbflux_core::{
    ColumnKind, ColumnMeta, QueryResult, Value, field_path_to_dotted, value_at_path,
};

/// A column of the flattened grid.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FlatColumn {
    /// Path of the field below the row's root.
    pub path: Vec<String>,
    /// Header label: the dotted path, or `.child` under a group.
    pub label: String,
    /// Short type of the values (`str`, `dec`, `obj`).
    pub type_label: String,
    /// Dotted path of the expanded object this column belongs to.
    pub group: Option<String>,
}

impl FlatColumn {
    pub fn dotted(&self) -> String {
        field_path_to_dotted(&self.path)
    }
}

/// One cell of the flattened grid.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FlatCell {
    Value(Value),
    /// The row's document has no such field.
    Missing,
}

impl FlatCell {
    pub fn value(&self) -> Option<&Value> {
        match self {
            FlatCell::Value(value) => Some(value),
            FlatCell::Missing => None,
        }
    }
}

/// One row of the flattened grid and where it lives in the loaded page.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FlatRow {
    /// Index of the document in the page.
    pub document: usize,
    /// Path of the row's root inside the document: empty for a document row,
    /// `["items", "2"]` for the third element of `items`.
    pub base_path: Vec<String>,
    pub cells: Vec<FlatCell>,
}

/// The flattened grid.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct FlatView {
    pub columns: Vec<FlatColumn>,
    pub rows: Vec<FlatRow>,
}

impl FlatView {
    /// The flattened grid as a result the grid machinery understands. Missing
    /// fields read as null here; the table model keeps them apart.
    pub fn to_query_result(
        &self,
        source: &QueryResult,
        identity_columns: &[String],
    ) -> QueryResult {
        let columns = self
            .columns
            .iter()
            .map(|column| ColumnMeta {
                name: column.dotted(),
                type_name: column.type_label.clone(),
                kind: column_kind(&column.type_label),
                nullable: true,
                is_primary_key: identity_columns.contains(&column.dotted()),
            })
            .collect();

        let rows = self
            .rows
            .iter()
            .map(|row| {
                row.cells
                    .iter()
                    .map(|cell| cell.value().cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect();

        let mut result = QueryResult::json(columns, rows, source.execution_time);
        result.affected_rows = source.affected_rows;
        result
    }
}

/// Where the grid currently looks: the page's documents, or a nested value
/// of one of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StepTarget {
    pub document: usize,
    pub path: Vec<String>,
}

/// Documents of a page as values, rebuilt from the driver's rows. Top-level
/// fields that hold null are kept, so a document shows exactly the fields the
/// driver reported.
pub(crate) fn documents_from_result(result: &QueryResult) -> Vec<Value> {
    result
        .rows
        .iter()
        .map(|row| {
            Value::Document(
                result
                    .columns
                    .iter()
                    .zip(row.iter())
                    .map(|(column, value)| (column.name.clone(), value.clone()))
                    .collect(),
            )
        })
        .collect()
}

/// Flattens `documents`.
///
/// `top_order` is the field order of the page (the driver's column order),
/// `expanded` holds the dotted paths of the object columns shown as groups,
/// relative to the rows being shown, and `step` selects a nested value to
/// show instead of the documents.
pub(crate) fn flatten(
    documents: &[Value],
    top_order: &[String],
    expanded: &BTreeSet<String>,
    step: Option<&StepTarget>,
) -> FlatView {
    let row_roots = row_roots(documents, step);

    let first_level_order: Vec<String> = match step {
        None => ordered_with_id_first(top_order),
        Some(_) => union_keys(row_roots.iter().map(|(_, _, value)| *value)),
    };

    let scalar_rows = step.is_some()
        && !row_roots.is_empty()
        && row_roots
            .iter()
            .all(|(_, _, value)| !matches!(value, Value::Document(_)));

    let mut columns = Vec::new();

    if scalar_rows {
        columns.push(FlatColumn {
            path: Vec::new(),
            label: "value".to_string(),
            type_label: dominant_type_label(row_roots.iter().map(|(_, _, value)| Some(*value))),
            group: None,
        });
    } else {
        let roots: Vec<&Value> = row_roots.iter().map(|(_, _, value)| *value).collect();
        push_columns(
            &mut columns,
            &roots,
            &[],
            &first_level_order,
            expanded,
            None,
        );
    }

    let rows = row_roots
        .iter()
        .map(|(document, base_path, root)| FlatRow {
            document: *document,
            base_path: base_path.clone(),
            cells: columns
                .iter()
                .map(|column| match value_at_path(root, &column.path) {
                    Some(value) => FlatCell::Value(value.clone()),
                    None => FlatCell::Missing,
                })
                .collect(),
        })
        .collect();

    FlatView { columns, rows }
}

/// `(document, base path, root value)` for each row to show.
fn row_roots<'a>(
    documents: &'a [Value],
    step: Option<&StepTarget>,
) -> Vec<(usize, Vec<String>, &'a Value)> {
    let Some(step) = step else {
        return documents
            .iter()
            .enumerate()
            .map(|(index, document)| (index, Vec::new(), document))
            .collect();
    };

    let Some(target) = documents
        .get(step.document)
        .and_then(|document| value_at_path(document, &step.path))
    else {
        return Vec::new();
    };

    match target {
        Value::Array(items) => items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let mut base_path = step.path.clone();
                base_path.push(index.to_string());
                (step.document, base_path, item)
            })
            .collect(),
        other => vec![(step.document, step.path.clone(), other)],
    }
}

fn push_columns(
    columns: &mut Vec<FlatColumn>,
    roots: &[&Value],
    prefix: &[String],
    keys: &[String],
    expanded: &BTreeSet<String>,
    group: Option<&str>,
) {
    for key in keys {
        let mut path = prefix.to_vec();
        path.push(key.clone());
        let dotted = field_path_to_dotted(&path);

        let values: Vec<Option<&Value>> = roots
            .iter()
            .map(|root| value_at_path(root, &path))
            .collect();

        let is_object = values
            .iter()
            .flatten()
            .any(|value| matches!(value, Value::Document(_)));

        if is_object && expanded.contains(&dotted) {
            let child_keys = union_keys(values.iter().flatten().copied());
            push_columns(columns, roots, &path, &child_keys, expanded, Some(&dotted));
            continue;
        }

        let label = match group {
            Some(_) => format!(".{key}"),
            None => dotted.clone(),
        };

        columns.push(FlatColumn {
            path,
            label,
            type_label: dominant_type_label(values.into_iter()),
            group: group.map(str::to_string),
        });
    }
}

/// Keys of the documents among `values`, in the order they first appear.
fn union_keys<'a>(values: impl Iterator<Item = &'a Value>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut keys = Vec::new();

    for value in values {
        if let Value::Document(fields) = value {
            for key in fields.keys() {
                if seen.insert(key.clone()) {
                    keys.push(key.clone());
                }
            }
        }
    }

    ordered_with_id_first(&keys)
}

fn ordered_with_id_first(keys: &[String]) -> Vec<String> {
    let mut ordered: Vec<String> = keys.to_vec();
    if let Some(position) = ordered.iter().position(|key| key == "_id") {
        let id = ordered.remove(position);
        ordered.insert(0, id);
    }
    ordered
}

/// Short type of one value, as shown next to a column name.
pub(crate) fn type_label(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Int(_) => "int",
        Value::Float(_) => "dbl",
        Value::Decimal(_) => "dec",
        Value::Text(_) => "str",
        Value::ObjectId(_) => "oid",
        Value::DateTime(_) | Value::Date(_) => "date",
        Value::Time(_) => "time",
        Value::Array(_) => "arr",
        Value::Document(_) => "obj",
        Value::Bytes(_) => "bin",
        Value::Json(_) => "json",
        Value::Unsupported(_) => "?",
    }
}

/// The most common non-null type among `values`; `null` when every value is
/// null or missing.
fn dominant_type_label<'a>(values: impl Iterator<Item = Option<&'a Value>>) -> String {
    let mut counts: Vec<(&'static str, usize)> = Vec::new();

    for value in values.flatten() {
        if value.is_null() {
            continue;
        }

        let label = type_label(value);
        match counts.iter_mut().find(|(existing, _)| *existing == label) {
            Some(entry) => entry.1 += 1,
            None => counts.push((label, 1)),
        }
    }

    counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map_or("null", |(label, _)| label)
        .to_string()
}

fn column_kind(type_label: &str) -> ColumnKind {
    match type_label {
        "int" => ColumnKind::Integer,
        "dbl" | "dec" => ColumnKind::Float,
        "date" => ColumnKind::Timestamp,
        "str" | "oid" => ColumnKind::Text,
        _ => ColumnKind::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn document(entries: &[(&str, Value)]) -> Value {
        Value::Document(
            entries
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect::<BTreeMap<_, _>>(),
        )
    }

    fn product(amount: &str, currency: Option<&str>) -> Value {
        let mut price = vec![("amount", Value::Decimal(amount.into()))];
        if let Some(currency) = currency {
            price.push(("currency", Value::Text(currency.into())));
        }

        document(&[
            ("_id", Value::ObjectId("a".repeat(24))),
            ("sku", Value::Text("CAT-1".into())),
            ("price", document(&price)),
            ("tags", Value::Array(vec![Value::Text("x".into())])),
        ])
    }

    fn order() -> Vec<String> {
        ["sku", "_id", "price", "tags"]
            .iter()
            .map(|key| key.to_string())
            .collect()
    }

    fn labels(view: &FlatView) -> Vec<&str> {
        view.columns
            .iter()
            .map(|column| column.label.as_str())
            .collect()
    }

    #[test]
    fn id_comes_first_then_document_order() {
        let view = flatten(
            &[product("1", Some("USD"))],
            &order(),
            &BTreeSet::new(),
            None,
        );

        assert_eq!(labels(&view), vec!["_id", "sku", "price", "tags"]);
        assert_eq!(
            view.columns
                .iter()
                .map(|c| c.type_label.as_str())
                .collect::<Vec<_>>(),
            vec!["oid", "str", "obj", "arr"]
        );
    }

    #[test]
    fn expanded_object_becomes_a_group_of_child_columns() {
        let expanded: BTreeSet<String> = ["price".to_string()].into_iter().collect();
        let view = flatten(
            &[product("405.00", Some("USD")), product("1", None)],
            &order(),
            &expanded,
            None,
        );

        assert_eq!(
            labels(&view),
            vec!["_id", "sku", ".amount", ".currency", "tags"]
        );
        let amount = &view.columns[2];
        assert_eq!(amount.path, vec!["price".to_string(), "amount".to_string()]);
        assert_eq!(amount.group.as_deref(), Some("price"));
        assert_eq!(amount.type_label, "dec");

        assert_eq!(
            view.rows[0].cells[3],
            FlatCell::Value(Value::Text("USD".into()))
        );
        assert_eq!(view.rows[1].cells[3], FlatCell::Missing);
    }

    #[test]
    fn a_field_absent_from_a_document_is_missing() {
        let with_note = document(&[("_id", Value::Int(1)), ("note", Value::Text("x".into()))]);
        let without_note = document(&[("_id", Value::Int(2))]);
        let top: Vec<String> = vec!["_id".into(), "note".into()];

        let view = flatten(&[with_note, without_note], &top, &BTreeSet::new(), None);

        assert_eq!(view.rows[1].cells[1], FlatCell::Missing);
    }

    #[test]
    fn stepping_into_an_array_of_documents_lists_its_elements() {
        let order_document = document(&[
            ("_id", Value::Int(1)),
            (
                "items",
                Value::Array(vec![
                    document(&[("sku", Value::Text("a".into())), ("qty", Value::Int(1))]),
                    document(&[("sku", Value::Text("b".into()))]),
                ]),
            ),
        ]);

        let step = StepTarget {
            document: 0,
            path: vec!["items".to_string()],
        };
        let view = flatten(
            &[order_document],
            &["_id".to_string(), "items".to_string()],
            &BTreeSet::new(),
            Some(&step),
        );

        assert_eq!(labels(&view), vec!["qty", "sku"]);
        assert_eq!(view.rows.len(), 2);
        assert_eq!(
            view.rows[1].base_path,
            vec!["items".to_string(), "1".to_string()]
        );
        assert_eq!(view.rows[1].cells[0], FlatCell::Missing);
    }

    #[test]
    fn stepping_into_an_array_of_scalars_shows_one_value_column() {
        let step = StepTarget {
            document: 0,
            path: vec!["tags".to_string()],
        };

        let view = flatten(
            &[product("1", None)],
            &order(),
            &BTreeSet::new(),
            Some(&step),
        );

        assert_eq!(labels(&view), vec!["value"]);
        assert_eq!(view.columns[0].path, Vec::<String>::new());
        assert_eq!(
            view.rows[0].base_path,
            vec!["tags".to_string(), "0".to_string()]
        );
    }

    #[test]
    fn query_result_marks_the_identity_column() {
        let source = QueryResult::json(Vec::new(), Vec::new(), std::time::Duration::ZERO);
        let view = flatten(&[product("1", None)], &order(), &BTreeSet::new(), None);

        let result = view.to_query_result(&source, &["_id".to_string()]);

        assert!(result.columns[0].is_primary_key);
        assert!(!result.columns[1].is_primary_key);
        assert_eq!(result.rows[0].len(), 4);
    }
}
