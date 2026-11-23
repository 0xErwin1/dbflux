use super::DataGridPanel;
use dbflux_core::{ColumnMeta, DocumentFilter, QueryResult, Value};
use gpui::{App, Context};

/// Returns `(column_index, column_name)` for every column marked as a primary key.
/// Used by mutations and context-menu handlers to build document filters generically.
pub(super) fn extract_pk_columns(result: &QueryResult) -> Vec<(usize, String)> {
    result
        .columns
        .iter()
        .enumerate()
        .filter(|(_, col)| col.is_primary_key)
        .map(|(idx, col)| (idx, col.name.clone()))
        .collect()
}

/// Filter that keys a document by its `_id` column, read through `value_at`.
/// `None` when the result has no `_id` column or the value cannot key a
/// document (null, nested, binary, or a non-finite float, which JSON would
/// turn into `null` and so match the wrong document).
pub(super) fn document_id_filter(
    columns: &[ColumnMeta],
    value_at: impl FnOnce(usize) -> Option<Value>,
) -> Option<DocumentFilter> {
    let index = columns.iter().position(|column| column.name == "_id")?;

    let id = match value_at(index)? {
        Value::ObjectId(oid) => serde_json::json!({ "$oid": oid }),
        Value::Text(text) => serde_json::json!(text),
        Value::Int(number) => serde_json::json!(number),
        Value::Float(number) if number.is_finite() => serde_json::json!(number),
        Value::Bool(flag) => serde_json::json!(flag),
        _ => return None,
    };

    Some(DocumentFilter::new(serde_json::json!({ "_id": id })))
}

/// Joins a multi-line query onto one line, collapsing every whitespace run to
/// a single space, so it fits a one-row toolbar label.
pub(super) fn single_line(query: &str) -> String {
    query.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(super) fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::json!(*f),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            serde_json::json!({"$binary": {"hex": hex}})
        }
        Value::Json(j) => serde_json::from_str(j).unwrap_or(serde_json::Value::String(j.clone())),
        Value::Decimal(d) => serde_json::Value::String(d.clone()),
        Value::DateTime(dt) => serde_json::json!({"$date": dt.to_rfc3339()}),
        Value::Date(d) => serde_json::Value::String(d.to_string()),
        Value::Time(t) => serde_json::Value::String(t.to_string()),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Document(doc) => {
            let map: serde_json::Map<String, serde_json::Value> = doc
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        Value::ObjectId(oid) => serde_json::json!({"$oid": oid}),
        Value::Unsupported(type_name) => serde_json::json!({"$unsupported": type_name}),
    }
}

impl DataGridPanel {
    /// Database the connection's `table_details` cache is keyed by for the table
    /// this panel shows.
    ///
    /// The table's own database leads — a table opened from the sidebar carries
    /// it even when the connection recorded no active database, because
    /// connecting leaves `active_database` unset and only clicking the database
    /// node sets it. This is the key the fetch that fills the cache writes with,
    /// so every reader must build it the same way.
    pub(super) fn table_details_database(
        connected: &dbflux_core::ConnectedProfile,
        database: Option<&str>,
    ) -> String {
        database
            .or(connected.active_database.as_deref())
            .unwrap_or("default")
            .to_string()
    }

    /// Cached details of the table this panel shows, keyed exactly as
    /// [`DataGridPanel::table_details_database`] documents.
    ///
    /// Readers that build the key tuple themselves can drift from the writer —
    /// that drift is what left a reopened table read-only — so they read the
    /// entry through here instead.
    pub(super) fn table_details_for<'a>(
        &'a self,
        cx: &'a App,
    ) -> Option<&'a dbflux_core::TableInfo> {
        let (profile_id, table_ref, database) = match &self.source {
            super::DataSource::Table {
                profile_id,
                database,
                table,
                ..
            } => (*profile_id, table, database.as_deref()),
            super::DataSource::Collection { .. } | super::DataSource::QueryResult { .. } => {
                return None;
            }
        };

        let state = self.app_state.read(cx);
        let connected = state.connections().get(&profile_id)?;
        let cache_key = (
            Self::table_details_database(connected, database),
            table_ref.schema.clone(),
            table_ref.name.clone(),
        );
        connected.table_details.get(&cache_key)
    }

    pub(super) fn get_column_default(&self, col: usize, cx: &Context<Self>) -> Option<String> {
        let col_name = self.result.columns.get(col)?.name.clone();
        let columns = self.table_details_for(cx)?.columns.as_deref()?;

        columns
            .iter()
            .find(|c| c.name == col_name)
            .and_then(|c| c.default_value.clone())
    }

    /// Returns the cached `ColumnInfo` list for the current table, if available.
    pub(super) fn get_column_details(
        &self,
        cx: &Context<Self>,
    ) -> Option<Vec<dbflux_core::ColumnInfo>> {
        self.table_details_for(cx)?.columns.clone()
    }

    /// Returns the set of local column names that are FK source columns for the
    /// current table, using the cached `TableInfo.foreign_keys`.
    pub(super) fn get_fk_column_names(
        &self,
        cx: &Context<Self>,
    ) -> std::collections::HashSet<String> {
        let Some(details) = self.table_details_for(cx) else {
            return std::collections::HashSet::new();
        };

        details
            .foreign_keys
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .flat_map(|fk| fk.columns.iter().cloned())
            .collect()
    }

    pub(super) fn get_all_column_defaults(&self, cx: &Context<Self>) -> Vec<Option<String>> {
        let Some(columns) = self
            .table_details_for(cx)
            .and_then(|d| d.columns.as_deref())
        else {
            return vec![None; self.result.columns.len()];
        };

        // Map result columns to their defaults
        self.result
            .columns
            .iter()
            .map(|col| {
                columns
                    .iter()
                    .find(|c| c.name == col.name)
                    .and_then(|c| c.default_value.clone())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{document_id_filter, single_line};
    use dbflux_core::{ColumnKind, ColumnMeta, Value};

    fn column(name: &str) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            type_name: String::new(),
            kind: ColumnKind::Unknown,
            nullable: true,
            is_primary_key: false,
        }
    }

    #[test]
    fn document_id_filter_keys_a_numeric_id() {
        let columns = [column("name"), column("_id")];
        let row = [Value::Text("first".into()), Value::Int(7)];

        let filter = document_id_filter(&columns, |index| row.get(index).cloned());

        assert_eq!(
            filter.map(|filter| filter.filter),
            Some(serde_json::json!({ "_id": 7 }))
        );
    }

    #[test]
    fn document_id_filter_rejects_a_non_finite_float_id() {
        let columns = [column("_id")];

        for number in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let row = [Value::Float(number)];
            assert!(document_id_filter(&columns, |index| row.get(index).cloned()).is_none());
        }
    }

    #[test]
    fn document_id_filter_needs_an_id_column() {
        let columns = [column("name")];
        let row = [Value::Text("first".into())];

        assert!(document_id_filter(&columns, |index| row.get(index).cloned()).is_none());
    }

    #[test]
    fn single_line_collapses_a_multi_line_query() {
        let flux = "from(bucket: \"metrics\")\n  |> range(start: -24h)\n  |> limit(n: 100)";

        assert_eq!(
            single_line(flux),
            "from(bucket: \"metrics\") |> range(start: -24h) |> limit(n: 100)"
        );
    }
}
