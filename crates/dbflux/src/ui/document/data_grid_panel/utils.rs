use super::DataGridPanel;
use dbflux_core::Value;
use gpui::Context;

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
    }
}

impl DataGridPanel {
    pub(super) fn get_column_default(&self, col: usize, cx: &Context<Self>) -> Option<String> {
        let (profile_id, table_ref) = match &self.source {
            super::DataSource::Table {
                profile_id, table, ..
            } => (*profile_id, table),
            super::DataSource::Collection { .. } => return None,
            super::DataSource::QueryResult { .. } => return None,
        };

        let col_name = self.result.columns.get(col)?.name.clone();

        let state = self.app_state.read(cx);
        let connected = state.connections().get(&profile_id)?;
        let database = connected.active_database.as_deref().unwrap_or("default");
        let cache_key = (database.to_string(), table_ref.name.clone());
        let table_info = connected.table_details.get(&cache_key)?;
        let columns = table_info.columns.as_deref()?;

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
        let (profile_id, table_ref) = match &self.source {
            super::DataSource::Table {
                profile_id, table, ..
            } => (*profile_id, table),
            super::DataSource::Collection { .. } => return None,
            super::DataSource::QueryResult { .. } => return None,
        };

        let state = self.app_state.read(cx);
        let connected = state.connections().get(&profile_id)?;
        let database = connected.active_database.as_deref().unwrap_or("default");
        let cache_key = (database.to_string(), table_ref.name.clone());
        let table_info = connected.table_details.get(&cache_key)?;

        table_info.columns.clone()
    }

    pub(super) fn get_all_column_defaults(&self, cx: &Context<Self>) -> Vec<Option<String>> {
        let (profile_id, table_ref) = match &self.source {
            super::DataSource::Table {
                profile_id, table, ..
            } => (*profile_id, table),
            super::DataSource::Collection { .. } => {
                return vec![None; self.result.columns.len()];
            }
            super::DataSource::QueryResult { .. } => {
                return vec![None; self.result.columns.len()];
            }
        };

        let state = self.app_state.read(cx);
        let connected = match state.connections().get(&profile_id) {
            Some(c) => c,
            None => return vec![None; self.result.columns.len()],
        };

        let database = connected.active_database.as_deref().unwrap_or("default");
        let cache_key = (database.to_string(), table_ref.name.clone());
        let table_info = match connected.table_details.get(&cache_key) {
            Some(t) => t,
            None => return vec![None; self.result.columns.len()],
        };

        let columns = match table_info.columns.as_deref() {
            Some(c) => c,
            None => return vec![None; self.result.columns.len()],
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
