use dbflux_core::{
    CollectionPresentation, ColumnInfo, Connection, DatabaseInfo, DbError, DbSchemaInfo, TableInfo,
    TableStorageHint, Value, ViewInfo,
};

use crate::connection::ClickHouseConnection;
use crate::dialect::CLICKHOUSE_DIALECT;
use crate::types::{clickhouse_type_is_nullable, parse_clickhouse_type};
use dbflux_core::SqlDialect;

const DATABASES_SQL: &str = "SELECT name FROM system.databases ORDER BY name";

pub(crate) fn list_databases(
    connection: &ClickHouseConnection,
) -> Result<Vec<DatabaseInfo>, DbError> {
    let current = connection.active_database();
    let result = connection.execute_sql(DATABASES_SQL, Some("system"), None, None, None)?;
    Ok(result
        .rows
        .iter()
        .filter_map(|row| text_at(row, 0))
        .map(|name| DatabaseInfo {
            is_current: current.as_deref() == Some(name),
            name: name.to_string(),
        })
        .collect())
}

pub(crate) fn schema_for_database(
    connection: &ClickHouseConnection,
    database: &str,
) -> Result<DbSchemaInfo, DbError> {
    let database_literal = CLICKHOUSE_DIALECT.value_to_literal(&Value::Text(database.to_string()));
    let sql = format!(
        "SELECT name, engine FROM system.tables WHERE database = {database_literal} ORDER BY name"
    );
    let result = connection.execute_sql(&sql, Some("system"), None, None, None)?;
    let mut tables = Vec::new();
    let mut views = Vec::new();
    for row in &result.rows {
        let (Some(name), Some(engine)) = (text_at(row, 0), text_at(row, 1)) else {
            continue;
        };
        if is_view_engine(engine) {
            views.push(ViewInfo {
                name: name.to_string(),
                schema: Some(database.to_string()),
            });
        } else {
            tables.push(shallow_table(database, name));
        }
    }
    Ok(DbSchemaInfo {
        name: database.to_string(),
        tables,
        views,
        custom_types: None,
    })
}

pub(crate) fn table_details(
    connection: &ClickHouseConnection,
    database: &str,
    table: &str,
) -> Result<TableInfo, DbError> {
    let database_literal = CLICKHOUSE_DIALECT.value_to_literal(&Value::Text(database.to_string()));
    let table_literal = CLICKHOUSE_DIALECT.value_to_literal(&Value::Text(table.to_string()));
    let columns_sql = format!(
        "SELECT name, type, default_kind, default_expression, is_in_primary_key, is_in_sorting_key, \
         is_in_partition_key, is_in_sampling_key, compression_codec \
         FROM system.columns WHERE database = {database_literal} AND table = {table_literal} \
         ORDER BY position"
    );
    let table_sql = format!(
        "SELECT engine, partition_key, sorting_key, primary_key, sampling_key, total_rows, total_bytes \
         FROM system.tables WHERE database = {database_literal} AND name = {table_literal} LIMIT 1"
    );

    let column_result = connection.execute_sql(&columns_sql, Some("system"), None, None, None)?;
    let table_result = connection.execute_sql(&table_sql, Some("system"), None, Some(1), None)?;
    if column_result.rows.is_empty() && table_result.rows.is_empty() {
        return Err(DbError::ObjectNotFound(
            format!("ClickHouse table {database}.{table} was not found").into(),
        ));
    }

    let columns = column_result
        .rows
        .iter()
        .filter_map(|row| column_info(row))
        .collect();
    let storage_hints = table_result
        .rows
        .first()
        .map(|row| build_storage_hints(row, &column_result.rows))
        .unwrap_or_default();

    Ok(TableInfo {
        name: table.to_string(),
        schema: Some(database.to_string()),
        columns: Some(columns),
        indexes: None,
        foreign_keys: None,
        constraints: None,
        sample_fields: None,
        presentation: CollectionPresentation::DataGrid,
        child_items: None,
        storage_hints: Some(storage_hints),
    })
}

fn column_info(row: &[Value]) -> Option<ColumnInfo> {
    let name = text_at(row, 0)?;
    let type_name = text_at(row, 1)?;
    let data_type = parse_clickhouse_type(type_name);
    Some(ColumnInfo {
        name: name.to_string(),
        type_name: type_name.to_string(),
        nullable: clickhouse_type_is_nullable(&data_type),
        is_primary_key: false,
        default_value: (text_at(row, 2) == Some("DEFAULT"))
            .then(|| text_at(row, 3))
            .flatten()
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        enum_values: None,
    })
}

fn shallow_table(database: &str, name: &str) -> TableInfo {
    TableInfo {
        name: name.to_string(),
        schema: Some(database.to_string()),
        columns: None,
        indexes: None,
        foreign_keys: None,
        constraints: None,
        sample_fields: None,
        presentation: CollectionPresentation::DataGrid,
        child_items: None,
        storage_hints: None,
    }
}

fn is_view_engine(engine: &str) -> bool {
    matches!(
        engine,
        "View" | "MaterializedView" | "LiveView" | "WindowView"
    )
}

fn build_storage_hints(row: &[Value], column_rows: &[Vec<Value>]) -> Vec<TableStorageHint> {
    let mut hints = Vec::new();
    push_detail_hint(&mut hints, "Engine", text_at(row, 0));
    push_detail_hint(&mut hints, "Partition Key", text_at(row, 1));
    push_detail_hint(&mut hints, "Sorting Key", text_at(row, 2));
    push_detail_hint(&mut hints, "Primary Key", text_at(row, 3));
    push_detail_hint(&mut hints, "Sampling Key", text_at(row, 4));

    let total_rows = integer_text_at(row, 5);
    let total_bytes = integer_text_at(row, 6);
    if total_rows.is_some() || total_bytes.is_some() {
        hints.push(TableStorageHint {
            label: "Stored Data".to_string(),
            columns: Vec::new(),
            detail: Some(format!(
                "{} rows, {} bytes",
                total_rows.as_deref().unwrap_or("unknown"),
                total_bytes.as_deref().unwrap_or("unknown")
            )),
        });
    }

    let codec_columns = column_rows
        .iter()
        .filter_map(|row| {
            let column = text_at(row, 0)?;
            let codec = text_at(row, 8)?;
            (!codec.is_empty()).then(|| format!("{column}: {codec}"))
        })
        .collect::<Vec<_>>();
    if !codec_columns.is_empty() {
        hints.push(TableStorageHint {
            label: "Compression".to_string(),
            columns: Vec::new(),
            detail: Some(codec_columns.join(", ")),
        });
    }
    hints
}

fn push_detail_hint(hints: &mut Vec<TableStorageHint>, label: &str, detail: Option<&str>) {
    if let Some(detail) = detail.filter(|detail| !detail.is_empty()) {
        hints.push(TableStorageHint {
            label: label.to_string(),
            columns: Vec::new(),
            detail: Some(detail.to_string()),
        });
    }
}

fn text_at(row: &[Value], index: usize) -> Option<&str> {
    match row.get(index) {
        Some(Value::Text(value) | Value::Json(value) | Value::Decimal(value)) => Some(value),
        _ => None,
    }
}

fn integer_text_at(row: &[Value], index: usize) -> Option<String> {
    match row.get(index) {
        Some(Value::Int(value)) => Some(value.to_string()),
        Some(Value::Decimal(value)) => Some(value.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{build_storage_hints, column_info, is_view_engine};
    use dbflux_core::Value;

    #[test]
    fn recognizes_clickhouse_view_engines() {
        assert!(is_view_engine("View"));
        assert!(is_view_engine("MaterializedView"));
        assert!(!is_view_engine("MergeTree"));
    }

    #[test]
    fn storage_hints_include_engine_keys_and_size() {
        let row = vec![
            Value::Text("MergeTree".to_string()),
            Value::Text("toYYYYMM(ts)".to_string()),
            Value::Text("(ts, id)".to_string()),
            Value::Text("id".to_string()),
            Value::Text(String::new()),
            Value::Int(10),
            Value::Int(1024),
        ];
        let hints = build_storage_hints(&row, &[]);
        assert!(hints.iter().any(|hint| hint.label == "Engine"));
        assert!(hints.iter().any(|hint| hint.label == "Partition Key"));
        assert!(hints.iter().any(|hint| hint.label == "Stored Data"));
    }

    #[test]
    fn column_metadata_is_non_editable_and_only_uses_default_kind() {
        let default_column = column_info(&[
            Value::Text("id".to_string()),
            Value::Text("LowCardinality(Nullable(UInt64))".to_string()),
            Value::Text("DEFAULT".to_string()),
            Value::Text("42".to_string()),
            Value::Int(1),
        ])
        .expect("valid column");
        assert!(default_column.nullable);
        assert!(!default_column.is_primary_key);
        assert_eq!(default_column.default_value.as_deref(), Some("42"));

        let alias_column = column_info(&[
            Value::Text("derived".to_string()),
            Value::Text("UInt64".to_string()),
            Value::Text("ALIAS".to_string()),
            Value::Text("id + 1".to_string()),
        ])
        .expect("valid column");
        assert_eq!(alias_column.default_value, None);
    }
}
