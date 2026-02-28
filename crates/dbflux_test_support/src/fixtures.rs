use dbflux_core::{
    ColumnInfo, ColumnMeta, DataStructure, DatabaseInfo, DbSchemaInfo, QueryResult,
    RelationalSchema, Row, SchemaSnapshot, TableInfo, Value,
};
use std::time::Duration;

pub fn table_result(columns: Vec<ColumnMeta>, rows: Vec<Row>) -> QueryResult {
    QueryResult::table(columns, rows, None, Duration::ZERO)
}

pub fn json_result(rows: Vec<Row>) -> QueryResult {
    QueryResult::json(Vec::new(), rows, Duration::ZERO)
}

pub fn text_result(body: impl Into<String>) -> QueryResult {
    QueryResult::text(body.into(), Duration::ZERO)
}

pub fn binary_result(bytes: Vec<u8>) -> QueryResult {
    QueryResult::binary(bytes, Duration::ZERO)
}

pub fn column(name: impl Into<String>, type_name: impl Into<String>, nullable: bool) -> ColumnMeta {
    ColumnMeta {
        name: name.into(),
        type_name: type_name.into(),
        nullable,
    }
}

pub fn int_cell(value: i64) -> Value {
    Value::Int(value)
}

pub fn text_cell(value: impl Into<String>) -> Value {
    Value::Text(value.into())
}

pub fn empty_schema() -> SchemaSnapshot {
    SchemaSnapshot::default()
}

pub fn relational_schema_with_table(
    database: impl Into<String>,
    schema_name: impl Into<String>,
    table_name: impl Into<String>,
) -> SchemaSnapshot {
    let database = database.into();
    let schema_name = schema_name.into();
    let table_name = table_name.into();

    let table = TableInfo {
        name: table_name,
        schema: Some(schema_name.clone()),
        columns: Some(vec![ColumnInfo {
            name: "id".to_string(),
            type_name: "integer".to_string(),
            nullable: false,
            is_primary_key: true,
            default_value: None,
            enum_values: None,
        }]),
        indexes: None,
        foreign_keys: None,
        constraints: None,
        sample_fields: None,
    };

    let schema = DbSchemaInfo {
        name: schema_name,
        tables: vec![table],
        views: Vec::new(),
        custom_types: None,
    };

    SchemaSnapshot {
        structure: DataStructure::Relational(RelationalSchema {
            databases: vec![DatabaseInfo {
                name: database.clone(),
                is_current: true,
            }],
            current_database: Some(database),
            schemas: vec![schema],
            tables: Vec::new(),
            views: Vec::new(),
        }),
    }
}
