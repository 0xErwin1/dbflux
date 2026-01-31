use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use dbflux_core::{
    CodeGenScope, CodeGeneratorInfo, ColumnInfo, ColumnMeta, Connection, ConnectionProfile,
    DbConfig, DbDriver, DbError, DbKind, DbSchemaInfo, DriverFormDef, FormValues, IndexInfo,
    QueryCancelHandle, QueryHandle, QueryRequest, QueryResult, Row, SQLITE_FORM,
    SchemaLoadingStrategy, SchemaSnapshot, TableInfo, Value, ViewInfo,
};
use rusqlite::{Connection as RusqliteConnection, InterruptHandle};

pub struct SqliteDriver;

impl SqliteDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SqliteDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DbDriver for SqliteDriver {
    fn kind(&self) -> DbKind {
        DbKind::SQLite
    }

    fn description(&self) -> &'static str {
        "File-based embedded database"
    }

    fn requires_password(&self) -> bool {
        false
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        _password: Option<&str>,
        _ssh_secret: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let path = match &profile.config {
            DbConfig::SQLite { path } => path.clone(),
            _ => {
                return Err(DbError::InvalidProfile(
                    "Expected SQLite configuration".to_string(),
                ));
            }
        };

        let conn = RusqliteConnection::open(&path)
            .map_err(|e| DbError::ConnectionFailed(e.to_string()))?;

        let interrupt_handle = conn.get_interrupt_handle();

        Ok(Box::new(SqliteConnection {
            conn: Mutex::new(conn),
            interrupt_handle,
            cancelled: Arc::new(AtomicBool::new(false)),
            path,
        }))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let path = match &profile.config {
            DbConfig::SQLite { path } => path.clone(),
            _ => {
                return Err(DbError::InvalidProfile(
                    "Expected SQLite configuration".to_string(),
                ));
            }
        };

        let conn = RusqliteConnection::open(&path)
            .map_err(|e| DbError::ConnectionFailed(e.to_string()))?;

        conn.execute_batch("SELECT 1")
            .map_err(|e| DbError::ConnectionFailed(e.to_string()))?;

        Ok(())
    }

    fn form_definition(&self) -> &'static DriverFormDef {
        &SQLITE_FORM
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let path = values
            .get("path")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("File path is required".to_string()))?;

        Ok(DbConfig::SQLite {
            path: PathBuf::from(path),
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let mut values = HashMap::new();

        if let DbConfig::SQLite { path } = config {
            values.insert("path".to_string(), path.to_string_lossy().to_string());
        }

        values
    }
}

pub struct SqliteConnection {
    conn: Mutex<RusqliteConnection>,
    interrupt_handle: InterruptHandle,
    cancelled: Arc<AtomicBool>,
    #[allow(dead_code)]
    path: PathBuf,
}

struct SqliteCancelHandle {
    cancelled: Arc<AtomicBool>,
    interrupt_handle: InterruptHandle,
}

impl QueryCancelHandle for SqliteCancelHandle {
    fn cancel(&self) -> Result<(), DbError> {
        self.cancelled.store(true, Ordering::SeqCst);
        self.interrupt_handle.interrupt();
        log::info!("[CANCEL] SQLite interrupt signal sent via handle");
        Ok(())
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

const SQLITE_CODE_GENERATORS: &[CodeGeneratorInfo] = &[
    CodeGeneratorInfo {
        id: "select_star",
        label: "SELECT *",
        scope: CodeGenScope::TableOrView,
        order: 0,
        destructive: false,
    },
    CodeGeneratorInfo {
        id: "insert",
        label: "INSERT INTO",
        scope: CodeGenScope::Table,
        order: 5,
        destructive: false,
    },
    CodeGeneratorInfo {
        id: "update",
        label: "UPDATE",
        scope: CodeGenScope::Table,
        order: 6,
        destructive: false,
    },
    CodeGeneratorInfo {
        id: "delete",
        label: "DELETE",
        scope: CodeGenScope::Table,
        order: 7,
        destructive: false,
    },
    CodeGeneratorInfo {
        id: "create_table",
        label: "CREATE TABLE",
        scope: CodeGenScope::Table,
        order: 10,
        destructive: false,
    },
    CodeGeneratorInfo {
        id: "drop_table",
        label: "DROP TABLE",
        scope: CodeGenScope::Table,
        order: 20,
        destructive: true,
    },
];

impl Connection for SqliteConnection {
    fn ping(&self) -> Result<(), DbError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;
        conn.execute_batch("SELECT 1")
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        self.cancelled.store(false, Ordering::SeqCst);

        let start = Instant::now();
        let conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

        let stmt_result = conn.prepare(&req.sql);

        let mut stmt = match stmt_result {
            Ok(s) => s,
            Err(e) => {
                if self.cancelled.load(Ordering::SeqCst) {
                    log::info!("[QUERY] SQLite query was interrupted");
                    return Err(DbError::Cancelled);
                }
                return Err(DbError::QueryFailed(format!("{:?}", e)));
            }
        };

        let column_count = stmt.column_count();
        let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let columns: Vec<ColumnMeta> = column_names
            .into_iter()
            .map(|name| ColumnMeta {
                name,
                type_name: "TEXT".to_string(),
                nullable: true,
            })
            .collect();

        let mut rows: Vec<Row> = Vec::new();
        let query_result = stmt.query([]);

        let mut result_rows = match query_result {
            Ok(r) => r,
            Err(e) => {
                if self.cancelled.load(Ordering::SeqCst) {
                    log::info!("[QUERY] SQLite query was interrupted");
                    return Err(DbError::Cancelled);
                }
                return Err(DbError::QueryFailed(format!("{:?}", e)));
            }
        };

        loop {
            let next_result = result_rows.next();

            match next_result {
                Ok(Some(row)) => {
                    let mut values: Vec<Value> = Vec::with_capacity(column_count);
                    for i in 0..column_count {
                        let value = sqlite_value_to_value(row, i);
                        values.push(value);
                    }
                    rows.push(values);

                    if let Some(limit) = req.limit
                        && rows.len() >= limit as usize
                    {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    if self.cancelled.load(Ordering::SeqCst) {
                        log::info!("[QUERY] SQLite query was interrupted during iteration");
                        return Err(DbError::Cancelled);
                    }
                    return Err(DbError::QueryFailed(format!("{:?}", e)));
                }
            }
        }

        Ok(QueryResult {
            columns,
            rows,
            affected_rows: None,
            execution_time: start.elapsed(),
        })
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        self.cancel_active()
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.cancelled.store(true, Ordering::SeqCst);
        self.interrupt_handle.interrupt();
        log::info!("[CANCEL] SQLite interrupt signal sent");
        Ok(())
    }

    fn cancel_handle(&self) -> Arc<dyn QueryCancelHandle> {
        Arc::new(SqliteCancelHandle {
            cancelled: self.cancelled.clone(),
            interrupt_handle: self
                .conn
                .lock()
                .map(|c| c.get_interrupt_handle())
                .expect("Failed to get interrupt handle"),
        })
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

        let tables = self.get_tables(&conn)?;
        let views = self.get_views(&conn)?;

        let main_schema = DbSchemaInfo {
            name: "main".to_string(),
            tables,
            views,
            custom_types: None,
        };

        Ok(SchemaSnapshot {
            databases: Vec::new(),
            current_database: None,
            schemas: vec![main_schema],
            tables: Vec::new(),
            views: Vec::new(),
        })
    }

    fn kind(&self) -> DbKind {
        DbKind::SQLite
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::SingleDatabase
    }

    fn table_details(
        &self,
        _database: &str,
        _schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        log::info!("[SCHEMA] Fetching details for table: {}", table);

        let conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

        let columns = self.get_columns(&conn, table)?;
        let indexes = self.get_indexes(&conn, table)?;

        log::info!(
            "[SCHEMA] Table {}: {} columns, {} indexes",
            table,
            columns.len(),
            indexes.len()
        );

        Ok(TableInfo {
            name: table.to_string(),
            schema: None,
            columns: Some(columns),
            indexes: Some(indexes),
            foreign_keys: None,
            constraints: None,
        })
    }

    fn code_generators(&self) -> &'static [CodeGeneratorInfo] {
        SQLITE_CODE_GENERATORS
    }

    fn generate_code(&self, generator_id: &str, table: &TableInfo) -> Result<String, DbError> {
        match generator_id {
            "select_star" => Ok(sqlite_generate_select_star(table)),
            "insert" => Ok(sqlite_generate_insert(table)),
            "update" => Ok(sqlite_generate_update(table)),
            "delete" => Ok(sqlite_generate_delete(table)),
            "create_table" => Ok(sqlite_generate_create_table(table)),
            "drop_table" => Ok(sqlite_generate_drop_table(table)),
            _ => Err(DbError::NotSupported(format!(
                "Code generator '{}' not supported",
                generator_id
            ))),
        }
    }
}

impl SqliteConnection {
    fn get_tables(&self, conn: &RusqliteConnection) -> Result<Vec<TableInfo>, DbError> {
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

        let table_names: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        let tables = table_names
            .into_iter()
            .map(|name| TableInfo {
                name,
                schema: None,
                columns: None,
                indexes: None,
                foreign_keys: None,
                constraints: None,
            })
            .collect();

        Ok(tables)
    }

    fn get_columns(
        &self,
        conn: &RusqliteConnection,
        table: &str,
    ) -> Result<Vec<ColumnInfo>, DbError> {
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info('{}')", table))
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

        let columns = stmt
            .query_map([], |row| {
                Ok(ColumnInfo {
                    name: row.get(1)?,
                    type_name: row.get::<_, String>(2).unwrap_or_default(),
                    nullable: row.get::<_, i32>(3).unwrap_or(1) == 0,
                    is_primary_key: row.get::<_, i32>(5).unwrap_or(0) == 1,
                    default_value: row.get::<_, Option<String>>(4).unwrap_or(None),
                })
            })
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(columns)
    }

    fn get_indexes(
        &self,
        conn: &RusqliteConnection,
        table: &str,
    ) -> Result<Vec<IndexInfo>, DbError> {
        let mut stmt = conn
            .prepare(&format!("PRAGMA index_list('{}')", table))
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

        let index_list: Vec<(String, bool)> = stmt
            .query_map([], |row| Ok((row.get(1)?, row.get::<_, i32>(2)? == 1)))
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        let mut indexes = Vec::new();
        for (index_name, is_unique) in index_list {
            let mut col_stmt = conn
                .prepare(&format!("PRAGMA index_info('{}')", index_name))
                .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

            let columns: Vec<String> = col_stmt
                .query_map([], |row| row.get(2))
                .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?
                .filter_map(|r| r.ok())
                .collect();

            indexes.push(IndexInfo {
                name: index_name,
                columns,
                is_unique,
                is_primary: false,
            });
        }

        Ok(indexes)
    }

    fn get_views(&self, conn: &RusqliteConnection) -> Result<Vec<ViewInfo>, DbError> {
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name")
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?;

        let views = stmt
            .query_map([], |row| {
                Ok(ViewInfo {
                    name: row.get(0)?,
                    schema: None,
                })
            })
            .map_err(|e| DbError::QueryFailed(format!("{:?}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(views)
    }
}

fn sqlite_value_to_value(row: &rusqlite::Row, idx: usize) -> Value {
    use rusqlite::types::ValueRef;

    match row.get_ref(idx) {
        Ok(ValueRef::Null) => Value::Null,
        Ok(ValueRef::Integer(i)) => Value::Int(i),
        Ok(ValueRef::Real(f)) => Value::Float(f),
        Ok(ValueRef::Text(t)) => Value::Text(String::from_utf8_lossy(t).to_string()),
        Ok(ValueRef::Blob(b)) => Value::Bytes(b.to_vec()),
        Err(_) => Value::Null,
    }
}

fn sqlite_quote_ident(ident: &str) -> String {
    debug_assert!(!ident.is_empty(), "identifier cannot be empty");
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn sqlite_generate_select_star(table: &TableInfo) -> String {
    format!(
        "SELECT * FROM {} LIMIT 100;",
        sqlite_quote_ident(&table.name)
    )
}

fn sqlite_generate_insert(table: &TableInfo) -> String {
    let quoted_name = sqlite_quote_ident(&table.name);
    let cols = table.columns.as_deref().unwrap_or(&[]);

    if cols.is_empty() {
        return format!("INSERT INTO {} DEFAULT VALUES;", quoted_name);
    }

    let columns: Vec<String> = cols.iter().map(|c| sqlite_quote_ident(&c.name)).collect();

    let placeholders: Vec<&str> = vec!["?"; cols.len()];

    format!(
        "INSERT INTO {} ({})\nVALUES ({});",
        quoted_name,
        columns.join(", "),
        placeholders.join(", ")
    )
}

fn sqlite_generate_update(table: &TableInfo) -> String {
    let quoted_name = sqlite_quote_ident(&table.name);
    let cols = table.columns.as_deref().unwrap_or(&[]);

    if cols.is_empty() {
        return format!(
            "UPDATE {}\nSET -- no columns\nWHERE <condition>;",
            quoted_name
        );
    }

    let pk_columns: Vec<&str> = cols
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.name.as_str())
        .collect();

    let non_pk_columns: Vec<&str> = cols
        .iter()
        .filter(|c| !c.is_primary_key)
        .map(|c| c.name.as_str())
        .collect();

    let set_columns = if non_pk_columns.is_empty() {
        &cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>()[..]
    } else {
        &non_pk_columns[..]
    };

    let set_clauses: Vec<String> = set_columns
        .iter()
        .map(|col| format!("{} = ?", sqlite_quote_ident(col)))
        .collect();

    let where_clause = if pk_columns.is_empty() {
        "<condition>".to_string()
    } else {
        pk_columns
            .iter()
            .map(|col| format!("{} = ?", sqlite_quote_ident(col)))
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    format!(
        "UPDATE {}\nSET {}\nWHERE {};",
        quoted_name,
        set_clauses.join(",\n    "),
        where_clause
    )
}

fn sqlite_generate_delete(table: &TableInfo) -> String {
    let quoted_name = sqlite_quote_ident(&table.name);
    let cols = table.columns.as_deref().unwrap_or(&[]);

    let pk_columns: Vec<&str> = cols
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.name.as_str())
        .collect();

    let where_clause = if pk_columns.is_empty() {
        "<condition>".to_string()
    } else {
        pk_columns
            .iter()
            .map(|col| format!("{} = ?", sqlite_quote_ident(col)))
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    format!("DELETE FROM {}\nWHERE {};", quoted_name, where_clause)
}

fn sqlite_generate_create_table(table: &TableInfo) -> String {
    let mut sql = format!("CREATE TABLE {} (\n", sqlite_quote_ident(&table.name));
    let cols = table.columns.as_deref().unwrap_or(&[]);

    let pk_columns: Vec<&ColumnInfo> = cols.iter().filter(|c| c.is_primary_key).collect();

    // SQLite: INTEGER PRIMARY KEY has special rowid semantics when inline
    let single_integer_pk =
        pk_columns.len() == 1 && pk_columns[0].type_name.eq_ignore_ascii_case("INTEGER");

    for (i, col) in cols.iter().enumerate() {
        // Handle empty type names (SQLite allows columns without explicit types)
        let mut line = if col.type_name.is_empty() {
            format!("    {}", sqlite_quote_ident(&col.name))
        } else {
            format!("    {} {}", sqlite_quote_ident(&col.name), col.type_name)
        };

        if !col.nullable {
            line.push_str(" NOT NULL");
        }

        // SQLite: INTEGER PRIMARY KEY inline for rowid semantics
        if single_integer_pk && col.is_primary_key {
            line.push_str(" PRIMARY KEY");
        }

        if let Some(ref default) = col.default_value {
            line.push_str(&format!(" DEFAULT {}", default));
        }

        let is_last_column = i == cols.len() - 1;
        let needs_pk_constraint = !pk_columns.is_empty() && !single_integer_pk;

        if !is_last_column || needs_pk_constraint {
            line.push(',');
        }

        sql.push_str(&line);
        sql.push('\n');
    }

    // Add composite PRIMARY KEY constraint (only if not single INTEGER PK)
    if !pk_columns.is_empty() && !single_integer_pk {
        let pk_quoted: Vec<String> = pk_columns
            .iter()
            .map(|c| sqlite_quote_ident(&c.name))
            .collect();
        sql.push_str(&format!("    PRIMARY KEY ({})\n", pk_quoted.join(", ")));
    }

    sql.push_str(");");
    sql
}

fn sqlite_generate_drop_table(table: &TableInfo) -> String {
    format!("DROP TABLE {};", sqlite_quote_ident(&table.name))
}
