use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use dbflux_core::{
    CodeGenCapabilities, CodeGenScope, CodeGenerator, CodeGeneratorInfo, ColumnInfo, ColumnMeta,
    Connection, ConnectionProfile, ConstraintInfo, ConstraintKind, CreateIndexRequest, CrudResult,
    DatabaseCategory, DbConfig, DbDriver, DbError, DbKind, DbSchemaInfo, DriverCapabilities,
    DriverFormDef, DriverMetadata, DropIndexRequest, ForeignKeyInfo, FormValues, FormattedError,
    Icon, IndexInfo, PlaceholderStyle, QueryCancelHandle, QueryErrorFormatter, QueryHandle,
    QueryLanguage, QueryRequest, QueryResult, ReindexRequest, RelationalSchema, Row, RowDelete,
    RowInsert, RowPatch, SQLITE_FORM, SchemaForeignKeyInfo, SchemaIndexInfo, SchemaLoadingStrategy,
    SchemaSnapshot, SqlDialect, SqlQueryBuilder, TableInfo, Value, ViewInfo,
    generate_delete_template, generate_drop_table, generate_insert_template, generate_select_star,
    generate_update_template,
};
use rusqlite::{Connection as RusqliteConnection, InterruptHandle};

/// SQLite driver metadata.
pub static METADATA: DriverMetadata = DriverMetadata {
    id: "sqlite",
    display_name: "SQLite",
    description: "Embedded file-based database",
    category: DatabaseCategory::Relational,
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::VIEWS.bits()
            | DriverCapabilities::INDEXES.bits()
            | DriverCapabilities::FOREIGN_KEYS.bits()
            | DriverCapabilities::CHECK_CONSTRAINTS.bits()
            | DriverCapabilities::UNIQUE_CONSTRAINTS.bits()
            | DriverCapabilities::TRIGGERS.bits()
            | DriverCapabilities::INSERT.bits()
            | DriverCapabilities::UPDATE.bits()
            | DriverCapabilities::DELETE.bits()
            | DriverCapabilities::PAGINATION.bits()
            | DriverCapabilities::SORTING.bits()
            | DriverCapabilities::FILTERING.bits()
            | DriverCapabilities::EXPORT_CSV.bits()
            | DriverCapabilities::EXPORT_JSON.bits()
            | DriverCapabilities::QUERY_CANCELLATION.bits(),
    ),
    default_port: None,
    uri_scheme: "sqlite",
    icon: Icon::Sqlite,
};

/// SQLite SQL dialect implementation.
pub struct SqliteDialect;

impl SqlDialect for SqliteDialect {
    fn quote_identifier(&self, name: &str) -> String {
        sqlite_quote_ident(name)
    }

    fn qualified_table(&self, _schema: Option<&str>, table: &str) -> String {
        // SQLite doesn't use schema prefixes for table references
        sqlite_quote_ident(table)
    }

    fn value_to_literal(&self, value: &Value) -> String {
        value_to_sqlite_literal(value)
    }

    fn escape_string(&self, s: &str) -> String {
        sqlite_escape_string(s)
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::QuestionMark
    }
}

static SQLITE_DIALECT: SqliteDialect = SqliteDialect;

// =============================================================================
// SQLite Code Generator
// =============================================================================

pub struct SqliteCodeGenerator;

static SQLITE_CODE_GENERATOR: SqliteCodeGenerator = SqliteCodeGenerator;

impl SqliteCodeGenerator {
    fn quote(&self, name: &str) -> String {
        SQLITE_DIALECT.quote_identifier(name)
    }

    fn qualified(&self, schema: Option<&str>, name: &str) -> String {
        SQLITE_DIALECT.qualified_table(schema, name)
    }
}

impl CodeGenerator for SqliteCodeGenerator {
    fn capabilities(&self) -> CodeGenCapabilities {
        CodeGenCapabilities::CRUD
            | CodeGenCapabilities::INDEXES
            | CodeGenCapabilities::REINDEX
            | CodeGenCapabilities::CREATE_TABLE
            | CodeGenCapabilities::DROP_TABLE
    }

    fn generate_create_index(&self, req: &CreateIndexRequest) -> Option<String> {
        let unique = if req.unique { "UNIQUE " } else { "" };
        let table = self.qualified(req.schema_name, req.table_name);
        let cols = req
            .columns
            .iter()
            .map(|c| self.quote(c))
            .collect::<Vec<_>>()
            .join(", ");

        Some(format!(
            "CREATE {}INDEX {} ON {} ({});",
            unique,
            self.quote(req.index_name),
            table,
            cols
        ))
    }

    fn generate_drop_index(&self, req: &DropIndexRequest) -> Option<String> {
        let index = self.qualified(req.schema_name, req.index_name);
        Some(format!("DROP INDEX {};", index))
    }

    fn generate_reindex(&self, req: &ReindexRequest) -> Option<String> {
        let index = self.qualified(req.schema_name, req.index_name);
        Some(format!("REINDEX {};", index))
    }
}

// =============================================================================

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

    fn metadata(&self) -> &'static DriverMetadata {
        &METADATA
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
    fn metadata(&self) -> &'static DriverMetadata {
        &METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;
        conn.execute_batch("SELECT 1")
            .map_err(|e| format_sqlite_query_error(&e))
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
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        let stmt_result = conn.prepare(&req.sql);

        let mut stmt = match stmt_result {
            Ok(s) => s,
            Err(e) => {
                if self.cancelled.load(Ordering::SeqCst) {
                    log::info!("[QUERY] SQLite query was interrupted");
                    return Err(DbError::Cancelled);
                }
                return Err(format_sqlite_query_error(&e));
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
                return Err(format_sqlite_query_error(&e));
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
                    return Err(format_sqlite_query_error(&e));
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
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        let tables = self.get_tables(&conn)?;
        let views = self.get_views(&conn)?;

        let main_schema = DbSchemaInfo {
            name: "main".to_string(),
            tables,
            views,
            custom_types: None,
        };

        Ok(SchemaSnapshot::relational(RelationalSchema {
            databases: Vec::new(),
            current_database: None,
            schemas: vec![main_schema],
            tables: Vec::new(),
            views: Vec::new(),
        }))
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
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        let columns = self.get_columns(&conn, table)?;
        let indexes = self.get_indexes(&conn, table)?;
        let foreign_keys = self.get_foreign_keys(&conn, table)?;
        let constraints = self.get_constraints(&conn, table)?;

        log::info!(
            "[SCHEMA] Table {}: {} columns, {} indexes, {} FKs, {} constraints",
            table,
            columns.len(),
            indexes.len(),
            foreign_keys.len(),
            constraints.len()
        );

        Ok(TableInfo {
            name: table.to_string(),
            schema: None,
            columns: Some(columns),
            indexes: Some(indexes),
            foreign_keys: Some(foreign_keys),
            constraints: Some(constraints),
        })
    }

    fn schema_indexes(
        &self,
        _database: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<SchemaIndexInfo>, DbError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        self.get_all_indexes(&conn)
    }

    fn schema_foreign_keys(
        &self,
        _database: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        self.get_all_foreign_keys(&conn)
    }

    fn code_generators(&self) -> &'static [CodeGeneratorInfo] {
        SQLITE_CODE_GENERATORS
    }

    fn generate_code(&self, generator_id: &str, table: &TableInfo) -> Result<String, DbError> {
        match generator_id {
            "select_star" => Ok(generate_select_star(&SQLITE_DIALECT, table, 100)),
            "insert" => Ok(generate_insert_template(&SQLITE_DIALECT, table)),
            "update" => Ok(generate_update_template(&SQLITE_DIALECT, table)),
            "delete" => Ok(generate_delete_template(&SQLITE_DIALECT, table)),
            // SQLite needs special handling for INTEGER PRIMARY KEY (rowid semantics)
            "create_table" => Ok(sqlite_generate_create_table(table)),
            "drop_table" => Ok(generate_drop_table(&SQLITE_DIALECT, table)),
            _ => Err(DbError::NotSupported(format!(
                "Code generator '{}' not supported",
                generator_id
            ))),
        }
    }

    fn update_row(&self, patch: &RowPatch) -> Result<CrudResult, DbError> {
        if !patch.identity.is_valid() {
            return Err(DbError::QueryFailed(
                "Cannot update row: invalid row identity (missing primary key)".to_string(),
            ));
        }

        if !patch.has_changes() {
            return Err(DbError::QueryFailed("No changes to save".to_string()));
        }

        let builder = SqlQueryBuilder::new(&SQLITE_DIALECT);

        let update_sql = builder
            .build_update(patch, false)
            .ok_or_else(|| DbError::QueryFailed("Failed to build UPDATE query".to_string()))?;

        log::debug!("[UPDATE] Executing: {}", update_sql);

        let conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        let affected = conn
            .execute(&update_sql, [])
            .map_err(|e| format_sqlite_query_error(&e))?;

        if affected == 0 {
            return Ok(CrudResult::empty());
        }

        let select_sql = builder
            .build_select_by_identity(patch.schema.as_deref(), &patch.table, &patch.identity)
            .ok_or_else(|| DbError::QueryFailed("Failed to build SELECT query".to_string()))?;

        log::debug!("[UPDATE] Re-querying: {}", select_sql);

        let mut stmt = conn
            .prepare(&select_sql)
            .map_err(|e| format_sqlite_query_error(&e))?;

        let column_count = stmt.column_count();

        let mut rows_iter = stmt.query([]).map_err(|e| format_sqlite_query_error(&e))?;

        if let Some(row) = rows_iter
            .next()
            .map_err(|e| format_sqlite_query_error(&e))?
        {
            let returning_row: Row = (0..column_count)
                .map(|i| sqlite_value_to_value(row, i))
                .collect();
            Ok(CrudResult::success(returning_row))
        } else {
            Ok(CrudResult::new(affected as u64, None))
        }
    }

    fn insert_row(&self, insert: &RowInsert) -> Result<CrudResult, DbError> {
        if !insert.is_valid() {
            return Err(DbError::QueryFailed(
                "Cannot insert row: no columns specified".to_string(),
            ));
        }

        let builder = SqlQueryBuilder::new(&SQLITE_DIALECT);

        let insert_sql = builder
            .build_insert(insert, false)
            .ok_or_else(|| DbError::QueryFailed("Failed to build INSERT query".to_string()))?;

        log::debug!("[INSERT] Executing: {}", insert_sql);

        let conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        conn.execute(&insert_sql, [])
            .map_err(|e| format_sqlite_query_error(&e))?;

        let rowid = conn.last_insert_rowid();
        let table_name = SQLITE_DIALECT.qualified_table(insert.schema.as_deref(), &insert.table);

        let select_sql = format!(
            "SELECT * FROM {} WHERE rowid = {} LIMIT 1",
            table_name, rowid
        );

        log::debug!("[INSERT] Re-querying: {}", select_sql);

        let mut stmt = conn
            .prepare(&select_sql)
            .map_err(|e| format_sqlite_query_error(&e))?;

        let column_count = stmt.column_count();

        let mut rows_iter = stmt.query([]).map_err(|e| format_sqlite_query_error(&e))?;

        if let Some(row) = rows_iter
            .next()
            .map_err(|e| format_sqlite_query_error(&e))?
        {
            let returning_row: Row = (0..column_count)
                .map(|i| sqlite_value_to_value(row, i))
                .collect();
            Ok(CrudResult::success(returning_row))
        } else {
            Ok(CrudResult::new(1, None))
        }
    }

    fn delete_row(&self, delete: &RowDelete) -> Result<CrudResult, DbError> {
        if !delete.is_valid() {
            return Err(DbError::QueryFailed(
                "Cannot delete row: invalid row identity (missing primary key)".to_string(),
            ));
        }

        let builder = SqlQueryBuilder::new(&SQLITE_DIALECT);

        let select_sql = builder
            .build_select_by_identity(delete.schema.as_deref(), &delete.table, &delete.identity)
            .ok_or_else(|| DbError::QueryFailed("Failed to build SELECT query".to_string()))?;

        log::debug!("[DELETE] Fetching row: {}", select_sql);

        let conn = self
            .conn
            .lock()
            .map_err(|e| DbError::QueryFailed(format!("Lock error: {}", e)))?;

        let returning_row = {
            let mut stmt = conn
                .prepare(&select_sql)
                .map_err(|e| format_sqlite_query_error(&e))?;

            let column_count = stmt.column_count();

            let mut rows_iter = stmt.query([]).map_err(|e| format_sqlite_query_error(&e))?;

            rows_iter
                .next()
                .map_err(|e| format_sqlite_query_error(&e))?
                .map(|row| {
                    (0..column_count)
                        .map(|i| sqlite_value_to_value(row, i))
                        .collect::<Row>()
                })
        };

        let delete_sql = builder
            .build_delete(delete, false)
            .ok_or_else(|| DbError::QueryFailed("Failed to build DELETE query".to_string()))?;

        log::debug!("[DELETE] Executing: {}", delete_sql);

        let affected = conn
            .execute(&delete_sql, [])
            .map_err(|e| format_sqlite_query_error(&e))?;

        if affected == 0 {
            return Ok(CrudResult::empty());
        }

        Ok(CrudResult::new(affected as u64, returning_row))
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &SQLITE_DIALECT
    }

    fn code_generator(&self) -> &dyn CodeGenerator {
        &SQLITE_CODE_GENERATOR
    }
}

impl SqliteConnection {
    fn get_tables(&self, conn: &RusqliteConnection) -> Result<Vec<TableInfo>, DbError> {
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            .map_err(|e| format_sqlite_query_error(&e))?;

        let table_names: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| format_sqlite_query_error(&e))?
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
            .map_err(|e| format_sqlite_query_error(&e))?;

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
            .map_err(|e| format_sqlite_query_error(&e))?
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
            .map_err(|e| format_sqlite_query_error(&e))?;

        let index_list: Vec<(String, bool)> = stmt
            .query_map([], |row| Ok((row.get(1)?, row.get::<_, i32>(2)? == 1)))
            .map_err(|e| format_sqlite_query_error(&e))?
            .filter_map(|r| r.ok())
            .collect();

        let mut indexes = Vec::new();
        for (index_name, is_unique) in index_list {
            let mut col_stmt = conn
                .prepare(&format!("PRAGMA index_info('{}')", index_name))
                .map_err(|e| format_sqlite_query_error(&e))?;

            let columns: Vec<String> = col_stmt
                .query_map([], |row| row.get(2))
                .map_err(|e| format_sqlite_query_error(&e))?
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
            .map_err(|e| format_sqlite_query_error(&e))?;

        let views = stmt
            .query_map([], |row| {
                Ok(ViewInfo {
                    name: row.get(0)?,
                    schema: None,
                })
            })
            .map_err(|e| format_sqlite_query_error(&e))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(views)
    }

    fn get_foreign_keys(
        &self,
        conn: &RusqliteConnection,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>, DbError> {
        let mut stmt = conn
            .prepare(&format!("PRAGMA foreign_key_list('{}')", table))
            .map_err(|e| format_sqlite_query_error(&e))?;

        // PRAGMA foreign_key_list returns: id, seq, table, from, to, on_update, on_delete, match
        let fk_rows: Vec<(i32, String, String, String, String, String)> = stmt
            .query_map([], |row| {
                Ok((
                    row.get(0)?,              // id
                    row.get::<_, String>(2)?, // table (referenced)
                    row.get::<_, String>(3)?, // from (local column)
                    row.get::<_, String>(4)?, // to (referenced column)
                    row.get::<_, String>(5)?, // on_update
                    row.get::<_, String>(6)?, // on_delete
                ))
            })
            .map_err(|e| format_sqlite_query_error(&e))?
            .filter_map(|r| r.ok())
            .collect();

        // Group by FK id
        let mut fk_map: HashMap<i32, ForeignKeyInfo> = HashMap::new();
        for (id, ref_table, from_col, to_col, on_update, on_delete) in fk_rows {
            let entry = fk_map.entry(id).or_insert_with(|| ForeignKeyInfo {
                name: format!("fk_{}", id),
                columns: Vec::new(),
                referenced_table: ref_table,
                referenced_schema: None,
                referenced_columns: Vec::new(),
                on_update: if on_update == "NO ACTION" {
                    None
                } else {
                    Some(on_update)
                },
                on_delete: if on_delete == "NO ACTION" {
                    None
                } else {
                    Some(on_delete)
                },
            });
            entry.columns.push(from_col);
            entry.referenced_columns.push(to_col);
        }

        Ok(fk_map.into_values().collect())
    }

    fn get_constraints(
        &self,
        conn: &RusqliteConnection,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>, DbError> {
        // SQLite doesn't have a direct way to get CHECK constraints via PRAGMA
        // We need to parse the CREATE TABLE statement
        let mut stmt = conn
            .prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name=?")
            .map_err(|e| format_sqlite_query_error(&e))?;

        let sql: Option<String> = stmt.query_row([table], |row| row.get(0)).ok();

        let mut constraints = Vec::new();

        if let Some(create_sql) = sql {
            // Simple regex-like parsing for CHECK constraints
            // This is a basic implementation; production code might need a proper parser
            let upper_sql = create_sql.to_uppercase();
            if upper_sql.contains("CHECK") {
                // Extract CHECK constraints (simplified)
                for (i, part) in create_sql.split("CHECK").skip(1).enumerate() {
                    if let Some(paren_start) = part.find('(') {
                        let mut depth = 1;
                        let mut end = paren_start + 1;
                        for c in part[paren_start + 1..].chars() {
                            if c == '(' {
                                depth += 1;
                            } else if c == ')' {
                                depth -= 1;
                                if depth == 0 {
                                    break;
                                }
                            }
                            end += c.len_utf8();
                        }
                        let check_expr = part[paren_start + 1..end].trim().to_string();
                        constraints.push(ConstraintInfo {
                            name: format!("check_{}", i),
                            kind: ConstraintKind::Check,
                            columns: Vec::new(),
                            check_clause: Some(check_expr),
                        });
                    }
                }
            }
        }

        // Get UNIQUE constraints from indexes
        let mut idx_stmt = conn
            .prepare(&format!("PRAGMA index_list('{}')", table))
            .map_err(|e| format_sqlite_query_error(&e))?;

        let unique_indexes: Vec<(String, String)> = idx_stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?, // name
                    row.get::<_, String>(3)?, // origin (c=CREATE INDEX, u=UNIQUE, pk=PRIMARY KEY)
                ))
            })
            .map_err(|e| format_sqlite_query_error(&e))?
            .filter_map(|r| r.ok())
            .filter(|(_, origin)| origin == "u") // Only UNIQUE constraints, not indexes
            .collect();

        for (index_name, _) in unique_indexes {
            let mut col_stmt = conn
                .prepare(&format!("PRAGMA index_info('{}')", index_name))
                .map_err(|e| format_sqlite_query_error(&e))?;

            let columns: Vec<String> = col_stmt
                .query_map([], |row| row.get(2))
                .map_err(|e| format_sqlite_query_error(&e))?
                .filter_map(|r| r.ok())
                .collect();

            constraints.push(ConstraintInfo {
                name: index_name,
                kind: ConstraintKind::Unique,
                columns,
                check_clause: None,
            });
        }

        Ok(constraints)
    }

    fn get_all_indexes(&self, conn: &RusqliteConnection) -> Result<Vec<SchemaIndexInfo>, DbError> {
        // Get all tables
        let mut tables_stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            .map_err(|e| format_sqlite_query_error(&e))?;

        let table_names: Vec<String> = tables_stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| format_sqlite_query_error(&e))?
            .filter_map(|r| r.ok())
            .collect();

        let mut all_indexes = Vec::new();

        for table_name in table_names {
            let mut stmt = conn
                .prepare(&format!("PRAGMA index_list('{}')", table_name))
                .map_err(|e| format_sqlite_query_error(&e))?;

            let index_list: Vec<(String, bool, String)> = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(1)?,   // name
                        row.get::<_, i32>(2)? == 1, // unique
                        row.get::<_, String>(3)?,   // origin
                    ))
                })
                .map_err(|e| format_sqlite_query_error(&e))?
                .filter_map(|r| r.ok())
                .collect();

            for (index_name, is_unique, origin) in index_list {
                let mut col_stmt = conn
                    .prepare(&format!("PRAGMA index_info('{}')", index_name))
                    .map_err(|e| format_sqlite_query_error(&e))?;

                let columns: Vec<String> = col_stmt
                    .query_map([], |row| row.get(2))
                    .map_err(|e| format_sqlite_query_error(&e))?
                    .filter_map(|r| r.ok())
                    .collect();

                all_indexes.push(SchemaIndexInfo {
                    name: index_name,
                    table_name: table_name.clone(),
                    columns,
                    is_unique,
                    is_primary: origin == "pk",
                });
            }
        }

        Ok(all_indexes)
    }

    fn get_all_foreign_keys(
        &self,
        conn: &RusqliteConnection,
    ) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
        // Get all tables
        let mut tables_stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            .map_err(|e| format_sqlite_query_error(&e))?;

        let table_names: Vec<String> = tables_stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| format_sqlite_query_error(&e))?
            .filter_map(|r| r.ok())
            .collect();

        let mut all_fks = Vec::new();

        for table_name in table_names {
            let mut stmt = conn
                .prepare(&format!("PRAGMA foreign_key_list('{}')", table_name))
                .map_err(|e| format_sqlite_query_error(&e))?;

            let fk_rows: Vec<(i32, String, String, String, String, String)> = stmt
                .query_map([], |row| {
                    Ok((
                        row.get(0)?,              // id
                        row.get::<_, String>(2)?, // table (referenced)
                        row.get::<_, String>(3)?, // from (local column)
                        row.get::<_, String>(4)?, // to (referenced column)
                        row.get::<_, String>(5)?, // on_update
                        row.get::<_, String>(6)?, // on_delete
                    ))
                })
                .map_err(|e| format_sqlite_query_error(&e))?
                .filter_map(|r| r.ok())
                .collect();

            // Group by FK id
            let mut fk_map: HashMap<i32, SchemaForeignKeyInfo> = HashMap::new();
            for (id, ref_table, from_col, to_col, on_update, on_delete) in fk_rows {
                let entry = fk_map.entry(id).or_insert_with(|| SchemaForeignKeyInfo {
                    name: format!("{}_fk_{}", table_name, id),
                    table_name: table_name.clone(),
                    columns: Vec::new(),
                    referenced_schema: None,
                    referenced_table: ref_table,
                    referenced_columns: Vec::new(),
                    on_update: if on_update == "NO ACTION" {
                        None
                    } else {
                        Some(on_update)
                    },
                    on_delete: if on_delete == "NO ACTION" {
                        None
                    } else {
                        Some(on_delete)
                    },
                });
                entry.columns.push(from_col);
                entry.referenced_columns.push(to_col);
            }

            all_fks.extend(fk_map.into_values());
        }

        Ok(all_fks)
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

pub struct SqliteErrorFormatter;

impl SqliteErrorFormatter {
    fn format_sqlite_error(e: &rusqlite::Error) -> FormattedError {
        match e {
            rusqlite::Error::SqliteFailure(err, msg) => {
                let message = msg.clone().unwrap_or_else(|| format!("{:?}", err.code));

                FormattedError::new(message)
                    .with_code(format!("{:?} ({})", err.code, err.extended_code))
            }
            _ => FormattedError::new(e.to_string()),
        }
    }
}

impl QueryErrorFormatter for SqliteErrorFormatter {
    fn format_query_error(&self, error: &(dyn std::error::Error + 'static)) -> FormattedError {
        if let Some(sqlite_err) = error.downcast_ref::<rusqlite::Error>() {
            Self::format_sqlite_error(sqlite_err)
        } else {
            FormattedError::new(error.to_string())
        }
    }
}

fn format_sqlite_query_error(e: &rusqlite::Error) -> DbError {
    let formatted = SqliteErrorFormatter::format_sqlite_error(e);
    let message = formatted.to_display_string();
    log::error!("SQLite query failed: {}", message);
    formatted.into_query_error()
}

fn sqlite_quote_ident(ident: &str) -> String {
    debug_assert!(!ident.is_empty(), "identifier cannot be empty");
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Convert a Value to a safe SQLite literal string.
fn value_to_sqlite_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.is_nan() || f.is_infinite() {
                // SQLite doesn't have NaN/Infinity, store as NULL
                "NULL".to_string()
            } else {
                f.to_string()
            }
        }
        Value::Decimal(s) => {
            // SQLite stores decimals as REAL, quote as string literal
            format!("'{}'", sqlite_escape_string(s))
        }
        Value::Text(s) => format!("'{}'", sqlite_escape_string(s)),
        Value::Json(s) => format!("'{}'", sqlite_escape_string(s)),
        Value::Bytes(b) => format!("X'{}'", hex::encode(b)),
        Value::DateTime(dt) => format!("'{}'", dt.to_rfc3339()),
        Value::Date(d) => format!("'{}'", d.format("%Y-%m-%d")),
        Value::Time(t) => format!("'{}'", t.format("%H:%M:%S%.f")),
        Value::ObjectId(id) => format!("'{}'", sqlite_escape_string(id)),
        Value::Array(arr) => {
            let json = serde_json::to_string(arr).unwrap_or_else(|_| "[]".to_string());
            format!("'{}'", sqlite_escape_string(&json))
        }
        Value::Document(doc) => {
            let json = serde_json::to_string(doc).unwrap_or_else(|_| "{}".to_string());
            format!("'{}'", sqlite_escape_string(&json))
        }
    }
}

/// Escape a string for use inside a SQLite single-quoted literal.
fn sqlite_escape_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// SQLite-specific CREATE TABLE to handle INTEGER PRIMARY KEY rowid semantics.
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
