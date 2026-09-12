//! Synchronous `Connection` adapter over the async `turso_serverless` SDK.
//!
//! One `TursoConnection` wraps one Hrana stream. The root connection (built by
//! the driver) additionally owns an `ExecutionSessionFactory` that hands out
//! child connections on fresh streams, so an interactive editor transaction
//! never shares a stream with grid CRUD or MCP work.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use dbflux_core::{
    CodeGenerator, CodeGeneratorInfo, ColumnInfo, ColumnKind, ColumnMeta, Connection,
    ConnectionExt, ConstraintInfo, ConstraintKind, CrudResult, DbError, DbKind, DbSchemaInfo,
    DescribeRequest, DocumentConnection, DriverMetadata, ExecutionSessionFactory, ExplainRequest,
    ForeignKeyInfo, FormattedError, IndexData, IndexInfo, KeyValueConnection, OrderByColumn,
    QueryErrorFormatter, QueryGenerator, QueryHandle, QueryLanguage, QueryRequest, QueryResult,
    RelationalConnection, RelationalSchema, Row, RowDelete, RowInsert, RowPatch,
    SchemaForeignKeyInfo, SchemaIndexInfo, SchemaLoadingStrategy, SchemaSnapshot, SemanticPlan,
    SemanticRequest, SortDirection, SqlDialect, SqlMutationGenerator, SqlQueryBuilder, TableInfo,
    Value, ViewInfo, generate_delete_template, generate_drop_table, generate_insert_template,
    generate_select_star, generate_update_template,
};
use turso_serverless::{
    BatchResult, Builder, Connection as SdkConnection, Database, Value as TursoValue,
};

use crate::dialect::{
    TURSO_CODE_GENERATOR, TURSO_DIALECT, collect_filter_values, escape_string,
    generate_create_table, kind_from_decltype, kind_from_values, quote_ident,
    translate_filter_to_sql, value_from_turso, value_to_param,
};
use crate::driver::METADATA;
use crate::session::TursoSessionFactory;

// =============================================================================
// Runtime bridge
// =============================================================================

/// Owns the Tokio runtime that drives SDK futures for one profile.
///
/// Shared between the root connection and every child session so a profile
/// costs one runtime, not one per stream.
pub(crate) struct Worker {
    runtime: tokio::runtime::Runtime,
}

impl Worker {
    pub(crate) fn new() -> Result<Self, DbError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("dbflux-turso-io")
            .enable_all()
            .build()
            .map_err(|e| {
                DbError::connection_failed(format!("Failed to start Turso I/O runtime: {e}"))
            })?;
        Ok(Self { runtime })
    }

    /// Drives a future to completion from synchronous code.
    ///
    /// Driver methods run on plain background threads, where `block_on` is
    /// fine. When a caller is already inside a Tokio context (the MCP server
    /// runs tools under `spawn_blocking`), nesting `block_on` would panic, so
    /// the future is driven from a scoped helper thread instead.
    pub(crate) fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        if tokio::runtime::Handle::try_current().is_ok() {
            std::thread::scope(|scope| {
                let handle = scope.spawn(|| self.runtime.block_on(future));
                match handle.join() {
                    Ok(output) => output,
                    Err(payload) => std::panic::resume_unwind(payload),
                }
            })
        } else {
            self.runtime.block_on(future)
        }
    }
}

// =============================================================================
// Connection
// =============================================================================

pub struct TursoConnection {
    sdk: SdkConnection,
    worker: Arc<Worker>,
    /// Present on the root connection only. Child sessions never expose a
    /// factory, so generic helpers cannot accidentally nest isolation.
    factory: Option<TursoSessionFactory>,
}

impl TursoConnection {
    /// Builds the root connection for a profile, opening its first stream.
    pub(crate) fn open_root(builder: Builder) -> Result<Self, DbError> {
        let worker = Arc::new(Worker::new()?);
        // `build` is async for API symmetry but performs no I/O.
        let database = worker.block_on(builder.build()).map_err(map_sdk_error)?;
        let sdk = database.connect().map_err(map_sdk_error)?;
        let factory = TursoSessionFactory::new(database, worker.clone());
        Ok(Self {
            sdk,
            worker,
            factory: Some(factory),
        })
    }

    /// Builds a child connection on a fresh stream for an isolated session.
    pub(crate) fn open_child(database: &Database, worker: Arc<Worker>) -> Result<Self, DbError> {
        let sdk = database.connect().map_err(map_sdk_error)?;
        Ok(Self {
            sdk,
            worker,
            factory: None,
        })
    }

    /// Whether the stream currently has an open transaction, as reported by
    /// the server after the last statement. Local, no I/O.
    pub(crate) fn in_transaction(&self) -> bool {
        !self.sdk.is_autocommit().unwrap_or(true)
    }

    pub(crate) fn rollback(&self) -> Result<(), DbError> {
        self.run("ROLLBACK", Vec::new()).map(|_| ())
    }

    /// Releases the server-side stream. Any open transaction is rolled back
    /// by the server when the stream closes.
    pub(crate) fn close_stream(&self) -> Result<(), DbError> {
        self.worker
            .block_on(self.sdk.close())
            .map_err(map_sdk_error)
    }

    /// Runs one statement with bound parameters and materializes the result.
    fn run(&self, sql: &str, params: Vec<TursoValue>) -> Result<QueryResult, DbError> {
        let start = Instant::now();
        let mut results = self.run_batch(vec![(sql.to_string(), params)])?;
        let Some(result) = results.pop() else {
            return Err(DbError::query_failed(
                "Turso returned no result for statement",
            ));
        };
        Ok(batch_result_to_query_result(result, None, start))
    }

    fn run_batch(
        &self,
        statements: Vec<(String, Vec<TursoValue>)>,
    ) -> Result<Vec<BatchResult>, DbError> {
        self.worker
            .block_on(self.sdk.batch(statements))
            .map_err(map_sdk_error)
    }

    fn run_sql(&self, sql: &str) -> Result<QueryResult, DbError> {
        self.run(sql, Vec::new())
    }

    fn pragma(&self, name: &str, argument: &str) -> Result<QueryResult, DbError> {
        self.run_sql(&format!("PRAGMA {name}('{}')", escape_string(argument)))
    }

    // -------------------------------------------------------------------------
    // Schema helpers
    // -------------------------------------------------------------------------

    fn table_names(&self) -> Result<Vec<String>, DbError> {
        let result = self.run_sql(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )?;
        Ok(result
            .rows
            .into_iter()
            .filter_map(|row| row.into_iter().next().and_then(value_to_string))
            .collect())
    }

    fn get_tables(&self) -> Result<Vec<TableInfo>, DbError> {
        Ok(self
            .table_names()?
            .into_iter()
            .map(|name| TableInfo {
                name,
                schema: None,
                columns: None,
                indexes: None,
                foreign_keys: None,
                constraints: None,
                sample_fields: None,
                presentation: dbflux_core::CollectionPresentation::DataGrid,
                child_items: None,
                storage_hints: None,
            })
            .collect())
    }

    fn get_views(&self) -> Result<Vec<ViewInfo>, DbError> {
        let result =
            self.run_sql("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name")?;
        Ok(result
            .rows
            .into_iter()
            .filter_map(|row| row.into_iter().next().and_then(value_to_string))
            .map(|name| ViewInfo { name, schema: None })
            .collect())
    }

    fn get_columns(&self, table: &str) -> Result<Vec<ColumnInfo>, DbError> {
        let exists = self.run(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            vec![TursoValue::Text(table.to_string())],
        )?;
        if exists.rows.is_empty() {
            return Err(DbError::ObjectNotFound(
                format!("Table '{table}' not found").into(),
            ));
        }

        // PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
        let result = self.pragma("table_info", table)?;
        Ok(result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.get(1).and_then(|v| value_to_string(v.clone()))?;
                let type_name = row
                    .get(2)
                    .and_then(|v| value_to_string(v.clone()))
                    .unwrap_or_default();
                let notnull = row.get(3).and_then(value_to_i64).unwrap_or(1);
                let pk = row.get(5).and_then(value_to_i64).unwrap_or(0);
                let default_value = row.get(4).and_then(|v| value_to_string(v.clone()));
                Some(ColumnInfo {
                    name,
                    type_name,
                    // INTEGER PRIMARY KEY reports notnull=0 but is implicitly NOT NULL.
                    nullable: notnull == 0 && pk == 0,
                    is_primary_key: pk > 0,
                    default_value,
                    enum_values: None,
                })
            })
            .collect())
    }

    /// PRAGMA index_list columns: seq, name, unique, origin, partial.
    fn index_list(&self, table: &str) -> Result<Vec<(String, bool, String)>, DbError> {
        let result = self.pragma("index_list", table)?;
        Ok(result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.get(1).and_then(|v| value_to_string(v.clone()))?;
                let unique = row.get(2).and_then(value_to_i64).unwrap_or(0) == 1;
                let origin = row
                    .get(3)
                    .and_then(|v| value_to_string(v.clone()))
                    .unwrap_or_default();
                Some((name, unique, origin))
            })
            .collect())
    }

    /// PRAGMA index_info columns: seqno, cid, name.
    fn index_columns(&self, index: &str) -> Result<Vec<String>, DbError> {
        let result = self.pragma("index_info", index)?;
        Ok(result
            .rows
            .iter()
            .filter_map(|row| row.get(2).and_then(|v| value_to_string(v.clone())))
            .collect())
    }

    fn get_indexes(&self, table: &str) -> Result<Vec<IndexInfo>, DbError> {
        let mut indexes = Vec::new();
        for (name, is_unique, origin) in self.index_list(table)? {
            let columns = self.index_columns(&name)?;
            indexes.push(IndexInfo {
                name,
                columns,
                is_unique,
                is_primary: origin == "pk",
            });
        }
        Ok(indexes)
    }

    /// PRAGMA foreign_key_list columns: id, seq, table, from, to, on_update, on_delete, match.
    fn foreign_key_rows(&self, table: &str) -> Result<Vec<ForeignKeyRow>, DbError> {
        let result = self.pragma("foreign_key_list", table)?;
        Ok(result
            .rows
            .iter()
            .filter_map(|row| {
                let text = |index: usize| row.get(index).and_then(|v| value_to_string(v.clone()));
                Some(ForeignKeyRow {
                    id: row.first().and_then(value_to_i64)?,
                    referenced_table: text(2)?,
                    from_column: text(3)?,
                    to_column: text(4)?,
                    on_update: text(5).unwrap_or_default(),
                    on_delete: text(6).unwrap_or_default(),
                })
            })
            .collect())
    }

    fn get_foreign_keys(&self, table: &str) -> Result<Vec<ForeignKeyInfo>, DbError> {
        let mut fk_map: HashMap<i64, ForeignKeyInfo> = HashMap::new();
        for row in self.foreign_key_rows(table)? {
            let entry = fk_map.entry(row.id).or_insert_with(|| ForeignKeyInfo {
                name: format!("fk_{}", row.id),
                columns: Vec::new(),
                referenced_table: row.referenced_table,
                referenced_schema: None,
                referenced_columns: Vec::new(),
                on_update: fk_action(row.on_update),
                on_delete: fk_action(row.on_delete),
            });
            entry.columns.push(row.from_column);
            entry.referenced_columns.push(row.to_column);
        }
        let mut keys: Vec<ForeignKeyInfo> = fk_map.into_values().collect();
        keys.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(keys)
    }

    fn get_constraints(&self, table: &str) -> Result<Vec<ConstraintInfo>, DbError> {
        let result = self.run(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?",
            vec![TursoValue::Text(table.to_string())],
        )?;
        let create_sql = result
            .rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next())
            .and_then(value_to_string);

        let mut constraints = create_sql
            .as_deref()
            .map(extract_check_constraints)
            .unwrap_or_default();

        for (name, _, origin) in self.index_list(table)? {
            if origin != "u" {
                continue;
            }
            let columns = self.index_columns(&name)?;
            constraints.push(ConstraintInfo {
                name,
                kind: ConstraintKind::Unique,
                columns,
                check_clause: None,
            });
        }

        Ok(constraints)
    }

    fn get_all_indexes(&self) -> Result<Vec<SchemaIndexInfo>, DbError> {
        let mut all = Vec::new();
        for table_name in self.table_names()? {
            for (name, is_unique, origin) in self.index_list(&table_name)? {
                let columns = self.index_columns(&name)?;
                all.push(SchemaIndexInfo {
                    name,
                    table_name: table_name.clone(),
                    columns,
                    is_unique,
                    is_primary: origin == "pk",
                });
            }
        }
        Ok(all)
    }

    fn get_all_foreign_keys(&self) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
        let mut all = Vec::new();
        for table_name in self.table_names()? {
            let mut fk_map: HashMap<i64, SchemaForeignKeyInfo> = HashMap::new();
            for row in self.foreign_key_rows(&table_name)? {
                let entry = fk_map
                    .entry(row.id)
                    .or_insert_with(|| SchemaForeignKeyInfo {
                        name: format!("{table_name}_fk_{}", row.id),
                        table_name: table_name.clone(),
                        columns: Vec::new(),
                        referenced_schema: None,
                        referenced_table: row.referenced_table,
                        referenced_columns: Vec::new(),
                        on_update: fk_action(row.on_update),
                        on_delete: fk_action(row.on_delete),
                    });
                entry.columns.push(row.from_column);
                entry.referenced_columns.push(row.to_column);
            }
            let mut keys: Vec<SchemaForeignKeyInfo> = fk_map.into_values().collect();
            keys.sort_by(|a, b| a.name.cmp(&b.name));
            all.extend(keys);
        }
        Ok(all)
    }

    fn select_first_row(&self, sql: &str) -> Result<Option<Row>, DbError> {
        Ok(self.run_sql(sql)?.rows.into_iter().next())
    }
}

/// One row of `PRAGMA foreign_key_list`; several rows share an `id` for a
/// composite key.
struct ForeignKeyRow {
    id: i64,
    referenced_table: String,
    from_column: String,
    to_column: String,
    on_update: String,
    on_delete: String,
}

fn fk_action(action: String) -> Option<String> {
    if action.is_empty() || action == "NO ACTION" {
        None
    } else {
        Some(action)
    }
}

/// Pulls `CHECK (...)` expressions out of a CREATE TABLE statement. SQLite has
/// no PRAGMA for check constraints, so the DDL text is the only source.
fn extract_check_constraints(create_sql: &str) -> Vec<ConstraintInfo> {
    let mut constraints = Vec::new();
    if !create_sql.to_uppercase().contains("CHECK") {
        return constraints;
    }

    for (i, part) in create_sql.split("CHECK").skip(1).enumerate() {
        let Some(paren_start) = part.find('(') else {
            continue;
        };
        let body = &part[paren_start + 1..];
        let mut depth = 1usize;
        let mut end = 0usize;
        for c in body.chars() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            end += c.len_utf8();
        }
        constraints.push(ConstraintInfo {
            name: format!("check_{i}"),
            kind: ConstraintKind::Check,
            columns: Vec::new(),
            check_clause: Some(body[..end].trim().to_string()),
        });
    }

    constraints
}

fn value_to_string(value: Value) -> Option<String> {
    match value {
        Value::Text(s) => Some(s),
        Value::Int(i) => Some(i.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Null => None,
        other => Some(TURSO_DIALECT.value_to_literal(&other)),
    }
}

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Int(i) => Some(*i),
        Value::Bool(b) => Some(i64::from(*b)),
        Value::Text(s) => s.parse().ok(),
        _ => None,
    }
}

/// Materializes one SDK batch result into a `QueryResult`.
///
/// Row-returning statements report `affected_rows = None` because the server
/// answers zero for them, which would read as "nothing happened".
fn batch_result_to_query_result(
    result: BatchResult,
    limit: Option<u32>,
    start: Instant,
) -> QueryResult {
    let mut rows: Vec<Row> = result
        .rows()
        .iter()
        .map(|row| {
            (0..row.column_count())
                .map(|i| {
                    row.get_value(i)
                        .map(value_from_turso)
                        .unwrap_or_else(|_| Value::Unsupported("turso-value".to_string()))
                })
                .collect()
        })
        .collect();

    if let Some(limit) = limit {
        rows.truncate(limit as usize);
    }

    let columns: Vec<ColumnMeta> = result
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let decl = column.decl_type();
            let mut kind = kind_from_decltype(decl);
            if kind == ColumnKind::Unknown && decl.is_none() {
                kind = kind_from_values(rows.iter().filter_map(|row| row.get(index)));
            }
            ColumnMeta {
                name: column.name().to_string(),
                type_name: decl
                    .filter(|d| !d.is_empty())
                    .map(str::to_uppercase)
                    .unwrap_or_else(|| "TEXT".to_string()),
                kind,
                nullable: true,
                is_primary_key: false,
            }
        })
        .collect();

    let affected_rows = if columns.is_empty() {
        Some(result.rows_affected())
    } else {
        None
    };

    QueryResult::table(columns, rows, affected_rows, start.elapsed())
}

// =============================================================================
// Connection trait
// =============================================================================

impl Connection for TursoConnection {
    fn metadata(&self) -> &DriverMetadata {
        &METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        self.run_sql("SELECT 1").map(|_| ())
    }

    fn close(&mut self) -> Result<(), DbError> {
        let children = match self.factory.as_ref() {
            Some(factory) => factory.shutdown(),
            None => Ok(()),
        };
        let own = self.close_stream();
        match (children, own) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(e), _) | (Ok(()), Err(e)) => Err(e),
        }
    }

    fn execution_session_factory(&self) -> Option<&dyn ExecutionSessionFactory> {
        self.factory
            .as_ref()
            .map(|factory| factory as &dyn ExecutionSessionFactory)
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        let start = Instant::now();
        let params = req
            .params
            .iter()
            .map(value_to_param)
            .collect::<Result<Vec<_>, _>>()?;

        let statements = QueryLanguage::Sql.split_statements(&req.sql);
        if statements.len() <= 1 {
            let mut results = self.run_batch(vec![(req.sql.clone(), params)])?;
            let Some(result) = results.pop() else {
                return Err(DbError::query_failed(
                    "Turso returned no result for statement",
                ));
            };
            return Ok(batch_result_to_query_result(result, req.limit, start));
        }

        if !params.is_empty() {
            return Err(DbError::query_failed(
                "Bound parameters are only supported for single-statement queries",
            ));
        }

        // A script runs as one non-transactional batch: one round trip, each
        // statement committing as it goes, stopping at the first failure.
        let batch: Vec<(String, Vec<TursoValue>)> = statements
            .into_iter()
            .map(|statement| (statement, Vec::new()))
            .collect();
        let results = self.run_batch(batch)?;
        let mut result_sets = results
            .into_iter()
            .map(|result| batch_result_to_query_result(result, req.limit, start));

        let Some(mut primary) = result_sets.next() else {
            return Ok(QueryResult::table(vec![], vec![], Some(0), start.elapsed()));
        };
        for extra in result_sets {
            primary.push_additional_result(extra);
        }
        Ok(primary)
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "Turso does not support cancelling an in-flight request".into(),
        ))
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let tables = self.get_tables()?;
        let views = self.get_views()?;

        Ok(SchemaSnapshot::relational(RelationalSchema {
            databases: Vec::new(),
            current_database: None,
            schemas: vec![DbSchemaInfo {
                name: "main".to_string(),
                tables,
                views,
                custom_types: None,
            }],
            tables: Vec::new(),
            views: Vec::new(),
        }))
    }

    fn kind(&self) -> DbKind {
        DbKind::Turso
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
        let columns = self.get_columns(table)?;
        let indexes = self.get_indexes(table)?;
        let foreign_keys = self.get_foreign_keys(table)?;
        let constraints = self.get_constraints(table)?;

        Ok(TableInfo {
            name: table.to_string(),
            schema: None,
            columns: Some(columns),
            indexes: Some(IndexData::Relational(indexes)),
            foreign_keys: Some(foreign_keys),
            constraints: Some(constraints),
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::DataGrid,
            child_items: None,
            storage_hints: None,
        })
    }

    fn view_details(
        &self,
        _database: &str,
        _schema: Option<&str>,
        view: &str,
    ) -> Result<ViewInfo, DbError> {
        // Make sure the view exists before answering.
        self.get_columns(view)?;
        Ok(ViewInfo {
            name: view.to_string(),
            schema: None,
        })
    }

    fn schema_indexes(
        &self,
        _database: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<SchemaIndexInfo>, DbError> {
        self.get_all_indexes()
    }

    fn schema_foreign_keys(
        &self,
        _database: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
        self.get_all_foreign_keys()
    }

    fn fetch_dependents(
        &self,
        _database: &str,
        _schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<dbflux_core::RelationRef>, DbError> {
        use dbflux_core::{RelationKind, RelationRef};

        let mut dependents = Vec::new();

        let objects = self.run(
            "SELECT type, name FROM sqlite_master WHERE type IN ('view', 'trigger') AND sql LIKE '%' || ? || '%'",
            vec![TursoValue::Text(table.to_string())],
        )?;
        for row in objects.rows {
            let mut cells = row.into_iter();
            let object_type = cells.next().and_then(value_to_string);
            let name = cells.next().and_then(value_to_string);
            let kind = match object_type.as_deref() {
                Some("view") => RelationKind::View,
                Some("trigger") => RelationKind::Trigger,
                _ => continue,
            };
            if let Some(qualified_name) = name {
                dependents.push(RelationRef {
                    kind,
                    qualified_name,
                });
            }
        }

        for child in self.table_names()? {
            let references_table = self
                .foreign_key_rows(&child)?
                .iter()
                .any(|row| row.referenced_table == table);
            if references_table {
                dependents.push(RelationRef {
                    kind: RelationKind::ForeignKeyChild,
                    qualified_name: child,
                });
            }
        }

        Ok(dependents)
    }

    fn fetch_row_by_pk(
        &self,
        _database: &str,
        _schema: &str,
        table: &str,
        pk_column: &str,
        pk_value: &Value,
    ) -> Result<Option<HashMap<String, Value>>, DbError> {
        let sql = format!(
            "SELECT * FROM {} WHERE {} = ? LIMIT 1",
            quote_ident(table),
            quote_ident(pk_column),
        );
        let result = self.run(&sql, vec![value_to_param(pk_value)?])?;
        let columns = result.columns;
        let Some(row) = result.rows.into_iter().next() else {
            return Ok(None);
        };
        Ok(Some(
            columns
                .into_iter()
                .zip(row)
                .map(|(column, value)| (column.name, value))
                .collect(),
        ))
    }

    fn referenced_tables(&self, query: &str) -> Option<Vec<dbflux_core::QueryTableRef>> {
        Some(dbflux_core::extract_referenced_tables(query))
    }

    fn code_generators(&self) -> Vec<CodeGeneratorInfo> {
        use dbflux_core::CodeGenScope;
        vec![
            CodeGeneratorInfo {
                id: "create_table".into(),
                label: "CREATE TABLE".into(),
                scope: CodeGenScope::Table,
                order: 10,
                destructive: false,
            },
            CodeGeneratorInfo {
                id: "drop_table".into(),
                label: "DROP TABLE".into(),
                scope: CodeGenScope::Table,
                order: 20,
                destructive: true,
            },
        ]
    }

    fn generate_code(&self, generator_id: &str, table: &TableInfo) -> Result<String, DbError> {
        match generator_id {
            "select_star" => Ok(generate_select_star(&TURSO_DIALECT, table, 100)),
            "insert" => Ok(generate_insert_template(&TURSO_DIALECT, table)),
            "update" => Ok(generate_update_template(&TURSO_DIALECT, table)),
            "delete" => Ok(generate_delete_template(&TURSO_DIALECT, table)),
            "create_table" => Ok(generate_create_table(table)),
            "drop_table" => Ok(generate_drop_table(&TURSO_DIALECT, table)),
            _ => Err(DbError::NotSupported(format!(
                "Code generator '{generator_id}' not supported"
            ))),
        }
    }

    fn update_row(&self, patch: &RowPatch) -> Result<CrudResult, DbError> {
        if !patch.identity.is_valid() {
            return Err(DbError::query_failed(
                "Cannot update row: invalid row identity (missing primary key)",
            ));
        }
        if !patch.has_changes() {
            return Err(DbError::query_failed("No changes to save"));
        }

        let builder = SqlQueryBuilder::new(&TURSO_DIALECT);
        let update_sql = builder
            .build_update(patch, false)
            .ok_or_else(|| DbError::query_failed("Failed to build UPDATE query"))?;
        log::debug!("[UPDATE] Executing: {update_sql}");

        let affected = self.run_sql(&update_sql)?.affected_rows.unwrap_or(0);
        if affected == 0 {
            return Ok(CrudResult::empty());
        }

        let select_sql = builder
            .build_select_by_identity(patch.schema.as_deref(), &patch.table, &patch.identity)
            .ok_or_else(|| DbError::query_failed("Failed to build SELECT query"))?;
        match self.select_first_row(&select_sql)? {
            Some(row) => Ok(CrudResult::success(row)),
            None => Ok(CrudResult::new(affected, None)),
        }
    }

    fn insert_row(&self, insert: &RowInsert) -> Result<CrudResult, DbError> {
        if !insert.is_valid() {
            return Err(DbError::query_failed(
                "Cannot insert row: no columns specified",
            ));
        }

        let builder = SqlQueryBuilder::new(&TURSO_DIALECT);
        let insert_sql = builder
            .build_insert(insert, false)
            .ok_or_else(|| DbError::query_failed("Failed to build INSERT query"))?;
        log::debug!("[INSERT] Executing: {insert_sql}");

        let mut results = self.run_batch(vec![(insert_sql, Vec::new())])?;
        let Some(result) = results.pop() else {
            return Err(DbError::query_failed("Turso returned no result for INSERT"));
        };

        let Some(rowid) = result.last_insert_rowid() else {
            return Ok(CrudResult::new(result.rows_affected(), None));
        };
        let table_name = TURSO_DIALECT.qualified_table(insert.schema.as_deref(), &insert.table);
        let select_sql = format!("SELECT * FROM {table_name} WHERE rowid = {rowid} LIMIT 1");
        match self.select_first_row(&select_sql)? {
            Some(row) => Ok(CrudResult::success(row)),
            None => Ok(CrudResult::new(result.rows_affected().max(1), None)),
        }
    }

    fn delete_row(&self, delete: &RowDelete) -> Result<CrudResult, DbError> {
        if !delete.identity.is_valid() {
            return Err(DbError::query_failed(
                "Cannot delete row: invalid row identity (missing primary key)",
            ));
        }

        let builder = SqlQueryBuilder::new(&TURSO_DIALECT);
        let select_sql = builder
            .build_select_by_identity(delete.schema.as_deref(), &delete.table, &delete.identity)
            .ok_or_else(|| DbError::query_failed("Failed to build SELECT query"))?;
        let returning_row = self.select_first_row(&select_sql)?;

        let delete_sql = builder
            .build_delete(delete, false)
            .ok_or_else(|| DbError::query_failed("Failed to build DELETE query"))?;
        log::debug!("[DELETE] Executing: {delete_sql}");

        let affected = self.run_sql(&delete_sql)?.affected_rows.unwrap_or(0);
        if affected == 0 {
            return Ok(CrudResult::empty());
        }
        Ok(CrudResult::new(affected, returning_row))
    }

    fn explain(&self, request: &ExplainRequest) -> Result<QueryResult, DbError> {
        let query = match &request.query {
            Some(q) => q.clone(),
            None => format!(
                "SELECT * FROM {} LIMIT 100",
                request.table.quoted_with(&TURSO_DIALECT)
            ),
        };
        self.run_sql(&format!("EXPLAIN QUERY PLAN {query}"))
    }

    fn describe_table(&self, request: &DescribeRequest) -> Result<QueryResult, DbError> {
        self.pragma("table_info", &request.table.name)
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &TURSO_DIALECT
    }

    fn code_generator(&self) -> &dyn CodeGenerator {
        &TURSO_CODE_GENERATOR
    }

    fn query_generator(&self) -> Option<&dyn QueryGenerator> {
        static GENERATOR: SqlMutationGenerator = SqlMutationGenerator::new(&TURSO_DIALECT);
        Some(&GENERATOR)
    }

    fn plan_semantic_request(&self, request: &SemanticRequest) -> Result<SemanticPlan, DbError> {
        plan_semantic_request(request)
    }

    fn build_select_sql(
        &self,
        table: &str,
        columns: &[String],
        filter: Option<&Value>,
        order_by: &[OrderByColumn],
        limit: u32,
        offset: u32,
    ) -> String {
        let cols = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut sql = format!("SELECT {cols} FROM {}", quote_ident(table));

        if let Some(where_clause) = filter.map(translate_filter_to_sql)
            && !where_clause.is_empty()
        {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clause);
        }

        if !order_by.is_empty() {
            let order_parts = order_by
                .iter()
                .map(|col| {
                    let dir = match col.direction {
                        SortDirection::Ascending => "ASC",
                        SortDirection::Descending => "DESC",
                    };
                    format!("{} {dir}", col.column.quoted_with(&TURSO_DIALECT))
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(" ORDER BY ");
            sql.push_str(&order_parts);
        }

        sql.push_str(&format!(" LIMIT {limit} OFFSET {offset}"));
        sql
    }

    fn build_insert_sql(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
    ) -> (String, Vec<Value>) {
        let cols = columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = vec!["?"; values.len()].join(", ");
        (
            format!(
                "INSERT INTO {} ({cols}) VALUES ({placeholders})",
                quote_ident(table)
            ),
            values.to_vec(),
        )
    }

    fn build_update_sql(
        &self,
        table: &str,
        set: &[(String, Value)],
        filter: Option<&Value>,
    ) -> (String, Vec<Value>) {
        let set_str = set
            .iter()
            .map(|(col, _)| format!("{} = ?", quote_ident(col)))
            .collect::<Vec<_>>()
            .join(", ");

        let mut sql = format!("UPDATE {} SET {set_str}", quote_ident(table));
        let mut params: Vec<Value> = set.iter().map(|(_, v)| v.clone()).collect();

        if let Some(f) = filter {
            let where_clause = translate_filter_to_sql(f);
            if !where_clause.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_clause);
            }
            collect_filter_values(f, &mut params);
        }

        (sql, params)
    }

    fn build_delete_sql(&self, table: &str, filter: Option<&Value>) -> (String, Vec<Value>) {
        let mut sql = format!("DELETE FROM {}", quote_ident(table));
        let mut params = Vec::new();

        if let Some(f) = filter {
            let where_clause = translate_filter_to_sql(f);
            if !where_clause.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_clause);
            }
            collect_filter_values(f, &mut params);
        }

        (sql, params)
    }

    fn build_upsert_sql(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        conflict_columns: &[String],
        update_columns: &[String],
    ) -> (String, Vec<Value>) {
        let cols = columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = vec!["?"; values.len()].join(", ");
        let conflict_cols = conflict_columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let update_str = update_columns
            .iter()
            .map(|col| format!("{} = ?", quote_ident(col)))
            .collect::<Vec<_>>()
            .join(", ");

        (
            format!(
                "INSERT INTO {} ({cols}) VALUES ({placeholders}) ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_str}",
                quote_ident(table)
            ),
            values.to_vec(),
        )
    }

    fn build_count_sql(&self, table: &str, filter: Option<&Value>) -> String {
        let mut sql = format!("SELECT COUNT(*) FROM {}", quote_ident(table));
        if let Some(where_clause) = filter.map(translate_filter_to_sql)
            && !where_clause.is_empty()
        {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clause);
        }
        sql
    }

    fn build_truncate_sql(&self, table: &str) -> String {
        format!("DELETE FROM {}", quote_ident(table))
    }

    fn build_drop_index_sql(
        &self,
        index_name: &str,
        _table_name: Option<&str>,
        if_exists: bool,
    ) -> String {
        if if_exists {
            format!("DROP INDEX IF EXISTS {}", quote_ident(index_name))
        } else {
            format!("DROP INDEX {}", quote_ident(index_name))
        }
    }

    fn version_query(&self) -> &'static str {
        "SELECT sqlite_version()"
    }

    fn supports_transactional_ddl(&self) -> bool {
        true
    }

    fn translate_filter(&self, filter: &Value) -> Result<String, DbError> {
        Ok(translate_filter_to_sql(filter))
    }
}

impl RelationalConnection for TursoConnection {}

impl ConnectionExt for TursoConnection {
    fn as_relational(&self) -> Option<&dyn RelationalConnection> {
        Some(self)
    }

    fn as_document(&self) -> Option<&dyn DocumentConnection> {
        None
    }

    fn as_keyvalue(&self) -> Option<&dyn KeyValueConnection> {
        None
    }
}

// =============================================================================
// Semantic planning (pure SQL generation, no I/O)
// =============================================================================

fn plan_semantic_request(request: &SemanticRequest) -> Result<SemanticPlan, DbError> {
    use dbflux_core::{PlannedQuery, SemanticPlanKind, render_semantic_filter_sql};

    let single = |sql: String| {
        SemanticPlan::single_query(
            SemanticPlanKind::Query,
            PlannedQuery::new(QueryLanguage::Sql, sql),
        )
    };

    match request {
        SemanticRequest::TableBrowse(request) => {
            let sql = if let Some(filter) = request.semantic_filter.as_ref() {
                let mut sql = format!(
                    "SELECT * FROM {} WHERE {}",
                    request.table.quoted_with(&TURSO_DIALECT),
                    render_semantic_filter_sql(filter, &TURSO_DIALECT)?
                );
                if !request.order_by.is_empty() {
                    let order_by = request
                        .order_by
                        .iter()
                        .map(|column| {
                            let direction = match column.direction {
                                SortDirection::Ascending => "ASC",
                                SortDirection::Descending => "DESC",
                            };
                            format!("{} {direction}", column.column.quoted_with(&TURSO_DIALECT))
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    sql.push_str(" ORDER BY ");
                    sql.push_str(&order_by);
                }
                sql.push_str(&format!(
                    " LIMIT {} OFFSET {}",
                    request.pagination.limit(),
                    request.pagination.offset()
                ));
                sql
            } else {
                request.build_sql_with(&TURSO_DIALECT)
            };
            Ok(single(sql))
        }
        SemanticRequest::TableCount(request) => {
            let quoted_table = request.table.quoted_with(&TURSO_DIALECT);
            let sql = if let Some(filter) = request.semantic_filter.as_ref() {
                format!(
                    "SELECT COUNT(*) FROM {quoted_table} WHERE {}",
                    render_semantic_filter_sql(filter, &TURSO_DIALECT)?
                )
            } else {
                match request.filter.as_deref().map(str::trim) {
                    Some(filter) if !filter.is_empty() => {
                        format!("SELECT COUNT(*) FROM {quoted_table} WHERE {filter}")
                    }
                    _ => format!("SELECT COUNT(*) FROM {quoted_table}"),
                }
            };
            Ok(single(sql))
        }
        SemanticRequest::Aggregate(request) => Ok(SemanticPlan::single_query(
            SemanticPlanKind::Query,
            PlannedQuery::new(QueryLanguage::Sql, request.build_sql_with(&TURSO_DIALECT)?)
                .with_database(request.target_database.clone()),
        )),
        SemanticRequest::Explain(request) => {
            let query = request.query.clone().unwrap_or_else(|| {
                format!(
                    "SELECT * FROM {} LIMIT 100",
                    request.table.quoted_with(&TURSO_DIALECT)
                )
            });
            Ok(single(format!("EXPLAIN QUERY PLAN {query}")))
        }
        SemanticRequest::Describe(request) => Ok(single(format!(
            "PRAGMA table_info({})",
            quote_ident(&request.table.name)
        ))),
        SemanticRequest::Mutation(mutation) => {
            static GENERATOR: SqlMutationGenerator = SqlMutationGenerator::new(&TURSO_DIALECT);
            GENERATOR.plan_mutation(mutation).ok_or_else(|| {
                DbError::NotSupported(
                    "Turso semantic planning does not support this mutation".into(),
                )
            })
        }
        _ => Err(DbError::NotSupported(
            "Turso semantic planning does not support this request".into(),
        )),
    }
}

// =============================================================================
// Error mapping
// =============================================================================

pub struct TursoErrorFormatter;

impl TursoErrorFormatter {
    fn format(error: &turso_serverless::Error) -> FormattedError {
        use turso_serverless::Error as E;
        match error {
            E::BatchStatementFailed { index, error, .. } => Self::format(error)
                .with_detail(format!("Statement {} of the batch failed", index + 1)),
            E::BatchRollbackFailed {
                error,
                rollback_error,
            } => Self::format(error).with_detail(format!("Rollback also failed: {rollback_error}")),
            E::Busy(message) | E::BusySnapshot(message) => {
                FormattedError::new(message.clone()).with_retriable(true)
            }
            E::Http(message) => FormattedError::new(sanitize_http_message(message)),
            other => FormattedError::new(other.to_string()),
        }
    }
}

impl QueryErrorFormatter for TursoErrorFormatter {
    fn format_query_error(&self, error: &(dyn std::error::Error + 'static)) -> FormattedError {
        match error.downcast_ref::<turso_serverless::Error>() {
            Some(sdk_error) => Self::format(sdk_error),
            None => FormattedError::new(error.to_string()),
        }
    }
}

/// Transport messages may embed the response body. Keep only the first line
/// so a server HTML error page does not end up in a toast.
fn sanitize_http_message(message: &str) -> String {
    message
        .lines()
        .next()
        .unwrap_or("HTTP request failed")
        .to_string()
}

/// Classifies an SDK error into the `DbError` variant the UI reacts to.
pub(crate) fn map_sdk_error(error: turso_serverless::Error) -> DbError {
    use turso_serverless::Error as E;

    let formatted = TursoErrorFormatter::format(&error);
    log::error!("Turso request failed: {}", formatted.to_display_string());

    let root = match &error {
        E::BatchStatementFailed { error, .. } | E::BatchRollbackFailed { error, .. } => {
            error.as_ref()
        }
        other => other,
    };

    match root {
        E::Constraint(_) => DbError::ConstraintViolation(formatted),
        E::Readonly(_) => DbError::PermissionDenied(formatted),
        E::Http(message) => {
            if is_auth_failure(message) {
                DbError::AuthFailed(formatted)
            } else {
                DbError::ConnectionFailed(formatted)
            }
        }
        E::Error(message) | E::Misuse(message) => classify_sql_message(message, formatted),
        _ => DbError::QueryFailed(formatted),
    }
}

fn is_auth_failure(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("401")
        || lower.contains("403")
        || lower.contains("unauthorized")
        || lower.contains("forbidden")
        || lower.contains("invalid token")
}

/// Server messages arrive with a prefix such as `SQLite error: ` or
/// `SQL string could not be parsed: `, so classification matches anywhere in
/// the text rather than at the start.
fn classify_sql_message(message: &str, formatted: FormattedError) -> DbError {
    let lower = message.to_ascii_lowercase();
    if lower.contains("syntax error")
        || lower.contains("parse error")
        || lower.contains("could not be parsed")
    {
        DbError::SyntaxError(formatted)
    } else if lower.contains("no such table")
        || lower.contains("no such column")
        || lower.contains("no such index")
        || lower.contains("no such view")
    {
        DbError::ObjectNotFound(formatted)
    } else if lower.contains("not authorized") || lower.contains("permission") {
        DbError::PermissionDenied(formatted)
    } else {
        DbError::QueryFailed(formatted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_serverless::Error as E;

    #[test]
    fn sdk_errors_map_to_ui_facing_variants() {
        assert!(matches!(
            map_sdk_error(E::Constraint("UNIQUE constraint failed".into())),
            DbError::ConstraintViolation(_)
        ));
        assert!(matches!(
            map_sdk_error(E::Http("HTTP 401 Unauthorized".into())),
            DbError::AuthFailed(_)
        ));
        assert!(matches!(
            map_sdk_error(E::Http("connection refused".into())),
            DbError::ConnectionFailed(_)
        ));
        assert!(matches!(
            map_sdk_error(E::Error("near \"SELEC\": syntax error".into())),
            DbError::SyntaxError(_)
        ));
        assert!(matches!(
            map_sdk_error(E::Error("SQLite error: no such table: nope".into())),
            DbError::ObjectNotFound(_)
        ));
        assert!(matches!(
            map_sdk_error(E::Error(
                "SQL string could not be parsed: syntax error around L1:6: `SELEC`".into()
            )),
            DbError::SyntaxError(_)
        ));
        assert!(matches!(
            map_sdk_error(E::Readonly("attempt to write a readonly database".into())),
            DbError::PermissionDenied(_)
        ));
    }

    #[test]
    fn batch_failures_unwrap_to_the_inner_statement_error() {
        let error = E::BatchStatementFailed {
            index: 1,
            error: Box::new(E::Constraint("NOT NULL constraint failed".into())),
            results: Vec::new(),
        };
        match map_sdk_error(error) {
            DbError::ConstraintViolation(formatted) => {
                assert_eq!(formatted.message, "NOT NULL constraint failed");
                assert_eq!(
                    formatted.detail.as_deref(),
                    Some("Statement 2 of the batch failed")
                );
            }
            other => panic!("unexpected mapping: {other:?}"),
        }
    }

    #[test]
    fn http_messages_keep_only_the_first_line() {
        let formatted = TursoErrorFormatter::format(&E::Http("HTTP 500\n<html>…".into()));
        assert_eq!(formatted.message, "HTTP 500");
        assert!(TursoErrorFormatter::format(&E::Busy("busy".into())).retriable);
    }

    #[test]
    fn check_constraints_are_extracted_from_ddl() {
        let constraints = extract_check_constraints(
            "CREATE TABLE t (a INT CHECK (a > 0), b TEXT, CHECK (length(b) < 10))",
        );
        assert_eq!(constraints.len(), 2);
        assert_eq!(constraints[0].check_clause.as_deref(), Some("a > 0"));
        assert_eq!(
            constraints[1].check_clause.as_deref(),
            Some("length(b) < 10")
        );
        assert!(extract_check_constraints("CREATE TABLE t (a INT)").is_empty());
    }

    #[test]
    fn pragma_helpers_and_value_readers() {
        assert_eq!(value_to_i64(&Value::Text("3".into())), Some(3));
        assert_eq!(value_to_i64(&Value::Float(1.0)), None);
        assert_eq!(value_to_string(Value::Int(7)), Some("7".into()));
        assert_eq!(value_to_string(Value::Null), None);
        assert_eq!(fk_action("NO ACTION".into()), None);
        assert_eq!(fk_action("CASCADE".into()), Some("CASCADE".into()));
    }
}
