use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Instant;

use chrono::{DateTime, NaiveDate, NaiveTime};
use dbflux_core::{
    ColumnInfo, ColumnKind, ColumnMeta, Connection, ConnectionExt, ConstraintInfo, ConstraintKind,
    CrudResult, DatabaseInfo, DbError, DbKind, DbSchemaInfo, DocumentConnection, DriverMetadata,
    ExecutionSession, ExecutionSessionFactory, ExplainRequest, ForeignKeyInfo, IndexData,
    IndexInfo, KeyValueConnection, QueryCancelHandle, QueryGenerator, QueryHandle, QueryRequest,
    QueryResult, RelationalConnection, RelationalSchema, Row, RowDelete, RowInsert, RowPatch,
    SchemaForeignKeyInfo, SchemaIndexInfo, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect,
    SqlMutationGenerator, SqlQueryBuilder, TableInfo, Value, ViewInfo,
};
use duckdb::arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
use duckdb::types::{TimeUnit, Value as DuckValue};

use crate::dialect::DUCKDB_DIALECT;
use crate::driver::{InstanceSettings, METADATA, fork_instance};

/// One DuckDB connection plus the handle that interrupts it from another
/// thread. The driver registry keeps weak references to these so a new
/// connection can be cloned from any that is still open.
pub(crate) struct SharedDuckDb {
    connection: Mutex<duckdb::Connection>,
    interrupt: Arc<duckdb::InterruptHandle>,
    key: String,
    settings: InstanceSettings,
}

impl SharedDuckDb {
    pub(crate) fn new(
        connection: duckdb::Connection,
        key: String,
        settings: InstanceSettings,
    ) -> Self {
        Self {
            interrupt: connection.interrupt_handle(),
            connection: Mutex::new(connection),
            key,
            settings,
        }
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn settings(&self) -> &InstanceSettings {
        &self.settings
    }

    fn lock(&self) -> Result<MutexGuard<'_, duckdb::Connection>, DbError> {
        self.connection
            .lock()
            .map_err(|_| DbError::connection_failed("DuckDB connection mutex poisoned"))
    }

    pub(crate) fn try_clone(&self) -> Result<duckdb::Connection, DbError> {
        self.lock()?
            .try_clone()
            .map_err(|error| DbError::connection_failed(format_duckdb_error(&error)))
    }
}

pub struct DuckDbConnection {
    shared: Arc<SharedDuckDb>,
    cancelled: Arc<AtomicBool>,
    active_catalog: Arc<Mutex<Option<String>>>,
    /// Present on the connection the driver returns, absent on the session
    /// connections it opens.
    sessions: Option<DuckDbSessionFactory>,
}

/// Opens isolated sessions, each its own DuckDB connection to the same
/// instance, so a transaction begun in the editor never captures the
/// sidebar, grid, or MCP requests that run on other sessions.
struct DuckDbSessionFactory {
    shared: Arc<SharedDuckDb>,
    root_catalog: Arc<Mutex<Option<String>>>,
    closed: AtomicBool,
    children: Arc<SessionRegistry>,
}

type SessionRegistry = Mutex<Vec<Weak<DuckDbSession>>>;

fn lock_sessions(
    registry: &SessionRegistry,
) -> Result<MutexGuard<'_, Vec<Weak<DuckDbSession>>>, DbError> {
    registry
        .lock()
        .map_err(|_| DbError::query_failed("DuckDB session registry poisoned"))
}

impl DuckDbSessionFactory {
    fn children(&self) -> Result<MutexGuard<'_, Vec<Weak<DuckDbSession>>>, DbError> {
        lock_sessions(&self.children)
    }
}

impl ExecutionSessionFactory for DuckDbSessionFactory {
    fn open(&self) -> Result<Arc<dyn ExecutionSession>, DbError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(DbError::connection_failed(
                "DuckDB connection is shut down; no new sessions can be opened",
            ));
        }

        let connection = DuckDbConnection::open(fork_instance(&self.shared)?, false)?;
        let catalog = self
            .root_catalog
            .lock()
            .map_err(|_| DbError::query_failed("DuckDB catalog mutex poisoned"))?
            .clone();
        if catalog.is_some() && catalog != connection.active_database() {
            connection.set_active_database(catalog.as_deref())?;
        }

        let session = Arc::new(DuckDbSession {
            connection: Arc::new(connection),
            closed: AtomicBool::new(false),
        });
        let mut children = self.children()?;
        children.retain(|child| child.strong_count() > 0);
        children.push(Arc::downgrade(&session));
        Ok(session)
    }

    fn shutdown(&self) -> Result<(), DbError> {
        self.closed.store(true, Ordering::SeqCst);
        let children: Vec<Arc<DuckDbSession>> = self
            .children()?
            .drain(..)
            .filter_map(|child| child.upgrade())
            .collect();

        let mut first_error = None;
        for child in children {
            if let Err(error) = child.close() {
                log::warn!("DuckDB session failed to close: {error}");
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

struct DuckDbSession {
    connection: Arc<DuckDbConnection>,
    closed: AtomicBool,
}

impl ExecutionSession for DuckDbSession {
    fn connection(&self) -> Arc<dyn Connection> {
        self.connection.clone()
    }

    fn close(&self) -> Result<(), DbError> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        self.connection.rollback_open_transaction().map(|_| ())
    }

    /// Ends one scoped operation. A transaction still open here was begun
    /// through a surface that cannot finish it, so it is rolled back and
    /// reported rather than left open on the shared instance.
    fn finish_operation(&self) -> Result<(), DbError> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        if self.connection.rollback_open_transaction()? {
            return Err(DbError::query_failed(
                "Operation finished with an open transaction; it was rolled back",
            ));
        }
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

struct DuckDbCancelHandle {
    shared: Arc<SharedDuckDb>,
    cancelled: Arc<AtomicBool>,
    /// The editor runs on session connections, each with its own interrupt
    /// handle, while cancel reaches the root, so the root interrupts them too.
    sessions: Option<Arc<SessionRegistry>>,
}

impl QueryCancelHandle for DuckDbCancelHandle {
    fn cancel(&self) -> Result<(), DbError> {
        self.cancelled.store(true, Ordering::SeqCst);
        self.shared.interrupt.interrupt();
        if let Some(sessions) = &self.sessions {
            let open: Vec<Arc<DuckDbSession>> = lock_sessions(sessions)?
                .iter()
                .filter_map(Weak::upgrade)
                .filter(|session| !session.is_closed())
                .collect();
            for session in open {
                if let Err(error) = session.connection.cancel_active() {
                    log::warn!("DuckDB failed to cancel a session query: {error}");
                }
            }
        }
        Ok(())
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

pub(crate) fn format_duckdb_error(error: &duckdb::Error) -> String {
    match error {
        duckdb::Error::DuckDBFailure(_, Some(message)) => message.clone(),
        other => other.to_string(),
    }
}

impl DuckDbConnection {
    pub(crate) fn new(shared: Arc<SharedDuckDb>) -> Result<Self, DbError> {
        Self::open(shared, true)
    }

    fn open(shared: Arc<SharedDuckDb>, with_sessions: bool) -> Result<Self, DbError> {
        let active_catalog = Arc::new(Mutex::new(None));
        let sessions = with_sessions.then(|| DuckDbSessionFactory {
            shared: shared.clone(),
            root_catalog: active_catalog.clone(),
            closed: AtomicBool::new(false),
            children: Arc::new(Mutex::new(Vec::new())),
        });
        let connection = Self {
            shared,
            cancelled: Arc::new(AtomicBool::new(false)),
            active_catalog,
            sessions,
        };
        let current = connection.query_strings("SELECT current_database()")?;
        *connection.catalog_slot()? = current.into_iter().next();
        Ok(connection)
    }

    fn catalog_slot(&self) -> Result<MutexGuard<'_, Option<String>>, DbError> {
        self.active_catalog
            .lock()
            .map_err(|_| DbError::query_failed("DuckDB catalog mutex poisoned"))
    }

    fn query_error(&self, error: duckdb::Error) -> DbError {
        if self.cancelled.load(Ordering::SeqCst) {
            DbError::Cancelled
        } else {
            DbError::query_failed(format_duckdb_error(&error))
        }
    }

    /// Takes the connection for one request. The cancel flag is cleared only
    /// once the lock is held, so a request waiting behind a query being
    /// cancelled cannot clear the flag that query is about to read.
    fn begin_request(&self) -> Result<MutexGuard<'_, duckdb::Connection>, DbError> {
        let connection = self.shared.lock()?;
        self.cancelled.store(false, Ordering::SeqCst);
        Ok(connection)
    }

    fn run(&self, sql: &str, params: &[Value], limit: Option<u32>) -> Result<QueryResult, DbError> {
        let started = Instant::now();
        let connection = self.begin_request()?;
        run_query(&connection, sql, params, limit, started).map_err(|error| self.query_error(error))
    }

    /// Runs `sql` inside a `READ ONLY` transaction, which DuckDB enforces.
    /// DuckDB's own parser must first accept the text as SELECT statements
    /// only (`json_serialize_sql` refuses anything else), so no statement can
    /// end the read-only transaction and write after it. A read-only
    /// transaction still lets a SELECT read files and URLs, so the parsed
    /// statements are also checked for table functions and file names that
    /// reach outside the database.
    fn run_read_only(
        &self,
        sql: &str,
        params: &[Value],
        limit: Option<u32>,
    ) -> Result<QueryResult, DbError> {
        let started = Instant::now();
        let connection = self.begin_request()?;

        let serialized: String = connection
            .query_row(
                "SELECT json_serialize_sql(?::VARCHAR)::VARCHAR",
                [sql],
                |row| row.get(0),
            )
            .map_err(|error| self.query_error(error))?;
        if let Some(reason) = read_only_rejection(&serialized) {
            return Err(DbError::NotSupported(format!(
                "DuckDB: read-only enforcement {reason}; the request was rejected before execution"
            )));
        }
        // duckdb-rs always reports autocommit, so let DuckDB refuse the BEGIN
        // when the session is already inside a transaction.
        if let Err(error) = connection.execute_batch("BEGIN TRANSACTION READ ONLY") {
            return Err(DbError::NotSupported(format!(
                "DuckDB: read-only enforcement could not start a read-only transaction ({}); the request was rejected before execution",
                format_duckdb_error(&error)
            )));
        }
        let result = run_query(&connection, sql, params, limit, started);
        let rollback = connection.execute_batch("ROLLBACK");

        let result = result.map_err(|error| self.query_error(error))?;
        rollback.map_err(|error| self.query_error(error))?;
        Ok(result)
    }

    /// Rolls back the open transaction, if any, and reports whether there
    /// was one.
    fn rollback_open_transaction(&self) -> Result<bool, DbError> {
        let connection = self.shared.lock()?;
        match connection.execute_batch("ROLLBACK") {
            Ok(()) => Ok(true),
            // duckdb-rs cannot report the transaction state, so DuckDB
            // refusing the ROLLBACK is how an idle session shows.
            Err(error) if format_duckdb_error(&error).contains("no transaction is active") => {
                Ok(false)
            }
            Err(error) => Err(self.query_error(error)),
        }
    }

    /// First column of every row, as text.
    fn query_strings(&self, sql: &str) -> Result<Vec<String>, DbError> {
        Ok(self
            .run(sql, &[], None)?
            .rows
            .into_iter()
            .filter_map(|row| match row.into_iter().next() {
                Some(Value::Text(text)) => Some(text),
                _ => None,
            })
            .collect())
    }

    fn literal(text: &str) -> String {
        format!("'{}'", DUCKDB_DIALECT.escape_string(text))
    }

    /// The primary key columns, foreign keys, and CHECK and UNIQUE constraints
    /// of each table `filter` selects, by table name.
    fn load_constraints(
        &self,
        filter: &str,
        schema: &str,
    ) -> Result<BTreeMap<String, TableConstraints>, DbError> {
        let rows = self
            .run(
                &format!(
                    "SELECT table_name, constraint_type, constraint_name, constraint_column_names, \
                     referenced_table, referenced_column_names, expression \
                     FROM duckdb_constraints() WHERE {filter} ORDER BY table_name, constraint_index"
                ),
                &[],
                None,
            )?
            .rows;

        let mut tables = BTreeMap::<String, TableConstraints>::new();
        for row in rows {
            let Ok(
                [
                    table,
                    kind,
                    name,
                    columns,
                    referenced_table,
                    referenced_columns,
                    expression,
                ],
            ) = <[Value; 7]>::try_from(row)
            else {
                continue;
            };
            let (Value::Text(table), Value::Text(kind), Value::Text(name)) = (table, kind, name)
            else {
                continue;
            };
            let TableConstraints {
                primary_key,
                foreign_keys,
                checks_and_uniques,
            } = tables.entry(table).or_default();
            let columns = text_list(columns);
            match kind.as_str() {
                "PRIMARY KEY" => *primary_key = columns,
                "FOREIGN KEY" => {
                    if let Value::Text(referenced_table) = referenced_table {
                        foreign_keys.push(ForeignKeyInfo {
                            name,
                            columns,
                            referenced_table,
                            // DuckDB only allows foreign keys within one schema.
                            referenced_schema: Some(schema.to_string()),
                            referenced_columns: text_list(referenced_columns),
                            on_delete: None,
                            on_update: None,
                        });
                    }
                }
                "UNIQUE" => checks_and_uniques.push(ConstraintInfo {
                    name,
                    kind: ConstraintKind::Unique,
                    columns,
                    check_clause: None,
                }),
                "CHECK" => checks_and_uniques.push(ConstraintInfo {
                    name,
                    kind: ConstraintKind::Check,
                    columns,
                    check_clause: match expression {
                        Value::Text(expression) => Some(expression),
                        _ => None,
                    },
                }),
                _ => {}
            }
        }
        Ok(tables)
    }

    fn load_columns(
        &self,
        filter: &str,
        primary_key: &[String],
    ) -> Result<Vec<ColumnInfo>, DbError> {
        Ok(self
            .run(
                &format!(
                    "SELECT column_name, data_type, is_nullable, column_default \
                     FROM duckdb_columns() WHERE {filter} ORDER BY column_index"
                ),
                &[],
                None,
            )?
            .rows
            .into_iter()
            .filter_map(|row| match <[Value; 4]>::try_from(row) {
                Ok([Value::Text(name), Value::Text(type_name), nullable, default]) => {
                    Some(ColumnInfo {
                        is_primary_key: primary_key.contains(&name),
                        name,
                        type_name,
                        nullable: nullable != Value::Bool(false),
                        default_value: match default {
                            Value::Text(default) => Some(default),
                            _ => None,
                        },
                        enum_values: None,
                    })
                }
                _ => None,
            })
            .collect())
    }

    fn load_indexes(
        &self,
        filter: &str,
        table: &str,
        primary_key: Vec<String>,
    ) -> Result<Vec<IndexInfo>, DbError> {
        let mut indexes: Vec<IndexInfo> =
            primary_key_index(table, primary_key).into_iter().collect();
        indexes.extend(
            self.load_index_rows(filter)?
                .into_iter()
                .map(|(_, index)| index),
        );
        Ok(indexes)
    }

    /// The indexes `filter` selects with the table each belongs to. Primary
    /// keys have no entry here; they come from the constraints.
    fn load_index_rows(&self, filter: &str) -> Result<Vec<(String, IndexInfo)>, DbError> {
        // `expressions` is the indexed expression list as text, such as `[a, b]`.
        Ok(self
            .run(
                &format!(
                    "SELECT table_name, index_name, is_unique, expressions FROM duckdb_indexes() \
                     WHERE {filter} ORDER BY table_name, index_name"
                ),
                &[],
                None,
            )?
            .rows
            .into_iter()
            .filter_map(|row| match <[Value; 4]>::try_from(row) {
                Ok(
                    [
                        Value::Text(table),
                        Value::Text(name),
                        is_unique,
                        Value::Text(expressions),
                    ],
                ) => Some((
                    table,
                    IndexInfo {
                        name,
                        columns: expressions
                            .trim_start_matches('[')
                            .trim_end_matches(']')
                            .split(", ")
                            .filter(|column| !column.is_empty())
                            .map(str::to_string)
                            .collect(),
                        is_unique: is_unique == Value::Bool(true),
                        is_primary: false,
                    },
                )),
                _ => None,
            })
            .collect())
    }

    /// The filter selecting every object of `schema` in `database`, the active
    /// catalog when `database` is empty.
    fn schema_filter(&self, database: &str, schema: Option<&str>) -> (String, String) {
        let catalog = match database {
            "" => self.active_database().unwrap_or_default(),
            name => name.to_string(),
        };
        let schema = schema.unwrap_or("main").to_string();
        let filter = format!(
            "database_name = {} AND schema_name = {}",
            Self::literal(&catalog),
            Self::literal(&schema)
        );
        (filter, schema)
    }

    /// Runs an insert or delete with `RETURNING *` and reports the first
    /// returned row.
    fn run_returning(&self, sql: Option<String>) -> Result<CrudResult, DbError> {
        let sql = sql.ok_or_else(|| DbError::query_failed("Failed to build the statement"))?;
        log::debug!("[DUCKDB] {sql}");
        let result = self.run(&sql, &[], None)?;
        let affected = result.rows.len() as u64;
        match result.rows.into_iter().next() {
            Some(row) => Ok(CrudResult::new(affected, Some(row))),
            None => Ok(CrudResult::empty()),
        }
    }
}

impl Connection for DuckDbConnection {
    fn metadata(&self) -> &DriverMetadata {
        &METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        self.run("SELECT 1", &[], None).map(|_| ())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, request: &QueryRequest) -> Result<QueryResult, DbError> {
        if request.statement_timeout.is_some() {
            return Err(DbError::NotSupported(
                "DuckDB: the requested statement timeout cannot be honored; the request was rejected before execution".to_string(),
            ));
        }
        if request.read_only.is_required() {
            return self.run_read_only(&request.sql, &request.params, request.limit);
        }

        self.run(&request.sql, &request.params, request.limit)
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        self.cancel_active()
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.cancel_handle().cancel()
    }

    fn cancel_handle(&self) -> Arc<dyn QueryCancelHandle> {
        Arc::new(DuckDbCancelHandle {
            shared: self.shared.clone(),
            cancelled: self.cancelled.clone(),
            sessions: self
                .sessions
                .as_ref()
                .map(|factory| factory.children.clone()),
        })
    }

    /// `schema()` only lists catalogs; their tables load per database through
    /// `schema_for_database`. Declaring the snapshot authoritative would make
    /// the sidebar show the current catalog as empty.
    fn schema_snapshot_authority(&self) -> dbflux_core::SchemaSnapshotAuthority {
        dbflux_core::SchemaSnapshotAuthority::EnumerationOnly
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        // A `USE` run from the editor changes the catalog behind our back.
        let current = self.query_strings("SELECT current_database()")?;
        *self.catalog_slot()? = current.into_iter().next();

        Ok(SchemaSnapshot::relational(RelationalSchema {
            databases: self.list_databases()?,
            current_database: self.active_database(),
            schemas: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
        }))
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        let current = self.active_database();
        Ok(self
            .query_strings(
                "SELECT database_name FROM duckdb_databases() WHERE NOT internal ORDER BY database_name",
            )?
            .into_iter()
            .map(|name| DatabaseInfo {
                is_current: current.as_deref() == Some(name.as_str()),
                name,
            })
            .collect())
    }

    fn schema_for_database(&self, name: &str) -> Result<DbSchemaInfo, DbError> {
        let database = Self::literal(name);
        let pairs = |sql: String| -> Result<Vec<(String, String)>, DbError> {
            Ok(self
                .run(&sql, &[], None)?
                .rows
                .into_iter()
                .filter_map(|row| match <[Value; 2]>::try_from(row) {
                    Ok([Value::Text(schema), Value::Text(name)]) => Some((schema, name)),
                    _ => None,
                })
                .collect())
        };

        let tables = pairs(format!(
            "SELECT schema_name, table_name FROM duckdb_tables() \
             WHERE database_name = {database} AND NOT internal ORDER BY 1, 2"
        ))?
        .into_iter()
        .map(|(schema, name)| TableInfo {
            name,
            schema: Some(schema),
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::DataGrid,
            child_items: None,
            storage_hints: None,
            pseudo_columns: Box::default(),
        })
        .collect();

        let views = pairs(format!(
            "SELECT schema_name, view_name FROM duckdb_views() \
             WHERE database_name = {database} AND NOT internal ORDER BY 1, 2"
        ))?
        .into_iter()
        .map(|(schema, name)| ViewInfo {
            name,
            schema: Some(schema),
        })
        .collect();

        Ok(DbSchemaInfo {
            name: name.to_string(),
            tables,
            views,
            custom_types: None,
        })
    }

    fn table_details(
        &self,
        database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        let (schema_filter, schema) = self.schema_filter(database, schema);
        let filter = format!("{schema_filter} AND table_name = {}", Self::literal(table));

        let TableConstraints {
            primary_key,
            foreign_keys,
            checks_and_uniques,
        } = self
            .load_constraints(&filter, &schema)?
            .remove(table)
            .unwrap_or_default();
        let columns = self.load_columns(&filter, &primary_key)?;
        let indexes = self.load_indexes(&filter, table, primary_key)?;

        Ok(TableInfo {
            name: table.to_string(),
            schema: Some(schema),
            columns: Some(columns),
            indexes: Some(IndexData::Relational(indexes)),
            foreign_keys: Some(foreign_keys),
            constraints: Some(checks_and_uniques),
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::DataGrid,
            child_items: None,
            storage_hints: None,
            pseudo_columns: Box::default(),
        })
    }

    fn schema_indexes(
        &self,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<SchemaIndexInfo>, DbError> {
        let (filter, schema) = self.schema_filter(database, schema);
        let primary_keys = self
            .load_constraints(&filter, &schema)?
            .into_iter()
            .filter_map(|(table, constraints)| {
                primary_key_index(&table, constraints.primary_key).map(|index| (table, index))
            });
        let mut indexes: Vec<(String, IndexInfo)> = primary_keys.collect();
        indexes.extend(self.load_index_rows(&filter)?);
        Ok(indexes
            .into_iter()
            .map(|(table_name, index)| SchemaIndexInfo {
                name: index.name,
                table_name,
                columns: index.columns,
                is_unique: index.is_unique,
                is_primary: index.is_primary,
            })
            .collect())
    }

    fn schema_foreign_keys(
        &self,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
        let (filter, schema) = self.schema_filter(database, schema);
        Ok(self
            .load_constraints(&filter, &schema)?
            .into_iter()
            .flat_map(|(table_name, constraints)| {
                constraints
                    .foreign_keys
                    .into_iter()
                    .map(move |key| SchemaForeignKeyInfo {
                        name: key.name,
                        table_name: table_name.clone(),
                        columns: key.columns,
                        referenced_schema: key.referenced_schema,
                        referenced_table: key.referenced_table,
                        referenced_columns: key.referenced_columns,
                        on_delete: key.on_delete,
                        on_update: key.on_update,
                    })
            })
            .collect())
    }

    fn set_active_database(&self, database: Option<&str>) -> Result<(), DbError> {
        let Some(database) = database else {
            return Ok(());
        };
        self.run(
            &format!("USE {}", DUCKDB_DIALECT.quote_identifier(database)),
            &[],
            None,
        )?;
        *self.catalog_slot()? = Some(database.to_string());
        Ok(())
    }

    fn active_database(&self) -> Option<String> {
        self.active_catalog
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
    }

    fn kind(&self) -> DbKind {
        DbKind::DuckDB
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::LazyPerDatabase
    }

    fn referenced_tables(&self, query: &str) -> Option<Vec<dbflux_core::QueryTableRef>> {
        Some(dbflux_core::extract_referenced_tables(query))
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
        // DuckDB refuses `UPDATE ... RETURNING` on a row a foreign key
        // references, even when no key column changes, so the row is read back
        // separately.
        let builder = SqlQueryBuilder::new(&DUCKDB_DIALECT);
        let update = builder
            .build_update(patch, false)
            .ok_or_else(|| DbError::query_failed("Failed to build the statement"))?;
        log::debug!("[DUCKDB] {update}");
        let affected = match self
            .run(&update, &[], None)?
            .rows
            .first()
            .map(Vec::as_slice)
        {
            Some([Value::Int(count)]) => *count,
            _ => 0,
        };
        if affected == 0 {
            return Ok(CrudResult::empty());
        }

        let select = builder
            .build_select_by_identity(patch.schema.as_deref(), &patch.table, &patch.identity)
            .ok_or_else(|| DbError::query_failed("Failed to build the statement"))?;
        let row = self.run(&select, &[], None)?.rows.into_iter().next();
        Ok(CrudResult::new(affected as u64, row))
    }

    fn insert_row(&self, insert: &RowInsert) -> Result<CrudResult, DbError> {
        if !insert.is_valid() {
            return Err(DbError::query_failed(
                "Cannot insert row: no columns specified",
            ));
        }
        self.run_returning(SqlQueryBuilder::new(&DUCKDB_DIALECT).build_insert(insert, true))
    }

    fn delete_row(&self, delete: &RowDelete) -> Result<CrudResult, DbError> {
        if !delete.is_valid() {
            return Err(DbError::query_failed(
                "Cannot delete row: invalid row identity (missing primary key)",
            ));
        }
        self.run_returning(SqlQueryBuilder::new(&DUCKDB_DIALECT).build_delete(delete, true))
    }

    fn explain(&self, request: &ExplainRequest) -> Result<QueryResult, DbError> {
        let query = match &request.query {
            Some(query) => query.clone(),
            None => format!(
                "SELECT * FROM {} LIMIT 100",
                request.table.quoted_with(self.dialect())
            ),
        };
        self.run(&format!("EXPLAIN {query}"), &[], None)
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &DUCKDB_DIALECT
    }

    fn execution_session_factory(&self) -> Option<&dyn ExecutionSessionFactory> {
        self.sessions
            .as_ref()
            .map(|factory| factory as &dyn ExecutionSessionFactory)
    }

    fn query_generator(&self) -> Option<&dyn QueryGenerator> {
        static GENERATOR: SqlMutationGenerator = SqlMutationGenerator::new(&DUCKDB_DIALECT);
        Some(&GENERATOR)
    }

    fn version_query(&self) -> &'static str {
        "SELECT version()"
    }

    fn supports_transactional_ddl(&self) -> bool {
        true
    }
}

impl RelationalConnection for DuckDbConnection {}

impl ConnectionExt for DuckDbConnection {
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

/// Why the serialized statements in `serialized` may not run as a read-only
/// request, or `None` when they may.
fn read_only_rejection(serialized: &str) -> Option<String> {
    let parsed: serde_json::Value = match serde_json::from_str(serialized) {
        Ok(parsed) => parsed,
        Err(error) => return Some(format!("could not read the parsed statements ({error})")),
    };
    if parsed.get("error").and_then(serde_json::Value::as_bool) != Some(false) {
        // A syntax error also lands here, and its message says more than the
        // SELECT-only rule does.
        return Some(
            match parsed
                .get("error_message")
                .and_then(serde_json::Value::as_str)
            {
                Some(message) if !message.is_empty() => {
                    format!("accepts SELECT statements only ({message})")
                }
                _ => "accepts SELECT statements only".to_string(),
            },
        );
    }
    parsed.get("statements").and_then(external_reference)
}

/// The first table reference in a parsed statement that can read files or
/// the network: a table function outside a fixed allow-list, or a table name
/// DuckDB would resolve as a file path (`FROM 'data.csv'`).
fn external_reference(node: &serde_json::Value) -> Option<String> {
    use serde_json::Value as Json;

    match node {
        Json::Array(items) => items.iter().find_map(external_reference),
        Json::Object(fields) => {
            fn text(value: Option<&Json>) -> &str {
                value.and_then(Json::as_str).unwrap_or_default()
            }
            match text(fields.get("type")) {
                "TABLE_FUNCTION" => {
                    let name = text(fields.get("function").and_then(|f| f.get("function_name")));
                    if !is_internal_table_function(name) {
                        return Some(format!(
                            "does not allow the table function `{name}`, which can read files or the network"
                        ));
                    }
                }
                "BASE_TABLE" => {
                    let name = text(fields.get("table_name"));
                    if name.contains(['.', '/', '\\', ':']) {
                        return Some(format!(
                            "does not allow the table name `{name}`, which DuckDB can read as a file"
                        ));
                    }
                }
                _ => {}
            }
            fields.values().find_map(external_reference)
        }
        _ => None,
    }
}

/// Table functions that only read the database's own state.
fn is_internal_table_function(name: &str) -> bool {
    matches!(
        name,
        "range" | "generate_series" | "unnest" | "json_each" | "json_tree"
    ) || name.starts_with("pragma_")
        || name.starts_with("ducklake_")
        || (name.starts_with("duckdb_") && name != "duckdb_secrets")
}

/// Runs every statement in `sql` and returns the rows of the last one,
/// keeping at most `limit` of them.
fn run_query(
    connection: &duckdb::Connection,
    sql: &str,
    params: &[Value],
    limit: Option<u32>,
    started: Instant,
) -> Result<QueryResult, duckdb::Error> {
    let mut statement = connection.prepare(sql)?;
    let params = params.iter().map(to_duckdb_value);
    let mut rows = statement.query(duckdb::params_from_iter(params))?;

    let columns: Vec<ColumnMeta> = match rows.as_ref() {
        Some(statement) => (0..statement.column_count())
            .map(|index| {
                let data_type = statement.column_type(index);
                Ok(ColumnMeta {
                    name: statement.column_name(index)?.clone(),
                    type_name: type_name(&data_type),
                    kind: column_kind(&data_type),
                    nullable: true,
                    is_primary_key: false,
                })
            })
            .collect::<Result<_, duckdb::Error>>()?,
        None => Vec::new(),
    };

    let mut collected = Vec::new();
    let mut truncated = false;
    while let Some(row) = rows.next()? {
        if limit.is_some_and(|limit| collected.len() >= limit as usize) {
            truncated = true;
            break;
        }
        let values = (0..columns.len())
            .map(|index| row.get::<_, DuckValue>(index).map(from_duckdb_value))
            .collect::<Result<Row, _>>()?;
        collected.push(values);
    }

    let mut result = QueryResult::table(columns, collected, None, started.elapsed());
    result.set_rows_truncated(truncated);
    Ok(result)
}

fn to_duckdb_value(value: &Value) -> DuckValue {
    match value {
        Value::Null | Value::Unsupported(_) => DuckValue::Null,
        Value::Bool(value) => DuckValue::Boolean(*value),
        Value::Int(value) => DuckValue::BigInt(*value),
        Value::Float(value) => DuckValue::Double(*value),
        Value::Bytes(bytes) => DuckValue::Blob(bytes.clone()),
        Value::Text(text) | Value::Json(text) | Value::Decimal(text) | Value::ObjectId(text) => {
            DuckValue::Text(text.clone())
        }
        Value::DateTime(value) => DuckValue::Text(value.to_rfc3339()),
        Value::Date(value) => DuckValue::Text(value.to_string()),
        Value::Time(value) => DuckValue::Text(value.to_string()),
        Value::Array(values) => DuckValue::List(values.iter().map(to_duckdb_value).collect()),
        Value::Document(_) => DuckValue::Text(DUCKDB_DIALECT.value_to_literal(value)),
    }
}

#[derive(Default)]
struct TableConstraints {
    primary_key: Vec<String>,
    foreign_keys: Vec<ForeignKeyInfo>,
    checks_and_uniques: Vec<ConstraintInfo>,
}

/// DuckDB lists no index for a primary key, so the key is shown as one.
fn primary_key_index(table: &str, primary_key: Vec<String>) -> Option<IndexInfo> {
    (!primary_key.is_empty()).then(|| IndexInfo {
        name: format!("{table}_pkey"),
        columns: primary_key,
        is_unique: true,
        is_primary: true,
    })
}

fn text_list(value: Value) -> Vec<String> {
    match value {
        Value::Array(items) => items
            .into_iter()
            .filter_map(|item| match item {
                Value::Text(text) => Some(text),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn integer(value: i128) -> Value {
    i64::try_from(value)
        .map(Value::Int)
        .unwrap_or_else(|_| Value::Decimal(value.to_string()))
}

fn from_duckdb_value(value: DuckValue) -> Value {
    match value {
        DuckValue::Null => Value::Null,
        DuckValue::Boolean(value) => Value::Bool(value),
        DuckValue::TinyInt(value) => Value::Int(value.into()),
        DuckValue::SmallInt(value) => Value::Int(value.into()),
        DuckValue::Int(value) => Value::Int(value.into()),
        DuckValue::BigInt(value) => Value::Int(value),
        DuckValue::UTinyInt(value) => Value::Int(value.into()),
        DuckValue::USmallInt(value) => Value::Int(value.into()),
        DuckValue::UInt(value) => Value::Int(value.into()),
        DuckValue::UBigInt(value) => integer(value.into()),
        DuckValue::HugeInt(value) => integer(value),
        DuckValue::UHugeInt(value) => i128::try_from(value)
            .map(integer)
            .unwrap_or_else(|_| Value::Decimal(value.to_string())),
        // Widening the f32 directly would show 3.14 as 3.140000104904175.
        DuckValue::Float(value) => Value::Float(
            value
                .to_string()
                .parse()
                .unwrap_or_else(|_| f64::from(value)),
        ),
        DuckValue::Double(value) => Value::Float(value),
        DuckValue::Decimal(value) => Value::Decimal(value.to_string()),
        DuckValue::Timestamp(unit, value) => DateTime::from_timestamp_micros(unit.to_micros(value))
            .map(Value::DateTime)
            .unwrap_or_else(|| Value::Unsupported(format!("timestamp {value}"))),
        DuckValue::Text(text) | DuckValue::Enum(text) => Value::Text(text),
        DuckValue::Blob(bytes) | DuckValue::Geometry(bytes) => Value::Bytes(bytes),
        DuckValue::Date32(days) => NaiveDate::from_ymd_opt(1970, 1, 1)
            .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(days.into())))
            .map(Value::Date)
            .unwrap_or_else(|| Value::Unsupported(format!("date {days}"))),
        DuckValue::Time64(unit, value) => {
            let micros = TimeUnit::to_micros(&unit, value);
            NaiveTime::from_num_seconds_from_midnight_opt(
                (micros / 1_000_000) as u32,
                ((micros % 1_000_000) * 1000) as u32,
            )
            .map(Value::Time)
            .unwrap_or_else(|| Value::Unsupported(format!("time {value}")))
        }
        DuckValue::Interval {
            months,
            days,
            nanos,
        } => Value::Text(interval_text(months, days, nanos)),
        DuckValue::List(values) | DuckValue::Array(values) => {
            Value::Array(values.into_iter().map(from_duckdb_value).collect())
        }
        DuckValue::Struct(fields) => Value::Document(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), from_duckdb_value(value.clone())))
                .collect::<BTreeMap<_, _>>(),
        ),
        DuckValue::Map(entries) => Value::Document(
            entries
                .iter()
                .map(|(key, value)| {
                    let key = match from_duckdb_value(key.clone()) {
                        Value::Text(text) => text,
                        other => DUCKDB_DIALECT.value_to_literal(&other),
                    };
                    (key, from_duckdb_value(value.clone()))
                })
                .collect::<BTreeMap<_, _>>(),
        ),
        DuckValue::Union(value) => from_duckdb_value(*value),
        other => Value::Unsupported(format!("{other:?}")),
    }
}

/// An interval in DuckDB's own notation, such as `1 year 2 months 3 days 00:01:30`.
fn interval_text(months: i32, days: i32, nanos: i64) -> String {
    let mut parts: Vec<String> = [(months / 12, "year"), (months % 12, "month"), (days, "day")]
        .into_iter()
        .filter(|(value, _)| *value != 0)
        .map(|(value, unit)| {
            let plural = if value.abs() == 1 { "" } else { "s" };
            format!("{value} {unit}{plural}")
        })
        .collect();

    let micros = nanos / 1000;
    if micros != 0 || parts.is_empty() {
        let sign = if micros < 0 { "-" } else { "" };
        let micros = micros.unsigned_abs();
        let seconds = micros / 1_000_000;
        let mut time = format!(
            "{sign}{:02}:{:02}:{:02}",
            seconds / 3600,
            seconds / 60 % 60,
            seconds % 60
        );
        if !micros.is_multiple_of(1_000_000) {
            time.push_str(&format!(".{:06}", micros % 1_000_000));
        }
        parts.push(time);
    }
    parts.join(" ")
}

fn type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "NULL".into(),
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::UInt8 => "UTINYINT".into(),
        DataType::UInt16 => "USMALLINT".into(),
        DataType::UInt32 => "UINTEGER".into(),
        DataType::UInt64 => "UBIGINT".into(),
        DataType::Float16 | DataType::Float32 => "FLOAT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "VARCHAR".into(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BLOB".into(),
        DataType::Date32 | DataType::Date64 => "DATE".into(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".into(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP WITH TIME ZONE".into(),
        DataType::Timestamp(unit, None) => match unit {
            ArrowTimeUnit::Second => "TIMESTAMP_S".into(),
            ArrowTimeUnit::Millisecond => "TIMESTAMP_MS".into(),
            ArrowTimeUnit::Microsecond => "TIMESTAMP".into(),
            ArrowTimeUnit::Nanosecond => "TIMESTAMP_NS".into(),
        },
        DataType::Interval(_) | DataType::Duration(_) => "INTERVAL".into(),
        DataType::List(_) | DataType::LargeList(_) | DataType::ListView(_) => "LIST".into(),
        DataType::FixedSizeList(_, size) => format!("ARRAY[{size}]"),
        DataType::Struct(_) => "STRUCT".into(),
        DataType::Map(_, _) => "MAP".into(),
        DataType::Union(_, _) => "UNION".into(),
        DataType::Dictionary(_, _) => "ENUM".into(),
        other => format!("{other:?}"),
    }
}

fn column_kind(data_type: &DataType) -> ColumnKind {
    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => ColumnKind::Integer,
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => ColumnKind::Float,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => ColumnKind::Timestamp,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Dictionary(_, _) => {
            ColumnKind::Text
        }
        _ => ColumnKind::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DuckDbDriver;
    use dbflux_core::{ConnectionProfile, DbConfig, DbDriver, RowIdentity, RowPatch};

    fn connect() -> Box<dyn Connection> {
        DuckDbDriver::new()
            .connect(&ConnectionProfile::new("duck", DbConfig::default_duckdb()))
            .unwrap()
    }

    #[test]
    fn executes_multiple_statements_and_maps_types() {
        let connection = connect();
        let result = connection
            .execute(&QueryRequest::new(
                "CREATE TABLE t (id INTEGER, price DECIMAL(10,2), ts TIMESTAMP, tags VARCHAR[], meta STRUCT(k VARCHAR));
                 INSERT INTO t VALUES (1, 9.99, TIMESTAMP '2026-01-02 03:04:05', ['a', 'b'], {'k': 'v'});
                 SELECT * FROM t",
            ))
            .unwrap();

        let kinds: Vec<_> = result.columns.iter().map(|column| column.kind).collect();
        assert_eq!(
            kinds,
            [
                ColumnKind::Integer,
                ColumnKind::Float,
                ColumnKind::Timestamp,
                ColumnKind::Unknown,
                ColumnKind::Unknown
            ]
        );
        let row = &result.rows[0];
        assert_eq!(row[0], Value::Int(1));
        assert_eq!(row[1], Value::Decimal("9.99".into()));
        assert!(
            matches!(&row[2], Value::DateTime(ts) if ts.to_rfc3339() == "2026-01-02T03:04:05+00:00")
        );
        assert_eq!(
            row[3],
            Value::Array(vec![Value::Text("a".into()), Value::Text("b".into())])
        );
        assert!(
            matches!(&row[4], Value::Document(fields) if fields.get("k") == Some(&Value::Text("v".into())))
        );
    }

    #[test]
    fn row_limit_truncates_the_result() {
        let connection = connect();
        let mut request = QueryRequest::new("SELECT * FROM range(10)");
        request.limit = Some(3);
        let result = connection.execute(&request).unwrap();
        assert_eq!(result.rows.len(), 3);
        assert!(result.rows_truncated());
    }

    #[test]
    fn lists_catalogs_tables_and_primary_keys() {
        let connection = connect();
        connection
            .execute(&QueryRequest::new(
                "CREATE SCHEMA sales;
                 CREATE TABLE sales.customers (id INTEGER PRIMARY KEY);
                 CREATE TABLE sales.orders (id INTEGER PRIMARY KEY, note VARCHAR NOT NULL DEFAULT 'x',
                     customer INTEGER REFERENCES sales.customers(id), qty INTEGER CHECK (qty > 0),
                     code VARCHAR UNIQUE);
                 CREATE INDEX orders_qty ON sales.orders(qty);
                 CREATE VIEW sales.v AS SELECT 1",
            ))
            .unwrap();

        let catalog = connection.active_database().unwrap();
        assert!(
            connection
                .list_databases()
                .unwrap()
                .iter()
                .any(|database| database.name == catalog && database.is_current)
        );

        let schema = connection.schema_for_database(&catalog).unwrap();
        assert_eq!(schema.name, catalog);
        assert_eq!(schema.tables[1].name, "orders");
        assert_eq!(schema.tables[1].schema.as_deref(), Some("sales"));
        assert_eq!(schema.views[0].name, "v");

        let details = connection
            .table_details(&catalog, Some("sales"), "orders")
            .unwrap();
        let columns = details.columns.unwrap();
        assert!(columns[0].is_primary_key);
        assert!(!columns[1].nullable);
        assert_eq!(columns[1].default_value.as_deref(), Some("'x'"));

        let foreign_keys = details.foreign_keys.unwrap();
        assert_eq!(foreign_keys[0].columns, ["customer"]);
        assert_eq!(foreign_keys[0].referenced_table, "customers");
        assert_eq!(foreign_keys[0].referenced_columns, ["id"]);

        let constraints = details.constraints.unwrap();
        assert!(constraints.iter().any(|constraint| {
            constraint.kind == ConstraintKind::Check
                && constraint.check_clause.as_deref() == Some("(qty > 0)")
        }));
        assert!(constraints.iter().any(|constraint| {
            constraint.kind == ConstraintKind::Unique && constraint.columns == ["code"]
        }));

        let Some(IndexData::Relational(indexes)) = details.indexes else {
            panic!("relational indexes expected");
        };
        assert!(
            indexes
                .iter()
                .any(|index| index.is_primary && index.columns == ["id"])
        );
        assert!(indexes.iter().any(|index| {
            index.name == "orders_qty" && !index.is_unique && index.columns == ["qty"]
        }));

        let schema_indexes = connection.schema_indexes(&catalog, Some("sales")).unwrap();
        assert!(schema_indexes.iter().any(|index| {
            index.table_name == "customers" && index.is_primary && index.columns == ["id"]
        }));
        assert!(
            schema_indexes
                .iter()
                .any(|index| index.table_name == "orders" && index.name == "orders_qty")
        );
        let schema_foreign_keys = connection
            .schema_foreign_keys(&catalog, Some("sales"))
            .unwrap();
        assert_eq!(schema_foreign_keys.len(), 1);
        assert_eq!(schema_foreign_keys[0].table_name, "orders");
        assert_eq!(schema_foreign_keys[0].referenced_table, "customers");
    }

    #[test]
    fn sessions_keep_transactions_apart_from_the_root_connection() {
        let connection = connect();
        connection
            .execute(&QueryRequest::new("CREATE TABLE t AS SELECT 1 AS id"))
            .unwrap();

        let factory = connection.execution_session_factory().unwrap();
        let session = factory.open().unwrap();
        session
            .connection()
            .execute(&QueryRequest::new("BEGIN; INSERT INTO t VALUES (2)"))
            .unwrap();

        // The root connection is outside the session's transaction.
        let count = connection
            .execute(&read_only("SELECT count(*) FROM t"))
            .unwrap();
        assert_eq!(count.rows, vec![vec![Value::Int(1)]]);

        assert!(session.finish_operation().is_err());
        assert!(session.is_closed());
        let count = connection
            .execute(&QueryRequest::new("SELECT count(*) FROM t"))
            .unwrap();
        assert_eq!(count.rows, vec![vec![Value::Int(1)]]);

        let idle = factory.open().unwrap();
        idle.finish_operation().unwrap();
    }

    #[test]
    fn row_crud_returns_the_changed_row() {
        let connection = connect();
        connection
            .execute(&QueryRequest::new(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, name VARCHAR)",
            ))
            .unwrap();

        let inserted = connection
            .insert_row(&RowInsert::new(
                "items".into(),
                None,
                vec!["id".into(), "name".into()],
                vec![Value::Int(1), Value::Text("one".into())],
            ))
            .unwrap();
        assert_eq!(
            inserted.returning_row,
            Some(vec![Value::Int(1), Value::Text("one".into())])
        );

        // DuckDB refuses `UPDATE ... RETURNING` on a row a foreign key references.
        connection
            .execute(&QueryRequest::new(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, item INTEGER REFERENCES items(id));
                 INSERT INTO orders VALUES (10, 1)",
            ))
            .unwrap();
        let identity = RowIdentity::new(vec!["id".into()], vec![Value::Int(1)]);
        let updated = connection
            .update_row(&RowPatch::new(
                identity.clone(),
                "items".into(),
                None,
                vec![("name".into(), Value::Text("uno".into()))],
            ))
            .unwrap();
        assert_eq!(updated.affected_rows, 1);
        assert_eq!(
            updated.returning_row,
            Some(vec![Value::Int(1), Value::Text("uno".into())])
        );

        // Inside a transaction the caller opened, the update joins it.
        connection
            .execute(&QueryRequest::new("BEGIN TRANSACTION"))
            .unwrap();
        connection
            .update_row(&RowPatch::new(
                identity.clone(),
                "items".into(),
                None,
                vec![("name".into(), Value::Text("eins".into()))],
            ))
            .unwrap();
        connection.execute(&QueryRequest::new("ROLLBACK")).unwrap();
        let name = connection
            .execute(&QueryRequest::new("SELECT name FROM items WHERE id = 1"))
            .unwrap();
        assert_eq!(name.rows, vec![vec![Value::Text("uno".into())]]);

        connection
            .execute(&QueryRequest::new("DELETE FROM orders"))
            .unwrap();

        let deleted = connection
            .delete_row(&RowDelete::new(identity, "items".into(), None))
            .unwrap();
        assert_eq!(deleted.affected_rows, 1);
    }

    #[test]
    fn cancel_interrupts_a_running_query() {
        let connection: Arc<dyn Connection> = Arc::from(connect());
        let handle = connection.cancel_handle();
        let worker = std::thread::spawn({
            let connection = connection.clone();
            move || {
                connection.execute(&QueryRequest::new(
                    "SELECT count(*) FROM range(10000000000) a",
                ))
            }
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        handle.cancel().unwrap();
        assert!(matches!(worker.join().unwrap(), Err(DbError::Cancelled)));
    }

    #[test]
    fn cancel_on_the_root_interrupts_session_queries() {
        let connection: Arc<dyn Connection> = Arc::from(connect());
        let factory = connection.execution_session_factory().unwrap();
        let busy = factory.open().unwrap();
        let idle = factory.open().unwrap();

        let worker = std::thread::spawn({
            let busy = busy.connection();
            move || {
                busy.execute(&QueryRequest::new(
                    "SELECT count(*) FROM range(10000000000) a",
                ))
            }
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        connection.cancel_handle().cancel().unwrap();
        assert!(matches!(worker.join().unwrap(), Err(DbError::Cancelled)));

        let after = idle
            .connection()
            .execute(&QueryRequest::new("SELECT 1"))
            .unwrap();
        assert_eq!(after.rows, vec![vec![Value::Int(1)]]);
    }

    #[test]
    fn reals_and_intervals_read_as_written() {
        let connection = connect();
        let row = connection
            .execute(&QueryRequest::new(
                "SELECT 2.71::REAL, INTERVAL 90 SECONDS, INTERVAL '1 year 2 months 1 day', \
                 INTERVAL '1.5 seconds', INTERVAL 0 SECONDS",
            ))
            .unwrap()
            .rows
            .remove(0);
        assert_eq!(
            row,
            vec![
                Value::Float(2.71),
                Value::Text("00:01:30".into()),
                Value::Text("1 year 2 months 1 day".into()),
                Value::Text("00:00:01.500000".into()),
                Value::Text("00:00:00".into()),
            ]
        );
    }

    #[test]
    fn read_only_syntax_errors_name_the_error() {
        let Err(DbError::NotSupported(message)) = connect().execute(&read_only("SELEC 1")) else {
            panic!("a syntax error is refused");
        };
        assert!(message.contains("syntax error"), "{message}");
    }

    fn read_only(sql: &str) -> QueryRequest {
        let mut request = QueryRequest::new(sql);
        request.read_only = dbflux_core::ReadOnlyEnforcement::Required;
        request
    }

    #[test]
    fn read_only_requests_run_selects_and_refuse_writes() {
        let connection = connect();
        connection
            .execute(&QueryRequest::new("CREATE TABLE t AS SELECT 1 AS id"))
            .unwrap();

        let result = connection
            .execute(&read_only("SELECT * FROM t; FROM t"))
            .unwrap();
        assert_eq!(result.rows, vec![vec![Value::Int(1)]]);

        assert_eq!(
            connection
                .execute(&read_only("SELECT count(*) FROM range(3), duckdb_tables()"))
                .unwrap()
                .rows
                .len(),
            1
        );

        for sql in [
            "INSERT INTO t VALUES (2)",
            "SELECT 1; INSERT INTO t VALUES (2)",
            "COMMIT; INSERT INTO t VALUES (2)",
            "SELECT * FROM read_text('/etc/hostname')",
            "SELECT (SELECT count(*) FROM read_csv('data.csv'))",
            "FROM 'data.parquet'",
            "SELECT * FROM t, 's3://bucket/x'",
        ] {
            assert!(
                matches!(
                    connection.execute(&read_only(sql)),
                    Err(DbError::NotSupported(_))
                ),
                "{sql}"
            );
        }

        let count = connection
            .execute(&QueryRequest::new("SELECT count(*) FROM t"))
            .unwrap();
        assert_eq!(count.rows, vec![vec![Value::Int(1)]]);

        connection.execute(&QueryRequest::new("BEGIN")).unwrap();
        assert!(matches!(
            connection.execute(&read_only("SELECT 1")),
            Err(DbError::NotSupported(_))
        ));
    }

    #[test]
    fn schema_refresh_sees_catalogs_attached_from_the_editor() {
        let connection = connect();
        connection
            .execute(&QueryRequest::new("ATTACH ':memory:' AS other; USE other"))
            .unwrap();

        let structure = connection.schema().unwrap().structure;
        let dbflux_core::DataStructure::Relational(schema) = structure else {
            panic!("relational schema expected");
        };
        assert_eq!(schema.current_database.as_deref(), Some("other"));
        assert!(schema.schemas.is_empty() && schema.tables.is_empty());
        assert_eq!(
            connection.schema_snapshot_authority(),
            dbflux_core::SchemaSnapshotAuthority::EnumerationOnly
        );
        assert!(
            schema
                .databases
                .iter()
                .any(|database| database.name == "other" && database.is_current)
        );
    }
}
