#[allow(dead_code)]
mod syntax;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use dbflux_core::{
    DbError, FormattedError, PreparedTableAlter, TableAlterOperation, TableAlterOutcome,
    TableAlterPlanner, TableAlterPreview, TableAlterRequest, TableAlterRoute,
};
use rusqlite::{Connection as RusqliteConnection, OptionalExtension};

use crate::driver::SqliteConnectionState;

pub(crate) struct SqliteTableAlterPlanner {
    state: Arc<Mutex<SqliteConnectionState>>,
}

impl SqliteTableAlterPlanner {
    pub(crate) fn new(state: Arc<Mutex<SqliteConnectionState>>) -> Self {
        Self { state }
    }
}

impl TableAlterPlanner for SqliteTableAlterPlanner {
    fn prepare(&self, request: &TableAlterRequest) -> Result<Box<dyn PreparedTableAlter>, DbError> {
        if request.table.name.is_empty() {
            return Err(DbError::NotSupported(
                "SQLite table alteration planning requires a non-empty table name".to_string(),
            ));
        }
        if request
            .table
            .schema
            .as_deref()
            .is_some_and(|schema| !schema.eq_ignore_ascii_case("main"))
        {
            return Err(DbError::NotSupported(
                "SQLite table alteration planning supports the main schema only".to_string(),
            ));
        }
        if !sqlite_drop_column_supported() {
            return Err(DbError::NotSupported(
                "this SQLite engine requires a rebuild for DROP COLUMN, which is not installed"
                    .to_string(),
            ));
        }

        let columns = selected_native_drop_columns(&request.operations)?;
        let state = SqliteConnectionState::lock_checked(&self.state)?;
        if !state.is_autocommit() {
            return Err(DbError::NotSupported(
                "SQLite table alteration planning requires an autocommit connection".to_string(),
            ));
        }

        let schema_version = schema_version(&state)?;
        let source_sql = table_source_sql(&state, &request.table.name)?.ok_or_else(|| {
            DbError::NotSupported(format!(
                "SQLite table alteration planning requires catalog SQL for main.{}",
                request.table.name
            ))
        })?;
        for column in &columns {
            if !table_has_column(&state, &request.table.name, column)? {
                return Err(DbError::NotSupported(format!(
                    "SQLite native DROP cannot find column {} on main.{}",
                    column, request.table.name
                )));
            }
        }

        let statements = columns
            .iter()
            .map(|column| native_drop_statement(&request.table.name, column))
            .collect::<Vec<_>>();
        for statement in &statements {
            state.prepare(statement).map_err(|error| {
                native_error(
                    &request.table.name,
                    format!("SQLite rejected native DROP preparation: {error}"),
                )
            })?;
        }

        Ok(Box::new(NativeDropPlan {
            state: self.state.clone(),
            table: request.table.name.clone(),
            columns,
            schema_version,
            source_sql,
            preview: TableAlterPreview {
                route: TableAlterRoute::Native,
                statements,
                warnings: vec![
                    "Statements are illustrative; the driver executes the prepared plan."
                        .to_string(),
                    "SQLite validates retained dependencies and data when execution begins."
                        .to_string(),
                ],
                table_atomic: true,
                driver_managed: true,
            },
        }))
    }
}

struct NativeDropPlan {
    state: Arc<Mutex<SqliteConnectionState>>,
    table: String,
    columns: Vec<String>,
    schema_version: i64,
    source_sql: String,
    preview: TableAlterPreview,
}

impl PreparedTableAlter for NativeDropPlan {
    fn preview(&self) -> &TableAlterPreview {
        &self.preview
    }

    fn execute(self: Box<Self>) -> Result<TableAlterOutcome, DbError> {
        let mut state = SqliteConnectionState::lock_checked(&self.state)?;
        if !state.is_autocommit() {
            return Err(DbError::NotSupported(
                "SQLite table alteration execution requires an autocommit connection".to_string(),
            ));
        }
        #[cfg(test)]
        if let Some((_, hook)) = BEFORE_NATIVE_DROP_BEGIN
            .lock()
            .expect("native DROP test hook mutex should not be poisoned")
            .take_if(|(thread_id, _)| *thread_id == std::thread::current().id())
        {
            hook();
        }

        state.execute_batch("BEGIN IMMEDIATE").map_err(|error| {
            native_error(
                &self.table,
                format!("SQLite could not begin the native DROP transaction: {error}"),
            )
        })?;
        if let Err(error) = native_plan_is_fresh(&state, &self) {
            return rollback_native_failure(&mut state, &self.table, error);
        }
        for column in &self.columns {
            let statement = native_drop_statement(&self.table, column);
            let preparation_error = {
                let prepared = state.prepare(&statement);
                prepared.err().map(|error| {
                    native_error(
                        &self.table,
                        format!("SQLite rejected native DROP for column {column}: {error}"),
                    )
                })
            };
            if let Some(error) = preparation_error {
                return rollback_native_failure(&mut state, &self.table, error);
            }
            if let Err(error) = state.execute_batch(&statement) {
                return rollback_native_failure(
                    &mut state,
                    &self.table,
                    native_error(
                        &self.table,
                        format!("SQLite could not drop column {column}: {error}"),
                    ),
                );
            }
        }
        if let Err(error) = state.execute_batch("COMMIT") {
            return rollback_native_failure(
                &mut state,
                &self.table,
                native_error(
                    &self.table,
                    format!("SQLite could not commit native DROP: {error}"),
                ),
            );
        }
        if !state.is_autocommit() {
            state.mark_unusable("native DROP committed with an uncertain transaction state");
            return Err(native_error(
                &self.table,
                "SQLite native DROP committed with an uncertain transaction state; reconnect before retrying"
                    .to_string(),
            ));
        }

        Ok(TableAlterOutcome {
            statement_count: self.columns.len(),
            table_atomic: true,
        })
    }
}

fn native_plan_is_fresh(
    state: &SqliteConnectionState,
    plan: &NativeDropPlan,
) -> Result<(), DbError> {
    if schema_version(state)? != plan.schema_version
        || table_source_sql(state, &plan.table)? != Some(plan.source_sql.clone())
    {
        return Err(DbError::NotSupported(format!(
            "SQLite table alteration plan for main.{} is stale; refresh the preview",
            plan.table
        )));
    }
    for column in &plan.columns {
        if !table_has_column(state, &plan.table, column)? {
            return Err(DbError::NotSupported(format!(
                "SQLite table alteration plan for main.{} is stale: column {} changed",
                plan.table, column
            )));
        }
    }
    Ok(())
}

fn selected_native_drop_columns(
    operations: &[TableAlterOperation],
) -> Result<Vec<String>, DbError> {
    if operations.is_empty() {
        return Err(DbError::NotSupported(
            "SQLite native DROP planning requires at least one selected column".to_string(),
        ));
    }
    let mut seen = HashSet::new();
    let mut columns = Vec::with_capacity(operations.len());
    for operation in operations {
        let TableAlterOperation::DropColumn { name } = operation else {
            return Err(DbError::NotSupported(
                "SQLite rebuild planning for non-DROP table alterations is not installed"
                    .to_string(),
            ));
        };
        if name.is_empty() || !seen.insert(name.to_ascii_lowercase()) {
            return Err(DbError::NotSupported(
                "SQLite native DROP planning requires unique non-empty column names".to_string(),
            ));
        }
        columns.push(name.clone());
    }
    Ok(columns)
}

fn sqlite_drop_column_supported() -> bool {
    rusqlite::version_number() >= 3_035_000
}

fn native_drop_statement(table: &str, column: &str) -> String {
    format!(
        "ALTER TABLE main.{} DROP COLUMN {}",
        quote_identifier(table),
        quote_identifier(column)
    )
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn schema_version(connection: &RusqliteConnection) -> Result<i64, DbError> {
    connection
        .query_row("PRAGMA main.schema_version", [], |row| row.get(0))
        .map_err(|error| {
            DbError::query_failed(format!("could not read SQLite schema version: {error}"))
        })
}

fn table_source_sql(
    connection: &RusqliteConnection,
    table: &str,
) -> Result<Option<String>, DbError> {
    connection
        .query_row(
            "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| {
            DbError::query_failed(format!("could not read SQLite catalog SQL: {error}"))
        })
}

fn table_has_column(
    connection: &RusqliteConnection,
    table: &str,
    selected_column: &str,
) -> Result<bool, DbError> {
    let mut statement = connection
        .prepare(&format!(
            "PRAGMA main.table_info({})",
            quote_identifier(table)
        ))
        .map_err(|error| {
            DbError::query_failed(format!("could not inspect SQLite table columns: {error}"))
        })?;
    let mut rows = statement.query([]).map_err(|error| {
        DbError::query_failed(format!("could not read SQLite table columns: {error}"))
    })?;
    while let Some(row) = rows.next().map_err(|error| {
        DbError::query_failed(format!("could not read SQLite table columns: {error}"))
    })? {
        let name: String = row.get(1).map_err(|error| {
            DbError::query_failed(format!("could not decode SQLite column name: {error}"))
        })?;
        if name.eq_ignore_ascii_case(selected_column) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn rollback_native_failure(
    state: &mut SqliteConnectionState,
    table: &str,
    primary: DbError,
) -> Result<TableAlterOutcome, DbError> {
    let rollback = state.execute_batch("ROLLBACK");
    if rollback.is_ok() && state.is_autocommit() {
        return Err(native_error(
            table,
            format!("{primary}; the native DROP transaction was rolled back"),
        ));
    }

    let detail = match rollback {
        Ok(()) => "SQLite rollback did not restore autocommit".to_string(),
        Err(error) => format!("SQLite rollback failed: {error}"),
    };
    state.mark_unusable(detail.clone());
    Err(DbError::QueryFailed(
        FormattedError::new("SQLite native DROP failed with uncertain cleanup")
            .with_detail(format!("{primary}. {detail}"))
            .with_hint("Reconnect and inspect the table before retrying")
            .with_retriable(false),
    ))
}

fn native_error(table: &str, detail: String) -> DbError {
    DbError::QueryFailed(
        FormattedError::new("SQLite native table alteration failed")
            .with_detail(detail)
            .with_location(dbflux_core::ErrorLocation {
                schema: Some("main".to_string()),
                table: Some(table.to_string()),
                column: None,
                constraint: None,
            })
            .with_retriable(false),
    )
}

#[cfg(test)]
static BEFORE_NATIVE_DROP_BEGIN: Mutex<Option<(std::thread::ThreadId, Box<dyn FnOnce() + Send>)>> =
    Mutex::new(None);

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use rusqlite::Connection as RusqliteConnection;

    use super::BEFORE_NATIVE_DROP_BEGIN;
    use crate::driver::{SqliteConnection, SqliteConnectionState};
    use dbflux_core::{Connection, TableAlterRequest, TableRef};

    #[test]
    fn quarantined_state_rejects_checked_access_but_retains_interrupt_access() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        let interrupt_handle = state
            .lock()
            .expect("state mutex should not be poisoned")
            .interrupt_handle();

        state
            .lock()
            .expect("state mutex should not be poisoned")
            .mark_unusable("cleanup outcome is uncertain");

        assert!(SqliteConnectionState::lock_checked(&state).is_err());
        interrupt_handle.interrupt();
    }

    #[test]
    fn sqlite_connection_opts_into_a_fail_closed_planner() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        let connection = SqliteConnection::for_test(state);
        let planner = connection
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam");
        let request = TableAlterRequest {
            table: TableRef::new("people"),
            operations: Vec::new(),
            expected_before: Vec::new(),
        };

        assert!(planner.prepare(&request).is_err());
    }

    #[test]
    fn planner_prepares_and_consumes_a_connection_bound_native_drop_plan() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch(
                "CREATE TABLE people (id INTEGER PRIMARY KEY, obsolete TEXT, retained TEXT)",
            )
            .expect("test table should be created");
        let connection = SqliteConnection::for_test(state.clone());
        let planner = connection
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam");
        let request = TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![dbflux_core::TableAlterOperation::DropColumn {
                name: "obsolete".to_string(),
            }],
            expected_before: Vec::new(),
        };

        let plan = planner
            .prepare(&request)
            .expect("a supported native DROP should prepare without mutation");
        assert_eq!(plan.preview().route, dbflux_core::TableAlterRoute::Native);
        assert_eq!(
            plan.preview().statements,
            ["ALTER TABLE main.\"people\" DROP COLUMN \"obsolete\""]
        );
        assert_eq!(
            state
                .lock()
                .expect("state mutex should not be poisoned")
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('people') WHERE name = 'obsolete'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .expect("column count query should succeed"),
            1,
            "prepare must remain read-only"
        );

        let outcome = plan.execute().expect("prepared plan should execute once");
        assert_eq!(outcome.statement_count, 1);
        assert_eq!(
            state
                .lock()
                .expect("state mutex should not be poisoned")
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('people') WHERE name = 'obsolete'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .expect("column count query should succeed"),
            0,
            "consuming the plan must apply the bound native DROP"
        );
    }

    #[test]
    fn native_plan_targets_main_when_temp_table_shadows_it() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch(
                "CREATE TABLE main.people (id INTEGER PRIMARY KEY, obsolete TEXT, retained TEXT); \
                 INSERT INTO main.people VALUES (1, 'main obsolete', 'main retained'); \
                 CREATE TEMP TABLE people (id INTEGER PRIMARY KEY, obsolete TEXT, retained TEXT); \
                 INSERT INTO people VALUES (2, 'temp obsolete', 'temp retained')",
            )
            .expect("colliding tables should be created");
        let connection = SqliteConnection::for_test(state.clone());
        let request = TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![dbflux_core::TableAlterOperation::DropColumn {
                name: "obsolete".to_string(),
            }],
            expected_before: Vec::new(),
        };

        connection
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam")
            .prepare(&request)
            .expect("the main table should prepare despite a temp shadow")
            .execute()
            .expect("the requested main table should be altered");

        let state = state.lock().expect("state mutex should not be poisoned");
        assert_eq!(
            state
                .query_row(
                    "SELECT instr(sql, 'obsolete') FROM main.sqlite_master WHERE type = 'table' AND name = 'people'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("main catalog query should succeed"),
            0,
            "the requested main table must lose the selected column"
        );
        assert_eq!(
            state
                .query_row("SELECT obsolete FROM temp.people WHERE id = 2", [], |row| {
                    row.get::<_, String>(0)
                })
                .expect("temp table data query should succeed"),
            "temp obsolete",
            "the shadowing temp table and its retained data must remain untouched"
        );
    }

    #[test]
    fn native_plan_rejects_schema_drift_before_it_starts_a_transaction() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch("CREATE TABLE people (id INTEGER PRIMARY KEY, obsolete TEXT)")
            .expect("test table should be created");
        let connection = SqliteConnection::for_test(state.clone());
        let request = TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![dbflux_core::TableAlterOperation::DropColumn {
                name: "obsolete".to_string(),
            }],
            expected_before: Vec::new(),
        };
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam")
            .prepare(&request)
            .expect("native DROP should prepare against the original catalog");

        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch("ALTER TABLE people ADD COLUMN concurrent_change TEXT")
            .expect("external catalog change should succeed");

        assert!(
            plan.execute().is_err(),
            "stale plans must fail before mutation"
        );
        assert_eq!(
            state
                .lock()
                .expect("state mutex should not be poisoned")
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('people') WHERE name = 'obsolete'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .expect("column count query should succeed"),
            1,
            "the stale plan must not drop the selected column"
        );
    }

    #[test]
    fn native_plan_rechecks_schema_after_acquiring_the_write_lock() {
        let directory = tempfile::tempdir().expect("test directory should be created");
        let database_path = directory.path().join("schema-race.db");
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open(&database_path).expect("file-backed SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch("CREATE TABLE main.people (id INTEGER PRIMARY KEY, obsolete TEXT)")
            .expect("test table should be created");
        let connection = SqliteConnection::for_test(state.clone());
        let request = TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![dbflux_core::TableAlterOperation::DropColumn {
                name: "obsolete".to_string(),
            }],
            expected_before: Vec::new(),
        };
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam")
            .prepare(&request)
            .expect("native DROP should prepare against the original catalog");

        let competing_path = database_path.clone();
        *BEFORE_NATIVE_DROP_BEGIN
            .lock()
            .expect("native DROP test hook mutex should not be poisoned") = Some((
            std::thread::current().id(),
            Box::new(move || {
                RusqliteConnection::open(competing_path)
                    .expect("second SQLite connection should open")
                    .execute_batch(
                        "DROP TABLE main.people; \
                         CREATE TABLE main.people (id INTEGER PRIMARY KEY, obsolete TEXT, replacement TEXT)",
                    )
                    .expect("second connection should replace the schema before the write lock");
            }),
        ));

        let error = plan.execute().expect_err("the stale plan must be rejected");
        assert!(
            error.to_string().contains("stale"),
            "the execution-time recheck must report stale catalog state: {error}"
        );
        assert_eq!(
            state
                .lock()
                .expect("state mutex should not be poisoned")
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('people') WHERE name = 'obsolete'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("main table info query should succeed"),
            1,
            "schema replacement before BEGIN IMMEDIATE must not drop the original column"
        );
    }

    #[test]
    fn pooled_aliases_share_quarantine_while_remaining_interruptible() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        let first_alias = SqliteConnection::for_test(state.clone());
        let second_alias = SqliteConnection::for_test(state.clone());

        first_alias
            .ping()
            .expect("usable shared state should allow ordinary work");
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .mark_unusable("rollback certainty is unavailable");

        assert!(first_alias.ping().is_err());
        assert!(second_alias.ping().is_err());
        first_alias
            .cancel_active()
            .expect("interrupt remains available after quarantine");
        second_alias
            .cancel_handle()
            .cancel()
            .expect("alias interrupt remains available");
    }
}
