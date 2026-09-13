#[allow(dead_code)]
mod syntax;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use dbflux_core::{
    DbError, FormattedError, PreparedTableAlter, TableAlterExpectedColumn, TableAlterOperation,
    TableAlterOutcome, TableAlterPlanner, TableAlterPreview, TableAlterRequest, TableAlterRoute,
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

        let capture = capture_native_plan(
            &state,
            &request.table.name,
            &columns,
            &request.expected_before,
        )?;

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
            capture,
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
    capture: NativePlanCapture,
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
    let capture = capture_native_plan(
        state,
        &plan.table,
        &plan.columns,
        &plan.capture.expected_before,
    )?;
    if capture != plan.capture {
        let changed = if capture.source_sql != plan.capture.source_sql {
            "source catalog SQL changed"
        } else if capture.selected_columns != plan.capture.selected_columns {
            "selected columns changed"
        } else if capture.settings != plan.capture.settings {
            "connection settings changed"
        } else if capture.main_catalog != plan.capture.main_catalog {
            "main catalog changed"
        } else {
            "visible schema identity changed"
        };
        return Err(DbError::NotSupported(format!(
            "SQLite table alteration plan for main.{} is stale: {changed}; refresh the preview",
            plan.table
        )));
    }
    reject_known_native_dependencies(state, &plan.table, &plan.columns)
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

#[derive(Debug, PartialEq, Eq)]
struct NativePlanCapture {
    source_sql: String,
    selected_columns: Vec<String>,
    expected_before: Vec<TableAlterExpectedColumn>,
    settings: NativeConnectionSettings,
    main_catalog: Vec<CatalogEntry>,
    schema_versions: Vec<SchemaVersion>,
}

#[derive(Debug, PartialEq, Eq)]
struct NativeConnectionSettings {
    foreign_keys: i64,
    writable_schema: i64,
    legacy_alter_table: i64,
    ignore_check_constraints: i64,
    defer_foreign_keys: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct CatalogEntry {
    object_type: String,
    name: String,
    table_name: String,
    sql: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
struct SchemaVersion {
    schema: String,
    version: i64,
}

fn capture_native_plan(
    connection: &RusqliteConnection,
    table: &str,
    selected_columns: &[String],
    expected_before: &[TableAlterExpectedColumn],
) -> Result<NativePlanCapture, DbError> {
    let before = capture_native_plan_once(connection, table, selected_columns, expected_before)?;
    #[cfg(test)]
    if let Some((_, hook)) = BEFORE_NATIVE_CAPTURE_SECOND_READ
        .lock()
        .expect("native capture test hook mutex should not be poisoned")
        .take_if(|(thread_id, _)| *thread_id == std::thread::current().id())
    {
        hook();
    }
    let after = capture_native_plan_once(connection, table, selected_columns, expected_before)?;
    if before != after {
        return Err(DbError::NotSupported(format!(
            "SQLite table alteration source changed during planning for main.{table}; refresh the preview"
        )));
    }
    Ok(after)
}

fn capture_native_plan_once(
    connection: &RusqliteConnection,
    table: &str,
    selected_columns: &[String],
    expected_before: &[TableAlterExpectedColumn],
) -> Result<NativePlanCapture, DbError> {
    let source_sql = table_source_sql(connection, table)?.ok_or_else(|| {
        DbError::NotSupported(format!(
            "SQLite table alteration planning requires catalog SQL for main.{table}"
        ))
    })?;
    let columns = table_columns(connection, table)?;
    for column in selected_columns {
        if !columns
            .iter()
            .any(|candidate| candidate.name.eq_ignore_ascii_case(column))
        {
            return Err(DbError::NotSupported(format!(
                "SQLite native DROP cannot find column {column} on main.{table}"
            )));
        }
    }
    validate_expected_source(table, selected_columns, expected_before, &columns)?;

    let capture = NativePlanCapture {
        source_sql,
        selected_columns: selected_columns.to_vec(),
        expected_before: expected_before.to_vec(),
        settings: capture_native_connection_settings(connection)?,
        main_catalog: capture_main_catalog(connection)?,
        schema_versions: capture_schema_versions(connection)?,
    };
    reject_known_native_dependencies(connection, table, selected_columns)?;
    Ok(capture)
}

fn capture_native_connection_settings(
    connection: &RusqliteConnection,
) -> Result<NativeConnectionSettings, DbError> {
    let settings = NativeConnectionSettings {
        foreign_keys: pragma_flag(connection, "foreign_keys")?,
        writable_schema: pragma_flag(connection, "writable_schema")?,
        legacy_alter_table: pragma_flag(connection, "legacy_alter_table")?,
        ignore_check_constraints: pragma_flag(connection, "ignore_check_constraints")?,
        defer_foreign_keys: pragma_flag(connection, "defer_foreign_keys")?,
    };
    for (name, enabled) in [
        ("writable_schema", settings.writable_schema),
        ("legacy_alter_table", settings.legacy_alter_table),
        (
            "ignore_check_constraints",
            settings.ignore_check_constraints,
        ),
        ("defer_foreign_keys", settings.defer_foreign_keys),
    ] {
        if enabled != 0 {
            return Err(DbError::NotSupported(format!(
                "SQLite native DROP planning rejects unsafe connection setting {name}"
            )));
        }
    }
    Ok(settings)
}

fn pragma_flag(connection: &RusqliteConnection, pragma: &str) -> Result<i64, DbError> {
    connection
        .query_row(&format!("PRAGMA {pragma}"), [], |row| row.get(0))
        .map_err(|error| {
            DbError::query_failed(format!("could not read SQLite PRAGMA {pragma}: {error}"))
        })
}

fn capture_main_catalog(connection: &RusqliteConnection) -> Result<Vec<CatalogEntry>, DbError> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, sql FROM main.sqlite_master \
             WHERE substr(lower(name), 1, 7) <> 'sqlite_' ORDER BY type, name, tbl_name",
        )
        .map_err(|error| {
            DbError::query_failed(format!("could not inspect SQLite catalog: {error}"))
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok(CatalogEntry {
                object_type: row.get(0)?,
                name: row.get(1)?,
                table_name: row.get(2)?,
                sql: row.get(3)?,
            })
        })
        .map_err(|error| {
            DbError::query_failed(format!("could not read SQLite catalog: {error}"))
        })?;
    rows.map(|entry| {
        entry.map_err(|error| {
            DbError::query_failed(format!("could not decode SQLite catalog: {error}"))
        })
    })
    .collect()
}

fn capture_schema_versions(connection: &RusqliteConnection) -> Result<Vec<SchemaVersion>, DbError> {
    let mut statement = connection
        .prepare("PRAGMA database_list")
        .map_err(|error| {
            DbError::query_failed(format!("could not list SQLite schemas: {error}"))
        })?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| {
            DbError::query_failed(format!("could not read SQLite schemas: {error}"))
        })?;
    let schemas = rows
        .map(|schema| {
            schema.map_err(|error| {
                DbError::query_failed(format!("could not decode SQLite schema: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut versions = schemas
        .into_iter()
        .filter(|schema| !schema.eq_ignore_ascii_case("temp"))
        .map(|schema| {
            connection
                .query_row(
                    &format!("PRAGMA {}.schema_version", quote_identifier(&schema)),
                    [],
                    |row| row.get(0),
                )
                .map(|version| SchemaVersion { schema, version })
                .map_err(|error| {
                    DbError::query_failed(format!("could not read SQLite schema version: {error}"))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    versions.sort_by(|left, right| left.schema.cmp(&right.schema));
    Ok(versions)
}

fn reject_known_native_dependencies(
    connection: &RusqliteConnection,
    table: &str,
    selected_columns: &[String],
) -> Result<(), DbError> {
    for column in selected_columns {
        if column_is_primary_key(connection, table, column)? {
            return Err(DbError::NotSupported(format!(
                "SQLite native DROP found known dependency: primary key main.{table}.{column}"
            )));
        }
        reject_selected_index_dependency(connection, table, column)?;
        reject_selected_foreign_key_dependency(connection, table, column)?;
    }
    Ok(())
}

fn reject_selected_index_dependency(
    connection: &RusqliteConnection,
    table: &str,
    column: &str,
) -> Result<(), DbError> {
    let mut statement = connection
        .prepare(&format!(
            "PRAGMA main.index_list({})",
            quote_identifier(table)
        ))
        .map_err(|error| {
            DbError::query_failed(format!("could not inspect SQLite indexes: {error}"))
        })?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| {
            DbError::query_failed(format!("could not read SQLite indexes: {error}"))
        })?;
    for index in rows {
        let index = index.map_err(|error| {
            DbError::query_failed(format!("could not decode SQLite index name: {error}"))
        })?;
        let mut columns = connection
            .prepare(&format!(
                "PRAGMA main.index_xinfo({})",
                quote_identifier(&index)
            ))
            .map_err(|error| {
                DbError::query_failed(format!("could not inspect SQLite index: {error}"))
            })?;
        let entries = columns
            .query_map([], |row| {
                Ok((row.get::<_, Option<String>>(2)?, row.get::<_, i64>(5)?))
            })
            .map_err(|error| {
                DbError::query_failed(format!("could not read SQLite index: {error}"))
            })?;
        for entry in entries {
            let (indexed_column, is_key) = entry.map_err(|error| {
                DbError::query_failed(format!("could not decode SQLite index: {error}"))
            })?;
            if is_key != 0
                && indexed_column
                    .as_deref()
                    .is_some_and(|indexed_column| indexed_column.eq_ignore_ascii_case(column))
            {
                return Err(DbError::NotSupported(format!(
                    "SQLite native DROP found known dependency: index main.{index} uses selected column {column}"
                )));
            }
        }
    }
    Ok(())
}

fn reject_selected_foreign_key_dependency(
    connection: &RusqliteConnection,
    table: &str,
    column: &str,
) -> Result<(), DbError> {
    for (referenced_table, from, to) in foreign_keys_for_table(connection, table)? {
        if from.eq_ignore_ascii_case(column) {
            return Err(DbError::NotSupported(format!(
                "SQLite native DROP found known dependency: foreign key from main.{table}.{from}"
            )));
        }
        if referenced_table.eq_ignore_ascii_case(table)
            && to
                .as_deref()
                .is_some_and(|to| to.eq_ignore_ascii_case(column))
        {
            return Err(DbError::NotSupported(format!(
                "SQLite native DROP found known dependency: self foreign key references main.{table}.{column}"
            )));
        }
    }

    for child_table in main_table_names(connection)? {
        for (referenced_table, _from, to) in foreign_keys_for_table(connection, &child_table)? {
            if referenced_table.eq_ignore_ascii_case(table)
                && (to
                    .as_deref()
                    .is_some_and(|to| to.eq_ignore_ascii_case(column))
                    || (to.is_none() && column_is_primary_key(connection, table, column)?))
            {
                return Err(DbError::NotSupported(format!(
                    "SQLite native DROP found known dependency: foreign key from main.{child_table} references main.{table}.{column}"
                )));
            }
        }
    }
    Ok(())
}

fn foreign_keys_for_table(
    connection: &RusqliteConnection,
    table: &str,
) -> Result<Vec<(String, String, Option<String>)>, DbError> {
    let mut statement = connection
        .prepare(&format!(
            "PRAGMA main.foreign_key_list({})",
            quote_identifier(table)
        ))
        .map_err(|error| {
            DbError::query_failed(format!("could not inspect SQLite foreign keys: {error}"))
        })?;
    let rows = statement
        .query_map([], |row| Ok((row.get(2)?, row.get(3)?, row.get(4)?)))
        .map_err(|error| {
            DbError::query_failed(format!("could not read SQLite foreign keys: {error}"))
        })?;
    rows.map(|foreign_key| {
        foreign_key.map_err(|error| {
            DbError::query_failed(format!("could not decode SQLite foreign key: {error}"))
        })
    })
    .collect()
}

fn main_table_names(connection: &RusqliteConnection) -> Result<Vec<String>, DbError> {
    let mut statement = connection
        .prepare(
            "SELECT name FROM main.sqlite_master WHERE type = 'table' AND substr(lower(name), 1, 7) <> 'sqlite_'",
        )
        .map_err(|error| {
            DbError::query_failed(format!("could not inspect SQLite tables: {error}"))
        })?;
    let rows = statement
        .query_map([], |row| row.get(0))
        .map_err(|error| DbError::query_failed(format!("could not read SQLite tables: {error}")))?;
    rows.map(|table| {
        table.map_err(|error| {
            DbError::query_failed(format!("could not decode SQLite table: {error}"))
        })
    })
    .collect()
}

fn column_is_primary_key(
    connection: &RusqliteConnection,
    table: &str,
    column: &str,
) -> Result<bool, DbError> {
    let mut statement = connection
        .prepare(&format!(
            "PRAGMA main.table_info({})",
            quote_identifier(table)
        ))
        .map_err(|error| {
            DbError::query_failed(format!("could not inspect SQLite table columns: {error}"))
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(5)?))
        })
        .map_err(|error| {
            DbError::query_failed(format!("could not read SQLite table columns: {error}"))
        })?;
    for row in rows {
        let (name, primary_key_order) = row.map_err(|error| {
            DbError::query_failed(format!("could not decode SQLite table columns: {error}"))
        })?;
        if primary_key_order != 0 && name.eq_ignore_ascii_case(column) {
            return Ok(true);
        }
    }
    Ok(false)
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

fn table_source_sql(
    connection: &RusqliteConnection,
    table: &str,
) -> Result<Option<String>, DbError> {
    connection
        .query_row(
            "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map(|source| source.flatten())
        .map_err(|error| {
            DbError::query_failed(format!("could not read SQLite catalog SQL: {error}"))
        })
}

#[derive(Debug, Clone)]
struct TableColumnCapture {
    name: String,
    type_name: String,
    nullable: bool,
    default: Option<String>,
}

fn validate_expected_source(
    table: &str,
    selected_columns: &[String],
    expected_before: &[TableAlterExpectedColumn],
    columns: &[TableColumnCapture],
) -> Result<(), DbError> {
    let mut seen = HashSet::new();
    for expected in expected_before {
        if !seen.insert(expected.name.to_ascii_lowercase())
            || !selected_columns
                .iter()
                .any(|selected| selected.eq_ignore_ascii_case(&expected.name))
        {
            return Err(DbError::NotSupported(format!(
                "SQLite expected source column {} is not a unique selected column on main.{table}",
                expected.name
            )));
        }
        let actual = columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&expected.name))
            .ok_or_else(|| {
                DbError::NotSupported(format!(
                    "SQLite expected source column {} is missing from main.{table}",
                    expected.name
                ))
            })?;
        if expected
            .type_name
            .as_deref()
            .is_some_and(|expected| !type_expectation_matches(expected, &actual.type_name))
            || expected
                .nullable
                .is_some_and(|expected| expected != actual.nullable)
            || expected.default.as_ref().is_some_and(|expected| {
                !default_expectation_matches(expected, actual.default.as_deref())
            })
        {
            return Err(DbError::NotSupported(format!(
                "SQLite expected source values for main.{table}.{} no longer match; refresh the preview",
                expected.name
            )));
        }
    }
    Ok(())
}

fn type_expectation_matches(expected: &str, actual: &str) -> bool {
    expected == actual
        || normalize_type_name(expected)
            .zip(normalize_type_name(actual))
            .is_some_and(|(expected, actual)| expected == actual)
}

fn normalize_type_name(type_name: &str) -> Option<String> {
    let mut normalized = String::new();
    let mut pending_space = false;
    for character in type_name.trim().chars() {
        if character.is_ascii_whitespace() {
            pending_space = true;
        } else if character.is_ascii_alphanumeric() || matches!(character, '(' | ')' | ',') {
            if pending_space && !normalized.is_empty() {
                normalized.push(' ');
            }
            pending_space = false;
            normalized.push(character.to_ascii_uppercase());
        } else {
            return None;
        }
    }
    (!normalized.is_empty()).then_some(normalized)
}

fn default_expectation_matches(expected: &Option<String>, actual: Option<&str>) -> bool {
    match (expected, actual) {
        (None, None) => true,
        (Some(expected), Some(actual)) => {
            normalize_keyword_default(expected)
                .zip(normalize_keyword_default(actual))
                .is_some_and(|(expected, actual)| expected == actual)
                || expected == actual
        }
        _ => false,
    }
}

fn normalize_keyword_default(default: &str) -> Option<String> {
    let mut candidate = default.trim();
    while let Some(inner) = outer_parenthesized(candidate) {
        candidate = inner.trim();
    }
    matches!(
        candidate.to_ascii_uppercase().as_str(),
        "NULL" | "TRUE" | "FALSE" | "CURRENT_TIME" | "CURRENT_DATE" | "CURRENT_TIMESTAMP"
    )
    .then(|| candidate.to_ascii_uppercase())
}

fn outer_parenthesized(value: &str) -> Option<&str> {
    let value = value.trim();
    if !value.starts_with('(') || !value.ends_with(')') {
        return None;
    }
    let mut depth = 0;
    for (index, character) in value.char_indices() {
        match character {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 && index + character.len_utf8() != value.len() {
                    return None;
                }
                if depth < 0 {
                    return None;
                }
            }
            _ => {}
        }
    }
    (depth == 0).then(|| &value[1..value.len() - 1])
}

fn table_columns(
    connection: &RusqliteConnection,
    table: &str,
) -> Result<Vec<TableColumnCapture>, DbError> {
    let mut statement = connection
        .prepare(&format!(
            "PRAGMA main.table_info({})",
            quote_identifier(table)
        ))
        .map_err(|error| {
            DbError::query_failed(format!("could not inspect SQLite table columns: {error}"))
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok(TableColumnCapture {
                name: row.get(1)?,
                type_name: row.get(2)?,
                nullable: row.get::<_, i64>(3)? == 0,
                default: row.get(4)?,
            })
        })
        .map_err(|error| {
            DbError::query_failed(format!("could not read SQLite table columns: {error}"))
        })?;
    rows.map(|column| {
        column.map_err(|error| {
            DbError::query_failed(format!("could not decode SQLite table columns: {error}"))
        })
    })
    .collect()
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
static BEFORE_NATIVE_CAPTURE_SECOND_READ: Mutex<
    Option<(std::thread::ThreadId, Box<dyn FnOnce() + Send>)>,
> = Mutex::new(None);

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use rusqlite::Connection as RusqliteConnection;

    use super::{BEFORE_NATIVE_CAPTURE_SECOND_READ, BEFORE_NATIVE_DROP_BEGIN};
    use crate::driver::{SqliteConnection, SqliteConnectionState};
    use dbflux_core::{
        Connection, TableAlterExpectedColumn, TableAlterOperation, TableAlterRequest, TableRef,
    };

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
    fn native_drop_ignores_unrelated_main_and_temp_views_and_triggers() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch(
                "CREATE TABLE main.people (id INTEGER PRIMARY KEY, obsolete TEXT, retained TEXT); \
                 CREATE TABLE main.audit (id INTEGER PRIMARY KEY, message TEXT); \
                 CREATE VIEW main.audit_view AS SELECT message FROM audit; \
                 CREATE TEMP VIEW temp_audit_view AS SELECT message FROM main.audit; \
                 CREATE TRIGGER main.audit_trigger AFTER INSERT ON main.audit \
                 BEGIN INSERT INTO audit (message) VALUES ('main trigger'); END; \
                 CREATE TEMP TRIGGER temp_audit_trigger AFTER INSERT ON main.audit \
                 BEGIN INSERT INTO audit (message) VALUES ('temp trigger'); END",
            )
            .expect("unrelated views and triggers should be created");
        let connection = SqliteConnection::for_test(state.clone());
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam")
            .prepare(&TableAlterRequest {
                table: TableRef::new("people"),
                operations: vec![dbflux_core::TableAlterOperation::DropColumn {
                    name: "obsolete".to_string(),
                }],
                expected_before: Vec::new(),
            })
            .expect("unrelated main and temp objects must not reject native planning");

        assert_eq!(plan.preview().route, dbflux_core::TableAlterRoute::Native);
        plan.execute()
            .expect("native DROP must retain unrelated objects owned by SQLite");
        let state = state.lock().expect("state mutex should not be poisoned");
        for object in ["audit_view", "audit_trigger"] {
            assert_eq!(
                state
                    .query_row(
                        "SELECT COUNT(*) FROM main.sqlite_master WHERE name = ?1",
                        [object],
                        |row| row.get::<_, i64>(0),
                    )
                    .expect("main catalog query should succeed"),
                1,
                "unrelated main object {object} must be retained"
            );
        }
        assert_eq!(
            state
                .query_row(
                    "SELECT COUNT(*) FROM temp.sqlite_temp_master WHERE name IN ('temp_audit_view', 'temp_audit_trigger')",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("temp catalog query should succeed"),
            2,
            "unrelated temp objects must be retained"
        );
    }

    #[test]
    fn native_prepare_validates_expected_source_values_without_mutation() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch(
                "CREATE TABLE main.people (
                    id INTEGER PRIMARY KEY,
                    obsolete TEXT NOT NULL DEFAULT ((NULL)),
                    retained TEXT DEFAULT 'NULL'
                );
                INSERT INTO main.people (obsolete) VALUES ('value')",
            )
            .expect("test table should be created");
        let connection = SqliteConnection::for_test(state.clone());
        let matching = TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![TableAlterOperation::DropColumn {
                name: "obsolete".to_string(),
            }],
            expected_before: vec![TableAlterExpectedColumn {
                name: "obsolete".to_string(),
                type_name: Some("text".to_string()),
                nullable: Some(false),
                default: Some(Some("NULL".to_string())),
            }],
        };
        connection
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam")
            .prepare(&matching)
            .expect("equivalent selected source expectations must prepare");

        let mismatching = TableAlterRequest {
            expected_before: vec![TableAlterExpectedColumn {
                name: "obsolete".to_string(),
                type_name: Some("INTEGER".to_string()),
                nullable: Some(true),
                default: Some(Some("'NULL'".to_string())),
            }],
            ..matching
        };
        let error = connection
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam")
            .prepare(&mismatching)
            .err()
            .expect("mismatched expected source values must reject planning");
        assert!(
            error.to_string().contains("expected source"),
            "the mismatch must be actionable: {error}"
        );
        assert_eq!(
            state
                .lock()
                .expect("state mutex should not be poisoned")
                .query_row("SELECT obsolete FROM main.people", [], |row| row
                    .get::<_, String>(0))
                .expect("source row should remain readable"),
            "value",
            "expected-source rejection must remain read-only"
        );
    }

    #[test]
    fn native_prepare_and_execute_accept_exact_valid_custom_and_empty_type_metadata() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch(
                "CREATE TABLE main.custom_type (
                    id INTEGER PRIMARY KEY,
                    obsolete my_type,
                    retained TEXT
                );
                INSERT INTO main.custom_type VALUES (1, 'remove', 'custom retained');
                CREATE TABLE main.empty_type (
                    id INTEGER PRIMARY KEY,
                    obsolete,
                    retained TEXT
                );
                INSERT INTO main.empty_type VALUES (2, 'remove', 'empty retained')",
            )
            .expect("native type fixtures should be created");
        let connection = SqliteConnection::for_test(state.clone());

        for (table, retained_value) in [
            ("custom_type", "custom retained"),
            ("empty_type", "empty retained"),
        ] {
            let actual_type = super::table_columns(
                &state.lock().expect("state mutex should not be poisoned"),
                table,
            )
            .expect("generic column metadata should be readable")
            .into_iter()
            .find(|column| column.name == "obsolete")
            .expect("selected column metadata should exist")
            .type_name;
            let plan = connection
                .table_alter_planner()
                .expect("SQLite must opt into the table-alter planner seam")
                .prepare(&TableAlterRequest {
                    table: TableRef::new(table),
                    operations: vec![TableAlterOperation::DropColumn {
                        name: "obsolete".to_string(),
                    }],
                    expected_before: vec![TableAlterExpectedColumn {
                        name: "obsolete".to_string(),
                        type_name: Some(actual_type),
                        nullable: Some(true),
                        default: Some(None),
                    }],
                })
                .expect("exact generic type metadata must prepare natively");
            assert_eq!(plan.preview().route, dbflux_core::TableAlterRoute::Native);
            plan.execute()
                .expect("exact generic type metadata must execute the native DROP");
            assert_eq!(
                state
                    .lock()
                    .expect("state mutex should not be poisoned")
                    .query_row(&format!("SELECT retained FROM main.{table}"), [], |row| row
                        .get::<_, String>(0),)
                    .expect("retained data should remain readable"),
                retained_value
            );
        }
    }

    #[test]
    fn native_prepare_rejects_active_transactions_missing_catalog_sql_and_all_unsafe_settings() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch("CREATE TABLE main.people (id INTEGER PRIMARY KEY, obsolete TEXT)")
            .expect("test table should be created");
        let connection = SqliteConnection::for_test(state.clone());
        let request = TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![TableAlterOperation::DropColumn {
                name: "obsolete".to_string(),
            }],
            expected_before: Vec::new(),
        };

        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch("BEGIN")
            .expect("test transaction should begin");
        assert!(
            connection
                .table_alter_planner()
                .expect("SQLite must opt into the table-alter planner seam")
                .prepare(&request)
                .is_err(),
            "active transactions must reject native planning"
        );
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch("ROLLBACK")
            .expect("test transaction should roll back");

        for setting in [
            "writable_schema",
            "legacy_alter_table",
            "ignore_check_constraints",
            "defer_foreign_keys",
        ] {
            state
                .lock()
                .expect("state mutex should not be poisoned")
                .execute_batch(&format!("PRAGMA {setting} = ON"))
                .expect("unsafe setting should be enabled for the fixture");
            let result = connection
                .table_alter_planner()
                .expect("SQLite must opt into the table-alter planner seam")
                .prepare(&request);
            if setting == "defer_foreign_keys" {
                assert!(
                    result.is_ok(),
                    "SQLite resets defer_foreign_keys outside a transaction, so no unsafe state remains"
                );
            } else {
                let error = result
                    .err()
                    .expect("unsafe settings must reject native planning");
                assert!(
                    error.to_string().contains(setting),
                    "the rejection must name {setting}: {error}"
                );
            }
            state
                .lock()
                .expect("state mutex should not be poisoned")
                .execute_batch(&format!("PRAGMA {setting} = OFF"))
                .expect("unsafe setting should be restored after the fixture");
        }

        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch(
                "PRAGMA writable_schema = ON;
                 UPDATE main.sqlite_master SET sql = NULL WHERE type = 'table' AND name = 'people';
                 PRAGMA writable_schema = OFF",
            )
            .expect("fixture should remove the source catalog SQL");
        let error = connection
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam")
            .prepare(&request)
            .err()
            .expect("missing source catalog SQL must reject planning");
        assert!(
            error.to_string().contains("requires catalog SQL"),
            "the rejection must identify missing source catalog SQL: {error}"
        );
    }

    #[test]
    fn native_execution_rejects_settings_drift_without_changing_schema_or_data() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch(
                "CREATE TABLE main.people (id INTEGER PRIMARY KEY, obsolete TEXT, retained TEXT);
                 INSERT INTO main.people VALUES (1, 'remove', 'keep')",
            )
            .expect("test table should be created");
        let connection = SqliteConnection::for_test(state.clone());
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam")
            .prepare(&TableAlterRequest {
                table: TableRef::new("people"),
                operations: vec![TableAlterOperation::DropColumn {
                    name: "obsolete".to_string(),
                }],
                expected_before: Vec::new(),
            })
            .expect("native DROP should prepare before settings drift");
        let changed_foreign_keys = {
            let state = state.lock().expect("state mutex should not be poisoned");
            1 - state
                .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                .expect("initial setting inspection should succeed")
        };
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch(&format!("PRAGMA foreign_keys = {changed_foreign_keys}"))
            .expect("fixture should change the captured setting");
        let error = plan
            .execute()
            .expect_err("settings drift must reject execution");
        assert!(
            error.to_string().contains("connection settings changed"),
            "the stale result must identify settings drift: {error}"
        );
        let state = state.lock().expect("state mutex should not be poisoned");
        assert_eq!(
            state
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('people') WHERE name = 'obsolete'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("table inspection should succeed"),
            1
        );
        assert_eq!(
            state
                .query_row("SELECT retained FROM main.people", [], |row| row
                    .get::<_, String>(0))
                .expect("retained row should remain readable"),
            "keep"
        );
        assert_eq!(
            state
                .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                .expect("setting inspection should succeed"),
            changed_foreign_keys,
            "native planning must not restore or overwrite the user's changed setting"
        );
    }

    #[test]
    fn native_prepare_rejects_catalog_change_between_read_only_capture_passes() {
        let directory = tempfile::tempdir().expect("test directory should be created");
        let database_path = directory.path().join("capture-race.db");
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open(&database_path).expect("file-backed SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch("CREATE TABLE main.people (id INTEGER PRIMARY KEY, obsolete TEXT)")
            .expect("test table should be created");
        let competing_path = database_path.clone();
        *BEFORE_NATIVE_CAPTURE_SECOND_READ
            .lock()
            .expect("native capture test hook mutex should not be poisoned") = Some((
            std::thread::current().id(),
            Box::new(move || {
                RusqliteConnection::open(competing_path)
                    .expect("second SQLite connection should open")
                    .execute_batch("ALTER TABLE main.people ADD COLUMN concurrent_change TEXT")
                    .expect("second connection should change the schema between capture passes");
            }),
        ));

        let error = SqliteConnection::for_test(state.clone())
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam")
            .prepare(&TableAlterRequest {
                table: TableRef::new("people"),
                operations: vec![TableAlterOperation::DropColumn {
                    name: "obsolete".to_string(),
                }],
                expected_before: Vec::new(),
            })
            .err()
            .expect("inconsistent read-only capture must reject planning");
        assert!(
            error.to_string().contains("changed during planning"),
            "the rejection must identify the inconsistent capture: {error}"
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
                .expect("main table inspection should succeed"),
            1,
            "capture rejection must not execute the selected DROP"
        );
    }

    #[test]
    fn native_dependency_scan_includes_user_tables_with_sqlite_prefix_lookalikes() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch(
                "CREATE TABLE main.parent (id INTEGER PRIMARY KEY, obsolete TEXT, retained TEXT);
                 CREATE TABLE main.sqliteXchild (
                     id INTEGER PRIMARY KEY,
                     parent_obsolete TEXT REFERENCES parent(obsolete)
                 )",
            )
            .expect("foreign-key fixture should be created");
        let error = SqliteConnection::for_test(state.clone())
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam")
            .prepare(&TableAlterRequest {
                table: TableRef::new("parent"),
                operations: vec![TableAlterOperation::DropColumn {
                    name: "obsolete".to_string(),
                }],
                expected_before: Vec::new(),
            })
            .err()
            .expect("sqlite-prefix lookalike child must be scanned for inbound dependencies");
        assert!(
            error.to_string().contains("sqliteXchild"),
            "the actionable dependency must name the user child table: {error}"
        );
        assert!(
            super::capture_main_catalog(
                &state.lock().expect("state mutex should not be poisoned"),
            )
            .expect("catalog capture should succeed")
            .iter()
            .any(|entry| entry.name == "sqliteXchild"),
            "literal reserved-prefix filtering must retain the user table in the catalog"
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
