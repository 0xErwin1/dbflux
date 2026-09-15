#[allow(dead_code)]
mod syntax;

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use dbflux_core::{
    DbError, FormattedError, OwnedDefaultSpec, PreparedTableAlter, TableAlterExpectedColumn,
    TableAlterOperation, TableAlterOutcome, TableAlterPlanner, TableAlterPreview,
    TableAlterRequest, TableAlterRoute,
};

use self::syntax::{
    ColumnChange, CreateTable, DefaultChange, KeyDeclaration, KeyDeclarationOrigin, KeyKind,
    KeyTerm, TableConstraint, parse_create_index, parse_create_table,
};
use rusqlite::types::ValueRef;
use rusqlite::{Connection as RusqliteConnection, OptionalExtension};

use crate::driver::SqliteConnectionState;

pub(crate) struct SqliteTableAlterPlanner {
    state: Arc<Mutex<SqliteConnectionState>>,
    cancelled: Arc<AtomicBool>,
}

impl SqliteTableAlterPlanner {
    pub(crate) fn new(
        state: Arc<Mutex<SqliteConnectionState>>,
        cancelled: Arc<AtomicBool>,
    ) -> Self {
        Self { state, cancelled }
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
        if let Some(rebuild_changes) = selected_rebuild_changes(&request.operations)? {
            let state = SqliteConnectionState::lock_checked(&self.state)?;
            if !state.is_autocommit() {
                return Err(DbError::NotSupported(
                    "SQLite table alteration planning requires an autocommit connection"
                        .to_string(),
                ));
            }
            return prepare_rebuild_plan(
                &state,
                self.state.clone(),
                self.cancelled.clone(),
                request,
                rebuild_changes,
            );
        }
        let columns = selected_native_drop_columns(&request.operations)?;
        if !sqlite_drop_column_supported() {
            let state = SqliteConnectionState::lock_checked(&self.state)?;
            if !state.is_autocommit() {
                return Err(DbError::NotSupported(
                    "SQLite table alteration planning requires an autocommit connection"
                        .to_string(),
                ));
            }
            return prepare_rebuild_plan(
                &state,
                self.state.clone(),
                self.cancelled.clone(),
                request,
                RebuildChanges {
                    alterations: Vec::new(),
                    drops: columns,
                },
            );
        }

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

fn prepare_rebuild_plan(
    connection: &RusqliteConnection,
    state: Arc<Mutex<SqliteConnectionState>>,
    cancelled: Arc<AtomicBool>,
    request: &TableAlterRequest,
    changes: RebuildChanges,
) -> Result<Box<dyn PreparedTableAlter>, DbError> {
    Ok(prepare_rebuild_plan_typed(
        connection, state, cancelled, request, changes,
    )?)
}

fn prepare_rebuild_plan_typed(
    connection: &RusqliteConnection,
    state: Arc<Mutex<SqliteConnectionState>>,
    cancelled: Arc<AtomicBool>,
    request: &TableAlterRequest,
    changes: RebuildChanges,
) -> Result<Box<RebuildPlan>, DbError> {
    let table = &request.table.name;
    let observation = capture_rebuild_observation(connection, table)?;
    #[cfg(test)]
    if let Some((_, hook)) = AFTER_REBUILD_OBSERVATION_CAPTURE
        .lock()
        .expect("rebuild observation test hook mutex should not be poisoned")
        .take_if(|(thread_id, _)| *thread_id == std::thread::current().id())
    {
        hook();
    }
    let source_sql = observation.source_sql.clone();
    let columns = observation.columns.clone();
    let catalog = observation.catalog.clone();
    let parsed = parse_create_table(&source_sql).map_err(|reason| {
        DbError::NotSupported(format!(
            "SQLite rebuild cannot safely reconstruct main.{table}: {reason}"
        ))
    })?;
    let selected = changes
        .alterations
        .iter()
        .map(|change| change.name.clone())
        .chain(changes.drops.iter().cloned())
        .collect::<Vec<_>>();
    if let Some(column) = columns.iter().find(|column| column.hidden != 0) {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild for main.{table} rejects hidden or generated column {}",
            column.name
        )));
    }
    for name in &selected {
        if !columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(name))
        {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild cannot find selected column {name} on main.{table}"
            )));
        }
    }
    validate_expected_source(table, &selected, &request.expected_before, &columns)?;
    validate_parsed_table_metadata(table, &parsed, &columns)?;
    validate_rebuild_index_metadata(table, &parsed, &columns, &observation.indexes)?;
    let resolved_foreign_keys = resolve_foreign_key_relationships(
        table,
        &observation.table_metadata,
        &observation.foreign_key_metadata,
    )?;
    validate_parsed_foreign_keys(table, &parsed, &resolved_foreign_keys)?;
    reject_rebuild_global_objects(table, &catalog, &observation.temp_catalog)?;
    reject_rebuild_attachments(&observation.databases)?;
    reject_rebuild_foreign_key_violations(table, observation.foreign_key_violation.as_deref())?;
    reject_rebuild_dependencies(
        table,
        &selected,
        &columns,
        &observation.indexes,
        &resolved_foreign_keys,
    )?;
    let table_metadata = observation
        .table_metadata
        .iter()
        .find(|metadata| metadata.name.eq_ignore_ascii_case(table))
        .ok_or_else(|| {
            DbError::NotSupported(format!(
                "SQLite rebuild planning cannot find captured metadata for main.{table}"
            ))
        })?;
    let identity = rebuild_identity(table, &columns, &parsed, &table_metadata.rowid_aliases)?;
    let replacement = format!("__dbflux_rebuild_{table}");
    if catalog
        .iter()
        .any(|entry| entry.name.eq_ignore_ascii_case(&replacement))
    {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild replacement name main.{replacement} collides with an existing catalog object"
        )));
    }
    let rebuilt_sql = parsed
        .rewrite_rebuild(&changes.alterations, &changes.drops, &replacement)
        .map_err(|reason| {
            DbError::NotSupported(format!(
                "SQLite rebuild cannot reconstruct main.{table}: {reason}"
            ))
        })?;
    let rewritten = parse_create_table(&rebuilt_sql).map_err(|reason| {
        DbError::NotSupported(format!(
            "SQLite rebuild cannot validate reconstructed foreign key source for main.{table}: {reason}"
        ))
    })?;
    validate_foreign_key_source_preservation(table, &parsed, &rewritten)?;
    validate_rebuild_semantic_delta(table, &parsed, &rewritten, &changes)?;
    let retained = parsed
        .column_names()
        .filter(|name| {
            !changes
                .drops
                .iter()
                .any(|drop| name.eq_ignore_ascii_case(drop))
        })
        .filter(|name| !name.eq_ignore_ascii_case(&identity))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let copy = RebuildCopyIntent::new(identity.clone(), retained.clone());
    validate_rebuild_identity_after_rewrite(table, &identity, &rewritten, &copy)?;
    let statements = lifecycle_preview_statements(
        table,
        &replacement,
        &rebuilt_sql,
        &copy,
        &observation.indexes,
    );
    let exact_index_sql = observation
        .indexes
        .iter()
        .filter_map(|index| index.sql.clone())
        .collect::<Vec<_>>();
    let capture = RebuildCapture {
        observation,
        prepared_create_sql: rebuilt_sql,
        final_expected_facts: RebuildExpectedFacts {
            selected_changes: changes,
            retained_columns: retained.clone(),
            identity: identity.clone(),
            explicit_index_sql: exact_index_sql.clone(),
            foreign_key_intent: resolved_foreign_keys.clone(),
        },
        replacement,
        copy,
    };
    Ok(Box::new(RebuildPlan {
        state,
        cancelled,
        capture,
        preview: TableAlterPreview {
            route: TableAlterRoute::Rebuild,
            statements,
            warnings: vec![
                "NOT EXECUTABLE: driver-managed illustrative lifecycle intent only; do not submit preview statements as apply SQL.".to_string(),
                "Preparation remains read-only; driver-managed execution validates and copies data with exact streamed ValueRef comparison.".to_string(),
            ],
            table_atomic: true,
            driver_managed: true,
        },
    }))
}

#[allow(dead_code)]
fn obsolete_prepare_rebuild_plan_body(
    _connection: &RusqliteConnection,
    _table: &str,
) -> Result<Option<String>, DbError> {
    Ok(None)
}
/*
        DbError::NotSupported(format!(
            "SQLite rebuild planning requires catalog SQL for main.{table}"
        ))
    })?;
    let parsed = parse_create_table(&source_sql).map_err(|reason| {
        DbError::NotSupported(format!(
            "SQLite rebuild cannot safely reconstruct main.{table}: {reason}"
        ))
    })?;
    let selected = changes
        .alterations
        .iter()
        .map(|change| change.name.clone())
        .chain(changes.drops.iter().cloned())
        .collect::<Vec<_>>();
    let columns = table_columns(connection, table)?;
    if let Some(column) = columns.iter().find(|column| column.hidden != 0) {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild for main.{table} rejects hidden or generated column {}",
            column.name
        )));
    }
    for name in &selected {
        if !columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(name))
        {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild cannot find selected column {name} on main.{table}"
            )));
        }
    }
    validate_expected_source(table, &selected, &request.expected_before, &columns)?;
    validate_parsed_table_metadata(table, &parsed, &columns)?;
    capture_native_connection_settings(connection)?;
    let catalog = capture_main_catalog(connection)?;
    reject_rebuild_global_objects(connection, table, &catalog)?;
    reject_rebuild_attachments(connection)?;
    reject_rebuild_foreign_key_violations(connection, table)?;
    reject_known_native_dependencies(connection, table, &selected)?;
    let identity = rebuild_identity(connection, table, &columns)?;
    let replacement = format!("__dbflux_rebuild_{table}");
    if catalog
        .iter()
        .any(|entry| entry.name.eq_ignore_ascii_case(&replacement))
    {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild replacement name main.{replacement} collides with an existing catalog object"
        )));
    }
    let rebuilt_sql = parsed
        .rewrite_rebuild(&changes.alterations, &changes.drops, &replacement)
        .map_err(|reason| {
            DbError::NotSupported(format!(
                "SQLite rebuild cannot reconstruct main.{table}: {reason}"
            ))
        })?;
    let retained = parsed
        .column_names()
        .filter(|name| {
            !changes
                .drops
                .iter()
                .any(|drop| name.eq_ignore_ascii_case(drop))
        })
        .filter(|name| !name.eq_ignore_ascii_case(&identity))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let copy_columns = std::iter::once(identity.clone())
        .chain(retained.iter().cloned())
        .map(|column| quote_identifier(&column))
        .collect::<Vec<_>>()
        .join(", ");
    let mut statements = vec![
        "BEGIN IMMEDIATE;".to_string(),
        rebuilt_sql,
        format!(
            "INSERT INTO main.{} ({copy_columns}) SELECT {copy_columns} FROM main.{};",
            quote_identifier(&replacement),
            quote_identifier(table)
        ),
        format!(
            "-- compare exact identity and storage values before replacing main.{}",
            quote_identifier(table)
        ),
        format!(
            "-- driver-managed atomic replacement and index restoration for main.{}",
            quote_identifier(table)
        ),
    ];
    let index_intent = rebuild_index_intent(connection, table, &catalog)?;
    let exact_index_sql = catalog
        .iter()
        .filter(|entry| {
            entry.object_type == "index" && entry.table_name.eq_ignore_ascii_case(table)
        })
        .filter_map(|entry| entry.sql.clone())
        .collect::<Vec<_>>();
    let capture = RebuildCapture {
        source_sql,
        settings: capture_native_connection_settings(connection)?,
        catalog,
        schema_versions: capture_schema_versions(connection)?,
        columns,
        retained_columns: retained,
        identity,
        replacement,
        exact_index_sql,
        foreign_keys: capture_all_foreign_keys(connection)?,
    };
    statements.extend(index_intent);
    statements.push("COMMIT;".to_string());
    Ok(Box::new(RebuildPlan {
        capture,
        preview: TableAlterPreview {
            route: TableAlterRoute::Rebuild,
            statements,
            warnings: vec![
                "Driver-managed atomic table plan; preview statements are illustrative and are not executable apply SQL.".to_string(),
                "Preparation remains read-only; driver-managed execution validates existing data compatibility.".to_string(),
            ],
            table_atomic: true,
            driver_managed: true,
        },
    }))
}
*/

fn reject_rebuild_global_objects(
    table: &str,
    catalog: &[CatalogEntry],
    temp_catalog: &[CatalogEntry],
) -> Result<(), DbError> {
    if let Some(object) = catalog
        .iter()
        .find(|object| matches!(object.object_type.as_str(), "view" | "trigger"))
    {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild for main.{table} rejects global main view or trigger {}",
            object.name
        )));
    }
    if temp_catalog
        .iter()
        .any(|object| matches!(object.object_type.as_str(), "view" | "trigger"))
    {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild for main.{table} rejects global main or temp view or trigger"
        )));
    }
    Ok(())
}

fn reject_rebuild_attachments(databases: &[DatabaseIdentity]) -> Result<(), DbError> {
    if databases
        .iter()
        .any(|database| !matches!(database.name.as_str(), "main" | "temp"))
    {
        return Err(DbError::NotSupported(
            "SQLite rebuild planning rejects connections with attached schemas".to_string(),
        ));
    }
    Ok(())
}

fn reject_rebuild_foreign_key_violations(
    table: &str,
    violation: Option<&str>,
) -> Result<(), DbError> {
    if let Some(child) = violation {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild for main.{table} rejects pre-existing foreign key violation in main.{child}"
        )));
    }
    Ok(())
}

fn rebuild_identity(
    table: &str,
    columns: &[TableColumnCapture],
    parsed: &CreateTable,
    rowid_aliases: &[String],
) -> Result<String, DbError> {
    let declarations = parsed
        .key_declarations()
        .into_iter()
        .map(|declaration| resolve_key_declaration(table, parsed, declaration))
        .collect::<Result<Vec<_>, _>>()?;
    let primary_keys = columns
        .iter()
        .filter(|column| column.primary_key_order != 0)
        .collect::<Vec<_>>();
    if let [column] = primary_keys.as_slice()
        && column.primary_key_order == 1
        && declarations.iter().any(|declaration| {
            declaration.kind == KeyKind::PrimaryKey
                && declaration.terms.len() == 1
                && declaration
                    .terms
                    .first()
                    .is_some_and(|term| term.column.eq_ignore_ascii_case(&column.name))
                && integer_primary_key_is_rowid_alias(declaration, parsed, columns)
        })
    {
        return Ok(column.name.clone());
    }
    if let Some(alias) = rowid_aliases.iter().find(|alias| {
        !columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(alias))
    }) {
        return Ok(alias.clone());
    }
    Err(DbError::NotSupported(format!(
        "SQLite rebuild for main.{table} requires an INTEGER PRIMARY KEY or accessible hidden rowid"
    )))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedKeyTerm {
    column: String,
    descending: bool,
    collation: String,
}

fn validate_rebuild_index_metadata(
    table: &str,
    parsed: &CreateTable,
    columns: &[TableColumnCapture],
    indexes: &[IndexCapture],
) -> Result<(), DbError> {
    let declarations = parsed
        .key_declarations()
        .into_iter()
        .map(|declaration| resolve_key_declaration(table, parsed, declaration))
        .collect::<Result<Vec<_>, _>>()?;

    let expected_autoindexes = reconcile_autoindex_intents(&declarations, parsed, columns);
    let mut unmatched_autoindexes = Vec::new();
    for index in indexes {
        if index.partial {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild for main.{table} rejects partial index main.{}",
                index.name
            )));
        }
        let captured_keys = captured_key_terms(index);
        match index.origin.as_str() {
            "c" => validate_explicit_index_metadata(table, parsed, index, &captured_keys)?,
            "pk" | "u" => unmatched_autoindexes.push(index),
            _ => {
                return Err(DbError::NotSupported(format!(
                    "SQLite rebuild catalog proof cannot account for index origin on main.{}",
                    index.name
                )));
            }
        }
    }

    for expected in &expected_autoindexes {
        let Some(position) = unmatched_autoindexes.iter().position(|index| {
            index.unique == expected.unique
                && index.origin == expected.origin
                && key_terms_match(&expected.terms, &captured_key_terms(index))
        }) else {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild catalog proof cannot account for physical {} autoindex on main.{table}",
                key_kind_name(expected.kind)
            )));
        };
        unmatched_autoindexes.remove(position);
    }
    if let Some(index) = unmatched_autoindexes.first() {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild catalog proof cannot account for autoindex main.{}",
            index.name
        )));
    }
    Ok(())
}

fn resolve_key_declaration(
    table: &str,
    parsed: &CreateTable,
    declaration: KeyDeclaration,
) -> Result<ResolvedKeyDeclaration, DbError> {
    let terms = declaration
        .terms
        .iter()
        .map(|term| resolve_key_term(table, parsed, term))
        .collect::<Result<Vec<_>, _>>()?;
    if terms.is_empty() {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild catalog proof found an empty {} on main.{table}",
            key_kind_name(declaration.kind)
        )));
    }
    Ok(ResolvedKeyDeclaration {
        kind: declaration.kind,
        origin: declaration.origin,
        terms,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedKeyDeclaration {
    kind: KeyKind,
    origin: KeyDeclarationOrigin,
    terms: Vec<ResolvedKeyTerm>,
}

fn resolve_key_term(
    table: &str,
    parsed: &CreateTable,
    term: &KeyTerm,
) -> Result<ResolvedKeyTerm, DbError> {
    let column = parsed
        .column_facts()
        .find(|column| column.name.eq_ignore_ascii_case(&term.column))
        .ok_or_else(|| {
            DbError::NotSupported(format!(
                "SQLite rebuild catalog proof names unknown key column {} on main.{table}",
                term.column
            ))
        })?;
    Ok(ResolvedKeyTerm {
        column: column.name.clone(),
        descending: term.descending,
        collation: term
            .explicit_collation
            .clone()
            .unwrap_or_else(|| column.collation.clone()),
    })
}

fn captured_key_terms(index: &IndexCapture) -> Vec<ResolvedKeyTerm> {
    index
        .keys
        .iter()
        .map(|key| ResolvedKeyTerm {
            column: key.column.clone(),
            descending: key.descending,
            collation: key.collation.clone(),
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PhysicalAutoindexIntent {
    kind: KeyKind,
    unique: bool,
    origin: String,
    terms: Vec<ResolvedKeyTerm>,
}

fn reconcile_autoindex_intents(
    declarations: &[ResolvedKeyDeclaration],
    parsed: &CreateTable,
    columns: &[TableColumnCapture],
) -> Vec<PhysicalAutoindexIntent> {
    let mut intents = Vec::new();
    for declaration in declarations {
        if declaration.kind == KeyKind::PrimaryKey
            && integer_primary_key_is_rowid_alias(declaration, parsed, columns)
        {
            continue;
        }
        if let Some(intent) = intents
            .iter_mut()
            .find(|intent: &&mut PhysicalAutoindexIntent| {
                key_terms_equivalent_for_constraint(&intent.terms, &declaration.terms)
            })
        {
            if declaration.kind == KeyKind::PrimaryKey {
                intent.kind = KeyKind::PrimaryKey;
                intent.origin = "pk".to_string();
            }
            continue;
        }
        intents.push(PhysicalAutoindexIntent {
            kind: declaration.kind,
            unique: true,
            origin: match declaration.kind {
                KeyKind::PrimaryKey => "pk".to_string(),
                KeyKind::Unique => "u".to_string(),
            },
            terms: declaration.terms.clone(),
        });
    }
    intents
}

fn key_terms_equivalent_for_constraint(
    left: &[ResolvedKeyTerm],
    right: &[ResolvedKeyTerm],
) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.column.eq_ignore_ascii_case(&right.column)
                && left.collation.eq_ignore_ascii_case(&right.collation)
        })
}

fn key_terms_match(left: &[ResolvedKeyTerm], right: &[ResolvedKeyTerm]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.column.eq_ignore_ascii_case(&right.column)
                && left.descending == right.descending
                && left.collation.eq_ignore_ascii_case(&right.collation)
        })
}

fn integer_primary_key_is_rowid_alias(
    declaration: &ResolvedKeyDeclaration,
    parsed: &CreateTable,
    columns: &[TableColumnCapture],
) -> bool {
    let [term] = declaration.terms.as_slice() else {
        return false;
    };
    (matches!(declaration.origin, KeyDeclarationOrigin::Table) || !term.descending)
        && columns.iter().any(|column| {
            column.name.eq_ignore_ascii_case(&term.column)
                && column.primary_key_order == 1
                && column.type_name == "INTEGER"
        })
        && parsed.column_facts().any(|column| {
            column.name.eq_ignore_ascii_case(&term.column)
                && column.declared_type.as_deref() == Some("INTEGER")
                && column.primary_key_order == 1
        })
}

fn validate_explicit_index_metadata(
    table: &str,
    parsed: &CreateTable,
    index: &IndexCapture,
    captured_keys: &[ResolvedKeyTerm],
) -> Result<(), DbError> {
    let sql = index.sql.as_deref().ok_or_else(|| {
        DbError::NotSupported(format!(
            "SQLite rebuild cannot account for explicit index main.{}",
            index.name
        ))
    })?;
    let definition = parse_create_index(sql).map_err(|reason| {
        DbError::NotSupported(format!(
            "SQLite rebuild catalog proof cannot parse explicit index main.{}: {reason}",
            index.name
        ))
    })?;
    let definition_keys = definition
        .keys
        .iter()
        .map(|term| resolve_key_term(table, parsed, term))
        .collect::<Result<Vec<_>, _>>()?;
    if !definition.name.eq_ignore_ascii_case(&index.name)
        || !definition.target_table.eq_ignore_ascii_case(table)
        || definition.unique != index.unique
        || !key_terms_match(&definition_keys, captured_keys)
    {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild catalog proof disagrees with captured explicit index main.{}",
            index.name
        )));
    }
    Ok(())
}

fn key_kind_name(kind: KeyKind) -> &'static str {
    match kind {
        KeyKind::PrimaryKey => "PRIMARY KEY",
        KeyKind::Unique => "UNIQUE",
    }
}

#[allow(dead_code)]
fn rebuild_index_intent(indexes: &[IndexCapture]) -> Result<Vec<String>, DbError> {
    indexes
        .iter()
        .filter(|index| index.origin == "c")
        .map(|index| {
            let sql = index.sql.as_deref().ok_or_else(|| {
                DbError::NotSupported(format!(
                    "SQLite rebuild cannot account for explicit index main.{}",
                    index.name
                ))
            })?;
            Ok(format!("-- restore exact index main.{}: {sql}", index.name))
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexKeyCapture {
    sequence: i64,
    column: String,
    descending: bool,
    collation: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexCapture {
    name: String,
    unique: bool,
    origin: String,
    partial: bool,
    keys: Vec<IndexKeyCapture>,
    sql: Option<String>,
}

fn capture_index_metadata(
    connection: &RusqliteConnection,
    table: &str,
    catalog: &[CatalogEntry],
) -> Result<Vec<IndexCapture>, DbError> {
    let mut statement = connection
        .prepare(&format!(
            "PRAGMA main.index_list({})",
            quote_identifier(table)
        ))
        .map_err(|error| {
            DbError::query_failed(format!(
                "could not inspect indexes for main.{table}: {error}"
            ))
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .map_err(|error| {
            DbError::query_failed(format!("could not read indexes for main.{table}: {error}"))
        })?;
    let mut indexes = Vec::new();
    for row in rows {
        let (name, unique, origin, partial) = row.map_err(|error| {
            DbError::query_failed(format!("could not decode index for main.{table}: {error}"))
        })?;
        if !matches!(origin.as_str(), "c" | "u" | "pk") || partial != 0 {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild cannot prove index main.{name} metadata"
            )));
        }
        let sql = catalog
            .iter()
            .find(|entry| entry.object_type == "index" && entry.name == name)
            .and_then(|entry| entry.sql.clone());
        if (origin == "c" && sql.is_none()) || (origin != "c" && sql.is_some()) {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild catalog proof disagrees with index origin for main.{name}"
            )));
        }
        let mut details = connection
            .prepare(&format!(
                "PRAGMA main.index_xinfo({})",
                quote_identifier(&name)
            ))
            .map_err(|error| {
                DbError::query_failed(format!("could not inspect index main.{name}: {error}"))
            })?;
        let key_rows = details
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })
            .map_err(|error| {
                DbError::query_failed(format!("could not read index main.{name}: {error}"))
            })?;
        let mut keys = Vec::new();
        for key_row in key_rows {
            let (sequence, column_id, column, descending, collation, is_key) =
                key_row.map_err(|error| {
                    DbError::query_failed(format!("could not decode index main.{name}: {error}"))
                })?;
            if is_key == 0 {
                continue;
            }
            let column = column.filter(|_| column_id >= 0).ok_or_else(|| {
                DbError::NotSupported(format!(
                    "SQLite rebuild rejects expression index main.{name}"
                ))
            })?;
            if !matches!(collation.as_str(), "BINARY" | "NOCASE" | "RTRIM") {
                return Err(DbError::NotSupported(format!(
                    "SQLite rebuild rejects custom collation on index main.{name}"
                )));
            }
            keys.push(IndexKeyCapture {
                sequence,
                column,
                descending: descending != 0,
                collation,
            });
        }
        keys.sort_by_key(|key| key.sequence);
        if keys.is_empty() {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild cannot prove key columns for main.{name}"
            )));
        }
        indexes.push(IndexCapture {
            name,
            unique: unique != 0,
            origin,
            partial: partial != 0,
            keys,
            sql,
        });
    }
    indexes.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(indexes)
}

#[derive(Debug, Clone)]
struct RebuildChanges {
    alterations: Vec<ColumnChange>,
    drops: Vec<String>,
}

fn selected_rebuild_changes(
    operations: &[TableAlterOperation],
) -> Result<Option<RebuildChanges>, DbError> {
    if operations.is_empty() {
        return Err(DbError::NotSupported(
            "SQLite table alteration planning requires at least one selected column".to_string(),
        ));
    }
    let mut names = HashSet::new();
    let mut alterations = Vec::new();
    let mut drops = Vec::new();
    let mut needs_rebuild = false;
    for operation in operations {
        match operation {
            TableAlterOperation::AlterColumn {
                name,
                new_type,
                nullable,
                default,
            } => {
                if name.is_empty() || !names.insert(name.to_ascii_lowercase()) {
                    return Err(DbError::NotSupported(
                        "SQLite rebuild planning requires unique non-empty selected column names"
                            .to_string(),
                    ));
                }
                if new_type.is_none() && nullable.is_none() && default.is_none() {
                    return Err(DbError::NotSupported(format!(
                        "SQLite rebuild planning requires a selected alteration for main column {name}"
                    )));
                }
                needs_rebuild = true;
                alterations.push(ColumnChange {
                    name: name.clone(),
                    new_type: new_type.clone(),
                    nullable: *nullable,
                    default: default.as_ref().map(|default| match default {
                        OwnedDefaultSpec::Drop => DefaultChange::Drop,
                        OwnedDefaultSpec::Set(value) => DefaultChange::Set(value.clone()),
                    }),
                });
            }
            TableAlterOperation::DropColumn { name } => {
                if name.is_empty() || !names.insert(name.to_ascii_lowercase()) {
                    return Err(DbError::NotSupported(
                        "SQLite rebuild planning requires unique non-empty selected column names"
                            .to_string(),
                    ));
                }
                drops.push(name.clone());
            }
            TableAlterOperation::Unsupported { description } => {
                return Err(DbError::NotSupported(format!(
                    "SQLite table alteration request contains an unsupported selected change: {description}"
                )));
            }
        }
    }
    if needs_rebuild {
        Ok(Some(RebuildChanges { alterations, drops }))
    } else {
        Ok(None)
    }
}

type ForeignKeyReference = (String, String, String, Option<String>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct RebuildObservation {
    source_sql: String,
    settings: NativeConnectionSettings,
    catalog: Vec<CatalogEntry>,
    temp_catalog: Vec<CatalogEntry>,
    databases: Vec<DatabaseIdentity>,
    schema_versions: Vec<SchemaVersion>,
    columns: Vec<TableColumnCapture>,
    table_metadata: Vec<TableMetadataCapture>,
    indexes: Vec<IndexCapture>,
    foreign_keys: Vec<ForeignKeyReference>,
    foreign_key_metadata: Vec<ForeignKeyCapture>,
    foreign_key_violation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DatabaseIdentity {
    sequence: i64,
    name: String,
    file_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableMetadataCapture {
    name: String,
    columns: Vec<TableColumnCapture>,
    rowid_aliases: Vec<String>,
}

fn capture_rebuild_observation(
    connection: &RusqliteConnection,
    table: &str,
) -> Result<RebuildObservation, DbError> {
    let before = capture_rebuild_observation_once(connection, table)?;
    #[cfg(test)]
    if let Some((_, hook)) = BEFORE_REBUILD_CAPTURE_SECOND_READ
        .lock()
        .expect("rebuild capture test hook mutex should not be poisoned")
        .take_if(|(thread_id, _)| *thread_id == std::thread::current().id())
    {
        hook();
    }
    let after = capture_rebuild_observation_once(connection, table)?;
    if before != after {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild source changed during planning for main.{table}; refresh the preview"
        )));
    }
    Ok(after)
}

fn capture_rebuild_observation_once(
    connection: &RusqliteConnection,
    table: &str,
) -> Result<RebuildObservation, DbError> {
    let catalog = capture_main_catalog(connection)?;
    let source_sql = catalog
        .iter()
        .find(|entry| entry.object_type == "table" && entry.name.eq_ignore_ascii_case(table))
        .and_then(|entry| entry.sql.clone())
        .ok_or_else(|| {
            DbError::NotSupported(format!(
                "SQLite rebuild planning requires catalog SQL for main.{table}"
            ))
        })?;
    let temp_catalog = capture_temp_catalog(connection)?;
    let databases = capture_database_identities(connection)?;
    let table_metadata = capture_table_metadata(connection, &catalog)?;
    let columns = table_metadata
        .iter()
        .find(|metadata| metadata.name.eq_ignore_ascii_case(table))
        .map(|metadata| metadata.columns.clone())
        .ok_or_else(|| {
            DbError::NotSupported(format!(
                "SQLite rebuild planning cannot find table metadata for main.{table}"
            ))
        })?;
    let table_names = table_metadata
        .iter()
        .map(|metadata| metadata.name.as_str())
        .collect::<Vec<_>>();
    Ok(RebuildObservation {
        source_sql,
        settings: capture_native_connection_settings(connection)?,
        temp_catalog,
        schema_versions: capture_rebuild_schema_versions(connection, &databases)?,
        indexes: capture_index_metadata(connection, table, &catalog)?,
        foreign_keys: capture_all_foreign_keys(connection, &table_names)?,
        foreign_key_metadata: capture_all_foreign_key_metadata(connection, &table_names)?,
        foreign_key_violation: capture_foreign_key_violation(connection)?,
        catalog,
        databases,
        columns,
        table_metadata,
    })
}

fn validate_rebuild_semantic_delta(
    table: &str,
    source: &CreateTable,
    rewritten: &CreateTable,
    changes: &RebuildChanges,
) -> Result<(), DbError> {
    let source_columns = source.column_facts().collect::<Vec<_>>();
    let rewritten_columns = rewritten.column_facts().collect::<Vec<_>>();
    let retained_source = source_columns
        .iter()
        .filter(|column| {
            !changes
                .drops
                .iter()
                .any(|drop| column.name.eq_ignore_ascii_case(drop))
        })
        .collect::<Vec<_>>();
    if retained_source.len() != rewritten_columns.len()
        || retained_source
            .iter()
            .zip(&rewritten_columns)
            .any(|(before, after)| !before.name.eq_ignore_ascii_case(&after.name))
    {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild semantic proof changed retained column order or names for main.{table}"
        )));
    }
    for (before, after) in retained_source.iter().zip(&rewritten_columns) {
        let change = changes
            .alterations
            .iter()
            .find(|change| change.name.eq_ignore_ascii_case(&before.name));
        if let Some(change) = change {
            let mut expected = (**before).clone();
            if let Some(new_type) = &change.new_type {
                expected.declared_type = Some(new_type.clone());
            }
            if let Some(nullable) = change.nullable {
                expected.nullable = nullable;
            }
            match &change.default {
                Some(DefaultChange::Drop) => expected.default = None,
                Some(DefaultChange::Set(value)) => expected.default = Some(value.clone()),
                None => {}
            }
            if !column_facts_match(&expected, after) {
                return Err(DbError::NotSupported(format!(
                    "SQLite rebuild semantic proof cannot establish only the requested delta for main.{table}.{}",
                    before.name
                )));
            }
        } else if !column_facts_match(before, after) {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild semantic proof changed unselected declaration facts for main.{table}.{}",
                before.name
            )));
        }
    }
    Ok(())
}

fn column_facts_match(left: &self::syntax::ColumnFacts, right: &self::syntax::ColumnFacts) -> bool {
    left.name.eq_ignore_ascii_case(&right.name)
        && left.declared_type == right.declared_type
        && left.nullable == right.nullable
        && left.default == right.default
        && left.collation == right.collation
        && left.primary_key_order == right.primary_key_order
        && left.primary_key_descending == right.primary_key_descending
        && left.unique == right.unique
        && match (&left.foreign_key, &right.foreign_key) {
            (Some(left), Some(right)) => {
                left.columns == right.columns
                    && left.parent_table.eq_ignore_ascii_case(&right.parent_table)
                    && left.parent_columns == right.parent_columns
                    && left.on_update == right.on_update
                    && left.on_delete == right.on_delete
                    && left.deferrable == right.deferrable
            }
            (None, None) => true,
            _ => false,
        }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RebuildCopyIntent {
    identity: String,
    source_projection: Vec<String>,
    replacement_projection: Vec<String>,
    comparison: ExactStreamedComparisonPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExactStreamedComparisonPolicy {
    ordered_rows: bool,
    row_count: bool,
    integer_identity: bool,
    storage_class: bool,
    exact_integer: bool,
    real_bits: bool,
    text_and_blob_bytes: bool,
    invalid_utf8_and_nul: bool,
}

impl RebuildCopyIntent {
    fn new(identity: String, retained_columns: Vec<String>) -> Self {
        let source_projection = std::iter::once(identity.clone())
            .chain(retained_columns)
            .collect::<Vec<_>>();
        Self {
            replacement_projection: source_projection.clone(),
            identity,
            source_projection,
            comparison: ExactStreamedComparisonPolicy {
                ordered_rows: true,
                row_count: true,
                integer_identity: true,
                storage_class: true,
                exact_integer: true,
                real_bits: true,
                text_and_blob_bytes: true,
                invalid_utf8_and_nul: true,
            },
        }
    }

    fn quoted_projection(&self) -> String {
        self.source_projection
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn validate_rebuild_identity_after_rewrite(
    table: &str,
    identity: &str,
    rewritten: &CreateTable,
    copy: &RebuildCopyIntent,
) -> Result<(), DbError> {
    if copy.identity != identity
        || copy.source_projection.first() != Some(&identity.to_string())
        || copy.source_projection.len() != copy.replacement_projection.len()
        || copy.source_projection.iter().skip(1).any(|column| {
            column.eq_ignore_ascii_case(identity)
                || !rewritten
                    .column_names()
                    .any(|rewritten_column| rewritten_column.eq_ignore_ascii_case(column))
        })
    {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild identity projection is not stable after rewrite for main.{table}"
        )));
    }
    Ok(())
}

fn lifecycle_preview_statements(
    table: &str,
    replacement: &str,
    rebuilt_sql: &str,
    copy: &RebuildCopyIntent,
    indexes: &[IndexCapture],
) -> Vec<String> {
    let projection = copy.quoted_projection();
    let comparison = &copy.comparison;
    let mut statements = vec![
        "INTENT: verify usable connection, autocommit, safe settings, source freshness, and private replacement-name availability.".to_string(),
        "INTENT: save foreign_keys; set foreign_keys = OFF; foreign_keys readback; all before BEGIN.".to_string(),
        "BEGIN IMMEDIATE;".to_string(),
        "INTENT: under lock revalidate freshness and baseline foreign-key cleanliness.".to_string(),
        rebuilt_sql.to_string(),
        format!(
            "INSERT INTO main.{} ({projection}) SELECT {projection} FROM main.{};",
            quote_identifier(replacement),
            quote_identifier(table)
        ),
        format!(
            "INTENT: exact streamed comparison of ordered source/replacement projections using ValueRef: rowcount={}, integer identity={}, storage class={}, exact integer={}, REAL bits={}, TEXT/BLOB bytes={}, invalid UTF-8/NUL={}; no SQL equality, hashes, lossy DbValue, or table buffering.",
            comparison.row_count,
            comparison.integer_identity,
            comparison.storage_class,
            comparison.exact_integer,
            comparison.real_bits,
            comparison.text_and_blob_bytes,
            comparison.invalid_utf8_and_nul,
        ),
        "INTENT: statement finalization completes before destructive DROP.".to_string(),
        format!("DROP TABLE main.{};", quote_identifier(table)),
        format!(
            "ALTER TABLE main.{} RENAME TO {};",
            quote_identifier(replacement),
            quote_identifier(table)
        ),
    ];
    statements.extend(
        indexes
            .iter()
            .filter(|index| index.origin == "c")
            .filter_map(|index| {
                index
                    .sql
                    .as_ref()
                    .map(|sql| format!("INTENT: restore exact index main.{}: {sql}", index.name))
            }),
    );
    statements.extend([
        "INTENT: verify final schema, explicit indexes, non-target catalog, no private-name leak, foreign_key_check, and one-row integrity_check result of ok.".to_string(),
        "COMMIT;".to_string(),
        "INTENT: after transaction ends, restore prior foreign_keys and read back the setting.".to_string(),
        "INTENT FAILURE: rollback, verify autocommit, then restore foreign_keys; surface cleanup failure, quarantine, and commit certainty rather than claiming success.".to_string(),
    ]);
    statements
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RebuildGuardStage {
    SavingForeignKeys,
    DisablingForeignKeys,
    Beginning,
    Active,
    Committing,
    RollingBack,
    RestoringForeignKeys,
    Finished,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitCertainty {
    NotAttempted,
    RolledBack,
    ConfirmedCommitted,
    Uncertain,
}

#[allow(dead_code)]
#[derive(Default)]
struct RebuildGuardFailures {
    primary: Option<DbError>,
    rollback: Option<String>,
    restoration: Option<String>,
}

#[allow(dead_code)]
impl RebuildGuardFailures {
    fn has_cleanup_failure(&self) -> bool {
        self.rollback.is_some() || self.restoration.is_some()
    }

    fn details(&self, stage: RebuildGuardStage, certainty: CommitCertainty) -> String {
        let mut details = vec![format!("stage={stage:?}; commit_certainty={certainty:?}")];
        if let Some(primary) = &self.primary {
            details.push(format!("primary failure: {primary}"));
        }
        if let Some(rollback) = &self.rollback {
            details.push(format!("rollback failure: {rollback}"));
        }
        if let Some(restoration) = &self.restoration {
            details.push(format!("foreign-key restoration failure: {restoration}"));
        }
        details.join("; ")
    }
}

#[cfg(test)]
#[derive(Default)]
struct RebuildGuardFaults {
    fail_disable_set: bool,
    fail_disable_readback: bool,
    fail_begin: bool,
    fail_commit_while_active: bool,
    fail_commit_after_commit: bool,
    fail_rollback: bool,
    fail_restore_set: bool,
    fail_restore_readback: bool,
}

#[allow(dead_code)]
#[derive(Clone, Copy)]
enum RebuildGuardFault {
    DisableSet,
    DisableReadback,
    Begin,
    CommitWhileActive,
    CommitAfterCommit,
    Rollback,
    RestoreSet,
    RestoreReadback,
}

/// Private transaction and foreign-key cleanup boundary for a later rebuild executor.
/// It deliberately has no public execution wiring.
#[allow(dead_code)]
struct RebuildTransactionGuard<'state> {
    state: &'state mut SqliteConnectionState,
    original_foreign_keys: i64,
    stage: RebuildGuardStage,
    certainty: CommitCertainty,
    failures: RebuildGuardFailures,
    finished: bool,
    #[cfg(test)]
    faults: RebuildGuardFaults,
}

#[allow(dead_code)]
impl<'state> RebuildTransactionGuard<'state> {
    fn begin(state: &'state mut SqliteConnectionState) -> Result<Self, DbError> {
        Self::begin_inner(
            state,
            #[cfg(test)]
            rebuild_guard_faults_for_current_thread(),
        )
    }

    #[cfg(test)]
    fn begin_with_faults(
        state: &'state mut SqliteConnectionState,
        faults: RebuildGuardFaults,
    ) -> Result<Self, DbError> {
        Self::begin_inner(state, faults)
    }

    fn begin_inner(
        state: &'state mut SqliteConnectionState,
        #[cfg(test)] faults: RebuildGuardFaults,
    ) -> Result<Self, DbError> {
        if !state.is_autocommit() {
            return Err(rebuild_guard_caller_owned_transaction_error());
        }
        let original_foreign_keys = read_foreign_keys(state)
            .map_err(|error| rebuild_guard_sqlite_error("save foreign_keys", &error))?;
        let disable = if Self::faulted(
            #[cfg(test)]
            &faults,
            RebuildGuardFault::DisableSet,
        ) {
            Err(rusqlite::Error::InvalidQuery)
        } else {
            state.execute_batch("PRAGMA foreign_keys = OFF")
        };
        if let Err(error) = disable {
            return Err(rebuild_guard_sqlite_error("disable foreign_keys", &error));
        }
        let disabled = if Self::faulted(
            #[cfg(test)]
            &faults,
            RebuildGuardFault::DisableReadback,
        ) {
            Err(rusqlite::Error::InvalidQuery)
        } else {
            read_foreign_keys(state)
        };
        if let Err(error) = disabled {
            return Err(rebuild_guard_setup_failure(
                state,
                original_foreign_keys,
                "verify disabled foreign_keys",
                error,
                #[cfg(test)]
                &faults,
            ));
        }
        if let Ok(value) = disabled
            && value != 0
        {
            return Err(rebuild_guard_setup_failure(
                state,
                original_foreign_keys,
                "verify disabled foreign_keys",
                rusqlite::Error::InvalidQuery,
                #[cfg(test)]
                &faults,
            ));
        }
        let begin = if Self::faulted(
            #[cfg(test)]
            &faults,
            RebuildGuardFault::Begin,
        ) {
            Err(rusqlite::Error::InvalidQuery)
        } else {
            state.execute_batch("BEGIN IMMEDIATE")
        };
        if let Err(error) = begin {
            return Err(rebuild_guard_setup_failure(
                state,
                original_foreign_keys,
                "begin immediate transaction",
                error,
                #[cfg(test)]
                &faults,
            ));
        }
        Ok(Self {
            state,
            original_foreign_keys,
            stage: RebuildGuardStage::Active,
            certainty: CommitCertainty::NotAttempted,
            failures: RebuildGuardFailures::default(),
            finished: false,
            #[cfg(test)]
            faults,
        })
    }

    #[cfg(test)]
    fn faulted(faults: &RebuildGuardFaults, fault: RebuildGuardFault) -> bool {
        match fault {
            RebuildGuardFault::DisableSet => faults.fail_disable_set,
            RebuildGuardFault::DisableReadback => faults.fail_disable_readback,
            RebuildGuardFault::Begin => faults.fail_begin,
            RebuildGuardFault::CommitWhileActive => faults.fail_commit_while_active,
            RebuildGuardFault::CommitAfterCommit => faults.fail_commit_after_commit,
            RebuildGuardFault::Rollback => faults.fail_rollback,
            RebuildGuardFault::RestoreSet => faults.fail_restore_set,
            RebuildGuardFault::RestoreReadback => faults.fail_restore_readback,
        }
    }

    #[cfg(not(test))]
    fn faulted(_fault: RebuildGuardFault) -> bool {
        false
    }

    fn connection(&self) -> &RusqliteConnection {
        self.state
    }

    fn finish<T>(mut self, result: Result<T, DbError>) -> Result<T, DbError> {
        let result = match result {
            Ok(value) => self.commit().map(|()| value),
            Err(primary) => {
                self.failures.primary = Some(primary);
                self.rollback_after_failure();
                Err(())
            }
        };
        self.restore_foreign_keys();
        self.finished = true;
        self.stage = RebuildGuardStage::Finished;

        match result {
            Ok(value)
                if self.certainty == CommitCertainty::ConfirmedCommitted
                    && !self.failures.has_cleanup_failure() =>
            {
                Ok(value)
            }
            Ok(_) | Err(()) => Err(self.finish_error()),
        }
    }

    fn commit(&mut self) -> Result<(), ()> {
        self.stage = RebuildGuardStage::Committing;
        let commit = if self.has_fault(
            #[cfg(test)]
            RebuildGuardFault::CommitWhileActive,
        ) {
            Err(rusqlite::Error::InvalidQuery)
        } else {
            self.state.execute_batch("COMMIT")
        };
        if let Err(error) = commit {
            self.failures.primary = Some(rebuild_guard_sqlite_error("commit", &error));
            if self.state.is_autocommit() {
                self.certainty = CommitCertainty::Uncertain;
            } else {
                self.rollback_after_failure();
            }
            return Err(());
        }
        if self.has_fault(
            #[cfg(test)]
            RebuildGuardFault::CommitAfterCommit,
        ) {
            self.failures.primary = Some(rebuild_guard_sqlite_error(
                "commit returned an injected transport error after SQLite completed COMMIT",
                &rusqlite::Error::InvalidQuery,
            ));
            self.certainty = CommitCertainty::Uncertain;
            return Err(());
        }
        if self.state.is_autocommit() {
            self.certainty = CommitCertainty::ConfirmedCommitted;
            Ok(())
        } else {
            self.certainty = CommitCertainty::Uncertain;
            Err(())
        }
    }

    fn rollback_after_failure(&mut self) {
        self.stage = RebuildGuardStage::RollingBack;
        if self.state.is_autocommit() {
            self.certainty = CommitCertainty::RolledBack;
            return;
        }
        let rollback = if self.has_fault(
            #[cfg(test)]
            RebuildGuardFault::Rollback,
        ) {
            Err(rusqlite::Error::InvalidQuery)
        } else {
            self.state.execute_batch("ROLLBACK")
        };
        match rollback {
            Ok(()) if self.state.is_autocommit() => {
                self.certainty = CommitCertainty::RolledBack;
            }
            Ok(()) => {
                self.failures.rollback =
                    Some("SQLite rollback did not restore autocommit".to_string());
                self.certainty = CommitCertainty::Uncertain;
            }
            Err(error) => {
                self.failures.rollback = Some(sqlite_error_detail(&error));
                self.certainty = if self.state.is_autocommit() {
                    CommitCertainty::RolledBack
                } else {
                    CommitCertainty::Uncertain
                };
            }
        }
    }

    fn restore_foreign_keys(&mut self) {
        self.stage = RebuildGuardStage::RestoringForeignKeys;
        if !self.state.is_autocommit() {
            self.failures.restoration = Some(
                "foreign_keys restoration was skipped because the transaction remains active"
                    .to_string(),
            );
            self.certainty = CommitCertainty::Uncertain;
            self.quarantine("transaction state is uncertain during foreign-key restoration");
            return;
        }
        let restore = if self.has_fault(
            #[cfg(test)]
            RebuildGuardFault::RestoreSet,
        ) {
            Err(rusqlite::Error::InvalidQuery)
        } else {
            self.state.execute_batch(&format!(
                "PRAGMA foreign_keys = {}",
                self.original_foreign_keys
            ))
        };
        if let Err(error) = restore {
            self.failures.restoration =
                Some(format!("set foreign_keys: {}", sqlite_error_detail(&error)));
            self.quarantine("foreign_keys restoration failed");
            return;
        }
        let restored = if self.has_fault(
            #[cfg(test)]
            RebuildGuardFault::RestoreReadback,
        ) {
            Err(rusqlite::Error::InvalidQuery)
        } else {
            read_foreign_keys(self.state)
        };
        match restored {
            Ok(value) if value == self.original_foreign_keys => {}
            Ok(value) => {
                self.failures.restoration = Some(format!(
                    "foreign_keys readback was {value}, expected {}",
                    self.original_foreign_keys
                ));
                self.quarantine("foreign_keys restoration readback disagreed");
            }
            Err(error) => {
                self.failures.restoration = Some(format!(
                    "read foreign_keys: {}",
                    sqlite_error_detail(&error)
                ));
                self.quarantine("foreign_keys restoration readback failed");
            }
        }
    }

    fn has_fault(&self, #[cfg(test)] fault: RebuildGuardFault) -> bool {
        #[cfg(test)]
        {
            Self::faulted(&self.faults, fault)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    fn quarantine(&mut self, reason: &str) {
        self.state.mark_unusable(reason);
    }

    fn finish_error(&mut self) -> DbError {
        if self.certainty == CommitCertainty::Uncertain || self.failures.restoration.is_some() {
            self.quarantine("rebuild transaction cleanup did not establish a reusable connection");
        }
        if !self.failures.has_cleanup_failure()
            && self.certainty == CommitCertainty::RolledBack
            && let Some(primary) = self.failures.primary.take()
        {
            return primary;
        }
        let mut formatted =
            FormattedError::new("SQLite rebuild transaction did not complete safely")
                .with_detail(self.failures.details(self.stage, self.certainty))
                .with_location(dbflux_core::ErrorLocation {
                    schema: Some("main".to_string()),
                    table: None,
                    column: None,
                    constraint: None,
                })
                .with_hint(
                    "Reconnect and inspect the database before retrying the table alteration",
                )
                .with_retriable(false);
        if let Some(code) = self
            .failures
            .primary
            .as_ref()
            .and_then(DbError::formatted)
            .and_then(|primary| primary.code.as_deref())
        {
            formatted = formatted.with_code(code);
        }
        DbError::QueryFailed(formatted)
    }
}

impl Drop for RebuildTransactionGuard<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        log::error!("SQLite rebuild transaction guard dropped without explicit finish");
        if !self.state.is_autocommit()
            && let Err(error) = self.state.execute_batch("ROLLBACK")
        {
            log::error!("SQLite rebuild transaction guard drop rollback failed: {error}");
        }
        if self.state.is_autocommit() {
            if let Err(error) = self.state.execute_batch(&format!(
                "PRAGMA foreign_keys = {}",
                self.original_foreign_keys
            )) {
                log::error!(
                    "SQLite rebuild transaction guard drop foreign-key restoration failed: {error}"
                );
            }
        } else {
            log::error!("SQLite rebuild transaction guard drop left an active transaction");
        }
        self.state
            .mark_unusable("rebuild transaction guard dropped without explicit completion");
    }
}

#[allow(dead_code)]
fn read_foreign_keys(connection: &RusqliteConnection) -> Result<i64, rusqlite::Error> {
    connection.query_row("PRAGMA foreign_keys", [], |row| row.get(0))
}

#[allow(dead_code)]
fn sqlite_error_detail(error: &rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(code, detail) => format!(
            "SQLite {:?} (extended code {}): {}",
            code.code,
            code.extended_code,
            detail.as_deref().unwrap_or("no SQLite detail")
        ),
        _ => error.to_string(),
    }
}

#[allow(dead_code)]
fn rebuild_guard_caller_owned_transaction_error() -> DbError {
    DbError::QueryFailed(
        FormattedError::new("SQLite rebuild transaction requires an autocommit connection")
            .with_detail(
                "caller-owned transaction is active; no foreign-key settings or transaction cleanup were attempted",
            )
            .with_location(dbflux_core::ErrorLocation {
                schema: Some("main".to_string()),
                table: None,
                column: None,
                constraint: None,
            })
            .with_hint("Finish or roll back the caller transaction before retrying")
            .with_retriable(false),
    )
}

#[allow(dead_code)]
fn rebuild_guard_sqlite_error(action: &str, error: &rusqlite::Error) -> DbError {
    let mut formatted = FormattedError::new("SQLite rebuild transaction setup failed")
        .with_detail(format!("{action}: {}", sqlite_error_detail(error)))
        .with_location(dbflux_core::ErrorLocation {
            schema: Some("main".to_string()),
            table: None,
            column: None,
            constraint: None,
        })
        .with_hint("Inspect the SQLite connection state before retrying")
        .with_retriable(false);
    if let rusqlite::Error::SqliteFailure(code, _) = error {
        formatted = formatted.with_code(format!("{}", code.extended_code));
    }
    DbError::QueryFailed(formatted)
}

#[allow(dead_code)]
fn rebuild_guard_setup_failure(
    state: &mut SqliteConnectionState,
    original_foreign_keys: i64,
    action: &str,
    error: rusqlite::Error,
    #[cfg(test)] faults: &RebuildGuardFaults,
) -> DbError {
    let mut details = vec![format!("{action}: {}", sqlite_error_detail(&error))];
    if !state.is_autocommit() {
        match state.execute_batch("ROLLBACK") {
            Ok(()) if state.is_autocommit() => {}
            Ok(()) => {
                details.push("setup rollback did not restore autocommit".to_string());
                state.mark_unusable("setup cleanup left an active transaction");
            }
            Err(rollback_error) => {
                details.push(format!(
                    "setup rollback: {}",
                    sqlite_error_detail(&rollback_error)
                ));
                if !state.is_autocommit() {
                    state.mark_unusable("setup cleanup rollback failed");
                }
            }
        }
    }
    #[cfg(test)]
    let restore_faulted = RebuildTransactionGuard::faulted(faults, RebuildGuardFault::RestoreSet);
    #[cfg(not(test))]
    let restore_faulted = false;
    let restore = if !state.is_autocommit() || restore_faulted {
        Err(rusqlite::Error::InvalidQuery)
    } else {
        state.execute_batch(&format!("PRAGMA foreign_keys = {original_foreign_keys}"))
    };
    if let Err(restore_error) = restore {
        details.push(format!(
            "foreign_keys restoration: {}",
            sqlite_error_detail(&restore_error)
        ));
        state.mark_unusable("setup cleanup could not restore foreign_keys");
    } else {
        let readback = read_foreign_keys(state);
        match readback {
            Ok(value) if value == original_foreign_keys => {}
            Ok(value) => {
                details.push(format!(
                    "foreign_keys restoration readback was {value}, expected {original_foreign_keys}"
                ));
                state.mark_unusable("setup cleanup foreign_keys readback disagreed");
            }
            Err(readback_error) => {
                details.push(format!(
                    "foreign_keys restoration readback: {}",
                    sqlite_error_detail(&readback_error)
                ));
                state.mark_unusable("setup cleanup could not read foreign_keys");
            }
        }
    }
    let mut formatted = FormattedError::new("SQLite rebuild transaction setup failed")
        .with_detail(details.join("; "))
        .with_location(dbflux_core::ErrorLocation {
            schema: Some("main".to_string()),
            table: None,
            column: None,
            constraint: None,
        })
        .with_hint("Reconnect and inspect the SQLite connection state before retrying")
        .with_retriable(false);
    if let rusqlite::Error::SqliteFailure(code, _) = error {
        formatted = formatted.with_code(code.extended_code.to_string());
    }
    DbError::QueryFailed(formatted)
}

#[derive(Debug, Clone)]
struct RebuildExpectedFacts {
    selected_changes: RebuildChanges,
    retained_columns: Vec<String>,
    identity: String,
    explicit_index_sql: Vec<String>,
    foreign_key_intent: Vec<ResolvedForeignKeyRelationship>,
}

#[derive(Debug, Clone)]
struct RebuildCapture {
    observation: RebuildObservation,
    prepared_create_sql: String,
    final_expected_facts: RebuildExpectedFacts,
    replacement: String,
    copy: RebuildCopyIntent,
}

struct RebuildPlan {
    state: Arc<Mutex<SqliteConnectionState>>,
    cancelled: Arc<AtomicBool>,
    capture: RebuildCapture,
    preview: TableAlterPreview,
}

impl PreparedTableAlter for RebuildPlan {
    fn preview(&self) -> &TableAlterPreview {
        &self.preview
    }

    fn execute(self: Box<Self>) -> Result<TableAlterOutcome, DbError> {
        self.execute_guarded()
    }
}

impl RebuildPlan {
    fn execute_guarded(self: Box<Self>) -> Result<TableAlterOutcome, DbError> {
        let mut state = SqliteConnectionState::lock_checked(&self.state)?;
        if !state.is_autocommit() {
            return Err(rebuild_guard_caller_owned_transaction_error());
        }
        let settings = capture_native_connection_settings(&state)?;
        if settings != self.capture.observation.settings {
            return Err(DbError::NotSupported(
                "SQLite rebuild plan connection settings changed; refresh the preview".to_string(),
            ));
        }
        revalidate_rebuild_plan(&state, &self.capture)?;

        // This operation exclusively owns the shared connection from this point until cleanup.
        self.cancelled.store(false, Ordering::SeqCst);
        #[cfg(test)]
        if let Some((_, hook)) = BEFORE_REBUILD_WRITE_LOCK
            .lock()
            .expect("rebuild write-lock test hook mutex should not be poisoned")
            .take_if(|(thread_id, _)| *thread_id == std::thread::current().id())
        {
            hook();
        }
        let guard = RebuildTransactionGuard::begin(&mut state)?;
        let result = execute_rebuild(&guard, &self.capture, &self.cancelled);
        guard.finish(result)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RebuildExecutionStage {
    Create,
    Copy,
    Compare,
    Drop,
    Rename,
    Index,
    FinalValidation,
    Precommit,
}

fn execute_rebuild(
    guard: &RebuildTransactionGuard<'_>,
    capture: &RebuildCapture,
    cancelled: &AtomicBool,
) -> Result<TableAlterOutcome, DbError> {
    let connection = guard.connection();
    check_rebuild_cancelled(cancelled, "before rebuild validation")?;
    revalidate_rebuild_plan(connection, capture)?;
    check_rebuild_cancelled(cancelled, "before replacement creation")?;
    connection
        .execute_batch(&capture.prepared_create_sql)
        .map_err(|error| rebuild_sqlite_execution_error("create replacement table", &error))?;
    inject_rebuild_execution_fault(connection, RebuildExecutionStage::Create)?;
    check_rebuild_cancelled(cancelled, "before data copy")?;

    let table = rebuild_table_name(capture)?;
    let projection = capture.copy.quoted_projection();
    connection
        .execute_batch(&format!(
            "INSERT INTO main.{} ({projection}) SELECT {projection} FROM main.{}",
            quote_identifier(&capture.replacement),
            quote_identifier(table),
        ))
        .map_err(|error| rebuild_sqlite_execution_error("copy retained rows", &error))?;
    inject_rebuild_execution_fault(connection, RebuildExecutionStage::Copy)?;
    compare_rebuild_rows(connection, capture, cancelled)?;
    inject_rebuild_execution_fault(connection, RebuildExecutionStage::Compare)?;
    check_rebuild_cancelled(cancelled, "before source drop")?;
    connection
        .execute_batch(&format!("DROP TABLE main.{}", quote_identifier(table)))
        .map_err(|error| rebuild_sqlite_execution_error("drop source table", &error))?;
    inject_rebuild_execution_fault(connection, RebuildExecutionStage::Drop)?;
    check_rebuild_cancelled(cancelled, "before replacement rename")?;
    connection
        .execute_batch(&format!(
            "ALTER TABLE main.{} RENAME TO {}",
            quote_identifier(&capture.replacement),
            quote_identifier(table),
        ))
        .map_err(|error| rebuild_sqlite_execution_error("rename replacement table", &error))?;
    inject_rebuild_execution_fault(connection, RebuildExecutionStage::Rename)?;
    for index_sql in &capture.final_expected_facts.explicit_index_sql {
        check_rebuild_cancelled(cancelled, "before index restoration")?;
        connection
            .execute_batch(index_sql)
            .map_err(|error| rebuild_sqlite_execution_error("restore explicit index", &error))?;
        inject_rebuild_execution_fault(connection, RebuildExecutionStage::Index)?;
    }
    validate_rebuild_final_state(connection, capture)?;
    inject_rebuild_execution_fault(connection, RebuildExecutionStage::FinalValidation)?;
    check_rebuild_cancelled(cancelled, "immediately before commit")?;
    inject_rebuild_execution_fault(connection, RebuildExecutionStage::Precommit)?;
    Ok(TableAlterOutcome {
        statement_count: 4 + capture.final_expected_facts.explicit_index_sql.len(),
        table_atomic: true,
    })
}

fn inject_rebuild_execution_fault(
    connection: &RusqliteConnection,
    stage: RebuildExecutionStage,
) -> Result<(), DbError> {
    #[cfg(not(test))]
    let _ = (connection, stage);
    #[cfg(test)]
    {
        let mut fault = REBUILD_EXECUTION_FAULT
            .lock()
            .expect("rebuild execution fault mutex should not be poisoned");
        if fault.as_ref().is_some_and(|(thread_id, selected)| {
            *thread_id == std::thread::current().id() && *selected == stage
        }) {
            *fault = None;
            return Err(rebuild_execution_error(&format!(
                "injected rebuild execution fault at {stage:?}"
            )));
        }
    }
    #[cfg(test)]
    run_rebuild_stage_hook(connection, stage)?;
    Ok(())
}

fn rebuild_table_name(capture: &RebuildCapture) -> Result<&str, DbError> {
    capture
        .observation
        .catalog
        .iter()
        .find(|entry| {
            entry.object_type == "table"
                && entry.sql.as_deref() == Some(&capture.observation.source_sql)
        })
        .map(|entry| entry.name.as_str())
        .ok_or_else(|| {
            rebuild_execution_error("prepared rebuild source table is no longer identifiable")
        })
}

fn revalidate_rebuild_plan(
    connection: &RusqliteConnection,
    capture: &RebuildCapture,
) -> Result<(), DbError> {
    let mut current = capture_rebuild_observation(connection, rebuild_table_name(capture)?)?;
    // The guard is the sole owner of this intentional connection-setting transition.
    current.settings.foreign_keys = capture.observation.settings.foreign_keys;
    if current != capture.observation {
        return Err(DbError::NotSupported(
            "SQLite rebuild plan is stale or its private replacement name now collides; refresh the preview"
                .to_string(),
        ));
    }
    Ok(())
}

fn check_rebuild_cancelled(cancelled: &AtomicBool, _stage: &str) -> Result<(), DbError> {
    if cancelled.load(Ordering::SeqCst) {
        return Err(DbError::Cancelled);
    }
    Ok(())
}

fn compare_rebuild_rows(
    connection: &RusqliteConnection,
    capture: &RebuildCapture,
    cancelled: &AtomicBool,
) -> Result<(), DbError> {
    let table = rebuild_table_name(capture)?;
    let projection = capture.copy.quoted_projection();
    let source_sql = format!(
        "SELECT {projection} FROM main.{} ORDER BY {}",
        quote_identifier(table),
        quote_identifier(&capture.copy.identity),
    );
    let replacement_sql = format!(
        "SELECT {projection} FROM main.{} ORDER BY {}",
        quote_identifier(&capture.replacement),
        quote_identifier(&capture.copy.identity),
    );
    compare_exact_row_streams(
        connection,
        &source_sql,
        &replacement_sql,
        capture.copy.source_projection.len(),
        cancelled,
    )
}

fn compare_exact_row_streams(
    connection: &RusqliteConnection,
    source_sql: &str,
    replacement_sql: &str,
    column_count: usize,
    cancelled: &AtomicBool,
) -> Result<(), DbError> {
    let mut source_statement = connection
        .prepare(source_sql)
        .map_err(|error| rebuild_sqlite_execution_error("prepare source comparison", &error))?;
    let mut replacement_statement = match connection.prepare(replacement_sql) {
        Ok(statement) => statement,
        Err(error) => {
            let primary = Err(rebuild_sqlite_execution_error(
                "prepare replacement comparison",
                &error,
            ));
            let source_finalization = finalize_comparison_statement(source_statement, true);
            return finish_comparison_result(primary, source_finalization, Ok(()));
        }
    };
    let primary = (|| -> Result<(), DbError> {
        let mut source_rows = source_statement
            .query([])
            .map_err(|error| rebuild_sqlite_execution_error("read source comparison", &error))?;
        let mut replacement_rows = replacement_statement.query([]).map_err(|error| {
            rebuild_sqlite_execution_error("read replacement comparison", &error)
        })?;
        let mut row_number = 0_u64;
        loop {
            check_rebuild_cancelled(cancelled, "while comparing copied rows")?;
            let source = source_rows.next().map_err(|error| {
                rebuild_sqlite_execution_error("advance source comparison", &error)
            })?;
            let replacement = replacement_rows.next().map_err(|error| {
                rebuild_sqlite_execution_error("advance replacement comparison", &error)
            })?;
            match (source, replacement) {
                (None, None) => return Ok(()),
                (Some(_), None) | (None, Some(_)) => {
                    return Err(rebuild_execution_error(
                        "SQLite rebuild row count changed during exact copy comparison",
                    ));
                }
                (Some(source), Some(replacement)) => {
                    for column in 0..column_count {
                        let source = source.get_ref(column).map_err(|error| {
                            rebuild_sqlite_execution_error("read source value", &error)
                        })?;
                        let replacement = replacement.get_ref(column).map_err(|error| {
                            rebuild_sqlite_execution_error("read replacement value", &error)
                        })?;
                        if !sqlite_value_refs_equal(source, replacement) {
                            return Err(rebuild_execution_error(&format!(
                                "SQLite rebuild exact copy comparison differed at row {row_number}, column {column}"
                            )));
                        }
                    }
                    row_number += 1;
                    #[cfg(test)]
                    cancel_comparison_after_row(row_number, cancelled);
                }
            }
        }
    })();
    let source_finalization = finalize_comparison_statement(source_statement, true);
    let replacement_finalization = finalize_comparison_statement(replacement_statement, false);
    finish_comparison_result(primary, source_finalization, replacement_finalization)
}

fn finalize_comparison_statement(
    statement: rusqlite::Statement<'_>,
    _source: bool,
) -> Result<(), rusqlite::Error> {
    let result = statement.finalize();
    #[cfg(test)]
    if comparison_finalize_fault(_source) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    result
}

#[cfg(test)]
fn cancel_comparison_after_row(row_number: u64, cancelled: &AtomicBool) {
    if row_number != 1 {
        return;
    }
    let mut hook = COMPARISON_CANCEL_AFTER_ROW
        .lock()
        .expect("comparison cancellation hook mutex should not be poisoned");
    if hook
        .as_ref()
        .is_some_and(|(thread_id, _)| *thread_id == std::thread::current().id())
    {
        if let Some((_, cancellation)) = hook.take() {
            cancellation.store(true, Ordering::SeqCst);
        }
    }
    let _cancelled = cancelled;
}

#[cfg(test)]
fn comparison_finalize_fault(source: bool) -> bool {
    let mut guard = COMPARISON_FINALIZE_FAULTS
        .lock()
        .expect("comparison finalization fault hook mutex should not be poisoned");
    let Some((thread_id, faults)) = guard.as_mut() else {
        return false;
    };
    if *thread_id != std::thread::current().id() {
        return false;
    }
    let failed = if source {
        std::mem::take(&mut faults.source)
    } else {
        std::mem::take(&mut faults.replacement)
    };
    if !faults.source && !faults.replacement {
        *guard = None;
    }
    failed
}

fn finish_comparison_result(
    primary: Result<(), DbError>,
    source_finalization: Result<(), rusqlite::Error>,
    replacement_finalization: Result<(), rusqlite::Error>,
) -> Result<(), DbError> {
    if source_finalization.is_ok() && replacement_finalization.is_ok() {
        return primary;
    }
    let mut details = Vec::new();
    if let Err(primary) = &primary {
        details.push(format!("primary comparison failure: {primary}"));
    }
    if let Err(error) = &source_finalization {
        details.push(format!(
            "source statement finalization: {}",
            sqlite_error_detail(error)
        ));
    }
    if let Err(error) = &replacement_finalization {
        details.push(format!(
            "replacement statement finalization: {}",
            sqlite_error_detail(error)
        ));
    }
    let mut formatted = FormattedError::new("SQLite rebuild comparison cleanup failed")
        .with_detail(details.join("; "))
        .with_location(dbflux_core::ErrorLocation {
            schema: Some("main".to_string()),
            table: None,
            column: None,
            constraint: None,
        })
        .with_retriable(false);
    if let Some(code) = primary
        .as_ref()
        .err()
        .and_then(DbError::formatted)
        .and_then(|formatted| formatted.code.as_deref())
    {
        formatted = formatted.with_code(code);
    } else if let Some(code) = source_finalization
        .as_ref()
        .err()
        .and_then(sqlite_extended_code)
        .or_else(|| {
            replacement_finalization
                .as_ref()
                .err()
                .and_then(sqlite_extended_code)
        })
    {
        formatted = formatted.with_code(code);
    }
    Err(DbError::QueryFailed(formatted))
}

fn sqlite_extended_code(error: &rusqlite::Error) -> Option<String> {
    match error {
        rusqlite::Error::SqliteFailure(code, _) => Some(code.extended_code.to_string()),
        _ => None,
    }
}

fn sqlite_value_refs_equal(left: ValueRef<'_>, right: ValueRef<'_>) -> bool {
    match (left, right) {
        (ValueRef::Null, ValueRef::Null) => true,
        (ValueRef::Integer(left), ValueRef::Integer(right)) => left == right,
        (ValueRef::Real(left), ValueRef::Real(right)) => left.to_bits() == right.to_bits(),
        (ValueRef::Text(left), ValueRef::Text(right)) => left == right,
        (ValueRef::Blob(left), ValueRef::Blob(right)) => left == right,
        _ => false,
    }
}

fn validate_single_integrity_text_row(
    connection: &RusqliteConnection,
    sql: &str,
) -> Result<(), DbError> {
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| rebuild_sqlite_execution_error("prepare integrity_check", &error))?;
    let primary = (|| -> Result<(), DbError> {
        if statement.column_count() != 1 {
            return Err(rebuild_execution_error(
                "SQLite rebuild integrity_check must return exactly one column",
            ));
        }
        let mut rows = statement
            .query([])
            .map_err(|error| rebuild_sqlite_execution_error("run integrity_check", &error))?;
        let row = rows
            .next()
            .map_err(|error| rebuild_sqlite_execution_error("read integrity_check", &error))?
            .ok_or_else(|| {
                rebuild_execution_error("SQLite rebuild integrity_check returned no rows")
            })?;
        if !matches!(
            row.get_ref(0)
                .map_err(|error| rebuild_sqlite_execution_error("read integrity_check", &error))?,
            ValueRef::Text(value) if value == b"ok"
        ) {
            return Err(rebuild_execution_error(
                "SQLite rebuild integrity_check did not return exactly raw TEXT ok",
            ));
        }
        if rows
            .next()
            .map_err(|error| rebuild_sqlite_execution_error("read integrity_check", &error))?
            .is_some()
        {
            return Err(rebuild_execution_error(
                "SQLite rebuild integrity_check returned more than one row",
            ));
        }
        Ok(())
    })();
    finish_integrity_result(primary, finalize_integrity_statement(statement))
}

fn finalize_integrity_statement(statement: rusqlite::Statement<'_>) -> Result<(), rusqlite::Error> {
    let result = statement.finalize();
    #[cfg(test)]
    if let Some(code) = INTEGRITY_FINALIZE_FAULT
        .lock()
        .expect("integrity finalization fault mutex should not be poisoned")
        .take_if(|(thread_id, _)| *thread_id == std::thread::current().id())
        .map(|(_, code)| code)
    {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(code),
            Some("injected integrity finalization failure".to_string()),
        ));
    }
    result
}

fn finish_integrity_result(
    primary: Result<(), DbError>,
    finalization: Result<(), rusqlite::Error>,
) -> Result<(), DbError> {
    match (primary, finalization) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(error)) => Err(rebuild_sqlite_execution_error(
            "finalize integrity_check",
            &error,
        )),
        (Err(primary), Ok(())) => Err(primary),
        (Err(primary), Err(error)) => {
            let mut formatted =
                FormattedError::new("SQLite rebuild integrity_check cleanup failed")
                    .with_detail(format!(
                        "primary integrity_check failure: {primary}; statement finalization: {}",
                        sqlite_error_detail(&error)
                    ))
                    .with_location(dbflux_core::ErrorLocation {
                        schema: Some("main".to_string()),
                        table: None,
                        column: None,
                        constraint: None,
                    })
                    .with_retriable(false);
            if let Some(code) = primary
                .formatted()
                .and_then(|formatted| formatted.code.as_deref())
            {
                formatted = formatted.with_code(code);
            } else if let Some(code) = sqlite_extended_code(&error) {
                formatted = formatted.with_code(code);
            }
            Err(DbError::QueryFailed(formatted))
        }
    }
}

fn validate_final_index_capture(
    expected: &[IndexCapture],
    actual: &[IndexCapture],
) -> Result<(), DbError> {
    if actual.len() < expected.len() {
        return Err(rebuild_execution_error(
            "SQLite rebuild expected index is absent from the final observation",
        ));
    }
    if actual.len() > expected.len() {
        return Err(rebuild_execution_error(
            "SQLite rebuild final observation contains an extra index",
        ));
    }
    for (expected, actual) in expected.iter().zip(actual) {
        if expected.name != actual.name {
            return Err(rebuild_execution_error(
                "SQLite rebuild index name or order differs from the prepared expectation",
            ));
        }
        if expected.unique != actual.unique {
            return Err(rebuild_execution_error(
                "SQLite rebuild index uniqueness differs from the prepared expectation",
            ));
        }
        if expected.origin != actual.origin {
            return Err(rebuild_execution_error(
                "SQLite rebuild index origin differs from the prepared expectation",
            ));
        }
        if expected.sql != actual.sql {
            return Err(rebuild_execution_error(
                "SQLite rebuild index SQL differs from the prepared expectation",
            ));
        }
        if expected.keys.len() != actual.keys.len() {
            return Err(rebuild_execution_error(
                "SQLite rebuild index key membership differs from the prepared expectation",
            ));
        }
        for (expected_key, actual_key) in expected.keys.iter().zip(&actual.keys) {
            if expected_key.sequence != actual_key.sequence {
                return Err(rebuild_execution_error(
                    "SQLite rebuild index key order differs from the prepared expectation",
                ));
            }
            if expected_key.column != actual_key.column {
                return Err(rebuild_execution_error(
                    "SQLite rebuild index key column differs from the prepared expectation",
                ));
            }
            if expected_key.descending != actual_key.descending {
                return Err(rebuild_execution_error(
                    "SQLite rebuild index key direction differs from the prepared expectation",
                ));
            }
            if expected_key.collation != actual_key.collation {
                return Err(rebuild_execution_error(
                    "SQLite rebuild index key collation differs from the prepared expectation",
                ));
            }
        }
    }
    Ok(())
}

fn validate_final_connection_settings(
    expected: &NativeConnectionSettings,
    actual: &NativeConnectionSettings,
) -> Result<(), DbError> {
    for (name, expected, actual) in [
        ("foreign_keys", 0, actual.foreign_keys),
        (
            "writable_schema",
            expected.writable_schema,
            actual.writable_schema,
        ),
        (
            "legacy_alter_table",
            expected.legacy_alter_table,
            actual.legacy_alter_table,
        ),
        (
            "ignore_check_constraints",
            expected.ignore_check_constraints,
            actual.ignore_check_constraints,
        ),
        (
            "defer_foreign_keys",
            expected.defer_foreign_keys,
            actual.defer_foreign_keys,
        ),
    ] {
        if expected != actual {
            return Err(rebuild_execution_error(&format!(
                "SQLite rebuild changed protected connection setting {name}",
            )));
        }
    }
    Ok(())
}

fn validate_prepared_final_facts(
    table: &str,
    capture: &RebuildCapture,
    final_observation: &RebuildObservation,
    final_table: &CreateTable,
) -> Result<(), DbError> {
    let actual_retained = final_table
        .column_names()
        .filter(|column| !column.eq_ignore_ascii_case(&capture.final_expected_facts.identity))
        .map(str::to_string)
        .collect::<Vec<_>>();
    if actual_retained != capture.final_expected_facts.retained_columns {
        return Err(rebuild_execution_error(
            "SQLite rebuild retained column names or order differ from the prepared expectation",
        ));
    }
    let metadata = final_observation
        .table_metadata
        .iter()
        .find(|metadata| metadata.name.eq_ignore_ascii_case(table))
        .ok_or_else(|| {
            rebuild_execution_error("SQLite rebuild cannot find final table metadata")
        })?;
    let identity = rebuild_identity(
        table,
        &final_observation.columns,
        final_table,
        &metadata.rowid_aliases,
    )?;
    if identity != capture.final_expected_facts.identity {
        return Err(rebuild_execution_error(
            "SQLite rebuild identity differs from the prepared expectation",
        ));
    }
    validate_final_index_capture(&capture.observation.indexes, &final_observation.indexes)?;
    validate_rebuild_index_metadata(
        table,
        final_table,
        &final_observation.columns,
        &final_observation.indexes,
    )?;
    validate_final_connection_settings(&capture.observation.settings, &final_observation.settings)
}

fn validate_rebuild_final_observation(
    capture: &RebuildCapture,
    final_observation: &RebuildObservation,
) -> Result<(), DbError> {
    let table = rebuild_table_name(capture)?;
    if final_observation.foreign_key_violation.is_some() {
        return Err(rebuild_execution_error(
            "SQLite rebuild foreign_key_check found a violation",
        ));
    }
    let source = parse_create_table(&capture.observation.source_sql).map_err(|reason| {
        rebuild_execution_error(&format!("could not parse prepared source SQL: {reason}"))
    })?;
    let final_table = parse_create_table(&final_observation.source_sql).map_err(|reason| {
        rebuild_execution_error(&format!("could not parse final replacement SQL: {reason}"))
    })?;
    validate_rebuild_semantic_delta(
        table,
        &source,
        &final_table,
        &capture.final_expected_facts.selected_changes,
    )?;
    validate_parsed_table_metadata(table, &final_table, &final_observation.columns)?;
    validate_prepared_final_facts(table, capture, final_observation, &final_table)?;
    let final_indexes = final_observation
        .indexes
        .iter()
        .filter_map(|index| index.sql.clone())
        .collect::<Vec<_>>();
    if final_indexes != capture.final_expected_facts.explicit_index_sql {
        return Err(rebuild_execution_error(
            "SQLite rebuild did not restore the prepared explicit indexes",
        ));
    }
    let relationships = resolve_foreign_key_relationships(
        table,
        &final_observation.table_metadata,
        &final_observation.foreign_key_metadata,
    )?;
    if relationships != capture.final_expected_facts.foreign_key_intent {
        return Err(rebuild_execution_error(
            "SQLite rebuild foreign-key metadata differs from the prepared intent",
        ));
    }
    if final_observation.catalog.iter().any(|entry| {
        entry.name.eq_ignore_ascii_case(&capture.replacement)
            || entry.table_name.eq_ignore_ascii_case(&capture.replacement)
    }) {
        return Err(rebuild_execution_error(
            "SQLite rebuild leaked its private replacement object",
        ));
    }
    let unaffected_before = capture.observation.catalog.iter().filter(|entry| {
        !entry.name.eq_ignore_ascii_case(table) && !entry.table_name.eq_ignore_ascii_case(table)
    });
    let unaffected_after = final_observation.catalog.iter().filter(|entry| {
        !entry.name.eq_ignore_ascii_case(table) && !entry.table_name.eq_ignore_ascii_case(table)
    });
    if !unaffected_before.eq(unaffected_after) {
        return Err(rebuild_execution_error(
            "SQLite rebuild changed non-target main catalog metadata",
        ));
    }
    if final_observation.temp_catalog != capture.observation.temp_catalog {
        return Err(rebuild_execution_error(
            "SQLite rebuild changed non-target temp catalog metadata",
        ));
    }
    if final_observation.databases != capture.observation.databases {
        return Err(rebuild_execution_error(
            "SQLite rebuild changed database list metadata",
        ));
    }
    Ok(())
}

fn validate_rebuild_final_state(
    connection: &RusqliteConnection,
    capture: &RebuildCapture,
) -> Result<(), DbError> {
    let table = rebuild_table_name(capture)?;
    let final_observation = capture_rebuild_observation_once(connection, table)?;
    validate_rebuild_final_observation(capture, &final_observation)?;
    validate_single_integrity_text_row(connection, "PRAGMA main.integrity_check")
}

fn rebuild_execution_error(message: &str) -> DbError {
    DbError::QueryFailed(
        FormattedError::new("SQLite rebuild execution failed")
            .with_detail(message)
            .with_location(dbflux_core::ErrorLocation {
                schema: Some("main".to_string()),
                table: None,
                column: None,
                constraint: None,
            })
            .with_retriable(false),
    )
}

fn rebuild_sqlite_execution_error(action: &str, error: &rusqlite::Error) -> DbError {
    if matches!(
        error,
        rusqlite::Error::SqliteFailure(code, _)
            if code.code == rusqlite::ErrorCode::OperationInterrupted
    ) {
        return DbError::Cancelled;
    }
    let mut formatted = FormattedError::new("SQLite rebuild execution failed")
        .with_detail(format!("{action}: {}", sqlite_error_detail(error)))
        .with_location(dbflux_core::ErrorLocation {
            schema: Some("main".to_string()),
            table: None,
            column: None,
            constraint: None,
        })
        .with_retriable(false);
    if let rusqlite::Error::SqliteFailure(code, _) = error {
        formatted = formatted.with_code(code.extended_code.to_string());
    }
    DbError::QueryFailed(formatted)
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct NativeConnectionSettings {
    foreign_keys: i64,
    writable_schema: i64,
    legacy_alter_table: i64,
    ignore_check_constraints: i64,
    defer_foreign_keys: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogEntry {
    object_type: String,
    name: String,
    table_name: String,
    sql: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

fn capture_database_identities(
    connection: &RusqliteConnection,
) -> Result<Vec<DatabaseIdentity>, DbError> {
    let mut statement = connection
        .prepare("PRAGMA database_list")
        .map_err(|error| {
            DbError::query_failed(format!("could not list SQLite schemas: {error}"))
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok(DatabaseIdentity {
                sequence: row.get(0)?,
                name: row.get(1)?,
                file_name: row.get(2)?,
            })
        })
        .map_err(|error| {
            DbError::query_failed(format!("could not read SQLite schemas: {error}"))
        })?;
    let mut databases = rows.collect::<Result<Vec<_>, _>>().map_err(|error| {
        DbError::query_failed(format!("could not decode SQLite schemas: {error}"))
    })?;
    databases.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(databases)
}

fn capture_temp_catalog(connection: &RusqliteConnection) -> Result<Vec<CatalogEntry>, DbError> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, sql FROM temp.sqlite_temp_master \
             WHERE substr(lower(name), 1, 7) <> 'sqlite_' ORDER BY type, name, tbl_name",
        )
        .map_err(|error| {
            DbError::query_failed(format!("could not inspect temp catalog: {error}"))
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
        .map_err(|error| DbError::query_failed(format!("could not read temp catalog: {error}")))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| DbError::query_failed(format!("could not decode temp catalog: {error}")))
}

fn capture_table_metadata(
    connection: &RusqliteConnection,
    catalog: &[CatalogEntry],
) -> Result<Vec<TableMetadataCapture>, DbError> {
    let mut tables = catalog
        .iter()
        .filter(|entry| entry.object_type == "table")
        .map(|entry| {
            Ok(TableMetadataCapture {
                name: entry.name.clone(),
                columns: table_columns(connection, &entry.name)?,
                rowid_aliases: capture_rowid_aliases(connection, &entry.name),
            })
        })
        .collect::<Result<Vec<_>, DbError>>()?;
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(tables)
}

fn capture_rowid_aliases(connection: &RusqliteConnection, table: &str) -> Vec<String> {
    ["rowid", "_rowid_", "oid"]
        .into_iter()
        .filter(|alias| {
            connection
                .prepare(&format!(
                    "SELECT {alias} FROM main.{} LIMIT 0",
                    quote_identifier(table)
                ))
                .is_ok()
        })
        .map(str::to_string)
        .collect()
}

fn capture_rebuild_schema_versions(
    connection: &RusqliteConnection,
    databases: &[DatabaseIdentity],
) -> Result<Vec<SchemaVersion>, DbError> {
    let mut versions = databases
        .iter()
        .filter(|database| !database.name.eq_ignore_ascii_case("temp"))
        .map(|database| {
            connection
                .query_row(
                    &format!("PRAGMA {}.schema_version", quote_identifier(&database.name)),
                    [],
                    |row| row.get(0),
                )
                .map(|version| SchemaVersion {
                    schema: database.name.clone(),
                    version,
                })
                .map_err(|error| {
                    DbError::query_failed(format!("could not read SQLite schema version: {error}"))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    versions.sort_by(|left, right| left.schema.cmp(&right.schema));
    Ok(versions)
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

fn reject_rebuild_dependencies(
    table: &str,
    selected_columns: &[String],
    columns: &[TableColumnCapture],
    indexes: &[IndexCapture],
    foreign_keys: &[ResolvedForeignKeyRelationship],
) -> Result<(), DbError> {
    for column in selected_columns {
        if columns.iter().any(|candidate| {
            candidate.primary_key_order != 0 && candidate.name.eq_ignore_ascii_case(column)
        }) {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild found known dependency: primary key main.{table}.{column}"
            )));
        }
        if let Some(index) = indexes.iter().find(|index| {
            index
                .keys
                .iter()
                .any(|key| key.column.eq_ignore_ascii_case(column))
        }) {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild found known dependency: index main.{} uses selected column {column}",
                index.name
            )));
        }
        if let Some(relationship) = foreign_keys.iter().find(|relationship| {
            (relationship.child_table.eq_ignore_ascii_case(table)
                && relationship
                    .child_columns
                    .iter()
                    .any(|child_column| child_column.eq_ignore_ascii_case(column)))
                || (relationship.parent_table.eq_ignore_ascii_case(table)
                    && relationship
                        .parent_columns
                        .iter()
                        .any(|parent_column| parent_column.eq_ignore_ascii_case(column)))
        }) {
            let dependency = if relationship.child_table.eq_ignore_ascii_case(table) {
                format!("from main.{}.{column}", relationship.child_table)
            } else {
                format!(
                    "from main.{} references main.{}.{column}",
                    relationship.child_table, relationship.parent_table
                )
            };
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild found known dependency: foreign key {dependency}"
            )));
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn reject_rebuild_dependencies_legacy(
    table: &str,
    selected_columns: &[String],
    columns: &[TableColumnCapture],
    indexes: &[IndexCapture],
    foreign_keys: &[(String, String, String, Option<String>)],
    table_metadata: &[TableMetadataCapture],
) -> Result<(), DbError> {
    for column in selected_columns {
        if columns.iter().any(|candidate| {
            candidate.primary_key_order != 0 && candidate.name.eq_ignore_ascii_case(column)
        }) {
            return Err(DbError::NotSupported(format!(
                "SQLite native DROP found known dependency: primary key main.{table}.{column}"
            )));
        }
        if let Some(index) = indexes.iter().find(|index| {
            index
                .keys
                .iter()
                .any(|key| key.column.eq_ignore_ascii_case(column))
        }) {
            return Err(DbError::NotSupported(format!(
                "SQLite native DROP found known dependency: index main.{} uses selected column {column}",
                index.name
            )));
        }
        for (child_table, parent_table, child_column, parent_column) in foreign_keys {
            if child_table.eq_ignore_ascii_case(table) && child_column.eq_ignore_ascii_case(column)
            {
                return Err(DbError::NotSupported(format!(
                    "SQLite native DROP found known dependency: foreign key from main.{table}.{child_column}"
                )));
            }
            if parent_table.eq_ignore_ascii_case(table)
                && (parent_column
                    .as_deref()
                    .is_some_and(|parent_column| parent_column.eq_ignore_ascii_case(column))
                    || (parent_column.is_none()
                        && table_metadata
                            .iter()
                            .find(|metadata| metadata.name.eq_ignore_ascii_case(table))
                            .is_some_and(|metadata| {
                                metadata.columns.iter().any(|candidate| {
                                    candidate.primary_key_order != 0
                                        && candidate.name.eq_ignore_ascii_case(column)
                                })
                            })))
            {
                return Err(DbError::NotSupported(format!(
                    "SQLite native DROP found known dependency: foreign key from main.{child_table} references main.{table}.{column}"
                )));
            }
        }
    }
    Ok(())
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

fn capture_foreign_key_violation(
    connection: &RusqliteConnection,
) -> Result<Option<String>, DbError> {
    connection
        .query_row("PRAGMA main.foreign_key_check", [], |row| row.get(0))
        .optional()
        .map_err(|error| {
            DbError::query_failed(format!("could not check SQLite foreign keys: {error}"))
        })
}

fn capture_all_foreign_keys(
    connection: &RusqliteConnection,
    tables: &[&str],
) -> Result<Vec<ForeignKeyReference>, DbError> {
    let mut foreign_keys = Vec::new();
    for table in tables {
        for (referenced, from, to) in foreign_keys_for_table(connection, table)? {
            foreign_keys.push(((*table).to_string(), referenced, from, to));
        }
    }
    foreign_keys.sort();
    Ok(foreign_keys)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ForeignKeyCapture {
    child_table: String,
    relation_id: i64,
    sequence: i64,
    parent_table: String,
    child_column: String,
    parent_column: Option<String>,
    on_update: String,
    on_delete: String,
    match_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedForeignKeyRelationship {
    child_table: String,
    relation_id: i64,
    parent_table: String,
    child_columns: Vec<String>,
    parent_columns: Vec<String>,
    declared_parent_columns: Option<Vec<String>>,
    on_update: String,
    on_delete: String,
    match_name: String,
}

fn resolve_foreign_key_relationships(
    rebuild_table: &str,
    table_metadata: &[TableMetadataCapture],
    foreign_keys: &[ForeignKeyCapture],
) -> Result<Vec<ResolvedForeignKeyRelationship>, DbError> {
    let mut grouped = std::collections::BTreeMap::<(String, i64), Vec<&ForeignKeyCapture>>::new();
    for foreign_key in foreign_keys {
        grouped
            .entry((
                foreign_key.child_table.to_ascii_lowercase(),
                foreign_key.relation_id,
            ))
            .or_default()
            .push(foreign_key);
    }

    let mut relationships = Vec::with_capacity(grouped.len());
    for ((_, relation_id), mut rows) in grouped {
        rows.sort_by_key(|row| row.sequence);
        let first = rows.first().ok_or_else(|| {
            DbError::NotSupported(format!(
                "SQLite rebuild catalog proof cannot resolve an empty foreign key relationship for main.{rebuild_table}"
            ))
        })?;
        if rows
            .iter()
            .enumerate()
            .any(|(sequence, row)| row.sequence != sequence as i64)
        {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild catalog proof has non-contiguous foreign key sequence for main.{}",
                first.child_table
            )));
        }
        if rows.iter().any(|row| {
            !row.child_table.eq_ignore_ascii_case(&first.child_table)
                || !row.parent_table.eq_ignore_ascii_case(&first.parent_table)
                || !row.on_update.eq_ignore_ascii_case(&first.on_update)
                || !row.on_delete.eq_ignore_ascii_case(&first.on_delete)
                || !row.match_name.eq_ignore_ascii_case(&first.match_name)
        }) {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild catalog proof has inconsistent foreign key metadata for main.{} relation {relation_id}",
                first.child_table
            )));
        }
        if !first.match_name.eq_ignore_ascii_case("NONE") {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild catalog proof for main.{} has unsupported foreign key match metadata",
                first.child_table
            )));
        }

        let child = table_metadata
            .iter()
            .find(|metadata| metadata.name.eq_ignore_ascii_case(&first.child_table))
            .ok_or_else(|| {
                DbError::NotSupported(format!(
                    "SQLite rebuild catalog proof cannot find foreign key child table main.{}",
                    first.child_table
                ))
            })?;
        let parent = table_metadata
            .iter()
            .find(|metadata| metadata.name.eq_ignore_ascii_case(&first.parent_table))
            .ok_or_else(|| {
                DbError::NotSupported(format!(
                    "SQLite rebuild catalog proof cannot find foreign key parent table main.{}",
                    first.parent_table
                ))
            })?;
        let child_columns = rows
            .iter()
            .map(|row| row.child_column.clone())
            .collect::<Vec<_>>();
        if child_columns.iter().any(|column| {
            !child
                .columns
                .iter()
                .any(|candidate| candidate.name.eq_ignore_ascii_case(column))
        }) {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild catalog proof names an unknown foreign key child column on main.{}",
                child.name
            )));
        }

        let explicit_parent_columns = rows.iter().any(|row| row.parent_column.is_some());
        if explicit_parent_columns && rows.iter().any(|row| row.parent_column.is_none()) {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild catalog proof has mixed implicit and explicit parent columns for main.{} relation {relation_id}",
                child.name
            )));
        }
        let (parent_columns, declared_parent_columns) = if explicit_parent_columns {
            let columns = rows
                .iter()
                .map(|row| row.parent_column.clone().ok_or_else(|| {
                    DbError::NotSupported(format!(
                        "SQLite rebuild catalog proof has missing explicit parent column for main.{}",
                        child.name
                    ))
                }))
                .collect::<Result<Vec<_>, _>>()?;
            if columns.iter().any(|column| {
                !parent
                    .columns
                    .iter()
                    .any(|candidate| candidate.name.eq_ignore_ascii_case(column))
            }) {
                return Err(DbError::NotSupported(format!(
                    "SQLite rebuild catalog proof names an unknown foreign key parent column on main.{}",
                    parent.name
                )));
            }
            (columns.clone(), Some(columns))
        } else {
            let mut primary_key = parent
                .columns
                .iter()
                .filter(|column| column.primary_key_order != 0)
                .collect::<Vec<_>>();
            primary_key.sort_by_key(|column| column.primary_key_order);
            if primary_key.is_empty()
                || primary_key
                    .iter()
                    .enumerate()
                    .any(|(index, column)| column.primary_key_order != (index + 1) as i64)
                || primary_key.len() != child_columns.len()
            {
                return Err(DbError::NotSupported(format!(
                    "SQLite rebuild catalog proof cannot resolve implicit parent primary key for main.{} relation {relation_id}",
                    child.name
                )));
            }
            (
                primary_key
                    .into_iter()
                    .map(|column| column.name.clone())
                    .collect(),
                None,
            )
        };
        relationships.push(ResolvedForeignKeyRelationship {
            child_table: child.name.clone(),
            relation_id,
            parent_table: parent.name.clone(),
            child_columns,
            parent_columns,
            declared_parent_columns,
            on_update: first.on_update.clone(),
            on_delete: first.on_delete.clone(),
            match_name: first.match_name.clone(),
        });
    }
    Ok(relationships)
}

fn capture_all_foreign_key_metadata(
    connection: &RusqliteConnection,
    tables: &[&str],
) -> Result<Vec<ForeignKeyCapture>, DbError> {
    let mut foreign_keys = Vec::new();
    for child_table in tables {
        let mut statement = connection
            .prepare(&format!(
                "PRAGMA main.foreign_key_list({})",
                quote_identifier(child_table)
            ))
            .map_err(|error| {
                DbError::query_failed(format!(
                    "could not inspect foreign keys for main.{child_table}: {error}"
                ))
            })?;
        let rows = statement
            .query_map([], |row| {
                Ok(ForeignKeyCapture {
                    child_table: (*child_table).to_string(),
                    relation_id: row.get(0)?,
                    sequence: row.get(1)?,
                    parent_table: row.get(2)?,
                    child_column: row.get(3)?,
                    parent_column: row.get(4)?,
                    on_update: row.get(5)?,
                    on_delete: row.get(6)?,
                    match_name: row.get(7)?,
                })
            })
            .map_err(|error| {
                DbError::query_failed(format!(
                    "could not read foreign keys for main.{child_table}: {error}"
                ))
            })?;
        for foreign_key in rows {
            foreign_keys.push(foreign_key.map_err(|error| {
                DbError::query_failed(format!(
                    "could not decode foreign key for main.{child_table}: {error}"
                ))
            })?);
        }
    }
    foreign_keys.sort_by(|left, right| {
        (
            &left.child_table,
            left.relation_id,
            left.sequence,
            &left.parent_table,
        )
            .cmp(&(
                &right.child_table,
                right.relation_id,
                right.sequence,
                &right.parent_table,
            ))
    });
    Ok(foreign_keys)
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
    sqlite_engine_version() >= 3_035_000
}

fn sqlite_engine_version() -> i32 {
    #[cfg(test)]
    if let Some((_, version)) = SQLITE_ENGINE_VERSION_OVERRIDE
        .lock()
        .expect("SQLite engine-version test hook mutex should not be poisoned")
        .take_if(|(thread_id, _)| *thread_id == std::thread::current().id())
    {
        return version;
    }
    rusqlite::version_number()
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableColumnCapture {
    name: String,
    type_name: String,
    nullable: bool,
    default: Option<String>,
    primary_key_order: i64,
    hidden: i64,
}

fn validate_parsed_table_metadata(
    table: &str,
    parsed: &CreateTable,
    columns: &[TableColumnCapture],
) -> Result<(), DbError> {
    let parsed_columns = parsed.column_facts().collect::<Vec<_>>();
    if parsed_columns.len() != columns.len() {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild catalog proof for main.{table} cannot account for every stored column"
        )));
    }
    for (parsed_column, catalog_column) in parsed_columns.iter().zip(columns) {
        let parsed_type = parsed_column.declared_type.as_deref().unwrap_or("");
        if !parsed_column
            .name
            .eq_ignore_ascii_case(&catalog_column.name)
            || parsed_type != catalog_column.type_name
            || parsed_column.nullable != catalog_column.nullable
            || !parsed_default_matches_catalog(
                parsed_column.default.as_deref(),
                catalog_column.default.as_deref(),
            )
            || parsed_column.primary_key_order != catalog_column.primary_key_order
            || catalog_column.hidden != 0
        {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild catalog proof for main.{table} disagrees with table_xinfo for column {}",
                catalog_column.name
            )));
        }
    }
    let parsed_primary_keys = parsed
        .constraints()
        .iter()
        .filter(|constraint| matches!(constraint, TableConstraint::PrimaryKey(_)))
        .count();
    if parsed_primary_keys > 1 {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild catalog proof for main.{table} has ambiguous PRIMARY KEY metadata"
        )));
    }
    Ok(())
}

fn validate_parsed_foreign_keys(
    table: &str,
    parsed: &CreateTable,
    relationships: &[ResolvedForeignKeyRelationship],
) -> Result<(), DbError> {
    let parsed_foreign_keys = parsed.foreign_keys().collect::<Vec<_>>();
    let target_relationships = relationships
        .iter()
        .filter(|relationship| relationship.child_table.eq_ignore_ascii_case(table))
        .collect::<Vec<_>>();
    if parsed_foreign_keys.len() != target_relationships.len() {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild catalog proof for main.{table} disagrees with foreign key relationship count"
        )));
    }

    let mut consumed = vec![false; target_relationships.len()];
    for parsed_foreign_key in parsed_foreign_keys {
        let Some((position, _)) =
            target_relationships
                .iter()
                .enumerate()
                .find(|(position, relationship)| {
                    consumed.get(*position).is_some_and(|consumed| !consumed)
                        && relationship
                            .parent_table
                            .eq_ignore_ascii_case(&parsed_foreign_key.parent_table)
                        && relationship
                            .on_update
                            .eq_ignore_ascii_case(&parsed_foreign_key.on_update)
                        && relationship
                            .on_delete
                            .eq_ignore_ascii_case(&parsed_foreign_key.on_delete)
                        && relationship.match_name.eq_ignore_ascii_case("NONE")
                        && identifier_lists_match(
                            &relationship.child_columns,
                            &parsed_foreign_key.columns,
                        )
                        && match (
                            parsed_foreign_key.parent_columns.as_deref(),
                            relationship.declared_parent_columns.as_deref(),
                        ) {
                            (Some(parsed_columns), Some(declared_columns)) => {
                                identifier_lists_match(parsed_columns, declared_columns)
                                    && identifier_lists_match(
                                        parsed_columns,
                                        &relationship.parent_columns,
                                    )
                            }
                            (None, None) => true,
                            _ => false,
                        }
                })
        else {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild catalog proof cannot resolve foreign key intent for main.{table}"
            )));
        };
        *consumed.get_mut(position).ok_or_else(|| {
            DbError::NotSupported(format!(
                "SQLite rebuild catalog proof cannot resolve foreign key intent for main.{table}"
            ))
        })? = true;
    }
    if consumed.iter().any(|consumed| !consumed) {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild catalog proof has extra foreign key relationships for main.{table}"
        )));
    }
    Ok(())
}

fn identifier_lists_match(left: &[String], right: &[String]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

fn validate_foreign_key_source_preservation(
    table: &str,
    parsed: &CreateTable,
    rewritten: &CreateTable,
) -> Result<(), DbError> {
    if parsed.foreign_key_declaration_sources() == rewritten.foreign_key_declaration_sources() {
        return Ok(());
    }
    Err(DbError::NotSupported(format!(
        "SQLite rebuild catalog proof cannot preserve foreign key source for main.{table}"
    )))
}

#[allow(dead_code)]
fn validate_parsed_foreign_keys_legacy(
    table: &str,
    parsed: &CreateTable,
    foreign_keys: &[ForeignKeyCapture],
) -> Result<(), DbError> {
    let parsed_foreign_keys = parsed.foreign_keys().collect::<Vec<_>>();
    let target_foreign_keys = foreign_keys
        .iter()
        .filter(|foreign_key| foreign_key.child_table.eq_ignore_ascii_case(table))
        .collect::<Vec<_>>();
    let mut relations = std::collections::BTreeMap::<i64, Vec<&ForeignKeyCapture>>::new();
    for foreign_key in target_foreign_keys {
        if foreign_key.match_name != "NONE" {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild catalog proof for main.{table} has unsupported foreign key match metadata"
            )));
        }
        relations
            .entry(foreign_key.relation_id)
            .or_default()
            .push(foreign_key);
    }
    for relation in relations.values_mut() {
        relation.sort_by_key(|foreign_key| foreign_key.sequence);
        if relation
            .iter()
            .enumerate()
            .any(|(sequence, foreign_key)| foreign_key.sequence != sequence as i64)
        {
            return Err(DbError::NotSupported(format!(
                "SQLite rebuild catalog proof has non-contiguous foreign key sequence for main.{table}"
            )));
        }
    }
    if parsed_foreign_keys.len() != relations.len() {
        return Err(DbError::NotSupported(format!(
            "SQLite rebuild catalog proof for main.{table} disagrees with foreign key relationship count"
        )));
    }
    let mut matched = std::collections::BTreeSet::new();
    for parsed_foreign_key in parsed_foreign_keys {
        let relation_id = relations
            .iter()
            .filter(|(relation_id, relation)| {
                !matched.contains(*relation_id)
                    && relation.len() == parsed_foreign_key.columns.len()
                    && relation.iter().all(|foreign_key| {
                        foreign_key
                            .parent_table
                            .eq_ignore_ascii_case(&parsed_foreign_key.parent_table)
                            && foreign_key.on_update == parsed_foreign_key.on_update
                            && foreign_key.on_delete == parsed_foreign_key.on_delete
                    })
                    && relation.iter().zip(&parsed_foreign_key.columns).all(
                        |(foreign_key, child_column)| {
                            foreign_key.child_column.eq_ignore_ascii_case(child_column)
                        },
                    )
                    && parsed_foreign_key.parent_columns.as_ref().is_some_and(|columns| {
                        relation.iter().zip(columns).all(|(foreign_key, parent_column)| {
                            foreign_key
                                .parent_column
                                .as_deref()
                                .is_some_and(|column| column.eq_ignore_ascii_case(parent_column))
                        })
                    })
            })
            .map(|(relation_id, _)| *relation_id)
            .next()
            .ok_or_else(|| {
                DbError::NotSupported(format!(
                    "SQLite rebuild catalog proof cannot resolve foreign key intent for main.{table}"
                ))
            })?;
        matched.insert(relation_id);
    }
    Ok(())
}

fn parsed_default_matches_catalog(parsed: Option<&str>, catalog: Option<&str>) -> bool {
    match (parsed, catalog) {
        (None, None) => true,
        (Some(parsed), Some(catalog)) if parsed == catalog => true,
        (Some(parsed), Some(catalog)) => {
            let mut parsed = parsed.trim();
            while let Some(inner) = outer_parenthesized(parsed) {
                parsed = inner.trim();
            }
            parsed == catalog
        }
        _ => false,
    }
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
            "PRAGMA main.table_xinfo({})",
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
                primary_key_order: row.get(5)?,
                hidden: row.get(6)?,
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
static REBUILD_TEST_HOOK_LOCK: Mutex<()> = Mutex::new(());

#[cfg(test)]
static BEFORE_REBUILD_CAPTURE_SECOND_READ: Mutex<
    Option<(std::thread::ThreadId, Box<dyn FnOnce() + Send>)>,
> = Mutex::new(None);

#[cfg(test)]
static AFTER_REBUILD_OBSERVATION_CAPTURE: Mutex<
    Option<(std::thread::ThreadId, Box<dyn FnOnce() + Send>)>,
> = Mutex::new(None);

#[cfg(test)]
static BEFORE_NATIVE_DROP_BEGIN: Mutex<Option<(std::thread::ThreadId, Box<dyn FnOnce() + Send>)>> =
    Mutex::new(None);

#[cfg(test)]
static BEFORE_REBUILD_WRITE_LOCK: Mutex<Option<(std::thread::ThreadId, Box<dyn FnOnce() + Send>)>> =
    Mutex::new(None);

#[cfg(test)]
static SQLITE_ENGINE_VERSION_OVERRIDE: Mutex<Option<(std::thread::ThreadId, i32)>> =
    Mutex::new(None);

#[cfg(test)]
static BEFORE_NATIVE_CAPTURE_SECOND_READ: Mutex<
    Option<(std::thread::ThreadId, Box<dyn FnOnce() + Send>)>,
> = Mutex::new(None);

#[cfg(test)]
#[derive(Default)]
struct ComparisonFinalizeFaults {
    source: bool,
    replacement: bool,
}

#[cfg(test)]
static COMPARISON_FINALIZE_FAULTS: Mutex<
    Option<(std::thread::ThreadId, ComparisonFinalizeFaults)>,
> = Mutex::new(None);

#[cfg(test)]
static COMPARISON_CANCEL_AFTER_ROW: Mutex<Option<(std::thread::ThreadId, Arc<AtomicBool>)>> =
    Mutex::new(None);

#[cfg(test)]
static INTEGRITY_FINALIZE_FAULT: Mutex<Option<(std::thread::ThreadId, i32)>> = Mutex::new(None);

#[cfg(test)]
static REBUILD_EXECUTION_FAULT: Mutex<Option<(std::thread::ThreadId, RebuildExecutionStage)>> =
    Mutex::new(None);

#[cfg(test)]
static REBUILD_STAGE_HOOK: Mutex<
    Option<(
        std::thread::ThreadId,
        RebuildExecutionStage,
        Box<dyn FnOnce(&RusqliteConnection) -> Result<(), rusqlite::Error> + Send>,
    )>,
> = Mutex::new(None);

#[cfg(test)]
static REBUILD_GUARD_FAULTS: Mutex<Option<(std::thread::ThreadId, RebuildGuardFaults)>> =
    Mutex::new(None);

#[cfg(test)]
fn run_rebuild_stage_hook(
    connection: &RusqliteConnection,
    stage: RebuildExecutionStage,
) -> Result<(), DbError> {
    let hook = REBUILD_STAGE_HOOK
        .lock()
        .expect("rebuild stage hook mutex should not be poisoned")
        .take_if(|(thread_id, selected, _)| {
            *thread_id == std::thread::current().id() && *selected == stage
        });
    if let Some((_, _, hook)) = hook {
        hook(connection).map_err(|error| {
            rebuild_sqlite_execution_error("run rebuild test stage hook", &error)
        })?;
    }
    Ok(())
}

#[cfg(test)]
fn rebuild_guard_faults_for_current_thread() -> RebuildGuardFaults {
    REBUILD_GUARD_FAULTS
        .lock()
        .expect("rebuild guard fault mutex should not be poisoned")
        .take_if(|(thread_id, _)| *thread_id == std::thread::current().id())
        .map(|(_, faults)| faults)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use rusqlite::Connection as RusqliteConnection;

    use super::{
        AFTER_REBUILD_OBSERVATION_CAPTURE, BEFORE_NATIVE_CAPTURE_SECOND_READ,
        BEFORE_NATIVE_DROP_BEGIN, BEFORE_REBUILD_CAPTURE_SECOND_READ, BEFORE_REBUILD_WRITE_LOCK,
        ForeignKeyCapture, REBUILD_TEST_HOOK_LOCK, SQLITE_ENGINE_VERSION_OVERRIDE,
        TableColumnCapture, TableMetadataCapture, resolve_foreign_key_relationships,
    };
    use crate::driver::{SqliteConnection, SqliteConnectionState};
    use dbflux_core::{
        Connection, DbError, PreparedTableAlter, TableAlterExpectedColumn, TableAlterOperation,
        TableAlterRequest, TableRef,
    };

    #[test]
    fn rebuild_cancellation_is_typed_before_any_cleanup_is_needed() {
        let cancelled = AtomicBool::new(false);
        cancelled.store(true, Ordering::SeqCst);
        assert!(matches!(
            super::check_rebuild_cancelled(&cancelled, "during exact comparison"),
            Err(DbError::Cancelled)
        ));
        assert!(matches!(
            super::compare_exact_row_streams(
                &RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
                "SELECT 1",
                "SELECT 1",
                1,
                &cancelled,
            ),
            Err(DbError::Cancelled)
        ));
    }

    fn prepare_rebuild_plan_for_test(
        state: Arc<Mutex<SqliteConnectionState>>,
        request: &TableAlterRequest,
    ) -> Result<Box<super::RebuildPlan>, DbError> {
        prepare_rebuild_plan_with_cancellation_for_test(
            state,
            request,
            Arc::new(AtomicBool::new(false)),
        )
    }

    fn prepare_rebuild_plan_with_cancellation_for_test(
        state: Arc<Mutex<SqliteConnectionState>>,
        request: &TableAlterRequest,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Box<super::RebuildPlan>, DbError> {
        let changes = super::selected_rebuild_changes(&request.operations)?
            .expect("test request must select the rebuild route");
        let state_guard = SqliteConnectionState::lock_checked(&state)?;
        super::prepare_rebuild_plan_typed(&state_guard, state.clone(), cancelled, request, changes)
    }

    fn clear_rebuild_test_faults() {
        *super::REBUILD_EXECUTION_FAULT
            .lock()
            .expect("execution fault mutex should not be poisoned") = None;
        *super::REBUILD_STAGE_HOOK
            .lock()
            .expect("stage hook mutex should not be poisoned") = None;
        *super::REBUILD_GUARD_FAULTS
            .lock()
            .expect("guard fault mutex should not be poisoned") = None;
        *super::COMPARISON_CANCEL_AFTER_ROW
            .lock()
            .expect("comparison cancellation hook mutex should not be poisoned") = None;
        *super::COMPARISON_FINALIZE_FAULTS
            .lock()
            .expect("comparison finalization fault mutex should not be poisoned") = None;
    }

    #[test]
    fn rebuild_execution_faults_use_the_private_plan_entry_and_roll_back_every_precommit_stage() {
        let _hook_lock = super::REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild hook lock should not be poisoned");
        for stage in [
            super::RebuildExecutionStage::Create,
            super::RebuildExecutionStage::Copy,
            super::RebuildExecutionStage::Compare,
            super::RebuildExecutionStage::Drop,
            super::RebuildExecutionStage::Rename,
            super::RebuildExecutionStage::Index,
            super::RebuildExecutionStage::FinalValidation,
            super::RebuildExecutionStage::Precommit,
        ] {
            let state = Arc::new(Mutex::new(SqliteConnectionState::new(
                RusqliteConnection::open_in_memory().expect("fixture SQLite should open"),
            )));
            state
                .lock()
                .expect("state mutex should not be poisoned")
                .execute_batch(
                    "PRAGMA foreign_keys = ON; \
                     CREATE TABLE parent (id INTEGER PRIMARY KEY); \
                     CREATE TABLE target (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, retained TEXT, obsolete TEXT, parent_id INTEGER REFERENCES parent(id)); \
                     CREATE INDEX target_retained ON target(retained DESC); \
                     INSERT INTO parent VALUES (1); \
                     INSERT INTO target VALUES (-7, 'raw', 'keep', 'remove', 1)",
                )
                .expect("fixture should be created");
            let request = TableAlterRequest {
                table: TableRef::new("target"),
                operations: vec![
                    TableAlterOperation::AlterColumn {
                        name: "payload".to_string(),
                        new_type: Some("VARCHAR(16)".to_string()),
                        nullable: Some(false),
                        default: None,
                    },
                    TableAlterOperation::DropColumn {
                        name: "obsolete".to_string(),
                    },
                ],
                expected_before: Vec::new(),
            };
            let plan = prepare_rebuild_plan_for_test(state.clone(), &request)
                .expect("fixture plan should prepare");
            *super::REBUILD_EXECUTION_FAULT
                .lock()
                .expect("execution fault mutex should not be poisoned") =
                Some((std::thread::current().id(), stage));
            let execution = plan.execute_guarded();
            clear_rebuild_test_faults();
            let error = execution.expect_err("one-shot stage fault must fail the private executor");
            assert!(
                error
                    .to_string()
                    .contains("injected rebuild execution fault")
            );
            let state = state.lock().expect("state mutex should not be poisoned");
            assert!(state.is_autocommit(), "{stage:?} must finish rollback");
            assert_eq!(
                state
                    .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                    .expect("foreign keys should be readable"),
                1,
                "{stage:?} must restore foreign keys"
            );
            assert_eq!(
                state.query_row(
                    "SELECT id || ':' || payload || ':' || retained || ':' || obsolete FROM target",
                    [],
                    |row| row.get::<_, String>(0),
                ).expect("original row should remain"),
                "-7:raw:keep:remove",
                "{stage:?} must retain original rows"
            );
            assert_eq!(
                state.query_row(
                    "SELECT sql FROM main.sqlite_master WHERE type = 'index' AND name = 'target_retained'",
                    [],
                    |row| row.get::<_, String>(0),
                ).expect("original index should remain"),
                "CREATE INDEX target_retained ON target(retained DESC)",
                "{stage:?} must retain original indexes"
            );
            assert_eq!(
                state.query_row(
                    "SELECT COUNT(*) FROM main.sqlite_master WHERE name LIKE '__dbflux_rebuild_%'",
                    [],
                    |row| row.get::<_, i64>(0),
                ).expect("private catalog should be readable"),
                0,
                "{stage:?} must not leak its replacement table"
            );
        }
    }

    #[test]
    fn rebuild_plan_integrates_commit_rollback_and_foreign_key_restore_faults() {
        let _hook_lock = super::REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild hook lock should not be poisoned");
        let make_plan = || {
            let state = Arc::new(Mutex::new(SqliteConnectionState::new(
                RusqliteConnection::open_in_memory().expect("fixture SQLite should open"),
            )));
            state
                .lock()
                .expect("state mutex should not be poisoned")
                .execute_batch(
                    "PRAGMA foreign_keys = ON; \
                     CREATE TABLE target (id INTEGER PRIMARY KEY, payload TEXT, obsolete TEXT); \
                     INSERT INTO target VALUES (-7, 'raw', 'remove')",
                )
                .expect("fixture should be created");
            let request = TableAlterRequest {
                table: TableRef::new("target"),
                operations: vec![
                    TableAlterOperation::AlterColumn {
                        name: "payload".to_string(),
                        new_type: Some("VARCHAR(16)".to_string()),
                        nullable: None,
                        default: None,
                    },
                    TableAlterOperation::DropColumn {
                        name: "obsolete".to_string(),
                    },
                ],
                expected_before: Vec::new(),
            };
            let plan = prepare_rebuild_plan_for_test(state.clone(), &request)
                .expect("fixture plan should prepare");
            (state, plan)
        };

        let assert_quarantined = |state: &Arc<Mutex<SqliteConnectionState>>, reason: &str| {
            let checked_alias = state.clone();
            let interrupt_handle = state
                .lock()
                .expect("state mutex should not be poisoned")
                .interrupt_handle();
            assert!(
                SqliteConnectionState::lock_checked(state).is_err(),
                "{reason}"
            );
            assert!(
                SqliteConnectionState::lock_checked(&checked_alias).is_err(),
                "every checked alias must observe the failure-caused quarantine"
            );
            assert!(
                prepare_rebuild_plan_for_test(checked_alias, &rebuild_alter_and_drop_request())
                    .is_err(),
                "a second prepared plan must reject the quarantined connection"
            );
            interrupt_handle.interrupt();
        };

        let (state, plan) = make_plan();
        *super::REBUILD_GUARD_FAULTS
            .lock()
            .expect("guard fault mutex should not be poisoned") = Some((
            std::thread::current().id(),
            super::RebuildGuardFaults {
                fail_commit_while_active: true,
                ..Default::default()
            },
        ));
        let execution = plan.execute_guarded();
        clear_rebuild_test_faults();
        assert!(execution.is_err());
        let state_guard = state.lock().expect("state mutex should not be poisoned");
        assert!(state_guard.is_autocommit());
        assert_eq!(
            state_guard
                .query_row("SELECT payload || ':' || obsolete FROM target", [], |row| {
                    row.get::<_, String>(0)
                })
                .expect("rollback must retain original row"),
            "raw:remove"
        );
        drop(state_guard);
        assert!(SqliteConnectionState::lock_checked(&state).is_ok());

        let (state, plan) = make_plan();
        *super::REBUILD_GUARD_FAULTS
            .lock()
            .expect("guard fault mutex should not be poisoned") = Some((
            std::thread::current().id(),
            super::RebuildGuardFaults {
                fail_commit_after_commit: true,
                ..Default::default()
            },
        ));
        let execution = plan.execute_guarded();
        clear_rebuild_test_faults();
        let error = execution.expect_err("post-commit error is uncertain, not success");
        assert!(error.to_string().contains("Uncertain"));
        assert_eq!(
            state
                .lock()
                .expect("state mutex should not be poisoned")
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('target') WHERE name = 'obsolete'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("committed schema should remain inspectable"),
            0,
            "post-commit transport uncertainty is distinct from rollback"
        );
        assert_quarantined(
            &state,
            "post-commit uncertainty must quarantine the connection",
        );

        let (state, plan) = make_plan();
        *super::REBUILD_GUARD_FAULTS
            .lock()
            .expect("guard fault mutex should not be poisoned") = Some((
            std::thread::current().id(),
            super::RebuildGuardFaults {
                fail_restore_readback: true,
                ..Default::default()
            },
        ));
        let execution = plan.execute_guarded();
        clear_rebuild_test_faults();
        let error = execution.expect_err("committed-but-restore-failed is not success");
        assert!(
            error
                .to_string()
                .contains("foreign-key restoration failure")
        );
        assert_eq!(
            state
                .lock()
                .expect("state mutex should not be poisoned")
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('target') WHERE name = 'obsolete'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("committed schema should remain inspectable"),
            0,
            "committed-but-cleanup-failed must remain distinct from rollback"
        );
        assert_quarantined(
            &state,
            "foreign-key restoration failure must quarantine the connection",
        );

        let (state, plan) = make_plan();
        *super::REBUILD_EXECUTION_FAULT
            .lock()
            .expect("execution fault mutex should not be poisoned") = Some((
            std::thread::current().id(),
            super::RebuildExecutionStage::Create,
        ));
        *super::REBUILD_GUARD_FAULTS
            .lock()
            .expect("guard fault mutex should not be poisoned") = Some((
            std::thread::current().id(),
            super::RebuildGuardFaults {
                fail_rollback: true,
                ..Default::default()
            },
        ));
        let execution = plan.execute_guarded();
        clear_rebuild_test_faults();
        assert!(execution.is_err());
        assert_quarantined(
            &state,
            "rollback uncertainty must quarantine the connection",
        );
    }

    #[test]
    fn sqlite_interrupt_is_a_typed_clean_cancellation() {
        let interruption = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_INTERRUPT),
            Some("interrupted by SQLite".to_string()),
        );
        assert!(matches!(
            super::rebuild_sqlite_execution_error("advance source comparison", &interruption),
            DbError::Cancelled
        ));
    }

    #[test]
    fn sqlite_ffi_progress_handler_interrupts_an_actual_comparison_query() {
        unsafe extern "C" fn interrupt_progress(_: *mut std::ffi::c_void) -> std::ffi::c_int {
            1
        }

        let connection =
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open");
        // SAFETY: this test owns the connection; the callback is non-capturing and is
        // unregistered before the connection is reused or dropped.
        unsafe {
            rusqlite::ffi::sqlite3_progress_handler(
                connection.handle(),
                1,
                Some(interrupt_progress),
                std::ptr::null_mut(),
            );
        }
        let result = super::compare_exact_row_streams(
            &connection,
            "SELECT 1 UNION ALL SELECT 2",
            "SELECT 1 UNION ALL SELECT 2",
            1,
            &AtomicBool::new(false),
        );
        // SAFETY: unregisters the test-only callback from the same owned connection.
        unsafe {
            rusqlite::ffi::sqlite3_progress_handler(
                connection.handle(),
                0,
                None,
                std::ptr::null_mut(),
            );
        }
        assert!(matches!(result, Err(DbError::Cancelled)));
    }

    #[test]
    fn exact_streamed_comparison_cancels_midstream_and_still_finalizes_readers() {
        let _hook_lock = super::REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild hook lock should not be poisoned");
        let cancelled = Arc::new(AtomicBool::new(false));
        *super::COMPARISON_CANCEL_AFTER_ROW
            .lock()
            .expect("comparison cancellation hook should not be poisoned") =
            Some((std::thread::current().id(), cancelled.clone()));
        let connection =
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open");
        assert!(matches!(
            super::compare_exact_row_streams(
                &connection,
                "SELECT 1 UNION ALL SELECT 2",
                "SELECT 1 UNION ALL SELECT 2",
                1,
                &cancelled,
            ),
            Err(DbError::Cancelled)
        ));
        connection
            .execute_batch(
                "CREATE TABLE finalization_proof(value INTEGER); DROP TABLE finalization_proof",
            )
            .expect("midstream cancellation must finalize both readers before later DDL");
    }

    #[test]
    fn exact_streamed_comparison_preserves_raw_values_and_rejects_mismatches() {
        let connection =
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open");
        connection
            .execute_batch(
                "CREATE TABLE source (id INTEGER PRIMARY KEY, null_value, integer_value, real_value, text_value, blob_value); \
                 CREATE TABLE replacement (id INTEGER PRIMARY KEY, null_value, integer_value, real_value, text_value, blob_value); \
                 INSERT INTO source VALUES (-7, NULL, 9223372036854775807, -0.0, CAST(X'80004E554C' AS TEXT), X'00FF00'); \
                 INSERT INTO source VALUES (42, NULL, -9223372036854775807, 1.5, CAST(X'410042' AS TEXT), X''); \
                 INSERT INTO replacement VALUES (-7, NULL, 9223372036854775807, -0.0, CAST(X'80004E554C' AS TEXT), X'00FF00'); \
                 INSERT INTO replacement VALUES (42, NULL, -9223372036854775807, 1.5, CAST(X'410042' AS TEXT), X'');",
            )
            .expect("raw comparison fixture should be created");
        let cancelled = AtomicBool::new(false);
        super::compare_exact_row_streams(
            &connection,
            "SELECT id, null_value, integer_value, real_value, text_value, blob_value FROM source ORDER BY id",
            "SELECT id, null_value, integer_value, real_value, text_value, blob_value FROM replacement ORDER BY id",
            6,
            &cancelled,
        )
        .expect("ordered multirow NULL, INTEGER, REAL, raw TEXT, and BLOB values should compare exactly");
        connection
            .execute_batch("DROP TABLE source")
            .expect("comparison statements must finalize before a destructive operation");

        for (label, source, replacement, columns) in [
            ("row count", "SELECT 1", "SELECT 1 WHERE 0", 1),
            ("identity", "SELECT -7", "SELECT 42", 1),
            ("NULL", "SELECT NULL", "SELECT 'NULL'", 1),
            ("storage class", "SELECT 1", "SELECT '1'", 1),
            (
                "exact i64",
                "SELECT 9223372036854775807",
                "SELECT 9223372036854775806",
                1,
            ),
            ("REAL bits", "SELECT -0.0", "SELECT 0.0", 1),
            ("raw BLOB bytes", "SELECT X'00FF00'", "SELECT X'00FE00'", 1),
            (
                "raw invalid UTF-8",
                "SELECT CAST(X'80' AS TEXT)",
                "SELECT CAST(X'81' AS TEXT)",
                1,
            ),
            ("coercion", "SELECT CAST(X'31' AS TEXT)", "SELECT 1", 1),
        ] {
            assert!(
                super::compare_exact_row_streams(
                    &connection,
                    source,
                    replacement,
                    columns,
                    &cancelled
                )
                .is_err(),
                "{label} mismatch must be rejected without SQL coercion"
            );
        }
        assert!(
            !super::sqlite_value_refs_equal(
                rusqlite::types::ValueRef::Real(-0.0),
                rusqlite::types::ValueRef::Real(0.0),
            ),
            "REAL comparison must use IEEE bits rather than numeric equality"
        );
    }

    #[test]
    fn integrity_reader_accepts_actual_pragma_and_rejects_non_exact_rows() {
        let connection =
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open");
        super::validate_single_integrity_text_row(&connection, "PRAGMA main.integrity_check")
            .expect("actual integrity_check must return raw TEXT ok");
        for (label, sql) in [
            ("no rows", "SELECT 'ok' WHERE 0"),
            ("extra rows", "SELECT 'ok' UNION ALL SELECT 'unexpected'"),
            ("BLOB ok", "SELECT X'6F6B'"),
            ("NULL", "SELECT NULL"),
            ("wrong text", "SELECT 'okay'"),
            ("invalid UTF-8 text", "SELECT CAST(X'80' AS TEXT)"),
            ("NUL text", "SELECT CAST(X'6F006B' AS TEXT)"),
            ("INTEGER", "SELECT 1"),
            ("REAL", "SELECT 1.5"),
            ("multiple columns", "SELECT 'ok', 'extra'"),
        ] {
            assert!(
                super::validate_single_integrity_text_row(&connection, sql).is_err(),
                "integrity reader must reject {label}"
            );
        }
    }

    #[test]
    fn integrity_reader_finalization_preserves_structured_code_precedence_and_cancellation() {
        let _hook_lock = super::REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild hook lock should not be poisoned");
        let connection =
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open");
        *super::INTEGRITY_FINALIZE_FAULT
            .lock()
            .expect("integrity finalization fault mutex should not be poisoned") =
            Some((std::thread::current().id(), rusqlite::ffi::SQLITE_BUSY));
        let finalization_only =
            super::validate_single_integrity_text_row(&connection, "SELECT 'ok'")
                .expect_err("injected finalization fault must fail an otherwise valid result");
        assert_eq!(
            finalization_only
                .formatted()
                .and_then(|formatted| formatted.code.as_deref()),
            Some("5")
        );

        *super::INTEGRITY_FINALIZE_FAULT
            .lock()
            .expect("integrity finalization fault mutex should not be poisoned") =
            Some((std::thread::current().id(), rusqlite::ffi::SQLITE_BUSY));
        let primary_and_finalization =
            super::validate_single_integrity_text_row(&connection, "SELECT X'6F6B'")
                .expect_err("invalid primary result plus finalization fault must compose");
        let details = primary_and_finalization
            .formatted()
            .and_then(|formatted| formatted.detail.as_deref())
            .unwrap_or_default();
        assert!(details.contains("primary integrity_check failure"));
        assert!(details.contains("statement finalization"));
        assert_eq!(
            primary_and_finalization
                .formatted()
                .and_then(|formatted| formatted.code.as_deref()),
            Some("5")
        );

        let constraint = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT),
            Some("primary constraint".to_string()),
        );
        let busy = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            Some("finalization busy".to_string()),
        );
        let primary_code = super::finish_integrity_result(
            Err(super::rebuild_sqlite_execution_error(
                "primary",
                &constraint,
            )),
            Err(busy),
        )
        .expect_err("primary and finalization errors must compose");
        assert_eq!(
            primary_code
                .formatted()
                .and_then(|formatted| formatted.code.as_deref()),
            Some("19")
        );
        let cancellation = super::finish_integrity_result(
            Err(DbError::Cancelled),
            Err(rusqlite::Error::InvalidQuery),
        )
        .expect_err("cleanup failure must not return bare cancellation");
        assert!(!matches!(cancellation, DbError::Cancelled));
    }

    #[test]
    fn replacement_prepare_failure_still_observes_source_finalization() {
        let _hook_lock = super::REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild hook lock should not be poisoned");
        *super::COMPARISON_FINALIZE_FAULTS
            .lock()
            .expect("finalize fault hook should not be poisoned") = Some((
            std::thread::current().id(),
            super::ComparisonFinalizeFaults {
                source: true,
                replacement: false,
            },
        ));
        let connection =
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open");
        let error = super::compare_exact_row_streams(
            &connection,
            "SELECT 1",
            "SELECT FROM",
            1,
            &AtomicBool::new(false),
        )
        .expect_err("replacement prepare and source finalization failures must compose");
        let detail = error
            .formatted()
            .and_then(|formatted| formatted.detail.as_deref())
            .unwrap_or_default();
        assert!(detail.contains("prepare replacement comparison"));
        assert!(detail.contains("source statement finalization"));
    }

    #[test]
    fn comparison_cleanup_uses_the_first_available_sqlite_code() {
        let replacement = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            Some("replacement finalize busy".to_string()),
        );
        let error = super::finish_comparison_result(
            Err(DbError::query_failed("primary without SQLite code")),
            Err(rusqlite::Error::InvalidQuery),
            Err(replacement),
        )
        .expect_err("cleanup failures must remain composite");
        let formatted = error.formatted().expect("composite must remain structured");
        assert_eq!(formatted.code.as_deref(), Some("5"));
        let detail = formatted.detail.as_deref().unwrap_or_default();
        assert!(detail.contains("source statement finalization"));
        assert!(detail.contains("replacement statement finalization"));

        let cancellation = super::finish_comparison_result(
            Err(DbError::Cancelled),
            Err(rusqlite::Error::InvalidQuery),
            Ok(()),
        )
        .expect_err("cleanup failure must not return bare cancellation");
        assert!(!matches!(cancellation, DbError::Cancelled));
    }

    #[test]
    fn comparison_finalization_failures_are_reported_after_both_readers_close() {
        let _hook_lock = super::REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild hook lock should not be poisoned");
        *super::COMPARISON_FINALIZE_FAULTS
            .lock()
            .expect("finalize fault hook should not be poisoned") = Some((
            std::thread::current().id(),
            super::ComparisonFinalizeFaults {
                source: true,
                replacement: true,
            },
        ));
        let connection =
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open");
        let cancelled = AtomicBool::new(false);
        let error =
            super::compare_exact_row_streams(&connection, "SELECT 1", "SELECT 1", 1, &cancelled)
                .expect_err("both result-bearing statement finalization failures must be surfaced");
        let detail = error
            .formatted()
            .and_then(|formatted| formatted.detail.as_deref())
            .unwrap_or_default();
        assert!(detail.contains("source statement finalization"));
        assert!(detail.contains("replacement statement finalization"));
    }

    #[test]
    fn rebuild_transaction_guard_commits_mutation_and_restores_foreign_keys() {
        let mut state = SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        );
        state
            .execute_batch("PRAGMA foreign_keys = ON; CREATE TABLE sample (value INTEGER)")
            .expect("fixture should be created");

        let guard = super::RebuildTransactionGuard::begin(&mut state)
            .expect("guard should begin its private transaction");
        guard
            .connection()
            .execute("INSERT INTO sample VALUES (7)", [])
            .expect("guarded test mutation should succeed");
        assert_eq!(guard.finish(Ok(1_u64)).expect("cleanup should succeed"), 1);
        assert!(state.is_autocommit());
        assert_eq!(
            state
                .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                .expect("foreign key setting should be readable"),
            1
        );
        assert_eq!(
            state
                .query_row("SELECT value FROM sample", [], |row| row.get::<_, i64>(0))
                .expect("committed mutation should be readable"),
            7
        );
    }

    #[test]
    fn rebuild_transaction_guard_restores_foreign_keys_when_initially_on_or_off() {
        for foreign_keys in [0_i64, 1] {
            let mut state = SqliteConnectionState::new(
                RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
            );
            state
                .execute_batch(&format!(
                    "PRAGMA foreign_keys = {foreign_keys}; CREATE TABLE sample (value INTEGER)"
                ))
                .expect("fixture should be created");
            let guard =
                super::RebuildTransactionGuard::begin(&mut state).expect("guard should begin");
            guard
                .connection()
                .execute("INSERT INTO sample VALUES (7)", [])
                .expect("mutation should succeed");
            assert_eq!(guard.finish(Ok(1_u64)).expect("cleanup should succeed"), 1);
            assert!(state.is_autocommit());
            assert_eq!(
                state
                    .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                    .expect("foreign keys should be readable"),
                foreign_keys
            );
        }
    }

    #[test]
    fn rebuild_transaction_guard_rejects_caller_owned_transactions_without_cleanup() {
        for foreign_keys in [0_i64, 1] {
            let shared = Arc::new(Mutex::new(SqliteConnectionState::new(
                RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
            )));
            let mut state = shared.lock().expect("state mutex should not be poisoned");
            state
                .execute_batch(&format!(
                    "PRAGMA foreign_keys = {foreign_keys}; CREATE TABLE sample (value INTEGER); \
                     BEGIN; INSERT INTO sample VALUES (7)"
                ))
                .expect("caller transaction fixture should be created");

            let error = match super::RebuildTransactionGuard::begin(&mut state) {
                Ok(_) => panic!("guard must reject a caller-owned transaction"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("caller-owned transaction"));
            assert!(
                !state.is_autocommit(),
                "caller transaction must remain active"
            );
            assert_eq!(
                state
                    .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                    .expect("foreign key setting should remain readable"),
                foreign_keys,
                "guard must not assign the foreign-key setting"
            );
            assert_eq!(
                state
                    .query_row("SELECT value FROM sample", [], |row| row.get::<_, i64>(0))
                    .expect("caller mutation must remain visible"),
                7,
                "guard must not roll back caller work"
            );
            drop(state);
            assert!(
                SqliteConnectionState::lock_checked(&shared).is_ok(),
                "rejection must not quarantine the caller connection"
            );

            shared
                .lock()
                .expect("state mutex should not be poisoned")
                .execute_batch("ROLLBACK")
                .expect("fixture must clean up its caller transaction");
        }
    }

    #[test]
    fn rebuild_transaction_guard_preserves_sqlite_codes_through_cleanup_composites() {
        let directory = tempfile::tempdir().expect("test directory should be created");
        let database_path = directory.path().join("guard-setup-busy.sqlite");
        let blocker = RusqliteConnection::open(&database_path).expect("blocker should open");
        let mut state = SqliteConnectionState::new(
            RusqliteConnection::open(&database_path).expect("guard connection should open"),
        );
        state
            .execute_batch(
                "PRAGMA foreign_keys = ON; CREATE TABLE sample (value INTEGER PRIMARY KEY)",
            )
            .expect("fixture should be created");
        blocker
            .execute_batch("BEGIN IMMEDIATE")
            .expect("blocker should own the write lock");

        let error = match super::RebuildTransactionGuard::begin(&mut state) {
            Ok(_) => panic!("busy setup must fail"),
            Err(error) => error,
        };
        let formatted = error
            .formatted()
            .expect("setup error must remain formatted");
        assert_eq!(formatted.code.as_deref(), Some("5"));
        assert!(!formatted.retriable);
        assert!(
            formatted
                .detail
                .as_deref()
                .unwrap_or_default()
                .contains("begin immediate transaction")
        );
        blocker
            .execute_batch("ROLLBACK")
            .expect("blocker transaction should clean up");

        let guard = super::RebuildTransactionGuard::begin_with_faults(
            &mut state,
            super::RebuildGuardFaults {
                fail_restore_readback: true,
                ..Default::default()
            },
        )
        .expect("guard should begin");
        guard
            .connection()
            .execute("INSERT INTO sample VALUES (1)", [])
            .expect("first insert should succeed");
        let constraint = guard
            .connection()
            .execute("INSERT INTO sample VALUES (1)", [])
            .expect_err("duplicate primary key must fail");
        let expected_code = match &constraint {
            rusqlite::Error::SqliteFailure(code, _) => code.extended_code.to_string(),
            other => panic!("expected a SQLite constraint error, got {other:?}"),
        };
        let error = guard
            .finish(Err::<(), _>(super::rebuild_guard_sqlite_error(
                "insert duplicate primary key",
                &constraint,
            )))
            .expect_err("cleanup composite must retain the primary error metadata");
        let formatted = error
            .formatted()
            .expect("cleanup composite must remain formatted");
        assert_eq!(formatted.code.as_deref(), Some(expected_code.as_str()));
        assert!(!formatted.retriable);
        let detail = formatted.detail.as_deref().unwrap_or_default();
        assert!(detail.contains("primary failure"));
        assert!(detail.contains("foreign-key restoration failure"));
    }

    #[test]
    fn rebuild_transaction_guard_rolls_back_primary_and_automatic_rollback_failures() {
        let mut state = SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        );
        state
            .execute_batch(
                "PRAGMA foreign_keys = ON; CREATE TABLE sample (value INTEGER PRIMARY KEY)",
            )
            .expect("fixture should be created");
        let guard = super::RebuildTransactionGuard::begin(&mut state).expect("guard should begin");
        guard
            .connection()
            .execute("INSERT INTO sample VALUES (7)", [])
            .expect("mutation should succeed");
        let primary = DbError::query_failed("primary test failure");
        assert!(
            guard
                .finish(Err::<(), _>(primary))
                .unwrap_err()
                .to_string()
                .contains("primary test failure")
        );
        assert_eq!(
            state
                .query_row("SELECT COUNT(*) FROM sample", [], |row| row
                    .get::<_, i64>(0))
                .expect("count should work"),
            0
        );

        let guard = super::RebuildTransactionGuard::begin(&mut state).expect("guard should begin");
        guard
            .connection()
            .execute("INSERT INTO sample VALUES (1)", [])
            .expect("first mutation should succeed");
        let automatic = guard
            .connection()
            .execute("INSERT OR ROLLBACK INTO sample VALUES (1)", [])
            .expect_err("SQLite must roll back this transaction");
        assert!(guard.connection().is_autocommit());
        let error = guard
            .finish(Err::<(), _>(super::rebuild_guard_sqlite_error(
                "automatic rollback",
                &automatic,
            )))
            .unwrap_err();
        assert!(!error.to_string().contains("no transaction"));
        assert_eq!(
            state
                .query_row("SELECT COUNT(*) FROM sample", [], |row| row
                    .get::<_, i64>(0))
                .expect("count should work"),
            0
        );
        assert_eq!(
            state
                .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                .expect("foreign keys should be readable"),
            1
        );
    }

    #[test]
    fn rebuild_transaction_guard_distinguishes_commit_error_certainty_and_quarantines() {
        let shared = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        let mut state = shared.lock().expect("state mutex should not be poisoned");
        state
            .execute_batch("PRAGMA foreign_keys = ON; CREATE TABLE sample (value INTEGER)")
            .expect("fixture should be created");
        let guard = super::RebuildTransactionGuard::begin_with_faults(
            &mut state,
            super::RebuildGuardFaults {
                fail_commit_while_active: true,
                ..Default::default()
            },
        )
        .expect("guard should begin");
        guard
            .connection()
            .execute("INSERT INTO sample VALUES (7)", [])
            .expect("mutation should succeed");
        assert!(
            guard
                .finish(Ok(()))
                .unwrap_err()
                .to_string()
                .contains("commit")
        );
        assert!(state.is_autocommit());
        assert_eq!(
            state
                .query_row("SELECT COUNT(*) FROM sample", [], |row| row
                    .get::<_, i64>(0))
                .expect("count should work"),
            0
        );
        drop(state);
        assert!(
            SqliteConnectionState::lock_checked(&shared).is_ok(),
            "verified rollback must not quarantine"
        );

        let mut state = shared.lock().expect("state mutex should not be poisoned");
        let guard = super::RebuildTransactionGuard::begin_with_faults(
            &mut state,
            super::RebuildGuardFaults {
                fail_commit_after_commit: true,
                ..Default::default()
            },
        )
        .expect("guard should begin");
        guard
            .connection()
            .execute("INSERT INTO sample VALUES (9)", [])
            .expect("mutation should succeed");
        let error = guard
            .finish(Ok(()))
            .expect_err("commit return error after SQLite commit is uncertain");
        assert!(error.to_string().contains("Uncertain"));
        assert_eq!(
            state
                .query_row("SELECT COUNT(*) FROM sample", [], |row| row
                    .get::<_, i64>(0))
                .expect("count should work"),
            1
        );
        drop(state);
        assert!(
            SqliteConnectionState::lock_checked(&shared).is_err(),
            "uncertain commit must quarantine aliases"
        );
    }

    #[test]
    fn rebuild_transaction_guard_reports_cleanup_failures_after_commit_or_cancellation() {
        let shared = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        let mut state = shared.lock().expect("state mutex should not be poisoned");
        state
            .execute_batch("PRAGMA foreign_keys = ON; CREATE TABLE sample (value INTEGER)")
            .expect("fixture should be created");
        let guard = super::RebuildTransactionGuard::begin_with_faults(
            &mut state,
            super::RebuildGuardFaults {
                fail_restore_readback: true,
                ..Default::default()
            },
        )
        .expect("guard should begin");
        guard
            .connection()
            .execute("INSERT INTO sample VALUES (7)", [])
            .expect("mutation should succeed");
        let error = guard
            .finish(Ok(()))
            .expect_err("confirmed commit cleanup failure must be surfaced");
        assert!(error.to_string().contains("ConfirmedCommitted"));
        assert!(
            error
                .to_string()
                .contains("foreign-key restoration failure")
        );
        assert_eq!(
            state
                .query_row("SELECT COUNT(*) FROM sample", [], |row| row
                    .get::<_, i64>(0))
                .expect("committed value should work"),
            1
        );
        drop(state);
        assert!(SqliteConnectionState::lock_checked(&shared).is_err());

        let shared = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        let mut state = shared.lock().expect("state mutex should not be poisoned");
        state
            .execute_batch("PRAGMA foreign_keys = ON; CREATE TABLE sample (value INTEGER)")
            .expect("fixture should be created");
        let guard = super::RebuildTransactionGuard::begin_with_faults(
            &mut state,
            super::RebuildGuardFaults {
                fail_rollback: true,
                fail_restore_set: true,
                ..Default::default()
            },
        )
        .expect("guard should begin");
        let error = guard
            .finish(Err::<(), _>(DbError::Cancelled))
            .expect_err("cleanup failures must not return bare cancellation");
        assert!(error.to_string().contains("rollback failure"));
        assert!(
            error
                .to_string()
                .contains("foreign-key restoration failure")
        );
        assert!(
            !state.is_autocommit(),
            "an injected rollback failure must leave the guard transaction active"
        );
        drop(state);
        assert!(
            SqliteConnectionState::lock_checked(&shared).is_err(),
            "uncertain cancellation cleanup must quarantine all aliases"
        );
    }

    #[test]
    fn rebuild_transaction_guard_setup_cleanup_and_drop_fail_closed() {
        let shared = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        let mut state = shared.lock().expect("state mutex should not be poisoned");
        state
            .execute_batch("PRAGMA foreign_keys = ON; CREATE TABLE sample (value INTEGER)")
            .expect("fixture should be created");
        assert!(
            super::RebuildTransactionGuard::begin_with_faults(
                &mut state,
                super::RebuildGuardFaults {
                    fail_disable_readback: true,
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert!(
            super::RebuildTransactionGuard::begin_with_faults(
                &mut state,
                super::RebuildGuardFaults {
                    fail_begin: true,
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert_eq!(
            state
                .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                .expect("setup cleanup must restore FK"),
            1
        );
        {
            let guard = super::RebuildTransactionGuard::begin(&mut state)
                .expect("drop fixture should begin");
            guard
                .connection()
                .execute("INSERT INTO sample VALUES (7)", [])
                .expect("mutation should succeed");
        }
        assert!(state.is_autocommit());
        assert_eq!(
            state
                .query_row("SELECT COUNT(*) FROM sample", [], |row| row
                    .get::<_, i64>(0))
                .expect("drop rollback should work"),
            0
        );
        drop(state);
        assert!(
            SqliteConnectionState::lock_checked(&shared).is_err(),
            "unfinished guard drop must quarantine aliases"
        );
    }

    #[test]
    fn rebuild_copy_intent_keeps_exact_projections_and_declares_all_typed_comparisons() {
        for (label, identity, retained, expected) in [
            (
                "genuine INTEGER PRIMARY KEY",
                "id",
                vec!["payload", "inline_desc", "table_desc"],
                vec!["id", "payload", "inline_desc", "table_desc"],
            ),
            (
                "hidden rowid",
                "_rowid_",
                vec!["id", "payload", "inline_desc", "table_desc"],
                vec!["_rowid_", "id", "payload", "inline_desc", "table_desc"],
            ),
        ] {
            let copy = super::RebuildCopyIntent::new(
                identity.to_string(),
                retained.into_iter().map(str::to_string).collect(),
            );
            let expected = expected.into_iter().map(str::to_string).collect::<Vec<_>>();
            assert_eq!(copy.identity, identity, "{label} identity must be retained");
            assert_eq!(
                copy.source_projection, expected,
                "{label} source order must be exact"
            );
            assert_eq!(
                copy.replacement_projection, copy.source_projection,
                "{label} replacement projection must exactly equal the source projection"
            );
            assert_eq!(
                copy.source_projection
                    .iter()
                    .filter(|column| column.eq_ignore_ascii_case(identity))
                    .count(),
                1,
                "{label} identity must occur exactly once"
            );
            assert_eq!(
                copy.quoted_projection(),
                copy.source_projection
                    .iter()
                    .map(|column| format!("\"{column}\""))
                    .collect::<Vec<_>>()
                    .join(", "),
                "{label} projection must only quote retained names"
            );
            assert!(copy.comparison.ordered_rows);
            assert!(copy.comparison.row_count);
            assert!(copy.comparison.integer_identity);
            assert!(copy.comparison.storage_class);
            assert!(copy.comparison.exact_integer);
            assert!(copy.comparison.real_bits);
            assert!(copy.comparison.text_and_blob_bytes);
            assert!(copy.comparison.invalid_utf8_and_nul);
        }

        let inline = super::parse_create_table(
            "CREATE TABLE inline_desc(id INTEGER PRIMARY KEY DESC, payload TEXT)",
        )
        .expect("inline DESC primary key should parse");
        let table = super::parse_create_table(
            "CREATE TABLE table_desc(id INTEGER, payload TEXT, PRIMARY KEY(id DESC))",
        )
        .expect("table DESC primary key should parse");
        assert!(inline.key_declarations()[0].terms[0].descending);
        assert!(table.key_declarations()[0].terms[0].descending);
        assert_ne!(
            inline.key_declarations()[0].origin,
            table.key_declarations()[0].origin,
            "inline and table DESC declarations must remain distinguishable"
        );
    }

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
    fn old_engine_pure_drop_uses_read_only_rebuild_route() {
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
        *SQLITE_ENGINE_VERSION_OVERRIDE
            .lock()
            .expect("engine-version test hook mutex should not be poisoned") =
            Some((std::thread::current().id(), 3_034_000));
        let plan = SqliteConnection::for_test(state)
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam")
            .prepare(&TableAlterRequest {
                table: TableRef::new("people"),
                operations: vec![TableAlterOperation::DropColumn {
                    name: "obsolete".to_string(),
                }],
                expected_before: Vec::new(),
            })
            .expect("older SQLite pure drop should prepare a conservative rebuild");
        assert_eq!(plan.preview().route, dbflux_core::TableAlterRoute::Rebuild);
    }

    fn file_rebuild_fixture(
        file_name: &str,
        setup: &str,
    ) -> (
        std::path::PathBuf,
        std::path::PathBuf,
        Arc<Mutex<SqliteConnectionState>>,
        super::NativeConnectionSettings,
    ) {
        let directory = tempfile::tempdir().expect("test directory should be created");
        let database_path = directory.path().join(file_name);
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open(&database_path)
                .expect("primary SQLite connection should open"),
        )));
        state
            .lock()
            .expect("primary state mutex should not be poisoned")
            .execute_batch(setup)
            .expect("two-connection fixture should be created");
        let settings = {
            let state_guard = state
                .lock()
                .expect("primary state mutex should not be poisoned");
            super::capture_native_connection_settings(&state_guard)
                .expect("fixture connection settings should be readable")
        };
        let retained_path = directory.keep();
        (retained_path, database_path, state, settings)
    }

    fn rebuild_alter_and_drop_request() -> TableAlterRequest {
        TableAlterRequest {
            table: TableRef::new("target"),
            operations: vec![
                TableAlterOperation::AlterColumn {
                    name: "payload".to_string(),
                    new_type: Some("VARCHAR(16)".to_string()),
                    nullable: None,
                    default: None,
                },
                TableAlterOperation::DropColumn {
                    name: "obsolete".to_string(),
                },
            ],
            expected_before: Vec::new(),
        }
    }

    fn install_before_rebuild_write_lock_hook(hook: Box<dyn FnOnce() + Send>) {
        *BEFORE_REBUILD_WRITE_LOCK
            .lock()
            .expect("write-lock hook mutex should not be poisoned") =
            Some((std::thread::current().id(), hook));
    }

    fn clear_before_rebuild_write_lock_hook() {
        *BEFORE_REBUILD_WRITE_LOCK
            .lock()
            .expect("write-lock hook mutex should not be poisoned") = None;
    }

    fn assert_rebuild_boundary(
        state: &Arc<Mutex<SqliteConnectionState>>,
        settings_before: &super::NativeConnectionSettings,
        private_object_count: i64,
    ) {
        let state_guard = state
            .lock()
            .expect("primary state mutex should not be poisoned");
        assert!(
            state_guard.is_autocommit(),
            "execution must restore autocommit"
        );
        assert_eq!(
            super::capture_native_connection_settings(&state_guard)
                .expect("connection settings should remain readable"),
            settings_before.clone(),
            "execution must preserve every protected connection setting"
        );
        assert_eq!(
            state_guard
                .query_row(
                    "SELECT COUNT(*) FROM main.sqlite_master WHERE name LIKE '__dbflux_rebuild_%'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("private catalog should be readable"),
            private_object_count,
            "the expected private catalog boundary must be retained"
        );
        drop(state_guard);
        assert!(
            SqliteConnectionState::lock_checked(state).is_ok(),
            "execution must leave the primary connection usable"
        );
    }

    #[test]
    fn execute_guarded_rebuild_rejects_schema_change_in_preflight_to_write_lock_gap() {
        let _hook_lock = REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild test hook mutex should not be poisoned");
        let (_directory, database_path, state, settings_before) = file_rebuild_fixture(
            "rebuild-write-lock-schema-race.db",
            "PRAGMA foreign_keys = ON; \
             CREATE TABLE target (id INTEGER PRIMARY KEY, payload TEXT, retained TEXT, obsolete TEXT, parent_id INTEGER REFERENCES parent(id)); \
             CREATE TABLE parent (id INTEGER PRIMARY KEY); \
             INSERT INTO parent VALUES (1); \
             INSERT INTO target VALUES (7, 'prepared', 'before-gap', 'remove', 1)",
        );
        let request = rebuild_alter_and_drop_request();
        let plan = prepare_rebuild_plan_for_test(state.clone(), &request)
            .expect("private rebuild plan should prepare before the external change");
        let competing_path = database_path.clone();
        install_before_rebuild_write_lock_hook(Box::new(move || {
            RusqliteConnection::open(competing_path)
                .expect("external SQLite connection should open")
                .execute_batch(
                    "ALTER TABLE target ADD COLUMN external_change TEXT DEFAULT 'committed'; \
                     UPDATE target SET retained = 'externally-committed' WHERE id = 7",
                )
                .expect("external schema change should commit in the preflight-to-lock gap");
        }));

        let execution = plan.execute_guarded();
        clear_before_rebuild_write_lock_hook();
        let error =
            execution.expect_err("under-lock freshness must reject the external schema change");
        assert!(
            error.to_string().contains("stale"),
            "the error must identify stale under-lock state: {error}"
        );

        {
            let state_guard = state
                .lock()
                .expect("primary state mutex should not be poisoned");
            assert_eq!(
                state_guard
                    .query_row(
                        "SELECT obsolete || ':' || external_change || ':' || retained FROM target WHERE id = 7",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .expect("external committed row should remain readable"),
                "remove:committed:externally-committed",
                "rejection must preserve both source data and the committed external change"
            );
            assert_eq!(
                state_guard
                    .query_row(
                        "SELECT COUNT(*) FROM pragma_table_info('target') WHERE name = 'obsolete'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .expect("source schema should be readable"),
                1,
                "stale rejection must not mutate the selected source schema"
            );
        }
        assert_rebuild_boundary(&state, &settings_before, 0);
    }

    #[test]
    fn execute_guarded_rebuild_preserves_external_private_replacement_name_created_before_lock() {
        let _hook_lock = REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild test hook mutex should not be poisoned");
        let (_directory, database_path, state, settings_before) = file_rebuild_fixture(
            "rebuild-write-lock-name-race.db",
            "PRAGMA foreign_keys = ON; \
             CREATE TABLE target (id INTEGER PRIMARY KEY, payload TEXT, retained TEXT, obsolete TEXT); \
             INSERT INTO target VALUES (7, 'prepared', 'before-gap', 'remove')",
        );
        let request = rebuild_alter_and_drop_request();
        let plan = prepare_rebuild_plan_for_test(state.clone(), &request)
            .expect("private rebuild plan should prepare before the replacement-name collision");
        let competing_path = database_path.clone();
        install_before_rebuild_write_lock_hook(Box::new(move || {
            RusqliteConnection::open(competing_path)
                .expect("external SQLite connection should open")
                .execute_batch(
                    "CREATE TABLE __dbflux_rebuild_target (id INTEGER PRIMARY KEY, external_payload TEXT); \
                     INSERT INTO __dbflux_rebuild_target VALUES (91, 'externally-owned')",
                )
                .expect("external replacement-name object should commit in the preflight-to-lock gap");
        }));

        let execution = plan.execute_guarded();
        clear_before_rebuild_write_lock_hook();
        let error =
            execution.expect_err("under-lock collision check must reject the external object");
        assert!(
            error
                .to_string()
                .contains("private replacement name now collides"),
            "the error must identify the externally-created private name: {error}"
        );

        {
            let state_guard = state
                .lock()
                .expect("primary state mutex should not be poisoned");
            assert_eq!(
                state_guard
                    .query_row(
                        "SELECT id || ':' || external_payload FROM __dbflux_rebuild_target",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .expect("external replacement object should remain readable"),
                "91:externally-owned",
                "rejection must not destroy externally-owned replacement data"
            );
            assert_eq!(
                state_guard
                    .query_row(
                        "SELECT obsolete || ':' || retained FROM target WHERE id = 7",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .expect("source table should remain readable"),
                "remove:before-gap",
                "collision rejection must not mutate the source table"
            );
        }
        assert_rebuild_boundary(&state, &settings_before, 1);
    }

    #[test]
    fn public_rebuild_copies_rows_committed_after_preparation() {
        let (_directory, database_path, state, settings_before) = file_rebuild_fixture(
            "rebuild-fresh-data.db",
            "PRAGMA foreign_keys = ON; \
             CREATE TABLE parent (id INTEGER PRIMARY KEY); \
             CREATE TABLE target (id INTEGER PRIMARY KEY, payload TEXT, retained TEXT, obsolete TEXT, parent_id INTEGER REFERENCES parent(id)); \
             CREATE INDEX target_retained ON target(retained); \
             INSERT INTO parent VALUES (1); \
             INSERT INTO target VALUES (7, 'prepared', 'before-update', 'remove', 1)",
        );
        let request = rebuild_alter_and_drop_request();
        let public_plan = prepare_rebuild_plan_for_test(state.clone(), &request)
            .expect("public rebuild plan should prepare before the external data commit");
        RusqliteConnection::open(&database_path)
            .expect("external SQLite connection should open")
            .execute_batch(
                "UPDATE target SET payload = 'externally-updated', retained = 'after-update' WHERE id = 7; \
                 INSERT INTO target VALUES (19, 'externally-inserted', 'after-insert', 'remove', 1)",
            )
            .expect("external data changes should commit without a schema change");

        let outcome = public_plan
            .execute()
            .expect("public executor must copy data committed after preparation");
        assert_eq!(outcome.statement_count, 5);
        let state_guard = state
            .lock()
            .expect("primary state mutex should not be poisoned");
        let mut statement = state_guard
            .prepare("SELECT id, payload, retained, parent_id FROM target ORDER BY id")
            .expect("rebuilt rows should be queryable");
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .expect("rebuilt rows should decode")
            .collect::<Result<Vec<_>, _>>()
            .expect("rebuilt row iteration should succeed");
        assert_eq!(
            rows,
            vec![
                (
                    7,
                    "externally-updated".to_string(),
                    "after-update".to_string(),
                    1
                ),
                (
                    19,
                    "externally-inserted".to_string(),
                    "after-insert".to_string(),
                    1
                ),
            ],
            "private execution must copy fresh external values and identities, not prepared data"
        );
        assert_eq!(
            state_guard
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('target') WHERE name = 'obsolete'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("rebuilt schema should be readable"),
            0,
            "private execution must apply the requested rebuild mutation"
        );
        assert_eq!(
            state_guard
                .query_row(
                    "SELECT sql FROM main.sqlite_master WHERE type = 'index' AND name = 'target_retained'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .expect("explicit index should be restored"),
            "CREATE INDEX target_retained ON target(retained)"
        );
        drop(statement);
        drop(state_guard);
        assert_rebuild_boundary(&state, &settings_before, 0);
    }

    #[test]
    fn rebuild_capture_rejects_schema_disagreement_between_read_only_passes() {
        let _hook_lock = REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild test hook mutex should not be poisoned");
        let directory = tempfile::tempdir().expect("test directory should be created");
        let database_path = directory.path().join("rebuild-capture-race.db");
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open(&database_path).expect("file-backed SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch("CREATE TABLE main.people (id INTEGER PRIMARY KEY, payload TEXT)")
            .expect("test table should be created");
        let competing_path = database_path.clone();
        *BEFORE_REBUILD_CAPTURE_SECOND_READ
            .lock()
            .expect("rebuild capture test hook mutex should not be poisoned") = Some((
            std::thread::current().id(),
            Box::new(move || {
                RusqliteConnection::open(competing_path)
                    .expect("second SQLite connection should open")
                    .execute_batch("ALTER TABLE main.people ADD COLUMN concurrent_change TEXT")
                    .expect("second connection should change the schema between capture passes");
            }),
        ));

        let error = match SqliteConnection::for_test(state.clone())
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam")
            .prepare(&TableAlterRequest {
                table: TableRef::new("people"),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "payload".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            }) {
            Ok(_) => panic!("inconsistent rebuild capture must reject planning"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("changed during planning"),
            "the rejection must identify the inconsistent capture: {error}"
        );
        assert_eq!(
            state
                .lock()
                .expect("state mutex should not be poisoned")
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('people') WHERE name = 'payload'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("selected column inspection should succeed"),
            1,
            "capture rejection must not alter the selected source column"
        );
    }

    #[test]
    fn rebuild_dependency_guard_uses_captured_foreign_keys_after_live_schema_changes() {
        let _hook_lock = REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild test hook mutex should not be poisoned");
        let directory = tempfile::tempdir().expect("test directory should be created");
        let database_path = directory.path().join("rebuild-observation.db");
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open(&database_path).expect("file-backed SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch(
                "CREATE TABLE main.parent (id INTEGER PRIMARY KEY); \
                     CREATE TABLE main.child (a INTEGER REFERENCES parent(id), payload TEXT)",
            )
            .expect("foreign-key fixture should be created");
        let foreign_keys_before = state
            .lock()
            .expect("state mutex should not be poisoned")
            .query_row(
                "SELECT COUNT(*) FROM pragma_foreign_key_list('child')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("fixture child foreign key should be readable");
        assert_eq!(foreign_keys_before, 1, "fixture must start with the FK");

        let competing_path = database_path.clone();
        *AFTER_REBUILD_OBSERVATION_CAPTURE
            .lock()
            .expect("rebuild observation test hook mutex should not be poisoned") = Some((
            std::thread::current().id(),
            Box::new(move || {
                RusqliteConnection::open(competing_path)
                    .expect("second SQLite connection should open")
                    .execute_batch(
                        "PRAGMA foreign_keys = OFF; \
                             BEGIN IMMEDIATE; \
                             CREATE TABLE main.child_replacement (a INTEGER, payload TEXT); \
                             INSERT INTO main.child_replacement SELECT a, payload FROM main.child; \
                             DROP TABLE main.child; \
                             ALTER TABLE main.child_replacement RENAME TO child; \
                             COMMIT",
                    )
                    .expect("test hook should replace child with identical columns but no FK");
            }),
        ));

        let result = SqliteConnection::for_test(state.clone())
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam")
            .prepare(&TableAlterRequest {
                table: TableRef::new("child"),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "a".to_string(),
                    new_type: Some("BIGINT".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            });

        let error = match result {
            Ok(_) => panic!(
                "captured child FK must reject alteration even after the live definition loses it"
            ),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("foreign key from main.child.a"),
            "the captured dependency must be actionable: {error}"
        );
        assert_eq!(
            state
                .lock()
                .expect("state mutex should not be poisoned")
                .query_row(
                    "SELECT COUNT(*) FROM pragma_foreign_key_list('child')",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("mutated child foreign keys should be readable"),
            0,
            "the test hook must remove the live FK after capture"
        );
    }

    #[test]
    fn rebuild_preview_uses_captured_identity_and_index_facts_after_live_schema_changes() {
        let _hook_lock = REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild test hook mutex should not be poisoned");
        let directory = tempfile::tempdir().expect("test directory should be created");
        let database_path = directory.path().join("rebuild-preview-observation.db");
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open(&database_path).expect("file-backed SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch(
                "CREATE TABLE main.people (a INTEGER, payload TEXT); \
                     CREATE INDEX main.people_a ON people(a)",
            )
            .expect("captured identity and index fixture should be created");
        let competing_path = database_path.clone();
        *AFTER_REBUILD_OBSERVATION_CAPTURE
            .lock()
            .expect("rebuild observation test hook mutex should not be poisoned") = Some((
            std::thread::current().id(),
            Box::new(move || {
                RusqliteConnection::open(competing_path)
                    .expect("second SQLite connection should open")
                    .execute_batch(
                        "BEGIN IMMEDIATE; \
                             CREATE TABLE main.people_replacement (
                                rowid TEXT, _rowid_ TEXT, oid TEXT, payload TEXT
                             ); \
                             DROP TABLE main.people; \
                             ALTER TABLE main.people_replacement RENAME TO people; \
                             COMMIT",
                    )
                    .expect("test hook should replace the live identity and remove the index");
            }),
        ));

        let plan = SqliteConnection::for_test(state.clone())
            .table_alter_planner()
            .expect("SQLite must opt into the table-alter planner seam")
            .prepare(&TableAlterRequest {
                table: TableRef::new("people"),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "payload".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })
            .expect("preview must use the captured rowid and explicit index facts");
        assert!(
            plan.preview()
                .statements
                .iter()
                .any(|statement| statement.contains("SELECT \"rowid\"")),
            "captured hidden identity must drive the copy preview"
        );
        assert!(
            plan.preview().statements.iter().any(|statement| {
                statement.contains(
                    "restore exact index main.people_a: CREATE INDEX people_a ON people(a)",
                )
            }),
            "captured explicit index SQL must remain in the preview"
        );
        assert_eq!(
            state
                .lock()
                .expect("state mutex should not be poisoned")
                .query_row(
                    "SELECT COUNT(*) FROM pragma_index_list('people') WHERE name = 'people_a'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("mutated live index metadata should be readable"),
            0,
            "the test hook must remove the live index after capture"
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
    fn foreign_key_resolution_uses_pk_ordinal_and_rejects_incomplete_captures() {
        let table = |name: &str, columns: &[(&str, i64)]| TableMetadataCapture {
            name: name.to_string(),
            columns: columns
                .iter()
                .map(|(name, primary_key_order)| TableColumnCapture {
                    name: (*name).to_string(),
                    type_name: "INTEGER".to_string(),
                    nullable: true,
                    default: None,
                    primary_key_order: *primary_key_order,
                    hidden: 0,
                })
                .collect(),
            rowid_aliases: Vec::new(),
        };
        let metadata = vec![
            table("parent", &[("a", 2), ("b", 1)]),
            table("child", &[("x", 0), ("y", 0), ("payload", 0)]),
        ];
        let relationship_rows = vec![
            ForeignKeyCapture {
                child_table: "child".to_string(),
                relation_id: 4,
                sequence: 0,
                parent_table: "parent".to_string(),
                child_column: "x".to_string(),
                parent_column: None,
                on_update: "NO ACTION".to_string(),
                on_delete: "NO ACTION".to_string(),
                match_name: "NONE".to_string(),
            },
            ForeignKeyCapture {
                child_table: "child".to_string(),
                relation_id: 4,
                sequence: 1,
                parent_table: "parent".to_string(),
                child_column: "y".to_string(),
                parent_column: None,
                on_update: "NO ACTION".to_string(),
                on_delete: "NO ACTION".to_string(),
                match_name: "NONE".to_string(),
            },
        ];
        let resolved = resolve_foreign_key_relationships("child", &metadata, &relationship_rows)
            .expect("implicit composite parent columns should resolve from PK ordinal order");
        assert_eq!(resolved[0].parent_columns, ["b", "a"]);
        assert!(resolved[0].declared_parent_columns.is_none());
        let parsed = super::syntax::parse_create_table(
            "CREATE TABLE child (x INTEGER, y INTEGER, payload TEXT, FOREIGN KEY (x, y) REFERENCES parent)",
        )
        .expect("bounded child source should parse");
        let mut action_disagreement = relationship_rows.clone();
        for row in &mut action_disagreement {
            row.on_delete = "CASCADE".to_string();
        }
        let resolved_actions =
            resolve_foreign_key_relationships("child", &metadata, &action_disagreement)
                .expect("consistent captured action rows should resolve");
        assert!(super::validate_parsed_foreign_keys("child", &parsed, &resolved_actions).is_err());
        assert!(
            super::reject_rebuild_dependencies(
                "child",
                &["x".to_string()],
                &metadata[1].columns,
                &[],
                &resolved,
            )
            .is_err()
        );
        assert!(
            super::reject_rebuild_dependencies(
                "parent",
                &["b".to_string()],
                &metadata[0].columns,
                &[],
                &resolved,
            )
            .is_err()
        );

        let mut non_contiguous = relationship_rows.clone();
        non_contiguous[1].sequence = 2;
        assert!(resolve_foreign_key_relationships("child", &metadata, &non_contiguous).is_err());

        let arity_mismatch = vec![relationship_rows[0].clone()];
        assert!(resolve_foreign_key_relationships("child", &metadata, &arity_mismatch).is_err());

        let mut missing_parent = relationship_rows.clone();
        for row in &mut missing_parent {
            row.parent_table = "missing_parent".to_string();
        }
        assert!(resolve_foreign_key_relationships("child", &metadata, &missing_parent).is_err());

        let mut mismatched_actions = relationship_rows.clone();
        mismatched_actions[1].on_delete = "CASCADE".to_string();
        assert!(
            resolve_foreign_key_relationships("child", &metadata, &mismatched_actions).is_err()
        );
    }

    #[test]
    fn rebuild_semantic_proof_rejects_unrequested_selected_column_fact_changes() {
        let source = super::syntax::parse_create_table(
            "CREATE TABLE t (payload TEXT COLLATE NOCASE CONSTRAINT payload_not_null NOT NULL CONSTRAINT payload_default DEFAULT 'keep' UNIQUE, untouched TEXT DEFAULT X'0A')",
        )
        .expect("source definition should parse");
        let tampered = super::syntax::parse_create_table(
            "CREATE TABLE t (payload VARCHAR(9) COLLATE BINARY DEFAULT NULL, untouched TEXT DEFAULT X'0A')",
        )
        .expect("tampered definition should parse");
        let changes = super::RebuildChanges {
            alterations: vec![super::ColumnChange {
                name: "payload".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            drops: Vec::new(),
        };

        let error = super::validate_rebuild_semantic_delta("t", &source, &tampered, &changes)
            .expect_err(
                "semantic proof must reject unrequested nullable/default/collation/constraint changes",
            );
        assert!(
            error.to_string().contains("semantic proof"),
            "the rejection must identify the semantic proof: {error}"
        );
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum RawRebuildValue {
        Null,
        Integer(i64),
        Real(u64),
        Text(Vec<u8>),
        Blob(Vec<u8>),
    }

    fn raw_rebuild_rows(connection: &RusqliteConnection, sql: &str) -> Vec<Vec<RawRebuildValue>> {
        let mut statement = connection
            .prepare(sql)
            .expect("raw rebuild snapshot query should prepare");
        let column_count = statement.column_count();
        let mut rows = statement
            .query([])
            .expect("raw rebuild snapshot query should execute");
        let mut snapshot = Vec::new();
        while let Some(row) = rows.next().expect("raw rebuild snapshot should advance") {
            let values = (0..column_count)
                .map(|column| {
                    match row
                        .get_ref(column)
                        .expect("raw rebuild snapshot value should decode")
                    {
                        rusqlite::types::ValueRef::Null => RawRebuildValue::Null,
                        rusqlite::types::ValueRef::Integer(value) => {
                            RawRebuildValue::Integer(value)
                        }
                        rusqlite::types::ValueRef::Real(value) => {
                            RawRebuildValue::Real(value.to_bits())
                        }
                        rusqlite::types::ValueRef::Text(value) => {
                            RawRebuildValue::Text(value.to_vec())
                        }
                        rusqlite::types::ValueRef::Blob(value) => {
                            RawRebuildValue::Blob(value.to_vec())
                        }
                    }
                })
                .collect();
            snapshot.push(values);
        }
        snapshot
    }

    fn rebuild_schema_snapshot(connection: &RusqliteConnection) -> Vec<Vec<RawRebuildValue>> {
        raw_rebuild_rows(
            connection,
            "SELECT type, name, tbl_name, sql FROM main.sqlite_master \
                 WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
        )
    }

    #[test]
    fn rebuild_plan_executes_private_lifecycle_and_restores_foreign_keys() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        state
                .lock()
                .expect("state mutex should not be poisoned")
                .execute_batch(
                    "PRAGMA foreign_keys = ON; \
                     CREATE TABLE parent (id INTEGER PRIMARY KEY); \
                     CREATE TABLE target (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, retained TEXT, obsolete TEXT, parent_id INTEGER REFERENCES parent(id)); \
                     CREATE INDEX target_retained ON target(retained); \
                     INSERT INTO parent VALUES (1); \
                     INSERT INTO target VALUES (-7, 'raw payload', 'keep', 'remove', 1)",
                )
                .expect("rebuild fixture should be created");
        let request = TableAlterRequest {
            table: TableRef::new("target"),
            operations: vec![
                TableAlterOperation::AlterColumn {
                    name: "payload".to_string(),
                    new_type: Some("VARCHAR(16)".to_string()),
                    nullable: Some(false),
                    default: Some(dbflux_core::OwnedDefaultSpec::Set("'future'".to_string())),
                },
                TableAlterOperation::DropColumn {
                    name: "obsolete".to_string(),
                },
            ],
            expected_before: Vec::new(),
        };
        let plan = prepare_rebuild_plan_for_test(state.clone(), &request)
            .expect("rebuild plan should prepare without mutation");

        let outcome = plan
            .execute_guarded()
            .expect("private rebuild execution should commit its lifecycle");
        assert_eq!(outcome.statement_count, 5);
        let state = state.lock().expect("state mutex should not be poisoned");
        assert!(state.is_autocommit());
        assert_eq!(
            state
                .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                .expect("foreign key setting should be readable"),
            1
        );
        assert_eq!(
            state
                .query_row("SELECT id FROM target", [], |row| row.get::<_, i64>(0))
                .expect("rebuilt identity should be retained"),
            -7
        );
        assert_eq!(
            state
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('target') WHERE name = 'obsolete'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("rebuilt columns should be readable"),
            0
        );
        assert_eq!(
                state
                    .query_row(
                        "SELECT sql FROM main.sqlite_master WHERE type = 'index' AND name = 'target_retained'",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .expect("explicit index should be restored"),
                "CREATE INDEX target_retained ON target(retained)"
            );
    }

    #[test]
    fn public_rebuild_preserves_raw_values_for_integer_primary_key_and_hidden_rowid() {
        for (label, create_target, insert_target, identity) in [
            (
                "INTEGER PRIMARY KEY",
                "CREATE TABLE target (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, nullable_value TEXT, integer_value INTEGER, real_value REAL, text_value TEXT, blob_value BLOB, retained TEXT, obsolete TEXT, parent_id INTEGER REFERENCES parent(id))",
                "INSERT INTO target VALUES (-19, CAST(X'80' AS TEXT), NULL, -9223372036854775807, -0.0, CAST(X'6F006B' AS TEXT), X'00FF', 'keep-negative', 'remove-negative', 1); INSERT INTO target VALUES (73, 'ordinary', 'nullable', 17, 1.5, 'ordinary text', X'1020', 'keep-sparse', 'remove-sparse', 1)",
                "id",
            ),
            (
                "hidden rowid",
                "CREATE TABLE target (payload TEXT NOT NULL, nullable_value TEXT, integer_value INTEGER, real_value REAL, text_value TEXT, blob_value BLOB, retained TEXT, obsolete TEXT, parent_id INTEGER REFERENCES parent(id))",
                "INSERT INTO target(rowid, payload, nullable_value, integer_value, real_value, text_value, blob_value, retained, obsolete, parent_id) VALUES (-19, CAST(X'80' AS TEXT), NULL, -9223372036854775807, -0.0, CAST(X'6F006B' AS TEXT), X'00FF', 'keep-negative', 'remove-negative', 1); INSERT INTO target(rowid, payload, nullable_value, integer_value, real_value, text_value, blob_value, retained, obsolete, parent_id) VALUES (73, 'ordinary', 'nullable', 17, 1.5, 'ordinary text', X'1020', 'keep-sparse', 'remove-sparse', 1)",
                "rowid",
            ),
        ] {
            let state = Arc::new(Mutex::new(SqliteConnectionState::new(
                RusqliteConnection::open_in_memory().expect("fixture SQLite should open"),
            )));
            state.lock().expect("state mutex should not be poisoned").execute_batch(&format!(
                    "PRAGMA foreign_keys = ON; CREATE TABLE parent (id INTEGER PRIMARY KEY); {create_target}; CREATE INDEX target_retained_desc ON target(retained DESC); INSERT INTO parent VALUES (1); {insert_target}",
                )).expect("rich rebuild fixture should be created");
            let request = TableAlterRequest {
                table: TableRef::new("target"),
                operations: vec![
                    TableAlterOperation::AlterColumn {
                        name: "payload".to_string(),
                        new_type: Some("VARCHAR(16)".to_string()),
                        nullable: Some(false),
                        default: Some(dbflux_core::OwnedDefaultSpec::Set("'future'".to_string())),
                    },
                    TableAlterOperation::DropColumn {
                        name: "obsolete".to_string(),
                    },
                ],
                expected_before: Vec::new(),
            };
            let query = format!(
                "SELECT {identity}, payload, nullable_value, integer_value, real_value, text_value, blob_value, retained, parent_id FROM target ORDER BY {identity}"
            );
            let (raw_before, index_before, foreign_keys_before, settings_before) = {
                let state = state.lock().expect("state mutex should not be poisoned");
                (
                    raw_rebuild_rows(&state, &query),
                    raw_rebuild_rows(
                        &state,
                        "SELECT sql FROM main.sqlite_master WHERE type = 'index' AND name = 'target_retained_desc'",
                    ),
                    raw_rebuild_rows(
                        &state,
                        "SELECT id, seq, \"table\", \"from\", \"to\", on_update, on_delete, match FROM pragma_foreign_key_list('target') ORDER BY id, seq",
                    ),
                    super::capture_native_connection_settings(&state)
                        .expect("protected settings should be captured"),
                )
            };
            assert!(
                raw_before
                    .iter()
                    .flatten()
                    .any(|value| matches!(value, RawRebuildValue::Null))
            );
            assert!(
                raw_before
                    .iter()
                    .flatten()
                    .any(|value| matches!(value, RawRebuildValue::Integer(-9223372036854775807)))
            );
            assert!(raw_before.iter().flatten().any(
                |value| matches!(value, RawRebuildValue::Real(bits) if *bits == 1.5_f64.to_bits())
            ));
            assert!(
                raw_before
                    .iter()
                    .flatten()
                    .any(|value| matches!(value, RawRebuildValue::Text(bytes) if bytes == b"\x80"))
            );
            assert!(
                raw_before
                    .iter()
                    .flatten()
                    .any(|value| matches!(value, RawRebuildValue::Text(bytes) if bytes == b"o\0k"))
            );
            assert!(
                raw_before.iter().flatten().any(
                    |value| matches!(value, RawRebuildValue::Blob(bytes) if bytes == b"\0\xff")
                )
            );

            prepare_rebuild_plan_for_test(state.clone(), &request)
                .expect("public execution fixture should prepare")
                .execute()
                .unwrap_or_else(|error| panic!("{label} public rebuild should succeed: {error}"));

            let state = state.lock().expect("state mutex should not be poisoned");
            assert!(state.is_autocommit(), "{label} must restore autocommit");
            assert_eq!(
                state
                    .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                    .expect("foreign key setting should be readable"),
                1
            );
            assert_eq!(
                super::capture_native_connection_settings(&state)
                    .expect("protected settings should remain readable"),
                settings_before,
                "successful rebuild must restore every protected connection setting"
            );
            assert_eq!(
                raw_rebuild_rows(&state, &query),
                raw_before,
                "{label} must retain raw source values"
            );
            assert_eq!(
                raw_rebuild_rows(
                    &state,
                    "SELECT sql FROM main.sqlite_master WHERE type = 'index' AND name = 'target_retained_desc'"
                ),
                index_before,
                "{label} must restore the exact explicit index"
            );
            assert_eq!(
                raw_rebuild_rows(
                    &state,
                    "SELECT id, seq, \"table\", \"from\", \"to\", on_update, on_delete, match FROM pragma_foreign_key_list('target') ORDER BY id, seq"
                ),
                foreign_keys_before,
                "{label} must preserve foreign-key metadata"
            );
            let final_columns = raw_rebuild_rows(
                &state,
                "SELECT name, type, \"notnull\", dflt_value, pk, hidden FROM pragma_table_xinfo('target') ORDER BY cid",
            );
            assert!(final_columns.iter().any(|column| {
                column
                    == &vec![
                        RawRebuildValue::Text(b"payload".to_vec()),
                        RawRebuildValue::Text(b"VARCHAR(16)".to_vec()),
                        RawRebuildValue::Integer(1),
                        RawRebuildValue::Text(b"'future'".to_vec()),
                        RawRebuildValue::Integer(0),
                        RawRebuildValue::Integer(0),
                    ]
            }));
            assert!(
                final_columns.iter().all(|column| {
                    !matches!(column.first(), Some(RawRebuildValue::Text(name)) if name == b"obsolete")
                }),
                "final column metadata must remove only the selected column"
            );
            assert_eq!(
                state
                    .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .expect("foreign key check should be readable"),
                0
            );
            state.execute_batch(&format!(
                    "INSERT INTO target ({identity}, nullable_value, integer_value, real_value, text_value, blob_value, retained, parent_id) VALUES (1001, NULL, 0, 0.0, 'new', X'01', 'future row', 1)"
                )).expect("future row should receive the new default");
            assert_eq!(
                raw_rebuild_rows(
                    &state,
                    &format!("SELECT payload FROM target WHERE {identity} = 1001")
                ),
                vec![vec![RawRebuildValue::Text(b"future".to_vec())]],
                "{label} default must affect only future inserts"
            );
        }
    }

    fn assert_guarded_rollback(
        state: &Arc<Mutex<SqliteConnectionState>>,
        schema_before: Vec<Vec<RawRebuildValue>>,
        rows_before: Vec<Vec<RawRebuildValue>>,
        settings_before: super::NativeConnectionSettings,
    ) {
        let state_guard = state.lock().expect("state mutex should not be poisoned");
        assert!(state_guard.is_autocommit());
        assert_eq!(rebuild_schema_snapshot(&state_guard), schema_before);
        assert_eq!(
            raw_rebuild_rows(
                &state_guard,
                "SELECT id, payload, retained, obsolete, parent_id FROM target"
            ),
            rows_before
        );
        assert_eq!(
            super::capture_native_connection_settings(&state_guard)
                .expect("connection settings should remain readable"),
            settings_before
        );
        assert_eq!(
            state_guard
                .query_row(
                    "SELECT COUNT(*) FROM main.sqlite_master WHERE name LIKE '__dbflux_rebuild_%'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .expect("private catalog should be readable"),
            0
        );
        drop(state_guard);
        assert!(SqliteConnectionState::lock_checked(state).is_ok());
    }

    #[test]
    fn execute_guarded_rebuild_rejects_incompatible_affinity_and_rolls_back() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("fixture SQLite should open"),
        )));
        state.lock().expect("state mutex should not be poisoned").execute_batch(
                "PRAGMA foreign_keys = ON; CREATE TABLE parent (id INTEGER PRIMARY KEY); CREATE TABLE target (id INTEGER PRIMARY KEY, payload TEXT, retained TEXT, obsolete TEXT, parent_id INTEGER REFERENCES parent(id)); CREATE INDEX target_retained_desc ON target(retained DESC); INSERT INTO parent VALUES (1); INSERT INTO target VALUES (-7, '0007', 'keep', 'remove', 1)",
            ).expect("affinity fixture should be created");
        let request = TableAlterRequest {
            table: TableRef::new("target"),
            operations: vec![
                TableAlterOperation::AlterColumn {
                    name: "payload".to_string(),
                    new_type: Some("INTEGER".to_string()),
                    nullable: None,
                    default: None,
                },
                TableAlterOperation::DropColumn {
                    name: "obsolete".to_string(),
                },
            ],
            expected_before: Vec::new(),
        };
        let (schema_before, rows_before, settings_before) = {
            let state = state.lock().expect("state mutex should not be poisoned");
            (
                rebuild_schema_snapshot(&state),
                raw_rebuild_rows(
                    &state,
                    "SELECT id, payload, retained, obsolete, parent_id FROM target",
                ),
                super::capture_native_connection_settings(&state)
                    .expect("connection settings should be captured"),
            )
        };
        let error = prepare_rebuild_plan_for_test(state.clone(), &request)
            .expect("affinity plan should prepare")
            .execute_guarded()
            .expect_err("TEXT storage must not be silently coerced to INTEGER");
        assert!(matches!(error, DbError::QueryFailed(_)));
        assert!(error.to_string().contains("exact copy comparison differed"));
        assert_guarded_rollback(&state, schema_before, rows_before, settings_before);
    }

    #[test]
    fn execute_guarded_rebuild_rejects_existing_null_for_not_null_and_rolls_back() {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("fixture SQLite should open"),
        )));
        state.lock().expect("state mutex should not be poisoned").execute_batch(
                "PRAGMA foreign_keys = ON; CREATE TABLE parent (id INTEGER PRIMARY KEY); CREATE TABLE target (id INTEGER PRIMARY KEY, payload TEXT, retained TEXT, obsolete TEXT, parent_id INTEGER REFERENCES parent(id)); CREATE INDEX target_retained_desc ON target(retained DESC); INSERT INTO parent VALUES (1); INSERT INTO target VALUES (-7, NULL, 'keep', 'remove', 1)",
            ).expect("not-null fixture should be created");
        let request = TableAlterRequest {
            table: TableRef::new("target"),
            operations: vec![
                TableAlterOperation::AlterColumn {
                    name: "payload".to_string(),
                    new_type: None,
                    nullable: Some(false),
                    default: None,
                },
                TableAlterOperation::DropColumn {
                    name: "obsolete".to_string(),
                },
            ],
            expected_before: Vec::new(),
        };
        let (schema_before, rows_before, settings_before) = {
            let state = state.lock().expect("state mutex should not be poisoned");
            (
                rebuild_schema_snapshot(&state),
                raw_rebuild_rows(
                    &state,
                    "SELECT id, payload, retained, obsolete, parent_id FROM target",
                ),
                super::capture_native_connection_settings(&state)
                    .expect("connection settings should be captured"),
            )
        };
        let error = prepare_rebuild_plan_for_test(state.clone(), &request)
            .expect("not-null plan should prepare")
            .execute_guarded()
            .expect_err("existing NULL must not receive a backfill or coercion");
        assert_eq!(
            error
                .formatted()
                .and_then(|formatted| formatted.code.as_deref()),
            Some("1299"),
            "copy failure must preserve SQLite SQLITE_CONSTRAINT_NOTNULL"
        );
        assert_guarded_rollback(&state, schema_before, rows_before, settings_before);
    }

    #[derive(Debug, PartialEq, Eq)]
    struct FailedRebuildBoundary {
        main_catalog: Vec<Vec<RawRebuildValue>>,
        target_rows: Vec<Vec<RawRebuildValue>>,
        parent_rows: Vec<Vec<RawRebuildValue>>,
        temp_catalog: Vec<Vec<RawRebuildValue>>,
        sentinel_rows: Vec<Vec<RawRebuildValue>>,
        settings: super::NativeConnectionSettings,
    }

    fn capture_failed_rebuild_boundary(state: &SqliteConnectionState) -> FailedRebuildBoundary {
        FailedRebuildBoundary {
            main_catalog: rebuild_schema_snapshot(state),
            target_rows: raw_rebuild_rows(
                state,
                "SELECT id, payload, retained, obsolete, parent_id FROM target ORDER BY id",
            ),
            parent_rows: raw_rebuild_rows(state, "SELECT id, label FROM parent ORDER BY id"),
            temp_catalog: raw_rebuild_rows(
                state,
                "SELECT type, name, tbl_name, sql FROM temp.sqlite_temp_master ORDER BY type, name",
            ),
            sentinel_rows: raw_rebuild_rows(state, "SELECT value FROM sentinel ORDER BY value"),
            settings: super::capture_native_connection_settings(state)
                .expect("failed rebuild settings should be readable"),
        }
    }

    fn assert_failed_rebuild_boundary(
        state: &Arc<Mutex<SqliteConnectionState>>,
        boundary: &FailedRebuildBoundary,
    ) {
        let state_guard = state.lock().expect("state mutex should not be poisoned");
        assert!(
            state_guard.is_autocommit(),
            "failure must complete rollback"
        );
        assert_eq!(&capture_failed_rebuild_boundary(&state_guard), boundary);
        assert_eq!(
            state_guard
                .query_row(
                    "SELECT COUNT(*) FROM main.sqlite_master WHERE name LIKE '__dbflux_rebuild_%'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("private catalog should be readable"),
            0,
            "failure must not retain a replacement object",
        );
        state_guard
            .execute_batch("INSERT INTO sentinel VALUES ('subsequent-write')")
            .expect("a clean rollback must leave a subsequent write usable");
        drop(state_guard);
        assert!(SqliteConnectionState::lock_checked(state).is_ok());
    }

    fn lifecycle_rebuild_fixture() -> (
        Arc<Mutex<SqliteConnectionState>>,
        TableAlterRequest,
        FailedRebuildBoundary,
    ) {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("fixture SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch(
                "PRAGMA foreign_keys = ON; \
                 CREATE TABLE parent (id INTEGER PRIMARY KEY, label TEXT NOT NULL); \
                 CREATE TABLE target (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, retained TEXT, obsolete TEXT, parent_id INTEGER REFERENCES parent(id)); \
                 CREATE INDEX target_retained ON target(retained DESC); \
                 CREATE TABLE sentinel (value TEXT NOT NULL); \
                 CREATE TEMP TABLE temp_sentinel (value TEXT NOT NULL); \
                 INSERT INTO parent VALUES (1, 'parent-before'); \
                 INSERT INTO target VALUES (-7, 'first', 'keep-first', 'remove-first', 1); \
                 INSERT INTO target VALUES (9, 'second', 'keep-second', 'remove-second', 1); \
                 INSERT INTO sentinel VALUES ('before'); \
                 INSERT INTO temp_sentinel VALUES ('temp-before')",
            )
            .expect("lifecycle fixture should be created");
        let request = rebuild_alter_and_drop_request();
        let boundary = {
            let state_guard = state.lock().expect("state mutex should not be poisoned");
            capture_failed_rebuild_boundary(&state_guard)
        };
        (state, request, boundary)
    }

    #[test]
    fn execute_guarded_rebuild_cancellation_is_bare_only_after_verified_rollback() {
        let _hook_lock = REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild hook lock should not be poisoned");
        let (state, request, boundary) = lifecycle_rebuild_fixture();
        let cancelled = Arc::new(AtomicBool::new(false));
        let plan = prepare_rebuild_plan_with_cancellation_for_test(
            state.clone(),
            &request,
            cancelled.clone(),
        )
        .expect("cancellation fixture should prepare");
        *super::COMPARISON_CANCEL_AFTER_ROW
            .lock()
            .expect("comparison cancellation hook mutex should not be poisoned") =
            Some((std::thread::current().id(), cancelled));

        let error = plan
            .execute_guarded()
            .expect_err("shared cancellation must stop the private lifecycle");
        clear_rebuild_test_faults();
        assert!(matches!(error, DbError::Cancelled));
        assert_failed_rebuild_boundary(&state, &boundary);
    }

    #[test]
    fn execute_guarded_rebuild_composes_cancellation_with_cleanup_failures_and_quarantines_aliases()
    {
        let _hook_lock = REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild hook lock should not be poisoned");
        let (state, request, _) = lifecycle_rebuild_fixture();
        let checked_alias = state.clone();
        let interrupt_handle = state
            .lock()
            .expect("state mutex should not be poisoned")
            .interrupt_handle();
        let cancelled = Arc::new(AtomicBool::new(false));
        let plan = prepare_rebuild_plan_with_cancellation_for_test(
            state.clone(),
            &request,
            cancelled.clone(),
        )
        .expect("cancellation fixture should prepare");
        *super::COMPARISON_CANCEL_AFTER_ROW
            .lock()
            .expect("comparison cancellation hook mutex should not be poisoned") =
            Some((std::thread::current().id(), cancelled));
        *super::REBUILD_GUARD_FAULTS
            .lock()
            .expect("guard fault mutex should not be poisoned") = Some((
            std::thread::current().id(),
            super::RebuildGuardFaults {
                fail_rollback: true,
                fail_restore_readback: true,
                ..Default::default()
            },
        ));

        let error = plan
            .execute_guarded()
            .expect_err("cleanup uncertainty must compose with cancellation");
        clear_rebuild_test_faults();
        assert!(matches!(error, DbError::QueryFailed(_)));
        let detail = error
            .formatted()
            .and_then(|formatted| formatted.detail.as_deref())
            .expect("composite failure must preserve structured cleanup evidence");
        assert!(
            detail.contains("primary failure") && detail.to_ascii_lowercase().contains("cancel"),
            "composite failure must retain the cancellation primary: {detail}"
        );
        assert!(detail.contains("rollback failure"));
        assert!(detail.contains("foreign-key restoration failure"));
        assert!(SqliteConnectionState::lock_checked(&state).is_err());
        assert!(SqliteConnectionState::lock_checked(&checked_alias).is_err());
        interrupt_handle.interrupt();
    }

    #[test]
    fn execute_guarded_rebuild_clears_a_stale_cancellation_only_after_it_owns_the_operation() {
        let (state, request, _) = lifecycle_rebuild_fixture();
        let cancelled = Arc::new(AtomicBool::new(true));
        let stale_plan = prepare_rebuild_plan_with_cancellation_for_test(
            state.clone(),
            &request,
            cancelled.clone(),
        )
        .expect("stale-cancellation fixture should prepare");
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch("PRAGMA foreign_keys = OFF")
            .expect("fixture should make the prepared plan stale");
        assert!(stale_plan.execute_guarded().is_err());
        assert!(
            cancelled.load(Ordering::SeqCst),
            "a rejected preflight must not consume a cancellation owned by another operation"
        );
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch("PRAGMA foreign_keys = ON")
            .expect("fixture should restore the preflight setting");
        prepare_rebuild_plan_with_cancellation_for_test(state, &request, cancelled.clone())
            .expect("fresh plan should prepare")
            .execute_guarded()
            .expect("the operation that owns the connection may clear a stale cancellation");
        assert!(!cancelled.load(Ordering::SeqCst));
    }

    #[test]
    fn execute_guarded_rebuild_finalization_faults_prevent_drop_and_preserve_failure_boundary() {
        let _hook_lock = REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild hook lock should not be poisoned");
        let (state, request, boundary) = lifecycle_rebuild_fixture();
        *super::COMPARISON_FINALIZE_FAULTS
            .lock()
            .expect("comparison finalization fault mutex should not be poisoned") = Some((
            std::thread::current().id(),
            super::ComparisonFinalizeFaults {
                source: true,
                replacement: true,
            },
        ));
        let error = prepare_rebuild_plan_for_test(state.clone(), &request)
            .expect("finalization fixture should prepare")
            .execute_guarded()
            .expect_err("comparison finalization must prevent the destructive drop");
        clear_rebuild_test_faults();
        let detail = error
            .formatted()
            .and_then(|formatted| formatted.detail.as_deref())
            .expect("comparison cleanup failure must preserve both finalization causes");
        assert!(detail.contains("source statement finalization"));
        assert!(detail.contains("replacement statement finalization"));
        assert_failed_rebuild_boundary(&state, &boundary);
    }

    #[test]
    fn execute_guarded_rebuild_recovers_after_sqlite_engine_automatically_rolls_back() {
        let _hook_lock = REBUILD_TEST_HOOK_LOCK
            .lock()
            .expect("rebuild hook lock should not be poisoned");
        let (state, request, _) = lifecycle_rebuild_fixture();
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch("CREATE UNIQUE INDEX target_retained_unique ON target(retained)")
            .expect("fixture should have a uniqueness constraint for engine rollback");
        let boundary = {
            let state_guard = state.lock().expect("state mutex should not be poisoned");
            capture_failed_rebuild_boundary(&state_guard)
        };
        *super::REBUILD_STAGE_HOOK
            .lock()
            .expect("stage hook mutex should not be poisoned") = Some((
            std::thread::current().id(),
            super::RebuildExecutionStage::Compare,
            Box::new(|connection| {
                connection.execute_batch(
                    "INSERT OR ROLLBACK INTO target (id, payload, retained, obsolete, parent_id) \
                     VALUES (31, 'engine-payload', 'keep-first', 'engine-drop', 1)",
                )
            }),
        ));

        let error = prepare_rebuild_plan_for_test(state.clone(), &request)
            .expect("engine rollback fixture should prepare")
            .execute_guarded()
            .expect_err("SQLite conflict policy must end the transaction automatically");
        clear_rebuild_test_faults();
        assert!(matches!(error, DbError::QueryFailed(_)));
        assert!(error.to_string().contains("run rebuild test stage hook"));
        assert_failed_rebuild_boundary(&state, &boundary);
    }

    fn execute_final_observation_fixture(
        create_target: &str,
        insert_target: &str,
    ) -> (super::RebuildCapture, super::RebuildObservation) {
        let state = Arc::new(Mutex::new(SqliteConnectionState::new(
            RusqliteConnection::open_in_memory().expect("in-memory SQLite should open"),
        )));
        state
            .lock()
            .expect("state mutex should not be poisoned")
            .execute_batch(&format!(
                "PRAGMA foreign_keys = ON; \
                 CREATE TABLE parent (id INTEGER PRIMARY KEY); \
                 {create_target}; \
                 CREATE INDEX target_retained_desc ON target(retained DESC); \
                 INSERT INTO parent VALUES (1); \
                 {insert_target}",
            ))
            .expect("rebuild final-observation fixture should be created");
        let request = TableAlterRequest {
            table: TableRef::new("target"),
            operations: vec![
                TableAlterOperation::AlterColumn {
                    name: "payload".to_string(),
                    new_type: Some("VARCHAR(16)".to_string()),
                    nullable: Some(false),
                    default: None,
                },
                TableAlterOperation::DropColumn {
                    name: "obsolete".to_string(),
                },
            ],
            expected_before: Vec::new(),
        };
        let plan = prepare_rebuild_plan_for_test(state.clone(), &request)
            .expect("private rebuild plan should prepare");
        let capture = plan.capture.clone();
        plan.execute_guarded()
            .expect("private rebuild executor should reconstruct the fixture");
        let state = state.lock().expect("state mutex should not be poisoned");
        let observation = super::capture_rebuild_observation_once(&state, "target")
            .expect("reconstructed table observation should be captured");
        (capture, observation)
    }

    fn observe_during_finalization(
        observation: &super::RebuildObservation,
    ) -> super::RebuildObservation {
        let mut observation = observation.clone();
        observation.settings.foreign_keys = 0;
        observation
    }

    #[test]
    fn final_observation_validator_accepts_reconstructed_integer_primary_key_and_hidden_rowid() {
        for (label, create_target, insert_target, identity) in [
            (
                "INTEGER PRIMARY KEY",
                "CREATE TABLE target (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, retained TEXT, obsolete TEXT, parent_id INTEGER REFERENCES parent(id), UNIQUE(retained, parent_id))",
                "INSERT INTO target VALUES (-7, 'payload', 'retained', 'obsolete', 1)",
                "id",
            ),
            (
                "hidden rowid",
                "CREATE TABLE target (payload TEXT NOT NULL, retained TEXT, obsolete TEXT, parent_id INTEGER REFERENCES parent(id), UNIQUE(retained, parent_id))",
                "INSERT INTO target VALUES ('payload', 'retained', 'obsolete', 1)",
                "rowid",
            ),
        ] {
            let (capture, observation) =
                execute_final_observation_fixture(create_target, insert_target);
            assert!(observation.indexes.iter().any(|index| index.origin == "c"));
            assert!(observation.indexes.iter().any(|index| index.origin == "u"));
            assert_eq!(capture.final_expected_facts.identity, identity);
            super::validate_rebuild_final_observation(
                &capture,
                &observe_during_finalization(&observation),
            )
            .unwrap_or_else(|error| panic!("{label} final observation must validate: {error}"));
        }
    }

    #[test]
    fn final_observation_validator_rejects_each_cloned_integrity_mutation() {
        enum Mutation {
            RetainedOrder,
            RetainedMembership,
            TableMetadataAbsent,
            IndexAbsent,
            IndexExtra,
            IndexName,
            IndexPartial,
            IndexKeyMembership,
            IndexKeyName,
            IndexKeyOrder,
            IndexDirection,
            IndexCollation,
            IndexUnique,
            IndexOrigin,
            IndexSql,
            UnrequestedSemanticChange,
            ForeignKeyIntent,
            ForeignKeyViolation,
            NonTargetMain,
            NonTargetTemp,
            DatabaseList,
            PrivateNameLeak,
            ForeignKeys,
            WritableSchema,
            LegacyAlterTable,
            IgnoreCheckConstraints,
            DeferForeignKeys,
        }

        let (capture, observation) = execute_final_observation_fixture(
            "CREATE TABLE target (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, retained TEXT, obsolete TEXT, parent_id INTEGER REFERENCES parent(id), UNIQUE(retained, parent_id))",
            "INSERT INTO target VALUES (-7, 'payload', 'retained', 'obsolete', 1)",
        );
        let observation = observe_during_finalization(&observation);
        for (label, mutation, expected) in [
            (
                "retained membership/order",
                Mutation::RetainedOrder,
                "semantic proof changed retained column order or names",
            ),
            (
                "retained membership",
                Mutation::RetainedMembership,
                "semantic proof changed retained column order or names",
            ),
            (
                "table metadata absence",
                Mutation::TableMetadataAbsent,
                "cannot find final table metadata",
            ),
            (
                "index absent",
                Mutation::IndexAbsent,
                "expected index is absent",
            ),
            (
                "index extra",
                Mutation::IndexExtra,
                "contains an extra index",
            ),
            ("index name", Mutation::IndexName, "index name or order"),
            (
                "index partial",
                Mutation::IndexPartial,
                "rejects partial index",
            ),
            (
                "index key membership",
                Mutation::IndexKeyMembership,
                "index key membership",
            ),
            ("index key name", Mutation::IndexKeyName, "index key column"),
            (
                "index key order",
                Mutation::IndexKeyOrder,
                "index key order",
            ),
            (
                "index direction",
                Mutation::IndexDirection,
                "index key direction",
            ),
            (
                "index collation",
                Mutation::IndexCollation,
                "index key collation",
            ),
            ("index unique", Mutation::IndexUnique, "index uniqueness"),
            ("index origin", Mutation::IndexOrigin, "index origin"),
            ("index SQL", Mutation::IndexSql, "index SQL"),
            (
                "unrequested selected-column semantic change",
                Mutation::UnrequestedSemanticChange,
                "semantic proof changed unselected",
            ),
            (
                "foreign key intent",
                Mutation::ForeignKeyIntent,
                "foreign-key metadata differs",
            ),
            (
                "foreign key violation",
                Mutation::ForeignKeyViolation,
                "foreign_key_check found a violation",
            ),
            (
                "non-target MAIN catalog",
                Mutation::NonTargetMain,
                "non-target main catalog",
            ),
            (
                "non-target TEMP catalog",
                Mutation::NonTargetTemp,
                "non-target temp catalog",
            ),
            ("database list", Mutation::DatabaseList, "database list"),
            (
                "private name leak",
                Mutation::PrivateNameLeak,
                "private replacement object",
            ),
            ("foreign_keys", Mutation::ForeignKeys, "foreign_keys"),
            (
                "writable_schema",
                Mutation::WritableSchema,
                "writable_schema",
            ),
            (
                "legacy_alter_table",
                Mutation::LegacyAlterTable,
                "legacy_alter_table",
            ),
            (
                "ignore_check_constraints",
                Mutation::IgnoreCheckConstraints,
                "ignore_check_constraints",
            ),
            (
                "defer_foreign_keys",
                Mutation::DeferForeignKeys,
                "defer_foreign_keys",
            ),
        ] {
            let mut mutated = observation.clone();
            match mutation {
                Mutation::RetainedOrder => {
                    mutated.source_sql = "CREATE TABLE target (id INTEGER PRIMARY KEY, retained TEXT, payload VARCHAR(16) NOT NULL, parent_id INTEGER REFERENCES parent(id), UNIQUE(retained, parent_id))".to_string();
                }
                Mutation::RetainedMembership => {
                    mutated.source_sql = "CREATE TABLE target (id INTEGER PRIMARY KEY, payload VARCHAR(16) NOT NULL, replacement TEXT, parent_id INTEGER REFERENCES parent(id), UNIQUE(replacement, parent_id))".to_string();
                }
                Mutation::TableMetadataAbsent => mutated.table_metadata.clear(),
                Mutation::IndexAbsent => {
                    mutated.indexes.pop();
                }
                Mutation::IndexExtra => {
                    let mut extra = mutated.indexes[0].clone();
                    extra.name = "extra_index".to_string();
                    mutated.indexes.push(extra);
                }
                Mutation::IndexName => mutated.indexes[0].name.push_str("_renamed"),
                Mutation::IndexPartial => mutated.indexes[0].partial = true,
                Mutation::IndexKeyMembership => {
                    mutated.indexes[0].keys.pop();
                }
                Mutation::IndexKeyName => mutated.indexes[0].keys[0].column = "other".to_string(),
                Mutation::IndexKeyOrder => mutated.indexes[0].keys[0].sequence += 1,
                Mutation::IndexDirection => {
                    mutated.indexes[0].keys[0].descending = !mutated.indexes[0].keys[0].descending;
                }
                Mutation::IndexCollation => {
                    mutated.indexes[0].keys[0].collation = "NOCASE".to_string()
                }
                Mutation::IndexUnique => mutated.indexes[0].unique = !mutated.indexes[0].unique,
                Mutation::IndexOrigin => mutated.indexes[0].origin = "pk".to_string(),
                Mutation::IndexSql => {
                    mutated.indexes[0].sql =
                        Some("CREATE INDEX changed ON target(payload)".to_string());
                }
                Mutation::UnrequestedSemanticChange => {
                    mutated.source_sql = "CREATE TABLE target (id INTEGER PRIMARY KEY, payload VARCHAR(16) NOT NULL, retained BLOB, parent_id INTEGER REFERENCES parent(id), UNIQUE(retained, parent_id))".to_string();
                }
                Mutation::ForeignKeyIntent => {
                    mutated.foreign_key_metadata[0].on_delete = "CASCADE".to_string();
                }
                Mutation::ForeignKeyViolation => {
                    mutated.foreign_key_violation = Some("target".to_string())
                }
                Mutation::NonTargetMain => {
                    let parent = mutated
                        .catalog
                        .iter_mut()
                        .find(|entry| entry.name == "parent")
                        .expect("fixture must retain parent catalog entry");
                    parent.sql = Some("CREATE TABLE parent (id TEXT)".to_string());
                }
                Mutation::NonTargetTemp => mutated.temp_catalog.push(super::CatalogEntry {
                    object_type: "table".to_string(),
                    name: "temp_extra".to_string(),
                    table_name: "temp_extra".to_string(),
                    sql: Some("CREATE TABLE temp_extra(value)".to_string()),
                }),
                Mutation::DatabaseList => mutated.databases[0].file_name.push_str("-changed"),
                Mutation::PrivateNameLeak => mutated.catalog.push(super::CatalogEntry {
                    object_type: "table".to_string(),
                    name: capture.replacement.clone(),
                    table_name: capture.replacement.clone(),
                    sql: Some("CREATE TABLE leaked(value)".to_string()),
                }),
                Mutation::ForeignKeys => mutated.settings.foreign_keys = 1,
                Mutation::WritableSchema => mutated.settings.writable_schema += 1,
                Mutation::LegacyAlterTable => mutated.settings.legacy_alter_table += 1,
                Mutation::IgnoreCheckConstraints => mutated.settings.ignore_check_constraints += 1,
                Mutation::DeferForeignKeys => mutated.settings.defer_foreign_keys += 1,
            }
            let error = super::validate_rebuild_final_observation(&capture, &mutated)
                .expect_err(&format!("{label} mutation must be rejected"));
            assert!(
                error.to_string().contains(expected),
                "{label} must report its own assertion ({expected}), got: {error}"
            );
        }
    }

    #[test]
    fn final_observation_validator_rejects_hidden_rowid_identity_mutation() {
        let (capture, observation) = execute_final_observation_fixture(
            "CREATE TABLE target (payload TEXT NOT NULL, retained TEXT, obsolete TEXT, parent_id INTEGER REFERENCES parent(id), UNIQUE(retained, parent_id))",
            "INSERT INTO target VALUES ('payload', 'retained', 'obsolete', 1)",
        );
        let mut observation = observe_during_finalization(&observation);
        observation
            .table_metadata
            .iter_mut()
            .find(|metadata| metadata.name == "target")
            .expect("fixture must contain target metadata")
            .rowid_aliases = vec!["alternate_rowid".to_string()];
        let error = super::validate_rebuild_final_observation(&capture, &observation)
            .expect_err("hidden rowid identity mutation must be rejected");
        assert!(error.to_string().contains("identity differs"));
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
