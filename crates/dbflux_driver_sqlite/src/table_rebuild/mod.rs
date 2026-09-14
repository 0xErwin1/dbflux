#[allow(dead_code)]
mod syntax;

use std::collections::HashSet;
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
        if let Some(rebuild_changes) = selected_rebuild_changes(&request.operations)? {
            let state = SqliteConnectionState::lock_checked(&self.state)?;
            if !state.is_autocommit() {
                return Err(DbError::NotSupported(
                    "SQLite table alteration planning requires an autocommit connection"
                        .to_string(),
                ));
            }
            return prepare_rebuild_plan(&state, self.state.clone(), request, rebuild_changes);
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
    request: &TableAlterRequest,
    changes: RebuildChanges,
) -> Result<Box<dyn PreparedTableAlter>, DbError> {
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
        retained_columns: retained,
        identity,
        replacement,
        copy,
        exact_index_sql,
        foreign_key_intent: resolved_foreign_keys,
    };
    Ok(Box::new(RebuildPlan {
        state,
        capture,
        preview: TableAlterPreview {
            route: TableAlterRoute::Rebuild,
            statements,
            warnings: vec![
                "NOT EXECUTABLE: driver-managed illustrative lifecycle intent only; do not submit preview statements as apply SQL.".to_string(),
                "Data is not validated or copied by preparation; exact streamed ValueRef comparison and execution are unavailable until Unit5 installs the lifecycle.".to_string(),
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
                "Existing data compatibility is not validated by preparation; rebuild execution lifecycle is not installed.".to_string(),
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
                && declaration.terms[0]
                    .column
                    .eq_ignore_ascii_case(&column.name)
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

#[derive(Debug)]
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
    foreign_keys: Vec<(String, String, String, Option<String>)>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct RebuildCapture {
    observation: RebuildObservation,
    retained_columns: Vec<String>,
    identity: String,
    replacement: String,
    copy: RebuildCopyIntent,
    exact_index_sql: Vec<String>,
    foreign_key_intent: Vec<ResolvedForeignKeyRelationship>,
}

struct RebuildPlan {
    state: Arc<Mutex<SqliteConnectionState>>,
    capture: RebuildCapture,
    preview: TableAlterPreview,
}

impl PreparedTableAlter for RebuildPlan {
    fn preview(&self) -> &TableAlterPreview {
        &self.preview
    }

    fn execute(self: Box<Self>) -> Result<TableAlterOutcome, DbError> {
        let _state = &self.state;
        let _capture = &self.capture;
        Err(DbError::NotSupported(
            "SQLite rebuild execution lifecycle is not installed; this read-only plan did not change schema, data, or connection settings"
                .to_string(),
        ))
    }
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
) -> Result<Vec<(String, String, String, Option<String>)>, DbError> {
    let mut foreign_keys = Vec::new();
    for table in tables {
        for (referenced, from, to) in foreign_keys_for_table(connection, &table)? {
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
                quote_identifier(&child_table)
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
                    !consumed[*position]
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
        consumed[position] = true;
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
static SQLITE_ENGINE_VERSION_OVERRIDE: Mutex<Option<(std::thread::ThreadId, i32)>> =
    Mutex::new(None);

#[cfg(test)]
static BEFORE_NATIVE_CAPTURE_SECOND_READ: Mutex<
    Option<(std::thread::ThreadId, Box<dyn FnOnce() + Send>)>,
> = Mutex::new(None);

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use rusqlite::Connection as RusqliteConnection;

    use super::{
        AFTER_REBUILD_OBSERVATION_CAPTURE, BEFORE_NATIVE_CAPTURE_SECOND_READ,
        BEFORE_NATIVE_DROP_BEGIN, BEFORE_REBUILD_CAPTURE_SECOND_READ, ForeignKeyCapture,
        REBUILD_TEST_HOOK_LOCK, SQLITE_ENGINE_VERSION_OVERRIDE, TableColumnCapture,
        TableMetadataCapture, resolve_foreign_key_relationships,
    };
    use crate::driver::{SqliteConnection, SqliteConnectionState};
    use dbflux_core::{
        Connection, TableAlterExpectedColumn, TableAlterOperation, TableAlterRequest, TableRef,
    };

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
