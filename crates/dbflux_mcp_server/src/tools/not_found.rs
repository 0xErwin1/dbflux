//! Hints for calls that name a table, collection or column the schema
//! metadata does not list.
//!
//! The lookup runs only after the driver has failed a call with an error that
//! can mean a missing object. It reads schema metadata instead of the engine's
//! error text, and it reports only what it observed: a name that is not listed
//! may not exist, or the connection may not have access to it. Each part of a
//! hint is included only when the client may call the tool that normally
//! returns it. When the metadata cannot settle the question the original error
//! is returned unchanged.
//!
//! `select_data` on tables also checks its column references before it runs
//! (see [`check_columns`]), because some engines do not fail on an unknown
//! column.

use std::sync::Arc;

use dbflux_core::{
    ColumnRef, Connection, DataStructure, DatabaseCategory, DbError, DbSchemaInfo, LogErr,
    RelationalSchema, SchemaSnapshot, SemanticFieldRef, SemanticFilter, TableInfo, TableRef,
    ViewInfo, suggest_names,
};

use crate::{
    governance::{self, HintPermissions},
    state::ServerState,
};

/// Above this many columns the full list is left out of the error.
const MAX_LISTED_COLUMNS: usize = 20;

const NOT_LISTED_REASON: &str = "It may not exist, or this connection may not have access to it.";

const NOT_RUN: &str = "The query was not run.";

/// The error of a failed tool call, with whether a not-found lookup is worth
/// running for it.
#[derive(Debug)]
pub(crate) struct CallFailure {
    pub message: String,
    may_be_missing_object: bool,
}

impl CallFailure {
    /// A failure the driver reported. `message` is the text the tool returns.
    pub(crate) fn from_driver(message: String, error: &DbError) -> Self {
        Self {
            message,
            may_be_missing_object: may_be_missing_object(error),
        }
    }
}

/// A failure raised before or outside the driver call, which never runs the
/// lookup.
impl From<String> for CallFailure {
    fn from(message: String) -> Self {
        Self {
            message,
            may_be_missing_object: false,
        }
    }
}

impl From<&str> for CallFailure {
    fn from(message: &str) -> Self {
        Self::from(message.to_string())
    }
}

/// Whether a driver error can be the result of naming an object that does not
/// exist. Only the generic query failure and the explicit not-found variant
/// qualify; every other variant names a different cause.
fn may_be_missing_object(error: &DbError) -> bool {
    match error {
        DbError::QueryFailed(_) | DbError::ObjectNotFound(_) => true,
        DbError::ConnectionFailed(_)
        | DbError::AuthFailed(_)
        | DbError::ConstraintViolation(_)
        | DbError::SyntaxError(_)
        | DbError::PermissionDenied(_)
        | DbError::Timeout
        | DbError::Cancelled
        | DbError::NotSupported(_)
        | DbError::InvalidProfile(_)
        | DbError::ValueResolutionFailed(_)
        | DbError::IoError(_)
        | DbError::Parse(_)
        | DbError::Unsupported(_) => false,
    }
}

/// A failed call against one table or collection.
pub(crate) struct FailedCall<'a> {
    pub state: &'a ServerState,
    pub connection_id: &'a str,
    pub connection: &'a Arc<dyn Connection>,
    pub target: &'a TableRef,

    /// The `database` argument of the call, when it passed one.
    pub database: Option<&'a str>,

    /// Column names the call referenced in its filter and sort.
    pub columns: Vec<String>,
}

/// A column a call names, with the table it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnReference {
    pub table: TableRef,
    pub column: String,
}

/// A `select_data` call whose column references are checked before it runs.
pub(crate) struct ColumnCheck<'a> {
    pub state: &'a ServerState,
    pub connection_id: &'a str,
    pub connection: &'a Arc<dyn Connection>,

    /// The `database` argument of the call, when it passed one.
    pub database: Option<&'a str>,

    pub references: Vec<ColumnReference>,
}

/// The names a table's metadata lists, as the column check and the hint
/// compare them.
struct TableColumns {
    columns: Vec<String>,

    /// Names the driver declared the engine resolves on this table although
    /// `columns` does not list them. They count as present but are never
    /// suggested or listed, because they are not columns of the table.
    pseudo_columns: Vec<String>,
}

impl TableColumns {
    fn from_details(details: TableInfo) -> Option<Self> {
        Some(Self {
            columns: details
                .columns?
                .into_iter()
                .map(|column| column.name)
                .collect(),
            pseudo_columns: details.pseudo_columns.into_vec(),
        })
    }

    fn lists(&self, name: &str) -> bool {
        contains_name(&self.columns, name) || contains_name(&self.pseudo_columns, name)
    }
}

/// Where a name is looked up.
#[derive(Clone)]
struct Scope {
    database: Option<String>,
    schema: Option<String>,

    /// Whether the caller named the database, in the call or in the table name.
    database_from_caller: bool,
}

/// What the schema metadata says about a scope.
struct Listing {
    /// Table, view or collection names. `None` when the metadata cannot say
    /// what the scope holds.
    names: Option<Vec<String>>,

    database_count: usize,
}

/// Returns the failure message with a hint in front of it when the target
/// table or collection, or a referenced column, is not listed in the schema
/// metadata. Any other failure returns the message unchanged.
pub(crate) async fn explain(call: FailedCall<'_>, failure: CallFailure) -> String {
    if !failure.may_be_missing_object {
        return failure.message;
    }

    match hint(&call).await {
        Some(hint) => format!("{hint}\n\nOriginal error: {}", failure.message),
        None => failure.message,
    }
}

/// Column names tested by `filter` that belong to `table`. Nested paths and
/// names qualified with another table are left out.
pub(crate) fn filter_columns(filter: Option<&SemanticFilter>, table: &TableRef) -> Vec<String> {
    let Some(filter) = filter else {
        return Vec::new();
    };

    filter
        .field_refs()
        .into_iter()
        .filter_map(|field| match field {
            SemanticFieldRef::Column(column) => column_of_table(column, table),
            SemanticFieldRef::Path(_) => None,
        })
        .collect()
}

/// The name of `column` when it is unqualified or qualified with `table`.
pub(crate) fn column_of_table(column: &ColumnRef, table: &TableRef) -> Option<String> {
    let belongs = column
        .table
        .as_deref()
        .is_none_or(|qualifier| qualifier == table.name);

    belongs.then(|| column.name.clone())
}

/// Whether `text` is a column name, optionally qualified, and not an
/// expression: letters, digits, underscores and dots only.
pub(crate) fn is_plain_identifier(text: &str) -> bool {
    !text.is_empty()
        && text
            .chars()
            .all(|character| character.is_alphanumeric() || character == '_' || character == '.')
}

/// Extends a "column not found" message with the closest names and, when the
/// list is short, every available column.
pub(crate) fn with_column_hints(message: String, wanted: &str, available: &[&str]) -> String {
    let hints = column_hints(wanted, available);

    if hints.is_empty() {
        return message;
    }

    format!("{message}. {}", hints.join(" "))
}

/// Whether `text` can be checked as a column name: a plain identifier with no
/// table qualifier left in it.
pub(crate) fn is_checkable_column(text: &str) -> bool {
    is_plain_identifier(text) && !text.contains('.')
}

/// Refuses the call when it names a column its table's metadata does not
/// list, and returns the refusal message.
///
/// Some engines do not fail on an unknown column: SQLite reads an unknown
/// double-quoted identifier as a string literal, so a misspelled column in a
/// filter returns no rows instead of an error. The check reads the column
/// names of each referenced table once, through `table_details`, and compares
/// names case-insensitively, because the driver metadata does not say how the
/// engine folds a column name. A pseudo-column the driver declared for the
/// table, such as SQLite's `rowid`, counts as listed.
///
/// It never refuses on missing evidence. A table whose scope cannot be
/// resolved, whose lookup fails or is not supported, or whose metadata lists
/// no columns is not checked, and the call runs as before.
///
/// The refusal names the closest and the available columns only when the
/// client may call `describe_object`. Without that permission it says only
/// that the column is not listed for the table.
pub(crate) async fn check_columns(check: ColumnCheck<'_>) -> Result<(), String> {
    let mut tables: Vec<TableRef> = Vec::new();

    for reference in &check.references {
        if !tables.contains(&reference.table) {
            tables.push(reference.table.clone());
        }
    }

    if tables.is_empty() {
        return Ok(());
    }

    let connection = check.connection.clone();
    let database = check.database.map(str::to_string);

    let listed: Vec<(TableRef, Option<TableColumns>)> = tokio::task::spawn_blocking(move || {
        tables
            .into_iter()
            .map(|table| {
                let columns = listed_columns(connection.as_ref(), &table, database.as_deref());
                (table, columns)
            })
            .collect()
    })
    .await
    .log_err_with("Column check task failed")
    .unwrap_or_default();

    for (table, columns) in &listed {
        let Some(columns) = columns else {
            continue;
        };

        let unlisted = check
            .references
            .iter()
            .filter(|reference| &reference.table == table)
            .find(|reference| !columns.lists(&reference.column));

        if let Some(reference) = unlisted {
            let allowed = governance::hint_permissions(check.state, check.connection_id).await;

            let message = if allowed.column_names {
                unlisted_column_message(&reference.column, &table.name, &columns.columns)
            } else {
                unlisted_column_line(&reference.column, &table.name)
            };

            return Err(format!("{message}\n\n{NOT_RUN}"));
        }
    }

    Ok(())
}

/// Reads the column names of `table` for the column check. `None` means the
/// check cannot be made for it, which is logged at debug level only.
fn listed_columns(
    connection: &dyn Connection,
    table: &TableRef,
    database: Option<&str>,
) -> Option<TableColumns> {
    let syntax = connection.metadata().syntax.as_ref();
    let supports_schemas = syntax.is_some_and(|syntax| syntax.supports_schemas);

    // Without schemas, a qualifier on the table name is not a schema, and the
    // metadata lookup may not honour it, so the columns could belong to
    // another table.
    let schema = match (&table.schema, supports_schemas) {
        (Some(schema), true) => Some(schema.clone()),
        (None, true) => match syntax.and_then(|syntax| syntax.default_schema.clone()) {
            Some(schema) => Some(schema),
            None => {
                log::debug!(
                    "Column check skipped for '{}': no schema to look it up in",
                    table.name
                );
                return None;
            }
        },
        (Some(qualifier), false) => {
            log::debug!(
                "Column check skipped for '{qualifier}.{}': the driver has no schemas",
                table.name
            );
            return None;
        }
        (None, false) => None,
    };

    let database = database
        .map(str::to_string)
        .or_else(|| connection.active_database())
        .unwrap_or_default();

    let details = match connection.table_details(&database, schema.as_deref(), &table.name) {
        Ok(details) => details,
        Err(error) => {
            log::debug!(
                "Column check skipped for '{}': cannot read its columns: {error}",
                table.name
            );
            return None;
        }
    };

    let columns = TableColumns::from_details(details).filter(|listed| !listed.columns.is_empty());

    if columns.is_none() {
        log::debug!(
            "Column check skipped for '{}': the metadata lists no columns",
            table.name
        );
    }

    columns
}

async fn hint(call: &FailedCall<'_>) -> Option<String> {
    let allowed = governance::hint_permissions(call.state, call.connection_id).await;

    if !allowed.table_names && !allowed.column_names {
        return None;
    }

    let is_collection = matches!(
        call.connection.metadata().category,
        DatabaseCategory::Document | DatabaseCategory::LogStream
    );

    let (scope, listing) = read_scope(call, is_collection, allowed.table_names).await?;

    if let Some(names) = &listing.names
        && !contains_name(names, &call.target.name)
    {
        return Some(unlisted_object_message(
            if is_collection { "collection" } else { "table" },
            &call.target.name,
            &scope,
            names,
            listing.database_count,
            allowed,
        ));
    }

    if is_collection || call.columns.is_empty() || !allowed.column_names {
        return None;
    }

    let columns = table_columns(call.connection, &scope, &call.target.name).await?;

    if columns.columns.is_empty() {
        return None;
    }

    let unlisted = call.columns.iter().find(|column| !columns.lists(column))?;

    Some(unlisted_column_message(
        unlisted,
        &call.target.name,
        &columns.columns,
    ))
}

/// Resolves the scope the call queried and, when `list_names` is set, reads
/// what the metadata lists in it. `None` means the scope cannot be resolved:
/// a schema-capable driver without a default schema, or a qualifier that is
/// not a database of the connection.
async fn read_scope(
    call: &FailedCall<'_>,
    is_collection: bool,
    list_names: bool,
) -> Option<(Scope, Listing)> {
    let syntax = call.connection.metadata().syntax.as_ref();
    let supports_schemas = syntax.is_some_and(|syntax| syntax.supports_schemas);

    // With schemas, an unqualified name is looked up in the default schema
    // only. Without schemas, a qualifier on the table name is a database name.
    let (schema, qualifier_database) = if supports_schemas {
        let schema = match &call.target.schema {
            Some(schema) => schema.clone(),
            None => syntax.and_then(|syntax| syntax.default_schema.clone())?,
        };

        (Some(schema), None)
    } else {
        (None, call.target.schema.clone())
    };

    let connection = call.connection.clone();
    let call_database = call.database.map(str::to_string);

    tokio::task::spawn_blocking(move || {
        let snapshot = if list_names {
            lookup_result(connection.schema(), "read the schema")
        } else {
            None
        };

        let databases: Vec<&str> = snapshot
            .iter()
            .flat_map(|snapshot| snapshot.databases())
            .map(|database| database.name.as_str())
            .collect();

        if let Some(qualifier) = &qualifier_database
            && !databases.contains(&qualifier.as_str())
        {
            return None;
        }

        let database_from_caller = qualifier_database.is_some() || call_database.is_some();
        let database = qualifier_database
            .or(call_database)
            .or_else(|| connection.active_database())
            .or_else(|| snapshot.as_ref().and_then(current_database));

        let database_count = databases.len();
        let scope = Scope {
            database,
            schema,
            database_from_caller,
        };

        let names = snapshot
            .as_ref()
            .and_then(|snapshot| listed_names(connection.as_ref(), snapshot, &scope, is_collection))
            .filter(|names| !names.is_empty());

        Some((
            scope,
            Listing {
                names,
                database_count,
            },
        ))
    })
    .await
    .log_err_with("Not-found hint lookup task failed")?
}

/// Reads the table, view or collection names of `scope`. `None` means the
/// metadata cannot say what the scope holds.
fn listed_names(
    connection: &dyn Connection,
    snapshot: &SchemaSnapshot,
    scope: &Scope,
    is_collection: bool,
) -> Option<Vec<String>> {
    let schema = scope.schema.as_deref();

    if !is_collection
        && let DataStructure::Relational(relational) = &snapshot.structure
        && lists_objects(relational)
    {
        return relational_names(relational, schema);
    }

    // Drivers that load schemas lazily, and document drivers, list no
    // objects in the snapshot.
    let database = scope.database.as_deref()?;
    let listing = lookup_result(
        connection.schema_for_database(database),
        "list the objects of a database",
    )?;

    object_names(&listing.tables, &listing.views, schema)
}

/// Reads the column and pseudo-column names of a table. `None` means the
/// driver has no column metadata for it or the lookup failed.
async fn table_columns(
    connection: &Arc<dyn Connection>,
    scope: &Scope,
    table: &str,
) -> Option<TableColumns> {
    let connection = connection.clone();
    let database = scope.database.clone().unwrap_or_default();
    let schema = scope.schema.clone();
    let table = table.to_string();

    let details = tokio::task::spawn_blocking(move || {
        lookup_result(
            connection.table_details(&database, schema.as_deref(), &table),
            "read the columns of a table",
        )
    })
    .await
    .log_err_with("Column lookup task failed")??;

    TableColumns::from_details(details)
}

/// Unwraps a metadata lookup. A driver that does not implement the lookup is
/// expected and logged at debug level; any other failure is logged as an
/// error.
fn lookup_result<T>(result: Result<T, DbError>, action: &str) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(DbError::NotSupported(reason) | DbError::Unsupported(reason)) => {
            log::debug!("Not-found hint: the driver cannot {action}: {reason}");
            None
        }
        Err(error) => {
            log::error!("Not-found hint: failed to {action}: {error}");
            None
        }
    }
}

fn current_database(snapshot: &SchemaSnapshot) -> Option<String> {
    let declared = match &snapshot.structure {
        DataStructure::Relational(relational) => relational.current_database.clone(),
        DataStructure::Document(document) => document.current_database.clone(),
        _ => None,
    };

    declared.or_else(|| {
        snapshot
            .databases()
            .iter()
            .find(|database| database.is_current)
            .map(|database| database.name.clone())
    })
}

fn lists_objects(relational: &RelationalSchema) -> bool {
    !relational.schemas.is_empty() || !relational.tables.is_empty() || !relational.views.is_empty()
}

/// Names listed in a relational snapshot. With `schema` set, only that schema
/// counts, and `None` is returned when the snapshot does not know it.
fn relational_names(relational: &RelationalSchema, schema: Option<&str>) -> Option<Vec<String>> {
    let top_level = object_names(&relational.tables, &relational.views, schema);

    let Some(wanted) = schema else {
        let mut names = top_level.unwrap_or_default();

        for listing in &relational.schemas {
            names.extend(all_names(listing));
        }

        return Some(names);
    };

    let known_schema = relational
        .schemas
        .iter()
        .find(|listing| listing.name == wanted);

    match (known_schema, top_level) {
        (None, None) => None,
        (known_schema, top_level) => {
            let mut names = top_level.unwrap_or_default();
            names.extend(known_schema.into_iter().flat_map(all_names));

            Some(names)
        }
    }
}

fn all_names(listing: &DbSchemaInfo) -> Vec<String> {
    let tables = listing.tables.iter().map(|table| table.name.clone());
    let views = listing.views.iter().map(|view| view.name.clone());

    tables.chain(views).collect()
}

/// Names of the tables and views in `schema`. Without a schema every object
/// counts. With one, only objects that declare that schema count, and `None`
/// is returned when no object declares it.
fn object_names(
    tables: &[TableInfo],
    views: &[ViewInfo],
    schema: Option<&str>,
) -> Option<Vec<String>> {
    let in_schema =
        |object_schema: Option<&str>| schema.is_none_or(|wanted| object_schema == Some(wanted));

    let table_names = tables
        .iter()
        .filter(|table| in_schema(table.schema.as_deref()))
        .map(|table| table.name.clone());

    let view_names = views
        .iter()
        .filter(|view| in_schema(view.schema.as_deref()))
        .map(|view| view.name.clone());

    let names: Vec<String> = table_names.chain(view_names).collect();

    (schema.is_none() || !names.is_empty()).then_some(names)
}

/// A name that differs only in case counts as present, because the metadata
/// does not say how the engine folds an identifier in this position.
fn contains_name(names: &[String], wanted: &str) -> bool {
    let wanted = wanted.to_lowercase();

    names.iter().any(|name| name.to_lowercase() == wanted)
}

fn unlisted_object_message(
    kind: &str,
    name: &str,
    scope: &Scope,
    names: &[String],
    database_count: usize,
    allowed: HintPermissions,
) -> String {
    let database = scope
        .database
        .as_deref()
        .filter(|_| scope.database_from_caller || allowed.databases);

    let place = match (scope.schema.as_deref(), database) {
        (Some(schema), Some(database)) => format!("schema '{schema}' of database '{database}'"),
        (Some(schema), None) => format!("schema '{schema}'"),
        (None, Some(database)) => format!("database '{database}'"),
        (None, None) => "the current database".to_string(),
    };

    let mut lines = vec![format!(
        "{} '{name}' is not listed in {place}. {NOT_LISTED_REASON}",
        capitalized(kind)
    )];

    let suggestions = suggest_names(name, names.iter().map(String::as_str));

    if !suggestions.is_empty() {
        lines.push(format!("Did you mean: {}?", suggestions.join(", ")));
    }

    if allowed.databases && !scope.database_from_caller && database_count > 1 {
        lines.push(format!(
            "This call did not pass `database`, so the {kind} may be in another database. Pass \
             `database` to select one (`list_databases` lists them)."
        ));
    }

    lines.join("\n")
}

fn unlisted_column_message(column: &str, table: &str, columns: &[String]) -> String {
    let available: Vec<&str> = columns.iter().map(String::as_str).collect();

    let mut lines = vec![unlisted_column_line(column, table)];
    lines.extend(column_hints(column, &available));

    lines.join("\n")
}

fn unlisted_column_line(column: &str, table: &str) -> String {
    format!("Column '{column}' is not listed among the columns of table '{table}'.")
}

fn column_hints(wanted: &str, available: &[&str]) -> Vec<String> {
    let mut hints = Vec::new();

    let suggestions = suggest_names(wanted, available.iter().copied());

    if !suggestions.is_empty() {
        hints.push(format!("Did you mean: {}?", suggestions.join(", ")));
    }

    if !available.is_empty() && available.len() <= MAX_LISTED_COLUMNS {
        hints.push(format!("Available columns: {}", available.join(", ")));
    }

    hints
}

fn capitalized(word: &str) -> String {
    let mut characters = word.chars();

    match characters.next() {
        Some(first) => first.to_uppercase().chain(characters).collect(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: HintPermissions = HintPermissions {
        table_names: true,
        column_names: true,
        databases: true,
    };

    fn names(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn table(name: &str, schema: Option<&str>) -> TableInfo {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "schema": schema,
            "columns": null,
            "indexes": null
        }))
        .expect("build the table metadata")
    }

    fn scope(database: Option<&str>, schema: Option<&str>, database_from_caller: bool) -> Scope {
        Scope {
            database: database.map(str::to_string),
            schema: schema.map(str::to_string),
            database_from_caller,
        }
    }

    #[test]
    fn only_query_failures_and_not_found_errors_run_the_lookup() {
        let looked_up = [DbError::query_failed("x"), DbError::object_not_found("x")];
        let skipped = [
            DbError::connection_failed("x"),
            DbError::auth_failed("x"),
            DbError::constraint_violation("x"),
            DbError::syntax_error("x"),
            DbError::permission_denied("x"),
            DbError::Timeout,
            DbError::Cancelled,
            DbError::NotSupported("x".into()),
            DbError::InvalidProfile("x".into()),
            DbError::value_resolution_failed("x"),
            DbError::IoError(std::io::Error::other("x")),
            DbError::Parse("x".into()),
            DbError::Unsupported("x".into()),
        ];

        for error in &looked_up {
            assert!(may_be_missing_object(error), "{error:?}");
        }

        for error in &skipped {
            assert!(!may_be_missing_object(error), "{error:?}");
        }

        assert!(!CallFailure::from("early").may_be_missing_object);
    }

    #[test]
    fn unlisted_object_message_states_only_what_was_observed() {
        let message = unlisted_object_message(
            "table",
            "usres",
            &scope(Some("app"), Some("public"), false),
            &names(&["users", "orders"]),
            3,
            ALL,
        );

        assert_eq!(
            message,
            "Table 'usres' is not listed in schema 'public' of database 'app'. It may not \
             exist, or this connection may not have access to it.\n\
             Did you mean: users?\n\
             This call did not pass `database`, so the table may be in another database. Pass \
             `database` to select one (`list_databases` lists them)."
        );
    }

    #[test]
    fn database_details_need_the_list_databases_permission() {
        let without_databases = HintPermissions {
            databases: false,
            ..ALL
        };

        let inferred = unlisted_object_message(
            "collection",
            "invoices",
            &scope(Some("logs"), None, false),
            &names(&["users"]),
            3,
            without_databases,
        );
        assert_eq!(
            inferred,
            format!(
                "Collection 'invoices' is not listed in the current database. {NOT_LISTED_REASON}"
            )
        );

        let named_by_caller = unlisted_object_message(
            "collection",
            "invoices",
            &scope(Some("logs"), None, true),
            &names(&["users"]),
            3,
            without_databases,
        );
        assert_eq!(
            named_by_caller,
            format!("Collection 'invoices' is not listed in database 'logs'. {NOT_LISTED_REASON}")
        );
    }

    #[test]
    fn a_qualified_name_is_checked_against_its_schema_only() {
        let relational = RelationalSchema {
            schemas: vec![
                DbSchemaInfo {
                    name: "public".into(),
                    tables: vec![table("users", Some("public"))],
                    views: Vec::new(),
                    custom_types: None,
                },
                DbSchemaInfo {
                    name: "audit".into(),
                    tables: vec![table("events", Some("audit"))],
                    views: Vec::new(),
                    custom_types: None,
                },
            ],
            ..RelationalSchema::default()
        };

        assert_eq!(
            relational_names(&relational, Some("audit")),
            Some(names(&["events"]))
        );
        assert_eq!(relational_names(&relational, Some("nope")), None);
        assert_eq!(
            relational_names(&relational, None),
            Some(names(&["users", "events"]))
        );
    }

    #[test]
    fn a_lazy_listing_only_counts_objects_that_declare_the_schema() {
        let tables = [table("users", Some("dbo")), table("loose", None)];

        assert_eq!(
            object_names(&tables, &[], Some("dbo")),
            Some(names(&["users"]))
        );
        assert_eq!(object_names(&tables, &[], Some("nope")), None);
        assert_eq!(
            object_names(&tables, &[], None),
            Some(names(&["users", "loose"]))
        );
    }

    #[test]
    fn long_column_lists_are_left_out() {
        let many: Vec<String> = (0..=MAX_LISTED_COLUMNS)
            .map(|index| format!("column_{index}"))
            .collect();
        let available: Vec<&str> = many.iter().map(String::as_str).collect();

        let message = with_column_hints("column 'colum_3' not found".into(), "colum_3", &available);

        assert!(message.contains("Did you mean: column_3"), "got: {message}");
        assert!(!message.contains("Available columns"), "got: {message}");
    }

    #[test]
    fn a_name_that_differs_only_in_case_counts_as_present() {
        assert!(contains_name(&names(&["Users"]), "users"));
        assert!(!contains_name(&names(&["Users"]), "user"));
    }

    #[test]
    fn a_declared_pseudo_column_counts_as_listed_in_any_case() {
        let listed = TableColumns {
            columns: names(&["label"]),
            pseudo_columns: names(&["rowid", "_rowid_"]),
        };

        assert!(listed.lists("label"));
        assert!(listed.lists("ROWID"));
        assert!(listed.lists("_rowid_"));
        assert!(!listed.lists("oid"));
    }

    #[test]
    fn expressions_are_not_plain_identifiers() {
        assert!(is_plain_identifier("created_at"));
        assert!(is_plain_identifier("users.email"));

        for expression in ["lower(email)", "count(*)", "a + b", "*", "", "\"my col\""] {
            assert!(!is_plain_identifier(expression), "{expression}");
        }
    }

    #[test]
    fn filter_columns_skips_paths_and_other_tables() {
        let filter = dbflux_core::parse_semantic_filter_json(&serde_json::json!({
            "status": "active",
            "users.email": "a@b.c",
            "orders.total": 1,
            "profile.address.city": "x"
        }))
        .expect("parse the filter")
        .expect("a non-empty filter");

        let mut columns = filter_columns(Some(&filter), &TableRef::new("users"));
        columns.sort();

        assert_eq!(columns, ["email", "status"]);
    }
}
