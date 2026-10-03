//! `select_data` across joined tables.
//!
//! The call is translated into a `VisualQuerySpec` and rendered by the
//! connection's structured SELECT generator. Nothing the client sends is
//! copied into the SQL: table names, aliases and column names must be plain
//! identifiers and are quoted by the dialect, the join condition is parsed
//! into column comparisons, and filter values are rendered as dialect
//! literals.
//!
//! A call without joins takes the same path when `columns` names a
//! pseudo-column, because the browse path reads `SELECT *`, which never
//! returns one.

use std::sync::Arc;

use dbflux_core::{
    ColumnMeta, ColumnRef, Comparator, Connection, ExecutionClassification, FilterNode,
    JoinComparison, JoinKind, JoinStep, ProjectedColumn, Projection, QueryGenerator, QueryRequest,
    QueryResult, SemanticFilter, SortEntry, SourceTable, SqlDialect, TableRef, VisualQuerySpec,
    VisualSortDirection, WhereOperator, classify_query_for_governance, is_plain_sql_identifier,
    join_on_conditions, parse_join_condition, semantic_filter_to_filter_node,
};

use crate::{
    helper::value_to_json,
    server::DbFluxServer,
    state::ServerState,
    tools::{
        not_found::{self, CallFailure, ColumnCheck, ColumnReference},
        read::{JoinSpec, OrderByItem},
    },
};

const JOINS_UNSUPPORTED: &str = "This connection's driver does not support joins in select_data. \
     Query each table separately.";

/// The arguments of a `select_data` call that has joins.
pub(crate) struct JoinedSelect<'a> {
    pub table: &'a str,
    pub columns: Option<&'a [String]>,
    pub filter: Option<&'a SemanticFilter>,
    pub order_by: Option<&'a [OrderByItem]>,
    pub limit: u32,
    pub offset: u32,
    pub joins: &'a [JoinSpec],
    pub database: Option<&'a str>,
}

/// Why a `select_data` call runs as a generated SELECT, which words its
/// refusals.
#[derive(Clone, Copy)]
pub(crate) enum QueryShape {
    Joined,

    /// A call without joins whose `columns` names a pseudo-column.
    PseudoColumns,
}

impl QueryShape {
    fn condition(self) -> &'static str {
        match self {
            Self::Joined => "when joins are used",
            Self::PseudoColumns => "when columns names a pseudo-column",
        }
    }

    fn filter_context(self) -> &'static str {
        match self {
            Self::Joined => "with joins",
            Self::PseudoColumns => "with a pseudo-column in columns",
        }
    }

    fn query_name(self) -> &'static str {
        match self {
            Self::Joined => "generated join query",
            Self::PseudoColumns => "generated query",
        }
    }
}

/// A table of the query: its name and the alias the generated SQL gives it.
struct ScopedTable {
    name: String,
    alias: String,
}

/// The tables a column reference can name, main table first.
struct JoinScope {
    tables: Vec<ScopedTable>,
    shape: QueryShape,
}

impl JoinScope {
    fn source_alias(&self) -> &str {
        self.tables
            .first()
            .map(|table| table.alias.as_str())
            .unwrap_or_default()
    }

    /// Every name a reference may use: each alias and each table name.
    fn qualifiers(&self) -> Vec<&str> {
        let mut qualifiers: Vec<&str> = Vec::new();

        for table in &self.tables {
            for name in [table.alias.as_str(), table.name.as_str()] {
                if !qualifiers.contains(&name) {
                    qualifiers.push(name);
                }
            }
        }

        qualifiers
    }

    /// The alias `qualifier` refers to. An alias wins over a table name, and
    /// a table name that two tables share must be written as an alias.
    fn alias_of(&self, qualifier: &str) -> Result<&str, String> {
        if let Some(table) = self.tables.iter().find(|table| table.alias == qualifier) {
            return Ok(&table.alias);
        }

        let mut named = self.tables.iter().filter(|table| table.name == qualifier);

        match (named.next(), named.next()) {
            (Some(table), None) => Ok(&table.alias),
            (Some(_), Some(_)) => Err(format!(
                "'{qualifier}' names more than one table in this query; use its alias"
            )),
            (None, _) => Err(format!(
                "'{qualifier}' is not a table in this query (known: {})",
                self.qualifiers().join(", ")
            )),
        }
    }

    /// Resolves a column written as `qualifier.column` or `column`. A bare
    /// column belongs to the main table.
    fn resolve(&self, qualifier: Option<&str>, column: &str) -> Result<(String, String), String> {
        require_plain_identifier("column", column, self.shape)?;

        let alias = match qualifier {
            Some(qualifier) => self.alias_of(qualifier)?,
            None => self.source_alias(),
        };

        Ok((alias.to_string(), column.to_string()))
    }

    fn resolve_text(&self, text: &str) -> Result<(String, String), String> {
        match text.split_once('.') {
            Some((qualifier, column)) => self.resolve(Some(qualifier), column),
            None => self.resolve(None, text),
        }
    }
}

/// Security invariant: every table name, alias and column name the client
/// sends passes through here before it reaches the generator. A plain
/// identifier cannot close the dialect's quoting and cannot contain a
/// parameter placeholder (`?`, `$1`, `@p1`), which the generator would
/// otherwise replace with a filter value inside the identifier.
fn require_plain_identifier(kind: &str, value: &str, shape: QueryShape) -> Result<(), String> {
    if is_plain_sql_identifier(value) {
        return Ok(());
    }

    Err(format!(
        "{kind} '{value}' must be a plain identifier {} \
         (letters, digits and underscores, not starting with a digit)",
        shape.condition()
    ))
}

fn require_plain_table(
    table: &TableRef,
    dialect: &dyn SqlDialect,
    shape: QueryShape,
) -> Result<(), String> {
    if let Some(schema) = &table.schema {
        require_plain_identifier("schema", schema, shape)?;
        require_schema_kept(schema, &table.name, dialect)?;
    }

    require_plain_identifier("table", &table.name, shape)
}

/// Refuses a schema-qualified table on a dialect that renders the table name
/// without its schema, because the query would read another table.
fn require_schema_kept(schema: &str, table: &str, dialect: &dyn SqlDialect) -> Result<(), String> {
    if dialect.qualified_table(Some(schema), table) != dialect.qualified_table(None, table) {
        return Ok(());
    }

    Err(format!(
        "Table '{schema}.{table}': this connection does not qualify table names with a schema \
         in generated queries, so the join would read '{table}' instead. Use '{table}' without \
         a qualifier when that is the table you mean"
    ))
}

/// Characters that can end a quoted database name or a statement in the
/// driver code that switches databases.
const DATABASE_NAME_FORBIDDEN: [char; 7] = ['`', '"', '\'', '[', ']', '\\', ';'];

/// Checks the `database` argument of a generated SELECT before it reaches the
/// driver. Database names are not held to the plain-identifier rule, because
/// engines accept names with hyphens, dots and spaces. Only characters that
/// can break quoting are refused: quotes of every dialect, brackets,
/// backslash, semicolon and control characters.
pub(super) fn require_safe_database_name(database: &str, shape: QueryShape) -> Result<(), String> {
    if database.is_empty() {
        return Err(format!("database must not be empty {}", shape.condition()));
    }

    match database
        .chars()
        .find(|character| character.is_control() || DATABASE_NAME_FORBIDDEN.contains(character))
    {
        Some(character) => Err(format!(
            "database name contains {character:?}, which is not accepted {}",
            shape.condition()
        )),
        None => Ok(()),
    }
}

fn join_kind(join_type: &str) -> Result<JoinKind, String> {
    match join_type.trim().to_ascii_lowercase().as_str() {
        "inner" => Ok(JoinKind::Inner),
        "left" => Ok(JoinKind::Left),
        "right" => Ok(JoinKind::Right),
        "full" => Ok(JoinKind::Full),
        other => Err(format!(
            "Unsupported join type '{other}'. Supported join types: inner, left, right, full"
        )),
    }
}

/// Adds one join to `scope` and returns its step. The condition may name the
/// main table, this join and the joins before it.
fn join_step(
    join: &JoinSpec,
    target: &TableRef,
    scope: &mut JoinScope,
    dialect: &dyn SqlDialect,
) -> Result<JoinStep, String> {
    let kind = join_kind(&join.r#type)?;
    require_plain_table(target, dialect, scope.shape)?;

    let alias = match &join.alias {
        Some(alias) => {
            require_plain_identifier("alias", alias, scope.shape)?;
            alias.clone()
        }
        None => target.name.clone(),
    };

    if scope
        .tables
        .iter()
        .any(|table| table.alias.eq_ignore_ascii_case(&alias))
    {
        return Err(format!(
            "'{alias}' is already used by another table in this query; give this join a different alias"
        ));
    }

    scope.tables.push(ScopedTable {
        name: target.name.clone(),
        alias: alias.clone(),
    });

    let comparisons = parse_join_condition(&join.on, &scope.qualifiers())
        .map_err(|error| format!("Join on '{}': {error}", target.name))?
        .into_iter()
        .map(|mut comparison| {
            comparison.left.qualifier = scope.alias_of(&comparison.left.qualifier)?.to_string();
            comparison.right.qualifier = scope.alias_of(&comparison.right.qualifier)?.to_string();

            Ok(comparison)
        })
        .collect::<Result<Vec<JoinComparison>, String>>()?;

    let on = join_on_conditions(&comparisons, |reference| {
        format!(
            "{}.{}",
            dialect.quote_identifier(&reference.qualifier),
            dialect.quote_identifier(&reference.column)
        )
    });

    Ok(JoinStep {
        kind,
        from_alias: scope.source_alias().to_string(),
        to_schema: target.schema.clone(),
        to_table: target.name.clone(),
        to_alias: alias,
        on,
    })
}

/// Builds the projection. A qualified entry is returned under the name it was
/// written with, so two tables with the same column name stay apart.
fn projection(request: &JoinedSelect<'_>, scope: &JoinScope) -> Result<Projection, String> {
    let mut entries: Vec<String> = request.columns.unwrap_or_default().to_vec();
    let has_source_columns = !entries.is_empty();

    for (join, table) in request.joins.iter().zip(scope.tables.iter().skip(1)) {
        for column in join.columns.as_deref().unwrap_or_default() {
            require_plain_identifier("column", column, scope.shape)?;
            entries.push(format!("{}.{column}", table.alias));
        }
    }

    if entries.is_empty() {
        return Ok(Projection::All);
    }

    if !has_source_columns {
        return Err(
            "A join lists `columns` but the call does not: list the main table's columns in \
             the top-level `columns`"
                .to_string(),
        );
    }

    let mut projected: Vec<ProjectedColumn> = Vec::with_capacity(entries.len());
    let mut output_names: Vec<&str> = Vec::with_capacity(entries.len());

    for entry in &entries {
        if output_names.contains(&entry.as_str()) {
            return Err(format!("Column '{entry}' is listed more than once"));
        }
        output_names.push(entry);

        let (source_alias, column) = scope.resolve_text(entry)?;

        projected.push(ProjectedColumn {
            source_alias,
            column,
            alias: entry.contains('.').then(|| entry.clone()),
        });
    }

    Ok(Projection::Explicit(projected))
}

fn sort_entries(
    order_by: Option<&[OrderByItem]>,
    scope: &JoinScope,
) -> Result<Vec<SortEntry>, String> {
    order_by
        .unwrap_or_default()
        .iter()
        .map(|item| {
            let (source_alias, column) = scope.resolve_text(&item.column)?;

            let direction = sort_direction(item.direction.as_deref())?;

            Ok(SortEntry {
                source_alias,
                column,
                direction,
            })
        })
        .collect()
}

/// Reads an `order_by` direction. Anything but `asc` or `desc`, in any case,
/// is refused instead of falling back to ascending.
fn sort_direction(direction: Option<&str>) -> Result<VisualSortDirection, String> {
    match direction.map(str::to_ascii_lowercase).as_deref() {
        None | Some("asc") => Ok(VisualSortDirection::Asc),
        Some("desc") => Ok(VisualSortDirection::Desc),
        Some(_) => Err(format!(
            "Unsupported order_by direction '{}'. Use 'asc' or 'desc'",
            direction.unwrap_or_default()
        )),
    }
}

fn filter_node(
    filter: Option<&SemanticFilter>,
    scope: &JoinScope,
) -> Result<Option<FilterNode>, String> {
    let Some(filter) = filter else {
        return Ok(None);
    };

    semantic_filter_to_filter_node(filter, &|column: &ColumnRef| {
        scope.resolve(column.table.as_deref(), &column.name)
    })
    .map(Some)
    .map_err(|error| format!("Filter error {}: {error}", scope.shape.filter_context()))
}

/// Translates the call into the spec the generator renders. `join_tables`
/// holds the resolved table of each join, in order. Every join in the result
/// is complete, because the generator leaves out a join it finds incomplete.
fn build_spec(
    request: &JoinedSelect<'_>,
    source: &TableRef,
    join_tables: &[TableRef],
    dialect: &dyn SqlDialect,
    shape: QueryShape,
) -> Result<VisualQuerySpec, String> {
    require_plain_table(source, dialect, shape)?;

    if request.limit == 0 {
        return Err(format!("limit must be at least 1 {}", shape.condition()));
    }

    let mut scope = JoinScope {
        tables: vec![ScopedTable {
            name: source.name.clone(),
            alias: source.name.clone(),
        }],
        shape,
    };

    let joins = request
        .joins
        .iter()
        .zip(join_tables)
        .map(|(join, target)| join_step(join, target, &mut scope, dialect))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(VisualQuerySpec {
        source: SourceTable {
            schema: source.schema.clone(),
            table: source.name.clone(),
            alias: source.name.clone(),
        },
        projection: projection(request, &scope)?,
        joins,
        filter: filter_node(request.filter, &scope)?,
        group_by: Vec::new(),
        aggregates: Vec::new(),
        having: None,
        sort: sort_entries(request.order_by, &scope)?,
        limit: Some(u64::from(request.limit)),
        offset: u64::from(request.offset),
    })
}

fn uses_comparator(node: &FilterNode, comparator: Comparator) -> bool {
    match node {
        FilterNode::Predicate(predicate) => predicate.comparator == comparator,
        FilterNode::Group { children, .. } => children
            .iter()
            .any(|child| uses_comparator(child, comparator)),
    }
}

/// The columns the spec names in its projection, filter and sort, each with
/// the table its alias stands for.
fn spec_column_references(spec: &VisualQuerySpec) -> Vec<ColumnReference> {
    let source = std::iter::once((
        spec.source.alias.as_str(),
        spec.source.schema.as_deref(),
        spec.source.table.as_str(),
    ));
    let joined = spec.joins.iter().map(|join| {
        (
            join.to_alias.as_str(),
            join.to_schema.as_deref(),
            join.to_table.as_str(),
        )
    });

    let tables: Vec<(&str, TableRef)> = source
        .chain(joined)
        .map(|(alias, schema, table)| {
            let table = match schema {
                Some(schema) => TableRef::with_schema(schema, table),
                None => TableRef::new(table),
            };

            (alias, table)
        })
        .collect();

    let mut named: Vec<(&str, &str)> = Vec::new();

    if let Projection::Explicit(columns) = &spec.projection {
        named.extend(
            columns
                .iter()
                .map(|column| (column.source_alias.as_str(), column.column.as_str())),
        );
    }

    if let Some(filter) = &spec.filter {
        filter_columns(filter, &mut named);
    }

    named.extend(
        spec.sort
            .iter()
            .map(|entry| (entry.source_alias.as_str(), entry.column.as_str())),
    );

    named
        .into_iter()
        .filter_map(|(alias, column)| {
            let (_, table) = tables.iter().find(|(known, _)| *known == alias)?;

            Some(ColumnReference {
                table: table.clone(),
                column: column.to_string(),
            })
        })
        .collect()
}

fn filter_columns<'a>(node: &'a FilterNode, named: &mut Vec<(&'a str, &'a str)>) {
    match node {
        FilterNode::Predicate(predicate) => {
            named.push((predicate.source_alias.as_str(), predicate.column.as_str()));
        }
        FilterNode::Group { children, .. } => {
            for child in children {
                filter_columns(child, named);
            }
        }
    }
}

/// Serializes the result with unique column names, so a name two tables share
/// is not lost when a row becomes a JSON object. A repeated name gets the
/// first numeric suffix that neither an earlier name nor any column of the
/// result uses: `id`, `id_2`, or `id_3` when the result has an `id_2` column.
fn serialize_joined_result(result: &QueryResult) -> serde_json::Value {
    let names = unique_column_names(&result.columns);

    let rows: Vec<serde_json::Value> = result
        .rows
        .iter()
        .map(|row| {
            let object = names
                .iter()
                .zip(row.iter())
                .map(|(name, cell)| (name.clone(), value_to_json(cell)))
                .collect::<serde_json::Map<_, _>>();

            serde_json::Value::Object(object)
        })
        .collect();

    serde_json::json!({
        "columns": names,
        "rows": rows,
        "row_count": result.rows.len(),
    })
}

fn unique_column_names(columns: &[ColumnMeta]) -> Vec<String> {
    let mut names: Vec<String> = Vec::with_capacity(columns.len());

    for column in columns {
        let mut name = column.name.clone();
        let mut occurrence = 2;

        while names.contains(&name) {
            name = format!("{}_{occurrence}", column.name);
            occurrence += 1;

            // A suffixed name never takes the name of another column.
            if columns.iter().any(|other| other.name == name) {
                name = column.name.clone();
            }
        }

        names.push(name);
    }

    names
}

/// Serializes the result of a generated SELECT without joins under the names
/// `columns` wrote, in that order, the way the browse path does. The names
/// are taken by position because the projection is explicit and the engine
/// may report another name: SQLite names a projected `rowid` after the
/// `INTEGER PRIMARY KEY` it aliases.
fn serialize_projected_result(
    result: &QueryResult,
    columns: &[String],
) -> Result<serde_json::Value, String> {
    if result.columns.len() != columns.len() {
        return Err(format!(
            "Select error: the generated query returned {} columns for the {} requested",
            result.columns.len(),
            columns.len()
        ));
    }

    let rows: Vec<serde_json::Value> = result
        .rows
        .iter()
        .map(|row| {
            let object = columns
                .iter()
                .zip(row.iter())
                .map(|(name, cell)| (name.clone(), value_to_json(cell)))
                .collect::<serde_json::Map<_, _>>();

            serde_json::Value::Object(object)
        })
        .collect();

    Ok(serde_json::json!({
        "columns": columns,
        "rows": rows,
        "row_count": result.rows.len(),
    }))
}

impl DbFluxServer {
    /// Runs a `select_data` call that has joins and returns the result with
    /// the SQL that produced it. The columns the call names are checked
    /// against each table's metadata before the query runs.
    pub(super) async fn select_data_joined(
        state: &ServerState,
        connection_id: &str,
        connection: &Arc<dyn Connection>,
        request: JoinedSelect<'_>,
    ) -> Result<(serde_json::Value, String), CallFailure> {
        let (query_request, references) = Self::plan_joined_select(connection, &request)?;
        let sql = query_request.sql.clone();

        not_found::check_columns(ColumnCheck {
            state,
            connection_id,
            connection,
            database: request.database,
            references,
        })
        .await?;

        let result = Self::execute_generated(connection, query_request).await?;

        Ok((serialize_joined_result(&result), sql))
    }

    /// Runs a `select_data` call without joins as a generated SELECT, so a
    /// pseudo-column named in `columns` comes back, and returns the result
    /// with the SQL that produced it. The caller has already checked the
    /// columns. `None` means the connection cannot generate the query, and the
    /// caller keeps the browse path.
    pub(super) async fn select_data_projecting_pseudo_columns(
        connection: &Arc<dyn Connection>,
        request: JoinedSelect<'_>,
        columns: &[String],
    ) -> Result<Option<(serde_json::Value, String)>, CallFailure> {
        let shape = QueryShape::PseudoColumns;

        let Some(generator) = connection.query_generator() else {
            return Ok(None);
        };

        if let Some(database) = request.database {
            require_safe_database_name(database, shape)?;
        }

        let source = Self::table_ref_for_connection(connection, request.table);
        let spec = build_spec(&request, &source, &[], connection.dialect(), shape)?;

        let Some(query_request) =
            Self::generate_read_query(connection, generator, &spec, request.database, shape)?
        else {
            return Ok(None);
        };

        let sql = query_request.sql.clone();
        let result = Self::execute_generated(connection, query_request).await?;

        Ok(Some((serialize_projected_result(&result, columns)?, sql)))
    }

    async fn execute_generated(
        connection: &Arc<dyn Connection>,
        query_request: QueryRequest,
    ) -> Result<QueryResult, CallFailure> {
        let conn = connection.clone();
        #[allow(clippy::result_large_err)]
        let result = tokio::task::spawn_blocking(move || conn.execute(&query_request))
            .await
            .map_err(|e| format!("Blocking task failed: {}", e))?
            .map_err(|e| CallFailure::from_driver(format!("Select error: {}", e), &e))?;

        Ok(result)
    }

    /// Checks that the driver supports joins, generates the query and checks
    /// that it is a read. Also returns the columns the query names, each with
    /// its table.
    fn plan_joined_select(
        connection: &Arc<dyn Connection>,
        request: &JoinedSelect<'_>,
    ) -> Result<(QueryRequest, Vec<ColumnReference>), String> {
        let shape = QueryShape::Joined;

        let declares_joins = connection
            .metadata()
            .query
            .as_ref()
            .is_some_and(|query| query.supports_joins);

        let generator = connection
            .query_generator()
            .filter(|_| declares_joins)
            .ok_or_else(|| JOINS_UNSUPPORTED.to_string())?;

        let source = Self::table_ref_for_connection(connection, request.table);
        let join_tables: Vec<TableRef> = request
            .joins
            .iter()
            .map(|join| Self::table_ref_for_connection(connection, &join.table))
            .collect();

        let spec = build_spec(request, &source, &join_tables, connection.dialect(), shape)?;

        let query_request =
            Self::generate_read_query(connection, generator, &spec, request.database, shape)?
                .ok_or_else(|| JOINS_UNSUPPORTED.to_string())?;

        Ok((query_request, spec_column_references(&spec)))
    }

    /// Renders `spec` through the connection's generator and returns it only
    /// when it classifies as a read. `None` means the generator cannot render
    /// a SELECT.
    fn generate_read_query(
        connection: &Arc<dyn Connection>,
        generator: &dyn QueryGenerator,
        spec: &VisualQuerySpec,
        database: Option<&str>,
        shape: QueryShape,
    ) -> Result<Option<QueryRequest>, String> {
        let metadata = connection.metadata();

        let declares_ilike = metadata
            .query
            .as_ref()
            .is_some_and(|query| query.where_operators.contains(&WhereOperator::ILike));

        if !declares_ilike
            && spec
                .filter
                .as_ref()
                .is_some_and(|filter| uses_comparator(filter, Comparator::ILike))
        {
            return Err(format!(
                "$ilike is not supported {} on this connection, because its driver does \
                 not declare case-insensitive matching. Use $like instead",
                shape.filter_context()
            ));
        }

        let Some(select) = generator
            .generate_select(spec)
            .map_err(|error| format!("Select error: {error}"))?
        else {
            return Ok(None);
        };

        let query_request = select.to_query_request(connection.dialect());

        // Security invariant: the generated text runs only when it classifies
        // as a single read, whatever the generator produced.
        let classification = classify_query_for_governance(
            &metadata.query_language,
            &query_request.sql,
            Some(connection.language_service()),
        );

        if !matches!(
            classification,
            ExecutionClassification::Metadata | ExecutionClassification::Read
        ) {
            return Err(format!(
                "Select error: the {} is not a read-only query and was not run",
                shape.query_name()
            ));
        }

        Ok(Some(
            query_request
                .with_database(database.map(str::to_string))
                .with_confirmed_ceiling(ExecutionClassification::Read),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use dbflux_core::{
        DbDriver, DbError, DbKind, DefaultSqlDialect, DriverMetadata, GeneratedQuery,
        MutationCategory, MutationRequest, QueryCapabilities, QueryGenError, QueryGenerator,
        QueryHandle, SchemaLoadingStrategy, SchemaSnapshot, parse_semantic_filter_json,
    };

    fn join(join_type: &str, table: &str, alias: Option<&str>, on: &str) -> JoinSpec {
        JoinSpec {
            r#type: join_type.to_string(),
            table: table.to_string(),
            on: on.to_string(),
            alias: alias.map(str::to_string),
            columns: None,
        }
    }

    fn request<'a>(joins: &'a [JoinSpec]) -> JoinedSelect<'a> {
        JoinedSelect {
            table: "users",
            columns: None,
            filter: None,
            order_by: None,
            limit: 100,
            offset: 0,
            joins,
            database: None,
        }
    }

    fn sql_for(request: &JoinedSelect<'_>) -> Result<String, String> {
        let join_tables: Vec<TableRef> = request
            .joins
            .iter()
            .map(|join| TableRef::from_qualified(&join.table))
            .collect();

        let spec = build_spec(
            request,
            &TableRef::with_schema("public", "users"),
            &join_tables,
            &DefaultSqlDialect,
            QueryShape::Joined,
        )?;

        dbflux_core::SqlMutationGenerator::new(&DefaultSqlDialect)
            .generate_select(&spec)
            .map_err(|error| error.to_string())?
            .map(|select| select.to_query_request(&DefaultSqlDialect).sql)
            .ok_or_else(|| "no query".to_string())
    }

    #[test]
    fn generated_sql_quotes_every_identifier_and_inlines_values_as_literals() {
        let joins = [
            join("left", "orders", Some("o"), "users.id = o.user_id"),
            join(
                "INNER",
                "sales.items",
                None,
                "items.order_id = o.id AND items.qty >= o.min_qty",
            ),
        ];
        let columns = vec!["email".to_string(), "o.total".to_string()];
        let filter = parse_semantic_filter_json(&serde_json::json!({ "o.status": "it's" }))
            .expect("the filter parses");
        let order_by = [OrderByItem {
            column: "items.qty".to_string(),
            direction: Some("DESC".to_string()),
        }];

        let mut request = request(&joins);
        request.columns = Some(&columns);
        request.filter = filter.as_ref();
        request.order_by = Some(&order_by);
        request.limit = 25;
        request.offset = 5;

        assert_eq!(
            sql_for(&request).expect("the call is valid"),
            "SELECT \"users\".\"email\", \"o\".\"total\" AS \"o.total\"\n\
             FROM \"public\".\"users\" AS \"users\"\n\
             LEFT JOIN \"orders\" AS \"o\" ON \"users\".\"id\" = \"o\".\"user_id\"\n\
             INNER JOIN \"sales\".\"items\" AS \"items\" ON \"items\".\"order_id\" = \"o\".\"id\" \
             AND \"items\".\"qty\" >= \"o\".\"min_qty\"\n\
             WHERE \"o\".\"status\" = 'it''s'\n\
             ORDER BY \"items\".\"qty\" DESC\n\
             LIMIT 25\n\
             OFFSET 5"
        );
    }

    #[test]
    fn a_table_name_resolves_to_the_alias_of_its_join() {
        let joins = [join(
            "inner",
            "orders",
            Some("o"),
            "users.id = orders.user_id",
        )];

        let sql = sql_for(&request(&joins)).expect("the table name names the aliased join");

        assert!(
            sql.contains("ON \"users\".\"id\" = \"o\".\"user_id\""),
            "{sql}"
        );
    }

    #[test]
    fn a_table_joined_twice_must_be_named_by_alias() {
        let self_join = [join(
            "inner",
            "users",
            Some("manager"),
            "users.manager_id = manager.id",
        )];
        let sql = sql_for(&request(&self_join)).expect("'users' names the main table");
        assert!(
            sql.contains("ON \"users\".\"manager_id\" = \"manager\".\"id\""),
            "{sql}"
        );

        let joins = [
            join("inner", "orders", Some("first"), "users.id = first.user_id"),
            join("inner", "orders", Some("last"), "users.id = orders.user_id"),
        ];
        let error = sql_for(&request(&joins)).expect_err("'orders' is ambiguous");
        assert!(error.contains("names more than one table"), "{error}");
    }

    #[test]
    fn a_join_cannot_reference_a_later_join() {
        let joins = [
            join("inner", "orders", None, "orders.id = items.order_id"),
            join("inner", "items", None, "items.order_id = orders.id"),
        ];

        let error = sql_for(&request(&joins)).expect_err("'items' is joined later");

        assert!(error.contains("not a table in this query"), "{error}");
    }

    #[test]
    fn every_requested_join_is_in_the_generated_sql() {
        let joins = [
            join("inner", "orders", None, "users.id = orders.user_id"),
            join("right", "items", None, "items.order_id = orders.id"),
            join("full", "refunds", None, "refunds.item_id = items.id"),
        ];

        let sql = sql_for(&request(&joins)).expect("the call is valid");

        assert_eq!(sql.matches(" JOIN ").count(), joins.len(), "{sql}");
        assert!(sql.contains("RIGHT JOIN \"items\""), "{sql}");
        assert!(sql.contains("FULL OUTER JOIN \"refunds\""), "{sql}");
    }

    #[test]
    fn invalid_calls_are_rejected_before_generation() {
        let valid_on = "users.id = orders.user_id";

        let cases = [
            (join("cross", "orders", None, valid_on), "join type"),
            (join("inner", "orders", None, ""), "Accepted form"),
            (join("inner", "or ders", None, valid_on), "plain identifier"),
            (join("inner", "", None, valid_on), "plain identifier"),
            (
                join("inner", "orders", Some(""), valid_on),
                "plain identifier",
            ),
            (
                join("inner", "orders", Some("Users"), valid_on),
                "already used",
            ),
            (join("inner", "users", None, valid_on), "already used"),
        ];

        for (join, expected) in cases {
            let joins = [join];
            let error = sql_for(&request(&joins)).expect_err("the call is invalid");

            assert!(error.contains(expected), "expected '{expected}': {error}");
        }

        let joins = [join("inner", "orders", None, valid_on)];
        let mut zero_limit = request(&joins);
        zero_limit.limit = 0;
        let error = sql_for(&zero_limit).expect_err("limit 0 would drop the LIMIT clause");
        assert!(error.contains("limit must be at least 1"), "{error}");
    }

    #[test]
    fn join_columns_need_the_top_level_columns() {
        let mut joined = join("inner", "orders", None, "users.id = orders.user_id");
        joined.columns = Some(vec!["total".to_string()]);
        let joins = [joined];

        let error = sql_for(&request(&joins)).expect_err("the main table's columns are missing");
        assert!(error.contains("top-level `columns`"), "{error}");

        let columns = vec!["orders.total".to_string()];
        let mut duplicated = request(&joins);
        duplicated.columns = Some(&columns);
        let error = sql_for(&duplicated).expect_err("the column is listed twice");
        assert!(error.contains("listed more than once"), "{error}");
    }

    #[test]
    fn an_unknown_sort_direction_is_rejected() {
        let joins = [join("inner", "orders", None, "users.id = orders.user_id")];

        for direction in ["DESC", "Asc"] {
            let order_by = [OrderByItem {
                column: "orders.total".to_string(),
                direction: Some(direction.to_string()),
            }];
            let mut sorted = request(&joins);
            sorted.order_by = Some(&order_by);

            assert!(sql_for(&sorted).is_ok(), "{direction} is a direction");
        }

        let order_by = [OrderByItem {
            column: "orders.total".to_string(),
            direction: Some("descending".to_string()),
        }];
        let mut sorted = request(&joins);
        sorted.order_by = Some(&order_by);

        let error = sql_for(&sorted).expect_err("'descending' is not a direction");
        assert!(
            error.contains("Unsupported order_by direction 'descending'"),
            "{error}"
        );
    }

    #[test]
    fn a_suffix_never_takes_the_name_of_another_column() {
        let columns = |names: &[&str]| -> Vec<ColumnMeta> {
            names
                .iter()
                .map(|name| ColumnMeta {
                    name: name.to_string(),
                    type_name: "text".to_string(),
                    kind: dbflux_core::ColumnKind::Text,
                    nullable: true,
                    is_primary_key: false,
                })
                .collect()
        };

        assert_eq!(
            unique_column_names(&columns(&["id", "id_2", "id"])),
            ["id", "id_2", "id_3"]
        );
        assert_eq!(
            unique_column_names(&columns(&["id", "id", "id_2", "id_2"])),
            ["id", "id_3", "id_2", "id_2_2"]
        );
        assert_eq!(
            unique_column_names(&columns(&["id", "name", "id"])),
            ["id", "name", "id_2"]
        );
    }

    #[test]
    fn database_names_that_can_break_quoting_are_rejected() {
        for accepted in ["analytics", "my-db", "sales.2024", "Sales Q1"] {
            assert!(
                require_safe_database_name(accepted, QueryShape::Joined).is_ok(),
                "{accepted} is a database name"
            );
        }

        for rejected in [
            "", "a`b", "a\"b", "a'b", "a[b", "a]b", "a\\b", "a;b", "a\nb", "a\0b",
        ] {
            assert!(
                require_safe_database_name(rejected, QueryShape::Joined).is_err(),
                "{rejected:?} must be rejected"
            );
        }
    }

    /// A connection that declares join support and whose generator returns
    /// `select_sql` for every SELECT spec, or nothing.
    struct GeneratorConnection {
        metadata: DriverMetadata,
        generator: FixedSelectGenerator,
    }

    struct FixedSelectGenerator {
        select_sql: Option<&'static str>,
    }

    impl QueryGenerator for FixedSelectGenerator {
        fn supported_categories(&self) -> &'static [MutationCategory] {
            &[]
        }

        fn generate_mutation(&self, _mutation: &MutationRequest) -> Option<GeneratedQuery> {
            None
        }

        fn generate_select(
            &self,
            _spec: &VisualQuerySpec,
        ) -> Result<Option<dbflux_core::SelectQuery>, QueryGenError> {
            Ok(self.select_sql.map(|sql| dbflux_core::SelectQuery {
                sql: sql.to_string(),
                params: Vec::new(),
            }))
        }
    }

    impl Connection for GeneratorConnection {
        fn metadata(&self) -> &DriverMetadata {
            &self.metadata
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, _request: &QueryRequest) -> Result<QueryResult, DbError> {
            Err(DbError::query_failed("planning never executes"))
        }

        fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Ok(SchemaSnapshot::default())
        }

        fn kind(&self) -> DbKind {
            DbKind::Postgres
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::ConnectionPerDatabase
        }

        fn dialect(&self) -> &dyn SqlDialect {
            &DefaultSqlDialect
        }

        fn query_generator(&self) -> Option<&dyn QueryGenerator> {
            Some(&self.generator)
        }
    }

    fn generator_connection(
        select_sql: Option<&'static str>,
        where_operators: Vec<WhereOperator>,
    ) -> Arc<dyn Connection> {
        let mut metadata =
            DbDriver::metadata(&dbflux_test_support::FakeDriver::new(DbKind::Postgres)).clone();
        metadata.query = Some(QueryCapabilities {
            supports_joins: true,
            where_operators,
            ..QueryCapabilities::default()
        });

        Arc::new(GeneratorConnection {
            metadata,
            generator: FixedSelectGenerator { select_sql },
        })
    }

    fn plan(
        connection: &Arc<dyn Connection>,
        filter: Option<serde_json::Value>,
    ) -> Result<String, String> {
        let joins = [join("inner", "orders", None, "users.id = orders.user_id")];
        let filter = filter
            .and_then(|filter| parse_semantic_filter_json(&filter).expect("the filter parses"));

        let mut joined = request(&joins);
        joined.filter = filter.as_ref();

        DbFluxServer::plan_joined_select(connection, &joined)
            .map(|(query_request, _)| query_request.sql)
    }

    #[test]
    fn a_generated_query_that_is_not_a_read_is_refused() {
        let connection = generator_connection(
            Some("DELETE FROM \"users\""),
            QueryCapabilities::default().where_operators,
        );

        let error = plan(&connection, None).expect_err("a DELETE is not a read");
        assert!(error.contains("not a read-only query"), "{error}");

        let reading = generator_connection(
            Some("SELECT 1"),
            QueryCapabilities::default().where_operators,
        );
        assert_eq!(plan(&reading, None).as_deref(), Ok("SELECT 1"));
    }

    #[test]
    fn a_generator_without_select_support_gets_the_unsupported_error() {
        let connection = generator_connection(None, QueryCapabilities::default().where_operators);

        assert_eq!(plan(&connection, None), Err(JOINS_UNSUPPORTED.to_string()));
    }

    #[test]
    fn ilike_needs_the_driver_to_declare_it() {
        let filter = serde_json::json!({ "orders.status": { "$ilike": "open%" } });

        let without = generator_connection(
            Some("SELECT 1"),
            QueryCapabilities::default().where_operators,
        );
        let error = plan(&without, Some(filter.clone())).expect_err("$ilike is not declared");
        assert!(
            error.contains("$ilike is not supported with joins"),
            "{error}"
        );

        let mut operators = QueryCapabilities::default().where_operators;
        operators.push(WhereOperator::ILike);
        let with = generator_connection(Some("SELECT 1"), operators);
        assert!(plan(&with, Some(filter)).is_ok());
    }
}
