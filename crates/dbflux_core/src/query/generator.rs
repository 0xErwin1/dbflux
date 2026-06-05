use crate::data::crud::MutationRequest;
use crate::driver::capabilities::QueryLanguage;
use crate::query::semantic::{PlannedQuery, SemanticPlan, SemanticPlanKind};
use crate::query::visual_query::VisualQuerySpec;
use crate::schema::types::ColumnInfo;
use crate::sql::dialect::{PlaceholderStyle, SqlDialect};
use crate::sql::generation::{
    SqlGenerationOptions, SqlGenerationRequest, SqlOperation, SqlValueMode,
};
use crate::sql::query_builder::SqlQueryBuilder;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MutationCategory {
    Sql,
    Document,
    KeyValue,
}

impl MutationRequest {
    pub fn category(&self) -> MutationCategory {
        if self.is_sql() {
            MutationCategory::Sql
        } else if self.is_document() {
            MutationCategory::Document
        } else {
            MutationCategory::KeyValue
        }
    }
}

#[derive(Debug, Clone)]
pub struct GeneratedQuery {
    pub language: QueryLanguage,
    pub text: String,
}

/// Error returned by `QueryGenerator::generate_select`.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum QueryGenError {
    #[error("this generator does not support structured SELECT")]
    Unsupported,
    #[error("invalid spec: {0}")]
    InvalidSpec(String),
    #[error("identifier cannot be escaped: {0}")]
    IdentifierEscape(String),
}

/// A structured SELECT query ready for execution.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectQuery {
    /// Parameterized SQL text.
    pub sql: String,
    /// Bound parameter values in placeholder order.
    pub params: Vec<crate::Value>,
}

impl SelectQuery {
    /// Produces a human-readable SQL string with all parameter placeholders
    /// replaced by their dialect-quoted literal values.
    ///
    /// The result is intended for display in a read-only editor tab ("Open in
    /// editor"). It is NOT suitable for execution — the literal substitution
    /// is for readability only. The substitution respects the dialect's
    /// placeholder style:
    /// - `?` placeholders (SQLite/MySQL): replaced left-to-right.
    /// - `$N` placeholders (PostgreSQL): each `$N` is replaced by `params[N-1]`.
    /// - `@pN` placeholders (SQL Server): each `@pN` is replaced by `params[N-1]`.
    pub fn materialize_for_editor(&self, dialect: &dyn crate::sql::dialect::SqlDialect) -> String {
        inline_params(&self.sql, &self.params, dialect)
    }
}

/// Replace every parameter placeholder in `sql` with its dialect-quoted literal.
///
/// This is both the display path (read-only editor tabs, builder preview,
/// confirmation dialogs) and the execution path for the visual query builder:
/// drivers in this codebase execute `QueryRequest.sql` verbatim and do not bind
/// `QueryRequest.params`, so structured mutations/selects must inline their
/// values before dispatch. The dialect's `value_to_literal` performs the
/// quoting/escaping, mirroring how every other mutation path executes.
///
/// The substitution respects the dialect's placeholder style:
/// - `?` placeholders (SQLite/MySQL): replaced left-to-right.
/// - `$N` placeholders (PostgreSQL): each `$N` is replaced by `params[N-1]`.
/// - `@pN` placeholders (SQL Server): each `@pN` is replaced by `params[N-1]`.
pub fn inline_params(
    sql: &str,
    params: &[crate::Value],
    dialect: &dyn crate::sql::dialect::SqlDialect,
) -> String {
    use crate::sql::dialect::PlaceholderStyle;

    match dialect.placeholder_style() {
        PlaceholderStyle::QuestionMark | PlaceholderStyle::NamedColon => {
            let mut result = String::with_capacity(sql.len());
            let mut param_iter = params.iter();
            let chars: Vec<char> = sql.chars().collect();
            let mut i = 0;

            while i < chars.len() {
                if chars[i] == '?' {
                    if let Some(val) = param_iter.next() {
                        result.push_str(&dialect.value_to_literal(val));
                    } else {
                        result.push('?');
                    }
                    i += 1;
                } else {
                    result.push(chars[i]);
                    i += 1;
                }
            }

            result
        }

        PlaceholderStyle::DollarNumber => {
            materialize_numbered_placeholders(sql, params, dialect, '$', "")
        }

        PlaceholderStyle::AtSign => {
            materialize_numbered_placeholders(sql, params, dialect, '@', "p")
        }
    }
}

/// Replace `<prefix><number>` placeholders in `sql` with dialect literals.
///
/// E.g. `$1` with prefix=`'$'` prefix_str=`""`, or `@p1` with prefix=`'@'`
/// prefix_str=`"p"`.
fn materialize_numbered_placeholders(
    sql: &str,
    params: &[crate::Value],
    dialect: &dyn crate::sql::dialect::SqlDialect,
    prefix_char: char,
    prefix_str: &str,
) -> String {
    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let prefix_bytes = prefix_str.as_bytes();
    let prefix_byte = prefix_char as u8;
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == prefix_byte {
            let start = i;
            i += 1;

            // Consume the optional prefix_str (e.g. "p" for AtSign style).
            if !prefix_bytes.is_empty() {
                let end = i + prefix_bytes.len();
                if end <= bytes.len() && &bytes[i..end] == prefix_bytes {
                    i = end;
                } else {
                    // Emit raw and continue.
                    result.push_str(&sql[start..i]);
                    continue;
                }
            }

            // Consume digits to form the placeholder index.
            let digit_start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }

            if i == digit_start {
                // No digits — not a placeholder, emit raw.
                result.push_str(&sql[start..i]);
                continue;
            }

            let index_str = &sql[digit_start..i];
            if let Ok(n) = index_str.parse::<usize>()
                && n >= 1
                && n <= params.len()
            {
                result.push_str(&dialect.value_to_literal(&params[n - 1]));
                continue;
            }

            // Out-of-range or parse error — emit raw.
            result.push_str(&sql[start..i]);
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }

    result
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationTemplateOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct MutationTemplateRequest<'a> {
    pub operation: MutationTemplateOperation,
    pub schema: Option<&'a str>,
    pub table: &'a str,
    pub columns: &'a [ColumnInfo],
    pub options: SqlGenerationOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadTemplateOperation {
    SelectAll,
    SelectWhere,
}

#[derive(Debug, Clone)]
pub struct ReadTemplateRequest<'a> {
    pub operation: ReadTemplateOperation,
    pub schema: Option<&'a str>,
    pub table: &'a str,
    pub columns: &'a [ColumnInfo],
    pub options: SqlGenerationOptions,
}

impl From<GeneratedQuery> for PlannedQuery {
    fn from(value: GeneratedQuery) -> Self {
        Self::new(value.language, value.text)
    }
}

/// Produces native query/command text from a `MutationRequest`.
///
/// Accessed via `Connection::query_generator()`.
/// Request used by `QueryGenerator::template_for_collection`.
///
/// Carries the information available from a sidebar collection node so the
/// driver can build a query template pre-seeded with the correct bucket and
/// measurement name.
#[derive(Debug, Clone)]
pub struct CollectionTemplateRequest<'a> {
    /// Name of the collection (measurement, table, etc.).
    pub collection: &'a str,
    /// Database or bucket the collection belongs to.
    pub database: &'a str,
}

/// Errors specific to visual-spec SQL generation.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum GeneratorError {
    #[error("UPDATE spec contains no assignments; at least one SET clause is required")]
    EmptyAssignments,
    #[error("unsupported spec: {0}")]
    Unsupported(String),
}

/// Output of `generate_update_from_spec` / `generate_delete_from_spec`.
///
/// `used_raw_expression` is set to `true` when at least one `AssignmentValue::Expression`
/// was present, signalling the classification layer to raise `RawExpressionInSet` without
/// inspecting the SQL text for a marker.
#[derive(Debug, Clone, PartialEq)]
pub struct GeneratedMutation {
    /// Parameterized SQL text ready for execution.
    pub sql: String,
    /// Bound parameter values in placeholder order.
    pub params: Vec<crate::Value>,
    /// True when at least one assignment used `AssignmentValue::Expression`.
    pub used_raw_expression: bool,
}

impl GeneratedMutation {
    /// Produces a human-readable SQL string with all parameter placeholders
    /// replaced by their dialect-quoted literal values.
    ///
    /// Intended for display in the visual builder preview and the mutation
    /// confirmation dialog. It is NOT suitable for execution — execution must
    /// use `sql` + `params` so values stay bound. See
    /// [`SelectQuery::materialize_for_editor`] for the SELECT counterpart.
    pub fn materialize_for_editor(&self, dialect: &dyn crate::sql::dialect::SqlDialect) -> String {
        inline_params(&self.sql, &self.params, dialect)
    }
}

pub trait QueryGenerator: Send + Sync {
    fn supported_categories(&self) -> &'static [MutationCategory];

    fn generate_mutation(&self, mutation: &MutationRequest) -> Option<GeneratedQuery>;

    fn generate_template(&self, _request: &MutationTemplateRequest<'_>) -> Option<GeneratedQuery> {
        None
    }

    fn generate_read_template(&self, _request: &ReadTemplateRequest<'_>) -> Option<GeneratedQuery> {
        None
    }

    /// Generate a query template pre-seeded with the given collection and database.
    ///
    /// Called by the UI when the user selects "Query Measurement" / "Query Collection"
    /// from the sidebar context menu. The returned query is opened in a new code
    /// document so the user can run or modify it immediately.
    ///
    /// Drivers that do not support collection-level templates return `None` (default).
    /// The UI falls back gracefully and does not show the menu item when `None` is
    /// returned for this specific collection.
    fn template_for_collection(
        &self,
        _request: &CollectionTemplateRequest<'_>,
    ) -> Option<GeneratedQuery> {
        None
    }

    fn plan_mutation(&self, mutation: &MutationRequest) -> Option<SemanticPlan> {
        self.generate_mutation(mutation).map(|query| {
            SemanticPlan::single_query(SemanticPlanKind::MutationPreview, query.into())
        })
    }

    /// Render a structured SELECT spec to a parameterized query.
    ///
    /// The default returns `Ok(None)` meaning "not supported by this dialect".
    /// `Err` means the spec is malformed for this dialect.
    /// Only `SqlMutationGenerator` overrides this in v1.
    fn generate_select(
        &self,
        _spec: &VisualQuerySpec,
    ) -> Result<Option<SelectQuery>, QueryGenError> {
        Ok(None)
    }

    /// Generate a parameterized UPDATE statement from a `VisualMutationSpec`.
    ///
    /// Returns `Err(GeneratorError::EmptyAssignments)` when the spec carries no
    /// assignments. The default returns `Err(GeneratorError::Unsupported)`.
    fn generate_update_from_spec(
        &self,
        _spec: &crate::query::visual_query::VisualMutationSpec,
    ) -> Result<GeneratedMutation, GeneratorError> {
        Err(GeneratorError::Unsupported(
            "this generator does not support visual UPDATE".to_string(),
        ))
    }

    /// Generate a parameterized DELETE statement from a `VisualMutationSpec`.
    ///
    /// A `filter: None` spec produces a bare `DELETE FROM table` — the caller
    /// (classification layer) is responsible for the dangerous-query gate.
    /// The default returns `Err(GeneratorError::Unsupported)`.
    fn generate_delete_from_spec(
        &self,
        _spec: &crate::query::visual_query::VisualMutationSpec,
    ) -> Result<GeneratedMutation, GeneratorError> {
        Err(GeneratorError::Unsupported(
            "this generator does not support visual DELETE".to_string(),
        ))
    }

    /// Generate a parameterized UPDATE for one keyset chunk.
    ///
    /// The generated SQL merges `spec.filter` (user predicate) and the PK keyset
    /// into a single `WHERE (<user_filter>) AND (<pk_cols>) IN (row_constructors)`.
    /// Only one WHERE keyword is emitted. `pk_values` is one `Vec<Value>` per row.
    ///
    /// The default delegates to `generate_update_from_spec` then appends the IN
    /// clause — overridden by `SqlMutationGenerator` to emit correct SQL.
    fn generate_update_chunk_from_spec(
        &self,
        _spec: &crate::query::visual_query::VisualMutationSpec,
        pk_cols: &[&str],
        pk_values: &[Vec<crate::Value>],
    ) -> Result<GeneratedMutation, GeneratorError> {
        Err(GeneratorError::Unsupported(format!(
            "generator does not support chunked UPDATE (pk_cols={:?}, rows={})",
            pk_cols,
            pk_values.len()
        )))
    }

    /// Generate a parameterized DELETE for one keyset chunk.
    ///
    /// The generated SQL merges `spec.filter` (user predicate) and the PK keyset
    /// into a single `WHERE (<user_filter>) AND (<pk_cols>) IN (row_constructors)`.
    /// Only one WHERE keyword is emitted. `pk_values` is one `Vec<Value>` per row.
    ///
    /// The default delegates to `generate_delete_from_spec` then appends the IN
    /// clause — overridden by `SqlMutationGenerator` to emit correct SQL.
    fn generate_delete_chunk_from_spec(
        &self,
        _spec: &crate::query::visual_query::VisualMutationSpec,
        pk_cols: &[&str],
        pk_values: &[Vec<crate::Value>],
    ) -> Result<GeneratedMutation, GeneratorError> {
        Err(GeneratorError::Unsupported(format!(
            "generator does not support chunked DELETE (pk_cols={:?}, rows={})",
            pk_cols,
            pk_values.len()
        )))
    }

    /// Replaces parameter placeholders in `query` with dialect-quoted literals,
    /// producing human-readable SQL suitable for a read-only editor tab.
    ///
    /// Default implementation returns the raw parameterized SQL unchanged.
    /// `SqlMutationGenerator` overrides this to call
    /// [`SelectQuery::materialize_for_editor`] with the dialect.
    fn materialize_select_for_editor(&self, query: &SelectQuery) -> String {
        query.sql.clone()
    }
}

// =============================================================================
// SQL Mutation Generator
// =============================================================================

/// `QueryGenerator` for SQL drivers, backed by `SqlQueryBuilder`.
///
/// Each SQL driver creates a static instance with its dialect:
/// ```ignore
/// static GENERATOR: SqlMutationGenerator = SqlMutationGenerator::new(&POSTGRES_DIALECT);
/// ```
pub struct SqlMutationGenerator {
    dialect: &'static dyn SqlDialect,
}

impl SqlMutationGenerator {
    pub const fn new(dialect: &'static dyn SqlDialect) -> Self {
        Self { dialect }
    }
}

impl QueryGenerator for SqlMutationGenerator {
    fn supported_categories(&self) -> &'static [MutationCategory] {
        &[MutationCategory::Sql]
    }

    fn generate_mutation(&self, mutation: &MutationRequest) -> Option<GeneratedQuery> {
        let builder = SqlQueryBuilder::new(self.dialect);

        let text = match mutation {
            MutationRequest::SqlUpdate(patch) => builder.build_update(patch, false)?,
            MutationRequest::SqlUpdateMany(update) => builder.build_update_many(update)?,
            MutationRequest::SqlInsert(insert) => builder.build_insert(insert, false)?,
            MutationRequest::SqlUpsert(upsert) => builder.build_upsert(upsert)?,
            MutationRequest::SqlDelete(delete) => builder.build_delete(delete, false)?,
            MutationRequest::SqlDeleteMany(delete) => builder.build_delete_many(delete)?,
            _ => return None,
        };

        Some(GeneratedQuery {
            language: QueryLanguage::Sql,
            text,
        })
    }

    fn generate_template(&self, request: &MutationTemplateRequest<'_>) -> Option<GeneratedQuery> {
        let operation = match request.operation {
            MutationTemplateOperation::Insert => SqlOperation::Insert,
            MutationTemplateOperation::Update => SqlOperation::Update,
            MutationTemplateOperation::Delete => SqlOperation::Delete,
        };

        let pk_indices: Vec<usize> = request
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| column.is_primary_key.then_some(index))
            .collect();

        let sql = crate::generate_sql(
            self.dialect,
            &SqlGenerationRequest {
                operation,
                schema: request.schema,
                table: request.table,
                columns: request.columns,
                values: SqlValueMode::WithPlaceholders,
                pk_indices: &pk_indices,
                options: request.options.clone(),
            },
        );

        Some(GeneratedQuery {
            language: QueryLanguage::Sql,
            text: sql,
        })
    }

    fn generate_read_template(&self, request: &ReadTemplateRequest<'_>) -> Option<GeneratedQuery> {
        let operation = match request.operation {
            ReadTemplateOperation::SelectAll => None,
            ReadTemplateOperation::SelectWhere => Some(SqlOperation::SelectWhere),
        };

        let sql = if let Some(operation) = operation {
            crate::generate_sql(
                self.dialect,
                &SqlGenerationRequest {
                    operation,
                    schema: request.schema,
                    table: request.table,
                    columns: request.columns,
                    values: SqlValueMode::WithPlaceholders,
                    pk_indices: &[],
                    options: request.options.clone(),
                },
            )
        } else {
            let table_ref = if request.options.fully_qualified {
                self.dialect.qualified_table(request.schema, request.table)
            } else {
                self.dialect.quote_identifier(request.table)
            };

            if request.options.compact {
                format!("SELECT * FROM {};", table_ref)
            } else {
                format!("SELECT *\nFROM {};", table_ref)
            }
        };

        Some(GeneratedQuery {
            language: QueryLanguage::Sql,
            text: sql,
        })
    }

    fn generate_select(
        &self,
        spec: &VisualQuerySpec,
    ) -> Result<Option<SelectQuery>, QueryGenError> {
        let sql = SqlSelectBuilder::new(self.dialect).build(spec)?;
        Ok(Some(sql))
    }

    fn materialize_select_for_editor(&self, query: &SelectQuery) -> String {
        query.materialize_for_editor(self.dialect)
    }

    fn generate_delete_from_spec(
        &self,
        spec: &crate::query::visual_query::VisualMutationSpec,
    ) -> Result<GeneratedMutation, GeneratorError> {
        let mut params: Vec<crate::Value> = Vec::new();
        let mut param_index: usize = 1;

        let table = self
            .dialect
            .qualified_table(spec.from.schema.as_deref(), &spec.from.name);

        let where_clause = SqlSelectBuilder::new(self.dialect)
            .build_where(spec.filter.as_ref(), &mut params, &mut param_index)
            .map_err(|e| GeneratorError::Unsupported(e.to_string()))?;

        let sql = match where_clause {
            None => format!("DELETE FROM {table}"),
            Some(w) => format!("DELETE FROM {table}\n{w}"),
        };

        rewrite_placeholders_if_needed(self.dialect, sql, params, false)
    }

    fn generate_update_from_spec(
        &self,
        spec: &crate::query::visual_query::VisualMutationSpec,
    ) -> Result<GeneratedMutation, GeneratorError> {
        use crate::query::visual_query::{AssignmentValue, MutationKind};

        let assignments = match &spec.kind {
            MutationKind::Update { assignments } => {
                if assignments.is_empty() {
                    return Err(GeneratorError::EmptyAssignments);
                }
                assignments
            }
            MutationKind::Delete => {
                return Err(GeneratorError::Unsupported(
                    "DELETE passed to generate_update_from_spec".to_string(),
                ));
            }
        };

        let mut params: Vec<crate::Value> = Vec::new();
        let mut param_index: usize = 1;
        let mut used_raw_expression = false;

        let table = self
            .dialect
            .qualified_table(spec.from.schema.as_deref(), &spec.from.name);

        let set_clauses: Vec<String> = assignments
            .iter()
            .map(|a| {
                let col = self.dialect.quote_identifier(&a.column);
                match &a.value {
                    AssignmentValue::Literal(lit) => {
                        let ph = self.next_placeholder(&mut param_index);
                        let val = scalar_to_value(lit);
                        params.push(val);
                        format!("{col} = {ph}")
                    }
                    AssignmentValue::Expression(expr) => {
                        used_raw_expression = true;
                        format!("{col} = {expr}")
                    }
                    AssignmentValue::Null => format!("{col} = NULL"),
                    AssignmentValue::Default => format!("{col} = DEFAULT"),
                }
            })
            .collect();

        let where_clause = SqlSelectBuilder::new(self.dialect)
            .build_where(spec.filter.as_ref(), &mut params, &mut param_index)
            .map_err(|e| GeneratorError::Unsupported(e.to_string()))?;

        let set_str = set_clauses.join(", ");
        let sql = match where_clause {
            None => format!("UPDATE {table}\nSET {set_str}"),
            Some(w) => format!("UPDATE {table}\nSET {set_str}\n{w}"),
        };

        rewrite_placeholders_if_needed(self.dialect, sql, params, used_raw_expression)
    }

    fn generate_delete_chunk_from_spec(
        &self,
        spec: &crate::query::visual_query::VisualMutationSpec,
        pk_cols: &[&str],
        pk_values: &[Vec<crate::Value>],
    ) -> Result<GeneratedMutation, GeneratorError> {
        let mut params: Vec<crate::Value> = Vec::new();
        let mut param_index: usize = 1;

        let table = self
            .dialect
            .qualified_table(spec.from.schema.as_deref(), &spec.from.name);

        let filter_expr = match spec.filter.as_ref() {
            None => None,
            Some(node) => {
                let builder = SqlSelectBuilder::new(self.dialect);
                let expr = builder
                    .render_filter_node(node, &mut params, &mut param_index)
                    .map_err(|e| GeneratorError::Unsupported(e.to_string()))?;
                if expr.is_empty() { None } else { Some(expr) }
            }
        };

        let pk_in = build_pk_in_clause(
            self.dialect,
            pk_cols,
            pk_values,
            &mut params,
            &mut param_index,
        );

        let where_parts: Vec<String> = [filter_expr, Some(pk_in)]
            .into_iter()
            .flatten()
            .filter(|s| !s.is_empty())
            .collect();

        let sql = if where_parts.is_empty() {
            format!("DELETE FROM {table}")
        } else if where_parts.len() == 1 {
            format!("DELETE FROM {table}\nWHERE {}", where_parts[0])
        } else {
            format!(
                "DELETE FROM {table}\nWHERE ({}) AND ({})",
                where_parts[0], where_parts[1]
            )
        };

        rewrite_placeholders_if_needed(self.dialect, sql, params, false)
    }

    fn generate_update_chunk_from_spec(
        &self,
        spec: &crate::query::visual_query::VisualMutationSpec,
        pk_cols: &[&str],
        pk_values: &[Vec<crate::Value>],
    ) -> Result<GeneratedMutation, GeneratorError> {
        use crate::query::visual_query::{AssignmentValue, MutationKind};

        let assignments = match &spec.kind {
            MutationKind::Update { assignments } => {
                if assignments.is_empty() {
                    return Err(GeneratorError::EmptyAssignments);
                }
                assignments
            }
            MutationKind::Delete => {
                return Err(GeneratorError::Unsupported(
                    "DELETE passed to generate_update_chunk_from_spec".to_string(),
                ));
            }
        };

        let mut params: Vec<crate::Value> = Vec::new();
        let mut param_index: usize = 1;
        let mut used_raw_expression = false;

        let table = self
            .dialect
            .qualified_table(spec.from.schema.as_deref(), &spec.from.name);

        let set_clauses: Vec<String> = assignments
            .iter()
            .map(|a| {
                let col = self.dialect.quote_identifier(&a.column);
                match &a.value {
                    AssignmentValue::Literal(lit) => {
                        let ph = self.next_placeholder(&mut param_index);
                        let val = scalar_to_value(lit);
                        params.push(val);
                        format!("{col} = {ph}")
                    }
                    AssignmentValue::Expression(expr) => {
                        used_raw_expression = true;
                        format!("{col} = {expr}")
                    }
                    AssignmentValue::Null => format!("{col} = NULL"),
                    AssignmentValue::Default => format!("{col} = DEFAULT"),
                }
            })
            .collect();

        let filter_expr = match spec.filter.as_ref() {
            None => None,
            Some(node) => {
                let builder = SqlSelectBuilder::new(self.dialect);
                let expr = builder
                    .render_filter_node(node, &mut params, &mut param_index)
                    .map_err(|e| GeneratorError::Unsupported(e.to_string()))?;
                if expr.is_empty() { None } else { Some(expr) }
            }
        };

        let pk_in = build_pk_in_clause(
            self.dialect,
            pk_cols,
            pk_values,
            &mut params,
            &mut param_index,
        );

        let where_parts: Vec<String> = [filter_expr, Some(pk_in)]
            .into_iter()
            .flatten()
            .filter(|s| !s.is_empty())
            .collect();

        let set_str = set_clauses.join(", ");
        let sql = if where_parts.is_empty() {
            format!("UPDATE {table}\nSET {set_str}")
        } else if where_parts.len() == 1 {
            format!("UPDATE {table}\nSET {set_str}\nWHERE {}", where_parts[0])
        } else {
            format!(
                "UPDATE {table}\nSET {set_str}\nWHERE ({}) AND ({})",
                where_parts[0], where_parts[1]
            )
        };

        rewrite_placeholders_if_needed(self.dialect, sql, params, used_raw_expression)
    }
}

impl SqlMutationGenerator {
    fn next_placeholder(&self, param_index: &mut usize) -> String {
        match self.dialect.placeholder_style() {
            PlaceholderStyle::DollarNumber => {
                let p = format!("${}", *param_index);
                *param_index += 1;
                p
            }
            PlaceholderStyle::AtSign => {
                let p = format!("@p{}", *param_index);
                *param_index += 1;
                p
            }
            _ => {
                *param_index += 1;
                "?".to_string()
            }
        }
    }
}

/// Build the PK keyset predicate for a chunk WHERE clause.
///
/// For a single-column PK, emits `pk_col IN (?, ?, ?)` on all dialects.
///
/// For a multi-column PK, emits one of two forms depending on dialect support:
/// - Dialects that support row-value constructors (`supports_row_constructor_in = true`):
///   `(pk0, pk1) IN ((?,?), (?,?))`
/// - T-SQL (SQL Server), which does NOT support row-value constructors:
///   `((pk0 = ? AND pk1 = ?) OR (pk0 = ? AND pk1 = ?))`
fn build_pk_in_clause(
    dialect: &dyn SqlDialect,
    pk_cols: &[&str],
    pk_values: &[Vec<crate::Value>],
    params: &mut Vec<crate::Value>,
    param_index: &mut usize,
) -> String {
    let make_ph = |idx: usize| match dialect.placeholder_style() {
        PlaceholderStyle::DollarNumber => format!("${idx}"),
        PlaceholderStyle::AtSign => format!("@p{idx}"),
        _ => "?".to_string(),
    };

    if pk_cols.len() == 1 {
        let col = dialect.quote_identifier(pk_cols[0]);
        let placeholders: Vec<String> = pk_values
            .iter()
            .map(|row| {
                let ph = make_ph(*param_index);
                *param_index += 1;
                params.push(row.first().cloned().unwrap_or(crate::Value::Null));
                ph
            })
            .collect();
        format!("{col} IN ({})", placeholders.join(", "))
    } else if dialect.supports_row_constructor_in() {
        let cols: Vec<String> = pk_cols
            .iter()
            .map(|c| dialect.quote_identifier(c))
            .collect();
        let col_list = cols.join(", ");
        let row_constructors: Vec<String> = pk_values
            .iter()
            .map(|row| {
                let phs: Vec<String> = (0..pk_cols.len())
                    .map(|ci| {
                        let ph = make_ph(*param_index);
                        *param_index += 1;
                        params.push(row.get(ci).cloned().unwrap_or(crate::Value::Null));
                        ph
                    })
                    .collect();
                format!("({})", phs.join(", "))
            })
            .collect();
        format!("({col_list}) IN ({})", row_constructors.join(", "))
    } else {
        // OR-of-ANDs for dialects that do not support row-value constructors (e.g. T-SQL).
        let and_terms: Vec<String> = pk_values
            .iter()
            .map(|row| {
                let eq_parts: Vec<String> = pk_cols
                    .iter()
                    .enumerate()
                    .map(|(ci, col)| {
                        let quoted = dialect.quote_identifier(col);
                        let ph = make_ph(*param_index);
                        *param_index += 1;
                        params.push(row.get(ci).cloned().unwrap_or(crate::Value::Null));
                        format!("{quoted} = {ph}")
                    })
                    .collect();
                format!("({})", eq_parts.join(" AND "))
            })
            .collect();
        format!("({})", and_terms.join(" OR "))
    }
}

/// Rewrite `$N` placeholders for QuestionMark/AtSign dialects.
///
/// The filter builder emits `$N` style internally; this converts those to `?` or `@pN`
/// for dialects that require it. For DollarNumber dialects, returns unchanged.
fn rewrite_placeholders_if_needed(
    dialect: &dyn SqlDialect,
    sql: String,
    params: Vec<crate::Value>,
    used_raw_expression: bool,
) -> Result<GeneratedMutation, GeneratorError> {
    let sql = match dialect.placeholder_style() {
        PlaceholderStyle::QuestionMark | PlaceholderStyle::NamedColon => {
            let mut out = sql;
            let mut i = 1usize;
            loop {
                let ph = format!("${i}");
                if out.contains(&ph) {
                    out = out.replacen(&ph, "?", 1);
                    i += 1;
                } else {
                    break;
                }
            }
            out
        }
        PlaceholderStyle::AtSign => {
            let mut out = sql;
            let mut i = 1usize;
            loop {
                let ph = format!("${i}");
                if out.contains(&ph) {
                    out = out.replacen(&ph, &format!("@p{i}"), 1);
                    i += 1;
                } else {
                    break;
                }
            }
            out
        }
        PlaceholderStyle::DollarNumber => sql,
    };

    Ok(GeneratedMutation {
        sql,
        params,
        used_raw_expression,
    })
}

fn scalar_to_value(lit: &crate::query::visual_query::ScalarLiteral) -> crate::Value {
    use crate::query::visual_query::ScalarLiteral;
    match lit {
        ScalarLiteral::Text(s) => crate::Value::Text(s.clone()),
        ScalarLiteral::Integer(n) => crate::Value::Int(*n),
        ScalarLiteral::Float(f) => crate::Value::Float(*f),
        ScalarLiteral::Bool(b) => crate::Value::Bool(*b),
        ScalarLiteral::Timestamp(s) => crate::Value::Text(s.clone()),
        ScalarLiteral::Null => crate::Value::Null,
    }
}

// =============================================================================
// SQL SELECT builder for VisualQuerySpec
// =============================================================================

/// Render a `FilterNode` to a parameterized SQL predicate using `dialect`.
///
/// Returns `None` when `filter` is `None` or produces an empty expression.
/// Appends bound values to `params` and advances `param_index` for each placeholder.
///
/// Used by the chunked executor to include the user filter in the PK SELECT query
/// without duplicating the filter-rendering logic from `SqlSelectBuilder`.
pub fn render_filter_node_sql(
    filter: Option<&crate::query::visual_query::FilterNode>,
    dialect: &dyn SqlDialect,
    params: &mut Vec<crate::Value>,
    param_index: &mut usize,
) -> Option<String> {
    SqlSelectBuilder::new(dialect)
        .build_where(filter, params, param_index)
        .unwrap_or(None)
}

/// Build a parameterized SELECT query from `spec` using `dialect`.
///
/// Exposed as `pub(crate)` so the relational-filter count wrapper can call it
/// without going through `SqlMutationGenerator` (which requires a `&'static`
/// dialect reference).
pub(crate) fn build_select_query(
    spec: &VisualQuerySpec,
    dialect: &dyn SqlDialect,
) -> Result<SelectQuery, QueryGenError> {
    SqlSelectBuilder::new(dialect).build(spec)
}

/// Builds the grouped total-count subquery for external callers (e.g. lib.rs public API).
pub(crate) fn build_grouped_count_query(
    spec: &VisualQuerySpec,
    dialect: &dyn SqlDialect,
) -> Result<SelectQuery, QueryGenError> {
    SqlSelectBuilder::new(dialect).build_count_of_grouped(spec)
}

/// Re-exported for the relational_filter crate module so it can access the
/// builder without re-implementing projection/join/filter rendering.
pub(crate) struct SqlSelectBuilder<'a> {
    dialect: &'a dyn SqlDialect,
}

/// Walks a `JoinFilterNode` tree and returns `true` if at least one leaf
/// `JoinPredicate` has non-empty `left` and `right` sides. Used by the
/// generator to skip joins whose ON clause would render empty.
fn join_node_has_complete_predicate(node: &crate::query::visual_query::JoinFilterNode) -> bool {
    use crate::query::visual_query::JoinFilterNode;
    match node {
        JoinFilterNode::Predicate(p) => !p.left.trim().is_empty() && !p.right.trim().is_empty(),
        JoinFilterNode::Group { children, .. } => {
            children.iter().any(join_node_has_complete_predicate)
        }
    }
}

impl<'a> SqlSelectBuilder<'a> {
    fn new(dialect: &'a dyn SqlDialect) -> Self {
        Self { dialect }
    }

    fn build(&self, spec: &VisualQuerySpec) -> Result<SelectQuery, QueryGenError> {
        if spec.is_grouped() {
            self.build_grouped(spec)
        } else {
            self.build_ungrouped(spec)
        }
    }

    fn build_ungrouped(&self, spec: &VisualQuerySpec) -> Result<SelectQuery, QueryGenError> {
        let mut params: Vec<crate::Value> = Vec::new();
        let mut param_index: usize = 1;

        let projection = self.build_projection(&spec.projection);
        let from_clause = self.build_from(&spec.source);
        let joins = self.build_joins(&spec.joins);
        let where_clause = self.build_where(spec.filter.as_ref(), &mut params, &mut param_index)?;
        let order_by = self.build_order_by(&spec.sort);
        let limit_offset = self.build_limit_offset(spec.limit, spec.offset);

        let mut parts: Vec<String> = Vec::new();
        parts.push(format!("SELECT {}", projection));
        parts.push(format!("FROM {}", from_clause));

        for join in joins {
            parts.push(join);
        }

        if let Some(w) = where_clause {
            parts.push(w);
        }

        if let Some(o) = order_by {
            parts.push(o);
        }

        if let Some(lo) = limit_offset {
            parts.push(lo);
        }

        Ok(SelectQuery {
            sql: parts.join("\n"),
            params,
        })
    }

    fn build_grouped(&self, spec: &VisualQuerySpec) -> Result<SelectQuery, QueryGenError> {
        let mut params: Vec<crate::Value> = Vec::new();
        let mut param_index: usize = 1;

        let projection = self.build_projection_grouped(&spec.group_by, &spec.aggregates)?;
        let from_clause = self.build_from(&spec.source);
        let joins = self.build_joins(&spec.joins);
        let where_clause = self.build_where(spec.filter.as_ref(), &mut params, &mut param_index)?;
        let group_by_clause = self.build_group_by(&spec.group_by);
        let having_clause = self.build_having(
            spec.having.as_ref(),
            &spec.aggregates,
            &mut params,
            &mut param_index,
        )?;
        let order_by = self.build_order_by_grouped(&spec.sort, &spec.group_by, &spec.aggregates);
        let limit_offset = self.build_limit_offset(spec.limit, spec.offset);

        let mut parts: Vec<String> = Vec::new();
        parts.push(format!("SELECT {}", projection));
        parts.push(format!("FROM {}", from_clause));

        for join in joins {
            parts.push(join);
        }

        if let Some(w) = where_clause {
            parts.push(w);
        }

        if let Some(g) = group_by_clause {
            parts.push(g);
        }

        if let Some(h) = having_clause {
            parts.push(h);
        }

        if let Some(o) = order_by {
            parts.push(o);
        }

        if let Some(lo) = limit_offset {
            parts.push(lo);
        }

        Ok(SelectQuery {
            sql: parts.join("\n"),
            params,
        })
    }

    /// Builds a `SELECT COUNT(*) FROM (<grouped query without LIMIT/OFFSET>) AS _dbflux_count_subq`.
    ///
    /// The inner query is built by cloning the spec and zeroing the pagination fields.
    /// This is the correct way to count the number of result rows for a grouped query.
    pub(crate) fn build_count_of_grouped(
        &self,
        spec: &VisualQuerySpec,
    ) -> Result<SelectQuery, QueryGenError> {
        let mut inner_spec = spec.clone();
        inner_spec.limit = None;
        inner_spec.offset = 0;

        let inner = self.build(&inner_spec)?;

        let subq_alias = self.dialect.quote_identifier("_dbflux_count_subq");
        Ok(SelectQuery {
            sql: format!("SELECT COUNT(*) FROM ({}) AS {}", inner.sql, subq_alias),
            params: inner.params,
        })
    }

    fn build_projection_grouped(
        &self,
        group_by: &[crate::query::visual_query::GroupByEntry],
        aggregates: &[crate::query::visual_query::AggregateSpec],
    ) -> Result<String, QueryGenError> {
        use crate::query::visual_query::AggFn;

        let mut parts: Vec<String> = Vec::new();

        for entry in group_by {
            parts.push(format!(
                "{}.{}",
                self.dialect.quote_identifier(&entry.source_alias),
                self.dialect.quote_identifier(&entry.column)
            ));
        }

        for agg in aggregates {
            let alias = self.dialect.quote_identifier(&agg.alias);
            let expr = match agg.function {
                AggFn::CountStar => format!("COUNT(*) AS {}", alias),
                AggFn::CountDistinct => {
                    let source = agg.source_alias.as_deref().ok_or_else(|| {
                        QueryGenError::InvalidSpec(
                            "CountDistinct requires source_alias".to_string(),
                        )
                    })?;
                    let col = agg.column.as_deref().ok_or_else(|| {
                        QueryGenError::InvalidSpec("CountDistinct requires column".to_string())
                    })?;
                    format!(
                        "COUNT(DISTINCT {}.{}) AS {}",
                        self.dialect.quote_identifier(source),
                        self.dialect.quote_identifier(col),
                        alias
                    )
                }
                fn_name => {
                    let source = agg.source_alias.as_deref().ok_or_else(|| {
                        QueryGenError::InvalidSpec(format!("{:?} requires source_alias", fn_name))
                    })?;
                    let col = agg.column.as_deref().ok_or_else(|| {
                        QueryGenError::InvalidSpec(format!("{:?} requires column", fn_name))
                    })?;
                    let sql_fn = match fn_name {
                        AggFn::Count => "COUNT",
                        AggFn::Sum => "SUM",
                        AggFn::Avg => "AVG",
                        AggFn::Min => "MIN",
                        AggFn::Max => "MAX",
                        _ => unreachable!("handled above"),
                    };
                    format!(
                        "{}({}.{}) AS {}",
                        sql_fn,
                        self.dialect.quote_identifier(source),
                        self.dialect.quote_identifier(col),
                        alias
                    )
                }
            };
            parts.push(expr);
        }

        if parts.is_empty() {
            return Err(QueryGenError::InvalidSpec(
                "grouped query must have at least one group-by column or aggregate".to_string(),
            ));
        }

        Ok(parts.join(", "))
    }

    fn build_group_by(
        &self,
        group_by: &[crate::query::visual_query::GroupByEntry],
    ) -> Option<String> {
        if group_by.is_empty() {
            return None;
        }

        let cols: Vec<String> = group_by
            .iter()
            .map(|g| {
                format!(
                    "{}.{}",
                    self.dialect.quote_identifier(&g.source_alias),
                    self.dialect.quote_identifier(&g.column)
                )
            })
            .collect();

        Some(format!("GROUP BY {}", cols.join(", ")))
    }

    fn build_order_by_grouped(
        &self,
        sort: &[crate::query::visual_query::SortEntry],
        group_by: &[crate::query::visual_query::GroupByEntry],
        aggregates: &[crate::query::visual_query::AggregateSpec],
    ) -> Option<String> {
        use crate::query::visual_query::SortDirection;
        use std::collections::HashSet;

        let valid_group_by_pairs: HashSet<(&str, &str)> = group_by
            .iter()
            .map(|g| (g.source_alias.as_str(), g.column.as_str()))
            .collect();
        let valid_agg_aliases: HashSet<&str> =
            aggregates.iter().map(|a| a.alias.as_str()).collect();

        let entries: Vec<String> = sort
            .iter()
            .filter(|s| {
                valid_group_by_pairs.contains(&(s.source_alias.as_str(), s.column.as_str()))
                    || valid_agg_aliases.contains(s.column.as_str())
            })
            .map(|s| {
                let col = if valid_agg_aliases.contains(s.column.as_str()) {
                    self.dialect.quote_identifier(&s.column)
                } else {
                    format!(
                        "{}.{}",
                        self.dialect.quote_identifier(&s.source_alias),
                        self.dialect.quote_identifier(&s.column),
                    )
                };
                let dir = match s.direction {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                };
                format!("{} {}", col, dir)
            })
            .collect();

        if entries.is_empty() {
            None
        } else {
            Some(format!("ORDER BY {}", entries.join(", ")))
        }
    }

    fn build_projection(&self, projection: &crate::query::visual_query::Projection) -> String {
        use crate::query::visual_query::Projection;

        match projection {
            Projection::All => "*".to_string(),
            Projection::Explicit(cols) => cols
                .iter()
                .map(|c| {
                    let col_expr = format!(
                        "{}.{}",
                        self.dialect.quote_identifier(&c.source_alias),
                        self.dialect.quote_identifier(&c.column)
                    );
                    match &c.alias {
                        Some(a) => format!("{} AS {}", col_expr, self.dialect.quote_identifier(a)),
                        None => col_expr,
                    }
                })
                .collect::<Vec<_>>()
                .join(", "),
        }
    }

    fn build_from(&self, source: &crate::query::visual_query::SourceTable) -> String {
        let table_ref = self
            .dialect
            .qualified_table(source.schema.as_deref(), &source.table);
        let alias = self.dialect.quote_identifier(&source.alias);

        if source.alias == source.table && source.schema.is_none() {
            table_ref
        } else {
            format!("{} AS {}", table_ref, alias)
        }
    }

    fn build_joins(&self, joins: &[crate::query::visual_query::JoinStep]) -> Vec<String> {
        use crate::query::visual_query::{JoinKind, JoinOn};

        joins
            .iter()
            .filter(|j| {
                // Skip joins that are still being authored in the UI: a row
                // with empty `to_table` / `to_alias` / `from_alias` cannot
                // produce valid SQL and would trip identifier-quoting asserts
                // in dialect drivers (e.g. PostgreSQL).
                let header_ok = !j.to_table.trim().is_empty()
                    && !j.to_alias.trim().is_empty()
                    && !j.from_alias.trim().is_empty();
                if !header_ok {
                    return false;
                }
                // For structured Conditions, require at least one fully
                // populated predicate so we never emit `ON ` with nothing.
                match &j.on {
                    JoinOn::Conditions(root) => join_node_has_complete_predicate(root),
                    JoinOn::RawExpression(expr) => !expr.trim().is_empty(),
                    JoinOn::FkPath { .. } => true,
                }
            })
            .map(|j| {
                let kind_sql = match j.kind {
                    JoinKind::Inner => "INNER JOIN",
                    JoinKind::Left => "LEFT JOIN",
                    JoinKind::Right => "RIGHT JOIN",
                    JoinKind::Full => "FULL OUTER JOIN",
                };

                let table_ref = self
                    .dialect
                    .qualified_table(j.to_schema.as_deref(), &j.to_table);
                let alias = self.dialect.quote_identifier(&j.to_alias);

                let on_expr = match &j.on {
                    JoinOn::FkPath {
                        from_column,
                        to_column,
                    } => format!(
                        "{}.{} = {}.{}",
                        self.dialect.quote_identifier(&j.from_alias),
                        self.dialect.quote_identifier(from_column),
                        self.dialect.quote_identifier(&j.to_alias),
                        self.dialect.quote_identifier(to_column),
                    ),
                    JoinOn::RawExpression(expr) => expr.clone(),
                    JoinOn::Conditions(root) => self.render_join_filter_node(root, true),
                };

                format!("{} {} AS {} ON {}", kind_sql, table_ref, alias, on_expr)
            })
            .collect()
    }

    /// Renders structured ON conditions as `lhs <op> rhs AND ...`.
    ///
    /// Partial predicates (missing left or right reference) are skipped so
    /// the user can author a join incrementally without breaking the SQL
    /// preview. Both sides are treated as raw SQL expressions: the caller
    /// is responsible for typing dotted identifiers like `users.id`. Quoting
    /// of identifiers is intentionally skipped because each side may contain
    /// function calls, casts, or constants, and the dialect-quoting helper
    /// only handles bare identifiers.
    /// Recursively renders a `JoinFilterNode` tree as SQL.
    ///
    /// Groups emit their children joined by AND / OR. Non-root groups are
    /// always parenthesised so precedence stays explicit; the root only
    /// gets parens when it would otherwise leak its operator into the
    /// surrounding clause. Incomplete predicates (missing left or right)
    /// are skipped so partially-edited rows do not break the preview.
    fn render_join_filter_node(
        &self,
        node: &crate::query::visual_query::JoinFilterNode,
        is_root: bool,
    ) -> String {
        use crate::query::visual_query::{BoolOp, Comparator, JoinFilterNode};

        match node {
            JoinFilterNode::Predicate(p) => {
                if p.left.trim().is_empty() || p.right.trim().is_empty() {
                    return String::new();
                }
                let cmp = match p.op {
                    Comparator::Eq => "=",
                    Comparator::Neq => "<>",
                    Comparator::Gt => ">",
                    Comparator::Lt => "<",
                    Comparator::Gte => ">=",
                    Comparator::Lte => "<=",
                    Comparator::Like => "LIKE",
                    Comparator::ILike => "ILIKE",
                    Comparator::In => "IN",
                    Comparator::IsNull => "IS NULL",
                    Comparator::IsNotNull => "IS NOT NULL",
                };
                format!("{} {} {}", p.left.trim(), cmp, p.right.trim())
            }
            JoinFilterNode::Group { op, children, .. } => {
                let parts: Vec<String> = children
                    .iter()
                    .map(|c| self.render_join_filter_node(c, false))
                    .filter(|s| !s.is_empty())
                    .collect();

                if parts.is_empty() {
                    return String::new();
                }
                if parts.len() == 1 {
                    return parts.into_iter().next().unwrap();
                }

                let sep = match op {
                    BoolOp::And => " AND ",
                    BoolOp::Or => " OR ",
                };
                let joined = parts.join(sep);

                if is_root && matches!(op, BoolOp::And) {
                    // Top-level AND can live unparenthesised since the
                    // surrounding ON expects the same conjunction.
                    joined
                } else {
                    format!("({joined})")
                }
            }
        }
    }

    fn build_where(
        &self,
        filter: Option<&crate::query::visual_query::FilterNode>,
        params: &mut Vec<crate::Value>,
        param_index: &mut usize,
    ) -> Result<Option<String>, QueryGenError> {
        self.render_predicate_clause("WHERE", filter, params, param_index)
    }

    fn build_having(
        &self,
        having: Option<&crate::query::visual_query::FilterNode>,
        aggregates: &[crate::query::visual_query::AggregateSpec],
        params: &mut Vec<crate::Value>,
        param_index: &mut usize,
    ) -> Result<Option<String>, QueryGenError> {
        if self.dialect.having_repeats_aggregate_expressions() && !aggregates.is_empty() {
            let expanded = having.map(|node| self.expand_having_aliases(node, aggregates));
            self.render_predicate_clause("HAVING", expanded.as_ref(), params, param_index)
        } else {
            self.render_predicate_clause("HAVING", having, params, param_index)
        }
    }

    /// Rewrites a HAVING `FilterNode` tree so that any predicate whose
    /// `source_alias` is empty and whose `column` matches an aggregate alias
    /// is replaced with a predicate using the full aggregate expression as its
    /// column text. Used for dialects like SQL Server that do not permit
    /// referencing SELECT-list aliases in HAVING.
    fn expand_having_aliases(
        &self,
        node: &crate::query::visual_query::FilterNode,
        aggregates: &[crate::query::visual_query::AggregateSpec],
    ) -> crate::query::visual_query::FilterNode {
        use crate::query::visual_query::FilterNode;

        match node {
            FilterNode::Predicate(pred) => {
                if pred.source_alias.trim().is_empty()
                    && let Some(agg) = aggregates.iter().find(|a| a.alias == pred.column)
                {
                    let expr = self.build_aggregate_expression(agg);
                    let mut expanded = pred.clone();
                    expanded.source_alias = "\x00raw".to_string();
                    expanded.column = expr;
                    return FilterNode::Predicate(expanded);
                }
                FilterNode::Predicate(pred.clone())
            }
            FilterNode::Group { op, children } => FilterNode::Group {
                op: *op,
                children: children
                    .iter()
                    .map(|c| self.expand_having_aliases(c, aggregates))
                    .collect(),
            },
        }
    }

    /// Builds the SQL expression for a single aggregate (without the alias).
    fn build_aggregate_expression(
        &self,
        agg: &crate::query::visual_query::AggregateSpec,
    ) -> String {
        use crate::query::visual_query::AggFn;

        match agg.function {
            AggFn::CountStar => "COUNT(*)".to_string(),
            AggFn::CountDistinct => {
                if let (Some(source), Some(col)) = (&agg.source_alias, &agg.column) {
                    format!(
                        "COUNT(DISTINCT {}.{})",
                        self.dialect.quote_identifier(source),
                        self.dialect.quote_identifier(col),
                    )
                } else {
                    "COUNT(DISTINCT NULL)".to_string()
                }
            }
            fn_name => {
                let sql_fn = match fn_name {
                    AggFn::Count => "COUNT",
                    AggFn::Sum => "SUM",
                    AggFn::Avg => "AVG",
                    AggFn::Min => "MIN",
                    AggFn::Max => "MAX",
                    _ => unreachable!("CountStar and CountDistinct handled above"),
                };
                if let (Some(source), Some(col)) = (&agg.source_alias, &agg.column) {
                    format!(
                        "{}({}.{})",
                        sql_fn,
                        self.dialect.quote_identifier(source),
                        self.dialect.quote_identifier(col),
                    )
                } else {
                    format!("{}(NULL)", sql_fn)
                }
            }
        }
    }

    fn render_predicate_clause(
        &self,
        keyword: &str,
        filter: Option<&crate::query::visual_query::FilterNode>,
        params: &mut Vec<crate::Value>,
        param_index: &mut usize,
    ) -> Result<Option<String>, QueryGenError> {
        match filter {
            None => Ok(None),
            Some(node) => {
                let expr = self.render_filter_node(node, params, param_index)?;
                if expr.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(format!("{} {}", keyword, expr)))
                }
            }
        }
    }

    fn render_filter_node(
        &self,
        node: &crate::query::visual_query::FilterNode,
        params: &mut Vec<crate::Value>,
        param_index: &mut usize,
    ) -> Result<String, QueryGenError> {
        use crate::query::visual_query::{BoolOp, FilterNode};

        match node {
            FilterNode::Predicate(pred) => {
                // Skip predicates the user is still authoring: an empty column
                // reference would trip identifier-quoting asserts in dialect
                // drivers (PostgreSQL's pg_quote_ident is debug-asserted).
                if pred.column.trim().is_empty() {
                    return Ok(String::new());
                }
                self.render_predicate(pred, params, param_index)
            }
            FilterNode::Group { op, children } => {
                if children.is_empty() {
                    return Ok(String::new());
                }

                let op_str = match op {
                    BoolOp::And => " AND ",
                    BoolOp::Or => " OR ",
                };

                let mut parts = Vec::new();
                for child in children {
                    let expr = self.render_filter_node(child, params, param_index)?;
                    if !expr.is_empty() {
                        parts.push(expr);
                    }
                }

                if parts.is_empty() {
                    return Ok(String::new());
                }

                if parts.len() == 1 {
                    Ok(parts.remove(0))
                } else {
                    Ok(format!("({})", parts.join(op_str)))
                }
            }
        }
    }

    fn render_predicate(
        &self,
        pred: &crate::query::visual_query::Predicate,
        params: &mut Vec<crate::Value>,
        param_index: &mut usize,
    ) -> Result<String, QueryGenError> {
        use crate::query::visual_query::{Comparator, PredicateValue};

        let col = if pred.source_alias == "\x00raw" {
            pred.column.clone()
        } else if pred.source_alias.trim().is_empty() {
            self.dialect.quote_identifier(&pred.column)
        } else {
            format!(
                "{}.{}",
                self.dialect.quote_identifier(&pred.source_alias),
                self.dialect.quote_identifier(&pred.column),
            )
        };

        match pred.comparator {
            Comparator::IsNull => Ok(format!("{} IS NULL", col)),
            Comparator::IsNotNull => Ok(format!("{} IS NOT NULL", col)),
            Comparator::In => {
                let values = match &pred.value {
                    PredicateValue::List(list) => list,
                    _ => {
                        return Err(QueryGenError::InvalidSpec(format!(
                            "IN comparator requires PredicateValue::List for column {}",
                            pred.column
                        )));
                    }
                };

                let placeholders: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let ph = self.placeholder(*param_index);
                        params.push(literal_to_value(v));
                        *param_index += 1;
                        ph
                    })
                    .collect();

                Ok(format!("{} IN ({})", col, placeholders.join(", ")))
            }
            cmp => {
                let op = match cmp {
                    Comparator::Eq => "=",
                    Comparator::Neq => "<>",
                    Comparator::Gt => ">",
                    Comparator::Lt => "<",
                    Comparator::Gte => ">=",
                    Comparator::Lte => "<=",
                    Comparator::Like => "LIKE",
                    Comparator::ILike => "ILIKE",
                    _ => unreachable!("handled above"),
                };

                let value = match &pred.value {
                    PredicateValue::Single(v) => v,
                    _ => {
                        return Err(QueryGenError::InvalidSpec(format!(
                            "comparator {} requires a single value for column {}",
                            op, pred.column
                        )));
                    }
                };

                let ph = self.placeholder(*param_index);
                params.push(literal_to_value(value));
                *param_index += 1;

                Ok(format!("{} {} {}", col, op, ph))
            }
        }
    }

    fn placeholder(&self, index: usize) -> String {
        match self.dialect.placeholder_style() {
            PlaceholderStyle::DollarNumber => format!("${}", index),
            PlaceholderStyle::AtSign => format!("@p{}", index),
            _ => "?".to_string(),
        }
    }

    fn build_order_by(&self, sort: &[crate::query::visual_query::SortEntry]) -> Option<String> {
        use crate::query::visual_query::SortDirection;

        if sort.is_empty() {
            return None;
        }

        let entries: Vec<String> = sort
            .iter()
            .map(|s| {
                let col = format!(
                    "{}.{}",
                    self.dialect.quote_identifier(&s.source_alias),
                    self.dialect.quote_identifier(&s.column),
                );
                let dir = match s.direction {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                };
                format!("{} {}", col, dir)
            })
            .collect();

        Some(format!("ORDER BY {}", entries.join(", ")))
    }

    fn build_limit_offset(&self, limit: Option<u64>, offset: u64) -> Option<String> {
        let effective_limit = limit.filter(|&n| n > 0);

        match (effective_limit, offset) {
            (None, 0) => None,
            (Some(n), 0) => Some(format!("LIMIT {}", n)),
            (None, o) => Some(format!("OFFSET {}", o)),
            (Some(n), o) => Some(format!("LIMIT {}\nOFFSET {}", n, o)),
        }
    }
}

fn literal_to_value(lit: &crate::query::visual_query::LiteralValue) -> crate::Value {
    use crate::query::visual_query::LiteralValue;

    match lit {
        LiteralValue::Text(s) => crate::Value::Text(s.clone()),
        LiteralValue::Integer(i) => crate::Value::Int(*i),
        LiteralValue::Float(f) => crate::Value::Float(*f),
        LiteralValue::Bool(b) => crate::Value::Bool(*b),
        LiteralValue::Timestamp(s) => crate::Value::Text(s.clone()),
        LiteralValue::Null => crate::Value::Null,
    }
}

/// Test-only minimal implementation of `QueryGenerator` that:
/// - Rejects empty UPDATE assignments with `GeneratorError::EmptyAssignments`
/// - Accepts all other UPDATE specs (returns a placeholder SQL string)
/// - Accepts all DELETE specs
#[cfg(test)]
pub(crate) struct MockMutationGenerator;

#[cfg(test)]
impl QueryGenerator for MockMutationGenerator {
    fn supported_categories(&self) -> &'static [MutationCategory] {
        &[]
    }

    fn generate_mutation(&self, _: &MutationRequest) -> Option<GeneratedQuery> {
        None
    }

    fn generate_update_from_spec(
        &self,
        spec: &crate::query::visual_query::VisualMutationSpec,
    ) -> Result<GeneratedMutation, GeneratorError> {
        use crate::query::visual_query::MutationKind;

        match &spec.kind {
            MutationKind::Update { assignments } => {
                if assignments.is_empty() {
                    return Err(GeneratorError::EmptyAssignments);
                }
                Ok(GeneratedMutation {
                    sql: format!("UPDATE {} SET ...", spec.from.name),
                    params: vec![],
                    used_raw_expression: false,
                })
            }
            MutationKind::Delete => Err(GeneratorError::Unsupported(
                "DELETE passed to generate_update_from_spec".to_string(),
            )),
        }
    }

    fn generate_delete_from_spec(
        &self,
        spec: &crate::query::visual_query::VisualMutationSpec,
    ) -> Result<GeneratedMutation, GeneratorError> {
        Ok(GeneratedMutation {
            sql: format!("DELETE FROM {}", spec.from.name),
            params: vec![],
            used_raw_expression: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GeneratedQuery, MutationCategory, MutationTemplateOperation, MutationTemplateRequest,
        QueryGenerator, ReadTemplateOperation, ReadTemplateRequest, SelectQuery,
        SqlMutationGenerator,
    };
    use crate::{
        ColumnInfo, DefaultSqlDialect, DocumentFilter, DocumentUpdate, KeySetRequest,
        MutationRequest, QueryLanguage, RowDelete, RowIdentity, RowInsert, RowPatch,
        SemanticFilter, SemanticPlanKind, SqlDeleteRequest, SqlGenerationOptions, SqlUpdateRequest,
        SqlUpsertRequest, Value, WhereOperator,
    };

    static DIALECT: DefaultSqlDialect = DefaultSqlDialect;

    #[test]
    fn categories_are_classified_by_mutation_kind() {
        let sql = MutationRequest::sql_insert(RowInsert::new(
            "users".to_string(),
            Some("public".to_string()),
            vec!["id".to_string()],
            vec![Value::Int(1)],
        ));

        let document = MutationRequest::document_update(DocumentUpdate::new(
            "users".to_string(),
            DocumentFilter::new(serde_json::json!({"_id": "a"})),
            serde_json::json!({"$set": {"name": "alice"}}),
        ));

        let key_value = MutationRequest::KeyValueSet(KeySetRequest::new("k", b"v".to_vec()));

        assert_eq!(sql.category(), MutationCategory::Sql);
        assert_eq!(document.category(), MutationCategory::Document);
        assert_eq!(key_value.category(), MutationCategory::KeyValue);
    }

    #[test]
    fn sql_generator_supports_only_sql_category() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        assert_eq!(generator.supported_categories(), &[MutationCategory::Sql]);
    }

    #[test]
    fn sql_generator_handles_insert_update_delete_upsert_and_rejects_non_sql() {
        let generator = SqlMutationGenerator::new(&DIALECT);

        let insert = MutationRequest::sql_insert(RowInsert::new(
            "users".to_string(),
            Some("public".to_string()),
            vec!["id".to_string(), "name".to_string()],
            vec![Value::Int(1), Value::Text("alice".to_string())],
        ));

        let update = MutationRequest::sql_update(RowPatch::new(
            RowIdentity::composite(vec!["id".to_string()], vec![Value::Int(1)]),
            "users".to_string(),
            Some("public".to_string()),
            vec![("name".to_string(), Value::Text("bob".to_string()))],
        ));

        let delete = MutationRequest::sql_delete(RowDelete::new(
            RowIdentity::composite(vec!["id".to_string()], vec![Value::Int(1)]),
            "users".to_string(),
            Some("public".to_string()),
        ));

        let upsert = MutationRequest::sql_upsert(SqlUpsertRequest::new(
            "users".to_string(),
            Some("public".to_string()),
            vec!["id".to_string(), "name".to_string()],
            vec![Value::Int(1), Value::Text("alice".to_string())],
            vec!["id".to_string()],
            vec![("name".to_string(), Value::Text("bob".to_string()))],
        ));

        let filtered_update = MutationRequest::sql_update_many(SqlUpdateRequest::new(
            "users".to_string(),
            Some("public".to_string()),
            SemanticFilter::compare("status", WhereOperator::Eq, Value::Text("active".into())),
            vec![("archived".to_string(), Value::Bool(true))],
        ));

        let filtered_delete = MutationRequest::sql_delete_many(
            SqlDeleteRequest::new(
                "users".to_string(),
                Some("public".to_string()),
                SemanticFilter::compare(
                    "status",
                    WhereOperator::Eq,
                    Value::Text("inactive".into()),
                ),
            )
            .with_returning(vec!["id".to_string()]),
        );

        let doc = MutationRequest::document_update(DocumentUpdate::new(
            "users".to_string(),
            DocumentFilter::new(serde_json::json!({"_id": "a"})),
            serde_json::json!({"$set": {"name": "alice"}}),
        ));

        let insert_query = generator.generate_mutation(&insert);
        assert!(insert_query.is_some());

        let update_query = generator.generate_mutation(&update);
        assert!(update_query.is_some());

        let delete_query = generator.generate_mutation(&delete);
        assert!(delete_query.is_some());

        let upsert_query = generator.generate_mutation(&upsert);
        assert!(upsert_query.is_some());
        assert!(
            upsert_query
                .as_ref()
                .is_some_and(|query| query.text.contains("INSERT INTO"))
        );

        let filtered_update_query = generator.generate_mutation(&filtered_update);
        assert!(filtered_update_query.is_some());
        assert!(
            filtered_update_query
                .as_ref()
                .is_some_and(|query| query.text.contains("WHERE"))
        );

        let filtered_delete_query = generator.generate_mutation(&filtered_delete);
        assert!(filtered_delete_query.is_some());
        assert!(
            filtered_delete_query
                .as_ref()
                .is_some_and(|query| query.text.contains("DELETE FROM"))
        );

        let doc_query = generator.generate_mutation(&doc);
        assert!(doc_query.is_none());
    }

    #[test]
    fn query_generator_plan_mutation_wraps_generated_query() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let insert = MutationRequest::sql_insert(RowInsert::new(
            "users".to_string(),
            Some("public".to_string()),
            vec!["id".to_string()],
            vec![Value::Int(1)],
        ));

        let plan = generator
            .plan_mutation(&insert)
            .expect("sql mutation should produce a plan");

        assert_eq!(plan.kind, SemanticPlanKind::MutationPreview);
        assert_eq!(plan.queries.len(), 1);
        assert_eq!(plan.queries[0].language, QueryLanguage::Sql);
        assert!(plan.queries[0].text.contains("INSERT"));
    }

    #[test]
    fn sql_generator_builds_placeholder_templates_for_table_preview() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                type_name: "integer".to_string(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
                enum_values: None,
            },
            ColumnInfo {
                name: "name".to_string(),
                type_name: "text".to_string(),
                nullable: true,
                is_primary_key: false,
                default_value: None,
                enum_values: None,
            },
        ];

        let generated = generator
            .generate_template(&MutationTemplateRequest {
                operation: MutationTemplateOperation::Update,
                schema: Some("public"),
                table: "users",
                columns: &columns,
                options: SqlGenerationOptions {
                    fully_qualified: true,
                    compact: false,
                },
            })
            .expect("sql template should generate");

        assert_eq!(generated.language, QueryLanguage::Sql);
        assert!(generated.text.contains("UPDATE \"public\".\"users\""));
        assert!(generated.text.contains("WHERE \"id\" ="));
    }

    #[test]
    fn sql_generator_builds_select_where_read_templates() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                type_name: "integer".to_string(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
                enum_values: None,
            },
            ColumnInfo {
                name: "name".to_string(),
                type_name: "text".to_string(),
                nullable: true,
                is_primary_key: false,
                default_value: None,
                enum_values: None,
            },
        ];

        let generated = generator
            .generate_read_template(&ReadTemplateRequest {
                operation: ReadTemplateOperation::SelectWhere,
                schema: Some("public"),
                table: "users",
                columns: &columns,
                options: SqlGenerationOptions {
                    fully_qualified: true,
                    compact: false,
                },
            })
            .expect("read template should generate");

        assert_eq!(generated.language, QueryLanguage::Sql);
        assert!(generated.text.contains("SELECT *"));
        assert!(generated.text.contains("FROM \"public\".\"users\""));
        assert!(generated.text.contains("WHERE \"id\" ="));
        assert!(generated.text.contains("AND \"name\" ="));
    }

    #[test]
    fn sql_generator_builds_select_all_read_templates_without_columns() {
        let generator = SqlMutationGenerator::new(&DIALECT);

        let generated = generator
            .generate_read_template(&ReadTemplateRequest {
                operation: ReadTemplateOperation::SelectAll,
                schema: Some("public"),
                table: "active_users",
                columns: &[],
                options: SqlGenerationOptions {
                    fully_qualified: true,
                    compact: false,
                },
            })
            .expect("select all template should generate");

        assert_eq!(generated.language, QueryLanguage::Sql);
        assert!(generated.text.contains("SELECT *"));
        assert!(generated.text.contains("FROM \"public\".\"active_users\";"));
    }

    // -------------------------------------------------------------------------
    // generate_select tests
    // -------------------------------------------------------------------------

    use crate::query::visual_query::{
        BoolOp, Comparator, FilterNode, JoinFilterNode, JoinKind, JoinOn, JoinPredicate, JoinStep,
        LiteralValue, Predicate, PredicateValue, ProjectedColumn, Projection,
        SortDirection as VSort, SortEntry, SourceTable, VisualQuerySpec,
    };

    fn users_spec() -> VisualQuerySpec {
        VisualQuerySpec {
            source: SourceTable {
                schema: None,
                table: "users".to_string(),
                alias: "users".to_string(),
            },
            projection: Projection::All,
            joins: vec![],
            filter: None,
            group_by: vec![],
            aggregates: vec![],
            having: None,
            sort: vec![],
            limit: Some(100),
            offset: 0,
        }
    }

    // -------------------------------------------------------------------------
    // materialize_for_editor tests
    // -------------------------------------------------------------------------

    #[test]
    fn materialize_question_mark_integer() {
        let q = SelectQuery {
            sql: "SELECT * FROM t WHERE id = ?".to_string(),
            params: vec![Value::Int(42)],
        };
        assert_eq!(
            q.materialize_for_editor(&DIALECT),
            "SELECT * FROM t WHERE id = 42"
        );
    }

    #[test]
    fn mutation_materialize_inlines_set_and_where_literals() {
        let m = super::GeneratedMutation {
            sql: "UPDATE t\nSET x = ?\nWHERE id = ?".to_string(),
            params: vec![Value::Text("1".to_string()), Value::Int(7)],
            used_raw_expression: false,
        };
        assert_eq!(
            m.materialize_for_editor(&DIALECT),
            "UPDATE t\nSET x = '1'\nWHERE id = 7"
        );
    }

    #[test]
    fn materialize_question_mark_string_with_embedded_quote() {
        let q = SelectQuery {
            sql: "SELECT * FROM t WHERE name = ?".to_string(),
            params: vec![Value::Text("O'Brien".to_string())],
        };
        assert_eq!(
            q.materialize_for_editor(&DIALECT),
            "SELECT * FROM t WHERE name = 'O''Brien'"
        );
    }

    #[test]
    fn materialize_question_mark_null() {
        let q = SelectQuery {
            sql: "SELECT * FROM t WHERE x = ?".to_string(),
            params: vec![Value::Null],
        };
        assert_eq!(
            q.materialize_for_editor(&DIALECT),
            "SELECT * FROM t WHERE x = NULL"
        );
    }

    #[test]
    fn materialize_question_mark_float() {
        let q = SelectQuery {
            sql: "SELECT * FROM t WHERE score > ?".to_string(),
            params: vec![Value::Float(3.14)],
        };
        let result = q.materialize_for_editor(&DIALECT);
        assert!(
            result.contains("3.14"),
            "expected float literal, got: {}",
            result
        );
    }

    #[test]
    fn materialize_question_mark_bool() {
        let q = SelectQuery {
            sql: "SELECT * FROM t WHERE active = ?".to_string(),
            params: vec![Value::Bool(true)],
        };
        assert_eq!(
            q.materialize_for_editor(&DIALECT),
            "SELECT * FROM t WHERE active = TRUE"
        );
    }

    #[test]
    fn materialize_question_mark_multiple_params() {
        let q = SelectQuery {
            sql: "SELECT * FROM t WHERE a = ? AND b = ?".to_string(),
            params: vec![Value::Int(1), Value::Text("hello".to_string())],
        };
        assert_eq!(
            q.materialize_for_editor(&DIALECT),
            "SELECT * FROM t WHERE a = 1 AND b = 'hello'"
        );
    }

    #[test]
    fn materialize_dollar_number_postgresql() {
        use crate::sql::dialect::PlaceholderStyle;

        struct PgDialect;
        impl crate::sql::dialect::SqlDialect for PgDialect {
            fn quote_identifier(&self, name: &str) -> String {
                format!("\"{}\"", name.replace('"', "\"\""))
            }
            fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
                match schema {
                    Some(s) => format!(
                        "{}.{}",
                        self.quote_identifier(s),
                        self.quote_identifier(table)
                    ),
                    None => self.quote_identifier(table),
                }
            }
            fn value_to_literal(&self, value: &Value) -> String {
                DIALECT.value_to_literal(value)
            }
            fn escape_string(&self, s: &str) -> String {
                s.replace('\'', "''")
            }
            fn placeholder_style(&self) -> PlaceholderStyle {
                PlaceholderStyle::DollarNumber
            }
        }

        let q = SelectQuery {
            sql: "SELECT * FROM t WHERE a = $1 AND b = $2".to_string(),
            params: vec![Value::Int(5), Value::Text("world".to_string())],
        };
        assert_eq!(
            q.materialize_for_editor(&PgDialect),
            "SELECT * FROM t WHERE a = 5 AND b = 'world'"
        );
    }

    #[test]
    fn materialize_dollar_number_out_of_order() {
        use crate::sql::dialect::PlaceholderStyle;

        struct PgDialect;
        impl crate::sql::dialect::SqlDialect for PgDialect {
            fn quote_identifier(&self, name: &str) -> String {
                format!("\"{}\"", name)
            }
            fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
                match schema {
                    Some(s) => format!(
                        "{}.{}",
                        self.quote_identifier(s),
                        self.quote_identifier(table)
                    ),
                    None => self.quote_identifier(table),
                }
            }
            fn value_to_literal(&self, value: &Value) -> String {
                DIALECT.value_to_literal(value)
            }
            fn escape_string(&self, s: &str) -> String {
                s.replace('\'', "''")
            }
            fn placeholder_style(&self) -> PlaceholderStyle {
                PlaceholderStyle::DollarNumber
            }
        }

        // $2 before $1 is unusual but must work
        let q = SelectQuery {
            sql: "SELECT * FROM t WHERE b = $2 AND a = $1".to_string(),
            params: vec![Value::Int(10), Value::Int(20)],
        };
        assert_eq!(
            q.materialize_for_editor(&PgDialect),
            "SELECT * FROM t WHERE b = 20 AND a = 10"
        );
    }

    #[test]
    fn materialize_at_sign_sqlserver() {
        use crate::sql::dialect::PlaceholderStyle;

        struct MssqlDialect;
        impl crate::sql::dialect::SqlDialect for MssqlDialect {
            fn quote_identifier(&self, name: &str) -> String {
                format!("[{}]", name)
            }
            fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
                match schema {
                    Some(s) => format!(
                        "{}.{}",
                        self.quote_identifier(s),
                        self.quote_identifier(table)
                    ),
                    None => self.quote_identifier(table),
                }
            }
            fn value_to_literal(&self, value: &Value) -> String {
                DIALECT.value_to_literal(value)
            }
            fn escape_string(&self, s: &str) -> String {
                s.replace('\'', "''")
            }
            fn placeholder_style(&self) -> PlaceholderStyle {
                PlaceholderStyle::AtSign
            }
        }

        let q = SelectQuery {
            sql: "SELECT * FROM t WHERE id = @p1".to_string(),
            params: vec![Value::Int(7)],
        };
        assert_eq!(
            q.materialize_for_editor(&MssqlDialect),
            "SELECT * FROM t WHERE id = 7"
        );
    }

    #[test]
    fn materialize_no_params_returns_sql_unchanged() {
        let q = SelectQuery {
            sql: "SELECT * FROM t".to_string(),
            params: vec![],
        };
        assert_eq!(q.materialize_for_editor(&DIALECT), "SELECT * FROM t");
    }

    #[test]
    fn default_impl_returns_none() {
        struct StubGenerator;
        impl QueryGenerator for StubGenerator {
            fn supported_categories(&self) -> &'static [MutationCategory] {
                &[]
            }
            fn generate_mutation(&self, _: &MutationRequest) -> Option<GeneratedQuery> {
                None
            }
        }

        let result = StubGenerator.generate_select(&users_spec());
        assert_eq!(result, Ok(None));
    }

    #[test]
    fn select_star_from_table_with_alias() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let q = generator
            .generate_select(&users_spec())
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            q.sql.contains("SELECT *"),
            "expected SELECT *, got: {}",
            q.sql
        );
        assert!(
            q.sql.contains("\"users\""),
            "expected quoted table, got: {}",
            q.sql
        );
        assert!(q.params.is_empty());
    }

    #[test]
    fn explicit_projection_emits_named_columns_in_order() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.projection = Projection::Explicit(vec![
            ProjectedColumn {
                source_alias: "users".to_string(),
                column: "id".to_string(),
                alias: None,
            },
            ProjectedColumn {
                source_alias: "users".to_string(),
                column: "name".to_string(),
                alias: None,
            },
        ]);

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        let pos_id = q.sql.find("\"id\"").expect("must contain id");
        let pos_name = q.sql.find("\"name\"").expect("must contain name");
        assert!(
            pos_id < pos_name,
            "id must appear before name in: {}",
            q.sql
        );
    }

    #[test]
    fn single_eq_predicate_produces_where_clause_and_param() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.filter = Some(FilterNode::Predicate(Predicate {
            source_alias: "users".to_string(),
            column: "status".to_string(),
            comparator: Comparator::Eq,
            value: PredicateValue::Single(LiteralValue::Text("active".to_string())),
            node_id: 0,
        }));

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(q.sql.contains("WHERE"), "expected WHERE, got: {}", q.sql);
        assert!(
            q.sql.contains("\"status\""),
            "expected quoted column, got: {}",
            q.sql
        );
        assert_eq!(q.params.len(), 1, "expected 1 param, got: {:?}", q.params);
    }

    #[test]
    fn nested_and_or_produces_correct_paren_grouping() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();

        spec.filter = Some(FilterNode::Group {
            op: BoolOp::And,
            children: vec![
                FilterNode::Predicate(Predicate {
                    source_alias: "users".to_string(),
                    column: "active".to_string(),
                    comparator: Comparator::Eq,
                    value: PredicateValue::Single(LiteralValue::Bool(true)),
                    node_id: 0,
                }),
                FilterNode::Group {
                    op: BoolOp::Or,
                    children: vec![
                        FilterNode::Predicate(Predicate {
                            source_alias: "users".to_string(),
                            column: "role".to_string(),
                            comparator: Comparator::Eq,
                            value: PredicateValue::Single(LiteralValue::Text("admin".to_string())),
                            node_id: 0,
                        }),
                        FilterNode::Predicate(Predicate {
                            source_alias: "users".to_string(),
                            column: "role".to_string(),
                            comparator: Comparator::Eq,
                            value: PredicateValue::Single(LiteralValue::Text(
                                "superuser".to_string(),
                            )),
                            node_id: 0,
                        }),
                    ],
                },
            ],
        });

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(q.sql.contains("WHERE"), "expected WHERE, got: {}", q.sql);
        assert!(q.sql.contains('('), "expected parentheses, got: {}", q.sql);
        assert!(
            q.sql.contains("OR"),
            "expected OR inside parens, got: {}",
            q.sql
        );
        assert!(q.sql.contains("AND"), "expected AND, got: {}", q.sql);
        assert_eq!(q.params.len(), 3);
    }

    #[test]
    fn incomplete_join_is_skipped() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.joins = vec![JoinStep {
            kind: JoinKind::Inner,
            from_alias: "users".to_string(),
            to_schema: None,
            to_table: String::new(),
            to_alias: String::new(),
            on: JoinOn::RawExpression(String::new()),
        }];

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            !q.sql.contains("JOIN"),
            "expected no JOIN keyword when row is incomplete, got: {}",
            q.sql
        );
    }

    #[test]
    fn inner_join_with_fk_path() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.joins = vec![JoinStep {
            kind: JoinKind::Inner,
            from_alias: "users".to_string(),
            to_schema: None,
            to_table: "orders".to_string(),
            to_alias: "orders".to_string(),
            on: JoinOn::FkPath {
                from_column: "id".to_string(),
                to_column: "user_id".to_string(),
            },
        }];

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            q.sql.contains("INNER JOIN"),
            "expected INNER JOIN, got: {}",
            q.sql
        );
        assert!(
            q.sql.contains("\"orders\""),
            "expected orders table, got: {}",
            q.sql
        );
        assert!(q.sql.contains("ON"), "expected ON clause, got: {}", q.sql);
        assert!(
            q.sql.contains("\"id\""),
            "expected from column, got: {}",
            q.sql
        );
        assert!(
            q.sql.contains("\"user_id\""),
            "expected to column, got: {}",
            q.sql
        );
    }

    #[test]
    fn left_join_with_raw_expression() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.joins = vec![JoinStep {
            kind: JoinKind::Left,
            from_alias: "users".to_string(),
            to_schema: None,
            to_table: "profiles".to_string(),
            to_alias: "profiles".to_string(),
            on: JoinOn::RawExpression("users.id = profiles.user_id".to_string()),
        }];

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            q.sql.contains("LEFT JOIN"),
            "expected LEFT JOIN, got: {}",
            q.sql
        );
        assert!(
            q.sql.contains("users.id = profiles.user_id"),
            "expected raw expression, got: {}",
            q.sql
        );
    }

    #[test]
    fn multi_hop_two_join_chain() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.joins = vec![
            JoinStep {
                kind: JoinKind::Left,
                from_alias: "users".to_string(),
                to_schema: None,
                to_table: "orders".to_string(),
                to_alias: "orders".to_string(),
                on: JoinOn::FkPath {
                    from_column: "id".to_string(),
                    to_column: "user_id".to_string(),
                },
            },
            JoinStep {
                kind: JoinKind::Left,
                from_alias: "orders".to_string(),
                to_schema: None,
                to_table: "items".to_string(),
                to_alias: "items".to_string(),
                on: JoinOn::FkPath {
                    from_column: "id".to_string(),
                    to_column: "order_id".to_string(),
                },
            },
        ];

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        let first_join = q.sql.find("LEFT JOIN").expect("must have first JOIN");
        let second_join = q.sql.rfind("LEFT JOIN").expect("must have second JOIN");
        assert!(first_join != second_join, "must have two separate JOINs");
        assert!(q.sql.contains("\"orders\""));
        assert!(q.sql.contains("\"items\""));
    }

    #[test]
    fn join_conditions_nested_or_inside_and_root_parenthesises() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();

        // Root AND of: (a.x = b.x OR a.y = b.y) AND (a.z = b.z)
        let root = JoinFilterNode::Group {
            node_id: 0,
            op: BoolOp::And,
            children: vec![
                JoinFilterNode::Group {
                    node_id: 0,
                    op: BoolOp::Or,
                    children: vec![
                        JoinFilterNode::Predicate(JoinPredicate {
                            node_id: 0,
                            left: "a.x".to_string(),
                            op: Comparator::Eq,
                            right: "b.x".to_string(),
                        }),
                        JoinFilterNode::Predicate(JoinPredicate {
                            node_id: 0,
                            left: "a.y".to_string(),
                            op: Comparator::Eq,
                            right: "b.y".to_string(),
                        }),
                    ],
                },
                JoinFilterNode::Predicate(JoinPredicate {
                    node_id: 0,
                    left: "a.z".to_string(),
                    op: Comparator::Eq,
                    right: "b.z".to_string(),
                }),
            ],
        };

        spec.joins = vec![JoinStep {
            kind: JoinKind::Inner,
            from_alias: "a".to_string(),
            to_schema: None,
            to_table: "b".to_string(),
            to_alias: "b".to_string(),
            on: JoinOn::Conditions(root),
        }];

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");

        // Nested OR must be parenthesised; root AND must NOT add outer parens.
        assert!(
            q.sql.contains("(a.x = b.x OR a.y = b.y)"),
            "expected parenthesised OR group, got: {}",
            q.sql
        );
        assert!(
            q.sql.contains("AND a.z = b.z"),
            "expected AND between groups, got: {}",
            q.sql
        );
        // Root AND must not wrap the whole expression: the closing paren of
        // the OR sub-group is followed by " AND a.z = b.z" outside of any
        // additional grouping.
        assert!(
            q.sql.contains(") AND a.z = b.z"),
            "root AND should not be wrapped in outer parens, got: {}",
            q.sql
        );
    }

    #[test]
    fn join_conditions_incomplete_predicates_skipped() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();

        // One complete predicate, one half-typed (right empty).
        let root = JoinFilterNode::Group {
            node_id: 0,
            op: BoolOp::And,
            children: vec![
                JoinFilterNode::Predicate(JoinPredicate {
                    node_id: 0,
                    left: "a.id".to_string(),
                    op: Comparator::Eq,
                    right: "b.a_id".to_string(),
                }),
                JoinFilterNode::Predicate(JoinPredicate {
                    node_id: 0,
                    left: "a.tenant".to_string(),
                    op: Comparator::Eq,
                    right: String::new(),
                }),
            ],
        };

        spec.joins = vec![JoinStep {
            kind: JoinKind::Inner,
            from_alias: "a".to_string(),
            to_schema: None,
            to_table: "b".to_string(),
            to_alias: "b".to_string(),
            on: JoinOn::Conditions(root),
        }];

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(q.sql.contains("a.id = b.a_id"), "got: {}", q.sql);
        assert!(
            !q.sql.contains("a.tenant"),
            "incomplete leaf leaked into SQL: {}",
            q.sql
        );
    }

    #[test]
    fn order_by_multi_key() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.sort = vec![
            SortEntry {
                source_alias: "users".to_string(),
                column: "name".to_string(),
                direction: VSort::Asc,
            },
            SortEntry {
                source_alias: "users".to_string(),
                column: "created_at".to_string(),
                direction: VSort::Desc,
            },
        ];

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            q.sql.contains("ORDER BY"),
            "expected ORDER BY, got: {}",
            q.sql
        );
        let pos_name = q.sql.find("\"name\"").expect("must contain name");
        let pos_created = q
            .sql
            .find("\"created_at\"")
            .expect("must contain created_at");
        assert!(pos_name < pos_created);
        assert!(q.sql.contains("ASC"), "expected ASC, got: {}", q.sql);
        assert!(q.sql.contains("DESC"), "expected DESC, got: {}", q.sql);
    }

    #[test]
    fn limit_zero_produces_no_limit_clause() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.limit = Some(0);

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(!q.sql.contains("LIMIT"), "must not emit LIMIT 0: {}", q.sql);
    }

    #[test]
    fn limit_none_produces_no_limit_clause() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.limit = None;

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(!q.sql.contains("LIMIT"), "must not emit LIMIT: {}", q.sql);
    }

    #[test]
    fn positive_limit_produces_limit_clause() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let q = generator
            .generate_select(&users_spec())
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            q.sql.contains("LIMIT 100"),
            "expected LIMIT 100, got: {}",
            q.sql
        );
    }

    #[test]
    fn offset_zero_produces_no_offset_clause() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let q = generator
            .generate_select(&users_spec())
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            !q.sql.contains("OFFSET"),
            "must not emit OFFSET 0: {}",
            q.sql
        );
    }

    #[test]
    fn positive_offset_produces_offset_clause() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.offset = 10;

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            q.sql.contains("OFFSET 10"),
            "expected OFFSET 10, got: {}",
            q.sql
        );
    }

    #[test]
    fn dollar_number_placeholder_used_when_dialect_says_so() {
        use crate::sql::dialect::{PlaceholderStyle, SqlDialect};

        struct DollarDialect;
        impl SqlDialect for DollarDialect {
            fn quote_identifier(&self, name: &str) -> String {
                DIALECT.quote_identifier(name)
            }
            fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
                DIALECT.qualified_table(schema, table)
            }
            fn value_to_literal(&self, value: &crate::Value) -> String {
                DIALECT.value_to_literal(value)
            }
            fn escape_string(&self, s: &str) -> String {
                s.replace('\'', "''")
            }
            fn placeholder_style(&self) -> PlaceholderStyle {
                PlaceholderStyle::DollarNumber
            }
        }

        static DOLLAR: DollarDialect = DollarDialect;
        let generator = SqlMutationGenerator::new(&DOLLAR);
        let mut spec = users_spec();
        spec.filter = Some(FilterNode::Predicate(Predicate {
            source_alias: "users".to_string(),
            column: "id".to_string(),
            comparator: Comparator::Eq,
            value: PredicateValue::Single(LiteralValue::Integer(1)),
            node_id: 0,
        }));

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            q.sql.contains("$1"),
            "expected $1 placeholder, got: {}",
            q.sql
        );
    }

    #[test]
    fn question_mark_placeholder_used_when_dialect_says_so() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.filter = Some(FilterNode::Predicate(Predicate {
            source_alias: "users".to_string(),
            column: "id".to_string(),
            comparator: Comparator::Eq,
            value: PredicateValue::Single(LiteralValue::Integer(1)),
            node_id: 0,
        }));

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            q.sql.contains('?'),
            "expected ? placeholder, got: {}",
            q.sql
        );
    }

    #[test]
    fn reserved_word_column_name_is_quoted() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.projection = Projection::Explicit(vec![ProjectedColumn {
            source_alias: "users".to_string(),
            column: "order".to_string(),
            alias: None,
        }]);

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            q.sql.contains("\"order\""),
            "reserved word must be quoted, got: {}",
            q.sql
        );
    }

    #[test]
    fn empty_sort_produces_no_order_by() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let q = generator
            .generate_select(&users_spec())
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            !q.sql.contains("ORDER BY"),
            "must not emit ORDER BY when sort is empty: {}",
            q.sql
        );
    }

    #[test]
    fn schema_qualified_table_emitted_when_schema_present() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.source.schema = Some("public".to_string());

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            q.sql.contains("\"public\".\"users\""),
            "expected schema-qualified FROM, got: {}",
            q.sql
        );
    }

    #[test]
    fn is_null_predicate_emits_is_null_without_placeholder() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.filter = Some(FilterNode::Predicate(Predicate {
            source_alias: "users".to_string(),
            column: "deleted_at".to_string(),
            comparator: Comparator::IsNull,
            value: PredicateValue::None,
            node_id: 0,
        }));

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            q.sql.contains("IS NULL"),
            "expected IS NULL, got: {}",
            q.sql
        );
        assert!(q.params.is_empty(), "IS NULL must have no params");
    }

    #[test]
    fn in_predicate_emits_in_clause_with_params() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.filter = Some(FilterNode::Predicate(Predicate {
            source_alias: "users".to_string(),
            column: "id".to_string(),
            comparator: Comparator::In,
            value: PredicateValue::List(vec![
                LiteralValue::Integer(1),
                LiteralValue::Integer(2),
                LiteralValue::Integer(3),
            ]),
            node_id: 0,
        }));

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(q.sql.contains("IN"), "expected IN clause, got: {}", q.sql);
        assert_eq!(q.params.len(), 3, "IN with 3 values must have 3 params");
    }

    #[test]
    fn column_alias_is_emitted_in_select() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.projection = Projection::Explicit(vec![ProjectedColumn {
            source_alias: "users".to_string(),
            column: "name".to_string(),
            alias: Some("customer_name".to_string()),
        }]);

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");
        assert!(
            q.sql.contains("\"customer_name\""),
            "expected aliased column, got: {}",
            q.sql
        );
    }

    // T-13 — [RED] Tests for QueryGenerator trait extensions (spec B-5, DR-3.1, DR-3.3)

    mod mutation_generator_tests {
        use super::super::{
            GeneratedMutation, GeneratorError, MockMutationGenerator, QueryGenerator,
        };
        use crate::query::table_browser::TableRef;
        use crate::query::visual_query::{
            Assignment, AssignmentValue, MutationKind, ScalarLiteral, VisualMutationSpec,
        };

        fn table_ref(name: &str) -> TableRef {
            TableRef {
                schema: None,
                name: name.to_string(),
            }
        }

        fn spec_delete_no_filter(table: &str) -> VisualMutationSpec {
            VisualMutationSpec {
                from: table_ref(table),
                filter: None,
                kind: MutationKind::Delete,
            }
        }

        fn spec_update_empty_assignments(table: &str) -> VisualMutationSpec {
            VisualMutationSpec {
                from: table_ref(table),
                filter: None,
                kind: MutationKind::Update {
                    assignments: vec![],
                },
            }
        }

        fn spec_update_with_literal(table: &str) -> VisualMutationSpec {
            VisualMutationSpec {
                from: table_ref(table),
                filter: None,
                kind: MutationKind::Update {
                    assignments: vec![Assignment {
                        column: "name".to_string(),
                        value: AssignmentValue::Literal(ScalarLiteral::Text("Alice".to_string())),
                    }],
                },
            }
        }

        // B-5: generate_update_from_spec with empty assignments → EmptyAssignments error
        #[test]
        fn empty_assignments_returns_error() {
            let mock = MockMutationGenerator;
            let spec = spec_update_empty_assignments("users");
            let result = mock.generate_update_from_spec(&spec);
            assert!(
                matches!(result, Err(GeneratorError::EmptyAssignments)),
                "expected EmptyAssignments, got: {result:?}"
            );
        }

        // Return type is Result<GeneratedMutation, GeneratorError>
        #[test]
        fn delete_returns_generated_mutation() {
            let mock = MockMutationGenerator;
            let spec = spec_delete_no_filter("orders");
            let result = mock.generate_delete_from_spec(&spec);
            assert!(result.is_ok(), "delete should succeed: {result:?}");
        }

        // GeneratedMutation fields are accessible
        #[test]
        fn generated_mutation_fields_accessible() {
            let mock = MockMutationGenerator;
            let spec = spec_update_with_literal("users");
            let result = mock.generate_update_from_spec(&spec).expect("must succeed");
            let _ = &result.sql;
            let _ = &result.params;
            let _ = result.used_raw_expression;
        }

        // GeneratorError::EmptyAssignments has Display
        #[test]
        fn generator_error_empty_assignments_has_display() {
            let err = GeneratorError::EmptyAssignments;
            let msg = err.to_string();
            assert!(!msg.is_empty());
        }
    }

    // T-15 — [RED] Tests for SqlMutationGenerator::generate_delete_from_spec (B-1, B-2, B-8)
    // T-17 — [RED] Tests for SqlMutationGenerator::generate_update_from_spec (B-3–B-7)

    mod sql_mutation_generator_tests {
        use super::{DIALECT, QueryGenerator, SqlMutationGenerator};
        use crate::Value;
        use crate::query::table_browser::TableRef;
        use crate::query::visual_query::{
            Assignment, AssignmentValue, BoolOp, Comparator, FilterNode, LiteralValue,
            MutationKind, Predicate, PredicateValue, ScalarLiteral, VisualMutationSpec,
        };
        use crate::sql::dialect::{PlaceholderStyle, SqlDialect};

        fn table_ref(name: &str) -> TableRef {
            TableRef {
                schema: None,
                name: name.to_string(),
            }
        }

        fn filter_id_eq(id: i64) -> FilterNode {
            FilterNode::Predicate(Predicate {
                source_alias: "t".to_string(),
                column: "id".to_string(),
                comparator: Comparator::Eq,
                value: PredicateValue::Single(LiteralValue::Integer(id)),
                node_id: 0,
            })
        }

        fn delete_spec_with_filter(table: &str) -> VisualMutationSpec {
            VisualMutationSpec {
                from: table_ref(table),
                filter: Some(filter_id_eq(42)),
                kind: MutationKind::Delete,
            }
        }

        fn delete_spec_no_filter(table: &str) -> VisualMutationSpec {
            VisualMutationSpec {
                from: table_ref(table),
                filter: None,
                kind: MutationKind::Delete,
            }
        }

        fn update_spec_with_literal_and_filter(table: &str) -> VisualMutationSpec {
            VisualMutationSpec {
                from: table_ref(table),
                filter: Some(filter_id_eq(7)),
                kind: MutationKind::Update {
                    assignments: vec![Assignment {
                        column: "name".to_string(),
                        value: AssignmentValue::Literal(ScalarLiteral::Text("Alice".to_string())),
                    }],
                },
            }
        }

        fn update_spec_no_filter(table: &str) -> VisualMutationSpec {
            VisualMutationSpec {
                from: table_ref(table),
                filter: None,
                kind: MutationKind::Update {
                    assignments: vec![Assignment {
                        column: "status".to_string(),
                        value: AssignmentValue::Literal(ScalarLiteral::Text("active".to_string())),
                    }],
                },
            }
        }

        fn update_spec_with_expression(table: &str) -> VisualMutationSpec {
            VisualMutationSpec {
                from: table_ref(table),
                filter: Some(filter_id_eq(1)),
                kind: MutationKind::Update {
                    assignments: vec![Assignment {
                        column: "price".to_string(),
                        value: AssignmentValue::Expression("price * 1.1".to_string()),
                    }],
                },
            }
        }

        // MySQL dialect for B-7 dialect parity test
        struct MySqlDialect;
        impl SqlDialect for MySqlDialect {
            fn quote_identifier(&self, name: &str) -> String {
                format!("`{}`", name)
            }
            fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
                match schema {
                    Some(s) => format!(
                        "{}.{}",
                        self.quote_identifier(s),
                        self.quote_identifier(table)
                    ),
                    None => self.quote_identifier(table),
                }
            }
            fn value_to_literal(&self, value: &Value) -> String {
                DIALECT.value_to_literal(value)
            }
            fn escape_string(&self, s: &str) -> String {
                s.replace('\'', "''")
            }
            fn placeholder_style(&self) -> PlaceholderStyle {
                PlaceholderStyle::QuestionMark
            }
        }
        static MYSQL: MySqlDialect = MySqlDialect;

        // Postgres dialect (DollarNumber placeholders) for chunk parameter tests
        struct PgDialect;
        impl SqlDialect for PgDialect {
            fn quote_identifier(&self, name: &str) -> String {
                format!("\"{}\"", name)
            }
            fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
                match schema {
                    Some(s) => format!(
                        "{}.{}",
                        self.quote_identifier(s),
                        self.quote_identifier(table)
                    ),
                    None => self.quote_identifier(table),
                }
            }
            fn value_to_literal(&self, value: &Value) -> String {
                DIALECT.value_to_literal(value)
            }
            fn escape_string(&self, s: &str) -> String {
                s.replace('\'', "''")
            }
            fn placeholder_style(&self) -> PlaceholderStyle {
                PlaceholderStyle::DollarNumber
            }
        }
        static PG: PgDialect = PgDialect;

        // B-1: generate_delete_from_spec with filter (PostgreSQL default dialect)
        #[test]
        fn b1_delete_with_filter_postgres() {
            let generator = SqlMutationGenerator::new(&DIALECT);
            let spec = delete_spec_with_filter("orders");
            let result = generator
                .generate_delete_from_spec(&spec)
                .expect("must succeed");
            assert!(
                result.sql.contains("DELETE FROM"),
                "must have DELETE FROM: {}",
                result.sql
            );
            assert!(
                result.sql.contains("orders"),
                "must reference table: {}",
                result.sql
            );
            assert!(
                result.sql.contains("WHERE"),
                "must have WHERE: {}",
                result.sql
            );
            assert!(
                result.sql.contains("id"),
                "WHERE must reference id: {}",
                result.sql
            );
            assert!(!result.used_raw_expression);
        }

        // B-2: generate_delete_from_spec without filter — no WHERE clause, no error
        #[test]
        fn b2_delete_no_filter_produces_bare_delete() {
            let generator = SqlMutationGenerator::new(&DIALECT);
            let spec = delete_spec_no_filter("orders");
            let result = generator
                .generate_delete_from_spec(&spec)
                .expect("must succeed");
            assert!(
                result.sql.contains("DELETE FROM"),
                "must have DELETE FROM: {}",
                result.sql
            );
            assert!(
                result.sql.contains("orders"),
                "must reference table: {}",
                result.sql
            );
            assert!(
                !result.sql.contains("WHERE"),
                "must NOT have WHERE: {}",
                result.sql
            );
        }

        // B-8: generate_delete_from_spec — SQLite (uses DIALECT which is QuestionMark)
        #[test]
        fn b8_delete_with_filter_sqlite_dialect() {
            let generator = SqlMutationGenerator::new(&DIALECT);
            let spec = delete_spec_with_filter("orders");
            let result = generator
                .generate_delete_from_spec(&spec)
                .expect("must succeed");
            assert!(
                result.sql.contains("?") || result.sql.contains("id"),
                "SQLite uses ? placeholder: {}",
                result.sql
            );
        }

        // B-3: generate_update_from_spec with assignments and filter (PostgreSQL)
        #[test]
        fn b3_update_with_literal_and_filter() {
            let generator = SqlMutationGenerator::new(&DIALECT);
            let spec = update_spec_with_literal_and_filter("users");
            let result = generator
                .generate_update_from_spec(&spec)
                .expect("must succeed");
            assert!(
                result.sql.contains("UPDATE"),
                "must have UPDATE: {}",
                result.sql
            );
            assert!(
                result.sql.contains("users"),
                "must reference table: {}",
                result.sql
            );
            assert!(result.sql.contains("SET"), "must have SET: {}", result.sql);
            assert!(
                result.sql.contains("name"),
                "must reference column: {}",
                result.sql
            );
            assert!(
                result.sql.contains("WHERE"),
                "must have WHERE: {}",
                result.sql
            );
            assert!(!result.used_raw_expression);
            assert!(!result.params.is_empty(), "literal must be a bound param");
        }

        // B-4: generate_update_from_spec without filter
        #[test]
        fn b4_update_no_filter_no_where_clause() {
            let generator = SqlMutationGenerator::new(&DIALECT);
            let spec = update_spec_no_filter("users");
            let result = generator
                .generate_update_from_spec(&spec)
                .expect("must succeed");
            assert!(
                result.sql.contains("UPDATE"),
                "must have UPDATE: {}",
                result.sql
            );
            assert!(
                !result.sql.contains("WHERE"),
                "must NOT have WHERE: {}",
                result.sql
            );
        }

        // B-5: generate_update_from_spec with empty assignments → error
        #[test]
        fn b5_update_empty_assignments_error() {
            let generator = SqlMutationGenerator::new(&DIALECT);
            let spec = VisualMutationSpec {
                from: table_ref("users"),
                filter: None,
                kind: MutationKind::Update {
                    assignments: vec![],
                },
            };
            let result = generator.generate_update_from_spec(&spec);
            assert!(
                matches!(
                    result,
                    Err(crate::query::generator::GeneratorError::EmptyAssignments)
                ),
                "expected EmptyAssignments, got: {result:?}"
            );
        }

        // B-6: AssignmentValue::Expression is interpolated inline, used_raw_expression = true,
        // and NO `/* expr */` comment appears in the SQL (delivery decision #6188 locks this:
        // the side-channel flag, not SQL text, carries the literal-vs-expression signal).
        #[test]
        fn b6_expression_assignment_inline_and_flags_raw() {
            let generator = SqlMutationGenerator::new(&DIALECT);
            // spec has expression assignment + filter (filter_id_eq(1) adds one bound param)
            let spec = update_spec_with_expression("products");
            let result = generator
                .generate_update_from_spec(&spec)
                .expect("must succeed");
            assert!(
                result.sql.contains("price * 1.1"),
                "expression must be inline: {}",
                result.sql
            );
            assert!(result.used_raw_expression, "must set used_raw_expression");
            assert!(
                !result.sql.contains("/* expr */"),
                "SQL must NOT contain /* expr */ annotation (use used_raw_expression flag instead): {}",
                result.sql
            );
            // The WHERE clause adds one param for id = 1; the expression assignment itself
            // must NOT add a param. So params.len() == 1 (only the WHERE param).
            assert_eq!(
                result.params.len(),
                1,
                "only WHERE param expected, expression is not bound"
            );
        }

        // B-7: MySQL backtick quoting and ? placeholder
        #[test]
        fn b7_mysql_backtick_and_question_mark() {
            let generator = SqlMutationGenerator::new(&MYSQL);
            let spec = update_spec_with_literal_and_filter("users");
            let result = generator
                .generate_update_from_spec(&spec)
                .expect("must succeed");
            assert!(
                result.sql.contains('`'),
                "MySQL must use backtick quoting: {}",
                result.sql
            );
            assert!(
                result.sql.contains('?'),
                "MySQL must use ? placeholder: {}",
                result.sql
            );
        }

        // -----------------------------------------------------------------------
        // Chunk generator tests (F-1 fixes)
        // -----------------------------------------------------------------------

        fn pk_values_single(ids: &[i64]) -> Vec<Vec<Value>> {
            ids.iter().map(|id| vec![Value::Int(*id)]).collect()
        }

        fn pk_values_composite(pairs: &[(i64, i64)]) -> Vec<Vec<Value>> {
            pairs
                .iter()
                .map(|(a, b)| vec![Value::Int(*a), Value::Int(*b)])
                .collect()
        }

        // DR-10.x: chunked_update_with_user_filter_emits_single_where
        // When spec.filter is Some, the chunk DML must contain exactly one WHERE keyword.
        #[test]
        fn chunked_update_with_user_filter_emits_single_where() {
            let generator = SqlMutationGenerator::new(&DIALECT);
            let spec = update_spec_with_literal_and_filter("orders");
            let pk_values = pk_values_single(&[1, 2, 3]);
            let result = generator
                .generate_update_chunk_from_spec(&spec, &["id"], &pk_values)
                .expect("must succeed");
            let where_count = result.sql.matches("WHERE").count();
            assert_eq!(
                where_count, 1,
                "must contain exactly one WHERE; SQL: {}",
                result.sql
            );
        }

        // DR-13.x: chunked_delete_with_composite_pk_uses_row_constructor
        // A 2-column PK must produce (pk0, pk1) IN ((?,?), ...) row-constructor syntax.
        #[test]
        fn chunked_delete_with_composite_pk_uses_row_constructor() {
            let generator = SqlMutationGenerator::new(&DIALECT);
            let spec = VisualMutationSpec {
                from: table_ref("orders"),
                filter: None,
                kind: MutationKind::Delete,
            };
            let pk_values = pk_values_composite(&[(1, 10), (2, 20)]);
            let result = generator
                .generate_delete_chunk_from_spec(&spec, &["tenant_id", "id"], &pk_values)
                .expect("must succeed");
            assert!(
                result.sql.contains("\"tenant_id\", \"id\"")
                    || result.sql.contains("(\"tenant_id\", \"id\")"),
                "composite PK must be row-constructor form; SQL: {}",
                result.sql
            );
            assert!(
                result.sql.contains("IN"),
                "must have IN clause; SQL: {}",
                result.sql
            );
            assert_eq!(
                result.params.len(),
                4,
                "2 rows × 2 PK cols = 4 params; got {} params; SQL: {}",
                result.params.len(),
                result.sql
            );
        }

        // Verifies that placeholder numbering starts fresh ($1) for each new chunk statement.
        // In Postgres, `generate_delete_chunk_from_spec` for a chunk should use $1, $2, ... .
        #[test]
        fn chunked_chunk_param_numbering_postgres() {
            let generator = SqlMutationGenerator::new(&PG);
            let spec = VisualMutationSpec {
                from: table_ref("events"),
                filter: None,
                kind: MutationKind::Delete,
            };
            let pk_values = pk_values_single(&[100, 200]);
            let result = generator
                .generate_delete_chunk_from_spec(&spec, &["id"], &pk_values)
                .expect("must succeed");
            assert!(
                result.sql.contains("$1") && result.sql.contains("$2"),
                "placeholders must start at $1; SQL: {}",
                result.sql
            );
            assert_eq!(result.params.len(), 2, "2 pk values = 2 params");
        }

        // chunked DELETE with user filter: filter param then pk param
        #[test]
        fn chunked_delete_filter_params_before_pk_params() {
            let generator = SqlMutationGenerator::new(&DIALECT);
            let spec = VisualMutationSpec {
                from: table_ref("users"),
                filter: Some(filter_id_eq(42)),
                kind: MutationKind::Delete,
            };
            let pk_values = pk_values_single(&[10, 20]);
            let result = generator
                .generate_delete_chunk_from_spec(&spec, &["pk"], &pk_values)
                .expect("must succeed");
            assert_eq!(
                result.params.len(),
                3,
                "1 filter param + 2 pk params = 3 total; SQL: {}",
                result.sql
            );
            assert!(
                result.sql.contains("WHERE"),
                "must have WHERE; SQL: {}",
                result.sql
            );
            let where_count = result.sql.matches("WHERE").count();
            assert_eq!(
                where_count, 1,
                "must have exactly one WHERE; SQL: {}",
                result.sql
            );
        }

        // -----------------------------------------------------------------------
        // F-R2-1: MSSQL composite PK must use OR-of-ANDs, not row-constructor IN
        // -----------------------------------------------------------------------

        struct MssqlTestDialect;
        impl SqlDialect for MssqlTestDialect {
            fn quote_identifier(&self, name: &str) -> String {
                format!("[{}]", name.replace(']', "]]"))
            }
            fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
                match schema {
                    Some(s) => format!(
                        "{}.{}",
                        self.quote_identifier(s),
                        self.quote_identifier(table)
                    ),
                    None => self.quote_identifier(table),
                }
            }
            fn value_to_literal(&self, value: &Value) -> String {
                DIALECT.value_to_literal(value)
            }
            fn escape_string(&self, s: &str) -> String {
                s.replace('\'', "''")
            }
            fn placeholder_style(&self) -> PlaceholderStyle {
                PlaceholderStyle::QuestionMark
            }
            fn supports_row_constructor_in(&self) -> bool {
                false
            }
        }
        static MSSQL: MssqlTestDialect = MssqlTestDialect;

        // DR-13.x: MSSQL composite PK must expand to OR-of-ANDs (T-SQL cannot use row constructors in IN).
        #[test]
        fn composite_pk_in_clause_mssql_uses_or_of_ands() {
            let generator = SqlMutationGenerator::new(&MSSQL);
            let spec = VisualMutationSpec {
                from: table_ref("orders"),
                filter: None,
                kind: MutationKind::Delete,
            };
            let pk_values = pk_values_composite(&[(1, 10), (2, 20)]);
            let result = generator
                .generate_delete_chunk_from_spec(&spec, &["tenant_id", "order_id"], &pk_values)
                .expect("must succeed");

            assert!(
                result.sql.contains("OR"),
                "MSSQL composite PK must use OR-of-ANDs; SQL: {}",
                result.sql
            );
            assert!(
                !result.sql.contains(") IN ("),
                "MSSQL must NOT use row-constructor IN; SQL: {}",
                result.sql
            );
            assert_eq!(
                result.params.len(),
                4,
                "2 rows × 2 PK cols = 4 params; got {}; SQL: {}",
                result.params.len(),
                result.sql
            );
        }

        // DR-13.x: PostgreSQL composite PK keeps row-constructor syntax.
        #[test]
        fn composite_pk_in_clause_postgres_uses_row_constructor() {
            let generator = SqlMutationGenerator::new(&PG);
            let spec = VisualMutationSpec {
                from: table_ref("orders"),
                filter: None,
                kind: MutationKind::Delete,
            };
            let pk_values = pk_values_composite(&[(1, 10), (2, 20)]);
            let result = generator
                .generate_delete_chunk_from_spec(&spec, &["a", "b"], &pk_values)
                .expect("must succeed");

            assert!(
                result.sql.contains("IN"),
                "Postgres composite PK must use row-constructor IN; SQL: {}",
                result.sql
            );
            assert!(
                !result.sql.contains(" OR "),
                "Postgres must NOT use OR-of-ANDs for composite PK; SQL: {}",
                result.sql
            );
        }

        // DR-13.x: SQLite composite PK keeps row-constructor syntax.
        #[test]
        fn composite_pk_in_clause_sqlite_uses_row_constructor() {
            // SQLite default dialect uses QuestionMark and double-quoted identifiers —
            // same as DefaultSqlDialect which has supports_row_constructor_in = true.
            let generator = SqlMutationGenerator::new(&DIALECT);
            let spec = VisualMutationSpec {
                from: table_ref("events"),
                filter: None,
                kind: MutationKind::Delete,
            };
            let pk_values = pk_values_composite(&[(3, 30), (4, 40)]);
            let result = generator
                .generate_delete_chunk_from_spec(&spec, &["a", "b"], &pk_values)
                .expect("must succeed");

            assert!(
                result.sql.contains("IN"),
                "SQLite composite PK must use row-constructor IN; SQL: {}",
                result.sql
            );
            assert!(
                !result.sql.contains(" OR "),
                "SQLite must NOT use OR-of-ANDs; SQL: {}",
                result.sql
            );
        }

        // DR-13.x: MySQL composite PK keeps row-constructor syntax.
        #[test]
        fn composite_pk_in_clause_mysql_uses_row_constructor() {
            let generator = SqlMutationGenerator::new(&MYSQL);
            let spec = VisualMutationSpec {
                from: table_ref("sales"),
                filter: None,
                kind: MutationKind::Delete,
            };
            let pk_values = pk_values_composite(&[(5, 50)]);
            let result = generator
                .generate_delete_chunk_from_spec(&spec, &["a", "b"], &pk_values)
                .expect("must succeed");

            assert!(
                result.sql.contains("IN"),
                "MySQL composite PK must use row-constructor IN; SQL: {}",
                result.sql
            );
            assert!(
                !result.sql.contains(" OR "),
                "MySQL must NOT use OR-of-ANDs; SQL: {}",
                result.sql
            );
        }

        // F-R2-4: single_tx DELETE must use qualified_table (include schema when present).
        #[test]
        fn single_tx_delete_includes_schema_when_present() {
            let generator = SqlMutationGenerator::new(&PG);
            let spec = VisualMutationSpec {
                from: crate::query::table_browser::TableRef {
                    schema: Some("public".to_string()),
                    name: "orders".to_string(),
                },
                filter: None,
                kind: MutationKind::Delete,
            };
            let result = generator
                .generate_delete_from_spec(&spec)
                .expect("must succeed");
            assert!(
                result.sql.contains("\"public\".\"orders\""),
                "single-tx DELETE must use qualified table; SQL: {}",
                result.sql
            );
        }

        // F-R2-4: single_tx UPDATE must use qualified_table (include schema when present).
        #[test]
        fn single_tx_update_includes_schema_when_present() {
            let generator = SqlMutationGenerator::new(&PG);
            let spec = VisualMutationSpec {
                from: crate::query::table_browser::TableRef {
                    schema: Some("public".to_string()),
                    name: "orders".to_string(),
                },
                filter: None,
                kind: MutationKind::Update {
                    assignments: vec![Assignment {
                        column: "status".to_string(),
                        value: AssignmentValue::Literal(ScalarLiteral::Text("done".to_string())),
                    }],
                },
            };
            let result = generator
                .generate_update_from_spec(&spec)
                .expect("must succeed");
            assert!(
                result.sql.contains("\"public\".\"orders\""),
                "single-tx UPDATE must use qualified table; SQL: {}",
                result.sql
            );
        }
    }

    // -------------------------------------------------------------------------
    // Grouped ORDER BY qualification tests (fix #1)
    // -------------------------------------------------------------------------

    #[test]
    fn grouped_order_by_qualifies_group_column_with_source_alias() {
        use crate::query::visual_query::{AggFn, AggregateSpec, GroupByEntry};

        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.projection = Projection::Explicit(vec![]);
        spec.group_by = vec![GroupByEntry {
            source_alias: "users".to_string(),
            column: "country".to_string(),
        }];
        spec.aggregates = vec![AggregateSpec {
            function: AggFn::Sum,
            source_alias: Some("users".to_string()),
            column: Some("amount".to_string()),
            alias: "total".to_string(),
        }];
        spec.sort = vec![SortEntry {
            source_alias: "users".to_string(),
            column: "country".to_string(),
            direction: VSort::Asc,
        }];

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");

        assert!(
            q.sql.contains("\"users\".\"country\" ASC"),
            "group-by ORDER BY column must be qualified, got: {}",
            q.sql
        );
    }

    #[test]
    fn grouped_order_by_aggregate_alias_stays_unqualified() {
        use crate::query::visual_query::{AggFn, AggregateSpec, GroupByEntry};

        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.projection = Projection::Explicit(vec![]);
        spec.group_by = vec![GroupByEntry {
            source_alias: "users".to_string(),
            column: "country".to_string(),
        }];
        spec.aggregates = vec![AggregateSpec {
            function: AggFn::Sum,
            source_alias: Some("users".to_string()),
            column: Some("amount".to_string()),
            alias: "total".to_string(),
        }];
        spec.sort = vec![SortEntry {
            source_alias: "users".to_string(),
            column: "total".to_string(),
            direction: VSort::Desc,
        }];

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");

        assert!(
            q.sql.contains("\"total\" DESC"),
            "aggregate alias ORDER BY must be unqualified, got: {}",
            q.sql
        );
        assert!(
            !q.sql.contains("\"users\".\"total\""),
            "aggregate alias must not be table-qualified, got: {}",
            q.sql
        );
    }

    #[test]
    fn grouped_order_by_drops_sort_with_wrong_source_alias() {
        use crate::query::visual_query::{AggFn, AggregateSpec, GroupByEntry};

        let generator = SqlMutationGenerator::new(&DIALECT);
        let mut spec = users_spec();
        spec.projection = Projection::Explicit(vec![]);
        spec.group_by = vec![GroupByEntry {
            source_alias: "users".to_string(),
            column: "country".to_string(),
        }];
        spec.aggregates = vec![AggregateSpec {
            function: AggFn::CountStar,
            source_alias: None,
            column: None,
            alias: "cnt".to_string(),
        }];
        spec.sort = vec![SortEntry {
            source_alias: "orders".to_string(),
            column: "country".to_string(),
            direction: VSort::Asc,
        }];

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");

        assert!(
            !q.sql.contains("ORDER BY"),
            "sort with mismatched source_alias must be dropped, got: {}",
            q.sql
        );
    }

    // -------------------------------------------------------------------------
    // Count subquery alias quoting test (fix #3)
    // -------------------------------------------------------------------------

    #[test]
    fn grouped_count_subquery_alias_is_quoted() {
        use super::build_grouped_count_query;
        use crate::query::visual_query::{AggFn, AggregateSpec, GroupByEntry};

        let mut spec = users_spec();
        spec.projection = Projection::Explicit(vec![]);
        spec.group_by = vec![GroupByEntry {
            source_alias: "users".to_string(),
            column: "country".to_string(),
        }];
        spec.aggregates = vec![AggregateSpec {
            function: AggFn::CountStar,
            source_alias: None,
            column: None,
            alias: "cnt".to_string(),
        }];

        let q = build_grouped_count_query(&spec, &DIALECT).expect("must succeed");
        assert!(
            q.sql.contains("\"_dbflux_count_subq\""),
            "subquery alias must be dialect-quoted, got: {}",
            q.sql
        );
    }

    // -------------------------------------------------------------------------
    // MSSQL HAVING aggregate expression expansion tests (fix #9)
    // -------------------------------------------------------------------------

    #[test]
    fn mssql_having_expands_aggregate_alias_to_full_expression() {
        use crate::query::visual_query::{
            AggFn, AggregateSpec, BoolOp, Comparator, FilterNode, GroupByEntry, LiteralValue,
            Predicate, PredicateValue,
        };
        use crate::sql::dialect::{PlaceholderStyle, SqlDialect};

        struct MssqlHavingDialect;
        impl SqlDialect for MssqlHavingDialect {
            fn quote_identifier(&self, name: &str) -> String {
                format!("[{}]", name.replace(']', "]]"))
            }
            fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
                match schema {
                    Some(s) => format!(
                        "{}.{}",
                        self.quote_identifier(s),
                        self.quote_identifier(table)
                    ),
                    None => self.quote_identifier(table),
                }
            }
            fn value_to_literal(&self, value: &Value) -> String {
                DIALECT.value_to_literal(value)
            }
            fn escape_string(&self, s: &str) -> String {
                s.replace('\'', "''")
            }
            fn placeholder_style(&self) -> PlaceholderStyle {
                PlaceholderStyle::QuestionMark
            }
            fn having_repeats_aggregate_expressions(&self) -> bool {
                true
            }
        }

        static MSSQL_HAVING: MssqlHavingDialect = MssqlHavingDialect;
        let generator = SqlMutationGenerator::new(&MSSQL_HAVING);

        let mut spec = users_spec();
        spec.projection = Projection::Explicit(vec![]);
        spec.group_by = vec![GroupByEntry {
            source_alias: "users".to_string(),
            column: "country".to_string(),
        }];
        spec.aggregates = vec![AggregateSpec {
            function: AggFn::Sum,
            source_alias: Some("users".to_string()),
            column: Some("amount".to_string()),
            alias: "total".to_string(),
        }];
        spec.having = Some(FilterNode::Group {
            op: BoolOp::And,
            children: vec![FilterNode::Predicate(Predicate {
                source_alias: "".to_string(),
                column: "total".to_string(),
                comparator: Comparator::Gt,
                value: PredicateValue::Single(LiteralValue::Integer(100)),
                node_id: 1,
            })],
        });

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");

        assert!(
            q.sql.contains("SUM([users].[amount])"),
            "MSSQL HAVING must repeat the aggregate expression, got: {}",
            q.sql
        );
        assert!(
            !q.sql.contains("HAVING [total]"),
            "MSSQL HAVING must not use the alias, got: {}",
            q.sql
        );
    }

    #[test]
    fn non_mssql_having_uses_alias() {
        use crate::query::visual_query::{
            AggFn, AggregateSpec, BoolOp, Comparator, FilterNode, GroupByEntry, LiteralValue,
            Predicate, PredicateValue,
        };

        let generator = SqlMutationGenerator::new(&DIALECT);

        let mut spec = users_spec();
        spec.projection = Projection::Explicit(vec![]);
        spec.group_by = vec![GroupByEntry {
            source_alias: "users".to_string(),
            column: "country".to_string(),
        }];
        spec.aggregates = vec![AggregateSpec {
            function: AggFn::Sum,
            source_alias: Some("users".to_string()),
            column: Some("amount".to_string()),
            alias: "total".to_string(),
        }];
        spec.having = Some(FilterNode::Group {
            op: BoolOp::And,
            children: vec![FilterNode::Predicate(Predicate {
                source_alias: "".to_string(),
                column: "total".to_string(),
                comparator: Comparator::Gt,
                value: PredicateValue::Single(LiteralValue::Integer(100)),
                node_id: 1,
            })],
        });

        let q = generator
            .generate_select(&spec)
            .expect("must succeed")
            .expect("must be Some");

        assert!(
            q.sql.contains("HAVING \"total\""),
            "non-MSSQL HAVING must use the alias, got: {}",
            q.sql
        );
    }
}
