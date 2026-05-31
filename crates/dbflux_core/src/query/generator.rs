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
        use crate::sql::dialect::PlaceholderStyle;

        let style = dialect.placeholder_style();

        match style {
            PlaceholderStyle::QuestionMark | PlaceholderStyle::NamedColon => {
                let mut result = String::with_capacity(self.sql.len());
                let mut param_iter = self.params.iter();
                let chars: Vec<char> = self.sql.chars().collect();
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
                materialize_numbered_placeholders(&self.sql, &self.params, dialect, '$', "")
            }

            PlaceholderStyle::AtSign => {
                materialize_numbered_placeholders(&self.sql, &self.params, dialect, '@', "p")
            }
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
}

// =============================================================================
// SQL SELECT builder for VisualQuerySpec
// =============================================================================

struct SqlSelectBuilder<'a> {
    dialect: &'a dyn SqlDialect,
}

impl<'a> SqlSelectBuilder<'a> {
    fn new(dialect: &'a dyn SqlDialect) -> Self {
        Self { dialect }
    }

    fn build(&self, spec: &VisualQuerySpec) -> Result<SelectQuery, QueryGenError> {
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
            parts.push(format!("WHERE {}", w));
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
                    JoinOn::Conditions { predicates, .. } => predicates
                        .iter()
                        .any(|p| !p.left.trim().is_empty() && !p.right.trim().is_empty()),
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
                    JoinOn::Conditions { op, predicates } => {
                        self.render_join_conditions(*op, predicates)
                    }
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
    fn render_join_conditions(
        &self,
        op: crate::query::visual_query::BoolOp,
        preds: &[crate::query::visual_query::JoinPredicate],
    ) -> String {
        use crate::query::visual_query::{BoolOp, Comparator};

        let parts: Vec<String> = preds
            .iter()
            .filter(|p| !p.left.trim().is_empty() && !p.right.trim().is_empty())
            .map(|p| {
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
            })
            .collect();

        let sep = match op {
            BoolOp::And => " AND ",
            BoolOp::Or => " OR ",
        };

        // Parenthesise OR groups so the connector binds correctly when
        // multiple joins are emitted in the same FROM clause.
        if parts.len() > 1 && matches!(op, BoolOp::Or) {
            format!("({})", parts.join(sep))
        } else {
            parts.join(sep)
        }
    }

    fn build_where(
        &self,
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
                    Ok(Some(expr))
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
                if pred.column.trim().is_empty() || pred.source_alias.trim().is_empty() {
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

        let col = format!(
            "{}.{}",
            self.dialect.quote_identifier(&pred.source_alias),
            self.dialect.quote_identifier(&pred.column),
        );

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
        BoolOp, Comparator, FilterNode, JoinKind, JoinOn, JoinStep, LiteralValue, Predicate,
        PredicateValue, ProjectedColumn, Projection, SortDirection as VSort, SortEntry,
        SourceTable, VisualQuerySpec,
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
}
