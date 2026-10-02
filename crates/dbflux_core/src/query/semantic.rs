use serde::{Deserialize, Serialize};

use crate::{
    CollectionBrowseRequest, CollectionCountRequest, ColumnRef, DbError, DescribeRequest,
    ExplainRequest, MutationRequest, OrderByColumn, QueryRequest, SqlDialect, TableBrowseRequest,
    TableCountRequest, TableRef, Value,
    driver::capabilities::{QueryLanguage, WhereOperator},
    query::visual_query::{
        BoolOp, Comparator, FilterNode, LiteralValue, Predicate, PredicateValue,
    },
};

/// Typed reference to a field used in semantic filters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SemanticFieldRef {
    Column(ColumnRef),
    Path(Vec<String>),
}

impl SemanticFieldRef {
    pub fn column(column: ColumnRef) -> Self {
        Self::Column(column)
    }

    pub fn named(name: impl Into<String>) -> Self {
        Self::Column(ColumnRef::new(name))
    }

    pub fn path(segments: Vec<String>) -> Self {
        Self::Path(segments)
    }
}

impl From<ColumnRef> for SemanticFieldRef {
    fn from(value: ColumnRef) -> Self {
        Self::Column(value)
    }
}

impl From<&str> for SemanticFieldRef {
    fn from(value: &str) -> Self {
        Self::named(value)
    }
}

/// A single typed predicate inside a semantic filter tree.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SemanticPredicate {
    pub field: SemanticFieldRef,
    pub operator: WhereOperator,
    pub value: Option<Value>,
}

impl SemanticPredicate {
    pub fn new(field: impl Into<SemanticFieldRef>, operator: WhereOperator, value: Value) -> Self {
        Self {
            field: field.into(),
            operator,
            value: Some(value),
        }
    }

    pub fn null(field: impl Into<SemanticFieldRef>) -> Self {
        Self {
            field: field.into(),
            operator: WhereOperator::Null,
            value: None,
        }
    }
}

/// Driver-neutral filter AST for semantic browse/count requests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SemanticFilter {
    Predicate(SemanticPredicate),
    And(Vec<SemanticFilter>),
    Or(Vec<SemanticFilter>),
    Not(Box<SemanticFilter>),
}

impl SemanticFilter {
    pub fn predicate(predicate: SemanticPredicate) -> Self {
        Self::Predicate(predicate)
    }

    pub fn compare(
        field: impl Into<SemanticFieldRef>,
        operator: WhereOperator,
        value: Value,
    ) -> Self {
        Self::Predicate(SemanticPredicate::new(field, operator, value))
    }

    pub fn null(field: impl Into<SemanticFieldRef>) -> Self {
        Self::Predicate(SemanticPredicate::null(field))
    }

    pub fn and(filters: Vec<SemanticFilter>) -> Self {
        Self::And(filters)
    }

    pub fn or(filters: Vec<SemanticFilter>) -> Self {
        Self::Or(filters)
    }

    pub fn negate(filter: SemanticFilter) -> Self {
        Self::Not(Box::new(filter))
    }

    /// Returns every field the filter tests, in tree order. A field tested
    /// more than once appears once per predicate.
    pub fn field_refs(&self) -> Vec<&SemanticFieldRef> {
        match self {
            Self::Predicate(predicate) => vec![&predicate.field],
            Self::And(filters) | Self::Or(filters) => {
                filters.iter().flat_map(Self::field_refs).collect()
            }
            Self::Not(filter) => filter.field_refs(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SemanticRequestKind {
    TableBrowse,
    TableCount,
    Aggregate,
    CollectionBrowse,
    CollectionCount,
    Explain,
    Describe,
    Mutation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateFunction {
    pub fn as_sql(self) -> &'static str {
        match self {
            Self::Count => "COUNT",
            Self::Sum => "SUM",
            Self::Avg => "AVG",
            Self::Min => "MIN",
            Self::Max => "MAX",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateSpec {
    pub function: AggregateFunction,
    pub column: Option<ColumnRef>,
    pub alias: String,
}

impl AggregateSpec {
    pub fn new(
        function: AggregateFunction,
        column: Option<ColumnRef>,
        alias: impl Into<String>,
    ) -> Self {
        Self {
            function,
            column,
            alias: alias.into(),
        }
    }

    pub fn count_all(alias: impl Into<String>) -> Self {
        Self::new(AggregateFunction::Count, None, alias)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateRequest {
    pub table: TableRef,
    pub filter: Option<SemanticFilter>,
    pub group_by: Vec<ColumnRef>,
    pub aggregations: Vec<AggregateSpec>,
    pub having: Option<SemanticFilter>,
    pub order_by: Vec<OrderByColumn>,
    pub limit: Option<u32>,
    pub target_database: Option<String>,
}

impl AggregateRequest {
    pub fn new(table: TableRef) -> Self {
        Self {
            table,
            filter: None,
            group_by: Vec::new(),
            aggregations: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            target_database: None,
        }
    }

    pub fn with_filter(mut self, filter: SemanticFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_group_by(mut self, group_by: Vec<ColumnRef>) -> Self {
        self.group_by = group_by;
        self
    }

    pub fn with_aggregations(mut self, aggregations: Vec<AggregateSpec>) -> Self {
        self.aggregations = aggregations;
        self
    }

    pub fn with_having(mut self, having: SemanticFilter) -> Self {
        self.having = Some(having);
        self
    }

    pub fn with_order_by(mut self, order_by: Vec<OrderByColumn>) -> Self {
        self.order_by = order_by;
        self
    }

    pub fn with_limit(mut self, limit: Option<u32>) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_target_database(mut self, database: Option<String>) -> Self {
        self.target_database = database;
        self
    }

    pub fn build_sql_with(&self, dialect: &dyn SqlDialect) -> Result<String, DbError> {
        if self.aggregations.is_empty() {
            return Err(DbError::query_failed(
                "Aggregate request requires at least one aggregation",
            ));
        }

        let mut select_clauses = self
            .group_by
            .iter()
            .map(|column| column.quoted_with(dialect))
            .collect::<Vec<_>>();

        select_clauses.extend(self.aggregations.iter().map(|aggregation| {
            let column = aggregation
                .column
                .as_ref()
                .map(|column| column.quoted_with(dialect))
                .unwrap_or_else(|| "*".to_string());

            format!(
                "{}({}) AS {}",
                aggregation.function.as_sql(),
                column,
                dialect.quote_identifier(&aggregation.alias)
            )
        }));

        let mut sql = format!(
            "SELECT {} FROM {}",
            select_clauses.join(", "),
            self.table.quoted_with(dialect)
        );

        if let Some(filter) = self.filter.as_ref() {
            let where_clause = render_semantic_filter_sql(filter, dialect)?;
            sql.push_str(" WHERE ");
            sql.push_str(&where_clause);
        }

        if !self.group_by.is_empty() {
            let group_by = self
                .group_by
                .iter()
                .map(|column| column.quoted_with(dialect))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(" GROUP BY ");
            sql.push_str(&group_by);
        }

        if let Some(having) = self.having.as_ref() {
            let having_clause = render_semantic_filter_sql(having, dialect)?;
            sql.push_str(" HAVING ");
            sql.push_str(&having_clause);
        }

        if !self.order_by.is_empty() {
            let order_by = self
                .order_by
                .iter()
                .map(|column| {
                    let direction = match column.direction {
                        crate::SortDirection::Ascending => "ASC",
                        crate::SortDirection::Descending => "DESC",
                    };

                    format!("{} {}", column.column.quoted_with(dialect), direction)
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(" ORDER BY ");
            sql.push_str(&order_by);
        }

        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        Ok(sql)
    }
}

/// Driver-neutral request envelope for driver-owned planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SemanticRequest {
    TableBrowse(TableBrowseRequest),
    TableCount(TableCountRequest),
    Aggregate(AggregateRequest),
    CollectionBrowse(CollectionBrowseRequest),
    CollectionCount(CollectionCountRequest),
    Explain(ExplainRequest),
    Describe(DescribeRequest),
    Mutation(MutationRequest),
}

impl SemanticRequest {
    pub fn kind(&self) -> SemanticRequestKind {
        match self {
            Self::TableBrowse(_) => SemanticRequestKind::TableBrowse,
            Self::TableCount(_) => SemanticRequestKind::TableCount,
            Self::Aggregate(_) => SemanticRequestKind::Aggregate,
            Self::CollectionBrowse(_) => SemanticRequestKind::CollectionBrowse,
            Self::CollectionCount(_) => SemanticRequestKind::CollectionCount,
            Self::Explain(_) => SemanticRequestKind::Explain,
            Self::Describe(_) => SemanticRequestKind::Describe,
            Self::Mutation(_) => SemanticRequestKind::Mutation,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SemanticPlanKind {
    Query,
    MutationPreview,
}

/// Planned native query/command text emitted by a driver.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedQuery {
    pub language: QueryLanguage,
    pub text: String,
    pub target_database: Option<String>,
}

impl PlannedQuery {
    pub fn new(language: QueryLanguage, text: impl Into<String>) -> Self {
        Self {
            language,
            text: text.into(),
            target_database: None,
        }
    }

    pub fn with_database(mut self, database: Option<String>) -> Self {
        self.target_database = database;
        self
    }

    pub fn into_query_request(self) -> QueryRequest {
        QueryRequest::new(self.text).with_database(self.target_database)
    }
}

/// Planning result for a semantic request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticPlan {
    pub kind: SemanticPlanKind,
    pub queries: Vec<PlannedQuery>,
    pub warnings: Vec<String>,
}

impl SemanticPlan {
    pub fn new(kind: SemanticPlanKind) -> Self {
        Self {
            kind,
            queries: Vec::new(),
            warnings: Vec::new(),
        }
    }

    pub fn single_query(kind: SemanticPlanKind, query: PlannedQuery) -> Self {
        Self {
            kind,
            queries: vec![query],
            warnings: Vec::new(),
        }
    }

    pub fn with_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }

    pub fn primary_query(&self) -> Option<&PlannedQuery> {
        self.queries.first()
    }
}

/// Optional driver-owned planner for semantic requests.
pub trait SemanticPlanner: Send + Sync {
    fn supported_requests(&self) -> &'static [SemanticRequestKind];

    fn plan(&self, request: &SemanticRequest) -> Option<SemanticPlan>;
}

pub fn parse_semantic_filter_json(
    filter: &serde_json::Value,
) -> Result<Option<SemanticFilter>, String> {
    match filter {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Object(map) => {
            if map.is_empty() {
                return Ok(None);
            }

            let mut filters = Vec::new();

            for (key, value) in map {
                filters.push(parse_filter_entry(key, value)?);
            }

            Ok(collapse_filter_list(filters))
        }
        serde_json::Value::String(text) => {
            if text.trim().starts_with('{') {
                let parsed = serde_json::from_str::<serde_json::Value>(text).map_err(|_| {
                    format!(
                        "Filter must be a JSON object. Received a string that looks like JSON but failed to parse: {}",
                        text
                    )
                })?;

                parse_semantic_filter_json(&parsed)
            } else {
                Err(format!(
                    "Filter must be a JSON object, not a string. Received: {:?}. If you intended to pass a JSON object, do not wrap it in quotes.",
                    text
                ))
            }
        }
        serde_json::Value::Array(_) => Err(
            "Filter must be a JSON object, not an array. Use an object with column conditions instead. Example: {\"column_name\": \"value\"} or {\"id\": {\"$gt\": 10}}".to_string(),
        ),
        serde_json::Value::Bool(value) => Err(format!(
            "Filter must be a JSON object, not a boolean. Received: {}. Use an object with column conditions instead. Example: {{\"column_name\": \"value\"}}",
            value
        )),
        serde_json::Value::Number(value) => Err(format!(
            "Filter must be a JSON object, not a number. Received: {}. Use an object with column conditions instead. Example: {{\"column_name\": \"value\"}}",
            value
        )),
    }
}

pub fn render_semantic_filter_sql(
    filter: &SemanticFilter,
    dialect: &dyn SqlDialect,
) -> Result<String, DbError> {
    match filter {
        SemanticFilter::Predicate(predicate) => render_sql_predicate(predicate, dialect),
        SemanticFilter::And(filters) => render_filter_group(filters, "AND", dialect),
        SemanticFilter::Or(filters) => render_filter_group(filters, "OR", dialect),
        SemanticFilter::Not(filter) => {
            let rendered = render_semantic_filter_sql(filter, dialect)?;
            Ok(format!("NOT ({})", rendered))
        }
    }
}

/// Maps a column reference to the `(table alias, column)` pair of a filter
/// predicate, or says why the reference is not valid.
pub type ColumnResolver<'a> = dyn Fn(&ColumnRef) -> Result<(String, String), String> + 'a;

/// Converts a semantic filter into the visual-query filter tree, which the
/// structured SELECT generator renders with quoted identifiers and bound
/// literals.
///
/// `resolve_column` maps each column reference to the `(table alias, column)`
/// pair the generator quotes. The conversion is exact: an operator, field
/// shape or value the filter tree cannot express is an error that names it,
/// never an approximation. That includes a number at or beyond 2^63 in
/// magnitude, which JSON parsing has already turned into a possibly rounded
/// float.
pub fn semantic_filter_to_filter_node(
    filter: &SemanticFilter,
    resolve_column: &ColumnResolver<'_>,
) -> Result<FilterNode, String> {
    match filter {
        SemanticFilter::Predicate(predicate) => {
            predicate_to_filter_node(predicate, false, resolve_column)
        }
        SemanticFilter::And(filters) => filter_group(BoolOp::And, filters, resolve_column),
        SemanticFilter::Or(filters) => filter_group(BoolOp::Or, filters, resolve_column),
        SemanticFilter::Not(inner) => match inner.as_ref() {
            SemanticFilter::Predicate(predicate) if is_null_test(predicate) => {
                predicate_to_filter_node(predicate, true, resolve_column)
            }
            _ => Err(
                "$not is supported here only around a null test (use $ne or $exists instead)"
                    .to_string(),
            ),
        },
    }
}

fn filter_group(
    op: BoolOp,
    filters: &[SemanticFilter],
    resolve_column: &ColumnResolver<'_>,
) -> Result<FilterNode, String> {
    if filters.is_empty() {
        return Err("a filter group cannot be empty".to_string());
    }

    let children = filters
        .iter()
        .map(|filter| semantic_filter_to_filter_node(filter, resolve_column))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(FilterNode::Group { op, children })
}

fn is_null_test(predicate: &SemanticPredicate) -> bool {
    match predicate.operator {
        WhereOperator::Null => true,
        WhereOperator::Eq => matches!(predicate.value, None | Some(Value::Null)),
        _ => false,
    }
}

fn predicate_to_filter_node(
    predicate: &SemanticPredicate,
    negated_null_test: bool,
    resolve_column: &ColumnResolver<'_>,
) -> Result<FilterNode, String> {
    let column = match &predicate.field {
        SemanticFieldRef::Column(column) => column,
        SemanticFieldRef::Path(segments) => {
            return Err(format!(
                "nested path field '{}' is not supported here",
                segments.join(".")
            ));
        }
    };

    let (source_alias, column) = resolve_column(column)?;
    let is_null_value = matches!(predicate.value, None | Some(Value::Null));

    let (comparator, value) = match predicate.operator {
        _ if negated_null_test => (Comparator::IsNotNull, PredicateValue::None),
        WhereOperator::Null => (Comparator::IsNull, PredicateValue::None),
        WhereOperator::Eq if is_null_value => (Comparator::IsNull, PredicateValue::None),
        WhereOperator::Ne if is_null_value => (Comparator::IsNotNull, PredicateValue::None),
        WhereOperator::Eq => (Comparator::Eq, single_literal(predicate)?),
        WhereOperator::Ne => (Comparator::Neq, single_literal(predicate)?),
        WhereOperator::Gt => (Comparator::Gt, single_literal(predicate)?),
        WhereOperator::Gte => (Comparator::Gte, single_literal(predicate)?),
        WhereOperator::Lt => (Comparator::Lt, single_literal(predicate)?),
        WhereOperator::Lte => (Comparator::Lte, single_literal(predicate)?),
        WhereOperator::Like => (Comparator::Like, single_literal(predicate)?),
        WhereOperator::ILike => (Comparator::ILike, single_literal(predicate)?),
        WhereOperator::In => (Comparator::In, literal_list(predicate)?),
        unsupported => {
            return Err(format!(
                "operator {} is not supported here",
                where_operator_key(unsupported)
            ));
        }
    };

    Ok(FilterNode::Predicate(Predicate {
        source_alias,
        column,
        comparator,
        value,
        node_id: 0,
    }))
}

fn single_literal(predicate: &SemanticPredicate) -> Result<PredicateValue, String> {
    let value = predicate.value.as_ref().unwrap_or(&Value::Null);

    literal_from_value(value).map(PredicateValue::Single)
}

fn literal_list(predicate: &SemanticPredicate) -> Result<PredicateValue, String> {
    let Some(Value::Array(items)) = predicate.value.as_ref() else {
        return Err("$in requires an array of values".to_string());
    };

    if items.is_empty() {
        return Err("$in requires at least one value".to_string());
    }

    items
        .iter()
        .map(literal_from_value)
        .collect::<Result<Vec<_>, _>>()
        .map(PredicateValue::List)
}

/// Magnitude from which a JSON number has no exact `i64` and was parsed as a
/// float, possibly rounded: 2^63.
const OUT_OF_INTEGER_RANGE: f64 = 9_223_372_036_854_775_808.0;

fn literal_from_value(value: &Value) -> Result<LiteralValue, String> {
    match value {
        Value::Null => Ok(LiteralValue::Null),
        Value::Bool(value) => Ok(LiteralValue::Bool(*value)),
        Value::Int(value) => Ok(LiteralValue::Integer(*value)),
        Value::Float(value) if *value >= OUT_OF_INTEGER_RANGE || *value < -OUT_OF_INTEGER_RANGE => {
            Err(format!(
                "the number {value} is outside the 64-bit integer range and cannot be compared \
                 exactly here"
            ))
        }
        Value::Float(value) => Ok(LiteralValue::Float(*value)),
        Value::Text(value) => Ok(LiteralValue::Text(value.clone())),
        Value::Array(_) => Err("an array value is not supported here".to_string()),
        Value::Document(_) => Err("an object value is not supported here".to_string()),
        _ => Err("only text, number, boolean and null values are supported here".to_string()),
    }
}

/// The JSON key a filter uses for `operator`, for error messages.
fn where_operator_key(operator: WhereOperator) -> &'static str {
    match operator {
        WhereOperator::Eq => "$eq",
        WhereOperator::Ne => "$ne",
        WhereOperator::Gt => "$gt",
        WhereOperator::Gte => "$gte",
        WhereOperator::Lt => "$lt",
        WhereOperator::Lte => "$lte",
        WhereOperator::Like => "$like",
        WhereOperator::ILike => "$ilike",
        WhereOperator::Regex => "$regex",
        WhereOperator::Null => "null",
        WhereOperator::In => "$in",
        WhereOperator::NotIn => "$nin",
        WhereOperator::Contains => "$contains",
        WhereOperator::Overlap => "$overlap",
        WhereOperator::ContainsAll => "$all",
        WhereOperator::ContainsAny => "contains-any",
        WhereOperator::Size => "$size",
        WhereOperator::And => "$and",
        WhereOperator::Or => "$or",
        WhereOperator::Not => "$not",
    }
}

fn collapse_filter_list(filters: Vec<SemanticFilter>) -> Option<SemanticFilter> {
    match filters.len() {
        0 => None,
        1 => filters.into_iter().next(),
        _ => Some(SemanticFilter::and(filters)),
    }
}

fn parse_filter_entry(key: &str, value: &serde_json::Value) -> Result<SemanticFilter, String> {
    match key {
        "$and" => parse_logical_list("$and", value, SemanticFilter::and),
        "$or" => parse_logical_list("$or", value, SemanticFilter::or),
        "$not" => {
            let nested = parse_required_filter(value)?;
            Ok(SemanticFilter::negate(nested))
        }
        _ => parse_field_filter(key, value),
    }
}

fn parse_logical_list(
    operator: &str,
    value: &serde_json::Value,
    build: fn(Vec<SemanticFilter>) -> SemanticFilter,
) -> Result<SemanticFilter, String> {
    let values = value
        .as_array()
        .ok_or_else(|| format!("{} requires an array of filter objects", operator))?;

    if values.is_empty() {
        return Err(format!("{} requires at least one filter object", operator));
    }

    let mut filters = Vec::with_capacity(values.len());

    for item in values {
        filters.push(parse_required_filter(item)?);
    }

    Ok(build(filters))
}

fn parse_required_filter(value: &serde_json::Value) -> Result<SemanticFilter, String> {
    parse_semantic_filter_json(value)?.ok_or_else(|| "Filter object cannot be empty".to_string())
}

fn parse_field_filter(key: &str, value: &serde_json::Value) -> Result<SemanticFilter, String> {
    let field = parse_field_ref(key);

    match value {
        serde_json::Value::Object(map)
            if !map.is_empty() && map.keys().all(|operator| operator.starts_with('$')) =>
        {
            parse_operator_map(field, key, map)
        }
        serde_json::Value::Null => Ok(SemanticFilter::null(field)),
        _ => Ok(SemanticFilter::compare(
            field,
            WhereOperator::Eq,
            json_to_value(value.clone()),
        )),
    }
}

fn parse_operator_map(
    field: SemanticFieldRef,
    key: &str,
    map: &serde_json::Map<String, serde_json::Value>,
) -> Result<SemanticFilter, String> {
    let mut filters = Vec::new();

    for (operator, value) in map {
        if operator == "$options" {
            continue;
        }

        let filter = match operator.as_str() {
            "$eq" => match value {
                serde_json::Value::Null => SemanticFilter::null(field.clone()),
                _ => SemanticFilter::compare(
                    field.clone(),
                    WhereOperator::Eq,
                    json_to_value(value.clone()),
                ),
            },
            "$ne" => match value {
                serde_json::Value::Null => {
                    SemanticFilter::negate(SemanticFilter::null(field.clone()))
                }
                _ => SemanticFilter::compare(
                    field.clone(),
                    WhereOperator::Ne,
                    json_to_value(value.clone()),
                ),
            },
            "$gt" => SemanticFilter::compare(
                field.clone(),
                WhereOperator::Gt,
                json_to_value(value.clone()),
            ),
            "$gte" => SemanticFilter::compare(
                field.clone(),
                WhereOperator::Gte,
                json_to_value(value.clone()),
            ),
            "$lt" => SemanticFilter::compare(
                field.clone(),
                WhereOperator::Lt,
                json_to_value(value.clone()),
            ),
            "$lte" => SemanticFilter::compare(
                field.clone(),
                WhereOperator::Lte,
                json_to_value(value.clone()),
            ),
            "$like" => SemanticFilter::compare(
                field.clone(),
                WhereOperator::Like,
                json_to_value(value.clone()),
            ),
            "$ilike" => SemanticFilter::compare(
                field.clone(),
                WhereOperator::ILike,
                json_to_value(value.clone()),
            ),
            "$regex" => SemanticFilter::compare(
                field.clone(),
                WhereOperator::Regex,
                json_to_value(value.clone()),
            ),
            "$in" => SemanticFilter::compare(
                field.clone(),
                WhereOperator::In,
                json_to_value(value.clone()),
            ),
            "$nin" => SemanticFilter::compare(
                field.clone(),
                WhereOperator::NotIn,
                json_to_value(value.clone()),
            ),
            "$contains" => SemanticFilter::compare(
                field.clone(),
                WhereOperator::Contains,
                json_to_value(value.clone()),
            ),
            "$overlap" => SemanticFilter::compare(
                field.clone(),
                WhereOperator::Overlap,
                json_to_value(value.clone()),
            ),
            "$all" => SemanticFilter::compare(
                field.clone(),
                WhereOperator::ContainsAll,
                json_to_value(value.clone()),
            ),
            "$size" => SemanticFilter::compare(
                field.clone(),
                WhereOperator::Size,
                json_to_value(value.clone()),
            ),
            "$exists" => parse_exists_filter(field.clone(), key, value)?,
            _ => return Err(format!("Unknown operator: {}", operator)),
        };

        filters.push(filter);
    }

    if let Some(options) = map.get("$options")
        && !map.contains_key("$regex")
    {
        return Err(format!(
            "$options requires $regex for field {} (received {:?})",
            key, options
        ));
    }

    collapse_filter_list(filters).ok_or_else(|| format!("Filter for field {} cannot be empty", key))
}

fn parse_exists_filter(
    field: SemanticFieldRef,
    key: &str,
    value: &serde_json::Value,
) -> Result<SemanticFilter, String> {
    match value.as_bool() {
        Some(true) => Ok(SemanticFilter::negate(SemanticFilter::null(field))),
        Some(false) => Ok(SemanticFilter::null(field)),
        None => Err(format!(
            "$exists requires a boolean value for field {}",
            key
        )),
    }
}

fn parse_field_ref(key: &str) -> SemanticFieldRef {
    let segments: Vec<String> = key.split('.').map(str::to_string).collect();

    if segments.len() > 2 {
        SemanticFieldRef::path(segments)
    } else {
        SemanticFieldRef::column(ColumnRef::from_qualified(key))
    }
}

fn json_to_value(value: serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(value) => Value::Bool(value),
        serde_json::Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                Value::Int(integer)
            } else if let Some(float) = value.as_f64() {
                Value::Float(float)
            } else {
                Value::Text(value.to_string())
            }
        }
        serde_json::Value::String(value) => Value::Text(value),
        serde_json::Value::Array(values) => {
            Value::Array(values.into_iter().map(json_to_value).collect())
        }
        serde_json::Value::Object(values) => Value::Document(
            values
                .into_iter()
                .map(|(key, value)| (key, json_to_value(value)))
                .collect(),
        ),
    }
}

fn render_filter_group(
    filters: &[SemanticFilter],
    joiner: &str,
    dialect: &dyn SqlDialect,
) -> Result<String, DbError> {
    let mut rendered = Vec::with_capacity(filters.len());

    for filter in filters {
        rendered.push(format!(
            "({})",
            render_semantic_filter_sql(filter, dialect)?
        ));
    }

    Ok(rendered.join(&format!(" {} ", joiner)))
}

fn render_sql_predicate(
    predicate: &SemanticPredicate,
    dialect: &dyn SqlDialect,
) -> Result<String, DbError> {
    let field = render_sql_field(&predicate.field, dialect)?;

    match predicate.operator {
        WhereOperator::Null => Ok(format!("{} IS NULL", field)),
        WhereOperator::Eq => match predicate.value.as_ref().unwrap_or(&Value::Null) {
            Value::Null => Ok(format!("{} IS NULL", field)),
            value => Ok(format!("{} = {}", field, dialect.value_to_literal(value))),
        },
        WhereOperator::Ne => match predicate.value.as_ref().unwrap_or(&Value::Null) {
            Value::Null => Ok(format!("{} IS NOT NULL", field)),
            value => Ok(format!("{} <> {}", field, dialect.value_to_literal(value))),
        },
        WhereOperator::Gt
        | WhereOperator::Gte
        | WhereOperator::Lt
        | WhereOperator::Lte
        | WhereOperator::Like
        | WhereOperator::ILike
        | WhereOperator::Regex
        | WhereOperator::Contains
        | WhereOperator::Overlap
        | WhereOperator::ContainsAll => {
            let value = require_predicate_value(predicate)?;
            Ok(format!(
                "{} {} {}",
                field,
                predicate.operator.sql_symbol(),
                dialect.value_to_literal(value)
            ))
        }
        WhereOperator::In | WhereOperator::NotIn => {
            let value = require_predicate_value(predicate)?;
            let Value::Array(items) = value else {
                return Err(DbError::NotSupported(format!(
                    "Semantic operator {:?} requires an array value",
                    predicate.operator
                )));
            };

            let literals = items
                .iter()
                .map(|item| dialect.value_to_literal(item))
                .collect::<Vec<_>>()
                .join(", ");

            Ok(format!(
                "{} {} ({})",
                field,
                predicate.operator.sql_symbol(),
                literals
            ))
        }
        WhereOperator::Size
        | WhereOperator::ContainsAny
        | WhereOperator::And
        | WhereOperator::Or
        | WhereOperator::Not => Err(DbError::NotSupported(format!(
            "Semantic SQL rendering does not support operator {:?}",
            predicate.operator
        ))),
    }
}

fn render_sql_field(field: &SemanticFieldRef, dialect: &dyn SqlDialect) -> Result<String, DbError> {
    match field {
        SemanticFieldRef::Column(column) => Ok(column.quoted_with(dialect)),
        SemanticFieldRef::Path(_) => Err(DbError::NotSupported(
            "Semantic SQL rendering does not support nested path fields".into(),
        )),
    }
}

fn require_predicate_value(predicate: &SemanticPredicate) -> Result<&Value, DbError> {
    predicate.value.as_ref().ok_or_else(|| {
        DbError::NotSupported(format!(
            "Semantic operator {:?} requires a value",
            predicate.operator
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        AggregateFunction, AggregateRequest, AggregateSpec, PlannedQuery, SemanticFieldRef,
        SemanticFilter, SemanticPlan, SemanticPlanKind, SemanticPredicate, SemanticRequest,
        SemanticRequestKind, parse_semantic_filter_json, render_semantic_filter_sql,
        semantic_filter_to_filter_node,
    };
    use crate::query::visual_query::{
        BoolOp, Comparator, FilterNode, LiteralValue, Predicate, PredicateValue,
    };
    use crate::{
        CollectionRef, ColumnRef, DefaultSqlDialect, OrderByColumn, QueryLanguage, QueryRequest,
        TableBrowseRequest, TableRef, Value, WhereOperator,
    };

    #[test]
    fn semantic_filter_builders_preserve_typed_values() {
        let filter = SemanticFilter::and(vec![
            SemanticFilter::compare("status", WhereOperator::Eq, Value::Text("active".into())),
            SemanticFilter::null(SemanticFieldRef::path(vec![
                "profile".into(),
                "deleted_at".into(),
            ])),
        ]);

        assert!(matches!(filter, SemanticFilter::And(children) if children.len() == 2));
    }

    #[test]
    fn field_refs_lists_every_tested_field_in_tree_order() {
        let filter = SemanticFilter::and(vec![
            SemanticFilter::compare("status", WhereOperator::Eq, Value::Text("active".into())),
            SemanticFilter::or(vec![
                SemanticFilter::null("deleted_at"),
                SemanticFilter::negate(SemanticFilter::null("email")),
            ]),
        ]);

        assert_eq!(
            filter.field_refs(),
            [
                &SemanticFieldRef::named("status"),
                &SemanticFieldRef::named("deleted_at"),
                &SemanticFieldRef::named("email"),
            ]
        );
    }

    fn to_filter_node(json: serde_json::Value) -> Result<FilterNode, String> {
        let filter = parse_semantic_filter_json(&json)
            .expect("the filter parses")
            .expect("the filter is not empty");

        semantic_filter_to_filter_node(&filter, &|column: &ColumnRef| {
            let alias = column.table.clone().unwrap_or_else(|| "users".to_string());

            Ok((alias, column.name.clone()))
        })
    }

    fn predicate(
        alias: &str,
        column: &str,
        comparator: Comparator,
        value: PredicateValue,
    ) -> FilterNode {
        FilterNode::Predicate(Predicate {
            source_alias: alias.to_string(),
            column: column.to_string(),
            comparator,
            value,
            node_id: 0,
        })
    }

    #[test]
    fn filter_node_conversion_rejects_integers_beyond_the_64_bit_range() {
        for json in [
            serde_json::json!({ "id": 9_223_372_036_854_775_808_u64 }),
            serde_json::json!({ "id": { "$in": [1, 18_446_744_073_709_551_615_u64] } }),
            serde_json::json!({ "id": { "$gt": -1.0e19 } }),
        ] {
            let error = to_filter_node(json.clone()).expect_err("the value is approximated");

            assert!(error.contains("64-bit integer range"), "{json}: {error}");
        }

        for (json, expected) in [
            (
                serde_json::json!({ "id": i64::MAX }),
                LiteralValue::Integer(i64::MAX),
            ),
            (
                serde_json::json!({ "id": i64::MIN }),
                LiteralValue::Integer(i64::MIN),
            ),
            (serde_json::json!({ "id": 1.5 }), LiteralValue::Float(1.5)),
        ] {
            assert_eq!(
                to_filter_node(json).expect("the value is exact"),
                predicate(
                    "users",
                    "id",
                    Comparator::Eq,
                    PredicateValue::Single(expected)
                )
            );
        }
    }

    #[test]
    fn filter_node_conversion_maps_comparisons_and_qualified_columns() {
        let node = to_filter_node(serde_json::json!({
            "status": "active",
            "orders.total": { "$gte": 10.5 }
        }))
        .expect("comparisons convert");

        let FilterNode::Group { op, mut children } = node else {
            panic!("two keys should convert to a group, got {node:?}");
        };

        // The children follow the JSON object's key order, which is sorted or
        // insertion order depending on whether another crate in the build
        // enables serde_json's `preserve_order`. The AND does not depend on it.
        children.sort_by_key(|child| match child {
            FilterNode::Predicate(predicate) => {
                (predicate.source_alias.clone(), predicate.column.clone())
            }
            other => panic!("expected only predicates, got {other:?}"),
        });

        assert_eq!(op, BoolOp::And);
        assert_eq!(
            children,
            vec![
                predicate(
                    "orders",
                    "total",
                    Comparator::Gte,
                    PredicateValue::Single(LiteralValue::Float(10.5)),
                ),
                predicate(
                    "users",
                    "status",
                    Comparator::Eq,
                    PredicateValue::Single(LiteralValue::Text("active".into())),
                ),
            ]
        );
    }

    #[test]
    fn filter_node_conversion_maps_every_scalar_operator() {
        let cases = [
            ("$eq", Comparator::Eq),
            ("$ne", Comparator::Neq),
            ("$gt", Comparator::Gt),
            ("$gte", Comparator::Gte),
            ("$lt", Comparator::Lt),
            ("$lte", Comparator::Lte),
            ("$like", Comparator::Like),
            ("$ilike", Comparator::ILike),
        ];

        for (operator, comparator) in cases {
            let node = to_filter_node(serde_json::json!({ "age": { operator: 7 } }))
                .unwrap_or_else(|error| panic!("{operator} should convert: {error}"));

            assert_eq!(
                node,
                predicate(
                    "users",
                    "age",
                    comparator,
                    PredicateValue::Single(LiteralValue::Integer(7)),
                ),
                "operator {operator}"
            );
        }
    }

    #[test]
    fn filter_node_conversion_maps_null_checks() {
        let is_null = predicate("users", "email", Comparator::IsNull, PredicateValue::None);
        let is_not_null = predicate(
            "users",
            "email",
            Comparator::IsNotNull,
            PredicateValue::None,
        );

        let null_forms = [
            serde_json::json!({ "email": null }),
            serde_json::json!({ "email": { "$eq": null } }),
            serde_json::json!({ "email": { "$exists": false } }),
        ];
        for form in null_forms {
            assert_eq!(to_filter_node(form.clone()), Ok(is_null.clone()), "{form}");
        }

        let not_null_forms = [
            serde_json::json!({ "email": { "$ne": null } }),
            serde_json::json!({ "email": { "$exists": true } }),
            serde_json::json!({ "$not": { "email": null } }),
        ];
        for form in not_null_forms {
            assert_eq!(
                to_filter_node(form.clone()),
                Ok(is_not_null.clone()),
                "{form}"
            );
        }
    }

    #[test]
    fn filter_node_conversion_maps_in_and_logic() {
        let node = to_filter_node(serde_json::json!({
            "$or": [
                { "id": { "$in": [1, 2] } },
                { "$and": [{ "active": true }, { "name": { "$like": "a%" } }] }
            ]
        }))
        .expect("logic converts");

        assert_eq!(
            node,
            FilterNode::Group {
                op: BoolOp::Or,
                children: vec![
                    predicate(
                        "users",
                        "id",
                        Comparator::In,
                        PredicateValue::List(vec![
                            LiteralValue::Integer(1),
                            LiteralValue::Integer(2)
                        ]),
                    ),
                    FilterNode::Group {
                        op: BoolOp::And,
                        children: vec![
                            predicate(
                                "users",
                                "active",
                                Comparator::Eq,
                                PredicateValue::Single(LiteralValue::Bool(true)),
                            ),
                            predicate(
                                "users",
                                "name",
                                Comparator::Like,
                                PredicateValue::Single(LiteralValue::Text("a%".into())),
                            ),
                        ],
                    },
                ],
            }
        );
    }

    #[test]
    fn filter_node_conversion_rejects_what_the_filter_tree_cannot_express() {
        let cases = [
            (serde_json::json!({ "id": { "$nin": [1, 2] } }), "$nin"),
            (serde_json::json!({ "name": { "$regex": "^a" } }), "$regex"),
            (
                serde_json::json!({ "tags": { "$contains": ["a"] } }),
                "$contains",
            ),
            (
                serde_json::json!({ "tags": { "$overlap": ["a"] } }),
                "$overlap",
            ),
            (serde_json::json!({ "tags": { "$all": ["a"] } }), "$all"),
            (serde_json::json!({ "tags": { "$size": 2 } }), "$size"),
            (serde_json::json!({ "$not": { "id": 1 } }), "$not"),
            (serde_json::json!({ "a.b.c": 1 }), "nested"),
            (serde_json::json!({ "id": { "$in": [] } }), "$in"),
            (serde_json::json!({ "id": { "$in": 3 } }), "$in"),
            (serde_json::json!({ "id": { "$in": [[1]] } }), "array"),
            (serde_json::json!({ "id": { "$gt": [1] } }), "array"),
            (
                serde_json::json!({ "meta": { "$gt": { "a": 1 } } }),
                "object",
            ),
        ];

        for (filter, expected) in cases {
            let error =
                to_filter_node(filter.clone()).expect_err(&format!("{filter} must be rejected"));

            assert!(
                error.contains(expected),
                "the rejection of {filter} should name {expected}: {error}"
            );
        }
    }

    #[test]
    fn filter_node_conversion_propagates_column_resolution_errors() {
        let filter = SemanticFilter::compare("ghost.id", WhereOperator::Eq, Value::Int(1));
        let filter = match filter {
            SemanticFilter::Predicate(mut predicate) => {
                predicate.field = SemanticFieldRef::column(ColumnRef::from_qualified("ghost.id"));
                SemanticFilter::Predicate(predicate)
            }
            other => other,
        };

        let error = semantic_filter_to_filter_node(&filter, &|column: &ColumnRef| {
            Err(format!("unknown table {:?}", column.table))
        })
        .expect_err("a column that does not resolve is rejected");

        assert!(error.contains("unknown table"), "{error}");
    }

    #[test]
    fn semantic_request_kind_matches_variant() {
        let request = SemanticRequest::TableBrowse(TableBrowseRequest::new(TableRef::new("users")));
        assert_eq!(request.kind(), SemanticRequestKind::TableBrowse);

        let request = SemanticRequest::CollectionCount(crate::CollectionCountRequest::new(
            CollectionRef::new("app", "users"),
        ));
        assert_eq!(request.kind(), SemanticRequestKind::CollectionCount);

        let request = SemanticRequest::Aggregate(
            AggregateRequest::new(TableRef::new("orders"))
                .with_aggregations(vec![AggregateSpec::count_all("total")]),
        );
        assert_eq!(request.kind(), SemanticRequestKind::Aggregate);
    }

    #[test]
    fn aggregate_request_builds_sql_from_semantic_fields() {
        let dialect = DefaultSqlDialect;
        let request = AggregateRequest::new(TableRef::new("orders"))
            .with_filter(SemanticFilter::compare(
                "status",
                WhereOperator::Eq,
                Value::Text("paid".into()),
            ))
            .with_group_by(vec![ColumnRef::new("customer_id")])
            .with_aggregations(vec![
                AggregateSpec::new(
                    AggregateFunction::Sum,
                    Some(ColumnRef::new("amount")),
                    "total_amount",
                ),
                AggregateSpec::count_all("total_orders"),
            ])
            .with_having(SemanticFilter::compare(
                "total_amount",
                WhereOperator::Gt,
                Value::Int(100),
            ))
            .with_order_by(vec![OrderByColumn::desc("total_amount")])
            .with_limit(Some(25));

        let sql = request
            .build_sql_with(&dialect)
            .expect("aggregate request should render");

        assert_eq!(
            sql,
            "SELECT \"customer_id\", SUM(\"amount\") AS \"total_amount\", COUNT(*) AS \"total_orders\" FROM \"orders\" WHERE \"status\" = 'paid' GROUP BY \"customer_id\" HAVING \"total_amount\" > 100 ORDER BY \"total_amount\" DESC LIMIT 25"
        );
    }

    #[test]
    fn planned_query_converts_to_query_request() {
        let query = PlannedQuery::new(QueryLanguage::Sql, "SELECT 1")
            .with_database(Some("analytics".into()));

        let request: QueryRequest = query.into_query_request();

        assert_eq!(request.sql, "SELECT 1");
        assert_eq!(request.database.as_deref(), Some("analytics"));
    }

    #[test]
    fn semantic_plan_primary_query_returns_first_query() {
        let plan = SemanticPlan::single_query(
            SemanticPlanKind::Query,
            PlannedQuery::new(QueryLanguage::Sql, "SELECT * FROM users"),
        )
        .with_warning("preview only");

        assert_eq!(
            plan.primary_query().map(|query| query.text.as_str()),
            Some("SELECT * FROM users")
        );
        assert_eq!(plan.warnings, vec!["preview only"]);
    }

    #[test]
    fn semantic_predicate_null_uses_null_operator_without_value() {
        let predicate = SemanticPredicate::null("deleted_at");

        assert_eq!(predicate.operator, WhereOperator::Null);
        assert!(predicate.value.is_none());
    }

    #[test]
    fn parses_implicit_and_filter_json() {
        let filter = serde_json::json!({
            "status": "active",
            "age": { "$gte": 18 }
        });

        let parsed = parse_semantic_filter_json(&filter)
            .expect("filter should parse")
            .expect("filter should not be empty");

        assert!(matches!(parsed, SemanticFilter::And(children) if children.len() == 2));
    }

    #[test]
    fn parses_explicit_or_filter_json() {
        let filter = serde_json::json!({
            "$or": [
                { "role": "admin" },
                { "role": "moderator" }
            ]
        });

        let parsed = parse_semantic_filter_json(&filter)
            .expect("filter should parse")
            .expect("filter should not be empty");

        assert!(matches!(parsed, SemanticFilter::Or(children) if children.len() == 2));
    }

    #[test]
    fn rejects_non_object_filter_values() {
        let error = parse_semantic_filter_json(&serde_json::json!(true)).unwrap_err();
        assert!(error.contains("boolean"));
    }

    #[test]
    fn renders_semantic_filter_to_sql() {
        let dialect = DefaultSqlDialect;
        let filter = SemanticFilter::and(vec![
            SemanticFilter::compare("status", WhereOperator::Eq, Value::Text("active".into())),
            SemanticFilter::compare("age", WhereOperator::Gte, Value::Int(18)),
        ]);

        let sql = render_semantic_filter_sql(&filter, &dialect).expect("filter should render");

        assert_eq!(sql, "(\"status\" = 'active') AND (\"age\" >= 18)");
    }

    #[test]
    fn renders_null_and_not_null_sql() {
        let dialect = DefaultSqlDialect;

        let is_null = render_semantic_filter_sql(&SemanticFilter::null("deleted_at"), &dialect)
            .expect("null filter should render");
        assert_eq!(is_null, "\"deleted_at\" IS NULL");

        let is_not_null = render_semantic_filter_sql(
            &SemanticFilter::negate(SemanticFilter::null("deleted_at")),
            &dialect,
        )
        .expect("not-null filter should render");

        assert_eq!(is_not_null, "NOT (\"deleted_at\" IS NULL)");
    }
}
