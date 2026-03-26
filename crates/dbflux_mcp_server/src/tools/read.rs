//! Read operation tools for MCP server.
//!
//! Provides type-safe parameter structs for read operations:
//! - `select_data`: Query records with filtering, sorting, pagination, and joins
//! - `count_records`: Count records matching a filter
//! - `aggregate_data`: Perform aggregations with grouping and having clauses

use std::sync::Arc;

use dbflux_core::{
    AggregateFunction, AggregateRequest, AggregateSpec as CoreAggregateSpec,
    CollectionBrowseRequest, CollectionCountRequest, CollectionRef, ColumnRef, Connection,
    DatabaseCategory, OrderByColumn, Pagination, QueryResult, SemanticRequest, SortDirection,
    TableBrowseRequest, TableCountRequest, TableRef, parse_semantic_filter_json,
};
use rmcp::{
    ErrorData,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content},
    schemars::JsonSchema,
    tool, tool_router,
};
use serde::Deserialize;

use crate::{
    helper::{IntoErrorData, serialize_query_result},
    server::DbFluxServer,
    state::ServerState,
};

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct OrderByItem {
    #[schemars(description = "Column name to sort by")]
    pub column: String,

    #[schemars(description = "Sort direction: 'asc' or 'desc' (default: 'asc')")]
    pub direction: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct JoinSpec {
    #[schemars(description = "Join type: 'inner', 'left', or 'right'")]
    pub r#type: String,

    #[schemars(description = "Table to join")]
    pub table: String,

    #[schemars(description = "Join condition (e.g., 'users.id = orders.user_id')")]
    pub on: String,

    #[schemars(description = "Optional alias for the joined table")]
    pub alias: Option<String>,

    #[schemars(description = "Columns to select from the joined table")]
    pub columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct AggregationSpec {
    #[schemars(description = "Aggregation function: 'count', 'sum', 'avg', 'min', 'max'")]
    pub function: String,

    #[schemars(description = "Column to aggregate (use '*' for count)")]
    pub column: String,

    #[schemars(description = "Alias for the result column")]
    pub alias: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SelectDataParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Table or collection name")]
    pub table: String,

    #[schemars(description = "Columns to select (default: all columns)")]
    pub columns: Option<Vec<String>>,

    #[schemars(description = "Filter conditions as JSON object")]
    pub r#where: Option<serde_json::Value>,

    #[schemars(description = "Sort order")]
    pub order_by: Option<Vec<OrderByItem>>,

    #[schemars(description = "Maximum rows to return (default: 100, max: 10000)")]
    pub limit: Option<u32>,

    #[schemars(description = "Number of rows to skip")]
    pub offset: Option<u32>,

    #[schemars(description = "Join operations (relational databases only)")]
    pub joins: Option<Vec<JoinSpec>>,

    #[schemars(description = "Optional database/schema name")]
    pub database: Option<String>,
}

impl SelectDataParams {
    pub const DEFAULT_LIMIT: u32 = 100;
    pub const MAX_LIMIT: u32 = 10000;

    pub fn effective_limit(&self) -> u32 {
        self.limit
            .unwrap_or(Self::DEFAULT_LIMIT)
            .min(Self::MAX_LIMIT)
    }

    pub fn effective_offset(&self) -> u32 {
        self.offset.unwrap_or(0)
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CountRecordsParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Table or collection name")]
    pub table: String,

    #[schemars(description = "Filter conditions as JSON object")]
    pub r#where: Option<serde_json::Value>,

    #[schemars(description = "Optional database/schema name")]
    pub database: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AggregateDataParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,

    #[schemars(description = "Table or collection name")]
    pub table: String,

    #[schemars(description = "Filter conditions as JSON object")]
    pub r#where: Option<serde_json::Value>,

    #[schemars(description = "Columns to group by")]
    pub group_by: Vec<String>,

    #[schemars(description = "Aggregation functions to apply")]
    pub aggregations: Vec<AggregationSpec>,

    #[schemars(description = "Filter conditions for aggregated results (HAVING clause)")]
    pub having: Option<serde_json::Value>,

    #[schemars(description = "Sort order for results")]
    pub order_by: Option<Vec<OrderByItem>>,

    #[schemars(description = "Maximum rows to return")]
    pub limit: Option<u32>,

    #[schemars(description = "Optional database/schema name")]
    pub database: Option<String>,
}

#[tool_router(router = read_router, vis = "pub")]
impl DbFluxServer {
    #[tool(description = "Select data from a table with filtering, sorting, and pagination")]
    async fn select_data(
        &self,
        Parameters(params): Parameters<SelectDataParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let table = params.table.clone();
        let columns = params.columns.clone();
        let filter = params.r#where.clone();
        let order_by = params.order_by.clone();
        let limit = params.effective_limit();
        let offset = params.effective_offset();
        let joins = params.joins.clone();
        let database = params.database.clone();

        self.governance
            .authorize_and_execute(
                "select_data",
                Some(&params.connection_id),
                ExecutionClassification::Read,
                move || async move {
                    let result = Self::select_data_impl(
                        state,
                        &connection_id,
                        &table,
                        columns.as_deref(),
                        filter.as_ref(),
                        order_by.as_deref(),
                        limit,
                        offset,
                        joins.as_deref(),
                        database.as_deref(),
                    )
                    .await
                    .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Count records in a table with optional filter")]
    async fn count_records(
        &self,
        Parameters(params): Parameters<CountRecordsParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let table = params.table.clone();
        let filter = params.r#where.clone();
        let database = params.database.clone();

        self.governance
            .authorize_and_execute(
                "count_records",
                Some(&params.connection_id),
                ExecutionClassification::Read,
                move || async move {
                    let count = Self::count_records_impl(
                        state,
                        &connection_id,
                        &table,
                        filter.as_ref(),
                        database.as_deref(),
                    )
                    .await
                    .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&serde_json::json!({ "count": count }))
                            .unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(
        description = "Perform aggregation operations (COUNT, SUM, AVG, MIN, MAX) with grouping"
    )]
    async fn aggregate_data(
        &self,
        Parameters(params): Parameters<AggregateDataParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();
        let table = params.table.clone();
        let filter = params.r#where.clone();
        let group_by = params.group_by.clone();
        let aggregations = params.aggregations.clone();
        let having = params.having.clone();
        let order_by = params.order_by.clone();
        let limit = params.limit;
        let database = params.database.clone();

        self.governance
            .authorize_and_execute(
                "aggregate_data",
                Some(&params.connection_id),
                ExecutionClassification::Read,
                move || async move {
                    let result = Self::aggregate_data_impl(
                        state,
                        &connection_id,
                        &table,
                        filter.as_ref(),
                        &group_by,
                        &aggregations,
                        having.as_ref(),
                        order_by.as_deref(),
                        limit,
                        database.as_deref(),
                    )
                    .await
                    .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_data_impl(
        state: ServerState,
        connection_id: &str,
        table: &str,
        columns: Option<&[String]>,
        filter: Option<&serde_json::Value>,
        order_by: Option<&[OrderByItem]>,
        limit: u32,
        offset: u32,
        _joins: Option<&[JoinSpec]>,
        database: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let semantic_filter = filter
            .map(parse_semantic_filter_json)
            .transpose()?
            .flatten();

        let connection = if let Some(target_db) = database {
            let current_db = Self::get_current_database(&state, connection_id).await?;

            if target_db != current_db.as_deref().unwrap_or("") {
                Self::connect_with_database(state, connection_id, target_db).await?
            } else {
                Self::get_or_connect(state, connection_id).await?
            }
        } else {
            Self::get_or_connect(state, connection_id).await?
        };

        match connection.metadata().category {
            DatabaseCategory::Document => {
                Self::select_data_document(
                    &connection,
                    table,
                    database,
                    filter,
                    semantic_filter.as_ref(),
                    limit,
                    offset,
                )
                .await
            }
            DatabaseCategory::Relational
            | DatabaseCategory::KeyValue
            | DatabaseCategory::Graph
            | DatabaseCategory::TimeSeries
            | DatabaseCategory::WideColumn => {
                Self::select_data_table(
                    &connection,
                    table,
                    columns,
                    semantic_filter.as_ref(),
                    order_by,
                    limit,
                    offset,
                )
                .await
            }
        }
    }

    /// Handle select_data for document databases (MongoDB, DynamoDB)
    async fn select_data_document(
        connection: &Arc<dyn Connection>,
        table: &str,
        database: Option<&str>,
        filter: Option<&serde_json::Value>,
        semantic_filter: Option<&dbflux_core::SemanticFilter>,
        limit: u32,
        offset: u32,
    ) -> Result<serde_json::Value, String> {
        // For document databases, database parameter is required
        #[allow(clippy::unnecessary_lazy_evaluations)]
        let db_name = database.ok_or_else(|| {
            "Database parameter is required for document databases. \
             Use: select_data(connection_id, table, database=\"db_name\", ...)"
        })?;

        let collection_ref = CollectionRef::new(db_name, table);
        let pagination = Pagination::Offset {
            limit,
            offset: offset as u64,
        };

        let mut request = CollectionBrowseRequest::new(collection_ref).with_pagination(pagination);

        if let Some(f) = filter {
            request = request.with_filter(f.clone());
        }

        if let Some(filter) = semantic_filter {
            request = request.with_semantic_filter(filter.clone());
        }

        let conn = connection.clone();
        #[allow(clippy::result_large_err)]
        let query_result = tokio::task::spawn_blocking(move || conn.browse_collection(&request))
            .await
            .map_err(|e| format!("Blocking task failed: {}", e))?
            .map_err(|e| format!("Select error: {}", e))?;

        Ok(serialize_query_result(&query_result))
    }

    /// Handle select_data for drivers that expose table browse semantics.
    #[allow(clippy::too_many_arguments)]
    async fn select_data_table(
        connection: &Arc<dyn Connection>,
        table: &str,
        columns: Option<&[String]>,
        semantic_filter: Option<&dbflux_core::SemanticFilter>,
        order_by: Option<&[OrderByItem]>,
        limit: u32,
        offset: u32,
    ) -> Result<serde_json::Value, String> {
        let pagination = Pagination::Offset {
            limit,
            offset: offset as u64,
        };

        let mut request =
            TableBrowseRequest::new(Self::table_ref_for_connection(connection, table))
                .with_pagination(pagination)
                .with_order_by(Self::order_by_columns(order_by));

        if let Some(filter) = semantic_filter {
            request = request.with_semantic_filter(filter.clone());
        }

        let conn = connection.clone();
        log::debug!("select_data_table: spawning blocking task for browse_table");
        #[allow(clippy::result_large_err)]
        let query_result = tokio::task::spawn_blocking(move || conn.browse_table(&request))
            .await
            .map_err(|e| format!("Blocking task failed: {}", e))?
            .map_err(|e| format!("Select error: {}", e))?;

        log::debug!(
            "select_data_table: query completed, serializing {} rows",
            query_result.rows.len()
        );
        let result = Self::serialize_selected_result(&query_result, columns)?;
        log::debug!("select_data_table: serialization complete");
        Ok(result)
    }

    async fn count_records_impl(
        state: ServerState,
        connection_id: &str,
        table: &str,
        filter: Option<&serde_json::Value>,
        database: Option<&str>,
    ) -> Result<u64, String> {
        let semantic_filter = filter
            .map(parse_semantic_filter_json)
            .transpose()?
            .flatten();

        let connection = if let Some(target_db) = database {
            let current_db = Self::get_current_database(&state, connection_id).await?;

            if target_db != current_db.as_deref().unwrap_or("") {
                Self::connect_with_database(state, connection_id, target_db).await?
            } else {
                Self::get_or_connect(state, connection_id).await?
            }
        } else {
            Self::get_or_connect(state, connection_id).await?
        };

        match connection.metadata().category {
            DatabaseCategory::Document => {
                #[allow(clippy::unnecessary_lazy_evaluations)]
                let db_name = database.ok_or_else(|| {
                    "Database parameter is required for document databases. Use: count_records(connection_id, table, database=\"db_name\", ...)"
                })?;

                let mut request = CollectionCountRequest::new(CollectionRef::new(db_name, table));

                if let Some(filter) = filter {
                    request = request.with_filter(filter.clone());
                }

                if let Some(filter) = semantic_filter.as_ref() {
                    request = request.with_semantic_filter(filter.clone());
                }

                let conn = connection.clone();
                tokio::task::spawn_blocking(move || conn.count_collection(&request))
                    .await
                    .map_err(|e| format!("Blocking task failed: {}", e))?
                    .map_err(|e| format!("Count error: {}", e))
            }
            DatabaseCategory::Relational
            | DatabaseCategory::KeyValue
            | DatabaseCategory::Graph
            | DatabaseCategory::TimeSeries
            | DatabaseCategory::WideColumn => {
                let mut request =
                    TableCountRequest::new(Self::table_ref_for_connection(&connection, table));

                if let Some(filter) = semantic_filter.as_ref() {
                    request = request.with_semantic_filter(filter.clone());
                }

                let conn = connection.clone();
                tokio::task::spawn_blocking(move || conn.count_table(&request))
                    .await
                    .map_err(|e| format!("Blocking task failed: {}", e))?
                    .map_err(|e| format!("Count error: {}", e))
            }
        }
    }

    fn table_ref_for_connection(connection: &Arc<dyn Connection>, table: &str) -> TableRef {
        let default_schema = connection
            .metadata()
            .syntax
            .as_ref()
            .filter(|syntax| syntax.supports_schemas && !table.contains('.'))
            .and_then(|syntax| syntax.default_schema.clone());

        if let Some(schema) = default_schema {
            TableRef::with_schema(schema, table)
        } else {
            TableRef::from_qualified(table)
        }
    }

    fn order_by_columns(order_by: Option<&[OrderByItem]>) -> Vec<OrderByColumn> {
        order_by
            .unwrap_or_default()
            .iter()
            .map(|item| {
                let direction = match item.direction.as_deref() {
                    Some(direction) if direction.eq_ignore_ascii_case("desc") => {
                        SortDirection::Descending
                    }
                    _ => SortDirection::Ascending,
                };

                OrderByColumn::from_name(&item.column, direction)
            })
            .collect()
    }

    fn serialize_selected_result(
        result: &QueryResult,
        columns: Option<&[String]>,
    ) -> Result<serde_json::Value, String> {
        let Some(columns) = columns else {
            return Ok(serialize_query_result(result));
        };

        if columns.is_empty() {
            return Ok(serialize_query_result(result));
        }

        let mut indices = Vec::with_capacity(columns.len());

        for column in columns {
            let index = result
                .columns
                .iter()
                .position(|meta| meta.name == *column)
                .ok_or_else(|| {
                    format!("Select error: column '{}' not found in result set", column)
                })?;
            indices.push(index);
        }

        let rows = result
            .rows
            .iter()
            .map(|row| {
                let mut object = serde_json::Map::new();

                for (column, index) in columns.iter().zip(indices.iter()) {
                    object.insert(column.clone(), crate::helper::value_to_json(&row[*index]));
                }

                serde_json::Value::Object(object)
            })
            .collect::<Vec<_>>();

        Ok(serde_json::json!({
            "columns": columns,
            "rows": rows,
            "row_count": result.rows.len(),
        }))
    }

    #[allow(clippy::too_many_arguments)]
    async fn aggregate_data_impl(
        state: ServerState,
        connection_id: &str,
        table: &str,
        filter: Option<&serde_json::Value>,
        group_by: &[String],
        aggregations: &[AggregationSpec],
        having: Option<&serde_json::Value>,
        order_by: Option<&[OrderByItem]>,
        limit: Option<u32>,
        database: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let semantic_filter = filter
            .map(parse_semantic_filter_json)
            .transpose()?
            .flatten();
        let semantic_having = having
            .map(parse_semantic_filter_json)
            .transpose()?
            .flatten();

        let connection = if let Some(target_db) = database {
            let current_db = Self::get_current_database(&state, connection_id).await?;

            if target_db != current_db.as_deref().unwrap_or("") {
                Self::connect_with_database(state, connection_id, target_db).await?
            } else {
                Self::get_or_connect(state, connection_id).await?
            }
        } else {
            Self::get_or_connect(state, connection_id).await?
        };

        let mut request = AggregateRequest::new(Self::table_ref_for_connection(&connection, table))
            .with_group_by(
                group_by
                    .iter()
                    .map(|column| ColumnRef::from_qualified(column))
                    .collect(),
            )
            .with_aggregations(Self::aggregate_specs(aggregations)?)
            .with_order_by(Self::order_by_columns(order_by))
            .with_limit(limit)
            .with_target_database(database.map(str::to_string));

        if let Some(filter) = semantic_filter {
            request = request.with_filter(filter);
        }

        if let Some(having) = semantic_having {
            request = request.with_having(having);
        }

        let semantic_request = SemanticRequest::Aggregate(request);
        let result = Self::execute_aggregate_semantic_request(
            connection.clone(),
            semantic_request,
            database.map(str::to_string),
        )
        .await?;

        Ok(serialize_query_result(&result))
    }

    async fn execute_aggregate_semantic_request(
        connection: Arc<dyn Connection>,
        semantic_request: SemanticRequest,
        target_database: Option<String>,
    ) -> Result<QueryResult, String> {
        let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
            let plan = connection
                .plan_semantic_request(&semantic_request)
                .map_err(|e| format!("Aggregate planning error: {}", e))?;
            let planned_query = plan.primary_query().cloned().ok_or_else(|| {
                "Aggregate planning error: driver returned no executable query".to_string()
            })?;

            let mut request = planned_query.into_query_request();
            if request.database.is_none() {
                request = request.with_database(target_database);
            }

            #[allow(clippy::large_enum_variant)]
            connection
                .execute(&request)
                .map_err(|e| format!("Aggregate error: {}", e))
        })
        .await
        .map_err(|e| format!("Blocking task failed: {}", e))?;

        result
    }

    fn aggregate_specs(aggregations: &[AggregationSpec]) -> Result<Vec<CoreAggregateSpec>, String> {
        aggregations
            .iter()
            .map(|aggregation| {
                let function = Self::aggregate_function(&aggregation.function)?;
                let column = if aggregation.column == "*" {
                    None
                } else {
                    Some(ColumnRef::from_qualified(&aggregation.column))
                };

                Ok(CoreAggregateSpec::new(
                    function,
                    column,
                    aggregation.alias.clone(),
                ))
            })
            .collect()
    }

    fn aggregate_function(function: &str) -> Result<AggregateFunction, String> {
        match function.trim().to_ascii_lowercase().as_str() {
            "count" => Ok(AggregateFunction::Count),
            "sum" => Ok(AggregateFunction::Sum),
            "avg" => Ok(AggregateFunction::Avg),
            "min" => Ok(AggregateFunction::Min),
            "max" => Ok(AggregateFunction::Max),
            other => Err(format!(
                "Unsupported aggregation function '{}'. Supported functions: count, sum, avg, min, max",
                other
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use dbflux_core::{
        DbError, DbKind, DefaultSqlDialect, DriverCapabilities, DriverMetadata,
        MutationCapabilities, PlaceholderStyle, QueryCapabilities, QueryHandle, QueryLanguage,
        QueryResult, SchemaLoadingStrategy, SchemaSnapshot, SyntaxInfo, TransactionCapabilities,
    };
    use std::sync::LazyLock;

    static TEST_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
        id: "test".into(),
        display_name: "Test".into(),
        description: "Test driver".into(),
        category: DatabaseCategory::Relational,
        query_language: QueryLanguage::Sql,
        capabilities: DriverCapabilities::empty(),
        default_port: None,
        uri_scheme: "test".into(),
        icon: dbflux_core::Icon::Database,
        syntax: Some(SyntaxInfo {
            identifier_quote: '"',
            string_quote: '\'',
            placeholder_style: PlaceholderStyle::QuestionMark,
            supports_schemas: true,
            default_schema: Some("public".into()),
            case_sensitive_identifiers: true,
        }),
        query: Some(QueryCapabilities::default()),
        mutation: Some(MutationCapabilities::default()),
        ddl: None,
        transactions: Some(TransactionCapabilities::default()),
        limits: None,
        classification_override: None,
    });

    struct UnsupportedAggregateConnection;

    impl Connection for UnsupportedAggregateConnection {
        fn metadata(&self) -> &DriverMetadata {
            &TEST_METADATA
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, _req: &dbflux_core::QueryRequest) -> Result<QueryResult, DbError> {
            panic!("aggregate execution should not run when planning is unsupported")
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

        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &DefaultSqlDialect
        }

        fn plan_semantic_request(
            &self,
            _request: &SemanticRequest,
        ) -> Result<dbflux_core::SemanticPlan, DbError> {
            Err(DbError::NotSupported(
                "aggregate semantics are not supported by this driver".into(),
            ))
        }
    }

    #[test]
    fn test_select_data_params_default_limit() {
        let params = SelectDataParams {
            connection_id: "test".to_string(),
            table: "users".to_string(),
            columns: None,
            r#where: None,
            order_by: None,
            limit: None,
            offset: None,
            joins: None,
            database: None,
        };

        assert_eq!(params.effective_limit(), 100);
        assert_eq!(params.effective_offset(), 0);
    }

    #[test]
    fn test_select_data_params_limit_capped() {
        let params = SelectDataParams {
            connection_id: "test".to_string(),
            table: "users".to_string(),
            columns: None,
            r#where: None,
            order_by: None,
            limit: Some(50000),
            offset: None,
            joins: None,
            database: None,
        };

        assert_eq!(params.effective_limit(), 10000);
    }

    #[test]
    fn test_select_data_params_custom_limit() {
        let params = SelectDataParams {
            connection_id: "test".to_string(),
            table: "users".to_string(),
            columns: None,
            r#where: None,
            order_by: None,
            limit: Some(500),
            offset: Some(100),
            joins: None,
            database: None,
        };

        assert_eq!(params.effective_limit(), 500);
        assert_eq!(params.effective_offset(), 100);
    }

    #[test]
    fn test_aggregate_function_accepts_case_insensitive_names() {
        assert!(matches!(
            DbFluxServer::aggregate_function("SuM"),
            Ok(AggregateFunction::Sum)
        ));
    }

    #[test]
    fn test_aggregate_function_rejects_unknown_names() {
        let error = DbFluxServer::aggregate_function("median").unwrap_err();
        assert!(error.contains("Unsupported aggregation function"));
    }

    #[tokio::test]
    async fn aggregate_execution_returns_explicit_unsupported_error() {
        let error = DbFluxServer::execute_aggregate_semantic_request(
            Arc::new(UnsupportedAggregateConnection),
            SemanticRequest::Aggregate(AggregateRequest::new(TableRef::new("users"))),
            None,
        )
        .await
        .unwrap_err();

        assert!(error.contains("Aggregate planning error"));
        assert!(error.contains("aggregate semantics are not supported by this driver"));
    }
}
