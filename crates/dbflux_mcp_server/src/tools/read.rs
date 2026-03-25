//! Read operation tools for MCP server.
//!
//! Provides type-safe parameter structs for read operations:
//! - `select_data`: Query records with filtering, sorting, pagination, and joins
//! - `count_records`: Count records matching a filter
//! - `aggregate_data`: Perform aggregations with grouping and having clauses

use dbflux_core::{ColumnRef, QueryRequest, TableRef, Value};
use rmcp::{
    ErrorData,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content},
    schemars::JsonSchema,
    tool, tool_router,
};
use serde::Deserialize;

use crate::{
    helper::{IntoErrorData, *},
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

    /// Quotes a column reference, handling qualified names (table.column or schema.table.column).
    ///
    /// Uses `ColumnRef` for simple and two-part qualified names.
    /// For multi-part qualified names (schema.table.column), splits and quotes each part.
    fn quote_column_reference(column: &str, dialect: &dyn dbflux_core::SqlDialect) -> String {
        let parts: Vec<&str> = column.split('.').collect();
        match parts.len() {
            1 => {
                let col_ref = ColumnRef::new(column);
                col_ref.quoted_with(dialect)
            }
            2 => {
                let col_ref = ColumnRef::from_qualified(column);
                col_ref.quoted_with(dialect)
            }
            _ => {
                // Multi-part qualified name (schema.table.column or more)
                parts
                    .iter()
                    .map(|part| dialect.quote_identifier(part))
                    .collect::<Vec<_>>()
                    .join(".")
            }
        }
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
        joins: Option<&[JoinSpec]>,
        database: Option<&str>,
    ) -> Result<serde_json::Value, String> {
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

        let dialect = connection.dialect();

        // Build column list
        let column_list = if let Some(cols) = columns {
            if cols.is_empty() {
                "*".to_string()
            } else {
                cols.iter()
                    .map(|c| Self::quote_column_reference(c, dialect))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        } else {
            "*".to_string()
        };

        // Build base query
        // Apply default schema if the driver supports schemas and no schema qualifier is present
        let table_with_schema = {
            let default_schema = connection
                .metadata()
                .syntax
                .as_ref()
                .filter(|s| s.supports_schemas && !table.contains('.'))
                .and_then(|s| s.default_schema.clone());

            if let Some(schema) = default_schema {
                format!("{}.{}", schema, table)
            } else {
                table.to_string()
            }
        };
        let table_ref = TableRef::from_qualified(&table_with_schema);
        let table_quoted = table_ref.quoted_with(dialect);

        let mut sql = format!("SELECT {} FROM {}", column_list, table_quoted);

        // Add joins if specified
        if let Some(joins_list) = joins {
            for join in joins_list {
                let join_type = match join.r#type.to_uppercase().as_str() {
                    "LEFT" => "LEFT JOIN",
                    "RIGHT" => "RIGHT JOIN",
                    "INNER" => "INNER JOIN",
                    _ => "JOIN",
                };
                let join_table = dialect.quote_identifier(&join.table);
                sql.push_str(&format!(" {} {} ON {}", join_type, join_table, join.on));
            }
        }

        // Add WHERE clause from filter
        if let Some(f) = filter {
            let where_clause = json_filter_to_sql(f, dialect)?;
            if !where_clause.is_empty() {
                sql.push_str(&format!(" WHERE {}", where_clause));
            }
        }

        // Add ORDER BY
        if let Some(order) = order_by
            && !order.is_empty()
        {
            let order_clauses: Vec<String> = order
                .iter()
                .map(|o| {
                    let dir = o
                        .direction
                        .as_deref()
                        .map(|d| d.to_uppercase())
                        .unwrap_or_else(|| "ASC".to_string());
                    let col_ref = ColumnRef::from_qualified(&o.column);
                    format!("{} {}", col_ref.quoted_with(dialect), dir)
                })
                .collect();
            sql.push_str(&format!(" ORDER BY {}", order_clauses.join(", ")));
        }

        // Add pagination
        sql.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

        let mut request = QueryRequest::new(&sql);
        if let Some(db) = database {
            request = request.with_database(Some(db.to_string()));
        }

        let conn = connection.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
            #[allow(clippy::large_enum_variant)]
            conn.execute(&request)
                .map_err(|e| format!("Select error: {}", e))
        })
        .await
        .map_err(|e| format!("Blocking task failed: {}", e))?;

        let result = result?;
        Ok(serialize_query_result(&result))
    }

    async fn count_records_impl(
        state: ServerState,
        connection_id: &str,
        table: &str,
        filter: Option<&serde_json::Value>,
        database: Option<&str>,
    ) -> Result<u64, String> {
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

        let dialect = connection.dialect();

        // Apply default schema if the driver supports schemas and no schema qualifier is present
        let table_with_schema = {
            let default_schema = connection
                .metadata()
                .syntax
                .as_ref()
                .filter(|s| s.supports_schemas && !table.contains('.'))
                .and_then(|s| s.default_schema.clone());

            if let Some(schema) = default_schema {
                format!("{}.{}", schema, table)
            } else {
                table.to_string()
            }
        };
        let table_ref = TableRef::from_qualified(&table_with_schema);
        let table_quoted = table_ref.quoted_with(dialect);

        let mut sql = format!("SELECT COUNT(*) FROM {}", table_quoted);

        if let Some(f) = filter {
            let where_clause = json_filter_to_sql(f, dialect)?;
            if !where_clause.is_empty() {
                sql.push_str(&format!(" WHERE {}", where_clause));
            }
        }

        let mut request = QueryRequest::new(&sql);
        if let Some(db) = database {
            request = request.with_database(Some(db.to_string()));
        }

        let conn = connection.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
            #[allow(clippy::large_enum_variant)]
            conn.execute(&request)
                .map_err(|e| format!("Count error: {}", e))
        })
        .await
        .map_err(|e| format!("Blocking task failed: {}", e))?;

        let result = result?;
        let count = result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|val| match val {
                Value::Int(i) => Some(*i as u64),
                _ => None,
            })
            .unwrap_or(0);

        Ok(count)
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
        let connection = Self::get_or_connect(state, connection_id).await?;
        let dialect = connection.dialect();

        // Apply default schema if the driver supports schemas and no schema qualifier is present
        let table_with_schema = {
            let default_schema = connection
                .metadata()
                .syntax
                .as_ref()
                .filter(|s| s.supports_schemas && !table.contains('.'))
                .and_then(|s| s.default_schema.clone());

            if let Some(schema) = default_schema {
                format!("{}.{}", schema, table)
            } else {
                table.to_string()
            }
        };
        let table_ref = TableRef::from_qualified(&table_with_schema);
        let table_quoted = table_ref.quoted_with(dialect);

        // Build aggregation expressions
        let agg_exprs: Vec<String> = aggregations
            .iter()
            .map(|agg| {
                let func = agg.function.to_uppercase();
                let col = if agg.column == "*" {
                    "*".to_string()
                } else {
                    dialect.quote_identifier(&agg.column)
                };
                format!(
                    "{}({}) AS {}",
                    func,
                    col,
                    dialect.quote_identifier(&agg.alias)
                )
            })
            .collect();

        // Build GROUP BY columns
        let group_cols: Vec<String> = group_by
            .iter()
            .map(|c| dialect.quote_identifier(c))
            .collect();

        let mut sql = format!(
            "SELECT {}, {} FROM {}",
            group_cols.join(", "),
            agg_exprs.join(", "),
            table_quoted
        );

        // Add WHERE clause
        if let Some(f) = filter {
            let where_clause = json_filter_to_sql(f, dialect)?;
            if !where_clause.is_empty() {
                sql.push_str(&format!(" WHERE {}", where_clause));
            }
        }

        // Add GROUP BY
        sql.push_str(&format!(" GROUP BY {}", group_cols.join(", ")));

        // Add HAVING clause
        if let Some(h) = having {
            let having_clause = json_filter_to_sql(h, dialect)?;
            if !having_clause.is_empty() {
                sql.push_str(&format!(" HAVING {}", having_clause));
            }
        }

        // Add ORDER BY
        if let Some(order) = order_by
            && !order.is_empty()
        {
            let order_clauses: Vec<String> = order
                .iter()
                .map(|o| {
                    let dir = o
                        .direction
                        .as_deref()
                        .map(|d| d.to_uppercase())
                        .unwrap_or_else(|| "ASC".to_string());
                    let col_ref = ColumnRef::from_qualified(&o.column);
                    format!("{} {}", col_ref.quoted_with(dialect), dir)
                })
                .collect();
            sql.push_str(&format!(" ORDER BY {}", order_clauses.join(", ")));
        }

        // Add LIMIT
        if let Some(lim) = limit {
            sql.push_str(&format!(" LIMIT {}", lim));
        }

        let mut request = QueryRequest::new(&sql);
        if let Some(db) = database {
            request = request.with_database(Some(db.to_string()));
        }

        let conn = connection.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
            #[allow(clippy::large_enum_variant)]
            conn.execute(&request)
                .map_err(|e| format!("Aggregate error: {}", e))
        })
        .await
        .map_err(|e| format!("Blocking task failed: {}", e))?;

        let result = result?;
        Ok(serialize_query_result(&result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_quote_column_reference_simple() {
        use dbflux_core::DefaultSqlDialect;

        let dialect = DefaultSqlDialect;
        let result = DbFluxServer::quote_column_reference("name", &dialect);
        assert_eq!(result, r#""name""#);
    }

    #[test]
    fn test_quote_column_reference_qualified() {
        use dbflux_core::DefaultSqlDialect;

        let dialect = DefaultSqlDialect;
        let result = DbFluxServer::quote_column_reference("users.name", &dialect);
        assert_eq!(result, r#""users"."name""#);
    }

    #[test]
    fn test_quote_column_reference_fully_qualified() {
        use dbflux_core::DefaultSqlDialect;

        let dialect = DefaultSqlDialect;
        let result = DbFluxServer::quote_column_reference("public.users.name", &dialect);
        assert_eq!(result, r#""public"."users"."name""#);
    }
}
