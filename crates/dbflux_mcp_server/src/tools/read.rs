//! Read operation tools for MCP server.
//!
//! Provides type-safe parameter structs for read operations:
//! - `select_data`: Query records with filtering, sorting, and pagination
//! - `count_records`: Count records matching a filter
//! - `aggregate_data`: Perform aggregations with grouping and having clauses

use std::sync::Arc;

use dbflux_core::{
    AggregateFunction, AggregateRequest, AggregateSpec as CoreAggregateSpec,
    CollectionBrowseRequest, CollectionCountRequest, CollectionRef, ColumnRef, Connection,
    DatabaseCategory, OrderByColumn, Pagination, QueryResult, SemanticFilter, SemanticRequest,
    SortDirection, TableBrowseRequest, TableCountRequest, TableRef, parse_semantic_filter_json,
};
use rmcp::{
    ErrorData, handler::server::wrapper::Parameters, model::CallToolResult, schemars::JsonSchema,
    tool, tool_router,
};
use serde::Deserialize;

use crate::{
    helper::{IntoErrorData, serialize_query_result, to_json_content},
    server::DbFluxServer,
    state::ServerState,
    tools::{
        join_select::{self, JoinedSelect},
        not_found::{self, CallFailure, ColumnCheck, ColumnReference, FailedCall},
    },
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
    #[schemars(
        description = "Join type: 'inner', 'left', 'right' or 'full'. Not every engine has 'right' and 'full'"
    )]
    pub r#type: String,

    #[schemars(
        description = "Table to join: a plain name, or schema.table on connections that qualify table names with a schema"
    )]
    pub table: String,

    #[schemars(
        description = "Join condition: column comparisons joined by AND, e.g. 'users.id = o.user_id AND users.org = o.org'. \
                       Operators: = != < <= > >=. Each side is qualifier.column, where the qualifier is the name or alias \
                       of the main table, of this join or of an earlier join. Nothing else is accepted: no literals, \
                       functions, OR or parentheses"
    )]
    pub on: String,

    #[schemars(description = "Alias for the joined table. Needed to join the same table twice")]
    pub alias: Option<String>,

    #[schemars(
        description = "Columns of this table to return, named alias.column in the result. Needs the top-level columns"
    )]
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

    #[schemars(description = crate::tools::WHERE_FILTER_DESCRIPTION)]
    pub r#where: Option<serde_json::Value>,

    #[schemars(description = "Sort order")]
    pub order_by: Option<Vec<OrderByItem>>,

    #[schemars(description = "Maximum rows to return (default: 100, max: 10000)")]
    pub limit: Option<u32>,

    #[schemars(description = "Number of rows to skip")]
    pub offset: Option<u32>,

    #[schemars(
        description = "Joins to other tables. Runs only on drivers that declare join support; the others return an error. \
                       With joins, write a column of a joined table as qualifier.column in columns, where and order_by; \
                       a bare name is a column of the main table. Table, alias and column names must be plain identifiers. \
                       Result columns are named as written in columns. Without columns every column of every table is \
                       returned and a repeated name gets a numeric suffix no other result column uses (id, id_2). \
                       where accepts $eq $ne $gt $gte $lt $lte $in $like $exists, null, $and and $or, and $ilike on \
                       drivers that declare it. order_by direction must be asc or desc"
    )]
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

    #[schemars(description = crate::tools::WHERE_FILTER_DESCRIPTION)]
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

    #[schemars(description = crate::tools::WHERE_FILTER_DESCRIPTION)]
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
        use crate::governance::AuditDetails;
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
            .authorize_and_execute_audited(
                "select_data",
                Some(&params.connection_id),
                ExecutionClassification::Read,
                move || async move {
                    let (result, sql_text) = Self::select_data_impl(
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

                    Ok((
                        CallToolResult::success(vec![to_json_content(&result)?]),
                        AuditDetails { query: sql_text },
                    ))
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

                    Ok(CallToolResult::success(vec![to_json_content(
                        &serde_json::json!({ "count": count }),
                    )?]))
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
        use crate::governance::AuditDetails;
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
            .authorize_and_execute_audited(
                "aggregate_data",
                Some(&params.connection_id),
                ExecutionClassification::Read,
                move || async move {
                    let (result, sql_text) = Self::aggregate_data_impl(
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

                    Ok((
                        CallToolResult::success(vec![to_json_content(&result)?]),
                        AuditDetails { query: sql_text },
                    ))
                },
            )
            .await
    }

    /// Returns the result and, for a call with joins, the SQL that produced
    /// it. A call without joins goes through the driver's browse path and
    /// has no query text to record.
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
    ) -> Result<(serde_json::Value, Option<String>), String> {
        let semantic_filter = filter
            .map(parse_semantic_filter_json)
            .transpose()?
            .flatten();

        let joins = joins.filter(|joins| !joins.is_empty());

        if joins.is_some()
            && let Some(database) = database
        {
            join_select::require_safe_database_name(database)?;
        }

        let connection = if let Some(target_db) = database {
            let current_db = Self::get_current_database(&state, connection_id).await?;

            if target_db != current_db.as_deref().unwrap_or("") {
                Self::connect_with_database(state.clone(), connection_id, target_db).await?
            } else {
                Self::get_or_connect(state.clone(), connection_id).await?
            }
        } else {
            Self::get_or_connect(state.clone(), connection_id).await?
        };

        let target = Self::hint_target(&connection, table);

        // The column check covers every column the post-failure hint would
        // look up, so a checked call does not read the columns a second time.
        let columns_checked = joins.is_some()
            || matches!(connection.metadata().category, DatabaseCategory::Relational);

        let outcome = match joins {
            Some(joins) => Self::select_data_joined(
                &state,
                connection_id,
                &connection,
                JoinedSelect {
                    table,
                    columns,
                    filter: semantic_filter.as_ref(),
                    order_by,
                    limit,
                    offset,
                    joins,
                    database,
                },
            )
            .await
            .map(|(value, sql)| (value, Some(sql))),
            None => {
                let check = ColumnCheck {
                    state: &state,
                    connection_id,
                    connection: &connection,
                    database,
                    references: if columns_checked {
                        Self::checked_columns(semantic_filter.as_ref(), order_by, columns, &target)
                    } else {
                        Vec::new()
                    },
                };

                Self::select_data_single_source(
                    check,
                    table,
                    columns,
                    filter,
                    semantic_filter.as_ref(),
                    order_by,
                    limit,
                    offset,
                )
                .await
                .map(|value| (value, None))
            }
        };

        let failure = match outcome {
            Ok(selected) => return Ok(selected),
            Err(failure) => failure,
        };

        let referenced = if columns_checked {
            Vec::new()
        } else {
            Self::referenced_columns(semantic_filter.as_ref(), order_by, &target, &[])
        };

        Err(not_found::explain(
            FailedCall {
                state: &state,
                connection_id,
                connection: &connection,
                target: &target,
                database,
                columns: referenced,
            },
            failure,
        )
        .await)
    }

    /// Handles select_data without joins through the driver's browse path,
    /// after the column check of `check` passes.
    #[allow(clippy::too_many_arguments)]
    async fn select_data_single_source(
        check: ColumnCheck<'_>,
        table: &str,
        columns: Option<&[String]>,
        filter: Option<&serde_json::Value>,
        semantic_filter: Option<&SemanticFilter>,
        order_by: Option<&[OrderByItem]>,
        limit: u32,
        offset: u32,
    ) -> Result<serde_json::Value, CallFailure> {
        let connection = check.connection;
        let database = check.database;

        not_found::check_columns(check).await?;

        match connection.metadata().category {
            DatabaseCategory::Document | DatabaseCategory::LogStream => {
                Self::select_data_document(
                    connection,
                    table,
                    database,
                    filter,
                    semantic_filter,
                    limit,
                    offset,
                )
                .await
            }
            // ObjectStorage has no `select_data` support yet (bucket/object
            // listing goes through `ObjectStoreConnection`, not `select_data`).
            // It falls into the table-shaped path rather than a separate MCP
            // error case so a future object-storage MCP tool can extend it.
            DatabaseCategory::Relational
            | DatabaseCategory::KeyValue
            | DatabaseCategory::Graph
            | DatabaseCategory::TimeSeries
            | DatabaseCategory::WideColumn
            | DatabaseCategory::ObjectStorage => {
                Self::select_data_table(
                    connection,
                    table,
                    columns,
                    semantic_filter,
                    order_by,
                    limit,
                    offset,
                )
                .await
            }
        }
    }

    /// The table or collection a failed call targeted, as the not-found hint
    /// looks it up. Collection names are taken whole, because they may
    /// contain dots.
    fn hint_target(connection: &Arc<dyn Connection>, table: &str) -> TableRef {
        match connection.metadata().category {
            DatabaseCategory::Document | DatabaseCategory::LogStream => TableRef::new(table),
            _ => Self::table_ref_for_connection(connection, table),
        }
    }

    /// Column names of `target` that a call referenced in its filter and sort,
    /// leaving out the names in `excluded` (aggregate aliases).
    fn referenced_columns(
        semantic_filter: Option<&SemanticFilter>,
        order_by: Option<&[OrderByItem]>,
        target: &TableRef,
        excluded: &[String],
    ) -> Vec<String> {
        let sorted = order_by
            .unwrap_or_default()
            .iter()
            .filter(|item| not_found::is_plain_identifier(&item.column))
            .filter_map(|item| {
                not_found::column_of_table(&ColumnRef::from_qualified(&item.column), target)
            });

        not_found::filter_columns(semantic_filter, target)
            .into_iter()
            .chain(sorted)
            .filter(|column| !excluded.contains(column))
            .collect()
    }

    /// Columns of `target` a call without joins names in `where`, `order_by`
    /// and `columns`, for the column check. Nested paths, expressions and
    /// names qualified with another table are left out.
    fn checked_columns(
        semantic_filter: Option<&SemanticFilter>,
        order_by: Option<&[OrderByItem]>,
        columns: Option<&[String]>,
        target: &TableRef,
    ) -> Vec<ColumnReference> {
        let projected = columns
            .unwrap_or_default()
            .iter()
            .filter(|column| not_found::is_plain_identifier(column))
            .filter_map(|column| {
                not_found::column_of_table(&ColumnRef::from_qualified(column), target)
            });

        Self::referenced_columns(semantic_filter, order_by, target, &[])
            .into_iter()
            .chain(projected)
            .filter(|column| not_found::is_checkable_column(column))
            .map(|column| ColumnReference {
                table: target.clone(),
                column,
            })
            .collect()
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
    ) -> Result<serde_json::Value, CallFailure> {
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
            .map_err(|e| CallFailure::from_driver(format!("Select error: {}", e), &e))?;

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
    ) -> Result<serde_json::Value, CallFailure> {
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
            .map_err(|e| CallFailure::from_driver(format!("Select error: {}", e), &e))?;

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
                Self::connect_with_database(state.clone(), connection_id, target_db).await?
            } else {
                Self::get_or_connect(state.clone(), connection_id).await?
            }
        } else {
            Self::get_or_connect(state.clone(), connection_id).await?
        };

        let outcome: Result<u64, CallFailure> = match connection.metadata().category {
            DatabaseCategory::Document | DatabaseCategory::LogStream => {
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
                tokio::task::spawn_blocking(move || {
                    conn.count_collection(&request)
                        .map_err(|e| CallFailure::from_driver(format!("Count error: {}", e), &e))
                })
                .await
                .map_err(|e| format!("Blocking task failed: {}", e))?
            }
            // See select_data's ObjectStorage note above: no dedicated
            // object-storage count path exists yet, so it shares the
            // table-shaped branch.
            DatabaseCategory::Relational
            | DatabaseCategory::KeyValue
            | DatabaseCategory::Graph
            | DatabaseCategory::TimeSeries
            | DatabaseCategory::WideColumn
            | DatabaseCategory::ObjectStorage => {
                let mut request =
                    TableCountRequest::new(Self::table_ref_for_connection(&connection, table));

                if let Some(filter) = semantic_filter.as_ref() {
                    request = request.with_semantic_filter(filter.clone());
                }

                let conn = connection.clone();
                tokio::task::spawn_blocking(move || {
                    conn.count_table(&request)
                        .map_err(|e| CallFailure::from_driver(format!("Count error: {}", e), &e))
                })
                .await
                .map_err(|e| format!("Blocking task failed: {}", e))?
            }
        };

        let failure = match outcome {
            Ok(count) => return Ok(count),
            Err(failure) => failure,
        };

        let target = Self::hint_target(&connection, table);
        let referenced = Self::referenced_columns(semantic_filter.as_ref(), None, &target, &[]);

        Err(not_found::explain(
            FailedCall {
                state: &state,
                connection_id,
                connection: &connection,
                target: &target,
                database,
                columns: referenced,
            },
            failure,
        )
        .await)
    }

    pub(super) fn table_ref_for_connection(
        connection: &Arc<dyn Connection>,
        table: &str,
    ) -> TableRef {
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
                    let available: Vec<&str> = result
                        .columns
                        .iter()
                        .map(|meta| meta.name.as_str())
                        .collect();

                    not_found::with_column_hints(
                        format!("Select error: column '{}' not found in result set", column),
                        column,
                        &available,
                    )
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
    ) -> Result<(serde_json::Value, Option<String>), String> {
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
                Self::connect_with_database(state.clone(), connection_id, target_db).await?
            } else {
                Self::get_or_connect(state.clone(), connection_id).await?
            }
        } else {
            Self::get_or_connect(state.clone(), connection_id).await?
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

        let semantic_filter_for_hint = semantic_filter.clone();

        if let Some(filter) = semantic_filter {
            request = request.with_filter(filter);
        }

        if let Some(having) = semantic_having {
            request = request.with_having(having);
        }

        let semantic_request = SemanticRequest::Aggregate(request);
        let outcome = Self::execute_aggregate_semantic_request(
            connection.clone(),
            semantic_request,
            database.map(str::to_string),
        )
        .await;

        match outcome {
            Ok((result, sql_text)) => Ok((serialize_query_result(&result), sql_text)),
            Err(failure) => {
                let target = Self::hint_target(&connection, table);
                let aliases: Vec<String> = aggregations
                    .iter()
                    .map(|aggregation| aggregation.alias.clone())
                    .collect();
                let referenced = Self::referenced_columns(
                    semantic_filter_for_hint.as_ref(),
                    order_by,
                    &target,
                    &aliases,
                );

                Err(not_found::explain(
                    FailedCall {
                        state: &state,
                        connection_id,
                        connection: &connection,
                        target: &target,
                        database,
                        columns: referenced,
                    },
                    failure,
                )
                .await)
            }
        }
    }

    async fn execute_aggregate_semantic_request(
        connection: Arc<dyn Connection>,
        semantic_request: SemanticRequest,
        target_database: Option<String>,
    ) -> Result<(QueryResult, Option<String>), CallFailure> {
        tokio::task::spawn_blocking(move || -> Result<_, CallFailure> {
            let plan = connection
                .plan_semantic_request(&semantic_request)
                .map_err(|e| format!("Aggregate planning error: {}", e))?;
            let sql_text = plan.primary_query().map(|q| q.text.clone());
            let planned_query = plan.primary_query().cloned().ok_or_else(|| {
                "Aggregate planning error: driver returned no executable query".to_string()
            })?;

            let mut request = planned_query.into_query_request();
            if request.database.is_none() {
                request = request.with_database(target_database);
            }

            #[allow(clippy::large_enum_variant)]
            let result = connection
                .execute(&request)
                .map_err(|e| CallFailure::from_driver(format!("Aggregate error: {}", e), &e))?;

            Ok((result, sql_text))
        })
        .await
        .map_err(|e| CallFailure::from(format!("Blocking task failed: {}", e)))?
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
        transfer_family: dbflux_core::TransferFamily::Sql,
        deployment_class: None,
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
        ssl_modes: None,
        ssl_cert_fields: None,
        classification_override: None,
        default_chunk_size: None,
        supports_lock_timeout: false,
        editor_profile: None,
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
        .unwrap_err()
        .message;

        assert!(error.contains("Aggregate planning error"));
        assert!(error.contains("aggregate semantics are not supported by this driver"));
    }

    #[cfg(feature = "sqlite")]
    fn build_sqlite_state_read(
        connection_id: &str,
        db_path: &std::path::Path,
    ) -> crate::state::ServerState {
        use crate::connection_cache::ConnectionCache;
        use dbflux_core::{DbConfig, NoopSecretStore, SecretManager};
        use dbflux_driver_sqlite::SqliteDriver;
        use dbflux_mcp::{McpRuntime, TrustedClientDto, builtin_policies, builtin_roles};
        use dbflux_policy::{ConnectionPolicyAssignment, PolicyBindingScope};
        use std::collections::HashMap;
        use tokio::sync::RwLock;

        let audit_path =
            dbflux_audit::temp_sqlite_path(&format!("read_test_{}.sqlite", uuid::Uuid::new_v4()));
        let audit_service =
            dbflux_audit::AuditService::new_sqlite(&audit_path).expect("test audit service");

        let mut runtime = McpRuntime::new(
            audit_service,
            Box::new(dbflux_approval::InMemoryPendingExecutionStore::default()),
        );
        for role in builtin_roles() {
            runtime.upsert_role_mut(role).expect("built-in role setup");
        }
        for policy in builtin_policies() {
            runtime
                .upsert_policy_mut(policy)
                .expect("built-in policy setup");
        }
        runtime
            .upsert_trusted_client_mut(TrustedClientDto {
                id: "test-client".to_string(),
                name: "Test".to_string(),
                issuer: None,
                active: true,
            })
            .expect("trusted client setup");
        runtime
            .save_connection_policy_assignment_mut(dbflux_mcp::ConnectionPolicyAssignmentDto {
                connection_id: connection_id.to_string(),
                assignments: vec![ConnectionPolicyAssignment {
                    actor_id: "test-client".to_string(),
                    scope: PolicyBindingScope {
                        connection_id: connection_id.to_string(),
                    },
                    role_ids: vec!["builtin/admin".to_string()],
                    policy_ids: vec![],
                }],
            })
            .expect("connection policy assignment setup");
        runtime.drain_events();

        let mut profile_manager = dbflux_core::ProfileManager::new_in_memory();
        let profile_id: uuid::Uuid = connection_id.parse().expect("test connection id");
        let mut profile = dbflux_core::ConnectionProfile::new(
            "sqlite-test",
            DbConfig::SQLite {
                path: db_path.to_path_buf(),
                connection_id: None,
            },
        );
        profile.id = profile_id;
        profile_manager.add(profile);

        let mut driver_registry = HashMap::new();
        driver_registry.insert(
            "sqlite".to_string(),
            Arc::new(SqliteDriver) as Arc<dyn dbflux_core::DbDriver>,
        );

        crate::state::ServerState {
            client_id: "test-client".to_string(),
            runtime: Arc::new(RwLock::new(runtime)),
            profile_manager: Arc::new(RwLock::new(profile_manager)),
            auth_profile_manager: Arc::new(RwLock::new(dbflux_core::AuthProfileManager::default())),
            driver_registry: Arc::new(driver_registry),
            auth_provider_registry: Arc::new(HashMap::new()),
            driver_settings: Arc::new(HashMap::new()),
            connection_cache: Arc::new(RwLock::new(ConnectionCache::new())),
            connection_setup_lock: Arc::new(tokio::sync::Mutex::new(())),
            secret_manager: Arc::new(SecretManager::new(Box::new(NoopSecretStore))),
            mcp_enabled_by_default: true,
        }
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn aggregate_data_impl_sql_text_is_some_and_contains_select() {
        let db_file = tempfile::NamedTempFile::new().expect("tempfile");
        let db_path = db_file.path().to_path_buf();
        let connection_id = uuid::Uuid::new_v4().to_string();

        {
            use rusqlite::Connection as RusqliteConnection;
            let conn = RusqliteConnection::open(&db_path).expect("open sqlite");
            conn.execute_batch(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL);
                 INSERT INTO orders (id, amount) VALUES (1, 100), (2, 200);",
            )
            .expect("seed table");
        }

        let state = build_sqlite_state_read(&connection_id, &db_path);

        let aggregations = vec![AggregationSpec {
            function: "COUNT".to_string(),
            column: "id".to_string(),
            alias: "cnt".to_string(),
        }];

        let (_, sql_text) = DbFluxServer::aggregate_data_impl(
            state,
            &connection_id,
            "orders",
            None,
            &[],
            &aggregations,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("aggregate_data_impl should succeed against a seeded SQLite table");

        let sql = sql_text.expect(
            "aggregate_data_impl must return Some(sql) via plan_semantic_request for SQLite",
        );
        assert!(!sql.is_empty(), "audit SQL must not be empty");
        assert!(
            sql.to_uppercase().contains("SELECT"),
            "audit SQL must contain SELECT keyword; got: {}",
            sql
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn select_data_impl_with_joins_returns_the_generated_sql() {
        let db_file = tempfile::NamedTempFile::new().expect("tempfile");
        let db_path = db_file.path().to_path_buf();
        let connection_id = uuid::Uuid::new_v4().to_string();

        {
            use rusqlite::Connection as RusqliteConnection;
            let conn = RusqliteConnection::open(&db_path).expect("open sqlite");
            conn.execute_batch(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
                 CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER);
                 INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Bo');
                 INSERT INTO orders (id, user_id, total) VALUES (10, 1, 30), (11, 2, 5);",
            )
            .expect("seed tables");
        }

        let state = build_sqlite_state_read(&connection_id, &db_path);

        let joins = vec![JoinSpec {
            r#type: "left".to_string(),
            table: "orders".to_string(),
            on: "users.id = o.user_id".to_string(),
            alias: Some("o".to_string()),
            columns: None,
        }];
        let columns = vec!["name".to_string(), "o.total".to_string()];
        let filter = serde_json::json!({ "o.total": { "$gte": 10 } });
        let order_by = vec![OrderByItem {
            column: "o.total".to_string(),
            direction: Some("desc".to_string()),
        }];

        let (result, sql_text) = DbFluxServer::select_data_impl(
            state.clone(),
            &connection_id,
            "users",
            Some(&columns),
            Some(&filter),
            Some(&order_by),
            50,
            0,
            Some(&joins),
            None,
        )
        .await
        .expect("the join runs against SQLite");

        assert_eq!(
            sql_text.as_deref(),
            Some(
                "SELECT \"users\".\"name\", \"o\".\"total\" AS \"o.total\"\n\
                 FROM \"users\"\n\
                 LEFT JOIN \"orders\" AS \"o\" ON \"users\".\"id\" = \"o\".\"user_id\"\n\
                 WHERE \"o\".\"total\" >= 10\n\
                 ORDER BY \"o\".\"total\" DESC\n\
                 LIMIT 50"
            )
        );
        assert_eq!(
            result["rows"],
            serde_json::json!([{ "name": "Ada", "o.total": 30 }])
        );

        let (_, plain_sql) = DbFluxServer::select_data_impl(
            state,
            &connection_id,
            "users",
            None,
            None,
            None,
            50,
            0,
            None,
            None,
        )
        .await
        .expect("the plain select runs against SQLite");

        assert_eq!(plain_sql, None, "a call without joins records no query");
    }
}
