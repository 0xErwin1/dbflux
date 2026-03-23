use dbflux_core::QueryRequest;
use rmcp::{
    ErrorData,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content},
    schemars::JsonSchema,
    tool,
};
use serde::Deserialize;

use crate::{
    helper::{IntoErrorData, *},
    server::DbFluxServer,
    state::ServerState,
};

#[derive(Debug, Deserialize, JsonSchema)]
pub struct PreviewMutationParams {
    #[schemars(description = "Connection ID")]
    pub connection_id: String,

    #[schemars(description = "SQL mutation query to preview (INSERT, UPDATE, DELETE, etc.)")]
    pub sql: String,

    #[schemars(description = "Optional database/schema name")]
    pub database: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct ExplainQueryParams {
    #[schemars(description = "Connection ID")]
    pub connection_id: String,

    #[schemars(description = "SQL query to explain (optional, requires table if not provided)")]
    pub sql: Option<String>,

    #[schemars(description = "Table name to explain (optional, requires sql if not provided)")]
    pub table: Option<String>,

    #[schemars(description = "Optional database/schema name")]
    pub database: Option<String>,
}

impl DbFluxServer {
    #[tool(description = "Explain query execution plan or table access strategy")]
    async fn explain_query(
        &self,
        Parameters(params): Parameters<ExplainQueryParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let sql = params.sql.clone();
        let table = params.table.clone();
        let database = params.database.clone();
        let connection_id = params.connection_id.clone();

        self.governance
            .authorize_and_execute(
                "explain_query",
                Some(&params.connection_id),
                ExecutionClassification::Read,
                move || async move {
                    let result = Self::explain_query_impl(
                        state,
                        &connection_id,
                        sql.as_deref(),
                        table.as_deref(),
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

    #[tool(description = "Preview the execution plan for a mutation query without executing it")]
    async fn preview_mutation(
        &self,
        Parameters(params): Parameters<PreviewMutationParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let sql = params.sql.clone();
        let database = params.database.clone();
        let connection_id = params.connection_id.clone();

        self.governance
            .authorize_and_execute(
                "preview_mutation",
                Some(&params.connection_id),
                ExecutionClassification::Read,
                move || async move {
                    let result = Self::preview_mutation_impl(
                        state,
                        &connection_id,
                        &sql,
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

    async fn explain_query_impl(
        state: ServerState,
        connection_id: &str,
        sql: Option<&str>,
        table: Option<&str>,
        database: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;

        let explain_sql = if let Some(query) = sql {
            format!("EXPLAIN {}", query)
        } else if let Some(tbl) = table {
            let dialect = connection.dialect();
            let quoted = dialect.quote_identifier(tbl);
            format!("EXPLAIN SELECT * FROM {} LIMIT 100", quoted)
        } else {
            return Err("Either 'sql' or 'table' parameter is required".to_string());
        };

        let mut request = QueryRequest::new(&explain_sql);
        if let Some(db) = database {
            request = request.with_database(Some(db.to_string()));
        }

        let result = connection
            .execute(&request)
            .map_err(|e| format!("Explain error: {}", e))?;

        Ok(serialize_query_result(&result))
    }

    async fn preview_mutation_impl(
        state: ServerState,
        connection_id: &str,
        sql: &str,
        database: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let connection = Self::get_or_connect(state, connection_id).await?;

        let explain_sql = format!("EXPLAIN {}", sql);

        let mut request = QueryRequest::new(&explain_sql);
        if let Some(db) = database {
            request = request.with_database(Some(db.to_string()));
        }

        let result = connection
            .execute(&request)
            .map_err(|e| format!("Preview error: {}", e))?;

        Ok(serialize_query_result(&result))
    }
}
