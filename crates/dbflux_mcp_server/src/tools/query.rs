use std::sync::Arc;

use dbflux_core::{
    Connection, ExplainRequest, SemanticRequest, TableRef, classify_query_for_governance,
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
#[allow(dead_code)] // Used by explain_query tool via #[tool] macro
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

#[tool_router(router = query_router, vis = "pub")]
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
        let connection =
            Self::query_connection_for_database(state, connection_id, database).await?;
        let request = Self::build_explain_request(&connection, sql, table)?;
        let result = Self::run_explain_request(connection, request, "Explain").await?;

        Ok(serialize_query_result(&result))
    }

    async fn preview_mutation_impl(
        state: ServerState,
        connection_id: &str,
        sql: &str,
        database: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let connection =
            Self::query_connection_for_database(state, connection_id, database).await?;
        let request = Self::build_explain_request(&connection, Some(sql), None)?;
        let result = Self::run_explain_request(connection, request, "Preview").await?;

        Ok(serialize_query_result(&result))
    }

    async fn query_connection_for_database(
        state: ServerState,
        connection_id: &str,
        database: Option<&str>,
    ) -> Result<Arc<dyn Connection>, String> {
        if let Some(target_db) = database {
            let current_db = Self::get_current_database(&state, connection_id).await?;

            if target_db != current_db.as_deref().unwrap_or("") {
                Self::connect_with_database(state, connection_id, target_db).await
            } else {
                Self::get_or_connect(state, connection_id).await
            }
        } else {
            Self::get_or_connect(state, connection_id).await
        }
    }

    fn build_explain_request(
        connection: &Arc<dyn Connection>,
        sql: Option<&str>,
        table: Option<&str>,
    ) -> Result<ExplainRequest, String> {
        if sql.is_none() && table.is_none() {
            return Err("Either 'sql' or 'table' parameter is required".to_string());
        }

        let table_ref = table
            .map(|table| Self::query_table_ref_for_connection(connection, table))
            .unwrap_or_else(|| TableRef::new("__dbflux_explain__"));

        let mut request = ExplainRequest::new(table_ref);

        if let Some(query) = sql {
            request = request.with_query(query);
        }

        Ok(request)
    }

    async fn run_explain_request(
        connection: Arc<dyn Connection>,
        request: ExplainRequest,
        error_prefix: &'static str,
    ) -> Result<dbflux_core::QueryResult, String> {
        tokio::task::spawn_blocking(move || {
            match connection.plan_semantic_request(&SemanticRequest::Explain(request.clone())) {
                Ok(plan) => {
                    let planned_query = plan.primary_query().cloned().ok_or_else(|| {
                        format!(
                            "{} planning error: driver returned no executable query",
                            error_prefix
                        )
                    })?;

                    let classification =
                        classify_query_for_governance(&planned_query.language, &planned_query.text);
                    if !matches!(
                        classification,
                        dbflux_policy::ExecutionClassification::Metadata
                            | dbflux_policy::ExecutionClassification::Read
                    ) {
                        return Err(format!(
                            "{} planning error: driver generated a non-read-only preview query",
                            error_prefix
                        ));
                    }

                    connection
                        .execute(&planned_query.into_query_request())
                        .map_err(|e| format!("{} error: {}", error_prefix, e))
                }
                Err(dbflux_core::DbError::NotSupported(_)) => connection
                    .explain(&request)
                    .map_err(|e| format!("{} error: {}", error_prefix, e)),
                Err(e) => Err(format!("{} planning error: {}", error_prefix, e)),
            }
        })
        .await
        .map_err(|e| format!("Blocking task failed: {}", e))?
    }

    fn query_table_ref_for_connection(connection: &Arc<dyn Connection>, table: &str) -> TableRef {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use dbflux_core::{
        DbError, DbKind, DefaultSqlDialect, DriverCapabilities, DriverMetadata, ExplainRequest,
        MutationCapabilities, PlaceholderStyle, QueryCapabilities, QueryHandle, QueryLanguage,
        QueryResult, SchemaLoadingStrategy, SchemaSnapshot, SemanticPlan, SemanticPlanKind,
        SyntaxInfo, TransactionCapabilities,
    };
    use std::sync::LazyLock;

    static TEST_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
        id: "test".into(),
        display_name: "Test".into(),
        description: "Test driver".into(),
        category: dbflux_core::DatabaseCategory::Relational,
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
    });

    struct TestConnection {
        plan: Option<SemanticPlan>,
        planner_supported: bool,
        explain_result: QueryResult,
        execute_result: QueryResult,
        executed_queries: std::sync::Mutex<Vec<String>>,
        explain_queries: std::sync::Mutex<Vec<Option<String>>>,
    }

    impl TestConnection {
        fn new(plan: Option<SemanticPlan>, planner_supported: bool) -> Self {
            Self {
                plan,
                planner_supported,
                explain_result: QueryResult::empty(),
                execute_result: QueryResult::empty(),
                executed_queries: std::sync::Mutex::new(Vec::new()),
                explain_queries: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl Connection for TestConnection {
        fn metadata(&self) -> &DriverMetadata {
            &TEST_METADATA
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, req: &dbflux_core::QueryRequest) -> Result<QueryResult, DbError> {
            self.executed_queries
                .lock()
                .expect("executed queries mutex poisoned")
                .push(req.sql.clone());
            Ok(self.execute_result.clone())
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

        fn explain(&self, request: &ExplainRequest) -> Result<QueryResult, DbError> {
            self.explain_queries
                .lock()
                .expect("explain queries mutex poisoned")
                .push(request.query.clone());
            Ok(self.explain_result.clone())
        }

        fn plan_semantic_request(
            &self,
            _request: &SemanticRequest,
        ) -> Result<SemanticPlan, DbError> {
            if self.planner_supported {
                self.plan.clone().ok_or_else(|| {
                    DbError::NotSupported("planner returned no semantic plan".into())
                })
            } else {
                Err(DbError::NotSupported("no plan".into()))
            }
        }
    }

    #[test]
    fn build_explain_request_uses_default_schema_for_tables() {
        let connection: Arc<dyn Connection> = Arc::new(TestConnection::new(None, false));

        let request =
            DbFluxServer::build_explain_request(&connection, None, Some("users")).unwrap();

        assert_eq!(request.table.schema.as_deref(), Some("public"));
        assert_eq!(request.table.name, "users");
        assert!(request.query.is_none());
    }

    #[tokio::test]
    async fn run_explain_request_executes_driver_plan_when_available() {
        let connection = Arc::new(TestConnection::new(
            Some(SemanticPlan::single_query(
                SemanticPlanKind::Query,
                dbflux_core::PlannedQuery::new(QueryLanguage::Sql, "EXPLAIN SELECT 1"),
            )),
            true,
        ));

        DbFluxServer::run_explain_request(
            connection.clone(),
            ExplainRequest::new(TableRef::new("users")).with_query("SELECT 1"),
            "Explain",
        )
        .await
        .unwrap();

        assert_eq!(
            connection
                .executed_queries
                .lock()
                .expect("executed queries mutex poisoned")
                .as_slice(),
            ["EXPLAIN SELECT 1"]
        );
        assert!(
            connection
                .explain_queries
                .lock()
                .expect("explain queries mutex poisoned")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn run_explain_request_falls_back_to_legacy_explain_when_planning_is_unsupported() {
        let connection = Arc::new(TestConnection::new(None, false));
        let request = ExplainRequest::new(TableRef::new("users"))
            .with_query("UPDATE users SET active = true");

        DbFluxServer::run_explain_request(connection.clone(), request, "Preview")
            .await
            .unwrap();

        assert!(
            connection
                .executed_queries
                .lock()
                .expect("executed queries mutex poisoned")
                .is_empty()
        );
        assert_eq!(
            connection
                .explain_queries
                .lock()
                .expect("explain queries mutex poisoned")
                .as_slice(),
            [Some("UPDATE users SET active = true".to_string())]
        );
    }

    #[tokio::test]
    async fn run_explain_request_rejects_non_read_only_plans() {
        let connection = Arc::new(TestConnection::new(
            Some(SemanticPlan::single_query(
                SemanticPlanKind::Query,
                dbflux_core::PlannedQuery::new(
                    QueryLanguage::Sql,
                    "UPDATE users SET active = true",
                ),
            )),
            true,
        ));

        let error = DbFluxServer::run_explain_request(
            connection.clone(),
            ExplainRequest::new(TableRef::new("users")).with_query("SELECT 1"),
            "Preview",
        )
        .await
        .expect_err("non-read-only preview plans must be rejected");

        assert!(error.contains("non-read-only preview query"));
        assert!(
            connection
                .executed_queries
                .lock()
                .expect("executed queries mutex poisoned")
                .is_empty()
        );
    }
}
