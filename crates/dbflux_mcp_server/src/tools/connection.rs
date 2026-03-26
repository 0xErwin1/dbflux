use rmcp::{
    ErrorData,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content},
    schemars::JsonSchema,
    tool, tool_router,
};
use serde::Deserialize;

use crate::{helper::IntoErrorData, server::DbFluxServer};

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ConnectParams {
    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DisconnectParams {
    #[schemars(description = "Connection ID to disconnect")]
    pub connection_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetConnectionInfoParams {
    #[schemars(description = "Connection ID to get info for")]
    pub connection_id: String,
}

#[tool_router(router = connection_router, vis = "pub")]
impl DbFluxServer {
    #[tool(description = "List all available database connections configured in DBFlux")]
    async fn list_connections(&self) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.governance.state.clone();
        self.governance
            .authorize_and_execute(
                "list_connections",
                None,
                ExecutionClassification::Metadata,
                move || async move {
                    let pm = state.profile_manager.read().await;
                    let runtime = state.runtime.read().await;
                    let client_id = &state.client_id;

                    // Only show connections where this client has an assignment
                    let connections: Vec<serde_json::Value> = pm
                        .profiles
                        .iter()
                        .filter(|profile| {
                            // Check if profile has MCP enabled
                            let mcp_enabled = profile
                                .mcp_governance
                                .as_ref()
                                .map(|g| g.enabled)
                                .unwrap_or(state.mcp_enabled_by_default);

                            if !mcp_enabled {
                                return false;
                            }

                            // Check if client has an assignment for this connection
                            runtime
                                .policy_assignments_for_engine()
                                .iter()
                                .any(|assignment| {
                                    assignment.actor_id == *client_id
                                        && assignment.scope.connection_id == profile.id.to_string()
                                })
                        })
                        .map(|profile| {
                            serde_json::json!({
                                "id": profile.id.to_string(),
                                "name": profile.name,
                                "driver_id": profile.driver_id(),
                                "kind": format!("{:?}", profile.kind()),
                            })
                        })
                        .collect();

                    let json_output = serde_json::json!({ "connections": connections });
                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&json_output).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Connect to a database using a configured connection")]
    async fn connect(
        &self,
        Parameters(params): Parameters<ConnectParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();

        self.governance
            .authorize_and_execute(
                "connect",
                Some(&params.connection_id),
                ExecutionClassification::Metadata,
                move || async move {
                    Self::connect_cached(state, &connection_id)
                        .await
                        .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&serde_json::json!({
                            "success": true,
                            "message": format!("Connected to {}", connection_id)
                        }))
                        .unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Disconnect from a database connection")]
    async fn disconnect(
        &self,
        Parameters(params): Parameters<DisconnectParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let connection_id = params.connection_id.clone();

        self.governance
            .authorize_and_execute(
                "disconnect",
                Some(&params.connection_id),
                ExecutionClassification::Metadata,
                move || async move {
                    {
                        let mut cache = state.connection_cache.write().await;
                        cache.remove(&connection_id);
                    }

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&serde_json::json!({
                            "success": true,
                            "message": format!("Disconnected from {}", connection_id)
                        }))
                        .unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(
        description = "Get information about a database connection (version, server info, status)"
    )]
    async fn get_connection_info(
        &self,
        Parameters(params): Parameters<GetConnectionInfoParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_core::QueryRequest;
        use dbflux_policy::ExecutionClassification;

        log::debug!("get_connection_info handler: START");
        let state = self.state.clone();
        let connection_id = params.connection_id.clone();

        self.governance
            .authorize_and_execute(
                "get_connection_info",
                Some(&params.connection_id),
                ExecutionClassification::Metadata,
                move || async move {
                    log::debug!("get_connection_info handler: inside governance block, about to get_or_connect");
                    let conn = Self::get_or_connect(state.clone(), &connection_id)
                        .await
                        .map_err(|e| e.into_error_data())?;

                    let metadata = conn.metadata();
                    let driver_type = format!("{:?}", conn.kind());
                    let category = metadata.category;
                    let current_database = conn.active_database();
                    let conn_for_blocking = conn.clone();

                    log::debug!("get_connection_info handler: about to spawn blocking task for version query");
                    let blocking_result: Result<Option<String>, tokio::task::JoinError> = tokio::task::spawn_blocking(move || {
                        let version_query = conn_for_blocking.version_query();
                        log::debug!("get_connection_info blocking: got version_query = {}", version_query);

                        let result = conn_for_blocking.execute(&QueryRequest {
                            sql: version_query.to_string(),
                            params: Vec::new(),
                            limit: Some(1),
                            offset: None,
                            statement_timeout: None,
                            database: None,
                        });
                        log::debug!("get_connection_info blocking: execute returned, result.is_ok() = {}", result.is_ok());

                        result
                            .ok()
                            .and_then(|r| {
                                log::debug!("get_connection_info blocking: result rows = {}", r.rows.len());
                                if !r.rows.is_empty() && !r.rows[0].is_empty() {
                                    let v = format!("{:?}", r.rows[0][0]);
                                    log::debug!("get_connection_info blocking: version = {}", v);
                                    Some(v)
                                } else {
                                    log::debug!("get_connection_info blocking: rows empty or [0] empty");
                                    None
                                }
                            })
                    })
                    .await;
                    log::debug!("get_connection_info handler: spawn_blocking .await completed, is_ok = {}", blocking_result.is_ok());
                    let version_info = blocking_result
                        .map_err(|e| format!("Blocking task failed: {}", e))
                        .ok()
                        .flatten();
                    log::debug!("get_connection_info handler: version_info = {:?}", version_info);

                    let mut info = serde_json::json!({
                        "connection_id": connection_id,
                        "driver_type": driver_type,
                        "category": format!("{:?}", category),
                        "status": "connected",
                    });

                    if let Some(version) = version_info {
                        info["version"] = serde_json::Value::String(version);
                    }

                    if let Some(db) = current_database {
                        info["current_database"] = serde_json::Value::String(db);
                    }

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&info).unwrap(),
                    )]))
                },
            )
            .await
    }
}
