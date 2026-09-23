use super::{
    BridgeResult, GpuiMcp, Json, Operation, Parameters, SelectAppArgs, ToolRouter, Value, json,
    object_output, tool, tool_router,
};

#[tool_router(router = connection_router)]
impl GpuiMcp {
    #[tool(
        description = "List live instrumented GPUI applications. A single app is selected automatically; when several are live, pass one returned target_id to select_app before using UI tools"
    )]
    async fn list_apps(&self) -> Result<Json<Value>, String> {
        let apps = self.registry.list_apps().await?;
        Ok(object_output(json!({
            "apps": apps,
            "count": apps.len(),
            "selection_required": apps.len() > 1 && apps.iter().all(|app| !app.selected),
        })))
    }

    #[tool(
        description = "Select which live GPUI application subsequent tools control. Use a target_id from list_apps. Selection persists for this initialized MCP transport"
    )]
    async fn select_app(
        &self,
        Parameters(args): Parameters<SelectAppArgs>,
    ) -> Result<Json<Value>, String> {
        if args.target_id.is_empty() || args.target_id.len() > 96 {
            return Err("target_id must contain 1 through 96 characters".to_owned());
        }
        let app = self.select_target(&args.target_id).await?;
        Ok(object_output(json!({
            "selected": app,
        })))
    }

    #[tool(
        description = "Check the selected GPUI bridge connection and return application identity and capabilities. Automatically selects the app when exactly one is live"
    )]
    async fn ping(&self) -> Result<Json<Value>, String> {
        let client = self.client().await?;
        let result = client.call(Operation::Ping).await?;
        let BridgeResult::Pong {
            app_id,
            pid,
            protocol_version,
        } = result
        else {
            return Err("bridge returned the wrong result for ping".to_owned());
        };
        Ok(object_output(json!({
            "ok": true,
            "app_id": app_id,
            "target_id": client.target_id(),
            "pid": pid,
            "protocol_version": protocol_version,
            "capabilities": client.descriptor().capabilities,
        })))
    }

    #[tool(description = "Alias for ping; diagnose whether the selected GPUI endpoint is live")]
    async fn check_connection(&self) -> Result<Json<Value>, String> {
        self.ping().await
    }
}

pub(super) fn router() -> ToolRouter<GpuiMcp> {
    GpuiMcp::connection_router()
}
