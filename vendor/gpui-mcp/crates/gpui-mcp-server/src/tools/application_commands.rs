use super::{
    BridgeResult, GpuiMcp, Json, Operation, Parameters, ToolRouter, Value, json, object_output,
    tool, tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value as JsonValue;

#[derive(Debug, Deserialize, JsonSchema)]
struct ExecuteApplicationCommandArgs {
    /// Exact command name returned by `list_app_commands`.
    name: String,
    /// JSON arguments conforming to the command's advertised input schema.
    arguments: JsonValue,
}

#[tool_router(router = application_command_router)]
impl GpuiMcp {
    #[tool(
        description = "List bounded structured commands advertised by the connected GPUI application, including each command's JSON input schema and mutation status"
    )]
    async fn list_app_commands(&self) -> Result<Json<Value>, String> {
        let result = self.call(Operation::ListApplicationCommands).await?;
        let BridgeResult::ApplicationCommands(commands) = result else {
            return Err("bridge returned the wrong application command list".to_owned());
        };
        Ok(object_output(json!({ "commands": commands })))
    }

    #[tool(
        description = "Execute one exact application-owned command with structured JSON arguments on GPUI's UI thread; inspect list_app_commands first and include the command's expected revision when its schema requires one"
    )]
    async fn execute_app_command(
        &self,
        Parameters(args): Parameters<ExecuteApplicationCommandArgs>,
    ) -> Result<Json<Value>, String> {
        let result = self
            .call(Operation::ExecuteApplicationCommand {
                name: args.name,
                arguments: args.arguments,
            })
            .await?;
        let BridgeResult::ApplicationCommand(result) = result else {
            return Err("bridge returned the wrong application command result".to_owned());
        };
        self.settle_after_refresh(std::time::Duration::from_secs(2))
            .await?;
        Ok(object_output(json!({ "ok": true, "result": result })))
    }
}

pub(super) fn router() -> ToolRouter<GpuiMcp> {
    GpuiMcp::application_command_router()
}
