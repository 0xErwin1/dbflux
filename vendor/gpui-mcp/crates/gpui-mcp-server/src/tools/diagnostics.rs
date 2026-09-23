use super::{
    BridgeResult, Duration, GpuiMcp, Json, LogsArgs, Operation, Parameters, RecordPerformanceArgs,
    ToolRouter, Value, ack_json, encode_error, json, object_output, performance_assessment, sleep,
    tool, tool_router, validate_timeout,
};

#[tool_router(router = diagnostics_router)]
impl GpuiMcp {
    #[tool(
        description = "Return bounded render-phase timings and observed repaint cadence from the semantic root wrapper; cadence is not FPS capacity for an event-driven UI"
    )]
    async fn get_frame_stats(&self) -> Result<Json<Value>, String> {
        let stats = self.frame_stats().await?;
        Ok(object_output(
            serde_json::to_value(stats).map_err(encode_error)?,
        ))
    }

    #[tool(
        description = "Observe frame statistics over a bounded interval and report before/after samples"
    )]
    async fn record_performance(
        &self,
        Parameters(args): Parameters<RecordPerformanceArgs>,
    ) -> Result<Json<Value>, String> {
        validate_timeout(args.duration_ms)?;
        let before = self.frame_stats().await?;
        sleep(Duration::from_millis(args.duration_ms)).await;
        let after = self.frame_stats().await?;
        Ok(object_output(json!({
            "duration_ms": args.duration_ms,
            "before": before,
            "after": after,
            "observed_frame_delta": after.frame_count.saturating_sub(before.frame_count),
            "cadence_note": "Event-driven applications repaint only when needed; a low observed cadence while idle is healthy and is not an FPS-capacity measurement.",
        })))
    }

    #[tool(description = "Return a concise current performance report")]
    async fn get_performance_report(&self) -> Result<Json<Value>, String> {
        let stats = self.frame_stats().await?;
        Ok(object_output(json!({
            "frame_stats": stats,
            "assessment": performance_assessment(&stats),
            "average_render_work_ms": stats.prepaint_average_ms + stats.root_paint_average_ms,
            "cadence_note": "estimated_fps is observed repaint cadence, not rendering capacity for an event-driven UI",
        })))
    }

    #[tool(
        description = "Return bounded, application-published diagnostic logs; secrets must not be published by the app"
    )]
    async fn get_logs(
        &self,
        Parameters(args): Parameters<LogsArgs>,
    ) -> Result<Json<Value>, String> {
        let result = self
            .call(Operation::GetLogs {
                limit: args.limit,
                min_level: args.min_level,
            })
            .await?;
        let BridgeResult::Logs(logs) = result else {
            return Err("bridge returned the wrong result for logs".to_owned());
        };
        Ok(object_output(
            json!({ "count": logs.len(), "entries": logs }),
        ))
    }

    #[tool(description = "Clear all retained application-published diagnostic logs")]
    async fn clear_logs(&self) -> Result<Json<Value>, String> {
        self.ack(Operation::ClearLogs).await?;
        Ok(ack_json("logs_cleared"))
    }
}

pub(super) fn router() -> ToolRouter<GpuiMcp> {
    GpuiMcp::diagnostics_router()
}
