use super::{
    BridgeResult, Duration, FrameReportArgs, GpuiMcp, Json, LogsArgs, Operation, Parameters,
    RecordPerformanceArgs, ToolRouter, Value, ack_json, average_render_work_ms, encode_error, json,
    object_output, performance_assessment, sleep, tool, tool_router, validate_timeout,
};

/// Per-frame samples `record_performance` returns alongside its summary.
const RECORDED_FRAME_LIMIT: u16 = 64;

#[tool_router(router = diagnostics_router)]
impl GpuiMcp {
    #[tool(
        description = "Return frame timing averages since the last mark_frames: GPUI's whole draw (draw_*), the bridge's own share of it (bridge_*), prepaint, paint, and observed repaint cadence; cadence is not FPS capacity for an event-driven UI"
    )]
    async fn get_frame_stats(&self) -> Result<Json<Value>, String> {
        let stats = self.frame_stats().await?;
        Ok(object_output(
            serde_json::to_value(stats).map_err(encode_error)?,
        ))
    }

    #[tool(
        description = "Start a frame measurement window: wait for frames already pending, then mark the last completed frame. get_frame_stats and get_frame_report then cover only frames completed after the mark."
    )]
    async fn mark_frames(&self) -> Result<Json<Value>, String> {
        self.settle_pending(Duration::from_secs(2)).await?;
        let BridgeResult::FrameStats(stats) = self.call(Operation::MarkFrames).await? else {
            return Err("bridge returned the wrong result for a frame mark".to_owned());
        };
        Ok(object_output(json!({
            "mark_frame_count": stats.mark_frame_count,
            "frame_stats": stats,
        })))
    }

    #[tool(
        description = "Report every frame completed after the last mark_frames (or after since_frame_count): per-frame GPUI draw time, the application's share (app_draw_ms) and the bridge's (bridge_ms), p50/p95/max, and which views rendered, why, and which replayed from cache"
    )]
    async fn get_frame_report(
        &self,
        Parameters(args): Parameters<FrameReportArgs>,
    ) -> Result<Json<Value>, String> {
        let report = self
            .frame_report(args.since_frame_count, args.frame_limit)
            .await?;
        Ok(object_output(
            serde_json::to_value(report).map_err(encode_error)?,
        ))
    }

    #[tool(
        description = "Observe frames over a bounded interval and report before/after statistics plus per-frame samples, percentiles, and view-cache activity for exactly the frames drawn in the interval"
    )]
    async fn record_performance(
        &self,
        Parameters(args): Parameters<RecordPerformanceArgs>,
    ) -> Result<Json<Value>, String> {
        validate_timeout(args.duration_ms)?;
        let before = self.frame_stats().await?;
        sleep(Duration::from_millis(args.duration_ms)).await;
        let after = self.frame_stats().await?;
        let report = self
            .frame_report(Some(before.frame_count), RECORDED_FRAME_LIMIT)
            .await?;
        Ok(object_output(json!({
            "duration_ms": args.duration_ms,
            "before": before,
            "after": after,
            "observed_frame_delta": after.frame_count.saturating_sub(before.frame_count),
            "frames": report,
            "cadence_note": "Event-driven applications repaint only when needed; a low observed cadence while idle is healthy and is not an FPS-capacity measurement.",
        })))
    }

    #[tool(description = "Return a concise current performance report")]
    async fn get_performance_report(&self) -> Result<Json<Value>, String> {
        let stats = self.frame_stats().await?;
        Ok(object_output(json!({
            "frame_stats": stats,
            "assessment": performance_assessment(&stats),
            "average_render_work_ms": average_render_work_ms(&stats),
            "average_draw_ms": stats.draw_average_ms,
            "average_bridge_ms": stats.bridge_average_ms,
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
