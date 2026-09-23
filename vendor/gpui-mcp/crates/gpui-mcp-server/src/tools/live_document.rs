use super::{
    BridgeResult, GpuiMcp, Json, LiveDocumentSource, Operation, Parameters, ToolRouter, Value,
    json, object_output, tool, tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::time::Instant;

#[derive(Debug, Deserialize, JsonSchema)]
struct PreviewLiveDocumentArgs {
    /// Active document revision this complete edit was based on.
    expected_revision: u64,
    /// Complete standard HTML source for the candidate revision.
    html: String,
    /// Complete standard CSS source for the candidate revision.
    css: String,
    /// Complete versioned binding document encoded as RON.
    bindings_ron: String,
}

#[tool_router(router = live_document_router)]
impl GpuiMcp {
    #[tool(
        description = "Return the active revisioned HTML/CSS/RON preview document; this capability is app opt-in and performs no filesystem access"
    )]
    async fn get_live_document(&self) -> Result<Json<Value>, String> {
        let result = self.call(Operation::GetLiveDocument).await?;
        let BridgeResult::LiveDocument(document) = result else {
            return Err("bridge returned the wrong live document result".to_owned());
        };
        Ok(object_output(json!({ "document": document })))
    }

    #[tool(
        description = "Compile and atomically preview a complete in-memory HTML/CSS/RON document against an expected revision; never writes files and keeps the last-good preview on failure"
    )]
    async fn preview_live_document(
        &self,
        Parameters(args): Parameters<PreviewLiveDocumentArgs>,
    ) -> Result<Json<Value>, String> {
        let started = Instant::now();
        let frame_count = self.frame_stats().await?.frame_count;
        let apply_started = Instant::now();
        let result = self
            .call(Operation::PreviewLiveDocument {
                expected_revision: args.expected_revision,
                source: LiveDocumentSource {
                    html: args.html,
                    css: args.css,
                    bindings_ron: args.bindings_ron,
                },
            })
            .await?;
        let apply_round_trip_ms = apply_started.elapsed().as_secs_f64() * 1_000.0;
        let BridgeResult::LiveDocumentPreview(preview) = result else {
            return Err("bridge returned the wrong live document preview result".to_owned());
        };
        let frame_wait_started = Instant::now();
        if preview.applied {
            self.wait_for_frame(frame_count, std::time::Duration::from_secs(2))
                .await?;
        }
        let frame_wait_ms = frame_wait_started.elapsed().as_secs_f64() * 1_000.0;
        Ok(object_output(json!({
            "ok": preview.applied,
            "preview": preview,
            "timing": {
                "apply_round_trip_ms": apply_round_trip_ms,
                "frame_wait_ms": frame_wait_ms,
                "total_ms": started.elapsed().as_secs_f64() * 1_000.0,
            },
        })))
    }
}

pub(super) fn router() -> ToolRouter<GpuiMcp> {
    GpuiMcp::live_document_router()
}
