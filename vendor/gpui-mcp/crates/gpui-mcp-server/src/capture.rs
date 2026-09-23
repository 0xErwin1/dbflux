use std::time::Duration;

use gpui_mcp_capture::{CaptureGeometry, CaptureTarget, ScreenshotOptions};
use gpui_mcp_protocol::{
    BridgeResult, Capability, Operation, Screenshot, ScreenshotTarget, WindowGeometry,
};
use tokio::time::timeout;

use crate::client::BridgeClient;

const CAPTURE_WORKER_DEADLINE: Duration = Duration::from_secs(15);

/// Capture an instrumented window from the external MCP process.
///
/// Keeping native capture outside the GPUI process is required on Windows, where native window
/// enumeration intentionally excludes windows owned by the calling process. The bridge only
/// supplies the stable process/window identity and current logical client-area geometry.
pub(crate) async fn capture(
    client: &BridgeClient,
    area: ScreenshotTarget,
) -> Result<Screenshot, String> {
    let descriptor = client.descriptor();
    if !descriptor.capabilities.supports(Capability::Screenshot) {
        return Err("the application platform does not expose native window capture".to_owned());
    }
    let window = descriptor
        .native_window_id
        .ok_or_else(|| "the application did not publish a native window identifier".to_owned())?;
    let target = CaptureTarget::new(descriptor.pid, window);
    let geometry = match area {
        ScreenshotTarget::Window => None,
        ScreenshotTarget::Region { .. } => Some(window_geometry(client).await?),
    };
    let options = ScreenshotOptions::new(area, geometry.map(capture_geometry));

    timeout(
        CAPTURE_WORKER_DEADLINE,
        tokio::task::spawn_blocking(move || gpui_mcp_capture::screenshot(target, options)),
    )
    .await
    .map_err(|_| "native screenshot capture exceeded the 15 second deadline".to_owned())?
    .map_err(|error| {
        tracing::warn!(%error, "native screenshot worker failed");
        "native screenshot worker failed".to_owned()
    })?
    .map_err(|error| {
        if let gpui_mcp_capture::CaptureFailure::TargetNotFound {
            window_count,
            pid_matches,
            window_matches,
        } = error
        {
            tracing::warn!(
                process_id = descriptor.pid.get(),
                native_window_id = window.get(),
                window_count,
                pid_matches,
                window_matches,
                "could not resolve exact native capture target"
            );
        }
        error.to_string()
    })
}

async fn window_geometry(client: &BridgeClient) -> Result<WindowGeometry, String> {
    match client.call(Operation::GetWindowGeometry).await? {
        BridgeResult::WindowGeometry(geometry) if geometry.is_valid() => Ok(geometry),
        BridgeResult::WindowGeometry(_) => {
            Err("the application returned invalid window geometry".to_owned())
        }
        _ => Err("the bridge returned the wrong result for window geometry".to_owned()),
    }
}

fn capture_geometry(geometry: WindowGeometry) -> CaptureGeometry {
    CaptureGeometry {
        content_bounds: geometry.content_bounds,
        viewport_size: (
            geometry.content_bounds.width,
            geometry.content_bounds.height,
        ),
        scale_factor: geometry.scale_factor,
    }
}

#[cfg(test)]
mod tests {
    use gpui_mcp_protocol::{Rect, WindowGeometry};

    use super::capture_geometry;

    #[test]
    fn maps_gpui_client_geometry_without_title_or_coordinate_fallbacks() {
        let geometry = WindowGeometry {
            content_bounds: Rect {
                x: 40.0,
                y: 70.0,
                width: 900.0,
                height: 640.0,
            },
            scale_factor: 1.5,
        };

        let capture = capture_geometry(geometry);
        assert_eq!(capture.content_bounds, geometry.content_bounds);
        assert_eq!(capture.viewport_size, (900.0, 640.0));
        assert!((capture.scale_factor - 1.5).abs() < f32::EPSILON);
    }
}
