use std::time::{Duration, Instant};

use gpui_mcp_capture::{CaptureFailure, CaptureTarget, LiveFrameStream};
use tokio::time::timeout;

use crate::recording::{FrameTiming, LiveRecorder, MAX_RECORDING_FRAMES, validate_artifact_name};

use super::{
    CallToolResult, Capability, CaptureNamedArgs, CompareImagesArgs, ElementArgs, GpuiMcp,
    Highlight, HighlightArgs, Json, MAX_IMAGE_SNAPSHOTS, Operation, Parameters, Point, Rect,
    RegionArgs, ScreenshotTarget, StartVideoRecordingArgs, ToolRouter, Value, ack_json,
    compare_images, get_node, image_result, object_output, require_bounds, tool, tool_router,
    validate_name,
};

const RECORDING_STOP_DEADLINE: Duration = Duration::from_secs(15);
const POINTER_CURSOR: [&[u8]; 24] = [
    b"#",
    b"##",
    b"#W#",
    b"#WW#",
    b"#WWW#",
    b"#WWWW#",
    b"#WWWWW#",
    b"#WWWWWW#",
    b"#WWWWWWW#",
    b"#WWWWWWWW#",
    b"#WWWWWWWWW#",
    b"#WWWWWWWWWW#",
    b"#WWWWWWWWWWW#",
    b"#WWWWWW######",
    b"#WWWW#",
    b"#WWW#W#",
    b"#WW#.#W#",
    b"#W#..#WW#",
    b"##....#WW#",
    b"#......#WW#",
    b".......#WW#",
    b"........#W#",
    b".........##",
    b"..........#",
];

#[tool_router(router = visual_router)]
impl GpuiMcp {
    #[tool(description = "Capture the full GPUI application window as an in-memory PNG")]
    async fn screenshot(&self) -> Result<CallToolResult, String> {
        self.capture(ScreenshotTarget::Window)
            .await
            .map(image_result)
    }

    #[tool(description = "Capture a window-relative logical rectangle as an in-memory PNG")]
    async fn screenshot_region(
        &self,
        Parameters(args): Parameters<RegionArgs>,
    ) -> Result<CallToolResult, String> {
        self.capture(ScreenshotTarget::Region {
            rect: Rect {
                x: args.x,
                y: args.y,
                width: args.width,
                height: args.height,
            },
        })
        .await
        .map(image_result)
    }

    #[tool(description = "Capture one semantic element's current bounds as an in-memory PNG")]
    async fn screenshot_element(
        &self,
        Parameters(args): Parameters<ElementArgs>,
    ) -> Result<CallToolResult, String> {
        let tree = self.tree().await?;
        let rect = require_bounds(get_node(&tree, &args.id)?)?;
        self.capture(ScreenshotTarget::Region { rect })
            .await
            .map(image_result)
    }

    #[tool(description = "Outline one or more semantic elements inside the GPUI window")]
    async fn highlight_elements(
        &self,
        Parameters(args): Parameters<HighlightArgs>,
    ) -> Result<Json<Value>, String> {
        if args.ids.is_empty() || args.ids.len() > 64 {
            return Err("ids must contain between 1 and 64 elements".to_owned());
        }
        let tree = self.tree().await?;
        let highlights = args
            .ids
            .iter()
            .map(|id| {
                let node = get_node(&tree, id)?;
                Ok(Highlight {
                    rect: require_bounds(node)?,
                    color: args.color.clone(),
                    label: node.label.clone().or_else(|| Some(id.clone())),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        self.ack_after_frame(Operation::SetHighlights { highlights })
            .await?;
        Ok(ack_json("highlighted"))
    }

    #[tool(description = "Clear all MCP visual outlines from the GPUI window")]
    async fn clear_highlights(&self) -> Result<Json<Value>, String> {
        self.ack_after_frame(Operation::ClearHighlights).await?;
        Ok(ack_json("highlights_cleared"))
    }

    #[tool(description = "Capture and save a full-window PNG under a bounded in-memory name")]
    async fn capture_screenshot_snapshot(
        &self,
        Parameters(args): Parameters<CaptureNamedArgs>,
    ) -> Result<CallToolResult, String> {
        validate_name(&args.name)?;
        let screenshot = self.capture(ScreenshotTarget::Window).await?;
        let mut snapshots = self.snapshots.write().await;
        if !snapshots.images.contains_key(&args.name)
            && snapshots.images.len() >= MAX_IMAGE_SNAPSHOTS
        {
            return Err("image snapshot capacity (8) has been reached".to_owned());
        }
        snapshots.images.insert(args.name, screenshot.clone());
        Ok(image_result(screenshot))
    }

    #[tool(
        description = "Compare two in-memory PNG snapshots and return deterministic pixel metrics"
    )]
    async fn compare_screenshots(
        &self,
        Parameters(args): Parameters<CompareImagesArgs>,
    ) -> Result<Json<Value>, String> {
        let (left, right) = self.stored_images(&args.left, &args.right).await?;
        let metrics = compare_images(&left, &right, args.tolerance)?.0;
        Ok(object_output(metrics))
    }

    #[tool(description = "Render an in-memory PNG diff for two saved screenshot snapshots")]
    async fn diff_screenshots(
        &self,
        Parameters(args): Parameters<CompareImagesArgs>,
    ) -> Result<CallToolResult, String> {
        let (left, right) = self.stored_images(&args.left, &args.right).await?;
        let (metrics, diff) = compare_images(&left, &right, args.tolerance)?;
        let mut result = image_result(diff);
        result.structured_content = Some(metrics);
        Ok(result)
    }

    #[tool(
        description = "Begin a continuous native-window recording and encode raw RGBA frames directly into H.264/MP4 as they arrive. artifact_name is validated before capture and stays inside the configured artifact directory. Keep this MCP transport open until stop_video_recording."
    )]
    async fn start_video_recording(
        &self,
        Parameters(args): Parameters<StartVideoRecordingArgs>,
    ) -> Result<Json<Value>, String> {
        validate_artifact_name(&args.artifact_name)?;
        let timing = FrameTiming::frames_per_second(args.frames_per_second)?;
        let _transition = self.target_transition.lock().await;
        let target = self.client().await?;
        let descriptor = target.descriptor();
        if !descriptor.capabilities.supports(Capability::Screenshot) {
            return Err(
                "the application platform does not expose native window capture".to_owned(),
            );
        }
        let window = descriptor.native_window_id.ok_or_else(|| {
            "the application did not publish a native window identifier".to_owned()
        })?;
        let capture_target = CaptureTarget::new(descriptor.pid, window);
        if self
            .recording_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
        {
            return Err("a video recording is already active".to_owned());
        }

        let pointer = self.current_pointer_location().await?;
        *self
            .pointer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = pointer;
        let root = largest_root_bounds(&self.tree().await?);
        let include_pointer = args.include_pointer;
        let pointer_state = self.pointer.clone();
        let (stream, first) = tokio::task::spawn_blocking(move || {
            let mut stream = LiveFrameStream::open(capture_target, args.frames_per_second)
                .map_err(|error| error.to_string())?;
            let frame = stream
                .next_frame(Duration::from_secs(5))
                .map_err(|error| error.to_string())?;
            Ok::<_, String>((stream, frame))
        })
        .await
        .map_err(|error| format!("native video capture worker failed: {error}"))??;
        // Opening a native capture session can cause a platform pointer notification. Reapply
        // GPUI's logical pointer once after the persistent stream is established; the stream
        // remains open for the rest of recording and does not churn capture sessions per frame.
        self.dispatch_pointer_input(super::PointerCommand::MouseMove {
            point: pointer,
            pressed_button: None,
        })
        .await?;
        let mut encoded_first = first.clone();
        if include_pointer && let Some(root) = root {
            draw_pointer(&mut encoded_first, pointer, root);
        }
        let recorder = LiveRecorder::start(
            &self.artifacts,
            &args.artifact_name,
            args.overwrite,
            timing,
            encoded_first,
        )?;

        let session_id = self
            .recording_session
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |current| current.checked_add(1),
            )
            .map_err(|_| "recording session identifier capacity was exhausted".to_owned())?
            + 1;
        let cancellation = super::CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let fps = args.frames_per_second;
        let join = tokio::task::spawn_blocking(move || {
            VideoCaptureTask {
                recorder,
                stream,
                latest_frame: first,
                worker_cancellation,
                pointer: pointer_state,
                root,
                include_pointer,
                frames_per_second: fps,
            }
            .run()
        });
        *self
            .recording_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(super::RecordingTask {
            cancellation,
            join,
            session_id,
        });

        Ok(object_output(serde_json::json!({
            "ok": true,
            "action": "video_recording_started",
            "session_id": session_id,
            "target_id": target.target_id(),
            "artifact_name": args.artifact_name,
            "format": "mp4",
            "codec": "h264",
            "pointer_overlay": args.include_pointer,
            "coordinate_space": "window_logical_pixels",
            "capture_mode": "continuous_native_frames",
            "frames_per_second": args.frames_per_second,
            "maximum_duration_seconds": MAX_RECORDING_FRAMES / 30,
            "artifact_directory": self.artifacts.directory().display().to_string(),
        })))
    }

    #[tool(
        description = "Stop the active live recording, finalize its already-streamed H.264/MP4 container, and atomically install the artifact selected at start."
    )]
    async fn stop_video_recording(&self) -> Result<Json<Value>, String> {
        let mut task = self
            .recording_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| "no active video recording".to_owned())?;
        let session_id = task.session_id;
        task.cancellation.cancel();
        let artifact = match timeout(RECORDING_STOP_DEADLINE, &mut task.join).await {
            Ok(Ok(result)) => result?,
            Ok(Err(error)) => {
                return Err(format!("live video recording worker failed: {error}"));
            }
            Err(_) => {
                task.join.abort();
                return Err("live video recording did not finalize within 15 seconds".to_owned());
            }
        };
        Ok(object_output(serde_json::json!({
            "path": artifact.path.display().to_string(),
            "artifact_name": artifact
                .path
                .file_name()
                .map_or_else(String::new, |name| name.to_string_lossy().into_owned()),
            "session_id": session_id,
            "frames": artifact.frame_count,
            "timeline_frames": artifact.timeline_frames,
            "frames_per_second": artifact.timing.configured_frames_per_second(),
            "duration_ms": artifact.duration_ms,
            "format": "mp4",
            "mime_type": "video/mp4",
            "codec": "h264",
            "width": artifact.width,
            "height": artifact.height,
            "dropped_frames": artifact.dropped_frames,
            "bytes": artifact.bytes,
        })))
    }
}

fn largest_root_bounds(tree: &super::UiTree) -> Option<Rect> {
    tree.roots
        .iter()
        .filter_map(|id| tree.nodes.get(id))
        .filter_map(|node| node.bounds)
        .max_by(|left, right| (left.width * left.height).total_cmp(&(right.width * right.height)))
}

struct VideoCaptureTask {
    recorder: LiveRecorder,
    stream: LiveFrameStream,
    latest_frame: image::RgbaImage,
    worker_cancellation: super::CancellationToken,
    pointer: std::sync::Arc<std::sync::Mutex<Point>>,
    root: Option<Rect>,
    include_pointer: bool,
    frames_per_second: u8,
}

impl VideoCaptureTask {
    fn run(self) -> Result<crate::recording::RecordingArtifact, String> {
        let Self {
            mut recorder,
            mut stream,
            mut latest_frame,
            worker_cancellation,
            pointer,
            root,
            include_pointer,
            frames_per_second,
        } = self;
        let period = Duration::from_nanos(1_000_000_000 / u64::from(frames_per_second));
        let started = Instant::now();
        let mut last_sample_at = started;
        let mut next_frame = Instant::now() + period;
        let mut consecutive_failures = 0_u8;
        while !worker_cancellation.is_cancelled() && !recorder.is_full() {
            let now = Instant::now();
            if now < next_frame {
                std::thread::sleep(next_frame - now);
            }
            if worker_cancellation.is_cancelled() {
                break;
            }
            match stream.next_frame(Duration::from_millis(1)) {
                Ok(frame) => {
                    consecutive_failures = 0;
                    latest_frame = frame;
                }
                Err(CaptureFailure::FrameTimeout) => {}
                Err(error) => {
                    recorder.record_drop();
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if consecutive_failures >= 3 {
                        return Err(format!("native frame stream failed: {error}"));
                    }
                    continue;
                }
            }
            let mut output_frame = latest_frame.clone();
            if include_pointer && let Some(root) = root {
                let point = *pointer
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                draw_pointer(&mut output_frame, point, root);
            }
            let sample_at = Instant::now();
            let elapsed_nanos = sample_at.duration_since(last_sample_at).as_nanos();
            let duration_ticks = elapsed_nanos
                .saturating_mul(u128::from(frames_per_second))
                .checked_add(500_000_000)
                .and_then(|value| value.checked_div(1_000_000_000))
                .and_then(|value| u32::try_from(value).ok())
                .unwrap_or(u32::MAX)
                .max(1);
            recorder.push_for(output_frame, duration_ticks)?;
            last_sample_at = sample_at;
            next_frame += period;
            let now = Instant::now();
            while next_frame <= now {
                next_frame += period;
                recorder.record_drop();
            }
        }
        recorder.finish()
    }
}

fn draw_pointer(image: &mut image::RgbaImage, pointer: Point, root: Rect) -> bool {
    if root.width <= 0.0 || root.height <= 0.0 {
        return false;
    }
    let scale = f64::from(image.width()) / f64::from(root.width);
    let top_inset = (f64::from(image.height()) - f64::from(root.height) * scale).max(0.0);
    let x = rounded_i32(f64::from(pointer.x - root.x) * scale);
    let y = rounded_i32(top_inset + f64::from(pointer.y - root.y) * scale);
    draw_pointer_marker(image, x, y)
}

fn draw_pointer_marker(image: &mut image::RgbaImage, x: i32, y: i32) -> bool {
    let Ok(width) = i32::try_from(image.width()) else {
        return false;
    };
    let Ok(height) = i32::try_from(image.height()) else {
        return false;
    };
    if !(0..width).contains(&x) || !(0..height).contains(&y) {
        return false;
    }
    for (offset_y, row) in POINTER_CURSOR.iter().enumerate() {
        let Ok(offset_y) = i32::try_from(offset_y) else {
            continue;
        };
        for (offset_x, pixel) in row.iter().copied().enumerate() {
            let color = match pixel {
                b'#' => image::Rgba([16, 18, 24, 255]),
                b'W' => image::Rgba([255, 255, 255, 255]),
                _ => continue,
            };
            let Ok(offset_x) = i32::try_from(offset_x) else {
                continue;
            };
            let pixel_x = x + offset_x;
            let pixel_y = y + offset_y;
            if (0..width).contains(&pixel_x) && (0..height).contains(&pixel_y) {
                let Ok(pixel_x) = u32::try_from(pixel_x) else {
                    continue;
                };
                let Ok(pixel_y) = u32::try_from(pixel_y) else {
                    continue;
                };
                image.put_pixel(pixel_x, pixel_y, color);
            }
        }
    }
    true
}

#[allow(clippy::cast_possible_truncation)]
fn rounded_i32(value: f64) -> i32 {
    if value.is_nan() {
        return 0;
    }
    value
        .round()
        .clamp(f64::from(i32::MIN), f64::from(i32::MAX)) as i32
}

pub(super) fn router() -> ToolRouter<GpuiMcp> {
    GpuiMcp::visual_router()
}

#[cfg(test)]
mod tests {
    use image::{Rgba, RgbaImage};

    use super::draw_pointer_marker;

    #[test]
    fn pointer_marker_is_visible_and_stays_inside_the_frame() {
        let mut image = RgbaImage::from_pixel(32, 32, Rgba([1, 2, 3, 255]));

        assert!(draw_pointer_marker(&mut image, 2, 2));
        assert_eq!(image.get_pixel(2, 2), &Rgba([16, 18, 24, 255]));
        assert_eq!(image.get_pixel(3, 4), &Rgba([255, 255, 255, 255]));
        assert!(!draw_pointer_marker(&mut image, -1, 2));
        assert_eq!(image.get_pixel(0, 0), &Rgba([1, 2, 3, 255]));
    }
}
