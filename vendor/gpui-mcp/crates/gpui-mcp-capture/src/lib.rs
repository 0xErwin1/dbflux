//! Native window capture by stable operating-system identity.

use std::io::Cursor;
use std::time::Duration;
#[cfg(target_os = "windows")]
use std::time::Instant;

use base64::Engine as _;
use gpui_mcp_protocol::{NativeWindowId, ProcessId, Rect, Screenshot, ScreenshotTarget};
use image::{DynamicImage, ImageFormat, RgbaImage};

const MAX_IMAGE_DIMENSION: u32 = 16_384;
const MAX_ENCODED_BYTES: usize = 16 * 1024 * 1024;
#[cfg(any(target_os = "windows", test))]
const COMPOSITOR_POLL_INTERVAL: Duration = Duration::from_millis(16);
const MIN_STABILITY_DEADLINE: Duration = Duration::from_millis(32);
const MAX_STABILITY_DEADLINE: Duration = Duration::from_secs(2);
const DEFAULT_SETTLE_DEADLINE: Duration = Duration::from_secs(1);
#[cfg(any(target_os = "windows", test))]
const MIN_FRESHNESS_SAMPLES: u8 = 3;
/// Ceiling on the extra budget a measured readback may earn.
///
/// The settle deadline bounds how long the capture is willing to *wait* for the
/// compositor; the readbacks themselves are work, and their cost scales with the
/// window's pixel count and with whether the server was built optimized. A
/// 5120x1440 window in a debug build spends most of a second in one readback, so
/// charging that work to a wait budget made every capture of such a window fail.
/// The allowance below credits the remaining samples with what the first one
/// actually cost, which needs no frame-size heuristic because a measured sample
/// already encodes both the size and the build profile. This ceiling keeps a
/// pathologically slow machine from widening the budget without limit.
#[cfg(any(target_os = "windows", test))]
const MAX_MEASURED_ALLOWANCE: Duration = Duration::from_secs(4);

/// Bounded Windows Graphics Capture freshness policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureOptions {
    settle_deadline: Duration,
}

impl CaptureOptions {
    /// Configure the maximum time Windows capture spends obtaining fresh compositor samples.
    ///
    /// # Errors
    ///
    /// Returns [`CaptureFailure::InvalidStabilityDeadline`] unless `deadline` is
    /// between 32 milliseconds and 2 seconds, inclusive.
    pub fn new(deadline: Duration) -> Result<Self, CaptureFailure> {
        if !(MIN_STABILITY_DEADLINE..=MAX_STABILITY_DEADLINE).contains(&deadline) {
            return Err(CaptureFailure::InvalidStabilityDeadline);
        }
        Ok(Self {
            settle_deadline: deadline,
        })
    }

    /// Return the configured compositor freshness deadline.
    #[must_use]
    pub const fn settle_deadline(self) -> Duration {
        self.settle_deadline
    }
}

impl Default for CaptureOptions {
    fn default() -> Self {
        Self {
            settle_deadline: DEFAULT_SETTLE_DEADLINE,
        }
    }
}

/// Logical screenshot selection and native capture policy.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScreenshotOptions {
    area: ScreenshotTarget,
    geometry: Option<CaptureGeometry>,
    capture: CaptureOptions,
}

impl ScreenshotOptions {
    /// Configure one screenshot request.
    #[must_use]
    pub const fn new(area: ScreenshotTarget, geometry: Option<CaptureGeometry>) -> Self {
        Self {
            area,
            geometry,
            capture: CaptureOptions {
                settle_deadline: DEFAULT_SETTLE_DEADLINE,
            },
        }
    }

    /// Set the bounded compositor-settlement policy.
    #[must_use]
    pub const fn settle(mut self, capture: CaptureOptions) -> Self {
        self.capture = capture;
        self
    }
}

/// Native window pixels plus display metadata reported by the operating system.
///
/// The image is bounded by this crate's safety limits and, on Windows, has
/// passed the same compositor-freshness policy used by MCP screenshots.
#[derive(Clone, Debug)]
pub struct NativeFrame {
    /// Captured native RGBA pixels, including any frame retained by the OS API.
    pub image: RgbaImage,
    /// Native global window origin when the platform reports one.
    pub origin: (i32, i32),
    /// Physical pixels per logical display pixel from the native monitor API.
    pub scale_factor: f32,
    /// Window dimensions reported by the native window API, when available.
    pub reported_size: (u32, u32),
}

/// GPUI client-area geometry used to map logical element coordinates into a decorated native
/// window capture.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CaptureGeometry {
    /// Drawable client-area size paired with GPUI's native-window global origin. Some platforms
    /// report the outer-window origin for a decorated window; the mapper derives the leading
    /// decoration inset from the difference against the native origin, without platform
    /// constants, and places regions with [`Self::scale_factor`].
    pub content_bounds: Rect,
    /// Semantic viewport size in GPUI logical pixels.
    pub viewport_size: (f32, f32),
    /// Physical pixels per GPUI logical pixel for the target window.
    pub scale_factor: f32,
}

/// Stable native window selector used by screenshot and video capture.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureTarget {
    process: ProcessId,
    window: NativeWindowId,
}

/// Continuous native RGBA frame source for video recording.
///
/// Windows uses one persistent Windows Graphics Capture session. macOS and Linux
/// sample their platform-native exact-window capture at the recorder cadence. Every
/// backend emits raw RGBA frames and is selected at compile time.
pub struct LiveFrameStream {
    inner: platform_stream::PlatformFrameStream,
}

impl LiveFrameStream {
    /// Open a continuous stream for one exact native window.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`CaptureFailure`] if the target cannot be resolved or
    /// the operating system refuses capture.
    pub fn open(target: CaptureTarget, frames_per_second: u8) -> Result<Self, CaptureFailure> {
        if !(1..=30).contains(&frames_per_second) {
            return Err(CaptureFailure::InvalidFrameRate);
        }
        Ok(Self {
            inner: platform_stream::PlatformFrameStream::open(target, frames_per_second)?,
        })
    }

    /// Wait for the next native frame up to `deadline`.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`CaptureFailure`] when capture closes, times out, or
    /// produces invalid dimensions.
    pub fn next_frame(&mut self, deadline: Duration) -> Result<RgbaImage, CaptureFailure> {
        let image = self.inner.next_frame(deadline)?;
        validate_image_dimensions(&image)?;
        Ok(image)
    }
}

impl CaptureTarget {
    /// Select one native window owned by one process.
    #[must_use]
    pub const fn new(process: ProcessId, window: NativeWindowId) -> Self {
        Self { process, window }
    }

    /// Process identifier that owns the native window.
    #[must_use]
    pub const fn process(self) -> ProcessId {
        self.process
    }

    /// Stable operating-system window identifier, when the bridge can provide one.
    #[must_use]
    pub const fn window(self) -> NativeWindowId {
        self.window
    }
}

/// A sanitized capture failure safe to return across the MCP boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureFailure {
    /// The requested logical region was invalid.
    InvalidRegion,
    /// The compositor stability deadline was outside the supported range.
    InvalidStabilityDeadline,
    /// The requested live frame rate was outside 1 through 30 FPS.
    InvalidFrameRate,
    /// No changed native frame arrived before the live stream deadline.
    FrameTimeout,
    /// A logical region was requested before GPUI published window geometry.
    MissingGeometry,
    /// Native windows could not be enumerated.
    WindowEnumeration,
    /// No native window matched the requested process and window identity.
    TargetNotFound {
        /// Number of enumerated candidate windows.
        window_count: usize,
        /// Number of candidates with the requested process ID.
        pid_matches: usize,
        /// Number of candidates with the requested native window identifier.
        window_matches: usize,
    },
    /// The operating system could not capture the matched window.
    CaptureUnavailable,
    /// The compositor did not produce enough fresh samples before the configured deadline.
    UnstableFrame,
    /// Captured dimensions exceeded the safety bound.
    DimensionsOutOfBounds,
    /// The requested region did not intersect the captured window.
    RegionOutsideWindow,
    /// PNG encoding failed.
    Encoding,
    /// The encoded PNG exceeded the safety bound.
    EncodedImageTooLarge,
}

impl std::fmt::Display for CaptureFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidRegion => "screenshot region must be finite and non-empty",
            Self::InvalidStabilityDeadline => {
                "capture stability deadline must be between 32 milliseconds and 2 seconds"
            }
            Self::InvalidFrameRate => "video frame rate must be between 1 and 30",
            Self::FrameTimeout => "native frame stream did not change before the deadline",
            Self::MissingGeometry => "GPUI window geometry is unavailable for region capture",
            Self::WindowEnumeration | Self::CaptureUnavailable => {
                "native screenshot capture is unavailable; verify desktop-session support and OS permission"
            }
            Self::TargetNotFound { .. } => "application window was not available for capture",
            Self::UnstableFrame => {
                "native screenshot capture did not produce a fresh frame before the deadline"
            }
            Self::DimensionsOutOfBounds => {
                "captured image dimensions are outside the safety bound"
            }
            Self::RegionOutsideWindow => "screenshot region is outside the application window",
            Self::Encoding => "screenshot encoding failed",
            Self::EncodedImageTooLarge => "encoded screenshot exceeds the 16 MiB safety bound",
        })
    }
}

impl std::error::Error for CaptureFailure {}

/// Capture a complete native window or one logical GPUI region as PNG.
///
/// # Errors
///
/// Returns a sanitized [`CaptureFailure`] when validation, native capture, or
/// bounded PNG encoding fails.
// GPUI regions are floating-point logical pixels while image crops use u32
// physical pixels. Values are validated, clamped, and images are capped before
// these intentional conversions.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
pub fn screenshot(
    window: CaptureTarget,
    options: ScreenshotOptions,
) -> Result<Screenshot, CaptureFailure> {
    validate_target(options.area)?;
    let native = frame(window, options.capture)?;
    let native_origin = native.origin;
    let reported_size = native.reported_size;
    let mut image = native.image;

    if let ScreenshotTarget::Region { rect } = options.area {
        let geometry = options.geometry.ok_or(CaptureFailure::MissingGeometry)?;
        let mapping = region_mapping(
            image.width(),
            image.height(),
            native_origin,
            reported_size,
            geometry,
        )?;
        let left = (mapping.offset_x + rect.x * mapping.scale_x)
            .floor()
            .max(0.0) as u32;
        let top = (mapping.offset_y + rect.y * mapping.scale_y)
            .floor()
            .max(0.0) as u32;
        let right = (mapping.offset_x + (rect.x + rect.width) * mapping.scale_x)
            .ceil()
            .clamp(0.0, image.width() as f32) as u32;
        let bottom = (mapping.offset_y + (rect.y + rect.height) * mapping.scale_y)
            .ceil()
            .clamp(0.0, image.height() as f32) as u32;
        if right <= left || bottom <= top || left >= image.width() || top >= image.height() {
            return Err(CaptureFailure::RegionOutsideWindow);
        }
        image = image::imageops::crop_imm(&image, left, top, right - left, bottom - top).to_image();
    }

    let width = image.width();
    let height = image.height();
    let mut bytes = Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(image)
        .write_to(&mut bytes, ImageFormat::Png)
        .map_err(|_| CaptureFailure::Encoding)?;
    let bytes = bytes.into_inner();
    if bytes.len() > MAX_ENCODED_BYTES {
        return Err(CaptureFailure::EncodedImageTooLarge);
    }
    Ok(Screenshot {
        mime_type: "image/png".to_owned(),
        base64_data: base64::engine::general_purpose::STANDARD.encode(bytes),
        width,
        height,
    })
}

/// Capture bounded native RGBA pixels for one exact native window.
///
/// This is the common platform abstraction used by MCP screenshots and visual
/// parity tests. It uses the platform backend selected by `xcap`: Windows
/// Graphics Capture on Windows, CoreGraphics on macOS, and X11 window capture
/// on Linux. Native Wayland compositors intentionally do not expose unattended
/// cross-process window capture by window identity.
///
/// # Errors
///
/// Returns a sanitized [`CaptureFailure`] when the window cannot be found,
/// native display metadata is unavailable, or capture does not settle.
pub fn frame(
    target: CaptureTarget,
    options: CaptureOptions,
) -> Result<NativeFrame, CaptureFailure> {
    capture_frame(target, |capture| {
        capture_after_compositor_settle(capture, options)
    })
}

/// Capture the newest available native RGBA frame without screenshot settling.
///
/// Video recording calls this repeatedly from one bounded recording worker. Unlike
/// [`frame`], this does not take extra compositor freshness samples for every output
/// frame, so the capture cadence is controlled by the video clock rather than the
/// screenshot stability policy.
///
/// # Errors
///
/// Returns a sanitized [`CaptureFailure`] when the native window cannot be resolved
/// or captured, or when the returned pixels exceed the image safety bounds.
pub fn live_frame(target: CaptureTarget) -> Result<NativeFrame, CaptureFailure> {
    capture_frame(target, |capture| capture())
}

fn validate_image_dimensions(image: &RgbaImage) -> Result<(), CaptureFailure> {
    if image.width() == 0
        || image.height() == 0
        || image.width() > MAX_IMAGE_DIMENSION
        || image.height() > MAX_IMAGE_DIMENSION
    {
        Err(CaptureFailure::DimensionsOutOfBounds)
    } else {
        Ok(())
    }
}

fn capture_frame(
    target: CaptureTarget,
    settle: impl FnOnce(
        &mut dyn FnMut() -> Result<RgbaImage, CaptureFailure>,
    ) -> Result<RgbaImage, CaptureFailure>,
) -> Result<NativeFrame, CaptureFailure> {
    let windows = xcap::Window::all().map_err(|_| CaptureFailure::WindowEnumeration)?;
    let native_window = find_native_window(windows, target)?;
    let origin = native_window
        .x()
        .and_then(|x| native_window.y().map(|y| (x, y)))
        .map_err(|_| CaptureFailure::CaptureUnavailable)?;
    let scale_factor = native_window
        .current_monitor()
        .and_then(|monitor| monitor.scale_factor())
        .map_err(|_| CaptureFailure::CaptureUnavailable)?;
    if !scale_factor.is_finite() || !(0.25..=8.0).contains(&scale_factor) {
        return Err(CaptureFailure::CaptureUnavailable);
    }
    let reported_size = native_window
        .width()
        .and_then(|width| native_window.height().map(|height| (width, height)))
        .map_err(|_| CaptureFailure::CaptureUnavailable)?;
    let image = settle(&mut || {
        native_window
            .capture_image()
            .map_err(|_| CaptureFailure::CaptureUnavailable)
    })?;
    validate_image_dimensions(&image)?;
    Ok(NativeFrame {
        image,
        origin,
        scale_factor,
        reported_size,
    })
}

#[cfg(target_os = "windows")]
mod platform_stream {
    use std::ffi::c_void;
    use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
    use std::time::Duration;

    use image::RgbaImage;
    use windows_capture::capture::{CaptureControl, Context, GraphicsCaptureApiHandler};
    use windows_capture::frame::Frame;
    use windows_capture::graphics_capture_api::InternalCaptureControl;
    use windows_capture::settings::{
        ColorFormat, CursorCaptureSettings, DirtyRegionSettings, DrawBorderSettings,
        MinimumUpdateIntervalSettings, SecondaryWindowSettings, Settings,
    };
    use windows_capture::window::Window;

    use super::{CaptureFailure, CaptureTarget, find_native_window};

    type FrameResult = Result<RgbaImage, CaptureFailure>;

    struct FrameHandler {
        frames: SyncSender<FrameResult>,
        scratch: Vec<u8>,
    }

    impl GraphicsCaptureApiHandler for FrameHandler {
        type Flags = SyncSender<FrameResult>;
        type Error = String;

        fn new(context: Context<Self::Flags>) -> Result<Self, Self::Error> {
            Ok(Self {
                frames: context.flags,
                scratch: Vec::new(),
            })
        }

        fn on_frame_arrived(
            &mut self,
            frame: &mut Frame,
            control: InternalCaptureControl,
        ) -> Result<(), Self::Error> {
            let width = frame.width();
            let height = frame.height();
            let result = frame
                .buffer()
                .map_err(|_| CaptureFailure::CaptureUnavailable)
                .and_then(|buffer| {
                    RgbaImage::from_raw(
                        width,
                        height,
                        buffer.as_nopadding_buffer(&mut self.scratch).to_vec(),
                    )
                    .ok_or(CaptureFailure::CaptureUnavailable)
                });
            match self.frames.try_send(result) {
                Ok(()) | Err(TrySendError::Full(_)) => Ok(()),
                Err(TrySendError::Disconnected(_)) => {
                    control.stop();
                    Ok(())
                }
            }
        }

        fn on_closed(&mut self) -> Result<(), Self::Error> {
            let _ = self
                .frames
                .try_send(Err(CaptureFailure::CaptureUnavailable));
            Ok(())
        }
    }

    pub(super) struct PlatformFrameStream {
        frames: Receiver<FrameResult>,
        control: Option<CaptureControl<FrameHandler, String>>,
    }

    impl PlatformFrameStream {
        pub(super) fn open(
            target: CaptureTarget,
            frames_per_second: u8,
        ) -> Result<Self, CaptureFailure> {
            // Preserve the process + native-window identity check before converting the exact
            // HWND into the Windows Graphics Capture source.
            let windows = xcap::Window::all().map_err(|_| CaptureFailure::WindowEnumeration)?;
            drop(find_native_window(windows, target)?);

            let raw_window = usize::try_from(target.window().get()).map_err(|_| {
                CaptureFailure::TargetNotFound {
                    window_count: 0,
                    pid_matches: 0,
                    window_matches: 0,
                }
            })? as *mut c_void;
            let window = Window::from_raw_hwnd(raw_window);
            if !window.is_valid() {
                return Err(CaptureFailure::CaptureUnavailable);
            }
            let (sender, frames) = sync_channel(2);
            let period = Duration::from_nanos(1_000_000_000 / u64::from(frames_per_second));
            let settings = Settings::new(
                window,
                CursorCaptureSettings::WithoutCursor,
                DrawBorderSettings::WithoutBorder,
                SecondaryWindowSettings::Exclude,
                MinimumUpdateIntervalSettings::Custom(period),
                DirtyRegionSettings::Default,
                ColorFormat::Rgba8,
                sender,
            );
            let control = FrameHandler::start_free_threaded(settings)
                .map_err(|_| CaptureFailure::CaptureUnavailable)?;
            Ok(Self {
                frames,
                control: Some(control),
            })
        }

        pub(super) fn next_frame(
            &mut self,
            deadline: Duration,
        ) -> Result<RgbaImage, CaptureFailure> {
            self.frames
                .recv_timeout(deadline)
                .map_err(|error| match error {
                    std::sync::mpsc::RecvTimeoutError::Timeout => CaptureFailure::FrameTimeout,
                    std::sync::mpsc::RecvTimeoutError::Disconnected => {
                        CaptureFailure::CaptureUnavailable
                    }
                })?
        }
    }

    impl Drop for PlatformFrameStream {
        fn drop(&mut self) {
            if let Some(control) = self.control.take() {
                drop(control.stop());
            }
        }
    }
}

#[cfg(not(target_os = "windows"))]
mod platform_stream {
    use std::time::Duration;

    use image::RgbaImage;

    use super::{CaptureFailure, CaptureTarget, live_frame};

    pub(super) struct PlatformFrameStream {
        target: CaptureTarget,
    }

    impl PlatformFrameStream {
        pub(super) fn open(
            target: CaptureTarget,
            _frames_per_second: u8,
        ) -> Result<Self, CaptureFailure> {
            // Validate capture synchronously so start_video_recording fails before returning.
            drop(live_frame(target)?);
            Ok(Self { target })
        }

        pub(super) fn next_frame(
            &mut self,
            _deadline: Duration,
        ) -> Result<RgbaImage, CaptureFailure> {
            live_frame(self.target).map(|frame| frame.image)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct RegionMapping {
    offset_x: f32,
    offset_y: f32,
    scale_x: f32,
    scale_y: f32,
}

#[allow(clippy::cast_precision_loss)]
fn region_mapping(
    image_width: u32,
    image_height: u32,
    native_origin: (i32, i32),
    reported_size: (u32, u32),
    geometry: CaptureGeometry,
) -> Result<RegionMapping, CaptureFailure> {
    let bounds = geometry.content_bounds;
    let (viewport_width, viewport_height) = geometry.viewport_size;
    let (reported_width, reported_height) = reported_size;
    let scale = geometry.scale_factor;
    if !bounds.is_valid()
        || !scale.is_finite()
        || scale <= 0.0
        || !viewport_width.is_finite()
        || !viewport_height.is_finite()
        || viewport_width <= 0.0
        || viewport_height <= 0.0
        || reported_width == 0
        || reported_height == 0
    {
        return Err(CaptureFailure::MissingGeometry);
    }

    // The native window API and the captured image do not have to report the same
    // pixel space. A capture process that is not per-monitor DPI aware reads
    // virtualized window coordinates while the operating system still hands back a
    // physical-pixel image, so the origin is converted with the ratio the platform
    // used to size that image before it is compared against GPUI's own geometry.
    let native_to_image_x = image_width as f32 / reported_width as f32;
    let native_to_image_y = image_height as f32 / reported_height as f32;
    let offset_x = bounds
        .x
        .mul_add(scale, -(native_origin.0 as f32 * native_to_image_x));
    let offset_y = bounds
        .y
        .mul_add(scale, -(native_origin.1 as f32 * native_to_image_y));
    if !offset_x.is_finite()
        || !offset_y.is_finite()
        || offset_x < 0.0
        || offset_y < 0.0
        || offset_x >= image_width as f32
        || offset_y >= image_height as f32
    {
        return Err(CaptureFailure::MissingGeometry);
    }

    // Consistency check only. The client area starts at `offset` inside the image
    // and the rest of the image is native decoration, so the ratio of what is left
    // to the logical viewport has to agree with GPUI's device scale.
    let implied_scale_x = (image_width as f32 - offset_x) / viewport_width;
    let implied_scale_y = (image_height as f32 - offset_y) / viewport_height;
    let minimum_scale = scale * 0.75;
    let maximum_scale = scale * 1.25;
    if !(minimum_scale..=maximum_scale).contains(&implied_scale_x)
        || !(minimum_scale..=maximum_scale).contains(&implied_scale_y)
    {
        return Err(CaptureFailure::MissingGeometry);
    }

    // Regions are placed with GPUI's own device scale rather than that implied
    // ratio. The ratio carries the window's trailing decoration inset, and using
    // it stretches every region by that inset over the width of the window, so a
    // crop picks up a column and a row that are outside the requested rectangle.
    Ok(RegionMapping {
        offset_x,
        offset_y,
        scale_x: scale,
        scale_y: scale,
    })
}

fn capture_after_compositor_settle<T>(
    capture: impl FnMut() -> Result<T, CaptureFailure>,
    options: CaptureOptions,
) -> Result<T, CaptureFailure> {
    #[cfg(target_os = "windows")]
    {
        capture_fresh_frame(
            capture,
            options,
            Instant::now,
            std::thread::sleep,
            CaptureFailure::UnstableFrame,
        )
    }

    #[cfg(not(target_os = "windows"))]
    {
        let _ = options;
        let mut capture = capture;
        capture()
    }
}

#[cfg(any(target_os = "windows", test))]
fn capture_fresh_frame<T, E: Clone>(
    mut capture: impl FnMut() -> Result<T, E>,
    options: CaptureOptions,
    mut now: impl FnMut() -> std::time::Instant,
    mut wait: impl FnMut(Duration),
    unstable_error: E,
) -> Result<T, E> {
    // Windows Graphics Capture can initially return an older composited frame. Take a bounded
    // sequence of ordered samples and return the newest one. Requiring byte equality is incorrect:
    // native capture borders and legitimate animation may change between every sample.
    let started = now();
    let mut latest = capture()?;
    // The first sample is the measurement. `settle_deadline` is the allowance
    // for waiting on the compositor, so the readbacks the policy itself demands
    // must not be paid out of it: a 5120x1440 window in an unoptimized build
    // spends most of a second inside one readback, and three of those overran a
    // one-second budget before this frame was ever judged stale. Credit the
    // remaining samples with what the first actually cost, bounded.
    let allowance = now()
        .saturating_duration_since(started)
        .saturating_mul(u32::from(MIN_FRESHNESS_SAMPLES.saturating_sub(1)))
        .min(MAX_MEASURED_ALLOWANCE);
    let Some(deadline) = started.checked_add(options.settle_deadline().saturating_add(allowance))
    else {
        return Err(unstable_error.clone());
    };
    for _ in 1..MIN_FRESHNESS_SAMPLES {
        let current_time = now();
        let Some(remaining) = deadline.checked_duration_since(current_time) else {
            return Err(unstable_error.clone());
        };
        if remaining.is_zero() {
            return Err(unstable_error.clone());
        }
        wait(COMPOSITOR_POLL_INTERVAL.min(remaining));
        if now() >= deadline {
            return Err(unstable_error.clone());
        }
        latest = capture()?;
    }
    Ok(latest)
}

fn validate_target(target: ScreenshotTarget) -> Result<(), CaptureFailure> {
    if let ScreenshotTarget::Region { rect } = target
        && (!rect.is_valid() || rect.width < 1.0 || rect.height < 1.0)
    {
        return Err(CaptureFailure::InvalidRegion);
    }
    Ok(())
}

fn find_native_window(
    windows: Vec<xcap::Window>,
    target: CaptureTarget,
) -> Result<xcap::Window, CaptureFailure> {
    let window_count = windows.len();
    let mut pid_matches = 0_usize;
    let mut window_matches = 0_usize;
    for window in windows {
        let pid_matches_window = window.pid().ok() == Some(target.process().get());
        let window_matches_window = window.id().ok() == Some(target.window().get());
        pid_matches += usize::from(pid_matches_window);
        window_matches += usize::from(window_matches_window);
        if pid_matches_window && window_matches_window {
            return Ok(window);
        }
    }
    Err(CaptureFailure::TargetNotFound {
        window_count,
        pid_matches,
        window_matches,
    })
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::time::{Duration, Instant};

    use gpui_mcp_protocol::{Rect, ScreenshotTarget};

    use super::{
        CaptureFailure, CaptureGeometry, CaptureOptions, DEFAULT_SETTLE_DEADLINE,
        MAX_MEASURED_ALLOWANCE, MIN_FRESHNESS_SAMPLES, RegionMapping, capture_fresh_frame,
        region_mapping, validate_target,
    };

    #[test]
    fn rejects_empty_and_non_finite_regions_before_native_access() {
        for rect in [
            Rect {
                width: 0.0,
                height: 10.0,
                ..Rect::default()
            },
            Rect {
                width: f32::NAN,
                height: 10.0,
                ..Rect::default()
            },
        ] {
            assert_eq!(
                validate_target(ScreenshotTarget::Region { rect }),
                Err(CaptureFailure::InvalidRegion)
            );
        }
    }

    #[test]
    fn freshness_sampling_returns_the_third_ordered_frame() -> Result<(), CaptureFailure> {
        let calls = Cell::new(0_usize);
        let elapsed = Cell::new(Duration::ZERO);
        let started = Instant::now();
        let frames = [1_u8, 1, 2];
        let captured = capture_fresh_frame(
            || {
                let call = calls.get();
                calls.set(call.saturating_add(1));
                Ok::<_, CaptureFailure>(
                    frames
                        .get(call)
                        .copied()
                        .or_else(|| frames.last().copied())
                        .unwrap_or(2),
                )
            },
            CaptureOptions::new(Duration::from_millis(100))?,
            || started + elapsed.get(),
            |duration| elapsed.set(elapsed.get().saturating_add(duration)),
            CaptureFailure::UnstableFrame,
        )?;

        assert_eq!(captured, 2);
        assert_eq!(calls.get(), 3);
        Ok(())
    }

    #[test]
    fn freshness_sampling_honors_the_deadline() -> Result<(), CaptureFailure> {
        let calls = Cell::new(0_u8);
        let elapsed = Cell::new(Duration::ZERO);
        let started = Instant::now();
        let captured = capture_fresh_frame(
            || {
                let frame = calls.get();
                calls.set(frame.saturating_add(1));
                Ok::<_, CaptureFailure>(frame)
            },
            CaptureOptions::new(Duration::from_millis(32))?,
            || started + elapsed.get(),
            |duration| elapsed.set(elapsed.get().saturating_add(duration)),
            CaptureFailure::UnstableFrame,
        );

        assert_eq!(captured, Err(CaptureFailure::UnstableFrame));
        assert_eq!(calls.get(), 2);
        Ok(())
    }

    /// A 5120x1440 window in an unoptimized build spends most of a second
    /// inside a single readback, so three of them cannot fit in a budget of one
    /// second — the debug server could not capture such a window at all.
    ///
    /// The frame is really allocated at that size rather than named, and the
    /// per-sample cost is the one measured against the launcher: three 29 MB
    /// samples overran a one-second deadline, so each cost at least a third of
    /// a second. The clock is injected, so the assertion is about the policy
    /// and not about how fast this machine happens to be today.
    #[test]
    fn a_large_frame_survives_a_readback_that_costs_most_of_the_deadline()
    -> Result<(), CaptureFailure> {
        let sample_cost = Duration::from_millis(500);
        let elapsed = Cell::new(Duration::ZERO);
        let started = Instant::now();
        let calls = Cell::new(0_usize);

        let captured = capture_fresh_frame(
            || {
                elapsed.set(elapsed.get().saturating_add(sample_cost));
                calls.set(calls.get().saturating_add(1));
                Ok::<_, CaptureFailure>(image::RgbaImage::new(5_120, 1_440))
            },
            CaptureOptions::default(),
            || started + elapsed.get(),
            |duration| elapsed.set(elapsed.get().saturating_add(duration)),
            CaptureFailure::UnstableFrame,
        )?;

        assert_eq!(captured.dimensions(), (5_120, 1_440));
        assert_eq!(
            calls.get(),
            usize::from(MIN_FRESHNESS_SAMPLES),
            "every freshness sample must still be taken; widening the budget must not \
             buy headroom by sampling less"
        );
        Ok(())
    }

    /// The counterpart, and the reason the fix is not simply a longer deadline:
    /// a window whose readbacks are cheap earns almost no allowance and keeps
    /// the one-second wait. Here the readback is cheap and the *waiting* is what
    /// overruns, which is what the settle deadline is for.
    ///
    /// This is the guard against the tempting non-fix — raising
    /// `DEFAULT_SETTLE_DEADLINE`, or granting the allowance unconditionally.
    /// Either would let this run past a second and fail the bound below.
    #[test]
    fn a_cheap_readback_earns_no_headroom_and_keeps_the_one_second_bound() {
        let elapsed = Cell::new(Duration::ZERO);
        let started = Instant::now();

        let captured = capture_fresh_frame(
            || {
                elapsed.set(elapsed.get().saturating_add(Duration::from_millis(4)));
                Ok::<_, CaptureFailure>(1_u8)
            },
            CaptureOptions::default(),
            || started + elapsed.get(),
            // A compositor that will not hand back a newer frame: each wait
            // costs far more than the poll interval asked for.
            |_| elapsed.set(elapsed.get().saturating_add(Duration::from_millis(600))),
            CaptureFailure::UnstableFrame,
        );

        assert_eq!(captured, Err(CaptureFailure::UnstableFrame));
        assert!(
            elapsed.get() < DEFAULT_SETTLE_DEADLINE.saturating_add(Duration::from_millis(600)),
            "a cheap sample must not buy a long budget; the run took {:?}",
            elapsed.get()
        );
    }

    /// A machine slow enough to make one readback pathological must still give
    /// up rather than widening the budget without limit. The first sample alone
    /// blows any bounded budget, so the policy stops there instead of paying for
    /// two more of them.
    #[test]
    fn the_measured_allowance_is_capped() {
        let elapsed = Cell::new(Duration::ZERO);
        let started = Instant::now();
        let calls = Cell::new(0_usize);

        let captured = capture_fresh_frame(
            || {
                calls.set(calls.get().saturating_add(1));
                elapsed.set(elapsed.get().saturating_add(Duration::from_secs(30)));
                Ok::<_, CaptureFailure>(0_u8)
            },
            CaptureOptions::default(),
            || started + elapsed.get(),
            |duration| elapsed.set(elapsed.get().saturating_add(duration)),
            CaptureFailure::UnstableFrame,
        );

        assert_eq!(captured, Err(CaptureFailure::UnstableFrame));
        assert_eq!(
            calls.get(),
            1,
            "a sample beyond the cap must end the attempt, not buy two more"
        );
        assert!(
            DEFAULT_SETTLE_DEADLINE.saturating_add(MAX_MEASURED_ALLOWANCE)
                < Duration::from_secs(30),
            "this test only means anything while the bounded budget is smaller \
             than the pathological sample it rejects"
        );
    }

    #[test]
    fn capture_stability_deadline_is_bounded() {
        assert!(CaptureOptions::new(Duration::from_millis(31)).is_err());
        assert!(CaptureOptions::new(Duration::from_millis(32)).is_ok());
        assert!(CaptureOptions::new(Duration::from_secs(2)).is_ok());
        assert!(CaptureOptions::new(Duration::from_millis(2_001)).is_err());
    }

    #[test]
    fn client_geometry_offsets_regions_below_native_title_chrome() {
        assert_eq!(
            region_mapping(
                1_442,
                933,
                (100, 100),
                (1_442, 933),
                CaptureGeometry {
                    content_bounds: Rect {
                        x: 101.0,
                        y: 132.0,
                        width: 1_440.0,
                        height: 900.0,
                    },
                    viewport_size: (1_440.0, 900.0),
                    scale_factor: 1.0,
                },
            ),
            Ok(RegionMapping {
                offset_x: 1.0,
                offset_y: 32.0,
                scale_x: 1.0,
                scale_y: 1.0,
            })
        );
    }

    /// A trailing decoration inset must not stretch the region. The window below
    /// carries a one-pixel border on every side and a 31-pixel caption, so the
    /// image is two pixels wider than the client area; scaling regions by the
    /// leftover ratio would place the right edge of a region one pixel outside
    /// the rectangle that was asked for.
    #[test]
    fn a_trailing_decoration_inset_does_not_stretch_regions() -> Result<(), CaptureFailure> {
        let mapping = region_mapping(
            642,
            452,
            (100, 100),
            (642, 452),
            CaptureGeometry {
                content_bounds: Rect {
                    x: 101.0,
                    y: 131.0,
                    width: 640.0,
                    height: 420.0,
                },
                viewport_size: (640.0, 420.0),
                scale_factor: 1.0,
            },
        )?;

        assert_eq!(
            mapping,
            RegionMapping {
                offset_x: 1.0,
                offset_y: 31.0,
                scale_x: 1.0,
                scale_y: 1.0,
            }
        );
        // A 104-pixel-wide region at x=32 ends at the 137th image column, not the
        // 138th that the leftover ratio would have reached.
        let right = mapping.offset_x + (32.0 + 104.0) * mapping.scale_x;
        assert!(
            (right.ceil() - 137.0).abs() < f32::EPSILON,
            "the right edge of the crop must be the requested edge, got {right}"
        );
        Ok(())
    }

    /// The native window API and the captured image do not have to agree on a
    /// pixel space. A capture process without per-monitor DPI awareness reads a
    /// virtualized origin while the image stays physical, and mixing the two
    /// places the crop hundreds of pixels away from the requested rectangle.
    #[test]
    fn a_virtualized_native_origin_is_converted_into_image_pixels() -> Result<(), CaptureFailure> {
        let mapping = region_mapping(
            2_160,
            1_440,
            (500, 300),
            (1_440, 960),
            CaptureGeometry {
                content_bounds: Rect {
                    x: 500.0,
                    y: 300.0,
                    width: 1_440.0,
                    height: 960.0,
                },
                viewport_size: (1_440.0, 960.0),
                scale_factor: 1.5,
            },
        )?;

        assert_eq!(
            mapping,
            RegionMapping {
                offset_x: 0.0,
                offset_y: 0.0,
                scale_x: 1.5,
                scale_y: 1.5,
            }
        );
        Ok(())
    }

    #[test]
    fn inconsistent_native_and_client_origins_are_rejected() {
        assert_eq!(
            region_mapping(
                1_435,
                932,
                (100, 132),
                (1_435, 932),
                CaptureGeometry {
                    content_bounds: Rect {
                        x: 100.0,
                        y: 100.0,
                        width: 1_435.0,
                        height: 900.0,
                    },
                    viewport_size: (1_440.0, 900.0),
                    scale_factor: 1.0,
                },
            ),
            Err(CaptureFailure::MissingGeometry)
        );
    }
}
