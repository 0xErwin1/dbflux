/// Platform detection utilities for window management.
///
/// Different window systems have different behaviors and requirements.
/// This module provides helpers to detect the current platform and
/// adjust window creation accordingly.
///
/// # Title bar on GNOME Wayland
///
/// GNOME Wayland (43+) deliberately does not implement the `zxdg-decoration-v1`
/// protocol for non-GTK apps. When GPUI requests server-side decorations (SSD),
/// GNOME responds with `ClientSide`, causing GPUI to enter CSD mode. Because
/// DBFlux does not render its own title bar, the window ends up with no title
/// bar at all — impossible to move or close with the mouse.
///
/// Current workaround: the `.desktop` files force XWayland by setting
/// `WAYLAND_DISPLAY=` (empty), so GPUI falls back to X11 where GNOME honors
/// `_MOTIF_WM_HINTS` and renders a proper server-side title bar.
///
/// TODO(csd): Implement client-side decorations (CSD) for Linux, similar to
/// how Zed renders its own title bar on GNOME Wayland. Steps:
/// - Detect CSD mode via `Window::window_decorations()` returning
///   `Decorations::Client { tiling }` and expose a flag to the root view.
/// - Render a thin title bar strip at the top of the root view containing the
///   window title, close/maximize/minimize buttons, and drag-to-move support
///   (via `window.start_window_move()`).
/// - Handle the `Tiling` bitflags from `Decorations::Client { tiling }` to
///   suppress edge shadows/borders on tiled sides.
/// - Once CSD is in place, remove the `WAYLAND_DISPLAY=` workaround from both
///   `.desktop` files (`resources/desktop/` and `packaging/`).
/// - Reference: Zed's `TitleBar` component for layout and button behavior.
use gpui::{WindowKind, WindowOptions, px};

/// Returns true if running on X11 (not Wayland, macOS, or Windows).
///
/// X11 has issues with `WindowKind::Floating` where it treats floating
/// windows as transient dialogs, which can cause rendering problems in
/// some compositors. On X11, we avoid using `WindowKind::Floating`.
///
/// Detection is based on environment variables:
/// - `WAYLAND_DISPLAY` indicates Wayland
/// - `DISPLAY` indicates X11 (if WAYLAND_DISPLAY is not set)
pub fn is_x11() -> bool {
    #[cfg(target_os = "linux")]
    {
        // If WAYLAND_DISPLAY is set, we're on Wayland, not X11
        if std::env::var("WAYLAND_DISPLAY").is_ok() {
            return false;
        }

        // If DISPLAY is set and WAYLAND_DISPLAY is not, we're on X11
        std::env::var("DISPLAY").is_ok()
    }

    #[cfg(not(target_os = "linux"))]
    {
        // macOS and Windows don't use X11
        false
    }
}

/// Returns the appropriate window kind for floating windows based on the platform.
///
/// - On X11: returns `None` (use default window kind to avoid transient dialog issues)
/// - On other platforms (Wayland, macOS, Windows): returns `Some(WindowKind::Floating)`
///
/// Use this when creating secondary windows (Settings, Connection Manager, etc.)
/// that should float on supported platforms but work correctly on X11.
pub fn floating_window_kind() -> Option<WindowKind> {
    if is_x11() {
        None
    } else {
        Some(WindowKind::Floating)
    }
}

/// Applies standard DBFlux window options: floating kind (where supported) and
/// min size so X11 window managers emit `WM_NORMAL_HINTS`, enabling floating
/// and resizing in tiling WMs like LeftWM, i3, and bspwm.
pub fn apply_window_options(options: &mut WindowOptions, min_width: f32, min_height: f32) {
    if let Some(kind) = floating_window_kind() {
        options.kind = kind;
    }

    options.window_min_size = Some(gpui::Size {
        width: px(min_width),
        height: px(min_height),
    });
}
