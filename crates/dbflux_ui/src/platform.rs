/// Platform detection utilities for window management.
///
/// Different window systems have different behaviors and requirements.
/// This module provides helpers to detect the current platform and
/// adjust window creation accordingly.
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
