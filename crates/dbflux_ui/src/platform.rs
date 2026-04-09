/// Platform detection utilities for window management.
///
/// Different window systems have different behaviors and requirements.
/// This module provides helpers to detect the current platform and
/// adjust window creation accordingly.
use crate::ui::icons::AppIcon;
use gpui::{
    App, ClickEvent, Decorations, InteractiveElement, IntoElement, ParentElement, Stateful, Styled,
    Window, WindowDecorations, WindowKind, WindowOptions, div, px, svg,
};
use gpui_component::ActiveTheme;
use gpui_component::InteractiveElementExt;

/// Title bar height for Linux CSD mode. Used for layout and client inset reporting.
pub const TITLE_BAR_HEIGHT: gpui::Pixels = px(32.0);

/// Returns the `WindowDecorations` value to request when creating a top-level window.
///
/// On Linux, requests `Client` (CSD) so Wayland compositors that honor the request
/// (e.g. GNOME, Sway) grant CSD mode and the app renders its own title bar.
/// On X11, requesting `Client` is safe because:
/// - Most X11 window managers ignore the request and keep server-side decorations.
/// - GPUI's `window.window_decorations()` returns the *actual negotiated result* at
///   runtime, so `should_render_csd()` will return `false` when the compositor kept SSD.
/// - No custom title bar renders unless the compositor actually switched to CSD mode.
///
/// On other platforms, returns `Server` explicitly to preserve the original behavior.
#[cfg(target_os = "linux")]
pub fn decoration_request() -> Option<WindowDecorations> {
    Some(WindowDecorations::Client)
}

/// Returns the `WindowDecorations` value to request when creating a top-level window.
///
/// On non-Linux platforms, returns `Server` explicitly to preserve original behavior
/// (not `None`, which leaves the decision to the platform default and could differ).
#[cfg(not(target_os = "linux"))]
pub fn decoration_request() -> Option<WindowDecorations> {
    Some(WindowDecorations::Server)
}

/// Backward-compatible alias used by main window creation in `main.rs`.
pub use decoration_request as main_window_decoration_request;

/// Returns `true` if the window is in client-side decoration (CSD) mode.
///
/// On Linux, checks if `window.window_decorations()` returns `Decorations::Client`.
/// On other platforms, always returns `false`.
#[cfg(target_os = "linux")]
pub fn should_render_csd(window: &Window) -> bool {
    matches!(window.window_decorations(), Decorations::Client { .. })
}

/// Returns `false` on non-Linux platforms (no CSD support needed).
#[cfg(not(target_os = "linux"))]
pub fn should_render_csd(_window: &Window) -> bool {
    false
}

/// Conditionally renders a CSD title bar for Linux and configures the client inset.
///
/// Call this at the start of every top-level window's `Render::render()` and store
/// the result. Prepend it as the first child of the root flex column.
///
/// Returns `Some(element)` when CSD is active (Linux Wayland with compositor granting
/// CSD), `None` otherwise. When `None` is returned on Linux, the client inset is
/// explicitly reset to zero to prevent stale insets.
pub fn render_csd_title_bar(
    window: &mut Window,
    cx: &mut App,
    title: &str,
) -> Option<Stateful<gpui::Div>> {
    if !should_render_csd(window) {
        #[cfg(target_os = "linux")]
        window.set_client_inset(px(0.0));
        return None;
    }

    window.set_client_inset(TITLE_BAR_HEIGHT);

    #[cfg(target_os = "linux")]
    {
        let controls = window.window_controls();
        let theme = cx.theme();
        let title_text = title.to_string();

        let make_button = |icon: AppIcon, handler: Box<dyn Fn(&mut Window) + 'static>| {
            div()
                .flex()
                .items_center()
                .justify_center()
                .w(px(46.0))
                .h_full()
                .cursor_pointer()
                .hover(move |d| d.bg(theme.secondary))
                .on_mouse_down(gpui::MouseButton::Left, move |_, window, _cx| {
                    handler(window);
                })
                .child(
                    svg()
                        .path(icon.path())
                        .size_4()
                        .text_color(theme.muted_foreground),
                )
        };

        let mut title_bar = div()
            .id("linux-csd-title-bar")
            .flex()
            .flex_row()
            .items_center()
            .h(TITLE_BAR_HEIGHT)
            .bg(theme.tab_bar)
            .border_b_1()
            .border_color(theme.border)
            .on_double_click(|_: &ClickEvent, window: &mut Window, _cx: &mut App| {
                window.zoom_window();
            })
            .on_mouse_down(
                gpui::MouseButton::Right,
                |event: &gpui::MouseDownEvent, window: &mut Window, _cx: &mut App| {
                    window.show_window_menu(event.position);
                },
            );

        let drag_area = div()
            .flex()
            .flex_row()
            .items_center()
            .flex_1()
            .h_full()
            .pl_3()
            .gap_2()
            .cursor_pointer()
            .on_mouse_down(gpui::MouseButton::Left, |_, window, _cx| {
                window.start_window_move();
            })
            .child(
                div()
                    .text_sm()
                    .text_color(theme.foreground)
                    .font_weight(gpui::FontWeight::MEDIUM)
                    .child(title_text),
            );

        title_bar = title_bar.child(drag_area);

        if controls.minimize {
            title_bar = title_bar.child(make_button(
                AppIcon::Minimize2,
                Box::new(|window| window.minimize_window()),
            ));
        }

        if controls.maximize {
            title_bar = title_bar.child(make_button(
                AppIcon::Maximize2,
                Box::new(|window| window.zoom_window()),
            ));
        }

        title_bar = title_bar.child(make_button(
            AppIcon::X,
            Box::new(|window| window.remove_window()),
        ));

        Some(title_bar)
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Backward-compatible alias: renders the title bar with a fixed "DBFlux" title.
/// Prefer `render_csd_title_bar` for new code that needs per-window titles.
pub fn render_linux_title_bar(window: &mut Window, cx: &mut App) -> impl IntoElement + 'static {
    match render_csd_title_bar(window, cx, "DBFlux") {
        Some(el) => el.into_any_element(),
        None => div().into_any_element(),
    }
}

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

/// Applies standard DBFlux window options for secondary windows (Settings, Connection
/// Manager, SSO Wizard, etc.): floating kind (where supported), min size so X11 window
/// managers emit `WM_NORMAL_HINTS`, and platform-appropriate decorations.
///
/// On Linux, requests CSD so secondary windows match the main window behavior and
/// render their own title bars. On other platforms, requests server-side decorations.
pub fn apply_window_options(options: &mut WindowOptions, min_width: f32, min_height: f32) {
    if let Some(kind) = floating_window_kind() {
        options.kind = kind;
    }

    options.window_min_size = Some(gpui::Size {
        width: px(min_width),
        height: px(min_height),
    });

    options.window_decorations = decoration_request();
}
