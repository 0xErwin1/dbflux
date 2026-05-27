//! Centralized entry point for opening (or focusing) the Settings window.
//!
//! Only one Settings window may exist at a time. All call sites should go
//! through [`open_or_focus_settings`] so the singleton invariant is preserved
//! regardless of which UI surface (workspace, sidebar footer, connection
//! manager) triggers the action.

use dbflux_ui_base::{AppStateEntity, platform};
use gpui::*;
use gpui_component::Root;

use super::{SettingsCoordinator, SettingsSectionId};

/// Opens the Settings window or focuses it if one is already open.
///
/// When a window already exists, focuses it and returns without invoking
/// `setup`. The `section` parameter is only honored when opening a new
/// window — existing windows keep whichever section the user was on.
///
/// `setup` runs inside the new window's `Context<Root>` and is the right
/// place to attach event subscriptions (e.g. forwarding `OpenScript` /
/// `OpenLoginModal` events back to the host entity that opened settings).
pub fn open_or_focus_settings<S>(
    app_state: Entity<AppStateEntity>,
    section: Option<SettingsSectionId>,
    cx: &mut App,
    setup: S,
) where
    S: FnOnce(&Entity<SettingsCoordinator>, &mut App) + 'static,
{
    if let Some(handle) = app_state.read(cx).settings_window {
        match handle.update(cx, |_root, window, _cx| {
            window.activate_window();
        }) {
            Ok(()) => return,
            Err(e) => {
                log::warn!("Stale settings window handle, reopening: {:?}", e);
                app_state.update(cx, |state, _| {
                    state.settings_window = None;
                });
            }
        }
    }

    let bounds = Bounds::centered(None, size(px(950.0), px(700.0)), cx);

    let mut options = WindowOptions {
        app_id: Some("dbflux".into()),
        titlebar: Some(TitlebarOptions {
            title: Some("Settings".into()),
            ..Default::default()
        }),
        window_bounds: Some(WindowBounds::Windowed(bounds)),
        focus: true,
        ..Default::default()
    };
    platform::apply_window_options(&mut options, 800.0, 600.0);

    let app_state_for_close = app_state.clone();
    let app_state_for_new = app_state.clone();

    let open_result = cx.open_window(options, move |window, cx| {
        window.on_window_should_close(cx, move |_window, cx| {
            app_state_for_close.update(cx, |state, _| {
                state.settings_window = None;
            });
            true
        });

        let settings = cx.new(|cx| match section {
            Some(section) => {
                SettingsCoordinator::new_with_section(app_state_for_new, section, window, cx)
            }
            None => SettingsCoordinator::new(app_state_for_new, window, cx),
        });

        setup(&settings, cx);

        cx.new(|cx| Root::new(settings, window, cx))
    });

    let Ok(handle) = open_result else {
        return;
    };

    app_state.update(cx, |state, _| {
        state.settings_window = Some(handle);
    });

    if let Err(e) = handle.update(cx, |_root, window, cx| {
        window.activate_window();
        cx.notify();
    }) {
        log::warn!("Failed to activate settings window: {:?}", e);
    }
}
