//! The interface size commands: the interface, editor and data grid font
//! sizes one step larger, one step smaller, or back to their defaults.
//!
//! The sizes are the general settings ones, so a change is saved like a
//! change made in Settings > General and reaches every window through
//! [`AppStateChanged`], which republishes the fonts.

use dbflux_core::InterfaceSizeChange;
use gpui::{App, Entity};

use crate::app_state_entity::{AppStateChanged, AppStateEntity};
use crate::user_error::{ErrorKind, UserFacingError, report_error};

#[cfg(test)]
mod tests;

/// Applies `change` to the saved font sizes. Returns `true` when the sizes
/// changed and were saved; a change that leaves every size where it was
/// (a step at the limits, a reset at the defaults) saves nothing.
pub fn change_interface_size(
    app_state: &Entity<AppStateEntity>,
    change: InterfaceSizeChange,
    cx: &mut App,
) -> bool {
    let current = app_state.read(cx).general_settings();
    let mut settings = current.clone();
    settings.change_interface_size(change);

    if settings == *current {
        return false;
    }

    let runtime = app_state.read(cx).storage_runtime();
    if let Err(error) = dbflux_app::config_loader::save_general_settings(runtime, &settings) {
        report_error(
            UserFacingError::new(
                ErrorKind::Storage,
                dbflux_i18n::t!("settings.general.save.error", error = error),
            ),
            cx,
        );
        return false;
    }

    app_state.update(cx, |state, cx| {
        state.update_general_settings(settings);
        cx.emit(AppStateChanged);
    });

    true
}
