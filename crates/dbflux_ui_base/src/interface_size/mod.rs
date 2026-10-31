//! The interface size commands: the interface, editor and data grid font
//! sizes one step larger, one step smaller, or back to their defaults.
//!
//! The sizes are the general settings ones, so a change reaches every window
//! through [`AppStateChanged`], which republishes the fonts, and is saved by
//! the settings writer like a change made in Settings > General.

use dbflux_core::InterfaceSizeChange;
use gpui::{App, Entity};

use crate::app_state_entity::{AppStateChanged, AppStateEntity};
use crate::settings_writer::persist_general_settings;

#[cfg(test)]
mod tests;

/// Applies `change` to the font sizes and saves them in the background.
/// Returns `true` when the sizes changed; a change that leaves every size
/// where it was (a step at the limits, a reset at the defaults) saves nothing.
/// The new sizes take effect at once, and a failed write is reported by the
/// settings writer.
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

    app_state.update(cx, |state, cx| {
        state.update_general_settings(settings.clone());
        cx.emit(AppStateChanged);
    });

    persist_general_settings(app_state, settings, |_saved, _cx| {}, cx);

    true
}
