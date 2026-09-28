//! Opt-in modal editing (Vim Normal/Insert/Visual) for multi-line editors.
//!
//! `machine` decides what a key means and where the cursor goes, without a
//! window or an input.

pub mod machine;

pub use machine::VimMode;

use gpui::{App, Global};

/// Key under which an editor reports its Vim mode in the key context.
pub const VIM_MODE_KEY: &str = "vim_mode";

/// Mode indicator shown under an editor while Vim mode is enabled.
pub fn vim_mode_label(mode: VimMode) -> String {
    match mode {
        VimMode::Normal => dbflux_i18n::t!("document.code.vim.normal"),
        VimMode::Insert => dbflux_i18n::t!("document.code.vim.insert"),
        VimMode::Replace => dbflux_i18n::t!("document.code.vim.replace"),
        VimMode::Visual => dbflux_i18n::t!("document.code.vim.visual"),
        VimMode::VisualLine => dbflux_i18n::t!("document.code.vim.visual_line"),
        VimMode::VisualBlock => dbflux_i18n::t!("document.code.vim.visual_block"),
    }
}

/// GPUI global carrying the Vim mode setting (Settings > General), so editors
/// in crates without the app state can follow it.
///
/// Absent means off: component tests that never publish it keep Vim disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VimSettingGlobal {
    pub enabled: bool,
}

impl Global for VimSettingGlobal {}

/// Publishes the Vim mode setting. Observers of the global run only when the
/// value changes.
pub fn set_vim_enabled(cx: &mut App, enabled: bool) {
    if vim_enabled(cx) == enabled && cx.has_global::<VimSettingGlobal>() {
        return;
    }

    cx.set_global(VimSettingGlobal { enabled });
}

/// Whether Vim mode is on, as last published with [`set_vim_enabled`].
pub fn vim_enabled(cx: &App) -> bool {
    cx.try_global::<VimSettingGlobal>()
        .is_some_and(|setting| setting.enabled)
}
