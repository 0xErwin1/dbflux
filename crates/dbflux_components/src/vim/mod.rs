//! Opt-in modal editing (Vim Normal/Insert/Visual) for multi-line editors.
//!
//! `machine` decides what a key means and where the cursor goes, without a
//! window or an input.

pub mod machine;

pub use machine::VimMode;

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
