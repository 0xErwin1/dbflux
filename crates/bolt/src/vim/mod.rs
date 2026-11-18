//! Opt-in modal editing (Vim Normal/Insert/Visual) for multi-line editors.
//!
//! `machine` decides what a key means and where the cursor goes, without a
//! window or an input. `binding` applies those decisions to an editor through
//! its public API, for any view that hosts one.
//!
//! What a product owns stays with the product: its leader action, the
//! run-command action that carries a leader command to the host, the key
//! contexts its keymap binds, and the mode labels it translates. Vim reads
//! them through the [`VimSeams`] the product installs at startup with
//! [`set_vim_seams`]. An editor wired without them answers its command keys
//! but no leader sequence and shows no mode label, as a component rendered
//! without a [`crate::keymap::CommandSet`] answers no commands.

mod binding;
pub mod machine;

pub use binding::{
    LeaderCommandHandler, RunCommandHandler, VimBinding, VimHost, VimSeams, set_vim_seams,
};
pub use machine::VimMode;

use gpui::{App, Global};

/// Key under which an editor reports its Vim mode in the key context.
pub const VIM_MODE_KEY: &str = "vim_mode";

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

#[cfg(test)]
mod tests {
    use gpui::TestAppContext;

    use super::*;

    #[gpui::test]
    fn an_app_without_a_published_setting_has_vim_disabled(cx: &mut TestAppContext) {
        cx.update(|cx| assert!(!vim_enabled(cx)));
    }

    #[gpui::test]
    fn publishing_the_setting_turns_vim_on_and_off(cx: &mut TestAppContext) {
        cx.update(|cx| {
            set_vim_enabled(cx, true);
            assert!(vim_enabled(cx));

            set_vim_enabled(cx, false);
            assert!(!vim_enabled(cx));
        });
    }

    #[gpui::test]
    fn republishing_the_same_value_keeps_observers_quiet(cx: &mut TestAppContext) {
        cx.update(|cx| {
            set_vim_enabled(cx, true);
            set_vim_enabled(cx, true);
            assert!(vim_enabled(cx));
        });
    }
}
