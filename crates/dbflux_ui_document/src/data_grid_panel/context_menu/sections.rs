//! The shared pieces of the data-grid context menu: the fixed entries of the
//! column header menu, the keycap a menu row shows, and the window-level
//! overlay the Bolt cascade hangs in.

use super::DataGridPanel;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::components::data_table::{ContextMenuAction, context_menu_keystroke};
use dbflux_ui_base::keymap::{chord_display_parts, effective_keymap, key_chord_from_gpui};
use gpui::prelude::*;
use gpui::{
    AnyElement, App, Context, IntoElement, MouseButton, SharedString, Window, deferred, div,
};

/// The shortcut shown on a menu row, formatted like the other keycaps in
/// the app (`Ctrl C`, `Delete`).
///
/// Row actions the data table binds come from its GPUI bindings; the ones
/// dispatched through the app keymap (inspect row, view value) come from the
/// Results layer.
pub(super) fn action_shortcut(action: ContextMenuAction, cx: &App) -> Option<SharedString> {
    if let Some(keystroke) = context_menu_keystroke(action, cx) {
        let label = chord_display_parts(&key_chord_from_gpui(&keystroke)).join(" ");

        return Some(label.into());
    }

    let command = match action {
        ContextMenuAction::InspectRow => Command::ToggleRowInspector,
        ContextMenuAction::ViewValue => Command::ToggleValuePanel,
        ContextMenuAction::FindSimilarRows => Command::FindSimilarRows,
        _ => return None,
    };

    effective_keymap()
        .chord_for_command(ContextId::Results, command)
        .map(|chord| chord_display_parts(chord).join(" ").into())
}

impl DataGridPanel {
    /// The entries of the column header menu that precede its filter
    /// operators, in display order. The tree the menu renders and the
    /// keyboard walks both read this list, so their rows agree.
    pub(in crate::data_grid_panel) fn column_header_menu_fixed_actions() -> [ContextMenuAction; 4] {
        [
            ContextMenuAction::CopyColumnName,
            ContextMenuAction::Order(dbflux_core::SortDirection::Ascending),
            ContextMenuAction::Order(dbflux_core::SortDirection::Descending),
            ContextMenuAction::RemoveOrdering,
        ]
    }

    /// Close the menu without running anything and hand focus back.
    fn dismiss_context_menu(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let is_document_view = self
            .context_menu
            .as_ref()
            .map(|menu| menu.is_document_view)
            .unwrap_or(false);

        self.context_menu = None;
        self.restore_focus_after_context_menu(is_document_view, window, cx);
        cx.notify();
    }

    /// Wraps the cascade in the deferred, window-level overlay: a full-size
    /// click-catcher (closes the menu) that also carries the menu's focus
    /// handle and keymap binding.
    pub(super) fn render_context_menu_overlay(
        &self,
        menu: AnyElement,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        // Use deferred() to render at window level for correct positioning
        deferred(
            div()
                .id("context-menu-overlay")
                .absolute()
                .top_0()
                .left_0()
                .size_full()
                .track_focus(&self.focus.context_menu_focus)
                // The grid reports the ContextMenu context while the menu is
                // open, so the keymap's menu keys arrive here first.
                .on_action(cx.listener(
                    |this, action: &dbflux_ui_base::keymap::RunCommand, window, cx| {
                        let handled = dbflux_ui_base::keymap::run_command(action)
                            .is_some_and(|command| this.dispatch_menu_command(command, window, cx));

                        if !handled {
                            cx.propagate();
                        }
                    },
                ))
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(|this, _, window, cx| this.dismiss_context_menu(window, cx)),
                )
                .on_mouse_down(
                    MouseButton::Right,
                    cx.listener(|this, _, window, cx| this.dismiss_context_menu(window, cx)),
                )
                // The overlay covers the grid, so a wheel here would scroll
                // nothing; closing instead keeps the menu from hanging over a
                // grid that has scrolled away from under it.
                .on_scroll_wheel(
                    cx.listener(|this, _, window, cx| this.dismiss_context_menu(window, cx)),
                )
                // The cascade hangs inside a "context-menu" element, so every
                // level's rows sit under it: the id shape the keyboard-coverage
                // registry already names (`context-menu.*`).
                .child(div().id("context-menu").child(menu)),
        )
        .with_priority(1)
    }
}

#[cfg(test)]
mod tests {
    /// The cascade must never be clipped by the element that mounts it. Bolt
    /// draws each open level as its own sibling panel, so no parent panel can
    /// clip a flyout; the overlay that mounts the cascade must not clip
    /// either.
    #[test]
    fn the_overlay_does_not_clip_the_cascade_it_mounts() {
        let source = include_str!("sections.rs");
        let wrapper_start = source
            .find(".id(\"context-menu\")")
            .expect("the cascade hangs in a context-menu element built in this file");
        let wrapper = &source[wrapper_start..];
        let wrapper_end = wrapper
            .find(".child(menu)")
            .expect("the wrapper takes the cascade as its child");
        assert!(
            !wrapper[..wrapper_end].contains(".overflow_hidden()"),
            "the element that mounts the cascade clips its flyouts"
        );

        let mounted = include_str!("mod.rs");
        assert!(
            mounted.contains("render_menu_tree("),
            "the cascade must render through render_menu_tree, which draws each \
             open level as a sibling panel, so no parent panel clips a flyout"
        );
    }

    #[test]
    fn context_menu_sections_keys_resolve_in_both_locales() {
        let keys = [
            "document.data.context_menu.filter.title",
            "document.data.context_menu.filter.cell_value",
            "document.data.context_menu.order.title",
            "document.data.context_menu.order.remove",
            "document.data.context_menu.generate_sql.title",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn context_menu_filter_title_differs_between_locales() {
        let en = dbflux_i18n::t!("document.data.context_menu.filter.title", locale = "en");
        let es = dbflux_i18n::t!("document.data.context_menu.filter.title", locale = "es");

        assert_eq!(en, "Filter");
        assert_ne!(en, es);
    }
}
