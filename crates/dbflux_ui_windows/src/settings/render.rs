use crate::tokens::SettingsMetrics;
use dbflux_components::components::tree_nav::FlatRow;
use dbflux_components::composites::{Island, ListRow};
use dbflux_components::controls::{Button, Input};
use dbflux_components::primitives::{Chamfer, ChamferRing, Icon, Kbd, Text};
use dbflux_components::tokens::{ChamferCut, ChromeColors, Fields, IslandMetrics, ShellMetrics};
use dbflux_ui_base::keymap::{RunCommand, run_command};
use dbflux_ui_base::platform;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::dialog::{Dialog, DialogButtonProps};

use super::{
    SETTINGS_SIDEBAR_GRIP_WIDTH, SETTINGS_SIDEBAR_MAX_WIDTH, SETTINGS_SIDEBAR_MIN_WIDTH,
    SettingsCoordinator, SettingsFocus, layout,
};

impl SettingsCoordinator {
    fn section_display_name(section: super::SettingsSectionId) -> String {
        match section {
            super::SettingsSectionId::General => dbflux_i18n::t!("settings.nav.general"),
            super::SettingsSectionId::Audit => dbflux_i18n::t!("settings.nav.audit"),
            #[cfg(feature = "mcp")]
            super::SettingsSectionId::McpClients => dbflux_i18n::t!("settings.nav.mcp_clients"),
            #[cfg(feature = "mcp")]
            super::SettingsSectionId::McpRoles => dbflux_i18n::t!("settings.nav.mcp_roles"),
            #[cfg(feature = "mcp")]
            super::SettingsSectionId::McpPolicies => dbflux_i18n::t!("settings.nav.mcp_policies"),
            super::SettingsSectionId::Keybindings => dbflux_i18n::t!("settings.nav.keybindings"),
            super::SettingsSectionId::Updates => dbflux_i18n::t!("settings.nav.updates"),
            super::SettingsSectionId::Proxies => dbflux_i18n::t!("settings.nav.proxies"),
            super::SettingsSectionId::SshTunnels => dbflux_i18n::t!("settings.nav.ssh_tunnels"),
            super::SettingsSectionId::AuthProfiles => {
                dbflux_i18n::t!("settings.auth_profiles.section_title")
            }
            super::SettingsSectionId::Services => dbflux_i18n::t!("settings.nav.services"),
            super::SettingsSectionId::Hooks => dbflux_i18n::t!("settings.nav.hooks"),
            super::SettingsSectionId::Drivers => dbflux_i18n::t!("settings.nav.drivers"),
            super::SettingsSectionId::About => dbflux_i18n::t!("settings.nav.about"),
        }
    }

    /// Navigation column (P1Settings*): the search field, then each group's
    /// label over its entries.
    fn render_sidebar(&self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let focused = self.focus_area == SettingsFocus::Sidebar;
        let cursor_pos = self.sidebar_tree.cursor();
        let rows = self.sidebar_tree.rows();

        let mut row_elements: Vec<AnyElement> = Vec::with_capacity(rows.len());

        for (idx, row) in rows.iter().enumerate() {
            let is_group = row.has_children && !row.selectable;

            if is_group {
                row_elements.push(Self::render_group_row(row));
            } else {
                let is_cursor = focused && idx == cursor_pos;
                row_elements.push(self.render_item_row(row, is_cursor, cx));
            }
        }

        let no_results = rows.is_empty();

        Island::new()
            .w_full()
            .h_full()
            .child(self.render_nav_search(window, cx))
            .child(
                div()
                    .id("settings-nav")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .flex()
                    .flex_col()
                    .children(row_elements)
                    .when(no_results, |list| {
                        list.child(
                            div()
                                .px(SettingsMetrics::NAV_ROW_PADDING_X)
                                .py(SettingsMetrics::NAV_GROUP_PADDING_TOP)
                                .child(
                                    Text::body(dbflux_i18n::t!("settings.nav.no_results"))
                                        .muted_foreground(),
                                ),
                        )
                    }),
            )
    }

    /// Search field of the navigation: a 30 px chamfered field with the
    /// search icon, the frameless input and the `/` keycap that focuses it.
    fn render_nav_search(&self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let search = self.nav_search.read(cx);
        let query_is_empty = search.value().is_empty();
        let focused = search.focus_handle(cx).contains_focused(window, cx);

        let mut shape = Chamfer::new(ChamferCut::CONTROL)
            .fill(theme.background)
            .border(theme.border);

        if focused {
            shape = shape.ring(ChamferRing::focus(ChromeColors::tint(theme)));
        }

        div()
            .flex_shrink_0()
            .p(SettingsMetrics::NAV_SEARCH_PADDING)
            .child(
                div()
                    .id("settings-nav-search")
                    .relative()
                    .flex()
                    .items_center()
                    .gap(Fields::GAP)
                    .h(Fields::HEIGHT)
                    .px(Fields::PADDING_X)
                    .text_size(Fields::TEXT)
                    .child(shape)
                    .child(
                        Icon::new(dbflux_components::icons::AppIcon::Search)
                            .size(SettingsMetrics::NAV_SEARCH_ICON)
                            .color(theme.muted_foreground),
                    )
                    .child(
                        div().flex_1().min_w_0().child(
                            Input::new(&self.nav_search)
                                .id("settings-nav-search-input")
                                .aria_label(dbflux_i18n::t!("settings.nav.search_placeholder"))
                                .small()
                                .appearance(false)
                                .cleanable(true),
                        ),
                    )
                    .when(query_is_empty, |field| field.child(Kbd::new("/"))),
            )
    }

    /// Group label of the navigation: an uppercase label, not selectable.
    fn render_group_row(row: &FlatRow) -> AnyElement {
        div()
            .id(SharedString::from(format!("cat-{}", row.id)))
            .flex()
            .items_center()
            .pt(SettingsMetrics::NAV_GROUP_PADDING_TOP)
            .px(SettingsMetrics::NAV_GROUP_PADDING_X)
            .pb(SettingsMetrics::NAV_GROUP_PADDING_BOTTOM)
            .child(Text::label(row.label.clone()).font_size(ShellMetrics::SECTION_LABEL_FONT))
            .into_any_element()
    }

    /// Entry of the navigation: icon and label on a 32 px row. The active
    /// section gets the tint wash and the left bar; the keyboard cursor
    /// draws the focus ring on any other row while the navigation holds
    /// focus.
    fn render_item_row(
        &self,
        row: &FlatRow,
        is_cursor: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();
        let row_id = row.id.clone();
        let is_active = Self::section_for_tree_id(row.id.as_ref()) == Some(self.active_section);

        let icon_color = if is_active {
            ChromeColors::tint(theme)
        } else {
            theme.muted_foreground
        };
        let text_color = if is_active {
            ChromeColors::strong(theme)
        } else {
            theme.foreground
        };

        ListRow::new(SharedString::from(format!("settings-nav-{}", row.id)))
            .selected(is_active)
            .selection_bar(true)
            .focused(is_cursor && !is_active)
            .build(cx)
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(SettingsMetrics::NAV_ROW_GAP)
            .h(SettingsMetrics::NAV_ROW_HEIGHT)
            .px(SettingsMetrics::NAV_ROW_PADDING_X)
            .on_click(cx.listener(move |this, _, window, cx| {
                if let Some(section) = Self::section_for_tree_id(row_id.as_ref()) {
                    this.sidebar_tree.select_by_id(row_id.as_ref());
                    this.request_section_transition(section, window, cx);
                }
            }))
            .when_some(row.icon, |row, icon| {
                row.child(
                    Icon::new(icon)
                        .size(SettingsMetrics::NAV_ICON)
                        .color(icon_color),
                )
            })
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .text_ellipsis()
                    .child(Text::body(row.label.clone()).color(text_color)),
            )
            .into_any_element()
    }
}

impl Render for SettingsCoordinator {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.sidebar_user_resized {
            self.sidebar_width = super::scaled_default_sidebar_width(cx);
        }

        if self.pending_focus_return {
            self.pending_focus_return = false;
            self.focus_area = SettingsFocus::Content;
            self.active_section_entity.focus_in(_window, cx);
            self.focus_handle.focus(_window, cx);
        }

        // A section asked to open a portability overlay; do it here where a
        // `Window` is available (the section-event subscription had none).
        if let Some(target) = self.pending_export_target.take() {
            self.import_visible = false;
            self.export_modal.update(cx, |modal, cx| {
                modal.open_target(target, _window, cx);
            });
        }
        if std::mem::take(&mut self.pending_import_open) {
            self.export_modal.update(cx, |modal, cx| modal.close(cx));
            self.import_visible = true;
            self.import_panel.update(cx, |panel, cx| {
                panel.reset(_window, cx);
            });
        }

        let _ = self.app_state.read(cx);

        let csd_title_bar = platform::render_csd_title_bar(
            _window,
            cx,
            &dbflux_i18n::t!("connection_manager.tab.settings"),
        );

        let has_title_row = csd_title_bar.is_some();

        div()
            .size_full()
            .relative()
            .bg(ChromeColors::desk(cx.theme()))
            .text_size(dbflux_components::tokens::FontSizes::BASE)
            .flex()
            .flex_col()
            .track_focus(&self.focus_handle)
            .key_context(self.root_key_context())
            .on_action(cx.listener(|this, action: &RunCommand, window, cx| {
                let handled = run_command(action)
                    .is_some_and(|command| this.handle_command(command, window, cx));

                if !handled {
                    cx.propagate();
                }
            }))
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                this.handle_key_event(event, window, cx);
            }))
            .when_some(csd_title_bar, |el, title_bar| el.child(title_bar))
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .flex()
                    .px(IslandMetrics::GAP)
                    .when(!has_title_row, |body| body.pt(IslandMetrics::GAP))
                    .child(
                        div()
                            .h_full()
                            .w(self.sidebar_width)
                            .flex_shrink_0()
                            .child(self.render_sidebar(_window, cx)),
                    )
                    .child(self.render_sidebar_grip(cx))
                    .child(Island::new().flex_1().min_w_0().h_full().child(
                        layout::section_container(self.active_section_view.clone()).h_full(),
                    )),
            )
            // Settings status footer
            .child(self.render_settings_footer(_window, cx))
            .when_some(self.pending_section_confirm, |element, target_section| {
                let confirm_entity = cx.entity().clone();
                let cancel_entity = confirm_entity.clone();
                let section_name = Self::section_display_name(target_section);

                element.child(
                    Dialog::new(cx)
                        .title(dbflux_i18n::t!("settings.discard.title"))
                        .button_props(DialogButtonProps::default().show_cancel(true))
                        .overlay_closable(false)
                        .close_button(false)
                        .on_ok(move |_, window, cx| {
                            confirm_entity.update(cx, |this, cx| {
                                this.confirm_section_transition(window, cx);
                            });
                            true
                        })
                        .on_cancel(move |_, _, cx| {
                            cancel_entity.update(cx, |this, cx| {
                                this.cancel_section_transition(cx);
                            });
                            true
                        })
                        .child(Text::body(crate::labels::settings_discard_body(
                            &section_name,
                        ))),
                )
            })
            .when(self.export_modal.read(cx).is_visible(), |root| {
                root.child(self.export_modal.clone())
            })
            .when(self.import_visible, |root| {
                root.child(self.import_panel.clone())
            })
    }
}

impl SettingsCoordinator {
    fn render_sidebar_grip(&self, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("settings-sidebar-grip")
            .h_full()
            .flex_shrink_0()
            .w(SETTINGS_SIDEBAR_GRIP_WIDTH)
            .cursor_col_resize()
            .hover(|el| el.bg(cx.theme().accent.opacity(0.25)))
            .when(self.sidebar_is_resizing, |el| {
                el.bg(ChromeColors::tint(cx.theme()))
            })
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, event: &MouseDownEvent, _, cx| {
                    this.sidebar_is_resizing = true;
                    this.sidebar_user_resized = true;
                    this.sidebar_resize_start_x = Some(event.position.x);
                    this.sidebar_resize_start_width = Some(this.sidebar_width);
                    cx.notify();
                }),
            )
            .on_mouse_move(cx.listener(|this, event: &MouseMoveEvent, _, cx| {
                if !this.sidebar_is_resizing {
                    return;
                }

                let Some(start_x) = this.sidebar_resize_start_x else {
                    return;
                };
                let Some(start_width) = this.sidebar_resize_start_width else {
                    return;
                };

                let delta = event.position.x - start_x;
                this.sidebar_width = (start_width + delta)
                    .clamp(SETTINGS_SIDEBAR_MIN_WIDTH, SETTINGS_SIDEBAR_MAX_WIDTH);
                cx.notify();
            }))
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.sidebar_is_resizing = false;
                    this.sidebar_resize_start_x = None;
                    this.sidebar_resize_start_width = None;
                    cx.notify();
                }),
            )
    }

    /// Footer (P1Settings*): the unsaved-changes count and the section's
    /// leading actions on the left; Close and the section's actions on the
    /// right.
    fn render_settings_footer(
        &self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let leading_actions = self
            .active_section_entity
            .render_footer_leading_actions(window, cx);
        let section_actions = self.active_section_entity.render_footer_actions(window, cx);
        let unsaved = self.active_section_entity.unsaved_change_count(cx);
        let theme = cx.theme();

        div()
            .flex_shrink_0()
            .h(SettingsMetrics::FOOTER_HEIGHT)
            .px(SettingsMetrics::FOOTER_PADDING_X)
            .flex()
            .items_center()
            .gap(SettingsMetrics::FOOTER_GAP)
            .when(unsaved > 0, |footer| {
                footer.child(unsaved_changes_marker(unsaved, theme.warning))
            })
            .when_some(leading_actions, |footer, actions| footer.child(actions))
            .child(div().flex_1())
            .child(
                Button::new("settings-close", dbflux_i18n::t!("settings.action.close"))
                    .secondary()
                    .when_some(super::close_shortcut(), Button::kbd)
                    .on_click(cx.listener(|this, _, window, _cx| {
                        this.try_close(window);
                    })),
            )
            .when_some(section_actions, |footer, actions| footer.child(actions))
    }
}

/// Text of the footer's unsaved-changes marker.
pub(super) fn unsaved_changes_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("settings.footer.unsaved.one")
    } else {
        dbflux_i18n::t!("settings.footer.unsaved.many", count = count)
    }
}

/// Warning diamond and "N unsaved changes" in the warning color.
fn unsaved_changes_marker(count: usize, color: Hsla) -> impl IntoElement {
    div()
        .id("settings-unsaved-changes")
        .flex()
        .items_center()
        .gap(SettingsMetrics::FOOTER_GAP)
        .text_size(SettingsMetrics::DIRTY_FONT)
        .text_color(color)
        .child(dbflux_components::primitives::status_diamond(
            color,
            SettingsMetrics::DIRTY_MARKER,
        ))
        .child(unsaved_changes_label(count))
}

#[cfg(test)]
mod section_title_i18n_tests {
    use super::SettingsCoordinator;
    use crate::settings::SettingsSectionId;

    #[test]
    fn section_display_name_covers_every_section_with_non_empty_titles() {
        let sections: &[SettingsSectionId] = &[
            SettingsSectionId::General,
            SettingsSectionId::Audit,
            #[cfg(feature = "mcp")]
            SettingsSectionId::McpClients,
            #[cfg(feature = "mcp")]
            SettingsSectionId::McpRoles,
            #[cfg(feature = "mcp")]
            SettingsSectionId::McpPolicies,
            SettingsSectionId::Keybindings,
            SettingsSectionId::Updates,
            SettingsSectionId::Proxies,
            SettingsSectionId::SshTunnels,
            SettingsSectionId::AuthProfiles,
            SettingsSectionId::Services,
            SettingsSectionId::Hooks,
            SettingsSectionId::Drivers,
            SettingsSectionId::About,
        ];

        for section in sections {
            let title = SettingsCoordinator::section_display_name(*section);

            assert!(!title.is_empty(), "{section:?} produced an empty title");
        }
    }

    #[test]
    fn section_display_name_matches_the_translated_nav_and_section_title_keys() {
        assert_eq!(
            SettingsCoordinator::section_display_name(SettingsSectionId::General),
            dbflux_i18n::t!("settings.nav.general")
        );
        assert_eq!(
            SettingsCoordinator::section_display_name(SettingsSectionId::Services),
            dbflux_i18n::t!("settings.nav.services")
        );
        assert_eq!(
            SettingsCoordinator::section_display_name(SettingsSectionId::AuthProfiles),
            dbflux_i18n::t!("settings.auth_profiles.section_title")
        );
    }

    #[test]
    fn unsaved_changes_label_uses_singular_and_plural_forms() {
        assert_eq!(super::unsaved_changes_label(1), "1 unsaved change");
        assert_eq!(super::unsaved_changes_label(3), "3 unsaved changes");
    }

    #[test]
    fn discard_dialog_and_close_action_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in ["settings.discard.title", "settings.action.close"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, key, "key {key} did not resolve for locale {locale}");
            }
        }
    }
}
