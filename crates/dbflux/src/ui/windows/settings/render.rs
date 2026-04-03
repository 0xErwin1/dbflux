use crate::ui::components::tree_nav::{self, FlatRow};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::Heights;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::dialog::Dialog;

use super::{
    SETTINGS_SIDEBAR_GRIP_WIDTH, SETTINGS_SIDEBAR_MAX_WIDTH, SETTINGS_SIDEBAR_MIN_WIDTH,
    SettingsCoordinator, SettingsFocus,
};

const INDENT_PX: f32 = 16.0;

impl SettingsCoordinator {
    fn section_display_name(section: super::SettingsSectionId) -> &'static str {
        match section {
            super::SettingsSectionId::General => "General",
            super::SettingsSectionId::Audit => "Audit",
            #[cfg(feature = "mcp")]
            super::SettingsSectionId::McpClients => "Clients",
            #[cfg(feature = "mcp")]
            super::SettingsSectionId::McpRoles => "Roles",
            #[cfg(feature = "mcp")]
            super::SettingsSectionId::McpPolicies => "Policies",
            super::SettingsSectionId::Keybindings => "Keybindings",
            super::SettingsSectionId::Proxies => "Proxy",
            super::SettingsSectionId::SshTunnels => "SSH Tunnels",
            super::SettingsSectionId::AuthProfiles => "Auth Profiles",
            super::SettingsSectionId::Services => "Services",
            super::SettingsSectionId::Hooks => "Hooks",
            super::SettingsSectionId::Drivers => "Drivers",
            super::SettingsSectionId::About => "About",
        }
    }

    fn render_sidebar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let border_color = cx.theme().border;
        let sidebar_bg = cx.theme().sidebar;
        let theme = cx.theme().clone();
        let focused = self.focus_area == SettingsFocus::Sidebar;
        let row_height = Heights::ROW;
        let line_color = tree_nav::tree_line_color(&theme);

        let rows = self.sidebar_tree.rows();
        let cursor_pos = self.sidebar_tree.cursor();

        let mut row_elements: Vec<AnyElement> = Vec::with_capacity(rows.len());

        for (idx, row) in rows.iter().enumerate() {
            let is_cursor = focused && idx == cursor_pos;
            let gutter = tree_nav::render_gutter(
                row.depth,
                row.is_last,
                &row.ancestors_continue,
                INDENT_PX,
                row_height,
                line_color,
                true,
            );
            let is_group = row.has_children && !row.selectable;

            let content: AnyElement = if is_group {
                self.render_group_row(row, is_cursor, &theme, cx)
            } else {
                self.render_item_row(row, is_cursor, focused, &theme, cx)
            };

            let mut outer = div()
                .flex()
                .items_center()
                .h(row_height)
                .child(gutter)
                .child(content);

            if is_group {
                outer = outer.mt_2();
            }

            row_elements.push(outer.into_any_element());
        }

        div()
            .w_full()
            .h_full()
            .border_r_1()
            .border_color(border_color)
            .bg(sidebar_bg)
            .flex()
            .flex_col()
            .p_2()
            .gap_0()
            .children(row_elements)
            .child(div().flex_1())
            .child({
                div().p_1().border_t_1().border_color(border_color).child(
                    Button::new("close-settings")
                        .label("Close")
                        .ghost()
                        .small()
                        .on_click(cx.listener(|this, _, window, _cx| {
                            this.try_close(window);
                        })),
                )
            })
    }

    fn render_group_row(
        &self,
        row: &FlatRow,
        is_cursor: bool,
        theme: &gpui_component::Theme,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let chevron_icon = if row.expanded {
            AppIcon::ChevronDown
        } else {
            AppIcon::ChevronRight
        };

        let row_id = row.id.clone();

        let inner = div()
            .flex()
            .items_center()
            .gap_1p5()
            .child(
                svg()
                    .path(chevron_icon.path())
                    .size(px(12.0))
                    .text_color(theme.muted_foreground),
            )
            .when_some(row.icon, |div, icon| {
                div.child(
                    svg()
                        .path(icon.path())
                        .size(px(14.0))
                        .text_color(theme.muted_foreground),
                )
            })
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(theme.muted_foreground)
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .text_ellipsis()
                    .child(row.label.clone()),
            );

        div()
            .id(SharedString::from(format!("cat-{}", row.id)))
            .flex_1()
            .h_full()
            .px_2()
            .flex()
            .items_center()
            .rounded(px(4.0))
            .cursor_pointer()
            .border_1()
            .border_color(if is_cursor {
                theme.primary
            } else {
                transparent_black()
            })
            .hover(|d| d.bg(theme.secondary))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.sidebar_tree.select_by_id(row_id.as_ref());

                let _ = this.sidebar_tree.activate();
                cx.notify();
            }))
            .child(inner)
            .into_any_element()
    }

    fn render_item_row(
        &self,
        row: &FlatRow,
        is_cursor: bool,
        sidebar_focused: bool,
        theme: &gpui_component::Theme,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let row_id = row.id.clone();
        let is_active = Self::section_for_tree_id(row.id.as_ref()) == Some(self.active_section);
        let show_active = is_active && !sidebar_focused;

        let content_inner = div()
            .flex()
            .items_center()
            .gap_2()
            .when_some(row.icon, |div, icon| {
                div.child(
                    svg()
                        .path(icon.path())
                        .size_4()
                        .text_color(theme.muted_foreground),
                )
            })
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .text_ellipsis()
                    .child(row.label.clone()),
            );

        div()
            .id(row.id.clone())
            .flex_1()
            .h_full()
            .px_2()
            .flex()
            .items_center()
            .rounded(px(4.0))
            .text_sm()
            .cursor_pointer()
            .border_1()
            .border_color(if is_cursor {
                theme.primary
            } else {
                transparent_black()
            })
            .when(show_active, |div| {
                div.bg(theme.secondary)
                    .font_weight(FontWeight::MEDIUM)
                    .border_l_2()
                    .border_color(theme.primary)
            })
            .when(!is_active, |div| {
                div.hover(|hover| hover.bg(theme.secondary))
            })
            .on_click(cx.listener(move |this, _, window, cx| {
                if let Some(section) = Self::section_for_tree_id(row_id.as_ref()) {
                    this.sidebar_tree.select_by_id(row_id.as_ref());
                    this.request_section_transition(section, window, cx);
                }
            }))
            .child(content_inner)
            .into_any_element()
    }
}

impl Render for SettingsCoordinator {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.pending_focus_return {
            self.pending_focus_return = false;
            self.focus_area = SettingsFocus::Content;
            self.active_section_entity.focus_in(_window, cx);
            self.focus_handle.focus(_window);
        }

        let _ = self.app_state.read(cx);

        div()
            .size_full()
            .bg(cx.theme().background)
            .flex()
            .track_focus(&self.focus_handle)
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                this.handle_key_event(event, window, cx);
            }))
            .child(
                div()
                    .h_full()
                    .w(self.sidebar_width)
                    .flex()
                    .flex_row()
                    .child(div().h_full().flex_1().child(self.render_sidebar(cx)))
                    .child(self.render_sidebar_grip(cx)),
            )
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .h_full()
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .bg(cx.theme().background)
                    .child(self.active_section_view.clone()),
            )
            .when_some(self.pending_section_confirm, |element, target_section| {
                let confirm_entity = cx.entity().clone();
                let cancel_entity = confirm_entity.clone();
                let section_name = Self::section_display_name(target_section).to_string();

                element.child(
                    Dialog::new(_window, cx)
                        .title("Discard Unsaved Changes")
                        .confirm()
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
                        .child(div().text_sm().child(format!(
                            "You have unsaved changes. Discard them and switch to {}?",
                            section_name
                        ))),
                )
            })
    }
}

impl SettingsCoordinator {
    fn render_sidebar_grip(&self, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("settings-sidebar-grip")
            .h_full()
            .w(SETTINGS_SIDEBAR_GRIP_WIDTH)
            .cursor_col_resize()
            .hover(|el| el.bg(cx.theme().accent.opacity(0.25)))
            .when(self.sidebar_is_resizing, |el| el.bg(cx.theme().primary))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, event: &MouseDownEvent, _, cx| {
                    this.sidebar_is_resizing = true;
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
}
