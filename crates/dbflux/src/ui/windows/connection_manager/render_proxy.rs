use crate::ui::components::dropdown::DropdownItem;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::{Icon, IconName};

use super::{ActiveTab, ConnectionManagerWindow, EditState, FormFocus};

impl ConnectionManagerWindow {
    pub(super) fn render_proxy_tab(&mut self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let proxies = self.app_state.read(cx).proxies().to_vec();
        let selected_proxy_id = self.selected_proxy_id;

        let show_focus =
            self.edit_state == EditState::Navigating && self.active_tab == ActiveTab::Proxy;
        let focus = self.form_focus;

        let ring_color = cx.theme().ring;
        let theme = cx.theme().clone();
        let muted_fg = theme.muted_foreground;

        let mut sections: Vec<AnyElement> = Vec::new();

        if proxies.is_empty() {
            sections.push(
                div()
                    .flex_1()
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .items_center()
                            .gap_2()
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(muted_fg)
                                    .child("No proxy profiles configured"),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(muted_fg)
                                    .child("Add proxies in Settings > Proxies"),
                            ),
                    )
                    .into_any_element(),
            );
            return sections;
        }

        // Proxy selector dropdown
        let proxy_items: Vec<DropdownItem> = proxies
            .iter()
            .map(|p| {
                let label = if p.enabled {
                    p.name.clone()
                } else {
                    format!("{} (disabled)", p.name)
                };
                DropdownItem::with_value(&label, p.id.to_string())
            })
            .collect();
        self.proxy_uuids = proxies.iter().map(|p| p.id).collect();

        let selected_proxy_index =
            selected_proxy_id.and_then(|id| proxies.iter().position(|p| p.id == id));

        let proxy_selector_focused = show_focus && focus == FormFocus::ProxySelector;
        let proxy_clear_focused = show_focus && focus == FormFocus::ProxyClear;
        self.proxy_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(proxy_items, cx);
            dropdown.set_selected_index(selected_proxy_index, cx);
            let focus_color = if proxy_selector_focused {
                Some(ring_color)
            } else {
                None
            };
            dropdown.set_focus_ring(focus_color, cx);
        });

        let has_selection = selected_proxy_id.is_some();

        let selector_row = div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(muted_fg)
                    .child("Select Proxy"),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(div().flex_1().child(self.proxy_dropdown.clone()))
                    .when(has_selection, |d| {
                        d.child(
                            div()
                                .rounded(px(4.0))
                                .border_2()
                                .when(proxy_clear_focused, |dd| dd.border_color(ring_color))
                                .when(!proxy_clear_focused, |dd| {
                                    dd.border_color(gpui::transparent_black())
                                })
                                .child(
                                    Button::new("clear-proxy")
                                        .label("Clear")
                                        .small()
                                        .ghost()
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.clear_proxy_selection(cx);
                                        })),
                                ),
                        )
                    }),
            );

        sections.push(selector_row.into_any_element());

        // Read-only proxy details when a proxy is selected
        if let Some(proxy) = selected_proxy_id
            .and_then(|id| proxies.iter().find(|p| p.id == id))
            .cloned()
        {
            let kind_label = format!("{:?}", proxy.kind);
            let host_port = format!("{}:{}", proxy.host, proxy.port);
            let auth_label = format!("{:?}", proxy.auth);
            let enabled_label = if proxy.enabled { "Yes" } else { "No" };
            let no_proxy_label = proxy.no_proxy.as_deref().unwrap_or("(none)").to_string();

            let edit_focused = show_focus && focus == FormFocus::ProxyEditInSettings;

            let details = self.render_section(
                "Proxy Details",
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(self.render_readonly_row("Type", &kind_label, &theme))
                    .child(self.render_readonly_row("Host", &host_port, &theme))
                    .child(self.render_readonly_row("Auth", &auth_label, &theme))
                    .child(self.render_readonly_row("Enabled", enabled_label, &theme))
                    .child(self.render_readonly_row("No Proxy", &no_proxy_label, &theme))
                    .child(
                        div()
                            .mt_1()
                            .rounded(px(4.0))
                            .border_2()
                            .when(edit_focused, |d| d.border_color(ring_color))
                            .when(!edit_focused, |d| d.border_color(gpui::transparent_black()))
                            .child(
                                Button::new("proxy-edit-in-settings")
                                    .label("Edit in Settings")
                                    .small()
                                    .ghost()
                                    .icon(Icon::new(IconName::ExternalLink)),
                            ),
                    ),
                &theme,
            );

            sections.push(details.into_any_element());
        }

        sections
    }
}
