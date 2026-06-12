use super::*;
use dbflux_components::primitives::{Icon, StatusDot, StatusDotVariant, Text};

impl Sidebar {
    pub(super) fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();
        let sidebar_for_import = cx.entity().clone();

        let state = self.app_state.read(cx);
        let connected_count = state.connections().len();
        let total_profiles = state.profiles().len();
        let idle_count = total_profiles.saturating_sub(connected_count);

        let status_text = format!("{} connected · {} idle", connected_count, idle_count);
        let dot_variant = if connected_count > 0 {
            StatusDotVariant::Success
        } else {
            StatusDotVariant::Idle
        };

        div()
            .w_full()
            .h(px(30.0))
            .flex()
            .items_center()
            .justify_between()
            .px(Spacing::SM)
            .border_t_1()
            .border_color(theme.border)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .child(StatusDot::new(dot_variant))
                    .child(
                        Text::body(status_text)
                            .font_size(FontSizes::XS)
                            .color(theme.muted_foreground),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .child(
                        div()
                            .id("import-connections-btn")
                            .flex()
                            .items_center()
                            .justify_center()
                            .size(px(22.0))
                            .rounded(Radii::SM)
                            .cursor_pointer()
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(move |_, _, cx| {
                                sidebar_for_import.update(cx, |_this, cx| {
                                    cx.emit(SidebarEvent::RequestImportConnections);
                                });
                            })
                            .child(
                                Icon::new(AppIcon::ArrowDown)
                                    .size(px(14.0))
                                    .color(theme.muted_foreground),
                            ),
                    )
                    .child(
                        div()
                            .id("settings-btn")
                            .flex()
                            .items_center()
                            .justify_center()
                            .size(px(22.0))
                            .rounded(Radii::SM)
                            .cursor_pointer()
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(move |_, _, cx| {
                                let sidebar = sidebar.clone();
                                dbflux_ui_windows::settings::open_or_focus_settings(
                                    app_state.clone(),
                                    None,
                                    cx,
                                    move |settings, cx| {
                                        cx.subscribe(
                                            settings,
                                            move |_settings, event: &dbflux_ui_windows::settings::SettingsEvent, cx| {
                                                sidebar.update(cx, |_this, cx| {
                                                    match event {
                                                        dbflux_ui_windows::settings::SettingsEvent::OpenScript { path } => {
                                                            cx.emit(SidebarEvent::OpenScript { path: path.clone() });
                                                        }
                                                        dbflux_ui_windows::settings::SettingsEvent::OpenLoginModal { .. } => {}
                                                    }
                                                });
                                            },
                                        )
                                        .detach();
                                    },
                                );
                            })
                            .child(
                                Icon::new(AppIcon::Settings)
                                    .size(px(14.0))
                                    .color(theme.muted_foreground),
                            ),
                    ),
            )
    }
}
