use super::*;
use crate::platform;
use dbflux_components::primitives::{Icon, StatusDot, StatusDotVariant, Text};

impl Sidebar {
    pub(super) fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();

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

                        let app_state_for_window = app_state.clone();
                        let mut options = WindowOptions {
                            app_id: Some("dbflux".into()),
                            titlebar: Some(TitlebarOptions {
                                title: Some("Settings".into()),
                                ..Default::default()
                            }),
                            window_bounds: Some(WindowBounds::Windowed(Bounds::centered(
                                None,
                                size(px(950.0), px(700.0)),
                                cx,
                            ))),
                            focus: true,
                            ..Default::default()
                        };
                        platform::apply_window_options(&mut options, 800.0, 600.0);

                        let _ = cx.open_window(
                            options,
                            |window, cx| {
                                let settings = cx.new(|cx| {
                                    SettingsWindow::new(app_state_for_window, window, cx)
                                });

                                cx.subscribe(
                                    &settings,
                                    move |_settings, event: &crate::ui::windows::settings::SettingsEvent, cx| {
                                        sidebar.update(cx, |_this, cx| {
                                            match event {
                                                crate::ui::windows::settings::SettingsEvent::OpenScript { path } => {
                                                    cx.emit(SidebarEvent::OpenScript { path: path.clone() });
                                                }
                                                crate::ui::windows::settings::SettingsEvent::OpenLoginModal { .. } => {}
                                            }
                                        });
                                    },
                                )
                                .detach();

                                cx.new(|cx| Root::new(settings, window, cx))
                            },
                        );
                    })
                    .child(
                        Icon::new(AppIcon::Settings)
                            .size(px(14.0))
                            .color(theme.muted_foreground),
                    ),
            )
    }
}
