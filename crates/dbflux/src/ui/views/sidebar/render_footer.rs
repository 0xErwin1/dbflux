use super::*;

impl Sidebar {
    pub(super) fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();

        div()
            .w_full()
            .px(Spacing::SM)
            .py(Spacing::XS)
            .border_t_1()
            .border_color(theme.border)
            .child(
                div()
                    .id("settings-btn")
                    .w_full()
                    .h(Heights::ROW)
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .px(Spacing::SM)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .text_size(FontSizes::SM)
                    .text_color(theme.muted_foreground)
                    .hover(|d| d.bg(theme.secondary).text_color(theme.foreground))
                    .on_click(move |_, _, cx| {
                        let sidebar = sidebar.clone();

                        if let Some(handle) = app_state.read(cx).settings_window {
                            if handle
                                .update(cx, |_root, window, _cx| window.activate_window())
                                .is_ok()
                            {
                                return;
                            }
                            app_state.update(cx, |state, _| {
                                state.settings_window = None;
                            });
                        }

                        let app_state_for_window = app_state.clone();
                        if let Ok(handle) = cx.open_window(
                            WindowOptions {
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
                                kind: WindowKind::Floating,
                                focus: true,
                                ..Default::default()
                            },
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
                                            }
                                        });
                                    },
                                )
                                .detach();

                                cx.new(|cx| Root::new(settings, window, cx))
                            },
                        ) {
                            app_state.update(cx, |state, _| {
                                state.settings_window = Some(handle);
                            });
                        }
                    })
                    .child(
                        svg()
                            .path(AppIcon::Settings.path())
                            .size_4()
                            .text_color(theme.muted_foreground),
                    )
                    .child("Settings"),
            )
    }
}
