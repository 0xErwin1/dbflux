use super::*;

impl Sidebar {
    pub fn render_menu_panel(
        theme: &gpui_component::Theme,
        items: &[ContextMenuItem],
        selected_index: Option<usize>,
        sidebar: Option<Entity<Self>>,
        panel_id: &str,
        is_parent_menu: bool,
    ) -> impl IntoElement {
        div()
            .min_w_40()
            .bg(theme.popover)
            .border_1()
            .border_color(theme.border)
            .rounded(Radii::MD)
            .shadow_lg()
            .py_1()
            .children(items.iter().enumerate().map(|(idx, item)| {
                let is_selected = selected_index == Some(idx);
                let is_submenu = matches!(item.action, ContextMenuAction::Submenu(_));
                let icon = item.action.icon();
                let sidebar_for_click = sidebar.clone();
                let item_id = SharedString::from(format!("{}-item-{}", panel_id, idx));

                let icon_color = if is_selected {
                    theme.accent_foreground
                } else {
                    theme.muted_foreground
                };

                div()
                    .id(item_id)
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap_4()
                    .px_3()
                    .py(px(6.0))
                    .text_size(FontSizes::SM)
                    .whitespace_nowrap()
                    .cursor_pointer()
                    .when(is_selected, |d| {
                        d.bg(theme.accent).text_color(theme.accent_foreground)
                    })
                    .when(!is_selected, |d| {
                        d.text_color(theme.foreground)
                            .hover(|d| d.bg(theme.list_active))
                    })
                    .when_some(sidebar_for_click, |d, sidebar| {
                        d.on_click(move |_, _, cx| {
                            if is_parent_menu {
                                sidebar
                                    .update(cx, |s, cx| s.context_menu_parent_execute_at(idx, cx));
                            } else {
                                sidebar.update(cx, |s, cx| s.context_menu_execute_at(idx, cx));
                            }
                        })
                    })
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .when_some(icon, |d, icon| {
                                d.child(svg().path(icon.path()).size_4().text_color(icon_color))
                            })
                            .child(item.label.clone()),
                    )
                    .when(is_submenu, |d| {
                        d.child(
                            svg()
                                .path(AppIcon::ChevronRight.path())
                                .size_4()
                                .text_color(theme.muted_foreground),
                        )
                    })
            }))
    }

    pub(super) fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let app_state = self.app_state.clone();

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
