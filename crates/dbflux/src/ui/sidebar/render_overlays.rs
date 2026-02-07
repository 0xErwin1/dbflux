use super::*;

impl Sidebar {
    pub(super) fn render_add_menu(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let app_state = self.app_state.clone();
        let sidebar_for_folder = cx.entity().clone();
        let sidebar_for_conn = cx.entity().clone();
        let sidebar_for_close = cx.entity().clone();
        let hover_bg = theme.list_active;

        div()
            .child(
                // Overlay to close on click outside
                div()
                    .id("add-menu-overlay")
                    .absolute()
                    .inset_0()
                    .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                        sidebar_for_close.update(cx, |this, cx| {
                            this.close_add_menu(cx);
                        });
                    }),
            )
            .child(
                // Menu dropdown positioned below the + button
                div()
                    .absolute()
                    .top(Heights::TOOLBAR)
                    .right(Spacing::XS)
                    .bg(theme.sidebar)
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::SM)
                    .py(Spacing::XS)
                    .min_w(px(140.0))
                    .shadow_md()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    .child(
                        div()
                            .id("add-folder-option")
                            .px(Spacing::SM)
                            .py(Spacing::XS)
                            .cursor_pointer()
                            .text_size(FontSizes::SM)
                            .text_color(theme.foreground)
                            .hover(move |d| d.bg(hover_bg))
                            .on_click(move |_, _, cx| {
                                sidebar_for_folder.update(cx, |this, cx| {
                                    this.close_add_menu(cx);
                                    this.create_root_folder(cx);
                                });
                            })
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::SM)
                                    .child(
                                        svg()
                                            .path(AppIcon::Folder.path())
                                            .size_4()
                                            .text_color(theme.muted_foreground),
                                    )
                                    .child("New Folder"),
                            ),
                    )
                    .child(
                        div()
                            .id("add-connection-option")
                            .px(Spacing::SM)
                            .py(Spacing::XS)
                            .cursor_pointer()
                            .text_size(FontSizes::SM)
                            .text_color(theme.foreground)
                            .hover(move |d| d.bg(theme.list_active))
                            .on_click(move |_, _, cx| {
                                sidebar_for_conn.update(cx, |this, cx| {
                                    this.close_add_menu(cx);
                                });
                                let app_state = app_state.clone();
                                cx.open_window(
                                    WindowOptions {
                                        app_id: Some("dbflux".into()),
                                        titlebar: Some(TitlebarOptions {
                                            title: Some("Connection Manager".into()),
                                            ..Default::default()
                                        }),
                                        window_bounds: Some(WindowBounds::Windowed(
                                            Bounds::centered(None, size(px(600.0), px(550.0)), cx),
                                        )),
                                        kind: WindowKind::Floating,
                                        ..Default::default()
                                    },
                                    |window, cx| {
                                        let manager = cx.new(|cx| {
                                            ConnectionManagerWindow::new(app_state, window, cx)
                                        });
                                        cx.new(|cx| Root::new(manager, window, cx))
                                    },
                                )
                                .ok();
                            })
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::SM)
                                    .child(
                                        svg()
                                            .path(AppIcon::Plug.path())
                                            .size_4()
                                            .text_color(theme.muted_foreground),
                                    )
                                    .child("New Connection"),
                            ),
                    ),
            )
    }
}
