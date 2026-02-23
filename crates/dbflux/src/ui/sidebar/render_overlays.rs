use super::*;

impl Sidebar {
    pub(super) fn render_add_menu(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let sidebar_for_close = cx.entity().clone();

        div()
            .absolute()
            .inset_0()
            .child(
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
                    .when(self.active_tab == SidebarTab::Connections, |el| {
                        self.add_connections_menu_items(el, cx)
                    })
                    .when(self.active_tab == SidebarTab::Scripts, |el| {
                        self.add_scripts_menu_items(el, cx)
                    }),
            )
    }

    fn add_connections_menu_items(&self, el: Div, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        let app_state = self.app_state.clone();
        let sidebar_for_folder = cx.entity().clone();
        let sidebar_for_conn = cx.entity().clone();
        let hover_bg = theme.list_active;

        el.child(
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
                            window_bounds: Some(WindowBounds::Windowed(Bounds::centered(
                                None,
                                size(px(600.0), px(550.0)),
                                cx,
                            ))),
                            kind: WindowKind::Floating,
                            ..Default::default()
                        },
                        |window, cx| {
                            let manager =
                                cx.new(|cx| ConnectionManagerWindow::new(app_state, window, cx));
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
        )
    }

    fn add_scripts_menu_items(&self, el: Div, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        let sidebar_for_file = cx.entity().clone();
        let sidebar_for_folder = cx.entity().clone();
        let sidebar_for_import = cx.entity().clone();
        let hover_bg = theme.list_active;
        let hover_bg2 = theme.list_active;
        let hover_bg3 = theme.list_active;

        el.child(
            div()
                .id("add-script-file")
                .px(Spacing::SM)
                .py(Spacing::XS)
                .cursor_pointer()
                .text_size(FontSizes::SM)
                .text_color(theme.foreground)
                .hover(move |d| d.bg(hover_bg))
                .on_click(move |_, _, cx| {
                    sidebar_for_file.update(cx, |this, cx| {
                        this.close_add_menu(cx);
                        this.create_script_file(cx);
                    });
                })
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .child(
                            svg()
                                .path(AppIcon::ScrollText.path())
                                .size_4()
                                .text_color(theme.muted_foreground),
                        )
                        .child("New File"),
                ),
        )
        .child(
            div()
                .id("add-script-folder")
                .px(Spacing::SM)
                .py(Spacing::XS)
                .cursor_pointer()
                .text_size(FontSizes::SM)
                .text_color(theme.foreground)
                .hover(move |d| d.bg(hover_bg2))
                .on_click(move |_, _, cx| {
                    sidebar_for_folder.update(cx, |this, cx| {
                        this.close_add_menu(cx);
                        this.create_script_folder(cx);
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
                .id("import-script")
                .px(Spacing::SM)
                .py(Spacing::XS)
                .cursor_pointer()
                .text_size(FontSizes::SM)
                .text_color(theme.foreground)
                .hover(move |d| d.bg(hover_bg3))
                .on_click(move |_, _, cx| {
                    sidebar_for_import.update(cx, |this, cx| {
                        this.close_add_menu(cx);
                        this.import_script(cx);
                    });
                })
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .child(
                            svg()
                                .path(AppIcon::Download.path())
                                .size_4()
                                .text_color(theme.muted_foreground),
                        )
                        .child("Import File"),
                ),
        )
    }
}
