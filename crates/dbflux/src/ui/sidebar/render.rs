use super::render_tree::{render_tree_item, TreeRenderParams};
use super::*;

impl Sidebar {
    fn render_tab_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let active_tab = self.active_tab;
        let sidebar = cx.entity().clone();
        let sidebar2 = cx.entity().clone();
        let focused = self.connections_focused;

        let tab_text_color = |active: bool| {
            if active {
                if focused {
                    theme.primary
                } else {
                    theme.foreground
                }
            } else {
                theme.muted_foreground
            }
        };

        let tab_border_color = |active: bool| {
            if active {
                theme.primary
            } else {
                gpui::transparent_black()
            }
        };

        let tab_font_weight = |active: bool| {
            if active && focused {
                FontWeight::BOLD
            } else if active {
                FontWeight::SEMIBOLD
            } else {
                FontWeight::MEDIUM
            }
        };

        div()
            .flex()
            .items_center()
            .justify_between()
            .h(Heights::TOOLBAR)
            .border_b_1()
            .border_color(theme.border)
            .child(
                div()
                    .flex()
                    .items_center()
                    .child(
                        div()
                            .id("tab-connections")
                            .px(Spacing::SM)
                            .h_full()
                            .flex()
                            .items_center()
                            .cursor_pointer()
                            .border_b_2()
                            .border_color(tab_border_color(active_tab == SidebarTab::Connections))
                            .text_size(FontSizes::XS)
                            .text_color(tab_text_color(active_tab == SidebarTab::Connections))
                            .font_weight(tab_font_weight(active_tab == SidebarTab::Connections))
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(move |_, _, cx| {
                                sidebar.update(cx, |this, cx| {
                                    this.set_active_tab(SidebarTab::Connections, cx);
                                });
                            })
                            .child("CONNECTIONS"),
                    )
                    .child(
                        div()
                            .id("tab-scripts")
                            .px(Spacing::SM)
                            .h_full()
                            .flex()
                            .items_center()
                            .cursor_pointer()
                            .border_b_2()
                            .border_color(tab_border_color(active_tab == SidebarTab::Scripts))
                            .text_size(FontSizes::XS)
                            .text_color(tab_text_color(active_tab == SidebarTab::Scripts))
                            .font_weight(tab_font_weight(active_tab == SidebarTab::Scripts))
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(move |_, _, cx| {
                                sidebar2.update(cx, |this, cx| {
                                    this.set_active_tab(SidebarTab::Scripts, cx);
                                });
                            })
                            .child("SCRIPTS"),
                    ),
            )
            .child({
                let sidebar_for_toggle = cx.entity().clone();
                let hover_bg = theme.secondary;
                div()
                    .id("add-button")
                    .w(Heights::ICON_LG)
                    .h(Heights::ICON_LG)
                    .flex()
                    .items_center()
                    .justify_center()
                    .rounded(Radii::SM)
                    .text_size(FontSizes::LG)
                    .text_color(theme.muted_foreground)
                    .cursor_pointer()
                    .hover(move |d| d.bg(hover_bg).text_color(theme.foreground))
                    .on_click(move |_, _, cx| {
                        sidebar_for_toggle.update(cx, |this, cx| {
                            this.toggle_add_menu(cx);
                        });
                    })
                    .child("+")
            })
    }

    fn render_action_bars(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .flex_shrink_0()
            .when(self.pending_delete_item.is_some(), |el| {
                el.child(
                    div()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .border_b_1()
                        .border_color(theme.border)
                        .bg(gpui::rgb(0x5C1F1F))
                        .flex()
                        .items_center()
                        .justify_center()
                        .text_size(FontSizes::XS)
                        .text_color(theme.foreground)
                        .child("Press x to confirm delete, ESC to cancel"),
                )
            })
    }

    fn render_connections_content(
        &self,
        tree_params: TreeRenderParams,
        sidebar_entity: &Entity<Self>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let has_entries = self.visible_entry_count > 0;
        let theme = cx.theme();
        let sidebar_for_root_drop = sidebar_entity.clone();
        let sidebar_for_clear_drop = sidebar_entity.clone();

        div()
            .flex_1()
            .flex()
            .flex_col()
            .overflow_hidden()
            .when(has_entries, |el| {
                el.child(
                    div()
                        .flex_1()
                        .overflow_hidden()
                        .on_drop(move |state: &SidebarDragState, _, cx| {
                            sidebar_for_root_drop.update(cx, |this, cx| {
                                this.stop_auto_scroll(cx);
                                this.clear_drop_target(cx);
                                this.clear_drag_hover_folder(cx);
                                this.handle_drop(state, None, cx);
                            });
                        })
                        .on_drag_move::<SidebarDragState>(move |_, _, cx| {
                            sidebar_for_clear_drop.update(cx, |this, cx| {
                                this.stop_auto_scroll(cx);
                                this.clear_drop_target(cx);
                                this.clear_drag_hover_folder(cx);
                            });
                        })
                        .child(tree(
                            &self.tree_state,
                            move |ix, entry, selected, _window, cx| {
                                render_tree_item(&tree_params, ix, entry, selected, cx)
                            },
                        )),
                )
            })
            .when(!has_entries, |el| {
                el.child(
                    div()
                        .flex_1()
                        .flex()
                        .flex_col()
                        .items_center()
                        .justify_center()
                        .gap(Spacing::SM)
                        .px(Spacing::MD)
                        .child(
                            div()
                                .text_size(FontSizes::SM)
                                .text_color(theme.muted_foreground)
                                .text_center()
                                .child("No connections yet"),
                        )
                        .child(
                            div()
                                .text_size(FontSizes::XS)
                                .text_color(theme.muted_foreground)
                                .text_center()
                                .child("Use + to add a new connection"),
                        ),
                )
            })
    }

    fn render_scripts_content(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let search_input = self.scripts_search_input.clone();
        let sidebar_entity = cx.entity().clone();
        let sidebar_for_root_drop = sidebar_entity.clone();
        let sidebar_for_clear_drop = sidebar_entity.clone();

        let has_entries = self
            .app_state
            .read(cx)
            .scripts_directory()
            .map(|d| !d.is_empty())
            .unwrap_or(false);

        let has_search = !self.scripts_search_query.is_empty();

        let tree_params = TreeRenderParams {
            connections: Vec::new(),
            active_id: None,
            profile_icons: HashMap::new(),
            active_databases: HashMap::new(),
            sidebar_entity: sidebar_entity.clone(),
            multi_selection: HashSet::new(),
            pending_delete: self.pending_delete_item.clone(),
            drop_target: None,
            scripts_drop_target: self.scripts_drop_target.clone(),
            editing_id: None,
            editing_script_path: self.editing_script_path.clone(),
            rename_input: self.rename_input.clone(),
            color_teal: gpui::rgb(0x4EC9B0).into(),
            color_yellow: gpui::rgb(0xDCDCAA).into(),
            color_blue: gpui::rgb(0x9CDCFE).into(),
            color_purple: gpui::rgb(0xC586C0).into(),
            color_gray: gpui::rgb(0x808080).into(),
            color_orange: gpui::rgb(0xCE9178).into(),
            color_schema: gpui::rgb(0x569CD6).into(),
            color_green: gpui::green(),
        };

        div()
            .flex_1()
            .flex()
            .flex_col()
            .overflow_hidden()
            // Search bar
            .child(
                div()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        Input::new(&search_input)
                            .xsmall()
                            .appearance(false)
                            .cleanable(true),
                    ),
            )
            // Tree or empty state
            .when(has_entries || has_search, |el| {
                el.child(
                    div()
                        .flex_1()
                        .overflow_hidden()
                        .on_drop(move |state: &ScriptsDragState, _, cx| {
                            sidebar_for_root_drop.update(cx, |this, cx| {
                                this.scripts_drop_target = None;
                                this.handle_script_drop_to_root(state, cx);
                            });
                        })
                        .on_drag_move::<ScriptsDragState>(move |_, _, cx| {
                            sidebar_for_clear_drop.update(cx, |this, cx| {
                                this.scripts_drop_target = None;
                                cx.notify();
                            });
                        })
                        .child(tree(
                            &self.scripts_tree_state,
                            move |ix, entry, selected, _window, cx| {
                                render_tree_item(&tree_params, ix, entry, selected, cx)
                            },
                        )),
                )
            })
            .when(!has_entries && !has_search, |el| {
                el.child(
                    div()
                        .flex_1()
                        .flex()
                        .flex_col()
                        .items_center()
                        .justify_center()
                        .gap(Spacing::SM)
                        .px(Spacing::MD)
                        .child(
                            div()
                                .text_size(FontSizes::SM)
                                .text_color(theme.muted_foreground)
                                .text_center()
                                .child("No scripts yet"),
                        )
                        .child(
                            div()
                                .text_size(FontSizes::XS)
                                .text_color(theme.muted_foreground)
                                .text_center()
                                .child("Use + to create a new script or import an existing file"),
                        ),
                )
            })
    }
}

impl Render for Sidebar {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        crate::ui::toast::flush_pending_toast(self.pending_toast.take(), window, cx);

        if let Some(item_id) = self.pending_rename_item.take() {
            self.start_rename(&item_id, window, cx);
        }

        let theme = cx.theme();
        let state = self.app_state.read(cx);
        let active_id = state.active_connection_id();
        let connections = state.connections().keys().copied().collect::<Vec<_>>();

        let profile_icons: HashMap<Uuid, dbflux_core::Icon> = state
            .profiles()
            .iter()
            .filter_map(|p| {
                state
                    .drivers()
                    .get(&p.kind())
                    .map(|driver| (p.id, driver.metadata().icon))
            })
            .collect();

        let active_databases = self.active_databases.clone();
        let sidebar_entity = cx.entity().clone();
        let multi_selection = self.multi_selection.clone();
        let pending_delete = self.pending_delete_item.clone();

        let tree_params = TreeRenderParams {
            connections,
            active_id,
            profile_icons,
            active_databases,
            sidebar_entity: sidebar_entity.clone(),
            multi_selection,
            pending_delete,
            drop_target: self.drop_target.clone(),
            scripts_drop_target: None,
            editing_id: self.editing_id,
            editing_script_path: None,
            rename_input: self.rename_input.clone(),
            color_teal: gpui::rgb(0x4EC9B0).into(),
            color_yellow: gpui::rgb(0xDCDCAA).into(),
            color_blue: gpui::rgb(0x9CDCFE).into(),
            color_purple: gpui::rgb(0xC586C0).into(),
            color_gray: gpui::rgb(0x808080).into(),
            color_orange: gpui::rgb(0xCE9178).into(),
            color_schema: gpui::rgb(0x569CD6).into(),
            color_green: gpui::green(),
        };

        let active_tab = self.active_tab;

        div()
            .relative()
            .flex()
            .flex_col()
            .size_full()
            .border_r_1()
            .border_color(theme.border)
            .bg(theme.sidebar)
            .child(self.render_tab_bar(cx))
            .child(self.render_action_bars(cx))
            .when(active_tab == SidebarTab::Connections, |el| {
                el.child(self.render_connections_content(tree_params, &sidebar_entity, cx))
            })
            .when(active_tab == SidebarTab::Scripts, |el| {
                el.child(self.render_scripts_content(cx))
            })
            .child(self.render_footer(cx))
            .when(self.add_menu_open, |el| el.child(self.render_add_menu(cx)))
    }
}
