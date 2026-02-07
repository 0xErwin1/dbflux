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

    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
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

        // Pre-compute profile_id -> Icon map for use in the tree closure
        // (closure requires 'static, so we can't borrow state inside it)
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

        let color_teal: Hsla = gpui::rgb(0x4EC9B0).into();
        let color_yellow: Hsla = gpui::rgb(0xDCDCAA).into();
        let color_blue: Hsla = gpui::rgb(0x9CDCFE).into();
        let color_purple: Hsla = gpui::rgb(0xC586C0).into();
        let color_gray: Hsla = gpui::rgb(0x808080).into();
        let color_orange: Hsla = gpui::rgb(0xCE9178).into();
        let color_schema: Hsla = gpui::rgb(0x569CD6).into();
        let color_green: Hsla = gpui::green();

        div()
            .relative()
            .flex()
            .flex_col()
            .size_full()
            .border_r_1()
            .border_color(theme.border)
            .bg(theme.sidebar)
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .px(Spacing::SM)
                    .h(Heights::TOOLBAR)
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .font_weight(if self.connections_focused {
                                FontWeight::BOLD
                            } else {
                                FontWeight::SEMIBOLD
                            })
                            .text_color(if self.connections_focused {
                                theme.primary
                            } else {
                                theme.muted_foreground
                            })
                            .child("CONNECTIONS"),
                    )
                    .child({
                        let sidebar_for_toggle = sidebar_entity.clone();
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
                    }),
            )
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
            .when(self.editing_id.is_some(), |el| {
                let rename_input = self.rename_input.clone();
                let sidebar_confirm = sidebar_entity.clone();
                let sidebar_cancel = sidebar_entity.clone();

                el.child(
                    div()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .border_b_1()
                        .border_color(theme.border)
                        .bg(theme.sidebar)
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .child(
                            div().flex_1().child(
                                Input::new(&rename_input)
                                    .xsmall()
                                    .appearance(false)
                                    .cleanable(false),
                            ),
                        )
                        .child(
                            div()
                                .id("rename-confirm")
                                .px(Spacing::XS)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(color_green)
                                .hover(|d| d.bg(theme.secondary))
                                .on_click(move |_, _, cx| {
                                    sidebar_confirm.update(cx, |this, cx| {
                                        this.commit_rename(cx);
                                    });
                                })
                                .child("\u{2713}"),
                        )
                        .child(
                            div()
                                .id("rename-cancel")
                                .px(Spacing::XS)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(theme.muted_foreground)
                                .hover(|d| d.bg(theme.secondary))
                                .on_click(move |_, _, cx| {
                                    sidebar_cancel.update(cx, |this, cx| {
                                        this.cancel_rename(cx);
                                    });
                                })
                                .child("\u{2715}"),
                        ),
                )
            })
            .child({
                let sidebar_for_root_drop = sidebar_entity.clone();
                let sidebar_for_clear_drop = sidebar_entity.clone();
                let current_drop_target = self.drop_target.clone();
                let drop_indicator_color = theme.accent;

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
                            let item = entry.item();
                            let item_id = item.id.clone();
                            let depth = entry.depth();

                            let node_kind = parse_node_kind(&item_id);
                            let parsed_id = parse_node_id(&item_id);

                            let is_connected = matches!(
                                &parsed_id,
                                Some(SchemaNodeId::Profile { profile_id })
                                    if connections.contains(profile_id)
                            );

                            let is_active = matches!(
                                &parsed_id,
                                Some(SchemaNodeId::Profile { profile_id })
                                    if active_id == Some(*profile_id)
                            );

                            // Check if this database is the active one for its connection
                            let is_active_database = matches!(
                                &parsed_id,
                                Some(SchemaNodeId::Database { profile_id, name })
                                    if active_databases
                                        .get(profile_id)
                                        .is_some_and(|active_db| active_db == name)
                            );

                            let theme = cx.theme();
                            let indent_per_level = 12.0_f32;
                            let is_folder = entry.is_folder();
                            let is_expanded = entry.is_expanded();

                            let needs_chevron = is_folder
                                && matches!(
                                    node_kind,
                                    SchemaNodeKind::ConnectionFolder
                                        | SchemaNodeKind::Table
                                        | SchemaNodeKind::View
                                        | SchemaNodeKind::Schema
                                        | SchemaNodeKind::TablesFolder
                                        | SchemaNodeKind::ViewsFolder
                                        | SchemaNodeKind::TypesFolder
                                        | SchemaNodeKind::ColumnsFolder
                                        | SchemaNodeKind::IndexesFolder
                                        | SchemaNodeKind::ForeignKeysFolder
                                        | SchemaNodeKind::ConstraintsFolder
                                        | SchemaNodeKind::SchemaIndexesFolder
                                        | SchemaNodeKind::SchemaForeignKeysFolder
                                        | SchemaNodeKind::CustomType
                                        | SchemaNodeKind::Database
                                        | SchemaNodeKind::Profile
                                );
                            let chevron_icon: Option<AppIcon> = if needs_chevron {
                                Some(if is_expanded {
                                    AppIcon::ChevronDown
                                } else {
                                    AppIcon::ChevronRight
                                })
                            } else {
                                None
                            };

                            let (node_icon, unicode_icon, icon_color): (
                                Option<AppIcon>,
                                &str,
                                Hsla,
                            ) = match node_kind {
                                SchemaNodeKind::ConnectionFolder => {
                                    (Some(AppIcon::Folder), "", theme.muted_foreground)
                                }
                                SchemaNodeKind::Profile => {
                                    let icon = parsed_id
                                        .as_ref()
                                        .and_then(|n| n.profile_id())
                                        .and_then(|id| profile_icons.get(&id).copied())
                                        .map(AppIcon::from_icon);

                                    let color = if is_connected {
                                        color_green
                                    } else {
                                        theme.muted_foreground
                                    };
                                    let unicode = if icon.is_none() {
                                        if is_connected { "●" } else { "○" }
                                    } else {
                                        ""
                                    };
                                    (icon, unicode, color)
                                }
                                SchemaNodeKind::Database => {
                                    (Some(AppIcon::Database), "", color_orange)
                                }
                                SchemaNodeKind::Schema => (Some(AppIcon::Layers), "", color_schema),
                                SchemaNodeKind::TablesFolder => {
                                    (Some(AppIcon::Table), "", color_teal)
                                }
                                SchemaNodeKind::ViewsFolder => (Some(AppIcon::Eye), "", color_yellow),
                                SchemaNodeKind::TypesFolder => {
                                    (Some(AppIcon::Braces), "", color_purple)
                                }
                                SchemaNodeKind::Table => (Some(AppIcon::Table), "", color_teal),
                                SchemaNodeKind::View => (Some(AppIcon::Eye), "", color_yellow),
                                SchemaNodeKind::CustomType => {
                                    (Some(AppIcon::Braces), "", color_purple)
                                }
                                SchemaNodeKind::ColumnsFolder => {
                                    (Some(AppIcon::Columns), "", color_blue)
                                }
                                SchemaNodeKind::IndexesFolder | SchemaNodeKind::SchemaIndexesFolder => {
                                    (Some(AppIcon::Hash), "", color_purple)
                                }
                                SchemaNodeKind::ForeignKeysFolder
                                | SchemaNodeKind::SchemaForeignKeysFolder => {
                                    (Some(AppIcon::KeyRound), "", color_orange)
                                }
                                SchemaNodeKind::ConstraintsFolder => {
                                    (Some(AppIcon::Lock), "", color_yellow)
                                }
                                SchemaNodeKind::Column => (Some(AppIcon::Columns), "", color_blue),
                                SchemaNodeKind::Index | SchemaNodeKind::SchemaIndex => {
                                    (Some(AppIcon::Hash), "", color_purple)
                                }
                                SchemaNodeKind::ForeignKey | SchemaNodeKind::SchemaForeignKey => {
                                    (Some(AppIcon::KeyRound), "", color_orange)
                                }
                                SchemaNodeKind::Constraint => (Some(AppIcon::Lock), "", color_yellow),
                                SchemaNodeKind::CollectionsFolder => {
                                    (Some(AppIcon::Folder), "", color_teal)
                                }
                                SchemaNodeKind::Collection => (Some(AppIcon::Box), "", color_teal),
                                _ => (None, "", theme.muted_foreground),
                            };

                            let label_color: Hsla = match node_kind {
                                SchemaNodeKind::ConnectionFolder => theme.foreground,
                                SchemaNodeKind::Profile => theme.foreground,
                                SchemaNodeKind::Database => color_orange,
                                SchemaNodeKind::Schema => color_schema,
                                SchemaNodeKind::TablesFolder
                                | SchemaNodeKind::ViewsFolder
                                | SchemaNodeKind::TypesFolder
                                | SchemaNodeKind::ColumnsFolder
                                | SchemaNodeKind::IndexesFolder
                                | SchemaNodeKind::ForeignKeysFolder
                                | SchemaNodeKind::ConstraintsFolder
                                | SchemaNodeKind::SchemaIndexesFolder
                                | SchemaNodeKind::SchemaForeignKeysFolder => color_gray,
                                SchemaNodeKind::Table => color_teal,
                                SchemaNodeKind::View => color_yellow,
                                SchemaNodeKind::CustomType => color_purple,
                                SchemaNodeKind::Column => color_blue,
                                SchemaNodeKind::Index | SchemaNodeKind::SchemaIndex => color_purple,
                                SchemaNodeKind::ForeignKey | SchemaNodeKind::SchemaForeignKey => {
                                    color_orange
                                }
                                SchemaNodeKind::Constraint => color_yellow,
                                SchemaNodeKind::CollectionsFolder => color_gray,
                                SchemaNodeKind::Collection => color_teal,
                                _ => theme.muted_foreground,
                            };

                            let is_table_or_view = matches!(
                                node_kind,
                                SchemaNodeKind::Table | SchemaNodeKind::View | SchemaNodeKind::Collection
                            );

                            let sidebar_for_mousedown = sidebar_entity.clone();
                            let item_id_for_mousedown = item_id.clone();
                            let sidebar_for_click = sidebar_entity.clone();
                            let item_id_for_click = item_id.clone();
                            let sidebar_for_chevron = sidebar_entity.clone();
                            let item_id_for_chevron = item_id.clone();

                            let guide_lines: Vec<_> = (0..depth)
                                .map(|_| {
                                    div()
                                        .w(px(indent_per_level))
                                        .h_full()
                                        .flex()
                                        .justify_center()
                                        .child(div().w(px(1.0)).h_full().bg(theme.border))
                                })
                                .collect();

                            let is_multi_selected = multi_selection.contains(item_id.as_ref());
                            let multi_select_bg = theme.list_active;

                            let is_pending_delete = pending_delete
                                .as_ref()
                                .is_some_and(|id| id == item_id.as_ref());
                            let pending_delete_bg: Hsla = gpui::rgb(0x5C1F1F).into();

                            let mut list_item = ListItem::new(ix)
                                .selected(selected)
                                .py(Spacing::XS)
                                .when(is_pending_delete, |el| el.bg(pending_delete_bg))
                                .when(is_multi_selected && !selected && !is_pending_delete, |el| {
                                    el.bg(multi_select_bg)
                                })
                                .child(
                                    div()
                                        .id(SharedString::from(format!("row-{}", item_id)))
                                        .w_full()
                                        .flex()
                                        .items_center()
                                        .gap(Spacing::XS)
                                        .children(guide_lines)
                                        .when(is_table_or_view, |el| {
                                            let sidebar_md = sidebar_for_mousedown.clone();
                                            let id_md = item_id_for_mousedown.clone();
                                            let sidebar_cl = sidebar_for_click.clone();
                                            let id_cl = item_id_for_click.clone();
                                            let is_collection =
                                                node_kind == SchemaNodeKind::Collection;
                                            el.on_mouse_down(MouseButton::Left, move |_, _, cx| {
                                                cx.stop_propagation();
                                                sidebar_md.update(cx, |this, cx| {
                                                    if let Some(idx) =
                                                        this.find_item_index(&id_md, cx)
                                                    {
                                                        this.tree_state.update(cx, |state, cx| {
                                                            state.set_selected_index(Some(idx), cx);
                                                        });
                                                    }
                                                    cx.emit(SidebarEvent::RequestFocus);
                                                    cx.notify();
                                                });
                                            })
                                            .on_click(
                                                move |event, _window, cx| {
                                                    if event.click_count() == 2 {
                                                        sidebar_cl.update(cx, |this, cx| {
                                                            if is_collection {
                                                                this.browse_collection(&id_cl, cx);
                                                            } else {
                                                                this.browse_table(&id_cl, cx);
                                                            }
                                                        });
                                                    }
                                                },
                                            )
                                        })
                                        // Intercept mouse_down for non-table folder items
                                        // to prevent TreeState from independently toggling
                                        // expansion. The sidebar owns expansion state via
                                        // expansion_overrides.
                                        .when(!is_table_or_view && is_folder, |el| {
                                            el.on_mouse_down(MouseButton::Left, |_, _, cx| {
                                                cx.stop_propagation();
                                            })
                                        })
                                        .child(
                                            div()
                                                .id(SharedString::from(format!(
                                                    "chevron-{}",
                                                    item_id
                                                )))
                                                .w(px(12.0))
                                                .flex()
                                                .justify_center()
                                                .when_some(chevron_icon, |el, icon| {
                                                    el.cursor_pointer()
                                                        .on_mouse_down(
                                                            MouseButton::Left,
                                                            |_, _, cx| {
                                                                cx.stop_propagation();
                                                            },
                                                        )
                                                        .on_click(move |_, _, cx| {
                                                            cx.stop_propagation();
                                                            sidebar_for_chevron.update(
                                                                cx,
                                                                |this, cx| {
                                                                    this.toggle_item_expansion(
                                                                        &item_id_for_chevron,
                                                                        cx,
                                                                    );
                                                                },
                                                            );
                                                        })
                                                        .child(
                                                            svg()
                                                                .path(icon.path())
                                                                .size_3()
                                                                .text_color(theme.muted_foreground),
                                                        )
                                                }),
                                        )
                                        .child(
                                            div()
                                                .w(Heights::ICON_SM)
                                                .flex()
                                                .justify_center()
                                                .when_some(node_icon, |el, icon| {
                                                    el.child(
                                                        svg()
                                                            .path(icon.path())
                                                            .size_3p5()
                                                            .text_color(icon_color),
                                                    )
                                                })
                                                .when(
                                                    node_icon.is_none() && !unicode_icon.is_empty(),
                                                    |el| {
                                                        el.text_size(FontSizes::SM)
                                                            .text_color(icon_color)
                                                            .child(unicode_icon)
                                                    },
                                                ),
                                        )
                                        .child(
                                            div()
                                                .flex_1()
                                                .overflow_hidden()
                                                .text_ellipsis()
                                                .text_size(FontSizes::SM)
                                                .text_color(label_color)
                                                .when(
                                                    node_kind == SchemaNodeKind::Profile && is_active,
                                                    |d| d.font_weight(FontWeight::SEMIBOLD),
                                                )
                                                .when(is_active_database, |d| {
                                                    d.font_weight(FontWeight::SEMIBOLD)
                                                })
                                                .when(
                                                    matches!(
                                                        node_kind,
                                                        SchemaNodeKind::TablesFolder
                                                            | SchemaNodeKind::ViewsFolder
                                                            | SchemaNodeKind::TypesFolder
                                                            | SchemaNodeKind::ColumnsFolder
                                                            | SchemaNodeKind::IndexesFolder
                                                            | SchemaNodeKind::ForeignKeysFolder
                                                            | SchemaNodeKind::ConstraintsFolder
                                                    ),
                                                    |d| d.font_weight(FontWeight::MEDIUM),
                                                )
                                                .child(item.label.clone()),
                                        )
                                        .when(
                                            matches!(
                                                node_kind,
                                                SchemaNodeKind::Profile
                                                    | SchemaNodeKind::ConnectionFolder
                                            ),
                                            |el| {
                                                let drag_node_id = match &parsed_id {
                                                    Some(SchemaNodeId::Profile { profile_id }) => {
                                                        Some(*profile_id)
                                                    }
                                                    Some(SchemaNodeId::ConnectionFolder {
                                                        node_id,
                                                    }) => Some(*node_id),
                                                    _ => None,
                                                };

                                                if let Some(node_id) = drag_node_id {
                                                    let drag_label = item.label.to_string();
                                                    let is_folder =
                                                        node_kind == SchemaNodeKind::ConnectionFolder;

                                                    // Collect additional nodes from multi-selection
                                                    let current_item_id = item_id.to_string();
                                                    let additional_nodes: Vec<Uuid> =
                                                        multi_selection
                                                            .iter()
                                                            .filter(|id| *id != &current_item_id)
                                                            .filter_map(|id| {
                                                                match parse_node_id(id) {
                                                                    Some(SchemaNodeId::Profile {
                                                                        profile_id,
                                                                    }) => Some(profile_id),
                                                                    Some(
                                                                        SchemaNodeId::ConnectionFolder {
                                                                            node_id,
                                                                        },
                                                                    ) => Some(node_id),
                                                                    _ => None,
                                                                }
                                                            })
                                                            .collect();

                                                    let total_count = 1 + additional_nodes.len();
                                                    let preview_label = if total_count > 1 {
                                                        format!(
                                                            "{} (+{} more)",
                                                            drag_label,
                                                            total_count - 1
                                                        )
                                                    } else {
                                                        drag_label
                                                    };

                                                    el.on_drag(
                                                        SidebarDragState {
                                                            node_id,
                                                            additional_nodes,
                                                            is_folder,
                                                            label: preview_label,
                                                        },
                                                        |state, _, _, cx| {
                                                            cx.new(|_| DragPreview {
                                                                label: state.label.clone(),
                                                            })
                                                        },
                                                    )
                                                } else {
                                                    el
                                                }
                                            },
                                        )
                                        // Drop indicator for "After" position
                                        .when(
                                            matches!(
                                                node_kind,
                                                SchemaNodeKind::Profile
                                                    | SchemaNodeKind::ConnectionFolder
                                            ),
                                            |el| {
                                                let is_drop_after = current_drop_target
                                                    .as_ref()
                                                    .map(|t| {
                                                        t.item_id == item_id.as_ref()
                                                            && t.position == DropPosition::After
                                                    })
                                                    .unwrap_or(false);

                                                if is_drop_after {
                                                    el.border_b_2()
                                                        .border_color(drop_indicator_color)
                                                } else {
                                                    el
                                                }
                                            },
                                        )
                                        // Profile drop handling (insert after)
                                        .when(node_kind == SchemaNodeKind::Profile, |el| {
                                            let item_id_for_drop = item_id.to_string();
                                            let item_id_for_move = item_id.to_string();
                                            let sidebar_for_drop = sidebar_entity.clone();
                                            let sidebar_for_move = sidebar_entity.clone();
                                            let item_ix = ix;

                                            el.drag_over::<SidebarDragState>(
                                                move |style, state, _, cx| {
                                                    // Parse profile_id from item_id
                                                    let profile_id = match parse_node_id(&item_id_for_move) {
                                                        Some(SchemaNodeId::Profile { profile_id }) => Some(profile_id),
                                                        _ => None,
                                                    };
                                                    // Don't allow dropping on self
                                                    if profile_id.is_some_and(|pid| state.node_id != pid) {
                                                        sidebar_for_move.update(cx, |this, cx| {
                                                            // Clear folder hover (moved away from folder)
                                                            this.clear_drag_hover_folder(cx);
                                                            this.set_drop_target(
                                                                item_id_for_move.clone(),
                                                                DropPosition::After,
                                                                cx,
                                                            );
                                                            // Check for auto-scroll
                                                            this.check_auto_scroll(item_ix, cx);
                                                        });
                                                    }
                                                    style
                                                },
                                            )
                                            .on_drop(
                                                move |state: &SidebarDragState, _, cx| {
                                                    sidebar_for_drop.update(cx, |this, cx| {
                                                        this.stop_auto_scroll(cx);
                                                        this.clear_drag_hover_folder(cx);
                                                        this.set_drop_target(
                                                            item_id_for_drop.clone(),
                                                            DropPosition::After,
                                                            cx,
                                                        );
                                                        this.handle_drop_with_position(state, cx);
                                                    });
                                                },
                                            )
                                        })
                                        // Folder drop handling (insert into)
                                        .when(node_kind == SchemaNodeKind::ConnectionFolder, |el| {
                                            let item_id_for_drop = item_id.to_string();
                                            let item_id_for_move = item_id.to_string();
                                            let sidebar_for_drop = sidebar_entity.clone();
                                            let sidebar_for_move = sidebar_entity.clone();
                                            let drop_target_bg = theme.drop_target;
                                            let item_ix = ix;

                                            if let Some(folder_id) = item_id
                                                .strip_prefix("conn_folder_")
                                                .and_then(|s| Uuid::parse_str(s).ok())
                                            {
                                                el.drag_over::<SidebarDragState>(
                                                    move |style, state, _, cx| {
                                                        if state.node_id != folder_id {
                                                            sidebar_for_move.update(
                                                                cx,
                                                                |this, cx| {
                                                                    this.set_drop_target(
                                                                        item_id_for_move.clone(),
                                                                        DropPosition::Into,
                                                                        cx,
                                                                    );
                                                                    // Start auto-expand timer
                                                                    this.start_drag_hover_folder(
                                                                        folder_id, cx,
                                                                    );
                                                                    // Check for auto-scroll
                                                                    this.check_auto_scroll(
                                                                        item_ix, cx,
                                                                    );
                                                                },
                                                            );
                                                            style.bg(drop_target_bg)
                                                        } else {
                                                            style
                                                        }
                                                    },
                                                )
                                                .on_drop(move |state: &SidebarDragState, _, cx| {
                                                    sidebar_for_drop.update(cx, |this, cx| {
                                                        this.stop_auto_scroll(cx);
                                                        this.clear_drag_hover_folder(cx);
                                                        this.set_drop_target(
                                                            item_id_for_drop.clone(),
                                                            DropPosition::Into,
                                                            cx,
                                                        );
                                                        this.handle_drop_with_position(state, cx);
                                                    });
                                                })
                                            } else {
                                                el
                                            }
                                        })
                                        // Menu button for items that have context menus
                                        .when(
                                            matches!(
                                                node_kind,
                                                SchemaNodeKind::Profile
                                                    | SchemaNodeKind::ConnectionFolder
                                                    | SchemaNodeKind::Table
                                                    | SchemaNodeKind::View
                                                    | SchemaNodeKind::Collection
                                                    | SchemaNodeKind::Database
                                                    | SchemaNodeKind::Index
                                                    | SchemaNodeKind::SchemaIndex
                                                    | SchemaNodeKind::ForeignKey
                                                    | SchemaNodeKind::SchemaForeignKey
                                                    | SchemaNodeKind::CustomType
                                            ),
                                            |el| {
                                                let sidebar_for_menu = sidebar_entity.clone();
                                                let item_id_for_menu = item_id.clone();
                                                let hover_bg = theme.secondary;

                                                el.child(
                                                    div()
                                                        .id(SharedString::from(format!(
                                                            "menu-btn-{}",
                                                            item_id_for_menu
                                                        )))
                                                        .flex_shrink_0()
                                                        .ml_auto()
                                                        .px_1()
                                                        .rounded(Radii::SM)
                                                        .cursor_pointer()
                                                        .hover(move |d| d.bg(hover_bg))
                                                        .on_mouse_down(
                                                            MouseButton::Left,
                                                            |_, _, cx| {
                                                                cx.stop_propagation();
                                                            },
                                                        )
                                                        .on_click({
                                                            let sidebar = sidebar_for_menu.clone();
                                                            let item_id = item_id_for_menu.clone();
                                                            move |event, _, cx| {
                                                                cx.stop_propagation();
                                                                let position = event.position();
                                                                sidebar.update(cx, |this, cx| {
                                                                    cx.emit(
                                                                        SidebarEvent::RequestFocus,
                                                                    );
                                                                    this.open_menu_for_item(
                                                                        &item_id, position, cx,
                                                                    );
                                                                });
                                                            }
                                                        })
                                                        .child("\u{22EF}"),
                                                )
                                            },
                                        )
                                        // Right-click context menu
                                        .when(
                                            matches!(
                                                node_kind,
                                                SchemaNodeKind::Profile
                                                    | SchemaNodeKind::ConnectionFolder
                                                    | SchemaNodeKind::Table
                                                    | SchemaNodeKind::View
                                                    | SchemaNodeKind::Collection
                                                    | SchemaNodeKind::Database
                                                    | SchemaNodeKind::Index
                                                    | SchemaNodeKind::SchemaIndex
                                                    | SchemaNodeKind::ForeignKey
                                                    | SchemaNodeKind::SchemaForeignKey
                                                    | SchemaNodeKind::CustomType
                                            ),
                                            |el| {
                                                let sidebar_for_ctx = sidebar_entity.clone();
                                                let item_id_for_ctx = item_id.clone();

                                                el.on_mouse_down(
                                                    MouseButton::Right,
                                                    move |event, _, cx| {
                                                        cx.stop_propagation();
                                                        let position = event.position;
                                                        sidebar_for_ctx.update(cx, |this, cx| {
                                                            cx.emit(SidebarEvent::RequestFocus);
                                                            this.open_menu_for_item(
                                                                &item_id_for_ctx,
                                                                position,
                                                                cx,
                                                            );
                                                        });
                                                    },
                                                )
                                            },
                                        ),
                                );

                            if node_kind.shows_pointer_cursor() {
                                list_item = list_item.cursor(CursorStyle::PointingHand);
                            }

                            if !is_table_or_view && node_kind.needs_click_handler() {
                                let item_id_for_click = item_id.clone();
                                let sidebar = sidebar_entity.clone();

                                list_item = list_item.on_click(move |event, _window, cx| {
                                    cx.stop_propagation();
                                    let click_count = event.click_count();
                                    let with_ctrl =
                                        event.modifiers().platform || event.modifiers().control;
                                    sidebar.update(cx, |this, cx| {
                                        this.handle_item_click(
                                            &item_id_for_click,
                                            click_count,
                                            with_ctrl,
                                            cx,
                                        );
                                    });
                                });
                            }

                            list_item
                        },
                    ))
            })
            .child(self.render_footer(cx))
            // Add menu dropdown
            .when(self.add_menu_open, |el| {
                let theme = cx.theme();
                let app_state = self.app_state.clone();
                let sidebar_for_folder = cx.entity().clone();
                let sidebar_for_conn = cx.entity().clone();
                let sidebar_for_close = cx.entity().clone();
                let hover_bg = theme.list_active;

                el.child(
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
                                                Bounds::centered(
                                                    None,
                                                    size(px(600.0), px(550.0)),
                                                    cx,
                                                ),
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
            })
    }
}
