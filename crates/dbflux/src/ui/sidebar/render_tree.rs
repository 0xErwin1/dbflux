use super::*;

pub(super) struct TreeRenderParams {
    pub connections: Vec<Uuid>,
    pub active_id: Option<Uuid>,
    pub profile_icons: HashMap<Uuid, dbflux_core::Icon>,
    pub active_databases: HashMap<Uuid, String>,
    pub sidebar_entity: Entity<Sidebar>,
    pub multi_selection: HashSet<String>,
    pub pending_delete: Option<String>,
    pub drop_target: Option<DropTarget>,
    pub color_teal: Hsla,
    pub color_yellow: Hsla,
    pub color_blue: Hsla,
    pub color_purple: Hsla,
    pub color_gray: Hsla,
    pub color_orange: Hsla,
    pub color_schema: Hsla,
    pub color_green: Hsla,
}

pub(super) fn render_tree_item(
    params: &TreeRenderParams,
    ix: usize,
    entry: &gpui_component::tree::TreeEntry,
    selected: bool,
    cx: &App,
) -> ListItem {
    let item = entry.item();
    let item_id = item.id.clone();
    let depth = entry.depth();

    let node_kind = parse_node_kind(&item_id);
    let parsed_id = parse_node_id(&item_id);

    let is_connected = matches!(
        &parsed_id,
        Some(SchemaNodeId::Profile { profile_id })
            if params.connections.contains(profile_id)
    );

    let is_active = matches!(
        &parsed_id,
        Some(SchemaNodeId::Profile { profile_id })
            if params.active_id == Some(*profile_id)
    );

    // Check if this database is the active one for its connection
    let is_active_database = matches!(
        &parsed_id,
        Some(SchemaNodeId::Database { profile_id, name })
            if params.active_databases
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

    let (node_icon, unicode_icon, icon_color) = resolve_node_icon(
        node_kind,
        &parsed_id,
        &params.profile_icons,
        is_connected,
        theme,
        params,
    );

    let label_color = resolve_label_color(node_kind, theme, params);

    let is_table_or_view = matches!(
        node_kind,
        SchemaNodeKind::Table | SchemaNodeKind::View | SchemaNodeKind::Collection
    );

    let sidebar_entity = &params.sidebar_entity;
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

    let is_multi_selected = params.multi_selection.contains(item_id.as_ref());
    let multi_select_bg = theme.list_active;

    let is_pending_delete = params
        .pending_delete
        .as_ref()
        .is_some_and(|id| id == item_id.as_ref());
    let pending_delete_bg: Hsla = gpui::rgb(0x5C1F1F).into();

    let current_drop_target = params.drop_target.as_ref();
    let drop_indicator_color = theme.accent;

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
                    let is_collection = node_kind == SchemaNodeKind::Collection;
                    el.on_mouse_down(MouseButton::Left, move |_, _, cx| {
                        cx.stop_propagation();
                        sidebar_md.update(cx, |this, cx| {
                            if let Some(idx) = this.find_item_index(&id_md, cx) {
                                this.tree_state.update(cx, |state, cx| {
                                    state.set_selected_index(Some(idx), cx);
                                });
                            }
                            cx.emit(SidebarEvent::RequestFocus);
                            cx.notify();
                        });
                    })
                    .on_click(move |event, _window, cx| {
                        if event.click_count() == 2 {
                            sidebar_cl.update(cx, |this, cx| {
                                if is_collection {
                                    this.browse_collection(&id_cl, cx);
                                } else {
                                    this.browse_table(&id_cl, cx);
                                }
                            });
                        }
                    })
                })
                // Intercept mouse_down for non-table folder items
                .when(!is_table_or_view && is_folder, |el| {
                    el.on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                })
                .child(
                    div()
                        .id(SharedString::from(format!("chevron-{}", item_id)))
                        .w(px(12.0))
                        .flex()
                        .justify_center()
                        .when_some(chevron_icon, |el, icon| {
                            el.cursor_pointer()
                                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                    cx.stop_propagation();
                                })
                                .on_click(move |_, _, cx| {
                                    cx.stop_propagation();
                                    sidebar_for_chevron.update(cx, |this, cx| {
                                        this.toggle_item_expansion(&item_id_for_chevron, cx);
                                    });
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
                            el.child(svg().path(icon.path()).size_3p5().text_color(icon_color))
                        })
                        .when(node_icon.is_none() && !unicode_icon.is_empty(), |el| {
                            el.text_size(FontSizes::SM)
                                .text_color(icon_color)
                                .child(unicode_icon)
                        }),
                )
                .child(
                    div()
                        .flex_1()
                        .overflow_hidden()
                        .text_ellipsis()
                        .text_size(FontSizes::SM)
                        .text_color(label_color)
                        .when(node_kind == SchemaNodeKind::Profile && is_active, |d| {
                            d.font_weight(FontWeight::SEMIBOLD)
                        })
                        .when(is_active_database, |d| d.font_weight(FontWeight::SEMIBOLD))
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
                        SchemaNodeKind::Profile | SchemaNodeKind::ConnectionFolder
                    ),
                    |el| {
                        let drag_node_id = match &parsed_id {
                            Some(SchemaNodeId::Profile { profile_id }) => Some(*profile_id),
                            Some(SchemaNodeId::ConnectionFolder { node_id }) => Some(*node_id),
                            _ => None,
                        };

                        if let Some(node_id) = drag_node_id {
                            let drag_label = item.label.to_string();
                            let is_folder = node_kind == SchemaNodeKind::ConnectionFolder;

                            // Collect additional nodes from multi-selection
                            let current_item_id = item_id.to_string();
                            let additional_nodes: Vec<Uuid> = params
                                .multi_selection
                                .iter()
                                .filter(|id| *id != &current_item_id)
                                .filter_map(|id| match parse_node_id(id) {
                                    Some(SchemaNodeId::Profile { profile_id }) => Some(profile_id),
                                    Some(SchemaNodeId::ConnectionFolder { node_id }) => {
                                        Some(node_id)
                                    }
                                    _ => None,
                                })
                                .collect();

                            let total_count = 1 + additional_nodes.len();
                            let preview_label = if total_count > 1 {
                                format!("{} (+{} more)", drag_label, total_count - 1)
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
                        SchemaNodeKind::Profile | SchemaNodeKind::ConnectionFolder
                    ),
                    |el| {
                        let is_drop_after = current_drop_target
                            .as_ref()
                            .map(|t| {
                                t.item_id == item_id.as_ref() && t.position == DropPosition::After
                            })
                            .unwrap_or(false);

                        if is_drop_after {
                            el.border_b_2().border_color(drop_indicator_color)
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

                    el.drag_over::<SidebarDragState>(move |style, state, _, cx| {
                        let profile_id = match parse_node_id(&item_id_for_move) {
                            Some(SchemaNodeId::Profile { profile_id }) => Some(profile_id),
                            _ => None,
                        };
                        if profile_id.is_some_and(|pid| state.node_id != pid) {
                            sidebar_for_move.update(cx, |this, cx| {
                                this.clear_drag_hover_folder(cx);
                                this.set_drop_target(
                                    item_id_for_move.clone(),
                                    DropPosition::After,
                                    cx,
                                );
                                this.check_auto_scroll(item_ix, cx);
                            });
                        }
                        style
                    })
                    .on_drop(move |state: &SidebarDragState, _, cx| {
                        sidebar_for_drop.update(cx, |this, cx| {
                            this.stop_auto_scroll(cx);
                            this.clear_drag_hover_folder(cx);
                            this.set_drop_target(item_id_for_drop.clone(), DropPosition::After, cx);
                            this.handle_drop_with_position(state, cx);
                        });
                    })
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
                        el.drag_over::<SidebarDragState>(move |style, state, _, cx| {
                            if state.node_id != folder_id {
                                sidebar_for_move.update(cx, |this, cx| {
                                    this.set_drop_target(
                                        item_id_for_move.clone(),
                                        DropPosition::Into,
                                        cx,
                                    );
                                    this.start_drag_hover_folder(folder_id, cx);
                                    this.check_auto_scroll(item_ix, cx);
                                });
                                style.bg(drop_target_bg)
                            } else {
                                style
                            }
                        })
                        .on_drop(
                            move |state: &SidebarDragState, _, cx| {
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
                            },
                        )
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
                                .id(SharedString::from(format!("menu-btn-{}", item_id_for_menu)))
                                .flex_shrink_0()
                                .ml_auto()
                                .px_1()
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .hover(move |d| d.bg(hover_bg))
                                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                    cx.stop_propagation();
                                })
                                .on_click({
                                    let sidebar = sidebar_for_menu.clone();
                                    let item_id = item_id_for_menu.clone();
                                    move |event, _, cx| {
                                        cx.stop_propagation();
                                        let position = event.position();
                                        sidebar.update(cx, |this, cx| {
                                            cx.emit(SidebarEvent::RequestFocus);
                                            this.open_menu_for_item(&item_id, position, cx);
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

                        el.on_mouse_down(MouseButton::Right, move |event, _, cx| {
                            cx.stop_propagation();
                            let position = event.position;
                            sidebar_for_ctx.update(cx, |this, cx| {
                                cx.emit(SidebarEvent::RequestFocus);
                                this.open_menu_for_item(&item_id_for_ctx, position, cx);
                            });
                        })
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
            let with_ctrl = event.modifiers().platform || event.modifiers().control;
            sidebar.update(cx, |this, cx| {
                this.handle_item_click(&item_id_for_click, click_count, with_ctrl, cx);
            });
        });
    }

    list_item
}

fn resolve_node_icon(
    node_kind: SchemaNodeKind,
    parsed_id: &Option<SchemaNodeId>,
    profile_icons: &HashMap<Uuid, dbflux_core::Icon>,
    is_connected: bool,
    theme: &gpui_component::Theme,
    params: &TreeRenderParams,
) -> (Option<AppIcon>, &'static str, Hsla) {
    match node_kind {
        SchemaNodeKind::ConnectionFolder => (Some(AppIcon::Folder), "", theme.muted_foreground),
        SchemaNodeKind::Profile => {
            let icon = parsed_id
                .as_ref()
                .and_then(|n| n.profile_id())
                .and_then(|id| profile_icons.get(&id).copied())
                .map(AppIcon::from_icon);

            let color = if is_connected {
                params.color_green
            } else {
                theme.muted_foreground
            };
            let unicode = if icon.is_none() {
                if is_connected { "\u{25CF}" } else { "\u{25CB}" }
            } else {
                ""
            };
            (icon, unicode, color)
        }
        SchemaNodeKind::Database => (Some(AppIcon::Database), "", params.color_orange),
        SchemaNodeKind::Schema => (Some(AppIcon::Layers), "", params.color_schema),
        SchemaNodeKind::TablesFolder => (Some(AppIcon::Table), "", params.color_teal),
        SchemaNodeKind::ViewsFolder => (Some(AppIcon::Eye), "", params.color_yellow),
        SchemaNodeKind::TypesFolder => (Some(AppIcon::Braces), "", params.color_purple),
        SchemaNodeKind::Table => (Some(AppIcon::Table), "", params.color_teal),
        SchemaNodeKind::View => (Some(AppIcon::Eye), "", params.color_yellow),
        SchemaNodeKind::CustomType => (Some(AppIcon::Braces), "", params.color_purple),
        SchemaNodeKind::ColumnsFolder => (Some(AppIcon::Columns), "", params.color_blue),
        SchemaNodeKind::IndexesFolder | SchemaNodeKind::SchemaIndexesFolder => {
            (Some(AppIcon::Hash), "", params.color_purple)
        }
        SchemaNodeKind::ForeignKeysFolder | SchemaNodeKind::SchemaForeignKeysFolder => {
            (Some(AppIcon::KeyRound), "", params.color_orange)
        }
        SchemaNodeKind::ConstraintsFolder => (Some(AppIcon::Lock), "", params.color_yellow),
        SchemaNodeKind::Column => (Some(AppIcon::Columns), "", params.color_blue),
        SchemaNodeKind::Index | SchemaNodeKind::SchemaIndex => {
            (Some(AppIcon::Hash), "", params.color_purple)
        }
        SchemaNodeKind::ForeignKey | SchemaNodeKind::SchemaForeignKey => {
            (Some(AppIcon::KeyRound), "", params.color_orange)
        }
        SchemaNodeKind::Constraint => (Some(AppIcon::Lock), "", params.color_yellow),
        SchemaNodeKind::CollectionsFolder => (Some(AppIcon::Folder), "", params.color_teal),
        SchemaNodeKind::Collection => (Some(AppIcon::Box), "", params.color_teal),
        _ => (None, "", theme.muted_foreground),
    }
}

fn resolve_label_color(
    node_kind: SchemaNodeKind,
    theme: &gpui_component::Theme,
    params: &TreeRenderParams,
) -> Hsla {
    match node_kind {
        SchemaNodeKind::ConnectionFolder => theme.foreground,
        SchemaNodeKind::Profile => theme.foreground,
        SchemaNodeKind::Database => params.color_orange,
        SchemaNodeKind::Schema => params.color_schema,
        SchemaNodeKind::TablesFolder
        | SchemaNodeKind::ViewsFolder
        | SchemaNodeKind::TypesFolder
        | SchemaNodeKind::ColumnsFolder
        | SchemaNodeKind::IndexesFolder
        | SchemaNodeKind::ForeignKeysFolder
        | SchemaNodeKind::ConstraintsFolder
        | SchemaNodeKind::SchemaIndexesFolder
        | SchemaNodeKind::SchemaForeignKeysFolder => params.color_gray,
        SchemaNodeKind::Table => params.color_teal,
        SchemaNodeKind::View => params.color_yellow,
        SchemaNodeKind::CustomType => params.color_purple,
        SchemaNodeKind::Column => params.color_blue,
        SchemaNodeKind::Index | SchemaNodeKind::SchemaIndex => params.color_purple,
        SchemaNodeKind::ForeignKey | SchemaNodeKind::SchemaForeignKey => params.color_orange,
        SchemaNodeKind::Constraint => params.color_yellow,
        SchemaNodeKind::CollectionsFolder => params.color_gray,
        SchemaNodeKind::Collection => params.color_teal,
        _ => theme.muted_foreground,
    }
}
