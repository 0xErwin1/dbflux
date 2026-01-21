use crate::app::AppState;
use crate::ui::editor::EditorPane;
use crate::ui::results::ResultsPane;
use crate::ui::windows::connection_manager::ConnectionManagerWindow;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::list::ListItem;
use gpui_component::menu::{DropdownMenu, PopupMenuItem};
use gpui_component::tree::{tree, TreeItem, TreeState};
use gpui_component::ActiveTheme;
use gpui_component::Root;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TreeNodeKind {
    Profile,
    Database,
    Schema,
    TablesFolder,
    ViewsFolder,
    Table,
    View,
    ColumnsFolder,
    IndexesFolder,
    Column,
    Index,
    Unknown,
}

impl TreeNodeKind {
    fn from_id(id: &str) -> Self {
        if id.starts_with("profile_") {
            Self::Profile
        } else if id.starts_with("db_") {
            Self::Database
        } else if id.starts_with("schema_") {
            Self::Schema
        } else if id.starts_with("tables_") {
            Self::TablesFolder
        } else if id.starts_with("views_") {
            Self::ViewsFolder
        } else if id.starts_with("table_") {
            Self::Table
        } else if id.starts_with("view_") {
            Self::View
        } else if id.starts_with("columns_") {
            Self::ColumnsFolder
        } else if id.starts_with("indexes_") {
            Self::IndexesFolder
        } else if id.starts_with("col_") {
            Self::Column
        } else if id.starts_with("idx_") {
            Self::Index
        } else {
            Self::Unknown
        }
    }

    fn is_clickable(&self) -> bool {
        matches!(
            self,
            Self::Profile | Self::Database | Self::Table | Self::View
        )
    }
}

pub struct Sidebar {
    app_state: Entity<AppState>,
    #[allow(dead_code)]
    editor: Entity<EditorPane>,
    results: Entity<ResultsPane>,
    tree_state: Entity<TreeState>,
    pending_view_table: Option<String>,
}

impl Sidebar {
    pub fn new(
        app_state: Entity<AppState>,
        editor: Entity<EditorPane>,
        results: Entity<ResultsPane>,
        cx: &mut Context<Self>,
    ) -> Self {
        let items = Self::build_tree_items(&app_state.read(cx));
        let tree_state = cx.new(|cx| TreeState::new(cx).items(items));

        cx.observe(&app_state, |this, _, cx| {
            this.refresh_tree(cx);
        })
        .detach();

        Self {
            app_state,
            editor,
            results,
            tree_state,
            pending_view_table: None,
        }
    }

    fn handle_item_click(&mut self, item_id: &str, click_count: usize, cx: &mut Context<Self>) {
        if item_id.starts_with("profile_") {
            if let Some(profile_id_str) = item_id.strip_prefix("profile_") {
                if let Ok(profile_id) = Uuid::parse_str(profile_id_str) {
                    let is_connected = self
                        .app_state
                        .read(cx)
                        .connections
                        .contains_key(&profile_id);
                    if is_connected {
                        self.app_state.update(cx, |state, cx| {
                            state.set_active_connection(profile_id);
                            cx.notify();
                        });
                    } else {
                        self.connect_to_profile(profile_id, cx);
                    }
                }
            }
        } else if item_id.starts_with("db_") {
            self.handle_database_click(item_id, cx);
        } else if click_count >= 2
            && (item_id.starts_with("table_") || item_id.starts_with("view_"))
        {
            // Format: table_{uuid}_{schema}_{table} or view_{uuid}_{schema}_{view}
            // UUID is 36 chars, so after prefix we have: {uuid}_{schema}_{name}
            let qualified_name = if let Some(rest) = item_id.strip_prefix("table_") {
                Self::parse_qualified_table_name(rest)
            } else if let Some(rest) = item_id.strip_prefix("view_") {
                Self::parse_qualified_table_name(rest)
            } else {
                None
            };

            if let Some(name) = qualified_name {
                self.pending_view_table = Some(name);
                cx.notify();
            }
        }
    }

    fn parse_qualified_table_name(rest: &str) -> Option<String> {
        // rest = "{uuid}_{schema}_{table_name}"
        // UUID is 36 chars
        if rest.len() < 38 {
            return None;
        }

        let after_uuid = &rest[37..]; // skip uuid and underscore
        let (schema, table) = after_uuid.split_once('_')?;
        Some(format!("{}.{}", schema, table))
    }

    fn handle_database_click(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(rest) = item_id.strip_prefix("db_") else {
            return;
        };

        // UUID is 36 chars (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
        // Format: db_{uuid}_{dbname} where dbname may contain underscores
        if rest.len() < 37 {
            return;
        }

        let profile_id_str = &rest[..36];
        let db_name = &rest[37..]; // skip the underscore after UUID

        let Ok(profile_id) = Uuid::parse_str(profile_id_str) else {
            return;
        };

        if self
            .app_state
            .read(cx)
            .is_operation_pending(profile_id, Some(db_name))
        {
            log::info!("Operation already pending for {}", db_name);
            return;
        }

        let params = match self.app_state.update(cx, |state, cx| {
            let result = state.prepare_switch_database(profile_id, db_name);
            if result.is_ok() {
                state.start_pending_operation(profile_id, Some(db_name));
            }
            cx.notify();
            result
        }) {
            Ok(p) => p,
            Err(e) => {
                log::info!("Switch database skipped: {}", e);
                return;
            }
        };

        let app_state = self.app_state.clone();
        let db_name_owned = db_name.to_string();

        cx.spawn(async move |_this, cx| {
            let result = std::thread::spawn(move || params.execute()).join();

            cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    state.finish_pending_operation(profile_id, Some(&db_name_owned));

                    match result {
                        Ok(Ok(res)) => {
                            state.apply_switch_database(
                                res.profile_id,
                                res.original_profile,
                                res.connection,
                                res.schema,
                            );
                            log::info!("Switched to database: {}", res.database);
                        }
                        Ok(Err(e)) => {
                            log::error!("Failed to switch database: {}", e);
                        }
                        Err(_) => {
                            log::error!("Database switch thread panicked");
                        }
                    }

                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    fn refresh_tree(&mut self, cx: &mut Context<Self>) {
        let items = Self::build_tree_items(&self.app_state.read(cx));
        self.tree_state.update(cx, |state, cx| {
            state.set_items(items, cx);
        });
        cx.notify();
    }

    fn build_tree_items(state: &AppState) -> Vec<TreeItem> {
        let mut items = Vec::new();

        for profile in &state.profiles {
            let profile_id = profile.id;
            let is_connected = state.connections.contains_key(&profile_id);
            let is_active = state.active_connection_id == Some(profile_id);
            let is_connecting = state.is_operation_pending(profile_id, None);

            let profile_label = if is_connecting {
                format!("{} (connecting...)", profile.name)
            } else {
                profile.name.clone()
            };

            let mut profile_item = TreeItem::new(format!("profile_{}", profile_id), profile_label);

            if is_connected {
                if let Some(connected) = state.connections.get(&profile_id) {
                    if let Some(ref schema) = connected.schema {
                        let mut profile_children = Vec::new();

                        if !schema.databases.is_empty() {
                            for db in &schema.databases {
                                let is_pending =
                                    state.is_operation_pending(profile_id, Some(&db.name));

                                let db_children = if db.is_current {
                                    Self::build_schema_children(profile_id, schema)
                                } else {
                                    Vec::new()
                                };

                                let db_label = if is_pending {
                                    format!("{} (loading...)", db.name)
                                } else if db.is_current {
                                    format!("{} (current)", db.name)
                                } else {
                                    db.name.clone()
                                };

                                profile_children.push(
                                    TreeItem::new(
                                        format!("db_{}_{}", profile_id, db.name),
                                        db_label,
                                    )
                                    .expanded(db.is_current)
                                    .children(db_children),
                                );
                            }
                        } else {
                            profile_children = Self::build_schema_children(profile_id, schema);
                        }

                        profile_item = profile_item.expanded(is_active).children(profile_children);
                    }
                }
            }

            items.push(profile_item);
        }

        items
    }

    fn build_schema_children(
        profile_id: Uuid,
        snapshot: &dbflux_core::SchemaSnapshot,
    ) -> Vec<TreeItem> {
        let mut children = Vec::new();

        for db_schema in &snapshot.schemas {
            let schema_content = Self::build_db_schema_content(profile_id, db_schema);

            children.push(
                TreeItem::new(
                    format!("schema_{}_{}", profile_id, db_schema.name),
                    db_schema.name.clone(),
                )
                .expanded(db_schema.name == "public")
                .children(schema_content),
            );
        }

        children
    }

    fn build_db_schema_content(
        profile_id: Uuid,
        db_schema: &dbflux_core::DbSchemaInfo,
    ) -> Vec<TreeItem> {
        let mut content = Vec::new();

        if !db_schema.tables.is_empty() {
            let table_children: Vec<TreeItem> = db_schema
                .tables
                .iter()
                .map(|table| Self::build_table_item(profile_id, &db_schema.name, table))
                .collect();

            content.push(
                TreeItem::new(
                    format!("tables_{}_{}", profile_id, db_schema.name),
                    format!("Tables ({})", db_schema.tables.len()),
                )
                .expanded(true)
                .children(table_children),
            );
        }

        if !db_schema.views.is_empty() {
            let view_children: Vec<TreeItem> = db_schema
                .views
                .iter()
                .map(|view| {
                    TreeItem::new(
                        format!("view_{}_{}_{}", profile_id, db_schema.name, view.name),
                        view.name.clone(),
                    )
                })
                .collect();

            content.push(
                TreeItem::new(
                    format!("views_{}_{}", profile_id, db_schema.name),
                    format!("Views ({})", db_schema.views.len()),
                )
                .expanded(true)
                .children(view_children),
            );
        }

        content
    }

    fn build_table_item(
        profile_id: Uuid,
        schema_name: &str,
        table: &dbflux_core::TableInfo,
    ) -> TreeItem {
        let mut table_sections: Vec<TreeItem> = Vec::new();

        if !table.columns.is_empty() {
            let column_children: Vec<TreeItem> = table
                .columns
                .iter()
                .map(|col| {
                    let pk_marker = if col.is_primary_key { " PK" } else { "" };
                    let nullable = if col.nullable { "?" } else { "" };
                    let label = format!("{}: {}{}{}", col.name, col.type_name, nullable, pk_marker);
                    TreeItem::new(
                        format!("col_{}_{}_{}", profile_id, table.name, col.name),
                        label,
                    )
                })
                .collect();

            table_sections.push(
                TreeItem::new(
                    format!("columns_{}_{}_{}", profile_id, schema_name, table.name),
                    format!("Columns ({})", table.columns.len()),
                )
                .expanded(true)
                .children(column_children),
            );
        }

        if !table.indexes.is_empty() {
            let index_children: Vec<TreeItem> = table
                .indexes
                .iter()
                .map(|idx| {
                    let unique_marker = if idx.is_unique { " UNIQUE" } else { "" };
                    let pk_marker = if idx.is_primary { " PK" } else { "" };
                    let cols = idx.columns.join(", ");
                    let label = format!("{} ({}){}{}", idx.name, cols, unique_marker, pk_marker);
                    TreeItem::new(
                        format!("idx_{}_{}_{}", profile_id, table.name, idx.name),
                        label,
                    )
                })
                .collect();

            table_sections.push(
                TreeItem::new(
                    format!("indexes_{}_{}_{}", profile_id, schema_name, table.name),
                    format!("Indexes ({})", table.indexes.len()),
                )
                .expanded(false)
                .children(index_children),
            );
        }

        TreeItem::new(
            format!("table_{}_{}_{}", profile_id, schema_name, table.name),
            table.name.clone(),
        )
        .expanded(false)
        .children(table_sections)
    }

    fn connect_to_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        if self
            .app_state
            .read(cx)
            .is_operation_pending(profile_id, None)
        {
            log::info!("Connection already pending for profile {}", profile_id);
            return;
        }

        let params = match self.app_state.update(cx, |state, cx| {
            let result = state.prepare_connect_profile(profile_id);
            if result.is_ok() {
                state.start_pending_operation(profile_id, None);
            }
            cx.notify();
            result
        }) {
            Ok(p) => p,
            Err(e) => {
                log::info!("Connect skipped: {}", e);
                return;
            }
        };

        let app_state = self.app_state.clone();

        cx.spawn(async move |_this, cx| {
            let result = std::thread::spawn(move || params.execute()).join();

            cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    state.finish_pending_operation(profile_id, None);

                    match result {
                        Ok(Ok(res)) => {
                            state.apply_connect_profile(res.profile, res.connection, res.schema);
                            log::info!("Connected successfully");
                        }
                        Ok(Err(e)) => {
                            log::error!("Failed to connect: {}", e);
                        }
                        Err(_) => {
                            log::error!("Connection thread panicked");
                        }
                    }

                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    fn disconnect_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            state.disconnect(profile_id);
            log::info!("Disconnected profile {}", profile_id);
            cx.notify();
        });
    }

    fn delete_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            if let Some(idx) = state.profiles.iter().position(|p| p.id == profile_id) {
                if let Some(removed) = state.remove_profile(idx) {
                    log::info!("Deleted profile: {}", removed.name);
                }
            }
            cx.notify();
        });
    }

    fn edit_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        let profile = self
            .app_state
            .read(cx)
            .profiles
            .iter()
            .find(|p| p.id == profile_id)
            .cloned();

        let Some(profile) = profile else {
            log::error!("Profile not found: {}", profile_id);
            return;
        };

        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(500.0), px(450.0)), cx);

        cx.open_window(
            WindowOptions {
                titlebar: Some(TitlebarOptions {
                    title: Some("Edit Connection".into()),
                    ..Default::default()
                }),
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                kind: WindowKind::Floating,
                ..Default::default()
            },
            |window, cx| {
                let manager = cx.new(|cx| {
                    ConnectionManagerWindow::new_for_edit(app_state, &profile, window, cx)
                });
                cx.new(|cx| Root::new(manager, window, cx))
            },
        )
        .ok();
    }
}

impl Render for Sidebar {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(table_name) = self.pending_view_table.take() {
            self.results.update(cx, |results, cx| {
                results.view_table(&table_name, window, cx);
            });
        }

        let theme = cx.theme();
        let app_state = self.app_state.clone();
        let state = self.app_state.read(cx);
        let active_id = state.active_connection_id;
        let connections = state.connections.keys().copied().collect::<Vec<_>>();
        let sidebar_entity = cx.entity().clone();

        div()
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
                    .px_2()
                    .h(px(36.0))
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(theme.foreground)
                            .child("Connections"),
                    )
                    .child(
                        Button::new("add-connection")
                            .ghost()
                            .compact()
                            .label("+")
                            .on_click(move |_, _, cx| {
                                let app_state = app_state.clone();
                                cx.open_window(
                                    WindowOptions {
                                        titlebar: Some(TitlebarOptions {
                                            title: Some("Connection Manager".into()),
                                            ..Default::default()
                                        }),
                                        window_bounds: Some(WindowBounds::Windowed(
                                            Bounds::centered(None, size(px(500.0), px(450.0)), cx),
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
                            }),
                    ),
            )
            .child(div().flex_1().overflow_hidden().child(tree(
                &self.tree_state,
                move |ix, entry, selected, _window, cx| {
                    let item = entry.item();
                    let item_id = item.id.clone();
                    let depth = entry.depth();

                    let node_kind = TreeNodeKind::from_id(&item_id);

                    let is_connected = if node_kind == TreeNodeKind::Profile {
                        item_id
                            .strip_prefix("profile_")
                            .and_then(|id_str| Uuid::parse_str(id_str).ok())
                            .is_some_and(|id| connections.contains(&id))
                    } else {
                        false
                    };

                    let is_active = if node_kind == TreeNodeKind::Profile {
                        item_id
                            .strip_prefix("profile_")
                            .and_then(|id_str| Uuid::parse_str(id_str).ok())
                            .is_some_and(|id| active_id == Some(id))
                    } else {
                        false
                    };

                    let theme = cx.theme();
                    let indent = px(depth as f32 * 12.0);
                    let is_folder = entry.is_folder();
                    let is_expanded = entry.is_expanded();

                    let chevron: Option<&str> = if is_folder && node_kind == TreeNodeKind::Table {
                        Some(if is_expanded { "▾" } else { "▸" })
                    } else {
                        None
                    };

                    let color_teal: Hsla = gpui::rgb(0x4EC9B0).into();
                    let color_yellow: Hsla = gpui::rgb(0xDCDCAA).into();
                    let color_blue: Hsla = gpui::rgb(0x9CDCFE).into();
                    let color_purple: Hsla = gpui::rgb(0xC586C0).into();
                    let color_gray: Hsla = gpui::rgb(0x808080).into();
                    let color_orange: Hsla = gpui::rgb(0xCE9178).into();
                    let color_schema: Hsla = gpui::rgb(0x569CD6).into();

                    let (icon, icon_color): (&str, Hsla) = match node_kind {
                        TreeNodeKind::Profile if is_connected => ("●", gpui::green().into()),
                        TreeNodeKind::Profile => ("○", theme.muted_foreground),
                        TreeNodeKind::Database => ("⬡", color_orange),
                        TreeNodeKind::Schema => ("▣", color_schema),
                        TreeNodeKind::TablesFolder => ("▤", color_teal),
                        TreeNodeKind::ViewsFolder => ("◫", color_yellow),
                        TreeNodeKind::Table => ("▦", color_teal),
                        TreeNodeKind::View => ("◧", color_yellow),
                        TreeNodeKind::ColumnsFolder => ("◈", color_blue),
                        TreeNodeKind::IndexesFolder => ("◇", color_purple),
                        TreeNodeKind::Column => ("•", color_blue),
                        TreeNodeKind::Index => ("◆", color_purple),
                        TreeNodeKind::Unknown => ("", theme.muted_foreground),
                    };

                    let label_color: Hsla = match node_kind {
                        TreeNodeKind::Profile => theme.foreground,
                        TreeNodeKind::Database => color_orange,
                        TreeNodeKind::Schema => color_schema,
                        TreeNodeKind::TablesFolder
                        | TreeNodeKind::ViewsFolder
                        | TreeNodeKind::ColumnsFolder
                        | TreeNodeKind::IndexesFolder => color_gray,
                        TreeNodeKind::Table => color_teal,
                        TreeNodeKind::View => color_yellow,
                        TreeNodeKind::Column => color_blue,
                        TreeNodeKind::Index => color_purple,
                        TreeNodeKind::Unknown => theme.muted_foreground,
                    };

                    let mut list_item = ListItem::new(ix)
                        .selected(selected)
                        .py(px(2.0))
                        .text_sm()
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_1()
                                .pl(indent)
                                .when_some(chevron, |el, ch| {
                                    el.child(
                                        div()
                                            .w(px(10.0))
                                            .text_xs()
                                            .text_color(theme.muted_foreground)
                                            .child(ch),
                                    )
                                })
                                .when(
                                    chevron.is_none() && node_kind == TreeNodeKind::Table,
                                    |el| el.child(div().w(px(10.0))),
                                )
                                .child(
                                    div()
                                        .w(px(14.0))
                                        .text_xs()
                                        .text_color(icon_color)
                                        .child(icon),
                                )
                                .child(
                                    div()
                                        .text_color(label_color)
                                        .when(
                                            node_kind == TreeNodeKind::Profile && is_active,
                                            |d| d.font_weight(FontWeight::SEMIBOLD),
                                        )
                                        .when(
                                            matches!(
                                                node_kind,
                                                TreeNodeKind::TablesFolder
                                                    | TreeNodeKind::ViewsFolder
                                            ),
                                            |d| d.text_xs().font_weight(FontWeight::MEDIUM),
                                        )
                                        .child(item.label.clone()),
                                ),
                        );

                    if node_kind.is_clickable() {
                        list_item = list_item.cursor(CursorStyle::PointingHand);
                        let item_id_for_click = item_id.clone();
                        let sidebar = sidebar_entity.clone();
                        list_item = list_item.on_click(move |event, _window, cx| {
                            let click_count = event.click_count();
                            sidebar.update(cx, |this, cx| {
                                this.handle_item_click(&item_id_for_click, click_count, cx);
                            });
                        });
                    }

                    if node_kind == TreeNodeKind::Profile {
                        if let Some(profile_id_str) = item_id.strip_prefix("profile_") {
                            if let Ok(profile_id) = Uuid::parse_str(profile_id_str) {
                                let sidebar_for_menu = sidebar_entity.clone();
                                let profile_connected = is_connected;

                                let btn_id: SharedString = format!("menu-{}", profile_id).into();

                                list_item = list_item.suffix(move |_window, _cx| {
                                    let sidebar = sidebar_for_menu.clone();
                                    let sidebar_action = sidebar.clone();
                                    let sidebar_edit = sidebar.clone();
                                    let sidebar_delete = sidebar.clone();

                                    Button::new(btn_id.clone())
                                        .ghost()
                                        .compact()
                                        .label("⋯")
                                        .on_click(|_ev, _window, cx| {
                                            cx.stop_propagation();
                                        })
                                        .dropdown_menu(move |menu, _window, _cx| {
                                            let menu = if profile_connected {
                                                let sidebar_disconnect = sidebar_action.clone();
                                                menu.item(
                                                    PopupMenuItem::new("Disconnect").on_click(
                                                        move |_ev, _window, cx| {
                                                            sidebar_disconnect.update(
                                                                cx,
                                                                |this, cx| {
                                                                    this.disconnect_profile(
                                                                        profile_id, cx,
                                                                    );
                                                                },
                                                            );
                                                        },
                                                    ),
                                                )
                                            } else {
                                                let sidebar_connect = sidebar_action.clone();
                                                menu.item(PopupMenuItem::new("Connect").on_click(
                                                    move |_ev, _window, cx| {
                                                        sidebar_connect.update(cx, |this, cx| {
                                                            this.connect_to_profile(profile_id, cx);
                                                        });
                                                    },
                                                ))
                                            };

                                            let sidebar_ed = sidebar_edit.clone();
                                            let sidebar_del = sidebar_delete.clone();
                                            menu.item(PopupMenuItem::new("Edit").on_click(
                                                move |_ev, _window, cx| {
                                                    sidebar_ed.update(cx, |this, cx| {
                                                        this.edit_profile(profile_id, cx);
                                                    });
                                                },
                                            ))
                                            .separator()
                                            .item(
                                                PopupMenuItem::new("Delete").on_click(
                                                    move |_ev, _window, cx| {
                                                        sidebar_del.update(cx, |this, cx| {
                                                            this.delete_profile(profile_id, cx);
                                                        });
                                                    },
                                                ),
                                            )
                                        })
                                });
                            }
                        }
                    }

                    list_item
                },
            )))
    }
}
