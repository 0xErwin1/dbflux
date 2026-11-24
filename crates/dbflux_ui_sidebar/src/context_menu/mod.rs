use super::*;
use dbflux_components::composites::{
    BackAction, ChildAction, HOVER_OPEN_DELAY, HoverTick, MenuNav, MenuNode,
};
use dbflux_core::DdlCapabilities;
use dbflux_ui_base::async_ext::AsyncUpdateResultExt;
use std::time::Instant;

/// Code-generation capabilities that fill the "Generate SQL" submenu of an
/// index row.
const INDEX_MENU_CAPABILITIES: CodeGenCapabilities = CodeGenCapabilities::CREATE_INDEX
    .union(CodeGenCapabilities::DROP_INDEX)
    .union(CodeGenCapabilities::REINDEX);

/// Code-generation capabilities that fill the "Generate SQL" submenu of a
/// foreign key row.
const FOREIGN_KEY_MENU_CAPABILITIES: CodeGenCapabilities =
    CodeGenCapabilities::ADD_FOREIGN_KEY.union(CodeGenCapabilities::DROP_FOREIGN_KEY);

/// Code-generation capabilities that fill the "Generate SQL" submenu of a
/// custom type row.
const CUSTOM_TYPE_MENU_CAPABILITIES: CodeGenCapabilities = CodeGenCapabilities::CREATE_TYPE
    .union(CodeGenCapabilities::ALTER_TYPE)
    .union(CodeGenCapabilities::DROP_TYPE);

/// Whether a row of `kind` opens a context menu on a connection whose driver
/// advertises `code_gen` (empty when the row has no connected profile).
/// Right click, the row's menu button and the keyboard menu command all
/// consult this, so a row never offers a menu with nothing in it.
///
/// Index, foreign key and custom type menus only hold generated SQL, so they
/// exist only when the driver can generate some of it. A custom type whose
/// driver can create or alter types but not drop them still gets an empty
/// menu for a composite type, since the type kind lives in the schema cache.
pub(crate) fn node_has_context_menu(kind: SchemaNodeKind, code_gen: CodeGenCapabilities) -> bool {
    if !node_kind_has_context_menu(kind) {
        return false;
    }

    match kind {
        SchemaNodeKind::Index | SchemaNodeKind::SchemaIndex => {
            code_gen.intersects(INDEX_MENU_CAPABILITIES)
        }
        SchemaNodeKind::ForeignKey | SchemaNodeKind::SchemaForeignKey => {
            code_gen.intersects(FOREIGN_KEY_MENU_CAPABILITIES)
        }
        SchemaNodeKind::CustomType => code_gen.intersects(CUSTOM_TYPE_MENU_CAPABILITIES),
        _ => true,
    }
}

/// Whether rows of `kind` can offer a context menu at all, independent of the
/// connection. `build_context_menu_items` defines an arm for exactly these
/// kinds.
pub(crate) fn node_kind_has_context_menu(kind: SchemaNodeKind) -> bool {
    match kind {
        SchemaNodeKind::ConnectionFolder
        | SchemaNodeKind::Profile
        | SchemaNodeKind::DatabasesFolder
        | SchemaNodeKind::Database
        | SchemaNodeKind::Table
        | SchemaNodeKind::View
        | SchemaNodeKind::Collection
        | SchemaNodeKind::CustomType
        | SchemaNodeKind::Index
        | SchemaNodeKind::SchemaIndex
        | SchemaNodeKind::ForeignKey
        | SchemaNodeKind::SchemaForeignKey
        | SchemaNodeKind::ScriptsFolder
        | SchemaNodeKind::ScriptFile
        | SchemaNodeKind::ScriptsRoot
        | SchemaNodeKind::DashboardsFolder
        | SchemaNodeKind::DashboardItem
        | SchemaNodeKind::RemoteDashboardsFolder
        | SchemaNodeKind::SavedChartItem
        | SchemaNodeKind::InstanceMetricsFolder
        | SchemaNodeKind::InstanceMetricLeaf
        | SchemaNodeKind::InstanceInspectorsFolder
        | SchemaNodeKind::InstanceInspectorLeaf
        | SchemaNodeKind::InstanceOverviewLeaf
        | SchemaNodeKind::RootSectionItem
        | SchemaNodeKind::Column
        | SchemaNodeKind::CollectionField => true,

        SchemaNodeKind::Loading
        | SchemaNodeKind::EmptyDatabasesFolder
        | SchemaNodeKind::Schema
        | SchemaNodeKind::TablesFolder
        | SchemaNodeKind::ViewsFolder
        | SchemaNodeKind::TypesFolder
        | SchemaNodeKind::TypesLoadingFolder
        | SchemaNodeKind::SchemaIndexesFolder
        | SchemaNodeKind::SchemaIndexesLoadingFolder
        | SchemaNodeKind::SchemaForeignKeysFolder
        | SchemaNodeKind::SchemaForeignKeysLoadingFolder
        | SchemaNodeKind::RoutinesFolder
        | SchemaNodeKind::RoutinesLoadingFolder
        | SchemaNodeKind::CollectionsFolder
        | SchemaNodeKind::MetricsFolder
        | SchemaNodeKind::MetricNamespaceFolder
        | SchemaNodeKind::MetricLeaf
        | SchemaNodeKind::RemoteDashboardItem
        | SchemaNodeKind::SavedChartsFolder
        | SchemaNodeKind::CollectionChild
        | SchemaNodeKind::CollectionChildrenMore
        | SchemaNodeKind::ColumnsFolder
        | SchemaNodeKind::IndexesFolder
        | SchemaNodeKind::ForeignKeysFolder
        | SchemaNodeKind::ConstraintsFolder
        | SchemaNodeKind::StorageHintsFolder
        | SchemaNodeKind::Constraint
        | SchemaNodeKind::StorageHintItem
        | SchemaNodeKind::Routine
        | SchemaNodeKind::DatabaseIndexesFolder
        | SchemaNodeKind::CollectionFieldsFolder
        | SchemaNodeKind::CollectionIndexesFolder
        | SchemaNodeKind::CollectionIndex
        | SchemaNodeKind::EnumValue
        | SchemaNodeKind::BaseType
        | SchemaNodeKind::Placeholder
        | SchemaNodeKind::DependentsFolder
        | SchemaNodeKind::DependentItem
        | SchemaNodeKind::RootSection
        | SchemaNodeKind::RootSectionLoadMore
        | SchemaNodeKind::SecretStore
        | SchemaNodeKind::SecretPath
        | SchemaNodeKind::InstanceFolder => false,
    }
}

impl Sidebar {
    /// The read error of the unavailable external scripts folder `item_id`
    /// names, or `None` when it is readable or not an external folder.
    fn unavailable_script_root_reason(&self, item_id: &str, cx: &App) -> Option<String> {
        let Some(SchemaNodeId::ScriptsRoot { path }) = parse_node_id(item_id) else {
            return None;
        };

        let mounted = self
            .app_state
            .read(cx)
            .scripts_directory()?
            .external_root_at(std::path::Path::new(&path))?;

        match mounted.availability() {
            dbflux_core::ScriptRootAvailability::Unavailable { reason } => Some(reason.clone()),
            _ => None,
        }
    }
}

/// The mono caption at the top of a schema object's menu: its qualified
/// name (`public.orders`), like the column and row the cell menu names.
pub(crate) fn menu_caption(item_id: &str) -> Option<String> {
    let qualified = |container: &str, name: &str| {
        if container.is_empty() {
            name.to_string()
        } else {
            format!("{container}.{name}")
        }
    };

    match parse_node_id(item_id)? {
        SchemaNodeId::Table { schema, name, .. }
        | SchemaNodeId::View { schema, name, .. }
        | SchemaNodeId::CustomType { schema, name, .. } => Some(qualified(&schema, &name)),
        SchemaNodeId::Collection { database, name, .. } => Some(qualified(&database, &name)),
        SchemaNodeId::Database { name, .. } => Some(name),
        SchemaNodeId::ScriptsRoot { path } => Some(path),
        _ => None,
    }
}

impl ContextMenuState {
    /// The pointer entered `index` of `level`: move the highlight there, and
    /// close the flyout that hangs off a different row of the same level.
    /// Resting on the row that owns the open flyout leaves the flyout fixed.
    /// Returns whether the highlight moved.
    ///
    /// This behaves like a standard nested menu rather than trailing the
    /// cursor: hovering the row that owns the open flyout leaves it fixed,
    /// hovering a different row of the same level switches the cascade to
    /// that row, and hovering a plain row collapses the flyout.
    fn hover_row(&mut self, level: usize, index: usize, now: Instant) -> bool {
        let nodes = ContextMenuItem::to_menu_nodes(&self.items);
        let depth_before = self.nav.cursor().depth();

        let moved = if self.nav.cursor().highlight(level) == Some(index) {
            false
        } else {
            self.nav.set_level(level, index, &nodes)
        };

        if self.nav.cursor().depth() < depth_before {
            // The highlight moved to another row of `level`, which closed the
            // flyout that hung off the old row: the open record must not keep
            // claiming that flyout is open.
            self.hover.mark_open(level, None);
        }

        self.hover.enter(level, index, now);

        moved
    }

    /// Runs one open-on-hover decision: opens the flyout of the row the
    /// pointer has rested on for the open delay, if it opens one. Returns
    /// whether the cascade changed.
    fn hover_tick(&mut self, now: Instant) -> bool {
        match self.hover.tick(now) {
            HoverTick::Open(level, index) => {
                let nodes = ContextMenuItem::to_menu_nodes(&self.items);
                let before = self.nav.cursor().clone();
                self.nav.hover(level, index, &nodes);
                self.hover.mark_open(level, Some(index));
                self.nav.cursor() != &before
            }
            // No pointer-left signal reaches the sidebar from the cascade
            // render (the component reports `Hovered` rows only), so a level
            // never closes from being left: hovering another row of the
            // parent level closes it instead, in `hover_row`. The arm stays
            // for the day a leave signal is wired.
            HoverTick::CloseLevel(level) => {
                let Some(owner) = self.nav.cursor().highlight(level) else {
                    return false;
                };
                let nodes = ContextMenuItem::to_menu_nodes(&self.items);
                let before = self.nav.cursor().clone();
                self.nav.set_level(level, owner, &nodes);
                self.nav.cursor() != &before
            }
            HoverTick::Nothing => false,
        }
    }
}

impl Sidebar {
    pub(crate) fn append_menu_section(
        items: &mut Vec<ContextMenuItem>,
        section: impl IntoIterator<Item = ContextMenuItem>,
    ) {
        let mut section_items: Vec<ContextMenuItem> = section
            .into_iter()
            .filter(ContextMenuItem::is_selectable)
            .collect();

        if section_items.is_empty() {
            return;
        }

        if !items.is_empty() {
            items.push(ContextMenuItem::separator());
        }

        items.append(&mut section_items);
    }

    pub(super) fn view_table_schema(&mut self, item_id: &str, cx: &mut Context<Self>) {
        self.set_expanded(item_id, true, cx);
    }

    pub fn open_item_menu(&mut self, position: Point<Pixels>, cx: &mut Context<Self>) {
        let entry = self.active_tree_state().read(cx).selected_entry().cloned();

        let Some(entry) = entry else {
            return;
        };

        let item_id = entry.item().id.to_string();
        self.open_menu_for_item(&item_id, position, cx);
    }

    pub fn open_menu_for_item(
        &mut self,
        item_id: &str,
        position: Point<Pixels>,
        cx: &mut Context<Self>,
    ) {
        let node_kind = parse_node_kind(item_id);
        if !node_has_context_menu(node_kind, self.get_capabilities_for_item(item_id, cx)) {
            return;
        }

        let mut items = self.build_context_menu_items(node_kind, item_id, cx);

        if items.is_empty() {
            return;
        }

        if let Some(caption) = menu_caption(item_id) {
            items.insert(0, ContextMenuItem::header(caption));
        }

        self.context_menu = Some(ContextMenuState {
            item_id: item_id.to_string(),
            items,
            nav: MenuNav::new(),
            hover: HoverIntent::new(),
            position,
        });

        if let Some(menu) = self.context_menu.as_mut() {
            // The menu opens on its first selectable row, skipping the
            // qualified-name header most menus carry.
            menu.nav.reset(&ContextMenuItem::to_menu_nodes(&menu.items));
        }
        cx.notify();
    }

    /// Runs the action the selected row's menu pairs with `command`'s key
    /// ([`ContextMenuAction::shortcut_command`]), so a key shown beside a
    /// menu action does what that action does. Returns `false` when the row
    /// has no such action.
    pub fn run_selected_menu_shortcut(
        &mut self,
        command: dbflux_app::keymap::Command,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(entry) = self.active_tree_state().read(cx).selected_entry().cloned() else {
            return false;
        };

        let item_id = entry.item().id.to_string();
        let node_kind = parse_node_kind(&item_id);

        if !node_has_context_menu(node_kind, self.get_capabilities_for_item(&item_id, cx)) {
            return false;
        }

        let items = self.build_context_menu_items(node_kind, &item_id, cx);

        let Some(selected_index) = items.iter().position(|item| {
            item.is_selectable() && item.action.shortcut_command() == Some(command)
        }) else {
            return false;
        };

        self.context_menu = Some(ContextMenuState {
            item_id,
            items,
            nav: MenuNav::new(),
            hover: HoverIntent::new(),
            position: Point::default(),
        });

        if let Some(menu) = self.context_menu.as_mut() {
            let nodes = ContextMenuItem::to_menu_nodes(&menu.items);
            menu.nav.set_level(0, selected_index, &nodes);
        }
        self.context_menu_execute(cx);

        true
    }

    /// Whether the table's connection supports the data-transfer Export/
    /// Migrate flows. Gated on `TransferFamily::Sql` (D1) — never on driver
    /// id — so any current or future SQL driver picks this up for free.
    fn table_supports_transfer(&self, item_id: &str, cx: &App) -> bool {
        let Some(SchemaNodeId::Table { profile_id, .. }) = parse_node_id(item_id) else {
            return false;
        };

        self.app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .is_some_and(|connected| {
                connected.connection.metadata().transfer_family == dbflux_core::TransferFamily::Sql
            })
    }

    /// Whether the profile's connection supports the data-transfer Import
    /// flow. Gated on `TransferFamily::Sql` (D1) — never on driver id — so
    /// any current or future SQL driver picks this up for free (T24).
    fn profile_supports_import(&self, item_id: &str, cx: &App) -> bool {
        let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(item_id) else {
            return false;
        };

        self.app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .is_some_and(|connected| {
                connected.connection.metadata().transfer_family == dbflux_core::TransferFamily::Sql
            })
    }

    /// Whether the connection/database node supports the schema-diff workflow.
    /// Gated on `DatabaseCategory::Relational` — never a driver id — so any
    /// current or future relational driver picks it up for free.
    fn node_supports_schema_diff(&self, item_id: &str, cx: &App) -> bool {
        let profile_id = match parse_node_id(item_id) {
            Some(SchemaNodeId::Profile { profile_id }) => profile_id,
            Some(SchemaNodeId::Database { profile_id, .. }) => profile_id,
            _ => return false,
        };

        self.app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .is_some_and(|connected| {
                connected.connection.metadata().category
                    == dbflux_core::DatabaseCategory::Relational
            })
    }

    /// Emits `SidebarEvent::RequestSchemaDiff` for a connection or database
    /// node. Non-relational nodes get an explicit "unsupported" toast rather
    /// than a silent no-op.
    fn open_schema_diff_from_context(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let (profile_id, database) = match parse_node_id(item_id) {
            Some(SchemaNodeId::Profile { profile_id }) => {
                let database = self
                    .app_state
                    .read(cx)
                    .connections()
                    .get(&profile_id)
                    .and_then(|connected| connected.active_database.clone());
                (profile_id, database)
            }
            Some(SchemaNodeId::Database { profile_id, name }) => (profile_id, Some(name)),
            _ => return,
        };

        if !self.node_supports_schema_diff(item_id, cx) {
            dbflux_ui_base::toast::Toast::warning(dbflux_i18n::t!(
                "sidebar.menu.schema_diff_unsupported"
            ))
            .push(cx);
            return;
        }

        cx.emit(SidebarEvent::RequestSchemaDiff {
            profile_id,
            database,
        });
    }

    /// Returns "Delete N items" when the right-clicked node is part of a
    /// multi-selection that contains more than one deletable item, otherwise
    /// `None`. Used to relabel the per-node "Delete" entry into a batch action
    /// when several items are selected.
    fn batch_delete_label(&self, item_id: &str) -> Option<String> {
        if !self.active_selection().contains(item_id) {
            return None;
        }
        let count = self.deletable_multi_selection().len();
        (count > 1).then(|| crate::labels::delete_items_label(count))
    }

    /// If the right-clicked item is part of a deletable multi-selection of >1
    /// items, open the batch delete confirmation and return `true`. Returns
    /// `false` otherwise so the caller can fall back to single-item flow.
    fn try_dispatch_batch_delete(&mut self, item_id: &str, cx: &mut Context<Self>) -> bool {
        if !self.active_selection().contains(item_id) {
            return false;
        }
        let ids = self.deletable_multi_selection();
        if ids.len() <= 1 {
            return false;
        }
        self.show_delete_confirm_modal_for_many(ids, cx);
        true
    }

    pub(super) fn build_context_menu_items(
        &self,
        node_kind: SchemaNodeKind,
        item_id: &str,
        cx: &App,
    ) -> Vec<ContextMenuItem> {
        match node_kind {
            SchemaNodeKind::Table | SchemaNodeKind::View => {
                let mut items = Vec::new();

                // P1Sidebar groups the table menu as: open and inspect,
                // then generate and transfer, then drop.
                let mut open_section = vec![ContextMenuItem::item(
                    dbflux_i18n::t!("sidebar.menu.open"),
                    ContextMenuAction::Open,
                )];

                if self.collection_supports_child_picker(item_id, cx) {
                    open_section.push(ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.browse_event_streams"),
                        ContextMenuAction::OpenChildPicker,
                    ));
                }

                open_section.push(ContextMenuItem::item(
                    dbflux_i18n::t!("sidebar.menu.view_schema"),
                    ContextMenuAction::ViewSchema,
                ));
                open_section.push(ContextMenuItem::item(
                    dbflux_i18n::t!("sidebar.menu.refresh"),
                    ContextMenuAction::RefreshObject,
                ));

                // Add "View Relationships" only for Table nodes (Views don't have FK metadata)
                if node_kind == SchemaNodeKind::Table
                    && self.is_relational_with_fk_support(item_id, cx)
                {
                    open_section.push(ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.view_relationships"),
                        ContextMenuAction::ViewRelationships,
                    ));
                }

                let copy_name_label = match node_kind {
                    SchemaNodeKind::View => dbflux_i18n::t!("sidebar.menu.copy_view_name"),
                    _ => dbflux_i18n::t!("sidebar.menu.copy_table_name"),
                };
                open_section.push(ContextMenuItem::item(
                    copy_name_label,
                    ContextMenuAction::CopyName,
                ));

                Self::append_menu_section(&mut items, open_section);

                let mut transfer_section = Vec::new();

                // Get code generators from driver (if connected)
                let generators = self.get_code_generators_for_item(item_id, node_kind, cx);
                if !generators.is_empty() {
                    transfer_section.push(
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.generate_sql"),
                            ContextMenuAction::Submenu(generators),
                        )
                        .with_icon(AppIcon::Code),
                    );
                }

                // Export (Table -> folder bundle, via the Export wizard) is
                // gated on the connection's transfer_family, never on driver
                // id (R7). Views are excluded — this batch scopes bulk export
                // to writable tables only. Format/folder/segment-size are
                // chosen in the wizard, not from this menu.
                if node_kind == SchemaNodeKind::Table && self.table_supports_transfer(item_id, cx) {
                    let count = self.export_table_selection_count(item_id);
                    let label = crate::labels::export_tables_label(count);

                    transfer_section.push(ContextMenuItem::item(
                        label,
                        ContextMenuAction::ExportTables,
                    ));
                }

                // Migrate (Table -> Table, cross-connection) is gated the
                // same way as Export — `TransferFamily::Sql`, never driver id
                // (R6/R7/R8). Opens the Migrate wizard pre-populated with the
                // resolved table selection; the wizard itself filters valid
                // targets to connected + transfer-compatible connections.
                if node_kind == SchemaNodeKind::Table && self.table_supports_transfer(item_id, cx) {
                    let count = self.migrate_table_selection_count(item_id);
                    let label = crate::labels::migrate_tables_label(count);

                    transfer_section.push(ContextMenuItem::item(
                        label,
                        ContextMenuAction::MigrateTables,
                    ));
                }

                Self::append_menu_section(&mut items, transfer_section);

                // Drop items gated on DDL capabilities
                if let Some(ddl) = self.get_ddl_capabilities(item_id, cx) {
                    let drop_allowed = match node_kind {
                        SchemaNodeKind::Table => ddl.supports_drop_table,
                        SchemaNodeKind::View => ddl.supports_drop_view,
                        _ => false,
                    };
                    if drop_allowed {
                        let label = match node_kind {
                            SchemaNodeKind::View => dbflux_i18n::t!("sidebar.menu.drop_view"),
                            _ => dbflux_i18n::t!("sidebar.menu.drop_table"),
                        };
                        Self::append_menu_section(
                            &mut items,
                            [ContextMenuItem::danger(label, ContextMenuAction::DropTable)],
                        );
                    }
                }

                items
            }
            SchemaNodeKind::Collection => {
                let mut items = Vec::new();

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.open"),
                        ContextMenuAction::Open,
                    )],
                );

                if self.collection_supports_child_picker(item_id, cx) {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.browse_event_streams"),
                            ContextMenuAction::OpenChildPicker,
                        )],
                    );
                }

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.refresh"),
                            ContextMenuAction::RefreshObject,
                        ),
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.copy_collection_name"),
                            ContextMenuAction::CopyName,
                        ),
                    ],
                );

                // "Query Measurement" is available for any time-series collection that
                // has a driver-provided template. The UI stays generic — no driver-id
                // branching here. The driver's QueryGenerator::template_for_collection
                // produces the correct language and query text.
                if self.collection_is_time_series(item_id, cx)
                    && self.collection_has_query_template(item_id, cx)
                {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.query_measurement"),
                            ContextMenuAction::QueryCollection,
                        )],
                    );
                }

                // The Generate Query submenu contains MongoDB-specific operation names.
                // Time-series measurements share the Collection node kind but do not use
                // this submenu — their query model is Flux/InfluxQL, not MQL.
                if !self.collection_is_event_stream(item_id, cx)
                    && !self.collection_is_time_series(item_id, cx)
                {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.generate_query"),
                            ContextMenuAction::Submenu(vec![
                                ContextMenuItem::item(
                                    "find",
                                    ContextMenuAction::GenerateCollectionCode(
                                        CollectionCodeKind::Find,
                                    ),
                                ),
                                ContextMenuItem::item(
                                    "insertOne",
                                    ContextMenuAction::GenerateCollectionCode(
                                        CollectionCodeKind::InsertOne,
                                    ),
                                ),
                                ContextMenuItem::item(
                                    "updateOne",
                                    ContextMenuAction::GenerateCollectionCode(
                                        CollectionCodeKind::UpdateOne,
                                    ),
                                ),
                                ContextMenuItem::item(
                                    "deleteOne",
                                    ContextMenuAction::GenerateCollectionCode(
                                        CollectionCodeKind::DeleteOne,
                                    ),
                                ),
                            ]),
                        )
                        .with_icon(AppIcon::Code)],
                    );
                }

                // Drop collection gated on DDL capabilities; not applicable to time-series
                // measurements which are not directly droppable through the collection abstraction.
                if !self.collection_is_time_series(item_id, cx)
                    && self
                        .get_ddl_capabilities(item_id, cx)
                        .is_some_and(|ddl| ddl.supports_drop_table)
                {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::danger(
                            dbflux_i18n::t!("sidebar.menu.drop_collection"),
                            ContextMenuAction::DropCollection,
                        )],
                    );
                }

                items
            }
            SchemaNodeKind::Profile => {
                let (is_connected, connect_failed) =
                    if let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(item_id) {
                        let state = self.app_state.read(cx);
                        (
                            state.connections().contains_key(&profile_id),
                            state.connect_failure(profile_id).is_some(),
                        )
                    } else {
                        (false, false)
                    };

                let mut items = Vec::new();

                if is_connected {
                    Self::append_menu_section(
                        &mut items,
                        [
                            ContextMenuItem::item(
                                dbflux_i18n::t!("sidebar.menu.disconnect"),
                                ContextMenuAction::Disconnect,
                            ),
                            ContextMenuItem::item(
                                dbflux_i18n::t!("sidebar.menu.refresh"),
                                ContextMenuAction::Refresh,
                            ),
                        ],
                    );
                } else {
                    let connect_label = if connect_failed {
                        dbflux_i18n::t!("sidebar.menu.retry_connect")
                    } else {
                        dbflux_i18n::t!("sidebar.menu.connect")
                    };
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            connect_label,
                            ContextMenuAction::Connect,
                        )],
                    );
                }

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.edit"),
                            ContextMenuAction::Edit,
                        ),
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.duplicate"),
                            ContextMenuAction::Duplicate,
                        ),
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.rename"),
                            ContextMenuAction::RenameFolder,
                        ),
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.export_ellipsis"),
                            ContextMenuAction::Export,
                        ),
                    ],
                );

                // Import (folder bundle -> tables) is offered for any connected
                // SQL-family profile, gated on `TransferFamily::Sql` like Export —
                // never on driver id (R7/R8).
                if is_connected && self.profile_supports_import(item_id, cx) {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.import_ellipsis"),
                            ContextMenuAction::ImportTables,
                        )],
                    );
                }

                // Compare Schema (relational only) — opens the schema-diff
                // document with this connection as the live target. Gated on
                // `DatabaseCategory::Relational`, never a driver id.
                if is_connected && self.node_supports_schema_diff(item_id, cx) {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.compare_schema"),
                            ContextMenuAction::CompareSchema,
                        )],
                    );
                }

                // Add "Move to..." submenu with available folders
                let move_to_items = self.build_move_to_submenu(item_id, cx);
                if !move_to_items.is_empty() {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.move_to"),
                            ContextMenuAction::Submenu(move_to_items),
                        )
                        .with_icon(AppIcon::Folder)],
                    );
                }

                let delete_label = self
                    .batch_delete_label(item_id)
                    .unwrap_or_else(|| dbflux_i18n::t!("sidebar.menu.delete"));
                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::danger(
                        delete_label,
                        ContextMenuAction::Delete,
                    )],
                );

                items
            }
            SchemaNodeKind::Database => {
                let is_loaded = self.is_database_schema_loaded(item_id, cx);
                let mut items = Vec::new();

                if is_loaded {
                    // Only show Close for databases that support it (MySQL/MariaDB)
                    if self.database_supports_close(item_id, cx) {
                        Self::append_menu_section(
                            &mut items,
                            [ContextMenuItem::item(
                                dbflux_i18n::t!("sidebar.menu.close"),
                                ContextMenuAction::CloseDatabase,
                            )],
                        );
                    }

                    if self.is_relational_with_fk_support(item_id, cx) {
                        Self::append_menu_section(
                            &mut items,
                            [ContextMenuItem::item(
                                dbflux_i18n::t!("sidebar.menu.view_schema_diagram"),
                                ContextMenuAction::ViewSchemaDiagram,
                            )],
                        );
                    }

                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.refresh"),
                            ContextMenuAction::RefreshDatabase,
                        )],
                    );

                    if self.database_has_native_console(item_id, cx) {
                        Self::append_menu_section(
                            &mut items,
                            [ContextMenuItem::item(
                                dbflux_i18n::t!("sidebar.menu.open_console"),
                                ContextMenuAction::OpenConsole,
                            )],
                        );
                    }

                    // Compare Schema (relational only), scoped to this database.
                    if self.node_supports_schema_diff(item_id, cx) {
                        Self::append_menu_section(
                            &mut items,
                            [ContextMenuItem::item(
                                dbflux_i18n::t!("sidebar.menu.compare_schema"),
                                ContextMenuAction::CompareSchema,
                            )],
                        );
                    }

                    // "New Query" opens an empty code document with this bucket/database
                    // pre-selected in the source-context dropdown. Available for any
                    // time-series database node — no driver-id branching.
                    if self.database_lists_measurements(item_id, cx) {
                        Self::append_menu_section(
                            &mut items,
                            [ContextMenuItem::item(
                                dbflux_i18n::t!("sidebar.menu.new_query"),
                                ContextMenuAction::NewQueryForDatabase,
                            )],
                        );
                    }

                    // Drop Database gated on DDL capabilities
                    if self
                        .get_ddl_capabilities(item_id, cx)
                        .is_some_and(|ddl| ddl.supports_drop_database)
                    {
                        Self::append_menu_section(
                            &mut items,
                            [ContextMenuItem::danger(
                                dbflux_i18n::t!("sidebar.menu.drop_database"),
                                ContextMenuAction::DropDatabase,
                            )],
                        );
                    }
                } else {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.open"),
                            ContextMenuAction::OpenDatabase,
                        )],
                    );
                }

                items
            }
            SchemaNodeKind::ConnectionFolder => {
                let mut items = Vec::new();

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.new_connection"),
                            ContextMenuAction::NewConnection,
                        ),
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.new_folder"),
                            ContextMenuAction::NewFolder,
                        ),
                    ],
                );

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.rename"),
                        ContextMenuAction::RenameFolder,
                    )],
                );

                let move_to_items = self.build_move_to_submenu(item_id, cx);
                if !move_to_items.is_empty() {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.move_to"),
                            ContextMenuAction::Submenu(move_to_items),
                        )
                        .with_icon(AppIcon::Folder)],
                    );
                }

                let delete_label = self
                    .batch_delete_label(item_id)
                    .unwrap_or_else(|| dbflux_i18n::t!("sidebar.menu.delete"));
                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::danger(
                        delete_label,
                        ContextMenuAction::DeleteFolder,
                    )],
                );

                items
            }

            SchemaNodeKind::Index | SchemaNodeKind::SchemaIndex => {
                let caps = self.get_capabilities_for_item(item_id, cx);
                let mut submenu = Vec::new();

                if caps.contains(CodeGenCapabilities::CREATE_INDEX) {
                    submenu.push(ContextMenuItem::item(
                        "CREATE INDEX",
                        ContextMenuAction::GenerateIndexSql(IndexSqlAction::Create),
                    ));
                }

                if caps.contains(CodeGenCapabilities::DROP_INDEX) {
                    submenu.push(ContextMenuItem::item(
                        "DROP INDEX",
                        ContextMenuAction::GenerateIndexSql(IndexSqlAction::Drop),
                    ));
                }

                if caps.contains(CodeGenCapabilities::REINDEX) {
                    submenu.push(ContextMenuItem::item(
                        "REINDEX",
                        ContextMenuAction::GenerateIndexSql(IndexSqlAction::Reindex),
                    ));
                }

                if submenu.is_empty() {
                    vec![]
                } else {
                    vec![
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.generate_sql"),
                            ContextMenuAction::Submenu(submenu),
                        )
                        .with_icon(AppIcon::Code),
                    ]
                }
            }

            SchemaNodeKind::ForeignKey | SchemaNodeKind::SchemaForeignKey => {
                let caps = self.get_capabilities_for_item(item_id, cx);
                let mut submenu = Vec::new();

                if caps.contains(CodeGenCapabilities::ADD_FOREIGN_KEY) {
                    submenu.push(ContextMenuItem::item(
                        "ADD CONSTRAINT",
                        ContextMenuAction::GenerateForeignKeySql(
                            ForeignKeySqlAction::AddConstraint,
                        ),
                    ));
                }

                if caps.contains(CodeGenCapabilities::DROP_FOREIGN_KEY) {
                    submenu.push(ContextMenuItem::item(
                        "DROP CONSTRAINT",
                        ContextMenuAction::GenerateForeignKeySql(
                            ForeignKeySqlAction::DropConstraint,
                        ),
                    ));
                }

                if submenu.is_empty() {
                    vec![]
                } else {
                    vec![
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.generate_sql"),
                            ContextMenuAction::Submenu(submenu),
                        )
                        .with_icon(AppIcon::Code),
                    ]
                }
            }

            SchemaNodeKind::CustomType => {
                let caps = self.get_capabilities_for_item(item_id, cx);
                let mut submenu = Vec::new();

                if caps.contains(CodeGenCapabilities::CREATE_TYPE)
                    && let Some(label) = self.create_type_sql_label(item_id, cx)
                {
                    submenu.push(ContextMenuItem::item(
                        label,
                        ContextMenuAction::GenerateTypeSql(TypeSqlAction::Create),
                    ));
                }

                if caps.contains(CodeGenCapabilities::ALTER_TYPE) && self.is_enum_type(item_id, cx)
                {
                    submenu.push(ContextMenuItem::item(
                        "ADD VALUE",
                        ContextMenuAction::GenerateTypeSql(TypeSqlAction::AddEnumValue),
                    ));
                }

                if caps.contains(CodeGenCapabilities::DROP_TYPE) {
                    submenu.push(ContextMenuItem::item(
                        "DROP TYPE",
                        ContextMenuAction::GenerateTypeSql(TypeSqlAction::Drop),
                    ));
                }

                if submenu.is_empty() {
                    vec![]
                } else {
                    vec![
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.generate_sql"),
                            ContextMenuAction::Submenu(submenu),
                        )
                        .with_icon(AppIcon::Code),
                    ]
                }
            }

            SchemaNodeKind::ScriptsFolder => {
                let mut items = Vec::new();

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.new_script_file"),
                            ContextMenuAction::NewScriptFile,
                        ),
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.new_script_folder"),
                            ContextMenuAction::NewScriptFolder,
                        ),
                    ],
                );

                // Only show rename/delete for subfolders, not the root
                if let Some(SchemaNodeId::ScriptsFolder { path: Some(_) }) = parse_node_id(item_id)
                {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.rename"),
                            ContextMenuAction::RenameScript,
                        )],
                    );

                    let delete_label = self
                        .batch_delete_label(item_id)
                        .unwrap_or_else(|| dbflux_i18n::t!("sidebar.menu.delete"));
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::danger(
                            delete_label,
                            ContextMenuAction::DeleteScript,
                        )],
                    );
                }

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.reveal_file_manager"),
                            ContextMenuAction::RevealInFileManager,
                        ),
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.copy_path"),
                            ContextMenuAction::CopyPath,
                        ),
                    ],
                );

                items
            }

            SchemaNodeKind::ScriptsRoot => {
                let mut items = Vec::new();

                // Why the folder could not be read; the tree row only says that
                // it could not.
                if let Some(reason) = self.unavailable_script_root_reason(item_id, cx) {
                    items.push(ContextMenuItem::header(
                        crate::labels::scripts_root_unavailable_reason_label(&reason),
                    ));
                }

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.new_script_file"),
                            ContextMenuAction::NewScriptFile,
                        ),
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.new_script_folder"),
                            ContextMenuAction::NewScriptFolder,
                        ),
                    ],
                );

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.refresh_scripts"),
                        ContextMenuAction::RefreshScripts,
                    )],
                );

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.reveal_file_manager"),
                            ContextMenuAction::RevealInFileManager,
                        ),
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.copy_path"),
                            ContextMenuAction::CopyPath,
                        ),
                    ],
                );

                // Not a danger item: it only forgets the registration.
                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.remove_external_folder"),
                        ContextMenuAction::RemoveExternalScriptsFolder,
                    )],
                );

                items
            }

            SchemaNodeKind::ScriptFile => {
                let mut items = Vec::new();

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.open"),
                        ContextMenuAction::OpenScript,
                    )],
                );

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.rename"),
                        ContextMenuAction::RenameScript,
                    )],
                );

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.reveal_file_manager"),
                            ContextMenuAction::RevealInFileManager,
                        ),
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.copy_path"),
                            ContextMenuAction::CopyPath,
                        ),
                    ],
                );

                let delete_label = self
                    .batch_delete_label(item_id)
                    .unwrap_or_else(|| dbflux_i18n::t!("sidebar.menu.delete"));
                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::danger(
                        delete_label,
                        ContextMenuAction::DeleteScript,
                    )],
                );

                items
            }

            SchemaNodeKind::DashboardsFolder => {
                let mut items = Vec::new();

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.new_dashboard"),
                        ContextMenuAction::NewDashboard,
                    )],
                );

                // "Import Dashboard from JSON..." is gated on the driver's
                // DASHBOARD_IMPORT capability — only drivers that vend
                // importable dashboard JSON (CloudWatch-style metric dashboards)
                // advertise this bit.
                let can_import = parse_node_id(item_id)
                    .and_then(|node_id| {
                        if let SchemaNodeId::DashboardsFolder { profile_id } = node_id {
                            Some(profile_id)
                        } else {
                            None
                        }
                    })
                    .and_then(|profile_id| {
                        let state = self.app_state.read(cx);
                        let conn = state.connections().get(&profile_id)?;
                        Some(conn.connection.metadata().capabilities)
                    })
                    .is_some_and(|caps| {
                        caps.contains(dbflux_core::DriverCapabilities::DASHBOARD_IMPORT)
                    });

                if can_import {
                    Self::append_menu_section(
                        &mut items,
                        [ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.import_dashboard"),
                            ContextMenuAction::ImportDashboard,
                        )],
                    );
                }

                items
            }

            SchemaNodeKind::RemoteDashboardsFolder => {
                vec![ContextMenuItem::item(
                    dbflux_i18n::t!("sidebar.menu.refresh"),
                    ContextMenuAction::RefreshRemoteDashboards,
                )]
            }

            SchemaNodeKind::DashboardItem => {
                let mut items = Vec::new();

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.open"),
                        ContextMenuAction::Open,
                    )],
                );

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.rename_ellipsis"),
                            ContextMenuAction::RenameDashboard,
                        ),
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.duplicate"),
                            ContextMenuAction::DuplicateDashboard,
                        ),
                    ],
                );

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::danger(
                        dbflux_i18n::t!("sidebar.menu.delete"),
                        ContextMenuAction::DeleteDashboard,
                    )],
                );

                items
            }

            SchemaNodeKind::SavedChartItem => {
                let mut items = Vec::new();

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.open"),
                        ContextMenuAction::Open,
                    )],
                );

                Self::append_menu_section(
                    &mut items,
                    [
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.rename_ellipsis"),
                            ContextMenuAction::RenameSavedChart,
                        ),
                        ContextMenuItem::item(
                            dbflux_i18n::t!("sidebar.menu.duplicate"),
                            ContextMenuAction::DuplicateSavedChart,
                        ),
                    ],
                );

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::danger(
                        dbflux_i18n::t!("sidebar.menu.delete"),
                        ContextMenuAction::DeleteSavedChart,
                    )],
                );

                items
            }

            SchemaNodeKind::DatabasesFolder => {
                vec![ContextMenuItem::item(
                    dbflux_i18n::t!("sidebar.menu.refresh"),
                    ContextMenuAction::Refresh,
                )]
            }

            SchemaNodeKind::InstanceMetricsFolder => {
                vec![ContextMenuItem::item(
                    dbflux_i18n::t!("sidebar.menu.refresh"),
                    ContextMenuAction::RefreshInstanceCatalog,
                )]
            }

            SchemaNodeKind::InstanceInspectorsFolder => {
                vec![ContextMenuItem::item(
                    dbflux_i18n::t!("sidebar.menu.refresh"),
                    ContextMenuAction::RefreshInstanceCatalog,
                )]
            }

            SchemaNodeKind::InstanceMetricLeaf => {
                let mut items = Vec::new();

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.open"),
                        ContextMenuAction::Open,
                    )],
                );

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.copy_metric_id"),
                        ContextMenuAction::CopyItemId,
                    )],
                );

                items
            }

            SchemaNodeKind::InstanceInspectorLeaf => {
                let mut items = Vec::new();

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.open"),
                        ContextMenuAction::Open,
                    )],
                );

                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.copy_inspector_id"),
                        ContextMenuAction::CopyItemId,
                    )],
                );

                items
            }

            SchemaNodeKind::InstanceOverviewLeaf => {
                let mut items = Vec::new();
                Self::append_menu_section(
                    &mut items,
                    [ContextMenuItem::item(
                        dbflux_i18n::t!("sidebar.menu.open"),
                        ContextMenuAction::Open,
                    )],
                );
                items
            }

            SchemaNodeKind::RootSectionItem => self.root_section_item_menu(item_id),

            SchemaNodeKind::Column => vec![
                ContextMenuItem::item(
                    dbflux_i18n::t!("sidebar.menu.copy_column_name"),
                    ContextMenuAction::CopyName,
                )
                .with_icon(AppIcon::Copy),
            ],

            SchemaNodeKind::CollectionField => vec![
                ContextMenuItem::item(
                    dbflux_i18n::t!("sidebar.menu.copy_field_name"),
                    ContextMenuAction::CopyName,
                )
                .with_icon(AppIcon::Copy),
            ],

            _ => vec![],
        }
    }

    /// Builds the "Move to..." submenu items for a profile or folder.
    fn build_move_to_submenu(&self, item_id: &str, cx: &App) -> Vec<ContextMenuItem> {
        let state = self.app_state.read(cx);
        let mut items = Vec::new();

        // Determine current node info (works for both profiles and folders)
        let (current_parent, current_node_id) = match parse_node_id(item_id) {
            Some(SchemaNodeId::Profile { profile_id }) => {
                let node = state.connection_tree().find_by_profile(profile_id);
                (node.and_then(|n| n.parent_id), node.map(|n| n.id))
            }
            Some(SchemaNodeId::ConnectionFolder { node_id }) => {
                let node = state.connection_tree().find_by_id(node_id);
                (node.and_then(|n| n.parent_id), Some(node_id))
            }
            _ => (None, None),
        };

        // Add "Root" option if not already at root
        if current_parent.is_some() {
            items.push(ContextMenuItem::item(
                dbflux_i18n::t!("sidebar.menu.root"),
                ContextMenuAction::MoveToFolder(None),
            ));
        }

        // Add all folders (except self and descendants for folders)
        let descendants = current_node_id
            .map(|id| state.connection_tree().get_descendants(id))
            .unwrap_or_default();

        for folder in state.connection_tree().folders() {
            // Skip if this is the current parent
            if Some(folder.id) == current_parent {
                continue;
            }
            // Skip self (for folders)
            if Some(folder.id) == current_node_id {
                continue;
            }
            // Skip descendants (would create cycle)
            if descendants.contains(&folder.id) {
                continue;
            }

            items.push(ContextMenuItem::item(
                folder.name.clone(),
                ContextMenuAction::MoveToFolder(Some(folder.id)),
            ));
        }

        items
    }

    pub(super) fn is_database_schema_loaded(&self, item_id: &str, cx: &App) -> bool {
        let Some(SchemaNodeId::Database { profile_id, name }) = parse_node_id(item_id) else {
            return false;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&profile_id) else {
            return false;
        };

        if conn.database_schemas.contains_key(&name) {
            return true;
        }

        if conn.database_connections.contains_key(&name) {
            return true;
        }

        conn.schema
            .as_ref()
            .and_then(|s| s.current_database())
            .is_some_and(|current| current == name)
    }

    /// Whether a database node supports Close (not available for the primary database).
    pub(super) fn database_supports_close(&self, item_id: &str, cx: &App) -> bool {
        let Some(SchemaNodeId::Database { profile_id, name }) = parse_node_id(item_id) else {
            return false;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&profile_id) else {
            return false;
        };

        let strategy = conn.connection.schema_loading_strategy();

        match strategy {
            SchemaLoadingStrategy::LazyPerDatabase => conn.database_schemas.contains_key(&name),
            SchemaLoadingStrategy::ConnectionPerDatabase => {
                conn.database_connections.contains_key(&name)
            }
            _ => false,
        }
    }

    /// Extract DDL capabilities from the driver metadata for the given item.
    pub(super) fn get_ddl_capabilities(&self, item_id: &str, cx: &App) -> Option<DdlCapabilities> {
        let profile_id = Self::extract_profile_id_from_item(item_id)?;
        let state = self.app_state.read(cx);
        let conn = state.connections().get(&profile_id)?;
        conn.connection.metadata().ddl.clone()
    }

    pub(super) fn collection_info_for_item(&self, item_id: &str, cx: &App) -> Option<TableInfo> {
        let SchemaNodeId::Collection {
            profile_id,
            database,
            name,
        } = parse_node_id(item_id)?
        else {
            return None;
        };

        let state = self.app_state.read(cx);
        let conn = state.connections().get(&profile_id)?;
        let cache_key = (database.clone(), Some(database.clone()), name.clone());

        if let Some(details) = conn.table_details.get(&cache_key) {
            return Some(details.clone());
        }

        conn.database_schemas
            .get(&database)
            .and_then(|schema| schema.tables.iter().find(|table| table.name == name))
            .cloned()
            .or_else(|| {
                conn.schema.as_ref().and_then(|schema| {
                    schema.collections().iter().find_map(|collection| {
                        if collection.name != name {
                            return None;
                        }

                        if collection
                            .database
                            .as_deref()
                            .is_some_and(|db| db != database)
                        {
                            return None;
                        }

                        Some(TableInfo {
                            name: collection.name.clone(),
                            schema: Some(database.clone()),
                            columns: None,
                            indexes: collection.indexes.clone().map(IndexData::Document),
                            foreign_keys: None,
                            constraints: None,
                            sample_fields: collection.sample_fields.clone(),
                            presentation: collection.presentation,
                            child_items: collection.child_items.clone(),
                            storage_hints: None,
                            pseudo_columns: Box::default(),
                        })
                    })
                })
            })
    }

    fn collection_supports_child_picker(&self, item_id: &str, cx: &App) -> bool {
        self.collection_info_for_item(item_id, cx)
            .is_some_and(|collection| {
                collection.presentation == CollectionPresentation::EventStream
                    || collection
                        .child_items
                        .as_ref()
                        .is_some_and(|items| !items.is_empty())
            })
    }

    fn collection_is_event_stream(&self, item_id: &str, cx: &App) -> bool {
        self.collection_info_for_item(item_id, cx)
            .is_some_and(|collection| {
                collection.presentation == CollectionPresentation::EventStream
            })
    }

    /// Returns true when the collection node declares the time-series facet.
    ///
    /// Used to suppress document-database-specific menu items (e.g. MongoDB generate
    /// query submenu) for measurements that are rendered as Collection nodes.
    fn collection_is_time_series(&self, item_id: &str, cx: &App) -> bool {
        let Some(SchemaNodeId::Collection {
            profile_id,
            database,
            name,
        }) = parse_node_id(item_id)
        else {
            return false;
        };

        self.app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .is_some_and(|connected| collection_has_time_series_facet(connected, &database, &name))
    }

    /// Returns true when the driver's `QueryGenerator` can produce a query template
    /// for this collection node. Used to gate the "Query Measurement" menu item.
    fn collection_has_query_template(&self, item_id: &str, cx: &App) -> bool {
        let Some(SchemaNodeId::Collection {
            profile_id,
            database,
            name,
        }) = parse_node_id(item_id)
        else {
            return false;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&profile_id) else {
            return false;
        };

        let Some(query_gen) = conn.connection.query_generator() else {
            return false;
        };

        let request = dbflux_core::CollectionTemplateRequest {
            collection: &name,
            database: &database,
        };

        query_gen.template_for_collection(&request).is_some()
    }

    /// Whether the node's connection offers a native console. Generic: read
    /// from the driver's capabilities, never from its id.
    fn database_has_native_console(&self, item_id: &str, cx: &App) -> bool {
        let Some(profile_id) = Self::extract_profile_id_from_item(item_id) else {
            return false;
        };

        self.app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .is_some_and(|conn| conn.connection.metadata().native_console().is_some())
    }

    /// Returns true when the database node lists measurements.
    ///
    /// Used to show the "New Query" action on bucket/database nodes whose
    /// objects are measurements, read from the snapshot's database layout
    /// rather than from a driver ID.
    fn database_lists_measurements(&self, item_id: &str, cx: &App) -> bool {
        let Some(profile_id) = Self::extract_profile_id_from_item(item_id) else {
            return false;
        };

        self.app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .is_some_and(lists_measurements)
    }

    fn open_child_picker(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let pending = PendingAction::OpenChildPicker {
            item_id: item_id.to_string(),
        };

        let status = match parse_node_id(item_id) {
            Some(SchemaNodeId::Collection {
                profile_id,
                database,
                name,
            }) if self.collection_is_event_stream(item_id, cx) => {
                self.ensure_collection_children(profile_id, &database, &name, pending, cx)
            }
            _ => self.ensure_table_details(item_id, pending, cx),
        };

        match status {
            TableDetailsStatus::Ready => {
                self.pending_child_picker_item = Some(item_id.to_string());
            }
            TableDetailsStatus::Loading => {
                self.pending_toast = Some(PendingToast {
                    message: crate::labels::loading_event_streams_toast_label(),
                    is_error: false,
                });
            }
            TableDetailsStatus::NotFound => {
                self.pending_toast = Some(PendingToast {
                    message: dbflux_i18n::t!("sidebar.overlay.child_picker.unsupported"),
                    is_error: true,
                });
            }
        }
    }

    /// Moves the highlight to the next selectable row of the deepest open
    /// level, wrapping at the level's end.
    pub fn context_menu_select_next(&mut self, cx: &mut Context<Self>) {
        if let Some(menu) = self.context_menu.as_mut()
            && menu
                .nav
                .step(&ContextMenuItem::to_menu_nodes(&menu.items), true)
        {
            cx.notify();
        }
    }

    /// Moves the highlight to the previous selectable row of the deepest open
    /// level, wrapping at the level's start.
    pub fn context_menu_select_prev(&mut self, cx: &mut Context<Self>) {
        if let Some(menu) = self.context_menu.as_mut()
            && menu
                .nav
                .step(&ContextMenuItem::to_menu_nodes(&menu.items), false)
        {
            cx.notify();
        }
    }

    /// Moves the highlight to the first selectable row of the deepest open
    /// level.
    pub fn context_menu_select_first(&mut self, cx: &mut Context<Self>) {
        if let Some(menu) = self.context_menu.as_mut() {
            let nodes = ContextMenuItem::to_menu_nodes(&menu.items);
            let level = menu.nav.cursor().depth().saturating_sub(1);
            let Some(first) = deepest_level_rows(&nodes, &menu.nav)
                .iter()
                .position(MenuNode::is_selectable)
            else {
                return;
            };

            if menu.nav.cursor().highlight(level) != Some(first)
                && menu.nav.set_level(level, first, &nodes)
            {
                cx.notify();
            }
        }
    }

    /// Moves the highlight to the last selectable row of the deepest open
    /// level.
    pub fn context_menu_select_last(&mut self, cx: &mut Context<Self>) {
        if let Some(menu) = self.context_menu.as_mut() {
            let nodes = ContextMenuItem::to_menu_nodes(&menu.items);
            let level = menu.nav.cursor().depth().saturating_sub(1);
            let Some(last) = deepest_level_rows(&nodes, &menu.nav)
                .iter()
                .rposition(MenuNode::is_selectable)
            else {
                return;
            };

            if menu.nav.cursor().highlight(level) != Some(last)
                && menu.nav.set_level(level, last, &nodes)
            {
                cx.notify();
            }
        }
    }

    /// The pointer entered `index` of `level` of the open menu: the highlight
    /// follows it, and resting on a submenu row opens its flyout (see
    /// [`Self::arm_context_menu_hover_open`]).
    pub fn context_menu_row_hovered(&mut self, level: usize, index: usize, cx: &mut Context<Self>) {
        if let Some(menu) = self.context_menu.as_mut()
            && menu.hover_row(level, index, Instant::now())
        {
            cx.notify();
        }

        if self
            .context_menu
            .as_ref()
            .is_some_and(|menu| menu.hover.pending().is_some())
        {
            self.arm_context_menu_hover_open(cx);
        }
    }

    /// Enter or activate the highlighted row of the deepest open level:
    /// Enter or Right on a submenu row opens its flyout, on a leaf row runs
    /// its action and closes the menu.
    pub fn context_menu_execute(&mut self, cx: &mut Context<Self>) {
        let Some(menu) = self.context_menu.as_mut() else {
            return;
        };

        let nodes = ContextMenuItem::to_menu_nodes(&menu.items);

        match menu.nav.open_child(&nodes) {
            ChildAction::Opened => {
                // Register the open in the hover intent, so resting the
                // pointer on the owning row afterwards re-arms nothing and
                // never resets the flyout's own highlight.
                let depth = menu.nav.cursor().depth();
                if depth >= 2
                    && let Some(owner) = menu.nav.cursor().highlight(depth - 2)
                {
                    menu.hover.mark_open(depth - 2, Some(owner));
                }
                cx.notify();
            }
            ChildAction::None => {}
            ChildAction::Activate => {
                let path = menu.nav.cursor().path().to_vec();
                self.context_menu_activated(&path, cx);
            }
        }
    }

    /// A leaf row of the open menu was activated, from a click or from
    /// [`Self::context_menu_execute`]: runs the row's action and closes the
    /// menu. No sidebar menu row keeps the menu open (none is checkable), so
    /// every activation closes it.
    pub fn context_menu_activated(&mut self, path: &[usize], cx: &mut Context<Self>) {
        let Some(menu) = self.context_menu.as_mut() else {
            return;
        };

        let Some(item) = Self::menu_item_at_path(&menu.items, path).cloned() else {
            return;
        };

        if !item.is_selectable() {
            return;
        }

        let item_id = menu.item_id.clone();
        self.run_menu_action(item.action, item_id, cx);

        self.context_menu = None;
        cx.notify();
    }

    /// A submenu row of the open menu was clicked: open its flyout, keeping
    /// the rest of the cascade.
    pub fn context_menu_open_flyout(&mut self, level: usize, index: usize, cx: &mut Context<Self>) {
        let Some(menu) = self.context_menu.as_mut() else {
            return;
        };

        let nodes = ContextMenuItem::to_menu_nodes(&menu.items);
        if menu.nav.hover(level, index, &nodes) {
            menu.hover.mark_open(level, Some(index));
            cx.notify();
        }
    }

    /// Runs the action a menu row carries, on the node the menu was opened
    /// on.
    fn run_menu_action(
        &mut self,
        action: ContextMenuAction,
        item_id: String,
        cx: &mut Context<Self>,
    ) {
        match action {
            // A flyout row reports an open, never an activation, so this arm
            // only guards a menu that changed under the pointer between
            // render and click: there is nothing to run.
            ContextMenuAction::Submenu(_) => {}
            ContextMenuAction::Open => {
                let node_kind = parse_node_kind(&item_id);
                match node_kind {
                    SchemaNodeKind::Collection => {
                        self.browse_collection(&item_id, cx);
                    }
                    SchemaNodeKind::DashboardItem
                    | SchemaNodeKind::SavedChartItem
                    | SchemaNodeKind::InstanceMetricLeaf
                    | SchemaNodeKind::InstanceInspectorLeaf
                    | SchemaNodeKind::InstanceOverviewLeaf
                    | SchemaNodeKind::RootSectionItem => {
                        // Delegate to execute_item which emits the correct sidebar event.
                        self.execute_item(&item_id, cx);
                    }
                    _ => {
                        self.browse_table(&item_id, cx);
                    }
                }
            }
            ContextMenuAction::OpenChildPicker => {
                self.open_child_picker(&item_id, cx);
            }
            ContextMenuAction::ViewSchema => {
                self.set_expanded(&item_id, true, cx);
            }
            ContextMenuAction::ViewRelationships => {
                self.open_schema_viz(&item_id, cx);
            }
            ContextMenuAction::ViewSchemaDiagram => {
                self.open_schema_diagram(&item_id, cx);
            }
            ContextMenuAction::GenerateCode(generator_id) => {
                self.generate_code(&item_id, &generator_id, cx);
            }
            ContextMenuAction::Connect => {
                if let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(&item_id) {
                    self.connect_to_profile(profile_id, cx);
                }
            }
            ContextMenuAction::Disconnect => {
                if let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(&item_id) {
                    self.request_disconnect(profile_id, cx);
                }
            }
            ContextMenuAction::Refresh => {
                let profile_id = match parse_node_id(&item_id) {
                    Some(SchemaNodeId::Profile { profile_id }) => Some(profile_id),
                    Some(SchemaNodeId::DatabasesFolder { profile_id }) => Some(profile_id),
                    _ => None,
                };
                if let Some(profile_id) = profile_id {
                    self.refresh_connection(profile_id, cx);
                }
            }
            ContextMenuAction::Edit => {
                if let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(&item_id) {
                    self.edit_profile(profile_id, cx);
                }
            }
            ContextMenuAction::Export => {
                if let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(&item_id) {
                    cx.emit(SidebarEvent::RequestExportConnection { profile_id });
                }
            }
            ContextMenuAction::ExportTables => {
                self.request_export_wizard(&item_id, cx);
            }
            ContextMenuAction::ImportTables => {
                if let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(&item_id) {
                    let database = self
                        .app_state
                        .read(cx)
                        .connections()
                        .get(&profile_id)
                        .and_then(|connected| connected.active_database.clone());
                    cx.emit(SidebarEvent::RequestImportWizard {
                        profile_id,
                        database,
                    });
                }
            }
            ContextMenuAction::MigrateTables => {
                self.migrate_selected_tables(&item_id, cx);
            }
            ContextMenuAction::Duplicate => {
                self.duplicate_profile(&item_id, cx);
            }
            ContextMenuAction::Delete => {
                if !self.try_dispatch_batch_delete(&item_id, cx) {
                    self.show_delete_confirm_modal(&item_id, cx);
                }
            }
            ContextMenuAction::OpenDatabase => {
                self.execute_item(&item_id, cx);
            }
            ContextMenuAction::CloseDatabase => {
                self.close_database(&item_id, cx);
            }
            ContextMenuAction::NewFolder => {
                self.create_folder_from_context(&item_id, cx);
            }
            ContextMenuAction::NewConnection => {
                self.create_connection_in_folder(&item_id, cx);
            }
            ContextMenuAction::RenameFolder => {
                self.pending_rename_item = Some(item_id.clone());
            }
            ContextMenuAction::DeleteFolder => {
                if !self.try_dispatch_batch_delete(&item_id, cx) {
                    self.show_delete_confirm_modal(&item_id, cx);
                }
            }
            ContextMenuAction::MoveToFolder(target_folder_id) => {
                self.move_item_to_folder(&item_id, target_folder_id, cx);
            }
            ContextMenuAction::GenerateIndexSql(action) => {
                self.generate_index_sql(&item_id, action, cx);
            }
            ContextMenuAction::GenerateForeignKeySql(action) => {
                self.generate_foreign_key_sql(&item_id, action, cx);
            }
            ContextMenuAction::GenerateTypeSql(action) => {
                self.generate_type_sql(&item_id, action, cx);
            }
            ContextMenuAction::GenerateCollectionCode(kind) => {
                self.generate_collection_code(&item_id, kind, cx);
            }
            ContextMenuAction::QueryCollection => {
                self.query_collection(&item_id, cx);
            }
            ContextMenuAction::OpenConsole => {
                if let Some(SchemaNodeId::Database { profile_id, name }) = parse_node_id(&item_id) {
                    cx.emit(SidebarEvent::OpenConsole {
                        profile_id,
                        database: name,
                    });
                }
            }
            ContextMenuAction::NewQueryForDatabase => {
                self.new_query_for_database(&item_id, cx);
            }
            ContextMenuAction::OpenScript => {
                self.execute_item(&item_id, cx);
            }
            ContextMenuAction::RenameScript => {
                self.pending_rename_item = Some(item_id.clone());
            }
            ContextMenuAction::DeleteScript => {
                if !self.try_dispatch_batch_delete(&item_id, cx) {
                    self.show_delete_confirm_modal(&item_id, cx);
                }
            }
            ContextMenuAction::NewScriptFile => {
                let parent = Self::parent_dir_from_item_id(&item_id);
                self.create_script_file_in(parent, cx);
            }
            ContextMenuAction::NewScriptFolder => {
                let parent = Self::parent_dir_from_item_id(&item_id);
                self.create_script_folder_in(parent, cx);
            }
            ContextMenuAction::RevealInFileManager => {
                self.reveal_in_file_manager(&item_id, cx);
            }
            ContextMenuAction::CopyPath => {
                self.copy_path_to_clipboard(&item_id, cx);
            }
            ContextMenuAction::RefreshScripts => {
                self.rescan_scripts(cx);
            }
            ContextMenuAction::RemoveExternalScriptsFolder => {
                self.remove_external_scripts_folder(&item_id, cx);
            }
            ContextMenuAction::RefreshDatabase => {
                self.refresh_schema_database(&item_id, cx);
            }
            ContextMenuAction::RefreshObject => {
                self.refresh_schema_object(&item_id, cx);
            }
            ContextMenuAction::DropDatabase => {
                self.show_ddl_confirm_modal(&item_id, "Database", cx);
            }
            ContextMenuAction::DropTable => {
                let node_kind = parse_node_kind(&item_id);
                let object_type = if node_kind == SchemaNodeKind::View {
                    "View"
                } else {
                    "Table"
                };
                self.show_ddl_confirm_modal(&item_id, object_type, cx);
            }
            ContextMenuAction::DropCollection => {
                self.show_ddl_confirm_modal(&item_id, "Collection", cx);
            }
            ContextMenuAction::NewDashboard => {
                if let Some(SchemaNodeId::DashboardsFolder { profile_id }) = parse_node_id(&item_id)
                {
                    cx.emit(SidebarEvent::RequestCreateDashboard { profile_id });
                }
            }
            ContextMenuAction::ImportDashboard => {
                if let Some(SchemaNodeId::DashboardsFolder { profile_id }) = parse_node_id(&item_id)
                {
                    cx.emit(SidebarEvent::RequestImportDashboard { profile_id });
                }
            }
            ContextMenuAction::RefreshRemoteDashboards => {
                if let Some(SchemaNodeId::RemoteDashboardsFolder { profile_id }) =
                    parse_node_id(&item_id)
                {
                    // Drop the cached listing and re-fetch so the next render
                    // shows the current upstream set.
                    self.app_state
                        .read(cx)
                        .remote_dashboard_cache()
                        .invalidate(profile_id);
                    self.spawn_fetch_remote_dashboards(profile_id, cx);
                    self.rebuild_tree_with_overrides(cx);
                }
            }
            ContextMenuAction::RenameDashboard => {
                if let Some(SchemaNodeId::DashboardItem { dashboard_id, .. }) =
                    parse_node_id(&item_id)
                {
                    cx.emit(SidebarEvent::RequestRenameDashboard { dashboard_id });
                }
            }
            ContextMenuAction::DeleteDashboard => {
                if let Some(SchemaNodeId::DashboardItem { dashboard_id, .. }) =
                    parse_node_id(&item_id)
                {
                    cx.emit(SidebarEvent::RequestDeleteDashboard { dashboard_id });
                }
            }
            ContextMenuAction::DuplicateDashboard => {
                if let Some(SchemaNodeId::DashboardItem { dashboard_id, .. }) =
                    parse_node_id(&item_id)
                {
                    cx.emit(SidebarEvent::RequestDuplicateDashboard { dashboard_id });
                }
            }
            ContextMenuAction::RenameSavedChart => {
                if let Some(SchemaNodeId::SavedChartItem { chart_id, .. }) = parse_node_id(&item_id)
                {
                    cx.emit(SidebarEvent::RequestRenameSavedChart { chart_id });
                }
            }
            ContextMenuAction::DeleteSavedChart => {
                if let Some(SchemaNodeId::SavedChartItem { chart_id, .. }) = parse_node_id(&item_id)
                {
                    cx.emit(SidebarEvent::RequestDeleteSavedChart { chart_id });
                }
            }
            ContextMenuAction::DuplicateSavedChart => {
                if let Some(SchemaNodeId::SavedChartItem { chart_id, .. }) = parse_node_id(&item_id)
                {
                    cx.emit(SidebarEvent::RequestDuplicateSavedChart { chart_id });
                }
            }
            ContextMenuAction::RefreshInstanceCatalog => {
                if let Some(profile_id) = parse_node_id(&item_id).and_then(|n| n.profile_id()) {
                    self.clear_instance_catalog_cache(profile_id);
                    self.spawn_fetch_instance_catalog(profile_id, cx);
                    self.rebuild_tree_with_overrides(cx);
                }
            }
            ContextMenuAction::CopyItemId => {
                if let Some(node_id) = parse_node_id(&item_id) {
                    let id_str = match &node_id {
                        SchemaNodeId::InstanceMetricLeaf { metric_id, .. } => metric_id.clone(),
                        SchemaNodeId::InstanceInspectorLeaf { metric_id, .. } => metric_id.clone(),
                        other => other.to_string(),
                    };
                    cx.write_to_clipboard(ClipboardItem::new_string(id_str));
                }
            }
            ContextMenuAction::CopyName => {
                if let Some(
                    SchemaNodeId::Column { name, .. }
                    | SchemaNodeId::CollectionField { name, .. }
                    | SchemaNodeId::Table { name, .. }
                    | SchemaNodeId::View { name, .. }
                    | SchemaNodeId::Collection { name, .. },
                ) = parse_node_id(&item_id)
                {
                    cx.write_to_clipboard(ClipboardItem::new_string(name));
                }
            }
            ContextMenuAction::CompareSchema => {
                self.open_schema_diff_from_context(&item_id, cx);
            }
            ContextMenuAction::RootSectionAction(action_id) => {
                self.request_root_section_action(&item_id, &action_id, cx);
            }
        }
    }

    /// Left or Escape inside the menu: closes the deepest open level only.
    /// Returns `false` at the root, so the caller dismisses the whole menu.
    pub fn context_menu_go_back(&mut self, cx: &mut Context<Self>) -> bool {
        let Some(menu) = self.context_menu.as_mut() else {
            return false;
        };

        match menu.nav.back() {
            BackAction::ClosedLevel => {
                cx.notify();
                true
            }
            BackAction::Dismissed => false,
        }
    }

    /// One open-on-hover decision, once the rest delay has passed: opens the
    /// flyout of the row the pointer rested on (see
    /// [`Self::arm_context_menu_hover_open`]).
    fn context_menu_hover_tick(&mut self, cx: &mut Context<Self>) {
        if let Some(menu) = self.context_menu.as_mut()
            && menu.hover_tick(Instant::now())
        {
            cx.notify();
        }
    }

    /// Schedules the open-on-hover decision for a row the pointer has
    /// entered: after the rest delay, one tick applies it. The task is
    /// fire-and-forget: a tick before the row's deadline decides nothing, and
    /// a tick after the menu closed finds no menu.
    fn arm_context_menu_hover_open(&mut self, cx: &mut Context<Self>) {
        cx.spawn(async move |this, cx| {
            cx.background_executor().timer(HOVER_OPEN_DELAY).await;
            this.update(cx, |sidebar, cx| sidebar.context_menu_hover_tick(cx))
                .log_if_dropped();
        })
        .detach();
    }

    /// The menu row `path` points at, following submenu rows down from the
    /// root.
    fn menu_item_at_path<'a>(
        items: &'a [ContextMenuItem],
        path: &[usize],
    ) -> Option<&'a ContextMenuItem> {
        let (last, parents) = path.split_last()?;

        let mut level_items = items;

        for index in parents {
            let ContextMenuAction::Submenu(children) = &level_items.get(*index)?.action else {
                return None;
            };

            level_items = children;
        }

        level_items.get(*last)
    }

    pub fn close_context_menu(&mut self, cx: &mut Context<Self>) {
        if self.context_menu.is_some() {
            self.context_menu = None;
            cx.notify();
        }
    }

    pub fn has_context_menu_open(&self) -> bool {
        self.context_menu.is_some()
    }

    pub fn context_menu_state(&self) -> Option<&ContextMenuState> {
        self.context_menu.as_ref()
    }

    /// Returns an approximate position for the context menu based on the selected item.
    /// Used for keyboard-triggered menu opening (m key).
    pub fn selected_item_menu_position(&self, cx: &App) -> Point<Pixels> {
        let header_height = px(40.0);
        let row_height = px(28.0);
        let menu_x = px(180.0);

        let index = self
            .active_tree_state()
            .read(cx)
            .selected_index()
            .unwrap_or(0);
        let y = header_height + (row_height * (index as f32));

        Point::new(menu_x, y)
    }

    /// Returns true if the profile supports FK metadata for schema visualization.
    /// Checks that the driver is Relational and has FOREIGN_KEYS capability.
    pub(super) fn is_relational_with_fk_support(&self, item_id: &str, cx: &App) -> bool {
        let Some(profile_id) = Self::extract_profile_id_from_item(item_id) else {
            return false;
        };
        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&profile_id) else {
            return false;
        };
        let metadata = conn.connection.metadata();
        metadata.category == DatabaseCategory::Relational
            && metadata
                .capabilities
                .contains(DriverCapabilities::FOREIGN_KEYS)
    }
}

/// The rows of the deepest open level of `nodes`, following the cursor path
/// of `nav` down from the root.
fn deepest_level_rows<'a>(nodes: &'a [MenuNode], nav: &MenuNav) -> &'a [MenuNode] {
    let Some((_, parents)) = nav.cursor().path().split_last() else {
        return nodes;
    };

    let mut rows = nodes;

    for index in parents {
        match rows.get(*index) {
            Some(node) => rows = &node.children,
            None => return &[],
        }
    }

    rows
}

/// Whether the collection declares the time-series facet, the facet the
/// measurement menu actions apply to.
fn collection_has_time_series_facet(
    connected: &ConnectedProfile,
    database: &str,
    name: &str,
) -> bool {
    connected
        .connection
        .collection_facets(&CollectionRef::new(database, name))
        .iter()
        .any(|facet| matches!(facet, dbflux_core::ObjectFacet::TimeSeries(_)))
}

/// Whether the connection's databases list their objects in a measurement
/// folder.
fn lists_measurements(connected: &ConnectedProfile) -> bool {
    connected.schema.as_ref().is_some_and(|schema| {
        schema.database_layout()
            == dbflux_core::DatabaseLayout::Folder(dbflux_core::ObjectFolder::Measurements)
    })
}

#[cfg(test)]
mod time_series_tests;

#[cfg(test)]
mod hover_tests {
    use super::{ChildAction, ContextMenuAction, ContextMenuItem, ContextMenuState, MenuNav};
    use dbflux_components::composites::{HOVER_OPEN_DELAY, HoverIntent};
    use gpui::{point, px};
    use std::time::{Duration, Instant};

    /// One fixed origin for every instant a test uses, so a 1 ms margin is a
    /// margin and not a race against the wall clock.
    fn at(millis: u64) -> Instant {
        static BASE: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

        *BASE.get_or_init(Instant::now) + Duration::from_millis(millis)
    }

    fn open_delay() -> u64 {
        HOVER_OPEN_DELAY.as_millis() as u64
    }

    fn item(label: &str) -> ContextMenuItem {
        ContextMenuItem::item(label, ContextMenuAction::Open)
    }

    fn submenu_item(label: &str, children: Vec<ContextMenuItem>) -> ContextMenuItem {
        ContextMenuItem::item(label, ContextMenuAction::Submenu(children))
    }

    /// Top-level menu with two submenu rows and one plain row: Export Table
    /// with two children, Migrate Table with two, and Drop Table.
    fn top_items() -> Vec<ContextMenuItem> {
        vec![
            submenu_item("Export Table", vec![item("as CSV"), item("as JSON")]),
            submenu_item("Migrate Table", vec![item("to Postgres"), item("to MySQL")]),
            item("Drop Table"),
        ]
    }

    /// The menu open on its root panel, the way `open_menu_for_item` leaves
    /// it: highlighted on its first selectable row.
    fn state_at_root() -> ContextMenuState {
        let mut state = ContextMenuState {
            item_id: "table".to_string(),
            items: top_items(),
            nav: MenuNav::new(),
            hover: HoverIntent::new(),
            position: point(px(0.0), px(0.0)),
        };
        state
            .nav
            .reset(&ContextMenuItem::to_menu_nodes(&state.items));

        state
    }

    /// The same menu with Export Table's flyout open, the way a click on that
    /// row leaves it.
    fn state_in_first_flyout() -> ContextMenuState {
        let mut state = state_at_root();
        let nodes = ContextMenuItem::to_menu_nodes(&state.items);
        assert_eq!(state.nav.open_child(&nodes), ChildAction::Opened);
        state.hover.mark_open(0, Some(0));

        state
    }

    #[test]
    fn hovering_the_owning_row_keeps_the_flyout_fixed() {
        let mut state = state_in_first_flyout();

        let changed = state.hover_row(0, 0, at(0));

        assert!(!changed);
        // The flyout stays open on its own rows.
        assert_eq!(state.nav.cursor().depth(), 2);
        assert_eq!(state.nav.cursor().highlight(1), Some(0));
        // Resting on the owning row re-opens nothing.
        assert!(!state.hover_tick(at(open_delay())));
        assert_eq!(state.nav.cursor().depth(), 2);
    }

    #[test]
    fn hovering_another_submenu_row_switches_the_open_flyout() {
        let mut state = state_in_first_flyout();

        assert!(state.hover_row(0, 1, at(0)));
        // The old flyout closed the moment the highlight moved off its row.
        assert_eq!(state.nav.cursor().depth(), 1);
        assert_eq!(state.nav.cursor().highlight(0), Some(1));

        // The new flyout opens once the pointer has rested on its row, on the
        // new owner, anchored on its first row.
        assert!(state.hover_tick(at(open_delay())));
        assert_eq!(state.nav.cursor().highlight(0), Some(1));
        assert_eq!(state.nav.cursor().depth(), 2);
        assert_eq!(state.nav.cursor().highlight(1), Some(0));
    }

    #[test]
    fn hovering_a_plain_row_collapses_the_flyout() {
        let mut state = state_in_first_flyout();

        assert!(state.hover_row(0, 2, at(0)));
        assert_eq!(state.nav.cursor().depth(), 1);
        assert_eq!(state.nav.cursor().highlight(0), Some(2));
        // Resting on the plain row opens nothing.
        assert!(!state.hover_tick(at(open_delay())));
        assert_eq!(state.nav.cursor().depth(), 1);
    }

    #[test]
    fn a_flyout_opens_only_after_the_pointer_rests_on_its_row() {
        let mut state = state_at_root();

        assert!(state.hover_row(0, 1, at(0)));
        // Sweeping onto the row is not resting on it: nothing opens yet.
        assert!(!state.hover_tick(at(open_delay() - 1)));
        assert_eq!(state.nav.cursor().depth(), 1);

        assert!(state.hover_tick(at(open_delay())));
        assert_eq!(state.nav.cursor().depth(), 2);
        assert_eq!(state.nav.cursor().highlight(0), Some(1));
        assert_eq!(state.nav.cursor().highlight(1), Some(0));
    }

    #[test]
    fn sweeping_across_rows_without_resting_opens_no_flyout() {
        let mut state = state_at_root();

        state.hover_row(0, 0, at(0));
        state.hover_row(0, 1, at(40));
        state.hover_row(0, 2, at(80));

        // No row was rested on, and the last one is plain.
        assert!(!state.hover_tick(at(80 + open_delay())));
        assert_eq!(state.nav.cursor().depth(), 1);
        assert_eq!(state.nav.cursor().highlight(0), Some(2));
    }
}

#[cfg(test)]
mod menu_i18n_tests {
    const B1_KEYS: [&str; 14] = [
        "sidebar.menu.open",
        "sidebar.menu.browse_event_streams",
        "sidebar.menu.view_schema",
        "sidebar.menu.refresh",
        "sidebar.menu.generate_sql",
        "sidebar.menu.export_table",
        "sidebar.menu.export_tables_many",
        "sidebar.menu.migrate_table",
        "sidebar.menu.migrate_tables_many",
        "sidebar.menu.drop_view",
        "sidebar.menu.drop_table",
        "sidebar.menu.query_measurement",
        "sidebar.menu.generate_query",
        "sidebar.menu.drop_collection",
    ];

    #[test]
    fn menu_b1_keys_resolve_in_both_locales() {
        for key in B1_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn menu_open_differs_between_locales() {
        let english = dbflux_i18n::t!("sidebar.menu.open", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.menu.open", locale = "es");

        assert_eq!(english, "Open");
        assert_eq!(spanish, "Abrir");
        assert_ne!(english, spanish);
    }

    const B2_KEYS: [&str; 22] = [
        "sidebar.menu.connect",
        "sidebar.menu.disconnect",
        "sidebar.menu.edit",
        "sidebar.menu.duplicate",
        "sidebar.menu.rename",
        "sidebar.menu.export_ellipsis",
        "sidebar.menu.import_ellipsis",
        "sidebar.menu.compare_schema",
        "sidebar.menu.move_to",
        "sidebar.menu.delete",
        "sidebar.menu.delete_count",
        "sidebar.menu.close",
        "sidebar.menu.new_query",
        "sidebar.menu.drop_database",
        "sidebar.menu.new_connection",
        "sidebar.menu.new_folder",
        "sidebar.menu.root",
        "sidebar.menu.new_script_file",
        "sidebar.menu.new_script_folder",
        "sidebar.menu.reveal_file_manager",
        "sidebar.menu.copy_path",
        "sidebar.menu.schema_diff_unsupported",
    ];

    #[test]
    fn menu_b2_keys_resolve_in_both_locales() {
        for key in B2_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn menu_connect_differs_between_locales() {
        let english = dbflux_i18n::t!("sidebar.menu.connect", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.menu.connect", locale = "es");

        assert_eq!(english, "Connect");
        assert_eq!(spanish, "Conectar");
        assert_ne!(english, spanish);
    }

    const B3_KEYS: [&str; 11] = [
        "sidebar.menu.new_dashboard",
        "sidebar.menu.import_dashboard",
        "sidebar.menu.rename_ellipsis",
        "sidebar.menu.delete",
        "sidebar.menu.copy_metric_id",
        "sidebar.menu.copy_inspector_id",
        "sidebar.menu.copy_column_name",
        "sidebar.menu.copy_field_name",
        "sidebar.menu.copy_table_name",
        "sidebar.menu.copy_view_name",
        "sidebar.menu.copy_collection_name",
    ];

    #[test]
    fn menu_b3_keys_resolve_in_both_locales() {
        for key in B3_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn menu_new_dashboard_differs_between_locales() {
        let english = dbflux_i18n::t!("sidebar.menu.new_dashboard", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.menu.new_dashboard", locale = "es");

        assert_eq!(english, "New dashboard…");
        assert_eq!(spanish, "Nuevo dashboard…");
        assert_ne!(english, spanish);
    }
}

#[cfg(test)]
mod menu_availability_tests {
    use super::{node_has_context_menu, node_kind_has_context_menu};
    use crate::table_loading::object_tree_adapter_tests::{
        AdapterFakeConnection, connect_profile, register_per_database_driver, snapshot_naming,
        test_app_state,
    };
    use crate::{ContextMenuAction, Sidebar, SidebarEvent};
    use dbflux_core::CodeGenCapabilities;
    use dbflux_core::{SchemaNodeId, SchemaNodeKind};
    use dbflux_ui_base::app_state_entity::AppStateEntity;
    use gpui::{
        AppContext as _, Bounds, Context, Entity, IntoElement, Modifiers, MouseButton, Pixels,
        Render, TestAppContext, VisualTestContext, Window, div, point, px,
    };
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    /// Every `SchemaNodeKind`, in declaration order.
    const ALL_KINDS: [SchemaNodeKind; 65] = [
        SchemaNodeKind::ConnectionFolder,
        SchemaNodeKind::Profile,
        SchemaNodeKind::DatabasesFolder,
        SchemaNodeKind::Database,
        SchemaNodeKind::EmptyDatabasesFolder,
        SchemaNodeKind::Loading,
        SchemaNodeKind::Schema,
        SchemaNodeKind::TablesFolder,
        SchemaNodeKind::ViewsFolder,
        SchemaNodeKind::TypesFolder,
        SchemaNodeKind::TypesLoadingFolder,
        SchemaNodeKind::SchemaIndexesFolder,
        SchemaNodeKind::SchemaIndexesLoadingFolder,
        SchemaNodeKind::SchemaForeignKeysFolder,
        SchemaNodeKind::SchemaForeignKeysLoadingFolder,
        SchemaNodeKind::RoutinesFolder,
        SchemaNodeKind::RoutinesLoadingFolder,
        SchemaNodeKind::CollectionsFolder,
        SchemaNodeKind::MetricsFolder,
        SchemaNodeKind::MetricNamespaceFolder,
        SchemaNodeKind::MetricLeaf,
        SchemaNodeKind::DashboardsFolder,
        SchemaNodeKind::DashboardItem,
        SchemaNodeKind::RemoteDashboardsFolder,
        SchemaNodeKind::RemoteDashboardItem,
        SchemaNodeKind::SavedChartsFolder,
        SchemaNodeKind::SavedChartItem,
        SchemaNodeKind::Table,
        SchemaNodeKind::View,
        SchemaNodeKind::Collection,
        SchemaNodeKind::CollectionChild,
        SchemaNodeKind::CollectionChildrenMore,
        SchemaNodeKind::CustomType,
        SchemaNodeKind::ColumnsFolder,
        SchemaNodeKind::IndexesFolder,
        SchemaNodeKind::ForeignKeysFolder,
        SchemaNodeKind::ConstraintsFolder,
        SchemaNodeKind::StorageHintsFolder,
        SchemaNodeKind::Column,
        SchemaNodeKind::Index,
        SchemaNodeKind::ForeignKey,
        SchemaNodeKind::Constraint,
        SchemaNodeKind::StorageHintItem,
        SchemaNodeKind::SchemaIndex,
        SchemaNodeKind::SchemaForeignKey,
        SchemaNodeKind::Routine,
        SchemaNodeKind::DatabaseIndexesFolder,
        SchemaNodeKind::CollectionFieldsFolder,
        SchemaNodeKind::CollectionField,
        SchemaNodeKind::CollectionIndexesFolder,
        SchemaNodeKind::CollectionIndex,
        SchemaNodeKind::EnumValue,
        SchemaNodeKind::BaseType,
        SchemaNodeKind::Placeholder,
        SchemaNodeKind::DependentsFolder,
        SchemaNodeKind::DependentItem,
        SchemaNodeKind::ScriptsFolder,
        SchemaNodeKind::ScriptFile,
        SchemaNodeKind::ScriptsRoot,
        SchemaNodeKind::InstanceMetricsFolder,
        SchemaNodeKind::InstanceMetricLeaf,
        SchemaNodeKind::InstanceInspectorsFolder,
        SchemaNodeKind::InstanceInspectorLeaf,
        SchemaNodeKind::InstanceOverviewLeaf,
        SchemaNodeKind::InstanceFolder,
    ];

    /// Kinds whose rows open a context menu.
    const KINDS_WITH_MENU: [SchemaNodeKind; 27] = [
        SchemaNodeKind::ConnectionFolder,
        SchemaNodeKind::Profile,
        SchemaNodeKind::DatabasesFolder,
        SchemaNodeKind::Database,
        SchemaNodeKind::Table,
        SchemaNodeKind::View,
        SchemaNodeKind::Collection,
        SchemaNodeKind::CustomType,
        SchemaNodeKind::Index,
        SchemaNodeKind::SchemaIndex,
        SchemaNodeKind::ForeignKey,
        SchemaNodeKind::SchemaForeignKey,
        SchemaNodeKind::ScriptsFolder,
        SchemaNodeKind::ScriptFile,
        SchemaNodeKind::ScriptsRoot,
        SchemaNodeKind::DashboardsFolder,
        SchemaNodeKind::DashboardItem,
        SchemaNodeKind::RemoteDashboardsFolder,
        SchemaNodeKind::SavedChartItem,
        SchemaNodeKind::InstanceMetricsFolder,
        SchemaNodeKind::InstanceMetricLeaf,
        SchemaNodeKind::InstanceInspectorsFolder,
        SchemaNodeKind::InstanceInspectorLeaf,
        SchemaNodeKind::InstanceOverviewLeaf,
        SchemaNodeKind::RootSectionItem,
        SchemaNodeKind::Column,
        SchemaNodeKind::CollectionField,
    ];

    #[test]
    fn every_node_kind_declares_whether_it_has_a_menu() {
        let mut all_kinds = ALL_KINDS.to_vec();
        all_kinds.push(SchemaNodeKind::RootSection);
        all_kinds.push(SchemaNodeKind::RootSectionItem);
        all_kinds.push(SchemaNodeKind::RootSectionLoadMore);
        all_kinds.push(SchemaNodeKind::SecretStore);
        all_kinds.push(SchemaNodeKind::SecretPath);

        let discriminants: Vec<usize> = all_kinds.iter().map(|kind| *kind as usize).collect();
        let expected: Vec<usize> = (0..=SchemaNodeKind::SecretPath as usize).collect();
        assert_eq!(
            discriminants, expected,
            "ALL_KINDS must list every SchemaNodeKind variant in declaration order"
        );

        for kind in all_kinds {
            assert_eq!(
                node_kind_has_context_menu(kind),
                KINDS_WITH_MENU.contains(&kind),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn generated_sql_menus_need_a_capability_that_fills_them() {
        let generated_sql_kinds = [
            (
                SchemaNodeKind::Index,
                CodeGenCapabilities::REINDEX,
                CodeGenCapabilities::DROP_FOREIGN_KEY,
            ),
            (
                SchemaNodeKind::SchemaIndex,
                CodeGenCapabilities::CREATE_INDEX,
                CodeGenCapabilities::DROP_TYPE,
            ),
            (
                SchemaNodeKind::ForeignKey,
                CodeGenCapabilities::DROP_FOREIGN_KEY,
                CodeGenCapabilities::DROP_INDEX,
            ),
            (
                SchemaNodeKind::SchemaForeignKey,
                CodeGenCapabilities::ADD_FOREIGN_KEY,
                CodeGenCapabilities::CREATE_TYPE,
            ),
            (
                SchemaNodeKind::CustomType,
                CodeGenCapabilities::DROP_TYPE,
                CodeGenCapabilities::REINDEX,
            ),
        ];

        for (kind, filling, unrelated) in generated_sql_kinds {
            assert!(
                !node_has_context_menu(kind, CodeGenCapabilities::empty()),
                "{kind:?} without code generation"
            );
            assert!(
                !node_has_context_menu(kind, unrelated),
                "{kind:?} with {unrelated:?}"
            );
            assert!(
                node_has_context_menu(kind, filling),
                "{kind:?} with {filling:?}"
            );
        }

        for kind in KINDS_WITH_MENU.into_iter().filter(|kind| {
            !generated_sql_kinds
                .iter()
                .any(|(generated_sql_kind, _, _)| generated_sql_kind == kind)
        }) {
            assert!(
                node_has_context_menu(kind, CodeGenCapabilities::empty()),
                "{kind:?} does not depend on code generation"
            );
        }
    }

    #[gpui::test]
    async fn saved_charts_folder_offers_no_menu(cx: &mut TestAppContext) {
        let (state, profile_id) = connected_profile(cx);
        let window = cx.add_window(|window, cx| Sidebar::new(state.clone(), window, cx));
        let folder_item = SchemaNodeId::SavedChartsFolder { profile_id }.to_string();

        assert!(!node_kind_has_context_menu(
            SchemaNodeKind::SavedChartsFolder
        ));
        window
            .update(cx, |sidebar, _, cx| {
                let items = sidebar.build_context_menu_items(
                    SchemaNodeKind::SavedChartsFolder,
                    &folder_item,
                    cx,
                );
                assert!(
                    items.is_empty(),
                    "saved charts folder must not list entries"
                );

                sidebar.open_menu_for_item(&folder_item, point(px(0.0), px(0.0)), cx);
                assert!(!sidebar.has_context_menu_open());
            })
            .expect("sidebar alive");
    }

    /// Flattens the sidebar tree into its item ids, depth-first.
    fn tree_rows(sidebar: &Entity<Sidebar>, cx: &mut VisualTestContext) -> Vec<String> {
        fn walk(items: &[gpui_component::tree::TreeItem], rows: &mut Vec<String>) {
            for item in items {
                rows.push(item.id.to_string());
                walk(&item.children, rows);
            }
        }

        sidebar.update(cx, |sidebar, cx| {
            let mut rows = Vec::new();
            walk(&sidebar.build_tree_items_with_overrides(cx), &mut rows);
            rows
        })
    }

    /// A row is named by its item, not by what it draws: while the inline
    /// editor stands in for the label, the row would otherwise be named by the
    /// menu glyph on its own.
    #[gpui::test]
    async fn a_row_keeps_its_name_while_it_is_renamed(cx: &mut TestAppContext) {
        use dbflux_components::accessibility::{FrameCapture, accessible_name};

        let (state, profile_id) = connected_profile(cx);
        let (sidebar, cx) =
            cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));

        let profile_item = tree_rows(&sidebar, cx)
            .into_iter()
            .find(|item| crate::parse_node_kind(item) == SchemaNodeKind::Profile)
            .expect("the profile row");
        sidebar.update(cx, |sidebar, cx| {
            sidebar.set_expanded(&profile_item, true, cx)
        });
        cx.run_until_parked();

        let profile_name = cx.update(|_, cx| {
            state
                .read(cx)
                .profiles()
                .iter()
                .find(|profile| profile.id == profile_id)
                .map(|profile| profile.name.clone())
                .expect("the connected profile")
        });

        let capture = FrameCapture::observe(cx);
        let frame = capture.frame(cx);
        assert_eq!(
            accessible_name(&frame, &format!("row-{profile_item}")),
            Some(profile_name.clone()),
            "the row is named by its item"
        );

        sidebar.update(cx, |sidebar, cx| {
            sidebar.editing_id = Some(profile_id);
            cx.notify();
        });
        let frame = capture.frame(cx);

        assert_eq!(
            accessible_name(&frame, &format!("row-{profile_item}")),
            Some(profile_name),
            "the inline editor does not take the name away"
        );
    }

    fn expand_first_of_kind(
        sidebar: &Entity<Sidebar>,
        kind: SchemaNodeKind,
        cx: &mut VisualTestContext,
    ) -> String {
        let item_id = tree_rows(sidebar, cx)
            .into_iter()
            .find(|id| crate::parse_node_kind(id) == kind)
            .unwrap_or_else(|| panic!("no {kind:?} row in the tree"));
        sidebar.update(cx, |sidebar, cx| sidebar.set_expanded(&item_id, true, cx));
        cx.run_until_parked();
        item_id
    }

    /// The fake driver generates no SQL, so an index row has nothing to put
    /// in its menu: no row button, and right click opens nothing.
    #[gpui::test]
    async fn index_row_without_code_generation_offers_no_menu(cx: &mut TestAppContext) {
        let (state, profile_id) = connected_profile(cx);
        let users = dbflux_core::TableInfo {
            name: "users".into(),
            schema: Some("main".into()),
            columns: Some(Vec::new()),
            indexes: Some(dbflux_core::IndexData::Relational(vec![
                dbflux_core::IndexInfo {
                    name: "users_pkey".into(),
                    columns: vec!["id".into()],
                    is_unique: true,
                    is_primary: true,
                },
            ])),
            foreign_keys: Some(Vec::new()),
            constraints: Some(Vec::new()),
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::DataGrid,
            child_items: None,
            storage_hints: None,
            pseudo_columns: Box::default(),
        };
        state.update(cx, |state, _| {
            state.set_database_schema(
                profile_id,
                "main".into(),
                dbflux_core::DbSchemaInfo {
                    name: "main".into(),
                    tables: vec![users],
                    views: Vec::new(),
                    custom_types: None,
                },
            );
        });

        let (sidebar, cx) =
            cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));
        let profile_item = SchemaNodeId::Profile { profile_id }.to_string();
        sidebar.update(cx, |sidebar, cx| {
            sidebar.set_expanded(&profile_item, true, cx)
        });
        cx.run_until_parked();

        expand_first_of_kind(&sidebar, SchemaNodeKind::Database, cx);
        let table_item = expand_first_of_kind(&sidebar, SchemaNodeKind::Table, cx);
        expand_first_of_kind(&sidebar, SchemaNodeKind::IndexesFolder, cx);
        let index_item = tree_rows(&sidebar, cx)
            .into_iter()
            .find(|id| crate::parse_node_kind(id) == SchemaNodeKind::Index)
            .expect("index row in the tree");

        rendered_bounds(cx, format!("menu-btn-{table_item}"));
        let index_row = rendered_bounds(cx, format!("row-{index_item}"));
        assert!(
            cx.debug_bounds(format!("menu-btn-{index_item}").leak())
                .is_none(),
            "an index row with no generated SQL must not show a menu button"
        );

        cx.simulate_mouse_down(center(index_row), MouseButton::Right, Modifiers::none());
        cx.simulate_mouse_up(center(index_row), MouseButton::Right, Modifiers::none());
        assert_eq!(open_menu_item_id(&sidebar, cx), None);

        sidebar.update(cx, |sidebar, cx| {
            sidebar.open_menu_for_item(&index_item, point(px(0.0), px(0.0)), cx);
            assert!(!sidebar.has_context_menu_open());
        });
    }

    struct EventRecorder;

    impl Render for EventRecorder {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
        }
    }

    /// Records the instance-leaf open events the sidebar emits, as
    /// `"<kind>:<id>"` strings.
    fn record_instance_open_events(
        sidebar: &Entity<Sidebar>,
        cx: &mut TestAppContext,
    ) -> (Entity<EventRecorder>, Arc<Mutex<Vec<String>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let recorder = cx.update(|cx| {
            cx.new(|cx| {
                let events = events.clone();
                cx.subscribe(sidebar, move |_, _, event: &SidebarEvent, _| {
                    let recorded = match event {
                        SidebarEvent::OpenInstanceMetric { metric_id, .. } => {
                            format!("metric:{metric_id}")
                        }
                        SidebarEvent::OpenInstanceInspector { metric_id, .. } => {
                            format!("inspector:{metric_id}")
                        }
                        SidebarEvent::OpenInstanceOverview { profile_id } => {
                            format!("overview:{profile_id}")
                        }
                        _ => return,
                    };
                    events.lock().expect("events").push(recorded);
                })
                .detach();
                EventRecorder
            })
        });
        (recorder, events)
    }

    fn connected_profile(cx: &mut TestAppContext) -> (Entity<AppStateEntity>, Uuid) {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let main = dbflux_core::DatabaseInfo {
            name: "main".into(),
            is_current: true,
        };

        let connection = AdapterFakeConnection::lazy();
        *connection.databases.lock().expect("fake databases") = vec![main.clone()];

        connect_profile(
            &state,
            cx,
            profile_id,
            connection,
            Some(snapshot_naming(vec![main])),
        );
        (state, profile_id)
    }

    /// Menus whose entries do not depend on driver capabilities are never
    /// empty for a kind the predicate allows, and a kind the predicate
    /// rejects never gets entries from the builder.
    #[gpui::test]
    async fn menu_builder_agrees_with_the_menu_predicate(cx: &mut TestAppContext) {
        let (state, profile_id) = connected_profile(cx);
        let window = cx.add_window(|window, cx| Sidebar::new(state.clone(), window, cx));

        let unconditional_menus = [
            SchemaNodeId::ConnectionFolder {
                node_id: Uuid::new_v4(),
            },
            SchemaNodeId::Profile { profile_id },
            SchemaNodeId::DatabasesFolder { profile_id },
            SchemaNodeId::Database {
                profile_id,
                name: "main".into(),
            },
            SchemaNodeId::Table {
                profile_id,
                database: Some("main".into()),
                schema: "public".into(),
                name: "users".into(),
            },
            SchemaNodeId::View {
                profile_id,
                database: Some("main".into()),
                schema: "public".into(),
                name: "active_users".into(),
            },
            SchemaNodeId::Collection {
                profile_id,
                database: "main".into(),
                name: "events".into(),
            },
            SchemaNodeId::ScriptsFolder { path: None },
            SchemaNodeId::ScriptFile {
                path: "/scripts/report.sql".into(),
            },
            SchemaNodeId::DashboardsFolder { profile_id },
            SchemaNodeId::DashboardItem {
                profile_id,
                dashboard_id: Uuid::new_v4(),
            },
            SchemaNodeId::RemoteDashboardsFolder { profile_id },
            SchemaNodeId::SavedChartItem {
                profile_id,
                chart_id: Uuid::new_v4(),
            },
            SchemaNodeId::InstanceMetricsFolder { profile_id },
            SchemaNodeId::InstanceMetricLeaf {
                profile_id,
                metric_id: "cpu".into(),
            },
            SchemaNodeId::InstanceInspectorsFolder { profile_id },
            SchemaNodeId::InstanceInspectorLeaf {
                profile_id,
                metric_id: "sessions".into(),
            },
            SchemaNodeId::InstanceOverviewLeaf { profile_id },
            SchemaNodeId::Column {
                profile_id,
                table: "users".into(),
                name: "email".into(),
            },
            SchemaNodeId::CollectionField {
                profile_id,
                collection: "events".into(),
                name: "payload".into(),
            },
        ];

        window
            .update(cx, |sidebar, _, cx| {
                for node_id in &unconditional_menus {
                    let kind = node_id.kind();
                    assert!(node_kind_has_context_menu(kind), "{kind:?}");

                    let items = sidebar.build_context_menu_items(kind, &node_id.to_string(), cx);
                    assert!(!items.is_empty(), "{kind:?} must build a menu");
                }

                for kind in ALL_KINDS
                    .into_iter()
                    .filter(|kind| !node_kind_has_context_menu(*kind))
                {
                    let items = sidebar.build_context_menu_items(kind, "unused", cx);
                    assert!(items.is_empty(), "{kind:?} must not build a menu");
                }
            })
            .expect("sidebar alive");
    }

    /// The column, collection field, table, view and collection menus copy
    /// the bare name, not the qualified one or the node id.
    #[gpui::test]
    async fn object_menus_copy_the_bare_name(cx: &mut TestAppContext) {
        let (state, profile_id) = connected_profile(cx);
        let window = cx.add_window(|window, cx| Sidebar::new(state.clone(), window, cx));

        let cases = [
            (
                SchemaNodeId::Column {
                    profile_id,
                    table: "users".into(),
                    name: "email".into(),
                },
                "Copy column name",
                "email",
            ),
            (
                SchemaNodeId::CollectionField {
                    profile_id,
                    collection: "events".into(),
                    name: "payload".into(),
                },
                "Copy field name",
                "payload",
            ),
            (
                SchemaNodeId::Table {
                    profile_id,
                    database: Some("app".into()),
                    schema: "public".into(),
                    name: "users".into(),
                },
                "Copy table name",
                "users",
            ),
            (
                SchemaNodeId::View {
                    profile_id,
                    database: None,
                    schema: "reporting".into(),
                    name: "active_users".into(),
                },
                "Copy view name",
                "active_users",
            ),
            (
                SchemaNodeId::Collection {
                    profile_id,
                    database: "app".into(),
                    name: "events".into(),
                },
                "Copy collection name",
                "events",
            ),
        ];

        for (node_id, label, expected) in cases {
            window
                .update(cx, |sidebar, _, cx| {
                    sidebar.open_menu_for_item(&node_id.to_string(), point(px(0.), px(0.)), cx);
                    let menu = sidebar.context_menu_state().expect("menu opens");
                    let index = menu
                        .items
                        .iter()
                        .position(|item| matches!(item.action, ContextMenuAction::CopyName))
                        .expect("menu offers the copy entry");
                    assert_eq!(menu.items[index].label.as_str(), label);
                    sidebar.context_menu_activated(&[index], cx);
                    assert!(!sidebar.has_context_menu_open());
                })
                .expect("sidebar alive");

            let copied = cx
                .read_from_clipboard()
                .and_then(|item| item.text())
                .expect("clipboard holds text");
            assert_eq!(copied, expected);
        }
    }

    fn center(bounds: Bounds<Pixels>) -> gpui::Point<Pixels> {
        point(
            bounds.origin.x + bounds.size.width / 2.0,
            bounds.origin.y + bounds.size.height / 2.0,
        )
    }

    fn rendered_bounds(cx: &mut VisualTestContext, selector: String) -> Bounds<Pixels> {
        let selector: &'static str = selector.leak();
        cx.update(|window, _| window.refresh());
        cx.run_until_parked();
        cx.debug_bounds(selector)
            .unwrap_or_else(|| panic!("{selector} was not rendered"))
    }

    fn open_menu_item_id(sidebar: &Entity<Sidebar>, cx: &mut VisualTestContext) -> Option<String> {
        sidebar.read_with(cx, |sidebar, _| {
            sidebar
                .context_menu_state()
                .map(|menu| menu.item_id.clone())
        })
    }

    /// The Databases folder was missing from the old right-click and row
    /// button lists. Right click, the row button and the keyboard menu
    /// command must all open its menu.
    #[gpui::test]
    async fn databases_folder_menu_opens_from_right_click_row_button_and_keyboard(
        cx: &mut TestAppContext,
    ) {
        let (state, profile_id) = connected_profile(cx);
        let (sidebar, cx) =
            cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));

        let profile_item = SchemaNodeId::Profile { profile_id }.to_string();
        let folder_item = SchemaNodeId::DatabasesFolder { profile_id }.to_string();
        sidebar.update(cx, |sidebar, cx| {
            sidebar.set_expanded(&profile_item, true, cx);
        });

        let row = rendered_bounds(cx, format!("row-{folder_item}"));
        cx.simulate_mouse_down(center(row), MouseButton::Right, Modifiers::none());
        cx.simulate_mouse_up(center(row), MouseButton::Right, Modifiers::none());
        assert_eq!(
            open_menu_item_id(&sidebar, cx),
            Some(folder_item.clone()),
            "right click must open the Databases folder menu"
        );

        sidebar.update(cx, |sidebar, cx| sidebar.close_context_menu(cx));
        let button = rendered_bounds(cx, format!("menu-btn-{folder_item}"));
        cx.simulate_click(center(button), Modifiers::none());
        assert_eq!(
            open_menu_item_id(&sidebar, cx),
            Some(folder_item.clone()),
            "the row button must open the Databases folder menu"
        );

        sidebar.update(cx, |sidebar, cx| {
            sidebar.close_context_menu(cx);

            let index = sidebar
                .find_item_index(&folder_item, cx)
                .expect("visible Databases folder");
            sidebar
                .tree_state
                .update(cx, |tree, cx| tree.set_selected_index(Some(index), cx));

            let position = sidebar.selected_item_menu_position(cx);
            sidebar.open_item_menu(position, cx);
        });
        assert_eq!(
            open_menu_item_id(&sidebar, cx),
            Some(folder_item),
            "the keyboard menu command must open the Databases folder menu"
        );
    }

    #[gpui::test]
    async fn instance_leaf_open_entry_opens_the_leaf(cx: &mut TestAppContext) {
        let (state, profile_id) = connected_profile(cx);
        let window = cx.add_window(|window, cx| Sidebar::new(state.clone(), window, cx));
        let sidebar = window.entity(cx).expect("sidebar entity");
        let (_recorder, events) = record_instance_open_events(&sidebar, cx);

        let leaves = [
            SchemaNodeId::InstanceMetricLeaf {
                profile_id,
                metric_id: "cpu".into(),
            },
            SchemaNodeId::InstanceInspectorLeaf {
                profile_id,
                metric_id: "sessions".into(),
            },
            SchemaNodeId::InstanceOverviewLeaf { profile_id },
        ];

        for leaf in &leaves {
            sidebar.update(cx, |sidebar, cx| {
                sidebar.open_menu_for_item(&leaf.to_string(), point(px(0.0), px(0.0)), cx);

                let open_index = sidebar
                    .context_menu_state()
                    .expect("leaf menu open")
                    .items
                    .iter()
                    .position(|item| matches!(item.action, ContextMenuAction::Open))
                    .expect("leaf menu has Open");
                sidebar.context_menu_activated(&[open_index], cx);
            });
        }

        assert_eq!(
            *events.lock().expect("events"),
            vec![
                "metric:cpu".to_string(),
                "inspector:sessions".to_string(),
                format!("overview:{profile_id}"),
            ]
        );
    }

    #[gpui::test]
    async fn failed_connect_marks_the_profile_until_a_retry_starts(cx: &mut TestAppContext) {
        let state = test_app_state(cx);
        let (connect_calls, fail_next_connect) = register_per_database_driver(&state, cx);

        let mut profile = dbflux_core::ConnectionProfile::new(
            "retry-test",
            dbflux_core::DbConfig::default_postgres(),
        );
        let profile_id = Uuid::new_v4();
        profile.id = profile_id;
        state.update(cx, |state, _| {
            state.profiles_mut().push(profile);
            state.connection_tree_mut().add_node(
                dbflux_core::ConnectionTreeNode::new_connection_ref(profile_id, None, 1000),
            );
        });

        let (sidebar, cx) =
            cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));
        let profile_item = SchemaNodeId::Profile { profile_id }.to_string();

        fail_next_connect.store(true, Ordering::SeqCst);
        sidebar.update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
        cx.run_until_parked();

        let failure = state.read_with(cx, |state, _| {
            state.connect_failure(profile_id).map(str::to_string)
        });
        assert!(
            failure
                .as_deref()
                .is_some_and(|error| error.contains("fake connect failure")),
            "failed connect must be recorded, got {failure:?}"
        );
        rendered_bounds(cx, format!("connect-error-{profile_id}"));

        let first_entry = sidebar.update(cx, |sidebar, cx| {
            sidebar.build_context_menu_items(SchemaNodeKind::Profile, &profile_item, cx)[0].clone()
        });
        assert_eq!(
            first_entry.label,
            dbflux_i18n::t!("sidebar.menu.retry_connect")
        );
        assert!(matches!(first_entry.action, ContextMenuAction::Connect));

        sidebar.update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
        assert!(
            !state.read_with(cx, |state, _| state.connect_failure(profile_id).is_some()),
            "starting a retry must clear the failure"
        );

        cx.run_until_parked();
        assert_eq!(connect_calls.load(Ordering::SeqCst), 2);
        state.read_with(cx, |state, _| {
            assert!(state.connections().contains_key(&profile_id));
            assert_eq!(state.connect_failure(profile_id), None);
        });
        cx.run_until_parked();
        assert!(
            cx.debug_bounds(format!("connect-error-{profile_id}").leak())
                .is_none(),
            "a connected profile must not show the error indicator"
        );
    }
}

#[cfg(test)]
mod menu_caption_tests {
    use super::menu_caption;
    use dbflux_core::SchemaNodeId;
    use uuid::Uuid;

    #[test]
    fn schema_object_menus_are_captioned_with_their_qualified_name() {
        let profile_id = Uuid::new_v4();

        let table = SchemaNodeId::Table {
            profile_id,
            database: None,
            schema: "public".to_string(),
            name: "orders".to_string(),
        };
        let collection = SchemaNodeId::Collection {
            profile_id,
            database: "shop".to_string(),
            name: "orders".to_string(),
        };
        let unqualified = SchemaNodeId::Table {
            profile_id,
            database: None,
            schema: String::new(),
            name: "orders".to_string(),
        };

        assert_eq!(
            menu_caption(&table.to_string()).as_deref(),
            Some("public.orders")
        );
        assert_eq!(
            menu_caption(&collection.to_string()).as_deref(),
            Some("shop.orders")
        );
        assert_eq!(
            menu_caption(&unqualified.to_string()).as_deref(),
            Some("orders")
        );
        assert_eq!(
            menu_caption(&SchemaNodeId::Profile { profile_id }.to_string()),
            None
        );
    }
}
