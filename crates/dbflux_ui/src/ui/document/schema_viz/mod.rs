use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::{Scrollable, ScrollableElement};

use dbflux_core::observability::actions as audit_actions;
use dbflux_core::observability::{
    AuditAction, EventCategory, EventOrigin, EventOutcome, EventRecord, EventSeverity,
};
use dbflux_core::{CancelToken, Connection, DbSchemaInfo, TableInfo, TaskKind, TaskTarget};
use dbflux_schema_viz::{
    graph::SchemaGraph,
    layout::{LayoutFormat, LayoutResult, NodeLayout},
};
use petgraph::graph::NodeIndex;
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

use crate::app::AppStateEntity;
use crate::keymap::ContextId;
use crate::ui::components::toast::{flush_pending_toast, PendingToast, ToastExt};
use crate::ui::document::handle::DocumentEvent;
use crate::ui::document::types::{DocumentId, DocumentState};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Spacing};

/// Direction for spatial selection navigation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    Left,
    Right,
    Up,
    Down,
}

/// Menu item for the schema viz context menu (top-level items).
#[derive(Debug, Clone)]
enum ContextMenuItem {
    ZoomIn,
    ZoomOut,
    Separator,
    LayoutMenu,
    CopyAsMenu,
    FocusOnTable,
}

/// Sub-menu that can be open inside the context menu.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubMenu {
    Layout,
    CopyAs,
}

impl SubMenu {
    fn items(&self) -> Vec<SubMenuItem> {
        match self {
            SubMenu::Layout => vec![
                SubMenuItem::LeftRight,
                SubMenuItem::Snowflake,
                SubMenuItem::Compact,
            ],
            SubMenu::CopyAs => vec![
                SubMenuItem::CopyAsDbml,
                SubMenuItem::CopyAsSql,
            ],
        }
    }
}

/// Item inside a sub-menu.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubMenuItem {
    LeftRight,
    Snowflake,
    Compact,
    CopyAsDbml,
    CopyAsSql,
}

impl SubMenuItem {
    fn label(&self) -> &'static str {
        match self {
            SubMenuItem::LeftRight => "Left-Right",
            SubMenuItem::Snowflake => "Snowflake",
            SubMenuItem::Compact => "Compact",
            SubMenuItem::CopyAsDbml => "Copy as DBML",
            SubMenuItem::CopyAsSql => "Copy as SQL",
        }
    }
}

// Node layout constants — shared between render_node and render_edges_overlay
// so that edge anchors always match rendered positions exactly.
//
// These must stay in sync with the padding/height values used in render_node.
const NODE_HEADER_PX: f32 = 30.0; // py(6)*2 + line-height(SM 12px ~18px) + border(1px) = ~31, round to 30
const NODE_BODY_TOP_PX: f32 = 2.0; // py(2) on the body container, top side
const NODE_ROW_PX: f32 = 18.0;     // explicit h() given to each column row div

/// Display mode for the schema diagram.
#[derive(Clone)]
pub enum SchemaVizMode {
    /// Focused view: one table + immediate FK neighbors.
    Focused {
        table: String,
        schema: Option<String>,
    },
    /// Global view: all tables in the schema (Phase 2, not yet implemented).
    Global,
}

/// Loading status for the schema diagram.
#[derive(Clone, PartialEq)]
pub enum LoadStatus {
    Loading,
    Ready,
    Error(String),
    NotSupported,
}

pub struct SchemaVizDocument {
    id: DocumentId,
    pub profile_id: Uuid,
    pub database: Option<String>,
    pub mode: SchemaVizMode,
    pub tables: Vec<TableInfo>,
    pub graph: Option<SchemaGraph>,
    pub layout: Option<LayoutResult>,
    pub load_status: LoadStatus,
    pub scroll_offset: Point<Pixels>,
    pub zoom: f32,
    pub focus_handle: FocusHandle,
    _subscriptions: Vec<Subscription>,
    // Dependencies
    app_state: Entity<AppStateEntity>,
    // Pan state
    is_panning: bool,
    pan_start: Point<Pixels>,
    pan_offset: Point<Pixels>,
    // Node interaction
    selected_node: Option<petgraph::graph::NodeIndex>,
    pending_details_panel: Option<petgraph::graph::NodeIndex>,
    // Drag state for node repositioning
    dragging_node: Option<petgraph::graph::NodeIndex>,
    drag_offset: Point<Pixels>,
    node_position_overrides: std::collections::HashMap<petgraph::graph::NodeIndex, Point<f32>>,
    // Layout
    pub layout_format: LayoutFormat,
    pub table_cap_warning: bool,
    // Cancellation
    cancel_token: Option<Arc<CancelToken>>,
    // Pending toast notification (set from sync context, flushed in render)
    pending_toast: Option<PendingToast>,
    // Toolbar dropdowns
    layout_menu_open: bool,
    export_menu_open: bool,
    // Context menu
    context_menu_open: bool,
    context_menu_position: Point<Pixels>,
    context_menu_target: Option<petgraph::graph::NodeIndex>,
    context_menu_selected_index: usize,
    context_menu_submenu: Option<SubMenu>,
    context_menu_submenu_selected_index: usize,
}

impl SchemaVizDocument {
    /// Returns the table name if in focused mode.
    pub fn table_name(&self) -> Option<&str> {
        match &self.mode {
            SchemaVizMode::Focused { table, .. } => Some(table.as_str()),
            SchemaVizMode::Global => None,
        }
    }

    /// Creates a new SchemaVizDocument and starts async loading.
    /// NOTE: This must be called from within a `cx.new(|cx| ...)` closure
    /// where `cx` is `Context<Self>`. The caller is responsible for wrapping
    /// this with `cx.new(|cx| SchemaVizDocument::new(..., cx))`.
    pub fn new(
        profile_id: Uuid,
        database: Option<String>,
        mode: SchemaVizMode,
        app_state: Entity<AppStateEntity>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let focus_handle = cx.focus_handle();
        let id = DocumentId::new();

        // Get the correct per-database connection using TaskTarget.
        // This is critical for ConnectionPerDatabase drivers (e.g., PostgreSQL) where
        // multiple databases exist on the same host and the primary connection may be
        // on a different database than the one the user selected.
        let task_target = TaskTarget {
            profile_id,
            database: database.clone(),
        };
        let connection = app_state
            .read(cx)
            .facade
            .connections
            .connection_for_task_target(&task_target);

        let entity = cx.entity().clone();

        let mut doc = Self {
            id,
            profile_id,
            database: database.clone(),
            mode: mode.clone(),
            tables: Vec::new(),
            graph: None,
            layout: None,
            load_status: LoadStatus::Loading,
            scroll_offset: Point::default(),
            zoom: 1.0,
            focus_handle,
            _subscriptions: Vec::new(),
            app_state: app_state.clone(),
            is_panning: false,
            pan_start: Point::default(),
            pan_offset: Point::default(),
            selected_node: None,
            pending_details_panel: None,
            dragging_node: None,
            drag_offset: Point::default(),
            node_position_overrides: std::collections::HashMap::new(),
            layout_format: LayoutFormat::LeftRight,
            table_cap_warning: false,
            cancel_token: None,
            pending_toast: None,
            layout_menu_open: false,
            export_menu_open: false,
            context_menu_open: false,
            context_menu_position: Point::default(),
            context_menu_target: None,
            context_menu_selected_index: 0,
            context_menu_submenu: None,
            context_menu_submenu_selected_index: 0,
        };

        // Spawn async loading task with the correct per-database connection
        doc.spawn_loading(profile_id, database, mode, connection, entity, cx);

        doc
    }

    /// Cancels any in-progress schema loading.
    pub fn cancel_loading(&mut self, cx: &App) {
        if let Some(ref cancel_token) = self.cancel_token {
            cancel_token.cancel();
        }

        let mode_str = match &self.mode {
            SchemaVizMode::Focused { .. } => "focused",
            SchemaVizMode::Global => "global",
        };
        let tables_loaded = self.tables.len();
        // Estimate total tables based on current state
        let tables_total = if self.load_status == LoadStatus::Ready {
            tables_loaded
        } else {
            0
        };

        let details = serde_json::json!({
            "mode": mode_str,
            "database": self.database,
            "tables_loaded": tables_loaded,
            "tables_total": tables_total,
        });
        self.emit_audit_event(
            EventSeverity::Warn,
            EventOutcome::Cancelled,
            audit_actions::SCHEMA_VIZ_CANCEL,
            "Schema diagram loading cancelled",
            details,
            cx,
        );
    }

    /// Emits an audit event for schema visualization operations.
    fn emit_audit_event(
        &self,
        severity: EventSeverity,
        outcome: EventOutcome,
        action: AuditAction,
        summary: &str,
        details: serde_json::Value,
        cx: &App,
    ) {
        let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
        let object_id = self
            .database
            .as_ref()
            .map(|d| format!("{}/{}", self.profile_id, d))
            .unwrap_or_else(|| self.profile_id.to_string());

        let event = EventRecord::new(now_ms, severity, EventCategory::Config, outcome)
            .with_summary(summary)
            .with_typed_action(action)
            .with_origin(EventOrigin::local())
            .with_actor_id("local")
            .with_connection_context(
                self.profile_id.to_string(),
                self.database.as_deref().unwrap_or(""),
                "",
            )
            .with_object_ref("schema_diagram", object_id);

        let event = event.with_details_json(details.to_string());

        if let Err(e) = self.app_state.read(cx).audit_service().record(event) {
            log::error!("CRITICAL: schema_viz audit event failed to record: {}", e);
        }
    }

    /// Spawns the background loading task for schema data.
    fn spawn_loading(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        mode: SchemaVizMode,
        connection: Option<Arc<dyn Connection>>,
        entity: Entity<Self>,
        cx: &mut Context<Self>,
    ) {
        // Register the task with the TasksPanel before spawning
        let (task_id, cancel_token) = {
            let database_label = database.as_deref().unwrap_or("global");
            let description = format!("Schema diagram: {}", database_label);
            let target = TaskTarget {
                profile_id,
                database: database.clone(),
            };
            self.app_state.update(cx, |state, _cx| {
                state.start_task_for_target(TaskKind::LoadSchema, description, Some(target))
            })
        };

        // Store cancel token so it can be triggered from cancel_loading()
        let cancel_token = Arc::new(cancel_token);
        self.cancel_token = Some(cancel_token.clone());

        let task = cx.background_executor().spawn(async move {
            Self::load_focused_schema_blocking(database, mode, connection, cancel_token)
        });

        let entity = entity.clone();
        let app_state = self.app_state.clone();
        cx.spawn(async move |_entity, cx| {
            let load_result = task.await;

            // Determine if cancelled by checking if error message is "Cancelled"
            let is_cancelled = load_result.as_ref().err().map(|e| e == "Cancelled").unwrap_or(false);

            if let Err(error) = cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    match load_result {
                        Ok((tables, graph, layout, capped, tables_loaded)) => {
                            let (focal_table, focal_schema) = match &doc.mode {
                                SchemaVizMode::Focused { table, schema } => {
                                    (table.clone(), schema.clone())
                                }
                                SchemaVizMode::Global => ("".to_string(), None),
                            };
                            let mode_str = match &doc.mode {
                                SchemaVizMode::Focused { .. } => "focused",
                                SchemaVizMode::Global => "global",
                            };
                            let table_count = tables.len();
                            let edge_count = graph.edge_count();
                            let layout_format = format!("{:?}", doc.layout_format);

                            let details = serde_json::json!({
                                "mode": mode_str,
                                "table": focal_table,
                                "schema": focal_schema,
                                "database": doc.database,
                                "table_count": table_count,
                                "edge_count": edge_count,
                                "layout_format": layout_format,
                                "tables_loaded": tables_loaded,
                            });
                            doc.emit_audit_event(
                                EventSeverity::Info,
                                EventOutcome::Success,
                                audit_actions::SCHEMA_VIZ_OPEN,
                                "Opened schema diagram",
                                details,
                                cx,
                            );

                            // Complete the task in the TasksPanel
                            app_state.update(cx, |state, cx| {
                                state.complete_task(task_id);
                                cx.emit(crate::app::AppStateChanged);
                            });

                            let viewport_width = 800.0;
                            let viewport_height = 600.0;
                            let initial_pan = if let Some(focal_node) =
                                graph.nodes().find(|(_, n)| {
                                    n.id.name == focal_table
                                        && n.id.schema.as_ref() == focal_schema.as_ref()
                                }) {
                                if let Some(focal_layout) = layout.nodes.get(&focal_node.0) {
                                    let focal_center_x = focal_layout.x + focal_layout.width / 2.0;
                                    let focal_center_y = focal_layout.y + focal_layout.height / 2.0;
                                    let viewport_center_x = viewport_width / 2.0;
                                    let viewport_center_y = viewport_height / 2.0;
                                    Some(Point::new(
                                        px(viewport_center_x - focal_center_x * doc.zoom),
                                        px(viewport_center_y - focal_center_y * doc.zoom),
                                    ))
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                            doc.tables = tables;
                            doc.graph = Some(graph);
                            doc.layout = Some(layout);
                            doc.load_status = LoadStatus::Ready;
                            doc.table_cap_warning = capped;
                            if let Some(pan) = initial_pan {
                                doc.pan_offset = pan;
                            }
                        }
                        Err(msg) => {
                            let mode_str = match &doc.mode {
                                SchemaVizMode::Focused { .. } => "focused",
                                SchemaVizMode::Global => "global",
                            };

                            // Emit cancel or error audit event
                            if is_cancelled {
                                let details = serde_json::json!({
                                    "mode": mode_str,
                                    "database": doc.database,
                                    "reason": "cancelled",
                                });
                                doc.emit_audit_event(
                                    EventSeverity::Warn,
                                    EventOutcome::Failure,
                                    audit_actions::SCHEMA_VIZ_CANCEL,
                                    "Schema diagram loading cancelled",
                                    details,
                                    cx,
                                );

                                // Cancel the task in the TasksPanel
                                app_state.update(cx, |state, cx| {
                                    state.cancel_task(task_id);
                                    cx.emit(crate::app::AppStateChanged);
                                });
                            } else {
                                let details = serde_json::json!({
                                    "mode": mode_str,
                                    "error": msg,
                                    "database": doc.database,
                                });
                                doc.emit_audit_event(
                                    EventSeverity::Error,
                                    EventOutcome::Failure,
                                    audit_actions::SCHEMA_VIZ_ERROR,
                                    "Schema diagram load error",
                                    details,
                                    cx,
                                );

                                // Fail the task in the TasksPanel
                                app_state.update(cx, |state, cx| {
                                    state.fail_task(task_id, msg.clone());
                                    cx.emit(crate::app::AppStateChanged);
                                });
                            }
                            doc.load_status = LoadStatus::Error(msg);
                        }
                    }
                    cx.notify();
                })
            }) {
                log::warn!("Failed to apply schema viz loading result: {:?}", error);
            }
        })
        .detach();
    }

    /// Loads schema data (blocking, runs on background executor).
    /// The connection must be the correct per-database connection (obtained via
    /// `connection_for_task_target` in `SchemaVizDocument::new`), not the primary connection.
    ///
    /// The `cancel_token` is checked at key points to allow cancellation.
    /// Returns `(tables, graph, layout, capped, tables_loaded)` where `tables_loaded`
    /// is the count of successfully loaded tables (useful for audit events on cancel).
    #[allow(clippy::collapsible_if)]
    fn load_focused_schema_blocking(
        database: Option<String>,
        mode: SchemaVizMode,
        connection: Option<Arc<dyn Connection>>,
        cancel_token: Arc<CancelToken>,
    ) -> Result<(Vec<TableInfo>, SchemaGraph, LayoutResult, bool, usize), String> {
        let connection =
            connection.ok_or_else(|| "Connection not found or not active".to_string())?;

        let db_name = database.ok_or_else(|| "No database specified".to_string())?;

        match mode {
            SchemaVizMode::Focused { table, schema } => {
                let metadata = connection.metadata();
                if !metadata
                    .capabilities
                    .contains(dbflux_core::DriverCapabilities::FOREIGN_KEYS)
                {
                    return Err("Foreign keys not supported by this driver".to_string());
                }

                if cancel_token.is_cancelled() {
                    return Err("Cancelled".into());
                }

                let focal_table = connection
                    .table_details(&db_name, schema.as_deref(), &table)
                    .map_err(|e| format!("Failed to fetch table details: {}", e))?;

                if cancel_token.is_cancelled() {
                    return Err("Cancelled".into());
                }

                let mut all_table_names: HashSet<(Option<String>, String)> = HashSet::new();
                all_table_names.insert((schema.clone(), table.clone()));

                if let Some(ref fks) = focal_table.foreign_keys {
                    for fk in fks {
                        all_table_names.insert((fk.referenced_schema.clone(), fk.referenced_table.clone()));
                    }
                }

                let mut all_tables = Vec::with_capacity(all_table_names.len());
                all_tables.push(focal_table.clone());
                let mut tables_loaded = 1; // focal table already loaded

                for (tbl_schema, tbl_name) in &all_table_names {
                    if tbl_name == &table && tbl_schema.as_deref() == schema.as_deref() {
                        continue;
                    }

                    if cancel_token.is_cancelled() {
                        return Err("Cancelled".into());
                    }

                    match connection.table_details(&db_name, tbl_schema.as_deref(), tbl_name) {
                        Ok(details) => {
                            tables_loaded += 1;
                            all_tables.push(details);
                        }
                        Err(e) => {
                            log::warn!(
                                "Failed to fetch details for table {}.{:?}: {}",
                                tbl_schema.as_deref().unwrap_or("<default>"),
                                tbl_name,
                                e
                            );
                        }
                    }
                }

                let inbound_neighbors =
                    Self::find_inbound_references(&all_tables, schema.as_deref(), &table);

                for (s, n) in inbound_neighbors {
                    all_table_names.insert((Some(s), n));
                }

                for (tbl_schema, tbl_name) in &all_table_names {
                    if !all_tables
                        .iter()
                        .any(|t| t.name == *tbl_name && t.schema.as_deref() == tbl_schema.as_deref())
                    {
                        if cancel_token.is_cancelled() {
                            return Err("Cancelled".into());
                        }

                        if let Ok(details) =
                            connection.table_details(&db_name, tbl_schema.as_deref(), tbl_name)
                        {
                            tables_loaded += 1;
                            all_tables.push(details);
                        }
                    }
                }

                let graph = SchemaGraph::build(&all_tables);
                let focused_graph = graph.neighborhood(&table, schema.as_deref(), 1);
                let layout = dbflux_schema_viz::layout::compute_layout(
                    &focused_graph,
                    LayoutFormat::LeftRight,
                    Some((table.as_str(), schema.as_deref())),
                );

                Ok((all_tables, focused_graph, layout, false, tables_loaded))
            }
            SchemaVizMode::Global => {
                let metadata = connection.metadata();
                if !metadata
                    .capabilities
                    .contains(dbflux_core::DriverCapabilities::FOREIGN_KEYS)
                {
                    return Err("Foreign keys not supported by this driver".to_string());
                }

                // Load ALL tables in the database
                let schema_info = connection
                    .schema_for_database(&db_name)
                    .map_err(|e| format!("Failed to list tables: {}", e))?;

                if cancel_token.is_cancelled() {
                    return Err("Cancelled".into());
                }

                const TABLE_CAP: usize = 100;
                let capped = schema_info.tables.len() > TABLE_CAP;
                let tables_to_load: Vec<_> = schema_info.tables.into_iter().take(TABLE_CAP).collect();

                let mut all_table_details = Vec::with_capacity(tables_to_load.len());
                let mut tables_loaded = 0;

                for tbl in &tables_to_load {
                    if cancel_token.is_cancelled() {
                        return Err("Cancelled".into());
                    }

                    match connection.table_details(&db_name, tbl.schema.as_deref(), &tbl.name) {
                        Ok(details) => {
                            tables_loaded += 1;
                            all_table_details.push(details);
                        }
                        Err(e) => log::warn!("Failed to fetch details for {}: {}", tbl.name, e),
                    }
                }

                let graph = SchemaGraph::build(&all_table_details);
                let layout = dbflux_schema_viz::layout::compute_layout(&graph, LayoutFormat::Compact, None);

                Ok((all_table_details, graph, layout, capped, tables_loaded))
            }
        }
    }

    /// Finds tables that reference the focal table via FK.
    /// Uses the already-fetched tables (which have FK data populated via table_details).
    fn find_inbound_references(
        tables: &[TableInfo],
        focal_schema: Option<&str>,
        focal_table: &str,
    ) -> Vec<(String, String)> {
        let mut neighbors = Vec::new();

        for tbl in tables {
            let Some(ref fks) = tbl.foreign_keys else {
                continue;
            };
            for fk in fks {
                if fk.referenced_table == focal_table
                    && fk.referenced_schema.as_deref() == focal_schema
                {
                    neighbors.push((
                        tbl.schema.clone().unwrap_or_else(|| "main".to_string()),
                        tbl.name.clone(),
                    ));
                }
            }
        }

        neighbors
    }

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn state(&self) -> DocumentState {
        match &self.load_status {
            LoadStatus::Loading => DocumentState::Loading,
            LoadStatus::Ready => DocumentState::Clean,
            LoadStatus::Error(_) => DocumentState::Error,
            LoadStatus::NotSupported => DocumentState::Error,
        }
    }

    pub fn focus(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
    }

    pub fn set_layout_format(&mut self, format: LayoutFormat, cx: &mut Context<Self>) {
        let old_format = self.layout_format;
        self.layout_format = format;
        if let Some(ref graph) = self.graph {
            let focal = match &self.mode {
                SchemaVizMode::Focused { table, schema } => {
                    Some((table.as_str(), schema.as_deref()))
                }
                SchemaVizMode::Global => {
                    if format == LayoutFormat::Snowflake {
                        // Auto-select the most-connected table as focal in global mode
                        graph.most_connected_node().map(|(_, node)| {
                            (node.id.name.as_str(), node.id.schema.as_deref())
                        })
                    } else {
                        None
                    }
                }
            };
            self.layout = Some(dbflux_schema_viz::layout::compute_layout(
                graph,
                format,
                focal,
            ));
            self.zoom = 1.0;
            self.pan_offset = Point::default();
            self.node_position_overrides.clear();
        }
        cx.notify();

        // Emit audit event after recomputation
        let mode_str = match &self.mode {
            SchemaVizMode::Focused { table, .. } => {
                let details = serde_json::json!({
                    "old_format": format!("{:?}", old_format),
                    "new_format": format!("{:?}", format),
                    "mode": "focused",
                    "table": table,
                });
                self.emit_audit_event(
                    EventSeverity::Info,
                    EventOutcome::Success,
                    audit_actions::SCHEMA_VIZ_LAYOUT_CHANGE,
                    "Changed schema diagram layout",
                    details,
                    cx,
                );
                return;
            }
            SchemaVizMode::Global => "global",
        };
        let details = serde_json::json!({
            "old_format": format!("{:?}", old_format),
            "new_format": format!("{:?}", format),
            "mode": mode_str,
        });
        self.emit_audit_event(
            EventSeverity::Info,
            EventOutcome::Success,
            audit_actions::SCHEMA_VIZ_LAYOUT_CHANGE,
            "Changed schema diagram layout",
            details,
            cx,
        );
    }

    /// Returns nodes sorted spatially: y first (top-to-bottom), then x (left-to-right).
    fn spatial_sorted_nodes(&self) -> Vec<NodeIndex> {
        let layout = match &self.layout {
            Some(l) => l,
            None => return Vec::new(),
        };

        let mut nodes: Vec<_> = layout
            .nodes
            .keys()
            .copied()
            .collect();

        nodes.sort_by(|&a, &b| {
            let pos_a = layout.nodes.get(&a);
            let pos_b = layout.nodes.get(&b);
            match (pos_a, pos_b) {
                (Some(node_a), Some(node_b)) => {
                    let y_cmp = node_a.y.partial_cmp(&node_b.y).unwrap_or(std::cmp::Ordering::Equal);
                    if y_cmp != std::cmp::Ordering::Equal {
                        y_cmp
                    } else {
                        node_a.x.partial_cmp(&node_b.x).unwrap_or(std::cmp::Ordering::Equal)
                    }
                }
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });

        nodes
    }

    /// Finds the next node in the given direction from the currently selected node.
    fn find_next_node(&self, direction: Direction) -> Option<NodeIndex> {
        let current = self.selected_node?;
        let sorted = self.spatial_sorted_nodes();
        let _current_idx = sorted.iter().position(|&idx| idx == current)?;

        let layout = self.layout.as_ref()?;
        let current_pos = layout.nodes.get(&current)?;

        let threshold = 20.0_f32;

        match direction {
            Direction::Left => {
                // Find nodes with x < current.x, pick the one with highest x
                let candidates: Vec<_> = sorted
                    .iter()
                    .filter(|&&idx| {
                        idx != current
                            && layout.nodes.get(&idx).map(|n| n.x < current_pos.x - threshold).unwrap_or(false)
                    })
                    .collect();

                candidates
                    .into_iter()
                    .max_by(|a, b| {
                        let pos_a = layout.nodes.get(a).unwrap();
                        let pos_b = layout.nodes.get(b).unwrap();
                        pos_a.x.partial_cmp(&pos_b.x).unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .copied()
                    .or_else(|| {
                        // Wrap: return rightmost node
                        sorted.last().copied()
                    })
            }
            Direction::Right => {
                // Find nodes with x > current.x, pick the one with lowest x
                let candidates: Vec<_> = sorted
                    .iter()
                    .filter(|&&idx| {
                        idx != current
                            && layout.nodes.get(&idx).map(|n| n.x > current_pos.x + threshold).unwrap_or(false)
                    })
                    .collect();

                candidates
                    .into_iter()
                    .min_by(|a, b| {
                        let pos_a = layout.nodes.get(a).unwrap();
                        let pos_b = layout.nodes.get(b).unwrap();
                        pos_a.x.partial_cmp(&pos_b.x).unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .copied()
                    .or_else(|| {
                        // Wrap: return leftmost node
                        sorted.first().copied()
                    })
            }
            Direction::Up => {
                // Find nodes with y < current.y, pick the one with highest y
                let candidates: Vec<_> = sorted
                    .iter()
                    .filter(|&&idx| {
                        idx != current
                            && layout.nodes.get(&idx).map(|n| n.y < current_pos.y - threshold).unwrap_or(false)
                    })
                    .collect();

                candidates
                    .into_iter()
                    .max_by(|a, b| {
                        let pos_a = layout.nodes.get(a).unwrap();
                        let pos_b = layout.nodes.get(b).unwrap();
                        pos_a.y.partial_cmp(&pos_b.y).unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .copied()
                    .or_else(|| {
                        // Wrap: return bottommost node
                        sorted.last().copied()
                    })
            }
            Direction::Down => {
                // Find nodes with y > current.y, pick the one with lowest y
                let candidates: Vec<_> = sorted
                    .iter()
                    .filter(|&&idx| {
                        idx != current
                            && layout.nodes.get(&idx).map(|n| n.y > current_pos.y + threshold).unwrap_or(false)
                    })
                    .collect();

                candidates
                    .into_iter()
                    .min_by(|a, b| {
                        let pos_a = layout.nodes.get(a).unwrap();
                        let pos_b = layout.nodes.get(b).unwrap();
                        pos_a.y.partial_cmp(&pos_b.y).unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .copied()
                    .or_else(|| {
                        // Wrap: return topmost node
                        sorted.first().copied()
                    })
            }
        }
    }

    /// Export the current schema graph as DBML to clipboard.
    pub fn export_dbml(&mut self, cx: &mut Context<Self>) {
        let graph = match &self.graph {
            Some(g) => g,
            None => return,
        };

        let scope = dbflux_schema_viz::DbmlScope::Subgraph;
        let dbml_result = dbflux_schema_viz::to_dbml(graph, scope);

        match dbml_result {
            Ok(dbml_text) => {
                cx.write_to_clipboard(ClipboardItem::new_string(dbml_text));

                self.pending_toast = Some(PendingToast {
                    message: "Schema exported to DBML (copied to clipboard)".into(),
                    is_error: false,
                });

                // Emit audit event
                let details = serde_json::json!({
                    "scope": "Subgraph",
                    "table_count": graph.node_count(),
                    "edge_count": graph.edge_count(),
                });
                self.emit_audit_event(
                    EventSeverity::Info,
                    EventOutcome::Success,
                    audit_actions::SCHEMA_VIZ_EXPORT_DBML,
                    "Exported schema to DBML",
                    details,
                    cx,
                );
            }
            Err(e) => {
                log::error!("DBML export failed: {}", e);

                self.pending_toast = Some(PendingToast {
                    message: format!("DBML export failed: {}", e),
                    is_error: true,
                });

                // Emit error audit event
                let details = serde_json::json!({
                    "error": e,
                });
                self.emit_audit_event(
                    EventSeverity::Error,
                    EventOutcome::Failure,
                    audit_actions::SCHEMA_VIZ_EXPORT_DBML,
                    "DBML export failed",
                    details,
                    cx,
                );
            }
        }
    }

    /// Copy the current schema graph as SQL DDL to clipboard.
    pub fn copy_as_sql(&mut self, cx: &mut Context<Self>) {
        let graph = match &self.graph {
            Some(g) => g,
            None => return,
        };

        let scope = dbflux_schema_viz::SqlScope::Subgraph;
        let sql_result = dbflux_schema_viz::to_sql(graph, scope);

        match sql_result {
            Ok(sql_text) => {
                cx.write_to_clipboard(ClipboardItem::new_string(sql_text.clone()));

                self.pending_toast = Some(PendingToast {
                    message: "Schema exported as SQL (copied to clipboard)".into(),
                    is_error: false,
                });

                let details = serde_json::json!({
                    "scope": "Subgraph",
                    "table_count": graph.node_count(),
                    "edge_count": graph.edge_count(),
                    "sql_length": sql_text.len(),
                });
                self.emit_audit_event(
                    EventSeverity::Info,
                    EventOutcome::Success,
                    audit_actions::SCHEMA_VIZ_EXPORT_SQL,
                    "Exported schema as SQL",
                    details,
                    cx,
                );
            }
            Err(e) => {
                log::error!("SQL export failed: {}", e);

                self.pending_toast = Some(PendingToast {
                    message: format!("SQL export failed: {}", e),
                    is_error: true,
                });

                let details = serde_json::json!({ "error": e });
                self.emit_audit_event(
                    EventSeverity::Error,
                    EventOutcome::Failure,
                    audit_actions::SCHEMA_VIZ_EXPORT_SQL,
                    "SQL export failed",
                    details,
                    cx,
                );
            }
        }
    }

    pub fn active_context(&self) -> ContextId {
        ContextId::SchemaViz
    }

    // ── Rendering helpers ──────────────────────────────────────────────────────

    fn render_loading(&self, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        div()
            .flex()
            .size_full()
            .items_center()
            .justify_center()
            .bg(theme.background)
            .flex_col()
            .gap(Spacing::MD)
            .child(
                div()
                    .size(px(32.0))
                    .border_2()
                    .border_color(theme.primary)
                    .rounded_full(),
            )
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .text_color(theme.muted_foreground)
                    .child("Loading schema..."),
            )
    }

    fn render_error(&self, msg: &str) -> Div {
        div()
            .flex()
            .size_full()
            .items_center()
            .justify_center()
            .child(
                div()
                    .text_color(gpui::red())
                    .child(format!("Error: {}", msg)),
            )
    }

    fn render_not_supported(&self) -> Div {
        div()
            .flex()
            .size_full()
            .items_center()
            .justify_center()
            .child("Schema diagram is not available for this database type")
    }

    fn layout_label(format: LayoutFormat) -> &'static str {
        match format {
            LayoutFormat::LeftRight => "Layout",
            LayoutFormat::Snowflake => "Layout",
            LayoutFormat::Compact => "Layout",
        }
    }

    fn make_layout_menu_item(
        &self,
        label: &'static str,
        format: LayoutFormat,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> Div {
        let is_selected = self.layout_format == format;
        div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .h(rems(1.6))
            .px(Spacing::SM)
            .rounded_sm()
            .cursor_pointer()
            .text_color(theme.foreground)
            .when(is_selected, |d| d.bg(theme.primary.opacity(0.1)))
            .on_mouse_down(MouseButton::Left, cx.listener(move |this, _, _, cx| {
                this.set_layout_format(format, cx);
                this.layout_menu_open = false;
                cx.notify();
            }))
            .child(div().text_size(FontSizes::SM).child(label))
                .when(is_selected, |d| {
                d.child(
                    svg()
                        .path(AppIcon::CircleCheck.path())
                        .size_3()
                        .text_color(theme.primary),
                )
            })
    }

    fn render_layout_menu(
        &self,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let items = vec![
            self.make_layout_menu_item(
                "Left-Right",
                LayoutFormat::LeftRight,
                theme,
                cx,
            ),
            self.make_layout_menu_item(
                "Snowflake",
                LayoutFormat::Snowflake,
                theme,
                cx,
            ),
            self.make_layout_menu_item(
                "Compact",
                LayoutFormat::Compact,
                theme,
                cx,
            ),
        ];

        self.build_dropdown_menu(items, cx)
    }

    fn render_export_menu(
        &self,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let items: Vec<Div> = vec![
            div()
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .h(rems(1.6))
                .px(Spacing::SM)
                .rounded_sm()
                .cursor_pointer()
                .text_color(theme.foreground)
                .on_mouse_down(MouseButton::Left, cx.listener(|this, _, _, cx| {
                    this.export_dbml(cx);
                    this.export_menu_open = false;
                    cx.notify();
                }))
                .child(div().text_size(FontSizes::SM).child("Copy as DBML")),
            div()
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .h(rems(1.6))
                .px(Spacing::SM)
                .rounded_sm()
                .cursor_pointer()
                .text_color(theme.foreground)
                .on_mouse_down(MouseButton::Left, cx.listener(|this, _, _, cx| {
                    this.copy_as_sql(cx);
                    this.export_menu_open = false;
                    cx.notify();
                }))
                .child(div().text_size(FontSizes::SM).child("Copy as SQL")),
        ];

        self.build_dropdown_menu(items, cx)
    }

    fn build_dropdown_menu(&self, items: Vec<Div>, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        deferred(
            div()
                .absolute()
                .top_full()
                .mt_1()
                .left_0()
                .min_w(px(140.0))
                .bg(theme.popover)
                .border_1()
                .border_color(theme.border)
                .rounded_md()
                .shadow_lg()
                .py(Spacing::XS)
                .occlude()
                .flex_col()
                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                    cx.stop_propagation();
                })
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.layout_menu_open = false;
                    this.export_menu_open = false;
                    cx.notify();
                }))
                .children(items),
        )
    }

    /// Returns the top-level context menu items (not expanded).
    fn context_menu_items(&self) -> Vec<ContextMenuItem> {
        let mut items = Vec::new();
        items.push(ContextMenuItem::ZoomIn);
        items.push(ContextMenuItem::ZoomOut);
        items.push(ContextMenuItem::Separator);
        items.push(ContextMenuItem::LayoutMenu);
        items.push(ContextMenuItem::CopyAsMenu);
        items.push(ContextMenuItem::Separator);
        if self.selected_node.is_some() {
            items.push(ContextMenuItem::FocusOnTable);
        }
        items
    }

    /// Renders the context menu with keyboard-navigable submenus.
    fn render_context_menu(
        &self,
        position: Point<Pixels>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let theme_clone = theme.clone();
        let layout_format = self.layout_format;
        let has_node_selected = self.selected_node.is_some();

        // Main menu items
        let all_items = self.context_menu_items();
        let visible_indices: Vec<usize> = all_items
            .iter()
            .enumerate()
            .filter(|(_, item)| !matches!(item, ContextMenuItem::FocusOnTable) || has_node_selected)
            .map(|(i, _)| i)
            .collect();

        // ── Main menu ────────────────────────────────────────────────────────
        let main_menu = {
            let selected_idx = self.context_menu_selected_index;
            div()
                .absolute()
                .left(position.x)
                .top(position.y)
                .min_w(px(160.0))
                .bg(theme.popover)
                .border_1()
                .border_color(theme.border)
                .rounded_md()
                .shadow_lg()
                .flex_col()
                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                    cx.stop_propagation();
                })
                .on_key_down(cx.listener(move |this, event: &KeyDownEvent, _, cx| {
                    use crate::keymap::key_chord_from_gpui;

                    let chord = key_chord_from_gpui(&event.keystroke);

                    // Count visible items in main menu
                    let has_node = this.selected_node.is_some();
                    let items = this.context_menu_items();
                    let count = items
                        .iter()
                        .filter(|i| !matches!(i, ContextMenuItem::FocusOnTable) || has_node)
                        .count();

                    if this.context_menu_submenu.is_some() {
                        // ── Submenu navigation ────────────────────────────────
                        let sub_idx = &mut this.context_menu_submenu_selected_index;
                        let submenu = this.context_menu_submenu.unwrap();
                        let sub_count = submenu.items().len();

                        match chord.key.as_str() {
                            "up" | "k" => {
                                if sub_count > 0 {
                                    *sub_idx = (*sub_idx + sub_count - 1) % sub_count;
                                    cx.notify();
                                }
                            }
                            "down" | "j" => {
                                if sub_count > 0 {
                                    *sub_idx = (*sub_idx + 1) % sub_count;
                                    cx.notify();
                                }
                            }
                            "enter" | "l" => {
                                // Activate submenu item and close
                                let submenu = this.context_menu_submenu.unwrap();
                                let sub_item = submenu.items()[this.context_menu_submenu_selected_index];
                                match submenu {
                                    SubMenu::Layout => {
                                        let format = match sub_item {
                                            SubMenuItem::LeftRight => LayoutFormat::LeftRight,
                                            SubMenuItem::Snowflake => LayoutFormat::Snowflake,
                                            SubMenuItem::Compact => LayoutFormat::Compact,
                                            _ => return,
                                        };
                                        this.set_layout_format(format, cx);
                                    }
                                    SubMenu::CopyAs => {
                                        match sub_item {
                                            SubMenuItem::CopyAsDbml => this.export_dbml(cx),
                                            SubMenuItem::CopyAsSql => this.copy_as_sql(cx),
                                            _ => return,
                                        }
                                    }
                                }
                                this.context_menu_open = false;
                                this.context_menu_submenu = None;
                                cx.notify();
                            }
                            "escape" | "h" | "left" => {
                                // Close submenu, return to main menu
                                this.context_menu_submenu = None;
                                cx.notify();
                            }
                            _ => {}
                        }
                    } else {
                        // ── Main menu navigation ─────────────────────────────
                        match chord.key.as_str() {
                            "up" | "k" => {
                                if count > 0 {
                                    this.context_menu_selected_index =
                                        (this.context_menu_selected_index + count - 1) % count;
                                    cx.notify();
                                }
                            }
                            "down" | "j" => {
                                if count > 0 {
                                    this.context_menu_selected_index =
                                        (this.context_menu_selected_index + 1) % count;
                                    cx.notify();
                                }
                            }
                            "right" | "enter" | "l" => {
                                // Open submenu if on LayoutMenu or CopyAsMenu, else activate
                                let has_node = this.selected_node.is_some();
                                let items = this.context_menu_items();
                                let filtered: Vec<_> = items
                                    .iter()
                                    .enumerate()
                                    .filter(|(_, i)| !matches!(i, ContextMenuItem::FocusOnTable) || has_node)
                                    .collect();
                                if let Some(&(_, item)) = filtered.get(this.context_menu_selected_index) {
                                    match item {
                                        ContextMenuItem::LayoutMenu => {
                                            this.context_menu_submenu = Some(SubMenu::Layout);
                                            this.context_menu_submenu_selected_index = 0;
                                            cx.notify();
                                        }
                                        ContextMenuItem::CopyAsMenu => {
                                            this.context_menu_submenu = Some(SubMenu::CopyAs);
                                            this.context_menu_submenu_selected_index = 0;
                                            cx.notify();
                                        }
                                        ContextMenuItem::ZoomIn => {
                                            this.zoom = (this.zoom * 1.25).min(4.0);
                                            this.context_menu_open = false;
                                            cx.notify();
                                        }
                                        ContextMenuItem::ZoomOut => {
                                            this.zoom = (this.zoom / 1.25).max(0.25);
                                            this.context_menu_open = false;
                                            cx.notify();
                                        }
                                        ContextMenuItem::Separator
                                        | ContextMenuItem::FocusOnTable => {}
                                    }
                                }
                            }
                            "escape" | "h" | "left" => {
                                this.context_menu_open = false;
                                cx.notify();
                            }
                            _ => {}
                        }
                    }
                }))
                .children(visible_indices.iter().enumerate().map(|(vis_idx, &orig_idx)| {
                    let item = &all_items[orig_idx];
                    let is_selected = vis_idx == selected_idx;
                    let theme = theme_clone.clone();

                    match item {
                        ContextMenuItem::Separator => {
                            div()
                                .h(px(1.0))
                                .mx(Spacing::SM)
                                .my(Spacing::XS)
                                .bg(theme.border)
                                .into_any_element()
                        }
                        ContextMenuItem::ZoomIn => {
                            div()
                                .flex()
                                .items_center()
                                .gap(Spacing::SM)
                                .h(rems(1.6))
                                .px(Spacing::SM)
                                .mx(Spacing::XS)
                                .rounded_sm()
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(theme.foreground)
                                .when(is_selected, |d| {
                                    d.bg(theme.accent).text_color(theme.accent_foreground)
                                })
                                .when(!is_selected, |d| d.hover(|d| d.bg(theme.secondary)))
                                .child(div().flex_1().child("Zoom In"))
                                .into_any_element()
                        }
                        ContextMenuItem::ZoomOut => {
                            div()
                                .flex()
                                .items_center()
                                .gap(Spacing::SM)
                                .h(rems(1.6))
                                .px(Spacing::SM)
                                .mx(Spacing::XS)
                                .rounded_sm()
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(theme.foreground)
                                .when(is_selected, |d| {
                                    d.bg(theme.accent).text_color(theme.accent_foreground)
                                })
                                .when(!is_selected, |d| d.hover(|d| d.bg(theme.secondary)))
                                .child(div().flex_1().child("Zoom Out"))
                                .into_any_element()
                        }
                        ContextMenuItem::LayoutMenu => {
                            div()
                                .flex()
                                .items_center()
                                .gap(Spacing::SM)
                                .h(rems(1.6))
                                .px(Spacing::SM)
                                .mx(Spacing::XS)
                                .rounded_sm()
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(theme.foreground)
                                .when(is_selected, |d| {
                                    d.bg(theme.accent).text_color(theme.accent_foreground)
                                })
                                .when(!is_selected, |d| d.hover(|d| d.bg(theme.secondary)))
                                .child(div().flex_1().child("Layout"))
                                .child(
                                    svg()
                                        .path(AppIcon::ChevronRight.path())
                                        .size_3()
                                        .text_color(theme.muted_foreground),
                                )
                                .into_any_element()
                        }
                        ContextMenuItem::CopyAsMenu => {
                            div()
                                .flex()
                                .items_center()
                                .gap(Spacing::SM)
                                .h(rems(1.6))
                                .px(Spacing::SM)
                                .mx(Spacing::XS)
                                .rounded_sm()
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(theme.foreground)
                                .when(is_selected, |d| {
                                    d.bg(theme.accent).text_color(theme.accent_foreground)
                                })
                                .when(!is_selected, |d| d.hover(|d| d.bg(theme.secondary)))
                                .child(div().flex_1().child("Copy as"))
                                .child(
                                    svg()
                                        .path(AppIcon::ChevronRight.path())
                                        .size_3()
                                        .text_color(theme.muted_foreground),
                                )
                                .into_any_element()
                        }
                        ContextMenuItem::FocusOnTable => {
                            div()
                                .flex()
                                .items_center()
                                .gap(Spacing::SM)
                                .h(rems(1.6))
                                .px(Spacing::SM)
                                .mx(Spacing::XS)
                                .rounded_sm()
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(theme.foreground)
                                .when(is_selected, |d| {
                                    d.bg(theme.accent).text_color(theme.accent_foreground)
                                })
                                .when(!is_selected, |d| d.hover(|d| d.bg(theme.secondary)))
                                .child(div().flex_1().child("Focus on this table"))
                                .into_any_element()
                        }
                    }
                }))
        };

        // ── Submenu (rendered to the right of main menu) ──────────────────
        let submenu_element = if let Some(submenu) = &self.context_menu_submenu {
            let sub_items = submenu.items();
            let sub_idx = self.context_menu_submenu_selected_index;
            let submenu_theme = theme.clone();

            // Position submenu to the right of the main menu, aligned to the parent item
            let main_width = px(160.0);

            // Determine which layout sub-item is currently active
            let active_layout = layout_format;

            Some(
                div()
                    .absolute()
                    .left(position.x + main_width + px(4.0))
                    .top(position.y)
                    .min_w(px(140.0))
                    .bg(submenu_theme.popover)
                    .border_1()
                    .border_color(submenu_theme.border)
                    .rounded_md()
                    .shadow_lg()
                    .flex_col()
                    .children(sub_items.iter().enumerate().map(|(i, sub_item)| {
                        let is_selected = i == sub_idx;
                        let is_active = match (submenu, sub_item) {
                            (SubMenu::Layout, SubMenuItem::LeftRight) => {
                                active_layout == LayoutFormat::LeftRight
                            }
                            (SubMenu::Layout, SubMenuItem::Snowflake) => {
                                active_layout == LayoutFormat::Snowflake
                            }
                            (SubMenu::Layout, SubMenuItem::Compact) => {
                                active_layout == LayoutFormat::Compact
                            }
                            _ => false,
                        };
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .h(rems(1.6))
                            .px(Spacing::SM)
                            .mx(Spacing::XS)
                            .rounded_sm()
                            .cursor_pointer()
                            .text_size(FontSizes::SM)
                            .text_color(submenu_theme.foreground)
                            .when(is_selected, |d| {
                                d.bg(submenu_theme.accent)
                                    .text_color(submenu_theme.accent_foreground)
                            })
                            .when(!is_selected, |d| d.hover(|d| d.bg(submenu_theme.secondary)))
                            .child(div().flex_1().child(sub_item.label()))
                            .when(is_active, |d| {
                                d.child(
                                    svg()
                                        .path(AppIcon::CircleCheck.path())
                                        .size_3()
                                        .text_color(submenu_theme.primary),
                                )
                            })
                            .into_any_element()
                    })),
            )
        } else {
            None
        };

        // Wrap in a transparent overlay that closes the menu when clicking outside
        let menu_entity = cx.entity().clone();
        deferred(
            div()
                .absolute()
                .top_0()
                .left_0()
                .size_full()
                .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                    menu_entity.update(cx, |this, cx| {
                        this.context_menu_open = false;
                        this.context_menu_submenu = None;
                        cx.notify();
                    });
                })
                .child(main_menu)
                .when_some(submenu_element, |parent, sub| parent.child(sub)),
        )
    }

    fn render_diagram(&self, _window: &mut Window, cx: &mut Context<Self>) -> Div {
        let zoom = self.zoom;
        let pan = self.pan_offset;

        let theme = cx.theme().clone();
        let background = theme.background;
        let tab_bar = theme.tab_bar;
        let border = theme.border;
        let muted_foreground = theme.muted_foreground;

        // The canvas is a single `relative()` div that fills the viewport.
        // All children (grid lines, edges, nodes) are `absolute()` within it.
        // Node screen position = graph_x * zoom + pan_x. No intermediate layer.
        // This ensures mouse event coordinates from on_scroll_wheel and on_mouse_move
        // are always in the same coordinate space as the pan_offset.
        let canvas = match &self.layout {
            Some(layout) => {
                // Grid: ~14 lines per axis covers any visible window at normal sizes.
                // Grid lines are in absolute canvas coords and do NOT move with pan/zoom —
                // they serve as a static background texture. This is intentional.
                let grid_size = 60.0_f32;
                let grid_visible = 3000.0_f32;
                let grid_count = (grid_visible / grid_size) as usize + 1;
                let grid_color = border.opacity(0.15);

                let mut canvas_children: Vec<AnyElement> = Vec::new();

                for i in 0..grid_count {
                    let x = i as f32 * grid_size;
                    canvas_children.push(
                        div()
                            .absolute()
                            .left(px(x))
                            .top(px(0.0))
                            .w(px(1.0))
                            .h(px(grid_visible))
                            .bg(grid_color)
                            .into_any_element(),
                    );
                }
                for i in 0..grid_count {
                    let y = i as f32 * grid_size;
                    canvas_children.push(
                        div()
                            .absolute()
                            .left(px(0.0))
                            .top(px(y))
                            .w(px(grid_visible))
                            .h(px(1.0))
                            .bg(grid_color)
                            .into_any_element(),
                    );
                }

                for seg in self.render_edges_overlay(layout, zoom, pan, &theme) {
                    canvas_children.push(seg.into_any_element());
                }
                for node_div in self.render_nodes(layout, zoom, pan, &theme, cx) {
                    canvas_children.push(node_div.into_any_element());
                }

                div()
                    .relative()
                    .size_full()
                    .bg(background)
                    .children(canvas_children)
            }
            None => div()
                .size_full()
                .bg(background)
                .child(self.render_error("No layout computed")),
        };

        let zoom_controls = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .px(Spacing::MD)
            .py(px(6.0))
            .bg(tab_bar)
            .border_b_1()
            .border_color(border)
            .children(vec![
                div().flex().items_center().gap(px(4.0)).child(
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(muted_foreground)
                        .child(format!("Zoom: {:.0}%", zoom * 100.0)),
                ),
                div().w(px(1.0)).h(px(16.0)).bg(border.opacity(0.5)),
                div()
                    .cursor_pointer()
                    .px(px(8.0))
                    .py(px(2.0))
                    .rounded_sm()
                    .when(self.zoom < 4.0, |d| {
                        d.on_mouse_down(
                            MouseButton::Left,
                            cx.listener(|this, _, _, cx| {
                                this.zoom = (this.zoom * 1.25).min(4.0);
                                cx.notify();
                            }),
                        )
                    })
                    .child("+"),
                div()
                    .cursor_pointer()
                    .px(px(8.0))
                    .py(px(2.0))
                    .rounded_sm()
                    .when(self.zoom > 0.25, |d| {
                        d.on_mouse_down(
                            MouseButton::Left,
                            cx.listener(|this, _, _, cx| {
                                this.zoom = (this.zoom / 1.25).max(0.25);
                                cx.notify();
                            }),
                        )
                    })
                    .child("-"),
                div()
                    .cursor_pointer()
                    .px(px(8.0))
                    .py(px(2.0))
                    .rounded_sm()
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, _, _, cx| {
                            this.zoom = 1.0;
                            this.pan_offset = Point::default();
                            cx.notify();
                        }),
                    )
                    .child("Reset"),
                div().w(px(1.0)).h(px(16.0)).bg(border.opacity(0.5)),
                // Layout dropdown
                div()
                    .relative()
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .px(px(8.0))
                            .py(px(2.0))
                            .rounded_sm()
                            .cursor_pointer()
                            .when(self.layout_menu_open, |d| {
                                d.bg(theme.primary.opacity(0.15))
                            })
                            .on_mouse_down(MouseButton::Left, cx.listener(|this, _, _, cx| {
                                this.layout_menu_open = !this.layout_menu_open;
                                this.export_menu_open = false;
                                cx.notify();
                            }))
                            .child(
                                div()
                                    .text_size(FontSizes::SM)
                                    .text_color(theme.foreground)
                                    .child(Self::layout_label(self.layout_format)),
                            )
                            .child(
                                svg()
                                    .path(AppIcon::ChevronDown.path())
                                    .size_3()
                                    .text_color(theme.muted_foreground),
                            ),
                    )
                    .when(self.layout_menu_open, |d| {
                        d.child(self.render_layout_menu(&theme, cx))
                    }),
                div().w(px(1.0)).h(px(16.0)).bg(border.opacity(0.5)),
                // Export dropdown
                div()
                    .relative()
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .px(px(8.0))
                            .py(px(2.0))
                            .rounded_sm()
                            .cursor_pointer()
                            .when(self.export_menu_open, |d| {
                                d.bg(theme.primary.opacity(0.15))
                            })
                            .on_mouse_down(MouseButton::Left, cx.listener(|this, _, _, cx| {
                                this.export_menu_open = !this.export_menu_open;
                                this.layout_menu_open = false;
                                cx.notify();
                            }))
                            .child(
                                div()
                                    .text_size(FontSizes::SM)
                                    .text_color(theme.foreground)
                                    .child("Export"),
                            )
                            .child(
                                svg()
                                    .path(AppIcon::ChevronDown.path())
                                    .size_3()
                                    .text_color(theme.muted_foreground),
                            ),
                    )
                    .when(self.export_menu_open, |d| {
                        d.child(self.render_export_menu(&theme, cx))
                    }),
            ]);

        // The viewport handles all pointer and scroll events.
        // It is `relative()` so child `absolute()` elements are anchored to it.
        // `overflow_hidden` clips elements outside the visible area.
        let viewport = div()
            .flex_1()
            .relative()
            .overflow_hidden()
            .track_focus(&self.focus_handle)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, event: &MouseDownEvent, _, _cx| {
                    if event.click_count == 1 && this.dragging_node.is_none() {
                        this.is_panning = true;
                        this.pan_start = event.position;
                    }
                }),
            )
            .on_mouse_move(cx.listener(|this, event: &MouseMoveEvent, _, cx| {
                if let Some(node_idx) = this.dragging_node {
                    let screen_x: f32 = event.position.x.into();
                    let screen_y: f32 = event.position.y.into();
                    let off_x: f32 = this.drag_offset.x.into();
                    let off_y: f32 = this.drag_offset.y.into();
                    let pan_x: f32 = this.pan_offset.x.into();
                    let pan_y: f32 = this.pan_offset.y.into();
                    let zoom = this.zoom;
                    // graph_x = (screen_x - drag_offset_x - pan_x) / zoom
                    let new_graph_x = (screen_x - off_x - pan_x) / zoom;
                    let new_graph_y = (screen_y - off_y - pan_y) / zoom;
                    this.node_position_overrides
                        .insert(node_idx, Point::new(new_graph_x, new_graph_y));
                    cx.notify();
                    return;
                }
                if !this.is_panning {
                    return;
                }
                let dx = event.position.x - this.pan_start.x;
                let dy = event.position.y - this.pan_start.y;
                if dx.abs() > px(0.5) || dy.abs() > px(0.5) {
                    this.pan_offset =
                        Point::new(this.pan_offset.x + dx, this.pan_offset.y + dy);
                    this.pan_start = event.position;
                    cx.notify();
                }
            }))
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(|this, _, _, _cx| {
                    this.is_panning = false;
                    this.dragging_node = None;
                }),
            )
            .on_scroll_wheel(cx.listener(|this, event: &ScrollWheelEvent, _, cx| {
                // event.position is relative to this viewport div — same space as pan_offset.
                let mouse = event.position;
                let old_zoom = this.zoom;
                let delta = event.delta.pixel_delta(px(1.0)).y;
                let factor = if delta > px(0.0) { 1.1_f32 } else { 0.9_f32 };
                let new_zoom = (old_zoom * factor).clamp(0.25, 4.0);

                if (new_zoom - old_zoom).abs() < 0.001 {
                    return;
                }

                // Keep the point under the mouse fixed:
                // graph_pt = (screen - pan) / old_zoom
                // new_pan  = screen - graph_pt * new_zoom
                let pan = this.pan_offset;
                let graph_x = (mouse.x - pan.x) / old_zoom;
                let graph_y = (mouse.y - pan.y) / old_zoom;
                let new_pan_x = mouse.x - graph_x * new_zoom;
                let new_pan_y = mouse.y - graph_y * new_zoom;

                this.pan_offset = Point::new(new_pan_x, new_pan_y);
                this.zoom = new_zoom;
                cx.notify();
            }))
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(|this, event: &MouseDownEvent, _, cx| {
                    this.context_menu_open = true;
                    this.context_menu_position = event.position;
                    this.context_menu_target = this.selected_node;
                    this.context_menu_selected_index = 0;
                    cx.notify();
                }),
            )
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, _, cx| {
                use crate::keymap::{key_chord_from_gpui, Modifiers};

                let chord = key_chord_from_gpui(&event.keystroke);
                let mods = &chord.modifiers;

                // Handle context menu navigation first
                if this.context_menu_open {
                    return;
                }

                // Zoom
                if (chord.key == "+" || chord.key == "=")
                    && mods.shift
                    && !mods.ctrl
                    && !mods.alt
                {
                    this.zoom = (this.zoom * 1.25).min(4.0);
                    cx.notify();
                    return;
                }
                if (chord.key == "-" || chord.key == "_")
                    && !mods.shift
                    && !mods.ctrl
                    && !mods.alt
                {
                    this.zoom = (this.zoom / 1.25).max(0.25);
                    cx.notify();
                    return;
                }

                // Layout shortcuts
                if !mods.shift && !mods.ctrl && !mods.alt {
                    match chord.key.as_str() {
                        "s" => {
                            this.set_layout_format(LayoutFormat::Snowflake, cx);
                            return;
                        }
                        "c" => {
                            this.set_layout_format(LayoutFormat::Compact, cx);
                            return;
                        }
                        "r" => {
                            this.set_layout_format(LayoutFormat::LeftRight, cx);
                            return;
                        }
                        "m" => {
                            this.context_menu_open = true;
                            this.context_menu_position = Point::new(px(100.0), px(100.0));
                            this.context_menu_target = this.selected_node;
                            this.context_menu_selected_index = 0;
                            cx.notify();
                            return;
                        }
                        "escape" => {
                            this.selected_node = None;
                            cx.notify();
                            return;
                        }
                        _ => {}
                    }
                }

                // Pan with arrow keys / h,j,k,l
                if !mods.shift && !mods.ctrl && !mods.alt {
                    match chord.key.as_str() {
                        "h" | "left" => {
                            let pan_x: f32 = this.pan_offset.x.into();
                            this.pan_offset = Point::new(px(pan_x + 50.0), this.pan_offset.y);
                            cx.notify();
                            return;
                        }
                        "l" | "right" => {
                            let pan_x: f32 = this.pan_offset.x.into();
                            this.pan_offset = Point::new(px(pan_x - 50.0), this.pan_offset.y);
                            cx.notify();
                            return;
                        }
                        "k" | "up" => {
                            let pan_y: f32 = this.pan_offset.y.into();
                            this.pan_offset = Point::new(this.pan_offset.x, px(pan_y + 50.0));
                            cx.notify();
                            return;
                        }
                        "j" | "down" => {
                            let pan_y: f32 = this.pan_offset.y.into();
                            this.pan_offset = Point::new(this.pan_offset.x, px(pan_y - 50.0));
                            cx.notify();
                            return;
                        }
                        _ => {}
                    }
                }

                // Selection navigation with Shift+arrows / Shift+hjkl
                if mods.shift && !mods.ctrl && !mods.alt {
                    match chord.key.as_str() {
                        "l" | "right" => {
                            if let Some(next) = this.find_next_node(Direction::Right) {
                                this.selected_node = Some(next);
                                cx.notify();
                            }
                            return;
                        }
                        "h" | "left" => {
                            if let Some(next) = this.find_next_node(Direction::Left) {
                                this.selected_node = Some(next);
                                cx.notify();
                            }
                            return;
                        }
                        "k" | "up" => {
                            if let Some(next) = this.find_next_node(Direction::Up) {
                                this.selected_node = Some(next);
                                cx.notify();
                            }
                            return;
                        }
                        "j" | "down" => {
                            if let Some(next) = this.find_next_node(Direction::Down) {
                                this.selected_node = Some(next);
                                cx.notify();
                            }
                            return;
                        }
                        _ => {}
                    }
                }

                // Move selected table with Alt+arrows / Alt+hjkl
                if mods.alt && !mods.shift && !mods.ctrl {
                    let Some(selected) = this.selected_node else {
                        return;
                    };

                    // Get current position (from override or layout)
                    let current_pos = this
                        .node_position_overrides
                        .get(&selected)
                        .copied()
                        .or_else(|| {
                            this.layout
                                .as_ref()
                                .and_then(|l| l.nodes.get(&selected))
                                .map(|n| Point::new(n.x, n.y))
                        })
                        .unwrap_or(Point::new(0.0, 0.0));

                    let mut new_pos = current_pos;

                    match chord.key.as_str() {
                        "h" | "left" => {
                            new_pos.x -= 20.0;
                        }
                        "l" | "right" => {
                            new_pos.x += 20.0;
                        }
                        "k" | "up" => {
                            new_pos.y -= 20.0;
                        }
                        "j" | "down" => {
                            new_pos.y += 20.0;
                        }
                        _ => return,
                    }

                    this.node_position_overrides.insert(selected, new_pos);
                    cx.notify();
                }
            }))
            .child(canvas);

        let cap_warning = self.table_cap_warning.then(|| {
            div()
                .px(Spacing::MD)
                .py(px(4.0))
                .bg(theme.primary.opacity(0.08))
                .border_b_1()
                .border_color(theme.border)
                .text_size(FontSizes::XS)
                .text_color(theme.muted_foreground)
                .child("Showing first 100 tables — the schema has more.")
        });

        // Context menu overlay
        let context_menu = self.context_menu_open.then(|| {
            self.render_context_menu(self.context_menu_position, cx)
        });

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(background)
            .child(zoom_controls)
            .children(cap_warning)
            .child(viewport)
            .when_some(context_menu, |d, menu| d.child(menu))
    }

    fn render_nodes(
        &self,
        layout: &LayoutResult,
        zoom: f32,
        pan: Point<Pixels>,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> Vec<Div> {
        let Some(graph) = &self.graph else {
            log::info!("DEBUG render_nodes: self.graph is None, returning empty");
            return Vec::new();
        };

        let node_count = graph.nodes().count();
        log::info!(
            "DEBUG render_nodes: graph.nodes() count={} layout.nodes.len={}",
            node_count,
            layout.nodes.len()
        );

        let dragging_node = self.dragging_node;

        graph
            .nodes()
            .filter_map(|(idx, node)| {
                let node_layout = layout.nodes.get(&idx)?;
                Some(self.render_node(
                    node,
                    node_layout,
                    zoom,
                    pan,
                    idx,
                    theme,
                    dragging_node,
                    cx,
                ))
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn render_node(
        &self,
        node: &dbflux_schema_viz::graph::TableNode,
        layout: &NodeLayout,
        zoom: f32,
        pan: Point<Pixels>,
        node_idx: petgraph::graph::NodeIndex,
        theme: &gpui_component::theme::Theme,
        dragging_node: Option<petgraph::graph::NodeIndex>,
        cx: &mut Context<Self>,
    ) -> Div {
        let width = layout.width;

        let pan_x: f32 = pan.x.into();
        let pan_y: f32 = pan.y.into();

        let position_override = self.node_position_overrides.get(&node_idx);
        let (node_left, node_top) = if let Some(pos) = position_override {
            (px(pos.x * zoom + pan_x), px(pos.y * zoom + pan_y))
        } else {
            (px(layout.x * zoom + pan_x), px(layout.y * zoom + pan_y))
        };

        let is_selected = self.selected_node.as_ref() == Some(&node_idx);

        let border_color = if is_selected {
            theme.primary
        } else {
            theme.border
        };

        let node_bg = theme.secondary;
        let header_text = theme.foreground;
        let muted_fg = theme.muted_foreground;

        let is_dragging = dragging_node == Some(node_idx);
        let cursor_style = if is_dragging {
            CursorStyle::PointingHand
        } else {
            CursorStyle::Arrow
        };

        let node_idx_clone = node_idx;

        let type_color = |type_name: &str, theme: &gpui_component::theme::Theme| -> Hsla {
            let lower = type_name.to_lowercase();
            if lower.contains("int")
                || lower.contains("serial")
                || lower.contains("numeric")
                || lower.contains("float")
                || lower.contains("double")
                || lower.contains("real")
                || lower.contains("decimal")
            {
                theme.primary
            } else if lower.contains("bool") {
                theme.muted_foreground.opacity(0.8)
            } else if lower.contains("timestamp")
                || lower.contains("date")
                || lower.contains("time")
                || lower.contains("interval")
            {
                theme.primary.opacity(0.7)
            } else if lower.contains("json")
                || lower.contains("xml")
                || lower.contains("array")
                || lower.contains("[]")
            {
                theme.accent_foreground
            } else if lower.contains("uuid") {
                theme.muted_foreground
            } else {
                theme.muted_foreground
            }
        };

        let header_title = if let Some(ref schema) = node.id.schema {
            div()
                .flex()
                .items_center()
                .gap(px(0.0))
                .child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(theme.primary)
                        .child(schema.clone()),
                )
                .child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(muted_fg)
                        .child(" · "),
                )
                .child(
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(header_text)
                        .font_weight(gpui::FontWeight::BOLD)
                        .child(node.id.name.clone()),
                )
        } else {
            div()
                .text_size(FontSizes::SM)
                .text_color(header_text)
                .font_weight(gpui::FontWeight::BOLD)
                .child(node.id.name.clone())
        };

        div()
            .absolute()
            .left(node_left)
            .top(node_top)
            .w(px(width))
            .border_1()
            .border_color(border_color)
            .rounded_md()
            .bg(node_bg)
            .shadow_sm()
            .overflow_hidden()
            .cursor(cursor_style)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, event: &MouseDownEvent, _, _cx| {
                    if event.click_count == 1 {
                        this.selected_node = Some(node_idx_clone);
                        this.pending_details_panel = Some(node_idx_clone);
                        this.dragging_node = Some(node_idx_clone);
                        this.is_panning = false;
                        let zoom = this.zoom;
                        let node_x = this
                            .node_position_overrides
                            .get(&node_idx_clone)
                            .map(|p| p.x)
                            .or_else(|| {
                                this.layout
                                    .as_ref()
                                    .and_then(|l| l.nodes.get(&node_idx_clone))
                                    .map(|n| n.x)
                            })
                            .unwrap_or(0.0);
                        let node_y = this
                            .node_position_overrides
                            .get(&node_idx_clone)
                            .map(|p| p.y)
                            .or_else(|| {
                                this.layout
                                    .as_ref()
                                    .and_then(|l| l.nodes.get(&node_idx_clone))
                                    .map(|n| n.y)
                            })
                            .unwrap_or(0.0);
                        let pan_x: f32 = this.pan_offset.x.into();
                        let pan_y: f32 = this.pan_offset.y.into();
                        let node_screen_x = px(node_x * zoom + pan_x);
                        let node_screen_y = px(node_y * zoom + pan_y);
                        this.drag_offset = Point::new(
                            event.position.x - node_screen_x,
                            event.position.y - node_screen_y,
                        );
                        this.node_position_overrides
                            .insert(node_idx_clone, Point::new(node_x, node_y));
                    }
                }),
            )
            .flex()
            .flex_col()
            .child(
                div()
                    .flex()
                    .items_center()
                    .px(px(10.0))
                    .py(px(6.0))
                    .bg(theme.tab_bar)
                    .border_b_1()
                    .border_color(theme.border)
                    .child(header_title),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .px(px(10.0))
                    .py(px(2.0))
                    .children(node.columns.iter().map(|col| {
                        let type_label = if col.is_pk {
                            format!("{} [pk]", col.type_name)
                        } else if col.is_fk {
                            format!("{} [fk]", col.type_name)
                        } else {
                            col.type_name.clone()
                        };
                        let col_type_color = type_color(&col.type_name, theme);
                        div()
                            .flex()
                            .items_center()
                            .h(px(NODE_ROW_PX))
                            .gap(px(4.0))
                            .overflow_hidden()
                            .text_size(FontSizes::XS)
                            .text_color(header_text)
                            .child(
                                div()
                                    .flex_1()
                                    .overflow_hidden()
                                    .text_ellipsis()
                                    .child(col.name.clone()),
                            )
                            .child(
                                div()
                                    .flex_shrink_0()
                                    .overflow_hidden()
                                    .text_ellipsis()
                                    .text_color(col_type_color)
                                    .child(type_label),
                            )
                    })),
            )
    }

    /// Renders edges as L-shaped CSS connectors.
    /// Uses node_position_overrides to get current node positions during drag.
    fn render_edges_overlay(
        &self,
        layout: &LayoutResult,
        zoom: f32,
        pan: Point<Pixels>,
        theme: &gpui_component::theme::Theme,
    ) -> Vec<Div> {
        let edge_color = theme.muted_foreground.opacity(0.5);

        let Some(graph) = &self.graph else {
            return Vec::new();
        };

        let pan_x: f32 = pan.x.into();
        let pan_y: f32 = pan.y.into();

        let mut segments = Vec::new();

        for edge_idx in graph.edge_indices() {
            let (source, target) = match graph.edge_endpoints(edge_idx) {
                Some((s, t)) => (s, t),
                None => continue,
            };
            let edge_weight = match graph.edge_weight(edge_idx) {
                Some(w) => w,
                None => continue,
            };
            let from_layout = match layout.nodes.get(&source) {
                Some(l) => l,
                None => continue,
            };
            let to_layout = match layout.nodes.get(&target) {
                Some(l) => l,
                None => continue,
            };

            let (from_x_base, from_y_base) = if let Some(pos) =
                self.node_position_overrides.get(&source)
            {
                (pos.x, pos.y)
            } else {
                (from_layout.x, from_layout.y)
            };
            let (to_x_base, to_y_base) = if let Some(pos) =
                self.node_position_overrides.get(&target)
            {
                (pos.x, pos.y)
            } else {
                (to_layout.x, to_layout.y)
            };

            let from_node_weight = match graph.node_weight(source) {
                Some(n) => n,
                None => continue,
            };
            let to_node_weight = match graph.node_weight(target) {
                Some(n) => n,
                None => continue,
            };

            // Column row Y offsets are fixed screen pixels — the node does NOT scale
            // its internal size with zoom; only its origin (x_base, y_base) scales.
            //
            // NODE_HEADER_PX, NODE_BODY_TOP_PX, NODE_ROW_PX must match render_node exactly.
            // Each column row has an explicit h(px(NODE_ROW_PX)) so the position is deterministic.
            let row_center = NODE_ROW_PX / 2.0;

            let from_col_y_px = edge_weight
                .from_columns
                .first()
                .and_then(|col_name| {
                    from_node_weight
                        .columns
                        .iter()
                        .position(|c| &c.name == col_name)
                })
                .map(|col_idx| {
                    NODE_HEADER_PX + NODE_BODY_TOP_PX + col_idx as f32 * NODE_ROW_PX + row_center
                })
                .unwrap_or(NODE_HEADER_PX + NODE_BODY_TOP_PX);

            let to_col_y_px = edge_weight
                .to_columns
                .first()
                .and_then(|col_name| {
                    to_node_weight
                        .columns
                        .iter()
                        .position(|c| &c.name == col_name)
                })
                .map(|col_idx| {
                    NODE_HEADER_PX + NODE_BODY_TOP_PX + col_idx as f32 * NODE_ROW_PX + row_center
                })
                .unwrap_or(NODE_HEADER_PX + NODE_BODY_TOP_PX);

            // Screen position: origin scales with zoom+pan, internal offset is fixed px.
            let from_x = from_x_base * zoom + pan_x + from_layout.width;
            let from_y = from_y_base * zoom + pan_y + from_col_y_px;
            let to_x = to_x_base * zoom + pan_x;
            let to_y = to_y_base * zoom + pan_y + to_col_y_px;

            let mid_x = (from_x + to_x) / 2.0;

            if (mid_x - from_x).abs() > 0.5 {
                let seg_left = from_x.min(mid_x);
                let seg_width = (mid_x - from_x).abs().max(1.0);
                segments.push(
                    div()
                        .absolute()
                        .left(px(seg_left))
                        .top(px(from_y - 1.0))
                        .w(px(seg_width))
                        .h(px(2.0))
                        .bg(edge_color),
                );
            }

            let vert_top = from_y.min(to_y);
            let vert_height = (to_y - from_y).abs().max(1.0);
            segments.push(
                div()
                    .absolute()
                    .left(px(mid_x - 1.0))
                    .top(px(vert_top))
                    .w(px(2.0))
                    .h(px(vert_height))
                    .bg(edge_color),
            );

            if (to_x - mid_x).abs() > 0.5 {
                let seg_left = mid_x.min(to_x);
                let seg_width = (to_x - mid_x).abs().max(1.0);
                segments.push(
                    div()
                        .absolute()
                        .left(px(seg_left))
                        .top(px(to_y - 1.0))
                        .w(px(seg_width))
                        .h(px(2.0))
                        .bg(edge_color),
                );
            }
        }

        segments
    }
}

impl EventEmitter<DocumentEvent> for SchemaVizDocument {}

impl Render for SchemaVizDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        flush_pending_toast(self.pending_toast.take(), window, cx);

        match &self.load_status {
            LoadStatus::Loading => self.render_loading(cx).into_any_element(),
            LoadStatus::Error(msg) => self.render_error(msg).into_any_element(),
            LoadStatus::NotSupported => self.render_not_supported().into_any_element(),
            LoadStatus::Ready => self.render_diagram(window, cx).into_any_element(),
        }
    }
}
