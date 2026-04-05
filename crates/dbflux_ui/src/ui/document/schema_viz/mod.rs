use gpui::prelude::*;
use gpui::*;
use gpui_component::scroll::{Scrollable, ScrollableElement};

use dbflux_core::{Connection, DbSchemaInfo, TableInfo};
use dbflux_schema_viz::{
    graph::SchemaGraph,
    layout::{LayoutResult, NodeLayout},
};
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

use crate::app::AppStateEntity;
use crate::keymap::ContextId;
use crate::ui::document::handle::DocumentEvent;
use crate::ui::document::types::{DocumentId, DocumentState};
use crate::ui::tokens::{FontSizes, Spacing};

/// Display mode for the schema diagram.
#[derive(Clone)]
pub enum SchemaVizMode {
    /// Focused view: one table + immediate FK neighbors.
    Focused { table: String, schema: Option<String> },
    /// Global view: all tables in the schema (Phase 2, not yet implemented).
    Global,
}

/// Loading status for the schema diagram.
#[derive(Clone)]
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

        // Get connection synchronously before spawning
        let connection = app_state.read(cx).get_connection(profile_id);

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
        };

        // Spawn async loading task
        doc.spawn_loading(profile_id, database, mode, connection, entity, cx);

        doc
    }

    /// Spawns the background loading task for schema data.
    fn spawn_loading(
        &mut self,
        _profile_id: Uuid,
        database: Option<String>,
        mode: SchemaVizMode,
        connection: Option<Arc<dyn Connection>>,
        entity: Entity<Self>,
        cx: &mut Context<Self>,
    ) {
        cx.spawn(async move |_entity, cx| {
            let load_result = match (mode, connection) {
                (SchemaVizMode::Focused { ref table, ref schema }, Some(connection)) => {
                    Self::load_focused_schema(
                        database.as_ref(),
                        table.as_str(),
                        schema.as_deref(),
                        connection,
                    )
                    .await
                }
                (SchemaVizMode::Focused { .. }, None) => {
                    Err("Connection not found or not active".to_string())
                }
                (SchemaVizMode::Global, _) => {
                    // Phase 2: implement global schema loading
                    Err("Global schema view not yet implemented".to_string())
                }
            };

            // Apply results in foreground
            if let Err(error) = cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    match load_result {
                        Ok((tables, graph, layout)) => {
                            doc.tables = tables;
                            doc.graph = Some(graph);
                            doc.layout = Some(layout);
                            doc.load_status = LoadStatus::Ready;
                        }
                        Err(msg) => {
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

    /// Loads schema data for focused mode.
    async fn load_focused_schema(
        database: Option<&String>,
        table: &str,
        schema: Option<&str>,
        connection: Arc<dyn Connection>,
    ) -> Result<(Vec<TableInfo>, SchemaGraph, LayoutResult), String> {
        // Check driver capabilities
        let metadata = connection.metadata();
        if !metadata.capabilities.contains(dbflux_core::DriverCapabilities::FOREIGN_KEYS) {
            return Err("Foreign keys not supported by this driver".to_string());
        }

        let db_name = database.ok_or_else(|| "No database specified".to_string())?;

        // Fetch focal table details
        let focal_table = connection
            .table_details(db_name, schema, table)
            .map_err(|e| format!("Failed to fetch table details: {}", e))?;

        // Collect all tables needed: focal + neighbors
        let mut all_table_names: HashSet<(Option<String>, String)> = HashSet::new();
        all_table_names.insert((schema.map(String::from), table.to_string()));

        // Get outbound FK neighbors (tables this table references)
        if let Some(ref fks) = focal_table.foreign_keys {
            for fk in fks {
                all_table_names.insert((
                    fk.referenced_schema.clone(),
                    fk.referenced_table.clone(),
                ));
            }
        }

        // Get inbound FK neighbors (tables that reference this table)
        let schema_snapshot = connection
            .schema()
            .map_err(|e| format!("Failed to fetch schema: {}", e))?;

        let inbound_neighbors =
            Self::find_inbound_references(&schema_snapshot, schema, table, &focal_table);

        for (s, n) in inbound_neighbors {
            all_table_names.insert((Some(s), n));
        }

        // Fetch details for all neighbor tables
        let mut all_tables = Vec::with_capacity(all_table_names.len());
        all_tables.push(focal_table.clone());

        for (tbl_schema, tbl_name) in &all_table_names {
            if tbl_name == table && tbl_schema.as_deref() == schema {
                continue; // Already added focal table
            }

            match connection.table_details(db_name, tbl_schema.as_deref(), tbl_name) {
                Ok(details) => all_tables.push(details),
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

        // Build graph and compute layout
        let graph = SchemaGraph::build(&all_tables);
        let focused_graph = graph.neighborhood(table, schema, 1);
        let layout = dbflux_schema_viz::layout::compute_layout(&focused_graph);

        Ok((all_tables, focused_graph, layout))
    }

    /// Finds tables that reference the focal table via FK.
    fn find_inbound_references(
        schema: &dbflux_core::SchemaSnapshot,
        focal_schema: Option<&str>,
        focal_table: &str,
        _focal_details: &TableInfo,
    ) -> Vec<(String, String)> {
        let mut neighbors = Vec::new();

        // Helper to check if a table references the focal table
        let references_focal = |tbl: &TableInfo| -> bool {
            let Some(ref fks) = tbl.foreign_keys else {
                return false;
            };
            fks.iter().any(|fk| {
                fk.referenced_table == focal_table
                    && fk.referenced_schema.as_deref() == focal_schema
            })
        };

        if let dbflux_core::DataStructure::Relational(rel) = &schema.structure {
            // For PostgreSQL-style schemas with named schemas
            if !rel.schemas.is_empty() {
                for schema_info in &rel.schemas {
                    for table in &schema_info.tables {
                        if references_focal(table) {
                            neighbors.push((schema_info.name.clone(), table.name.clone()));
                        }
                    }
                }
            } else {
                // For SQLite-style flat schemas (no named schemas)
                for table in &rel.tables {
                    if references_focal(table) {
                        neighbors.push(("main".to_string(), table.name.clone()));
                    }
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
            LoadStatus::NotSupported => DocumentState::Clean,
        }
    }

    pub fn focus(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
    }

    pub fn active_context(&self) -> ContextId {
        ContextId::Global
    }

    // ── Rendering helpers ──────────────────────────────────────────────────────

    fn render_loading(&self) -> Div {
        div()
            .flex()
            .size_full()
            .items_center()
            .justify_center()
            .child("Loading schema...")
    }

    fn render_error(&self, msg: &str) -> Div {
        div()
            .flex()
            .size_full()
            .items_center()
            .justify_center()
            .child(div().text_color(gpui::red()).child(format!("Error: {}", msg)))
    }

    fn render_not_supported(&self) -> Div {
        div()
            .flex()
            .size_full()
            .items_center()
            .justify_center()
            .child("Schema diagram is not available for this database type")
    }

    fn render_diagram(&self, _window: &mut Window, cx: &mut Context<Self>) -> Scrollable<Div> {
        let zoom = self.zoom;

        let inner = match &self.layout {
            Some(layout) => {
                let total_width = layout.total_width * zoom;
                let total_height = layout.total_height * zoom;
                div()
                    .relative()
                    .w(px(total_width))
                    .h(px(total_height))
                    .children(self.render_edges_overlay(layout, zoom))
                    .children(self.render_nodes(layout, zoom))
            }
            None => div().size_full().child(self.render_error("No layout computed")),
        };

        div()
            .size_full()
            .overflow_scrollbar()
            .track_focus(&self.focus_handle)
            .on_scroll_wheel(cx.listener(|this, event: &ScrollWheelEvent, _, cx| {
                let delta = event.delta.pixel_delta(px(1.0)).y;
                let factor = if delta > px(0.0) { 0.9_f32 } else { 1.1_f32 };
                this.zoom = (this.zoom * factor).clamp(0.25, 2.0);
                cx.notify();
            }))
            .child(inner)
    }

    fn render_nodes(&self, layout: &LayoutResult, zoom: f32) -> Vec<Div> {
        let Some(graph) = &self.graph else {
            return Vec::new();
        };

        graph
            .nodes()
            .filter_map(|(idx, node)| {
                let node_layout = layout.nodes.get(&idx)?;
                Some(self.render_node(node, node_layout, zoom))
            })
            .collect()
    }

    fn render_node(
        &self,
        node: &dbflux_schema_viz::graph::TableNode,
        layout: &NodeLayout,
        zoom: f32,
    ) -> Div {
        let scale = zoom;
        let x = layout.x * scale;
        let y = layout.y * scale;
        let width = layout.width * scale;
        let height = layout.height * scale;

        // Schema badge if non-default
        let schema_badge = node.id.schema.as_ref().map(|s| {
            div()
                .text_size(FontSizes::SM)
                .text_color(gpui::hsla(0.0, 0.0, 0.5, 0.7))
                .child(s.clone())
        });

        div()
            .absolute()
            .left(px(x))
            .top(px(y))
            .w(px(width))
            .h(px(height))
            .border_1()
            .border_color(gpui::hsla(0.0, 0.0, 0.5, 0.3))
            .rounded_md()
            .bg(gpui::white())
            .shadow_sm()
            .flex()
            .flex_col()
            .child(
                // Header: table name
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .px(Spacing::SM)
                    .py(px(4.0))
                    .bg(gpui::hsla(0.0, 0.0, 0.5, 0.1))
                    .border_b_1()
                    .border_color(gpui::hsla(0.0, 0.0, 0.5, 0.2))
                    .font_weight(gpui::FontWeight::BOLD)
                    .text_size(FontSizes::SM)
                    .children(schema_badge)
                    .child(node.id.name.clone()),
            )
            .children(node.columns.iter().map(|col| {
                let mut label = col.name.clone();
                label.push_str(": ");
                label.push_str(&col.type_name);
                if col.is_pk {
                    label.push_str(" [pk]");
                } else if col.is_fk {
                    label.push_str(" [fk]");
                }
                div()
                    .flex()
                    .items_center()
                    .px(Spacing::SM)
                    .py(px(2.0))
                    .text_size(FontSizes::XS)
                    .child(label)
            }))
    }

    /// Renders edges as CSS elements (horizontal connecting lines).
    /// Note: Full bezier edge rendering via GPUI canvas PathBuilder is not
    /// available in this codebase yet. This CSS fallback renders orthogonal connectors.
    fn render_edges_overlay(&self, layout: &LayoutResult, zoom: f32) -> Vec<Div> {
        let edges = layout.edges.clone();
        if edges.is_empty() {
            return Vec::new();
        }

        edges
            .iter()
            .filter_map(|edge| {
                let from_layout = layout.nodes.get(&edge.from_node)?;
                let to_layout = layout.nodes.get(&edge.to_node)?;

                let scale = zoom;
                let from_x = (from_layout.x + 220.0) * scale;
                let from_y = (from_layout.y + from_layout.height / 2.0) * scale;
                let to_x = to_layout.x * scale;

                // Draw a horizontal line connecting the two anchor points
                let dx = to_x - from_x;
                let line_width = dx.abs().max(1.0);
                let line_left = if dx >= 0.0 { from_x } else { to_x };

                Some(
                    div()
                        .absolute()
                        .left(px(line_left))
                        .top(px(from_y - 1.0))
                        .w(px(line_width))
                        .h(px(2.0))
                        .bg(gpui::hsla(0.0, 0.0, 0.5, 0.5)),
                )
            })
            .collect()
    }
}

impl EventEmitter<DocumentEvent> for SchemaVizDocument {}

impl Render for SchemaVizDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        match &self.load_status {
            LoadStatus::Loading => self.render_loading().into_any_element(),
            LoadStatus::Error(msg) => self.render_error(msg).into_any_element(),
            LoadStatus::NotSupported => self.render_not_supported().into_any_element(),
            LoadStatus::Ready => self.render_diagram(window, cx).into_any_element(),
        }
    }
}
