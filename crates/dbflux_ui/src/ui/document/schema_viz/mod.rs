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
use petgraph::graph::NodeIndex;

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
    // Pan state
    is_panning: bool,
    pan_start: Point<Pixels>,
    pan_offset: Point<Pixels>,
    // Node interaction
    selected_node: Option<petgraph::graph::NodeIndex>,
    pending_details_panel: Option<petgraph::graph::NodeIndex>,
    mouse_position: Point<Pixels>,
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
            is_panning: false,
            pan_start: Point::default(),
            pan_offset: Point::default(),
            selected_node: None,
            pending_details_panel: None,
            mouse_position: Point::default(),
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
        let task = cx.background_executor().spawn(async move {
            Self::load_focused_schema_blocking(database, mode, connection)
        });

        let entity = entity.clone();
        cx.spawn(async move |_entity, cx| {
            let load_result = task.await;

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

    /// Loads schema data for focused mode (blocking, runs on background executor).
    fn load_focused_schema_blocking(
        database: Option<String>,
        mode: SchemaVizMode,
        connection: Option<Arc<dyn Connection>>,
    ) -> Result<(Vec<TableInfo>, SchemaGraph, LayoutResult), String> {
        let (table, schema) = match mode {
            SchemaVizMode::Focused { table, schema } => (table, schema),
            SchemaVizMode::Global => {
                return Err("Global schema view not yet implemented".to_string());
            }
        };

        let connection = connection.ok_or_else(|| "Connection not found or not active".to_string())?;

        let metadata = connection.metadata();
        if !metadata.capabilities.contains(dbflux_core::DriverCapabilities::FOREIGN_KEYS) {
            return Err("Foreign keys not supported by this driver".to_string());
        }

        let db_name = database.ok_or_else(|| "No database specified".to_string())?;

        let focal_table = connection
            .table_details(&db_name, schema.as_deref(), &table)
            .map_err(|e| format!("Failed to fetch table details: {}", e))?;

        let mut all_table_names: HashSet<(Option<String>, String)> = HashSet::new();
        all_table_names.insert((schema.clone(), table.clone()));

        if let Some(ref fks) = focal_table.foreign_keys {
            for fk in fks {
                all_table_names.insert((fk.referenced_schema.clone(), fk.referenced_table.clone()));
            }
        }

        let mut all_tables = Vec::with_capacity(all_table_names.len());
        all_tables.push(focal_table.clone());

        for (tbl_schema, tbl_name) in &all_table_names {
            if tbl_name == &table && tbl_schema.as_deref() == schema.as_deref() {
                continue;
            }

            match connection.table_details(&db_name, tbl_schema.as_deref(), tbl_name) {
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

        let inbound_neighbors = Self::find_inbound_references(&all_tables, schema.as_deref(), &table);

        for (s, n) in inbound_neighbors {
            all_table_names.insert((Some(s), n));
        }

        for (tbl_schema, tbl_name) in &all_table_names {
            if !all_tables.iter().any(|t| {
                t.name == *tbl_name && t.schema.as_deref() == tbl_schema.as_deref()
            }) && let Ok(details) = connection.table_details(&db_name, tbl_schema.as_deref(), tbl_name) {
                all_tables.push(details);
            }
        }

        let graph = SchemaGraph::build(&all_tables);
        let focused_graph = graph.neighborhood(&table, schema.as_deref(), 1);
        let layout = dbflux_schema_viz::layout::compute_layout(&focused_graph);

        Ok((all_tables, focused_graph, layout))
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
            .bg(gpui::hsla(0.0, 0.0, 0.97, 1.0))
            .flex_col()
            .gap(Spacing::MD)
            .child(
                div()
                    .size(px(32.0))
                    .border_2()
                    .border_color(gpui::hsla(0.6, 0.6, 0.5, 0.7))
                    .rounded_full(),
            )
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .text_color(gpui::hsla(0.0, 0.0, 0.4, 0.8))
                    .child("Loading schema..."),
            )
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

    fn render_diagram(&self, _window: &mut Window, cx: &mut Context<Self>) -> Div {
        let zoom = self.zoom;
        let pan = self.pan_offset;
        let scale = zoom;

        // Grid cell size (spacing between nodes)
        let grid_size = 280.0 * scale;
        let grid_color = gpui::hsla(0.0, 0.0, 0.85, 0.5);

        let inner = match &self.layout {
            Some(layout) => {
                let total_width = layout.total_width * scale + 400.0;
                let total_height = layout.total_height * scale + 400.0;

                // Render grid background using layered divs
                let grid_lines_x: Vec<Div> = (0..((total_width / grid_size) as usize + 2))
                    .map(|i| {
                        let x = i as f32 * grid_size;
                        div()
                            .absolute()
                            .left(px(x))
                            .top(px(0.0))
                            .w(px(1.0))
                            .h(px(total_height))
                            .bg(grid_color)
                    })
                    .collect();

                let grid_lines_y: Vec<Div> = (0..((total_height / grid_size) as usize + 2))
                    .map(|i| {
                        let y = i as f32 * grid_size;
                        div()
                            .absolute()
                            .left(px(0.0))
                            .top(px(y))
                            .w(px(total_width))
                            .h(px(1.0))
                            .bg(grid_color)
                    })
                    .collect();

                div()
                    .relative()
                    .w(px(total_width))
                    .h(px(total_height))
                    .bg(gpui::hsla(0.0, 0.0, 0.97, 1.0)) // #F8F9FA
                    .children(grid_lines_x)
                    .children(grid_lines_y)
                    .children(self.render_edges_overlay(layout, scale, pan))
                    .children(self.render_nodes(layout, scale, pan, cx))
            }
            None => div()
                .size_full()
                .bg(gpui::hsla(0.0, 0.0, 0.97, 1.0))
                .child(self.render_error("No layout computed")),
        };

        // Zoom controls bar
        let zoom_controls = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .px(Spacing::MD)
            .py(px(6.0))
            .bg(gpui::hsla(0.0, 0.0, 0.98, 0.95))
            .border_b_1()
            .border_color(gpui::hsla(0.0, 0.0, 0.85, 0.3))
            .children([
                div()
                    .flex()
                    .items_center()
                    .gap(px(4.0))
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(gpui::hsla(0.0, 0.0, 0.4, 0.9))
                            .child(format!("Zoom: {:.0}%", zoom * 100.0)),
                    ),
                div()
                    .w(px(1.0))
                    .h(px(16.0))
                    .bg(gpui::hsla(0.0, 0.0, 0.85, 0.5)),
                div()
                    .cursor_pointer()
                    .px(px(8.0))
                    .py(px(2.0))
                    .rounded_sm()
                    .when(self.zoom < 4.0, |d| {
                        d.on_mouse_down(MouseButton::Left, cx.listener(|this, _, _, cx| {
                            this.zoom = (this.zoom * 1.25).min(4.0);
                            cx.notify();
                        }))
                    })
                    .child("+"),
                div()
                    .cursor_pointer()
                    .px(px(8.0))
                    .py(px(2.0))
                    .rounded_sm()
                    .when(self.zoom > 0.25, |d| {
                        d.on_mouse_down(MouseButton::Left, cx.listener(|this, _, _, cx| {
                            this.zoom = (this.zoom / 1.25).max(0.25);
                            cx.notify();
                        }))
                    })
                    .child("-"),
                div()
                    .cursor_pointer()
                    .px(px(8.0))
                    .py(px(2.0))
                    .rounded_sm()
                    .on_mouse_down(MouseButton::Left, cx.listener(|this, _, _, cx| {
                        this.zoom = 1.0;
                        this.pan_offset = Point::default();
                        cx.notify();
                    }))
                    .child("Reset"),
            ]);

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(gpui::hsla(0.0, 0.0, 0.97, 1.0))
            .child(zoom_controls)
            .child(
                div()
                    .flex_1()
                    .relative()
                    .overflow_hidden()
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, event: &MouseDownEvent, _, _cx| {
                            if event.click_count == 1 {
                                this.is_panning = true;
                                this.pan_start = event.position;
                            }
                        }),
                    )
                    .on_mouse_move(cx.listener(|this, event: &MouseMoveEvent, _, _cx| {
                        this.mouse_position = event.position;
                        if !this.is_panning {
                            return;
                        }
                        let dx = event.position.x - this.pan_start.x;
                        let dy = event.position.y - this.pan_start.y;
                        this.pan_offset = Point::new(
                            this.pan_offset.x + dx,
                            this.pan_offset.y + dy,
                        );
                        this.pan_start = event.position;
                    }))
                    .on_mouse_up(MouseButton::Left, cx.listener(|this, _, _, _cx| {
                        this.is_panning = false;
                    }))
                    .child(
                        div()
                            .size_full()
                            .overflow_scrollbar()
                            .track_focus(&self.focus_handle)
                            .on_scroll_wheel(cx.listener(|this, event: &ScrollWheelEvent, _, cx| {
                                let delta = event.delta.pixel_delta(px(1.0)).y;
                                let factor = if delta > px(0.0) { 0.9_f32 } else { 1.1_f32 };
                                this.zoom = (this.zoom * factor).clamp(0.25, 4.0);
                                cx.notify();
                            }))
                            .child(inner),
                    ),
            )
    }

    fn render_nodes(&self, layout: &LayoutResult, zoom: f32, pan: Point<Pixels>, cx: &mut Context<Self>) -> Vec<Div> {
        let Some(graph) = &self.graph else {
            return Vec::new();
        };

        let mouse_pos = self.mouse_position;

        graph
            .nodes()
            .filter_map(|(idx, node)| {
                let node_layout = layout.nodes.get(&idx)?;
                Some(self.render_node(node, node_layout, zoom, pan, idx, cx, mouse_pos))
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
        cx: &mut Context<Self>,
        mouse_pos: Point<Pixels>,
    ) -> Div {
        let scale = zoom;
        let x = layout.x * scale;
        let y = layout.y * scale;
        let width = layout.width * scale;
        let height = layout.height * scale;

        // Hit-test for hover: is the mouse currently over this node?
        let node_left = px(x) + pan.x;
        let node_top = px(y) + pan.y;
        let node_right = node_left + px(width);
        let node_bottom = node_top + px(height);

        let is_hovered = mouse_pos.x >= node_left
            && mouse_pos.x <= node_right
            && mouse_pos.y >= node_top
            && mouse_pos.y <= node_bottom;
        let is_selected = self.selected_node.as_ref() == Some(&node_idx);

        // Border color changes on hover/select
        let border_color = if is_selected {
            gpui::hsla(0.6, 0.8, 0.5, 0.8) // blue selection
        } else if is_hovered {
            gpui::hsla(0.6, 0.6, 0.5, 0.7) // blue hover
        } else {
            gpui::hsla(0.0, 0.0, 0.5, 0.3)
        };

        // Schema badge if non-default
        let schema_badge = node.id.schema.as_ref().map(|s| {
            div()
                .text_size(FontSizes::SM)
                .text_color(gpui::hsla(0.0, 0.0, 0.5, 0.7))
                .child(s.clone())
        });

        let node_idx_clone = node_idx;
        div()
            .absolute()
            .left(px(x) + pan.x)
            .top(px(y) + pan.y)
            .w(px(width))
            .h(px(height))
            .border_1()
            .border_color(border_color)
            .rounded_md()
            .bg(gpui::white())
            .shadow_sm()
            .cursor_pointer()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _event, _, _cx| {
                    this.selected_node = Some(node_idx_clone);
                    this.pending_details_panel = Some(node_idx_clone);
                }),
            )
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
                    .bg(if is_selected {
                        gpui::hsla(0.6, 0.5, 0.95, 0.9)
                    } else {
                        gpui::hsla(0.0, 0.0, 0.5, 0.1)
                    })
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
    fn render_edges_overlay(
        &self,
        layout: &LayoutResult,
        zoom: f32,
        pan: Point<Pixels>,
    ) -> Vec<Div> {
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
                let from_x = px((from_layout.x + from_layout.width) * scale) + pan.x;
                let from_y = px((from_layout.y + from_layout.height / 2.0) * scale) + pan.y;
                let to_x = px((to_layout.x + to_layout.width) * scale) + pan.x;

                // Draw a horizontal line connecting the two anchor points
                let dx = to_x - from_x;
                let line_width = dx.abs().max(px(1.0));
                let line_left = if dx >= px(0.0) { from_x } else { to_x };

                Some(
                    div()
                        .absolute()
                        .left(line_left)
                        .top(from_y - px(1.0))
                        .w(line_width)
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
