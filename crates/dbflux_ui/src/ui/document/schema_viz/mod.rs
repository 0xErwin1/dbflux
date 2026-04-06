use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::{Scrollable, ScrollableElement};

use dbflux_core::{Connection, DbSchemaInfo, TableInfo, TaskTarget};
use dbflux_schema_viz::{
    graph::SchemaGraph,
    layout::{LayoutResult, NodeLayout},
};
use petgraph::graph::NodeIndex;
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
    Focused {
        table: String,
        schema: Option<String>,
    },
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
    // Drag state for node repositioning
    dragging_node: Option<petgraph::graph::NodeIndex>,
    drag_offset: Point<Pixels>,
    node_position_overrides: std::collections::HashMap<petgraph::graph::NodeIndex, Point<f32>>,
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
            is_panning: false,
            pan_start: Point::default(),
            pan_offset: Point::default(),
            selected_node: None,
            pending_details_panel: None,
            dragging_node: None,
            drag_offset: Point::default(),
            node_position_overrides: std::collections::HashMap::new(),
        };

        // Spawn async loading task with the correct per-database connection
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
        let task = cx
            .background_executor()
            .spawn(async move { Self::load_focused_schema_blocking(database, mode, connection) });

        let entity = entity.clone();
        cx.spawn(async move |_entity, cx| {
            let load_result = task.await;

            if let Err(error) = cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    match load_result {
                        Ok((tables, graph, layout)) => {
                            let (focal_table, focal_schema) = match &doc.mode {
                                SchemaVizMode::Focused { table, schema } => {
                                    (table.clone(), schema.clone())
                                }
                                SchemaVizMode::Global => ("".to_string(), None),
                            };
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
                            if let Some(pan) = initial_pan {
                                doc.pan_offset = pan;
                            }
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
    /// The connection must be the correct per-database connection (obtained via
    /// `connection_for_task_target` in `SchemaVizDocument::new`), not the primary connection.
    #[allow(clippy::collapsible_if)]
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

        let connection =
            connection.ok_or_else(|| "Connection not found or not active".to_string())?;

        let db_name = database.ok_or_else(|| "No database specified".to_string())?;

        let metadata = connection.metadata();
        if !metadata
            .capabilities
            .contains(dbflux_core::DriverCapabilities::FOREIGN_KEYS)
        {
            return Err("Foreign keys not supported by this driver".to_string());
        }

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
                if let Ok(details) =
                    connection.table_details(&db_name, tbl_schema.as_deref(), tbl_name)
                {
                    all_tables.push(details);
                }
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

    fn render_diagram(&self, _window: &mut Window, cx: &mut Context<Self>) -> Div {
        let zoom = self.zoom;
        let pan = self.pan_offset;

        let theme = cx.theme().clone();
        let background = theme.background;
        let tab_bar = theme.tab_bar;
        let border = theme.border;
        let muted_foreground = theme.muted_foreground;

        log::info!(
            "DEBUG render_diagram: self.layout is {} self.graph is {}",
            if self.layout.is_some() {
                "Some"
            } else {
                "None"
            },
            if self.graph.is_some() { "Some" } else { "None" }
        );

        let inner = match &self.layout {
            Some(layout) => {
                let grid_size = 280.0;
                let total_width = layout.total_width + 400.0;
                let total_height = layout.total_height + 400.0;
                let grid_color = border.opacity(0.5);

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

                let scaled_width = layout.total_width * zoom;
                let scaled_height = layout.total_height * zoom;
                let zoomed_layer = div()
                    .absolute()
                    .left(pan.x)
                    .top(pan.y)
                    .w(px(scaled_width))
                    .h(px(scaled_height))
                    .children(self.render_edges_overlay(layout, zoom, pan, &theme))
                    .children(self.render_nodes(layout, zoom, pan, &theme, cx));

                div()
                    .relative()
                    .w(px(total_width))
                    .h(px(total_height))
                    .bg(background)
                    .children(grid_lines_x)
                    .children(grid_lines_y)
                    .child(zoomed_layer)
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
            .children([
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
            ]);

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(background)
            .child(zoom_controls)
            .child(
                div()
                    .flex_1()
                    .relative()
                    .overflow_hidden()
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
                            let new_screen_x: f32 = event.position.x.into();
                            let new_screen_y: f32 = event.position.y.into();
                            let drag_offset_x: f32 = this.drag_offset.x.into();
                            let drag_offset_y: f32 = this.drag_offset.y.into();
                            let zoom = this.zoom;
                            let pan_x: f32 = this.pan_offset.x.into();
                            let pan_y: f32 = this.pan_offset.y.into();
                            let new_graph_x = (new_screen_x - drag_offset_x - pan_x) / zoom;
                            let new_graph_y = (new_screen_y - drag_offset_y - pan_y) / zoom;
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
                    .child(
                        div()
                            .size_full()
                            .overflow_scrollbar()
                            .track_focus(&self.focus_handle)
                            .on_scroll_wheel(cx.listener(
                                |this, event: &ScrollWheelEvent, _, _cx| {
                                    let mouse_screen = event.position;
                                    let old_zoom = this.zoom;
                                    let delta = event.delta.pixel_delta(px(1.0)).y;
                                    let factor = if delta > px(0.0) { 1.1_f32 } else { 0.9_f32 };
                                    let new_zoom = (old_zoom * factor).clamp(0.25, 4.0);

                                    if (new_zoom - old_zoom).abs() < 0.001 {
                                        return;
                                    }

                                    let pan = this.pan_offset;

                                    let content_x = (mouse_screen.x - pan.x) / old_zoom;
                                    let content_y = (mouse_screen.y - pan.y) / old_zoom;

                                    let new_pan_x = mouse_screen.x - content_x * new_zoom;
                                    let new_pan_y = mouse_screen.y - content_y * new_zoom;

                                    this.pan_offset = Point::new(new_pan_x, new_pan_y);
                                    this.zoom = new_zoom;
                                },
                            ))
                            .child(inner),
                    ),
            )
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
        _pan: Point<Pixels>,
        node_idx: petgraph::graph::NodeIndex,
        theme: &gpui_component::theme::Theme,
        dragging_node: Option<petgraph::graph::NodeIndex>,
        cx: &mut Context<Self>,
    ) -> Div {
        let width = layout.width;
        let height = layout.height;

        let position_override = self.node_position_overrides.get(&node_idx);
        let (node_left, node_top) = if let Some(pos) = position_override {
            (px(pos.x * zoom), px(pos.y * zoom))
        } else {
            (px(layout.x * zoom), px(layout.y * zoom))
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

        // Schema badge: "schema · table_name" with schema in primary color, separator muted, table name bold
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
            .w(px(width.max(180.0)))
            .h(px(height))
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
                            .layout
                            .as_ref()
                            .and_then(|l| l.nodes.get(&node_idx_clone))
                            .map(|n| n.x)
                            .unwrap_or(0.0);
                        let node_y = this
                            .layout
                            .as_ref()
                            .and_then(|l| l.nodes.get(&node_idx_clone))
                            .map(|n| n.y)
                            .unwrap_or(0.0);
                        let node_screen_x = px(node_x * zoom) + this.pan_offset.x;
                        let node_screen_y = px(node_y * zoom) + this.pan_offset.y;
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
                    .pt(px(2.0))
                    .px(px(10.0))
                    .py(px(4.0))
                    .children(node.columns.iter().map(|col| {
                        let type_label = if col.is_pk {
                            format!("{} [pk]", col.type_name)
                        } else if col.is_fk {
                            format!("{} [fk]", col.type_name)
                        } else {
                            col.type_name.clone()
                        };
                        div()
                            .flex()
                            .items_center()
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
                                    .w(px(80.0))
                                    .overflow_hidden()
                                    .text_ellipsis()
                                    .text_color(muted_fg)
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
        _pan: Point<Pixels>,
        theme: &gpui_component::theme::Theme,
    ) -> Vec<Div> {
        let edge_color = theme.muted_foreground.opacity(0.5);

        let edges = layout.edges.clone();
        if edges.is_empty() {
            return Vec::new();
        }

        let mut segments = Vec::new();

        for edge in &edges {
            let from_layout = match layout.nodes.get(&edge.from_node) {
                Some(l) => l,
                None => continue,
            };
            let to_layout = match layout.nodes.get(&edge.to_node) {
                Some(l) => l,
                None => continue,
            };

            // Use override positions if available
            let (from_x_base, from_y_base) = if let Some(pos) =
                self.node_position_overrides.get(&edge.from_node)
            {
                (pos.x, pos.y)
            } else {
                (from_layout.x, from_layout.y)
            };
            let (to_x_base, to_y_base) = if let Some(pos) =
                self.node_position_overrides.get(&edge.to_node)
            {
                (pos.x, pos.y)
            } else {
                (to_layout.x, to_layout.y)
            };

            // Source: right edge of from_node, vertical center
            let from_x = (from_x_base + from_layout.width) * zoom;
            let from_y = (from_y_base + from_layout.height / 2.0) * zoom;
            // Target: left edge of to_node, vertical center
            let to_x = to_x_base * zoom;
            let to_y = (to_y_base + to_layout.height / 2.0) * zoom;

            // Midpoint x for L-shaped connector
            let mid_x = (from_x + to_x) / 2.0;

            // Segment 1: horizontal from source right to mid_x
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

            // Segment 2: vertical from from_y to to_y at mid_x
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

            // Segment 3: horizontal from mid_x to target left
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
        match &self.load_status {
            LoadStatus::Loading => self.render_loading(cx).into_any_element(),
            LoadStatus::Error(msg) => self.render_error(msg).into_any_element(),
            LoadStatus::NotSupported => self.render_not_supported().into_any_element(),
            LoadStatus::Ready => self.render_diagram(window, cx).into_any_element(),
        }
    }
}
