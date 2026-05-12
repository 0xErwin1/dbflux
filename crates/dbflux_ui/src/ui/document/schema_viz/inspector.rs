//! `SchemaInspector` — content entity rendered inside the workspace-level
//! inspector rail when the user double-clicks a table node in the schema-viz
//! document (or selects "Inspect schema" from the context menu).
//!
//! Pure content: no chrome, no resize, no close button. The frame is owned by
//! `WorkspaceInspector`.

use dbflux_components::primitives::Text;
use dbflux_components::tokens::{FontSizes, Radii, Spacing};
use dbflux_schema_viz::graph::{FkEdge, IndexSummary, TableNode};
use gpui::prelude::*;
use gpui::{
    App, Context, EventEmitter, FocusHandle, Focusable, Hsla, IntoElement, Render, Window, div, px,
};
use gpui_component::theme::ActiveTheme;

#[derive(Clone, Debug)]
pub struct OutgoingFk {
    pub fk_name: String,
    pub from_columns: Vec<String>,
    pub target_schema: Option<String>,
    pub target_table: String,
    pub to_columns: Vec<String>,
}

/// Snapshot of a table for the schema inspector. Built on the schema-viz side
/// and handed to the content entity; the entity does not look at the graph
/// directly so it can be opened against any schema source.
#[derive(Clone, Debug)]
pub struct SchemaInspectorSnapshot {
    pub node: TableNode,
    pub outgoing_fks: Vec<OutgoingFk>,
}

pub struct SchemaInspector {
    snapshot: SchemaInspectorSnapshot,
    focus_handle: FocusHandle,
}

#[derive(Clone, Debug)]
pub enum SchemaInspectorEvent {}

impl EventEmitter<SchemaInspectorEvent> for SchemaInspector {}

impl SchemaInspector {
    pub fn new(snapshot: SchemaInspectorSnapshot, cx: &mut Context<Self>) -> Self {
        Self {
            snapshot,
            focus_handle: cx.focus_handle(),
        }
    }

    pub fn open(&mut self, snapshot: SchemaInspectorSnapshot, cx: &mut Context<Self>) {
        self.snapshot = snapshot;
        cx.notify();
    }
}

impl Focusable for SchemaInspector {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for SchemaInspector {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let node = self.snapshot.node.clone();
        let outgoing = self.snapshot.outgoing_fks.clone();
        let has_indexes = !node.indexes.is_empty();
        let has_outgoing = !outgoing.is_empty();

        div()
            .id("schema-inspector-content")
            .size_full()
            .flex()
            .flex_col()
            .overflow_y_scroll()
            .track_focus(&self.focus_handle)
            .child(render_section_header("TABLE", theme))
            .child(render_table_summary(&node, theme))
            .child(render_section_header("COLUMNS", theme))
            .children(node.columns.iter().map(|col| render_column_row(col, theme)))
            .when(has_indexes, |d| {
                d.child(render_section_header("INDEXES", theme))
                    .children(node.indexes.iter().map(|idx| render_index_row(idx, theme)))
            })
            .when(has_outgoing, |d| {
                d.child(render_section_header("FOREIGN KEYS", theme))
                    .children(outgoing.iter().map(|fk| render_outgoing_fk(fk, theme)))
            })
    }
}

fn render_section_header(
    label: &'static str,
    theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    div()
        .px(Spacing::SM)
        .py(Spacing::XS)
        .border_b_1()
        .border_color(theme.border)
        .bg(theme.secondary.opacity(0.5))
        .child(
            Text::caption(label)
                .font_size(FontSizes::XS)
                .color(theme.muted_foreground),
        )
}

fn render_table_summary(
    node: &TableNode,
    theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    let qualified_name = match &node.id.schema {
        Some(s) => format!("{}.{}", s, node.id.name),
        None => node.id.name.clone(),
    };
    let col_count = node.columns.len();
    let idx_count = node.indexes.len();

    div()
        .flex()
        .flex_col()
        .gap(Spacing::XS)
        .px(Spacing::SM)
        .py(Spacing::SM)
        .border_b_1()
        .border_color(theme.border.opacity(0.5))
        .child(
            div()
                .text_size(FontSizes::SM)
                .text_color(theme.foreground)
                .font_weight(gpui::FontWeight::BOLD)
                .child(qualified_name),
        )
        .child(
            div()
                .text_size(FontSizes::XS)
                .text_color(theme.muted_foreground)
                .child(format!("{} columns · {} indexes", col_count, idx_count)),
        )
}

fn render_column_row(
    col: &dbflux_schema_viz::graph::ColumnSummary,
    theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    let is_nn = !col.nullable && !col.is_pk;
    let pk_color = theme.primary;
    let muted = theme.muted_foreground;

    div()
        .flex()
        .items_center()
        .gap(Spacing::XS)
        .px(Spacing::SM)
        .py(px(4.0))
        .border_b_1()
        .border_color(theme.border.opacity(0.5))
        .child(
            div()
                .flex_1()
                .min_w(px(0.0))
                .overflow_hidden()
                .text_ellipsis()
                .text_size(FontSizes::SM)
                .text_color(theme.foreground)
                .font_weight(gpui::FontWeight::MEDIUM)
                .child(col.name.clone()),
        )
        .when(col.is_pk, |d| d.child(badge("PK", true, pk_color, theme)))
        .when(col.is_fk, |d| d.child(badge("FK", false, pk_color, theme)))
        .when(is_nn, |d| {
            d.child(badge("NN", true, muted.opacity(0.5), theme))
        })
        .child(
            div()
                .flex_shrink_0()
                .text_size(FontSizes::XS)
                .text_color(muted)
                .child(col.type_name.clone()),
        )
}

fn render_index_row(idx: &IndexSummary, theme: &gpui_component::theme::Theme) -> impl IntoElement {
    let cols = idx.columns.join(", ");
    let label = format!("{} ({})", idx.name, cols);

    div()
        .flex()
        .items_center()
        .gap(Spacing::XS)
        .px(Spacing::SM)
        .py(px(4.0))
        .border_b_1()
        .border_color(theme.border.opacity(0.5))
        .child(
            div()
                .flex_1()
                .min_w(px(0.0))
                .overflow_hidden()
                .text_ellipsis()
                .text_size(FontSizes::SM)
                .text_color(theme.foreground)
                .child(label),
        )
        .when(idx.unique, |d| {
            d.child(badge("UNIQUE", false, theme.primary, theme))
        })
}

fn render_outgoing_fk(fk: &OutgoingFk, theme: &gpui_component::theme::Theme) -> impl IntoElement {
    let target = match &fk.target_schema {
        Some(s) => format!("{}.{}", s, fk.target_table),
        None => fk.target_table.clone(),
    };
    let arrow = format!(
        "{} → {}.{}",
        fk.from_columns.join(", "),
        target,
        fk.to_columns.join(", "),
    );

    div()
        .flex()
        .flex_col()
        .gap(px(2.0))
        .px(Spacing::SM)
        .py(px(6.0))
        .border_b_1()
        .border_color(theme.border.opacity(0.5))
        .child(
            div()
                .text_size(FontSizes::XS)
                .text_color(theme.muted_foreground)
                .child(fk.fk_name.clone()),
        )
        .child(
            div()
                .text_size(FontSizes::SM)
                .text_color(theme.foreground)
                .font_weight(gpui::FontWeight::MEDIUM)
                .child(arrow),
        )
}

fn badge(
    label: &str,
    filled: bool,
    color: Hsla,
    _theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    let base = div()
        .px(px(5.0))
        .py(px(1.0))
        .rounded(Radii::FULL)
        .text_size(FontSizes::XS)
        .font_weight(gpui::FontWeight::BOLD)
        .line_height(px(14.0));

    if filled {
        base.bg(color).text_color(gpui::white())
    } else {
        base.border_1().border_color(color).text_color(color)
    }
    .child(label.to_owned())
}

/// Build a snapshot of `node_idx` from a schema graph, collecting outgoing FKs.
pub fn snapshot_for_node(
    graph: &dbflux_schema_viz::graph::SchemaGraph,
    node_idx: petgraph::graph::NodeIndex,
) -> Option<SchemaInspectorSnapshot> {
    let node = graph.node_weight(node_idx)?.clone();

    let outgoing_fks: Vec<OutgoingFk> = graph
        .edge_indices()
        .filter_map(|edge_idx| {
            let (source, target) = graph.edge_endpoints(edge_idx)?;
            if source != node_idx {
                return None;
            }
            let target_node = graph.node_weight(target)?;
            let fk: &FkEdge = graph.edge_weight(edge_idx)?;
            Some(OutgoingFk {
                fk_name: fk.name.clone(),
                from_columns: fk.from_columns.clone(),
                target_schema: target_node.id.schema.clone(),
                target_table: target_node.id.name.clone(),
                to_columns: fk.to_columns.clone(),
            })
        })
        .collect();

    Some(SchemaInspectorSnapshot { node, outgoing_fks })
}
