//! `SchemaInspector` — content entity rendered inside the workspace-level
//! inspector rail when the user double-clicks a table node in the schema-viz
//! document (or selects "Inspect schema" from the context menu).
//!
//! Pure content: no chrome, no resize, no close button. The frame is owned by
//! `WorkspaceInspector`. Laid out as P1Schema's rail: a one-line summary,
//! then the columns, indexes, foreign keys and the tables that reference
//! this one.

use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{ChromeColors, SchemaInspectorMetrics, SyntaxColors};
use dbflux_components::typography::AppFonts;
use dbflux_schema_viz::graph::{FkEdge, IndexSummary, TableNode};
use gpui::prelude::*;
use gpui::{
    App, Context, EventEmitter, FocusHandle, Focusable, Hsla, IntoElement, Render, SharedString,
    Window, div,
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
    /// Columns of other tables whose foreign keys point at this table, as
    /// `table.column`.
    pub referenced_by: Vec<String>,
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
        let syntax = SyntaxColors::for_current(cx);
        let node = self.snapshot.node.clone();
        let outgoing = self.snapshot.outgoing_fks.clone();
        let referenced_by = self.snapshot.referenced_by.clone();

        let summary = dbflux_i18n::t!(
            "document.schema_viz.inspector.summary_full",
            columns = node.columns.len(),
            indexes = node.indexes.len(),
            foreign_keys = outgoing.len(),
            referenced_by = referenced_by.len()
        );

        let key_icon_color = |column: &dbflux_schema_viz::graph::ColumnSummary| {
            if column.is_pk {
                Some((AppIcon::KeyRound, theme.warning))
            } else if column.is_fk {
                Some((AppIcon::Link2, theme.info))
            } else {
                None
            }
        };

        let column_rows = node
            .columns
            .iter()
            .map(|column| {
                mono_row(
                    key_icon_color(column),
                    column.name.clone(),
                    Some(column.type_name.clone()),
                    theme,
                )
                .into_any_element()
            })
            .collect::<Vec<_>>();

        let index_rows = node
            .indexes
            .iter()
            .map(|index| {
                mono_row(
                    Some((AppIcon::Hash, syntax.number)),
                    index_label(index),
                    index
                        .unique
                        .then(|| dbflux_i18n::t!("document.schema_viz.inspector.unique")),
                    theme,
                )
                .into_any_element()
            })
            .collect::<Vec<_>>();

        let fk_rows = outgoing
            .iter()
            .map(|fk| {
                mono_row(
                    Some((AppIcon::Link2, theme.info)),
                    fk_label(fk),
                    None,
                    theme,
                )
                .into_any_element()
            })
            .collect::<Vec<_>>();

        let referenced_rows = referenced_by
            .iter()
            .map(|reference| mono_row(None, reference.clone(), None, theme).into_any_element())
            .collect::<Vec<_>>();

        div()
            .id("schema-inspector-content")
            .size_full()
            .flex()
            .flex_col()
            .gap(SchemaInspectorMetrics::GAP)
            .px(SchemaInspectorMetrics::PADDING_X)
            .py(SchemaInspectorMetrics::PADDING_Y)
            .overflow_y_scroll()
            .track_focus(&self.focus_handle)
            .child(
                div()
                    .text_size(SchemaInspectorMetrics::SUMMARY_FONT)
                    .text_color(theme.muted_foreground)
                    .child(summary),
            )
            .child(section(
                dbflux_i18n::t!("document.schema_viz.inspector.columns"),
                column_rows,
            ))
            .when(!index_rows.is_empty(), |d| {
                d.child(section(
                    dbflux_i18n::t!("document.schema_viz.inspector.indexes"),
                    index_rows,
                ))
            })
            .when(!fk_rows.is_empty(), |d| {
                d.child(section(
                    dbflux_i18n::t!("document.schema_viz.inspector.foreign_keys"),
                    fk_rows,
                ))
            })
            .when(!referenced_rows.is_empty(), |d| {
                d.child(section(
                    dbflux_i18n::t!("document.schema_viz.inspector.referenced_by"),
                    referenced_rows,
                ))
            })
    }
}

/// A section of the inspector: the uppercase label over its rows.
fn section(label: String, rows: Vec<gpui::AnyElement>) -> impl IntoElement {
    div()
        .flex()
        .flex_col()
        .gap(SchemaInspectorMetrics::ROW_GAP)
        .child(Text::label(label).font_size(SchemaInspectorMetrics::LABEL_FONT))
        .children(rows)
}

/// A 12 px mono row: an optional 11 px icon, the value in the strong color
/// and an optional muted trailing note (a column type, "unique").
fn mono_row(
    icon: Option<(AppIcon, Hsla)>,
    value: String,
    trailing: Option<String>,
    theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    div()
        .flex()
        .items_center()
        .gap(SchemaInspectorMetrics::ICON_GAP)
        .font_family(AppFonts::MONO)
        .text_size(SchemaInspectorMetrics::ROW_FONT)
        .when_some(icon, |row, (icon, color)| {
            row.child(
                Icon::new(icon)
                    .size(SchemaInspectorMetrics::ICON)
                    .color(color),
            )
        })
        .child(
            div()
                .flex_1()
                .min_w_0()
                .truncate()
                .text_color(ChromeColors::strong(theme))
                .child(SharedString::from(value)),
        )
        .when_some(trailing, |row, trailing| {
            row.child(
                div()
                    .flex_shrink_0()
                    .text_color(theme.muted_foreground)
                    .child(trailing),
            )
        })
}

/// `name (col, col)`.
fn index_label(index: &IndexSummary) -> String {
    format!("{} ({})", index.name, index.columns.join(", "))
}

/// `from → target.to`.
fn fk_label(fk: &OutgoingFk) -> String {
    let target = match &fk.target_schema {
        Some(schema) => format!("{}.{}", schema, fk.target_table),
        None => fk.target_table.clone(),
    };

    format!(
        "{} \u{2192} {}.{}",
        fk.from_columns.join(", "),
        target,
        fk.to_columns.join(", "),
    )
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

    let referenced_by: Vec<String> = graph
        .edge_indices()
        .filter_map(|edge_idx| {
            let (source, target) = graph.edge_endpoints(edge_idx)?;
            if target != node_idx {
                return None;
            }
            let source_node = graph.node_weight(source)?;
            let fk: &FkEdge = graph.edge_weight(edge_idx)?;
            Some(format!(
                "{}.{}",
                source_node.id.name,
                fk.from_columns.join(", ")
            ))
        })
        .collect();

    Some(SchemaInspectorSnapshot {
        node,
        outgoing_fks,
        referenced_by,
    })
}
