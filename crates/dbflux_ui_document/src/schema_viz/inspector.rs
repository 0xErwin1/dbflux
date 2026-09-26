//! `SchemaInspector` — the table details panel mounted in the workspace-level
//! inspector rail when the user selects a table node in the schema-viz
//! document (or picks "Inspect schema" from the context menu).
//!
//! Laid out as IslSchema's rail: a header with the qualified table name and a
//! close button, a one-line summary, then the indexes, foreign keys and the
//! tables that reference this one. The rail owns only the island and the
//! resize grip; this content draws its own header.

use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{ChromeColors, SchemaInspectorMetrics, SyntaxColors};
use dbflux_components::typography::AppFonts;
use dbflux_schema_viz::graph::{FkEdge, IndexSummary, TableNode};
use gpui::prelude::*;
use gpui::{
    AnyElement, App, Context, EventEmitter, FocusHandle, Focusable, FontWeight, Hsla, IntoElement,
    Render, SharedString, Window, div,
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

impl SchemaInspectorSnapshot {
    /// The table name, schema-qualified when the schema is known.
    pub fn qualified_name(&self) -> String {
        match &self.node.id.schema {
            Some(schema) => format!("{}.{}", schema, self.node.id.name),
            None => self.node.id.name.clone(),
        }
    }

    /// "5 columns · 3 indexes · 1 foreign key · referenced by 2".
    pub fn summary(&self) -> String {
        dbflux_i18n::t!(
            "document.schema_viz.inspector.summary_full",
            columns = crate::labels::schema_inspector_columns(self.node.columns.len()),
            indexes = crate::labels::schema_inspector_indexes(self.node.indexes.len()),
            foreign_keys = crate::labels::schema_inspector_foreign_keys(self.outgoing_fks.len()),
            referenced_by = self.referenced_by.len()
        )
    }
}

pub struct SchemaInspector {
    snapshot: SchemaInspectorSnapshot,
    focus_handle: FocusHandle,
}

/// Requests from the inspector's buttons; the owning diagram carries them out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaInspectorEvent {
    /// The close button: dismiss the rail.
    Close,
}

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

    #[cfg(test)]
    pub fn snapshot(&self) -> &SchemaInspectorSnapshot {
        &self.snapshot
    }

    fn render_header(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(SchemaInspectorMetrics::HEADER_GAP)
            .h(SchemaInspectorMetrics::HEADER_HEIGHT)
            .px(SchemaInspectorMetrics::HEADER_PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .child(
                Icon::new(AppIcon::Table)
                    .size(SchemaInspectorMetrics::HEADER_ICON)
                    .color(ChromeColors::tint(theme)),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .font_family(AppFonts::MONO)
                    .font_weight(FontWeight::BOLD)
                    .text_color(ChromeColors::strong(theme))
                    .child(self.snapshot.qualified_name()),
            )
            .child(
                Button::new(
                    "schema-inspector-close",
                    dbflux_i18n::t!("document.data.row_inspector.action.close"),
                )
                .icon(AppIcon::CircleX)
                .icon_only()
                .tab_stop(false)
                .on_click(cx.listener(|_, _, _, cx| {
                    cx.emit(SchemaInspectorEvent::Close);
                })),
            )
            .into_any_element()
    }
}

impl Focusable for SchemaInspector {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for SchemaInspector {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let header = self.render_header(cx);

        let index_color = SyntaxColors::for_current(cx).number;
        let theme = cx.theme();
        let summary = self.snapshot.summary();

        let index_rows: Vec<AnyElement> = self
            .snapshot
            .node
            .indexes
            .iter()
            .map(|index| {
                mono_row(
                    Some((AppIcon::Hash, index_color)),
                    index_label(index),
                    theme,
                )
                .into_any_element()
            })
            .collect();

        let own_schema = self.snapshot.node.id.schema.as_deref();
        let fk_rows: Vec<AnyElement> = self
            .snapshot
            .outgoing_fks
            .iter()
            .map(|fk| {
                mono_row(
                    Some((AppIcon::Cable, theme.info)),
                    fk_label(fk, own_schema),
                    theme,
                )
                .into_any_element()
            })
            .collect();

        let referenced_by = self.snapshot.referenced_by.clone();

        let mut sections: Vec<AnyElement> = Vec::new();

        if !index_rows.is_empty() {
            sections.push(section_label(dbflux_i18n::t!(
                "document.schema_viz.inspector.indexes"
            )));
            sections.extend(index_rows);
        }

        if !fk_rows.is_empty() {
            sections.push(section_label(dbflux_i18n::t!(
                "document.schema_viz.inspector.foreign_keys"
            )));
            sections.extend(fk_rows);
        }

        if !referenced_by.is_empty() {
            sections.push(section_label(dbflux_i18n::t!(
                "document.schema_viz.inspector.referenced_by"
            )));
            sections.push(
                div()
                    .flex()
                    .flex_col()
                    .font_family(AppFonts::MONO)
                    .text_size(SchemaInspectorMetrics::ROW_FONT)
                    .text_color(ChromeColors::strong(theme))
                    .children(referenced_by.into_iter().map(SharedString::from))
                    .into_any_element(),
            );
        }

        div()
            .id("schema-inspector-content")
            .size_full()
            .flex()
            .flex_col()
            .track_focus(&self.focus_handle)
            .child(header)
            .child(
                div()
                    .id("schema-inspector-body")
                    .flex_1()
                    .min_h_0()
                    .flex()
                    .flex_col()
                    .gap(SchemaInspectorMetrics::GAP)
                    .px(SchemaInspectorMetrics::PADDING_X)
                    .py(SchemaInspectorMetrics::PADDING_Y)
                    .overflow_y_scroll()
                    .child(
                        div()
                            .text_size(SchemaInspectorMetrics::SUMMARY_FONT)
                            .text_color(theme.muted_foreground)
                            .child(summary),
                    )
                    .children(sections),
            )
    }
}

/// The uppercase label over a group of rows.
fn section_label(label: String) -> AnyElement {
    Text::label(label)
        .font_size(SchemaInspectorMetrics::LABEL_FONT)
        .into_any_element()
}

/// A 12 px mono row: an optional 11 px icon and the value in the strong color.
fn mono_row(
    icon: Option<(AppIcon, Hsla)>,
    value: String,
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
}

/// `name (col, col)`.
fn index_label(index: &IndexSummary) -> String {
    format!("{} ({})", index.name, index.columns.join(", "))
}

/// `from → target.to`, the target schema-qualified only when it lives in
/// another schema than the inspected table.
fn fk_label(fk: &OutgoingFk, own_schema: Option<&str>) -> String {
    let target = match fk.target_schema.as_deref() {
        Some(schema) if Some(schema) != own_schema => format!("{}.{}", schema, fk.target_table),
        _ => fk.target_table.clone(),
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

#[cfg(test)]
mod tests {
    use super::{OutgoingFk, SchemaInspectorSnapshot, fk_label};
    use dbflux_schema_viz::graph::{ColumnSummary, IndexSummary, TableNode, TableNodeId};

    fn column(name: &str) -> ColumnSummary {
        ColumnSummary {
            name: name.to_owned(),
            type_name: "int8".to_owned(),
            is_pk: false,
            is_fk: false,
            nullable: false,
        }
    }

    fn orders_fk(target_schema: Option<&str>) -> OutgoingFk {
        OutgoingFk {
            fk_name: "orders_customer_fk".to_owned(),
            from_columns: vec!["customer_id".to_owned()],
            target_schema: target_schema.map(str::to_owned),
            target_table: "customers".to_owned(),
            to_columns: vec!["id".to_owned()],
        }
    }

    fn orders_snapshot() -> SchemaInspectorSnapshot {
        SchemaInspectorSnapshot {
            node: TableNode {
                id: TableNodeId {
                    schema: Some("public".to_owned()),
                    name: "orders".to_owned(),
                },
                columns: ["id", "customer_id", "status", "total", "created_at"]
                    .into_iter()
                    .map(column)
                    .collect(),
                indexes: ["orders_pkey", "orders_status_idx", "orders_created_idx"]
                    .into_iter()
                    .map(|name| IndexSummary {
                        name: name.to_owned(),
                        columns: vec!["id".to_owned()],
                        unique: false,
                    })
                    .collect(),
            },
            outgoing_fks: vec![orders_fk(Some("public"))],
            referenced_by: vec![
                "order_items.order_id".to_owned(),
                "payments.order_id".to_owned(),
            ],
        }
    }

    #[test]
    fn summary_counts_each_part_with_its_own_plural() {
        assert_eq!(
            orders_snapshot().summary(),
            "5 columns \u{b7} 3 indexes \u{b7} 1 foreign key \u{b7} referenced by 2"
        );
    }

    #[test]
    fn qualified_name_prefixes_the_schema() {
        assert_eq!(orders_snapshot().qualified_name(), "public.orders");
    }

    #[test]
    fn foreign_key_target_is_qualified_only_across_schemas() {
        assert_eq!(
            fk_label(&orders_fk(Some("public")), Some("public")),
            "customer_id \u{2192} customers.id"
        );
        assert_eq!(
            fk_label(&orders_fk(Some("billing")), Some("public")),
            "customer_id \u{2192} billing.customers.id"
        );
    }
}
