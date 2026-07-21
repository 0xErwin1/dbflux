/// Tests for SchemaVizDocument logic (pure unit tests — no GPUI harness required).
///
/// Imports are kept minimal (no `super::*`) to avoid triggering GPUI proc-macro
/// expansion across the full parent module during test compilation.
use super::SchemaVizDocument;
use dbflux_core::{ColumnInfo, ForeignKeyInfo, TableInfo};
use dbflux_schema_viz::{
    graph::SchemaGraph,
    layout::{LayoutFormat, compute_layout},
};

fn make_table(name: &str, n_cols: usize) -> TableInfo {
    TableInfo {
        name: name.to_owned(),
        schema: None,
        columns: Some(
            (0..n_cols)
                .map(|i| ColumnInfo {
                    name: format!("col_{}", i),
                    type_name: "text".to_owned(),
                    nullable: i > 0,
                    is_primary_key: i == 0,
                    default_value: None,
                    enum_values: None,
                })
                .collect(),
        ),
        indexes: None,
        foreign_keys: None,
        constraints: None,
        sample_fields: None,
        presentation: dbflux_core::CollectionPresentation::default(),
        child_items: None,
    }
}

fn make_fk_table(name: &str, ref_table: &str) -> TableInfo {
    TableInfo {
        name: name.to_owned(),
        schema: None,
        columns: Some(vec![
            ColumnInfo {
                name: "id".to_owned(),
                type_name: "integer".to_owned(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
                enum_values: None,
            },
            ColumnInfo {
                name: format!("{}_id", ref_table),
                type_name: "integer".to_owned(),
                nullable: true,
                is_primary_key: false,
                default_value: None,
                enum_values: None,
            },
        ]),
        indexes: None,
        foreign_keys: Some(vec![ForeignKeyInfo {
            name: format!("fk_{}_{}", name, ref_table),
            columns: vec![format!("{}_id", ref_table)],
            referenced_table: ref_table.to_owned(),
            referenced_schema: None,
            referenced_columns: vec!["id".to_owned()],
            on_delete: None,
            on_update: None,
        }]),
        constraints: None,
        sample_fields: None,
        presentation: dbflux_core::CollectionPresentation::default(),
        child_items: None,
    }
}

// ── T23: set_show_types toggles the field and recomputes layout ──────────
//
// We cannot instantiate SchemaVizDocument without a GPUI context, so we
// test the underlying behavior directly: calling compute_layout with
// show_types=true vs show_types=false produces the same number of nodes,
// confirming that toggle-driven recomputation would succeed.
#[test]
fn test_show_types_toggle_recomputes_layout() {
    let tables = vec![make_table("users", 3)];
    let graph = SchemaGraph::build(&tables);

    let layout_with = compute_layout(&graph, LayoutFormat::LeftRight, None, true, false);
    let layout_without = compute_layout(&graph, LayoutFormat::LeftRight, None, false, false);

    assert_eq!(
        layout_with.nodes.len(),
        layout_without.nodes.len(),
        "node count is stable after show_types toggle"
    );

    // Both layouts should contain the same node indices.
    let keys_with: std::collections::HashSet<_> = layout_with.nodes.keys().collect();
    let keys_without: std::collections::HashSet<_> = layout_without.nodes.keys().collect();
    assert_eq!(keys_with, keys_without, "same nodes present after toggle");
}

// ── T24: layout_label returns correct strings per variant ────────────────

#[test]
fn test_layout_label_left_right() {
    assert_eq!(
        SchemaVizDocument::layout_label(LayoutFormat::LeftRight),
        "Left-Right"
    );
}

#[test]
fn test_layout_label_snowflake() {
    assert_eq!(
        SchemaVizDocument::layout_label(LayoutFormat::Snowflake),
        "Snowflake"
    );
}

#[test]
fn test_layout_label_compact() {
    assert_eq!(
        SchemaVizDocument::layout_label(LayoutFormat::Compact),
        "Compact"
    );
}

// ── T25: counter values match layout.nodes.len() / edges.len() ───────────

#[test]
fn test_counter_matches_layout_nodes_and_edges() {
    // Build a graph with 2 tables and 1 FK edge: orders -> users
    let users = make_table("users", 2);
    let orders = make_fk_table("orders", "users");

    let graph = SchemaGraph::build(&[users, orders]);
    let layout = compute_layout(&graph, LayoutFormat::LeftRight, None, true, false);

    let n_tables = layout.nodes.len();
    let n_relations = layout.edges.len();

    assert_eq!(n_tables, 2, "2 tables should be in layout");
    assert_eq!(n_relations, 1, "1 FK relation should be in layout");

    // This mirrors the toolbar counter formula exactly:
    //   format!("{} tables · {} relations", n_tables, n_relations)
    let counter = format!("{} tables · {} relations", n_tables, n_relations);
    assert_eq!(counter, "2 tables · 1 relations");
}
