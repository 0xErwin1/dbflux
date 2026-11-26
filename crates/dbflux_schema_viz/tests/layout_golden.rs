//! Golden positions for `compute_layout` over a fixed schema, through the public
//! API only. The values were recorded from the layout before it was moved onto
//! the shared `dbflux_diagram` layout, so any drift in node placement, sizes or
//! edge anchors fails here.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use dbflux_core::{
    CollectionPresentation, ColumnInfo, ForeignKeyInfo, IndexData, IndexInfo, TableInfo,
};
use dbflux_schema_viz::graph::SchemaGraph;
use dbflux_schema_viz::layout::{LayoutFormat, LayoutResult, compute_layout};

fn column(name: &str, type_name: &str, primary: bool) -> ColumnInfo {
    ColumnInfo {
        name: name.to_owned(),
        type_name: type_name.to_owned(),
        nullable: !primary,
        is_primary_key: primary,
        default_value: None,
        enum_values: None,
    }
}

fn foreign_key(column_name: &str, referenced: &str, schema: Option<&str>) -> ForeignKeyInfo {
    ForeignKeyInfo {
        name: format!("fk_{column_name}_{referenced}"),
        columns: vec![column_name.to_owned()],
        referenced_table: referenced.to_owned(),
        referenced_schema: schema.map(str::to_owned),
        referenced_columns: vec!["id".to_owned()],
        on_delete: None,
        on_update: None,
    }
}

fn table(
    name: &str,
    schema: Option<&str>,
    extra_columns: &[&str],
    references: &[(&str, &str)],
    indexed: bool,
) -> TableInfo {
    let mut columns = vec![column("id", "integer", true)];
    columns.extend(extra_columns.iter().map(|name| column(name, "text", false)));
    columns.extend(
        references
            .iter()
            .map(|(column_name, _)| column(column_name, "integer", false)),
    );

    let foreign_keys: Vec<ForeignKeyInfo> = references
        .iter()
        .map(|(column_name, referenced)| foreign_key(column_name, referenced, schema))
        .collect();

    TableInfo {
        name: name.to_owned(),
        schema: schema.map(str::to_owned),
        columns: Some(columns),
        indexes: indexed.then(|| {
            IndexData::Relational(vec![IndexInfo {
                name: format!("{name}_lookup_idx"),
                columns: vec!["id".to_owned()],
                is_unique: true,
                is_primary: false,
            }])
        }),
        foreign_keys: (!foreign_keys.is_empty()).then_some(foreign_keys),
        constraints: None,
        sample_fields: None,
        storage_hints: None,
        presentation: CollectionPresentation::default(),
        child_items: None,
        pseudo_columns: Box::default(),
    }
}

/// Orders, customers, a cycle (ring_a -> ring_b -> ring_c -> ring_a), a self
/// reference, a wide table, an isolated table and tables with an index.
fn fixture() -> SchemaGraph {
    SchemaGraph::build(&[
        table("customers", Some("public"), &["name", "email"], &[], true),
        table(
            "orders",
            Some("public"),
            &["status"],
            &[("customer_id", "customers")],
            false,
        ),
        table(
            "order_items",
            Some("public"),
            &["quantity", "unit_price_with_a_long_column_name"],
            &[("order_id", "orders"), ("product_id", "products")],
            true,
        ),
        table("products", Some("public"), &["sku", "title"], &[], false),
        table(
            "employees",
            None,
            &["name"],
            &[("manager_id", "employees")],
            false,
        ),
        table("ring_a", None, &[], &[("next_id", "ring_b")], false),
        table("ring_b", None, &[], &[("next_id", "ring_c")], false),
        table("ring_c", None, &[], &[("next_id", "ring_a")], false),
        table(
            "a_table_with_an_extremely_long_name_that_needs_a_wide_card_to_read",
            None,
            &["col"],
            &[("customer_id", "customers")],
            false,
        ),
        table("isolated", None, &["note"], &[], false),
    ])
}

fn dump(graph: &SchemaGraph, layout: &LayoutResult) -> String {
    let mut lines: Vec<String> = graph
        .nodes()
        .map(|(index, node)| {
            let placed = layout.nodes.get(&index).expect("every node is placed");
            let label = match &node.id.schema {
                Some(schema) => format!("{schema}.{}", node.id.name),
                None => node.id.name.clone(),
            };
            format!(
                "node {label} x={} y={} w={} h={}",
                placed.x, placed.y, placed.width, placed.height
            )
        })
        .collect();
    lines.sort();

    let name_of = |index| graph.node_weight(index).expect("node").id.name.clone();
    let mut edges: Vec<String> = layout
        .edges
        .iter()
        .map(|edge| {
            format!(
                "edge {}->{} from=({}, {}) to=({}, {})",
                name_of(edge.from_node),
                name_of(edge.to_node),
                edge.from_anchor.0,
                edge.from_anchor.1,
                edge.to_anchor.0,
                edge.to_anchor.1
            )
        })
        .collect();
    edges.sort();
    lines.extend(edges);
    lines.push(format!(
        "total w={} h={}",
        layout.total_width, layout.total_height
    ));
    lines.join("\n")
}

fn render(
    format: LayoutFormat,
    focal: Option<(&str, Option<&str>)>,
    show_types: bool,
    show_indexes: bool,
) -> String {
    let graph = fixture();
    let layout = compute_layout(&graph, format, focal, show_types, show_indexes);
    dump(&graph, &layout)
}

const LEFT_RIGHT_TYPES: &str = r#"node a_table_with_an_extremely_long_name_that_needs_a_wide_card_to_read x=0 y=0 w=520 h=102
node employees x=0 y=166 w=218 h=102
node isolated x=0 y=332 w=200 h=78
node public.customers x=937 y=0 w=229 h=102
node public.order_items x=0 y=474 w=386 h=150
node public.orders x=616 y=0 w=225 h=102
node public.products x=616 y=166 w=221 h=102
node ring_a x=0 y=688 w=200 h=78
node ring_b x=616 y=332 w=200 h=78
node ring_c x=937 y=166 w=200 h=78
edge employees->employees from=(218, 217) to=(0, 217)
edge order_items->orders from=(386, 549) to=(616, 51)
edge order_items->products from=(386, 549) to=(616, 217)
edge orders->customers from=(841, 51) to=(937, 51)
edge ring_a->ring_b from=(200, 727) to=(616, 371)
edge ring_b->ring_c from=(816, 371) to=(937, 205)
edge ring_c->ring_a from=(1137, 205) to=(0, 727)
total w=1166 h=766"#;

const LEFT_RIGHT_INDEXES: &str = r#"node a_table_with_an_extremely_long_name_that_needs_a_wide_card_to_read x=0 y=0 w=520 h=102
node employees x=0 y=166 w=200 h=102
node isolated x=0 y=332 w=200 h=78
node public.customers x=933 y=0 w=229 h=140
node public.order_items x=0 y=474 w=322 h=188
node public.orders x=616 y=0 w=205 h=102
node public.products x=616 y=166 w=221 h=102
node ring_a x=0 y=726 w=200 h=78
node ring_b x=616 y=332 w=200 h=78
node ring_c x=933 y=204 w=200 h=78
edge employees->employees from=(200, 217) to=(0, 217)
edge order_items->orders from=(322, 568) to=(616, 51)
edge order_items->products from=(322, 568) to=(616, 217)
edge orders->customers from=(821, 51) to=(933, 70)
edge ring_a->ring_b from=(200, 765) to=(616, 371)
edge ring_b->ring_c from=(816, 371) to=(933, 243)
edge ring_c->ring_a from=(1133, 243) to=(0, 765)
total w=1162 h=804"#;

const COMPACT: &str = r#"node a_table_with_an_extremely_long_name_that_needs_a_wide_card_to_read x=0 y=0 w=520 h=102
node employees x=1136 y=0 w=218 h=102
node isolated x=1704 y=0 w=200 h=78
node public.customers x=568 y=0 w=229 h=140
node public.order_items x=0 y=188 w=386 h=188
node public.orders x=568 y=188 w=225 h=102
node public.products x=1136 y=188 w=221 h=102
node ring_a x=1704 y=188 w=200 h=78
node ring_b x=0 y=424 w=200 h=78
node ring_c x=568 y=424 w=200 h=78
edge employees->employees from=(1354, 51) to=(1136, 51)
edge order_items->orders from=(386, 282) to=(568, 239)
edge order_items->products from=(386, 282) to=(1136, 239)
edge orders->customers from=(793, 239) to=(568, 70)
edge ring_a->ring_b from=(1904, 227) to=(0, 463)
edge ring_b->ring_c from=(200, 463) to=(568, 463)
edge ring_c->ring_a from=(768, 463) to=(1704, 227)
total w=2272 h=502"#;

const SNOWFLAKE: &str = r#"node a_table_with_an_extremely_long_name_that_needs_a_wide_card_to_read x=0 y=850 w=520 h=102
node employees x=408 y=850 w=218 h=102
node isolated x=816 y=850 w=200 h=78
node public.customers x=385.5 y=49 w=229 h=102
node public.order_items x=307 y=625 w=386 h=150
node public.orders x=387.5 y=349 w=225 h=102
node public.products x=1224 y=850 w=221 h=102
node ring_a x=0 y=976 w=200 h=78
node ring_b x=408 y=976 w=200 h=78
node ring_c x=816 y=976 w=200 h=78
edge employees->employees from=(626, 901) to=(408, 901)
edge order_items->orders from=(693, 700) to=(387.5, 400)
edge order_items->products from=(693, 700) to=(1224, 901)
edge orders->customers from=(612.5, 400) to=(385.5, 100)
edge ring_a->ring_b from=(200, 1015) to=(408, 1015)
edge ring_b->ring_c from=(608, 1015) to=(816, 1015)
edge ring_c->ring_a from=(1016, 1015) to=(0, 1015)
total w=1545 h=1154"#;

const SNOWFLAKE_NO_FOCAL: &str = r#"node a_table_with_an_extremely_long_name_that_needs_a_wide_card_to_read x=0 y=0 w=520 h=102
node employees x=0 y=166 w=218 h=102
node isolated x=0 y=332 w=200 h=78
node public.customers x=937 y=0 w=229 h=102
node public.order_items x=0 y=474 w=386 h=150
node public.orders x=616 y=0 w=225 h=102
node public.products x=616 y=166 w=221 h=102
node ring_a x=0 y=688 w=200 h=78
node ring_b x=616 y=332 w=200 h=78
node ring_c x=937 y=166 w=200 h=78
edge employees->employees from=(218, 217) to=(0, 217)
edge order_items->orders from=(386, 549) to=(616, 51)
edge order_items->products from=(386, 549) to=(616, 217)
edge orders->customers from=(841, 51) to=(937, 51)
edge ring_a->ring_b from=(200, 727) to=(616, 371)
edge ring_b->ring_c from=(816, 371) to=(937, 205)
edge ring_c->ring_a from=(1137, 205) to=(0, 727)
total w=1166 h=766"#;

const SNOWFLAKE_MISSING_FOCAL: &str = r#"node a_table_with_an_extremely_long_name_that_needs_a_wide_card_to_read x=0 y=0 w=520 h=102
node employees x=0 y=166 w=218 h=102
node isolated x=0 y=332 w=200 h=78
node public.customers x=937 y=0 w=229 h=140
node public.order_items x=0 y=474 w=386 h=188
node public.orders x=616 y=0 w=225 h=102
node public.products x=616 y=166 w=221 h=102
node ring_a x=0 y=726 w=200 h=78
node ring_b x=616 y=332 w=200 h=78
node ring_c x=937 y=204 w=200 h=78
edge employees->employees from=(218, 217) to=(0, 217)
edge order_items->orders from=(386, 568) to=(616, 51)
edge order_items->products from=(386, 568) to=(616, 217)
edge orders->customers from=(841, 51) to=(937, 70)
edge ring_a->ring_b from=(200, 765) to=(616, 371)
edge ring_b->ring_c from=(816, 371) to=(937, 243)
edge ring_c->ring_a from=(1137, 243) to=(0, 765)
total w=1166 h=804"#;

#[test]
fn left_right_with_types_keeps_its_positions() {
    assert_eq!(
        render(LayoutFormat::LeftRight, None, true, false),
        LEFT_RIGHT_TYPES
    );
}

#[test]
fn left_right_with_indexes_keeps_its_positions() {
    assert_eq!(
        render(LayoutFormat::LeftRight, None, false, true),
        LEFT_RIGHT_INDEXES
    );
}

#[test]
fn compact_keeps_its_positions() {
    assert_eq!(render(LayoutFormat::Compact, None, true, true), COMPACT);
}

#[test]
fn snowflake_with_a_focal_table_keeps_its_positions() {
    assert_eq!(
        render(
            LayoutFormat::Snowflake,
            Some(("orders", Some("public"))),
            true,
            false
        ),
        SNOWFLAKE
    );
}

#[test]
fn snowflake_without_a_focal_table_falls_back_to_left_right() {
    assert_eq!(
        render(LayoutFormat::Snowflake, None, true, false),
        SNOWFLAKE_NO_FOCAL
    );
}

#[test]
fn snowflake_with_a_missing_focal_table_falls_back_to_left_right() {
    assert_eq!(
        render(LayoutFormat::Snowflake, Some(("nope", None)), true, true),
        SNOWFLAKE_MISSING_FOCAL
    );
}
