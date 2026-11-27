//! The view model and the geometry: plain data in, numbers out, no window.

use std::collections::HashSet;
use std::time::Duration;

use dbflux_core::PlanFormat;
use dbflux_diagram::route_edge;
use gpui::{point, px, size};

use super::fixtures::{analyzed_weekly_revenue, off_by, weekly_revenue, without_costs};
use crate::query_plan::Step;
use crate::query_plan::chrome::format_elapsed;
use crate::query_plan::diagram::geometry::{
    CardRows, DiagramGeometry, MAX_EDGE_WIDTH, MIN_EDGE_WIDTH, MIN_FIT_ZOOM, edge_width, fit_view,
    label_anchor, navigate,
};
use crate::query_plan::model::{PlanModel, format_cost, format_rows, format_share};
use crate::query_plan::tree::{RowMove, row_move, visible_rows};

fn model() -> PlanModel {
    PlanModel::from_plan(&weekly_revenue())
}

#[test]
fn nodes_are_numbered_in_reading_order_with_their_parents() {
    let model = model();
    let names: Vec<&str> = model
        .nodes
        .iter()
        .map(|node| node.operation.as_str())
        .collect();

    assert_eq!(
        names,
        [
            "Limit",
            "Sort",
            "HashAggregate",
            "Nested Loop",
            "Seq Scan",
            "Index Scan"
        ]
    );
    assert_eq!(model.roots, [0]);
    assert_eq!(model.nodes[4].parent, Some(3));
    assert_eq!(model.nodes[5].parent, Some(3));
    assert_eq!(model.nodes[3].children, [4, 5]);
    assert_eq!(model.nodes[5].depth, 4);
}

#[test]
fn self_cost_is_the_node_cost_without_its_children() {
    let model = model();

    let nested_loop = &model.nodes[3];
    let self_cost = nested_loop.self_cost.expect("nested loop has a self cost");
    assert!((self_cost - (9551.40 - 8214.50 - 0.42)).abs() < 1e-6);

    let seq_scan = &model.nodes[4];
    assert_eq!(seq_scan.self_cost, Some(8214.50));
}

#[test]
fn self_share_is_the_fraction_of_the_plan_total_and_matches_the_artboard() {
    let model = model();

    assert_eq!(model.total_cost, Some(9639.43));
    assert_eq!(format_share(model.nodes[4].self_share.unwrap()), "85.2%");
    assert_eq!(format_share(model.nodes[3].self_share.unwrap()), "13.9%");
    assert_eq!(format_share(model.nodes[5].self_share.unwrap()), "<0.1%");
    assert_eq!(format_share(model.nodes[0].self_share.unwrap()), "<0.1%");
}

#[test]
fn the_costliest_node_is_the_one_with_the_highest_self_cost() {
    let model = model();
    let costliest: Vec<usize> = model
        .nodes
        .iter()
        .filter(|node| node.is_costliest)
        .map(|node| node.id)
        .collect();

    assert_eq!(costliest, [4], "only the Seq Scan");
    assert_eq!(
        model.entry_node(),
        Some(4),
        "the keyboard lands on it first"
    );
}

#[test]
fn a_plan_without_costs_has_no_shares_no_costliest_and_hidden_columns() {
    let model = PlanModel::from_plan(&without_costs());

    assert_eq!(model.total_cost, None);
    assert_eq!(model.total_rows, None);
    assert!(model.nodes.iter().all(|node| node.self_share.is_none()));
    assert!(model.nodes.iter().all(|node| !node.is_costliest));
    assert!(!model.columns.cost);
    assert!(!model.columns.self_cost);
    assert!(!model.columns.rows);
    assert!(model.columns.index, "the index column has data");
    assert_eq!(model.entry_node(), Some(0), "without a costliest, the root");
}

#[test]
fn a_plan_with_costs_shows_every_column() {
    let columns = model().columns;

    assert!(columns.index && columns.cost && columns.self_cost && columns.rows);
}

#[test]
fn warnings_of_the_plan_and_its_nodes_are_listed_once() {
    let mut plan = weekly_revenue();
    plan.roots[0]
        .warnings
        .push("Seq Scan on orders reads the whole table.".into());
    plan.roots[0].warnings.push("Sort spills to disk.".into());
    let model = PlanModel::from_plan(&plan);

    assert_eq!(
        model.warnings,
        [
            "Seq Scan on orders reads the whole table.",
            "Sort spills to disk."
        ]
    );
}

#[test]
fn costs_rows_and_shares_are_formatted_the_way_the_artboards_show_them() {
    assert_eq!(format_cost(9639.43), "9,639.43");
    assert_eq!(format_cost(0.42), "0.42");
    assert_eq!(format_cost(8214.5), "8,214.50");
    assert_eq!(format_cost(1_234_567.891), "1,234,567.89");
    assert_eq!(format_rows(3100.0), "3,100");
    assert_eq!(format_rows(0.4), "0");
    assert_eq!(format_share(0.0), "0.0%");
    assert_eq!(format_share(1.0), "100.0%");
    assert_eq!(format_share(0.0004), "<0.1%");
}

#[test]
fn the_plan_time_reads_in_milliseconds_then_seconds() {
    assert_eq!(format_elapsed(Duration::from_micros(300)), "<1 ms");
    assert_eq!(format_elapsed(Duration::from_millis(4)), "4 ms");
    assert_eq!(format_elapsed(Duration::from_millis(1500)), "1.5 s");
}

#[test]
fn edge_width_follows_the_rows_on_a_log_scale() {
    let max = Some(3100.0);

    let one = edge_width(Some(1.0), max);
    let fifty = edge_width(Some(50.0), max);
    let busiest = edge_width(Some(3100.0), max);

    assert_eq!(one, MIN_EDGE_WIDTH + 0.5, "one row is among the thinnest");
    assert!(one < fifty && fifty < busiest);
    assert_eq!(busiest, MAX_EDGE_WIDTH);
    // A linear scale would draw 50 rows at about 1.1; the log scale keeps it
    // readable.
    assert!(fifty > 3.0);
}

#[test]
fn edge_width_steps_by_half_pixels() {
    for rows in [1.0, 7.0, 50.0, 612.0, 3100.0] {
        let width = edge_width(Some(rows), Some(3100.0));
        assert_eq!(width * 2.0, (width * 2.0).round(), "{rows} rows -> {width}");
    }
}

#[test]
fn edge_width_is_uniform_when_rows_are_unknown() {
    let default = dbflux_components::diagram::DEFAULT_EDGE_WIDTH;

    assert_eq!(edge_width(None, Some(100.0)), default);
    assert_eq!(edge_width(Some(100.0), None), default);
    assert_eq!(edge_width(None, None), default);
    assert_eq!(edge_width(Some(5.0), Some(0.0)), default);
}

#[test]
fn a_card_has_a_row_only_for_the_data_the_node_has() {
    let model = model();
    let seq_scan = CardRows::of(&model.nodes[4]);
    let limit = CardRows::of(&model.nodes[0]);

    assert!(seq_scan.relation && !seq_scan.index && seq_scan.condition);
    assert!(seq_scan.cost && seq_scan.share);
    assert!(!limit.relation && !limit.index && !limit.condition);
    assert!(limit.height() < seq_scan.height());

    let bare = PlanModel::from_plan(&without_costs());
    let scan = CardRows::of(&bare.nodes[0]);
    assert!(!scan.cost && !scan.share, "no costs, no cost or share rows");
}

#[test]
fn the_diagram_reads_left_to_right_from_the_leaves_to_the_root() {
    let model = model();
    let geometry = DiagramGeometry::build(&model);

    assert_eq!(geometry.nodes.len(), model.nodes.len());
    assert_eq!(
        geometry.edges.len(),
        model.nodes.len() - 1,
        "one edge per child"
    );

    let x = |id: usize| geometry.nodes[id].x;
    assert!(x(4) < x(3) && x(5) < x(3), "scans sit left of the join");
    assert!(
        x(3) < x(2) && x(2) < x(1) && x(1) < x(0),
        "the root is last"
    );

    for edge in &geometry.edges {
        let parent = model.nodes[edge.from].parent.unwrap();
        assert_eq!(edge.to, parent, "edges run from child to parent");
    }
}

#[test]
fn edges_carry_the_rows_of_their_child_with_a_width_for_them() {
    let model = model();
    let geometry = DiagramGeometry::build(&model);

    let from_seq_scan = geometry.edges.iter().find(|edge| edge.from == 4).unwrap();
    let from_index_scan = geometry.edges.iter().find(|edge| edge.from == 5).unwrap();

    assert_eq!(from_seq_scan.rows, Some(3100.0));
    assert_eq!(from_index_scan.rows, Some(1.0));
    assert!(from_seq_scan.width > from_index_scan.width);
}

#[test]
fn a_plan_without_rows_draws_every_edge_alike_and_unlabelled() {
    let model = PlanModel::from_plan(&without_costs());
    let geometry = DiagramGeometry::build(&model);

    assert!(geometry.edges.iter().all(|edge| edge.rows.is_none()));
    let widths: HashSet<u32> = geometry
        .edges
        .iter()
        .map(|edge| edge.width.to_bits())
        .collect();
    assert_eq!(widths.len(), 1);
}

#[test]
fn the_edge_label_sits_halfway_along_the_route() {
    let from = dbflux_diagram::NodeBounds {
        x: 0.0,
        y: 0.0,
        width: 100.0,
        height: 40.0,
    };
    let to = dbflux_diagram::NodeBounds {
        x: 300.0,
        y: 0.0,
        width: 100.0,
        height: 40.0,
    };
    let route = route_edge(from, to, 20.0, 20.0, 0, 1, &[from, to]);

    let (label_x, label_y) = label_anchor(&route);

    assert!(
        label_x > 100.0 && label_x < 300.0 - 12.0,
        "between the nodes and clear of the arrowhead: {label_x}"
    );
    assert!((label_y - 20.0).abs() < 1.0, "{label_y}");
}

#[test]
fn fit_never_shrinks_below_the_legible_zoom_and_anchors_at_the_top_left() {
    // A diagram far wider than the viewport: plain fit would pick about 0.2.
    let bounds = (0.0, 0.0, 4000.0, 600.0);
    let viewport = size(px(900.0), px(500.0));

    let (zoom, pan) = fit_view(bounds, viewport, 1.0).expect("fit");

    assert_eq!(zoom, MIN_FIT_ZOOM);
    assert_eq!(
        pan,
        point(px(48.0 - 0.0), px(48.0 - 0.0)),
        "margin at the top-left"
    );
}

#[test]
fn fit_keeps_the_plain_fit_when_it_is_already_legible() {
    let bounds = (0.0, 0.0, 800.0, 300.0);
    let viewport = size(px(1000.0), px(500.0));

    let (zoom, _) = fit_view(bounds, viewport, 1.0).expect("fit");

    assert!((MIN_FIT_ZOOM..=1.0).contains(&zoom), "{zoom}");
}

#[test]
fn a_small_diagram_is_not_blown_up_past_the_cap() {
    let bounds = (0.0, 0.0, 100.0, 80.0);
    let viewport = size(px(1600.0), px(900.0));

    let (zoom, _) = fit_view(bounds, viewport, 1.0).expect("fit");

    assert_eq!(zoom, 1.0);
}

#[test]
fn fit_without_a_viewport_does_nothing() {
    assert!(fit_view((0.0, 0.0, 100.0, 100.0), size(px(0.0), px(0.0)), 1.0).is_none());
}

#[test]
fn a_collapsed_node_hides_its_descendants_from_the_tree() {
    let model = model();

    assert_eq!(visible_rows(&model, &HashSet::new()), [0, 1, 2, 3, 4, 5]);
    assert_eq!(visible_rows(&model, &HashSet::from([3])), [0, 1, 2, 3]);
    assert_eq!(visible_rows(&model, &HashSet::from([1])), [0, 1]);
}

#[test]
fn tree_arrows_move_between_rows_and_fold_the_branch() {
    let model = model();
    let open = HashSet::new();
    let folded = HashSet::from([3]);

    assert_eq!(
        row_move(&model, &open, 0, Step::Down),
        Some(RowMove::Select(1))
    );
    assert_eq!(
        row_move(&model, &open, 0, Step::Up),
        None,
        "nothing above the first row"
    );
    assert_eq!(
        row_move(&model, &open, 5, Step::Down),
        None,
        "nothing under the last row"
    );
    assert_eq!(
        row_move(&model, &folded, 3, Step::Down),
        None,
        "hidden rows are skipped"
    );

    assert_eq!(
        row_move(&model, &open, 3, Step::Left),
        Some(RowMove::Collapse(3))
    );
    assert_eq!(
        row_move(&model, &folded, 3, Step::Right),
        Some(RowMove::Expand(3))
    );
    assert_eq!(
        row_move(&model, &open, 3, Step::Right),
        Some(RowMove::Select(4)),
        "to the first child"
    );
    assert_eq!(
        row_move(&model, &open, 4, Step::Left),
        Some(RowMove::Select(3)),
        "a leaf goes to its parent"
    );
}

#[test]
fn json_plans_are_shown_indented_and_other_formats_as_returned() {
    let model = model();
    assert_eq!(model.raw, "[\n  {\n    \"Plan\": {}\n  }\n]");
    assert_eq!(model.raw_format, PlanFormat::Json);

    let mut plan = weekly_revenue();
    plan.raw = r#"[{"Node Type": "Limit", "Alpha": 1}]"#.into();
    let keys_in_order = PlanModel::from_plan(&plan).raw;
    assert!(
        keys_in_order.find("Node Type") < keys_in_order.find("Alpha"),
        "the engine's key order is kept: {keys_in_order}"
    );

    let text = without_costs();
    assert_eq!(
        PlanModel::from_plan(&text).raw,
        text.raw,
        "text is untouched"
    );

    let mut broken = weekly_revenue();
    broken.raw = "{not json".into();
    assert_eq!(PlanModel::from_plan(&broken).raw, "{not json");
}

#[test]
fn up_and_down_stay_in_the_nodes_column_and_left_right_cross_layers() {
    use dbflux_diagram::Direction;

    let geometry = DiagramGeometry::build(&model());
    let step = |from: usize, direction| navigate(&geometry.nodes, from, direction);

    assert_eq!(step(4, Direction::Down), Some(5), "the scan below the scan");
    assert_eq!(step(5, Direction::Up), Some(4));
    assert_eq!(step(4, Direction::Right), Some(3), "toward the root");
    assert!(
        matches!(step(3, Direction::Left), Some(4 | 5)),
        "toward a leaf"
    );
    assert!(
        step(0, Direction::Down).is_some(),
        "a node alone in its column still moves"
    );
}

#[test]
fn the_edge_label_of_a_bent_route_sits_on_its_longest_horizontal_run() {
    use dbflux_diagram::{OrthogonalRoute, RoutePoint};

    let p = |x, y| RoutePoint { x, y };
    // Out 20, down, then a long run in: the label belongs on the long run.
    let route = OrthogonalRoute {
        points: vec![p(0.0, 0.0), p(20.0, 0.0), p(20.0, 80.0), p(120.0, 80.0)],
    };

    assert_eq!(label_anchor(&route), (70.0, 80.0));
}

#[test]
fn a_plan_edge_label_never_sits_on_its_arrowhead() {
    let geometry = DiagramGeometry::build(&model());

    for edge in &geometry.edges {
        let end = edge.route.end();
        let (label_x, _) = edge.label_at;
        assert!(end.x - label_x > 12.0, "{edge:?}");
    }
}

#[test]
fn every_plan_edge_joins_adjacent_columns_and_no_node_sits_on_its_route() {
    let model = model();
    let geometry = DiagramGeometry::build(&model);

    for edge in &geometry.edges {
        let (child, parent) = (geometry.nodes[edge.from], geometry.nodes[edge.to]);
        let between = geometry.nodes.iter().enumerate().any(|(id, node)| {
            id != edge.from && id != edge.to && node.x > child.x && node.x < parent.x
        });
        assert!(!between, "a node sits between the columns of {edge:?}");
    }
}

#[test]
fn a_leaf_beside_a_deep_branch_sits_in_the_column_of_its_sibling() {
    let model = PlanModel::from_plan(&super::fixtures::hash_join());
    let geometry = DiagramGeometry::build(&model);
    let id = |name: &str| {
        model
            .nodes
            .iter()
            .find(|node| node.operation == name)
            .map(|node| node.id)
            .expect(name)
    };
    let x = |name: &str| geometry.nodes[id(name)].x;

    assert_eq!(x("Seq Scan"), x("Hash"), "siblings share a column");
    assert!(x("Bitmap Heap Scan") < x("Hash"));
    assert!(x("Hash") < x("Hash Join"));
    assert_eq!(geometry.edges.len(), model.nodes.len() - 1);
    for edge in &geometry.edges {
        let (child, parent) = (geometry.nodes[edge.from], geometry.nodes[edge.to]);
        let skipped = geometry
            .nodes
            .iter()
            .any(|node| node.x > child.x + 1.0 && node.x + 1.0 < parent.x);
        assert!(!skipped, "{edge:?}");
    }
}

fn analyzed_model() -> PlanModel {
    PlanModel::from_plan(&analyzed_weekly_revenue())
}

#[test]
fn an_estimate_has_no_actuals_and_an_analyzed_plan_does() {
    let estimate = model();
    assert!(!estimate.analyzed);
    assert!(estimate.nodes.iter().all(|node| node.actual_rows.is_none()));
    assert!(!estimate.columns.actual_rows && !estimate.columns.time);
    assert!(!estimate.columns.loops && !estimate.columns.self_time);
    assert!(estimate.nodes.iter().all(|node| !node.is_slowest));
    assert!(
        estimate
            .nodes
            .iter()
            .all(|node| node.estimate_gap.is_none())
    );

    let analyzed = analyzed_model();
    assert!(analyzed.analyzed);
    assert_eq!(analyzed.planning_time, Some(0.9));
    assert_eq!(analyzed.execution_time, Some(412.6));
    assert!(analyzed.columns.actual_rows && analyzed.columns.time);
    assert!(analyzed.columns.loops && analyzed.columns.self_time);
}

#[test]
fn the_slowest_node_is_the_one_with_the_most_self_time_and_replaces_the_costliest() {
    let analyzed = analyzed_model();

    let slowest: Vec<usize> = analyzed
        .nodes
        .iter()
        .filter(|node| node.is_slowest)
        .map(|node| node.id)
        .collect();
    assert_eq!(slowest, [5], "the Index Scan, 169.1 ms across its loops");
    assert!(analyzed.nodes.iter().all(|node| !node.is_costliest));
    assert_eq!(analyzed.entry_node(), Some(5));

    let index_scan = &analyzed.nodes[5];
    assert_eq!(index_scan.loops, Some(48_200));
    let time = index_scan.actual_time.expect("total time across loops");
    assert!((time - 169.1).abs() < 1e-6, "{time}");
    let share = index_scan.self_time_share.expect("share of execution time");
    assert_eq!(format_share(share), "41.0%");
    assert_eq!(index_scan.share, index_scan.self_time_share);

    let limit = &analyzed.nodes[0];
    assert_eq!(format_share(limit.share.expect("share")), "<0.1%");
}

#[test]
fn the_estimate_gap_is_marked_on_one_node_and_only_from_ten_times() {
    let analyzed = analyzed_model();
    let marked: Vec<(usize, String)> = analyzed
        .nodes
        .iter()
        .filter_map(|node| node.estimate_gap.map(|gap| (node.id, format!("{gap:.1}"))))
        .collect();
    assert_eq!(
        marked,
        [(4, "15.5".to_string())],
        "the Seq Scan wins the tie with the Nested Loop by depth; the Sort at 12.2x is not the largest"
    );
    assert!(
        analyzed
            .warnings
            .iter()
            .any(|warning| warning.contains("48,200") && warning.contains("3,100")),
        "{:?}",
        analyzed.warnings
    );

    let marked = |estimated, actual| {
        PlanModel::from_plan(&off_by(estimated, actual))
            .nodes
            .iter()
            .any(|node| node.estimate_gap.is_some())
    };
    assert!(!marked(100.0, 999.0), "9.99x is below the threshold");
    assert!(marked(100.0, 1000.0), "10x is the threshold");
    assert!(marked(1000.0, 100.0), "fewer rows count too");
}

#[test]
fn edges_are_weighted_by_actual_rows_times_loops() {
    let analyzed = analyzed_model();

    assert_eq!(
        analyzed.nodes[5].edge_rows,
        Some(48_200.0),
        "1 row x 48,200 loops"
    );
    assert_eq!(analyzed.nodes[4].edge_rows, Some(48_200.0));
    assert_eq!(analyzed.nodes[1].edge_rows, Some(50.0));
    assert_eq!(analyzed.max_rows(), Some(48_200.0));

    let estimate = model();
    assert_eq!(estimate.nodes[5].edge_rows, estimate.nodes[5].rows);
}

#[test]
fn analyzed_totals_show_rows_and_leave_the_self_cost_column_out() {
    let analyzed = analyzed_model();

    assert_eq!(analyzed.total_rows, Some(50.0));
    assert!(!analyzed.columns.self_cost, "self time replaces self cost");
    assert!(analyzed.columns.cost, "the cost column stays");
    assert!(model().columns.self_cost);
}

#[test]
fn time_is_formatted_to_the_precision_that_reads() {
    use crate::query_plan::model::format_time;

    assert_eq!(format_time(0.9), "0.9 ms");
    assert_eq!(format_time(0.041), "0.041 ms");
    assert_eq!(format_time(96.2), "96.2 ms");
    assert_eq!(format_time(412.6), "412.6 ms");
    assert_eq!(format_time(0.0), "0 ms");
    assert_eq!(format_time(1500.0), "1.50 s");
}

#[test]
fn a_row_count_says_row_for_one_and_rows_otherwise() {
    use crate::query_plan::model::format_row_count;

    assert_eq!(format_row_count(1.0), "1 row");
    assert_eq!(format_row_count(0.0), "0 rows");
    assert_eq!(format_row_count(3100.0), "3,100 rows");
}

#[test]
fn a_ratio_drops_the_decimal_from_one_hundred_times() {
    use crate::query_plan::model::format_ratio;

    assert_eq!(format_ratio(15.5), "15.5×");
    assert_eq!(format_ratio(99.94), "99.9×");
    assert_eq!(format_ratio(100.0), "100×");
    assert_eq!(format_ratio(750.0), "750×");
}

#[test]
fn the_gap_warning_and_tooltip_use_the_row_phrase_and_the_short_ratio() {
    let plan = off_by(1.0, 750.0);
    let model = PlanModel::from_plan(&plan);
    let warning = model.warnings.last().expect("the gap warning");

    assert!(warning.contains("750 rows"), "{warning}");
    assert!(warning.contains("1 row"), "{warning}");
    assert!(
        warning.contains("750×") && !warning.contains("750.0"),
        "{warning}"
    );

    let body = crate::query_plan::diagram::gap_tooltip_body(&model.nodes[0]);
    assert!(
        body.contains("750 rows") && body.contains("1 row"),
        "{body}"
    );
}

#[test]
fn an_edge_knows_whether_its_rows_are_actual_or_an_estimate() {
    let mut plan = analyzed_weekly_revenue();
    // The engine gave no actual rows for this node, so its edge keeps the
    // estimate.
    plan.roots[0].children[0].children[0].children[0].children[1].actual_rows = None;
    let geometry = DiagramGeometry::build(&PlanModel::from_plan(&plan));

    let edge = |from| {
        geometry
            .edges
            .iter()
            .find(|edge| edge.from == from)
            .expect("an edge")
    };
    assert!(edge(4).rows_are_actual);
    assert!(!edge(5).rows_are_actual);
    assert_eq!(edge(5).rows, Some(1.0));

    let estimate = DiagramGeometry::build(&model());
    assert!(estimate.edges.iter().all(|edge| !edge.rows_are_actual));
}

#[test]
fn the_analyzed_tree_is_wider_than_a_narrow_surface_so_it_scrolls_sideways() {
    use crate::query_plan::tree::table_min_width;

    let analyzed = PlanModel::from_plan(&analyzed_weekly_revenue());
    let estimate = model();

    // Rems against the default 16 px rem: the columns alone are 850 px.
    let analyzed_px = table_min_width(analyzed.columns).0 * 16.0;
    let estimate_px = table_min_width(estimate.columns).0 * 16.0;
    assert!(analyzed_px > 850.0, "{analyzed_px}");
    assert!(analyzed_px > estimate_px);
}
