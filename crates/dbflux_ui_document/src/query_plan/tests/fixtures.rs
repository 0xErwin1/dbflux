//! The plan the tests read: the weekly revenue query of the design artboards,
//! a Seq Scan and an Index Scan under a Nested Loop, then an aggregate, a sort
//! and a limit.

use dbflux_core::{PlanCondition, PlanFormat, PlanNode, QueryPlan};

fn scan(
    operation: &str,
    relation: &str,
    cost: f64,
    rows: f64,
    description: &str,
    condition: (&str, &str),
) -> PlanNode {
    PlanNode {
        operation: operation.into(),
        relation: Some(relation.into()),
        estimated_startup_cost: Some(0.0),
        estimated_total_cost: Some(cost),
        estimated_rows: Some(rows),
        description: Some(description.into()),
        key_condition: Some(PlanCondition {
            label: condition.0.into(),
            value: condition.1.into(),
        }),
        details: vec![(condition.0.into(), condition.1.into())],
        ..PlanNode::default()
    }
}

fn above(operation: &str, cost: f64, rows: f64, children: Vec<PlanNode>) -> PlanNode {
    PlanNode {
        operation: operation.into(),
        estimated_startup_cost: Some(cost),
        estimated_total_cost: Some(cost),
        estimated_rows: Some(rows),
        children,
        ..PlanNode::default()
    }
}

/// Six nodes, in reading order: Limit, Sort, HashAggregate, Nested Loop,
/// Seq Scan, Index Scan.
pub(super) fn weekly_revenue() -> QueryPlan {
    let seq_scan = scan(
        "Seq Scan",
        "orders",
        8214.50,
        3100.0,
        "Reads every row of the table and keeps the ones that match the filter.",
        ("Filter", "(status = ANY ('{paid,shipped}'::text[]))"),
    );
    let mut index_scan = scan(
        "Index Scan",
        "customers",
        0.42,
        1.0,
        "Finds the matching rows through an index instead of reading the whole table.",
        ("Index Cond", "(id = o.customer_id)"),
    );
    index_scan.index = Some("customers_pkey".into());

    let nested_loop = above("Nested Loop", 9551.40, 3100.0, vec![seq_scan, index_scan]);
    let aggregate = above("HashAggregate", 9602.40, 612.0, vec![nested_loop]);
    let sort = above("Sort", 9639.31, 612.0, vec![aggregate]);
    let limit = above("Limit", 9639.43, 50.0, vec![sort]);

    let mut plan = QueryPlan::new(vec![limit], "[{\"Plan\": {}}]", PlanFormat::Json);
    plan.warnings
        .push("Seq Scan on orders reads the whole table.".into());
    plan
}

/// A plan with no costs and no rows, the way SQLite reports one.
pub(super) fn without_costs() -> QueryPlan {
    let mut search = PlanNode::new("SEARCH");
    search.relation = Some("customers".into());
    search.index = Some("sqlite_autoindex_customers_1".into());
    search.key_condition = Some(PlanCondition {
        label: "Index Cond".into(),
        value: "(id=?)".into(),
    });
    search.description =
        Some("Looks rows up through an index instead of scanning the whole table.".into());

    let mut scan = PlanNode::new("SCAN");
    scan.relation = Some("orders".into());
    scan.children = vec![search];

    QueryPlan::new(
        vec![scan],
        "SCAN orders\n  SEARCH customers",
        PlanFormat::Text,
    )
}

/// Limit > Sort > Aggregate > Sort > Hash Join > [Seq Scan,
/// Hash > Bitmap Heap Scan > Bitmap Index Scan]: a leaf (the Seq Scan) that is
/// a sibling of a deep branch.
pub(super) fn hash_join() -> QueryPlan {
    let mut node = PlanNode::new("Bitmap Index Scan");
    node.estimated_total_cost = Some(4.36);
    node.estimated_rows = Some(7.0);
    for operation in ["Bitmap Heap Scan", "Hash"] {
        let mut parent = PlanNode::new(operation);
        parent.estimated_total_cost = Some(14.5);
        parent.estimated_rows = Some(7.0);
        parent.children = vec![node];
        node = parent;
    }
    let mut seq = PlanNode::new("Seq Scan");
    seq.estimated_total_cost = Some(17.5);
    seq.estimated_rows = Some(750.0);
    let mut top = PlanNode::new("Hash Join");
    top.estimated_total_cost = Some(35.0);
    top.estimated_rows = Some(7.0);
    top.children = vec![seq, node];
    for operation in ["Sort", "Aggregate", "Sort", "Limit"] {
        let mut parent = PlanNode::new(operation);
        parent.estimated_total_cost = Some(35.4);
        parent.estimated_rows = Some(7.0);
        parent.children = vec![top];
        top = parent;
    }
    QueryPlan::new(vec![top], "{}", PlanFormat::Json)
}
