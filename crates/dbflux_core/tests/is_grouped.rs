use dbflux_core::{
    AggFn, GroupByEntry, Projection, SourceTable, VisualAggregateSpec, VisualQuerySpec,
};

fn base_spec() -> VisualQuerySpec {
    VisualQuerySpec {
        source: SourceTable {
            schema: None,
            table: "orders".to_string(),
            alias: "o".to_string(),
        },
        projection: Projection::All,
        joins: vec![],
        filter: None,
        group_by: vec![],
        aggregates: vec![],
        having: None,
        sort: vec![],
        limit: None,
        offset: 0,
    }
}

fn country_group_by() -> GroupByEntry {
    GroupByEntry {
        source_alias: "o".to_string(),
        column: "country".to_string(),
    }
}

fn sum_aggregate() -> VisualAggregateSpec {
    VisualAggregateSpec {
        function: AggFn::Sum,
        source_alias: Some("o".to_string()),
        column: Some("amount".to_string()),
        alias: "total".to_string(),
    }
}

#[test]
fn is_grouped_false_when_both_empty() {
    let spec = base_spec();
    assert!(!spec.is_grouped());
}

#[test]
fn is_grouped_true_when_group_by_nonempty() {
    let mut spec = base_spec();
    spec.group_by = vec![country_group_by()];
    assert!(spec.is_grouped());
}

#[test]
fn is_grouped_true_when_aggregates_only() {
    let mut spec = base_spec();
    spec.aggregates = vec![sum_aggregate()];
    assert!(spec.is_grouped());
}

#[test]
fn is_grouped_true_when_both_nonempty() {
    let mut spec = base_spec();
    spec.group_by = vec![country_group_by()];
    spec.aggregates = vec![sum_aggregate()];
    assert!(spec.is_grouped());
}
