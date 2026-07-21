use dbflux_core::project_aggregate_kinds;
use dbflux_core::{
    AggFn, ColumnKind, ColumnMeta, GroupByEntry, Projection, SourceTable, VisualAggregateSpec,
    VisualQuerySpec,
};

fn make_column(kind: ColumnKind) -> ColumnMeta {
    ColumnMeta {
        name: "col".to_string(),
        type_name: "text".to_string(),
        kind,
        nullable: true,
        is_primary_key: false,
    }
}

fn base_spec(group_by: Vec<GroupByEntry>, aggregates: Vec<VisualAggregateSpec>) -> VisualQuerySpec {
    VisualQuerySpec {
        source: SourceTable {
            schema: None,
            table: "orders".to_string(),
            alias: "o".to_string(),
        },
        projection: Projection::All,
        joins: vec![],
        filter: None,
        group_by,
        aggregates,
        having: None,
        sort: vec![],
        limit: None,
        offset: 0,
    }
}

// =============================================================================
// Count variants always produce Integer
// =============================================================================

#[test]
fn count_produces_integer() {
    let spec = base_spec(
        vec![],
        vec![VisualAggregateSpec {
            function: AggFn::Count,
            source_alias: Some("o".to_string()),
            column: Some("id".to_string()),
            alias: "cnt".to_string(),
        }],
    );
    let mut columns = vec![make_column(ColumnKind::Unknown)];
    project_aggregate_kinds(&spec, &mut columns);
    assert_eq!(
        columns[0].kind,
        ColumnKind::Integer,
        "Count must yield Integer"
    );
}

#[test]
fn count_star_produces_integer() {
    let spec = base_spec(
        vec![],
        vec![VisualAggregateSpec {
            function: AggFn::CountStar,
            source_alias: None,
            column: None,
            alias: "cnt".to_string(),
        }],
    );
    let mut columns = vec![make_column(ColumnKind::Unknown)];
    project_aggregate_kinds(&spec, &mut columns);
    assert_eq!(
        columns[0].kind,
        ColumnKind::Integer,
        "CountStar must yield Integer"
    );
}

#[test]
fn count_distinct_produces_integer() {
    let spec = base_spec(
        vec![],
        vec![VisualAggregateSpec {
            function: AggFn::CountDistinct,
            source_alias: Some("o".to_string()),
            column: Some("customer_id".to_string()),
            alias: "distinct_customers".to_string(),
        }],
    );
    let mut columns = vec![make_column(ColumnKind::Unknown)];
    project_aggregate_kinds(&spec, &mut columns);
    assert_eq!(
        columns[0].kind,
        ColumnKind::Integer,
        "CountDistinct must yield Integer"
    );
}

// =============================================================================
// Avg always produces Float
// =============================================================================

#[test]
fn avg_produces_float() {
    let spec = base_spec(
        vec![],
        vec![VisualAggregateSpec {
            function: AggFn::Avg,
            source_alias: Some("o".to_string()),
            column: Some("amount".to_string()),
            alias: "avg_amount".to_string(),
        }],
    );
    let mut columns = vec![make_column(ColumnKind::Integer)];
    project_aggregate_kinds(&spec, &mut columns);
    assert_eq!(
        columns[0].kind,
        ColumnKind::Float,
        "Avg must always yield Float"
    );
}

// =============================================================================
// Sum inherits the input column kind
// =============================================================================

#[test]
fn sum_on_integer_column_produces_integer() {
    let spec = base_spec(
        vec![],
        vec![VisualAggregateSpec {
            function: AggFn::Sum,
            source_alias: Some("o".to_string()),
            column: Some("quantity".to_string()),
            alias: "total_qty".to_string(),
        }],
    );
    let mut columns = vec![make_column(ColumnKind::Integer)];
    project_aggregate_kinds(&spec, &mut columns);
    assert_eq!(
        columns[0].kind,
        ColumnKind::Integer,
        "Sum on Integer-kind column must yield Integer"
    );
}

#[test]
fn sum_on_text_column_produces_unknown() {
    let spec = base_spec(
        vec![],
        vec![VisualAggregateSpec {
            function: AggFn::Sum,
            source_alias: Some("o".to_string()),
            column: Some("label".to_string()),
            alias: "sum_label".to_string(),
        }],
    );
    let mut columns = vec![make_column(ColumnKind::Text)];
    project_aggregate_kinds(&spec, &mut columns);
    assert_eq!(
        columns[0].kind,
        ColumnKind::Unknown,
        "Sum on Text-kind column must yield Unknown"
    );
}

// =============================================================================
// Min/Max inherit the input column kind
// =============================================================================

#[test]
fn max_on_text_column_produces_text() {
    let spec = base_spec(
        vec![],
        vec![VisualAggregateSpec {
            function: AggFn::Max,
            source_alias: Some("o".to_string()),
            column: Some("name".to_string()),
            alias: "max_name".to_string(),
        }],
    );
    let mut columns = vec![make_column(ColumnKind::Text)];
    project_aggregate_kinds(&spec, &mut columns);
    assert_eq!(
        columns[0].kind,
        ColumnKind::Text,
        "Max on Text-kind column must yield Text"
    );
}

#[test]
fn min_on_float_column_produces_float() {
    let spec = base_spec(
        vec![],
        vec![VisualAggregateSpec {
            function: AggFn::Min,
            source_alias: Some("o".to_string()),
            column: Some("price".to_string()),
            alias: "min_price".to_string(),
        }],
    );
    let mut columns = vec![make_column(ColumnKind::Float)];
    project_aggregate_kinds(&spec, &mut columns);
    assert_eq!(
        columns[0].kind,
        ColumnKind::Float,
        "Min on Float-kind column must yield Float"
    );
}

// =============================================================================
// Group-by columns come first, then aggregate columns
// =============================================================================

#[test]
fn group_by_columns_are_positionally_first() {
    let spec = base_spec(
        vec![GroupByEntry {
            source_alias: "o".to_string(),
            column: "country".to_string(),
        }],
        vec![VisualAggregateSpec {
            function: AggFn::CountStar,
            source_alias: None,
            column: None,
            alias: "cnt".to_string(),
        }],
    );

    let mut columns = vec![
        make_column(ColumnKind::Text),    // group-by column (index 0)
        make_column(ColumnKind::Unknown), // aggregate column (index 1)
    ];
    project_aggregate_kinds(&spec, &mut columns);

    assert_eq!(
        columns[0].kind,
        ColumnKind::Text,
        "group-by column must not be overwritten"
    );
    assert_eq!(
        columns[1].kind,
        ColumnKind::Integer,
        "CountStar at index 1 must yield Integer"
    );
}

// =============================================================================
// Columns slice shorter than spec — must not panic
// =============================================================================

#[test]
fn short_columns_slice_does_not_panic() {
    let spec = base_spec(
        vec![GroupByEntry {
            source_alias: "o".to_string(),
            column: "country".to_string(),
        }],
        vec![
            VisualAggregateSpec {
                function: AggFn::CountStar,
                source_alias: None,
                column: None,
                alias: "cnt".to_string(),
            },
            VisualAggregateSpec {
                function: AggFn::Sum,
                source_alias: Some("o".to_string()),
                column: Some("amount".to_string()),
                alias: "total".to_string(),
            },
        ],
    );

    let mut columns = vec![make_column(ColumnKind::Text)];
    project_aggregate_kinds(&spec, &mut columns);
    // No panic, and the only column (group-by) remains Text
    assert_eq!(columns[0].kind, ColumnKind::Text);
}
