use dbflux_core::{
    AggFn, Projection, SourceTable, SpecError, VisualAggregateSpec, VisualQuerySpec,
};

fn base_spec_with_aggregates(aggregates: Vec<VisualAggregateSpec>) -> VisualQuerySpec {
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
        aggregates,
        having: None,
        sort: vec![],
        limit: None,
        offset: 0,
    }
}

#[test]
fn count_star_with_column_is_invalid() {
    let spec = base_spec_with_aggregates(vec![VisualAggregateSpec {
        function: AggFn::CountStar,
        source_alias: Some("o".to_string()),
        column: Some("id".to_string()),
        alias: "cnt".to_string(),
    }]);
    let result = spec.is_runnable();
    assert!(
        matches!(result, Err(SpecError::InvalidAggregate(_))),
        "CountStar with column must be invalid, got: {:?}",
        result
    );
}

#[test]
fn sum_without_column_is_invalid() {
    let spec = base_spec_with_aggregates(vec![VisualAggregateSpec {
        function: AggFn::Sum,
        source_alias: None,
        column: None,
        alias: "total".to_string(),
    }]);
    let result = spec.is_runnable();
    assert!(
        matches!(result, Err(SpecError::InvalidAggregate(_))),
        "Sum without column must be invalid, got: {:?}",
        result
    );
}

#[test]
fn empty_alias_is_invalid() {
    let spec = base_spec_with_aggregates(vec![VisualAggregateSpec {
        function: AggFn::Count,
        source_alias: Some("o".to_string()),
        column: Some("id".to_string()),
        alias: String::new(),
    }]);
    let result = spec.is_runnable();
    assert!(
        matches!(result, Err(SpecError::InvalidAggregate(_))),
        "empty alias must be invalid, got: {:?}",
        result
    );
}

#[test]
fn whitespace_only_alias_is_invalid() {
    let spec = base_spec_with_aggregates(vec![VisualAggregateSpec {
        function: AggFn::Count,
        source_alias: Some("o".to_string()),
        column: Some("id".to_string()),
        alias: "   ".to_string(),
    }]);
    let result = spec.is_runnable();
    assert!(
        matches!(result, Err(SpecError::InvalidAggregate(_))),
        "whitespace-only alias must be invalid, got: {:?}",
        result
    );
}

#[test]
fn duplicate_alias_within_aggregates_is_invalid() {
    let spec = base_spec_with_aggregates(vec![
        VisualAggregateSpec {
            function: AggFn::Sum,
            source_alias: Some("o".to_string()),
            column: Some("amount".to_string()),
            alias: "total".to_string(),
        },
        VisualAggregateSpec {
            function: AggFn::Count,
            source_alias: Some("o".to_string()),
            column: Some("id".to_string()),
            alias: "total".to_string(),
        },
    ]);
    let result = spec.is_runnable();
    assert!(
        matches!(result, Err(SpecError::InvalidAggregate(_))),
        "duplicate alias must be invalid, got: {:?}",
        result
    );
}

#[test]
fn valid_aggregate_spec_passes() {
    let spec = base_spec_with_aggregates(vec![
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
    ]);
    assert_eq!(spec.is_runnable(), Ok(()));
}
