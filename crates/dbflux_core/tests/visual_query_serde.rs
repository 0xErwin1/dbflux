use dbflux_core::{
    AggFn, GroupByEntry, Projection, SourceTable, VisualAggregateSpec, VisualQuerySpec,
};

/// A JSON blob serialised before the group_by/aggregates/having fields were added.
/// The spec uses all existing fields but has no group_by, aggregates, or having keys.
const LEGACY_JSON: &str = r#"{
    "source": {"schema": null, "table": "orders", "alias": "orders"},
    "projection": "All",
    "joins": [],
    "filter": null,
    "sort": [],
    "limit": 100,
    "offset": 0
}"#;

#[test]
fn legacy_json_without_group_fields_deserializes_with_defaults() {
    let spec: VisualQuerySpec =
        serde_json::from_str(LEGACY_JSON).expect("legacy JSON must deserialise");

    assert!(
        spec.group_by.is_empty(),
        "group_by must default to empty vec"
    );
    assert!(
        spec.aggregates.is_empty(),
        "aggregates must default to empty vec"
    );
    assert!(spec.having.is_none(), "having must default to None");
    assert!(!spec.is_grouped(), "legacy spec must not be grouped");
}

#[test]
fn legacy_json_reserializes_without_new_keys_when_defaults() {
    let spec: VisualQuerySpec =
        serde_json::from_str(LEGACY_JSON).expect("legacy JSON must deserialise");

    let serialized = serde_json::to_string(&spec).expect("serialisation must succeed");
    let value: serde_json::Value =
        serde_json::from_str(&serialized).expect("round-trip parse must succeed");

    assert!(
        value.get("group_by").is_some(),
        "group_by appears because serde(default) still serializes empty vec"
    );
    assert!(
        value.get("aggregates").is_some(),
        "aggregates appears in output"
    );
}

#[test]
fn full_round_trip_with_all_new_fields_populated() {
    let spec = VisualQuerySpec {
        source: SourceTable {
            schema: Some("public".to_string()),
            table: "orders".to_string(),
            alias: "o".to_string(),
        },
        projection: Projection::All,
        joins: vec![],
        filter: None,
        group_by: vec![GroupByEntry {
            source_alias: "o".to_string(),
            column: "country".to_string(),
        }],
        aggregates: vec![VisualAggregateSpec {
            function: AggFn::Sum,
            source_alias: Some("o".to_string()),
            column: Some("amount".to_string()),
            alias: "total".to_string(),
        }],
        having: None,
        sort: vec![],
        limit: Some(50),
        offset: 0,
    };

    let json = serde_json::to_string(&spec).expect("serialisation must succeed");
    let roundtripped: VisualQuerySpec =
        serde_json::from_str(&json).expect("deserialisation must succeed");

    assert_eq!(spec, roundtripped);
    assert!(roundtripped.is_grouped());
    assert_eq!(roundtripped.group_by[0].column, "country");
    assert_eq!(roundtripped.aggregates[0].alias, "total");
}
