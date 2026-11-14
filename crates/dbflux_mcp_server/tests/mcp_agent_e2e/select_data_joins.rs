use super::*;

// ---------------------------------------------------------------------------
// select_data joins
// ---------------------------------------------------------------------------

#[tokio::test]
async fn inner_join_returns_matched_rows_under_the_names_written_in_columns() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let selected = agent
        .call_json(
            "select_data",
            customers_join_orders(&connection_id, "inner"),
        )
        .await;

    assert_eq!(
        selected,
        json!({
            "columns": ["name", "orders.total"],
            "rows": [
                { "name": "Ada", "orders.total": 30 },
                { "name": "Ada", "orders.total": 5 },
                { "name": "Bo", "orders.total": 20 }
            ],
            "row_count": 3
        })
    );
}

#[tokio::test]
async fn left_join_keeps_unmatched_rows_with_nulls() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let selected = agent
        .call_json("select_data", customers_join_orders(&connection_id, "LEFT"))
        .await;

    assert_eq!(selected["row_count"], json!(4), "got {selected}");
    assert_eq!(
        selected["rows"][3],
        json!({ "name": "Cy", "orders.total": null })
    );
}

#[tokio::test]
async fn join_filters_sorts_and_limits_on_joined_columns() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "customers",
                "columns": ["customers.name", "o.total"],
                "joins": [{
                    "type": "inner",
                    "table": "orders",
                    "alias": "o",
                    "on": "customers.id = o.customer_id"
                }],
                "where": { "o.total": { "$gte": 20 }, "name": { "$in": ["Ada", "Bo"] } },
                "order_by": [{ "column": "o.total", "direction": "desc" }],
                "limit": 1,
                "offset": 1
            }),
        )
        .await;

    assert_eq!(
        selected,
        json!({
            "columns": ["customers.name", "o.total"],
            "rows": [{ "customers.name": "Bo", "o.total": 20 }],
            "row_count": 1
        })
    );
}

#[tokio::test]
async fn two_joins_chain_through_an_earlier_join() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "customers",
                "columns": ["name", "orders.total", "payments.method"],
                "joins": [
                    {
                        "type": "inner",
                        "table": "orders",
                        "on": "customers.id = orders.customer_id"
                    },
                    {
                        "type": "inner",
                        "table": "payments",
                        "on": "payments.order_id = orders.id AND payments.id > orders.id"
                    }
                ]
            }),
        )
        .await;

    assert_eq!(
        selected["rows"],
        json!([{ "name": "Ada", "orders.total": 30, "payments.method": "card" }])
    );
}

#[tokio::test]
async fn join_without_columns_returns_every_column_and_numbers_repeated_names() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "customers",
                "joins": [{
                    "type": "inner",
                    "table": "orders",
                    "on": "customers.id = orders.customer_id"
                }],
                "where": { "orders.id": 12 }
            }),
        )
        .await;

    assert_eq!(
        selected,
        json!({
            "columns": ["id", "name", "id_2", "customer_id", "total"],
            "rows": [{ "id": 2, "name": "Bo", "id_2": 12, "customer_id": 2, "total": 20 }],
            "row_count": 1
        })
    );
}

#[tokio::test]
async fn join_columns_field_selects_columns_of_the_joined_table() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "customers",
                "columns": ["name"],
                "joins": [{
                    "type": "inner",
                    "table": "orders",
                    "alias": "o",
                    "on": "customers.id = o.customer_id",
                    "columns": ["total"]
                }],
                "where": { "o.id": 12 }
            }),
        )
        .await;

    assert_eq!(selected["rows"], json!([{ "name": "Bo", "o.total": 20 }]));
}

#[tokio::test]
async fn hostile_join_input_is_rejected_and_leaves_the_tables_intact() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let join = |on: &str| json!({ "type": "inner", "table": "orders", "on": on });
    let base = |extra: Value| {
        let mut arguments = json!({ "connection_id": connection_id, "table": "customers" });
        arguments
            .as_object_mut()
            .expect("arguments are an object")
            .extend(extra.as_object().cloned().unwrap_or_default());
        arguments
    };
    let valid_on = "customers.id = orders.customer_id";

    let rejected = [
        (
            base(
                json!({ "joins": [join("customers.id = orders.customer_id; DROP TABLE orders")] }),
            ),
            "Accepted form",
        ),
        (
            base(json!({ "joins": [join("customers.id = orders.customer_id OR 1=1")] })),
            "Accepted form",
        ),
        (
            base(json!({ "joins": [join("customers.id = (SELECT 1)")] })),
            "Accepted form",
        ),
        (
            base(json!({ "joins": [join("customers.id = orders.customer_id -- x")] })),
            "Accepted form",
        ),
        (
            base(json!({ "joins": [join("customers.id = payments.order_id")] })),
            "not a table in this query",
        ),
        (
            base(json!({ "joins": [{
                "type": "inner",
                "table": "orders\" ON 1=1; DROP TABLE orders; --",
                "on": valid_on
            }] })),
            "plain identifier",
        ),
        (
            base(json!({ "joins": [{
                "type": "inner", "table": "orders", "alias": "o; DROP TABLE orders", "on": valid_on
            }] })),
            "plain identifier",
        ),
        (
            base(json!({ "joins": [{ "type": "cross", "table": "orders", "on": valid_on }] })),
            "join type",
        ),
        (
            base(json!({ "joins": [{
                "type": "inner", "table": "orders", "alias": "customers", "on": valid_on
            }] })),
            "already used",
        ),
        (
            base(json!({ "joins": [join(valid_on)], "columns": ["name\" FROM customers; --"] })),
            "plain identifier",
        ),
        (
            base(json!({ "joins": [join(valid_on)], "columns": ["ghost.name"] })),
            "not a table in this query",
        ),
        (
            base(json!({ "joins": [join(valid_on)], "where": { "ghost.id": 1 } })),
            "not a table in this query",
        ),
        (
            base(json!({ "joins": [join(valid_on)], "where": { "na?me": "x" } })),
            "plain identifier",
        ),
        (
            base(json!({ "joins": [join(valid_on)], "where": { "name": { "$regex": "^A" } } })),
            "$regex",
        ),
        (
            base(json!({
                "joins": [join(valid_on)],
                "order_by": [{ "column": "name; DROP TABLE orders" }]
            })),
            "plain identifier",
        ),
    ];

    for (arguments, expected) in rejected {
        let error = tool_error(&agent, "select_data", arguments.clone()).await;

        assert!(
            error.contains(expected),
            "the rejection of {arguments} should mention '{expected}': {error}"
        );
    }

    // A value is data: it is rendered as a literal and matches nothing.
    let literal = agent
        .call_json(
            "select_data",
            base(json!({
                "joins": [join(valid_on)],
                "where": { "name": "x' OR '1'='1" }
            })),
        )
        .await;
    assert_eq!(literal["row_count"], json!(0), "got {literal}");

    for (table, expected) in [("customers", 3), ("orders", 3), ("payments", 1)] {
        let counted = agent
            .call_json(
                "count_records",
                json!({ "connection_id": connection_id, "table": table }),
            )
            .await;

        assert_eq!(counted["count"], json!(expected), "table {table}");
    }
}

#[tokio::test]
async fn select_without_joins_is_unchanged() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let arguments = json!({
        "connection_id": connection_id,
        "table": "orders",
        "columns": ["id", "total"],
        "where": { "customer_id": 1 },
        "order_by": [{ "column": "total" }]
    });
    let expected = json!({
        "columns": ["id", "total"],
        "rows": [{ "id": 11, "total": 5 }, { "id": 10, "total": 30 }],
        "row_count": 2
    });

    assert_eq!(
        agent.call_json("select_data", arguments.clone()).await,
        expected
    );

    let mut with_empty_joins = arguments;
    with_empty_joins["joins"] = json!([]);
    assert_eq!(
        agent.call_json("select_data", with_empty_joins).await,
        expected,
        "an empty joins array is the same as leaving it out"
    );
}

#[tokio::test]
async fn join_call_records_the_generated_query_in_the_audit_log() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    agent
        .call_json(
            "select_data",
            json!({ "connection_id": connection_id, "table": "customers" }),
        )
        .await;
    let plain = latest_select_data_audit_details(&agent).await;
    assert!(
        plain.get("query").is_none(),
        "a call without joins keeps its audit details: {plain}"
    );

    agent
        .call_json(
            "select_data",
            customers_join_orders(&connection_id, "inner"),
        )
        .await;
    let joined = latest_select_data_audit_details(&agent).await;

    let query = joined["query"]
        .as_str()
        .unwrap_or_else(|| panic!("a join call records its query: {joined}"));
    assert!(
        query.starts_with("[FINGERPRINT:"),
        "the audit sink fingerprints the query by default: {query}"
    );
    assert!(joined["query_length"].as_u64().unwrap_or(0) > 0, "{joined}");
}

#[tokio::test]
async fn joins_on_a_driver_without_join_support_get_an_explicit_error() {
    let join_arguments = |connection_id: &str| {
        json!({
            "connection_id": connection_id,
            "table": "users",
            "database": "postgres",
            "joins": [{ "type": "inner", "table": "orders", "on": "users.id = orders.user_id" }]
        })
    };

    // Declares join support but has no structured SELECT generator.
    let (agent, connection_id) = start_hint_agent(ALLOW_ALL_ROLE, &["postgres"]).await;
    let error = tool_error(&agent, "select_data", join_arguments(&connection_id)).await;
    assert!(
        error.contains("does not support joins in select_data"),
        "got: {error}"
    );

    // Declares no join support.
    for kind in [dbflux_core::DbKind::MongoDB, dbflux_core::DbKind::Redis] {
        let profile = ConnectionProfile::new("agent-fake", DbConfig::default_postgres());
        let connection_id = profile.id.to_string();
        let driver = dbflux_test_support::FakeDriver::new(kind).as_driver_arc();
        let agent = start_agent(ALLOW_ALL_ROLE, Some((driver, profile))).await;

        let error = tool_error(&agent, "select_data", join_arguments(&connection_id)).await;
        assert!(
            error.contains("does not support joins in select_data"),
            "driver {kind:?} got: {error}"
        );
    }
}

#[tokio::test]
async fn failed_join_query_gets_the_not_found_hint_for_the_main_table() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let message = tool_error(
        &agent,
        "select_data",
        json!({
            "connection_id": connection_id,
            "table": "custmers",
            "joins": [{
                "type": "inner",
                "table": "orders",
                "on": "custmers.id = orders.customer_id"
            }]
        }),
    )
    .await;

    assert!(
        message.starts_with(&format!(
            "Table 'custmers' is not listed in database 'main'. {NOT_LISTED_REASON}\n\
             Did you mean: customers?"
        )),
        "got: {message}"
    );
    assert!(message.contains("no such table"), "got: {message}");
}
