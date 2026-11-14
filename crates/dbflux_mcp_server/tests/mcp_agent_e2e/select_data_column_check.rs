use super::*;

// ---------------------------------------------------------------------------
// select_data column check
// ---------------------------------------------------------------------------

#[tokio::test]
async fn select_data_refuses_a_misspelled_column_before_running() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, false).await;

    let expected = format!(
        "Column 'labl' is not listed among the columns of table 'items'.\n\
         Did you mean: label?\n\
         Available columns: id, label\n\n\
         {NOT_RUN}"
    );

    let cases = [
        ("where", json!({ "where": { "labl": "alpha" } })),
        ("order_by", json!({ "order_by": [{ "column": "labl" }] })),
        ("columns", json!({ "columns": ["id", "labl"] })),
        (
            "qualified where",
            json!({ "where": { "items.labl": "alpha" } }),
        ),
    ];

    for (case, mut arguments) in cases {
        arguments["connection_id"] = json!(connection_id);
        arguments["table"] = json!("items");

        let message = tool_error(&agent, "select_data", arguments).await;

        assert_eq!(message, expected, "a misspelled column in {case}");
    }
}

#[tokio::test]
async fn select_data_column_check_keeps_valid_calls_unchanged() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, false).await;

    let filtered = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "items",
                "columns": ["id", "label"],
                "where": { "label": "alpha" },
                "order_by": [{ "column": "id" }]
            }),
        )
        .await;
    assert_eq!(
        filtered,
        json!({
            "columns": ["id", "label"],
            "rows": [{ "id": 1, "label": "alpha" }],
            "row_count": 1
        })
    );

    let other_case = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "items",
                "where": { "LABEL": "beta" },
                "order_by": [{ "column": "Id", "direction": "desc" }]
            }),
        )
        .await;
    assert_eq!(
        other_case["rows"],
        json!([{ "id": 2, "label": "beta" }]),
        "a name that differs only in case is not refused"
    );

    let nested = tool_error(
        &agent,
        "select_data",
        json!({
            "connection_id": connection_id,
            "table": "items",
            "where": { "label": { "$regex": "^a" } }
        }),
    )
    .await;
    assert!(
        nested.starts_with("Select error: "),
        "a listed column runs and keeps the driver error: {nested}"
    );
}

#[tokio::test]
async fn select_data_column_refusal_without_describe_object_names_nothing() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let admin = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile.clone()))).await;

    prepare_items_table(&admin, &connection_id, false).await;

    let reader = start_agent(READ_TOOLS_ONLY_ROLE, Some((sqlite_driver(), profile))).await;

    let message = tool_error(
        &reader,
        "select_data",
        json!({ "connection_id": connection_id, "table": "items", "where": { "labl": "alpha" } }),
    )
    .await;

    assert_eq!(
        message,
        format!("Column 'labl' is not listed among the columns of table 'items'.\n\n{NOT_RUN}")
    );
}

#[tokio::test]
async fn join_call_refuses_a_misspelled_column_of_the_joined_table() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let expected = format!(
        "Column 'totl' is not listed among the columns of table 'orders'.\n\
         Did you mean: total?\n\
         Available columns: id, customer_id, total\n\n\
         {NOT_RUN}"
    );

    let mut projected = customers_join_orders(&connection_id, "inner");
    projected["columns"] = json!(["name", "orders.totl"]);

    let mut filtered = customers_join_orders(&connection_id, "inner");
    filtered["joins"][0]["alias"] = json!("o");
    filtered["joins"][0]["on"] = json!("customers.id = o.customer_id");
    filtered["columns"] = json!(["name"]);
    filtered["order_by"] = json!([{ "column": "customers.id" }]);
    filtered["where"] = json!({ "o.totl": { "$gte": 1 } });

    for arguments in [projected, filtered] {
        let message = tool_error(&agent, "select_data", arguments.clone()).await;

        assert_eq!(message, expected, "arguments: {arguments}");
    }
}

#[tokio::test]
async fn join_calls_refuse_what_the_engine_would_misread() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let mut qualified_join = customers_join_orders(&connection_id, "inner");
    qualified_join["joins"][0]["table"] = json!("aux.orders");

    let mut qualified_source = customers_join_orders(&connection_id, "inner");
    qualified_source["table"] = json!("aux.customers");

    let mut ilike = customers_join_orders(&connection_id, "inner");
    ilike["where"] = json!({ "name": { "$ilike": "a%" } });

    let mut database = customers_join_orders(&connection_id, "inner");
    database["database"] = json!("main`; DROP TABLE orders; --");

    let cases = [
        (
            qualified_join,
            "Table 'aux.orders': this connection does not qualify table names with a schema",
        ),
        (
            qualified_source,
            "Table 'aux.customers': this connection does not qualify table names with a schema",
        ),
        (
            ilike,
            "$ilike is not supported with joins on this connection",
        ),
        (database, "database name contains '`'"),
    ];

    for (arguments, expected) in cases {
        let error = tool_error(&agent, "select_data", arguments.clone()).await;

        assert!(
            error.contains(expected),
            "the rejection of {arguments} should mention '{expected}': {error}"
        );
    }

    let counted = agent
        .call_json(
            "count_records",
            json!({ "connection_id": connection_id, "table": "orders" }),
        )
        .await;
    assert_eq!(counted["count"], json!(3));
}
