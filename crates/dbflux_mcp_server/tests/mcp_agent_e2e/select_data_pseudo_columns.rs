use super::*;

// ---------------------------------------------------------------------------
// select_data pseudo-columns
// ---------------------------------------------------------------------------

async fn start_rowid_agent() -> (Agent, String, tempfile::TempDir) {
    let directory = tempfile::tempdir().expect("create the test data directory");
    create_rowid_tables(&directory);

    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    agent
        .call_json("connect", json!({ "connection_id": connection_id }))
        .await;

    (agent, connection_id, directory)
}

#[tokio::test]
async fn select_data_accepts_sqlite_rowid_on_a_rowid_table() {
    let (agent, connection_id, _directory) = start_rowid_agent().await;

    let filtered = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "notes",
                "columns": ["label"],
                "where": { "ROWID": 2 }
            }),
        )
        .await;
    assert_eq!(filtered["rows"], json!([{ "label": "beta" }]));

    let sorted = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "notes",
                "order_by": [{ "column": "_rowid_", "direction": "desc" }],
                "limit": 2
            }),
        )
        .await;
    assert_eq!(
        sorted["rows"],
        json!([{ "label": "gamma" }, { "label": "beta" }])
    );
}

#[tokio::test]
async fn select_data_without_joins_returns_a_projected_pseudo_column() {
    let (agent, connection_id, _directory) = start_rowid_agent().await;

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "notes",
                "columns": ["rowid", "label"],
                "where": { "rowid": { "$gte": 2 } },
                "order_by": [{ "column": "rowid", "direction": "desc" }],
                "limit": 2
            }),
        )
        .await;

    assert_eq!(
        selected,
        json!({
            "columns": ["rowid", "label"],
            "rows": [
                { "rowid": 3, "label": "gamma" },
                { "rowid": 2, "label": "beta" }
            ],
            "row_count": 2
        })
    );

    let paged = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "notes",
                "columns": ["label", "OID"],
                "order_by": [{ "column": "label" }],
                "limit": 1,
                "offset": 1
            }),
        )
        .await;

    assert_eq!(
        paged,
        json!({
            "columns": ["label", "OID"],
            "rows": [{ "label": "beta", "OID": 2 }],
            "row_count": 1
        })
    );
}

/// SQLite names a projected `rowid` after the `INTEGER PRIMARY KEY` it
/// aliases. The result still uses the name the call wrote.
#[tokio::test]
async fn a_projected_rowid_keeps_its_requested_name_when_it_aliases_a_column() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, false).await;

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "items",
                "columns": ["_rowid_", "label"],
                "where": { "label": "beta" }
            }),
        )
        .await;

    assert_eq!(
        selected,
        json!({
            "columns": ["_rowid_", "label"],
            "rows": [{ "_rowid_": 2, "label": "beta" }],
            "row_count": 1
        })
    );
}

#[tokio::test]
async fn a_projected_pseudo_column_records_the_generated_query() {
    let (agent, connection_id, _directory) = start_rowid_agent().await;

    agent
        .call_json(
            "select_data",
            json!({ "connection_id": connection_id, "table": "notes", "columns": ["label"] }),
        )
        .await;
    let browsed = latest_select_data_audit_details(&agent).await;
    assert!(
        browsed.get("query").is_none(),
        "a call without a projected pseudo-column keeps the browse path: {browsed}"
    );

    agent
        .call_json(
            "select_data",
            json!({ "connection_id": connection_id, "table": "notes", "columns": ["rowid"] }),
        )
        .await;
    let generated = latest_select_data_audit_details(&agent).await;
    assert!(
        generated["query"]
            .as_str()
            .is_some_and(|query| query.starts_with("[FINGERPRINT:")),
        "a projected pseudo-column records its generated query: {generated}"
    );
}

#[tokio::test]
async fn a_projected_pseudo_column_refuses_filters_the_generated_query_cannot_express() {
    let (agent, connection_id, _directory) = start_rowid_agent().await;

    let message = tool_error(
        &agent,
        "select_data",
        json!({
            "connection_id": connection_id,
            "table": "notes",
            "columns": ["rowid", "label"],
            "where": { "label": { "$regex": "^a" } }
        }),
    )
    .await;

    assert!(
        message.starts_with("Filter error with a pseudo-column in columns: "),
        "got: {message}"
    );
}

#[tokio::test]
async fn select_data_keeps_pseudo_columns_out_of_the_refusal_hints() {
    let (agent, connection_id, _directory) = start_rowid_agent().await;

    let misspelled = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "notes", "where": { "labl": "alpha" } }),
    )
    .await;
    assert_eq!(
        misspelled,
        format!(
            "Column 'labl' is not listed among the columns of table 'notes'.\n\
             Did you mean: label?\n\
             Available columns: label\n\n\
             {NOT_RUN}"
        )
    );

    let near_rowid = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "notes", "columns": ["rowidd"] }),
    )
    .await;
    assert_eq!(
        near_rowid,
        format!(
            "Column 'rowidd' is not listed among the columns of table 'notes'.\n\
             Available columns: label\n\n\
             {NOT_RUN}"
        )
    );
}

#[tokio::test]
async fn select_data_refuses_rowid_on_a_without_rowid_table() {
    let (agent, connection_id, _directory) = start_rowid_agent().await;

    let cases = [
        ("where", json!({ "where": { "rowid": 1 } })),
        ("columns", json!({ "columns": ["rowid", "label"] })),
    ];

    for (case, mut arguments) in cases {
        arguments["connection_id"] = json!(connection_id);
        arguments["table"] = json!("codes");

        let message = tool_error(&agent, "select_data", arguments).await;

        assert!(
            message.starts_with("Column 'rowid' is not listed among the columns of table 'codes'."),
            "rowid in {case} must be refused, not read as a string literal: {message}"
        );
        assert!(message.ends_with(NOT_RUN), "rowid in {case}: {message}");
    }
}

#[tokio::test]
async fn join_call_accepts_a_pseudo_column_qualified_by_an_alias() {
    let (agent, connection_id, _directory) = start_join_agent().await;

    let mut arguments = customers_join_orders(&connection_id, "inner");
    arguments["joins"][0]["alias"] = json!("o");
    arguments["joins"][0]["on"] = json!("customers.id = o.customer_id");
    arguments["columns"] = json!(["name", "o.rowid", "customers.oid"]);
    arguments["where"] = json!({ "o.rowid": { "$gte": 11 } });
    arguments["order_by"] = json!([{ "column": "o._rowid_" }]);

    let selected = agent.call_json("select_data", arguments).await;

    assert_eq!(
        selected["rows"],
        json!([
            { "name": "Ada", "o.rowid": 11, "customers.oid": 1 },
            { "name": "Bo", "o.rowid": 12, "customers.oid": 2 }
        ])
    );
}

#[tokio::test]
async fn failure_hint_does_not_blame_a_declared_pseudo_column() {
    let (agent, connection_id, _directory) = start_rowid_agent().await;

    // `$regex` renders an operator SQLite rejects, so the call fails and the
    // not-found hint runs over the columns the filter names.
    let message = tool_error(
        &agent,
        "count_records",
        json!({
            "connection_id": connection_id,
            "table": "notes",
            "where": { "rowid": { "$regex": "^1" } }
        }),
    )
    .await;

    assert!(!message.contains("not listed"), "got: {message}");
    assert!(!message.contains("Did you mean"), "got: {message}");
}

#[tokio::test]
async fn a_projected_pseudo_column_keeps_the_browse_path_without_a_query_generator() {
    let (agent, connection_id) = start_hint_agent(ALLOW_ALL_ROLE, &["postgres"]).await;

    let browsed = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "users", "columns": ["id"] }),
    )
    .await;

    let projected = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "users", "columns": ["id", "rowid"] }),
    )
    .await;

    assert!(browsed.starts_with("Select error: "), "got: {browsed}");
    assert_eq!(
        projected, browsed,
        "without a generator the call takes the browse path and fails like it"
    );
}
