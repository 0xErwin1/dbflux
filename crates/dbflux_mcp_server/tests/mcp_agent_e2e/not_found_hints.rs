use super::*;

// ---------------------------------------------------------------------------
// Not-found hints
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unlisted_table_gets_a_suggestion_from_every_read_tool() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, false).await;

    let calls = [
        (
            "select_data",
            json!({ "connection_id": connection_id, "table": "itms" }),
        ),
        (
            "count_records",
            json!({ "connection_id": connection_id, "table": "itms" }),
        ),
        (
            "aggregate_data",
            json!({
                "connection_id": connection_id,
                "table": "itms",
                "group_by": [],
                "aggregations": [{ "function": "count", "column": "*", "alias": "total" }]
            }),
        ),
    ];

    for (tool, arguments) in calls {
        let message = tool_error(&agent, tool, arguments).await;

        assert!(
            message.starts_with(&format!(
                "Table 'itms' is not listed in database 'main'. {NOT_LISTED_REASON}\n\
                 Did you mean: items?\n\nOriginal error: "
            )),
            "{tool} should say where the table was looked up, got: {message}"
        );
        assert!(
            message.contains("no such table"),
            "{tool} should keep the driver error, got: {message}"
        );
        assert!(
            !message.contains("another database"),
            "SQLite lists no other database, got: {message}"
        );
    }

    let qualified = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "other.itms" }),
    )
    .await;
    assert!(
        qualified.starts_with("Select error: "),
        "a qualifier that is not a known database keeps the driver error, got: {qualified}"
    );
}

#[tokio::test]
async fn misspelled_column_gets_a_suggestion() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, false).await;

    let correct = agent
        .call_json(
            "select_data",
            json!({ "connection_id": connection_id, "table": "items", "columns": ["label"] }),
        )
        .await;
    assert_eq!(correct["columns"], json!(["label"]));
    assert_eq!(correct["row_count"], json!(2));

    // SQLite reads an unknown double-quoted identifier as a string literal, so
    // a misspelled column alone does not fail. `$regex` renders an operator
    // SQLite rejects, so the call fails, which is when the hint is added.
    // `select_data` refuses the column before running (see the column check
    // tests below); `count_records` still gets the hint after the failure.
    let expected = "Column 'labl' is not listed among the columns of table 'items'.\n\
                    Did you mean: label?\n\
                    Available columns: id, label\n\nOriginal error: ";

    let counted = tool_error(
        &agent,
        "count_records",
        json!({
            "connection_id": connection_id,
            "table": "items",
            "where": { "labl": { "$regex": "^a" } }
        }),
    )
    .await;
    assert!(counted.starts_with(expected), "got: {counted}");
}

#[tokio::test]
async fn failure_on_an_existing_table_with_valid_columns_keeps_the_original_error() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, false).await;

    let message = tool_error(
        &agent,
        "select_data",
        json!({
            "connection_id": connection_id,
            "table": "items",
            "where": { "label": { "$regex": "^a" } }
        }),
    )
    .await;

    assert!(message.starts_with("Select error: "), "got: {message}");
    assert!(!message.contains("not listed"), "got: {message}");
    assert!(!message.contains("Did you mean"), "got: {message}");
}

#[tokio::test]
async fn another_database_note_needs_several_databases_and_no_database_argument() {
    let (agent, connection_id) = start_hint_agent(ALLOW_ALL_ROLE, &["postgres", "analytics"]).await;

    let without_database = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "usres" }),
    )
    .await;
    assert_eq!(
        without_database,
        format!(
            "Table 'usres' is not listed in schema 'public' of database 'postgres'. \
             {NOT_LISTED_REASON}\n\
             Did you mean: users?\n\
             This call did not pass `database`, so the table may be in another database. \
             Pass `database` to select one (`list_databases` lists them).\n\n\
             Original error: Select error: engine says no"
        )
    );

    let with_database = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "usres", "database": "postgres" }),
    )
    .await;
    assert!(
        with_database.contains("Did you mean: users?"),
        "got: {with_database}"
    );
    assert!(
        !with_database.contains("another database"),
        "the call chose its database, got: {with_database}"
    );

    let described = tool_error(
        &agent,
        "describe_object",
        json!({ "connection_id": connection_id, "name": "ordrs" }),
    )
    .await;
    assert!(
        described.starts_with(&format!(
            "Table 'ordrs' is not listed in schema 'public' of database 'postgres'. \
             {NOT_LISTED_REASON}\nDid you mean: orders?\n"
        )),
        "describe_object looks in the default schema only, got: {described}"
    );
}

#[tokio::test]
async fn another_database_note_is_absent_with_a_single_database() {
    let (agent, connection_id) = start_hint_agent(ALLOW_ALL_ROLE, &["postgres"]).await;

    let message = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "usres" }),
    )
    .await;

    assert!(message.contains("Did you mean: users?"), "got: {message}");
    assert!(!message.contains("another database"), "got: {message}");
}

#[tokio::test]
async fn hint_is_skipped_when_the_metadata_cannot_settle_the_failure() {
    let (agent, connection_id) = start_hint_agent(ALLOW_ALL_ROLE, &["postgres", "analytics"]).await;
    let original = "Select error: engine says no";

    let cases = [
        ("an existing table", json!({ "table": "users" })),
        (
            "a qualifier that is not a schema of the database",
            json!({ "table": "nope.usres" }),
        ),
        (
            "an order_by expression",
            json!({ "table": "users", "order_by": [{ "column": "lower(emial)" }] }),
        ),
        (
            "a table with an empty column list",
            json!({ "table": "orders", "where": { "totl": 1 } }),
        ),
    ];

    for (case, mut arguments) in cases {
        arguments["connection_id"] = json!(connection_id);

        let message = tool_error(&agent, "select_data", arguments).await;

        assert_eq!(message, original, "{case} keeps the driver error");
    }

    let nested_path = tool_error(
        &agent,
        "select_data",
        json!({
            "connection_id": connection_id,
            "table": "users",
            "where": { "profile.address.city": "x" }
        }),
    )
    .await;
    assert!(
        nested_path.starts_with("Select error: ") && !nested_path.contains("not listed"),
        "a nested path is not checked and reaches the driver: {nested_path}"
    );

    let denied = tool_error(
        &agent,
        "count_records",
        json!({ "connection_id": connection_id, "table": "usres" }),
    )
    .await;
    assert_eq!(
        denied, "Count error: Permission denied: no access",
        "a permission error is never turned into a not-listed hint"
    );

    let unreadable_columns = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "usres", "where": { "emial": "a" } }),
    )
    .await;
    assert!(
        unreadable_columns.starts_with("Table 'usres' is not listed")
            && unreadable_columns.ends_with(original),
        "a table whose columns cannot be read is not refused and runs: {unreadable_columns}"
    );

    let column = tool_error(
        &agent,
        "select_data",
        json!({ "connection_id": connection_id, "table": "users", "where": { "emial": "a" } }),
    )
    .await;
    assert_eq!(
        column,
        format!(
            "Column 'emial' is not listed among the columns of table 'users'.\n\
             Did you mean: email?\n\
             Available columns: id, email\n\n\
             Original error: {original}"
        ),
        "a dialect that fails loudly runs the call and hints after the failure"
    );
}

#[tokio::test]
async fn hint_only_includes_what_the_client_may_list() {
    let original = "Select error: engine says no";
    let unlisted_table = json!({ "table": "usres" });
    let misspelled_column = json!({ "table": "users", "where": { "emial": "a" } });

    let (read_only, connection_id) =
        start_hint_agent(READ_TOOLS_ONLY_ROLE, &["postgres", "analytics"]).await;

    let mut arguments = unlisted_table.clone();
    arguments["connection_id"] = json!(connection_id);
    let message = tool_error(&read_only, "select_data", arguments).await;
    assert_eq!(
        message, original,
        "a client that may not list tables or describe objects gets no names"
    );

    let mut arguments = misspelled_column.clone();
    arguments["connection_id"] = json!(connection_id);
    let message = tool_error(&read_only, "select_data", arguments).await;
    assert_eq!(
        message, original,
        "a client that may not describe objects gets no column names"
    );

    let (with_tables, connection_id) =
        start_hint_agent(READ_AND_LIST_TABLES_ROLE, &["postgres", "analytics"]).await;

    let mut arguments = unlisted_table;
    arguments["connection_id"] = json!(connection_id);
    let message = tool_error(&with_tables, "select_data", arguments).await;
    assert_eq!(
        message,
        format!(
            "Table 'usres' is not listed in schema 'public'. {NOT_LISTED_REASON}\n\
             Did you mean: users?\n\n\
             Original error: {original}"
        ),
        "list_tables allows the table names, and nothing about databases"
    );

    let mut arguments = misspelled_column;
    arguments["connection_id"] = json!(connection_id);
    let message = tool_error(&with_tables, "select_data", arguments).await;
    assert_eq!(
        message, original,
        "column names need describe_object, which this client may not call"
    );
}
