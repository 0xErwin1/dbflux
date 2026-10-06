use super::*;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn agent_handshake_advertises_the_tool_catalog() {
    let agent = start_agent(ADMIN_ROLE, None).await;

    let info = agent
        .client
        .peer_info()
        .expect("the handshake should have negotiated server info")
        .clone();
    assert!(
        info.capabilities.tools.is_some(),
        "the server must advertise the tools capability"
    );
    assert!(
        info.capabilities.resources.is_none() && info.capabilities.prompts.is_none(),
        "the server advertises no resources or prompts, and must not claim otherwise"
    );

    let tools = agent.tools().await;
    assert!(
        tools.len() >= 30,
        "the catalog should expose the full tool surface, got {}",
        tools.len()
    );

    for tool in &tools {
        let schema_type = tool.input_schema.get("type").and_then(Value::as_str);
        assert_eq!(
            schema_type,
            Some("object"),
            "tool '{}' must publish an object input schema",
            tool.name
        );
    }

    for expected in [
        "list_connections",
        "list_tables",
        "select_data",
        "count_records",
        "preview_mutation",
        "create_table",
        "insert_record",
        "delete_records",
        "request_execution",
        "approve_execution",
        "query_audit_logs",
    ] {
        assert!(
            tools.iter().any(|tool| tool.name.as_ref() == expected),
            "tool '{expected}' is missing from the catalog"
        );
    }
}

#[tokio::test]
async fn agent_connects_and_browses_a_sqlite_profile() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ADMIN_ROLE, Some((sqlite_driver(), profile))).await;

    let connections = agent.call_json("list_connections", json!({})).await;
    let listed = connections["connections"]
        .as_array()
        .expect("list_connections should return a connections array");
    assert_eq!(
        listed.len(),
        1,
        "only the connection assigned to this actor should be listed"
    );
    assert_eq!(listed[0]["id"], json!(connection_id));
    assert_eq!(listed[0]["driver_id"], json!("sqlite"));

    let connected = agent
        .call_json("connect", json!({ "connection_id": connection_id }))
        .await;
    assert_eq!(connected["success"], json!(true));
    assert_eq!(
        connected["current_database"],
        json!("main"),
        "connect should name the database the session is on, got {connected}"
    );
    assert!(
        connected.get("databases").is_none(),
        "SQLite lists no databases, so the field is omitted, got {connected}"
    );

    let tables = agent
        .call_json("list_tables", json!({ "connection_id": connection_id }))
        .await;
    assert!(
        tables["tables"]
            .as_array()
            .expect("list_tables should return a tables array")
            .is_empty(),
        "a fresh database has no tables"
    );

    let info = agent
        .call_json(
            "get_connection_info",
            json!({ "connection_id": connection_id }),
        )
        .await;
    assert!(
        info.is_object(),
        "get_connection_info should describe the live connection, got {info}"
    );

    let disconnected = agent
        .call_json("disconnect", json!({ "connection_id": connection_id }))
        .await;
    assert_eq!(disconnected["success"], json!(true));
}

#[tokio::test]
async fn agent_creates_inserts_and_reads_rows_over_the_wire() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    let inserted = prepare_items_table(&agent, &connection_id, false).await;
    assert_eq!(inserted["inserted"], json!(2));

    let selected = agent
        .call_json(
            "select_data",
            json!({
                "connection_id": connection_id,
                "table": "items",
                "where": { "label": "alpha" },
                "limit": 10
            }),
        )
        .await;

    let rows = selected["rows"]
        .as_array()
        .expect("select_data should return a rows array");
    assert_eq!(rows.len(), 1, "the filter should select exactly one row");
    assert_eq!(rows[0]["id"], json!(1));
    assert_eq!(rows[0]["label"], json!("alpha"));

    let counted = agent
        .call_json(
            "count_records",
            json!({ "connection_id": connection_id, "table": "items" }),
        )
        .await;
    assert_eq!(counted["count"], json!(2));

    let tables = agent
        .call_json("list_tables", json!({ "connection_id": connection_id }))
        .await;
    let table_names: Vec<&str> = tables["tables"]
        .as_array()
        .expect("list_tables should return a tables array")
        .iter()
        .filter_map(|table| table.get("name").and_then(Value::as_str))
        .collect();
    assert!(
        table_names.contains(&"items"),
        "the table the agent created should be visible through list_tables, got {table_names:?}"
    );
}

#[tokio::test]
async fn list_tables_names_only_returns_names_as_strings() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ALLOW_ALL_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, false).await;

    let detailed = agent
        .call_json("list_tables", json!({ "connection_id": connection_id }))
        .await;
    let entries = detailed["tables"]
        .as_array()
        .expect("list_tables should return a tables array");
    assert_eq!(entries.len(), 1, "got {detailed}");

    let entry = entries[0]
        .as_object()
        .expect("without names_only each entry is an object");
    assert_eq!(entry["name"], json!("items"));
    assert_eq!(entry["kind"], json!("Table"));
    assert!(entry.contains_key("schema"), "got {detailed}");
    assert_eq!(entry.len(), 3, "got {detailed}");

    let explicit_default = agent
        .call_json(
            "list_tables",
            json!({ "connection_id": connection_id, "names_only": false }),
        )
        .await;
    assert_eq!(
        explicit_default, detailed,
        "names_only: false is the same as leaving it out"
    );

    let names = agent
        .call_json(
            "list_tables",
            json!({ "connection_id": connection_id, "names_only": true }),
        )
        .await;
    assert_eq!(names, json!({ "tables": ["items"] }));

    let collection_names = agent
        .call_json(
            "list_collections",
            json!({ "connection_id": connection_id, "names_only": true }),
        )
        .await;
    assert_eq!(collection_names, names);
}

#[tokio::test]
async fn read_only_agent_denial_is_a_jsonrpc_error_and_is_audited() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(READ_ONLY_ROLE, Some((sqlite_driver(), profile))).await;

    let error = agent
        .try_call(
            "insert_record",
            json!({
                "connection_id": connection_id,
                "table": "items",
                "records": [{ "id": 1, "label": "alpha" }]
            }),
        )
        .await
        .expect_err("a read-only actor must not be allowed to write");

    // A policy denial is a protocol error, not a tool result: an agent sees
    // `error.code` / `error.data.code`, never `is_error`.
    let rmcp::ServiceError::McpError(error) = error else {
        panic!("expected an MCP error from the denial, got a different transport error");
    };
    assert_eq!(error.code, ErrorCode::INVALID_REQUEST);
    assert_eq!(error.message.as_ref(), "tool denied by policy");
    assert_eq!(
        error.data.as_ref().and_then(|data| data.get("code")),
        Some(&json!("policy_denied"))
    );

    // The denial is the agent's own audited evidence. `query_audit_logs` returns
    // the legacy projection (`id, actor_id, tool_id, decision, reason`), where
    // `tool_id` is the audit action rather than the MCP tool name and the deny
    // reason is not surfaced, so the decision is what an agent can read back.
    let events = agent
        .call_json(
            "query_audit_logs",
            json!({ "actor_id": AGENT, "limit": 50 }),
        )
        .await;
    let entries = events
        .as_array()
        .expect("query_audit_logs should return an array of events");
    assert!(
        !entries.is_empty(),
        "the agent should be able to read its own audit trail: {events}"
    );

    let denials: Vec<&Value> = entries
        .iter()
        .filter(|entry| entry["decision"] == json!("failure"))
        .collect();
    assert_eq!(
        denials.len(),
        1,
        "exactly one authorization denial should be recorded: {events}"
    );
    assert_eq!(denials[0]["actor_id"], json!(AGENT));
}

#[tokio::test]
async fn ask_call_is_queued_approved_by_a_person_and_runs_exactly_once() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ADMIN_ROLE, Some((sqlite_driver(), profile))).await;

    let inserted = prepare_items_table(&agent, &connection_id, true).await;
    assert_eq!(inserted["inserted"], json!(2));

    let delete_arguments = json!({
        "connection_id": connection_id,
        "table": "items",
        "where": { "id": 1 }
    });

    let pending_id = expect_queued(
        agent
            .try_call("delete_records", delete_arguments.clone())
            .await,
    );

    let pending = agent
        .call_json("list_pending_executions", json!({ "actor_id": AGENT }))
        .await;
    assert!(
        pending.to_string().contains(&pending_id),
        "the queued call should be listed: {pending}"
    );

    let counted_before = agent
        .call_json(
            "count_records",
            json!({ "connection_id": connection_id, "table": "items" }),
        )
        .await;
    assert_eq!(
        counted_before["count"],
        json!(2),
        "a queued call must not run before it is approved"
    );

    agent.approve_as_person(&pending_id).await;

    let executed = agent
        .call_json("delete_records", delete_arguments.clone())
        .await;
    assert_eq!(executed["deleted"], json!(1));

    let second_pending = expect_queued(agent.try_call("delete_records", delete_arguments).await);
    assert_ne!(
        second_pending, pending_id,
        "one approval authorizes one call; repeating it queues a new request"
    );
}

#[tokio::test]
async fn rejected_call_never_runs() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ADMIN_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, true).await;

    let delete_arguments = json!({
        "connection_id": connection_id,
        "table": "items",
        "where": { "id": 2 }
    });
    let pending_id = expect_queued(
        agent
            .try_call("delete_records", delete_arguments.clone())
            .await,
    );

    agent
        .runtime
        .write()
        .await
        .reject_pending_execution_with_origin_mut(
            &pending_id,
            "local",
            Some("not approved"),
            EventOrigin::local(),
        )
        .expect("a person should be able to reject the pending execution");

    let status = agent
        .call_json("get_pending_execution", json!({ "pending_id": pending_id }))
        .await;
    assert_eq!(status["status"], json!("rejected"));
    assert_eq!(
        status["reason"],
        json!("not approved"),
        "the agent should read the reason the person gave: {status}"
    );

    expect_queued(agent.try_call("delete_records", delete_arguments).await);

    let counted = agent
        .call_json(
            "count_records",
            json!({ "connection_id": connection_id, "table": "items" }),
        )
        .await;
    assert_eq!(counted["count"], json!(2));
}

#[tokio::test]
async fn agent_requests_approval_and_cannot_approve_it_itself() {
    let directory = tempfile::tempdir().expect("create the test data directory");
    let profile = sqlite_profile(&directory);
    let connection_id = profile.id.to_string();
    let agent = start_agent(ADMIN_ROLE, Some((sqlite_driver(), profile))).await;

    prepare_items_table(&agent, &connection_id, true).await;

    let delete_arguments = json!({
        "connection_id": connection_id,
        "table": "items",
        "where": { "id": 1 }
    });

    let requested = agent
        .call_json(
            "request_execution",
            json!({
                "tool_id": "delete_records",
                "connection_id": connection_id,
                "params": delete_arguments
            }),
        )
        .await;
    let pending_id = requested["pending_id"]
        .as_str()
        .expect("request_execution should return a pending id")
        .to_string();
    assert_eq!(requested["status"], json!("pending"));

    for tool in ["approve_execution", "reject_execution"] {
        let error = agent
            .try_call(tool, json!({ "pending_id": pending_id }))
            .await
            .expect_err("an MCP client must never resolve a pending execution");
        let rmcp::ServiceError::McpError(error) = error else {
            panic!("expected an MCP error for {tool}");
        };
        assert_eq!(
            error.data.as_ref().and_then(|data| data.get("code")),
            Some(&json!("self_approval_forbidden")),
            "{tool}"
        );
    }

    agent.approve_as_person(&pending_id).await;

    let executed = agent.call_json("delete_records", delete_arguments).await;
    assert_eq!(executed["deleted"], json!(1));

    let counted = agent
        .call_json(
            "count_records",
            json!({ "connection_id": connection_id, "table": "items" }),
        )
        .await;
    assert_eq!(counted["count"], json!(1));
}
