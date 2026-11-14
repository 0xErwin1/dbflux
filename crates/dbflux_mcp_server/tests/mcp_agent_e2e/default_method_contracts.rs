//! Contracts for the `Connection` SQL-building trait defaults that used to
//! panic.
//!
//! Every call here crosses the real JSON-RPC boundary on the `HintDriver`
//! fixture from `main.rs`, whose connection inherits every trait default: it
//! overrides neither `build_truncate_sql`, nor `build_drop_index_sql`, nor
//! `version_query`. The calls are time-bounded because the old panicking
//! defaults killed the request instead of answering it.

use std::time::Duration;

use super::*;

const CALL_TIMEOUT: Duration = Duration::from_secs(15);

async fn start_default_method_agent() -> (Agent, String, Arc<HintDriver>) {
    let profile = ConnectionProfile::new("agent-fake", DbConfig::default_postgres());
    let connection_id = profile.id.to_string();
    let driver = HintDriver::with_databases(&["postgres"]);
    let agent = start_agent(ALLOW_ALL_ROLE, Some((driver.clone(), profile))).await;
    (agent, connection_id, driver)
}

/// Asserts that a tool call comes back as an MCP error whose message names the
/// operation and the driver, instead of hanging, panicking, or succeeding.
#[expect(
    clippy::panic,
    reason = "asserts the exact failure shape of a tool call on a driver without the \
        operation: a hang, a transport death, or a success must abort the test with \
        the observed outcome"
)]
async fn expect_operation_and_driver_error(
    agent: &Agent,
    tool: &str,
    arguments: Value,
    operation: &str,
    driver: &str,
) {
    let call = agent.try_call(tool, arguments);
    let result = tokio::time::timeout(CALL_TIMEOUT, call)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "'{tool}' never answered within {CALL_TIMEOUT:?}; the default method \
             likely panicked and killed the request instead of returning an error"
            )
        });

    let error = match result {
        Ok(ok) => {
            panic!("'{tool}' must fail on a driver without the operation, but it succeeded: {ok:?}")
        }
        Err(rmcp::ServiceError::McpError(error)) => error,
        Err(protocol_error) => panic!("'{tool}' failed at the transport level: {protocol_error}"),
    };

    let message = error.message.to_string();
    assert!(
        message.to_lowercase().contains(operation),
        "the error must name the '{operation}' operation: {message}"
    );
    assert!(
        message.contains(driver),
        "the error must name the '{driver}' driver: {message}"
    );
}

#[tokio::test]
async fn truncate_table_without_driver_support_is_a_tool_error_naming_operation_and_driver() {
    let (agent, connection_id, driver) = start_default_method_agent().await;

    expect_operation_and_driver_error(
        &agent,
        "truncate_table",
        json!({ "connection_id": connection_id, "table": "users", "confirm": "users" }),
        "truncate",
        "Postgres",
    )
    .await;
    assert!(driver.inner.stats().executed_requests.is_empty());
    assert!(driver.inner.stats().crud_ops.is_empty());
}

#[tokio::test]
async fn drop_index_without_driver_support_is_a_tool_error_naming_operation_and_driver() {
    let (agent, connection_id, driver) = start_default_method_agent().await;

    expect_operation_and_driver_error(
        &agent,
        "drop_index",
        json!({
            "connection_id": connection_id,
            "table": "users",
            "index_name": "idx_users_email",
            "if_exists": false
        }),
        "drop index",
        "Postgres",
    )
    .await;
    assert!(driver.inner.stats().executed_requests.is_empty());
    assert!(driver.inner.stats().crud_ops.is_empty());
}

#[tokio::test]
async fn connection_info_without_version_query_keeps_its_other_fields() {
    let (agent, connection_id, driver) = start_default_method_agent().await;

    let call = agent.try_call(
        "get_connection_info",
        json!({ "connection_id": connection_id }),
    );
    let result = tokio::time::timeout(CALL_TIMEOUT, call)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "'get_connection_info' never answered within {CALL_TIMEOUT:?}; a missing \
             version must not make the whole call fail"
            )
        });

    let result = match result {
        Ok(ok) => ok,
        Err(protocol_error) => {
            panic!("'get_connection_info' failed at the transport level: {protocol_error}")
        }
    };

    assert_ne!(result.is_error, Some(true));
    assert!(driver.inner.stats().executed_requests.is_empty());
    let info: Value = serde_json::from_str(&text_content(&result))
        .expect("get_connection_info returns a JSON document");

    assert_eq!(info["connection_id"], json!(connection_id), "got {info}");
    assert_eq!(info["driver_type"], json!("Postgres"), "got {info}");
    assert_eq!(info["category"], json!("Relational"), "got {info}");
    assert_eq!(info["status"], json!("connected"), "got {info}");
    assert_eq!(info["current_database"], json!("postgres"), "got {info}");
    assert!(
        info.get("version").is_none() || info["version"].is_null(),
        "a driver without version_query must not report a version: {info}"
    );
}
