use dbflux_audit::export::AuditExportFormat;
use dbflux_audit::query::AuditQueryFilter;
use dbflux_audit::{AuditService, temp_sqlite_path};
use dbflux_core::observability::actions::MCP_REJECT_EXECUTION;
use dbflux_core::observability::types::{EventCategory, EventOutcome, EventRecord, EventSeverity};

fn service_for_test(name: &str) -> AuditService {
    let path = temp_sqlite_path(name);

    if path.exists() {
        std::fs::remove_file(&path).expect("remove stale sqlite file");
    }

    AuditService::new_sqlite(&path).expect("sqlite service should initialize")
}

#[test]
fn appends_allow_deny_failure_and_supports_filtered_export() {
    let service = service_for_test("dbflux-audit-e2e.sqlite");

    // Event 1: Success query -> legacy decision "allow" (should NOT appear in deny/failure queries)
    service
        .record(
            EventRecord::new(
                1000,
                EventSeverity::Info,
                EventCategory::Query,
                EventOutcome::Success,
            )
            .with_typed_action(dbflux_core::observability::actions::QUERY_EXECUTE)
            .with_summary("Query executed")
            .with_actor_id("agent-a")
            .with_connection_context("conn-1", "main", "sqlite")
            .with_duration_ms(10),
        )
        .expect("allow record should succeed");

    // Second event: MCP reject -> legacy decision "deny"
    // (MCP category does not require duration_ms)
    service
        .record(
            EventRecord::new(
                1001,
                EventSeverity::Warn,
                EventCategory::Mcp,
                EventOutcome::Failure,
            )
            .with_typed_action(MCP_REJECT_EXECUTION)
            .with_summary("Rejected pending execution")
            .with_actor_id("agent-a")
            .with_object_ref("pending_execution", "pending-1")
            .with_error("policy", "policy denied"),
        )
        .expect("deny record should succeed");

    // Event 3: Query execution failed -> legacy decision "failure"
    // (Query category with query_execute_failed action and Failure outcome maps to "failure")
    service
        .record(
            EventRecord::new(
                1002,
                EventSeverity::Error,
                EventCategory::Query,
                EventOutcome::Failure,
            )
            .with_typed_action(dbflux_core::observability::actions::QUERY_EXECUTE_FAILED)
            .with_summary("Access denied")
            .with_actor_id("agent-a")
            .with_connection_context("conn-1", "main", "sqlite")
            .with_error("access denied", "access open failed")
            .with_duration_ms(5),
        )
        .expect("failure record should succeed");

    // Query events: 1 (success) and 1 (failure) = 2 total
    // MCP events: 1 (deny) = 1 total
    let all_events = service
        .query(&AuditQueryFilter::default())
        .expect("query should succeed");
    assert_eq!(all_events.len(), 3);

    // Only the MCP reject event maps to "deny" (Event 2)
    let denied_only = service
        .query(&AuditQueryFilter {
            decision: Some("deny".to_string()),
            ..Default::default()
        })
        .expect("query should succeed");

    assert_eq!(denied_only.len(), 1);
    assert_eq!(denied_only[0].tool_id, "reject_execution");

    // Only the query_execute_failed event maps to "failure" (Event 3)
    let failure_only = service
        .query(&AuditQueryFilter {
            decision: Some("failure".to_string()),
            ..Default::default()
        })
        .expect("query should succeed");

    assert_eq!(failure_only.len(), 1);
    assert_eq!(failure_only[0].tool_id, "query_execute_failed");

    // CSV export for failure events should show only event 3
    let csv = service
        .export(
            &AuditQueryFilter {
                decision: Some("failure".to_string()),
                ..Default::default()
            },
            AuditExportFormat::Csv,
        )
        .expect("csv export should succeed");
    assert!(csv.contains("failure"));
    assert!(!csv.contains("policy denied"));

    // JSON export for agent-a in time range [1001, 1002] includes 2 events (event 2 and 3)
    let json = service
        .export(
            &AuditQueryFilter {
                actor_id: Some("agent-a".to_string()),
                start_epoch_ms: Some(1001),
                end_epoch_ms: Some(1002),
                ..Default::default()
            },
            AuditExportFormat::Json,
        )
        .expect("json export should succeed");

    assert!(json.contains("reject_execution"));
    assert!(json.contains("query_execute_failed"));
    assert!(!json.contains("\"decision\": \"allow\""));
}

#[test]
fn legacy_query_preserves_oldest_first_order_for_consumers() {
    let service = service_for_test("dbflux-audit-legacy-order.sqlite");

    // First event: query execution (ts=1000)
    service
        .record(
            EventRecord::new(
                1000,
                EventSeverity::Info,
                EventCategory::Query,
                EventOutcome::Success,
            )
            .with_typed_action(dbflux_core::observability::actions::QUERY_EXECUTE)
            .with_summary("Query executed")
            .with_actor_id("agent-a")
            .with_connection_context("conn-1", "main", "sqlite")
            .with_duration_ms(10),
        )
        .expect("first record should succeed");

    // Second event: script execution denied (ts=1001)
    service
        .record(
            EventRecord::new(
                1001,
                EventSeverity::Warn,
                EventCategory::Script,
                EventOutcome::Failure,
            )
            .with_action("run_script")
            .with_summary("Script denied")
            .with_actor_id("agent-a")
            .with_error("policy", "policy")
            .with_object_ref("script", "script-1"),
        )
        .expect("second record should succeed");

    let events = service
        .query(&AuditQueryFilter::default())
        .expect("query should succeed");

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].tool_id, "query_execute"); // oldest first (after reverse)
    assert_eq!(events[1].tool_id, "run_script");
}
