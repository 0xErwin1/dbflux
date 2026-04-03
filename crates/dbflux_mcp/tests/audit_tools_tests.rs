use dbflux_audit::AuditService;
use dbflux_core::observability::{
    EventCategory, EventOutcome, EventRecord, EventSeverity,
    actions::{CONFIG_UPDATE, MCP_REJECT_EXECUTION},
};
use dbflux_mcp::governance_service::{AuditExportFormat, AuditQuery};
use dbflux_mcp::handlers::audit::{
    export_audit_logs, export_audit_logs_extended, get_audit_entry, get_audit_entry_extended,
    query_audit_logs, query_audit_logs_extended,
};

fn audit_service_for_test(file_name: &str) -> AuditService {
    let path = dbflux_audit::temp_sqlite_path(file_name);
    let _ = std::fs::remove_file(&path);
    AuditService::new_sqlite(&path).expect("audit service should initialize")
}

#[test]
fn audit_tools_filter_lookup_and_export_filtered_records() {
    use dbflux_core::observability::actions::MCP_APPROVE_EXECUTION;

    let service = audit_service_for_test("dbflux-mcp-audit-tools.sqlite");

    // Event 1: agent-a, MCP approve execution success -> legacy "allow"
    let first = service
        .record(
            EventRecord::new(
                1_000,
                EventSeverity::Info,
                EventCategory::Mcp,
                EventOutcome::Success,
            )
            .with_typed_action(MCP_APPROVE_EXECUTION)
            .with_summary("Approved execution")
            .with_actor_id("agent-a")
            .with_object_ref("pending_execution", "pending-1"),
        )
        .expect("first record should succeed");

    // Event 2: agent-b, MCP approve execution success -> legacy "allow"
    service
        .record(
            EventRecord::new(
                1_001,
                EventSeverity::Info,
                EventCategory::Mcp,
                EventOutcome::Success,
            )
            .with_typed_action(MCP_APPROVE_EXECUTION)
            .with_summary("Approved execution")
            .with_actor_id("agent-b")
            .with_object_ref("pending_execution", "pending-2"),
        )
        .expect("second record should succeed");

    // Event 3: agent-a, MCP reject execution failure -> legacy "deny"
    service
        .record(
            EventRecord::new(
                1_002,
                EventSeverity::Warn,
                EventCategory::Mcp,
                EventOutcome::Failure,
            )
            .with_typed_action(MCP_REJECT_EXECUTION)
            .with_summary("Rejected execution")
            .with_actor_id("agent-a")
            .with_object_ref("pending_execution", "pending-3")
            .with_error("policy", "policy denied"),
        )
        .expect("third record should succeed");

    let allow_for_agent_a = AuditQuery {
        actor_id: Some("agent-a".to_string()),
        tool_id: None,
        decision: Some("allow".to_string()),
        start_epoch_ms: None,
        end_epoch_ms: None,
        limit: None,
    };

    let filtered = query_audit_logs(&service, &allow_for_agent_a).expect("query should succeed");
    assert_eq!(filtered.len(), 1);
    let first_id = first.id.expect("id should be assigned");
    assert_eq!(filtered[0].id, first_id);
    assert_eq!(filtered[0].actor_id, "agent-a");

    let fetched = get_audit_entry(&service, first_id)
        .expect("get should succeed")
        .expect("entry should exist");
    assert_eq!(fetched.actor_id, "agent-a");
    assert_eq!(fetched.tool_id, "approve_execution");

    let json = export_audit_logs(&service, &allow_for_agent_a, AuditExportFormat::Json)
        .expect("json export should succeed");
    assert!(json.contains("approve_execution"));
    assert!(!json.contains("pending-2")); // agent-b's event should not appear
    assert!(!json.contains("policy denied"));
}

#[test]
fn extended_audit_tools_preserve_canonical_fields_for_new_rows() {
    let service = audit_service_for_test("dbflux-mcp-audit-tools-extended.sqlite");

    let stored = service
        .record(
            EventRecord::new(
                2_000,
                EventSeverity::Info,
                EventCategory::Config,
                EventOutcome::Success,
            )
            .with_action(CONFIG_UPDATE.as_str())
            .with_summary("Updated connection profile 'dev'")
            .with_object_ref("connection_profile", "profile-123")
            .with_actor_id("local"),
        )
        .expect("record should succeed");

    let query = AuditQuery {
        actor_id: None,
        tool_id: None,
        decision: None,
        start_epoch_ms: None,
        end_epoch_ms: None,
        limit: None,
    };

    let events = query_audit_logs_extended(&service, &query).expect("extended query should work");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action.as_deref(), Some(CONFIG_UPDATE.as_str()));
    assert_eq!(events[0].category.as_deref(), Some("config"));
    assert_eq!(events[0].object_id.as_deref(), Some("profile-123"));

    let fetched = get_audit_entry_extended(&service, stored.id.expect("id should be assigned"))
        .expect("extended get should work")
        .expect("entry should exist");
    assert_eq!(
        fetched.summary.as_deref(),
        Some("Updated connection profile 'dev'")
    );

    let json = export_audit_logs_extended(&service, &query, AuditExportFormat::Json)
        .expect("extended export should work");
    assert!(json.contains("\"action\": \"config_update\""));
    assert!(json.contains("\"object_type\": \"connection_profile\""));
}

#[test]
fn extended_export_populates_compatibility_fields_for_blank_canonical_rows() {
    let service = audit_service_for_test("dbflux-mcp-audit-tools-compat.sqlite");

    service
        .record(
            EventRecord::new(
                2_100,
                EventSeverity::Warn,
                EventCategory::Mcp,
                EventOutcome::Failure,
            )
            .with_typed_action(MCP_REJECT_EXECUTION)
            .with_summary("Rejected pending execution")
            .with_actor_id("reviewer-a")
            .with_object_ref("pending_execution", "pending-1")
            .with_error("rejected", "unsafe change"),
        )
        .expect("record should succeed");

    let query = AuditQuery {
        actor_id: None,
        tool_id: None,
        decision: None,
        start_epoch_ms: None,
        end_epoch_ms: None,
        limit: None,
    };

    let json = export_audit_logs_extended(&service, &query, AuditExportFormat::Json)
        .expect("extended export should work");

    assert!(json.contains("\"tool_id\": \"reject_execution\""));
    assert!(json.contains("\"decision\": \"deny\""));
    assert!(json.contains("\"action\": \"mcp_reject_execution\""));
    assert!(json.contains("\"outcome\": \"failure\""));
}
