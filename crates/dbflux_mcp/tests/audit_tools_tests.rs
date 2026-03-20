use dbflux_audit::AuditService;
use dbflux_mcp::governance_service::{AuditExportFormat, AuditQuery};
use dbflux_mcp::handlers::audit::{export_audit_logs, get_audit_entry, query_audit_logs};

fn audit_service_for_test(file_name: &str) -> AuditService {
    let path = dbflux_audit::temp_sqlite_path(file_name);
    let _ = std::fs::remove_file(&path);
    AuditService::new_sqlite(&path).expect("audit service should initialize")
}

#[test]
fn audit_tools_filter_lookup_and_export_filtered_records() {
    let service = audit_service_for_test("dbflux-mcp-audit-tools.sqlite");

    let first = service
        .append("agent-a", "read_query", "allow", None, 1_000)
        .expect("append allow should succeed");
    service
        .append("agent-b", "approve_execution", "allow", None, 1_001)
        .expect("append allow should succeed");
    service
        .append(
            "agent-a",
            "request_execution",
            "deny",
            Some("policy denied"),
            1_002,
        )
        .expect("append deny should succeed");

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
    assert_eq!(filtered[0].id, first.id);

    let fetched = get_audit_entry(&service, first.id)
        .expect("get should succeed")
        .expect("entry should exist");
    assert_eq!(fetched.actor_id, "agent-a");
    assert_eq!(fetched.tool_id, "read_query");

    let json = export_audit_logs(&service, &allow_for_agent_a, AuditExportFormat::Json)
        .expect("json export should succeed");
    assert!(json.contains("read_query"));
    assert!(!json.contains("approve_execution"));
    assert!(!json.contains("policy denied"));
}
