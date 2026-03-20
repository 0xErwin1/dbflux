use dbflux_audit::export::AuditExportFormat;
use dbflux_audit::query::AuditQueryFilter;
use dbflux_audit::{AuditService, temp_sqlite_path};

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

    service
        .append("agent-a", "read_query", "allow", None, 1000)
        .expect("append allow should succeed");
    service
        .append(
            "agent-a",
            "request_execution",
            "deny",
            Some("policy denied"),
            1001,
        )
        .expect("append deny should succeed");
    service
        .append(
            "agent-a",
            "read_query",
            "failure",
            Some("access open failed"),
            1002,
        )
        .expect("append failure should succeed");

    let denied_only = service
        .query(&AuditQueryFilter {
            decision: Some("deny".to_string()),
            ..Default::default()
        })
        .expect("query should succeed");

    assert_eq!(denied_only.len(), 1);
    assert_eq!(denied_only[0].tool_id, "request_execution");

    let failure_only = service
        .query(&AuditQueryFilter {
            decision: Some("failure".to_string()),
            ..Default::default()
        })
        .expect("query should succeed");

    assert_eq!(failure_only.len(), 1);
    assert_eq!(
        failure_only[0].reason.as_deref(),
        Some("access open failed")
    );

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

    assert!(json.contains("request_execution"));
    assert!(json.contains("access open failed"));
    assert!(!json.contains("\"decision\": \"allow\""));
}
