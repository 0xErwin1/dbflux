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
fn append_is_immutable_and_returns_stored_record() {
    let service = service_for_test("dbflux-audit-immutable.sqlite");

    let first = service
        .append("alice", "read_query", "allow", None, 1000)
        .expect("append should succeed");

    let fetched = service
        .get(first.id)
        .expect("get should succeed")
        .expect("record should exist");

    assert_eq!(first, fetched);
}

#[test]
fn query_filters_by_actor_and_tool() {
    let service = service_for_test("dbflux-audit-filter.sqlite");

    service
        .append("alice", "read_query", "allow", None, 1000)
        .expect("append should succeed");
    service
        .append("bob", "read_query", "deny", Some("untrusted client"), 1001)
        .expect("append should succeed");
    service
        .append("alice", "run_script", "deny", Some("policy"), 1002)
        .expect("append should succeed");

    let result = service
        .query(&AuditQueryFilter {
            actor_id: Some("alice".to_string()),
            tool_id: Some("read_query".to_string()),
            ..Default::default()
        })
        .expect("query should succeed");

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].actor_id, "alice");
    assert_eq!(result[0].tool_id, "read_query");
}

#[test]
fn export_supports_csv_and_json() {
    let service = service_for_test("dbflux-audit-export.sqlite");

    service
        .append("alice", "read_query", "allow", None, 1000)
        .expect("append should succeed");

    let csv = service
        .export(&AuditQueryFilter::default(), AuditExportFormat::Csv)
        .expect("csv export should succeed");
    assert!(csv.contains("actor_id"));
    assert!(csv.contains("alice"));

    let json = service
        .export(&AuditQueryFilter::default(), AuditExportFormat::Json)
        .expect("json export should succeed");
    assert!(json.contains("\"actor_id\": \"alice\""));
}
