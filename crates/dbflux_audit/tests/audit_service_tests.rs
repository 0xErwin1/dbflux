use dbflux_audit::export::AuditExportFormat;
use dbflux_audit::query::AuditQueryFilter;
use dbflux_audit::{AuditService, temp_sqlite_path};
use dbflux_core::observability::actions::{
    CONFIG_UPDATE, CONNECTION_CONNECT, CONNECTION_CONNECT_FAILED, CONNECTION_DISCONNECT,
    HOOK_EXECUTE, HOOK_EXECUTE_FAILED, MCP_APPROVE_EXECUTION, MCP_REJECT_EXECUTION, QUERY_EXECUTE,
    SCRIPT_EXECUTE, SCRIPT_EXECUTE_FAILED, SYSTEM_SHUTDOWN, SYSTEM_STARTUP,
};
use dbflux_core::observability::source::EventSinkError;
use dbflux_core::observability::types::{
    EventCategory, EventOutcome, EventRecord, EventSeverity, EventSourceId,
};

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
        .record(
            EventRecord::new(
                1000,
                EventSeverity::Info,
                EventCategory::Query,
                EventOutcome::Success,
            )
            .with_typed_action(QUERY_EXECUTE)
            .with_summary("Query executed")
            .with_actor_id("alice")
            .with_connection_context("conn-1", "main", "sqlite")
            .with_duration_ms(10),
        )
        .expect("record should succeed");

    let first_id = first.id.expect("id assigned");

    let fetched = service
        .get(first_id)
        .expect("get should succeed")
        .expect("record should exist");

    assert_eq!(fetched.id, first_id);
    assert_eq!(fetched.actor_id, "alice");
}

#[test]
fn query_filters_by_actor_and_tool() {
    let service = service_for_test("dbflux-audit-filter.sqlite");

    service
        .record(
            EventRecord::new(
                1000,
                EventSeverity::Info,
                EventCategory::Query,
                EventOutcome::Success,
            )
            .with_typed_action(QUERY_EXECUTE)
            .with_summary("Query executed")
            .with_actor_id("alice")
            .with_connection_context("conn-1", "main", "sqlite")
            .with_duration_ms(10),
        )
        .expect("first record should succeed");
    service
        .record(
            EventRecord::new(
                1001,
                EventSeverity::Info,
                EventCategory::Query,
                EventOutcome::Failure,
            )
            .with_typed_action(QUERY_EXECUTE)
            .with_summary("Query failed")
            .with_actor_id("bob")
            .with_error("untrusted client", "untrusted client")
            .with_connection_context("conn-1", "main", "sqlite")
            .with_duration_ms(5),
        )
        .expect("second record should succeed");
    service
        .record(
            EventRecord::new(
                1002,
                EventSeverity::Info,
                EventCategory::Script,
                EventOutcome::Failure,
            )
            .with_action("run_script")
            .with_summary("Script denied")
            .with_actor_id("alice")
            .with_error("policy", "policy")
            .with_object_ref("script", "script-1"),
        )
        .expect("third record should succeed");

    let result = service
        .query(&AuditQueryFilter {
            actor_id: Some("alice".to_string()),
            ..Default::default()
        })
        .expect("query should succeed");

    assert_eq!(result.len(), 2); // alice has 2 events: query_execute and run_script
    assert_eq!(result[0].actor_id, "alice");
    assert_eq!(result[1].actor_id, "alice");
}

#[test]
fn export_supports_csv_and_json() {
    let service = service_for_test("dbflux-audit-export.sqlite");

    service
        .record(
            EventRecord::new(
                1000,
                EventSeverity::Info,
                EventCategory::Query,
                EventOutcome::Success,
            )
            .with_typed_action(QUERY_EXECUTE)
            .with_summary("Query executed")
            .with_actor_id("alice")
            .with_connection_context("conn-1", "main", "sqlite")
            .with_duration_ms(10),
        )
        .expect("record should succeed");

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

#[test]
fn query_non_execution_event_does_not_require_duration() {
    let service = service_for_test("dbflux-audit-query-no-duration.sqlite");

    let event = EventRecord::new(
        1000,
        EventSeverity::Warn,
        EventCategory::Query,
        EventOutcome::Success,
    )
    .with_action("dangerous_query_confirmed")
    .with_summary("Dangerous query confirmed")
    .with_connection_context("conn-1", "main", "sqlite");

    service.record(event).expect("query event should record");
}

#[test]
fn script_event_with_object_ref_is_allowed() {
    let service = service_for_test("dbflux-audit-script-no-object.sqlite");

    let event = EventRecord::new(
        1000,
        EventSeverity::Info,
        EventCategory::Script,
        EventOutcome::Success,
    )
    .with_action("script_execute")
    .with_summary("Script executed")
    .with_object_ref("script", "print('hi')")
    .with_details_json(serde_json::json!({ "query": "print('hi')" }).to_string());

    service.record(event).expect("script event should record");
}

#[test]
fn config_event_with_object_ref_is_allowed() {
    let service = service_for_test("dbflux-audit-config-object-ref.sqlite");

    let event = EventRecord::new(
        1000,
        EventSeverity::Info,
        EventCategory::Config,
        EventOutcome::Success,
    )
    .with_action(CONFIG_UPDATE.as_str())
    .with_summary("Updated connection profile 'local sqlite'")
    .with_object_ref("connection_profile", "profile-123");

    let stored = service.record(event).expect("config event should record");

    assert_eq!(stored.object_type.as_deref(), Some("connection_profile"));
    assert_eq!(stored.object_id.as_deref(), Some("profile-123"));
}

#[test]
fn hook_event_with_object_ref_is_allowed() {
    let service = service_for_test("dbflux-audit-hook-object-ref.sqlite");

    let event = EventRecord::new(
        1000,
        EventSeverity::Info,
        EventCategory::Hook,
        EventOutcome::Success,
    )
    .with_action("hook.execute_start")
    .with_summary("Hook 'echo hello' started")
    .with_object_ref("hook", "echo hello")
    .with_connection_context("conn-1", "main", "sqlite");

    service.record(event).expect("hook event should record");
}

#[test]
fn invalid_details_json_is_rejected() {
    let service = service_for_test("dbflux-audit-invalid-details.sqlite");

    let event = EventRecord::new(
        1000,
        EventSeverity::Info,
        EventCategory::System,
        EventOutcome::Success,
    )
    .with_action("system.test")
    .with_summary("System event")
    .with_details_json("not-json");

    let err = service
        .record(event)
        .expect_err("invalid details_json must fail");

    assert!(matches!(
        err,
        dbflux_audit::AuditError::EventSink(EventSinkError::Serialization(_))
    ));
    assert!(err.to_string().contains("details_json must be valid JSON"));
}

#[test]
fn missing_actor_id_is_preserved_as_unknown_not_system() {
    let service = service_for_test("dbflux-audit-unknown-actor.sqlite");

    let event = EventRecord::new(
        1000,
        EventSeverity::Info,
        EventCategory::System,
        EventOutcome::Success,
    )
    .with_action("system.test")
    .with_summary("System event")
    .with_details_json(serde_json::json!({ "key": "value" }).to_string());

    let stored = service.record(event).expect("event should record");

    assert_eq!(stored.actor_id.as_deref(), Some("unknown"));
}

#[test]
fn details_json_is_fingerprinted_after_normalization() {
    let service = service_for_test("dbflux-audit-fingerprint.sqlite");

    let mut event = EventRecord::new(
        1000,
        EventSeverity::Info,
        EventCategory::Query,
        EventOutcome::Success,
    )
    .with_action(QUERY_EXECUTE.as_str())
    .with_summary("Query executed")
    .with_connection_context("conn-1", "main", "sqlite")
    .with_details_json("{ \n \"query\" : \"SELECT 1\" }")
    .with_duration_ms(12);
    event.source_id = EventSourceId::Local;

    let stored = service.record(event).expect("query event should record");
    let details = stored
        .details_json
        .expect("stored details_json should exist");

    assert!(details.contains("[FINGERPRINT:"));
    assert!(details.contains("\"query_length\":8"));
    assert!(!details.contains("SELECT 1"));
}

#[test]
fn max_detail_bytes_truncates_oversized_details_json() {
    let service = service_for_test("dbflux-audit-max-detail.sqlite");
    let max = 64usize;
    service.set_max_detail_bytes(max);
    service.set_redact_sensitive(false);

    // Use non-hex, non-base64 characters to avoid redaction masking the truncation
    let large_detail = "hello world ".repeat(20);
    let event = EventRecord::new(
        1000,
        EventSeverity::Info,
        EventCategory::System,
        EventOutcome::Success,
    )
    .with_action("system.test")
    .with_summary("System event")
    .with_details_json(serde_json::json!({ "note": large_detail }).to_string());

    let stored = service
        .record(event)
        .expect("oversized details_json should be truncated and stored");

    let details = stored
        .details_json
        .expect("stored details_json should be present after truncation");

    assert!(
        details.len() <= max,
        "stored details_json ({} bytes) must be <= max ({} bytes)",
        details.len(),
        max
    );

    let parsed: serde_json::Value =
        serde_json::from_str(&details).expect("truncated details must be valid JSON");

    assert_eq!(
        parsed.get("__truncated"),
        Some(&serde_json::Value::Bool(true)),
        "truncated envelope must have __truncated:true"
    );
    assert!(
        parsed.get("partial").is_some(),
        "truncated envelope must have a partial field"
    );
}

#[test]
fn max_detail_bytes_not_truncated_when_exactly_at_limit() {
    let service = service_for_test("dbflux-audit-max-detail-exact.sqlite");
    let details_str = serde_json::json!({ "k": "v" }).to_string();
    let max = details_str.len();
    service.set_max_detail_bytes(max);

    let event = EventRecord::new(
        1000,
        EventSeverity::Info,
        EventCategory::System,
        EventOutcome::Success,
    )
    .with_action("system.test")
    .with_summary("System event")
    .with_details_json(details_str.clone());

    let stored = service
        .record(event)
        .expect("details_json exactly at limit should store without truncation");

    let stored_details = stored
        .details_json
        .expect("stored details_json should be present");

    let parsed: serde_json::Value =
        serde_json::from_str(&stored_details).expect("stored details must be valid JSON");

    assert!(
        parsed.get("__truncated").is_none(),
        "details exactly at limit must not be truncated"
    );
}

#[test]
fn max_detail_bytes_handles_multibyte_utf8_boundary() {
    let service = service_for_test("dbflux-audit-max-detail-utf8.sqlite");
    let max = 80usize;
    service.set_max_detail_bytes(max);

    let multibyte = "é".repeat(50);
    let event = EventRecord::new(
        1000,
        EventSeverity::Info,
        EventCategory::System,
        EventOutcome::Success,
    )
    .with_action("system.test")
    .with_summary("System event")
    .with_details_json(serde_json::json!({ "data": multibyte }).to_string());

    let stored = service
        .record(event)
        .expect("multibyte details_json should be stored");

    let details = stored
        .details_json
        .expect("stored details_json should be present");

    assert!(
        details.len() <= max,
        "stored details_json ({} bytes) must be <= max ({} bytes)",
        details.len(),
        max
    );

    assert!(
        std::str::from_utf8(details.as_bytes()).is_ok(),
        "result must be valid UTF-8"
    );
    serde_json::from_str::<serde_json::Value>(&details).expect("result must be valid JSON");
}

#[test]
fn max_detail_bytes_envelope_within_limit_when_escaping_needed() {
    let service = service_for_test("dbflux-audit-max-detail-escape.sqlite");
    let max = 80usize;
    service.set_max_detail_bytes(max);

    let escaping_heavy = "\"\\".repeat(30);
    let event = EventRecord::new(
        1000,
        EventSeverity::Info,
        EventCategory::System,
        EventOutcome::Success,
    )
    .with_action("system.test")
    .with_summary("System event")
    .with_details_json(serde_json::json!({ "q": escaping_heavy }).to_string());

    let stored = service
        .record(event)
        .expect("details with heavy escaping should be stored");

    let details = stored
        .details_json
        .expect("stored details_json should be present");

    assert!(
        details.len() <= max,
        "envelope ({} bytes) must fit within max ({} bytes) even with escaping",
        details.len(),
        max
    );

    serde_json::from_str::<serde_json::Value>(&details).expect("envelope must be valid JSON");
}

#[test]
fn build_truncated_envelope_at_budget_zero_returns_minimal_valid_envelope() {
    let service = service_for_test("dbflux-audit-budget-zero.sqlite");
    let max = 34usize;
    service.set_max_detail_bytes(max);

    let all_quotes = "\"".repeat(max * 4);
    let event = EventRecord::new(
        1000,
        EventSeverity::Info,
        EventCategory::System,
        EventOutcome::Success,
    )
    .with_action("system.test")
    .with_summary("Pathological input")
    .with_details_json(serde_json::json!({ "q": all_quotes }).to_string());

    let stored = service
        .record(event)
        .expect("pathological input must not fail — must truncate gracefully");

    let details = stored
        .details_json
        .expect("details_json must be present after truncation");

    assert!(
        details.len() <= max,
        "envelope ({} bytes) must not exceed max ({} bytes) even for pathological all-quote input",
        details.len(),
        max
    );

    let parsed: serde_json::Value =
        serde_json::from_str(&details).expect("envelope must always be valid JSON");
    assert_eq!(
        parsed.get("__truncated").and_then(|v| v.as_bool()),
        Some(true),
        "envelope must contain __truncated:true"
    );
}

#[test]
fn legacy_query_get_and_export_fallback_to_canonical_action_and_outcome() {
    let service = service_for_test("dbflux-audit-canonical-legacy-fallback.sqlite");

    let stored = service
        .record(
            EventRecord::new(
                1000,
                EventSeverity::Info,
                EventCategory::Config,
                EventOutcome::Success,
            )
            .with_action(CONFIG_UPDATE.as_str())
            .with_summary("Updated connection profile 'local sqlite'")
            .with_actor_id("alice")
            .with_object_ref("connection_profile", "profile-123"),
        )
        .expect("config event should record");

    let query = AuditQueryFilter {
        tool_id: Some(CONFIG_UPDATE.as_str().to_string()),
        decision: Some(EventOutcome::Success.as_str().to_string()),
        ..Default::default()
    };

    let queried = service.query(&query).expect("legacy query should succeed");
    assert_eq!(queried.len(), 1);
    assert_eq!(queried[0].tool_id, CONFIG_UPDATE.as_str());
    assert_eq!(queried[0].decision, EventOutcome::Success.as_str());

    let fetched = service
        .get(stored.id.expect("id should be assigned"))
        .expect("legacy get should succeed")
        .expect("entry should exist");
    assert_eq!(fetched.tool_id, CONFIG_UPDATE.as_str());
    assert_eq!(fetched.decision, EventOutcome::Success.as_str());

    let json = service
        .export(&query, AuditExportFormat::Json)
        .expect("legacy export should succeed");
    assert!(json.contains("\"tool_id\": \"config_update\""));
    assert!(json.contains("\"decision\": \"success\""));
}

#[test]
fn legacy_api_round_trips_canonical_approval_and_rejection_rows() {
    let service = service_for_test("dbflux-audit-canonical-mcp-legacy-roundtrip.sqlite");

    let approved = service
        .record(
            EventRecord::new(
                1000,
                EventSeverity::Info,
                EventCategory::Mcp,
                EventOutcome::Success,
            )
            .with_typed_action(MCP_APPROVE_EXECUTION)
            .with_summary("Approved pending execution")
            .with_actor_id("reviewer-a")
            .with_object_ref("pending_execution", "pending-1"),
        )
        .expect("approval event should record");

    let rejected = service
        .record(
            EventRecord::new(
                1001,
                EventSeverity::Warn,
                EventCategory::Mcp,
                EventOutcome::Failure,
            )
            .with_typed_action(MCP_REJECT_EXECUTION)
            .with_summary("Rejected pending execution")
            .with_actor_id("reviewer-b")
            .with_object_ref("pending_execution", "pending-2")
            .with_error("rejected", "unsafe change"),
        )
        .expect("rejection event should record");

    let allow = service
        .query(&AuditQueryFilter {
            tool_id: Some("approve_execution".to_string()),
            decision: Some("allow".to_string()),
            ..Default::default()
        })
        .expect("allow query should succeed");
    assert_eq!(allow.len(), 1);
    assert_eq!(allow[0].tool_id, "approve_execution");
    assert_eq!(allow[0].decision, "allow");

    let deny = service
        .query(&AuditQueryFilter {
            tool_id: Some("reject_execution".to_string()),
            decision: Some("deny".to_string()),
            ..Default::default()
        })
        .expect("deny query should succeed");
    assert_eq!(deny.len(), 1);
    assert_eq!(deny[0].tool_id, "reject_execution");
    assert_eq!(deny[0].decision, "deny");

    let failure = service
        .query(&AuditQueryFilter {
            decision: Some("failure".to_string()),
            ..Default::default()
        })
        .expect("failure query should succeed");
    assert!(failure.is_empty());

    let fetched_approved = service
        .get(approved.id.expect("approval id should be assigned"))
        .expect("approval get should succeed")
        .expect("approval entry should exist");
    assert_eq!(fetched_approved.tool_id, "approve_execution");
    assert_eq!(fetched_approved.decision, "allow");

    let fetched_rejected = service
        .get(rejected.id.expect("rejection id should be assigned"))
        .expect("rejection get should succeed")
        .expect("rejection entry should exist");
    assert_eq!(fetched_rejected.tool_id, "reject_execution");
    assert_eq!(fetched_rejected.decision, "deny");

    let json = service
        .export(
            &AuditQueryFilter {
                tool_id: Some("reject_execution".to_string()),
                ..Default::default()
            },
            AuditExportFormat::Json,
        )
        .expect("legacy export should succeed");
    assert!(json.contains("\"tool_id\": \"reject_execution\""));
    assert!(json.contains("\"decision\": \"deny\""));
}

#[test]
fn system_startup_and_shutdown_events_record_successfully() {
    let service = service_for_test("dbflux-audit-system-lifecycle.sqlite");

    service
        .record(
            EventRecord::new(
                1000,
                EventSeverity::Info,
                EventCategory::System,
                EventOutcome::Success,
            )
            .with_typed_action(SYSTEM_STARTUP)
            .with_summary("DBFlux application started")
            .with_actor_id("system"),
        )
        .expect("startup event should record");

    service
        .record(
            EventRecord::new(
                2000,
                EventSeverity::Info,
                EventCategory::System,
                EventOutcome::Success,
            )
            .with_typed_action(SYSTEM_SHUTDOWN)
            .with_summary("DBFlux application initiating shutdown")
            .with_actor_id("system"),
        )
        .expect("shutdown event should record");

    let query = AuditQueryFilter {
        tool_id: Some("system_startup".to_string()),
        ..Default::default()
    };
    let queried = service.query(&query).expect("query should succeed");
    assert_eq!(queried.len(), 1);
    assert_eq!(queried[0].tool_id, "system_startup");
    assert_eq!(queried[0].decision, "success");

    let query2 = AuditQueryFilter {
        tool_id: Some("system_shutdown".to_string()),
        ..Default::default()
    };
    let queried2 = service.query(&query2).expect("query should succeed");
    assert_eq!(queried2.len(), 1);
    assert_eq!(queried2[0].tool_id, "system_shutdown");
    assert_eq!(queried2[0].decision, "success");
}

#[test]
fn panic_hook_records_panic_event() {
    let service = service_for_test("dbflux-audit-panic-hook.sqlite");
    service.set_capture_query_text(true);

    let panic_info =
        "assertion failed: `left == right`\n left: `42`\nright: `100` at src/main.rs:123:15";
    service
        .record_panic_best_effort(panic_info)
        .expect("record_panic_best_effort should return Some");

    let query = AuditQueryFilter {
        tool_id: Some("system_panic".to_string()),
        ..Default::default()
    };
    let queried = service.query(&query).expect("query should succeed");
    assert_eq!(queried.len(), 1);
    assert_eq!(queried[0].tool_id, "system_panic");
    assert_eq!(queried[0].decision, "failure");
}

#[test]
fn connection_lifecycle_events_record_correctly() {
    let service = service_for_test("dbflux-audit-conn-lifecycle.sqlite");

    service
        .record(
            EventRecord::new(
                1000,
                EventSeverity::Info,
                EventCategory::Connection,
                EventOutcome::Success,
            )
            .with_typed_action(CONNECTION_CONNECT)
            .with_summary("Profile 'local postgres' connected")
            .with_connection_context("conn-uuid-123", "mydb", "postgres"),
        )
        .expect("connection connect event should record");

    service
        .record(
            EventRecord::new(
                2000,
                EventSeverity::Info,
                EventCategory::Connection,
                EventOutcome::Success,
            )
            .with_typed_action(CONNECTION_DISCONNECT)
            .with_summary("Profile 'local postgres' disconnected")
            .with_connection_context("conn-uuid-123", "mydb", "postgres"),
        )
        .expect("connection disconnect event should record");

    service
        .record(
            EventRecord::new(
                3000,
                EventSeverity::Error,
                EventCategory::Connection,
                EventOutcome::Failure,
            )
            .with_typed_action(CONNECTION_CONNECT_FAILED)
            .with_summary("Connection failed: authentication error")
            .with_connection_context("conn-uuid-456", "mydb", "postgres")
            .with_error("auth", "invalid credentials"),
        )
        .expect("connection failed event should record");

    let query = AuditQueryFilter {
        tool_id: Some("connection_connect".to_string()),
        ..Default::default()
    };
    let connect_queried = service.query(&query).expect("query should succeed");
    assert_eq!(connect_queried.len(), 1);
    assert_eq!(connect_queried[0].tool_id, "connection_connect");
    assert_eq!(connect_queried[0].decision, "success");

    let query2 = AuditQueryFilter {
        tool_id: Some("connection_disconnect".to_string()),
        ..Default::default()
    };
    let disconnect_queried = service.query(&query2).expect("query should succeed");
    assert_eq!(disconnect_queried.len(), 1);
    assert_eq!(disconnect_queried[0].tool_id, "connection_disconnect");
    assert_eq!(disconnect_queried[0].decision, "success");

    let query3 = AuditQueryFilter {
        tool_id: Some("connection_connect_failed".to_string()),
        ..Default::default()
    };
    let failed_queried = service.query(&query3).expect("query should succeed");
    assert_eq!(failed_queried.len(), 1);
    assert_eq!(failed_queried[0].tool_id, "connection_connect_failed");
    assert_eq!(failed_queried[0].decision, "failure");
}

#[test]
fn hook_lifecycle_events_record_correctly() {
    let service = service_for_test("dbflux-audit-hook-lifecycle.sqlite");

    service
        .record(
            EventRecord::new(
                1000,
                EventSeverity::Info,
                EventCategory::Hook,
                EventOutcome::Success,
            )
            .with_typed_action(HOOK_EXECUTE)
            .with_summary("Hook 'echo hello' (PreConnect) started")
            .with_object_ref("hook", "echo hello")
            .with_connection_context("conn-uuid-123", "mydb", "postgres"),
        )
        .expect("hook start event should record");

    service
        .record(
            EventRecord::new(
                2000,
                EventSeverity::Info,
                EventCategory::Hook,
                EventOutcome::Success,
            )
            .with_typed_action(HOOK_EXECUTE)
            .with_summary("Hook 'echo hello' (PreConnect) completed")
            .with_object_ref("hook", "echo hello")
            .with_connection_context("conn-uuid-123", "mydb", "postgres")
            .with_duration_ms(50),
        )
        .expect("hook complete event should record");

    service
        .record(
            EventRecord::new(
                3000,
                EventSeverity::Error,
                EventCategory::Hook,
                EventOutcome::Failure,
            )
            .with_typed_action(HOOK_EXECUTE_FAILED)
            .with_summary("Hook 'echo hello' (PreConnect) failed: exit code 1")
            .with_object_ref("hook", "echo hello")
            .with_connection_context("conn-uuid-123", "mydb", "postgres")
            .with_error("hook", "exit code 1")
            .with_duration_ms(50),
        )
        .expect("hook failed event should record");

    let query = AuditQueryFilter {
        tool_id: Some("hook_execute".to_string()),
        ..Default::default()
    };
    let hook_queried = service.query(&query).expect("query should succeed");
    assert_eq!(hook_queried.len(), 2);
    assert_eq!(hook_queried[0].tool_id, "hook_execute");
    assert_eq!(hook_queried[0].decision, "success");

    let query2 = AuditQueryFilter {
        tool_id: Some("hook_execute_failed".to_string()),
        ..Default::default()
    };
    let failed_queried = service.query(&query2).expect("query should succeed");
    assert_eq!(failed_queried.len(), 1);
    assert_eq!(failed_queried[0].tool_id, "hook_execute_failed");
    assert_eq!(failed_queried[0].decision, "failure");
}

#[test]
fn script_execute_and_failed_are_valid_typed_actions() {
    let service = service_for_test("dbflux-audit-script-typed-actions.sqlite");

    service
        .record(
            EventRecord::new(
                1000,
                EventSeverity::Info,
                EventCategory::Script,
                EventOutcome::Success,
            )
            .with_typed_action(SCRIPT_EXECUTE)
            .with_summary("Script executed successfully")
            .with_object_ref("script", "hello.lua")
            .with_details_json(serde_json::json!({ "query": "print('hello')" }).to_string()),
        )
        .expect("script execute event should record");

    service
        .record(
            EventRecord::new(
                2000,
                EventSeverity::Error,
                EventCategory::Script,
                EventOutcome::Failure,
            )
            .with_typed_action(SCRIPT_EXECUTE_FAILED)
            .with_summary("Script failed: runtime error")
            .with_error("runtime", "undefined variable 'x'")
            .with_object_ref("script", "hello.lua")
            .with_details_json(serde_json::json!({ "query": "print(x)" }).to_string()),
        )
        .expect("script failed event should record");

    let query = AuditQueryFilter {
        tool_id: Some("script_execute".to_string()),
        ..Default::default()
    };
    let success_queried = service.query(&query).expect("query should succeed");
    assert_eq!(success_queried.len(), 1);
    assert_eq!(success_queried[0].tool_id, "script_execute");
    assert_eq!(success_queried[0].decision, "success");

    let query2 = AuditQueryFilter {
        tool_id: Some("script_execute_failed".to_string()),
        ..Default::default()
    };
    let failed_queried = service.query(&query2).expect("query should succeed");
    assert_eq!(failed_queried.len(), 1);
    assert_eq!(failed_queried[0].tool_id, "script_execute_failed");
    assert_eq!(failed_queried[0].decision, "failure");
}
