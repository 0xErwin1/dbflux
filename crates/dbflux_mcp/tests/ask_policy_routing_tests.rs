//! A policy class decided as Ask routes the call through the approval queue:
//! the first call is queued, a rejected call never runs, and an approved call
//! runs exactly once when it is repeated with the same arguments.

use dbflux_approval::InMemoryPendingExecutionStore;
use dbflux_audit::AuditService;
use dbflux_core::observability::actions::MCP_AUTHORIZE;
use dbflux_core::observability::{EventCategory, EventOrigin};
use dbflux_mcp::server::authorization::{
    APPROVAL_REQUIRED_CODE, AuthorizationOutcome, AuthorizationRequest, HUMAN_ONLY_APPROVAL_TOOLS,
    SELF_APPROVAL_FORBIDDEN_CODE, authorize_request,
};
use dbflux_mcp::server::request_context::RequestIdentity;
use dbflux_mcp::{
    APPROVAL_QUEUE_TOOLS, McpGovernanceService, McpRuntime, McpRuntimeEvent, ToolPolicyDto,
};
use dbflux_policy::{
    ConnectionPolicyAssignment, ExecutionClassification, PolicyBindingScope, PolicyEngine,
    ToolPolicy, TrustedClient, TrustedClientRegistry,
};

const ACTOR: &str = "agent-a";
const CONNECTION: &str = "conn-a";

fn audit_service_for_test(file_name: &str) -> AuditService {
    let path = dbflux_audit::temp_sqlite_path(file_name);
    if let Err(error) = std::fs::remove_file(&path) {
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::NotFound,
            "stale audit database could not be removed: {error}"
        );
    }
    AuditService::new_sqlite(&path).expect("audit service should initialize")
}

fn runtime_for_test(file_name: &str) -> McpRuntime {
    McpRuntime::new(
        audit_service_for_test(file_name),
        Box::new(InMemoryPendingExecutionStore::default()),
    )
}

fn trusted_registry() -> TrustedClientRegistry {
    TrustedClientRegistry::new(vec![TrustedClient {
        id: ACTOR.to_string(),
        name: "Agent A".to_string(),
        issuer: None,
        active: true,
    }])
}

/// Read is Allow, Write and Destructive are Ask, every other class is Deny.
fn ask_engine(tools: &[&str]) -> PolicyEngine {
    PolicyEngine::new(
        vec![ConnectionPolicyAssignment {
            actor_id: ACTOR.to_string(),
            scope: PolicyBindingScope {
                connection_id: CONNECTION.to_string(),
            },
            role_ids: Vec::new(),
            policy_ids: vec!["analyst".to_string()],
        }],
        Vec::new(),
        vec![ToolPolicy {
            id: "analyst".to_string(),
            allowed_tools: tools.iter().map(|tool| tool.to_string()).collect(),
            allowed_classes: vec![ExecutionClassification::Read],
            approval_classes: vec![
                ExecutionClassification::Write,
                ExecutionClassification::Destructive,
            ],
        }],
    )
}

fn request(tool_id: &str, classification: ExecutionClassification) -> AuthorizationRequest {
    AuthorizationRequest {
        identity: RequestIdentity {
            client_id: ACTOR.to_string(),
            issuer: None,
        },
        connection_id: CONNECTION.to_string(),
        tool_id: tool_id.to_string(),
        classification,
        mcp_enabled_for_connection: true,
        correlation_id: None,
    }
}

fn authorize(
    runtime: &mut McpRuntime,
    engine: &PolicyEngine,
    tool_id: &str,
    classification: ExecutionClassification,
    payload: serde_json::Value,
) -> AuthorizationOutcome {
    runtime
        .authorize_with_approval_mut(
            &trusted_registry(),
            engine,
            &request(tool_id, classification),
            payload,
            1,
        )
        .expect("authorization should succeed")
}

fn authorize_events(
    runtime: &McpRuntime,
) -> Vec<dbflux_storage::repositories::audit::AuditEventDto> {
    let mut entries = runtime
        .audit_service()
        .query_extended(&dbflux_audit::query::AuditQueryFilter {
            action: Some(MCP_AUTHORIZE.as_str().to_string()),
            category: Some(EventCategory::Mcp.as_str().to_string()),
            ..Default::default()
        })
        .expect("audit query should succeed");
    entries.sort_by_key(|entry| entry.id);
    entries
}

#[test]
fn ask_class_queues_the_call_and_audits_it_as_pending() {
    let mut runtime = runtime_for_test("dbflux-mcp-ask-queue.sqlite");
    let engine = ask_engine(&["update_records"]);
    let payload = serde_json::json!({"table": "users", "set": {"active": true}});

    let outcome = authorize(
        &mut runtime,
        &engine,
        "update_records",
        ExecutionClassification::Write,
        payload.clone(),
    );

    assert!(!outcome.allowed);
    assert_eq!(outcome.deny_code, Some(APPROVAL_REQUIRED_CODE));
    let pending_id = outcome
        .pending_execution_id
        .clone()
        .expect("a queued call carries its pending execution id");

    let pending = runtime
        .list_pending_executions()
        .expect("list pending should succeed");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, pending_id);
    assert_eq!(pending[0].tool_id, "update_records");
    assert_eq!(pending[0].actor_id, ACTOR);
    assert_eq!(pending[0].connection_id, CONNECTION);
    assert_eq!(pending[0].classification, ExecutionClassification::Write);

    let detail = runtime
        .get_pending_execution(&pending_id)
        .expect("pending detail should load");
    assert_eq!(detail.plan, payload);

    assert!(
        runtime
            .drain_events()
            .contains(&McpRuntimeEvent::PendingExecutionsUpdated)
    );

    let events = authorize_events(&runtime);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].outcome.as_deref(), Some("pending"));
    assert_eq!(
        events[0].error_code.as_deref(),
        Some(APPROVAL_REQUIRED_CODE)
    );
    assert_eq!(events[0].connection_id.as_deref(), Some(CONNECTION));
    assert_eq!(events[0].object_id.as_deref(), Some("update_records"));
    assert!(events[0].correlation_id.is_some());
    let details: serde_json::Value = serde_json::from_str(
        events[0]
            .details_json
            .as_deref()
            .expect("details_json should be present"),
    )
    .expect("details_json should be valid JSON");
    assert_eq!(details["decision"], "approval_required");
    assert_eq!(details["pending_execution_id"], pending_id.as_str());
}

#[test]
fn approved_call_runs_once_when_repeated_with_the_same_arguments() {
    let mut runtime = runtime_for_test("dbflux-mcp-ask-approve.sqlite");
    let engine = ask_engine(&["delete_records"]);
    let payload = serde_json::json!({"table": "users", "where": {"id": 7}});

    let queued = authorize(
        &mut runtime,
        &engine,
        "delete_records",
        ExecutionClassification::Destructive,
        payload.clone(),
    );
    let pending_id = queued
        .pending_execution_id
        .expect("the first call should be queued");

    runtime
        .approve_pending_execution_with_origin_mut(&pending_id, "local", EventOrigin::local())
        .expect("approval should succeed");

    let different_arguments = authorize(
        &mut runtime,
        &engine,
        "delete_records",
        ExecutionClassification::Destructive,
        serde_json::json!({"table": "users", "where": {"id": 8}}),
    );
    assert!(
        !different_arguments.allowed,
        "an approval must not authorize a call with other arguments"
    );
    assert!(different_arguments.pending_execution_id.is_some());

    let replay = authorize(
        &mut runtime,
        &engine,
        "delete_records",
        ExecutionClassification::Destructive,
        payload.clone(),
    );
    assert!(replay.allowed);
    assert_eq!(
        replay.approved_execution_id.as_deref(),
        Some(pending_id.as_str())
    );
    assert!(replay.deny_code.is_none());

    let second_replay = authorize(
        &mut runtime,
        &engine,
        "delete_records",
        ExecutionClassification::Destructive,
        payload,
    );
    assert!(
        !second_replay.allowed,
        "an approval authorizes exactly one call"
    );
    assert_eq!(second_replay.deny_code, Some(APPROVAL_REQUIRED_CODE));

    let events = authorize_events(&runtime);
    let outcomes: Vec<&str> = events
        .iter()
        .map(|event| event.outcome.as_deref().unwrap_or_default())
        .collect();
    assert_eq!(outcomes, vec!["pending", "pending", "success", "pending"]);
    let granted: serde_json::Value = serde_json::from_str(
        events[2]
            .details_json
            .as_deref()
            .expect("details_json should be present"),
    )
    .expect("details_json should be valid JSON");
    assert_eq!(granted["decision"], "approved");
    assert_eq!(granted["pending_execution_id"], pending_id.as_str());
}

#[test]
fn rejected_call_is_queued_again_and_never_runs() {
    let mut runtime = runtime_for_test("dbflux-mcp-ask-reject.sqlite");
    let engine = ask_engine(&["insert_record"]);
    let payload = serde_json::json!({"table": "users", "values": {"name": "Ada"}});

    let queued = authorize(
        &mut runtime,
        &engine,
        "insert_record",
        ExecutionClassification::Write,
        payload.clone(),
    );
    let pending_id = queued
        .pending_execution_id
        .expect("the first call should be queued");

    runtime
        .reject_pending_execution_with_origin_mut(
            &pending_id,
            "local",
            Some("not today"),
            EventOrigin::local(),
        )
        .expect("rejection should succeed");

    let retried = authorize(
        &mut runtime,
        &engine,
        "insert_record",
        ExecutionClassification::Write,
        payload,
    );

    assert!(!retried.allowed);
    assert_eq!(retried.deny_code, Some(APPROVAL_REQUIRED_CODE));
    assert_ne!(
        retried.pending_execution_id.as_deref(),
        Some(pending_id.as_str()),
        "a rejected call is queued as a new request"
    );
}

#[test]
fn allow_and_deny_classes_do_not_touch_the_approval_queue() {
    let mut runtime = runtime_for_test("dbflux-mcp-ask-untouched.sqlite");
    let engine = ask_engine(&["select_data", "drop_table"]);

    let allowed = authorize(
        &mut runtime,
        &engine,
        "select_data",
        ExecutionClassification::Read,
        serde_json::json!({}),
    );
    assert!(allowed.allowed);
    assert!(allowed.pending_execution_id.is_none());
    assert!(allowed.approved_execution_id.is_none());

    let denied = authorize(
        &mut runtime,
        &engine,
        "drop_table",
        ExecutionClassification::AdminDestructive,
        serde_json::json!({}),
    );
    assert!(!denied.allowed);
    assert_eq!(denied.deny_code, Some("policy_denied"));
    assert!(denied.pending_execution_id.is_none());

    assert!(
        runtime
            .list_pending_executions()
            .expect("list pending should succeed")
            .is_empty()
    );
}

fn engine_for_tools(
    tools: &[&str],
    allowed: Vec<ExecutionClassification>,
    approval: Vec<ExecutionClassification>,
) -> PolicyEngine {
    PolicyEngine::new(
        vec![ConnectionPolicyAssignment {
            actor_id: ACTOR.to_string(),
            scope: PolicyBindingScope {
                connection_id: CONNECTION.to_string(),
            },
            role_ids: Vec::new(),
            policy_ids: vec!["custom".to_string()],
        }],
        Vec::new(),
        vec![ToolPolicy {
            id: "custom".to_string(),
            allowed_tools: tools.iter().map(|tool| tool.to_string()).collect(),
            allowed_classes: allowed,
            approval_classes: approval,
        }],
    )
}

#[test]
fn approval_queue_tools_run_without_being_queued_under_ask() {
    let mut runtime = runtime_for_test("dbflux-mcp-ask-queue-tools.sqlite");
    let engine = engine_for_tools(
        APPROVAL_QUEUE_TOOLS,
        Vec::new(),
        vec![
            ExecutionClassification::Read,
            ExecutionClassification::Admin,
        ],
    );

    for tool_id in APPROVAL_QUEUE_TOOLS {
        for classification in [
            ExecutionClassification::Read,
            ExecutionClassification::Admin,
        ] {
            let outcome = authorize(
                &mut runtime,
                &engine,
                tool_id,
                classification,
                serde_json::json!({}),
            );

            assert!(outcome.allowed, "{tool_id} should run under Ask");
            assert!(outcome.pending_execution_id.is_none(), "{tool_id}");
        }
    }

    assert!(
        runtime
            .list_pending_executions()
            .expect("list pending should succeed")
            .is_empty()
    );
}

#[test]
fn mcp_clients_can_never_approve_or_reject_even_when_a_policy_allows_it() {
    let mut runtime = runtime_for_test("dbflux-mcp-no-self-approval.sqlite");
    let everything = vec![
        ExecutionClassification::Metadata,
        ExecutionClassification::Read,
        ExecutionClassification::Write,
        ExecutionClassification::Destructive,
        ExecutionClassification::AdminSafe,
        ExecutionClassification::Admin,
        ExecutionClassification::AdminDestructive,
    ];
    let engine = engine_for_tools(
        &["approve_execution", "reject_execution", "update_records"],
        everything,
        Vec::new(),
    );

    for tool_id in HUMAN_ONLY_APPROVAL_TOOLS {
        let outcome = authorize(
            &mut runtime,
            &engine,
            tool_id,
            ExecutionClassification::Admin,
            serde_json::json!({"pending_id": "00000000-0000-0000-0000-000000000000"}),
        );

        assert!(!outcome.allowed, "{tool_id} must be denied over MCP");
        assert_eq!(outcome.deny_code, Some(SELF_APPROVAL_FORBIDDEN_CODE));
        assert!(outcome.pending_execution_id.is_none());
    }

    let untrusted_outcome = authorize_request(
        &TrustedClientRegistry::new(Vec::new()),
        &engine,
        runtime.audit_service(),
        &request("approve_execution", ExecutionClassification::Admin),
        1,
    )
    .expect("authorization should succeed");
    assert_eq!(
        untrusted_outcome.deny_code,
        Some(SELF_APPROVAL_FORBIDDEN_CODE),
        "the self-approval rule applies before any other check"
    );

    let events = authorize_events(&runtime);
    assert_eq!(events.len(), 3);
    for event in &events {
        assert_eq!(event.outcome.as_deref(), Some("failure"));
        assert_eq!(
            event.error_code.as_deref(),
            Some(SELF_APPROVAL_FORBIDDEN_CODE)
        );
    }
}

#[test]
fn authorize_without_a_queue_reports_approval_required_and_audits_pending() {
    let audit_service = audit_service_for_test("dbflux-mcp-ask-no-queue.sqlite");
    let engine = ask_engine(&["update_records"]);

    let outcome = authorize_request(
        &trusted_registry(),
        &engine,
        &audit_service,
        &request("update_records", ExecutionClassification::Write),
        1,
    )
    .expect("authorization should succeed");

    assert!(!outcome.allowed);
    assert_eq!(outcome.deny_code, Some(APPROVAL_REQUIRED_CODE));
    assert!(outcome.pending_execution_id.is_none());

    let entries = audit_service
        .query_extended(&dbflux_audit::query::AuditQueryFilter {
            action: Some(MCP_AUTHORIZE.as_str().to_string()),
            ..Default::default()
        })
        .expect("audit query should succeed");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].outcome.as_deref(), Some("pending"));
}

#[test]
fn ask_decisions_survive_the_dto_round_trip_used_by_settings_and_storage() {
    let dto = ToolPolicyDto {
        id: "analyst".to_string(),
        allowed_tools: vec!["update_records".to_string()],
        allowed_classes: vec!["read".to_string()],
        approval_classes: vec!["write".to_string(), "admin_destructive".to_string()],
    };

    let policy = ToolPolicy::try_from(dto.clone()).expect("dto should convert");
    assert_eq!(policy.allowed_classes, vec![ExecutionClassification::Read]);
    assert_eq!(
        policy.approval_classes,
        vec![
            ExecutionClassification::Write,
            ExecutionClassification::AdminDestructive
        ]
    );
    assert_eq!(ToolPolicyDto::from(policy), dto);

    let legacy: ToolPolicyDto = serde_json::from_str(
        r#"{"id":"legacy","allowed_tools":["select_data"],"allowed_classes":["read"]}"#,
    )
    .expect("a policy serialized before Ask existed should deserialize");
    assert!(legacy.approval_classes.is_empty());
}
