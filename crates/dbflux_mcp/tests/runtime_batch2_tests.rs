use dbflux_audit::AuditService;
use dbflux_core::QueryLanguage;
use dbflux_core::observability::EventCategory;
use dbflux_core::observability::actions::MCP_AUTHORIZE;
use dbflux_mcp::handlers::query::{QueryExecutionRequest, QueryHandlerError, handle_query_tool};
use dbflux_mcp::handlers::scripts::{ScriptHandler, ScriptHandlerError, ScriptLifecycleState};
use dbflux_mcp::server::authorization::{AuthorizationRequest, authorize_request};
use dbflux_mcp::server::bootstrap::{
    BootstrapConfig, BootstrapError, TransportKind, validate_v1_transport_profile,
};
use dbflux_mcp::server::request_context::RequestIdentity;
use dbflux_mcp::server::router::{RouteTarget, route_tool};
use dbflux_policy::{
    ConnectionPolicyAssignment, ExecutionClassification, PolicyBindingScope, PolicyEngine,
    ToolPolicy, TrustedClient, TrustedClientRegistry,
};

fn allow_engine(tool: &str, class: ExecutionClassification) -> PolicyEngine {
    PolicyEngine::new(
        vec![ConnectionPolicyAssignment {
            actor_id: "agent-a".to_string(),
            scope: PolicyBindingScope {
                connection_id: "conn-a".to_string(),
            },
            role_ids: Vec::new(),
            policy_ids: vec!["policy-a".to_string()],
        }],
        Vec::new(),
        vec![ToolPolicy {
            id: "policy-a".to_string(),
            allowed_tools: vec![tool.to_string()],
            allowed_classes: vec![class],
        }],
    )
}

fn fresh_audit_service(file_name: &str) -> AuditService {
    let path = dbflux_audit::temp_sqlite_path(file_name);
    let _ = std::fs::remove_file(&path);
    AuditService::new_sqlite(&path).expect("audit service must initialize")
}

#[test]
fn bootstrap_rejects_tcp_only_profile() {
    let result = validate_v1_transport_profile(&BootstrapConfig {
        enabled_transports: vec![TransportKind::Tcp],
    });

    assert_eq!(result, Err(BootstrapError::TcpOnlyNotSupported));
}

#[test]
fn router_rejects_legacy_alias() {
    let result = route_tool("describe_table");
    assert!(result.is_err());
}

#[test]
fn router_routes_canonical_tool() {
    let result = route_tool("preview_mutation").expect("route must succeed");
    assert_eq!(result, RouteTarget::Query);
}

#[test]
fn authorization_denies_untrusted_and_audits_reason() {
    let audit_service = fresh_audit_service("dbflux-mcp-authz-test.sqlite");

    let trusted_registry = TrustedClientRegistry::new(vec![TrustedClient {
        id: "agent-a".to_string(),
        name: "Agent A".to_string(),
        issuer: None,
        active: true,
    }]);

    let policy_engine = allow_engine("read_query", ExecutionClassification::Read);

    let outcome = authorize_request(
        &trusted_registry,
        &policy_engine,
        &audit_service,
        &AuthorizationRequest {
            identity: RequestIdentity {
                client_id: "agent-b".to_string(),
                issuer: None,
            },
            connection_id: "conn-a".to_string(),
            tool_id: "read_query".to_string(),
            classification: ExecutionClassification::Read,
            mcp_enabled_for_connection: true,
            correlation_id: None,
        },
        10,
    )
    .expect("authorization should complete");

    assert!(!outcome.allowed);
    assert_eq!(outcome.deny_code, Some("untrusted_client"));

    let entries = audit_service
        .query_extended(&dbflux_audit::query::AuditQueryFilter {
            action: Some(MCP_AUTHORIZE.as_str().to_string()),
            category: Some(EventCategory::Mcp.as_str().to_string()),
            ..Default::default()
        })
        .expect("audit query should succeed");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].outcome.as_deref(), Some("failure"));
    assert_eq!(entries[0].error_code.as_deref(), Some("untrusted_client"));
}

#[test]
fn preview_mutation_never_executes() {
    let engine = allow_engine("preview_mutation", ExecutionClassification::Write);
    let response = handle_query_tool(
        &QueryExecutionRequest {
            actor_id: "agent-a".to_string(),
            connection_id: "conn-a".to_string(),
            tool_id: "preview_mutation".to_string(),
            query_language: QueryLanguage::Sql,
            query: "UPDATE users SET active = true".to_string(),
        },
        &engine,
    )
    .expect("preview should be allowed");

    assert!(response.preview_only);
    assert!(!response.execute);
    assert_eq!(response.classification, ExecutionClassification::Write);
}

#[test]
fn denied_query_fails_policy_gate() {
    let engine = allow_engine("read_query", ExecutionClassification::Read);
    let result = handle_query_tool(
        &QueryExecutionRequest {
            actor_id: "agent-a".to_string(),
            connection_id: "conn-a".to_string(),
            tool_id: "read_query".to_string(),
            query_language: QueryLanguage::Sql,
            query: "DROP TABLE users".to_string(),
        },
        &engine,
    );

    assert!(matches!(result, Err(QueryHandlerError::PolicyDenied)));
}

#[test]
fn script_run_requires_runnable_lifecycle() {
    let mut handler = ScriptHandler::default();
    let script = handler.create_script(
        "test".to_string(),
        "print('hi')".to_string(),
        ScriptLifecycleState::Draft,
    );

    let engine = allow_engine("run_script", ExecutionClassification::Admin);
    let result = handler.run_script(&engine, "agent-a", "conn-a", script.id);
    assert!(matches!(result, Err(ScriptHandlerError::NotRunnable)));
}
