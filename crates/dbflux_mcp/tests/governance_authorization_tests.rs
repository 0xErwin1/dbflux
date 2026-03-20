use dbflux_audit::AuditService;
use dbflux_mcp::server::authorization::{AuthorizationRequest, authorize_request};
use dbflux_mcp::server::request_context::RequestIdentity;
use dbflux_policy::{
    ConnectionPolicyAssignment, ExecutionClassification, PolicyBindingScope, PolicyEngine,
    ToolPolicy, TrustedClient, TrustedClientRegistry,
};

fn audit_service_for_test(file_name: &str) -> AuditService {
    let path = dbflux_audit::temp_sqlite_path(file_name);
    let _ = std::fs::remove_file(&path);
    AuditService::new_sqlite(&path).expect("audit service should initialize")
}

fn trusted_registry() -> TrustedClientRegistry {
    TrustedClientRegistry::new(vec![TrustedClient {
        id: "agent-a".to_string(),
        name: "Agent A".to_string(),
        issuer: None,
        active: true,
    }])
}

fn read_query_policy_engine() -> PolicyEngine {
    PolicyEngine::new(
        vec![ConnectionPolicyAssignment {
            actor_id: "agent-a".to_string(),
            scope: PolicyBindingScope {
                connection_id: "conn-a".to_string(),
            },
            role_ids: Vec::new(),
            policy_ids: vec!["policy-read".to_string()],
        }],
        Vec::new(),
        vec![ToolPolicy {
            id: "policy-read".to_string(),
            allowed_tools: vec!["read_query".to_string()],
            allowed_classes: vec![ExecutionClassification::Read],
        }],
    )
}

#[test]
fn trusted_client_policy_and_mcp_connection_gate_are_enforced() {
    let audit_service = audit_service_for_test("dbflux-mcp-governance-authz.sqlite");
    let trusted_clients = trusted_registry();
    let policy_engine = read_query_policy_engine();

    let allowed = authorize_request(
        &trusted_clients,
        &policy_engine,
        &audit_service,
        &AuthorizationRequest {
            identity: RequestIdentity {
                client_id: "agent-a".to_string(),
                issuer: None,
            },
            connection_id: "conn-a".to_string(),
            tool_id: "read_query".to_string(),
            classification: ExecutionClassification::Read,
            mcp_enabled_for_connection: true,
        },
        1,
    )
    .expect("authorization must succeed");

    assert!(allowed.allowed);
    assert!(allowed.deny_code.is_none());

    let denied_connection_gate = authorize_request(
        &trusted_clients,
        &policy_engine,
        &audit_service,
        &AuthorizationRequest {
            identity: RequestIdentity {
                client_id: "agent-a".to_string(),
                issuer: None,
            },
            connection_id: "conn-a".to_string(),
            tool_id: "read_query".to_string(),
            classification: ExecutionClassification::Read,
            mcp_enabled_for_connection: false,
        },
        2,
    )
    .expect("authorization must succeed");

    assert!(!denied_connection_gate.allowed);
    assert_eq!(
        denied_connection_gate.deny_code,
        Some("connection_not_mcp_enabled")
    );

    let entries = audit_service
        .query(&dbflux_audit::query::AuditQueryFilter::default())
        .expect("audit query should succeed");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].decision, "allow");
    assert_eq!(entries[1].decision, "deny");
    assert_eq!(
        entries[1].reason.as_deref(),
        Some("connection not MCP-enabled")
    );
}

#[test]
fn untrusted_and_deny_by_default_requests_are_rejected_and_audited() {
    let audit_service = audit_service_for_test("dbflux-mcp-governance-deny-default.sqlite");
    let trusted_clients = trusted_registry();
    let deny_by_default_engine = PolicyEngine::default();

    let denied_untrusted = authorize_request(
        &trusted_clients,
        &deny_by_default_engine,
        &audit_service,
        &AuthorizationRequest {
            identity: RequestIdentity {
                client_id: "agent-z".to_string(),
                issuer: None,
            },
            connection_id: "conn-a".to_string(),
            tool_id: "read_query".to_string(),
            classification: ExecutionClassification::Read,
            mcp_enabled_for_connection: true,
        },
        10,
    )
    .expect("authorization must succeed");

    assert!(!denied_untrusted.allowed);
    assert_eq!(denied_untrusted.deny_code, Some("untrusted_client"));

    let denied_by_policy = authorize_request(
        &trusted_clients,
        &deny_by_default_engine,
        &audit_service,
        &AuthorizationRequest {
            identity: RequestIdentity {
                client_id: "agent-a".to_string(),
                issuer: None,
            },
            connection_id: "conn-a".to_string(),
            tool_id: "read_query".to_string(),
            classification: ExecutionClassification::Read,
            mcp_enabled_for_connection: true,
        },
        11,
    )
    .expect("authorization must succeed");

    assert!(!denied_by_policy.allowed);
    assert_eq!(denied_by_policy.deny_code, Some("policy_denied"));
    assert_eq!(
        denied_by_policy.deny_reason.as_deref(),
        Some("no matching connection-scoped assignment")
    );

    let entries = audit_service
        .query(&dbflux_audit::query::AuditQueryFilter::default())
        .expect("audit query should succeed");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].reason.as_deref(), Some("untrusted client"));
    assert_eq!(
        entries[1].reason.as_deref(),
        Some("no matching connection-scoped assignment")
    );
}
