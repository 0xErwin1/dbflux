//! The assignment for tools that take no connection, built by
//! `create_global_assignments`.

use dbflux_mcp::{McpRuntime, builtin_policies, builtin_roles};

use super::create_global_assignments;

fn runtime_with_clients(file_name: &str, client_ids: &[&str]) -> McpRuntime {
    let path = dbflux_audit::temp_sqlite_path(&format!(
        "{}_{}.sqlite",
        file_name.trim_end_matches(".sqlite"),
        uuid::Uuid::new_v4()
    ));
    let audit =
        dbflux_audit::AuditService::new_sqlite(&path).expect("audit service should initialize");
    let mut runtime = McpRuntime::new(
        audit,
        Box::new(dbflux_approval::InMemoryPendingExecutionStore::default()),
    );

    for role in builtin_roles() {
        runtime.upsert_role_mut(role).expect("built-in role loads");
    }

    for policy in builtin_policies() {
        runtime
            .upsert_policy_mut(policy)
            .expect("built-in policy loads");
    }

    for client_id in client_ids {
        runtime
            .upsert_trusted_client_mut(dbflux_mcp::TrustedClientDto {
                id: client_id.to_string(),
                name: client_id.to_string(),
                issuer: None,
                active: true,
            })
            .expect("trusted client loads");
    }

    runtime
}

fn assign_role_on_connection(
    runtime: &mut McpRuntime,
    connection_id: &str,
    actor_id: &str,
    role_id: &str,
) {
    runtime
        .save_connection_policy_assignment_mut(dbflux_mcp::ConnectionPolicyAssignmentDto {
            connection_id: connection_id.to_string(),
            assignments: vec![dbflux_policy::ConnectionPolicyAssignment {
                actor_id: actor_id.to_string(),
                scope: dbflux_policy::PolicyBindingScope {
                    connection_id: connection_id.to_string(),
                },
                role_ids: vec![role_id.to_string()],
                policy_ids: vec![],
            }],
        })
        .expect("assignment saves");
}

fn evaluate_without_connection(
    runtime: &McpRuntime,
    actor_id: &str,
    tool_id: &str,
    classification: dbflux_policy::ExecutionClassification,
) -> dbflux_policy::PolicyDecision {
    dbflux_policy::PolicyEngine::new(
        runtime.policy_assignments_for_engine(),
        runtime.roles_for_engine(),
        runtime.policies_for_engine(),
    )
    .evaluate(&dbflux_policy::PolicyEvaluationRequest {
        actor_id: actor_id.to_string(),
        connection_id: String::new(),
        tool_id: tool_id.to_string(),
        classification,
    })
    .expect("policy evaluates")
}

#[test]
fn connectionless_tools_follow_the_roles_a_client_holds_on_its_connections() {
    use dbflux_policy::{ExecutionClassification, PolicyDecision};

    let mut runtime = runtime_with_clients("global-scope-admin.sqlite", &["agent"]);
    assign_role_on_connection(&mut runtime, "conn-a", "agent", "builtin/admin");

    create_global_assignments(&mut runtime).expect("global assignments save");

    assert_eq!(
        evaluate_without_connection(
            &runtime,
            "agent",
            "create_script",
            ExecutionClassification::Write
        ),
        PolicyDecision::RequireApproval
    );
    assert_eq!(
        evaluate_without_connection(
            &runtime,
            "agent",
            "delete_script",
            ExecutionClassification::Admin
        ),
        PolicyDecision::RequireApproval
    );
    assert_eq!(
        evaluate_without_connection(
            &runtime,
            "agent",
            "list_scripts",
            ExecutionClassification::Metadata
        ),
        PolicyDecision::Allow
    );
}

#[test]
fn connectionless_tools_stay_read_only_for_a_client_without_connection_roles() {
    use dbflux_policy::{ExecutionClassification, PolicyDecision, PolicyDecisionReason};

    let mut runtime = runtime_with_clients("global-scope-isolation.sqlite", &["agent", "other"]);
    assign_role_on_connection(&mut runtime, "conn-a", "agent", "builtin/admin");

    create_global_assignments(&mut runtime).expect("global assignments save");

    assert_eq!(
        evaluate_without_connection(
            &runtime,
            "other",
            "create_script",
            ExecutionClassification::Write
        ),
        PolicyDecision::Deny(PolicyDecisionReason::ToolDenied)
    );
    assert_eq!(
        evaluate_without_connection(
            &runtime,
            "other",
            "list_scripts",
            ExecutionClassification::Metadata
        ),
        PolicyDecision::Allow
    );
}

#[test]
fn connectionless_scope_excludes_inactive_clients_and_never_allows_approval_tools() {
    use dbflux_policy::{ExecutionClassification, PolicyDecision, PolicyDecisionReason};

    let mut runtime = runtime_with_clients("global-scope-inactive.sqlite", &["agent", "writer"]);
    runtime
        .upsert_trusted_client_mut(dbflux_mcp::TrustedClientDto {
            id: "inactive".to_string(),
            name: "inactive".to_string(),
            issuer: None,
            active: false,
        })
        .expect("trusted client loads");
    assign_role_on_connection(&mut runtime, "conn-a", "agent", "builtin/admin");
    assign_role_on_connection(&mut runtime, "conn-b", "writer", "builtin/write");
    assign_role_on_connection(&mut runtime, "conn-c", "inactive", "builtin/admin");

    create_global_assignments(&mut runtime).expect("global assignments save");

    let global_actors: Vec<String> = runtime
        .policy_assignments_for_engine()
        .into_iter()
        .filter(|assignment| assignment.scope.connection_id.is_empty())
        .map(|assignment| assignment.actor_id)
        .collect();
    assert!(!global_actors.contains(&"inactive".to_string()));

    assert_eq!(
        evaluate_without_connection(
            &runtime,
            "writer",
            "create_script",
            ExecutionClassification::Write
        ),
        PolicyDecision::RequireApproval
    );
    assert_eq!(
        evaluate_without_connection(
            &runtime,
            "writer",
            "delete_script",
            ExecutionClassification::Admin
        ),
        PolicyDecision::Deny(PolicyDecisionReason::ToolDenied)
    );
    assert_eq!(
        evaluate_without_connection(
            &runtime,
            "agent",
            "approve_execution",
            ExecutionClassification::Admin
        ),
        PolicyDecision::Deny(PolicyDecisionReason::ToolDenied)
    );
}
