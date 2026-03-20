use dbflux_approval::{ApprovalService, ExecutionPlan, InMemoryPendingExecutionStore};
use dbflux_mcp::handlers::approval::{approve_execution, reject_execution, request_execution};
use dbflux_policy::ExecutionClassification;

fn mutation_plan(query: &str) -> ExecutionPlan {
    ExecutionPlan {
        connection_id: "conn-a".to_string(),
        actor_id: "agent-a".to_string(),
        tool_id: "request_execution".to_string(),
        classification: ExecutionClassification::Write,
        payload: serde_json::json!({"query": query}),
    }
}

#[test]
fn approval_replays_exact_stored_plan_snapshot() {
    let mut approval_service = ApprovalService::new(InMemoryPendingExecutionStore::default());

    let original_plan = mutation_plan("UPDATE users SET active = true");
    let pending = request_execution(&mut approval_service, &original_plan);

    let mut changed_plan = original_plan.clone();
    changed_plan.payload = serde_json::json!({"query": "DROP TABLE users"});

    let replay = approve_execution(&mut approval_service, &pending.id.to_string())
        .expect("approval should succeed");

    assert_eq!(
        replay.payload,
        serde_json::json!({"query": "UPDATE users SET active = true"})
    );
    assert_ne!(replay.payload, changed_plan.payload);
    assert!(approval_service.list_pending().is_empty());
}

#[test]
fn rejected_execution_cannot_be_approved_and_never_executes() {
    let mut approval_service = ApprovalService::new(InMemoryPendingExecutionStore::default());

    let pending = request_execution(&mut approval_service, &mutation_plan("DELETE FROM users"));
    reject_execution(&mut approval_service, &pending.id.to_string())
        .expect("reject should succeed");

    let err = approve_execution(&mut approval_service, &pending.id.to_string())
        .expect_err("approved should fail after rejection");

    assert!(
        err.to_string()
            .contains("pending execution is not in pending state")
    );
    assert!(approval_service.list_pending().is_empty());
}
