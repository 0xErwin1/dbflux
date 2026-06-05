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

fn service() -> ApprovalService {
    ApprovalService::new(Box::new(InMemoryPendingExecutionStore::default()))
}

#[test]
fn approval_replays_exact_stored_plan_snapshot() {
    let mut approval_service = service();

    let original_plan = mutation_plan("UPDATE users SET active = true");
    let pending = request_execution(&mut approval_service, &original_plan)
        .expect("request_execution should succeed");

    let mut changed_plan = original_plan.clone();
    changed_plan.payload = serde_json::json!({"query": "DROP TABLE users"});

    let approved = approve_execution(&mut approval_service, &pending.id.to_string())
        .expect("approval should succeed");

    assert_eq!(
        approved.replay_plan.payload,
        serde_json::json!({"query": "UPDATE users SET active = true"})
    );
    assert_ne!(approved.replay_plan.payload, changed_plan.payload);
    assert!(
        approval_service
            .list_pending()
            .expect("list_pending should succeed")
            .is_empty()
    );
}

#[test]
fn rejected_execution_cannot_be_approved_and_never_executes() {
    let mut approval_service = service();

    let pending = request_execution(&mut approval_service, &mutation_plan("DELETE FROM users"))
        .expect("request_execution should succeed");
    reject_execution(&mut approval_service, &pending.id.to_string())
        .expect("reject should succeed");

    let err = approve_execution(&mut approval_service, &pending.id.to_string())
        .expect_err("approval should fail after rejection");

    let err_str = err.to_string();
    assert!(
        err_str.contains("pending execution is not in pending state")
            || err_str.contains("pending execution not found"),
        "expected a rejection-related error, got: {err_str}"
    );
    assert!(
        approval_service
            .list_pending()
            .expect("list_pending should succeed")
            .is_empty()
    );
}
