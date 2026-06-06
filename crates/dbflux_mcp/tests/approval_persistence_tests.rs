use dbflux_mcp::{McpGovernanceService, McpRuntime};

fn build_runtime(rt: &dbflux_storage::StorageRuntime) -> McpRuntime {
    let audit_path = dbflux_audit::temp_sqlite_path(&format!(
        "mcp_persistence_test_audit_{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let audit = dbflux_audit::AuditService::new_sqlite(&audit_path)
        .expect("audit service should initialize");

    let store = rt
        .pending_executions()
        .expect("pending_executions store should open");

    McpRuntime::new(audit, Box::new(store))
}

fn sample_plan() -> dbflux_approval::ExecutionPlan {
    dbflux_approval::ExecutionPlan {
        connection_id: "conn-a".to_string(),
        actor_id: "agent-a".to_string(),
        tool_id: "delete_rows".to_string(),
        classification: dbflux_policy::ExecutionClassification::Destructive,
        payload: serde_json::json!({"table": "users"}),
    }
}

/// Creates a pending approval via runtime A, drops it, builds runtime B over
/// the same DB, and asserts the entry is still listed as pending.
#[test]
fn pending_approval_survives_runtime_restart() {
    let rt = dbflux_storage::StorageRuntime::in_memory()
        .expect("in_memory storage runtime should succeed");

    let pending_id = {
        let mut runtime_a = build_runtime(&rt);
        let plan = sample_plan();
        let summary = runtime_a
            .request_execution_mut(runtime_a.classify_plan(
                plan.classification,
                plan.payload,
                plan.actor_id,
                plan.connection_id,
                plan.tool_id,
            ))
            .expect("request_execution_mut should succeed");
        summary.id
    };

    // Simulate restart: build a new runtime over the same StorageRuntime.
    let runtime_b = build_runtime(&rt);
    let pending_list = runtime_b
        .list_pending_executions()
        .expect("list_pending_executions should succeed on runtime B");

    assert_eq!(
        pending_list.len(),
        1,
        "pending approval must survive runtime restart"
    );
    assert_eq!(
        pending_list[0].id, pending_id,
        "survived entry must have the same id"
    );
}

/// Creates a pending approval with an already-expired TTL, builds a new runtime
/// over the same DB, and asserts the entry is absent (filtered at list time).
#[test]
fn expired_approval_is_filtered_at_list_time() {
    let rt = dbflux_storage::StorageRuntime::in_memory()
        .expect("in_memory storage runtime should succeed");

    {
        let runtime_a = build_runtime(&rt);
        let plan = sample_plan();
        let past_ms = 1_000i64;

        let store = rt
            .pending_executions()
            .expect("pending_executions store should open");
        let mut store: Box<dyn dbflux_approval::PendingExecutionStore> = Box::new(store);
        store
            .create_pending(&plan, Some(past_ms))
            .expect("create_pending with past expires_at should succeed");

        let _ = runtime_a;
    }

    let runtime_b = build_runtime(&rt);
    let pending_list = runtime_b
        .list_pending_executions()
        .expect("list_pending_executions should succeed on runtime B");

    assert!(
        pending_list.is_empty(),
        "expired entry must be filtered out at list time"
    );
}
