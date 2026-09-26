use dbflux_approval::{ExecutionPlan, PendingExecutionStore, PendingStatus};
use dbflux_policy::ExecutionClassification;
use dbflux_storage::StorageRuntime;

fn sample_plan() -> ExecutionPlan {
    ExecutionPlan {
        connection_id: "conn-test".to_string(),
        actor_id: "agent-a".to_string(),
        tool_id: "delete_rows".to_string(),
        classification: ExecutionClassification::Destructive,
        payload: serde_json::json!({"table": "users", "where": "id = 1"}),
    }
}

fn runtime() -> StorageRuntime {
    StorageRuntime::in_memory().expect("in_memory runtime should succeed")
}

#[test]
fn create_and_get_round_trip() {
    let rt = runtime();
    let mut store = rt.pending_executions().expect("store should open");

    let entry = store
        .create_pending(&sample_plan(), None)
        .expect("create_pending should succeed");

    assert_eq!(entry.status, PendingStatus::Pending);
    assert!(entry.created_at > 0);
    assert!(entry.expires_at.is_none());
    assert_eq!(entry.plan.tool_id, "delete_rows");
    assert_eq!(
        entry.plan.classification,
        ExecutionClassification::Destructive
    );

    let fetched = store
        .get_pending(entry.id)
        .expect("get_pending should succeed")
        .expect("entry should be found");

    assert_eq!(fetched.id, entry.id);
    assert_eq!(fetched.plan.connection_id, "conn-test");
    assert_eq!(
        fetched.plan.payload,
        serde_json::json!({"table": "users", "where": "id = 1"})
    );
}

#[test]
fn update_status_changes_stored_row() {
    let rt = runtime();
    let mut store = rt.pending_executions().expect("store should open");

    let entry = store
        .create_pending(&sample_plan(), None)
        .expect("create_pending should succeed");

    let updated = store
        .update_status(entry.id, PendingStatus::Approved)
        .expect("update_status should succeed")
        .expect("entry should be found after update");

    assert_eq!(updated.status, PendingStatus::Approved);

    // get_pending only returns entries that are still in Pending status and not expired.
    // A row whose status was just set to Approved must no longer be returned.
    let re_fetched = store
        .get_pending(entry.id)
        .expect("get_pending should succeed");
    assert!(
        re_fetched.is_none(),
        "approved entry must not be returned by get_pending"
    );
}

#[test]
fn list_pending_excludes_expired_entries() {
    let rt = runtime();
    let mut store = rt.pending_executions().expect("store should open");

    let past_ms = 1_000i64;
    store
        .create_pending(&sample_plan(), Some(past_ms))
        .expect("create_pending should succeed");

    let list = store.list_pending().expect("list_pending should succeed");
    assert!(
        list.is_empty(),
        "expired entry must not appear in list_pending"
    );
}

#[test]
fn list_pending_excludes_non_pending_status() {
    let rt = runtime();
    let mut store = rt.pending_executions().expect("store should open");

    let entry = store
        .create_pending(&sample_plan(), None)
        .expect("create_pending should succeed");
    store
        .update_status(entry.id, PendingStatus::Rejected)
        .expect("update_status should succeed");

    let list = store.list_pending().expect("list_pending should succeed");
    assert!(
        list.is_empty(),
        "rejected entry must not appear in list_pending"
    );
}

#[test]
fn list_pending_includes_non_expired_entries() {
    let rt = runtime();
    let mut store = rt.pending_executions().expect("store should open");

    let far_future_ms = i64::MAX;
    store
        .create_pending(&sample_plan(), Some(far_future_ms))
        .expect("create_pending should succeed");

    let list = store.list_pending().expect("list_pending should succeed");
    assert_eq!(list.len(), 1, "non-expired pending entry must be listed");
}

#[test]
fn restart_survival_same_db_retains_pending_entry() {
    let rt = StorageRuntime::in_memory().expect("in_memory runtime should succeed");

    let entry = {
        let mut store_a = rt.pending_executions().expect("store A should open");
        store_a
            .create_pending(&sample_plan(), None)
            .expect("create_pending should succeed")
    };

    // Simulate restart: open a second store over the same underlying DB path.
    // Both stores open independent connections to the same file, verifying
    // that the row survives across store-instance boundaries.
    let store_b = rt.pending_executions().expect("store B should open");
    let list = store_b
        .list_pending()
        .expect("list_pending on store B should succeed");

    assert_eq!(
        list.len(),
        1,
        "pending entry must survive store-instance restart"
    );
    assert_eq!(list[0].id, entry.id);
    assert_eq!(list[0].plan.tool_id, "delete_rows");
}

#[test]
fn get_pending_returns_none_for_unknown_id() {
    let rt = runtime();
    let store = rt.pending_executions().expect("store should open");
    let result = store
        .get_pending(uuid::Uuid::new_v4())
        .expect("get_pending should succeed for unknown id");
    assert!(result.is_none());
}

#[test]
fn payload_with_nested_json_survives_round_trip() {
    let rt = runtime();
    let mut store = rt.pending_executions().expect("store should open");

    let plan = ExecutionPlan {
        connection_id: "conn-a".to_string(),
        actor_id: "agent".to_string(),
        tool_id: "complex_tool".to_string(),
        classification: ExecutionClassification::Write,
        payload: serde_json::json!({
            "nested": {"key": "value", "arr": [1, 2, 3]},
            "flag": true
        }),
    };

    let entry = store
        .create_pending(&plan, None)
        .expect("create_pending should succeed");
    let fetched = store
        .get_pending(entry.id)
        .expect("get_pending should succeed")
        .expect("entry should be found");

    assert_eq!(fetched.plan.payload, plan.payload);
}

#[test]
fn approved_plan_is_consumed_exactly_once() {
    let rt = runtime();
    let mut store = rt.pending_executions().expect("store should open");

    let entry = store
        .create_pending(&sample_plan(), None)
        .expect("create_pending should succeed");
    store
        .update_status(entry.id, PendingStatus::Approved)
        .expect("update_status should succeed");

    let consumed = store
        .consume_approved(&sample_plan())
        .expect("consume_approved should succeed")
        .expect("the approved row should match the plan");

    assert_eq!(consumed.id, entry.id);
    assert_eq!(consumed.status, PendingStatus::Consumed);
    assert!(
        store
            .consume_approved(&sample_plan())
            .expect("consume_approved should succeed")
            .is_none(),
        "a consumed approval must not authorize a second call"
    );
}

#[test]
fn consume_approved_skips_pending_rejected_and_other_payloads() {
    let rt = runtime();
    let mut store = rt.pending_executions().expect("store should open");

    store
        .create_pending(&sample_plan(), None)
        .expect("create_pending should succeed");
    let rejected = store
        .create_pending(&sample_plan(), None)
        .expect("create_pending should succeed");
    store
        .update_status(rejected.id, PendingStatus::Rejected)
        .expect("update_status should succeed");

    let mut other = sample_plan();
    other.payload = serde_json::json!({"table": "users", "where": "id = 2"});
    let approved_other = store
        .create_pending(&other, None)
        .expect("create_pending should succeed");
    store
        .update_status(approved_other.id, PendingStatus::Approved)
        .expect("update_status should succeed");

    assert!(
        store
            .consume_approved(&sample_plan())
            .expect("consume_approved should succeed")
            .is_none()
    );
}

#[test]
fn purge_keeps_live_approvals_and_drops_consumed_and_rejected_rows() {
    let rt = runtime();
    let mut store = rt.pending_executions().expect("store should open");

    let approved = store
        .create_pending(&sample_plan(), None)
        .expect("create_pending should succeed");
    store
        .update_status(approved.id, PendingStatus::Approved)
        .expect("update_status should succeed");
    let rejected = store
        .create_pending(&sample_plan(), None)
        .expect("create_pending should succeed");
    store
        .update_status(rejected.id, PendingStatus::Rejected)
        .expect("update_status should succeed");
    let consumed = store
        .create_pending(&sample_plan(), None)
        .expect("create_pending should succeed");
    store
        .update_status(consumed.id, PendingStatus::Consumed)
        .expect("update_status should succeed");

    let purged = store
        .purge_terminal_and_expired(i64::MAX - 1)
        .expect("purge should succeed");

    assert_eq!(purged, 2);
    assert!(
        store
            .consume_approved(&sample_plan())
            .expect("consume_approved should succeed")
            .is_some(),
        "the approved row must survive the purge"
    );
}

#[test]
fn approval_given_by_one_process_is_consumed_by_another() {
    let path = std::env::temp_dir().join(format!(
        "dbflux_pending_cross_process_{}_{}.sqlite",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));

    let app_runtime = StorageRuntime::for_path(path.clone()).expect("app runtime should open");
    let server_runtime =
        StorageRuntime::for_path(path.clone()).expect("server runtime should open");
    let mut app_store = app_runtime
        .pending_executions()
        .expect("app store should open");
    let mut server_store = server_runtime
        .pending_executions()
        .expect("server store should open");

    let queued = server_store
        .create_pending(&sample_plan(), None)
        .expect("create_pending should succeed");
    let listed = app_store
        .list_pending()
        .expect("list_pending should succeed");
    assert!(listed.iter().any(|entry| entry.id == queued.id));

    app_store
        .update_status(queued.id, PendingStatus::Approved)
        .expect("update_status should succeed");

    let consumed = server_store
        .consume_approved(&sample_plan())
        .expect("consume_approved should succeed")
        .expect("the server should see the approval given by the app");
    assert_eq!(consumed.id, queued.id);

    for suffix in ["", "-wal", "-shm"] {
        let file = std::path::PathBuf::from(format!("{}{suffix}", path.display()));
        match std::fs::remove_file(&file) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("failed to remove {}: {error}", file.display()),
        }
    }
}
