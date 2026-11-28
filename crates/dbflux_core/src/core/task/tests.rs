use super::*;

#[tokio::test]
async fn cancelled_future_wakes_when_cancelled_from_another_thread() {
    let token = CancelToken::new();
    let waiter = tokio::spawn({
        let token = token.clone();
        async move { token.cancelled().await }
    });
    tokio::task::yield_now().await;

    let canceller = token.clone();
    std::thread::spawn(move || canceller.cancel())
        .join()
        .expect("cancel thread");

    tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
        .await
        .expect("cancelled() resolves after cancel")
        .expect("join");
}

#[test]
fn import_and_migrate_labels() {
    assert_eq!(TaskKind::Import.label(), "Import");
    assert_eq!(TaskKind::Migrate.label(), "Migrate");
}

#[test]
fn every_task_kind_has_a_non_empty_label() {
    let kinds = [
        TaskKind::Query,
        TaskKind::Connect,
        TaskKind::Disconnect,
        TaskKind::SwitchDatabase,
        TaskKind::LoadSchema,
        TaskKind::SchemaRefresh,
        TaskKind::SchemaDrop,
        TaskKind::Export,
        TaskKind::Import,
        TaskKind::Migrate,
        TaskKind::KeyScan,
        TaskKind::KeyGet,
        TaskKind::KeyMutation,
        TaskKind::DumpAnalysis,
    ];

    for kind in kinds {
        assert!(!kind.label().is_empty());
    }
}

#[test]
fn running_key_scan_task_is_cancellable() {
    let mut manager = TaskManager::new();
    let (id, _token) = manager.start(TaskKind::KeyScan, "SCAN 0");

    assert!(manager.get(id).expect("task exists").is_cancellable);
}

#[test]
fn key_reads_and_writes_are_not_cancellable_while_running() {
    let mut manager = TaskManager::new();

    for kind in [TaskKind::KeyGet, TaskKind::KeyMutation] {
        let (id, _token) = manager.start(kind, kind.label());
        let snapshot = manager.get(id).expect("task exists");

        assert_eq!(snapshot.status, TaskStatus::Running);
        assert!(
            !snapshot.is_cancellable,
            "{} must not offer a cancel that cannot stop the driver call",
            kind.label()
        );
    }
}

#[test]
fn a_task_marked_uncancellable_offers_no_cancel_while_running() {
    let mut manager = TaskManager::new();
    let (id, _token) = manager.start(TaskKind::Query, "UPDATE users SET name = 'a'");

    manager.mark_uncancellable(id);

    let snapshot = manager.get(id).expect("task exists");
    assert_eq!(snapshot.status, TaskStatus::Running);
    assert!(!snapshot.is_cancellable);
}

#[test]
fn running_query_task_is_cancellable_and_finished_one_is_not() {
    let mut manager = TaskManager::new();
    let (id, _token) = manager.start(TaskKind::Query, "SELECT 1");

    assert!(manager.get(id).expect("task exists").is_cancellable);

    manager.complete(id);

    assert!(!manager.get(id).expect("task exists").is_cancellable);
}

#[test]
fn query_text_reaches_the_snapshot_and_leaves_the_description_alone() {
    let mut manager = TaskManager::new();
    let full_query =
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) SELECT count(*) FROM c;";
    let (id, _token) = manager.start(TaskKind::Query, "WITH RECURSIVE c(x) AS ...");

    assert!(manager.get(id).expect("task exists").query_text.is_none());

    manager.set_query_text(id, full_query);

    let snapshot = manager.get(id).expect("task exists");
    assert_eq!(snapshot.query_text.as_deref(), Some(full_query));
    assert_eq!(snapshot.description, "WITH RECURSIVE c(x) AS ...");
}

#[test]
fn cleanup_keeps_failed_tasks_until_they_are_removed() {
    let mut manager = TaskManager::new();
    let (failed_id, _token) = manager.start(TaskKind::KeyScan, "SCAN 0");
    manager.fail(failed_id, "connection reset");

    manager.cleanup_completed(0);

    let failed = manager
        .get(failed_id)
        .expect("failed task survives cleanup");
    assert_eq!(
        failed.status,
        TaskStatus::Failed("connection reset".to_string())
    );

    manager.remove(failed_id);

    assert!(manager.get(failed_id).is_none());
}

#[test]
fn cleanup_removes_completed_and_cancelled_tasks_past_the_retention_window() {
    let mut manager = TaskManager::new();
    let (completed_id, _token) = manager.start(TaskKind::Query, "SELECT 1");
    let (cancelled_id, _token) = manager.start(TaskKind::Export, "Export");
    let (running_id, _token) = manager.start(TaskKind::LoadSchema, "Load");
    manager.complete(completed_id);
    manager.cancel(cancelled_id);

    manager.cleanup_completed(60);

    assert!(manager.get(completed_id).is_some());
    assert!(manager.get(cancelled_id).is_some());

    manager.cleanup_completed(0);

    assert!(manager.get(completed_id).is_none());
    assert!(manager.get(cancelled_id).is_none());
    assert!(manager.get(running_id).is_some());
}

fn start_with(
    manager: &mut TaskManager,
    policy: CancelPolicy,
    hook: Option<CancelHook>,
) -> TaskHandle {
    let mut request = TaskRequest::new(TaskKind::SchemaDrop, "DROP TABLE users").policy(policy);

    if let Some(hook) = hook {
        request = request.cancel_hook(hook);
    }

    manager.start_handle(request)
}

fn hook_returning(outcome: CancelOutcome) -> CancelHook {
    Arc::new(move || outcome.clone())
}

#[test]
fn immediate_cancel_marks_cancelled_at_once_and_fires_the_token() {
    let mut manager = TaskManager::new();
    let (id, token) = manager.start(TaskKind::Query, "SELECT 1");

    assert_eq!(manager.request_cancel(id), CancelRequestOutcome::Cancelled);

    assert_eq!(manager.get(id).expect("task").status, TaskStatus::Cancelled);
    assert!(token.is_cancelled());
    assert!(
        !manager.cancel(id),
        "a finished task cannot be cancelled again"
    );
}

#[test]
fn legacy_cancel_matches_the_immediate_request() {
    let mut manager = TaskManager::new();
    let (id, token) = manager.start(TaskKind::Query, "SELECT 1");

    assert!(manager.cancel(id));

    assert_eq!(manager.get(id).expect("task").status, TaskStatus::Cancelled);
    assert!(token.is_cancelled());
}

#[test]
fn worker_acknowledged_cancel_waits_for_the_worker() {
    let mut manager = TaskManager::new();
    let handle = start_with(&mut manager, CancelPolicy::WorkerAcknowledged, None);

    assert_eq!(
        manager.request_cancel(handle.id()),
        CancelRequestOutcome::Cancelling
    );

    let snapshot = manager.get(handle.id()).expect("task");
    assert_eq!(snapshot.status, TaskStatus::Cancelling);
    assert!(!snapshot.status.is_terminal());
    assert!(!snapshot.is_cancellable);
    assert!(handle.token().is_cancelled());
    assert_eq!(
        manager.request_cancel(handle.id()),
        CancelRequestOutcome::AlreadyCancelling
    );

    assert!(manager.acknowledge_cancelled(handle.id()));
    assert_eq!(
        manager.get(handle.id()).expect("task").status,
        TaskStatus::Cancelled
    );
}

#[test]
fn acknowledging_a_task_that_is_not_cancelling_changes_nothing() {
    let mut manager = TaskManager::new();
    let handle = start_with(&mut manager, CancelPolicy::WorkerAcknowledged, None);

    assert!(!manager.acknowledge_cancelled(handle.id()));
    assert_eq!(
        manager.get(handle.id()).expect("task").status,
        TaskStatus::Running
    );
}

#[test]
fn hook_reporting_stopped_cancels_at_once() {
    let mut manager = TaskManager::new();
    let handle = start_with(
        &mut manager,
        CancelPolicy::WorkerAcknowledged,
        Some(hook_returning(CancelOutcome::Stopped)),
    );

    assert_eq!(
        manager.request_cancel(handle.id()),
        CancelRequestOutcome::Cancelled
    );
    assert_eq!(
        manager.get(handle.id()).expect("task").status,
        TaskStatus::Cancelled
    );
    assert!(handle.token().is_cancelled());
}

#[test]
fn hook_reporting_requested_moves_to_cancelling() {
    let mut manager = TaskManager::new();
    let handle = start_with(
        &mut manager,
        CancelPolicy::WorkerAcknowledged,
        Some(hook_returning(CancelOutcome::Requested)),
    );

    assert_eq!(
        manager.request_cancel(handle.id()),
        CancelRequestOutcome::Cancelling
    );
    assert_eq!(
        manager.get(handle.id()).expect("task").status,
        TaskStatus::Cancelling
    );
}

#[test]
fn hook_reporting_not_supported_keeps_the_task_running_with_a_reason() {
    let mut manager = TaskManager::new();
    let handle = start_with(
        &mut manager,
        CancelPolicy::WorkerAcknowledged,
        Some(hook_returning(CancelOutcome::NotSupported {
            reason: "driver cannot cancel".into(),
        })),
    );

    assert_eq!(
        manager.request_cancel(handle.id()),
        CancelRequestOutcome::Refused {
            reason: "driver cannot cancel".into()
        }
    );

    let snapshot = manager.get(handle.id()).expect("task");
    assert_eq!(snapshot.status, TaskStatus::Running);
    assert!(!snapshot.is_cancellable);
    assert_eq!(
        snapshot.cancel_unavailable_reason.as_deref(),
        Some("driver cannot cancel")
    );
    assert!(!handle.token().is_cancelled());
}

#[test]
fn hook_that_could_not_deliver_the_cancel_refuses_and_allows_a_retry() {
    let mut manager = TaskManager::new();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hook: CancelHook = {
        let calls = calls.clone();
        Arc::new(move || {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            CancelOutcome::DeliveryFailed {
                reason: "could not start the cancel thread".into(),
            }
        })
    };
    let handle = start_with(&mut manager, CancelPolicy::WorkerAcknowledged, Some(hook));

    let expected = CancelRequestOutcome::Refused {
        reason: "could not start the cancel thread".into(),
    };
    assert_eq!(manager.request_cancel(handle.id()), expected);
    assert_eq!(manager.request_cancel(handle.id()), expected);

    let snapshot = manager.get(handle.id()).expect("task");
    assert_eq!(snapshot.status, TaskStatus::Running);
    assert!(snapshot.is_cancellable, "the user can try again");
    assert_eq!(snapshot.cancel_unavailable_reason, None);
    assert!(!handle.token().is_cancelled());
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[test]
fn a_finished_task_releases_its_cancel_hook() {
    let mut manager = TaskManager::new();
    let witness = Arc::new(());
    let hook_with_witness = || -> CancelHook {
        let witness = witness.clone();
        Arc::new(move || {
            let _held = &witness;
            CancelOutcome::Requested
        })
    };
    let request =
        || TaskRequest::new(TaskKind::Query, "q").policy(CancelPolicy::WorkerAcknowledged);

    let completed = manager.start_handle(request().cancel_hook(hook_with_witness()));
    let failed = manager.start_handle(request().cancel_hook(hook_with_witness()));
    let cancelled = manager.start_handle(request().cancel_hook(hook_with_witness()));
    let detailed = manager.start_handle(request().cancel_hook(hook_with_witness()));
    assert_eq!(Arc::strong_count(&witness), 5);

    manager.complete(completed.id());
    assert_eq!(Arc::strong_count(&witness), 4, "completed");
    manager.fail(failed.id(), "boom");
    assert_eq!(Arc::strong_count(&witness), 3, "failed");
    manager.request_cancel(cancelled.id());
    assert_eq!(
        Arc::strong_count(&witness),
        3,
        "kept while the worker stops"
    );
    assert!(manager.acknowledge_cancelled(cancelled.id()));
    assert_eq!(Arc::strong_count(&witness), 2, "cancelled");
    manager.complete_with_details(detailed.id(), "done");
    assert_eq!(Arc::strong_count(&witness), 1, "completed with details");
}

#[test]
fn point_of_no_return_refuses_cancel_without_touching_the_task() {
    let mut manager = TaskManager::new();
    let handle = start_with(&mut manager, CancelPolicy::WorkerAcknowledged, None);

    manager.mark_point_of_no_return(handle.id(), "DDL already sent");

    assert_eq!(
        manager.request_cancel(handle.id()),
        CancelRequestOutcome::Refused {
            reason: "DDL already sent".into()
        }
    );
    assert!(!manager.cancel(handle.id()));

    let snapshot = manager.get(handle.id()).expect("task");
    assert_eq!(snapshot.status, TaskStatus::Running);
    assert!(!snapshot.is_cancellable);
    assert_eq!(
        snapshot.cancel_unavailable_reason.as_deref(),
        Some("DDL already sent")
    );
    assert!(!handle.token().is_cancelled());
}

#[test]
fn request_cancel_on_finished_or_unknown_tasks_reports_not_running() {
    let mut manager = TaskManager::new();
    let (id, _token) = manager.start(TaskKind::Query, "SELECT 1");
    manager.complete(id);

    assert_eq!(manager.request_cancel(id), CancelRequestOutcome::NotRunning);
    assert_eq!(
        manager.request_cancel(TaskId::new_v4()),
        CancelRequestOutcome::NotRunning
    );
}

#[test]
fn completing_a_cancelling_task_records_that_the_work_finished() {
    let mut manager = TaskManager::new();
    let handle = start_with(&mut manager, CancelPolicy::WorkerAcknowledged, None);
    manager.request_cancel(handle.id());

    manager.complete(handle.id());

    let snapshot = manager.get(handle.id()).expect("task");
    assert_eq!(snapshot.status, TaskStatus::Completed);
}

#[test]
fn completing_a_cancelling_task_with_details_records_completed() {
    let mut manager = TaskManager::new();
    let handle = start_with(&mut manager, CancelPolicy::WorkerAcknowledged, None);
    manager.request_cancel(handle.id());

    manager.complete_with_details(handle.id(), "done anyway");

    let snapshot = manager.get(handle.id()).expect("task");
    assert_eq!(snapshot.status, TaskStatus::Completed);
    assert_eq!(snapshot.details.as_deref(), Some("done anyway"));
}

#[test]
fn failing_a_cancelling_task_records_the_failure() {
    let mut manager = TaskManager::new();
    let handle = start_with(&mut manager, CancelPolicy::WorkerAcknowledged, None);
    manager.request_cancel(handle.id());

    manager.fail(handle.id(), "connection lost");

    assert_eq!(
        manager.get(handle.id()).expect("task").status,
        TaskStatus::Failed("connection lost".into())
    );
}

#[test]
fn a_cancelling_task_still_counts_as_running_work() {
    let mut manager = TaskManager::new();
    let handle = start_with(&mut manager, CancelPolicy::WorkerAcknowledged, None);
    manager.request_cancel(handle.id());

    assert!(manager.has_running_tasks());
    assert_eq!(manager.active_count(), 1);
    assert_eq!(manager.running_tasks().len(), 1);
}

#[test]
fn mark_uncancellable_hides_cancel_but_legacy_cancel_still_works() {
    let mut manager = TaskManager::new();
    let (id, token) = manager.start(TaskKind::Query, "UPDATE t SET a = 1");

    manager.mark_uncancellable(id);

    let snapshot = manager.get(id).expect("task");
    assert!(!snapshot.is_cancellable);
    assert_eq!(snapshot.cancel_unavailable_reason, None);

    assert!(manager.cancel(id));
    assert!(token.is_cancelled());
    assert_eq!(manager.get(id).expect("task").status, TaskStatus::Cancelled);
}

#[test]
fn cancel_all_reports_cancelled_cancelling_and_refused_separately() {
    let mut manager = TaskManager::new();
    let (immediate, _token) = manager.start(TaskKind::Query, "SELECT 1");
    let acknowledged = start_with(&mut manager, CancelPolicy::WorkerAcknowledged, None);
    let locked = start_with(&mut manager, CancelPolicy::WorkerAcknowledged, None);
    manager.mark_point_of_no_return(locked.id(), "DDL already sent");

    let summary = manager.cancel_all_detailed();

    assert_eq!(
        summary,
        CancelAllSummary {
            cancelled: 1,
            cancelling: 1,
            refused: 1
        }
    );
    assert_eq!(
        manager.get(immediate).expect("task").status,
        TaskStatus::Cancelled
    );
    assert_eq!(
        manager.get(acknowledged.id()).expect("task").status,
        TaskStatus::Cancelling
    );
    assert_eq!(
        manager.get(locked.id()).expect("task").status,
        TaskStatus::Running
    );
}

#[test]
fn cancel_all_counts_every_accepted_request() {
    let mut manager = TaskManager::new();
    manager.start(TaskKind::Query, "SELECT 1");
    start_with(&mut manager, CancelPolicy::WorkerAcknowledged, None);

    assert_eq!(manager.cancel_all(), 2);
}

#[test]
fn start_handle_carries_target_and_profile() {
    let mut manager = TaskManager::new();
    let profile_id = Uuid::new_v4();
    let target = TaskTarget {
        profile_id,
        database: Some("app".into()),
    };

    let handle = manager.start_handle(
        TaskRequest::new(TaskKind::SchemaDrop, "DROP TABLE users").target(target.clone()),
    );

    let snapshot = manager.get(handle.id()).expect("task");
    assert_eq!(snapshot.target, Some(target));
    assert_eq!(snapshot.profile_id, Some(profile_id));
    assert_eq!(snapshot.status, TaskStatus::Running);
    assert!(snapshot.is_cancellable);
}

// ---------------------------------------------------------------------------
// Hierarchy, supersede and target dedup
// ---------------------------------------------------------------------------

fn target(database: Option<&str>) -> TaskTarget {
    TaskTarget {
        profile_id: Uuid::from_u128(7),
        database: database.map(str::to_string),
    }
}

fn acknowledged_request(kind: TaskKind) -> TaskRequest {
    TaskRequest::new(kind, kind.label()).policy(CancelPolicy::WorkerAcknowledged)
}

fn not_supported_hook() -> CancelHook {
    Arc::new(|| CancelOutcome::NotSupported {
        reason: "driver cannot cancel".to_string(),
    })
}

fn status_of(manager: &TaskManager, id: TaskId) -> TaskStatus {
    manager.get(id).expect("task exists").status
}

#[test]
fn a_child_records_its_parent_and_children_of_lists_them_in_start_order() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(TaskRequest::new(TaskKind::Connect, "connect"));
    let first = manager.start_handle(
        TaskRequest::new(
            TaskKind::Hook {
                phase: HookPhase::PreConnect,
            },
            "a",
        )
        .parent(parent.id()),
    );
    let second =
        manager.start_handle(TaskRequest::new(TaskKind::LoadSchema, "b").parent(parent.id()));
    let unrelated = manager.start_handle(TaskRequest::new(TaskKind::Query, "q"));

    assert_eq!(
        manager.get(first.id()).unwrap().parent_id,
        Some(parent.id())
    );
    assert_eq!(manager.get(parent.id()).unwrap().parent_id, None);
    assert_eq!(manager.get(unrelated.id()).unwrap().parent_id, None);

    let children: Vec<TaskId> = manager
        .children_of(parent.id())
        .iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(children, vec![first.id(), second.id()]);
    assert!(manager.children_of(unrelated.id()).is_empty());
}

#[test]
fn cancelling_a_parent_cascades_to_immediate_children() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(TaskRequest::new(TaskKind::Connect, "connect"));
    let child =
        manager.start_handle(TaskRequest::new(TaskKind::LoadSchema, "schema").parent(parent.id()));
    let grandchild =
        manager.start_handle(TaskRequest::new(TaskKind::Query, "q").parent(child.id()));

    assert_eq!(
        manager.request_cancel(parent.id()),
        CancelRequestOutcome::Cancelled
    );

    for handle in [&parent, &child, &grandchild] {
        assert_eq!(status_of(&manager, handle.id()), TaskStatus::Cancelled);
        assert!(handle.is_cancelled());
    }
}

#[test]
fn a_parent_is_cancelled_only_after_every_acknowledged_child_stopped() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(TaskRequest::new(TaskKind::Connect, "connect"));
    let first =
        manager.start_handle(acknowledged_request(TaskKind::LoadSchema).parent(parent.id()));
    let second = manager.start_handle(acknowledged_request(TaskKind::Export).parent(parent.id()));

    assert_eq!(
        manager.request_cancel(parent.id()),
        CancelRequestOutcome::Cancelling
    );
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelling);
    assert_eq!(status_of(&manager, first.id()), TaskStatus::Cancelling);

    assert!(manager.acknowledge_cancelled(first.id()));
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelling);

    assert!(manager.acknowledge_cancelled(second.id()));
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelled);
}

#[test]
fn a_parent_cannot_be_acknowledged_while_a_child_is_still_stopping() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(acknowledged_request(TaskKind::Connect));
    let child =
        manager.start_handle(acknowledged_request(TaskKind::LoadSchema).parent(parent.id()));

    manager.request_cancel(parent.id());

    assert!(!manager.acknowledge_cancelled(parent.id()));
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelling);

    assert!(manager.acknowledge_cancelled(child.id()));
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelled);
}

#[test]
fn a_parent_acknowledged_before_its_child_settles_when_the_last_child_stops() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(acknowledged_request(TaskKind::Connect));
    let child =
        manager.start_handle(acknowledged_request(TaskKind::LoadSchema).parent(parent.id()));
    manager.request_cancel(parent.id());

    assert!(!manager.acknowledge_cancelled(parent.id()));
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelling);

    assert!(manager.acknowledge_cancelled(child.id()));

    assert_eq!(
        status_of(&manager, parent.id()),
        TaskStatus::Cancelled,
        "the parent worker already returned and will not call again"
    );
}

#[test]
fn a_parent_acknowledged_after_its_children_stopped_settles_at_once() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(acknowledged_request(TaskKind::Connect));
    let child =
        manager.start_handle(acknowledged_request(TaskKind::LoadSchema).parent(parent.id()));
    manager.request_cancel(parent.id());
    assert!(manager.acknowledge_cancelled(child.id()));
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelling);

    assert!(manager.acknowledge_cancelled(parent.id()));

    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelled);
}

#[test]
fn a_pending_parent_waiting_on_a_refusing_child_settles_as_skipped() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(TaskRequest::new(TaskKind::Connect, "connect").pending());
    let child = manager.start_handle(
        acknowledged_request(TaskKind::SchemaDrop)
            .cancel_hook(not_supported_hook())
            .parent(parent.id()),
    );

    manager.request_cancel(parent.id());
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelling);

    manager.complete(child.id());

    assert_eq!(
        status_of(&manager, parent.id()),
        TaskStatus::Skipped,
        "a stage that never started still reads as not started"
    );
}

#[test]
fn a_pending_parent_without_in_flight_children_is_skipped_at_once() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(TaskRequest::new(TaskKind::Connect, "connect").pending());

    manager.request_cancel(parent.id());

    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Skipped);
}

#[test]
fn a_late_pending_leaf_under_a_completed_intermediate_of_a_cancelled_root_is_skipped() {
    let mut manager = TaskManager::new();
    let root = manager.start_handle(TaskRequest::new(TaskKind::Connect, "root"));
    let middle =
        manager.start_handle(TaskRequest::new(TaskKind::Connect, "middle").parent(root.id()));
    manager.complete(middle.id());
    manager.request_cancel(root.id());
    assert_eq!(status_of(&manager, root.id()), TaskStatus::Cancelled);

    let leaf = manager.start_handle(
        TaskRequest::new(TaskKind::LoadSchema, "leaf")
            .pending()
            .parent(middle.id()),
    );

    assert_eq!(status_of(&manager, leaf.id()), TaskStatus::Skipped);
    assert!(leaf.is_cancelled());
    assert!(!manager.activate(leaf.id()));
}

#[test]
fn a_late_running_leaf_under_a_completed_intermediate_of_a_cancelled_root_is_cancelled() {
    let mut manager = TaskManager::new();
    let root = manager.start_handle(TaskRequest::new(TaskKind::Connect, "root"));
    let middle =
        manager.start_handle(TaskRequest::new(TaskKind::Connect, "middle").parent(root.id()));
    manager.complete(middle.id());
    manager.request_cancel(root.id());

    let leaf = manager.start_handle(TaskRequest::new(TaskKind::Query, "leaf").parent(middle.id()));

    assert!(leaf.is_cancelled());
    assert_eq!(status_of(&manager, leaf.id()), TaskStatus::Cancelled);
}

#[test]
fn a_child_that_cannot_start_does_not_supersede_live_work() {
    let mut manager = TaskManager::new();
    let live = manager
        .start_handle(TaskRequest::new(TaskKind::LoadSchema, "live").supersede_key("schema"));
    let parent = manager.start_handle(TaskRequest::new(TaskKind::Connect, "connect"));
    manager.request_cancel(parent.id());

    let child = manager.start_handle(
        TaskRequest::new(TaskKind::LoadSchema, "child")
            .pending()
            .supersede_key("schema")
            .parent(parent.id()),
    );

    assert!(child.superseded().is_empty());
    assert_eq!(status_of(&manager, live.id()), TaskStatus::Running);
    assert!(!live.is_cancelled());
    assert_eq!(status_of(&manager, child.id()), TaskStatus::Skipped);
}

#[test]
fn a_child_that_cannot_stop_keeps_the_parent_from_reading_cancelled() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(TaskRequest::new(TaskKind::Connect, "connect"));
    let child = manager.start_handle(
        acknowledged_request(TaskKind::SchemaDrop)
            .cancel_hook(not_supported_hook())
            .parent(parent.id()),
    );

    assert_eq!(
        manager.request_cancel(parent.id()),
        CancelRequestOutcome::Cancelling
    );
    assert_eq!(status_of(&manager, child.id()), TaskStatus::Running);
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelling);

    manager.complete(child.id());
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelled);
}

#[test]
fn a_child_past_the_point_of_no_return_keeps_the_parent_from_reading_cancelled() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(TaskRequest::new(TaskKind::Connect, "connect"));
    let child =
        manager.start_handle(TaskRequest::new(TaskKind::SchemaDrop, "drop").parent(parent.id()));
    manager.mark_point_of_no_return(child.id(), "DDL already sent");

    assert_eq!(
        manager.request_cancel(parent.id()),
        CancelRequestOutcome::Cancelling
    );
    assert_eq!(status_of(&manager, child.id()), TaskStatus::Running);
    assert!(!child.is_cancelled());
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelling);

    manager.fail(child.id(), "boom");
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelled);
}

#[test]
fn a_parent_past_the_point_of_no_return_is_refused_but_its_children_are_cancelled() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(TaskRequest::new(TaskKind::SchemaDrop, "drop"));
    let child =
        manager.start_handle(TaskRequest::new(TaskKind::LoadSchema, "reload").parent(parent.id()));
    manager.mark_point_of_no_return(parent.id(), "DDL already sent");

    assert_eq!(
        manager.request_cancel(parent.id()),
        CancelRequestOutcome::Refused {
            reason: "DDL already sent".to_string()
        }
    );
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Running);
    assert_eq!(status_of(&manager, child.id()), TaskStatus::Cancelled);
}

#[test]
fn pending_children_are_skipped_when_the_parent_is_cancelled() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(TaskRequest::new(TaskKind::Connect, "connect"));
    let pending = manager.start_handle(
        TaskRequest::new(TaskKind::LoadSchema, "schema")
            .pending()
            .parent(parent.id()),
    );

    assert_eq!(status_of(&manager, pending.id()), TaskStatus::Pending);

    manager.request_cancel(parent.id());

    assert_eq!(status_of(&manager, pending.id()), TaskStatus::Skipped);
    assert!(TaskStatus::Skipped.is_terminal());
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelled);
}

#[test]
fn activate_moves_a_pending_task_to_running_and_pending_is_not_in_flight() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(TaskRequest::new(TaskKind::Connect, "connect"));
    let pending = manager.start_handle(
        TaskRequest::new(TaskKind::LoadSchema, "schema")
            .pending()
            .parent(parent.id()),
    );

    assert!(!TaskStatus::Pending.is_terminal());
    assert_eq!(manager.active_count(), 1);
    assert_eq!(manager.running_tasks().len(), 1);
    assert_eq!(
        manager.background_task_count(),
        1,
        "only the running parent counts"
    );
    assert!(
        manager
            .children_of(parent.id())
            .iter()
            .any(|c| c.id == pending.id())
    );

    assert!(manager.activate(pending.id()));
    assert_eq!(status_of(&manager, pending.id()), TaskStatus::Running);
    assert_eq!(manager.active_count(), 2);
    assert_eq!(manager.background_task_count(), 2);

    assert!(
        !manager.activate(pending.id()),
        "only a pending task can be activated"
    );
    assert!(!manager.activate(TaskId::new_v4()));
}

#[test]
fn a_pending_only_manager_has_no_running_tasks() {
    let mut manager = TaskManager::new();
    manager.start_handle(TaskRequest::new(TaskKind::Connect, "x").pending());

    assert!(!manager.has_running_tasks());
    assert_eq!(manager.current_status_message(), None);
}

#[test]
fn completing_a_parent_leaves_its_running_children_untouched() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(TaskRequest::new(TaskKind::Connect, "connect"));
    let child =
        manager.start_handle(TaskRequest::new(TaskKind::LoadSchema, "schema").parent(parent.id()));

    manager.complete(parent.id());

    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Completed);
    assert_eq!(status_of(&manager, child.id()), TaskStatus::Running);
    assert!(!child.is_cancelled());

    assert_eq!(
        manager.request_cancel(parent.id()),
        CancelRequestOutcome::NotRunning
    );
    assert_eq!(status_of(&manager, child.id()), TaskStatus::Running);
}

#[test]
fn failing_a_parent_leaves_its_running_children_untouched() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(TaskRequest::new(TaskKind::Connect, "connect"));
    let child =
        manager.start_handle(TaskRequest::new(TaskKind::LoadSchema, "schema").parent(parent.id()));

    manager.fail(parent.id(), "boom");

    assert_eq!(status_of(&manager, child.id()), TaskStatus::Running);
}

#[test]
fn starting_with_the_same_supersede_key_cancels_the_older_task_and_reports_it() {
    let mut manager = TaskManager::new();
    let first =
        manager.start_handle(TaskRequest::new(TaskKind::LoadSchema, "a").supersede_key("schema"));
    assert!(first.superseded().is_empty());

    let second =
        manager.start_handle(TaskRequest::new(TaskKind::LoadSchema, "b").supersede_key("schema"));

    assert_eq!(second.superseded(), &[first.id()]);
    assert_eq!(status_of(&manager, first.id()), TaskStatus::Cancelled);
    assert_eq!(status_of(&manager, second.id()), TaskStatus::Running);
}

#[test]
fn supersede_is_honest_about_an_older_task_that_is_still_stopping() {
    let mut manager = TaskManager::new();
    let first = manager.start_handle(acknowledged_request(TaskKind::Query).supersede_key("q"));
    let other =
        manager.start_handle(TaskRequest::new(TaskKind::Query, "other").supersede_key("other"));

    let second = manager.start_handle(TaskRequest::new(TaskKind::Query, "b").supersede_key("q"));

    assert_eq!(second.superseded(), &[first.id()]);
    assert_eq!(status_of(&manager, first.id()), TaskStatus::Cancelling);
    assert_eq!(status_of(&manager, other.id()), TaskStatus::Running);
}

#[test]
fn supersede_ignores_finished_tasks_and_other_keys() {
    let mut manager = TaskManager::new();
    let done = manager.start_handle(TaskRequest::new(TaskKind::Query, "a").supersede_key("q"));
    manager.complete(done.id());

    let next = manager.start_handle(TaskRequest::new(TaskKind::Query, "b").supersede_key("q"));

    assert!(next.superseded().is_empty());
    assert_eq!(status_of(&manager, done.id()), TaskStatus::Completed);
}

#[test]
fn try_start_refuses_a_duplicate_of_the_same_kind_and_target() {
    let mut manager = TaskManager::new();
    let first = manager
        .try_start(TaskRequest::new(TaskKind::SwitchDatabase, "switch").target(target(Some("a"))))
        .expect("first start succeeds");

    let busy = manager
        .try_start(
            TaskRequest::new(TaskKind::SwitchDatabase, "switch again").target(target(Some("a"))),
        )
        .err()
        .expect("duplicate refused");

    assert_eq!(busy.existing, first.id());
    assert!(manager.is_busy(TaskKind::SwitchDatabase, &target(Some("a"))));
    assert_eq!(
        manager.active_count(),
        1,
        "a refused start registers nothing"
    );
}

#[test]
fn try_start_allows_a_different_database_kind_or_profile() {
    let mut manager = TaskManager::new();
    manager
        .try_start(TaskRequest::new(TaskKind::SwitchDatabase, "s").target(target(Some("a"))))
        .expect("first");

    assert!(
        manager
            .try_start(TaskRequest::new(TaskKind::SwitchDatabase, "s").target(target(Some("b"))))
            .is_ok()
    );
    assert!(
        manager
            .try_start(TaskRequest::new(TaskKind::SwitchDatabase, "s").target(target(None)))
            .is_ok()
    );
    assert!(
        manager
            .try_start(TaskRequest::new(TaskKind::LoadSchema, "s").target(target(Some("a"))))
            .is_ok()
    );

    let other_profile = TaskTarget {
        profile_id: Uuid::from_u128(8),
        database: Some("a".into()),
    };
    assert!(!manager.is_busy(TaskKind::SwitchDatabase, &other_profile));
    assert!(
        manager
            .try_start(TaskRequest::new(TaskKind::SwitchDatabase, "s").target(other_profile))
            .is_ok()
    );
}

#[test]
fn try_start_allows_a_new_task_once_the_first_finished() {
    let mut manager = TaskManager::new();
    let first = manager
        .try_start(TaskRequest::new(TaskKind::Connect, "c").target(target(None)))
        .expect("first");
    manager.complete(first.id());

    assert!(!manager.is_busy(TaskKind::Connect, &target(None)));
    assert!(
        manager
            .try_start(TaskRequest::new(TaskKind::Connect, "c").target(target(None)))
            .is_ok()
    );
}

#[test]
fn a_cancelling_task_still_blocks_try_start() {
    let mut manager = TaskManager::new();
    let first = manager
        .try_start(acknowledged_request(TaskKind::SchemaDrop).target(target(Some("a"))))
        .expect("first");
    manager.request_cancel(first.id());
    assert_eq!(status_of(&manager, first.id()), TaskStatus::Cancelling);

    let busy = manager
        .try_start(TaskRequest::new(TaskKind::SchemaDrop, "again").target(target(Some("a"))))
        .err()
        .expect("still busy");
    assert_eq!(busy.existing, first.id());

    manager.acknowledge_cancelled(first.id());
    assert!(
        manager
            .try_start(TaskRequest::new(TaskKind::SchemaDrop, "again").target(target(Some("a"))))
            .is_ok()
    );
}

#[test]
fn pending_tasks_do_not_block_try_start() {
    let mut manager = TaskManager::new();
    manager.start_handle(
        TaskRequest::new(TaskKind::LoadSchema, "stage")
            .target(target(None))
            .pending(),
    );

    assert!(!manager.is_busy(TaskKind::LoadSchema, &target(None)));
    assert!(
        manager
            .try_start(TaskRequest::new(TaskKind::LoadSchema, "real").target(target(None)))
            .is_ok()
    );
}

#[test]
fn hook_kinds_share_one_group_whatever_their_phase() {
    let mut manager = TaskManager::new();
    manager
        .try_start(
            TaskRequest::new(
                TaskKind::Hook {
                    phase: HookPhase::PreConnect,
                },
                "h",
            )
            .target(target(None)),
        )
        .expect("first");

    assert!(manager.is_busy(
        TaskKind::Hook {
            phase: HookPhase::PostConnect
        },
        &target(None)
    ));
    assert!(
        manager
            .try_start(
                TaskRequest::new(
                    TaskKind::Hook {
                        phase: HookPhase::PostConnect
                    },
                    "h"
                )
                .target(target(None))
            )
            .is_err()
    );
}

#[test]
fn try_start_without_a_target_never_dedups() {
    let mut manager = TaskManager::new();
    assert!(
        manager
            .try_start(TaskRequest::new(TaskKind::Query, "a"))
            .is_ok()
    );
    assert!(
        manager
            .try_start(TaskRequest::new(TaskKind::Query, "a"))
            .is_ok()
    );
}

#[test]
fn legacy_start_paths_keep_their_behavior() {
    let mut manager = TaskManager::new();
    let (first, token) = manager.start_for_target(TaskKind::Query, "q", Some(target(None)));
    let (second, _) = manager.start_for_target(TaskKind::Query, "q", Some(target(None)));

    assert_eq!(
        manager.active_count(),
        2,
        "legacy start does not dedup or supersede"
    );
    assert_eq!(manager.get(first).unwrap().parent_id, None);

    assert!(manager.cancel(first));
    assert!(token.is_cancelled());
    assert_eq!(status_of(&manager, first), TaskStatus::Cancelled);
    assert_eq!(status_of(&manager, second), TaskStatus::Running);
}

// ---------------------------------------------------------------------------
// Cascade through terminal intermediates and registration under a cancelled parent
// ---------------------------------------------------------------------------

#[test]
fn cancelling_a_root_reaches_a_running_leaf_under_a_completed_intermediate() {
    let mut manager = TaskManager::new();
    let root = manager.start_handle(TaskRequest::new(TaskKind::Connect, "root"));
    let middle =
        manager.start_handle(TaskRequest::new(TaskKind::Connect, "middle").parent(root.id()));
    let leaf = manager.start_handle(acknowledged_request(TaskKind::Query).parent(middle.id()));
    manager.complete(middle.id());

    assert_eq!(
        manager.request_cancel(root.id()),
        CancelRequestOutcome::Cancelling
    );

    assert_eq!(status_of(&manager, leaf.id()), TaskStatus::Cancelling);
    assert!(leaf.is_cancelled());
    assert_eq!(status_of(&manager, middle.id()), TaskStatus::Completed);
    assert_eq!(status_of(&manager, root.id()), TaskStatus::Cancelling);

    assert!(manager.acknowledge_cancelled(leaf.id()));

    assert_eq!(status_of(&manager, leaf.id()), TaskStatus::Cancelled);
    assert_eq!(status_of(&manager, root.id()), TaskStatus::Cancelled);
}

#[test]
fn a_direct_cancel_of_a_completed_task_is_a_no_op_and_spares_its_children() {
    let mut manager = TaskManager::new();
    let middle = manager.start_handle(TaskRequest::new(TaskKind::Connect, "middle"));
    let leaf = manager.start_handle(acknowledged_request(TaskKind::Query).parent(middle.id()));
    manager.complete(middle.id());

    assert_eq!(
        manager.request_cancel(middle.id()),
        CancelRequestOutcome::NotRunning
    );

    assert_eq!(status_of(&manager, middle.id()), TaskStatus::Completed);
    assert_eq!(status_of(&manager, leaf.id()), TaskStatus::Running);
    assert!(!leaf.is_cancelled());
}

#[test]
fn completing_an_intermediate_leaves_its_running_leaf_untouched() {
    let mut manager = TaskManager::new();
    let root = manager.start_handle(TaskRequest::new(TaskKind::Connect, "root"));
    let middle =
        manager.start_handle(TaskRequest::new(TaskKind::Connect, "middle").parent(root.id()));
    let leaf = manager.start_handle(acknowledged_request(TaskKind::Query).parent(middle.id()));

    manager.complete(middle.id());

    assert_eq!(status_of(&manager, leaf.id()), TaskStatus::Running);
    assert!(!leaf.is_cancelled());
    assert_eq!(status_of(&manager, root.id()), TaskStatus::Running);
}

#[test]
fn a_pending_child_under_a_cancelled_parent_is_skipped_and_never_activates() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(TaskRequest::new(TaskKind::Connect, "connect"));
    manager.request_cancel(parent.id());

    let child = manager.start_handle(
        TaskRequest::new(TaskKind::LoadSchema, "schema")
            .pending()
            .parent(parent.id()),
    );

    assert_eq!(status_of(&manager, child.id()), TaskStatus::Skipped);
    assert!(child.is_cancelled());
    assert!(!manager.activate(child.id()));
    assert_eq!(status_of(&manager, child.id()), TaskStatus::Skipped);
    assert_eq!(manager.active_count(), 0);
}

#[test]
fn a_running_child_under_a_cancelling_parent_is_cancelled_at_registration() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(acknowledged_request(TaskKind::Connect));
    manager.request_cancel(parent.id());
    assert_eq!(status_of(&manager, parent.id()), TaskStatus::Cancelling);

    let child = manager.start_handle(acknowledged_request(TaskKind::Query).parent(parent.id()));

    assert!(child.is_cancelled());
    assert_eq!(status_of(&manager, child.id()), TaskStatus::Cancelling);
}

#[test]
fn a_child_under_a_running_parent_starts_normally() {
    let mut manager = TaskManager::new();
    let parent = manager.start_handle(TaskRequest::new(TaskKind::Connect, "connect"));

    let running = manager.start_handle(TaskRequest::new(TaskKind::Query, "q").parent(parent.id()));
    let pending = manager.start_handle(
        TaskRequest::new(TaskKind::LoadSchema, "s")
            .pending()
            .parent(parent.id()),
    );

    assert_eq!(status_of(&manager, running.id()), TaskStatus::Running);
    assert!(!running.is_cancelled());
    assert_eq!(status_of(&manager, pending.id()), TaskStatus::Pending);
    assert!(manager.activate(pending.id()));
}

#[test]
fn a_child_whose_supersede_cancels_its_parent_is_skipped() {
    let mut manager = TaskManager::new();
    let parent = manager
        .start_handle(TaskRequest::new(TaskKind::LoadSchema, "parent").supersede_key("schema"));

    let child = manager.start_handle(
        TaskRequest::new(TaskKind::LoadSchema, "child")
            .pending()
            .supersede_key("schema")
            .parent(parent.id()),
    );

    assert!(parent.is_cancelled());
    assert_eq!(status_of(&manager, child.id()), TaskStatus::Skipped);
    assert!(!manager.activate(child.id()));
}
