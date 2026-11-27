use super::*;

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
