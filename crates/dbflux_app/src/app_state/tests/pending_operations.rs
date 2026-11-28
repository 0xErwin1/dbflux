use super::*;
use dbflux_core::{CancelPolicy, TaskRequest, TaskStatus, TaskTarget};

fn connect_request(profile_id: Uuid) -> TaskRequest {
    TaskRequest::new(TaskKind::Connect, "connecting")
        .target(TaskTarget {
            profile_id,
            database: None,
        })
        .policy(CancelPolicy::WorkerAcknowledged)
}

/// Cancelling a connect attempt does not free the profile: the attempt turns
/// `Cancelled` only when its worker stops, and a retry is refused until then,
/// so the cancelled attempt can never release the retry's claim.
#[test]
fn a_cancelled_connect_attempt_keeps_the_profile_busy_until_its_worker_stops() {
    let mut state = state_for_task_tests();
    let profile_id = Uuid::new_v4();

    let attempt = state
        .try_start_task(connect_request(profile_id))
        .expect("first attempt starts");
    let stage = state.start_task_handle(
        TaskRequest::new(TaskKind::Connect, "stage")
            .parent(attempt.id())
            .policy(CancelPolicy::WorkerAcknowledged),
    );

    assert_eq!(
        state.cancel_running_connect_tasks_for_profile(profile_id),
        1,
        "stages are not counted as attempts"
    );
    assert!(
        state.try_start_task(connect_request(profile_id)).is_err(),
        "a retry must wait for the cancelled attempt to stop"
    );

    assert!(state.acknowledge_task_cancelled(stage.id()));
    assert!(state.acknowledge_task_cancelled(attempt.id()));

    let status = state.tasks().get(attempt.id()).map(|task| task.status);
    assert_eq!(status, Some(TaskStatus::Cancelled));
    assert!(state.try_start_task(connect_request(profile_id)).is_ok());
}

/// A direct connect has no stages and keeps the immediate cancel policy until
/// the direct path migrates: its task reads `Cancelled` on the request, so no
/// worker confirmation will free the profile, and the request frees it.
#[test]
fn cancelling_a_direct_connect_frees_the_profile_for_a_retry() {
    let mut state = state_for_task_tests();
    let profile_id = Uuid::new_v4();

    let direct = state
        .try_start_task(
            TaskRequest::new(TaskKind::Connect, "connecting").target(TaskTarget {
                profile_id,
                database: None,
            }),
        )
        .expect("direct attempt starts");
    assert!(state.start_pending_operation(profile_id, None));

    assert_eq!(
        state.cancel_running_connect_tasks_for_profile(profile_id),
        1
    );

    assert_eq!(
        state.tasks().get(direct.id()).map(|task| task.status),
        Some(TaskStatus::Cancelled)
    );
    assert!(
        !state.is_operation_pending(profile_id, None),
        "the sidebar must not keep showing the cancelled connect as busy"
    );
    assert!(state.start_pending_operation(profile_id, None));
}
