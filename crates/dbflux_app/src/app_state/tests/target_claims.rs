use super::*;
use dbflux_core::{CancelPolicy, TaskRequest, TaskStatus, TaskTarget};

fn connect_request(profile_id: Uuid) -> TaskRequest {
    TaskRequest::new(TaskKind::Connect, "connecting")
        .target(TaskTarget {
            profile_id,
            database: None,
        })
        .policy(CancelPolicy::WorkerAcknowledged)
        .claims_target()
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
    assert!(
        state.is_task_target_claimed(&TaskTarget::connection(profile_id)),
        "the sidebar spinner keeps spinning while the attempt is Cancelling"
    );

    assert!(state.acknowledge_task_cancelled(stage.id()));
    assert!(state.acknowledge_task_cancelled(attempt.id()));

    let status = state.tasks().get(attempt.id()).map(|task| task.status);
    assert_eq!(status, Some(TaskStatus::Cancelled));
    assert!(!state.is_task_target_claimed(&TaskTarget::connection(profile_id)));
    assert!(state.try_start_task(connect_request(profile_id)).is_ok());
}

/// The spinner and the duplicate guard read the same record: the task.
#[test]
fn the_connect_claim_is_the_task_and_clears_when_it_settles() {
    let mut state = state_for_task_tests();
    let profile_id = Uuid::new_v4();
    let target = TaskTarget::connection(profile_id);
    assert!(!state.is_task_target_claimed(&target));

    let attempt = state
        .try_start_task(connect_request(profile_id))
        .expect("attempt starts");
    assert!(state.is_task_target_claimed(&target));
    assert!(state.try_start_task(connect_request(profile_id)).is_err());

    state.complete_task(attempt.id());
    assert!(!state.is_task_target_claimed(&target));
}
