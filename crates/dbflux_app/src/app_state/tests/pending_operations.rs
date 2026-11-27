use super::*;

/// Cancelling a Connect task releases the profile's pending entry at once
/// so the user can retry. The cancelled attempt unwinding later must not
/// release the retry's entry; a task that was not cancelled still does.
#[test]
fn a_cancelled_connect_attempt_does_not_release_a_retrys_pending_entry() {
    let mut state = state_for_task_tests();
    let profile_id = Uuid::new_v4();

    assert!(state.start_pending_operation(profile_id, None));
    let (cancelled_attempt, _) =
        state.start_task_for_profile(TaskKind::Connect, "connecting", Some(profile_id));
    assert_eq!(
        state.cancel_running_connect_tasks_for_profile(profile_id),
        1
    );
    assert!(!state.is_operation_pending(profile_id, None));

    assert!(state.start_pending_operation(profile_id, None));
    let (retry_attempt, _) =
        state.start_task_for_profile(TaskKind::Connect, "connecting", Some(profile_id));

    state.release_connect_pending_operation(cancelled_attempt, profile_id);
    assert!(
        state.is_operation_pending(profile_id, None),
        "the cancelled attempt released the retry's pending entry"
    );

    state.release_connect_pending_operation(retry_attempt, profile_id);
    assert!(!state.is_operation_pending(profile_id, None));
}
