//! A connect attempt as a parent task with one child task per pipeline stage.
//!
//! Every stage that applies to the profile is registered up front as a
//! `pending()` child, so the tasks panel shows the whole pipeline from the
//! start. A stage turns `Running` when the pipeline reaches it and settles
//! when the next one begins or the attempt ends. Cancelling the parent
//! cascades through the task hierarchy: the running stage waits for the worker
//! (`Cancelling`), and the stages that never started read `Skipped`.
//! Cancelling a stage cancels the attempt: every stage's cancel hook cancels
//! the attempt's token, which is the one the pipeline watches.

use dbflux_core::{
    AccessKind, CancelOutcome, CancelPolicy, CancelToken, ConnectionProfile, PipelineState, TaskId,
    TaskKind, TaskRequest, TaskStatus,
};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::hook_phase_runner::DetachedHookScope;
use std::sync::Arc;
use uuid::Uuid;

/// The stages of a connect, in the order the flow reaches them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ConnectStage {
    PreConnectHooks,
    Authenticating,
    ResolvingValues,
    OpeningAccess,
    ConnectingDriver,
    FetchingSchema,
    PostConnectHooks,
}

impl ConnectStage {
    /// The stage a pipeline state belongs to; none for idle and terminal states.
    pub(super) fn of_state(state: &PipelineState) -> Option<Self> {
        match state {
            PipelineState::Authenticating { .. } | PipelineState::WaitingForLogin { .. } => {
                Some(Self::Authenticating)
            }
            PipelineState::ResolvingValues { .. } => Some(Self::ResolvingValues),
            PipelineState::OpeningAccess { .. } => Some(Self::OpeningAccess),
            PipelineState::Connecting { .. } => Some(Self::ConnectingDriver),
            PipelineState::FetchingSchema => Some(Self::FetchingSchema),
            PipelineState::Idle
            | PipelineState::Connected
            | PipelineState::Failed { .. }
            | PipelineState::Cancelled => None,
        }
    }

    /// The stage a failure names, by the stage identifier the pipeline and
    /// the connect flow report it under.
    pub(super) fn of_failure(stage: &str) -> Option<Self> {
        match stage {
            "pre_connect_hook" => Some(Self::PreConnectHooks),
            "authentication" => Some(Self::Authenticating),
            "value_resolution" => Some(Self::ResolvingValues),
            "access" => Some(Self::OpeningAccess),
            "driver_connect" => Some(Self::ConnectingDriver),
            "schema_fetch" => Some(Self::FetchingSchema),
            "post_connect_hook" => Some(Self::PostConnectHooks),
            _ => None,
        }
    }
}

/// What decides which stages a connect has.
pub(super) struct StagePlanInput<'a> {
    pub profile: &'a ConnectionProfile,
    /// Display name of the auth provider the pipeline will sign in with.
    pub auth_provider: Option<&'a str>,
    pub has_pre_connect_hooks: bool,
    pub has_post_connect_hooks: bool,
}

/// The stages that apply to the profile, in order, with their panel names.
pub(super) fn plan_stages(input: &StagePlanInput<'_>) -> Vec<(ConnectStage, String)> {
    let opens_tunnel = input
        .profile
        .access_kind
        .as_ref()
        .is_some_and(|kind| !matches!(kind, AccessKind::Direct));

    [
        (input.has_pre_connect_hooks, ConnectStage::PreConnectHooks),
        (input.auth_provider.is_some(), ConnectStage::Authenticating),
        (
            !input.profile.value_refs.is_empty(),
            ConnectStage::ResolvingValues,
        ),
        (opens_tunnel, ConnectStage::OpeningAccess),
        (true, ConnectStage::ConnectingDriver),
        (true, ConnectStage::FetchingSchema),
        (input.has_post_connect_hooks, ConnectStage::PostConnectHooks),
    ]
    .into_iter()
    .filter(|(applies, _)| *applies)
    .map(|(_, stage)| {
        (
            stage,
            crate::labels::connect_stage_label(stage, input.auth_provider),
        )
    })
    .collect()
}

/// How a connect attempt ended.
pub(super) enum ConnectOutcome<'a> {
    Completed,
    /// `stage` is the stage that failed, when the failure names one: the
    /// progress loop sees only the latest pipeline state, so it may never
    /// have entered it.
    Failed {
        error: &'a str,
        stage: Option<ConnectStage>,
    },
    Cancelled,
}

/// The tasks of one connect attempt, shared by the stage watcher and the
/// connect flow.
#[derive(Clone)]
pub(super) struct ConnectAttempt {
    task_id: TaskId,
    profile_id: Uuid,
    stages: Vec<(ConnectStage, TaskId)>,
    detached_hooks: DetachedHookScope,
}

impl ConnectAttempt {
    /// Registers every planned stage as a pending child of `task_id`.
    /// `attempt_token` is the token the pipeline watches; a cancel of any
    /// stage cancels it, so cancelling a part stops everything after it.
    pub(super) fn register(
        state: &mut AppStateEntity,
        task_id: TaskId,
        attempt_token: &CancelToken,
        profile_id: Uuid,
        plan: Vec<(ConnectStage, String)>,
    ) -> Self {
        let stages = plan
            .into_iter()
            .map(|(stage, label)| {
                let attempt_token = attempt_token.clone();
                let handle = state.start_task_handle(
                    TaskRequest::new(TaskKind::Connect, label)
                        .parent(task_id)
                        .pending()
                        .policy(CancelPolicy::WorkerAcknowledged)
                        .cancel_hook(Arc::new(move || {
                            attempt_token.cancel();
                            CancelOutcome::Requested
                        })),
                );
                (stage, handle.id())
            })
            .collect();

        Self {
            task_id,
            profile_id,
            stages,
            detached_hooks: DetachedHookScope::for_parent(task_id),
        }
    }

    pub(super) fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub(super) fn profile_id(&self) -> Uuid {
        self.profile_id
    }

    pub(super) fn detached_hooks(&self) -> &DetachedHookScope {
        &self.detached_hooks
    }

    fn stage_task(&self, stage: ConnectStage) -> Option<TaskId> {
        self.stages
            .iter()
            .find(|(candidate, _)| *candidate == stage)
            .map(|(_, id)| *id)
    }

    fn status_of(state: &AppStateEntity, id: TaskId) -> Option<TaskStatus> {
        state.tasks().get(id).map(|task| task.status)
    }

    /// Settles the stages before `stage` and starts `stage`. The pipeline
    /// reports its progress through a channel that keeps only the latest
    /// state, so an earlier stage may never have been seen running; it ran
    /// all the same. A stage skipped by a cancel is not started again.
    pub(super) fn enter(&self, state: &mut AppStateEntity, stage: ConnectStage) {
        for (earlier, id) in &self.stages {
            if *earlier < stage {
                state.activate_task(*id);
                state.complete_task(*id);
            }
        }

        if let Some(id) = self.stage_task(stage) {
            state.activate_task(id);
        }
    }

    pub(super) fn complete(&self, state: &mut AppStateEntity, stage: ConnectStage) {
        if let Some(id) = self.stage_task(stage) {
            state.complete_task(id);
        }
    }

    /// Settles the attempt, its stages and its profile claim.
    ///
    /// A failure that arrives after the user cancelled is a cancel: the
    /// attempt reads `Cancelled` once the worker has stopped, never `Failed`.
    pub(super) fn finish(&self, state: &mut AppStateEntity, outcome: ConnectOutcome<'_>) {
        let cancel_requested = matches!(
            Self::status_of(state, self.task_id),
            Some(TaskStatus::Cancelling | TaskStatus::Cancelled)
        );

        match outcome {
            ConnectOutcome::Completed => self.complete_all(state),
            ConnectOutcome::Failed { error, stage } if !cancel_requested => {
                self.fail_all(state, error, stage)
            }
            ConnectOutcome::Failed { .. } | ConnectOutcome::Cancelled => self.cancel_all(state),
        }

        state.finish_pending_operation(self.profile_id, None);
    }

    fn complete_all(&self, state: &mut AppStateEntity) {
        state.append_task_details(
            self.task_id,
            format!("{}\n", crate::labels::pipeline_completed_detail_label()),
        );

        for (_, id) in &self.stages {
            state.activate_task(*id);
            state.complete_task(*id);
        }
        state.complete_task(self.task_id);
    }

    /// Fails the stage the failure names and leaves the stages after it not
    /// started. The stages before it ran, even when the progress loop never
    /// saw them. A failure that names no stage of this attempt fails the
    /// stage that is running.
    fn fail_all(&self, state: &mut AppStateEntity, error: &str, failed: Option<ConnectStage>) {
        state.append_task_details(
            self.task_id,
            format!("{}\n", crate::labels::pipeline_failed_detail_label(error)),
        );

        let failed = failed.filter(|stage| self.stage_task(*stage).is_some());

        for (stage, id) in &self.stages {
            match (Self::status_of(state, *id), failed) {
                (Some(TaskStatus::Running | TaskStatus::Pending), Some(failed)) => {
                    if *stage < failed {
                        state.activate_task(*id);
                        state.complete_task(*id);
                    } else if *stage == failed {
                        state.activate_task(*id);
                        state.fail_task(*id, error);
                    } else {
                        state.request_cancel_task(*id);
                    }
                }
                (Some(TaskStatus::Running), None) => state.fail_task(*id, error),
                (Some(TaskStatus::Pending), None) => {
                    state.request_cancel_task(*id);
                }
                _ => {}
            }
        }
        self.detached_hooks
            .cancel_registered(state, self.profile_id);
        state.fail_task(self.task_id, error);
    }

    fn cancel_all(&self, state: &mut AppStateEntity) {
        state.append_task_details(
            self.task_id,
            format!("{}\n", crate::labels::pipeline_cancelled_detail_label()),
        );

        self.detached_hooks
            .cancel_registered(state, self.profile_id);
        for (_, id) in &self.stages {
            state.settle_task_discarded(*id);
        }
        state.settle_task_discarded(self.task_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{DbConfig, TaskTarget};
    use dbflux_storage::bootstrap::StorageRuntime;

    fn plan_for(
        profile: &ConnectionProfile,
        auth_provider: Option<&str>,
        pre: bool,
        post: bool,
    ) -> Vec<ConnectStage> {
        plan_stages(&StagePlanInput {
            profile,
            auth_provider,
            has_pre_connect_hooks: pre,
            has_post_connect_hooks: post,
        })
        .into_iter()
        .map(|(stage, _)| stage)
        .collect()
    }

    fn profile_with_everything() -> ConnectionProfile {
        let mut profile = ConnectionProfile::new("prod", DbConfig::default_postgres());
        profile.value_refs.insert(
            "password".to_string(),
            dbflux_core::values::ValueRef::literal("secret"),
        );
        profile.access_kind = Some(AccessKind::Ssh {
            ssh_tunnel_profile_id: Uuid::new_v4(),
        });
        profile
    }

    #[test]
    fn a_profile_with_auth_values_tunnel_and_hooks_gets_every_stage_in_order() {
        let profile = profile_with_everything();

        assert_eq!(
            plan_for(&profile, Some("AWS SSO"), true, true),
            vec![
                ConnectStage::PreConnectHooks,
                ConnectStage::Authenticating,
                ConnectStage::ResolvingValues,
                ConnectStage::OpeningAccess,
                ConnectStage::ConnectingDriver,
                ConnectStage::FetchingSchema,
                ConnectStage::PostConnectHooks,
            ]
        );
    }

    #[test]
    fn a_plain_profile_only_gets_the_driver_stages() {
        let profile = ConnectionProfile::new("local", DbConfig::default_postgres());

        assert_eq!(
            plan_for(&profile, None, false, false),
            vec![ConnectStage::ConnectingDriver, ConnectStage::FetchingSchema]
        );

        let mut direct = profile.clone();
        direct.access_kind = Some(AccessKind::Direct);
        assert_eq!(
            plan_for(&direct, None, false, false),
            plan_for(&profile, None, false, false),
            "direct access opens nothing"
        );
    }

    #[test]
    fn the_authentication_stage_names_the_provider() {
        let profile = profile_with_everything();
        let plan = plan_stages(&StagePlanInput {
            profile: &profile,
            auth_provider: Some("AWS SSO"),
            has_pre_connect_hooks: false,
            has_post_connect_hooks: false,
        });

        assert_eq!(plan[0].1, "Authenticating via AWS SSO");
        assert_eq!(plan[1].1, "Resolving values");
        assert_eq!(plan[2].1, "Opening tunnel");
    }

    fn state_with_attempt(
        plan: Vec<(ConnectStage, String)>,
    ) -> (AppStateEntity, ConnectAttempt, Uuid) {
        let (state, attempt, profile_id, _) = state_with_attempt_and_token(plan);
        (state, attempt, profile_id)
    }

    fn state_with_attempt_and_token(
        plan: Vec<(ConnectStage, String)>,
    ) -> (
        AppStateEntity,
        ConnectAttempt,
        Uuid,
        dbflux_core::CancelToken,
    ) {
        let mut state =
            AppStateEntity::new_with_storage_runtime(StorageRuntime::in_memory().expect("storage"))
                .expect("app state");
        let profile_id = Uuid::new_v4();
        let parent = state
            .try_start_task(
                TaskRequest::new(TaskKind::Connect, "Connect")
                    .target(TaskTarget {
                        profile_id,
                        database: None,
                    })
                    .policy(CancelPolicy::WorkerAcknowledged),
            )
            .expect("attempt starts");
        let attempt =
            ConnectAttempt::register(&mut state, parent.id(), parent.token(), profile_id, plan);
        (state, attempt, profile_id, parent.token().clone())
    }

    fn statuses(state: &AppStateEntity, attempt: &ConnectAttempt) -> Vec<TaskStatus> {
        state
            .child_tasks(attempt.task_id)
            .into_iter()
            .map(|task| task.status)
            .collect()
    }

    fn three_stages() -> Vec<(ConnectStage, String)> {
        vec![
            (ConnectStage::Authenticating, "auth".into()),
            (ConnectStage::ConnectingDriver, "driver".into()),
            (ConnectStage::FetchingSchema, "schema".into()),
        ]
    }

    #[test]
    fn stages_start_pending_and_run_one_at_a_time_in_order() {
        let (mut state, attempt, _) = state_with_attempt(three_stages());
        assert_eq!(statuses(&state, &attempt), vec![TaskStatus::Pending; 3]);

        attempt.enter(&mut state, ConnectStage::Authenticating);
        assert_eq!(
            statuses(&state, &attempt),
            vec![
                TaskStatus::Running,
                TaskStatus::Pending,
                TaskStatus::Pending
            ]
        );

        attempt.enter(&mut state, ConnectStage::ConnectingDriver);
        assert_eq!(
            statuses(&state, &attempt),
            vec![
                TaskStatus::Completed,
                TaskStatus::Running,
                TaskStatus::Pending
            ]
        );

        attempt.finish(&mut state, ConnectOutcome::Completed);
        assert_eq!(statuses(&state, &attempt), vec![TaskStatus::Completed; 3]);
        assert_eq!(
            state.tasks().get(attempt.task_id).map(|task| task.status),
            Some(TaskStatus::Completed)
        );
    }

    #[test]
    fn cancelling_the_attempt_during_a_stage_skips_the_stages_after_it() {
        let (mut state, attempt, profile_id) = state_with_attempt(three_stages());
        attempt.enter(&mut state, ConnectStage::Authenticating);

        assert_eq!(
            state.cancel_running_connect_tasks_for_profile(profile_id),
            1
        );
        assert_eq!(
            statuses(&state, &attempt),
            vec![
                TaskStatus::Cancelling,
                TaskStatus::Skipped,
                TaskStatus::Skipped
            ]
        );
        assert_eq!(
            state.tasks().get(attempt.task_id).map(|task| task.status),
            Some(TaskStatus::Cancelling),
            "the attempt reads Cancelled only once its worker stopped"
        );

        attempt.finish(&mut state, ConnectOutcome::Cancelled);

        assert_eq!(
            statuses(&state, &attempt),
            vec![
                TaskStatus::Cancelled,
                TaskStatus::Skipped,
                TaskStatus::Skipped
            ]
        );
        assert_eq!(
            state.tasks().get(attempt.task_id).map(|task| task.status),
            Some(TaskStatus::Cancelled)
        );
        assert!(!state.is_operation_pending(profile_id, None));
    }

    #[test]
    fn a_failure_fails_its_stage_and_leaves_the_later_ones_not_started() {
        let (mut state, attempt, _) = state_with_attempt(three_stages());
        attempt.enter(&mut state, ConnectStage::ConnectingDriver);

        attempt.finish(
            &mut state,
            ConnectOutcome::Failed {
                error: "refused",
                stage: Some(ConnectStage::ConnectingDriver),
            },
        );

        assert_eq!(
            statuses(&state, &attempt),
            vec![
                TaskStatus::Completed,
                TaskStatus::Failed("refused".to_string()),
                TaskStatus::Skipped
            ]
        );
        assert_eq!(
            state.tasks().get(attempt.task_id).map(|task| task.status),
            Some(TaskStatus::Failed("refused".to_string()))
        );
    }

    #[test]
    fn a_failure_after_a_cancel_request_still_reads_cancelled() {
        let (mut state, attempt, profile_id) = state_with_attempt(three_stages());
        attempt.enter(&mut state, ConnectStage::ConnectingDriver);
        state.cancel_running_connect_tasks_for_profile(profile_id);

        attempt.finish(
            &mut state,
            ConnectOutcome::Failed {
                error: "socket closed",
                stage: None,
            },
        );

        assert_eq!(
            state.tasks().get(attempt.task_id).map(|task| task.status),
            Some(TaskStatus::Cancelled)
        );
    }

    #[test]
    fn cancelling_a_running_stage_cancels_the_whole_attempt() {
        let (mut state, attempt, _, token) = state_with_attempt_and_token(three_stages());
        attempt.enter(&mut state, ConnectStage::Authenticating);
        let stage = attempt
            .stage_task(ConnectStage::Authenticating)
            .expect("stage");
        assert!(!token.is_cancelled());

        state.request_cancel_task(stage);

        assert!(
            token.is_cancelled(),
            "the pipeline watches the attempt's token, so a stage cancel must reach it"
        );
        assert_eq!(
            statuses(&state, &attempt)[0],
            TaskStatus::Cancelling,
            "the stage waits for the worker like the attempt"
        );

        attempt.finish(&mut state, ConnectOutcome::Cancelled);

        assert_eq!(
            statuses(&state, &attempt),
            vec![
                TaskStatus::Cancelled,
                TaskStatus::Skipped,
                TaskStatus::Skipped
            ]
        );
        assert_eq!(
            state.tasks().get(attempt.task_id).map(|task| task.status),
            Some(TaskStatus::Cancelled)
        );
    }

    #[test]
    fn a_failure_fails_the_stage_it_names_even_when_it_was_never_entered() {
        let (mut state, attempt, _) = state_with_attempt(three_stages());
        // The progress loop saw authentication, then only the failure.
        attempt.enter(&mut state, ConnectStage::Authenticating);

        attempt.finish(
            &mut state,
            ConnectOutcome::Failed {
                error: "refused",
                stage: Some(ConnectStage::ConnectingDriver),
            },
        );

        assert_eq!(
            statuses(&state, &attempt),
            vec![
                TaskStatus::Completed,
                TaskStatus::Failed("refused".to_string()),
                TaskStatus::Skipped
            ],
            "the driver stage failed; authentication is not blamed"
        );
    }

    #[test]
    fn a_failure_that_names_no_stage_fails_the_running_one() {
        let (mut state, attempt, _) = state_with_attempt(three_stages());
        attempt.enter(&mut state, ConnectStage::Authenticating);

        attempt.finish(
            &mut state,
            ConnectOutcome::Failed {
                error: "boom",
                stage: None,
            },
        );

        assert_eq!(
            statuses(&state, &attempt),
            vec![
                TaskStatus::Failed("boom".to_string()),
                TaskStatus::Skipped,
                TaskStatus::Skipped
            ]
        );
    }

    #[test]
    fn failure_identifiers_map_to_their_stage() {
        assert_eq!(
            ConnectStage::of_failure("driver_connect"),
            Some(ConnectStage::ConnectingDriver)
        );
        assert_eq!(
            ConnectStage::of_failure("authentication"),
            Some(ConnectStage::Authenticating)
        );
        assert_eq!(
            ConnectStage::of_failure("post_connect_hook"),
            Some(ConnectStage::PostConnectHooks)
        );
        assert_eq!(ConnectStage::of_failure("cancelled"), None);
    }
}
