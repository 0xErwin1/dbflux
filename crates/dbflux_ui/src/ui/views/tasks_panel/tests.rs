use super::{RECENT_FINISHED_TASK_LIMIT, TasksPanel, top_level_tasks, visible_finished_tasks};
use crate::app::{AppStateChanged, AppStateEntity};
use dbflux_core::{
    ConnectionProfile, DbConfig, TaskId, TaskKind, TaskManager, TaskRequest, TaskStatus, TaskTarget,
};
use dbflux_storage::bootstrap::StorageRuntime;
use gpui::{
    AccessibilityFrame, AppContext as _, Entity, FrameObserver, Modifiers, TestAppContext,
    VisualTestContext,
};
use std::sync::{Arc, Mutex};

fn test_app_state(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
    cx.update(dbflux_components::theme::init);

    cx.update(|cx| {
        cx.new(|_| {
            AppStateEntity::new_with_storage_runtime(
                StorageRuntime::in_memory().expect("test storage runtime"),
            )
            .expect("test app state")
        })
    })
}

fn start_task(
    app_state: &Entity<AppStateEntity>,
    kind: TaskKind,
    cx: &mut VisualTestContext,
) -> TaskId {
    app_state.update(cx, |state, cx| {
        let (task_id, _token) = state.start_task(kind, kind.label());
        cx.emit(AppStateChanged);
        task_id
    })
}

fn selector(prefix: &str, task_id: TaskId) -> &'static str {
    format!("{prefix}-{task_id}").leak()
}

#[gpui::test]
fn cancel_button_renders_only_for_cancellable_running_tasks(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });

    let query_id = start_task(&app_state, TaskKind::Query, window);
    let key_scan_id = start_task(&app_state, TaskKind::KeyScan, window);
    let key_read_and_write_ids =
        [TaskKind::KeyGet, TaskKind::KeyMutation].map(|kind| start_task(&app_state, kind, window));
    window.run_until_parked();

    assert!(
        window
            .debug_bounds(selector("cancel-task", query_id))
            .is_some(),
        "a running query keeps its cancel button"
    );
    assert!(
        window
            .debug_bounds(selector("cancel-task", key_scan_id))
            .is_some(),
        "a running key scan offers a cancel button"
    );

    for task_id in key_read_and_write_ids {
        assert!(
            window
                .debug_bounds(selector("cancel-task", task_id))
                .is_none(),
            "a key read or write must not render a cancel button"
        );
    }
}

fn start_acknowledged_query(
    app_state: &Entity<AppStateEntity>,
    outcome: dbflux_core::CancelOutcome,
    cx: &mut VisualTestContext,
) -> TaskId {
    app_state.update(cx, |state, cx| {
        let request = dbflux_core::TaskRequest::new(TaskKind::Query, "select 1")
            .policy(dbflux_core::CancelPolicy::WorkerAcknowledged)
            .cancel_hook(std::sync::Arc::new(move || outcome.clone()));
        let task_id = state.start_task_handle(request).id();
        cx.emit(AppStateChanged);
        task_id
    })
}

fn click_cancel(task_id: TaskId, window: &mut VisualTestContext) {
    let bounds = window
        .debug_bounds(selector("cancel-task", task_id))
        .expect("a running query renders its cancel button");
    window.simulate_click(bounds.center(), Modifiers::none());
    window.run_until_parked();
}

fn status_of(
    app_state: &Entity<AppStateEntity>,
    task_id: TaskId,
    cx: &mut VisualTestContext,
) -> TaskStatus {
    app_state.read_with(cx, |state, _| {
        state.tasks().get(task_id).expect("task exists").status
    })
}

#[gpui::test]
fn cancelling_a_query_the_driver_cannot_stop_does_not_mark_it_cancelled(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let task_id = start_acknowledged_query(
        &app_state,
        dbflux_core::CancelOutcome::NotSupported {
            reason: "Engine does not support cancelling a running command.".to_string(),
        },
        window,
    );
    window.run_until_parked();

    click_cancel(task_id, window);

    assert_eq!(status_of(&app_state, task_id, window), TaskStatus::Running);
    let reason = app_state.read_with(window, |state, _| {
        state
            .tasks()
            .get(task_id)
            .and_then(|task| task.cancel_unavailable_reason)
    });
    assert_eq!(
        reason.as_deref(),
        Some("Engine does not support cancelling a running command.")
    );
}

#[gpui::test]
fn cancelling_a_query_with_a_requested_hook_waits_for_the_worker(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let task_id =
        start_acknowledged_query(&app_state, dbflux_core::CancelOutcome::Requested, window);
    window.run_until_parked();

    click_cancel(task_id, window);

    assert_eq!(
        status_of(&app_state, task_id, window),
        TaskStatus::Cancelling
    );
}

#[gpui::test]
fn cancelling_an_immediate_task_still_marks_it_cancelled(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let task_id = start_task(&app_state, TaskKind::KeyScan, window);
    window.run_until_parked();

    click_cancel(task_id, window);

    assert_eq!(
        status_of(&app_state, task_id, window),
        TaskStatus::Cancelled
    );
}

#[gpui::test]
fn failed_task_survives_retention_and_is_removed_by_dismiss(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });

    let failed_id = start_task(&app_state, TaskKind::KeyScan, window);
    let completed_id = start_task(&app_state, TaskKind::Query, window);
    app_state.update(window, |state, cx| {
        state.fail_task(failed_id, "connection reset");
        state.complete_task(completed_id);
        state.tasks_mut().cleanup_completed(0);
        cx.emit(AppStateChanged);
    });
    window.run_until_parked();

    assert!(
        app_state.read_with(window, |state, _| state.tasks().get(completed_id).is_none()),
        "a completed task is removed once past the retention window"
    );
    assert!(
        window
            .debug_bounds(selector("dismiss-task", completed_id))
            .is_none()
    );

    let dismiss_bounds = window
        .debug_bounds(selector("dismiss-task", failed_id))
        .expect("a failed task renders a dismiss button after the retention sweep");

    window.simulate_click(dismiss_bounds.center(), Modifiers::none());
    window.run_until_parked();

    assert!(
        app_state.read_with(window, |state, _| state.tasks().get(failed_id).is_none()),
        "dismiss removes the failed task"
    );
    assert!(
        window
            .debug_bounds(selector("dismiss-task", failed_id))
            .is_none()
    );
}

#[test]
fn visible_finished_tasks_keeps_every_failure_and_caps_the_rest() {
    let mut manager = TaskManager::new();

    let failed_ids: Vec<TaskId> = (0..3)
        .map(|_| {
            let (id, _token) = manager.start(TaskKind::Export, "export");
            manager.fail(id, "disk full");
            id
        })
        .collect();

    for _ in 0..(RECENT_FINISHED_TASK_LIMIT + 3) {
        let (id, _token) = manager.start(TaskKind::Query, "query");
        manager.complete(id);
    }

    let (running_id, _token) = manager.start(TaskKind::Query, "running");

    let visible = visible_finished_tasks(manager.recent_tasks(usize::MAX));

    for failed_id in &failed_ids {
        assert!(visible.iter().any(|task| task.id == *failed_id));
    }
    assert!(visible.iter().all(|task| task.id != running_id));
    assert_eq!(
        visible
            .iter()
            .filter(|task| task.status == TaskStatus::Completed)
            .count(),
        RECENT_FINISHED_TASK_LIMIT
    );
}

fn add_profile(
    app_state: &Entity<AppStateEntity>,
    name: &str,
    cx: &mut VisualTestContext,
) -> uuid::Uuid {
    app_state.update(cx, |state, _| {
        let profile = ConnectionProfile::new(
            name,
            DbConfig::SQLite {
                path: std::path::PathBuf::from(":memory:"),
                connection_id: None,
            },
        );
        let profile_id = profile.id;
        state.profiles_mut().push(profile);
        profile_id
    })
}

fn target_of(profile_id: uuid::Uuid) -> TaskTarget {
    TaskTarget {
        profile_id,
        database: None,
    }
}

fn start_request(
    app_state: &Entity<AppStateEntity>,
    request: TaskRequest,
    cx: &mut VisualTestContext,
) -> TaskId {
    app_state.update(cx, |state, cx| {
        let task_id = state.start_task_handle(request).id();
        cx.emit(AppStateChanged);
        task_id
    })
}

fn notify(app_state: &Entity<AppStateEntity>, cx: &mut VisualTestContext) {
    app_state.update(cx, |_, cx| cx.emit(AppStateChanged));
    cx.run_until_parked();
}

/// Keeps the latest rendered accessibility frame of the window it observes.
#[derive(Default)]
struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

impl FrameObserver for FrameCapture {
    fn accessibility_updated(&self, frame: &AccessibilityFrame) {
        *self.0.lock().expect("frame capture lock") = Some(frame.clone());
    }
}

impl FrameCapture {
    fn observe(window: &mut VisualTestContext) -> Arc<Self> {
        let capture = Arc::new(Self::default());

        window.update(|window, _| {
            window.observe_frames(&capture);
            window.refresh();
        });
        window.run_until_parked();

        capture
    }

    /// Draws a new frame and returns it.
    fn frame(&self, window: &mut VisualTestContext) -> AccessibilityFrame {
        window.update(|window, _| window.refresh());
        window.run_until_parked();

        self.0
            .lock()
            .expect("frame capture lock")
            .clone()
            .expect("the window rendered a frame")
    }
}

fn accessibility_node<'a>(
    frame: &'a AccessibilityFrame,
    id: &str,
) -> Option<&'a gpui::accesskit::Node> {
    let (_, node) = frame.nodes().find(|(_, node)| node.id() == id)?;

    frame.accessibility_node(node)
}

fn shows_text(frame: &gpui::AccessibilityFrame, text: &str) -> bool {
    frame
        .nodes()
        .any(|(_, node)| node.content_text().contains(text))
}

fn cancel_button_is_disabled(frame: &gpui::AccessibilityFrame, task_id: TaskId) -> bool {
    accessibility_node(frame, &format!("cancel-task-button-{task_id}"))
        .expect("the row renders a cancel button")
        .is_disabled()
}

#[gpui::test]
fn a_row_reads_the_target_then_the_kind(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let capture = FrameCapture::observe(window);
    let profile_id = add_profile(&app_state, "prod-pg", window);

    start_request(
        &app_state,
        TaskRequest::new(TaskKind::Query, "select * from users").target(target_of(profile_id)),
        window,
    );
    start_request(
        &app_state,
        TaskRequest::new(TaskKind::Export, "export rows"),
        window,
    );
    notify(&app_state, window);

    let frame = capture.frame(window);
    assert!(shows_text(&frame, "prod-pg"), "the target names the row");
    assert!(shows_text(&frame, "Query"), "the kind labels the row");
    assert!(
        !shows_text(&frame, "select * from users"),
        "a targeted row shows target and kind instead of its description"
    );
    assert!(
        shows_text(&frame, "export rows"),
        "a task without a target keeps its description"
    );
}

#[gpui::test]
fn a_cancelling_task_shows_its_badge_its_reason_and_a_disabled_cancel(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let capture = FrameCapture::observe(window);
    let task_id =
        start_acknowledged_query(&app_state, dbflux_core::CancelOutcome::Requested, window);
    window.run_until_parked();
    click_cancel(task_id, window);

    let frame = capture.frame(window);
    assert_eq!(
        status_of(&app_state, task_id, window),
        TaskStatus::Cancelling
    );
    assert!(shows_text(
        &frame,
        &dbflux_i18n::t!("tasks_panel.cancelling_badge")
    ));
    assert!(shows_text(
        &frame,
        &dbflux_i18n::t!("tasks_panel.cancelling_reason_query")
    ));
    assert!(shows_text(
        &frame,
        &dbflux_i18n::t!("tasks_panel.cancelling_count", count = 1)
    ));
    assert!(
        cancel_button_is_disabled(&frame, task_id),
        "no cancel is offered while the worker confirms"
    );
}

#[gpui::test]
fn a_task_past_its_point_of_no_return_shows_why_it_cannot_be_cancelled(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let capture = FrameCapture::observe(window);
    let reason = dbflux_i18n::t!("tasks_panel.drop_point_of_no_return");
    let dropping = start_request(
        &app_state,
        TaskRequest::new(TaskKind::SchemaDrop, "DROP TABLE users"),
        window,
    );
    let plain_key_read = start_task(&app_state, TaskKind::KeyGet, window);
    app_state.update(window, |state, cx| {
        state
            .tasks_mut()
            .mark_point_of_no_return(dropping, reason.clone());
        cx.emit(AppStateChanged);
    });
    window.run_until_parked();

    let frame = capture.frame(window);
    assert!(shows_text(&frame, &reason));
    assert!(cancel_button_is_disabled(&frame, dropping));
    assert!(
        accessibility_node(&frame, &format!("cancel-task-button-{plain_key_read}")).is_none(),
        "a task that never had a cancel keeps none"
    );
}

fn start_connect_with_stages(
    app_state: &Entity<AppStateEntity>,
    window: &mut VisualTestContext,
) -> (TaskId, Vec<TaskId>) {
    let profile_id = add_profile(app_state, "staging", window);
    let parent = start_request(
        app_state,
        TaskRequest::new(TaskKind::Connect, "Connecting to staging").target(target_of(profile_id)),
        window,
    );
    let stages = ["Opening tunnel", "Connecting driver", "Fetching schema"]
        .into_iter()
        .enumerate()
        .map(|(index, description)| {
            let mut request = TaskRequest::new(TaskKind::Connect, description).parent(parent);
            if index > 0 {
                request = request.pending();
            }
            start_request(app_state, request, window)
        })
        .collect();

    (parent, stages)
}

#[gpui::test]
fn stages_render_under_their_parent_and_not_as_top_level_rows(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let capture = FrameCapture::observe(window);
    let (parent, stages) = start_connect_with_stages(&app_state, window);
    notify(&app_state, window);

    let frame = capture.frame(window);
    assert!(accessibility_node(&frame, &format!("task-row-{parent}")).is_some());
    for stage in &stages {
        assert!(
            accessibility_node(&frame, &format!("task-row-{stage}")).is_none(),
            "a stage is not a top-level row"
        );
    }
    for description in ["Opening tunnel", "Connecting driver", "Fetching schema"] {
        assert!(shows_text(&frame, description), "{description} is listed");
    }
    assert!(
        shows_text(&frame, &dbflux_i18n::t!("tasks_panel.stage_waiting")),
        "a stage that has not begun reads Waiting"
    );
    assert!(
        shows_text(&frame, &dbflux_i18n::t!("tasks_panel.running", count = 1)),
        "the running count counts the parent, not its stages"
    );
}

#[gpui::test]
fn a_cancelled_parent_reads_its_stopped_stage_and_the_ones_that_never_started(
    cx: &mut TestAppContext,
) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let capture = FrameCapture::observe(window);
    let (parent, _) = start_connect_with_stages(&app_state, window);
    app_state.update(window, |state, cx| {
        state.tasks_mut().request_cancel(parent);
        cx.emit(AppStateChanged);
    });
    window.run_until_parked();

    let frame = capture.frame(window);
    assert_eq!(status_of(&app_state, parent, window), TaskStatus::Cancelled);
    assert!(shows_text(
        &frame,
        &dbflux_i18n::t!("tasks_panel.stage_not_started")
    ));
    assert!(shows_text(
        &frame,
        &dbflux_i18n::t!("tasks_panel.stage_cancelled")
    ));
    assert!(shows_text(
        &frame,
        &dbflux_i18n::t!(
            "tasks_panel.cancelled_with_stages",
            stage = "Opening tunnel"
        )
    ));
}

#[gpui::test]
fn nested_stages_render_once_under_their_ancestors(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let capture = FrameCapture::observe(window);
    let (_, stages) = start_connect_with_stages(&app_state, window);
    let grandchild = start_request(
        &app_state,
        TaskRequest::new(TaskKind::Connect, "Resolving host").parent(stages[0]),
        window,
    );
    notify(&app_state, window);

    let frame = capture.frame(window);
    assert!(shows_text(&frame, "Opening tunnel"));
    assert!(
        shows_text(&frame, "Resolving host"),
        "a stage of a stage is listed under its parent"
    );
    assert!(
        accessibility_node(&frame, &format!("task-row-{grandchild}")).is_none(),
        "a nested stage is never a top-level row"
    );
}

#[gpui::test]
fn a_cancelled_parent_does_not_claim_later_stages_never_started_when_they_ran(
    cx: &mut TestAppContext,
) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let capture = FrameCapture::observe(window);
    let profile_id = add_profile(&app_state, "staging", window);
    let parent = start_request(
        &app_state,
        TaskRequest::new(TaskKind::Connect, "Connecting to staging").target(target_of(profile_id)),
        window,
    );
    for description in ["Opening tunnel", "Running hooks"] {
        start_request(
            &app_state,
            TaskRequest::new(TaskKind::Connect, description).parent(parent),
            window,
        );
    }
    app_state.update(window, |state, cx| {
        state.tasks_mut().request_cancel(parent);
        cx.emit(AppStateChanged);
    });
    window.run_until_parked();

    let frame = capture.frame(window);
    assert!(shows_text(
        &frame,
        &dbflux_i18n::t!("tasks_panel.cancelled_with_stages")
    ));
    assert!(
        !shows_text(
            &frame,
            &dbflux_i18n::t!(
                "tasks_panel.cancelled_with_stages_stopped",
                stage = "Opening tunnel"
            )
        ),
        "two stages that both ran must not read as 'the stages after it did not start'"
    );
}

#[gpui::test]
fn a_failed_stage_keeps_its_error_visible(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let capture = FrameCapture::observe(window);
    let (_, stages) = start_connect_with_stages(&app_state, window);
    app_state.update(window, |state, cx| {
        state.tasks_mut().fail(stages[0], "connection reset");
        cx.emit(AppStateChanged);
    });
    window.run_until_parked();

    let frame = capture.frame(window);
    assert!(
        shows_text(&frame, "connection reset"),
        "the failure message of a stage is shown under its row"
    );
}

#[gpui::test]
fn a_running_stage_whose_parent_was_removed_stays_a_top_level_row(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let capture = FrameCapture::observe(window);
    let (parent, stages) = start_connect_with_stages(&app_state, window);
    app_state.update(window, |state, cx| {
        state.tasks_mut().remove(parent);
        cx.emit(AppStateChanged);
    });
    window.run_until_parked();

    let listed: Vec<TaskId> = app_state.read_with(window, |state, _| {
        top_level_tasks(state.tasks())
            .into_iter()
            .map(|task| task.id)
            .collect()
    });
    assert_eq!(listed, vec![stages[0]]);
    let frame = capture.frame(window);
    assert!(
        accessibility_node(&frame, &format!("task-row-{}", stages[0])).is_some(),
        "the surviving running stage is a row of its own"
    );
}

#[gpui::test]
fn a_finished_stage_whose_parent_was_removed_is_listed(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let (parent, stages) = start_connect_with_stages(&app_state, window);
    app_state.update(window, |state, _| {
        state.tasks_mut().fail(stages[0], "refused");
        state.tasks_mut().remove(parent);
    });

    let listed: Vec<TaskId> = app_state.read_with(window, |state, _| {
        top_level_tasks(state.tasks())
            .into_iter()
            .map(|task| task.id)
            .collect()
    });
    assert!(listed.contains(&stages[0]));
}

#[gpui::test]
fn clearing_finished_tasks_keeps_a_parent_with_a_stage_in_flight(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (panel, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let (parent, stages) = start_connect_with_stages(&app_state, window);
    let done = start_task(&app_state, TaskKind::Query, window);
    app_state.update(window, |state, _| {
        state.tasks_mut().complete(parent);
        state.tasks_mut().complete(done);
    });

    panel.update(window, |panel, cx| panel.clear_finished(cx));

    app_state.read_with(window, |state, _| {
        assert!(
            state.tasks().get(done).is_none(),
            "plain finished task goes"
        );
        assert!(state.tasks().get(parent).is_some(), "the parent stays");
        assert!(state.tasks().get(stages[0]).is_some());
    });
}

#[gpui::test]
fn dismissing_a_finished_parent_leaves_its_in_flight_stage(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (panel, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let (parent, stages) = start_connect_with_stages(&app_state, window);
    app_state.update(window, |state, _| state.tasks_mut().complete(parent));

    panel.update(window, |panel, cx| panel.dismiss_task(parent, cx));

    app_state.read_with(window, |state, _| {
        assert!(state.tasks().get(parent).is_some());
        assert!(state.tasks().get(stages[0]).is_some());
    });
}

#[gpui::test]
fn a_stage_that_did_not_fail_keeps_its_output_visible(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        move |window, cx| TasksPanel::new(app_state, window, cx)
    });
    let capture = FrameCapture::observe(window);
    let (_, stages) = start_connect_with_stages(&app_state, window);
    app_state.update(window, |state, cx| {
        state
            .tasks_mut()
            .complete_with_details(stages[0], "tunnel listening on 5432");
        cx.emit(AppStateChanged);
    });
    window.run_until_parked();

    let frame = capture.frame(window);
    assert!(
        shows_text(&frame, "tunnel listening on 5432"),
        "the output of a completed stage is shown under its row"
    );
}
