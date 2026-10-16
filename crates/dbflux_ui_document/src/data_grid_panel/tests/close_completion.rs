use super::*;
use crate::data_grid_panel::PendingBatchRemaining;

// === #629 — the completion path a close waits on ===

/// A grid with no connection behind it, for driving the mutation completion
/// path directly, plus every event it emitted.
///
/// The completion path only touches the task slot, the toast host and the
/// document events, so no connection is needed to reach it — which is what
/// makes it testable at all: the run itself is started by a `cx.spawn` the
/// panel does not expose.
fn mutation_completion_panel(
    cx: &mut TestAppContext,
) -> (
    gpui::Entity<DataGridPanel>,
    &mut VisualTestContext,
    Rc<RefCell<Vec<DataGridEvent>>>,
) {
    panel_with_events(cx, vec![], false)
}

/// A grid that can stage edits: it carries the primary key the batch pipeline
/// needs and a loaded result to stage against, plus one row of data so a row
/// edit has something to address.
fn staged_edit_panel(
    cx: &mut TestAppContext,
) -> (
    gpui::Entity<DataGridPanel>,
    &mut VisualTestContext,
    Rc<RefCell<Vec<DataGridEvent>>>,
) {
    panel_with_events(cx, vec!["id".to_string()], true)
}

/// A grid panel in a real window, plus every event it emitted.
///
/// The completion and the batch paths only touch the task slot, the toast host
/// and the document events, so no connection is needed to reach them — which is
/// what makes them testable at all: a run that succeeds needs one, and takes it
/// through `app_state`.
fn panel_with_events(
    cx: &mut TestAppContext,
    pk_columns: Vec<String>,
    with_result: bool,
) -> (
    gpui::Entity<DataGridPanel>,
    &mut VisualTestContext,
    Rc<RefCell<Vec<DataGridEvent>>>,
) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let holder: Rc<RefCell<Option<gpui::Entity<DataGridPanel>>>> = Rc::new(RefCell::new(None));
    let handle = holder.clone();
    let seen: Rc<RefCell<Vec<DataGridEvent>>> = Rc::new(RefCell::new(Vec::new()));
    let sink = seen.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id: Uuid::nil(),
                database: Some("app".to_string()),
                table: TableRef::with_schema("public", "orders"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: None,
            };

            let mut panel =
                DataGridPanel::new_internal(source, app_state.clone(), pk_columns, window, cx);

            if with_result {
                panel.set_result(id_result(), cx);
            }

            panel
        });

        handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = holder.borrow().clone().expect("panel should be created");

    // Subscribed through the app, not the view's own context: the events this
    // test reads are the panel's, and the subscriber is the test itself.
    window.update(|_, cx| {
        cx.subscribe(
            &panel,
            move |_panel: gpui::Entity<DataGridPanel>, event: &DataGridEvent, _| {
                sink.borrow_mut().push(event.clone());
            },
        )
        .detach();
    });

    (panel, window, seen)
}

/// One mutation run, as its completion needs to report it.
fn mutation_run(intent: MutationIntent) -> MutationRun {
    MutationRun {
        task_id: Uuid::new_v4(),
        mode: crate::labels::VisualMutationTaskMode::SingleTransaction,
        table_name: "orders".to_string(),
        intent,
    }
}

fn reported_landing(seen: &Rc<RefCell<Vec<DataGridEvent>>>) -> Option<bool> {
    seen.borrow().iter().find_map(|event| match event {
        DataGridEvent::MutationFinished { landed } => Some(*landed),
        _ => None,
    })
}

fn asked_to_close(seen: &Rc<RefCell<Vec<DataGridEvent>>>) -> bool {
    seen.borrow()
        .iter()
        .any(|event| matches!(event, DataGridEvent::RequestClose))
}

/// What a panel emitted, in a shape a failed assertion can print.
fn event_kinds(seen: &Rc<RefCell<Vec<DataGridEvent>>>) -> Vec<&'static str> {
    seen.borrow()
        .iter()
        .map(|event| match event {
            DataGridEvent::MutationFinished { landed: true } => "finished:landed",
            DataGridEvent::MutationFinished { landed: false } => "finished:not-landed",
            DataGridEvent::RequestClose => "request-close",
            DataGridEvent::Focused => "focused",
            _ => "other",
        })
        .collect()
}

/// A run started from the grid's own apply affordance reports its outcome and
/// leaves the close flow alone: nothing is waiting on it.
#[gpui::test]
fn a_direct_run_reports_landing_without_asking_to_close(cx: &mut TestAppContext) {
    use crate::data_grid_panel::mutation_executor::MutationOutcome;

    let (panel, window, seen) = mutation_completion_panel(cx);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            let failure = panel.finish_mutation(
                mutation_run(MutationIntent::Direct),
                Ok(MutationOutcome::Success { rows_affected: 2 }),
                cx,
            );

            assert!(failure.is_none(), "a successful run reports no failure");
        });
    });

    assert_eq!(reported_landing(&seen), Some(true));
    assert!(
        !asked_to_close(&seen),
        "a run nobody is waiting on must not ask the tab to close"
    );
}

/// A close that is waiting on the apply closes the tab, and only that run
/// asks for it.
#[gpui::test]
fn an_apply_that_landed_asks_the_close_to_proceed(cx: &mut TestAppContext) {
    use crate::data_grid_panel::mutation_executor::MutationOutcome;

    let (panel, window, seen) = mutation_completion_panel(cx);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            let failure = panel.finish_mutation(
                mutation_run(MutationIntent::CloseAfterApply),
                Ok(MutationOutcome::Success { rows_affected: 1 }),
                cx,
            );

            assert!(failure.is_none(), "a successful run reports no failure");
        });
    });

    assert_eq!(reported_landing(&seen), Some(true));
    assert!(
        asked_to_close(&seen),
        "a landed apply must let the tab it was closing go"
    );
}

/// `MutationOutcome::Success { rows_affected: 0 }` is what the executor reports
/// when the statement ran and matched nothing — the row is gone, or its key
/// changed. A close waiting on that apply must not read it as a landed write
/// and close the tab over edits that never reached the database.
#[gpui::test]
fn an_apply_that_matched_no_rows_does_not_ask_the_close_to_proceed(cx: &mut TestAppContext) {
    use crate::data_grid_panel::mutation_executor::MutationOutcome;

    let (panel, window, seen) = mutation_completion_panel(cx);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            let failure = panel.finish_mutation(
                mutation_run(MutationIntent::CloseAfterApply),
                Ok(MutationOutcome::Success { rows_affected: 0 }),
                cx,
            );

            assert!(
                failure.is_none(),
                "a statement that matched nothing is not an execution failure"
            );
        });
    });

    assert_eq!(reported_landing(&seen), Some(false));
    assert!(
        !asked_to_close(&seen),
        "a statement that matched no rows leaves the tab open with its edits"
    );
}

/// A failed apply keeps the tab, reports the cause, and names the table so the
/// report is useful after the grid is gone.
#[gpui::test]
fn a_failed_apply_keeps_the_tab_and_reports_the_cause(cx: &mut TestAppContext) {
    use crate::data_grid_panel::mutation_executor::ExecutorError;

    let (panel, window, seen) = mutation_completion_panel(cx);

    let summary = window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel
                .finish_mutation(
                    mutation_run(MutationIntent::CloseAfterApply),
                    Err(ExecutorError::Transaction("deadlock detected".to_string())),
                    cx,
                )
                .expect("a failed run reports a failure")
                .summary
        })
    });

    assert!(
        summary.contains("orders"),
        "the report names the table: {summary}"
    );
    assert!(
        summary.contains("deadlock detected"),
        "the report carries the cause: {summary}"
    );
    assert_eq!(reported_landing(&seen), Some(false));
    assert!(
        !asked_to_close(&seen),
        "a failed apply must not close the tab"
    );
}

/// The failure report is chosen by the execution mode, so a chunked run's
/// partial-application wording is not lost now that one path serves all three.
#[gpui::test]
fn a_chunked_failure_reports_through_the_chunked_label(cx: &mut TestAppContext) {
    use crate::data_grid_panel::mutation_executor::ExecutorError;

    let (panel, window, _) = mutation_completion_panel(cx);

    let (chunked, single) = window.update(|_, app| {
        panel.update(app, |panel, cx| {
            let mut chunked_run = mutation_run(MutationIntent::Direct);
            chunked_run.mode = crate::labels::VisualMutationTaskMode::Chunked;
            let chunked = panel
                .finish_mutation(
                    chunked_run,
                    Err(ExecutorError::Transaction("chunk 4 failed".to_string())),
                    cx,
                )
                .expect("a failed run reports a failure")
                .summary;

            let single = panel
                .finish_mutation(
                    mutation_run(MutationIntent::Direct),
                    Err(ExecutorError::Transaction("chunk 4 failed".to_string())),
                    cx,
                )
                .expect("a failed run reports a failure")
                .summary;

            (chunked, single)
        })
    });

    assert_ne!(
        chunked, single,
        "a chunked run reports through its own label"
    );
    assert!(chunked.contains("chunk 4 failed"));
}

/// A cancelled apply is not a landed one: the tab keeps its edits.
#[gpui::test]
fn a_cancelled_apply_does_not_ask_the_close_to_proceed(cx: &mut TestAppContext) {
    use crate::data_grid_panel::mutation_executor::MutationOutcome;

    let (panel, window, seen) = mutation_completion_panel(cx);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            let failure = panel.finish_mutation(
                mutation_run(MutationIntent::CloseAfterApply),
                Ok(MutationOutcome::Cancelled { rows_affected: 0 }),
                cx,
            );

            assert!(failure.is_none(), "a cancellation is not a failure");
        });
    });

    assert_eq!(reported_landing(&seen), Some(false));
    assert!(
        !asked_to_close(&seen),
        "a cancelled apply leaves the tab open with its edits"
    );
}

// === #629 — the close that waits on the staged edits ===

/// Stages one edited cell in the first row, which is what the grid's own
/// "save all" action applies.
fn stage_row_edit(panel: &mut DataGridPanel, cx: &mut gpui::Context<DataGridPanel>) {
    let table_state = panel
        .grid_table
        .table_state
        .clone()
        .expect("a table source builds a table state");

    table_state.update(cx, |state, cx| {
        state.edit_buffer_mut().set_cell(
            0,
            0,
            dbflux_components::components::data_table::model::CellValue::int(7),
        );
        cx.notify();
    });
}

/// Stages one deleted row, which is the batch's other shape: it is parked on
/// the delete confirmation rather than pumped one operation at a time.
fn stage_row_delete(panel: &mut DataGridPanel, cx: &mut gpui::Context<DataGridPanel>) {
    let table_state = panel
        .grid_table
        .table_state
        .clone()
        .expect("a table source builds a table state");

    table_state.update(cx, |state, cx| {
        state.edit_buffer_mut().mark_for_delete(0);
        cx.notify();
    });
}

/// Nothing staged means nothing to apply, so the caller may close the tab.
#[gpui::test]
fn a_grid_with_nothing_staged_lets_the_close_proceed(cx: &mut TestAppContext) {
    let (panel, window, seen) = staged_edit_panel(cx);

    let started = window.update(|_, app| panel.update(app, |panel, cx| panel.apply_for_close(cx)));

    assert!(!started, "a clean grid has no apply to wait on");
    assert_eq!(reported_landing(&seen), None);
    assert!(!asked_to_close(&seen));
}

/// The staged edit never reaches the database, so the apply did not land: the
/// tab keeps its edits instead of closing over them.
#[gpui::test]
fn an_apply_that_did_not_reach_the_database_keeps_the_tab(cx: &mut TestAppContext) {
    let (panel, window, seen) = staged_edit_panel(cx);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            stage_row_edit(panel, cx);
            assert!(
                panel.apply_for_close(cx),
                "a grid with a staged row has an apply to wait on"
            );
        });
    });

    // The write runs on the background executor.
    window.run_until_parked();

    assert_eq!(reported_landing(&seen), Some(false));
    assert!(
        !asked_to_close(&seen),
        "an apply that did not land must not take the tab away"
    );
}

/// Registers a connection the batch can write through, under the profile the
/// test panels are built against.
///
/// Registered after the panel exists because the batch reads the connection
/// when the write runs, not when the grid is built.
fn register_writable_connection(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) {
    let _driver = register_writable_connection_with_driver(panel, window);
}

/// The same connection, with its driver kept so a test can count what ran.
fn register_writable_connection_with_driver(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> dbflux_test_support::fake_driver::FakeDriver {
    use dbflux_core::{ConnectedProfile, DbConfig, DbKind, MutationPolicy};
    use dbflux_test_support::fake_driver::FakeDriver;
    use std::path::PathBuf;

    let driver = FakeDriver::new(DbKind::SQLite);

    window.update(|_, app| {
        let app_state = panel.read(app).app_state.clone();

        app_state.update(app, |state, _| {
            let profile = dbflux_core::ConnectionProfile::new(
                "staged-edits",
                DbConfig::SQLite {
                    path: PathBuf::from(":memory:"),
                    connection_id: None,
                },
            );
            let connection = driver
                .connect_arc(&profile)
                .expect("the fake driver connects");

            state.connections_mut().insert(
                Uuid::nil(),
                ConnectedProfile {
                    profile,
                    connection,
                    schema: None,
                    mutation_policy: MutationPolicy::Allowed,
                    read_only_reason: None,
                    database_schemas: Default::default(),
                    table_details: Default::default(),
                    collection_children: Default::default(),
                    schema_types: Default::default(),
                    schema_columns: Default::default(),
                    schema_indexes: Default::default(),
                    schema_foreign_keys: Default::default(),
                    schema_routines: Default::default(),
                    dependents_cache: Default::default(),
                    active_database: None,
                    redis_key_cache: Default::default(),
                    database_connections: Default::default(),
                    proxy_tunnel: None,
                },
            );
        });
    });

    driver
}

/// One staged row edit that is what lets the close through.
#[gpui::test]
fn a_staged_edit_that_lands_asks_the_close_to_proceed(cx: &mut TestAppContext) {
    let (panel, window, seen) = staged_edit_panel(cx);
    register_writable_connection(&panel, window);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            stage_row_edit(panel, cx);
            assert!(
                panel.apply_for_close(cx),
                "a grid with a staged row has an apply to wait on"
            );
        });
    });

    window.run_until_parked();

    assert_eq!(reported_landing(&seen), Some(true));
    assert!(
        asked_to_close(&seen),
        "a landed apply must let the tab it was closing go"
    );
}

/// A batch of deletes takes its own tail: it parks on the delete
/// confirmation instead of staging remaining work, so the pump never runs and
/// the completion has to be reported by the delete's own success path.
#[gpui::test]
fn a_delete_only_apply_that_lands_asks_the_close_to_proceed(cx: &mut TestAppContext) {
    let (panel, window, seen) = staged_edit_panel(cx);
    register_writable_connection(&panel, window);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            stage_row_delete(panel, cx);
            let counts = panel.pending_edit_counts(cx);
            assert_eq!(counts.2, 1, "one staged delete, got {counts:?}");
            assert!(
                panel.apply_for_close(cx),
                "a grid with a staged delete has an apply to wait on"
            );
        });
    });

    // Applying reports through `cx.emit`, which this gpui queues: the batch is
    // only visible to the panel once the deferred effects are flushed.
    window.run_until_parked();

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            assert!(
                panel.has_delete_confirm(),
                "a table delete parks the batch on its confirmation"
            );
            panel.confirm_delete(window, cx);
        });
    });

    window.run_until_parked();

    assert_eq!(
        reported_landing(&seen),
        Some(true),
        "a landed delete must report it; emitted {:?}",
        event_kinds(&seen)
    );
    assert!(
        asked_to_close(&seen),
        "a landed delete must let the tab it was closing go"
    );
}

/// A close request must not re-enter the batch pump while an operation is
/// already in flight.
///
/// While the pump writes one row, the rows it has not started yet stay in
/// `pending_batch_remaining`. A second `apply_for_close` used to call the pump
/// again, starting a second write at the same time; the operation that landed
/// first could then report the close before the other reached the database.
///
/// The fake driver answers immediately, so a real operation cannot be held in
/// flight long enough to observe. The test stages the pump's in-flight shape
/// directly instead: one edit still to write, and the held row waiting.
#[gpui::test]
fn apply_for_close_does_not_start_a_second_batch_operation(cx: &mut TestAppContext) {
    let (panel, window, _seen) = staged_edit_panel(cx);
    register_writable_connection(&panel, window);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            stage_row_edit(panel, cx);
            panel.pending_batch_remaining = Some(PendingBatchRemaining {
                pending_inserts: Vec::new(),
                dirty_rows: vec![0],
            });

            assert!(
                panel.apply_for_close(cx),
                "a staged row with a batch in flight is an apply to wait on"
            );
            assert!(
                panel.pending_batch_remaining.is_some(),
                "the held row must still be waiting when the close request returns"
            );
        });
    });
}

/// A close request while a write is in flight must not ask the table to save
/// again.
///
/// The pump sets `batch_op_in_flight` when it has a write running; the close
/// returns without dispatching a Save All. The fake driver answers at once, so
/// the in-flight state is staged directly instead of holding a real operation.
#[gpui::test]
fn apply_for_close_while_an_operation_is_in_flight_does_not_save_all_again(
    cx: &mut TestAppContext,
) {
    let (panel, window, _seen) = staged_edit_panel(cx);
    register_writable_connection(&panel, window);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            stage_row_edit(panel, cx);
            panel.batch_op_in_flight = true;

            assert!(
                panel.apply_for_close(cx),
                "a write in flight is an apply to wait on"
            );
        });
    });

    // Nothing was asked to save, so the staged row is still pending.
    window.run_until_parked();

    let counts = window.update(|_, app| panel.read(app).pending_edit_counts(app));
    assert_eq!(
        counts,
        (0, 1, 0),
        "the close must not dispatch a second Save All while a write is in flight"
    );
}

/// A close after a batch operation failed must resume or report, not park.
///
/// The failed insert abandons the batch; the rows it still held must not look
/// like a write in flight to the next close.
#[gpui::test]
fn a_close_after_a_failed_batch_reports_back(cx: &mut TestAppContext) {
    // No connection is registered, so the first insert fails with "No connection".
    let (panel, window, seen) = staged_edit_panel(cx);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            let table_state = panel.grid_table.table_state.clone().expect("table state");
            table_state.update(cx, |state, cx| {
                state.edit_buffer_mut().add_pending_insert(vec![
                    dbflux_components::components::data_table::model::CellValue::int(1),
                ]);
                state.edit_buffer_mut().add_pending_insert(vec![
                    dbflux_components::components::data_table::model::CellValue::int(2),
                ]);
                cx.notify();
            });
            assert!(
                panel.apply_for_close(cx),
                "two staged inserts are an apply to wait on"
            );
        });
    });
    window.run_until_parked();

    // Drop the first attempt's `landed: false`.
    seen.borrow_mut().clear();

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            assert!(
                panel.apply_for_close(cx),
                "the remaining insert is still an apply to wait on"
            );
        });
    });
    window.run_until_parked();

    assert!(
        !seen.borrow().is_empty(),
        "the second close must resume or report, not park; got {:?}",
        event_kinds(&seen)
    );
}

/// A queued row whose edits were cleared before the pump reached it must not
/// park the close.
///
/// The pump marked it in flight and the save handler then returned with nothing
/// to write, leaving no completion to advance or finish the batch.
#[gpui::test]
fn a_queued_row_that_lost_its_edits_does_not_park_the_close(cx: &mut TestAppContext) {
    let (panel, window, seen) = staged_edit_panel(cx);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            // A close waiting on a batch whose one queued row has no edits left.
            panel.close_after_apply = true;
            panel.pending_batch_remaining = Some(PendingBatchRemaining {
                pending_inserts: Vec::new(),
                dirty_rows: vec![0],
            });
            panel.process_next_batch_op(cx);
        });
    });

    assert!(
        asked_to_close(&seen),
        "a queued row with no edits must let the close through; got {:?}",
        event_kinds(&seen)
    );
}

/// A batch of pending inserts must save every one of them.
///
/// The batch pumps positional indices from a snapshot, and each landed insert is
/// removed from the edit buffer, which shifts the indices the pump still holds.
#[gpui::test]
fn a_batch_of_inserts_saves_every_row(cx: &mut TestAppContext) {
    let (panel, window, seen) = staged_edit_panel(cx);
    let driver = register_writable_connection_with_driver(&panel, window);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            let table_state = panel.grid_table.table_state.clone().expect("table state");
            table_state.update(cx, |state, cx| {
                state.edit_buffer_mut().add_pending_insert(vec![
                    dbflux_components::components::data_table::model::CellValue::int(1),
                ]);
                state.edit_buffer_mut().add_pending_insert(vec![
                    dbflux_components::components::data_table::model::CellValue::int(2),
                ]);
                cx.notify();
            });
            assert!(
                panel.apply_for_close(cx),
                "two staged inserts are an apply to wait on"
            );
        });
    });
    window.run_until_parked();

    let inserted: Vec<i64> = driver
        .stats()
        .crud_ops
        .iter()
        .filter_map(|op| match op {
            dbflux_test_support::fake_driver::CrudOp::Insert(insert) => insert
                .assignments
                .iter()
                .find_map(|assignment| match assignment.value {
                    dbflux_core::Value::Int(value) => Some(value),
                    _ => None,
                }),
            _ => None,
        })
        .collect();
    assert_eq!(
        inserted,
        vec![1, 2],
        "both staged inserts must reach the driver, in order"
    );
    assert!(
        asked_to_close(&seen),
        "the batch must let the close through; got {:?}",
        event_kinds(&seen)
    );
}
