use super::*;
use dbflux_test_support::fake_driver::{CrudOp, FakeDriver};

// === The grid after a bulk delete that did not fully land ===

/// A keyed `orders` grid holding rows with ids 1, 2 and 3, in a window that is
/// never drawn, so a queued reload stays observable instead of running.
fn three_row_panel(
    cx: &mut TestAppContext,
) -> (
    gpui::Entity<DataGridPanel>,
    &mut VisualTestContext,
    Rc<RefCell<Vec<DataGridEvent>>>,
) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let seen: Rc<RefCell<Vec<DataGridEvent>>> = Rc::new(RefCell::new(Vec::new()));
    let sink = seen.clone();

    let panel = window.update(|window, app| {
        let source = DataSource::Table {
            profile_id: Uuid::nil(),
            database: Some("app".to_string()),
            table: TableRef::with_schema("public", "orders"),
            pagination: Pagination::default(),
            order_by: Vec::new(),
            total_rows: Some(3),
        };

        let panel = app.new(|cx| {
            DataGridPanel::new_internal(source, app_state, vec!["id".to_string()], window, cx)
        });
        panel.update(app, |panel, cx| {
            panel.set_result(
                QueryResult::table(
                    vec![key_column("id", true)],
                    (1..=3)
                        .map(|id| vec![dbflux_core::Value::Int(id)])
                        .collect(),
                    None,
                    Duration::ZERO,
                ),
                cx,
            );
        });

        app.subscribe(
            &panel,
            move |_panel: gpui::Entity<DataGridPanel>, event: &DataGridEvent, _| {
                sink.borrow_mut().push(event.clone());
            },
        )
        .detach();

        panel
    });

    (panel, window, seen)
}

fn mark_rows_for_delete(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
    rows: &[usize],
) {
    window.update(|_, app| {
        let table_state = panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("a table");
        table_state.update(app, |state, _| {
            for &row in rows {
                state.edit_buffer_mut().mark_for_delete(row);
            }
        });
    });
}

fn commit_deletes(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
    rows: Vec<usize>,
) {
    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.commit_bulk_deletes_table(
                Uuid::nil(),
                Some("app".to_string()),
                TableRef::with_schema("public", "orders"),
                rows,
                cx,
            );
        });
    });
    window.run_until_parked();
}

fn rows_marked_for_delete(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> Vec<usize> {
    window.update(|_, app| {
        let table_state = panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("a table");
        let mut rows = table_state.read(app).edit_buffer().pending_delete_rows();
        rows.sort_unstable();
        rows
    })
}

fn deleted_ids(driver: &FakeDriver) -> Vec<dbflux_core::Value> {
    driver
        .stats()
        .crud_ops
        .iter()
        .filter_map(|op| match op {
            CrudOp::Delete(delete) => match &delete.identity {
                dbflux_core::RecordIdentity::Composite { values, .. } => values.first().cloned(),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

fn reload_keeping_edits_is_queued(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> bool {
    window.update(|_, app| {
        let panel = panel.read(app);
        panel.pending.refresh && panel.pending.refresh_keeps_edits
    })
}

/// Row 1 is deleted, row 2 is refused and row 3 is never reached. The reload
/// drops row 1 from the grid and carries the marks of rows 2 and 3 over by
/// primary key, so saving again does not retry a row that is already gone.
#[gpui::test]
fn a_partially_failed_delete_reloads_and_keeps_the_rows_that_were_not_deleted_marked(
    cx: &mut TestAppContext,
) {
    let (panel, window, seen) = three_row_panel(cx);
    // The reload reads what is left in the table: rows 2 and 3.
    let driver = FakeDriver::new(dbflux_core::DbKind::SQLite)
        .with_default_result(QueryResult::table(
            vec![key_column("id", true)],
            (2..=3)
                .map(|id| vec![dbflux_core::Value::Int(id)])
                .collect(),
            None,
            Duration::ZERO,
        ))
        .with_delete_row_error(
            dbflux_core::Value::Int(2),
            "violates foreign key constraint",
        );
    register_fake_connection(&panel, window, &driver);

    mark_rows_for_delete(&panel, window, &[0, 1, 2]);
    commit_deletes(&panel, window, vec![0, 1, 2]);

    assert_eq!(deleted_ids(&driver), vec![dbflux_core::Value::Int(1)]);
    assert!(
        reload_keeping_edits_is_queued(&panel, window),
        "the deleted row must leave the grid through a reload that keeps the other marks"
    );
    assert_eq!(
        rows_marked_for_delete(&panel, window),
        vec![1, 2],
        "the rows that were not deleted must stay marked"
    );
    assert_eq!(
        last_toast_title(window),
        Some(crate::labels::partial_delete_label(
            crate::labels::MutationItemKind::Row,
            1,
            3,
            &dbflux_core::DbError::query_failed("violates foreign key constraint").to_string(),
        ))
    );
    assert!(
        !window.update(|_, app| panel.read(app).batch_op_in_flight),
        "a failed batch must not look like a write still in flight"
    );

    window.update(|window, app| {
        panel.update(app, |panel, cx| panel.process_pending_actions(window, cx));
    });
    window.run_until_parked();

    window.update(|_, app| {
        assert_eq!(
            panel.read(app).result.rows,
            vec![
                vec![dbflux_core::Value::Int(2)],
                vec![dbflux_core::Value::Int(3)],
            ],
            "the deleted row must leave the grid"
        );
    });
    assert_eq!(
        rows_marked_for_delete(&panel, window),
        vec![0, 1],
        "the marks follow rows 2 and 3 to their new positions"
    );
    assert!(
        !seen
            .borrow()
            .iter()
            .any(|event| matches!(event, DataGridEvent::RequestClose))
    );
}

/// Nothing was deleted before the failure, so there is nothing to drop from
/// the grid and every mark stays where it was.
#[gpui::test]
fn a_delete_that_fails_on_its_first_row_does_not_reload(cx: &mut TestAppContext) {
    let (panel, window, _seen) = three_row_panel(cx);
    let driver = FakeDriver::new(dbflux_core::DbKind::SQLite)
        .with_delete_row_error(dbflux_core::Value::Int(1), "locked");
    register_fake_connection(&panel, window, &driver);

    mark_rows_for_delete(&panel, window, &[0, 1]);
    commit_deletes(&panel, window, vec![0, 1]);

    assert!(deleted_ids(&driver).is_empty());
    assert!(!reload_keeping_edits_is_queued(&panel, window));
    assert_eq!(rows_marked_for_delete(&panel, window), vec![0, 1]);
}

/// A row whose primary key cannot be read is not deleted. The user is told
/// how many rows were left out, and a close waiting on the apply keeps the
/// tab, because those rows are still staged.
#[gpui::test]
fn a_row_without_a_readable_primary_key_is_reported_and_keeps_the_tab(cx: &mut TestAppContext) {
    let (panel, window, seen) = three_row_panel(cx);
    let driver = FakeDriver::new(dbflux_core::DbKind::SQLite);
    register_fake_connection(&panel, window, &driver);

    window.update(|_, app| panel.update(app, |panel, _| panel.close_after_apply = true));
    // Row 1 loses its key cell, so no primary key value can be read for it.
    window.update(|_, app| {
        let table_state = panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("a table");
        table_state.update(app, |state, _| state.apply_returning_row(1, &[]));
    });
    mark_rows_for_delete(&panel, window, &[0, 1]);
    commit_deletes(&panel, window, vec![0, 1]);

    assert_eq!(deleted_ids(&driver), vec![dbflux_core::Value::Int(1)]);
    assert_eq!(
        rows_marked_for_delete(&panel, window),
        vec![1],
        "a row that was not deleted stays marked"
    );
    assert_eq!(
        last_toast_title(window),
        Some(crate::labels::bulk_delete_unidentified_label(
            crate::labels::MutationItemKind::Row,
            1,
            1,
        ))
    );
    let events = seen.borrow();
    assert!(
        events
            .iter()
            .any(|event| matches!(event, DataGridEvent::MutationFinished { landed: false })),
        "a delete that left rows out did not land"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, DataGridEvent::RequestClose)),
        "the tab must stay open over rows that were not deleted"
    );
}
