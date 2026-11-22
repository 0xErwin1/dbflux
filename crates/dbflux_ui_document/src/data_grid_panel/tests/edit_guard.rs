//! The key check a save, delete, Save All or chunked mutation makes before it
//! sends anything (`data_grid_panel::edit_guard`).

use super::*;
use crate::data_grid_panel::PendingBatchRemaining;
use crate::data_grid_panel::PendingDeleteConfirm;
use crate::data_grid_panel::PendingMutationExec;
use crate::data_grid_panel::mutation_executor::MutationExecOptions;
use dbflux_components::components::data_table::model::CellValue;
use dbflux_core::{ColumnInfo, TableInfo};
use dbflux_test_support::fake_driver::{CrudOp, FakeDriver, FakeTableDetails};

fn orders_info(key: &[&str], columns: &[&str]) -> TableInfo {
    let column = |name: &str| ColumnInfo {
        name: name.to_string(),
        type_name: "text".to_string(),
        nullable: false,
        is_primary_key: key.contains(&name),
        default_value: None,
        enum_values: None,
    };

    TableInfo {
        name: "orders".to_string(),
        schema: Some("public".to_string()),
        columns: Some(columns.iter().map(|name| column(name)).collect()),
        indexes: None,
        foreign_keys: None,
        constraints: None,
        sample_fields: None,
        presentation: Default::default(),
        child_items: None,
        storage_hints: None,
        pseudo_columns: Box::default(),
    }
}

fn changed_key() -> FakeTableDetails {
    FakeTableDetails::Info(Box::new(orders_info(&["code"], &["id", "code", "name"])))
}

struct Fixture {
    panel: gpui::Entity<DataGridPanel>,
    driver: FakeDriver,
    seen: Rc<RefCell<Vec<DataGridEvent>>>,
}

/// An editable `public.orders` grid keyed by `id` over three rows, connected
/// to a fake whose `table_details` answers `fresh`. The cache holds the key
/// the grid opened with.
fn fixture(
    cx: &mut TestAppContext,
    fresh: FakeTableDetails,
) -> (Fixture, &mut gpui::VisualTestContext) {
    fixture_for(cx, fresh, dbflux_core::DbKind::SQLite)
}

fn fixture_for(
    cx: &mut TestAppContext,
    fresh: FakeTableDetails,
    kind: dbflux_core::DbKind,
) -> (Fixture, &mut gpui::VisualTestContext) {
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
                    vec![key_column("id", true), key_column("name", false)],
                    (1..=3)
                        .map(|id| {
                            vec![
                                dbflux_core::Value::Int(id),
                                dbflux_core::Value::Text(format!("order {id}")),
                            ]
                        })
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

    let driver = FakeDriver::new(kind).with_table_details("orders", fresh);
    register_fake_connection(&panel, window, &driver);

    window.update(|_, app| {
        let app_state = panel.read(app).app_state.clone();
        app_state.update(app, |state, _| {
            state.set_table_details(
                Uuid::nil(),
                "app".to_string(),
                Some("public".to_string()),
                "orders".to_string(),
                orders_info(&["id"], &["id", "name"]),
            );
        });
    });

    (
        Fixture {
            panel,
            driver,
            seen,
        },
        window,
    )
}

fn stage_name(fixture: &Fixture, window: &mut VisualTestContext, row: usize) {
    window.update(|_, app| {
        let table_state = fixture
            .panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("a table");
        table_state.update(app, |state, _| {
            state.stage_cell_value(row, 1, CellValue::text("renamed"));
        });
    });
}

fn stage_insert(fixture: &Fixture, window: &mut VisualTestContext, id: i64, name: &str) {
    window.update(|_, app| {
        let table_state = fixture
            .panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("a table");
        table_state.update(app, |state, _| {
            state
                .edit_buffer_mut()
                .add_pending_insert(vec![CellValue::int(id), CellValue::text(name)]);
        });
    });
}

fn mark_for_delete(fixture: &Fixture, window: &mut VisualTestContext, rows: &[usize]) {
    window.update(|_, app| {
        let table_state = fixture
            .panel
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

fn act(
    fixture: &Fixture,
    window: &mut VisualTestContext,
    action: impl FnOnce(&mut DataGridPanel, &mut gpui::Window, &mut gpui::Context<DataGridPanel>),
) {
    window.update(|window, app| {
        fixture
            .panel
            .update(app, |panel, cx| action(panel, window, cx));
    });
    window.run_until_parked();
}

fn writes(driver: &FakeDriver) -> Vec<CrudOp> {
    driver.stats().crud_ops
}

fn modal_visible(fixture: &Fixture, window: &mut VisualTestContext) -> bool {
    window.update(|_, app| fixture.panel.read(app).has_edit_guard_modal(app))
}

fn pending_counts(fixture: &Fixture, window: &mut VisualTestContext) -> (usize, usize, usize) {
    window.update(|_, app| fixture.panel.read(app).pending_edit_counts(app))
}

fn cached_columns(fixture: &Fixture, window: &mut VisualTestContext) -> Vec<String> {
    window.update(|_, app| {
        fixture
            .panel
            .read(app)
            .app_state
            .read(app)
            .get_table_details(Uuid::nil(), "app", Some("public"), "orders")
            .and_then(|details| details.columns.clone())
            .unwrap_or_default()
            .into_iter()
            .map(|column| column.name)
            .collect()
    })
}

fn modal_click(fixture: &Fixture, window: &mut VisualTestContext, reload: bool) {
    window.update(|_, app| {
        let modal = fixture.panel.read(app).edit_guard.modal.clone();
        modal.update(app, |modal, cx| {
            if reload {
                modal.refresh(cx);
            } else {
                modal.dismiss(cx);
            }
        });
    });
    window.run_until_parked();
}

fn chunked_delete(fixture: &Fixture, window: &mut VisualTestContext) {
    act(fixture, window, |panel, window, cx| {
        panel.pending_mutation_exec = Some(PendingMutationExec {
            #[cfg(feature = "telemetry")]
            usage: None,
            spec: dbflux_core::VisualMutationSpec {
                from: TableRef::with_schema("public", "orders"),
                filter: None,
                kind: dbflux_core::MutationKind::Delete,
            },
            opts: MutationExecOptions::chunked(1_000),
            profile_id: Uuid::nil(),
            intent: crate::data_grid_panel::MutationIntent::Direct,
        });
        panel.handle_mutation_confirm_outcome(
            dbflux_components::modals::MutationConfirmOutcome::Confirmed,
            window,
            cx,
        );
    });
}

fn mutation_finished(fixture: &Fixture) -> bool {
    fixture
        .seen
        .borrow()
        .iter()
        .any(|event| matches!(event, DataGridEvent::MutationFinished { .. }))
}

// === A changed key stops every kind of write ===

#[gpui::test]
fn a_save_against_a_changed_key_opens_the_modal_and_sends_nothing(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, changed_key());
    stage_name(&fixture, window, 0);

    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_row(0, false, cx)
    });

    assert!(writes(&fixture.driver).is_empty(), "no UPDATE was sent");
    assert!(modal_visible(&fixture, window));
    assert_eq!(
        pending_counts(&fixture, window),
        (0, 1, 0),
        "edits stay staged"
    );
    assert_eq!(
        cached_columns(&fixture, window),
        vec!["id".to_string(), "name".to_string()],
        "a blocked write leaves the cache for Reload to replace"
    );
}

#[gpui::test]
fn a_single_delete_against_a_changed_key_sends_nothing(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, changed_key());
    mark_for_delete(&fixture, window, &[0]);

    act(&fixture, window, |panel, window, cx| {
        panel.handle_commit_delete(0, cx);
        panel.confirm_delete(window, cx);
    });

    assert!(writes(&fixture.driver).is_empty(), "no DELETE was sent");
    assert!(modal_visible(&fixture, window));
    assert_eq!(pending_counts(&fixture, window), (0, 0, 1));
}

#[gpui::test]
fn a_bulk_delete_against_a_changed_key_checks_once_and_sends_nothing(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, changed_key());
    mark_for_delete(&fixture, window, &[0, 1, 2]);

    act(&fixture, window, |panel, window, cx| {
        panel.pending_delete_confirm = Some(PendingDeleteConfirm {
            row_indices: vec![0, 1, 2],
            is_table: true,
        });
        panel.confirm_delete(window, cx);
    });

    assert!(writes(&fixture.driver).is_empty());
    assert!(modal_visible(&fixture, window));
    assert_eq!(fixture.driver.table_details_calls(), 1, "once per batch");
}

#[gpui::test]
fn save_all_against_a_changed_key_checks_once_and_sends_nothing(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, changed_key());
    stage_name(&fixture, window, 0);
    stage_name(&fixture, window, 1);

    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_all(Vec::new(), Vec::new(), vec![0, 1], cx)
    });

    assert!(writes(&fixture.driver).is_empty());
    assert!(modal_visible(&fixture, window));
    assert_eq!(fixture.driver.table_details_calls(), 1, "once per batch");
    assert!(
        !window.update(|_, app| fixture.panel.read(app).batch_op_in_flight),
        "a stopped batch must not look like a write in flight"
    );
}

#[gpui::test]
fn a_chunked_mutation_against_a_changed_key_does_not_start(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, changed_key());

    chunked_delete(&fixture, window);

    assert!(modal_visible(&fixture, window));
    assert!(
        fixture.driver.stats().executed_requests.is_empty(),
        "no statement reached the connection"
    );
    assert!(!mutation_finished(&fixture), "the run never started");
}

#[gpui::test]
fn a_table_that_is_gone_stops_the_write_and_opens_the_modal(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, FakeTableDetails::NotFound);
    stage_name(&fixture, window, 0);

    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_row(0, false, cx)
    });

    assert!(writes(&fixture.driver).is_empty());
    assert!(modal_visible(&fixture, window));
    assert_eq!(pending_counts(&fixture, window), (0, 1, 0));
}

#[gpui::test]
fn a_failed_check_sends_nothing_reports_it_and_keeps_the_edits(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, FakeTableDetails::Failure("network down".to_string()));
    stage_name(&fixture, window, 0);

    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_row(0, false, cx)
    });

    assert!(writes(&fixture.driver).is_empty());
    assert!(!modal_visible(&fixture, window), "this is not a drift");
    assert_eq!(pending_counts(&fixture, window), (0, 1, 0));
    assert_eq!(
        last_toast_title(window),
        Some(crate::labels::edit_guard_check_failed_error(
            &dbflux_core::DbError::connection_failed("network down").to_string()
        ))
    );
}

// === An unchanged key lets the write go on, as before ===

#[gpui::test]
fn a_save_with_the_same_key_writes_and_refreshes_the_cache(cx: &mut TestAppContext) {
    let fresh = FakeTableDetails::Info(Box::new(orders_info(
        &["id"],
        &["id", "name", "created_at"],
    )));
    let (fixture, window) = fixture(cx, fresh);
    stage_name(&fixture, window, 0);

    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_row(0, false, cx)
    });

    assert!(matches!(
        writes(&fixture.driver).as_slice(),
        [CrudOp::Update(_)]
    ));
    assert!(!modal_visible(&fixture, window));
    assert_eq!(
        cached_columns(&fixture, window),
        vec![
            "id".to_string(),
            "name".to_string(),
            "created_at".to_string()
        ],
        "the fresh details replace the cached ones"
    );
}

#[gpui::test]
fn a_bulk_delete_with_the_same_key_checks_once_and_deletes_every_row(cx: &mut TestAppContext) {
    let fresh = FakeTableDetails::Info(Box::new(orders_info(&["id"], &["id", "name"])));
    let (fixture, window) = fixture(cx, fresh);
    mark_for_delete(&fixture, window, &[0, 1, 2]);

    act(&fixture, window, |panel, window, cx| {
        panel.pending_delete_confirm = Some(PendingDeleteConfirm {
            row_indices: vec![0, 1, 2],
            is_table: true,
        });
        panel.confirm_delete(window, cx);
    });

    let deletes = writes(&fixture.driver)
        .into_iter()
        .filter(|op| matches!(op, CrudOp::Delete(_)))
        .count();
    assert_eq!(deletes, 3);
    assert_eq!(fixture.driver.table_details_calls(), 1);
}

#[gpui::test]
fn save_all_with_the_same_key_checks_once_and_saves_every_row(cx: &mut TestAppContext) {
    let fresh = FakeTableDetails::Info(Box::new(orders_info(&["id"], &["id", "name"])));
    let (fixture, window) = fixture(cx, fresh);
    stage_name(&fixture, window, 0);
    stage_name(&fixture, window, 1);

    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_all(Vec::new(), Vec::new(), vec![0, 1], cx)
    });

    let updates = writes(&fixture.driver)
        .into_iter()
        .filter(|op| matches!(op, CrudOp::Update(_)))
        .count();
    assert_eq!(updates, 2);
    assert_eq!(fixture.driver.table_details_calls(), 1);
}

#[gpui::test]
fn a_chunked_mutation_with_the_same_key_starts(cx: &mut TestAppContext) {
    let fresh = FakeTableDetails::Info(Box::new(orders_info(&["id"], &["id", "name"])));
    let (fixture, window) = fixture(cx, fresh);

    chunked_delete(&fixture, window);

    assert!(!modal_visible(&fixture, window));
    assert!(mutation_finished(&fixture), "the run started and finished");
    assert!(
        !fixture.driver.stats().executed_requests.is_empty(),
        "the chunked statement reached the driver"
    );
}

/// A builder result names its key through the binding, which is what the
/// check compares with. The cache and the fresh read both key `orders` by
/// `code`, so the write goes through only if the grid took `code` from the
/// binding and not from the `id` it was opened with.
#[gpui::test]
fn a_builder_result_is_checked_against_its_binding_key(cx: &mut TestAppContext) {
    let keyed_by_code = orders_info(&["code"], &["id", "code", "name"]);
    let (fixture, window) = fixture(cx, FakeTableDetails::Info(Box::new(keyed_by_code.clone())));

    window.update(|_, app| {
        let app_state = fixture.panel.read(app).app_state.clone();
        app_state.update(app, |state, _| {
            state.set_table_details(
                Uuid::nil(),
                "app".to_string(),
                Some("public".to_string()),
                "orders".to_string(),
                keyed_by_code,
            );
        });
        fixture.panel.update(app, |panel, cx| {
            let spec = VisualQuerySpec {
                source: SourceTable {
                    schema: Some("public".to_string()),
                    table: "orders".to_string(),
                    alias: "orders".to_string(),
                },
                ..make_test_spec()
            };
            let binding = panel.compute_builder_binding(Some(&spec), Uuid::nil(), Some("app"), cx);
            panel.builder.current_visual_spec = Some(spec);
            panel.pk_columns = binding
                .as_ref()
                .map(|binding| binding.pk_columns.clone())
                .unwrap_or_default();
            panel.builder.builder_editable_binding = binding;
            panel.set_result(
                QueryResult::table(
                    vec![
                        key_column("id", false),
                        key_column("code", true),
                        key_column("name", false),
                    ],
                    vec![vec![
                        dbflux_core::Value::Int(1),
                        dbflux_core::Value::Text("c-1".to_string()),
                        dbflux_core::Value::Text("order 1".to_string()),
                    ]],
                    None,
                    Duration::ZERO,
                ),
                cx,
            );
        });
    });
    assert_eq!(
        window.update(|_, app| fixture.panel.read(app).pk_columns.clone()),
        vec!["code".to_string()],
        "the binding supplied the key"
    );
    window.update(|_, app| {
        let table_state = fixture
            .panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("a table");
        table_state.update(app, |state, _| {
            state.stage_cell_value(0, 2, CellValue::text("renamed"));
        });
    });

    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_row(0, false, cx)
    });

    let ops = writes(&fixture.driver);
    let [CrudOp::Update(patch)] = ops.as_slice() else {
        panic!("expected one UPDATE, got {ops:?}");
    };
    assert_eq!(patch.identity.columns(), ["code".to_string()]);
    assert_eq!(
        patch.identity.values(),
        [dbflux_core::Value::Text("c-1".to_string())]
    );
    assert!(!modal_visible(&fixture, window));
}

// === The modal's two ways out ===

#[gpui::test]
fn cancel_closes_the_modal_and_keeps_the_edits_staged(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, changed_key());
    stage_name(&fixture, window, 0);
    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_row(0, false, cx)
    });

    modal_click(&fixture, window, false);

    assert!(!modal_visible(&fixture, window));
    assert_eq!(pending_counts(&fixture, window), (0, 1, 0));
    assert!(writes(&fixture.driver).is_empty());
    assert_eq!(
        window.update(|_, app| fixture.panel.read(app).pk_columns.clone()),
        vec!["id".to_string()],
        "the grid keeps the key it had"
    );
}

#[gpui::test]
fn reload_table_applies_the_new_key_and_discards_the_edits(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, changed_key());
    stage_name(&fixture, window, 0);
    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_row(0, false, cx)
    });

    modal_click(&fixture, window, true);

    assert!(!modal_visible(&fixture, window));
    assert_eq!(pending_counts(&fixture, window), (0, 0, 0));
    assert_eq!(
        window.update(|_, app| fixture.panel.read(app).pk_columns.clone()),
        vec!["code".to_string()]
    );
    assert_eq!(
        cached_columns(&fixture, window),
        vec!["id".to_string(), "code".to_string(), "name".to_string()]
    );
    assert!(writes(&fixture.driver).is_empty());
}

#[gpui::test]
fn reload_table_after_the_key_was_dropped_leaves_the_grid_without_a_key(cx: &mut TestAppContext) {
    let dropped = FakeTableDetails::Info(Box::new(orders_info(&[], &["id", "name"])));
    let (fixture, window) = fixture(cx, dropped);
    stage_name(&fixture, window, 0);
    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_row(0, false, cx)
    });
    assert!(modal_visible(&fixture, window));

    modal_click(&fixture, window, true);

    assert!(
        window
            .update(|_, app| fixture.panel.read(app).pk_columns.clone())
            .is_empty(),
        "an empty key must replace the old one, not be ignored"
    );
    assert_eq!(pending_counts(&fixture, window), (0, 0, 0));
}

// === Existing behavior that shares the code ===

#[gpui::test]
fn a_save_all_with_no_row_updates_does_not_check_the_key(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, changed_key());
    stage_insert(&fixture, window, 10, "new");

    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_all(Vec::new(), vec![0], Vec::new(), cx)
    });

    assert_eq!(
        fixture.driver.table_details_calls(),
        0,
        "an insert writes no key"
    );
    assert!(!modal_visible(&fixture, window));
    assert!(
        matches!(writes(&fixture.driver).as_slice(), [CrudOp::Insert(_)]),
        "the insert was sent: {:?}",
        writes(&fixture.driver)
    );
}

fn same_key() -> FakeTableDetails {
    FakeTableDetails::Info(Box::new(orders_info(&["id"], &["id", "name"])))
}

fn reversed_rows() -> QueryResult {
    QueryResult::table(
        vec![key_column("id", true), key_column("name", false)],
        (1..=3)
            .rev()
            .map(|id| {
                vec![
                    dbflux_core::Value::Int(id),
                    dbflux_core::Value::Text(format!("order {id}")),
                ]
            })
            .collect(),
        None,
        Duration::ZERO,
    )
}

/// A reload that lands while the key check is in flight must not make the
/// delete hit a row the user did not mark.
#[gpui::test]
fn a_bulk_delete_targets_the_marked_row_after_a_reload_during_the_check(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    mark_for_delete(&fixture, window, &[0]); // id = 1

    window.update(|window, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.pending_delete_confirm = Some(PendingDeleteConfirm {
                row_indices: vec![0],
                is_table: true,
            });
            panel.confirm_delete(window, cx);
            // A keep-edits reload (e.g. after an independent insert) lands
            // before the check answers; rows come back in another order.
            panel.grid_table.keep_edits_on_reload = true;
            panel.set_result(reversed_rows(), cx);
        });
    });
    window.run_until_parked();

    let deleted: Vec<Vec<dbflux_core::Value>> = writes(&fixture.driver)
        .into_iter()
        .filter_map(|op| match op {
            CrudOp::Delete(delete) => Some(delete.identity.values().to_vec()),
            _ => None,
        })
        .collect();
    assert_eq!(
        deleted,
        vec![vec![dbflux_core::Value::Int(1)]],
        "only the marked row (id 1) may be deleted"
    );
}

/// Save All requested while a single-row check is in flight must not be lost
/// nor leave the batch flag stuck.
#[gpui::test]
fn save_all_during_an_in_flight_check_is_not_dropped(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    stage_name(&fixture, window, 0);
    stage_name(&fixture, window, 1);

    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_row(0, false, cx);
        panel.handle_save_all(Vec::new(), Vec::new(), vec![0, 1], cx);
    });

    let updates = writes(&fixture.driver)
        .into_iter()
        .filter(|op| matches!(op, CrudOp::Update(_)))
        .count();
    let in_flight = window.update(|_, app| fixture.panel.read(app).batch_op_in_flight);
    assert!(
        !in_flight,
        "batch_op_in_flight stuck true; updates sent={updates}, pending={:?}",
        pending_counts(&fixture, window)
    );
    assert_eq!(updates, 2, "row 1 must be saved too");
}

/// A delete confirmed while a row save check is in flight must not vanish.
#[gpui::test]
fn a_delete_confirmed_during_an_in_flight_check_is_not_dropped(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    stage_name(&fixture, window, 0);
    mark_for_delete(&fixture, window, &[2]);

    act(&fixture, window, |panel, window, cx| {
        panel.handle_save_row(0, false, cx);
        panel.pending_delete_confirm = Some(PendingDeleteConfirm {
            row_indices: vec![2],
            is_table: true,
        });
        panel.confirm_delete(window, cx);
    });

    assert_eq!(
        deleted_keys(&fixture.driver),
        vec![vec![dbflux_core::Value::Int(3)]],
        "the confirmed delete of id 3 was sent"
    );
    assert!(!window.update(|_, app| fixture.panel.read(app).batch_op_in_flight));
}

/// A close waiting on its apply while a row-save check runs must finish.
#[gpui::test]
fn a_close_during_an_in_flight_check_completes(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    stage_name(&fixture, window, 0);
    stage_name(&fixture, window, 1);

    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_row(0, false, cx);
        assert!(panel.apply_for_close(cx));
    });

    let closed = fixture
        .seen
        .borrow()
        .iter()
        .any(|event| matches!(event, DataGridEvent::RequestClose));
    assert!(closed, "the close was asked for after both edits landed");
    let mut updated = updated_keys(&fixture.driver);
    updated.sort_by_key(|key| format!("{key:?}"));
    assert_eq!(
        updated,
        vec![
            vec![dbflux_core::Value::Int(1)],
            vec![dbflux_core::Value::Int(2)]
        ]
    );
    assert_eq!(pending_counts(&fixture, window), (0, 0, 0));
}

/// Cancel then save again re-checks (no stale pass), and a changed key on the
/// second check still blocks.
#[gpui::test]
fn cancel_then_save_again_checks_again(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, changed_key());
    stage_name(&fixture, window, 0);
    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_row(0, false, cx)
    });
    modal_click(&fixture, window, false);
    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_row(0, false, cx)
    });

    assert_eq!(fixture.driver.table_details_calls(), 2);
    assert!(writes(&fixture.driver).is_empty());
    assert!(modal_visible(&fixture, window));
}

/// Reload when the table is gone: edits discarded, nothing sent, no panic.
#[gpui::test]
fn reload_table_after_the_table_is_gone_discards_the_edits(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, FakeTableDetails::NotFound);
    stage_name(&fixture, window, 0);
    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_row(0, false, cx)
    });
    modal_click(&fixture, window, true);

    assert!(!modal_visible(&fixture, window));
    assert_eq!(pending_counts(&fixture, window), (0, 0, 0));
    assert!(writes(&fixture.driver).is_empty());
    assert!(
        cached_columns(&fixture, window).is_empty(),
        "cache entry dropped"
    );
}

/// The key and values a row save sends are the ones read when it was
/// requested, even when a reload moves the row while the check runs.
#[gpui::test]
fn a_row_save_sends_the_key_read_before_a_reload_during_the_check(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    stage_name(&fixture, window, 0); // id = 1

    window.update(|_, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.handle_save_row(0, false, cx);
            land_table_reload(panel, &[3, 2, 1], cx);
        });
    });
    window.run_until_parked();

    let updated: Vec<Vec<dbflux_core::Value>> = writes(&fixture.driver)
        .into_iter()
        .filter_map(|op| match op {
            CrudOp::Update(patch) => Some(patch.identity.values().to_vec()),
            _ => None,
        })
        .collect();
    assert_eq!(updated, vec![vec![dbflux_core::Value::Int(1)]]);
}

fn rows_of(ids: &[i64]) -> QueryResult {
    QueryResult::table(
        vec![key_column("id", true), key_column("name", false)],
        ids.iter()
            .map(|id| {
                vec![
                    dbflux_core::Value::Int(*id),
                    dbflux_core::Value::Text(format!("order {id}")),
                ]
            })
            .collect(),
        None,
        Duration::ZERO,
    )
}

/// The table reload path (refresh, page, sort, the reload after a landed
/// mutation) goes through `apply_table_result`, not `set_result`.
fn land_table_reload(
    panel: &mut DataGridPanel,
    ids: &[i64],
    cx: &mut gpui::Context<DataGridPanel>,
) {
    panel.grid_table.keep_edits_on_reload = true;
    panel.apply_table_result(
        Uuid::nil(),
        TableRef::with_schema("public", "orders"),
        Pagination::default(),
        Vec::new(),
        Some(ids.len() as u64),
        rows_of(ids),
        cx,
    );
}

fn deleted_keys(driver: &FakeDriver) -> Vec<Vec<dbflux_core::Value>> {
    writes(driver)
        .into_iter()
        .filter_map(|op| match op {
            CrudOp::Delete(delete) => Some(delete.identity.values().to_vec()),
            _ => None,
        })
        .collect()
}

fn updated_keys(driver: &FakeDriver) -> Vec<Vec<dbflux_core::Value>> {
    writes(driver)
        .into_iter()
        .filter_map(|op| match op {
            CrudOp::Update(patch) => Some(patch.identity.values().to_vec()),
            _ => None,
        })
        .collect()
}

/// A table reload during a Save All check must either stop the batch with a
/// report or save every staged row; it must never skip a staged row silently.
#[gpui::test]
fn save_all_after_a_table_reload_during_the_check_loses_no_edit(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    stage_name(&fixture, window, 0); // id 1
    stage_name(&fixture, window, 1); // id 2

    window.update(|_, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.handle_save_all(Vec::new(), Vec::new(), vec![0, 1], cx);
            land_table_reload(panel, &[3, 2, 1], cx);
        });
    });
    window.run_until_parked();

    let mut updated = updated_keys(&fixture.driver);
    updated.sort_by_key(|key| format!("{key:?}"));
    let reported = last_toast_title(window).is_some();
    let stopped = updated.is_empty() && reported;
    let all_saved = updated
        == vec![
            vec![dbflux_core::Value::Int(1)],
            vec![dbflux_core::Value::Int(2)],
        ]
        && pending_counts(&fixture, window) == (0, 0, 0);
    assert!(
        stopped || all_saved,
        "exactly ids 1 and 2 must be saved: updated={updated:?} reported={reported} pending={:?}",
        pending_counts(&fixture, window)
    );
}

/// Same, with a close waiting: the tab must not close over a staged edit.
#[gpui::test]
fn close_apply_after_a_table_reload_during_the_check_does_not_close_over_edits(
    cx: &mut TestAppContext,
) {
    let (fixture, window) = fixture(cx, same_key());
    stage_name(&fixture, window, 0);

    window.update(|_, app| {
        fixture.panel.update(app, |panel, cx| {
            assert!(panel.apply_for_close(cx));
            land_table_reload(panel, &[3, 2, 1], cx);
        });
    });
    window.run_until_parked();

    let closed = fixture
        .seen
        .borrow()
        .iter()
        .any(|event| matches!(event, DataGridEvent::RequestClose));
    let pending = pending_counts(&fixture, window);
    let updated = updated_keys(&fixture.driver);
    assert!(
        !closed || (pending == (0, 0, 0) && updated == vec![vec![dbflux_core::Value::Int(1)]]),
        "tab asked to close without id 1 landing: pending={pending:?}, updates={updated:?}"
    );
}

/// A confirmed delete whose row left the grid during the check is either sent
/// (its key was checked) or reported; never dropped silently.
#[gpui::test]
fn a_confirmed_delete_whose_row_left_the_grid_is_not_dropped_silently(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    mark_for_delete(&fixture, window, &[0]); // id 1

    window.update(|window, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.pending_delete_confirm = Some(PendingDeleteConfirm {
                row_indices: vec![0],
                is_table: true,
            });
            panel.confirm_delete(window, cx);
            land_table_reload(panel, &[2, 3], cx);
        });
    });
    window.run_until_parked();

    let deleted = deleted_keys(&fixture.driver);
    let reported = last_toast_title(window)
        == Some(dbflux_i18n::t!(
            "document.data.mutation.error.bulk_delete_no_rows_identified"
        ));
    assert!(
        deleted == vec![vec![dbflux_core::Value::Int(1)]] || (deleted.is_empty() && reported),
        "id 1 was marked: it is deleted, or the skip is reported; got deletes={deleted:?}"
    );
    assert!(!window.update(|_, app| fixture.panel.read(app).batch_op_in_flight));
}

/// Same for a row save.
#[gpui::test]
fn a_row_save_whose_row_left_the_grid_is_not_dropped_silently(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    stage_name(&fixture, window, 0); // id 1

    window.update(|_, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.handle_save_row(0, false, cx);
            land_table_reload(panel, &[2, 3], cx);
        });
    });
    window.run_until_parked();

    let updated = updated_keys(&fixture.driver);
    let reported = last_toast_title(window)
        == Some(dbflux_i18n::t!(
            "document.data.mutation.error.staged_row_left_grid"
        ));
    assert!(
        updated == vec![vec![dbflux_core::Value::Int(1)]] || (updated.is_empty() && reported),
        "id 1 was edited: it is saved, or the skip is reported; got updates={updated:?}"
    );
}

/// A request queued behind a check that blocks is released, the batch flag
/// is cleared, and nothing is sent.
#[gpui::test]
fn requests_queued_behind_a_blocked_check_are_released(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, changed_key());
    stage_name(&fixture, window, 0);
    stage_name(&fixture, window, 1);
    mark_for_delete(&fixture, window, &[2]);

    act(&fixture, window, |panel, window, cx| {
        panel.handle_save_row(0, false, cx);
        panel.handle_save_all(Vec::new(), Vec::new(), vec![0, 1], cx);
        panel.pending_delete_confirm = Some(PendingDeleteConfirm {
            row_indices: vec![2],
            is_table: true,
        });
        panel.confirm_delete(window, cx);
    });

    assert!(writes(&fixture.driver).is_empty());
    assert!(modal_visible(&fixture, window));
    assert_eq!(fixture.driver.table_details_calls(), 1);
    assert!(!window.update(|_, app| fixture.panel.read(app).batch_op_in_flight));
    assert_eq!(pending_counts(&fixture, window), (0, 2, 1));
}

/// Several requests queued behind a passing check each run once, serially.
#[gpui::test]
fn requests_queued_behind_a_passing_check_all_run(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    stage_name(&fixture, window, 0);
    stage_name(&fixture, window, 1);
    mark_for_delete(&fixture, window, &[2]);

    act(&fixture, window, |panel, window, cx| {
        panel.handle_save_row(0, false, cx);
        panel.handle_save_row(1, false, cx);
        panel.pending_delete_confirm = Some(PendingDeleteConfirm {
            row_indices: vec![2],
            is_table: true,
        });
        panel.confirm_delete(window, cx);
    });

    let ops = writes(&fixture.driver);
    let updates = ops
        .iter()
        .filter(|op| matches!(op, CrudOp::Update(_)))
        .count();
    let deletes = ops
        .iter()
        .filter(|op| matches!(op, CrudOp::Delete(_)))
        .count();
    assert_eq!((updates, deletes), (2, 1), "{ops:?}");
    assert_eq!(
        fixture.driver.table_details_calls(),
        3,
        "one check per request"
    );
    assert!(!window.update(|_, app| fixture.panel.read(app).batch_op_in_flight));
    assert!(!modal_visible(&fixture, window));
}

/// The grid's key changing while a passing check runs triggers a fresh
/// check against the new key, which here no longer matches: nothing is sent.
#[gpui::test]
fn a_key_change_during_a_passing_check_is_checked_again(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    stage_name(&fixture, window, 0);

    window.update(|_, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.handle_save_row(0, false, cx);
            panel.pk_columns = vec!["name".to_string()];
        });
    });
    window.run_until_parked();

    assert_eq!(fixture.driver.table_details_calls(), 2);
    assert!(writes(&fixture.driver).is_empty());
    assert!(modal_visible(&fixture, window));
}

/// Revert while the key check runs: the row has nothing staged any more, so
/// nothing may be sent for it.
#[gpui::test]
fn a_row_reverted_during_the_check_is_not_saved(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    stage_name(&fixture, window, 0);

    window.update(|_, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.handle_save_row(0, false, cx);
            let table_state = panel.grid_table.table_state.clone().expect("a table");
            table_state.update(cx, |state, cx| state.revert_row(0, cx));
        });
    });
    window.run_until_parked();

    assert!(
        writes(&fixture.driver).is_empty(),
        "the reverted row was sent"
    );
}

/// A fetch that the table's details being cleared made stale is not trusted:
/// the check runs again and the write goes out after the second answer.
#[gpui::test]
fn a_check_made_stale_by_a_cache_clear_is_run_again(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    stage_name(&fixture, window, 0);

    window.update(|_, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.handle_save_row(0, false, cx);
            panel.app_state.update(cx, |state, _| {
                state.clear_table_details(Uuid::nil());
            });
        });
    });
    window.run_until_parked();

    assert_eq!(fixture.driver.table_details_calls(), 2);
    assert!(matches!(
        writes(&fixture.driver).as_slice(),
        [CrudOp::Update(_)]
    ));
}

/// A connection replaced while the check runs is checked itself before it is
/// written to.
#[gpui::test]
fn a_connection_replaced_during_the_check_is_checked_before_it_is_written(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    stage_name(&fixture, window, 0);
    let replacement =
        FakeDriver::new(dbflux_core::DbKind::SQLite).with_table_details("orders", changed_key());

    window.update(|window, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.handle_save_row(0, false, cx);
            let _ = window;
        });
    });
    register_fake_connection(&fixture.panel, window, &replacement);
    window.run_until_parked();

    assert_eq!(
        replacement.table_details_calls(),
        1,
        "the new connection was checked"
    );
    assert!(writes(&fixture.driver).is_empty());
    assert!(
        writes(&replacement).is_empty(),
        "the replacement's changed key stops the write"
    );
    assert!(modal_visible(&fixture, window));
}

/// A second Save All while the first one is checked must not send the same
/// insert again.
#[gpui::test]
fn a_second_save_all_during_the_check_does_not_resend_the_batch(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    stage_insert(&fixture, window, 10, "new");
    stage_name(&fixture, window, 0);

    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_all(Vec::new(), vec![0], vec![0], cx);
        panel.handle_save_all(Vec::new(), vec![0], vec![0], cx);
    });

    let ops = writes(&fixture.driver);
    let inserts = ops
        .iter()
        .filter(|op| matches!(op, CrudOp::Insert(_)))
        .count();
    let updates = ops
        .iter()
        .filter(|op| matches!(op, CrudOp::Update(_)))
        .count();
    assert_eq!((inserts, updates), (1, 1), "{ops:?}");
    assert_eq!(fixture.driver.table_details_calls(), 1);
}

/// An insert that an independent save removed while Save All was checked
/// renumbers the batch: the insert staged afterwards is not part of it.
#[gpui::test]
fn an_insert_removed_during_the_check_keeps_the_batch_in_step(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    stage_insert(&fixture, window, 10, "a");
    stage_insert(&fixture, window, 11, "b");
    stage_name(&fixture, window, 0);

    window.update(|_, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.handle_save_all(Vec::new(), vec![0, 1], vec![0], cx);
            let table_state = panel.grid_table.table_state.clone().expect("a table");
            table_state.update(cx, |state, _| {
                state.edit_buffer_mut().remove_pending_insert_by_idx(0);
            });
            panel.renumber_queued_inserts_after_removal(0, cx);
            table_state.update(cx, |state, _| {
                state
                    .edit_buffer_mut()
                    .add_pending_insert(vec![CellValue::int(12), CellValue::text("c")]);
            });
        });
    });
    window.run_until_parked();

    let inserted: Vec<String> = writes(&fixture.driver)
        .into_iter()
        .filter_map(|op| match op {
            CrudOp::Insert(insert) => Some(format!("{:?}", insert.assignments)),
            _ => None,
        })
        .collect();
    assert_eq!(inserted.len(), 1, "{inserted:?}");
    assert!(
        inserted[0].contains("\"b\""),
        "only b was in the batch: {inserted:?}"
    );
}

/// A delete confirmed while a Save All write runs must not take that
/// batch's parked rows down with it when its own check fails.
#[gpui::test]
fn a_failed_delete_check_does_not_drop_a_running_save_all_rows(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    stage_name(&fixture, window, 0);
    stage_name(&fixture, window, 1);
    mark_for_delete(&fixture, window, &[2]);

    window.update(|window, app| {
        fixture.panel.update(app, |panel, cx| {
            // Save All's first write is running with a second row parked.
            panel.batch_op_in_flight = true;
            panel.pending_batch_remaining = Some(PendingBatchRemaining {
                pending_inserts: Vec::new(),
                dirty_rows: vec![1],
                dirty_keys: Vec::new(),
            });
            fixture.driver.set_table_details("orders", changed_key());
            panel.pending_delete_confirm = Some(PendingDeleteConfirm {
                row_indices: vec![2],
                is_table: true,
            });
            panel.confirm_delete(window, cx);
        });
    });
    window.run_until_parked();

    assert!(modal_visible(&fixture, window));
    window.update(|_, app| {
        let panel = fixture.panel.read(app);
        assert!(
            panel.batch_op_in_flight,
            "the running write is still in flight"
        );
        assert!(
            panel.pending_batch_remaining.is_some(),
            "the parked row was dropped"
        );
    });
}

/// A confirmed delete where only one of two rows is still on the page is
/// reported with the rows it could not find.
#[gpui::test]
fn a_bulk_delete_reports_a_confirmed_row_that_left_the_grid(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, same_key());
    mark_for_delete(&fixture, window, &[0, 1]); // ids 1 and 2

    window.update(|window, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.pending_delete_confirm = Some(PendingDeleteConfirm {
                row_indices: vec![0, 1],
                is_table: true,
            });
            panel.confirm_delete(window, cx);
            land_table_reload(panel, &[2, 3], cx);
        });
    });
    window.run_until_parked();

    assert_eq!(
        deleted_keys(&fixture.driver),
        vec![vec![dbflux_core::Value::Int(2)]]
    );
    assert_eq!(
        last_toast_title(window),
        Some(crate::labels::bulk_delete_unidentified_label(
            crate::labels::MutationItemKind::Row,
            1,
            1
        )),
        "the skipped row is reported with the one that was deleted"
    );
}

/// A chunked run writes through the connection its key check used.
#[gpui::test]
fn a_chunked_mutation_runs_on_the_connection_the_key_was_checked_on(cx: &mut TestAppContext) {
    let (fixture, window) = fixture_for(cx, same_key(), dbflux_core::DbKind::Postgres);
    let slot =
        FakeDriver::new(dbflux_core::DbKind::Postgres).with_table_details("orders", same_key());
    window.update(|_, app| {
        let app_state = fixture.panel.read(app).app_state.clone();
        app_state.update(app, |state, _| {
            let profile = dbflux_core::ConnectionProfile::new(
                "slot",
                dbflux_core::DbConfig::default_postgres(),
            );
            let connection = slot.connect_arc(&profile).expect("the fake connects");
            state
                .connections_mut()
                .get_mut(&Uuid::nil())
                .expect("connected")
                .database_connections
                .insert(
                    "app".to_string(),
                    dbflux_core::DatabaseConnection {
                        connection,
                        schema: None,
                    },
                );
        });
    });

    chunked_delete(&fixture, window);

    assert_eq!(
        slot.table_details_calls(),
        1,
        "the key was checked on the slot"
    );
    assert!(
        !slot.stats().executed_requests.is_empty(),
        "the run used the slot's connection"
    );
    assert!(
        fixture.driver.stats().executed_requests.is_empty(),
        "the primary connection was not written to"
    );
}

/// With no prepared connection for the grid's database, a chunked run is
/// refused rather than started on the primary connection.
#[gpui::test]
fn a_chunked_mutation_without_a_connection_for_its_database_is_refused(cx: &mut TestAppContext) {
    let (fixture, window) = fixture_for(cx, same_key(), dbflux_core::DbKind::Postgres);

    chunked_delete(&fixture, window);

    assert!(fixture.driver.stats().executed_requests.is_empty());
    assert_eq!(fixture.driver.table_details_calls(), 0);
    assert!(!mutation_finished(&fixture), "the run never started");
    assert!(
        last_toast_title(window).is_some(),
        "the refusal is reported"
    );
}

/// The details the modal kept went stale while it was open: Reload table does
/// not write them, it reads the table's key again.
#[gpui::test]
fn reload_table_with_details_gone_stale_reads_the_key_again(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx, changed_key());
    stage_name(&fixture, window, 0);
    act(&fixture, window, |panel, _, cx| {
        panel.handle_save_row(0, false, cx)
    });
    assert!(modal_visible(&fixture, window));

    window.update(|_, app| {
        let app_state = fixture.panel.read(app).app_state.clone();
        app_state.update(app, |state, _| {
            state.clear_table_details(Uuid::nil());
        });
    });
    modal_click(&fixture, window, true);

    assert_eq!(pending_counts(&fixture, window), (0, 0, 0));
    assert_eq!(
        fixture.driver.table_details_calls(),
        2,
        "the stale answer was not trusted: the key was read again"
    );
    assert_eq!(
        window.update(|_, app| fixture.panel.read(app).pk_columns.clone()),
        vec!["code".to_string()]
    );
    assert_eq!(
        cached_columns(&fixture, window),
        vec!["id".to_string(), "code".to_string(), "name".to_string()]
    );
    assert!(writes(&fixture.driver).is_empty());
}
