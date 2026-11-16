//! Cells that hold a SQL expression: the "Set value" menu, "Set to default"
//! and what a save writes for them.

use super::*;
use dbflux_components::components::data_table::model::CellValue;
use dbflux_core::{
    ColumnInfo, ColumnKind, ColumnMeta, ConnectedProfile, DbConfig, MutationPolicy, TableInfo,
    TableRef,
};
use dbflux_test_support::fake_driver::FakeDriver;
use gpui::Entity;
use std::path::PathBuf;

struct Harness;

impl gpui::Render for Harness {
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        _cx: &mut gpui::Context<Self>,
    ) -> impl gpui::IntoElement {
        gpui::div()
    }
}

struct Fixture {
    panel: Entity<DataGridPanel>,
    table_state: Entity<dbflux_components::components::data_table::DataTableState>,
    driver: FakeDriver,
    profile_id: Uuid,
}

fn column(name: &str, type_name: &str, kind: ColumnKind) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        type_name: type_name.to_string(),
        kind,
        nullable: true,
        is_primary_key: false,
    }
}

fn column_info(name: &str, type_name: &str, default: Option<&str>) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        type_name: type_name.to_string(),
        nullable: true,
        is_primary_key: name == "id",
        default_value: default.map(str::to_string),
        enum_values: None,
    }
}

/// An editable `public.users` grid (`id`, `name`, `created_at`) on a SQL
/// connection, with `name` and `created_at` carrying a default.
fn fixture(cx: &mut TestAppContext) -> (Fixture, &mut gpui::VisualTestContext) {
    init_test_runtime(cx);

    let profile_id = Uuid::new_v4();
    let driver = FakeDriver::new(dbflux_core::DbKind::SQLite);
    let app_state = isolated_test_app_state(cx);

    cx.update(|cx| {
        app_state.update(cx, |app, _cx| {
            let profile = dbflux_core::ConnectionProfile::new(
                "fake-sqlite",
                DbConfig::SQLite {
                    path: PathBuf::from(":memory:"),
                    connection_id: None,
                },
            );
            let connection = driver.connect_arc(&profile).expect("fake connection");
            app.connections_mut().insert(
                profile_id,
                ConnectedProfile {
                    profile,
                    connection,
                    schema: None,
                    mutation_policy: MutationPolicy::default(),
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
                    active_database: Some("testdb".to_string()),
                    redis_key_cache: Default::default(),
                    database_connections: Default::default(),
                    proxy_tunnel: None,
                },
            );
            app.set_table_details(
                profile_id,
                "testdb".to_string(),
                Some("public".to_string()),
                "users".to_string(),
                TableInfo {
                    name: "users".to_string(),
                    schema: Some("public".to_string()),
                    columns: Some(vec![
                        column_info("id", "int4", None),
                        column_info("name", "text", Some("anonymous")),
                        column_info("created_at", "timestamptz", Some("now()")),
                    ]),
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: Default::default(),
                    child_items: None,
                    storage_hints: None,
                    pseudo_columns: Box::default(),
                },
            );
        });
    });

    let holder = Rc::new(RefCell::new(None));
    let handle = holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id,
                database: Some("testdb".to_string()),
                table: TableRef::with_schema("public", "users"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: None,
            };
            let mut panel =
                DataGridPanel::new_internal(source, app_state.clone(), Vec::new(), window, cx);
            let columns = vec![
                column("id", "int4", ColumnKind::Integer),
                column("name", "text", ColumnKind::Text),
                column("created_at", "timestamptz", ColumnKind::Timestamp),
            ];
            let rows = vec![vec![
                dbflux_core::Value::Int(1),
                dbflux_core::Value::Text("alice".to_string()),
                dbflux_core::Value::Text("2026-01-01".to_string()),
            ]];
            panel.result = QueryResult::table(columns, rows, None, Duration::ZERO);
            panel.pk_columns = vec!["id".to_string()];
            panel
        });
        handle.replace(Some(panel));
        Harness
    });

    let panel = holder.borrow().clone().expect("panel");
    window.update(|_, app| panel.update(app, |panel, cx| panel.rebuild_table(None, cx)));
    let table_state = window.update(|_, app| {
        panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("table state")
    });

    (
        Fixture {
            panel,
            table_state,
            driver,
            profile_id,
        },
        window,
    )
}

fn staged(
    window: &mut gpui::VisualTestContext,
    fixture: &Fixture,
    col: usize,
) -> Option<CellValue> {
    window.update(|_, app| {
        fixture
            .table_state
            .read(app)
            .edit_buffer()
            .row_changes(0)
            .into_iter()
            .find(|(staged_col, _)| *staged_col == col)
            .map(|(_, cell)| cell.clone())
    })
}

/// S3: "Set to default" stages the column default as an expression, never as
/// the text it spells.
#[gpui::test]
fn set_default_stages_the_default_as_an_expression(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx);

    window.update(|_, app| {
        fixture
            .panel
            .update(app, |panel, cx| panel.handle_set_default(0, 2, cx))
    });

    let cell = staged(window, &fixture, 2).expect("the default is staged");
    assert_eq!(cell.as_expression(), Some("now()"));
}

/// S4: the type values come from the connection's dialect, and picking one
/// stages it as an expression.
#[gpui::test]
fn type_values_come_from_the_dialect_and_stage_expressions(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx);

    let (offered, expected) = window.update(|_, app| {
        let panel = fixture.panel.read(app);
        let offered: Vec<String> = panel
            .set_value_entries(2, app)
            .into_iter()
            .filter_map(|entry| match entry.action {
                dbflux_components::components::data_table::ContextMenuAction::SetTypeValue(_) => {
                    Some(entry.label.to_string())
                }
                _ => None,
            })
            .collect();
        let expected: Vec<String> = panel
            .app_state
            .read(app)
            .connections()
            .get(&fixture.profile_id)
            .expect("connection")
            .connection
            .dialect()
            .type_value_expressions("timestamptz")
            .into_iter()
            .map(str::to_owned)
            .collect();
        (offered, expected)
    });
    assert_eq!(offered, expected);
    assert!(!offered.is_empty(), "a timestamp column is offered values");

    window.update(|_, app| {
        fixture
            .panel
            .update(app, |panel, cx| panel.handle_set_type_value(0, 2, 0, cx))
    });
    let cell = staged(window, &fixture, 2).expect("the type value is staged");
    assert_eq!(cell.as_expression(), Some(expected[0].as_str()));
}

/// A column without a default offers "Column default" disabled, with the
/// reason, and still offers NULL and a typed expression.
#[gpui::test]
fn a_column_without_a_default_disables_that_entry_with_a_reason(cx: &mut TestAppContext) {
    use dbflux_components::components::data_table::ContextMenuAction;

    let (fixture, window) = fixture(cx);

    let entries = window.update(|_, app| fixture.panel.read(app).set_value_entries(0, app));
    let first = entries.first().expect("column default entry");

    assert_eq!(first.action, ContextMenuAction::SetDefault);
    assert!(first.disabled);
    assert!(
        first
            .detail
            .as_ref()
            .is_some_and(|reason| reason.contains("id")),
        "the reason names the column"
    );
    assert!(
        entries
            .iter()
            .any(|e| e.action == ContextMenuAction::SetNull)
    );
    assert!(
        entries
            .iter()
            .any(|e| e.action == ContextMenuAction::SetExpression)
    );
}

/// S1: an expression staged on a row is written as the assignment's
/// expression, with no value bound.
#[gpui::test]
fn saving_an_expression_cell_writes_it_as_an_expression(cx: &mut TestAppContext) {
    use dbflux_test_support::fake_driver::CrudOp;

    let (fixture, window) = fixture(cx);
    let cell = CellValue::expression("now()");

    window.update(|_, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.save_table_row(
                fixture.profile_id,
                None,
                TableRef::with_schema("public", "users"),
                0,
                &[(2, &cell)],
                false,
                cx,
            )
        })
    });
    window.run_until_parked();

    let ops = fixture.driver.stats().crud_ops;
    let CrudOp::Update(patch) = &ops[0] else {
        panic!("expected an update");
    };
    assert_eq!(patch.changes[0].name, "created_at");
    assert_eq!(patch.changes[0].expression.as_deref(), Some("now()"));
}

/// A pending insert with an expression and a literal that spells a function
/// call writes the first as an expression and the second as a value.
#[gpui::test]
fn inserting_keeps_expressions_apart_from_literal_text(cx: &mut TestAppContext) {
    use dbflux_test_support::fake_driver::CrudOp;

    let (fixture, window) = fixture(cx);

    let insert_idx = window.update(|_, app| {
        fixture.table_state.update(app, |state, _| {
            state.edit_buffer_mut().add_pending_insert_after(
                0,
                vec![
                    CellValue::null(),
                    CellValue::text("now()"),
                    CellValue::expression("now()"),
                ],
            )
        })
    });
    window.update(|_, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.handle_commit_insert(insert_idx, false, cx)
        })
    });
    window.update(|window, app| {
        fixture
            .panel
            .update(app, |panel, cx| panel.confirm_expressions(window, cx))
    });
    window.run_until_parked();

    let ops = fixture.driver.stats().crud_ops;
    let CrudOp::Insert(insert) = &ops[0] else {
        panic!("expected an insert");
    };
    let name = insert
        .assignments
        .iter()
        .find(|a| a.name == "name")
        .expect("name");
    assert_eq!(name.expression, None);
    assert_eq!(name.value, dbflux_core::Value::Text("now()".into()));
    let created = insert
        .assignments
        .iter()
        .find(|a| a.name == "created_at")
        .expect("created_at");
    assert_eq!(created.expression.as_deref(), Some("now()"));
}

/// S5: an insert that carries an expression waits for confirmation and writes
/// nothing until it is given.
#[gpui::test]
fn an_insert_with_an_expression_waits_for_confirmation(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx);

    let insert_idx = window.update(|_, app| {
        fixture.table_state.update(app, |state, _| {
            state.edit_buffer_mut().add_pending_insert_after(
                0,
                vec![
                    CellValue::null(),
                    CellValue::text("bob"),
                    CellValue::expression("now()"),
                ],
            )
        })
    });
    window.update(|_, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.handle_commit_insert(insert_idx, false, cx)
        })
    });
    window.run_until_parked();

    assert!(window.update(|_, app| fixture.panel.read(app).has_expression_confirm()));
    assert!(
        fixture.driver.stats().crud_ops.is_empty(),
        "nothing ran yet"
    );

    window.update(|window, app| {
        fixture
            .panel
            .update(app, |panel, cx| panel.cancel_expressions(window, cx))
    });
    window.run_until_parked();

    assert!(!window.update(|_, app| fixture.panel.read(app).has_expression_confirm()));
    assert!(
        fixture.driver.stats().crud_ops.is_empty(),
        "declining writes nothing"
    );
}

/// A save of literals only, including text that spells a function call, is
/// not held.
#[gpui::test]
fn literal_saves_are_not_held(cx: &mut TestAppContext) {
    use dbflux_test_support::fake_driver::CrudOp;

    let (fixture, window) = fixture(cx);

    window.update(|_, app| {
        fixture.table_state.update(app, |state, cx| {
            state.stage_cell_value(0, 1, CellValue::text("now()"));
            cx.notify();
        })
    });
    window.update(|_, app| {
        fixture
            .panel
            .update(app, |panel, cx| panel.handle_save_row(0, false, cx))
    });
    window.run_until_parked();

    assert!(!window.update(|_, app| fixture.panel.read(app).has_expression_confirm()));
    let ops = fixture.driver.stats().crud_ops;
    let CrudOp::Update(patch) = &ops[0] else {
        panic!("expected an update");
    };
    assert_eq!(patch.changes[0].expression, None);
    assert_eq!(
        patch.changes[0].value,
        dbflux_core::Value::Text("now()".into())
    );
}

/// An update that carries an expression is held too, and runs once confirmed.
#[gpui::test]
fn an_update_with_an_expression_is_held_then_runs(cx: &mut TestAppContext) {
    use dbflux_test_support::fake_driver::CrudOp;

    let (fixture, window) = fixture(cx);

    window.update(|_, app| {
        fixture.table_state.update(app, |state, cx| {
            state.stage_cell_value(0, 2, CellValue::expression("now()"));
            cx.notify();
        })
    });
    window.update(|_, app| {
        fixture
            .panel
            .update(app, |panel, cx| panel.handle_save_row(0, false, cx))
    });
    window.run_until_parked();
    assert!(fixture.driver.stats().crud_ops.is_empty());

    window.update(|window, app| {
        fixture
            .panel
            .update(app, |panel, cx| panel.confirm_expressions(window, cx))
    });
    window.run_until_parked();

    let ops = fixture.driver.stats().crud_ops;
    let CrudOp::Update(patch) = &ops[0] else {
        panic!("expected an update");
    };
    assert_eq!(patch.changes[0].expression.as_deref(), Some("now()"));
}

/// The inline editor in expression mode lists the dialect's values for the
/// column's type, without the grid asking the dialect while it is only drawn.
#[gpui::test]
fn the_expression_editor_suggests_the_dialects_type_values(cx: &mut TestAppContext) {
    use dbflux_components::components::data_table::selection::CellCoord;

    let (fixture, window) = fixture(cx);

    window.update(|window, app| {
        fixture.table_state.update(app, |state, cx| {
            assert!(state.editor_suggestions().0.is_empty());
            assert!(state.start_editing_as_expression(CellCoord::new(0, 2), window, cx));
            assert!(
                !state.editor_suggestions().0.is_empty(),
                "a timestamp column is suggested type values"
            );
            state.stop_editing(false, cx);
        })
    });
}

/// The grid takes expressions from a SQL table and from nothing else: the
/// table state is told so, and a collection grid is not.
#[gpui::test]
fn only_a_sql_table_grid_allows_expressions(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx);

    assert!(window.update(|_, app| fixture.table_state.read(app).expressions_allowed()));

    window.update(|_, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.source = DataSource::Collection {
                profile_id: fixture.profile_id,
                collection: dbflux_core::CollectionRef::new("testdb", "users"),
                pagination: Pagination::default(),
                total_docs: None,
            };
            panel.rebuild_table(None, cx);
        })
    });
    let table_state = window.update(|_, app| {
        fixture
            .panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("table state")
    });

    assert!(!window.update(|_, app| table_state.read(app).expressions_allowed()));
}

/// A collection has no column default: "Set to default" there stages
/// nothing instead of a NULL for the document field.
#[gpui::test]
fn set_default_on_a_collection_stages_nothing(cx: &mut TestAppContext) {
    let (fixture, window) = fixture(cx);

    window.update(|_, app| {
        fixture.panel.update(app, |panel, _| {
            panel.source = DataSource::Collection {
                profile_id: fixture.profile_id,
                collection: dbflux_core::CollectionRef::new("testdb", "users"),
                pagination: Pagination::default(),
                total_docs: None,
            };
        })
    });
    window.update(|_, app| {
        fixture
            .panel
            .update(app, |panel, cx| panel.handle_set_default(0, 1, cx))
    });

    assert!(staged(window, &fixture, 1).is_none());
}

/// A confirmation holds its insert while another insert leaves the buffer:
/// confirming writes the insert that was held, not the one that moved into
/// its place.
#[gpui::test]
fn a_held_insert_follows_the_inserts_removed_before_it(cx: &mut TestAppContext) {
    use dbflux_test_support::fake_driver::CrudOp;

    let (fixture, window) = fixture(cx);

    let held_idx = window.update(|_, app| {
        fixture.table_state.update(app, |state, _| {
            let buffer = state.edit_buffer_mut();
            buffer.add_pending_insert_after(
                0,
                vec![
                    CellValue::null(),
                    CellValue::text("first"),
                    CellValue::null(),
                ],
            );
            buffer.add_pending_insert_after(
                0,
                vec![
                    CellValue::null(),
                    CellValue::text("second"),
                    CellValue::expression("now()"),
                ],
            )
        })
    });
    window.update(|_, app| {
        fixture.panel.update(app, |panel, cx| {
            panel.handle_commit_insert(held_idx, false, cx)
        })
    });
    assert!(window.update(|_, app| fixture.panel.read(app).has_expression_confirm()));

    // The first insert lands while the confirmation is open.
    window.update(|_, app| {
        fixture.table_state.update(app, |state, _| {
            state.edit_buffer_mut().remove_pending_insert_by_idx(0)
        });
        fixture.panel.update(app, |panel, _| {
            panel.renumber_queued_inserts_after_removal(0)
        });
    });

    window.update(|window, app| {
        fixture
            .panel
            .update(app, |panel, cx| panel.confirm_expressions(window, cx))
    });
    window.run_until_parked();

    let ops = fixture.driver.stats().crud_ops;
    let CrudOp::Insert(insert) = ops.first().expect("the held insert was written") else {
        panic!("expected an insert");
    };
    let name = insert
        .assignments
        .iter()
        .find(|assignment| assignment.name == "name")
        .expect("name");
    assert_eq!(name.value, dbflux_core::Value::Text("second".into()));
}
