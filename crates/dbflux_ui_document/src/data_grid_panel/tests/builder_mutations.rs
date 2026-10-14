use super::*;

// =========================================================================
// Tier 1b — start_editing gate (DataGridPanel level)
// =========================================================================

/// Aliased-PK builder results have no editable binding and no pk_indices,
/// so rebuild_table leaves the table read-only. This closes the aliased-PK
/// behavior as a panel-level assertion without going through the async mutation path.
#[test]
fn aliased_pk_builder_result_computes_no_editable_binding() {
    use dbflux_core::{ProjectedColumn, Projection, SourceTable, VisualQuerySpec};

    let spec = VisualQuerySpec {
        source: SourceTable {
            schema: Some("public".to_string()),
            table: "users".to_string(),
            alias: "users".to_string(),
        },
        projection: Projection::Explicit(vec![ProjectedColumn {
            source_alias: "users".to_string(),
            column: "id".to_string(),
            alias: Some("user_id".to_string()),
        }]),
        joins: vec![],
        filter: None,
        group_by: vec![],
        aggregates: vec![],
        having: None,
        sort: vec![],
        limit: Some(100),
        offset: 0,
    };

    // Provide a warm pk_lookup that recognises "id" as a PK — the aliased
    // projection still returns None because the alias blocks the WHERE clause.
    let binding = spec.compute_editable_binding(|_source_table| Some(vec!["id".to_string()]));

    assert!(
        binding.is_none(),
        "aliased PK must produce None binding (read-only): \
         the WHERE clause cannot reference the alias"
    );
}

// =========================================================================
// Tier 2 — mutation roundtrip with FakeDriver CRUD recording
// =========================================================================

/// Positive: an editable-safe builder result (single source table, PK projected
/// unaliased) triggers exactly one UPDATE via `save_table_row`. The recorded
/// RowPatch carries the correct PK identity and the edited column assignment.
#[gpui::test]
fn save_table_row_issues_one_update_for_editable_builder_result(cx: &mut TestAppContext) {
    use dbflux_components::components::data_table::model::CellValue;
    use dbflux_core::{
        ColumnKind, ColumnMeta, ConnectedProfile, DbConfig, MutationPolicy, TableRef,
    };
    use dbflux_test_support::fake_driver::{CrudOp, FakeDriver};
    use std::path::PathBuf;

    init_test_runtime(cx);

    let profile_id = Uuid::new_v4();
    let fake_driver = FakeDriver::new(dbflux_core::DbKind::SQLite);

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let storage_runtime = StorageRuntime::in_memory().expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage_runtime).expect("test storage setup")
        })
    });

    cx.update(|cx| {
        app_state.update(cx, |app, _cx| {
            let profile = dbflux_core::ConnectionProfile::new(
                "fake-sqlite",
                DbConfig::SQLite {
                    path: PathBuf::from(":memory:"),
                    connection_id: None,
                },
            );
            let connection = fake_driver
                .connect_arc(&profile)
                .expect("FakeDriver connection must succeed");

            let connected = ConnectedProfile {
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
                active_database: None,
                redis_key_cache: Default::default(),
                database_connections: Default::default(),
                proxy_tunnel: None,
            };
            app.connections_mut().insert(profile_id, connected);
        });
    });

    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id,
                database: None,
                table: TableRef::with_schema("public", "users"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: None,
            };

            let mut panel =
                DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx);

            // Two-column result: id (PK), name (Source).
            let columns = vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_name: "int4".to_string(),
                    kind: ColumnKind::Integer,
                    nullable: false,
                    is_primary_key: false,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_name: "text".to_string(),
                    kind: ColumnKind::Text,
                    nullable: true,
                    is_primary_key: false,
                },
            ];
            let rows = vec![vec![
                dbflux_core::Value::Int(1),
                dbflux_core::Value::Text("alice".to_string()),
            ]];
            panel.result = QueryResult::table(columns, rows, None, std::time::Duration::ZERO);
            panel.pk_columns = vec!["id".to_string()];

            panel
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel must be created");

    // Rebuild the table so table_state is populated and pk_indices are set.
    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.rebuild_table(None, cx);
        });
    });

    // Confirm table_state is editable (PK column was found).
    let is_editable = window.update(|_, app| {
        let p = panel.read(app);
        p.grid_table
            .table_state
            .as_ref()
            .expect("table_state must exist after rebuild_table")
            .read(app)
            .is_editable()
    });
    assert!(
        is_editable,
        "table must be editable after rebuild_table with a valid PK column"
    );

    // Call save_table_row directly with a single column-1 change (name → "bob").
    let new_cell = CellValue::text("bob");
    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.save_table_row(
                profile_id,
                None,
                TableRef::with_schema("public", "users"),
                0,
                &[(1, &new_cell)],
                cx,
            );
        });
    });

    // Settle background tasks so the async CRUD op completes.
    cx.run_until_parked();

    // Verify exactly one UPDATE was recorded.
    let ops = fake_driver.stats().crud_ops;
    assert_eq!(
        ops.len(),
        1,
        "save_table_row must issue exactly one update_row call; got: {:?}",
        ops.len()
    );

    let CrudOp::Update(patch) = &ops[0] else {
        panic!("expected CrudOp::Update, got something else");
    };

    // Identity: PK column "id" with value Int(1).
    let identity_cols = patch.identity.columns();
    let identity_vals = patch.identity.values();
    assert_eq!(
        identity_cols,
        &["id"],
        "identity must use the PK column name"
    );
    assert_eq!(
        identity_vals,
        &[dbflux_core::Value::Int(1)],
        "identity must carry the PK value from row 0"
    );

    // Change: column "name" assigned the new value "bob".
    assert_eq!(patch.changes.len(), 1, "patch must have exactly one change");
    assert_eq!(
        patch.changes[0].name, "name",
        "change must target the 'name' column"
    );
    assert_eq!(
        patch.changes[0].value,
        dbflux_core::Value::Text("bob".to_string()),
        "change must carry the new cell value"
    );
}

/// The panel's staging paths are handed *visual* rows while the edit buffer
/// is keyed by source rows. This covers the three that reach the buffer
/// directly — paste, set-default and set-null — on a grid where a pending
/// insert sits between the base rows, so the two index spaces disagree.
#[gpui::test]
fn staging_paths_write_the_row_the_grid_shows(cx: &mut TestAppContext) {
    use dbflux_components::components::data_table::model::CellValue;
    use dbflux_components::components::data_table::selection::CellCoord;
    use dbflux_core::{ColumnInfo, TableInfo};
    use dbflux_test_support::fake_driver::FakeDriver;

    init_test_runtime(cx);

    let profile_id = Uuid::new_v4();
    let app_state = isolated_test_app_state(cx);
    let fake_driver = FakeDriver::new(dbflux_core::DbKind::SQLite);

    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            use dbflux_core::{ConnectedProfile, DbConfig, MutationPolicy};

            let profile = dbflux_core::ConnectionProfile::new(
                "test",
                DbConfig::SQLite {
                    path: std::path::PathBuf::from(":memory:"),
                    connection_id: None,
                },
            );
            let connection = fake_driver
                .connect_arc(&profile)
                .expect("FakeDriver connection must succeed");
            let connected = ConnectedProfile {
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
            };
            app.connections_mut().insert(profile_id, connected);

            // `handle_set_default` reads the column's default from here.
            app.set_table_details(
                profile_id,
                "testdb".to_string(),
                Some("public".to_string()),
                "users".to_string(),
                TableInfo {
                    name: "users".to_string(),
                    schema: Some("public".to_string()),
                    columns: Some(vec![
                        ColumnInfo {
                            name: "id".to_string(),
                            type_name: "int4".to_string(),
                            nullable: false,
                            is_primary_key: true,
                            default_value: None,
                            enum_values: None,
                        },
                        ColumnInfo {
                            name: "name".to_string(),
                            type_name: "text".to_string(),
                            nullable: true,
                            is_primary_key: false,
                            default_value: Some("anonymous".to_string()),
                            enum_values: None,
                        },
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

    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    // Kept out of the window's tree: rendering the panel would drain its
    // pending actions into a query the stub connection cannot answer.
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
                ColumnMeta {
                    name: "id".to_string(),
                    type_name: "int4".to_string(),
                    kind: ColumnKind::Integer,
                    nullable: false,
                    is_primary_key: false,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_name: "text".to_string(),
                    kind: ColumnKind::Text,
                    nullable: true,
                    is_primary_key: false,
                },
            ];
            let rows = vec![
                vec![
                    dbflux_core::Value::Int(1),
                    dbflux_core::Value::Text("alice".to_string()),
                ],
                vec![
                    dbflux_core::Value::Int(2),
                    dbflux_core::Value::Text("bob".to_string()),
                ],
            ];
            panel.result = QueryResult::table(columns, rows, None, Duration::ZERO);
            panel.pk_columns = vec!["id".to_string()];
            panel
        });
        panel_handle.replace(Some(panel));
        Harness
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel must be created");

    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.rebuild_table(None, cx));
    });

    let table_state = window.update(|_, app| {
        panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("table_state must exist after rebuild_table")
    });
    assert!(
        window.update(|_, app| table_state.read(app).is_editable()),
        "the fixture grid must be editable"
    );

    let staged = |window: &mut gpui::VisualTestContext, row: usize| -> Vec<(usize, String)> {
        window.update(|_, app| {
            table_state
                .read(app)
                .edit_buffer()
                .row_changes(row)
                .into_iter()
                .map(|(col, value)| (col, value.display_text().to_string()))
                .collect()
        })
    };
    let insert_cell =
        |window: &mut gpui::VisualTestContext, insert_idx: usize, col: usize| -> String {
            window
                .update(|_, app| {
                    table_state
                        .read(app)
                        .edit_buffer()
                        .get_pending_insert_by_idx(insert_idx)
                        .and_then(|cells| cells.get(col))
                        .map(|cell| cell.display_text().to_string())
                })
                .unwrap_or_default()
        };

    // Base row 1 is visual row 1 while no insert is staged.
    window.update(|window, app| {
        app.write_to_clipboard(gpui::ClipboardItem::new_string("pasted".to_string()));
        table_state.update(app, |state, cx| state.select_cell(CellCoord::new(1, 1), cx));
        panel.update(app, |panel, cx| panel.handle_paste(window, cx));
    });
    assert_eq!(
        staged(window, 1),
        vec![(1usize, "pasted".to_string())],
        "paste must stage on the selected row"
    );

    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.handle_set_default(1, 1, cx));
    });
    assert_eq!(
        staged(window, 1),
        vec![(1usize, "anonymous".to_string())],
        "set-default must stage the column's default on the selected row"
    );

    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.handle_set_null(1, 1, cx));
    });
    assert_eq!(
        staged(window, 1),
        vec![(1usize, "NULL".to_string())],
        "set-null must overtake the value staged before it"
    );

    // The insert takes visual row 1, pushing base row 1 down to visual row 2.
    let insert_idx = window.update(|_, app| {
        table_state.update(app, |state, cx| {
            let insert_idx = state
                .edit_buffer_mut()
                .add_pending_insert_after(0, vec![CellValue::text(""), CellValue::text("")]);
            cx.notify();
            insert_idx
        })
    });

    window.update(|window, app| {
        app.write_to_clipboard(gpui::ClipboardItem::new_string("inserted".to_string()));
        table_state.update(app, |state, cx| state.select_cell(CellCoord::new(1, 1), cx));
        panel.update(app, |panel, cx| panel.handle_paste(window, cx));
    });
    assert_eq!(
        insert_cell(window, insert_idx, 1),
        "inserted",
        "a row the user added must take the pasted value"
    );
    assert_eq!(
        staged(window, 1),
        vec![(1usize, "NULL".to_string())],
        "the base row under the insert must not be written to"
    );

    // Visual row 2 is base row 1 again, one past the insert.
    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.handle_set_default(2, 1, cx));
    });
    assert_eq!(
        staged(window, 1),
        vec![(1usize, "anonymous".to_string())],
        "a row below a pending insert must still be reachable by its visual index"
    );
    assert!(
        staged(window, 0).is_empty(),
        "no staging path may write to a row the user did not pick"
    );
}

/// Copy flattens tabs and line breaks and writes a placeholder for a
/// value it cannot spell out, so pasting a cell's copied text back onto
/// it, or onto another cell holding the same kind of value, must not
/// stage that text as a change.
#[gpui::test]
fn pasting_a_cells_copied_text_back_is_not_a_change(cx: &mut TestAppContext) {
    use dbflux_components::components::data_table::selection::CellCoord;

    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    // Kept out of the window's tree: rendering the panel would drain its
    // pending actions into a query nothing can answer.
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

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id: Uuid::nil(),
                database: Some("testdb".to_string()),
                table: TableRef::with_schema("public", "notes"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: None,
            };
            let mut panel =
                DataGridPanel::new_internal(source, app_state.clone(), Vec::new(), window, cx);

            let column = |name: &str, type_name: &str, kind: ColumnKind| ColumnMeta {
                name: name.to_string(),
                type_name: type_name.to_string(),
                kind,
                nullable: true,
                is_primary_key: false,
            };
            let columns = vec![
                column("id", "int4", ColumnKind::Integer),
                column("note", "text", ColumnKind::Text),
                column("blob", "bytea", ColumnKind::Unknown),
                column("shape", "geometry", ColumnKind::Unknown),
            ];
            let rows = vec![
                vec![
                    dbflux_core::Value::Int(1),
                    dbflux_core::Value::Text("a\nb".to_string()),
                    dbflux_core::Value::Bytes(vec![1; 16]),
                    dbflux_core::Value::Unsupported("geometry".to_string()),
                ],
                vec![
                    dbflux_core::Value::Int(2),
                    dbflux_core::Value::Text("a\tb".to_string()),
                    dbflux_core::Value::Bytes(vec![2; 16]),
                    dbflux_core::Value::Unsupported("geometry".to_string()),
                ],
            ];
            panel.result = QueryResult::table(columns, rows, None, Duration::ZERO);
            panel.pk_columns = vec!["id".to_string()];
            panel
        });
        panel_handle.replace(Some(panel));
        Harness
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel must be created");

    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.rebuild_table(None, cx));
    });

    let table_state = window.update(|_, app| {
        panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("table_state must exist after rebuild_table")
    });
    assert!(
        window.update(|_, app| table_state.read(app).is_editable()),
        "the fixture grid must be editable"
    );

    let copy_and_paste =
        |window: &mut gpui::VisualTestContext, from: CellCoord, to: CellCoord| -> (String, bool) {
            window.update(|window, app| {
                let copied = table_state.update(app, |state, cx| {
                    state.select_cell(from, cx);
                    state.copy_selection().expect("one selected cell copies")
                });
                app.write_to_clipboard(gpui::ClipboardItem::new_string(copied.clone()));

                table_state.update(app, |state, cx| state.select_cell(to, cx));
                panel.update(app, |panel, cx| panel.handle_paste(window, cx));

                let staged = table_state.read(app).has_pending_operations();
                (copied, staged)
            })
        };

    let pastes = [
        (CellCoord::new(0, 1), CellCoord::new(0, 1)),
        (CellCoord::new(1, 1), CellCoord::new(1, 1)),
        (CellCoord::new(0, 2), CellCoord::new(0, 2)),
        (CellCoord::new(0, 2), CellCoord::new(1, 2)),
        (CellCoord::new(0, 3), CellCoord::new(0, 3)),
        (CellCoord::new(0, 3), CellCoord::new(1, 3)),
    ];

    for (from, to) in pastes {
        let (copied, staged) = copy_and_paste(window, from, to);

        assert!(
            !staged,
            "pasting {copied:?} copied from {from:?} onto {to:?} staged a change"
        );
    }

    window.update(|window, app| {
        app.write_to_clipboard(gpui::ClipboardItem::new_string("a  b".to_string()));
        table_state.update(app, |state, cx| state.select_cell(CellCoord::new(0, 1), cx));
        panel.update(app, |panel, cx| panel.handle_paste(window, cx));
    });
    assert!(
        window.update(|_, app| table_state.read(app).edit_buffer().is_cell_dirty(0, 1)),
        "pasting text that is not the cell's copied form is a change"
    );
}

#[gpui::test]
fn grouped_result_after_rebuild_leaves_record_mode(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

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
                DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx);
            panel.set_result(zero_row_result(), cx);
            panel
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            assert!(panel.record_view_available());
            panel.set_record_mode(true, cx);
            assert!(panel.record_mode());

            // The next query comes back grouped. The aggregate has no row
            // to transpose, so the rebuild must drop the mode instead of
            // reapplying the cached flag around the guard.
            panel.builder.current_visual_spec = Some(make_grouped_spec());
            panel.rebuild_table(None, cx);

            assert!(
                !panel.record_mode(),
                "a grouped result must leave record mode"
            );
            assert!(!panel.record_view_available());
            let table_in_record_mode = panel
                .grid_table
                .table_state
                .as_ref()
                .expect("rebuild creates a table state")
                .read(cx)
                .record_mode();
            assert!(
                !table_in_record_mode,
                "the fresh table state must not inherit record mode"
            );
        });
    });
}

#[gpui::test]
fn record_view_is_offered_only_while_the_grid_is_shown(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::QueryResult {
                result: Arc::new(zero_row_result()),
                original_query: "SELECT id, name FROM users".to_string(),
                profile_id: None,
            };

            let mut panel =
                DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx);
            panel.set_result(zero_row_result(), cx);
            panel
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            assert!(panel.record_view_available());

            // JSON, text and raw are result views without a grid, so the
            // toggle is not offered and the command does nothing there.
            panel.set_result_view_mode(super::super::ResultViewMode::Json, cx);
            assert!(!panel.record_view_available());
            panel.set_record_mode(true, cx);
            assert!(
                !panel.record_mode(),
                "record mode must not switch on behind a result view"
            );

            panel.set_result_view_mode(super::super::ResultViewMode::Table, cx);
            assert!(panel.record_view_available());
        });
    });
}
