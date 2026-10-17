use super::*;

/// A panel showing `id` and `note` of a table that also has a
/// `quantity` column between them, with `note` of the only row edited.
fn panel_with_edited_partial_row(
    cx: &mut TestAppContext,
) -> (gpui::Entity<DataGridPanel>, &mut VisualTestContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id: Uuid::nil(),
                database: Some("app".to_string()),
                table: TableRef::with_schema("public", "order_lines"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: Some(1),
            };

            let mut panel = DataGridPanel::new_internal(
                source,
                app_state.clone(),
                vec!["id".to_string()],
                window,
                cx,
            );

            panel.set_result(
                QueryResult::table(
                    vec![key_column("id", true), key_column("note", false)],
                    vec![vec![
                        dbflux_core::Value::Int(1),
                        dbflux_core::Value::Text("before".to_string()),
                    ]],
                    None,
                    Duration::ZERO,
                ),
                cx,
            );

            if let Some(table_state) = panel.grid_table.table_state.clone() {
                table_state.update(cx, |state, _cx| {
                    state.edit_buffer_mut().set_cell(
                        0,
                        1,
                        dbflux_components::components::data_table::model::CellValue::text("saved"),
                    );
                });
            }

            panel
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    (panel, window)
}

fn shown_cells(panel: &gpui::Entity<DataGridPanel>, window: &mut VisualTestContext) -> Vec<String> {
    window.update(|_, app| {
        let state = panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("a table");
        let state = state.read(app);
        let model = state.model();

        (0..model.col_count())
            .map(|col| {
                let base = model.cell(0, col).cloned().expect("a cell");
                state
                    .edit_buffer()
                    .get_cell(0, col, &base)
                    .display_text()
                    .to_string()
            })
            .collect()
    })
}

#[gpui::test]
fn a_saved_row_wider_than_the_grid_keeps_each_value_in_its_column(cx: &mut TestAppContext) {
    let (panel, window) = panel_with_edited_partial_row(cx);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.handle_save_result(
                0,
                Ok(dbflux_core::CrudResult::success(vec![
                    dbflux_core::Value::Int(1),
                    dbflux_core::Value::Int(5),
                    dbflux_core::Value::Text("saved".to_string()),
                ])),
                false,
                cx,
            )
        })
    });
    window.run_until_parked();

    assert_eq!(
        shown_cells(&panel, window),
        vec!["1".to_string(), "saved".to_string()],
        "the table's quantity column must not shift into note"
    );
}

#[gpui::test]
fn a_saved_row_matching_the_grid_replaces_the_row(cx: &mut TestAppContext) {
    let (panel, window) = panel_with_edited_partial_row(cx);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.handle_save_result(
                0,
                Ok(dbflux_core::CrudResult::success(vec![
                    dbflux_core::Value::Int(1),
                    dbflux_core::Value::Text("from the server".to_string()),
                ])),
                false,
                cx,
            )
        })
    });
    window.run_until_parked();

    assert_eq!(
        shown_cells(&panel, window),
        vec!["1".to_string(), "from the server".to_string()]
    );
}

// =========================================================================
// Reloads keep or refuse to drop pending edits (DBF-262)
// =========================================================================

/// Connection that answers a table browse with whatever `rows` holds when
/// the query runs, so a reload lands through the real request path.
struct StubBrowseConnection {
    rows: Arc<std::sync::Mutex<QueryResult>>,
}

impl dbflux_core::Connection for StubBrowseConnection {
    /// Facets as a driver of the stub's category declares them.
    fn collection_facets(
        &self,
        _collection: &dbflux_core::CollectionRef,
    ) -> Vec<dbflux_core::ObjectFacet> {
        dbflux_core::category_collection_facets(self.metadata().category)
    }

    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        use dbflux_core::{
            DatabaseCategory, DriverCapabilities, DriverMetadata, Icon as CoreIcon, QueryLanguage,
            TransferFamily,
        };

        static META: std::sync::OnceLock<DriverMetadata> = std::sync::OnceLock::new();
        META.get_or_init(|| DriverMetadata {
            id: "stub-browse".to_string(),
            display_name: "Stub".to_string(),
            description: "test".to_string(),
            category: DatabaseCategory::Relational,
            transfer_family: TransferFamily::Sql,
            deployment_class: None,
            query_language: QueryLanguage::Sql,
            capabilities: DriverCapabilities::empty(),
            default_port: None,
            uri_scheme: "stub".to_string(),
            icon: CoreIcon::Database,
            syntax: None,
            query: None,
            mutation: None,
            ddl: None,
            transactions: None,
            limits: None,
            ssl_modes: None,
            ssl_cert_fields: None,
            classification_override: None,
            default_chunk_size: None,
            supports_lock_timeout: false,
            editor_profile: None,
        })
    }

    fn kind(&self) -> dbflux_core::DbKind {
        dbflux_core::DbKind::SQLite
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        dbflux_core::SchemaLoadingStrategy::SingleDatabase
    }

    #[expect(
        clippy::unimplemented,
        reason = "test-only stub connection: raw browse does not call the dialect"
    )]
    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        unimplemented!("StubBrowseConnection::dialect is not reached by a raw browse")
    }

    fn ping(&self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn execute(&self, _: &dbflux_core::QueryRequest) -> Result<QueryResult, dbflux_core::DbError> {
        Err(dbflux_core::DbError::NotSupported("stub".to_string()))
    }

    fn browse_table(
        &self,
        _: &dbflux_core::TableBrowseRequest,
    ) -> Result<QueryResult, dbflux_core::DbError> {
        let rows = self
            .rows
            .lock()
            .map_err(|_| dbflux_core::DbError::NotSupported("poisoned".to_string()))?;
        Ok(rows.clone())
    }

    fn cancel(&self, _: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }
}

/// Registers a connected profile whose table browse returns `reloaded`.
fn register_browse_stub(
    app_state: &gpui::Entity<AppStateEntity>,
    reloaded: QueryResult,
    cx: &mut TestAppContext,
) -> Uuid {
    let profile_id = Uuid::new_v4();

    cx.update(|cx| {
        app_state.update(cx, |app, _cx| {
            let profile = dbflux_core::ConnectionProfile::new(
                "test",
                dbflux_core::DbConfig::SQLite {
                    path: std::path::PathBuf::from(":memory:"),
                    connection_id: None,
                },
            );
            let connected = dbflux_core::ConnectedProfile {
                profile,
                connection: Arc::new(StubBrowseConnection {
                    rows: Arc::new(std::sync::Mutex::new(reloaded)),
                }),
                schema: None,
                mutation_policy: dbflux_core::MutationPolicy::default(),
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

    profile_id
}

fn keyed_table_panel(
    window: &mut gpui::VisualTestContext,
    app_state: gpui::Entity<AppStateEntity>,
    profile_id: Uuid,
    pk_columns: Vec<String>,
) -> gpui::Entity<DataGridPanel> {
    window.update(|window, app| {
        let source = DataSource::Table {
            profile_id,
            database: Some("app".to_string()),
            table: TableRef::with_schema("public", "users"),
            pagination: Pagination::default(),
            order_by: Vec::new(),
            total_rows: Some(2),
        };

        let panel =
            app.new(|cx| DataGridPanel::new_internal(source, app_state, pk_columns, window, cx));
        panel.update(app, |panel, cx| {
            panel.set_result(keyed_result(&["1", "2"]), cx);
        });
        panel
    })
}

fn stage_name(panel: &DataGridPanel, row: usize, value: &str, cx: &mut gpui::App) {
    let table_state = panel.grid_table.table_state.clone().expect("table state");
    table_state.update(cx, |state, _cx| {
        state.stage_base_cell_value(
            row,
            1,
            dbflux_components::components::data_table::model::CellValue::text(value),
        );
    });
}

fn staged_names(panel: &DataGridPanel, cx: &gpui::App) -> Vec<(usize, String)> {
    let table_state = panel.grid_table.table_state.as_ref().expect("table state");
    let buffer = table_state.read(cx).edit_buffer();

    let mut staged: Vec<(usize, String)> = buffer
        .dirty_rows()
        .into_iter()
        .flat_map(|row| {
            buffer
                .row_changes(row)
                .into_iter()
                .map(move |(_, value)| (row, value.display_text().to_string()))
        })
        .collect();
    staged.sort();
    staged
}

/// Queues the reload a saved mutation asks for and issues it the way the
/// next render does, leaving the request in flight.
fn issue_mutation_reload(
    window: &mut gpui::VisualTestContext,
    panel: &gpui::Entity<DataGridPanel>,
) {
    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.queue_reload_after_mutation(cx);
            assert!(panel.pending.refresh, "the saved mutation must reload");

            panel.process_pending_actions(window, cx);
        });
    });
}

#[gpui::test]
fn a_reload_after_a_save_keeps_edits_on_other_rows(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    // The rows come back in another order: the edit must follow the row
    // with id 2, not stay at index 1.
    let profile_id = register_browse_stub(&app_state, keyed_result(&["2", "1"]), cx);
    let window = cx.add_empty_window();
    let panel = keyed_table_panel(window, app_state, profile_id, vec!["id".to_string()]);

    window.update(|_, app| {
        panel.update(app, |panel, cx| stage_name(panel, 1, "bob", cx));
    });
    issue_mutation_reload(window, &panel);
    window.run_until_parked();

    window.update(|_, app| {
        let panel = panel.read(app);
        assert_eq!(
            panel.result.rows[0][0],
            dbflux_core::Value::Text("2".to_string())
        );
        assert_eq!(staged_names(panel, app), vec![(0, "bob".to_string())]);
    });
    assert_eq!(last_toast_title(window), None);
}

#[gpui::test]
fn a_reload_after_a_save_reports_edits_whose_row_is_gone(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let profile_id = register_browse_stub(&app_state, keyed_result(&["2"]), cx);
    let window = cx.add_empty_window();
    let panel = keyed_table_panel(window, app_state, profile_id, vec!["id".to_string()]);

    window.update(|_, app| {
        panel.update(app, |panel, cx| stage_name(panel, 0, "alice", cx));
    });
    issue_mutation_reload(window, &panel);
    window.run_until_parked();

    window.update(|_, app| {
        assert!(staged_names(panel.read(app), app).is_empty());
    });
    assert_eq!(
        last_toast_title(window),
        Some(crate::labels::grid_edits_dropped_on_reload(1)),
        "an edit that could not be carried over must not vanish silently"
    );
}

/// A result that lacks part of a composite primary key cannot address one
/// row per save, so the grid must not take edits (a column projection can
/// drop a key column before the key is known).
#[gpui::test]
fn a_result_missing_part_of_a_composite_key_is_read_only(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let partial = keyed_table_panel(
        window,
        app_state.clone(),
        Uuid::nil(),
        vec!["tenant_id".to_string(), "id".to_string()],
    );
    let whole = keyed_table_panel(window, app_state, Uuid::nil(), vec!["id".to_string()]);

    window.update(|_, app| {
        let editable = |panel: &gpui::Entity<DataGridPanel>, app: &gpui::App| {
            panel
                .read(app)
                .grid_table
                .table_state
                .as_ref()
                .expect("table state")
                .read(app)
                .is_editable()
        };

        assert!(!editable(&partial, app), "tenant_id is not in the result");
        assert!(editable(&whole, app), "every key column is in the result");
    });
}

#[gpui::test]
fn a_reload_after_a_save_without_a_primary_key_is_refused(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = keyed_table_panel(window, app_state, Uuid::nil(), Vec::new());

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            stage_name(panel, 1, "bob", cx);
            panel.queue_reload_after_mutation(cx);

            assert!(
                !panel.pending.refresh,
                "edits addressed by position cannot survive a reload"
            );
            assert!(!panel.pending.refresh_keeps_edits);
        });
    });

    window.update(|_, app| {
        assert_eq!(
            staged_names(panel.read(app), app),
            vec![(1, "bob".to_string())]
        );
    });
    assert_eq!(
        last_toast_title(window),
        Some(crate::labels::grid_reload_blocked_by_pending_edits())
    );
}

#[gpui::test]
fn a_reload_after_a_save_without_edits_reloads_as_before(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let profile_id = register_browse_stub(&app_state, keyed_result(&["2"]), cx);
    let window = cx.add_empty_window();
    let panel = keyed_table_panel(window, app_state, profile_id, Vec::new());

    issue_mutation_reload(window, &panel);
    window.run_until_parked();

    window.update(|_, app| {
        assert_eq!(panel.read(app).result.row_count(), 1);
    });
    assert_eq!(last_toast_title(window), None);
}

/// A local re-sort rebuilds the table while the save reload is still
/// loading. The keep-edits intent belongs to the reload's request, so the
/// sort rebuild cannot take it, and the reload still carries the edit
/// staged after the sort.
#[gpui::test]
fn a_local_sort_rebuild_during_a_save_reload_leaves_it_the_intent(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let profile_id = register_browse_stub(&app_state, keyed_result(&["2", "1"]), cx);
    let window = cx.add_empty_window();
    let panel = keyed_table_panel(window, app_state, profile_id, vec!["id".to_string()]);

    issue_mutation_reload(window, &panel);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            assert!(
                !panel.grid_table.keep_edits_on_reload,
                "the in-flight request holds the intent, not the panel"
            );

            // What a pending local-sort rebuild runs.
            panel.rebuild_table(None, cx);

            stage_name(panel, 1, "bob", cx);
        });
    });
    window.run_until_parked();

    window.update(|_, app| {
        assert_eq!(
            staged_names(panel.read(app), app),
            vec![(0, "bob".to_string())],
            "the reload must still carry the edit on the row with id 2"
        );
    });
}

#[gpui::test]
fn a_save_reload_that_cannot_run_does_not_leak_its_intent(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = keyed_table_panel(window, app_state, Uuid::nil(), vec!["id".to_string()]);

    // The profile is not connected, so the request fails before it starts.
    issue_mutation_reload(window, &panel);
    window.run_until_parked();

    window.update(|_, app| {
        let panel = panel.read(app);
        assert!(!panel.grid_table.keep_edits_on_reload);
        assert!(!panel.pending.refresh_keeps_edits);
    });
    assert_eq!(
        last_toast_title(window),
        Some(dbflux_i18n::t!(
            "document.data.grid.error.connection_not_found"
        ))
    );
}

#[gpui::test]
fn page_change_with_pending_edits_keeps_them_and_skips_the_query(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = keyed_table_panel(window, app_state, Uuid::nil(), vec!["id".to_string()]);

    let handled = window.update(|window, app| {
        panel.update(app, |panel, cx| {
            stage_name(panel, 1, "bob", cx);
            panel.dispatch_command(Command::ResultsNextPage, window, cx)
        })
    });
    window.run_until_parked();

    assert!(handled);
    window.update(|_, app| {
        let panel = panel.read(app);

        assert_eq!(staged_names(panel, app), vec![(1, "bob".to_string())]);
        assert_eq!(
            panel.source.pagination().map(|p| p.offset()),
            Some(0),
            "the page must not move"
        );
        assert!(panel.refresh.state != GridState::Loading);
        assert!(!panel.runner.is_primary_active());
    });
    assert_eq!(
        last_toast_title(window),
        Some(crate::labels::grid_reload_blocked_by_pending_edits())
    );
}

#[gpui::test]
fn page_change_without_pending_edits_runs_the_query(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = keyed_table_panel(window, app_state, Uuid::nil(), vec!["id".to_string()]);

    let handled = window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.dispatch_command(Command::ResultsNextPage, window, cx)
        })
    });
    window.run_until_parked();

    assert!(handled);
    assert_eq!(
        last_toast_title(window),
        Some(dbflux_i18n::t!(
            "document.data.grid.error.connection_not_found"
        ))
    );
}

/// A pending delete alone counts as an unsaved edit: the reload would
/// drop it just the same.
#[gpui::test]
fn filter_change_with_a_pending_delete_keeps_the_filter_and_the_delete(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = keyed_table_panel(window, app_state, Uuid::nil(), vec!["id".to_string()]);

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel
                .filter_bar
                .filter_input
                .update(cx, |input, cx| input.set_value("id = '1'", window, cx));

            let table_state = panel.grid_table.table_state.clone().expect("table state");
            table_state.update(cx, |state, _cx| state.edit_buffer_mut().mark_for_delete(0));

            panel.replace_filter_and_reload("", window, cx);
        });
    });
    window.run_until_parked();

    window.update(|_, app| {
        let panel = panel.read(app);
        let table_state = panel.grid_table.table_state.as_ref().expect("table state");

        assert!(table_state.read(app).edit_buffer().is_pending_delete(0));
        assert_eq!(
            panel.filter_bar.filter_input.read(app).value().to_string(),
            "id = '1'",
            "a refused filter change must leave the filter the rows were loaded with"
        );
        assert!(!panel.runner.is_primary_active());
    });
    assert_eq!(
        last_toast_title(window),
        Some(crate::labels::grid_reload_blocked_by_pending_edits())
    );
}

#[gpui::test]
fn filter_change_without_pending_edits_runs_the_query(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = keyed_table_panel(window, app_state, Uuid::nil(), vec!["id".to_string()]);

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel
                .filter_bar
                .filter_input
                .update(cx, |input, cx| input.set_value("id = '1'", window, cx));

            panel.replace_filter_and_reload("", window, cx);
        });
    });
    window.run_until_parked();

    window.update(|_, app| {
        assert_eq!(
            panel
                .read(app)
                .filter_bar
                .filter_input
                .read(app)
                .value()
                .to_string(),
            ""
        );
    });
    assert_eq!(
        last_toast_title(window),
        Some(dbflux_i18n::t!(
            "document.data.grid.error.connection_not_found"
        ))
    );
}

/// Builds the keyed panel on an unconnected profile, optionally stages an
/// edit, and runs `act` on it.
fn with_keyed_panel<R>(
    cx: &mut TestAppContext,
    stage_edit: bool,
    act: impl FnOnce(&mut DataGridPanel, &mut gpui::Window, &mut gpui::Context<DataGridPanel>) -> R,
) -> (R, gpui::Entity<DataGridPanel>, &mut gpui::VisualTestContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = keyed_table_panel(window, app_state, Uuid::nil(), vec!["id".to_string()]);

    let outcome = window.update(|window, app| {
        panel.update(app, |panel, cx| {
            if stage_edit {
                stage_name(panel, 1, "bob", cx);
            }
            act(panel, window, cx)
        })
    });
    window.run_until_parked();

    (outcome, panel, window)
}

fn assert_blocked_with_edit_kept(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut gpui::VisualTestContext,
) {
    window.update(|_, app| {
        assert_eq!(
            staged_names(panel.read(app), app),
            vec![(1, "bob".to_string())]
        );
    });
    assert_eq!(
        last_toast_title(window),
        Some(crate::labels::grid_reload_blocked_by_pending_edits())
    );
}

fn request_server_sort(panel: &mut DataGridPanel, cx: &mut gpui::Context<DataGridPanel>) {
    let table_state = panel.grid_table.table_state.clone().expect("table state");
    table_state.update(cx, |state, cx| {
        state.set_sort(Some(TableSortState::ascending(1)), cx);
    });
}

#[gpui::test]
fn server_sort_with_pending_edits_is_refused_and_keeps_the_loaded_sort(cx: &mut TestAppContext) {
    let (_, panel, window) = with_keyed_panel(cx, true, |panel, _, cx| {
        request_server_sort(panel, cx);
    });

    window.update(|_, app| {
        let panel = panel.read(app);
        assert!(
            panel.pending.requery.is_none(),
            "the sort must not re-query"
        );
        assert_eq!(
            panel
                .grid_table
                .table_state
                .as_ref()
                .expect("table state")
                .read(app)
                .sort(),
            None,
            "the header must show the sort the rows were loaded with"
        );
    });
    assert_blocked_with_edit_kept(&panel, window);
}

#[gpui::test]
fn server_sort_without_pending_edits_queues_the_query(cx: &mut TestAppContext) {
    let (_, panel, window) = with_keyed_panel(cx, false, |panel, _, cx| {
        request_server_sort(panel, cx);
    });

    window.update(|_, app| {
        assert!(panel.read(app).pending.requery.is_some());
    });
}

/// Panel over a static query result, which sorts in memory.
fn static_keyed_panel(
    cx: &mut TestAppContext,
    pk_columns: Vec<String>,
) -> (gpui::Entity<DataGridPanel>, &mut gpui::VisualTestContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();

    let panel = window.update(|window, app| {
        let source = DataSource::QueryResult {
            result: Arc::new(keyed_result(&["1", "2"])),
            original_query: "SELECT * FROM users".to_string(),
            profile_id: None,
        };

        let panel =
            app.new(|cx| DataGridPanel::new_internal(source, app_state, pk_columns, window, cx));
        panel.update(app, |panel, cx| {
            panel.set_result(keyed_result(&["1", "2"]), cx);
        });
        panel
    });

    (panel, window)
}

/// Sorts the key column descending through the table header, then runs
/// the rebuild the next render would.
fn sort_ids_descending(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut gpui::VisualTestContext,
    stage_edit: bool,
) {
    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            if stage_edit {
                stage_name(panel, 1, "bob", cx);
            }

            let table_state = panel.grid_table.table_state.clone().expect("table state");
            table_state.update(cx, |state, cx| {
                state.set_sort(Some(TableSortState::descending(0)), cx);
            });
        });
    });

    window.update(|window, app| {
        panel.update(app, |panel, cx| panel.process_pending_actions(window, cx));
    });
    window.run_until_parked();
}

fn first_id(panel: &gpui::Entity<DataGridPanel>, window: &mut gpui::VisualTestContext) -> String {
    window.update(|_, app| match &panel.read(app).result.rows[0][0] {
        dbflux_core::Value::Text(id) => id.clone(),
        other => panic!("unexpected key cell {other:?}"),
    })
}

#[gpui::test]
fn local_sort_with_pending_edits_keeps_them_on_their_rows(cx: &mut TestAppContext) {
    let (panel, window) = static_keyed_panel(cx, vec!["id".to_string()]);

    sort_ids_descending(&panel, window, true);

    assert_eq!(first_id(&panel, window), "2");
    window.update(|_, app| {
        assert_eq!(
            staged_names(panel.read(app), app),
            vec![(0, "bob".to_string())],
            "the edit must move with the row with id 2"
        );
    });
    assert_eq!(last_toast_title(window), None);
}

#[gpui::test]
fn local_sort_without_a_primary_key_and_with_edits_is_refused(cx: &mut TestAppContext) {
    let (panel, window) = static_keyed_panel(cx, Vec::new());

    sort_ids_descending(&panel, window, true);

    assert_eq!(first_id(&panel, window), "1", "the rows must not move");
    window.update(|_, app| {
        let panel = panel.read(app);
        assert_eq!(staged_names(panel, app), vec![(1, "bob".to_string())]);
        assert_eq!(
            panel
                .grid_table
                .table_state
                .as_ref()
                .expect("table state")
                .read(app)
                .sort(),
            None,
            "the header must show the order the rows are in"
        );
    });
    assert_eq!(
        last_toast_title(window),
        Some(crate::labels::grid_reload_blocked_by_pending_edits())
    );
}

#[gpui::test]
fn local_sort_without_pending_edits_sorts_as_before(cx: &mut TestAppContext) {
    let (panel, window) = static_keyed_panel(cx, Vec::new());

    sort_ids_descending(&panel, window, false);

    assert_eq!(first_id(&panel, window), "2");
    assert_eq!(last_toast_title(window), None);
}

#[gpui::test]
fn builder_run_with_pending_edits_is_refused(cx: &mut TestAppContext) {
    let (_, panel, window) = with_keyed_panel(cx, true, |panel, window, cx| {
        panel.builder.builder_draft_spec = Some(make_test_spec());
        panel.handle_builder_event(&super::super::BuilderEvent::RunRequested, window, cx);

        assert!(
            !panel.builder.filter_input_hidden,
            "the spec must not be applied"
        );
        assert!(!panel.pending.refresh);
    });

    assert_blocked_with_edit_kept(&panel, window);
}

#[gpui::test]
fn builder_run_without_pending_edits_applies_the_spec(cx: &mut TestAppContext) {
    let (_, panel, window) = with_keyed_panel(cx, false, |panel, window, cx| {
        panel.builder.builder_draft_spec = Some(make_test_spec());
        panel.handle_builder_event(&super::super::BuilderEvent::RunRequested, window, cx);

        assert!(panel.builder.filter_input_hidden);
    });

    window.update(|_, app| {
        assert!(panel.read(app).builder.builder_draft_spec.is_some());
    });
    assert_eq!(
        last_toast_title(window),
        Some(dbflux_i18n::t!(
            "document.data.grid.error.connection_not_found"
        ))
    );
}

#[gpui::test]
fn builder_reset_with_pending_edits_is_refused(cx: &mut TestAppContext) {
    let (_, panel, window) = with_keyed_panel(cx, true, |panel, window, cx| {
        panel.builder.builder_draft_spec = Some(make_test_spec());
        panel.handle_builder_event(&super::super::BuilderEvent::ResetRequested, window, cx);

        assert!(
            panel.builder.builder_draft_spec.is_some(),
            "the spec the rows were loaded with must stay"
        );
    });

    assert_blocked_with_edit_kept(&panel, window);
}

#[gpui::test]
fn builder_reset_without_pending_edits_clears_the_spec(cx: &mut TestAppContext) {
    with_keyed_panel(cx, false, |panel, window, cx| {
        panel.builder.builder_draft_spec = Some(make_test_spec());
        panel.handle_builder_event(&super::super::BuilderEvent::ResetRequested, window, cx);

        assert!(panel.builder.builder_draft_spec.is_none());
    });
}

#[gpui::test]
fn closing_the_builder_restores_the_where_filter(cx: &mut TestAppContext) {
    with_keyed_panel(cx, false, |panel, window, cx| {
        assert!(panel.filter_input_visible());

        panel.open_query_builder(window, cx);
        assert!(
            !panel.filter_input_visible(),
            "the builder owns composition while it is open"
        );

        panel.builder.builder_draft_spec = Some(make_test_spec());
        panel.builder.visual_select = Some(dbflux_core::SelectQuery {
            sql: "SELECT * FROM users".to_string(),
            params: Vec::new(),
        });

        panel.handle_builder_event(&super::super::BuilderEvent::CloseRequested, window, cx);

        assert!(
            panel.filter_input_visible(),
            "closing the builder must bring the WHERE filter back"
        );
        assert!(
            panel.builder.visual_select.is_none(),
            "an unrun draft must not replace the raw filter on the next reload"
        );
        assert!(panel.builder.builder_panel.is_some());
    });
}

#[gpui::test]
fn dismissing_the_rail_restores_the_where_filter(cx: &mut TestAppContext) {
    with_keyed_panel(cx, false, |panel, window, cx| {
        panel.open_query_builder(window, cx);
        assert!(!panel.filter_input_visible());

        panel.clear_inspector_state(cx);

        assert!(panel.filter_input_visible());
    });
}

struct ShortGridHost {
    panel: gpui::Entity<DataGridPanel>,
}

impl gpui::Render for ShortGridHost {
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        _cx: &mut gpui::Context<Self>,
    ) -> impl gpui::IntoElement {
        use gpui::{InteractiveElement as _, ParentElement as _, Styled as _};

        gpui::div().size_full().child(
            gpui::div()
                .debug_selector(|| "short-grid-host".to_string())
                .h(gpui::px(160.0))
                .flex()
                .flex_col()
                .overflow_hidden()
                .child(self.panel.clone()),
        )
    }
}

// Dragging the editor/results split down used to push the footer under
// the next bar while the rows kept their height.
#[gpui::test]
fn a_short_grid_keeps_its_footer_inside_the_pane(cx: &mut TestAppContext) {
    let (_, window) = rendered_short_grid(cx);

    let host = window
        .debug_bounds("short-grid-host")
        .expect("the host should render");
    let footer = window
        .debug_bounds("data-grid-footer")
        .expect("the footer should render");

    assert!(
        footer.origin.y >= host.origin.y
            && footer.origin.y + footer.size.height <= host.origin.y + host.size.height,
        "footer {footer:?} must stay inside the pane {host:?}"
    );
}

/// A table grid rendered inside a `ShortGridHost` window.
fn rendered_short_grid(
    cx: &mut TestAppContext,
) -> (gpui::Entity<DataGridPanel>, &mut gpui::VisualTestContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let host = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id: Uuid::nil(),
                database: Some("app".to_string()),
                table: TableRef::with_schema("public", "users"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: Some(2),
            };

            let panel = cx.new(|cx| {
                DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
            });
            panel.update(cx, |panel, cx| {
                panel.set_result(keyed_result(&["1", "2"]), cx);
            });
            panel_handle.replace(Some(panel.clone()));

            ShortGridHost { panel }
        });

        Root::new(host, window, cx)
    });
    window.run_until_parked();
    window.update(|_, _| {});

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("the host should build its panel");

    (panel, window)
}

/// Counts the builder rail openings `panel` emits from now on.
fn count_builder_openings(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut gpui::VisualTestContext,
) -> (Rc<RefCell<usize>>, gpui::Subscription) {
    let openings = Rc::new(RefCell::new(0));

    let subscription = window.update(|_, app| {
        let openings = openings.clone();
        app.subscribe(panel, move |_, event: &DataGridEvent, _| {
            if let DataGridEvent::OpenInspector {
                content_has_header, ..
            } = event
            {
                assert!(
                    *content_has_header,
                    "the builder draws the rail's only header"
                );
                *openings.borrow_mut() += 1;
            }
        })
    });

    (openings, subscription)
}

#[gpui::test]
fn builder_closed_with_its_button_stays_closed_across_tab_switches(cx: &mut TestAppContext) {
    let (_, panel, window) = with_keyed_panel(cx, false, |panel, window, cx| {
        panel.open_query_builder(window, cx);
        panel.handle_builder_event(&super::super::BuilderEvent::CloseRequested, window, cx);
    });

    let (openings, _subscription) = count_builder_openings(&panel, window);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.set_active_tab(false, cx);
            panel.set_active_tab(true, cx);
        });
    });
    window.run_until_parked();

    assert_eq!(*openings.borrow(), 0, "a closed builder must not reopen");

    window.update(|window, app| {
        panel.update(app, |panel, cx| panel.open_query_builder(window, cx));
    });
    window.run_until_parked();

    assert_eq!(*openings.borrow(), 1, "opening it again must still work");

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.set_active_tab(false, cx);
            panel.set_active_tab(true, cx);
        });
    });
    window.run_until_parked();

    assert_eq!(
        *openings.borrow(),
        2,
        "an open builder is re-mounted when its tab comes back"
    );
}

#[gpui::test]
fn builder_dismissed_from_the_rail_stays_closed_across_tab_switches(cx: &mut TestAppContext) {
    let (_, panel, window) = with_keyed_panel(cx, false, |panel, window, cx| {
        panel.open_query_builder(window, cx);
        panel.clear_inspector_state(cx);
    });

    let (openings, _subscription) = count_builder_openings(&panel, window);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.set_active_tab(false, cx);
            panel.set_active_tab(true, cx);
        });
    });
    window.run_until_parked();

    assert_eq!(*openings.borrow(), 0);
}

#[gpui::test]
fn closing_the_builder_after_a_run_shows_the_notice_and_reset_restores_where(
    cx: &mut TestAppContext,
) {
    let (panel, window) = rendered_short_grid(cx);

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.open_query_builder(window, cx);
            panel.apply_builder_draft_spec(make_test_spec(), cx);
            panel.handle_builder_event(&super::super::BuilderEvent::CloseRequested, window, cx);
        });
    });
    window.run_until_parked();
    window.update(|_, _| {});

    window.update(|_, app| {
        let panel = panel.read(app);
        assert!(
            !panel.filter_input_visible(),
            "the applied spec drives the rows, so the WHERE input gives way"
        );
        assert!(panel.builder_notice_visible());
        assert!(panel.builder.builder_draft_spec.is_some());
    });
    assert!(
        window.debug_bounds("builder-query-notice").is_some(),
        "the filter row must say where the rows come from"
    );

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            assert!(panel.reset_builder_query(window, cx));
        });
    });
    window.run_until_parked();
    window.update(|_, _| {});

    window.update(|_, app| {
        let panel = panel.read(app);
        assert!(
            panel.filter_input_visible(),
            "Reset brings the WHERE input back"
        );
        assert!(!panel.builder_notice_visible());
        assert!(panel.builder.builder_draft_spec.is_none());
        assert!(panel.builder.builder_panel.is_none());
    });
    assert!(window.debug_bounds("builder-query-notice").is_none());
}

#[gpui::test]
fn the_notice_is_hidden_while_the_builder_is_open(cx: &mut TestAppContext) {
    with_keyed_panel(cx, false, |panel, window, cx| {
        panel.open_query_builder(window, cx);
        panel.apply_builder_draft_spec(make_test_spec(), cx);

        assert!(!panel.builder_notice_visible());
        assert!(!panel.filter_input_visible());
    });
}

#[gpui::test]
fn fk_rerun_with_pending_edits_is_refused_and_stops_resolving(cx: &mut TestAppContext) {
    use super::super::filter_bar::RelationalFilterState;

    let (_, panel, window) = with_keyed_panel(cx, true, |panel, _, cx| {
        panel.builder.relational_filter_state = RelationalFilterState::Resolving;
        panel.apply_fk_result(Vec::new(), cx);

        assert!(!panel.pending.refresh);
        assert!(matches!(
            panel.builder.relational_filter_state,
            RelationalFilterState::Inactive
        ));
    });

    assert_blocked_with_edit_kept(&panel, window);
}

#[gpui::test]
fn fk_rerun_without_pending_edits_queues_the_reload(cx: &mut TestAppContext) {
    use super::super::filter_bar::RelationalFilterState;

    with_keyed_panel(cx, false, |panel, _, cx| {
        panel.builder.relational_filter_state = RelationalFilterState::Resolving;
        panel.apply_fk_result(Vec::new(), cx);

        assert!(panel.pending.refresh);
    });
}

#[gpui::test]
fn chart_reexecute_with_pending_edits_is_refused(cx: &mut TestAppContext) {
    let (_, panel, window) = with_keyed_panel(cx, true, |panel, _, cx| {
        panel.chart_host_request_reexecute(cx);

        assert!(!panel.pending.refresh);
    });

    assert_blocked_with_edit_kept(&panel, window);
}

#[gpui::test]
fn chart_reexecute_without_pending_edits_queues_the_reload(cx: &mut TestAppContext) {
    with_keyed_panel(cx, false, |panel, _, cx| {
        panel.chart_host_request_reexecute(cx);

        assert!(panel.pending.refresh);
    });
}

#[gpui::test]
fn reload_keeps_record_mode_and_widths(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            assert!(panel.record_view_available());
            panel.set_record_mode(true, cx);
            widen_name_and_select(panel, cx);

            panel.apply_table_result(
                Uuid::nil(),
                TableRef::with_schema("public", "users"),
                Pagination::default(),
                Vec::new(),
                Some(2),
                reload_result(&["id", "name", "email"], 2),
                cx,
            );
        });
    });

    window.update(|_, app| {
        let panel = panel.read(app);
        assert!(panel.record_mode(), "the chosen presentation must survive");
        assert_eq!(name_width(panel, app), 260.0);
        assert!(
            panel
                .grid_table
                .table_state
                .as_ref()
                .expect("table state")
                .read(app)
                .record_mode(),
            "the grid entity itself must stay in record mode"
        );
    });
}

#[gpui::test]
fn document_shaped_query_result_opens_in_the_tree(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            DataGridPanel::new_for_result(
                Arc::new(nested_document_rows()),
                "db.products.find({})".to_string(),
                None,
                app_state.clone(),
                window,
                cx,
            )
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
            assert_eq!(
                panel.result_view_mode(),
                super::super::ResultViewMode::Table
            );
            assert!(!panel.uses_result_view());
            assert_eq!(
                panel.view_config.mode,
                crate::data_view::DataViewMode::Document,
                "a document-shaped result opens in the tree"
            );

            panel.set_query_result(
                Arc::new(reload_result(&["id", "name"], 2)),
                "SELECT id, name FROM users".to_string(),
                None,
                cx,
            );
            assert_eq!(
                panel.view_config.mode,
                crate::data_view::DataViewMode::Table,
                "a table-shaped result stays in the grid"
            );
        });
    });
}

#[gpui::test]
fn new_query_result_remaps_widths_by_name_and_drops_sort(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            DataGridPanel::new_for_result(
                Arc::new(reload_result(&["id", "name"], 2)),
                "SELECT id, name FROM users".to_string(),
                None,
                app_state.clone(),
                window,
                cx,
            )
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
            widen_name_and_select(panel, cx);
            let table_state = panel.grid_table.table_state.clone().expect("table state");
            table_state.update(cx, |state, _cx| {
                state.set_sort_without_emit(TableSortState::ascending(1));
            });

            panel.set_query_result(
                Arc::new(reload_result(&["name", "extra"], 2)),
                "SELECT name, extra FROM users".to_string(),
                None,
                cx,
            );
        });
    });

    window.update(|_, app| {
        let panel = panel.read(app);
        let table_state = panel.grid_table.table_state.as_ref().expect("table state");
        let state = table_state.read(app);

        assert_eq!(
            state.column_widths()[0],
            260.0,
            "`name` moved to index 0 and must carry its width with it"
        );
        assert_eq!(
            state.column_widths().len(),
            2,
            "widths must be sized to the new column count"
        );
        assert_eq!(
            state.sort(),
            None,
            "the old sort column index addresses a shape that is gone"
        );
        assert_eq!(state.selection().active, None);
    });
}
