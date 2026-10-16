use super::*;

#[gpui::test]
fn local_sort_after_a_refresh_with_more_rows_keeps_every_row(cx: &mut TestAppContext) {
    use crate::keyboard_test_support::{host_document, init_keyboard_runtime};

    init_keyboard_runtime(cx);
    let app_state = isolated_test_app_state(cx);

    let rows_with_ids = |ids: std::ops::RangeInclusive<i64>| {
        QueryResult::table(
            vec![key_column("id", true)],
            ids.map(|id| vec![dbflux_core::Value::Int(id)]).collect(),
            None,
            Duration::ZERO,
        )
    };

    let (host, window) = host_document(
        cx,
        move |window, cx| {
            cx.new(|cx| {
                let initial = rows_with_ids(1..=2);
                let source = DataSource::QueryResult {
                    result: Arc::new(initial.clone()),
                    original_query: "SELECT id FROM users".to_string(),
                    profile_id: None,
                };

                let mut panel = DataGridPanel::new_internal(
                    source,
                    app_state.clone(),
                    vec!["id".to_string()],
                    window,
                    cx,
                );
                panel.set_result(initial, cx);
                panel
            })
        },
        |panel, cx| panel.active_context(cx),
        DataGridPanel::dispatch_command,
    );
    let panel = window.update(|_, cx| host.read(cx).document.clone());

    window.update(|_, cx| {
        panel.update(cx, |panel, cx| {
            panel.apply_local_sort(0, dbflux_core::SortDirection::Descending, cx);
            // A refresh returning more rows than the sorted result had.
            panel.set_result(rows_with_ids(1..=3), cx);
            panel.apply_local_sort(0, dbflux_core::SortDirection::Descending, cx);
        });
    });

    let row_count = window.update(|_, cx| panel.read(cx).result.rows.len());
    assert_eq!(row_count, 3);
}

#[gpui::test]
fn server_sort_keeps_column_widths_and_cursor(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            widen_name_and_select(panel, cx);
            panel.apply_table_result(
                Uuid::nil(),
                TableRef::with_schema("public", "users"),
                Pagination::default(),
                vec![dbflux_core::OrderByColumn::from_name(
                    "name",
                    dbflux_core::SortDirection::Ascending,
                )],
                Some(2),
                reload_result(&["id", "name", "email"], 2),
                cx,
            );
        });
    });

    window.update(|_, app| {
        let panel = panel.read(app);
        assert_eq!(
            name_width(panel, app),
            260.0,
            "a server-side sort must not reset a user-adjusted column width"
        );
        assert_eq!(
            panel
                .grid_table
                .table_state
                .as_ref()
                .expect("table state")
                .read(app)
                .selection()
                .active,
            Some(CellCoord::new(1, 1)),
            "a sort reorders the same rows, so the cursor stays put"
        );
        assert_eq!(
            panel
                .grid_table
                .table_state
                .as_ref()
                .expect("table state")
                .read(app)
                .sort()
                .map(|sort| sort.column_ix),
            Some(1),
            "the result carries the server order, so the header shows it"
        );
    });
}

#[gpui::test]
fn an_unsorted_reload_drops_the_stale_header_indicator(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            widen_name_and_select(panel, cx);
            let table_state = panel.grid_table.table_state.clone().expect("table state");
            table_state.update(cx, |state, _cx| {
                state.set_sort_without_emit(TableSortState::ascending(1));
            });

            // A result with no `order_by` is unsorted. Keeping the previous
            // sort here would light up an arrow the new rows do not honour
            // (collection refreshes and instance snapshots reset their
            // local sort on every reload).
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
        assert_eq!(
            panel
                .grid_table
                .table_state
                .as_ref()
                .expect("table state")
                .read(app)
                .sort(),
            None,
            "an unsorted reload must drop the previous sort indicator"
        );
        assert_eq!(
            name_width(panel, app),
            260.0,
            "dropping the sort must not touch the column widths"
        );
    });
}

#[gpui::test]
fn a_rail_that_follows_the_cursor_is_left_to_the_reload(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            widen_name_and_select(panel, cx);

            // The reload's SelectionChanged handler runs open_row_inspector
            // itself when the rail follows a live cursor, so the explicit
            // pass after it must stand down.
            panel.inspector.follow_selection = true;
            assert!(panel.reload_refreshed_the_rail(cx));

            panel.inspector.follow_selection = false;
            assert!(
                !panel.reload_refreshed_the_rail(cx),
                "a rail that is not tracking needs the explicit re-snapshot"
            );
        });
    });
}

#[gpui::test]
fn toggle_row_inspector_opens_on_the_cursor_and_closes_again(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            widen_name_and_select(panel, cx);
            panel.toggle_row_inspector(cx);

            assert!(panel.row_inspector_is_open());
            assert_eq!(panel.inspector.inspector_row, Some((1, 1)));
            assert!(panel.inspector.row_inspector_content.is_some());

            panel.toggle_row_inspector(cx);

            assert!(!panel.row_inspector_is_open());
            assert!(panel.inspector.row_inspector_content.is_none());
        });
    });
}

#[gpui::test]
fn a_pinned_row_inspector_ignores_the_cursor(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            widen_name_and_select(panel, cx);
            panel.toggle_row_inspector(cx);
            panel.handle_row_inspector_event(
                super::super::row_inspector::RowInspectorContentEvent::TogglePin,
                cx,
            );
        });
    });

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            let table_state = panel.grid_table.table_state.clone().expect("table state");
            table_state.update(cx, |state, cx| {
                state.select_cell(CellCoord::new(0, 0), cx);
            });
        });
    });

    window.update(|_, app| {
        let panel = panel.read(app);
        assert!(panel.inspector.pinned);
        assert_eq!(
            panel.inspector.inspector_row,
            Some((1, 1)),
            "a pinned inspector keeps the row it was pinned on"
        );
        assert!(
            panel
                .inspector
                .row_inspector_content
                .as_ref()
                .expect("inspector content")
                .read(app)
                .is_pinned()
        );
    });
}

#[gpui::test]
fn closing_the_row_inspector_drops_the_pin(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            widen_name_and_select(panel, cx);
            panel.toggle_row_inspector(cx);
            panel.handle_row_inspector_event(
                super::super::row_inspector::RowInspectorContentEvent::TogglePin,
                cx,
            );
            panel.handle_row_inspector_event(
                super::super::row_inspector::RowInspectorContentEvent::Close,
                cx,
            );

            assert!(!panel.inspector.pinned);
            assert!(!panel.row_inspector_is_open());
        });
    });
}

#[gpui::test]
fn a_table_browse_offers_its_views_in_the_footer(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|_, app| {
        let panel = panel.read(app);

        assert!(panel.footer_hosts_view_switch());

        let modes = panel.available_result_view_modes(app);
        assert!(modes.contains(&super::super::ResultViewMode::Table));
        assert!(modes.contains(&super::super::ResultViewMode::Json));
    });
}

#[gpui::test]
fn a_table_keeps_its_json_view_across_a_reload(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.set_result_view_mode(super::super::ResultViewMode::Json, cx);
            assert!(panel.uses_result_view());

            panel.apply_table_result(
                Uuid::nil(),
                TableRef::with_schema("public", "users"),
                Pagination::default(),
                Vec::new(),
                Some(2),
                reload_result(&["id", "name", "email"], 2),
                cx,
            );

            assert_eq!(panel.result_view_mode(), super::super::ResultViewMode::Json);
        });
    });
}

#[gpui::test]
fn a_reload_that_cannot_run_does_not_leak_its_intent(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.grid_table.reload = super::super::TableReload::ResetRows;

            // The test profile is not connected, so this request never
            // reaches a driver. Its intent must not stay behind for the
            // next, unrelated reload to pick up.
            panel.run_table_query(
                Uuid::nil(),
                None,
                TableRef::with_schema("public", "users"),
                Pagination::default(),
                Vec::new(),
                None,
                window,
                cx,
            );

            assert_eq!(panel.grid_table.reload, super::super::TableReload::Preserve);
        });
    });
}

#[gpui::test]
fn pagination_keeps_column_widths_but_drops_the_cursor(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            widen_name_and_select(panel, cx);

            // What `go_to_next_page` marks before it issues the query.
            // `run_table_query` takes the mark and restores it on the
            // applying side; that restored state is what is modelled here.
            panel.grid_table.reload = super::super::TableReload::ResetRows;
            panel.apply_table_result(
                Uuid::nil(),
                TableRef::with_schema("public", "users"),
                Pagination::default().next_page(),
                Vec::new(),
                Some(2),
                reload_result(&["id", "name", "email"], 2),
                cx,
            );
        });
    });

    window.update(|_, app| {
        let panel = panel.read(app);
        assert_eq!(
            name_width(panel, app),
            260.0,
            "a page change must not reset a user-adjusted column width"
        );
        assert_eq!(
            panel
                .grid_table
                .table_state
                .as_ref()
                .expect("table state")
                .read(app)
                .selection()
                .active,
            None,
            "a row index on the old page points at unrelated data on the new one"
        );
    });
}

#[gpui::test]
fn reload_reuses_the_table_state_entity(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    let before = window.update(|_, app| {
        panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("table state")
            .entity_id()
    });

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
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

    let after = window.update(|_, app| {
        panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("table state")
            .entity_id()
    });

    assert_eq!(
        before, after,
        "a reload must update the existing state rather than rebuild it"
    );
}

#[gpui::test]
fn reload_drops_pending_edits(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            let table_state = panel.grid_table.table_state.clone().expect("table state");
            table_state.update(cx, |state, _cx| {
                state.stage_base_cell_value(
                    0,
                    1,
                    dbflux_components::components::data_table::model::CellValue::text("carol"),
                );
                assert!(state.has_pending_changes());
            });

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
        let table_state = panel.grid_table.table_state.as_ref().expect("table state");
        assert!(
            !table_state.read(app).has_pending_changes(),
            "a staged edit is keyed by a row index of the replaced result"
        );
    });
}

/// A user refresh (the refresh key, the toolbar button and the command
/// palette all dispatch it) keeps unsaved edits and does not re-run the
/// query; it warns instead.
#[gpui::test]
fn refresh_request_with_pending_edits_keeps_them_and_skips_the_query(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    let handled = window.update(|window, app| {
        panel.update(app, |panel, cx| {
            let table_state = panel.grid_table.table_state.clone().expect("table state");
            table_state.update(cx, |state, _cx| {
                state.stage_base_cell_value(
                    0,
                    1,
                    dbflux_components::components::data_table::model::CellValue::text("carol"),
                );
            });

            panel.dispatch_command(Command::RefreshSchema, window, cx)
        })
    });
    window.run_until_parked();

    assert!(handled);
    window.update(|_, app| {
        let panel = panel.read(app);
        let table_state = panel.grid_table.table_state.as_ref().expect("table state");

        assert!(table_state.read(app).has_pending_changes());
        assert!(panel.refresh.state != GridState::Loading);
        assert!(!panel.runner.is_primary_active());
    });
    assert_eq!(
        last_toast_title(window),
        Some(crate::labels::grid_reload_blocked_by_pending_edits())
    );
}

/// Without unsaved edits the same refresh reaches the query path, which
/// here stops at the missing connection.
#[gpui::test]
fn refresh_request_without_pending_edits_runs_the_query(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    let handled = window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.dispatch_command(Command::RefreshSchema, window, cx)
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
