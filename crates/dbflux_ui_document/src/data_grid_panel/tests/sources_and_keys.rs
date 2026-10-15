use super::*;

#[test]
fn resolve_orderable_sort_key_returns_trailing_primary_key() {
    // Partition key first, sort key last; both marked primary. The trailing
    // primary-key column is the orderable sort key.
    let columns = vec![
        key_column("pk", true),
        key_column("sk", true),
        key_column("attr", false),
    ];

    assert_eq!(
        super::super::resolve_orderable_sort_key(&columns),
        Some("sk".to_string())
    );
}

#[test]
fn resolve_orderable_sort_key_none_for_partition_only_key() {
    // A single primary-key column is a partition-only key with nothing to
    // order on, so no orderable key is resolved.
    let columns = vec![key_column("pk", true), key_column("attr", false)];

    assert_eq!(super::super::resolve_orderable_sort_key(&columns), None);
}

#[test]
fn resolve_orderable_sort_key_none_without_primary_keys() {
    // A result with no primary-key markers (e.g. a PartiQL `SELECT *`) yields
    // no orderable key, so no ORDER BY is seeded.
    let columns = vec![key_column("a", false), key_column("b", false)];

    assert_eq!(super::super::resolve_orderable_sort_key(&columns), None);
}

#[test]
fn mutations_enabled_truth_table() {
    let ungrouped_spec = {
        let mut s = make_test_spec();
        s.group_by = vec![];
        s.aggregates = vec![];
        s
    };
    let grouped_spec = make_grouped_spec();

    let cases: &[(&'static str, Option<VisualQuerySpec>, Vec<String>, bool)] = &[
        ("no spec, no pk", None, vec![], false),
        ("no spec, has pk", None, vec!["id".to_string()], true),
        (
            "ungrouped, no pk",
            Some(ungrouped_spec.clone()),
            vec![],
            false,
        ),
        (
            "ungrouped, has pk",
            Some(ungrouped_spec.clone()),
            vec!["id".to_string()],
            true,
        ),
        ("grouped, no pk", Some(grouped_spec.clone()), vec![], false),
        (
            "grouped, has pk",
            Some(grouped_spec.clone()),
            vec!["id".to_string()],
            false,
        ),
    ];

    for (label, spec, pk_cols, expected) in cases {
        let result = mutations_enabled_predicate(spec.as_ref(), pk_cols);
        assert_eq!(result, *expected, "mutations_enabled failed for: {}", label);
    }
}

#[test]
fn table_source_accessors_match_expected_values() {
    let table = TableRef::with_schema("public", "users");
    let pagination = Pagination::Offset {
        limit: 25,
        offset: 50,
    };

    let source = DataSource::Table {
        profile_id: Uuid::new_v4(),
        database: Some("app".to_string()),
        table: table.clone(),
        pagination: pagination.clone(),
        order_by: Vec::new(),
        total_rows: Some(123),
    };

    assert!(source.is_table());
    assert!(!source.is_collection());
    assert!(source.is_paginated());
    assert_eq!(source.database(), Some("app"));
    assert_eq!(source.table_ref(), Some(&table));
    assert_eq!(source.collection_ref(), None);
    assert_eq!(source.pagination(), Some(&pagination));
    assert_eq!(source.total_rows(), Some(123));
}

#[test]
fn collection_source_accessors_match_expected_values() {
    let collection = CollectionRef::new("app", "users");
    let pagination = Pagination::Offset {
        limit: 10,
        offset: 0,
    };

    let source = DataSource::Collection {
        profile_id: Uuid::new_v4(),
        collection: collection.clone(),
        pagination: pagination.clone(),
        total_docs: Some(17),
    };

    assert!(!source.is_table());
    assert!(source.is_collection());
    assert!(source.is_paginated());
    assert_eq!(source.database(), None);
    assert_eq!(source.table_ref(), None);
    assert_eq!(source.collection_ref(), Some(&collection));
    assert_eq!(source.pagination(), Some(&pagination));
    assert_eq!(source.total_rows(), Some(17));
}

#[test]
fn query_result_source_accessors_match_expected_values() {
    let source = DataSource::QueryResult {
        result: Arc::new(QueryResult::text(
            "ok".to_string(),
            std::time::Duration::ZERO,
        )),
        original_query: "PING".to_string(),
        profile_id: None,
    };

    assert!(!source.is_table());
    assert!(!source.is_collection());
    assert!(!source.is_paginated());
    assert_eq!(source.database(), None);
    assert_eq!(source.table_ref(), None);
    assert_eq!(source.collection_ref(), None);
    assert_eq!(source.pagination(), None);
    assert_eq!(source.total_rows(), None);
}

/// The table view's keys go through the keymap: the table's own keys
/// move the selection inside the table, and a key of the Results panel
/// (Ctrl+Space, the row inspector) reaches the grid as a command.
#[gpui::test]
fn table_keys_move_the_selection_and_open_the_row_inspector(cx: &mut TestAppContext) {
    use crate::keyboard_test_support::{host_document, init_keyboard_runtime};

    init_keyboard_runtime(cx);
    let app_state = isolated_test_app_state(cx);

    let (host, window) = host_document(
        cx,
        move |window, cx| {
            cx.new(|cx| {
                let source = DataSource::Table {
                    profile_id: Uuid::nil(),
                    database: Some("app".to_string()),
                    table: TableRef::with_schema("public", "users"),
                    pagination: Pagination::default(),
                    order_by: Vec::new(),
                    total_rows: Some(3),
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
                        vec![key_column("id", true)],
                        (1..=3)
                            .map(|id| vec![dbflux_core::Value::Int(id)])
                            .collect(),
                        None,
                        Duration::ZERO,
                    ),
                    cx,
                );
                panel
            })
        },
        |panel, cx| panel.active_context(cx),
        DataGridPanel::dispatch_command,
    );
    let panel = window.update(|_, cx| host.read(cx).document.clone());

    let inspector_requests = Rc::new(RefCell::new(0usize));
    window.update(|window, cx| {
        let inspector_requests = inspector_requests.clone();
        cx.subscribe(&panel, move |_, event: &DataGridEvent, _| {
            if matches!(event, DataGridEvent::OpenInspector { .. }) {
                *inspector_requests.borrow_mut() += 1;
            }
        })
        .detach();

        let table_state = panel
            .read(cx)
            .grid_table
            .table_state
            .clone()
            .expect("the result builds a table");
        let focus_handle = table_state.read(cx).focus_handle().clone();
        focus_handle.focus(window, cx);
    });
    window.run_until_parked();

    let active_row = |window: &mut VisualTestContext| {
        window.update(|_, cx| {
            panel
                .read(cx)
                .grid_table
                .table_state
                .as_ref()
                .and_then(|state| state.read(cx).selection().active)
                .map(|coord| coord.row)
        })
    };

    window.simulate_keystrokes("j");
    let first = active_row(window).expect("`j` selects a row");
    window.simulate_keystrokes("j");
    assert_eq!(active_row(window), Some(first + 1), "`j` moves down");
    window.simulate_keystrokes("k");
    assert_eq!(active_row(window), Some(first), "`k` moves up");

    window.simulate_keystrokes("ctrl-space");
    assert!(
        window
            .update(|_, cx| host.read(cx).commands.clone())
            .contains(&Command::ToggleRowInspector),
        "Ctrl+Space reaches the grid as the row inspector command"
    );
    assert_eq!(
        *inspector_requests.borrow(),
        1,
        "the grid opens the inspector"
    );
}

/// Ctrl+E opens the export menu with the keyboard in it: the menu keys
/// move through the formats, Enter runs the chosen one and Escape closes
/// the menu and hands the keyboard back to the table.
#[gpui::test]
fn ctrl_e_opens_an_export_menu_driven_by_the_menu_keys(cx: &mut TestAppContext) {
    use crate::keyboard_test_support::init_keyboard_runtime;
    use dbflux_app::keymap::ContextId;
    use dbflux_ui_base::{SaveTargetOutcome, SaveTargetProvider};

    init_keyboard_runtime(cx);

    let requested: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let provider: SaveTargetProvider = {
        let requested = requested.clone();
        Arc::new(move |request| {
            if let Ok(mut names) = requested.lock() {
                names.push(request.suggested_name.to_string());
            }
            gpui::Task::ready(SaveTargetOutcome::Cancelled)
        })
    };
    let app_state = cx.update(|cx| {
        cx.new(|_| {
            AppStateEntity::new_with_storage_runtime(
                StorageRuntime::in_memory().expect("isolated storage runtime"),
            )
            .expect("test storage setup")
            .with_save_target_override(provider)
        })
    });

    let (_host, panel, window) = host_result_grid(cx, app_state);
    let context =
        |window: &mut VisualTestContext| window.update(|_, cx| panel.read(cx).active_context(cx));

    window.simulate_keystrokes("ctrl-e");
    assert!(
        window.update(|_, cx| panel.read(cx).chrome.export_menu_open),
        "Ctrl+E opens the export menu"
    );
    assert_eq!(context(window), ContextId::ContextMenu);

    // Save as CSV, JSON (pretty), JSON (compact): the second entry.
    window.simulate_keystrokes("j enter");
    window.run_until_parked();

    assert!(
        !window.update(|_, cx| panel.read(cx).chrome.export_menu_open),
        "choosing a format closes the menu"
    );
    let names = requested
        .lock()
        .map(|names| names.clone())
        .unwrap_or_default();
    assert_eq!(names.len(), 1, "Enter starts one Save As, got {names:?}");
    assert!(
        names[0].ends_with(".json"),
        "Enter saves in the highlighted format, got {names:?}"
    );
    assert_eq!(context(window), ContextId::Results);

    // Up from the first entry wraps to the last copy entry, which puts
    // the compact JSON on the clipboard.
    window.simulate_keystrokes("ctrl-e k enter");
    window.run_until_parked();
    let copied = window
        .update(|_, cx| cx.read_from_clipboard())
        .and_then(|item| item.text())
        .unwrap_or_default();
    assert!(
        copied.starts_with("[{"),
        "Enter on a copy entry copies the result, got {copied:?}"
    );

    window.simulate_keystrokes("ctrl-e escape");
    assert!(
        !window.update(|_, cx| panel.read(cx).chrome.export_menu_open),
        "Escape closes the menu"
    );
    assert_eq!(context(window), ContextId::Results);
    let table_focused = window.update(|window, cx| {
        panel
            .read(cx)
            .grid_table
            .table_state
            .as_ref()
            .is_some_and(|state| state.read(cx).focus_handle().contains_focused(window, cx))
            || panel.read(cx).focus_handle.contains_focused(window, cx)
    });
    assert!(table_focused, "closing hands the keyboard back to the grid");
}

/// Ctrl+L from the grid moves the keyboard into the open value panel,
/// where J and K belong to the panel and not to the grid's cursor; Enter
/// edits the value and Escape stops editing; Ctrl+H goes back to the grid.
#[gpui::test]
fn ctrl_l_enters_the_value_panel_and_ctrl_h_returns(cx: &mut TestAppContext) {
    use crate::keyboard_test_support::init_keyboard_runtime;
    use dbflux_app::keymap::ContextId;

    init_keyboard_runtime(cx);
    let app_state = isolated_test_app_state(cx);
    let (panel, window) = host_grid_with_rail(cx, app_state);
    let context =
        |window: &mut VisualTestContext| window.update(|_, cx| panel.read(cx).active_context(cx));

    for keys in ["j", "v"] {
        window.simulate_keystrokes(keys);
        window.run_until_parked();
    }
    assert!(window.update(|_, cx| panel.read(cx).value_panel_is_open()));
    let row = active_row(&panel, window);

    window.simulate_keystrokes("ctrl-l");
    window.run_until_parked();
    assert_eq!(
        context(window),
        ContextId::Inspector,
        "Ctrl+L enters the panel"
    );
    assert!(!table_has_focus(&panel, window));

    window.simulate_keystrokes("j");
    window.run_until_parked();
    assert_eq!(
        active_row(&panel, window),
        row,
        "J inside the panel leaves the grid's cursor alone"
    );

    window.simulate_keystrokes("enter");
    window.run_until_parked();
    assert_eq!(
        context(window),
        ContextId::TextInput,
        "Enter edits the value"
    );

    window.simulate_keystrokes("escape");
    window.run_until_parked();
    assert_eq!(
        context(window),
        ContextId::Inspector,
        "Escape stops editing and stays in the panel"
    );

    window.simulate_keystrokes("ctrl-h");
    window.run_until_parked();
    assert_eq!(
        context(window),
        ContextId::Results,
        "Ctrl+H returns to the grid"
    );
    assert!(table_has_focus(&panel, window));
}

/// Ctrl+L enters the row inspector too, and Escape returns to the grid.
/// With no side panel open, Ctrl+L leaves the grid where it is.
#[gpui::test]
fn ctrl_l_enters_the_row_inspector_and_escape_returns(cx: &mut TestAppContext) {
    use crate::keyboard_test_support::init_keyboard_runtime;
    use dbflux_app::keymap::ContextId;

    init_keyboard_runtime(cx);
    let app_state = isolated_test_app_state(cx);
    let (panel, window) = host_grid_with_rail(cx, app_state);
    let context =
        |window: &mut VisualTestContext| window.update(|_, cx| panel.read(cx).active_context(cx));

    window.simulate_keystrokes("j");
    window.run_until_parked();
    let row = active_row(&panel, window);

    window.simulate_keystrokes("ctrl-l");
    window.run_until_parked();
    assert_eq!(context(window), ContextId::Results);
    assert!(table_has_focus(&panel, window), "no side panel to enter");

    window.simulate_keystrokes("ctrl-space");
    window.run_until_parked();
    assert!(window.update(|_, cx| panel.read(cx).row_inspector_is_open()));

    window.simulate_keystrokes("ctrl-l");
    window.run_until_parked();
    assert_eq!(context(window), ContextId::Inspector);

    window.simulate_keystrokes("k");
    window.run_until_parked();
    assert_eq!(active_row(&panel, window), row);

    window.simulate_keystrokes("escape");
    window.run_until_parked();
    assert_eq!(context(window), ContextId::Results);
    assert!(table_has_focus(&panel, window));
    assert!(
        window.update(|_, cx| panel.read(cx).row_inspector_is_open()),
        "leaving the inspector keeps it open"
    );
}

/// `m` opens the table menu from inside the value panel, and its Toolbar
/// submenu runs the panel's buttons: another format, then Revert.
#[gpui::test]
fn the_value_panel_buttons_run_from_the_table_menu(cx: &mut TestAppContext) {
    use super::super::context_menu::toolbar::ToolbarAction;
    use super::super::value_panel::ValuePanelButton;

    let (panel, window) = rail_keys::host_table_grid_with_rail(cx);
    let buttons = |window: &mut VisualTestContext| {
        window.update(|_, cx| {
            panel
                .read(cx)
                .inspector
                .value_panel
                .as_ref()
                .map(|value_panel| value_panel.read(cx).buttons(cx))
                .unwrap_or_default()
        })
    };

    for keys in ["j", "v", "ctrl-l"] {
        window.simulate_keystrokes(keys);
        window.run_until_parked();
    }

    let other_format = buttons(window)
        .into_iter()
        .find(|button| matches!(button, ValuePanelButton::Format(_)))
        .expect("the value panel offers the formats not shown");

    run_toolbar_entry(&panel, ToolbarAction::ValuePanel(other_format), window);
    assert!(
        !buttons(window).contains(&other_format),
        "the entry reads the value in that format"
    );

    // The menu hands the keyboard back to the grid; Ctrl+L, Enter, a
    // typed character and Escape change the value from the panel.
    window.simulate_keystrokes("ctrl-l");
    window.simulate_keystrokes("enter");
    window.run_until_parked();
    window.simulate_input("x");
    window.simulate_keystrokes("escape");
    window.run_until_parked();
    assert!(buttons(window).contains(&ValuePanelButton::Revert));

    run_toolbar_entry(
        &panel,
        ToolbarAction::ValuePanel(ValuePanelButton::Revert),
        window,
    );
    assert!(
        !buttons(window).contains(&ValuePanelButton::Revert),
        "Revert puts the stored value back"
    );
}

/// The Toolbar submenu pins and unpins the open row inspector, as its
/// pin button does.
#[gpui::test]
fn the_table_menu_pins_the_row_inspector(cx: &mut TestAppContext) {
    use super::super::context_menu::toolbar::ToolbarAction;
    use crate::keyboard_test_support::init_keyboard_runtime;

    init_keyboard_runtime(cx);
    let app_state = isolated_test_app_state(cx);
    let (panel, window) = host_grid_with_rail(cx, app_state);

    for keys in ["j", "ctrl-space"] {
        window.simulate_keystrokes(keys);
        window.run_until_parked();
    }
    assert!(window.update(|_, cx| panel.read(cx).row_inspector_is_open()));

    run_toolbar_entry(&panel, ToolbarAction::PinRowInspector, window);
    assert!(window.update(|_, cx| panel.read(cx).inspector.pinned));

    run_toolbar_entry(&panel, ToolbarAction::PinRowInspector, window);
    assert!(!window.update(|_, cx| panel.read(cx).inspector.pinned));
}

#[gpui::test]
fn filtered_empty_table_runtime_keeps_header_and_active_filter(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id: Uuid::nil(),
                database: Some("app".to_string()),
                table: TableRef::with_schema("public", "users"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: Some(0),
            };

            let mut panel = DataGridPanel::new_internal(
                source,
                app_state.clone(),
                vec!["id".to_string()],
                window,
                cx,
            );

            panel.set_result(zero_row_result(), cx);
            panel.filter_bar.filter_input.update(cx, |input, cx| {
                input.set_value("id = 999", window, cx);
            });

            panel
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    let (filter_value, has_table, row_count, col_count) = window.update(|_, app| {
        let panel = panel.read(app);
        let table_state = panel
            .grid_table
            .table_state
            .as_ref()
            .expect("filtered empty table should still build table state");
        let table_state = table_state.read(app);

        (
            panel.filter_bar.filter_input.read(app).value().to_string(),
            panel.grid_table.data_table.is_some(),
            table_state.row_count(),
            table_state.col_count(),
        )
    });

    assert_eq!(filter_value, "id = 999");
    assert!(
        has_table,
        "filtered empty table should keep table content active"
    );
    assert_eq!(
        row_count, 0,
        "filtered empty table should remain visually empty"
    );
    assert_eq!(col_count, 2, "filtered empty table should keep its headers");
}

#[gpui::test]
fn successful_insert_refresh_runtime_keeps_filter_and_can_stay_visually_empty(
    cx: &mut TestAppContext,
) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id: Uuid::nil(),
                database: Some("app".to_string()),
                table: TableRef::with_schema("public", "users"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: Some(0),
            };

            let mut panel = DataGridPanel::new_internal(
                source,
                app_state.clone(),
                vec!["id".to_string()],
                window,
                cx,
            );

            panel.set_result(zero_row_result(), cx);
            panel.filter_bar.filter_input.update(cx, |input, cx| {
                input.set_value("id = 999", window, cx);
            });

            panel
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    let refresh_was_queued = window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.handle_add_row(0, false, cx);

            // What the insert's success path does before it queues the
            // reload: the landed row leaves the staged inserts.
            let table_state = panel.grid_table.table_state.clone().expect("table state");
            table_state.update(cx, |state, _cx| {
                state.edit_buffer_mut().remove_pending_insert_by_idx(0);
            });

            panel.queue_refresh_after_mutation_success(false, cx);
            let refresh_was_queued = panel.pending.refresh;
            panel.set_result(zero_row_result(), cx);
            refresh_was_queued
        })
    });

    let (filter_value, pending_inserts) = window.update(|_, app| {
        let panel = panel.read(app);
        let pending_inserts = panel
            .grid_table
            .table_state
            .as_ref()
            .map(|state| state.read(app).edit_buffer().pending_insert_rows().len())
            .unwrap_or_default();

        (
            panel.filter_bar.filter_input.read(app).value().to_string(),
            pending_inserts,
        )
    });

    assert_eq!(filter_value, "id = 999");
    assert!(
        refresh_was_queued,
        "successful insert refresh should be queued"
    );
    assert_eq!(
        pending_inserts, 0,
        "the landed insert must not come back as a staged row after the reload"
    );

    let (row_count, col_count, has_table) = window.update(|_, app| {
        let panel = panel.read(app);
        let table_state = panel
            .grid_table
            .table_state
            .as_ref()
            .expect("post-refresh filtered result should still build table state");
        let table_state = table_state.read(app);

        (
            table_state.row_count(),
            table_state.col_count(),
            panel.grid_table.data_table.is_some(),
        )
    });

    assert!(
        has_table,
        "successful insert refresh should keep table mode active"
    );
    assert_eq!(row_count, 0, "filtered refresh may still be visually empty");
    assert_eq!(col_count, 2, "filtered refresh should keep headers visible");
}
