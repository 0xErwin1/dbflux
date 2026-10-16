use super::*;

#[gpui::test]
fn pending_edit_counts_empty_buffer_returns_zeros(cx: &mut TestAppContext) {
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
            panel
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    let counts = window.update(|_, app| panel.read(app).pending_edit_counts(app));

    assert_eq!(
        counts,
        (0, 0, 0),
        "fresh panel should have no pending changes"
    );
}

#[gpui::test]
fn pending_edit_counts_only_inserts(cx: &mut TestAppContext) {
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
            panel.handle_add_row(0, false, cx);
        });
    });

    let counts = window.update(|_, app| panel.read(app).pending_edit_counts(app));

    assert_eq!(counts.0, 1, "should have 1 pending insert");
    assert_eq!(counts.1, 0, "should have 0 pending updates");
    assert_eq!(counts.2, 0, "should have 0 pending deletes");
}

// P1 — Right-click always opens context menu; row actions appear in it

#[test]
fn context_menu_row_actions_field_stores_provider_actions() {
    use dbflux_core::InspectorRowAction;
    use std::sync::Arc;

    let actions = vec![
        InspectorRowAction {
            id: "kill".to_string(),
            label: "Kill Connection".to_string(),
            description: None,
            is_destructive: true,
        },
        InspectorRowAction {
            id: "cancel".to_string(),
            label: "Cancel Query".to_string(),
            description: None,
            is_destructive: false,
        },
    ];

    // Simulate what the ContextMenuRequested handler now does: call the
    // provider and store its actions in `row_actions`.
    let actions_clone = actions.clone();
    let provider: super::super::RowActionProvider =
        Arc::new(move |_metric_id| actions_clone.clone());
    let row_actions = provider("");

    assert_eq!(
        row_actions.len(),
        2,
        "both actions should be returned by the provider"
    );
    assert_eq!(row_actions[0].id, "kill");
    assert_eq!(row_actions[1].id, "cancel");
    assert!(
        row_actions[0].is_destructive,
        "kill action should be marked destructive"
    );
    assert!(
        !row_actions[1].is_destructive,
        "cancel action should not be marked destructive"
    );
}

#[test]
fn context_menu_row_actions_keyboard_nav_index_range() {
    // Verify that the index range for row actions is calculated correctly.
    // With: base_count=1 (only Copy), no filter/order/gen_sql/copy_query,
    // and 2 row actions:
    //   idx 0: Copy
    //   idx 1: separator (row actions)
    //   idx 2: Kill Connection
    //   idx 3: Cancel Query
    // total_count = 1 + (1+2) = 4
    // row_actions_start = 1 (after_copy_query = 1)
    // Action at selected_index=2: action_idx = 2 - 1 - 1 = 0 → "kill"
    // Action at selected_index=3: action_idx = 3 - 1 - 1 = 1 → "cancel"

    let row_action_count = 2usize;
    let row_actions_start = 1usize; // after_copy_query when no optional sections
    let total_count = row_actions_start + 1 + row_action_count;

    assert_eq!(
        total_count, 4,
        "total_count should include separator + 2 actions"
    );

    // selected_index=2 maps to action_idx=0
    let selected = 2usize;
    let in_range = selected > row_actions_start && selected <= row_actions_start + row_action_count;
    assert!(in_range, "index 2 should be in the row action range");
    let action_idx = selected - row_actions_start - 1;
    assert_eq!(action_idx, 0, "index 2 → action slot 0");

    // selected_index=3 maps to action_idx=1
    let selected = 3usize;
    let in_range = selected > row_actions_start && selected <= row_actions_start + row_action_count;
    assert!(in_range, "index 3 should be in the row action range");
    let action_idx = selected - row_actions_start - 1;
    assert_eq!(action_idx, 1, "index 3 → action slot 1");

    // The separator itself (index 1) should not be in range
    let selected = 1usize;
    let in_range = selected > row_actions_start && selected <= row_actions_start + row_action_count;
    assert!(
        !in_range,
        "separator index should not be in the action range"
    );
}

#[gpui::test]
fn apply_builder_draft_spec_sets_filter_input_hidden(cx: &mut TestAppContext) {
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
                total_rows: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    let spec = make_test_spec();

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            assert!(
                !panel.builder.filter_input_hidden,
                "filter input should be visible before builder opens"
            );
            assert!(
                panel.builder.builder_draft_spec.is_none(),
                "builder_draft_spec should be None before apply"
            );

            panel.apply_builder_draft_spec(spec.clone(), cx);

            assert!(
                panel.builder.filter_input_hidden,
                "filter input should be hidden after apply_builder_draft_spec"
            );
            assert!(
                panel.builder.builder_draft_spec.is_some(),
                "builder_draft_spec should be Some after apply"
            );
        });
    });
}

#[gpui::test]
fn clear_builder_draft_spec_restores_filter_input_visible(cx: &mut TestAppContext) {
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
                total_rows: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    let spec = make_test_spec();

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.apply_builder_draft_spec(spec.clone(), cx);

            assert!(
                panel.builder.filter_input_hidden,
                "should be hidden after apply"
            );
            assert!(
                panel.builder.builder_draft_spec.is_some(),
                "spec should be stored"
            );

            panel.clear_builder_draft_spec(cx);

            assert!(
                !panel.builder.filter_input_hidden,
                "filter input should be visible again after clear"
            );
            assert!(
                panel.builder.builder_draft_spec.is_none(),
                "builder_draft_spec should be None after clear"
            );
            assert!(
                panel.builder.visual_select.is_none(),
                "visual_select should be None after clear"
            );
        });
    });
}

#[gpui::test]
fn apply_builder_draft_spec_sets_pending_refresh(cx: &mut TestAppContext) {
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
                total_rows: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    let spec = make_test_spec();

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.apply_builder_draft_spec(spec.clone(), cx);

            assert!(
                panel.pending.refresh,
                "apply_builder_draft_spec should queue a refresh"
            );
        });
    });
}

#[gpui::test]
fn can_open_builder_true_for_document_collection_source(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_builder_stub_connection(
        cx,
        dbflux_core::DatabaseCategory::Document,
        dbflux_core::QueryLanguage::MongoQuery,
        Some(dbflux_core::QueryCapabilities::default()),
    );

    let panel = build_panel_for_collection(cx, app_state, profile_id);

    let result = cx.update(|app| panel.read(app).can_open_builder(app));

    assert!(
        result,
        "can_open_builder should be true for a Document collection with predicate operators"
    );
}

#[gpui::test]
fn can_open_builder_false_for_keyvalue_collection_source(cx: &mut TestAppContext) {
    // A key-value store publishes no predicate operators, so the builder is
    // excluded on both the category and the predicate check.
    let no_predicate_caps = dbflux_core::QueryCapabilities {
        where_operators: Vec::new(),
        ..dbflux_core::QueryCapabilities::default()
    };
    let (app_state, profile_id) = register_builder_stub_connection(
        cx,
        dbflux_core::DatabaseCategory::KeyValue,
        dbflux_core::QueryLanguage::RedisCommands,
        Some(no_predicate_caps),
    );

    let panel = build_panel_for_collection(cx, app_state, profile_id);

    let result = cx.update(|app| panel.read(app).can_open_builder(app));

    assert!(
        !result,
        "can_open_builder should be false for a non-Document collection without predicates"
    );
}

#[gpui::test]
fn can_open_builder_false_for_document_collection_without_predicates(cx: &mut TestAppContext) {
    let no_predicate_caps = dbflux_core::QueryCapabilities {
        where_operators: vec![
            dbflux_core::WhereOperator::And,
            dbflux_core::WhereOperator::Or,
        ],
        ..dbflux_core::QueryCapabilities::default()
    };
    let (app_state, profile_id) = register_builder_stub_connection(
        cx,
        dbflux_core::DatabaseCategory::Document,
        dbflux_core::QueryLanguage::MongoQuery,
        Some(no_predicate_caps),
    );

    let panel = build_panel_for_collection(cx, app_state, profile_id);

    let result = cx.update(|app| panel.read(app).can_open_builder(app));

    assert!(
        !result,
        "can_open_builder should be false when only logical predicate operators are published"
    );
}

#[gpui::test]
fn can_open_builder_true_for_sql_table_source(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_builder_stub_connection(
        cx,
        dbflux_core::DatabaseCategory::Relational,
        dbflux_core::QueryLanguage::Sql,
        Some(dbflux_core::QueryCapabilities::default()),
    );

    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id,
                database: Some("app".to_string()),
                table: TableRef::with_schema("public", "users"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    let result = window.update(|_, app| panel.read(app).can_open_builder(app));

    assert!(
        result,
        "can_open_builder should return true for Table source with SQL query language"
    );
}

#[gpui::test]
fn visual_select_caches_precomputed_query(cx: &mut TestAppContext) {
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
                total_rows: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    let pre_select = SelectQuery {
        sql: "SELECT * FROM public.users LIMIT 100".to_string(),
        params: vec![],
    };

    window.update(|_, app| {
        panel.update(app, |panel, _cx| {
            panel.builder.visual_select = Some(pre_select.clone());
        });
    });

    let stored = window.update(|_, app| panel.read(app).builder.visual_select.clone());

    assert_eq!(
        stored,
        Some(pre_select),
        "visual_select should cache the query"
    );
}

// T03: FK cache lives on DataGridPanel
#[gpui::test]
fn fk_cache_lives_on_data_grid_panel(cx: &mut TestAppContext) {
    use super::super::FkLoadState;

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
                total_rows: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
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
            assert!(
                matches!(panel.builder.fk_cache, FkLoadState::Loading),
                "initial fk_cache should be Loading"
            );

            let fk = dbflux_core::SchemaForeignKeyInfo {
                name: "fk_test".to_string(),
                table_name: "users".to_string(),
                columns: vec!["org_id".to_string()],
                referenced_schema: None,
                referenced_table: "organizations".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: None,
                on_update: None,
            };

            panel.apply_fk_result(vec![fk], cx);

            assert!(
                matches!(panel.builder.fk_cache, FkLoadState::Ready(_)),
                "fk_cache should be Ready after apply_fk_result"
            );

            panel.mark_fk_unavailable(cx);

            assert!(
                matches!(panel.builder.fk_cache, FkLoadState::Unavailable),
                "fk_cache should be Unavailable after mark_fk_unavailable"
            );
        });
    });
}

// T27: ensure_fk_cache_loaded is a no-op when cache is already Ready or Unavailable
#[gpui::test]
fn ensure_fk_cache_loaded_noop_when_ready(cx: &mut TestAppContext) {
    use super::super::FkLoadState;

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
                total_rows: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
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
            let fk = dbflux_core::SchemaForeignKeyInfo {
                name: "fk_test".to_string(),
                table_name: "users".to_string(),
                columns: vec!["org_id".to_string()],
                referenced_schema: None,
                referenced_table: "organizations".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: None,
                on_update: None,
            };

            panel.apply_fk_result(vec![fk], cx);

            assert!(
                matches!(panel.builder.fk_cache, FkLoadState::Ready(_)),
                "cache should be Ready before no-op test"
            );

            // Second call should not change the state back to Loading
            panel.ensure_fk_cache_loaded(cx);

            assert!(
                matches!(panel.builder.fk_cache, FkLoadState::Ready(_)),
                "ensure_fk_cache_loaded should be a no-op when cache is Ready"
            );
        });
    });
}

// T27: ensure_fk_cache_loaded is a no-op when cache is Unavailable
#[gpui::test]
fn ensure_fk_cache_loaded_noop_when_unavailable(cx: &mut TestAppContext) {
    use super::super::FkLoadState;

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
                total_rows: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
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
            panel.mark_fk_unavailable(cx);

            assert!(
                matches!(panel.builder.fk_cache, FkLoadState::Unavailable),
                "cache should be Unavailable before no-op test"
            );

            panel.ensure_fk_cache_loaded(cx);

            assert!(
                matches!(panel.builder.fk_cache, FkLoadState::Unavailable),
                "ensure_fk_cache_loaded should be a no-op when cache is Unavailable"
            );
        });
    });
}

// T28: apply_fk_result triggers pending_refresh when state was Resolving
#[gpui::test]
fn fk_result_triggers_requery_when_resolving(cx: &mut TestAppContext) {
    use super::super::{FkLoadState, filter_bar::RelationalFilterState};

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
                total_rows: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
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
            panel.builder.relational_filter_state = RelationalFilterState::Resolving;

            assert!(
                !panel.pending.refresh,
                "pending_refresh should be false before FK result arrives"
            );

            let fk = dbflux_core::SchemaForeignKeyInfo {
                name: "fk_test".to_string(),
                table_name: "users".to_string(),
                columns: vec!["org_id".to_string()],
                referenced_schema: None,
                referenced_table: "organizations".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: None,
                on_update: None,
            };

            panel.apply_fk_result(vec![fk], cx);

            assert!(
                matches!(panel.builder.fk_cache, FkLoadState::Ready(_)),
                "fk_cache should be Ready after apply_fk_result"
            );
            assert!(
                panel.pending.refresh,
                "pending_refresh should be set when FK result arrives while Resolving"
            );
        });
    });
}

// T31: Collection source never uses relational lowering — relational_filter_state stays Inactive
#[gpui::test]
fn collection_source_relational_state_stays_inactive(cx: &mut TestAppContext) {
    use super::super::filter_bar::RelationalFilterState;

    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Collection {
                profile_id: Uuid::nil(),
                collection: CollectionRef::new("app", "users"),
                pagination: Pagination::default(),
                total_docs: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    window.update(|_, app| {
        panel.update(app, |panel, _cx| {
            assert!(
                matches!(
                    panel.builder.relational_filter_state,
                    RelationalFilterState::Inactive
                ),
                "Collection source should start Inactive"
            );

            // Collection sources never enter the relational filter path, so
            // the state must remain Inactive even if FK data were present.
            assert!(
                !matches!(panel.source, DataSource::Table { .. }),
                "source must be a Collection for this test"
            );
        });
    });
}

// T31: FkLoadState::Unavailable leaves relational_filter_state Inactive (S-11)
#[gpui::test]
fn unavailable_fk_cache_leaves_state_inactive(cx: &mut TestAppContext) {
    use super::super::{FkLoadState, filter_bar::RelationalFilterState};

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
                total_rows: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
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
            panel.mark_fk_unavailable(cx);

            assert!(
                matches!(panel.builder.fk_cache, FkLoadState::Unavailable),
                "cache should be Unavailable"
            );

            // Even with Unavailable cache, relational_filter_state should remain
            // Inactive — no error shown for missing FK data (S-11)
            assert!(
                matches!(
                    panel.builder.relational_filter_state,
                    RelationalFilterState::Inactive
                ),
                "relational_filter_state must stay Inactive when FK cache is Unavailable"
            );
        });
    });
}

// T31: parse failure leaves relational_filter_state Inactive (FR-PARSE-7)
#[gpui::test]
fn parse_failure_leaves_relational_state_inactive(cx: &mut TestAppContext) {
    use super::super::{FkLoadState, filter_bar::RelationalFilterState};

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
                total_rows: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
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
            let fk = dbflux_core::SchemaForeignKeyInfo {
                name: "fk_test".to_string(),
                table_name: "users".to_string(),
                columns: vec!["org_id".to_string()],
                referenced_schema: None,
                referenced_table: "organizations".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: None,
                on_update: None,
            };

            panel.apply_fk_result(vec![fk], cx);

            assert!(
                matches!(panel.builder.fk_cache, FkLoadState::Ready(_)),
                "cache should be Ready for parse-failure test"
            );

            // Directly verify: parse error in parse_and_resolve leaves state Inactive.
            // `try_relational_filter` is private, so we verify the parse_and_resolve
            // error path via the public parse_and_resolve function directly.
            use dbflux_core::{DefaultSqlDialect, SourceTable, parse_and_resolve};

            // Syntactically invalid input — parser should fail
            let result = parse_and_resolve(
                "bare_identifier_no_comparator",
                SourceTable {
                    schema: None,
                    table: "users".to_string(),
                    alias: "users".to_string(),
                },
                &[],
                &DefaultSqlDialect,
            );

            // FR-PARSE-7: parse errors must be an error
            assert!(
                result.is_err(),
                "syntactically invalid input must produce a parse error"
            );

            // And the panel state should still be Inactive (parse error never modifies state)
            assert!(
                matches!(
                    panel.builder.relational_filter_state,
                    RelationalFilterState::Inactive
                ),
                "relational_filter_state must remain Inactive after parse error"
            );
        });
    });
}

// H-3 — ApprovalRequired does not open a confirmation modal (spec DR-12.4)
//
// On non-MCP builds the policy gate shows an error toast and returns early.
// On MCP builds it enqueues via ApprovalService and returns early.
// In either case `pending_mutation_modal` must remain None.
#[gpui::test]
fn h3_approval_required_does_not_open_confirmation_modal(cx: &mut TestAppContext) {
    use dbflux_core::{
        ConnectedProfile, Connection, DatabaseCategory, DbConfig, DbError, DbKind,
        DriverCapabilities, DriverMetadata, Icon as CoreIcon, MutationPolicy, QueryLanguage,
        QueryResult as CoreQueryResult, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect,
        TransferFamily,
    };
    use std::path::PathBuf;

    init_test_runtime(cx);

    struct StubSqlConnection2;

    impl Connection for StubSqlConnection2 {
        fn metadata(&self) -> &DriverMetadata {
            static META: std::sync::OnceLock<DriverMetadata> = std::sync::OnceLock::new();
            META.get_or_init(|| DriverMetadata {
                id: "stub-sql-2".to_string(),
                display_name: "Stub SQL 2".to_string(),
                description: "test stub for H-3".to_string(),
                category: DatabaseCategory::Relational,
                transfer_family: TransferFamily::Sql,
                deployment_class: None,
                query_language: QueryLanguage::Sql,
                capabilities: DriverCapabilities::empty(),
                default_port: None,
                uri_scheme: "stub2".to_string(),
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

        fn kind(&self) -> DbKind {
            DbKind::Postgres
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }

        #[expect(
            clippy::unimplemented,
            reason = "test-only stub connection: no test path in this suite calls the dialect"
        )]
        fn dialect(&self) -> &dyn SqlDialect {
            unimplemented!("StubSqlConnection2::dialect not needed")
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, _req: &dbflux_core::QueryRequest) -> Result<CoreQueryResult, DbError> {
            Err(DbError::NotSupported("stub2".to_string()))
        }

        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Ok(SchemaSnapshot::default())
        }
    }

    let profile_id = Uuid::new_v4();

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let storage_runtime = StorageRuntime::in_memory().expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage_runtime).expect("test storage setup")
        })
    });

    cx.update(|cx| {
        app_state.update(cx, |app, _cx| {
            let profile = dbflux_core::ConnectionProfile::new(
                "approval-test",
                DbConfig::SQLite {
                    path: PathBuf::from(":memory:"),
                    connection_id: None,
                },
            );
            let connected = ConnectedProfile {
                profile,
                connection: Arc::new(StubSqlConnection2),
                schema: None,
                mutation_policy: MutationPolicy::ApprovalRequired,
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
                database: Some("test".to_string()),
                table: TableRef::with_schema("public", "orders"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    let spec = dbflux_core::VisualMutationSpec {
        from: TableRef {
            schema: Some("public".to_string()),
            name: "orders".to_string(),
        },
        filter: None,
        kind: dbflux_core::MutationKind::Delete,
    };

    let opts = crate::data_grid_panel::mutation_executor::MutationExecOptions::single_transaction();

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.on_mutation_run_requested(spec, opts, None, window, cx);
        });
    });

    let pending_modal_is_none =
        window.update(|_, app| panel.read(app).pending.mutation_modal.is_none());

    assert!(
        pending_modal_is_none,
        "ApprovalRequired must not open a confirmation modal — \
         pending_mutation_modal must remain None"
    );
}

// =========================================================================
// FK display resolution in the grid
// =========================================================================

/// Connection stub whose `fetch_row_by_pk` counts its calls and answers the
/// id 8 with a customers row when `found` is set, so the resolution path can
/// be driven without a live database.
pub(super) struct FkResolveConnection {
    pub(super) fetches: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub(super) found: bool,
}

impl dbflux_core::Connection for FkResolveConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        static META: std::sync::OnceLock<dbflux_core::DriverMetadata> = std::sync::OnceLock::new();
        META.get_or_init(|| dbflux_core::DriverMetadata {
            id: "fk-resolve".to_string(),
            display_name: "FK Resolve".to_string(),
            description: "test stub for fk display".to_string(),
            category: dbflux_core::DatabaseCategory::Relational,
            transfer_family: dbflux_core::TransferFamily::Sql,
            deployment_class: None,
            query_language: dbflux_core::QueryLanguage::Sql,
            capabilities: dbflux_core::DriverCapabilities::empty(),
            default_port: None,
            uri_scheme: "fk-resolve".to_string(),
            icon: dbflux_core::Icon::Database,
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
        dbflux_core::DbKind::Postgres
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        dbflux_core::SchemaLoadingStrategy::SingleDatabase
    }

    #[expect(
        clippy::unimplemented,
        reason = "test-only stub connection: the fk display path never asks for a dialect"
    )]
    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        unimplemented!("FkResolveConnection::dialect not needed")
    }

    fn ping(&self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn execute(
        &self,
        _req: &dbflux_core::QueryRequest,
    ) -> Result<QueryResult, dbflux_core::DbError> {
        Err(dbflux_core::DbError::NotSupported("fk-resolve".to_string()))
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }

    fn fetch_row_by_pk(
        &self,
        _database: &str,
        _schema: &str,
        _table: &str,
        _pk_column: &str,
        pk_value: &dbflux_core::Value,
    ) -> Result<Option<std::collections::HashMap<String, dbflux_core::Value>>, dbflux_core::DbError>
    {
        self.fetches
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let name = match (self.found, pk_value) {
            (true, dbflux_core::Value::Int(8)) => {
                dbflux_core::Value::Text("María López".to_string())
            }
            // A textual id answers with itself as the id, like a UUID pk.
            (true, id @ dbflux_core::Value::Text(_)) => {
                return Ok(Some(std::collections::HashMap::from([
                    ("id".to_string(), id.clone()),
                    (
                        "name".to_string(),
                        dbflux_core::Value::Text("Norte".to_string()),
                    ),
                ])));
            }
            _ => return Ok(None),
        };

        Ok(Some(std::collections::HashMap::from([
            ("id".to_string(), dbflux_core::Value::Int(8)),
            ("name".to_string(), name),
        ])))
    }
}

/// Caches a referenced table whose `id` / `name` columns make `name` the
/// display column the resolution picks.
pub(super) fn cache_referenced_table_details(
    cx: &mut TestAppContext,
    app_state: &gpui::Entity<AppStateEntity>,
    profile_id: Uuid,
    table: &str,
) {
    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            let column =
                |name: &str, type_name: &str, is_primary_key: bool| dbflux_core::ColumnInfo {
                    name: name.to_string(),
                    type_name: type_name.to_string(),
                    nullable: true,
                    is_primary_key,
                    default_value: None,
                    enum_values: None,
                };

            app.set_table_details(
                profile_id,
                "app".to_string(),
                Some("public".to_string()),
                table.to_string(),
                dbflux_core::TableInfo {
                    name: table.to_string(),
                    schema: Some("public".to_string()),
                    columns: Some(vec![
                        column("id", "int4", true),
                        column("name", "text", false),
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
}

/// Caches the referenced customers table, whose `name` column is the
/// display column the resolution picks.
pub(super) fn cache_customers_details(
    cx: &mut TestAppContext,
    app_state: &gpui::Entity<AppStateEntity>,
    profile_id: Uuid,
) {
    cache_referenced_table_details(cx, app_state, profile_id, "customers");
}

/// A grid over a page of `public.orders` with the given columns, rows and
/// foreign keys, built the way a landed page leaves the panel.
fn panel_with_fk_page<'a>(
    cx: &'a mut TestAppContext,
    app_state: gpui::Entity<AppStateEntity>,
    profile_id: Uuid,
    columns: Vec<(&'a str, bool)>,
    rows: Vec<Vec<dbflux_core::Value>>,
    foreign_keys: Vec<dbflux_core::SchemaForeignKeyInfo>,
) -> (gpui::Entity<DataGridPanel>, &'a mut gpui::VisualTestContext) {
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id,
                database: Some("app".to_string()),
                table: TableRef::with_schema("public", "orders"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: None,
            };

            let mut panel = DataGridPanel::new_internal(
                source,
                app_state.clone(),
                vec!["id".to_string()],
                window,
                cx,
            );

            let column = |name: &str, is_primary_key: bool| ColumnMeta {
                name: name.to_string(),
                type_name: "int4".to_string(),
                kind: ColumnKind::Integer,
                nullable: false,
                is_primary_key,
            };
            panel.result = QueryResult::table(
                columns
                    .iter()
                    .map(|(name, is_primary_key)| column(name, *is_primary_key))
                    .collect(),
                rows,
                None,
                Duration::ZERO,
            );

            panel.builder.fk_cache = super::super::FkLoadState::Ready(foreign_keys);

            panel.rebuild_table(None, cx);
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

/// A grid over a page of `public.orders` whose `customer_id` foreign key
/// points at `public.customers`, with the same customer on both rows.
fn panel_with_fk_grid(
    cx: &mut TestAppContext,
    app_state: gpui::Entity<AppStateEntity>,
    profile_id: Uuid,
) -> (gpui::Entity<DataGridPanel>, &mut gpui::VisualTestContext) {
    panel_with_fk_page(
        cx,
        app_state,
        profile_id,
        vec![("id", true), ("customer_id", false)],
        vec![
            vec![dbflux_core::Value::Int(1), dbflux_core::Value::Int(8)],
            vec![dbflux_core::Value::Int(2), dbflux_core::Value::Int(8)],
        ],
        vec![dbflux_core::SchemaForeignKeyInfo {
            name: "orders_customer_fk".to_string(),
            table_name: "orders".to_string(),
            columns: vec!["customer_id".to_string()],
            referenced_schema: Some("public".to_string()),
            referenced_table: "customers".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: None,
            on_update: None,
        }],
    )
}

/// The fk cell as `(label, edit_text)` of `row`, col 1.
pub(super) fn fk_cell(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut gpui::VisualTestContext,
    row: usize,
) -> (Option<String>, String) {
    fk_cell_at(panel, window, row, 1)
}

/// The cell as `(label, edit_text)` of `row`, `col`.
pub(super) fn fk_cell_at(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut gpui::VisualTestContext,
    row: usize,
    col: usize,
) -> (Option<String>, String) {
    window.update(|_, app| {
        let panel_read = panel.read(app);
        let table_state = panel_read
            .grid_table
            .table_state
            .as_ref()
            .expect("rebuild builds the table");
        let state = table_state.read(app);
        let cell = state
            .model()
            .cell(row, col)
            .expect("the fk cell exists")
            .clone();

        (
            cell.fk_spans().map(|(label, _)| label.to_string()),
            cell.edit_text(),
        )
    })
}

#[gpui::test]
fn fk_display_resolution_labels_cells_and_caches_the_fetch(cx: &mut TestAppContext) {
    use std::sync::atomic::Ordering;

    let fetches = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let connection = std::sync::Arc::new(FkResolveConnection {
        fetches: fetches.clone(),
        found: true,
    });
    let (app_state, profile_id) = register_stub_connection(cx, connection);
    cache_customers_details(cx, &app_state, profile_id);
    enable_grid_fk_labels(cx, &app_state);

    let (panel, window) = panel_with_fk_grid(cx, app_state, profile_id);

    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.resolve_fk_display_values(cx));
    });
    window.run_until_parked();

    let (label, edit_text) = fk_cell(&panel, window, 0);
    assert_eq!(label.as_deref(), Some("María López"));
    assert_eq!(edit_text, "8", "editing must carry the raw id");
    let (second_label, _) = fk_cell(&panel, window, 1);
    assert_eq!(
        second_label.as_deref(),
        Some("María López"),
        "the repeated id on another row resolves from the same fetch"
    );
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        1,
        "the distinct id is fetched once"
    );

    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.resolve_fk_display_values(cx));
    });
    window.run_until_parked();

    assert_eq!(
        fetches.load(Ordering::SeqCst),
        1,
        "a resolved id is applied from the cache without a new query"
    );
    let (label, _) = fk_cell(&panel, window, 0);
    assert_eq!(label.as_deref(), Some("María López"));
}

#[gpui::test]
fn fk_display_missing_row_keeps_the_plain_cell_and_is_not_refetched(cx: &mut TestAppContext) {
    use std::sync::atomic::Ordering;

    let fetches = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let connection = std::sync::Arc::new(FkResolveConnection {
        fetches: fetches.clone(),
        found: false,
    });
    let (app_state, profile_id) = register_stub_connection(cx, connection);
    cache_customers_details(cx, &app_state, profile_id);
    enable_grid_fk_labels(cx, &app_state);

    let (panel, window) = panel_with_fk_grid(cx, app_state, profile_id);

    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.resolve_fk_display_values(cx));
    });
    window.run_until_parked();

    let (label, edit_text) = fk_cell(&panel, window, 0);
    assert_eq!(label, None, "an id no row answers keeps the plain cell");
    assert_eq!(edit_text, "8", "the raw id stays readable");
    assert_eq!(fetches.load(Ordering::SeqCst), 1);

    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.resolve_fk_display_values(cx));
    });
    window.run_until_parked();

    assert_eq!(
        fetches.load(Ordering::SeqCst),
        1,
        "a missing row is cached and not fetched again"
    );
}

/// One `fetch_rows_by_pks` call as [`FkBatchConnection`] recorded it.
struct RecordedBatchCall {
    table: String,
    values: Vec<dbflux_core::Value>,
    projection: Vec<String>,
}

/// Connection stub whose `fetch_rows_by_pks` records its calls and answers
/// from a fixed id → label table, so the batch path can be driven without a
/// live database. `fetch_row_by_pk` stays unsupported on purpose: any per-id
/// fallback in the UI fails these tests loudly instead of passing.
struct FkBatchConnection {
    calls: std::sync::Arc<std::sync::Mutex<Vec<RecordedBatchCall>>>,
}

impl dbflux_core::Connection for FkBatchConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        static META: std::sync::OnceLock<dbflux_core::DriverMetadata> = std::sync::OnceLock::new();
        META.get_or_init(|| dbflux_core::DriverMetadata {
            id: "fk-batch".to_string(),
            display_name: "FK Batch".to_string(),
            description: "test stub for the fk display batch path".to_string(),
            category: dbflux_core::DatabaseCategory::Relational,
            transfer_family: dbflux_core::TransferFamily::Sql,
            deployment_class: None,
            query_language: dbflux_core::QueryLanguage::Sql,
            capabilities: dbflux_core::DriverCapabilities::empty(),
            default_port: None,
            uri_scheme: "fk-batch".to_string(),
            icon: dbflux_core::Icon::Database,
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
        reason = "test-only stub connection: the fk display batch path never asks for a dialect"
    )]
    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        unimplemented!("FkBatchConnection::dialect not needed")
    }

    fn ping(&self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn execute(
        &self,
        _req: &dbflux_core::QueryRequest,
    ) -> Result<QueryResult, dbflux_core::DbError> {
        Err(dbflux_core::DbError::NotSupported("fk-batch".to_string()))
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }

    fn fetch_rows_by_pks(
        &self,
        _database: &str,
        _schema: &str,
        table: &str,
        pk_column: &str,
        values: &[dbflux_core::Value],
        projection: Option<&[String]>,
    ) -> Result<Vec<std::collections::HashMap<String, dbflux_core::Value>>, dbflux_core::DbError>
    {
        if let Ok(mut calls) = self.calls.lock() {
            calls.push(RecordedBatchCall {
                table: table.to_string(),
                values: values.to_vec(),
                projection: projection
                    .map(|columns| columns.to_vec())
                    .unwrap_or_default(),
            });
        }

        let labels: &[(i64, &str)] = match table {
            "customers" => &[(8, "María López"), (10, "Norte")],
            "vendors" => &[(5, "Acme")],
            _ => &[],
        };

        Ok(values
            .iter()
            .filter_map(|value| {
                let dbflux_core::Value::Int(id) = value else {
                    return None;
                };

                labels
                    .iter()
                    .find(|(known, _)| known == id)
                    .map(|(id, label)| {
                        std::collections::HashMap::from([
                            (pk_column.to_string(), dbflux_core::Value::Int(*id)),
                            (
                                "name".to_string(),
                                dbflux_core::Value::Text(label.to_string()),
                            ),
                        ])
                    })
            })
            .collect())
    }
}

/// The ids of a page whose foreign keys all point at one referenced table
/// ride a single batch fetch — with the pk and display column projected —
/// and the round swaps the grid's model once. An id the batch does not
/// answer is cached as a miss, so a second round fetches nothing.
#[gpui::test]
fn fk_display_batches_one_fetch_per_referenced_table_and_caches_misses(cx: &mut TestAppContext) {
    let calls: std::sync::Arc<std::sync::Mutex<Vec<RecordedBatchCall>>> = Default::default();
    let connection = std::sync::Arc::new(FkBatchConnection {
        calls: calls.clone(),
    });
    let (app_state, profile_id) = register_stub_connection(cx, connection);
    cache_customers_details(cx, &app_state, profile_id);
    enable_grid_fk_labels(cx, &app_state);

    let (panel, window) = panel_with_fk_page(
        cx,
        app_state,
        profile_id,
        vec![("id", true), ("customer_id", false)],
        vec![
            vec![dbflux_core::Value::Int(1), dbflux_core::Value::Int(8)],
            vec![dbflux_core::Value::Int(2), dbflux_core::Value::Int(9)],
            vec![dbflux_core::Value::Int(3), dbflux_core::Value::Int(10)],
            vec![dbflux_core::Value::Int(4), dbflux_core::Value::Int(8)],
        ],
        vec![dbflux_core::SchemaForeignKeyInfo {
            name: "orders_customer_fk".to_string(),
            table_name: "orders".to_string(),
            columns: vec!["customer_id".to_string()],
            referenced_schema: Some("public".to_string()),
            referenced_table: "customers".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: None,
            on_update: None,
        }],
    );

    // `set_model` emits SelectionChanged on every swap, so counting the
    // events counts the model swaps of the round.
    let swaps = Rc::new(RefCell::new(0usize));
    let swap_count = swaps.clone();
    let _subscription = window.update(|_, cx| {
        let table_state = panel
            .read(cx)
            .grid_table
            .table_state
            .clone()
            .expect("rebuild builds the table");

        cx.subscribe(
            &table_state,
            move |_, event: &dbflux_components::components::data_table::DataTableEvent, _| {
                if matches!(
                    event,
                    dbflux_components::components::data_table::DataTableEvent::SelectionChanged(_)
                ) {
                    *swap_count.borrow_mut() += 1;
                }
            },
        )
    });

    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.resolve_fk_display_values(cx));
    });
    window.run_until_parked();

    let recorded = calls.lock().expect("the stub records its calls");
    assert_eq!(
        recorded.len(),
        1,
        "every fk of the page points at one table, so one batch serves them all"
    );
    let call = recorded.first().expect("the batch call exists");
    assert_eq!(call.table, "customers");
    assert_eq!(
        call.values,
        vec![
            dbflux_core::Value::Int(8),
            dbflux_core::Value::Int(9),
            dbflux_core::Value::Int(10),
        ],
        "every distinct id of the page rides the same batch"
    );
    assert_eq!(
        call.projection,
        vec!["id".to_string(), "name".to_string()],
        "the batch projects the pk and the display column only"
    );
    drop(recorded);

    let (label, edit_text) = fk_cell(&panel, window, 0);
    assert_eq!(label.as_deref(), Some("María López"));
    assert_eq!(edit_text, "8", "editing must carry the raw id");
    let (missing_label, missing_edit) = fk_cell(&panel, window, 1);
    assert_eq!(
        missing_label, None,
        "an id the batch does not answer keeps the plain cell"
    );
    assert_eq!(missing_edit, "9");
    let (label, _) = fk_cell(&panel, window, 2);
    assert_eq!(label.as_deref(), Some("Norte"));
    let (label, _) = fk_cell(&panel, window, 3);
    assert_eq!(
        label.as_deref(),
        Some("María López"),
        "the repeated id resolves from the same batch"
    );
    assert_eq!(*swaps.borrow(), 1, "the round swaps the model exactly once");

    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.resolve_fk_display_values(cx));
    });
    window.run_until_parked();

    assert_eq!(
        calls.lock().expect("the stub records its calls").len(),
        1,
        "a cached miss is not fetched again"
    );
}

/// Foreign keys pointing at two distinct referenced tables produce one batch
/// per table, each carrying that table's ids and projection.
#[gpui::test]
fn fk_display_batches_one_fetch_per_distinct_referenced_table(cx: &mut TestAppContext) {
    let calls: std::sync::Arc<std::sync::Mutex<Vec<RecordedBatchCall>>> = Default::default();
    let connection = std::sync::Arc::new(FkBatchConnection {
        calls: calls.clone(),
    });
    let (app_state, profile_id) = register_stub_connection(cx, connection);
    cache_customers_details(cx, &app_state, profile_id);
    cache_referenced_table_details(cx, &app_state, profile_id, "vendors");
    enable_grid_fk_labels(cx, &app_state);

    let fk = |name: &str, column: &str, table: &str| dbflux_core::SchemaForeignKeyInfo {
        name: format!("orders_{name}_fk"),
        table_name: "orders".to_string(),
        columns: vec![column.to_string()],
        referenced_schema: Some("public".to_string()),
        referenced_table: table.to_string(),
        referenced_columns: vec!["id".to_string()],
        on_delete: None,
        on_update: None,
    };

    let (panel, window) = panel_with_fk_page(
        cx,
        app_state,
        profile_id,
        vec![("id", true), ("customer_id", false), ("vendor_id", false)],
        vec![
            vec![
                dbflux_core::Value::Int(1),
                dbflux_core::Value::Int(8),
                dbflux_core::Value::Int(5),
            ],
            vec![
                dbflux_core::Value::Int(2),
                dbflux_core::Value::Int(9),
                dbflux_core::Value::Int(6),
            ],
        ],
        vec![
            fk("customer", "customer_id", "customers"),
            fk("vendor", "vendor_id", "vendors"),
        ],
    );

    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.resolve_fk_display_values(cx));
    });
    window.run_until_parked();

    let recorded = calls.lock().expect("the stub records its calls");
    assert_eq!(
        recorded.len(),
        2,
        "two referenced tables answer to two batches"
    );
    for call in recorded.iter() {
        assert_eq!(
            call.projection,
            vec!["id".to_string(), "name".to_string()],
            "each batch projects the pk and the display column only"
        );
    }

    let customers = recorded
        .iter()
        .find(|call| call.table == "customers")
        .expect("customers ride their own batch");
    assert_eq!(
        customers.values,
        vec![dbflux_core::Value::Int(8), dbflux_core::Value::Int(9)]
    );
    let vendors = recorded
        .iter()
        .find(|call| call.table == "vendors")
        .expect("vendors ride their own batch");
    assert_eq!(
        vendors.values,
        vec![dbflux_core::Value::Int(5), dbflux_core::Value::Int(6)]
    );
    drop(recorded);

    let (customer, _) = fk_cell(&panel, window, 0);
    assert_eq!(customer.as_deref(), Some("María López"));
    let (vendor, _) = fk_cell_at(&panel, window, 0, 2);
    assert_eq!(vendor.as_deref(), Some("Acme"));
    let (customer, _) = fk_cell(&panel, window, 1);
    assert_eq!(customer, None, "the customers miss keeps its raw id");
    let (vendor, vendor_edit) = fk_cell_at(&panel, window, 1, 2);
    assert_eq!(vendor, None, "the vendors miss keeps its raw id");
    assert_eq!(vendor_edit, "6");
}

/// A grid over `orders` opened the way the sidebar opens SQLite tables —
/// `database: None`, schema-less — whose `customer_id` TEXT foreign key
/// points at `customers` keyed by a TEXT uuid id.
#[gpui::test]
fn fk_display_resolution_labels_text_uuid_ids(cx: &mut TestAppContext) {
    let fetches = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let connection = std::sync::Arc::new(FkResolveConnection {
        fetches: fetches.clone(),
        found: true,
    });
    let (app_state, profile_id) = register_stub_connection(cx, connection);
    enable_grid_fk_labels(cx, &app_state);

    let uuid = "44444444-0000-4000-8000-000000000001".to_string();
    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            app.set_table_details(
                profile_id,
                "default".to_string(),
                None,
                "customers".to_string(),
                dbflux_core::TableInfo {
                    name: "customers".to_string(),
                    schema: None,
                    columns: Some(vec![
                        dbflux_core::ColumnInfo {
                            name: "id".to_string(),
                            type_name: "TEXT".to_string(),
                            nullable: false,
                            is_primary_key: true,
                            default_value: None,
                            enum_values: None,
                        },
                        dbflux_core::ColumnInfo {
                            name: "name".to_string(),
                            type_name: "TEXT".to_string(),
                            nullable: false,
                            is_primary_key: false,
                            default_value: None,
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

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id,
                database: None,
                table: TableRef::new("orders".to_string()),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: None,
            };

            let mut panel = DataGridPanel::new_internal(
                source,
                app_state.clone(),
                vec!["id".to_string()],
                window,
                cx,
            );

            let column = |name: &str, is_primary_key: bool| ColumnMeta {
                name: name.to_string(),
                type_name: "TEXT".to_string(),
                kind: ColumnKind::Text,
                nullable: false,
                is_primary_key,
            };
            panel.result = QueryResult::table(
                vec![column("id", true), column("customer_id", false)],
                vec![vec![
                    dbflux_core::Value::Text(uuid.clone()),
                    dbflux_core::Value::Text(uuid.clone()),
                ]],
                None,
                Duration::ZERO,
            );

            panel.builder.fk_cache =
                super::super::FkLoadState::Ready(vec![dbflux_core::SchemaForeignKeyInfo {
                    name: "orders_customer_fk".to_string(),
                    table_name: "orders".to_string(),
                    columns: vec!["customer_id".to_string()],
                    referenced_schema: None,
                    referenced_table: "customers".to_string(),
                    referenced_columns: vec!["id".to_string()],
                    on_delete: None,
                    on_update: None,
                }]);

            panel.rebuild_table(None, cx);
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
        panel.update(app, |panel, cx| panel.resolve_fk_display_values(cx));
    });
    window.run_until_parked();

    window.update(|_, app| {
        let panel_read = panel.read(app);
        let table_state = panel_read
            .grid_table
            .table_state
            .as_ref()
            .expect("rebuild builds the table");
        let cell = table_state
            .read(app)
            .model()
            .cell(0, 1)
            .expect("the fk cell exists")
            .clone();

        assert_eq!(
            cell.fk_spans().map(|(label, _)| label.to_string()),
            Some("Norte".to_string()),
            "a TEXT uuid id must resolve to the referenced row's label"
        );
        assert_eq!(cell.edit_text(), uuid, "editing must carry the raw id");
    });
}

// =========================================================================
// FK labels through the real table-open flow
// =========================================================================

/// Connection stub that answers everything the table-open flow asks of it:
/// a page of `orders`, the schema's foreign keys and the referenced
/// customers row. The SQLite shape — the sidebar opens its tables with no
/// database name.
struct TableOpenFkConnection {
    fetches: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl dbflux_core::Connection for TableOpenFkConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        static META: std::sync::OnceLock<dbflux_core::DriverMetadata> = std::sync::OnceLock::new();
        META.get_or_init(|| dbflux_core::DriverMetadata {
            id: "table-open-fk".to_string(),
            display_name: "Table Open FK".to_string(),
            description: "test stub for the table-open fk flow".to_string(),
            category: dbflux_core::DatabaseCategory::Relational,
            transfer_family: dbflux_core::TransferFamily::Sql,
            deployment_class: None,
            query_language: dbflux_core::QueryLanguage::Sql,
            capabilities: dbflux_core::DriverCapabilities::empty(),
            default_port: None,
            uri_scheme: "table-open-fk".to_string(),
            icon: dbflux_core::Icon::Database,
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
        reason = "test-only stub connection: the table-open path never asks for a dialect"
    )]
    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        unimplemented!("TableOpenFkConnection::dialect not needed for this test")
    }

    fn ping(&self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn execute(
        &self,
        _req: &dbflux_core::QueryRequest,
    ) -> Result<QueryResult, dbflux_core::DbError> {
        Err(dbflux_core::DbError::NotSupported(
            "table-open-fk".to_string(),
        ))
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }

    fn browse_table(
        &self,
        _request: &dbflux_core::TableBrowseRequest,
    ) -> Result<QueryResult, dbflux_core::DbError> {
        let column = |name: &str, is_primary_key: bool| ColumnMeta {
            name: name.to_string(),
            type_name: "INTEGER".to_string(),
            kind: ColumnKind::Integer,
            nullable: false,
            is_primary_key,
        };

        Ok(QueryResult::table(
            vec![column("id", true), column("customer_id", false)],
            vec![vec![dbflux_core::Value::Int(1), dbflux_core::Value::Int(8)]],
            None,
            Duration::ZERO,
        ))
    }

    fn count_table(
        &self,
        _request: &dbflux_core::TableCountRequest,
    ) -> Result<u64, dbflux_core::DbError> {
        Ok(1)
    }

    fn schema_foreign_keys(
        &self,
        _database: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<dbflux_core::SchemaForeignKeyInfo>, dbflux_core::DbError> {
        Ok(vec![dbflux_core::SchemaForeignKeyInfo {
            name: "orders_customer_fk".to_string(),
            table_name: "orders".to_string(),
            columns: vec!["customer_id".to_string()],
            referenced_schema: None,
            referenced_table: "customers".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: None,
            on_update: None,
        }])
    }

    fn fetch_row_by_pk(
        &self,
        _database: &str,
        _schema: &str,
        _table: &str,
        _pk_column: &str,
        _pk_value: &dbflux_core::Value,
    ) -> Result<Option<std::collections::HashMap<String, dbflux_core::Value>>, dbflux_core::DbError>
    {
        self.fetches
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        Ok(Some(std::collections::HashMap::from([
            ("id".to_string(), dbflux_core::Value::Int(8)),
            (
                "name".to_string(),
                dbflux_core::Value::Text("María López".to_string()),
            ),
        ])))
    }
}

/// The startup flow behind `grid_fk_labels`: the setting is already on, the
/// table opens the way the sidebar opens SQLite ones — `database: None` —
/// and its first page lands. Nothing is pre-installed on the panel: the FK
/// metadata must load on its own for the cell to show the referenced row's
/// label instead of the raw id.
#[gpui::test]
fn fk_labels_resolve_when_the_table_opens_without_a_database_name(cx: &mut TestAppContext) {
    use std::sync::atomic::Ordering;

    let fetches = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let connection = std::sync::Arc::new(TableOpenFkConnection {
        fetches: fetches.clone(),
    });
    let (app_state, profile_id) = register_stub_connection(cx, connection);
    enable_grid_fk_labels(cx, &app_state);

    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            let column = |name: &str, is_primary_key: bool| dbflux_core::ColumnInfo {
                name: name.to_string(),
                type_name: if is_primary_key { "INTEGER" } else { "TEXT" }.to_string(),
                nullable: false,
                is_primary_key,
                default_value: None,
                enum_values: None,
            };

            // Keyed the way the panel resolves a nameless database:
            // `table_details_database` falls back to "default".
            app.set_table_details(
                profile_id,
                "default".to_string(),
                None,
                "orders".to_string(),
                dbflux_core::TableInfo {
                    name: "orders".to_string(),
                    schema: None,
                    columns: Some(vec![column("id", true), column("customer_id", false)]),
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
            app.set_table_details(
                profile_id,
                "default".to_string(),
                None,
                "customers".to_string(),
                dbflux_core::TableInfo {
                    name: "customers".to_string(),
                    schema: None,
                    columns: Some(vec![column("id", true), column("name", false)]),
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

    let (_, window) = cx.add_window_view(move |window, cx| {
        let panel = cx.new(|cx| {
            DataGridPanel::new_for_table(
                profile_id,
                TableRef::new("orders".to_string()),
                None,
                app_state.clone(),
                window,
                cx,
            )
        });
        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });
    window.run_until_parked();

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("the window builder stores the grid");

    let (label, edit_text) = fk_cell(&panel, window, 0);
    assert_eq!(
        label.as_deref(),
        Some("María López"),
        "the fk cell of a table opened without a database name resolves"
    );
    assert_eq!(edit_text, "8", "editing must carry the raw id");
    assert!(
        fetches.load(Ordering::SeqCst) >= 1,
        "the referenced row is fetched through the panel's own flow"
    );
}
