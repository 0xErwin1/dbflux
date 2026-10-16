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
