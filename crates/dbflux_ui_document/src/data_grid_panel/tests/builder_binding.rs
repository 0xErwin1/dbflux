use super::*;

// =========================================================================
// T2/T3/T5/T6 — compute_builder_binding + rebuild_table binding wiring
// =========================================================================

/// T3.1-B: compute_builder_binding returns None for a grouped spec, leaving
/// pk_columns empty so mutations_enabled() stays false.
#[test]
fn compute_builder_binding_returns_none_for_grouped_spec() {
    let grouped_spec = make_grouped_spec();
    // Cold lookup (returns None) — but grouped gate fires first.
    let result = mutations_enabled_predicate(Some(&grouped_spec), &[]);
    assert!(
        !result,
        "grouped spec must disable mutations regardless of pk"
    );
}

/// T3.1-C: compute_builder_binding returns None on cold schema cache (pk_lookup → None).
#[test]
fn compute_builder_binding_returns_none_on_cold_cache() {
    use dbflux_core::VisualQuerySpec;

    let spec = VisualQuerySpec {
        source: SourceTable {
            schema: None,
            table: "users".to_string(),
            alias: "users".to_string(),
        },
        projection: Projection::All,
        joins: vec![],
        filter: None,
        group_by: vec![],
        aggregates: vec![],
        having: None,
        sort: vec![],
        limit: Some(100),
        offset: 0,
    };

    // pk_lookup returns None (cold cache).
    let binding = spec.compute_editable_binding(|_| None);
    assert!(binding.is_none(), "cold cache must produce no binding");
}

/// T5/T6: rebuild_table with a binding that has Joined columns and insertable=false
/// results in: joined columns in readonly_columns, is_insertable=false.
#[gpui::test]
fn rebuild_table_marks_joined_cols_readonly_and_disables_insert(cx: &mut TestAppContext) {
    use dbflux_core::{ColumnOrigin, EditableBinding, TableRef as CoreTableRef};
    use std::collections::BTreeMap;

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

            let mut panel = DataGridPanel::new_internal(
                source,
                app_state.clone(),
                vec!["id".to_string()],
                window,
                cx,
            );

            // Simulate a builder result: columns = [id (Source, PK), amount (Joined)]
            let columns = vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_name: "int4".to_string(),
                    kind: ColumnKind::Unknown,
                    nullable: false,
                    is_primary_key: false,
                },
                ColumnMeta {
                    name: "amount".to_string(),
                    type_name: "numeric".to_string(),
                    kind: ColumnKind::Unknown,
                    nullable: true,
                    is_primary_key: false,
                },
            ];
            panel.result = QueryResult::table(columns, Vec::new(), None, Duration::ZERO);
            panel.pk_columns = vec!["id".to_string()];

            // Install the binding: id is Source/PK, amount is Joined, not insertable.
            let mut origin_map = BTreeMap::new();
            origin_map.insert("id".to_string(), ColumnOrigin::Source);
            origin_map.insert("amount".to_string(), ColumnOrigin::Joined);
            panel.builder.builder_editable_binding = Some(EditableBinding {
                table: CoreTableRef {
                    schema: Some("public".to_string()),
                    name: "users".to_string(),
                },
                pk_columns: vec!["id".to_string()],
                column_origin: origin_map,
                insertable: false,
            });
            // Simulate a committed visual spec so insert gate applies.
            panel.builder.current_visual_spec = Some(make_test_spec());

            panel
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    // Trigger rebuild_table.
    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.rebuild_table(None, cx);
        });
    });

    let (amount_col_ix, is_insertable, amount_is_readonly) = window.update(|_, app| {
        let panel = panel.read(app);
        let ts = panel
            .grid_table
            .table_state
            .as_ref()
            .expect("table state must exist");
        let ts = ts.read(app);

        let amount_col_ix = panel
            .result
            .columns
            .iter()
            .position(|c| c.name == "amount")
            .expect("amount column must exist");

        let is_insertable = ts.is_insertable();
        let amount_is_readonly = ts.readonly_columns().contains(&amount_col_ix);

        (amount_col_ix, is_insertable, amount_is_readonly)
    });

    assert!(
        amount_is_readonly,
        "amount (Joined origin) must be in readonly_columns (col {})",
        amount_col_ix
    );
    assert!(
        !is_insertable,
        "is_insertable must be false when binding.insertable=false"
    );
}

/// T3.1-A regression: grouped result stays read-only after rebuild_table.
#[gpui::test]
fn rebuild_table_grouped_spec_stays_read_only(cx: &mut TestAppContext) {
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

            // Simulate a grouped result with no binding.
            let columns = vec![ColumnMeta {
                name: "country".to_string(),
                type_name: "text".to_string(),
                kind: ColumnKind::Unknown,
                nullable: false,
                is_primary_key: false,
            }];
            panel.result = QueryResult::table(columns, Vec::new(), None, Duration::ZERO);
            panel.builder.current_visual_spec = Some(make_grouped_spec());
            panel.builder.builder_editable_binding = None;

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
            panel.rebuild_table(None, cx);
        });
    });

    let is_editable = window.update(|_, app| {
        let panel = panel.read(app);
        let ts = panel
            .grid_table
            .table_state
            .as_ref()
            .expect("table state must exist after rebuild");
        ts.read(app).is_editable()
    });

    assert!(
        !is_editable,
        "grouped builder result must remain read-only after rebuild"
    );
}

/// W2 — S6-A: cold-cache → editable upgrade via table-details arrival.
///
/// Simulates the arrival handler: table_details are inserted into the
/// connection cache (the same write the async fetch does before calling
/// `entity.update`), then `compute_builder_binding` is called. Verifies
/// that the binding upgrades from None (cold) to Some (warm) and that
/// `pending_rebuild` would then be set — confirming the cold→warm path.
#[gpui::test]
fn cold_cache_upgrades_to_editable_on_table_details_arrival(cx: &mut TestAppContext) {
    use dbflux_core::{ColumnInfo, TableInfo};

    init_test_runtime(cx);

    let profile_id = uuid::Uuid::new_v4();
    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let storage_runtime = StorageRuntime::in_memory().expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage_runtime).expect("test storage setup")
        })
    });

    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            use dbflux_core::{ConnectedProfile, DbConfig, MutationPolicy};
            use std::path::PathBuf;

            let profile = dbflux_core::ConnectionProfile::new(
                "test",
                DbConfig::SQLite {
                    path: PathBuf::from(":memory:"),
                    connection_id: None,
                },
            );
            let connected = ConnectedProfile {
                profile,
                connection: Arc::new(StubConnection) as Arc<dyn dbflux_core::Connection>,
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
                active_database: Some("app".to_string()),
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
                database: Some("app".to_string()),
                table: TableRef::with_schema("public", "users"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: None,
            };
            let mut panel =
                DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx);
            panel.builder.current_visual_spec = Some(make_test_spec());
            panel.builder.builder_editable_binding = None;
            panel
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    // Verify binding is None before details arrive (cold cache).
    let cold_binding = window.update(|_, app| {
        let p = panel.read(app);
        p.compute_builder_binding(
            p.builder.current_visual_spec.as_ref(),
            profile_id,
            Some("app"),
            app,
        )
    });
    assert!(cold_binding.is_none(), "cold cache must yield None binding");

    // Simulate table-details arrival: insert PK info into the connection cache.
    window.update(|_, app| {
        app_state.update(app, |app, _| {
            app.set_table_details(
                profile_id,
                "app".to_string(),
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

    // Now compute_builder_binding must return Some — the same call the arrival
    // handler makes before setting pending_rebuild = true.
    let warm_binding = window.update(|_, app| {
        let p = panel.read(app);
        p.compute_builder_binding(
            p.builder.current_visual_spec.as_ref(),
            profile_id,
            Some("app"),
            app,
        )
    });

    let binding = warm_binding
        .expect("warm cache must upgrade cold binding to Some after table-details arrive");
    assert_eq!(
        binding.pk_columns,
        vec!["id"],
        "binding must carry the PK column from the warm cache"
    );
    assert!(
        binding.insertable,
        "single-table All spec must be insertable"
    );
}

/// #634 — a table opened from the tree carries its own database while the
/// connection may never have recorded an active one. The cached details are
/// keyed by the table's database, so both lookups must lead with it: the one
/// `new_for_table` makes, and the fetch it falls back to, which must use
/// details that are already cached instead of failing on the populated key.
/// Leading with `active_database` left the second open of a table read-only
/// even though its primary key was cached.
#[gpui::test]
fn reopened_table_finds_cached_primary_key_without_an_active_database(cx: &mut TestAppContext) {
    use dbflux_core::{ColumnInfo, TableInfo};

    init_test_runtime(cx);

    let profile_id = uuid::Uuid::new_v4();
    let app_state = isolated_test_app_state(cx);
    let users = TableRef::with_schema("public", "users");

    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            use dbflux_core::{ConnectedProfile, DbConfig, MutationPolicy};
            use std::path::PathBuf;

            let profile = dbflux_core::ConnectionProfile::new(
                "test",
                DbConfig::SQLite {
                    path: PathBuf::from(":memory:"),
                    connection_id: None,
                },
            );
            let connected = ConnectedProfile {
                profile,
                connection: Arc::new(StubConnection) as Arc<dyn dbflux_core::Connection>,
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
                // Never set: the sidebar expands the current database without a
                // click, so opening one of its tables does not record it here.
                active_database: None,
                redis_key_cache: Default::default(),
                database_connections: Default::default(),
                proxy_tunnel: None,
            };
            app.connections_mut().insert(profile_id, connected);

            // Details written by a previous open, under the table's database.
            app.set_table_details(
                profile_id,
                "testdb".to_string(),
                Some("public".to_string()),
                "users".to_string(),
                TableInfo {
                    name: "users".to_string(),
                    schema: Some("public".to_string()),
                    columns: Some(vec![ColumnInfo {
                        name: "id".to_string(),
                        type_name: "int4".to_string(),
                        nullable: false,
                        is_primary_key: true,
                        default_value: None,
                        enum_values: None,
                    }]),
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

    let found = Rc::new(RefCell::new(Vec::new()));
    let found_handle = found.clone();
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();
    let fetch_panel_holder = Rc::new(RefCell::new(None));
    let fetch_panel_handle = fetch_panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        // `new_for_table` builds this panel from the cached details; it cannot
        // be called here because its refresh needs a driver dialect, so the
        // lookup it makes is invoked directly.
        let panel = cx.new(|cx| {
            let order_by = DataGridPanel::get_primary_key_columns(
                &app_state,
                profile_id,
                Some("testdb"),
                &users,
                cx,
            );
            let pk_columns: Vec<String> = order_by
                .iter()
                .map(|order| order.column.name.clone())
                .collect();
            found_handle.borrow_mut().extend(pk_columns.clone());

            let source = DataSource::Table {
                profile_id,
                database: Some("testdb".to_string()),
                table: users.clone(),
                pagination: Pagination::default(),
                order_by,
                total_rows: None,
            };
            let mut panel =
                DataGridPanel::new_internal(source, app_state.clone(), pk_columns, window, cx);
            panel.result = id_result();
            panel
        });

        // The path taken when that lookup comes back empty: with the details
        // cached, the fetch must use them rather than fail on
        // `prepare_fetch_table_details`, which refuses a populated key.
        let fetch_panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id,
                database: Some("testdb".to_string()),
                table: users.clone(),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: None,
            };
            let mut panel =
                DataGridPanel::new_internal(source, app_state.clone(), Vec::new(), window, cx);
            panel.fetch_table_details_for_pk(profile_id, &users, cx);
            panel
        });

        panel_handle.replace(Some(panel.clone()));
        fetch_panel_handle.replace(Some(fetch_panel.clone()));
        Root::new(panel, window, cx)
    });

    assert_eq!(
        *found.borrow(),
        vec!["id".to_string()],
        "cached details must be found under the table's database, not the active one"
    );

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");
    let edits_allowed = window.update(|_, app| panel.read(app).mutations_enabled());
    assert!(
        edits_allowed,
        "a table whose cached primary key was found must stay editable"
    );

    // #634 is about the grid's own gate, which `rebuild_table` fills from the
    // same PK indices, and about the `ORDER BY` the first page needs.
    let ordered_columns = window.update(|_, app| match &panel.read(app).source {
        DataSource::Table { order_by, .. } => order_by
            .iter()
            .map(|o| o.column.name.clone())
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    });
    assert_eq!(
        ordered_columns,
        vec!["id".to_string()],
        "the cached primary key must order the table's pages"
    );

    let grid_editable = window.update(|_, app| {
        panel.update(app, |panel, cx| panel.rebuild_table(None, cx));
        panel
            .read(app)
            .grid_table
            .table_state
            .as_ref()
            .expect("table_state must exist after rebuild_table")
            .read(app)
            .is_editable()
    });
    assert!(
        grid_editable,
        "the grid itself must be editable, not just the panel's mutations gate"
    );

    let fetch_panel = fetch_panel_holder
        .borrow()
        .clone()
        .expect("fetch panel should be created");
    let (fetched_pk_columns, fetch_pending) = window.update(|_, app| {
        let panel = fetch_panel.read(app);
        (panel.pk_columns.clone(), panel.pk_details_pending)
    });
    assert_eq!(
        fetched_pk_columns,
        vec!["id".to_string()],
        "details already cached must be used instead of failing the fetch"
    );
    assert!(
        !fetch_pending,
        "a cache hit must not leave the panel waiting for a fetch"
    );

    // The key arrived after the first page, so the table is read again in
    // key order. Until that page lands the rows on screen were built
    // without the key, and the banner must not flash over them.
    let resolving = window.update(|_, app| fetch_panel.read(app).primary_key_still_resolving());
    assert!(
        resolving,
        "the no-primary-key banner must wait for the page read in key order"
    );

    let resolving_after_page = window.update(|_, app| {
        fetch_panel.update(app, |panel, cx| {
            panel.apply_table_result(
                profile_id,
                users.clone(),
                Pagination::default(),
                Vec::new(),
                None,
                id_result(),
                cx,
            );
        });
        fetch_panel.read(app).primary_key_still_resolving()
    });
    assert!(
        !resolving_after_page,
        "the page read in key order ends the wait"
    );
}

/// #634 — the cold path. The first page is issued before the primary keys are
/// known, so it carries no `ORDER BY` and `LIMIT/OFFSET` paging can repeat or
/// skip rows. When the details arrive, the source must be rewritten with the
/// order just learned, the page must be re-issued with it, and the grid must
/// come out editable.
#[gpui::test]
fn first_open_requeries_with_the_primary_key_order_it_learned(cx: &mut TestAppContext) {
    use dbflux_core::{ColumnInfo, TableInfo};

    init_test_runtime(cx);

    let profile_id = uuid::Uuid::new_v4();
    let app_state = isolated_test_app_state(cx);
    let users = TableRef::with_schema("public", "users");

    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            use dbflux_core::{ConnectedProfile, DbConfig, MutationPolicy};
            use std::path::PathBuf;

            let profile = dbflux_core::ConnectionProfile::new(
                "test",
                DbConfig::SQLite {
                    path: PathBuf::from(":memory:"),
                    connection_id: None,
                },
            );
            let connected = ConnectedProfile {
                profile,
                connection: Arc::new(StubConnection) as Arc<dyn dbflux_core::Connection>,
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

    // The panel is deliberately kept out of the window's tree: rendering it
    // would drain `pending.requery` into a real query, which the stub
    // connection cannot answer.
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
            // The state `new_for_table` leaves behind on a cold cache: no
            // order yet, because the key columns are still unknown.
            let source = DataSource::Table {
                profile_id,
                database: Some("testdb".to_string()),
                table: users.clone(),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: None,
            };
            let mut panel =
                DataGridPanel::new_internal(source, app_state.clone(), Vec::new(), window, cx);
            panel.result = id_result();
            panel
        });
        panel_handle.replace(Some(panel));
        Harness
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    // The fetch stores the details and then hands the key columns over.
    window.update(|_, app| {
        app_state.update(app, |app, _| {
            app.set_table_details(
                profile_id,
                "testdb".to_string(),
                Some("public".to_string()),
                "users".to_string(),
                TableInfo {
                    name: "users".to_string(),
                    schema: Some("public".to_string()),
                    columns: Some(vec![ColumnInfo {
                        name: "id".to_string(),
                        type_name: "int4".to_string(),
                        nullable: false,
                        is_primary_key: true,
                        default_value: None,
                        enum_values: None,
                    }]),
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

    // Read both straight after the call: a render would already have consumed
    // the queued requery.
    let (ordered_columns, requery_order) = window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.apply_pk_details(vec!["id".to_string()], cx);

            let ordered = match &panel.source {
                DataSource::Table { order_by, .. } => order_by
                    .iter()
                    .map(|o| o.column.name.clone())
                    .collect::<Vec<_>>(),
                _ => Vec::new(),
            };
            let requery = panel.pending.requery.as_ref().map(|pending| {
                pending
                    .order_by
                    .iter()
                    .map(|o| o.column.name.clone())
                    .collect::<Vec<_>>()
            });
            (ordered, requery)
        })
    });

    assert_eq!(
        ordered_columns,
        vec!["id".to_string()],
        "the source must carry the order the panel just learned"
    );
    assert_eq!(
        requery_order,
        Some(vec!["id".to_string()]),
        "the unordered first page must be re-issued with that order"
    );

    let grid_editable = window.update(|_, app| {
        panel.update(app, |panel, cx| panel.rebuild_table(None, cx));
        panel
            .read(app)
            .grid_table
            .table_state
            .as_ref()
            .expect("table_state must exist after rebuild_table")
            .read(app)
            .is_editable()
    });
    assert!(
        grid_editable,
        "a first open whose primary key arrived late must still end up editable"
    );
}

/// W3 — S7-A: after a builder-result mutation the panel has pending_refresh=true
/// and visual_select set, so the next render cycle will call refresh() → run_visual_query
/// → pending_rebuild. Verifies the builder-specific state that gates re-query + re-bind.
#[gpui::test]
fn builder_result_mutation_sets_pending_refresh_with_visual_select(cx: &mut TestAppContext) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let visual_select = SelectQuery {
        sql: "SELECT id, name FROM public.users LIMIT 100".to_string(),
        params: vec![],
    };

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

            let mut panel =
                DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx);

            // Simulate a committed builder result: spec and pre-built SELECT are in place.
            panel.builder.current_visual_spec = Some(make_test_spec());
            panel.builder.visual_select = Some(visual_select.clone());
            panel.builder.builder_editable_binding = Some(dbflux_core::EditableBinding {
                table: dbflux_core::TableRef {
                    schema: Some("public".to_string()),
                    name: "users".to_string(),
                },
                pk_columns: vec!["id".to_string()],
                column_origin: Default::default(),
                insertable: true,
            });
            panel.pending.refresh = false;
            panel
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    // In one window.update: verify baseline, simulate mutation callback,
    // then assert the resulting state.
    window.update(|_, app| {
        panel.update(app, |p, cx| {
            // Baseline: pending_refresh starts false, visual_select is set.
            assert!(!p.pending.refresh, "pending_refresh must start false");
            assert!(
                p.builder.visual_select.is_some(),
                "visual_select must be set for the builder-result path"
            );

            // Simulate what the mutation success closure does.
            p.pending.refresh = true;
            cx.notify();

            // After mutation: pending_refresh=true AND visual_select is still set.
            // This is the exact state that causes refresh() → run_visual_query
            // (not run_table_query) on the next render tick, completing the S7-A loop.
            assert!(
                p.pending.refresh,
                "pending_refresh must be true after mutation to trigger re-query"
            );
            assert_eq!(
                p.builder.visual_select.as_ref().map(|s| s.sql.as_str()),
                Some("SELECT id, name FROM public.users LIMIT 100"),
                "visual_select must be preserved so refresh() routes to run_visual_query"
            );
        });
    });
}
