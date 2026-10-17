use std::sync::{Arc, Mutex};
use std::time::Duration;

use dbflux_app::keymap::Command;
use dbflux_components::components::data_table::ContextMenuAction;
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_components::theme;
use dbflux_core::{
    CollectionPresentation, ColumnInfo, ColumnMeta, ConnectedProfile, Connection,
    ConnectionProfile, DbConfig, DbError, GeneratedQuery, GeneratorError, MutationCategory,
    MutationRequest, NearestNeighborRequest, ObjectFacet, QueryGenerator, QueryLanguage,
    QueryRequest, QueryResult, TableBrowseRequest, TableInfo, TableRef, Value, VectorDistance,
    VectorFacet,
};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{AppContext, Entity, TestAppContext, VisualTestContext};
use uuid::Uuid;

use super::{DEFAULT_NEIGHBOR_LIMIT, next_distance};
use crate::data_grid_panel::DataGridPanel;
use crate::data_grid_panel::context_menu::toolbar::ToolbarAction;

#[derive(Default)]
struct Calls {
    browses: usize,
    executed: Vec<String>,
    neighbor_requests: Vec<NearestNeighborRequest>,
    fail_execute: bool,
}

/// Renders the request as text, so the test sees exactly what the panel
/// asked for.
struct StubGenerator {
    calls: Arc<Mutex<Calls>>,
}

impl QueryGenerator for StubGenerator {
    fn supported_categories(&self) -> &'static [MutationCategory] {
        &[]
    }

    fn generate_mutation(&self, _: &MutationRequest) -> Option<GeneratedQuery> {
        None
    }

    fn generate_nearest_neighbors(
        &self,
        request: &NearestNeighborRequest,
    ) -> Result<GeneratedQuery, GeneratorError> {
        self.calls
            .lock()
            .map_err(|_| GeneratorError::Unsupported("poisoned".into()))?
            .neighbor_requests
            .push(request.clone());

        Ok(GeneratedQuery {
            language: QueryLanguage::Sql,
            text: format!(
                "NEAREST {} {:?} LIMIT {}",
                request.column, request.distance, request.limit
            ),
        })
    }
}

struct StubVectorConnection {
    with_vector: bool,
    calls: Arc<Mutex<Calls>>,
    generator: StubGenerator,
}

fn column_meta(name: &str, is_primary_key: bool) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        type_name: "text".into(),
        nullable: true,
        is_primary_key,
        kind: dbflux_core::ColumnKind::Text,
    }
}

fn table_page() -> QueryResult {
    QueryResult::table(
        vec![
            column_meta("id", true),
            column_meta("label", false),
            column_meta("embedding", false),
        ],
        vec![
            vec![
                Value::Int(1),
                Value::Text("first".into()),
                Value::Text("[1,0,0]".into()),
            ],
            vec![
                Value::Int(2),
                Value::Text("second".into()),
                Value::Text("[0,1,0]".into()),
            ],
        ],
        None,
        Duration::ZERO,
    )
}

/// One neighbor whose label is the query that read it, so a test can tell
/// which search a result came from.
fn neighbors(sql: &str) -> QueryResult {
    QueryResult::table(
        vec![
            column_meta("id", false),
            column_meta("label", false),
            column_meta("embedding", false),
            column_meta("nearest_distance", false),
        ],
        vec![vec![
            Value::Int(1),
            Value::Text(sql.into()),
            Value::Text("[1,0,0]".into()),
            Value::Float(1.5),
        ]],
        None,
        Duration::ZERO,
    )
}

fn column(name: &str, type_name: &str, is_primary_key: bool) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        type_name: type_name.to_string(),
        nullable: !is_primary_key,
        is_primary_key,
        default_value: None,
        enum_values: None,
    }
}

impl Connection for StubVectorConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        use dbflux_core::{
            DatabaseCategory, DriverCapabilities, DriverMetadata, Icon as CoreIcon, TransferFamily,
        };

        static META: std::sync::OnceLock<DriverMetadata> = std::sync::OnceLock::new();
        META.get_or_init(|| DriverMetadata {
            id: "stub-vector".to_string(),
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

    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }

    fn ping(&self) -> Result<(), DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, request: &QueryRequest) -> Result<QueryResult, DbError> {
        let mut calls = self
            .calls
            .lock()
            .map_err(|_| DbError::query_failed("poisoned"))?;
        calls.executed.push(request.sql.clone());

        if calls.fail_execute {
            return Err(DbError::query_failed("operator does not exist"));
        }

        Ok(neighbors(&request.sql))
    }

    fn browse_table(&self, _: &TableBrowseRequest) -> Result<QueryResult, DbError> {
        self.calls
            .lock()
            .map_err(|_| DbError::query_failed("poisoned"))?
            .browses += 1;
        Ok(table_page())
    }

    fn count_table(&self, _: &dbflux_core::TableCountRequest) -> Result<u64, DbError> {
        Ok(2)
    }

    fn cancel(&self, _: &dbflux_core::QueryHandle) -> Result<(), DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }

    fn table_details(
        &self,
        _database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        Ok(TableInfo {
            name: table.to_string(),
            schema: schema.map(str::to_string),
            columns: Some(vec![
                column("id", "integer", true),
                column("label", "text", false),
                column("embedding", "vector(3)", false),
            ]),
            pseudo_columns: Box::default(),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: CollectionPresentation::default(),
            child_items: None,
            storage_hints: None,
        })
    }

    fn object_facets(&self, _table: &TableInfo) -> Vec<ObjectFacet> {
        let mut facets = vec![ObjectFacet::Tabular];
        if self.with_vector {
            facets.push(ObjectFacet::Vector(VectorFacet {
                column: "embedding".to_string(),
                dimension: Some(3),
            }));
        }
        facets
    }

    fn query_generator(&self) -> Option<&dyn QueryGenerator> {
        Some(&self.generator)
    }
}

/// An app state holding one connection to the stub, with its call log and
/// the toast host.
fn connect(
    with_vector: bool,
    cx: &mut TestAppContext,
) -> (
    Entity<AppStateEntity>,
    Uuid,
    Arc<Mutex<Calls>>,
    Entity<ToastHost>,
) {
    cx.update(gpui_component::init);
    cx.update(theme::init);
    let toasts = cx.update(|cx| {
        let host = cx.new(|_cx| ToastHost::new());
        cx.set_global(ToastGlobal { host: host.clone() });
        host
    });

    let calls = Arc::new(Mutex::new(Calls::default()));
    let profile_id = Uuid::new_v4();
    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let storage_runtime = StorageRuntime::in_memory().expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage_runtime).expect("test storage setup")
        })
    });

    cx.update(|cx| {
        app_state.update(cx, |app, _cx| {
            let connected = ConnectedProfile {
                profile: ConnectionProfile::new(
                    "vectors",
                    DbConfig::SQLite {
                        path: std::path::PathBuf::from(":memory:"),
                        connection_id: None,
                    },
                ),
                connection: Arc::new(StubVectorConnection {
                    with_vector,
                    calls: calls.clone(),
                    generator: StubGenerator {
                        calls: calls.clone(),
                    },
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

    (app_state, profile_id, calls, toasts)
}

fn open_table(
    with_vector: bool,
    cx: &mut TestAppContext,
) -> (
    Entity<DataGridPanel>,
    Arc<Mutex<Calls>>,
    Entity<ToastHost>,
    &mut VisualTestContext,
) {
    let (app_state, profile_id, calls, toasts) = connect(with_vector, cx);
    let window = cx.add_empty_window();
    let panel = window.update(|window, app| {
        app.new(|cx| {
            DataGridPanel::new_for_table(
                profile_id,
                TableRef::with_schema("public", "items"),
                None,
                app_state,
                window,
                cx,
            )
        })
    });
    window.run_until_parked();

    (panel, calls, toasts, window)
}

/// Like [`open_table`], but the panel is drawn in a window, so a test can
/// read what the frame shows.
fn open_rendered_table(cx: &mut TestAppContext) -> (Entity<DataGridPanel>, &mut VisualTestContext) {
    let (app_state, profile_id, _, _) = connect(true, cx);
    let panel_handle = std::rc::Rc::new(std::cell::RefCell::new(None));

    let (_, window) = cx.add_window_view({
        let panel_handle = panel_handle.clone();
        move |window, cx| {
            let panel = cx.new(|cx| {
                DataGridPanel::new_for_table(
                    profile_id,
                    TableRef::with_schema("public", "items"),
                    None,
                    app_state,
                    window,
                    cx,
                )
            });
            panel_handle.replace(Some(panel.clone()));
            gpui_component::Root::new(panel, window, cx)
        }
    });
    window.run_until_parked();
    window.update(|_, _| {});

    let panel = panel_handle.borrow().clone().expect("the panel is created");
    (panel, window)
}

/// Puts the cursor on the label of the second row.
fn select_second_row(panel: &Entity<DataGridPanel>, window: &mut VisualTestContext) {
    window.update(|_, app| {
        let table_state = panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("a table");
        table_state.update(app, |state, cx| state.select_cell(CellCoord::new(1, 1), cx));
    });
}

fn find_similar_rows(panel: &Entity<DataGridPanel>, window: &mut VisualTestContext) -> bool {
    let handled = window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.dispatch_command(Command::FindSimilarRows, window, cx)
        })
    });
    window.run_until_parked();
    handled
}

fn column_names(panel: &Entity<DataGridPanel>, window: &mut VisualTestContext) -> Vec<String> {
    window.update(|_, app| {
        panel
            .read(app)
            .result
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    })
}

fn menu_offers_find_similar(panel: &Entity<DataGridPanel>, window: &mut VisualTestContext) -> bool {
    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.open_context_menu_at_selection(window, cx);
            let menu = panel.context_menu.take().expect("the row menu opens");
            panel
                .with_find_similar_rows(Vec::new(), &menu, true, cx)
                .iter()
                .any(|item| item.action == Some(ContextMenuAction::FindSimilarRows))
        })
    })
}

#[test]
fn the_distance_cycles_through_every_function() {
    assert_eq!(next_distance(VectorDistance::L2), VectorDistance::Cosine);
    assert_eq!(
        next_distance(VectorDistance::Cosine),
        VectorDistance::InnerProduct
    );
    assert_eq!(
        next_distance(VectorDistance::InnerProduct),
        VectorDistance::L2
    );
}

#[gpui::test]
fn a_table_without_a_vector_facet_offers_no_similar_rows(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(false, cx);
    select_second_row(&panel, window);

    assert!(!menu_offers_find_similar(&panel, window));
    assert!(
        !find_similar_rows(&panel, window),
        "the command is not handled"
    );

    let calls = calls.lock().unwrap();
    assert!(calls.neighbor_requests.is_empty());
    assert!(calls.executed.is_empty());
}

#[gpui::test]
fn similar_rows_replace_the_page_with_the_neighbors_and_their_distance(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(true, cx);
    select_second_row(&panel, window);

    assert!(menu_offers_find_similar(&panel, window));
    assert!(find_similar_rows(&panel, window));

    {
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls.neighbor_requests,
            vec![NearestNeighborRequest {
                table: TableRef::with_schema("public", "items"),
                database: None,
                column: "embedding".to_string(),
                anchor: Value::Text("[0,1,0]".into()),
                distance: VectorDistance::L2,
                limit: DEFAULT_NEIGHBOR_LIMIT,
                exclude_primary_key: vec![("id".to_string(), Value::Int(2))],
            }]
        );
        assert_eq!(
            calls.executed,
            vec!["NEAREST embedding L2 LIMIT 10".to_string()]
        );
    }

    assert_eq!(
        column_names(&panel, window),
        vec!["L2 distance", "id", "label", "embedding"],
        "the distance comes first, under a header that names it"
    );

    window.update(|_, app| {
        let panel = panel.read(app);
        assert_eq!(
            panel.result.rows.len(),
            2,
            "the anchor row, then the neighbor"
        );
        assert_eq!(
            panel.result.rows[0],
            vec![
                Value::Text("anchor".into()),
                Value::Int(2),
                Value::Text("second".into()),
                Value::Text("[0,1,0]".into()),
            ]
        );
        assert_eq!(panel.result.rows[1][0], Value::Text("1.5000".into()));
        assert!(!panel.mutations_enabled(), "neighbor rows are read-only");
        let search = panel
            .nearest_neighbor_search()
            .expect("the notice is shown");
        assert_eq!(search.column, "embedding");
        assert_eq!(search.distance, VectorDistance::L2);
        assert_eq!(search.limit, 10);

        let actions = panel.toolbar_actions(app);
        assert!(actions.contains(&ToolbarAction::NearestDistance));
        assert!(actions.contains(&ToolbarAction::ClearNearest));
    });
}

#[gpui::test]
fn changing_the_distance_runs_the_search_again(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(true, cx);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));

    window
        .update(|_, app| panel.update(app, |panel, cx| panel.cycle_nearest_neighbor_distance(cx)));
    window.run_until_parked();

    let distances: Vec<VectorDistance> = calls
        .lock()
        .unwrap()
        .neighbor_requests
        .iter()
        .map(|request| request.distance)
        .collect();
    assert_eq!(distances, vec![VectorDistance::L2, VectorDistance::Cosine]);

    window.update(|_, app| {
        let search = panel.read(app).nearest_neighbor_search().cloned();
        assert_eq!(search.map(|s| s.distance), Some(VectorDistance::Cosine));
    });
}

#[gpui::test]
fn back_to_table_reads_the_page_again(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(true, cx);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));
    let browses_before = calls.lock().unwrap().browses;

    window.update(|window, app| {
        panel.update(app, |panel, cx| panel.clear_nearest_neighbors(window, cx))
    });
    window.run_until_parked();

    assert_eq!(calls.lock().unwrap().browses, browses_before + 1);
    assert_eq!(
        column_names(&panel, window),
        vec!["id", "label", "embedding"]
    );
    window.update(|_, app| {
        let panel = panel.read(app);
        assert!(panel.nearest_neighbor_search().is_none());
        assert_eq!(panel.result.rows.len(), 2);
        assert!(
            !panel
                .toolbar_actions(app)
                .contains(&ToolbarAction::ClearNearest)
        );
    });
}

#[gpui::test]
fn a_failed_search_reports_the_error_and_keeps_the_page(cx: &mut TestAppContext) {
    let (panel, calls, toasts, window) = open_table(true, cx);
    calls.lock().unwrap().fail_execute = true;
    select_second_row(&panel, window);

    let toasts_before = window.update(|_, app| toasts.read(app).latest_toast_id());
    assert!(find_similar_rows(&panel, window));

    assert_eq!(calls.lock().unwrap().executed.len(), 1);
    let toasts_after = window.update(|_, app| toasts.read(app).latest_toast_id());
    assert!(
        toasts_after.is_some() && toasts_after != toasts_before,
        "an error toast"
    );

    assert_eq!(
        column_names(&panel, window),
        vec!["id", "label", "embedding"]
    );
    window.update(|_, app| {
        let panel = panel.read(app);
        assert_eq!(panel.result.rows.len(), 2);
        assert!(panel.nearest_neighbor_search().is_none());
    });
}

/// The label of the first neighbor, under the pinned anchor row.
fn first_neighbor_label(
    panel: &Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> Option<Value> {
    window.update(|_, app| {
        panel
            .read(app)
            .result
            .rows
            .get(1)
            .and_then(|row| row.get(2).cloned())
    })
}

#[gpui::test(iterations = 32)]
fn a_refresh_drops_a_search_still_running(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(true, cx);
    select_second_row(&panel, window);

    // The search and the refresh start before either answer lands.
    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            assert!(panel.dispatch_command(Command::FindSimilarRows, window, cx));
            panel.refresh(window, cx);
        })
    });
    window.run_until_parked();

    assert_eq!(calls.lock().unwrap().executed.len(), 1, "the search ran");
    assert_eq!(
        column_names(&panel, window),
        vec!["id", "label", "embedding"]
    );
    window.update(|_, app| {
        let panel = panel.read(app);
        assert!(panel.nearest_neighbor_search().is_none());
        assert_eq!(panel.result.rows.len(), 2);
    });

    // A later read of the table page must not bring the neighbor bar of the
    // dropped search back while it loads.
    window.update(|window, app| panel.update(app, |panel, cx| panel.refresh(window, cx)));
    window.update(|_, app| {
        assert!(
            panel.read(app).neighbor_bar_search().is_none(),
            "a dropped search shows no neighbor bar"
        );
    });
    window.run_until_parked();
}

#[gpui::test(iterations = 32)]
fn a_new_distance_drops_the_search_it_replaces(cx: &mut TestAppContext) {
    let (panel, _calls, _, window) = open_table(true, cx);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.cycle_nearest_neighbor_distance(cx);
            let search = panel.nearest_neighbors.clone().expect("a search is shown");
            panel.run_nearest_neighbors(
                super::NearestNeighborSearch {
                    distance: VectorDistance::InnerProduct,
                    ..search
                },
                cx,
            );
        })
    });
    window.run_until_parked();

    assert_eq!(
        first_neighbor_label(&panel, window),
        Some(Value::Text(
            "NEAREST embedding InnerProduct LIMIT 10".into()
        ))
    );
    window.update(|_, app| {
        let search = panel.read(app).nearest_neighbor_search().cloned();
        assert_eq!(
            search.map(|s| s.distance),
            Some(VectorDistance::InnerProduct)
        );
    });
}

#[gpui::test]
fn pending_edits_block_finding_similar_rows(cx: &mut TestAppContext) {
    let (panel, calls, toasts, window) = open_table(true, cx);
    select_second_row(&panel, window);

    window.update(|_, app| {
        let table_state = panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("a table");
        table_state.update(app, |state, _cx| {
            state.stage_base_cell_value(
                0,
                1,
                dbflux_components::components::data_table::model::CellValue::text("edited"),
            );
        });
    });
    let toasts_before = window.update(|_, app| toasts.read(app).latest_toast_id());

    assert!(find_similar_rows(&panel, window), "the key is consumed");

    {
        let calls = calls.lock().unwrap();
        assert!(calls.neighbor_requests.is_empty());
        assert!(calls.executed.is_empty());
    }
    let toasts_after = window.update(|_, app| toasts.read(app).latest_toast_id());
    assert!(
        toasts_after.is_some() && toasts_after != toasts_before,
        "the guard warns"
    );
    window.update(|_, app| {
        let panel = panel.read(app);
        assert!(panel.has_pending_edits(app), "the edit is still staged");
        assert!(panel.nearest_neighbor_search().is_none());
        assert_eq!(panel.result.rows.len(), 2);
    });
}

fn set_filter(panel: &Entity<DataGridPanel>, text: &str, window: &mut VisualTestContext) {
    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.filter_bar.filter_input.update(cx, |input, cx| {
                input.set_value(text, window, cx);
            });
        });
    });
}

fn filter_text(panel: &Entity<DataGridPanel>, window: &mut VisualTestContext) -> String {
    window.update(|_, app| {
        panel
            .read(app)
            .filter_bar
            .filter_input
            .read(app)
            .value()
            .to_string()
    })
}

fn redraw(window: &mut VisualTestContext) {
    window.run_until_parked();
    window.update(|_, _| {});
}

/// The disabled state of each edit button in the header, `None` for a
/// button the frame does not draw.
fn edit_buttons_disabled(window: &mut VisualTestContext) -> Vec<Option<bool>> {
    use dbflux_components::accessibility::{FrameCapture, accessibility_node};

    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);
    ["undo-btn", "redo-btn", "save-btn", "revert-btn"]
        .iter()
        .map(|id| accessibility_node(&frame, id).map(|node| node.is_disabled()))
        .collect()
}

/// The neighbor bar takes the filter row's place at the same height; the
/// WHERE / LIMIT field and the builder and column-picker actions give way.
#[gpui::test]
fn similar_rows_replace_the_filter_row_with_the_neighbor_bar(cx: &mut TestAppContext) {
    let (panel, window) = open_rendered_table(cx);
    let filter_row = window
        .debug_bounds("data-grid-filter-controls")
        .expect("the table shows the filter field");
    assert!(window.debug_bounds("nearest-neighbors-bar").is_none());

    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));
    redraw(window);

    assert!(
        window.debug_bounds("data-grid-filter-controls").is_none(),
        "WHERE and LIMIT do not apply to the neighbors"
    );
    let bar = window
        .debug_bounds("nearest-neighbors-bar")
        .expect("the neighbor bar is drawn");
    assert_eq!(bar.size.height, filter_row.size.height);
    assert_eq!(bar.origin.y, filter_row.origin.y);

    window.update(|_, app| {
        let actions = panel.read(app).toolbar_actions(app);
        assert!(!actions.contains(&ToolbarAction::OpenBuilder));
        assert!(!actions.contains(&ToolbarAction::ColumnPicker));
        assert!(!actions.contains(&ToolbarAction::ClearFilter));
    });
}

#[gpui::test]
fn the_neighbor_bar_names_the_anchor_row_and_the_column(cx: &mut TestAppContext) {
    let (panel, window) = open_rendered_table(cx);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));

    window.update(|_, app| {
        assert_eq!(
            panel.read(app).nearest_neighbors_summary().as_deref(),
            Some("Similar to id = 2 by embedding")
        );
    });
}

/// The rows are read-only because they are neighbors, not because the table
/// has no key: no missing-key banner, and the key column keeps its icon.
#[gpui::test]
fn similar_rows_show_no_missing_primary_key_banner(cx: &mut TestAppContext) {
    let (panel, window) = open_rendered_table(cx);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));
    redraw(window);

    assert!(window.debug_bounds("missing-primary-key-banner").is_none());
    window.update(|_, app| {
        let table_state = panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("a table");
        let state = table_state.read(app);
        assert_eq!(
            state.pk_columns(),
            &[1],
            "the id column, after the distance, keeps its key icon"
        );
        assert!(state.has_reference_row(), "the anchor row is marked");
        assert!(!state.is_editable(), "neighbor rows stay read-only");
    });
}

#[gpui::test]
fn similar_rows_keep_the_edit_controls_disabled(cx: &mut TestAppContext) {
    let (panel, window) = open_rendered_table(cx);
    assert_eq!(edit_buttons_disabled(window), vec![Some(true); 4]);

    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));
    redraw(window);

    assert_eq!(
        edit_buttons_disabled(window),
        vec![Some(true); 4],
        "the header keeps its shape with every edit action disabled"
    );
}

#[gpui::test]
fn the_header_counts_the_neighbors_shown(cx: &mut TestAppContext) {
    let (panel, window) = open_rendered_table(cx);
    let table_meta = window.update(|_, app| panel.read(app).source_breadcrumb_meta());

    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));

    window.update(|_, app| {
        assert_eq!(
            panel.read(app).source_breadcrumb_meta(),
            crate::labels::breadcrumb_meta_label(4, Some(1))
        );
    });

    window.update(|window, app| {
        panel.update(app, |panel, cx| panel.clear_nearest_neighbors(window, cx))
    });
    window.run_until_parked();

    window.update(|_, app| {
        assert_eq!(panel.read(app).source_breadcrumb_meta(), table_meta);
    });
}

#[gpui::test]
fn back_to_table_restores_the_filter_row(cx: &mut TestAppContext) {
    let (panel, window) = open_rendered_table(cx);
    set_filter(&panel, "label <> ''", window);
    redraw(window);
    let actions_before = window.update(|_, app| panel.read(app).toolbar_actions(app));

    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));
    redraw(window);
    assert!(window.debug_bounds("data-grid-filter-controls").is_none());

    window.update(|window, app| {
        panel.update(app, |panel, cx| panel.clear_nearest_neighbors(window, cx))
    });
    redraw(window);

    assert!(window.debug_bounds("data-grid-filter-controls").is_some());
    assert!(window.debug_bounds("nearest-neighbors-bar").is_none());
    assert_eq!(filter_text(&panel, window), "label <> ''");
    window.update(|_, app| {
        assert_eq!(panel.read(app).toolbar_actions(app), actions_before);
    });
}

fn menu_item(action: ContextMenuAction) -> crate::data_grid_panel::ContextMenuItem {
    crate::data_grid_panel::ContextMenuItem {
        label: format!("{action:?}").into(),
        action: Some(action),
        icon: None,
        is_separator: false,
        is_danger: false,
        detail: None,
        disabled: false,
    }
}

/// The row menu as the grid builds it, with "Find similar rows" added.
fn row_menu(
    panel: &Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> Vec<crate::data_grid_panel::ContextMenuItem> {
    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.open_context_menu_at_selection(window, cx);
            let menu = panel.context_menu.take().expect("the row menu opens");
            let base = vec![
                menu_item(ContextMenuAction::Copy),
                menu_item(ContextMenuAction::ViewValue),
                menu_item(ContextMenuAction::Paste),
                menu_item(ContextMenuAction::AddRow),
                menu_item(ContextMenuAction::InspectRow),
                menu_item(ContextMenuAction::DuplicateRow),
                menu_item(ContextMenuAction::DeleteRow),
            ];
            panel.with_find_similar_rows(base, &menu, true, cx)
        })
    })
}

fn find_similar_entry(
    items: &[crate::data_grid_panel::ContextMenuItem],
) -> (usize, Option<String>, bool) {
    let (index, item) = items
        .iter()
        .enumerate()
        .find(|(_, item)| item.action == Some(ContextMenuAction::FindSimilarRows))
        .expect("the menu offers Find similar rows");
    (
        index,
        item.detail.as_ref().map(|detail| detail.to_string()),
        item.disabled,
    )
}

#[gpui::test]
fn find_similar_rows_sits_with_the_row_actions_and_names_what_it_reads(cx: &mut TestAppContext) {
    let (panel, _, _, window) = open_table(true, cx);
    select_second_row(&panel, window);

    let items = row_menu(&panel, window);
    let (index, detail, disabled) = find_similar_entry(&items);

    assert_eq!(
        items.get(index - 1).and_then(|item| item.action),
        Some(ContextMenuAction::InspectRow),
        "right after Inspect row"
    );
    assert_eq!(detail.as_deref(), Some("by embedding · nearest 10"));
    assert!(!disabled);
}

#[gpui::test]
fn find_similar_rows_is_disabled_with_the_reason_while_edits_are_pending(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(true, cx);
    select_second_row(&panel, window);
    window.update(|_, app| {
        let table_state = panel
            .read(app)
            .grid_table
            .table_state
            .clone()
            .expect("a table");
        table_state.update(app, |state, _cx| {
            state.stage_base_cell_value(
                0,
                1,
                dbflux_components::components::data_table::model::CellValue::text("edited"),
            );
        });
    });

    let (_, detail, disabled) = find_similar_entry(&row_menu(&panel, window));

    assert!(disabled);
    assert_eq!(detail.as_deref(), Some("Save or revert 1 change first"));
    assert!(calls.lock().unwrap().neighbor_requests.is_empty());
}

#[gpui::test]
fn find_similar_rows_is_disabled_on_a_row_without_a_vector(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(true, cx);
    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            let mut page = table_page();
            page.rows[1][2] = Value::Null;
            panel.set_result(page, cx);
        })
    });
    select_second_row(&panel, window);

    let (_, detail, disabled) = find_similar_entry(&row_menu(&panel, window));

    assert!(disabled);
    assert_eq!(detail.as_deref(), Some("No value in embedding"));
    assert!(!find_similar_rows(&panel, window) || calls.lock().unwrap().executed.is_empty());
}

#[gpui::test]
fn a_new_k_reads_that_many_neighbors(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(true, cx);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel
                .nearest
                .k_input
                .update(cx, |input, cx| input.set_value("3", window, cx));
            panel.apply_neighbor_limit(window, cx);
        })
    });
    window.run_until_parked();

    let calls = calls.lock().unwrap();
    assert_eq!(
        calls.executed.last().map(String::as_str),
        Some("NEAREST embedding L2 LIMIT 3")
    );
    drop(calls);
    window.update(|_, app| {
        assert_eq!(
            panel.read(app).nearest_neighbor_search().map(|s| s.limit),
            Some(3)
        );
    });
}

#[gpui::test]
fn a_k_out_of_range_is_refused(cx: &mut TestAppContext) {
    let (panel, calls, toasts, window) = open_table(true, cx);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));
    let toasts_before = window.update(|_, app| toasts.read(app).latest_toast_id());

    for text in ["0", "1001", "ten"] {
        window.update(|window, app| {
            panel.update(app, |panel, cx| {
                panel
                    .nearest
                    .k_input
                    .update(cx, |input, cx| input.set_value(text, window, cx));
                panel.apply_neighbor_limit(window, cx);
            })
        });
        window.run_until_parked();

        let value = window.update(|_, app| {
            panel
                .read(app)
                .nearest
                .k_input
                .read(app)
                .value()
                .to_string()
        });
        assert_eq!(value, "10", "{text} goes back to the K shown");
    }

    assert_eq!(calls.lock().unwrap().executed.len(), 1, "no search ran");
    let toasts_after = window.update(|_, app| toasts.read(app).latest_toast_id());
    assert!(toasts_after.is_some() && toasts_after != toasts_before);
}

#[gpui::test]
fn refresh_reads_the_same_neighbors_again(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(true, cx);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));
    let browses = calls.lock().unwrap().browses;

    window.update(|window, app| panel.update(app, |panel, cx| panel.request_refresh(window, cx)));
    window.run_until_parked();

    let calls = calls.lock().unwrap();
    assert_eq!(calls.browses, browses, "the table page is not read");
    assert_eq!(
        calls.executed,
        vec![
            "NEAREST embedding L2 LIMIT 10".to_string(),
            "NEAREST embedding L2 LIMIT 10".to_string()
        ]
    );
}

#[gpui::test]
fn escape_returns_to_the_table(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(true, cx);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));
    let browses = calls.lock().unwrap().browses;

    let handled = window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.dispatch_command(Command::Cancel, window, cx)
        })
    });
    window.run_until_parked();

    assert!(handled);
    assert_eq!(calls.lock().unwrap().browses, browses + 1);
    assert_eq!(
        column_names(&panel, window),
        vec!["id", "label", "embedding"]
    );
    window.update(|_, app| {
        assert!(panel.read(app).nearest_neighbor_search().is_none());
    });
}

#[gpui::test]
fn picking_a_distance_runs_the_search_with_it(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(true, cx);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.set_nearest_neighbor_distance(VectorDistance::Cosine, cx);
            panel.set_nearest_neighbor_distance(VectorDistance::Cosine, cx);
        })
    });
    window.run_until_parked();

    assert_eq!(
        calls.lock().unwrap().executed,
        vec![
            "NEAREST embedding L2 LIMIT 10".to_string(),
            "NEAREST embedding Cosine LIMIT 10".to_string()
        ],
        "picking the distance shown again runs nothing"
    );
    assert_eq!(column_names(&panel, window)[0], "Cosine distance");
}

#[gpui::test]
fn a_failed_search_leaves_the_neighbors_and_says_why_in_the_bar(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(true, cx);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));
    calls.lock().unwrap().fail_execute = true;

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.set_nearest_neighbor_distance(VectorDistance::InnerProduct, cx)
        })
    });
    window.run_until_parked();

    window.update(|_, app| {
        let panel = panel.read(app);
        assert_eq!(
            panel.nearest_neighbor_search().map(|s| s.distance),
            Some(VectorDistance::L2),
            "the shown neighbors stay"
        );
        assert_eq!(panel.result.rows.len(), 2);
        assert!(
            panel
                .nearest
                .error
                .as_deref()
                .is_some_and(|error| error.contains("operator does not exist"))
        );
    });
}

#[gpui::test]
fn the_footer_counts_the_neighbors(cx: &mut TestAppContext) {
    let (panel, _, _, window) = open_table(true, cx);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));

    window.update(|_, app| {
        let panel = panel.read(app);
        let footer = panel.rows_footer(panel.result.rows.len());
        assert!(footer.starts_with("1 nearest"), "{footer}");
    });
}

/// A longer vector column name must not push the distance switch and K.
#[gpui::test]
fn the_column_picker_keeps_its_width_for_any_column_name(cx: &mut TestAppContext) {
    let (panel, window) = open_rendered_table(cx);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));
    redraw(window);
    let before = window
        .debug_bounds("nearest-neighbors-column-slot")
        .expect("the column picker is drawn");

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.nearest.column_dropdown.update(cx, |dropdown, cx| {
                dropdown.set_items(
                    vec![dbflux_components::controls::DropdownItem::new(
                        "a_much_longer_vector_column_name",
                    )],
                    cx,
                );
                dropdown.set_selected_index(Some(0), cx);
            });
        })
    });
    redraw(window);

    let after = window
        .debug_bounds("nearest-neighbors-column-slot")
        .expect("the column picker is drawn");
    assert_eq!(after.size.width, before.size.width);
}

/// Shift+F clears a filter the user can see; the neighbor bar hides it, so
/// the command leaves it for Back to table.
#[gpui::test]
fn clear_filter_keeps_the_hidden_filter_while_similar_rows_show(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(true, cx);
    set_filter(&panel, "label <> ''", window);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));
    let browses = calls.lock().unwrap().browses;

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.dispatch_command(Command::ClearFilter, window, cx)
        })
    });
    window.run_until_parked();

    assert_eq!(filter_text(&panel, window), "label <> ''");
    assert_eq!(
        calls.lock().unwrap().browses,
        browses,
        "the table is not read"
    );
    window.update(|_, app| {
        assert!(panel.read(app).nearest_neighbor_search().is_some());
    });
}

/// Sorting by a header would read the table ordered by a column of the
/// neighbors (the distance does not exist in the table).
#[gpui::test]
fn header_sort_does_not_read_the_table_while_similar_rows_show(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(true, cx);
    // Opening the table queues its own reads; let them land first.
    window.update(|window, app| {
        panel.update(app, |panel, cx| panel.process_pending_actions(window, cx))
    });
    window.run_until_parked();
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));
    let browses = calls.lock().unwrap().browses;

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.handle_sort_request(0, dbflux_core::SortDirection::Ascending, cx)
        })
    });

    window.update(|_, app| {
        let panel = panel.read(app);
        assert!(panel.pending.requery.is_none(), "no table read is queued");
        assert!(panel.nearest_neighbor_search().is_some());
    });
    window.run_until_parked();
    assert_eq!(calls.lock().unwrap().browses, browses);
}

/// An auto-refresh tick reads the neighbors again instead of the table page.
#[gpui::test]
fn a_queued_refresh_reruns_the_search(cx: &mut TestAppContext) {
    let (panel, calls, _, window) = open_table(true, cx);
    // Opening the table queues its own reads; let them land first.
    window.update(|window, app| {
        panel.update(app, |panel, cx| panel.process_pending_actions(window, cx))
    });
    window.run_until_parked();
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));
    let browses = calls.lock().unwrap().browses;

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.pending.refresh = true;
            panel.process_pending_actions(window, cx);
        })
    });
    window.run_until_parked();

    let calls = calls.lock().unwrap();
    assert_eq!(calls.browses, browses);
    assert_eq!(calls.executed.len(), 2, "the search ran again");
}

/// Filter, Order, Generate SQL and Copy as SQL build table queries from the
/// row on screen, which carries the computed distance.
#[gpui::test]
fn table_query_actions_are_withheld_while_similar_rows_show(cx: &mut TestAppContext) {
    let (panel, _, _, window) = open_table(true, cx);
    window.update(|_, app| assert!(panel.read(app).offers_table_query_actions()));

    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));

    window.update(|_, app| assert!(!panel.read(app).offers_table_query_actions()));
}

/// The vector column picker is reachable from the Toolbar submenu.
#[gpui::test]
fn the_column_picker_has_a_toolbar_action(cx: &mut TestAppContext) {
    let (panel, _, _, window) = open_table(true, cx);
    select_second_row(&panel, window);
    assert!(find_similar_rows(&panel, window));

    window.update(|_, app| {
        assert!(
            panel
                .read(app)
                .toolbar_actions(app)
                .contains(&ToolbarAction::NearestColumn)
        );
    });
}
