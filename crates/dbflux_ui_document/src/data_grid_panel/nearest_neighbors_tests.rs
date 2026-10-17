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
            text: format!("NEAREST {} LIMIT {}", request.column, request.limit),
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

fn neighbors() -> QueryResult {
    QueryResult::table(
        vec![
            column_meta("id", false),
            column_meta("label", false),
            column_meta("embedding", false),
            column_meta("distance", false),
        ],
        vec![vec![
            Value::Int(1),
            Value::Text("first".into()),
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

        Ok(neighbors())
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

fn open_table(
    with_vector: bool,
    cx: &mut TestAppContext,
) -> (
    Entity<DataGridPanel>,
    Arc<Mutex<Calls>>,
    Entity<ToastHost>,
    &mut VisualTestContext,
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
            vec!["NEAREST embedding LIMIT 10".to_string()]
        );
    }

    assert_eq!(
        column_names(&panel, window),
        vec!["id", "label", "embedding", "distance"]
    );

    window.update(|_, app| {
        let panel = panel.read(app);
        assert_eq!(panel.result.rows.len(), 1);
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
