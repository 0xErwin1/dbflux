//! "Export all rows" pages through the whole read behind a grid (a table
//! browse, a collection browse, a builder query or a relational filter) and
//! writes every row to one file, and is not offered for a query result.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dbflux_core::{
    CollectionBrowseRequest, CollectionRef, ColumnKind, ColumnMeta, ConnectedProfile, Connection,
    ConnectionProfile, DbConfig, DbError, DefaultSqlDialect, ObjectFacet, Projection,
    QueryGenerator, QueryRequest, QueryResult, SourceTable, SqlMutationGenerator,
    TableBrowseRequest, TableRef, TaskKind, TaskStatus, Value, VisualQuerySpec,
};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_transfer::FileFormat;
use dbflux_ui_base::{AppStateEntity, SaveTargetOutcome, SaveTargetProvider};
use gpui::{AppContext, Entity, TestAppContext, VisualTestContext};
use uuid::Uuid;

use super::{init_test_runtime, isolated_test_app_state, key_column};
use crate::data_grid_panel::DataGridPanel;
use crate::data_grid_panel::context_menu::ExportMenuEntry;

/// More rows than one grid page and than one export page.
const TABLE_ROWS: i64 = 2_345;

static GENERATOR: SqlMutationGenerator = SqlMutationGenerator::new(&DefaultSqlDialect);

/// Every read the stub was asked for.
#[derive(Default)]
struct Reads {
    browses: Mutex<Vec<TableBrowseRequest>>,
    collection_browses: Mutex<Vec<CollectionBrowseRequest>>,
    executed: Mutex<Vec<String>>,
}

impl Reads {
    fn browse_count(&self) -> usize {
        self.browses.lock().map(|b| b.len()).unwrap_or_default()
    }

    fn browses_from(&self, start: usize) -> Vec<TableBrowseRequest> {
        self.browses
            .lock()
            .map(|b| b[start..].to_vec())
            .unwrap_or_default()
    }

    fn collection_browses(&self) -> Vec<CollectionBrowseRequest> {
        self.collection_browses
            .lock()
            .map(|b| b.clone())
            .unwrap_or_default()
    }

    fn executed(&self) -> Vec<String> {
        self.executed.lock().map(|e| e.clone()).unwrap_or_default()
    }
}

/// Serves `TABLE_ROWS` rows of `(id, name)` honouring the offset and limit of
/// each table browse and of each generated SELECT, and as many documents
/// from its collection, and records every read it was asked.
struct PagedTableConnection {
    reads: Arc<Reads>,
}

/// Index of the first document that carries an `email` field: later pages
/// have a column the grid's first page does not.
const FIRST_DOCUMENT_WITH_EMAIL: i64 = 1_500;

fn text_column(name: &str, kind: ColumnKind, is_primary_key: bool) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        type_name: "text".to_string(),
        kind,
        nullable: !is_primary_key,
        is_primary_key,
    }
}

/// `(id, name)` rows `offset..offset + limit`, cut at `TABLE_ROWS`.
fn user_rows(offset: u64, limit: u64) -> QueryResult {
    let start = (offset as i64).min(TABLE_ROWS);
    let end = (start + limit as i64).min(TABLE_ROWS);

    QueryResult::table(
        vec![
            text_column("id", ColumnKind::Integer, true),
            text_column("name", ColumnKind::Text, false),
        ],
        (start..end)
            .map(|id| vec![Value::Int(id), Value::Text(format!("user {id}"))])
            .collect(),
        None,
        Duration::ZERO,
    )
}

/// Reads the number after `keyword` in generated SQL.
fn sql_number_after(sql: &str, keyword: &str) -> Option<u64> {
    let upper = sql.to_uppercase();
    let start = upper.find(keyword)? + keyword.len();
    upper[start..]
        .split_whitespace()
        .next()
        .and_then(|number| number.parse().ok())
}

impl Connection for PagedTableConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        use dbflux_core::{
            DatabaseCategory, DriverCapabilities, DriverMetadata, Icon as CoreIcon, QueryLanguage,
            TransferFamily,
        };

        static META: std::sync::OnceLock<DriverMetadata> = std::sync::OnceLock::new();
        META.get_or_init(|| DriverMetadata {
            id: "stub-paged".to_string(),
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

    fn query_generator(&self) -> Option<&dyn QueryGenerator> {
        Some(&GENERATOR)
    }

    fn execute(&self, request: &QueryRequest) -> Result<QueryResult, DbError> {
        self.reads
            .executed
            .lock()
            .map_err(|_| DbError::query_failed("poisoned"))?
            .push(request.sql.clone());

        let limit = sql_number_after(&request.sql, "LIMIT").unwrap_or(TABLE_ROWS as u64);
        let offset = sql_number_after(&request.sql, "OFFSET").unwrap_or(0);
        Ok(user_rows(offset, limit))
    }

    fn browse_table(&self, request: &TableBrowseRequest) -> Result<QueryResult, DbError> {
        self.reads
            .browses
            .lock()
            .map_err(|_| DbError::query_failed("poisoned"))?
            .push(request.clone());

        Ok(user_rows(
            request.pagination.offset(),
            u64::from(request.pagination.limit()),
        ))
    }

    fn count_table(&self, _: &dbflux_core::TableCountRequest) -> Result<u64, DbError> {
        Ok(TABLE_ROWS as u64)
    }

    fn collection_facets(&self, _: &CollectionRef) -> Vec<ObjectFacet> {
        vec![ObjectFacet::Document]
    }

    /// Documents `{_id, name}`, plus `email` from `FIRST_DOCUMENT_WITH_EMAIL`
    /// on; a page's columns are the union of its documents' fields.
    fn browse_collection(&self, request: &CollectionBrowseRequest) -> Result<QueryResult, DbError> {
        self.reads
            .collection_browses
            .lock()
            .map_err(|_| DbError::query_failed("poisoned"))?
            .push(request.clone());

        let start = (request.pagination.offset() as i64).min(TABLE_ROWS);
        let end = (start + i64::from(request.pagination.limit())).min(TABLE_ROWS);
        let with_email = end > FIRST_DOCUMENT_WITH_EMAIL;

        let mut columns = vec![
            text_column("_id", ColumnKind::Integer, true),
            text_column("name", ColumnKind::Text, false),
        ];
        if with_email {
            columns.push(text_column("email", ColumnKind::Text, false));
        }

        let rows = (start..end)
            .map(|id| {
                let mut row = vec![Value::Int(id), Value::Text(format!("doc {id}"))];
                if with_email {
                    row.push(if id >= FIRST_DOCUMENT_WITH_EMAIL {
                        Value::Text(format!("{id}@example.com"))
                    } else {
                        Value::Null
                    });
                }
                row
            })
            .collect();

        Ok(QueryResult::json(columns, rows, Duration::ZERO))
    }

    fn count_collection(&self, _: &dbflux_core::CollectionCountRequest) -> Result<u64, DbError> {
        Ok(TABLE_ROWS as u64)
    }

    fn cancel(&self, _: &dbflux_core::QueryHandle) -> Result<(), DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }
}

fn connected_profile(connection: Arc<dyn Connection>) -> ConnectedProfile {
    ConnectedProfile {
        profile: ConnectionProfile::new(
            "paged",
            DbConfig::SQLite {
                path: PathBuf::from(":memory:"),
                connection_id: None,
            },
        ),
        connection,
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
    }
}

/// An app state whose Save As answers `target` and whose only connection is
/// a `PagedTableConnection`.
fn app_state_saving_to(
    target: PathBuf,
    reads: Arc<Reads>,
    cx: &mut TestAppContext,
) -> (Entity<AppStateEntity>, Uuid) {
    let provider: SaveTargetProvider = Arc::new(move |_request| {
        gpui::Task::ready(SaveTargetOutcome::Selected {
            path: target.clone(),
            used_fallback: false,
        })
    });
    let profile_id = Uuid::new_v4();

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            AppStateEntity::new_with_storage_runtime(
                StorageRuntime::in_memory().expect("isolated storage runtime"),
            )
            .expect("test storage setup")
            .with_save_target_override(provider)
        })
    });
    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            let connection: Arc<dyn Connection> = Arc::new(PagedTableConnection { reads });
            app.connections_mut()
                .insert(profile_id, connected_profile(connection));
        });
    });

    (app_state, profile_id)
}

fn scratch_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "dbflux_grid_export_all_{label}_{}_{}",
        std::process::id(),
        Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

fn open_table(
    app_state: Entity<AppStateEntity>,
    profile_id: Uuid,
    cx: &mut TestAppContext,
) -> (Entity<DataGridPanel>, &mut VisualTestContext) {
    let window = cx.add_empty_window();
    let panel = window.update(|window, app| {
        app.new(|cx| {
            DataGridPanel::new_for_table(
                profile_id,
                TableRef::with_schema("public", "users"),
                None,
                app_state,
                window,
                cx,
            )
        })
    });
    window.run_until_parked();

    (panel, window)
}

fn open_collection(
    app_state: Entity<AppStateEntity>,
    profile_id: Uuid,
    cx: &mut TestAppContext,
) -> (Entity<DataGridPanel>, &mut VisualTestContext) {
    let window = cx.add_empty_window();
    let panel = window.update(|window, app| {
        app.new(|cx| {
            DataGridPanel::new_for_collection(
                profile_id,
                CollectionRef::new("app", "events"),
                app_state,
                window,
                cx,
            )
        })
    });
    window.run_until_parked();

    (panel, window)
}

fn offers_export_all(panel: &Entity<DataGridPanel>, window: &mut VisualTestContext) -> bool {
    let entries = window.update(|_, cx| panel.read(cx).export_menu_entries());
    entries.contains(&ExportMenuEntry::SaveAll(FileFormat::Csv))
        && entries.contains(&ExportMenuEntry::SaveAll(FileFormat::Json))
}

fn export_all_csv(panel: &Entity<DataGridPanel>, window: &mut VisualTestContext) {
    window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            panel.export_all_rows(FileFormat::Csv, window, cx)
        })
    });
    window.run_until_parked();
}

/// The header line and the integer in the first field of every other line.
fn csv_header_and_ids(path: &std::path::Path) -> (String, Vec<i64>) {
    let contents = std::fs::read_to_string(path).expect("the export wrote the file");
    let mut lines = contents.lines();
    let header = lines.next().unwrap_or_default().to_string();
    let ids = lines
        .map(|line| {
            line.split(',')
                .next()
                .and_then(|id| id.parse().ok())
                .expect("each line starts with an id")
        })
        .collect();
    (header, ids)
}

fn assert_export_task_completed(
    app_state: &Entity<AppStateEntity>,
    description: &str,
    window: &mut VisualTestContext,
) {
    let task = window
        .update(|_, cx| {
            app_state
                .read(cx)
                .tasks()
                .recent_tasks(10)
                .into_iter()
                .find(|task| task.kind == TaskKind::Export)
        })
        .expect("the export runs as a task");
    assert_eq!(task.status, TaskStatus::Completed);
    assert_eq!(task.description, description);
}

fn users_spec(limit: Option<u64>, offset: u64) -> VisualQuerySpec {
    VisualQuerySpec {
        source: SourceTable {
            schema: Some("public".to_string()),
            table: "users".to_string(),
            alias: "users".to_string(),
        },
        projection: Projection::All,
        joins: Vec::new(),
        filter: None,
        group_by: Vec::new(),
        aggregates: Vec::new(),
        having: None,
        sort: Vec::new(),
        limit,
        offset,
    }
}

/// Runs `spec` through the builder the way its Run button does.
fn run_builder_spec(
    panel: &Entity<DataGridPanel>,
    spec: VisualQuerySpec,
    window: &mut VisualTestContext,
) {
    window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            panel.apply_builder_draft_spec(spec, cx);
            panel.refresh(window, cx);
        })
    });
    window.run_until_parked();
}

#[gpui::test]
fn export_all_rows_writes_every_row_of_the_table_to_one_file(cx: &mut TestAppContext) {
    init_test_runtime(cx);
    let dir = scratch_dir("csv");
    let target = dir.join("users.csv");
    let reads: Arc<Reads> = Arc::default();
    let (app_state, profile_id) = app_state_saving_to(target.clone(), reads.clone(), cx);
    let (panel, window) = open_table(app_state.clone(), profile_id, cx);

    let loaded_rows = window.update(|_, cx| panel.read(cx).result.rows.len());
    assert!(
        (loaded_rows as i64) < TABLE_ROWS,
        "the grid holds one page, got {loaded_rows} rows"
    );

    let entries = window.update(|_, cx| panel.read(cx).export_menu_entries());
    assert!(
        entries.contains(&ExportMenuEntry::SaveAll(FileFormat::Csv))
            && entries.contains(&ExportMenuEntry::SaveAll(FileFormat::Json)),
        "a table grid offers Export all rows, got {entries:?}"
    );

    let grid_browses = reads.browse_count();
    window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            panel.export_all_rows(FileFormat::Csv, window, cx)
        })
    });
    window.run_until_parked();

    let (header, ids) = csv_header_and_ids(&target);
    assert_eq!(header, "id,name");
    assert_eq!(ids, (0..TABLE_ROWS).collect::<Vec<_>>());

    let export_browses = reads.browses_from(grid_browses);
    assert!(
        export_browses.len() > 1,
        "the export reads the table page by page"
    );
    assert!(
        export_browses
            .iter()
            .all(|request| request.pagination.limit()
                == crate::data_grid_panel::export_all::EXPORT_ALL_PAGE_SIZE),
        "every export page uses the export page size"
    );

    assert_export_task_completed(&app_state, "Export all rows of public.users", window);

    std::fs::remove_dir_all(&dir).ok();
}

#[gpui::test]
fn export_all_rows_writes_every_document_of_the_collection_in_order(cx: &mut TestAppContext) {
    init_test_runtime(cx);
    let dir = scratch_dir("collection");
    let target = dir.join("events.csv");
    let reads: Arc<Reads> = Arc::default();
    let (app_state, profile_id) = app_state_saving_to(target.clone(), reads.clone(), cx);
    let (panel, window) = open_collection(app_state.clone(), profile_id, cx);

    let (loaded_rows, loaded_columns) = window.update(|_, cx| {
        let result = &panel.read(cx).result;
        let columns: Vec<String> = result.columns.iter().map(|c| c.name.clone()).collect();
        (result.rows.len(), columns)
    });
    assert!((loaded_rows as i64) < TABLE_ROWS, "the grid holds one page");
    assert!(
        offers_export_all(&panel, window),
        "a collection grid offers Export all rows"
    );

    let grid_browses = reads.collection_browses().len();
    export_all_csv(&panel, window);

    let (header, ids) = csv_header_and_ids(&target);
    assert_eq!(
        header,
        loaded_columns.join(","),
        "the file has the columns the loaded-rows export writes"
    );
    assert_eq!(header, "_id,name");
    assert_eq!(ids, (0..TABLE_ROWS).collect::<Vec<_>>());

    let export_browses = reads.collection_browses()[grid_browses..].to_vec();
    assert!(export_browses.len() > 1, "the export reads page by page");
    assert_eq!(export_browses[0].pagination.offset(), 0);
    assert!(
        export_browses
            .iter()
            .all(|request| request.pagination.limit()
                == crate::data_grid_panel::export_all::EXPORT_ALL_PAGE_SIZE)
    );

    assert_export_task_completed(&app_state, "Export all rows of app.events", window);
    std::fs::remove_dir_all(&dir).ok();
}

#[gpui::test]
fn export_all_rows_pages_the_builder_query_across_every_row(cx: &mut TestAppContext) {
    init_test_runtime(cx);
    let dir = scratch_dir("builder");
    let target = dir.join("users.csv");
    let reads: Arc<Reads> = Arc::default();
    let (app_state, profile_id) = app_state_saving_to(target.clone(), reads.clone(), cx);
    let (panel, window) = open_table(app_state.clone(), profile_id, cx);

    run_builder_spec(&panel, users_spec(None, 0), window);
    assert!(
        offers_export_all(&panel, window),
        "a builder-driven grid offers Export all rows"
    );

    let grid_queries = reads.executed().len();
    let grid_browses = reads.browse_count();
    export_all_csv(&panel, window);

    let (header, ids) = csv_header_and_ids(&target);
    assert_eq!(header, "id,name");
    assert_eq!(ids, (0..TABLE_ROWS).collect::<Vec<_>>());

    let export_queries = reads.executed()[grid_queries..].to_vec();
    assert!(
        export_queries.len() > 1,
        "the export pages the builder query: {export_queries:?}"
    );
    assert!(export_queries.iter().all(|sql| {
        sql_number_after(sql, "LIMIT")
            == Some(u64::from(
                crate::data_grid_panel::export_all::EXPORT_ALL_PAGE_SIZE,
            ))
    }));
    assert_eq!(
        reads.browse_count(),
        grid_browses,
        "the export runs the builder query, not a table browse"
    );

    assert_export_task_completed(&app_state, "Export all rows of public.users", window);
    std::fs::remove_dir_all(&dir).ok();
}

#[gpui::test]
fn export_all_rows_keeps_the_builder_limit_and_offset(cx: &mut TestAppContext) {
    init_test_runtime(cx);
    let dir = scratch_dir("builder_limit");
    let target = dir.join("users.csv");
    let reads: Arc<Reads> = Arc::default();
    let (app_state, profile_id) = app_state_saving_to(target.clone(), reads.clone(), cx);
    let (panel, window) = open_table(app_state.clone(), profile_id, cx);

    run_builder_spec(&panel, users_spec(Some(1_200), 10), window);
    assert!(offers_export_all(&panel, window));
    export_all_csv(&panel, window);

    let (_, ids) = csv_header_and_ids(&target);
    assert_eq!(
        ids,
        (10..1_210).collect::<Vec<_>>(),
        "the file holds the spec's own rows and no more"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[gpui::test]
fn export_all_rows_pages_the_query_an_active_relational_filter_runs(cx: &mut TestAppContext) {
    init_test_runtime(cx);
    let dir = scratch_dir("relational");
    let target = dir.join("users.csv");
    let reads: Arc<Reads> = Arc::default();
    let (app_state, profile_id) = app_state_saving_to(target.clone(), reads.clone(), cx);
    let (panel, window) = open_table(app_state.clone(), profile_id, cx);

    window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            panel.apply_fk_result(
                vec![dbflux_core::SchemaForeignKeyInfo {
                    name: "fk_users_org".to_string(),
                    table_name: "users".to_string(),
                    columns: vec!["org_id".to_string()],
                    referenced_schema: Some("public".to_string()),
                    referenced_table: "organizations".to_string(),
                    referenced_columns: vec!["id".to_string()],
                    on_delete: None,
                    on_update: None,
                }],
                cx,
            );
            panel.filter_bar.filter_input.update(cx, |input, cx| {
                input.set_value("org.name = 'Acme'", window, cx);
            });
            panel.refresh(window, cx);
        })
    });
    window.run_until_parked();

    assert!(
        window.update(|_, cx| matches!(
            panel.read(cx).builder.relational_filter_state,
            super::super::filter_bar::RelationalFilterState::Active { .. }
        )),
        "the filter lowers to a relational query"
    );
    assert!(
        offers_export_all(&panel, window),
        "a relational filter offers Export all rows"
    );

    let grid_queries = reads.executed().len();
    export_all_csv(&panel, window);

    let (_, ids) = csv_header_and_ids(&target);
    assert_eq!(ids, (0..TABLE_ROWS).collect::<Vec<_>>());
    let export_queries = reads.executed()[grid_queries..].to_vec();
    assert!(export_queries.len() > 1, "the export reads page by page");
    assert!(
        export_queries
            .iter()
            .all(|sql| sql.to_uppercase().contains("JOIN")),
        "every page runs the lowered join: {export_queries:?}"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[gpui::test]
fn export_all_rows_is_not_offered_for_a_query_result(cx: &mut TestAppContext) {
    init_test_runtime(cx);
    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = window.update(|window, app| {
        app.new(|cx| {
            DataGridPanel::new_for_result(
                Arc::new(QueryResult::table(
                    vec![key_column("id", false)],
                    vec![vec![Value::Int(1)]],
                    None,
                    Duration::ZERO,
                )),
                "SELECT id FROM users".to_string(),
                None,
                app_state,
                window,
                cx,
            )
        })
    });
    window.run_until_parked();

    let entries = window.update(|_, cx| panel.read(cx).export_menu_entries());
    assert!(
        !entries
            .iter()
            .any(|entry| matches!(entry, ExportMenuEntry::SaveAll(_))),
        "a query result offers only the loaded rows, got {entries:?}"
    );
    assert!(entries.contains(&ExportMenuEntry::Save(dbflux_export::ExportFormat::Csv)));
    assert!(!window.update(|_, cx| panel.read(cx).can_export_all_rows()));
}
