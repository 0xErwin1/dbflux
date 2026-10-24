//! "Export all rows" pages through the whole table behind a table grid and
//! writes every row to one file, and is not offered for a query result.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dbflux_core::{
    ColumnKind, ColumnMeta, ConnectedProfile, Connection, ConnectionProfile, DbConfig, DbError,
    QueryResult, TableBrowseRequest, TableRef, TaskKind, TaskStatus, Value,
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

/// Serves `TABLE_ROWS` rows of `(id, name)` honouring the offset and limit of
/// each browse, and records every browse it was asked.
struct PagedTableConnection {
    browses: Arc<Mutex<Vec<TableBrowseRequest>>>,
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

    fn execute(&self, _: &dbflux_core::QueryRequest) -> Result<QueryResult, DbError> {
        Err(DbError::NotSupported("browse only".to_string()))
    }

    fn browse_table(&self, request: &TableBrowseRequest) -> Result<QueryResult, DbError> {
        self.browses
            .lock()
            .map_err(|_| DbError::query_failed("poisoned"))?
            .push(request.clone());

        let start = (request.pagination.offset() as i64).min(TABLE_ROWS);
        let end = (start + i64::from(request.pagination.limit())).min(TABLE_ROWS);

        Ok(QueryResult::table(
            vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_name: "integer".to_string(),
                    kind: ColumnKind::Integer,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_name: "text".to_string(),
                    kind: ColumnKind::Text,
                    nullable: true,
                    is_primary_key: false,
                },
            ],
            (start..end)
                .map(|id| vec![Value::Int(id), Value::Text(format!("user {id}"))])
                .collect(),
            None,
            Duration::ZERO,
        ))
    }

    fn count_table(&self, _: &dbflux_core::TableCountRequest) -> Result<u64, DbError> {
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
    browses: Arc<Mutex<Vec<TableBrowseRequest>>>,
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
            let connection: Arc<dyn Connection> = Arc::new(PagedTableConnection { browses });
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

#[gpui::test]
fn export_all_rows_writes_every_row_of_the_table_to_one_file(cx: &mut TestAppContext) {
    init_test_runtime(cx);
    let dir = scratch_dir("csv");
    let target = dir.join("users.csv");
    let browses: Arc<Mutex<Vec<TableBrowseRequest>>> = Arc::default();
    let (app_state, profile_id) = app_state_saving_to(target.clone(), browses.clone(), cx);
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

    let grid_browses = browses.lock().map(|b| b.len()).unwrap_or_default();
    window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            panel.export_all_rows(FileFormat::Csv, window, cx)
        })
    });
    window.run_until_parked();

    let contents = std::fs::read_to_string(&target).expect("the export wrote the file");
    let mut lines = contents.lines();
    assert_eq!(lines.next(), Some("id,name"));
    let ids: Vec<i64> = lines
        .map(|line| {
            line.split(',')
                .next()
                .and_then(|id| id.parse().ok())
                .expect("each line starts with an id")
        })
        .collect();
    assert_eq!(ids, (0..TABLE_ROWS).collect::<Vec<_>>());

    let export_browses: Vec<TableBrowseRequest> = browses
        .lock()
        .map(|b| b[grid_browses..].to_vec())
        .unwrap_or_default();
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
    assert_eq!(task.description, "Export all rows of public.users");

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
