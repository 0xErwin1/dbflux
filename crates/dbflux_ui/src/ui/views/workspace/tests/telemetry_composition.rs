//! Test-only composition harness for the five GUI successful-use counters.
//!
//! One `AppStateEntity`, one foreground ledger: the harness drives the five
//! producers through their real production paths (code-document execution,
//! the workspace dashboard opener, the schema-viz opener and the data view's
//! query-builder rail), then runs exactly one `TelemetryScheduler::prepare`
//! advanced past the Basic cadence and asserts on the frozen Basic payload.
//!
//! Wire honesty: the payload assembly and the durable audit correlation are
//! always asserted. The payload freezes against a loopback endpoint; when no
//! collector is configured the only delivery attempt is a guaranteed-refused
//! loopback connection, and the test says so in its output instead of
//! claiming wire coverage. With `DBFLUX_TELEMETRY_TEST_ENDPOINT` set (a local
//! collector), the full delivery path runs and the scheduler's own
//! completion disposition carries the assertion — the collector is never
//! inspected by hand.

use crate::keymap::Command;
use crate::ui::document::pane::PaneHandle;
use crate::ui::document::{CodeDocument, DataDocument, DocumentId, Tab};
use crate::ui::views::workspace::Workspace;
use dbflux_app::telemetry::{BuildIdentity, TelemetryConfig};
use dbflux_audit::query::AuditQueryFilter;
use dbflux_core::{
    ColumnKind, ColumnMeta, ConnectionProfile, DatabaseCategory, DbConfig, DbError, DbKind,
    DefaultSqlDialect, DriverCapabilities, DriverMetadata, QueryCapabilities, QueryHandle,
    QueryLanguage, QueryRequest, QueryResult, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect,
    TableBrowseRequest, TableCountRequest, TableRef, TransferFamily, Value, WhereOperator,
};
use dbflux_storage::StorageRuntime;
use dbflux_telemetry::{Architecture, Platform};
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use dbflux_ui_base::{AppStateEntity, AppStateGlobal};
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Fixture RC identity: `0.9.0-rc.0` is admitted by `BuildIdentity::published`
/// while the dev workspace version `0.9.0-dev.0` is not, so the production
/// `initialize_telemetry` path alone can never freeze a payload in tests.
fn fixture_identity() -> Result<BuildIdentity, dbflux_app::telemetry::TelemetryError> {
    let platform = match std::env::consts::OS {
        "linux" => Platform::Linux,
        "macos" => Platform::Darwin,
        "windows" => Platform::Windows,
        _ => unreachable!("unsupported test platform"),
    };
    let architecture = match std::env::consts::ARCH {
        "x86_64" => Architecture::Amd64,
        "aarch64" => Architecture::Arm64,
        _ => unreachable!("unsupported test architecture"),
    };
    BuildIdentity::published(
        "0.9.0-rc.0",
        dbflux_core::ReleaseChannel::Rc,
        platform,
        architecture,
    )
}

/// The endpoint under test. The application's own `DBFLUX_TELEMETRY_*`
/// variables never reach this harness, so a developer's configuration cannot
/// send test events anywhere. Only the explicit test opt-in
/// `DBFLUX_TELEMETRY_TEST_ENDPOINT` exercises a real collector; otherwise the
/// harness uses a guaranteed refused loopback port so payload assembly and
/// audit correlation stay testable without pretending to reach a collector.
fn telemetry_config() -> (TelemetryConfig, bool) {
    let allow_loopback = std::env::var("DBFLUX_TELEMETRY_ALLOW_LOOPBACK_HTTP").ok();
    match std::env::var("DBFLUX_TELEMETRY_TEST_ENDPOINT") {
        Ok(endpoint) if !endpoint.is_empty() => (
            TelemetryConfig::from_values(Some(&endpoint), allow_loopback.as_deref(), true)
                .expect("configured test telemetry endpoint"),
            true,
        ),
        _ => (
            TelemetryConfig::from_values(Some("http://127.0.0.1:9/v1/events"), Some("1"), true)
                .expect("loopback telemetry endpoint"),
            false,
        ),
    }
}

fn text_columns(names: &[&str]) -> Vec<ColumnMeta> {
    names
        .iter()
        .map(|name| ColumnMeta {
            name: (*name).to_string(),
            type_name: "text".to_string(),
            // `Unknown` keeps chart detection out of the toolbar actions, so
            // the builder's position inside the toolbar submenu stays fixed.
            kind: ColumnKind::Unknown,
            nullable: true,
            is_primary_key: *name == "id",
        })
        .collect()
}

/// Complete metadata for the fixture table, shared by both stub connections.
fn fixture_users_table() -> dbflux_core::TableInfo {
    dbflux_core::TableInfo {
        name: "users".to_string(),
        schema: Some("public".to_string()),
        columns: Some(vec![dbflux_core::ColumnInfo {
            name: "id".to_string(),
            type_name: "integer".to_string(),
            nullable: false,
            is_primary_key: true,
            default_value: None,
            enum_values: None,
        }]),
        indexes: Some(dbflux_core::IndexData::Relational(vec![])),
        foreign_keys: Some(vec![]),
        constraints: Some(vec![]),
        sample_fields: None,
        presentation: dbflux_core::CollectionPresentation::DataGrid,
        child_items: None,
        storage_hints: None,
        pseudo_columns: Box::default(),
    }
}

fn three_row_result() -> QueryResult {
    let rows = ["a", "b", "c"]
        .iter()
        .map(|id| {
            vec![
                Value::Text((*id).to_string()),
                Value::Text("name".to_string()),
                Value::Text("email".to_string()),
            ]
        })
        .collect();
    QueryResult::table(
        text_columns(&["id", "name", "email"]),
        rows,
        None,
        Duration::ZERO,
    )
}

/// Answers every SQL path with the same successful table result, and offers
/// enough metadata for the visual query builder to open and run.
struct QueryStubConnection {
    metadata: DriverMetadata,
    executes: Arc<std::sync::atomic::AtomicUsize>,
    /// When set, `browse_table` fails — used to prove the builder run goes
    /// through the structured SELECT path, not a table browse.
    fail_browses: std::sync::atomic::AtomicBool,
    /// When set, `execute` fails — the negative control proving that a failed
    /// run never credits `query_run`.
    fail_executes: std::sync::atomic::AtomicBool,
}

impl QueryStubConnection {
    fn new() -> Self {
        Self::counted(Arc::new(std::sync::atomic::AtomicUsize::new(0)))
    }

    fn counted(executes: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        Self {
            executes,
            fail_browses: std::sync::atomic::AtomicBool::new(false),
            fail_executes: std::sync::atomic::AtomicBool::new(false),
            metadata: DriverMetadata {
                id: "stub-telemetry-query".to_string(),
                display_name: "Stub".to_string(),
                description: "test".to_string(),
                category: DatabaseCategory::Relational,
                transfer_family: TransferFamily::Sql,
                deployment_class: None,
                query_language: QueryLanguage::Sql,
                capabilities: DriverCapabilities::empty(),
                default_port: None,
                uri_scheme: "stub".to_string(),
                icon: dbflux_core::Icon::Database,
                syntax: None,
                query: Some(QueryCapabilities {
                    where_operators: vec![WhereOperator::Eq, WhereOperator::Like],
                    ..QueryCapabilities::default()
                }),
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
            },
        }
    }
}

impl dbflux_core::Connection for QueryStubConnection {
    fn metadata(&self) -> &DriverMetadata {
        &self.metadata
    }

    fn kind(&self) -> DbKind {
        DbKind::SQLite
    }

    // Skips the schema-drift preflight for connectionless documents: with no
    // active database the preflight has no fetch key and would only run the
    // same execution through a longer path.
    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::LazyPerDatabase
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &DefaultSqlDialect
    }

    fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
        static GENERATOR: dbflux_core::SqlMutationGenerator =
            dbflux_core::SqlMutationGenerator::new(&dbflux_core::DefaultSqlDialect);
        Some(&GENERATOR)
    }

    fn ping(&self) -> Result<(), DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, _: &QueryRequest) -> Result<QueryResult, DbError> {
        self.executes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.fail_executes.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(DbError::NotSupported(
                "fixture: a failed run must not credit query_run".to_string(),
            ));
        }
        Ok(three_row_result())
    }

    fn table_details(
        &self,
        _: &str,
        _: Option<&str>,
        table: &str,
    ) -> Result<dbflux_core::TableInfo, DbError> {
        if table == "users" {
            return Ok(fixture_users_table());
        }
        Err(DbError::NotSupported("unknown fixture table".to_string()))
    }

    fn browse_table(&self, _: &TableBrowseRequest) -> Result<QueryResult, DbError> {
        if self.fail_browses.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(DbError::NotSupported(
                "fixture: builder run must use the structured select".to_string(),
            ));
        }
        Ok(three_row_result())
    }

    fn count_table(&self, _: &TableCountRequest) -> Result<u64, DbError> {
        Ok(3)
    }

    fn cancel(&self, _: &QueryHandle) -> Result<(), DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        Ok(SchemaSnapshot::default())
    }
}

/// Serves complete schema metadata for one table so the schema-viz loader
/// finishes with `metadata_complete` and credits the explicit inspect.
struct SchemaStubConnection {
    metadata: DriverMetadata,
}

impl SchemaStubConnection {
    fn new() -> Self {
        Self {
            metadata: DriverMetadata {
                id: "stub-telemetry-schema".to_string(),
                display_name: "Stub".to_string(),
                description: "test".to_string(),
                category: DatabaseCategory::Relational,
                transfer_family: TransferFamily::Sql,
                deployment_class: None,
                query_language: QueryLanguage::Sql,
                capabilities: DriverCapabilities::FOREIGN_KEYS,
                default_port: None,
                uri_scheme: "stub".to_string(),
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
            },
        }
    }

    fn users_table() -> dbflux_core::TableInfo {
        dbflux_core::TableInfo {
            name: "users".to_string(),
            schema: None,
            columns: Some(vec![dbflux_core::ColumnInfo {
                name: "id".to_string(),
                type_name: "integer".to_string(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
                enum_values: None,
            }]),
            indexes: Some(dbflux_core::IndexData::Relational(vec![])),
            foreign_keys: Some(vec![]),
            constraints: Some(vec![]),
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::DataGrid,
            child_items: None,
            storage_hints: None,
            pseudo_columns: Box::default(),
        }
    }
}

impl dbflux_core::Connection for SchemaStubConnection {
    fn metadata(&self) -> &DriverMetadata {
        &self.metadata
    }

    fn kind(&self) -> DbKind {
        DbKind::SQLite
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::SingleDatabase
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &DefaultSqlDialect
    }

    fn ping(&self) -> Result<(), DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, _: &QueryRequest) -> Result<QueryResult, DbError> {
        Ok(QueryResult::empty())
    }

    fn cancel(&self, _: &QueryHandle) -> Result<(), DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        Ok(SchemaSnapshot::default())
    }

    fn schema_for_database(&self, database: &str) -> Result<dbflux_core::DbSchemaInfo, DbError> {
        Ok(dbflux_core::DbSchemaInfo {
            name: database.to_string(),
            tables: vec![Self::users_table()],
            views: vec![],
            custom_types: None,
        })
    }

    fn table_details(
        &self,
        _: &str,
        _: Option<&str>,
        table: &str,
    ) -> Result<dbflux_core::TableInfo, DbError> {
        if table == "users" {
            return Ok(Self::users_table());
        }
        Err(DbError::NotSupported("unknown fixture table".to_string()))
    }

    fn schema_columns(
        &self,
        _: &str,
        _: Option<&str>,
    ) -> Result<Vec<dbflux_core::SchemaColumnInfo>, DbError> {
        Ok(vec![dbflux_core::SchemaColumnInfo {
            table_name: "users".to_string(),
            column: dbflux_core::ColumnInfo {
                name: "id".to_string(),
                type_name: "integer".to_string(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
                enum_values: None,
            },
        }])
    }

    fn schema_indexes(
        &self,
        _: &str,
        _: Option<&str>,
    ) -> Result<Vec<dbflux_core::SchemaIndexInfo>, DbError> {
        Ok(vec![])
    }

    fn schema_foreign_keys(
        &self,
        _: &str,
        _: Option<&str>,
    ) -> Result<Vec<dbflux_core::SchemaForeignKeyInfo>, DbError> {
        Ok(vec![])
    }
}

// ---------------------------------------------------------------------------
// Shared harness state
// ---------------------------------------------------------------------------

struct Harness<'a> {
    app_state: Entity<AppStateEntity>,
    workspace: Entity<Workspace>,
    window: &'a mut VisualTestContext,
}

fn init_globals(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });
}

/// One app state, one window, one workspace — the single foreground ledger.
fn new_harness(cx: &mut TestAppContext) -> Harness<'_> {
    init_globals(cx);
    let app_state: Entity<AppStateEntity> = cx.update(|cx| {
        cx.new(|_| {
            let runtime = StorageRuntime::in_memory().expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    });
    cx.update(|cx| {
        cx.set_global(AppStateGlobal {
            entity: app_state.clone(),
        });
    });

    let holder = std::rc::Rc::new(std::cell::RefCell::new(None));
    let workspace_ref = holder.clone();
    let (_, window) = cx.add_window_view(|window, cx| {
        let workspace = cx.new(|cx| Workspace::new(app_state.clone(), window, cx));
        workspace_ref.replace(Some(workspace.clone()));
        gpui_component::Root::new(workspace, window, cx)
    });
    let workspace = holder.borrow().clone().expect("workspace created");
    window.run_until_parked();
    Harness {
        app_state,
        workspace,
        window,
    }
}

impl Harness<'_> {
    fn register_connection(&mut self, connection: Arc<dyn dbflux_core::Connection>) -> Uuid {
        let profile_id = Uuid::new_v4();
        let app_state = self.app_state.clone();
        self.window.update(|_, cx| {
            app_state.update(cx, |app, _| {
                let profile = ConnectionProfile::new(
                    "telemetry-stub",
                    DbConfig::SQLite {
                        path: std::path::PathBuf::from(":memory:"),
                        connection_id: None,
                    },
                );
                app.connections_mut().insert(
                    profile_id,
                    dbflux_core::ConnectedProfile {
                        profile,
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
                    },
                );
            });
        });
        profile_id
    }

    fn enable_telemetry(&mut self) -> bool {
        let (config, collector) = telemetry_config();
        let app_state = self.app_state.clone();
        self.window.update(|_, cx| {
            app_state.update(cx, |app, _| {
                app.initialize_telemetry_with_identity(
                    config,
                    fixture_identity(),
                    chrono::Utc::now(),
                )
                .expect("scheduler with fixture identity");
                let scheduler = app.telemetry_mut().expect("scheduler");
                scheduler
                    .set_basic(true, chrono::Utc::now())
                    .expect("basic");
                scheduler
                    .disclosure_dismissed(chrono::Utc::now())
                    .expect("disclosure");
            });
        });
        collector
    }

    /// The frozen Basic delivery JSON from the pending queue, if present.
    fn frozen_basic_payload(&mut self) -> Option<String> {
        let app_state = self.app_state.clone();
        self.window.update(|_, cx| {
            let scheduler = app_state.read(cx).telemetry()?;
            scheduler.state().pending().iter().find_map(|delivery| {
                let json = delivery.json();
                (serde_json::from_str::<serde_json::Value>(json)
                    .ok()?
                    .get("event")?
                    == "basic")
                    .then(|| json.to_owned())
            })
        })
    }

    /// Opens a document entity in the workspace tab manager and activates it,
    /// so its pending actions process through the real render cycle.
    fn open_tab(&mut self, pane: PaneHandle, id: DocumentId) {
        let workspace = self.workspace.clone();
        self.window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.tab_manager.update(cx, |manager, cx| {
                    manager.open(Tab::Pane(Box::new(pane)), cx);
                    manager.activate(id, cx);
                });
            });
        });
    }

    fn run_one_prepare(
        &mut self,
        now: chrono::DateTime<chrono::Utc>,
    ) -> dbflux_app::telemetry::Preparation {
        let app_state = self.app_state.clone();
        self.window.update(|_, cx| {
            app_state.update(cx, |app, _| {
                let scheduler = app.telemetry_mut().expect("scheduler");
                scheduler.prepare(now, "en").expect("prepare")
            })
        })
    }
}

// ---------------------------------------------------------------------------
// The composition test
// ---------------------------------------------------------------------------

#[gpui::test]
fn five_gui_counters_compose_into_one_basic_payload(cx: &mut TestAppContext) {
    let mut harness = new_harness(cx);
    let collector_configured = harness.enable_telemetry();
    if !collector_configured {
        eprintln!(
            "telemetry composition: DBFLUX_TELEMETRY_TEST_ENDPOINT is not configured; \
             the harness asserts local payload assembly and durable audit \
             correlation against a refused loopback port, and does NOT claim \
             collector wire coverage"
        );
    }

    // -- producer 1: query_run (user SQL run through the code document) ------
    let query_stub = Arc::new(QueryStubConnection::new());
    let query_profile = harness.register_connection(query_stub.clone());
    let (sql_document, sql_document_id, pane) = {
        let app_state = harness.app_state.clone();
        let profile_id = query_profile;
        harness.window.update(|window, cx| {
            let document = cx.new(|cx| {
                CodeDocument::new_with_language(
                    app_state.clone(),
                    Some(profile_id),
                    QueryLanguage::Sql,
                    window,
                    cx,
                )
            });
            let id = document.read(cx).id();
            let pane = CodeDocument::into_pane(document.clone(), cx);
            (document, id, pane)
        })
    };
    harness.open_tab(pane, sql_document_id);
    harness.window.run_until_parked();
    harness.window.update(|window, cx| {
        sql_document.update(cx, |doc, cx| {
            doc.set_content("SELECT id, name, email FROM users", window, cx);
            doc.run_query(window, cx);
        });
    });
    harness.window.run_until_parked();
    harness.window.run_until_parked();
    // Negative control: a failed run must not credit, so `query_run` is pinned
    // at exactly 1 by the final payload assertion.
    query_stub
        .fail_executes
        .store(true, std::sync::atomic::Ordering::SeqCst);
    harness.window.update(|window, cx| {
        sql_document.update(cx, |doc, cx| {
            doc.set_content("SELECT broken FROM users", window, cx);
            doc.run_query(window, cx);
        });
    });
    harness.window.run_until_parked();
    harness.window.run_until_parked();

    // -- producer 2: script_run (failure then success) ------------------------
    // Bash has no default interpreter on Windows, so the in-process Lua runner
    // stands in there; the other platforms keep the real Bash interpreter.
    // The failing script is the negative control: a started-but-failed script
    // must never credit, so the final payload's script_run stays at exactly 1.
    let (script_language, failing_script, passing_script) = if cfg!(windows) {
        (
            QueryLanguage::Lua,
            "error('script failure')",
            "print('script ok')",
        )
    } else {
        (
            QueryLanguage::Bash,
            "printf 'script failure\\n'; exit 1",
            "printf 'script ok\\n'",
        )
    };
    let (script_document, script_document_id, pane) = {
        let app_state = harness.app_state.clone();
        harness.window.update(|window, cx| {
            let document = cx.new(|cx| {
                CodeDocument::new_with_language(
                    app_state.clone(),
                    None,
                    script_language,
                    window,
                    cx,
                )
            });
            let id = document.read(cx).id();
            let pane = CodeDocument::into_pane(document.clone(), cx);
            (document, id, pane)
        })
    };
    harness.open_tab(pane, script_document_id);
    harness.window.run_until_parked();
    for body in [failing_script, passing_script] {
        harness.window.update(|window, cx| {
            script_document.update(cx, |doc, cx| {
                doc.set_content(body, window, cx);
                doc.run_query(window, cx);
            });
        });
        harness.window.run_until_parked();
        harness.window.run_until_parked();
    }
    // The failing script is the negative control: a started-but-failed run
    // must never credit, so the final payload pins script_run at exactly 1.

    // -- producer 3: dashboard_open (explicit sidebar open) -------------------
    let dashboard_profile = {
        let app_state = harness.app_state.clone();
        harness.window.update(|_, cx| {
            app_state.update(cx, |state, _| {
                // The dashboard manager requires the profile row to exist.
                state.storage_runtime().dbflux_db().execute(
                        "INSERT OR IGNORE INTO cfg_connection_profiles (id, name) VALUES (?1, 'telemetry-stub')",
                        [query_profile.to_string()],
                    )
                    .expect("profile row");
                state
                    .dashboards
                    .create_dashboard(
                        "telemetry-composition".into(),
                        None,
                        query_profile,
                        None,
                        dbflux_components::SavedChartRefreshPolicy::Off,
                    )
                    .expect("persist dashboard")
            })
        })
    };
    // Negative control: an invalid dashboard id must not credit. The valid
    // open below must therefore be the single credit in the final payload.
    harness.window.update(|_, cx| {
        let sidebar = harness.workspace.read(cx).sidebar.clone();
        sidebar.update(cx, |_, cx| {
            cx.emit(dbflux_ui_sidebar::SidebarEvent::OpenDashboard {
                dashboard_id: Uuid::new_v4(),
            });
        });
    });
    harness.window.run_until_parked();
    harness.window.update(|_, cx| {
        let sidebar = harness.workspace.read(cx).sidebar.clone();
        sidebar.update(cx, |_, cx| {
            cx.emit(dbflux_ui_sidebar::SidebarEvent::OpenDashboard {
                dashboard_id: dashboard_profile,
            });
        });
    });
    harness.window.run_until_parked();

    // -- producer 4: schema_inspect (explicit schema-viz open) ----------------
    let schema_profile = harness.register_connection(Arc::new(SchemaStubConnection::new()));
    // Negative control: a table the loaded metadata does not contain must not
    // credit, so `schema_inspect` is pinned at exactly 1 by the final payload
    // assertion.
    harness.window.update(|window, cx| {
        harness.workspace.update(cx, |workspace, cx| {
            workspace.open_schema_viz_document(
                schema_profile,
                Some("app".to_string()),
                None,
                "not_in_schema".to_string(),
                window,
                cx,
            );
        });
    });
    harness.window.run_until_parked();
    harness.window.update(|window, cx| {
        harness.workspace.update(cx, |workspace, cx| {
            workspace.open_schema_viz_document(
                schema_profile,
                Some("app".to_string()),
                None,
                "users".to_string(),
                window,
                cx,
            );
        });
    });
    harness.window.run_until_parked();

    // -- producer 5: visual_query_builder (rail run through the real handler) -
    let builder_executes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let builder_stub = Arc::new(QueryStubConnection::counted(builder_executes.clone()));
    let query_stub_for_builder = builder_stub.clone();
    let builder_profile = harness.register_connection(builder_stub);
    let (data_document, data_document_id, pane) = {
        let app_state = harness.app_state.clone();
        let profile_id = builder_profile;
        harness.window.update(|window, cx| {
            let document = cx.new(|cx| {
                DataDocument::new_for_table(
                    profile_id,
                    TableRef::with_schema("public", "users"),
                    Some("app".to_string()),
                    app_state.clone(),
                    window,
                    cx,
                )
            });
            let id = document.read(cx).id();
            let pane = DataDocument::into_pane(document.clone(), cx);
            (document, id, pane)
        })
    };
    harness.open_tab(pane, data_document_id);
    harness.window.run_until_parked();

    // Open the rail the way the keyboard does: the table menu (`m`), its
    // Toolbar trigger (last entry), then the first submenu entry. With this
    // fixture the submenu is [Export, OpenBuilder, ...], so one MenuDown
    // lands on OpenBuilder.
    fn dispatch_data(
        harness: &mut Harness<'_>,
        document: &Entity<DataDocument>,
        cmd: Command,
    ) -> bool {
        let document = document.clone();
        harness.window.update(|window, cx| {
            document.update(cx, |doc, cx| doc.dispatch_command(cmd, window, cx))
        })
    }
    assert!(
        dispatch_data(&mut harness, &data_document, Command::OpenContextMenu),
        "table menu"
    );
    assert!(
        dispatch_data(&mut harness, &data_document, Command::MenuUp),
        "toolbar trigger"
    );
    assert!(
        dispatch_data(&mut harness, &data_document, Command::MenuSelect),
        "open submenu"
    );
    assert!(
        dispatch_data(&mut harness, &data_document, Command::MenuDown),
        "select open-builder"
    );
    assert!(
        dispatch_data(&mut harness, &data_document, Command::MenuSelect),
        "run open-builder action"
    );
    harness.window.run_until_parked();
    // Opening the rail alone must not credit; the final assertion pins
    // visual_query_builder at exactly 1, so a credit here would surface as 2.

    // Run from the keyboard exactly as the rail answers it: Ctrl+L moves the
    // keyboard into the rail, then the run command emits `RunRequested` and
    // the grid's real handler drives the select.
    assert!(
        dispatch_data(&mut harness, &data_document, Command::FocusRight),
        "enter rail"
    );
    // Prove which execution path the run takes: from here on a browse fails,
    // so only the structured SELECT can answer the run.
    query_stub_for_builder
        .fail_browses
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(
        dispatch_data(&mut harness, &data_document, Command::RunQuery),
        "builder run"
    );
    harness.window.run_until_parked();
    harness.window.run_until_parked();
    // The structured SELECT ran through the production path (a browse would
    // have failed the fixture above).
    assert!(
        builder_executes.load(std::sync::atomic::Ordering::SeqCst) >= 1,
        "builder run must execute the structured select"
    );

    // -- one prepare, one frozen Basic payload --------------------------------
    // The scheduler enqueues the per-build installation ping ahead of the
    // Basic snapshot and returns the first ready delivery, so this prepare
    // materializes the Basic payload while the ping is what it hands back.
    // Both deliveries are drained below.
    let basic_due = chrono::Utc::now() + chrono::Duration::days(8);
    let first = harness.run_one_prepare(basic_due);
    assert!(
        matches!(first, dbflux_app::telemetry::Preparation::Job(_)),
        "Basic must be due and produce a delivery job"
    );

    let payload = harness
        .frozen_basic_payload()
        .expect("one frozen basic payload");
    let payload_value: serde_json::Value =
        serde_json::from_str(&payload).expect("frozen payload JSON");
    assert_eq!(payload_value["event"], "basic");
    let features = payload_value["features"]
        .as_object()
        .expect("features object")
        .clone();
    // Each producer ran exactly once and every negative control must have
    // credited nothing, so the counts are pinned exactly, not as floors.
    let expected = [
        ("query_run", 1),
        ("visual_query_builder", 1),
        ("dashboard_open", 1),
        ("script_run", 1),
        ("schema_inspect", 1),
    ];
    assert_eq!(
        features.len(),
        expected.len(),
        "exactly the five driven counters, got {features:?}"
    );
    for (feature, expected_count) in expected {
        let count = features
            .get(feature)
            .unwrap_or_else(|| panic!("missing counter {feature} in {features:?}"))
            .as_u64()
            .expect("numeric counter");
        assert_eq!(
            count, expected_count,
            "{feature} credited {count}, expected exactly {expected_count}: {features:?}"
        );
    }
    assert!(
        !features.contains_key("mcp_tool"),
        "the sixth counter is added by real MCP IPC in a later unit, not here"
    );
    println!("frozen basic payload: {payload}");
    eprintln!("frozen basic payload: {payload}");

    // The exact frozen bytes are what a delivery would submit.
    assert_eq!(
        harness.frozen_basic_payload().as_deref(),
        Some(payload.as_str()),
        "frozen payload bytes are stable"
    );

    // -- drain pending deliveries until the Basic snapshot itself is sent -----
    // The first ready delivery is the per-build installation ping, so the
    // Basic is attempted on a later prepare. Every delivery goes through the
    // scheduler's own attempt/complete pair; the wire result and the durable
    // audit rows are asserted for the Basic's own job when a collector is
    // configured, and for the ping's failed attempt otherwise.
    let mut basic_attempted = false;
    let mut last_correlation = None;
    let mut pending_job = match first {
        dbflux_app::telemetry::Preparation::Job(job) => Some(job),
        _ => unreachable!("checked above"),
    };
    for _ in 0..4 {
        let job = match pending_job.take() {
            Some(job) => job,
            None => match harness.run_one_prepare(basic_due) {
                dbflux_app::telemetry::Preparation::Job(job) => job,
                _ => break,
            },
        };
        let is_basic = serde_json::from_str::<serde_json::Value>(job.payload())
            .ok()
            .and_then(|value| {
                value
                    .get("event")
                    .and_then(|event| event.as_str())
                    .map(str::to_owned)
            })
            .as_deref()
            == Some("basic");
        let correlation = job.correlation_id();
        let completion = job.attempt();
        if is_basic {
            if collector_configured {
                assert!(
                    matches!(completion.code(), "stored" | "duplicate"),
                    "configured collector must acknowledge the Basic snapshot, got {}",
                    completion.code()
                );
            } else {
                assert!(
                    completion.code() == "network",
                    "the refused loopback port must yield a transport failure, not a fabricated ACK, got {}",
                    completion.code()
                );
            }
        }
        let disposition = harness.window.update(|_, cx| {
            harness.app_state.update(cx, |app, _| {
                let scheduler = app.telemetry_mut().expect("scheduler");
                scheduler
                    .complete(completion, chrono::Utc::now())
                    .expect("completion applied")
            })
        });
        assert_eq!(
            disposition,
            dbflux_app::telemetry::CompletionDisposition::Applied,
            "every delivery this prepare produced must be consumable"
        );
        last_correlation = Some(correlation);
        if is_basic {
            basic_attempted = true;
            break;
        }
    }
    if collector_configured {
        assert!(
            basic_attempted,
            "with a collector the Basic snapshot must be the delivery that was attempted"
        );
    }
    let correlation = if basic_attempted {
        last_correlation.expect("the Basic attempt recorded its correlation")
    } else {
        // Without a collector the never-acknowledged install ping keeps winning
        // the retry order, so the assembled Basic stays queued. That is the
        // honest outcome here; the audit assertions below then describe the
        // ping's own failed attempt.
        assert!(
            harness.frozen_basic_payload().is_some(),
            "without a collector the assembled Basic must stay in the durable queue"
        );
        last_correlation.expect("at least one delivery must be attempted")
    };

    let app_state = harness.app_state.clone();
    let events = harness.window.update(|_, cx| {
        let service = app_state.read(cx).audit_service().clone();
        service
            .query_extended(&AuditQueryFilter {
                action: Some("telemetry.delivery".to_string()),
                correlation_id: Some(correlation.to_string()),
                ..AuditQueryFilter::default()
            })
            .expect("audit query")
    });
    let phases: Vec<(String, String)> = events
        .iter()
        .filter_map(|event| {
            let details =
                serde_json::from_str::<serde_json::Value>(event.details_json.as_deref()?).ok()?;
            Some((
                details["phase"].as_str()?.to_string(),
                event.outcome.clone()?,
            ))
        })
        .collect();
    assert!(
        phases.iter().any(|(phase, _)| phase == "attempt"),
        "durable attempt row missing: {phases:?}"
    );
    assert!(
        phases.iter().any(|(phase, _)| phase == "outcome"),
        "durable outcome row missing: {phases:?}"
    );
    if collector_configured {
        assert!(
            phases
                .iter()
                .any(|(phase, outcome)| phase == "outcome" && outcome == "success"),
            "acknowledged delivery must record a successful outcome: {phases:?}"
        );
    } else {
        assert!(
            phases
                .iter()
                .any(|(phase, outcome)| phase == "outcome" && outcome == "failure"),
            "refused loopback delivery must record a failed outcome: {phases:?}"
        );
    }
}
