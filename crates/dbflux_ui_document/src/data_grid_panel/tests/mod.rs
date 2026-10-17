mod coverage;
pub(crate) mod rail_keys;

use super::{DataGridEvent, DataGridPanel, DataSource, GridState, MutationIntent, MutationRun};
use dbflux_app::keymap::Command;
use dbflux_components::theme;
use dbflux_core::{
    AggFn, CollectionRef, ColumnKind, ColumnMeta, GroupByEntry, Pagination, Projection,
    QueryResult, SelectQuery, SourceTable, TableRef, VisualAggregateSpec, VisualQuerySpec,
};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{AppContext, TestAppContext, VisualTestContext};
use gpui_component::Root;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

fn isolated_test_app_state(cx: &mut TestAppContext) -> gpui::Entity<AppStateEntity> {
    cx.update(|cx| {
        cx.new(|_| {
            let storage_runtime = StorageRuntime::in_memory().expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage_runtime).expect("test storage setup")
        })
    })
}

fn zero_row_columns() -> Vec<ColumnMeta> {
    vec![
        ColumnMeta {
            name: "id".to_string(),
            type_name: "int4".to_string(),
            kind: ColumnKind::Unknown,
            nullable: false,
            is_primary_key: true,
        },
        ColumnMeta {
            name: "name".to_string(),
            type_name: "text".to_string(),
            kind: ColumnKind::Unknown,
            nullable: true,
            is_primary_key: false,
        },
    ]
}

fn zero_row_result() -> QueryResult {
    QueryResult::table(zero_row_columns(), Vec::new(), None, Duration::ZERO)
}

/// One-column `id` result: `rebuild_table` maps the panel's key columns onto
/// the result by name, so a panel without a matching column stays read-only
/// no matter what the cache says.
fn id_result() -> QueryResult {
    QueryResult::table(
        vec![key_column("id", true)],
        vec![vec![dbflux_core::Value::Int(1)]],
        None,
        Duration::ZERO,
    )
}

fn key_column(name: &str, is_primary_key: bool) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        type_name: "S".to_string(),
        kind: ColumnKind::Text,
        nullable: false,
        is_primary_key,
    }
}

fn init_test_runtime(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(theme::init);
    cx.update(|cx| {
        let host = cx.new(|_cx| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });
}

/// Connection stub for tests that only need a connected profile to hang
/// cached metadata off; it answers no query.
///
/// `dialect()` panics rather than returning a stand-in: the test-only
/// dialects live in `dbflux_core` behind `#[cfg(test)]`, so any path that
/// reaches a dialect — `new_for_table` does, through `refresh` and
/// `run_query` — panics instead of failing an assertion. Tests here drive the
/// panel through `new_internal` and call the method under test directly.
struct StubConnection;

impl dbflux_core::Connection for StubConnection {
    /// Facets as a driver of the stub's category declares them.
    fn collection_facets(
        &self,
        _collection: &dbflux_core::CollectionRef,
    ) -> Vec<dbflux_core::ObjectFacet> {
        dbflux_core::category_collection_facets(self.metadata().category)
    }

    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        use dbflux_core::{
            DatabaseCategory, DriverCapabilities, DriverMetadata, Icon as CoreIcon, QueryLanguage,
            TransferFamily,
        };

        static META: std::sync::OnceLock<DriverMetadata> = std::sync::OnceLock::new();
        META.get_or_init(|| DriverMetadata {
            id: "stub".to_string(),
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

    #[expect(
        clippy::unimplemented,
        reason = "test-only stub connection: no test path in this suite calls the dialect"
    )]
    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        unimplemented!()
    }

    fn ping(&self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn execute(
        &self,
        _: &dbflux_core::QueryRequest,
    ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
        Err(dbflux_core::DbError::NotSupported("stub".to_string()))
    }

    fn cancel(&self, _: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }
}

fn make_grouped_spec() -> VisualQuerySpec {
    use dbflux_core::{AggFn, GroupByEntry, VisualAggregateSpec};

    VisualQuerySpec {
        source: SourceTable {
            schema: Some("public".to_string()),
            table: "orders".to_string(),
            alias: "orders".to_string(),
        },
        projection: Projection::Explicit(vec![]),
        joins: vec![],
        filter: None,
        group_by: vec![GroupByEntry {
            source_alias: "orders".to_string(),
            column: "country".to_string(),
        }],
        aggregates: vec![VisualAggregateSpec {
            function: AggFn::Sum,
            source_alias: Some("orders".to_string()),
            column: Some("amount".to_string()),
            alias: "total".to_string(),
        }],
        having: None,
        sort: vec![],
        limit: Some(100),
        offset: 0,
    }
}

/// Tests the `mutations_enabled` predicate directly without constructing
/// a full panel (avoiding the GPUI window requirement).
///
/// The predicate is: `!is_grouped(spec) && !pk_columns.is_empty()`.
/// This helper mirrors the logic in `DataGridPanel::mutations_enabled`.
fn mutations_enabled_predicate(spec: Option<&VisualQuerySpec>, pk_columns: &[String]) -> bool {
    if spec.map(|s| s.is_grouped()).unwrap_or(false) {
        return false;
    }
    !pk_columns.is_empty()
}

/// A three-row query result hosted under the app keymap, with the
/// table focused. `app_state` decides where a Save As goes.
fn host_result_grid(
    cx: &mut TestAppContext,
    app_state: gpui::Entity<AppStateEntity>,
) -> (
    gpui::Entity<crate::keyboard_test_support::KeymapHost<DataGridPanel>>,
    gpui::Entity<DataGridPanel>,
    &mut VisualTestContext,
) {
    use crate::keyboard_test_support::host_document;

    let (host, window) = host_document(
        cx,
        move |window, cx| {
            cx.new(|cx| {
                DataGridPanel::new_for_result(
                    Arc::new(QueryResult::table(
                        vec![key_column("id", false), key_column("name", false)],
                        (1..=3)
                            .map(|id| {
                                vec![
                                    dbflux_core::Value::Int(id),
                                    dbflux_core::Value::Text(format!("row {id}")),
                                ]
                            })
                            .collect(),
                        None,
                        Duration::ZERO,
                    )),
                    "SELECT id, name FROM users".to_string(),
                    None,
                    app_state,
                    window,
                    cx,
                )
            })
        },
        |panel, cx| panel.active_context(cx),
        DataGridPanel::dispatch_command,
    );
    let panel = window.update(|_, cx| host.read(cx).document.clone());

    window.update(|window, cx| {
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

    (host, panel, window)
}

/// Stands in for the workspace around a grid: the root carries the key
/// context the grid reports and routes keymap commands to it, and the
/// side panel the grid opens is drawn beside it, as the inspector rail
/// draws it, so focus can move into it.
struct RailHost {
    panel: gpui::Entity<DataGridPanel>,
    rail: Option<gpui::AnyView>,
    _subscription: gpui::Subscription,
}

impl gpui::Render for RailHost {
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        cx: &mut gpui::Context<Self>,
    ) -> impl gpui::IntoElement {
        use dbflux_ui_base::keymap::{
            RunCommand, WORKSPACE_KEY_CONTEXT, root_key_context, run_command,
        };
        use gpui::{InteractiveElement as _, ParentElement as _, Styled as _};

        let context = self.panel.read(cx).active_context(cx);

        gpui::div()
            .size_full()
            .flex()
            .key_context(root_key_context(WORKSPACE_KEY_CONTEXT, context, &[]))
            .on_action(cx.listener(|this, action: &RunCommand, window, cx| {
                let Some(command) = run_command(action) else {
                    return;
                };

                let handled = this
                    .panel
                    .update(cx, |panel, cx| panel.dispatch_command(command, window, cx));

                if !handled {
                    cx.propagate();
                }
            }))
            .child(gpui::div().flex_1().child(self.panel.clone()))
            .children(self.rail.clone())
    }
}

/// A three-row query result hosted beside its inspector rail, under the
/// app keymap, with the table focused.
fn host_grid_with_rail(
    cx: &mut TestAppContext,
    app_state: gpui::Entity<AppStateEntity>,
) -> (gpui::Entity<DataGridPanel>, &mut VisualTestContext) {
    let slot: Rc<RefCell<Option<gpui::Entity<DataGridPanel>>>> = Rc::default();

    let (_, window) = cx.add_window_view({
        let slot = slot.clone();
        move |window, cx| {
            let panel = cx.new(|cx| {
                DataGridPanel::new_for_result(
                    Arc::new(QueryResult::table(
                        vec![key_column("id", false), key_column("name", false)],
                        (1..=3)
                            .map(|id| {
                                vec![
                                    dbflux_core::Value::Int(id),
                                    dbflux_core::Value::Text(format!("row {id}")),
                                ]
                            })
                            .collect(),
                        None,
                        Duration::ZERO,
                    )),
                    "SELECT id, name FROM users".to_string(),
                    None,
                    app_state,
                    window,
                    cx,
                )
            });
            slot.replace(Some(panel.clone()));

            let host = cx.new(|cx| {
                let subscription = cx.subscribe(
                    &panel,
                    |host: &mut RailHost, _, event: &DataGridEvent, cx| match event {
                        DataGridEvent::OpenInspector { content, .. } => {
                            host.rail = Some(content.clone());
                            cx.notify();
                        }
                        DataGridEvent::CloseInspector => {
                            host.rail = None;
                            cx.notify();
                        }
                        _ => {}
                    },
                );

                RailHost {
                    panel: panel.clone(),
                    rail: None,
                    _subscription: subscription,
                }
            });

            Root::new(host, window, cx)
        }
    });
    window.run_until_parked();

    let panel = slot
        .borrow()
        .clone()
        .expect("the window builder stores the grid");
    window.update(|window, cx| {
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

    (panel, window)
}

/// Whether keyboard focus is on the grid: its table or the panel itself.
fn table_has_focus(panel: &gpui::Entity<DataGridPanel>, window: &mut VisualTestContext) -> bool {
    window.update(|window, cx| {
        panel
            .read(cx)
            .grid_table
            .table_state
            .as_ref()
            .is_some_and(|state| state.read(cx).focus_handle().is_focused(window))
            || panel.read(cx).focus_handle.is_focused(window)
    })
}

fn active_row(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> Option<usize> {
    window.update(|_, cx| {
        panel
            .read(cx)
            .grid_table
            .table_state
            .as_ref()
            .and_then(|state| state.read(cx).selection().active)
            .map(|coord| coord.row)
    })
}

/// Opens the table menu with `m` and runs the Toolbar submenu's `action`.
fn run_toolbar_entry(
    panel: &gpui::Entity<DataGridPanel>,
    action: super::context_menu::toolbar::ToolbarAction,
    window: &mut VisualTestContext,
) {
    use dbflux_app::keymap::ContextId;

    window.simulate_keystrokes("m");
    window.run_until_parked();
    assert_eq!(
        window.update(|_, cx| panel.read(cx).active_context(cx)),
        ContextId::ContextMenu,
        "`m` opens the table menu"
    );

    let index = window
        .update(|_, cx| panel.read(cx).toolbar_actions(cx))
        .iter()
        .position(|listed| *listed == action)
        .unwrap_or_else(|| panic!("the Toolbar submenu lists {action:?}"));

    let mut keys = vec!["k", "l"];
    keys.extend(std::iter::repeat_n("j", index));
    keys.push("enter");
    for key in keys {
        window.simulate_keystrokes(key);
        window.run_until_parked();
    }
}

fn make_test_spec() -> VisualQuerySpec {
    VisualQuerySpec {
        source: SourceTable {
            schema: Some("public".to_string()),
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
    }
}

/// Minimal `Connection` stub whose `metadata()` is driven entirely by the
/// category / query-language / capabilities passed at construction, so a
/// single stub serves every `can_open_builder` case without per-driver code.
struct StubBuilderConnection {
    metadata: dbflux_core::DriverMetadata,
}

impl dbflux_core::Connection for StubBuilderConnection {
    /// Facets as a driver of the stub's category declares them.
    fn collection_facets(
        &self,
        _collection: &dbflux_core::CollectionRef,
    ) -> Vec<dbflux_core::ObjectFacet> {
        dbflux_core::category_collection_facets(self.metadata().category)
    }

    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        &self.metadata
    }

    fn kind(&self) -> dbflux_core::DbKind {
        dbflux_core::DbKind::SQLite
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        dbflux_core::SchemaLoadingStrategy::SingleDatabase
    }

    #[expect(
        clippy::unimplemented,
        reason = "test-only stub connection: no test path in this suite calls the dialect"
    )]
    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        unimplemented!("StubBuilderConnection::dialect not needed for this test")
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
        Err(dbflux_core::DbError::NotSupported("stub".to_string()))
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }
}

/// Registers a `StubBuilderConnection` with the given capability metadata and
/// returns the app state plus the registered profile id.
fn register_builder_stub_connection(
    cx: &mut TestAppContext,
    category: dbflux_core::DatabaseCategory,
    query_language: dbflux_core::QueryLanguage,
    query: Option<dbflux_core::QueryCapabilities>,
) -> (gpui::Entity<AppStateEntity>, Uuid) {
    let metadata = dbflux_core::DriverMetadata {
        id: "stub-builder".to_string(),
        display_name: "Stub".to_string(),
        description: "test stub".to_string(),
        category,
        transfer_family: dbflux_core::TransferFamily::Incompatible,
        deployment_class: None,
        query_language,
        capabilities: dbflux_core::DriverCapabilities::empty(),
        default_port: None,
        uri_scheme: "stub".to_string(),
        icon: dbflux_core::Icon::Database,
        syntax: None,
        query,
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
    };

    register_stub_connection(cx, Arc::new(StubBuilderConnection { metadata }))
}

/// Registers `connection` under a new profile in an isolated app state and
/// returns the app state plus the profile id.
pub(super) fn register_stub_connection(
    cx: &mut TestAppContext,
    connection: Arc<dyn dbflux_core::Connection>,
) -> (gpui::Entity<AppStateEntity>, Uuid) {
    init_test_runtime(cx);

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
                "test",
                dbflux_core::DbConfig::SQLite {
                    path: std::path::PathBuf::from(":memory:"),
                    connection_id: None,
                },
            );
            let connected = dbflux_core::ConnectedProfile {
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
            };
            app.connections_mut().insert(profile_id, connected);
        });
    });

    (app_state, profile_id)
}

/// Builds a `DataGridPanel` over a `DataSource::Collection` for the given
/// profile and returns the panel entity.
fn build_panel_for_collection(
    cx: &mut TestAppContext,
    app_state: gpui::Entity<AppStateEntity>,
    profile_id: Uuid,
) -> gpui::Entity<DataGridPanel> {
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, _window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Collection {
                profile_id,
                collection: CollectionRef::new("db", "items"),
                pagination: Pagination::default(),
                total_docs: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    panel_holder
        .borrow()
        .clone()
        .expect("panel should be created")
}

// =========================================================================
// Reload keeps the grid state (#620)
// =========================================================================

use dbflux_components::components::data_table::SortState as TableSortState;
use dbflux_components::components::data_table::selection::CellCoord;
mod builder_binding;
mod builder_draft_and_fk;
mod builder_mutations;
mod close_completion;
mod collection_views;
mod facets;
mod reload_pending_edits;
mod reload_state;
mod sources_and_keys;
mod statement_outcome;

fn reload_columns(names: &[&str]) -> Vec<ColumnMeta> {
    names
        .iter()
        .map(|name| ColumnMeta {
            name: (*name).to_string(),
            type_name: "text".to_string(),
            kind: ColumnKind::Text,
            nullable: true,
            is_primary_key: *name == "id",
        })
        .collect()
}

fn reload_result(names: &[&str], row_count: usize) -> QueryResult {
    let rows = (0..row_count)
        .map(|_| {
            (0..names.len())
                .map(|_| dbflux_core::Value::Text("v".to_string()))
                .collect()
        })
        .collect();

    QueryResult::table(reload_columns(names), rows, None, Duration::ZERO)
}

/// Widen `name` and put the cursor on it, then hand back the state so the
/// caller can reload and check what survived.
fn widen_name_and_select(panel: &mut DataGridPanel, cx: &mut gpui::Context<DataGridPanel>) {
    let table_state = panel
        .grid_table
        .table_state
        .clone()
        .expect("table state must exist after the first result");

    table_state.update(cx, |state, cx| {
        state.set_column_width(1, 260.0, cx);
        state.select_cell(CellCoord::new(1, 1), cx);
    });
}

fn name_width(panel: &DataGridPanel, cx: &gpui::App) -> f32 {
    panel
        .grid_table
        .table_state
        .as_ref()
        .expect("table state must exist")
        .read(cx)
        .column_widths()[1]
}

fn table_panel(
    window: &mut gpui::VisualTestContext,
    app_state: gpui::Entity<AppStateEntity>,
) -> gpui::Entity<DataGridPanel> {
    window.update(|window, app| {
        let source = DataSource::Table {
            profile_id: Uuid::nil(),
            database: Some("app".to_string()),
            table: TableRef::with_schema("public", "users"),
            pagination: Pagination::default(),
            order_by: Vec::new(),
            total_rows: Some(2),
        };

        let panel = app.new(|cx| {
            DataGridPanel::new_internal(source, app_state, vec!["id".to_string()], window, cx)
        });
        panel.update(app, |panel, cx| {
            panel.set_result(reload_result(&["id", "name", "email"], 2), cx);
        });
        panel
    })
}

fn last_toast_title(window: &mut gpui::VisualTestContext) -> Option<String> {
    window.update(|_, app| {
        app.global::<ToastGlobal>()
            .host
            .read(app)
            .last_toast_title()
    })
}

/// A page of two products with a nested `price` object and an `items`
/// array, the second product lacking `price.currency`.
fn nested_document_rows() -> QueryResult {
    use dbflux_core::Value;
    use std::collections::BTreeMap;

    let column = |name: &str, is_primary_key: bool| ColumnMeta {
        name: name.to_string(),
        type_name: "BSON".to_string(),
        kind: ColumnKind::Unknown,
        nullable: true,
        is_primary_key,
    };

    let price = |currency: Option<&str>| {
        let mut fields = BTreeMap::new();
        fields.insert("amount".to_string(), Value::Decimal("405.00".into()));
        if let Some(currency) = currency {
            fields.insert("currency".to_string(), Value::Text(currency.into()));
        }
        Value::Document(fields)
    };

    let items = Value::Array(vec![
        Value::Document(BTreeMap::from([(
            "sku".to_string(),
            Value::Text("a".into()),
        )])),
        Value::Document(BTreeMap::from([(
            "sku".to_string(),
            Value::Text("b".into()),
        )])),
    ]);

    QueryResult::json(
        vec![
            column("_id", true),
            column("price", false),
            column("items", false),
        ],
        vec![
            vec![Value::Int(1), price(Some("USD")), items.clone()],
            vec![Value::Int(2), price(None), items],
        ],
        Duration::ZERO,
    )
}

/// `id` / `name` / `email` result whose rows carry `ids` in the key column.
fn keyed_result(ids: &[&str]) -> QueryResult {
    let rows = ids
        .iter()
        .map(|id| {
            vec![
                dbflux_core::Value::Text((*id).to_string()),
                dbflux_core::Value::Text("name".to_string()),
                dbflux_core::Value::Text("email".to_string()),
            ]
        })
        .collect();

    QueryResult::table(
        reload_columns(&["id", "name", "email"]),
        rows,
        None,
        Duration::ZERO,
    )
}
