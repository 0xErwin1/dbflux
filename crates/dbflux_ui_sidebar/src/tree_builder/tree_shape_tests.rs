//! Pins the connection subtree the sidebar builds for every schema paradigm:
//! item ids, labels, order, expansion and placeholder rows. Each fixture is
//! rendered to text and compared with a snapshot under `tree_shape_snapshots/`.

use super::Sidebar;
use dbflux_core::{
    CollectionChildInfo, CollectionIndexInfo, CollectionInfo, CollectionPresentation,
    ConnectedProfile, DatabaseCategory, DatabaseConnection, DatabaseInfo, DbSchemaInfo,
    DocumentSchema, DriverCapabilities, FieldInfo, IndexData, IndexDirection, IndexInfo,
    KeySpaceInfo, KeyValueSchema, MeasurementInfo, NavigatorView, RelationalSchema,
    SchemaLoadingStrategy, SchemaSnapshot, TableInfo, TimeSeriesSchema, ViewInfo,
};
use dbflux_ui_base::app_state_entity::AppStateEntity;
use gpui_component::tree::TreeItem;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

const PROFILE_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0001);

/// A connection whose category, capabilities and loading strategy the
/// fixture chooses. Its collections carry the facets an RPC driver of that
/// category gets.
pub(crate) struct ShapeConnection {
    metadata: dbflux_core::DriverMetadata,
    strategy: SchemaLoadingStrategy,
}

impl ShapeConnection {
    fn new(
        category: DatabaseCategory,
        capabilities: DriverCapabilities,
        strategy: SchemaLoadingStrategy,
    ) -> Self {
        Self {
            metadata: dbflux_core::DriverMetadata {
                id: "shape".to_string(),
                display_name: "Shape".to_string(),
                description: "tree shape fixture".to_string(),
                category,
                transfer_family: dbflux_core::TransferFamily::Sql,
                deployment_class: None,
                query_language: dbflux_core::QueryLanguage::Sql,
                capabilities,
                default_port: None,
                uri_scheme: "shape".to_string(),
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
            strategy,
        }
    }
}

impl dbflux_core::Connection for ShapeConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        &self.metadata
    }

    fn collection_facets(
        &self,
        _collection: &dbflux_core::CollectionRef,
    ) -> Vec<dbflux_core::ObjectFacet> {
        dbflux_core::category_collection_facets(self.metadata.category)
    }

    fn ping(&self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn execute(
        &self,
        _request: &dbflux_core::QueryRequest,
    ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
        Err(dbflux_core::DbError::NotSupported(
            "tree shape fixture".to_string(),
        ))
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, dbflux_core::DbError> {
        Ok(SchemaSnapshot::default())
    }

    fn kind(&self) -> dbflux_core::DbKind {
        dbflux_core::DbKind::SQLite
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        self.strategy
    }

    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }
}

pub(crate) fn connected_profile(
    category: DatabaseCategory,
    capabilities: DriverCapabilities,
    strategy: SchemaLoadingStrategy,
    schema: SchemaSnapshot,
) -> ConnectedProfile {
    let mut profile = dbflux_core::ConnectionProfile::new(
        "shape",
        dbflux_core::DbConfig::SQLite {
            path: std::path::PathBuf::from(":memory:"),
            connection_id: None,
        },
    );
    profile.id = PROFILE_ID;

    ConnectedProfile {
        profile,
        connection: Arc::new(ShapeConnection::new(category, capabilities, strategy)),
        schema: Some(schema),
        mutation_policy: dbflux_core::MutationPolicy::default(),
        read_only_reason: None,
        database_schemas: HashMap::new(),
        table_details: HashMap::new(),
        collection_children: HashMap::new(),
        schema_types: HashMap::new(),
        schema_columns: HashMap::new(),
        schema_indexes: HashMap::new(),
        schema_foreign_keys: HashMap::new(),
        schema_routines: HashMap::new(),
        dependents_cache: HashMap::new(),
        active_database: None,
        redis_key_cache: dbflux_core::RedisKeyCache::default(),
        database_connections: HashMap::new(),
        proxy_tunnel: None,
    }
}

fn empty_state() -> AppStateEntity {
    AppStateEntity::new_with_storage_runtime(
        dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("test storage"),
    )
    .expect("test app state")
}

/// Everything besides the connected profile that feeds the tree builder.
#[derive(Default)]
struct Surroundings {
    pending_databases: Vec<&'static str>,
    metric_fetch_errors: HashMap<String, String>,
    bucket_cache: HashMap<Uuid, Vec<dbflux_core::BucketInfo>>,
}

fn render_profile(connected: ConnectedProfile, surroundings: Surroundings) -> String {
    let profile = connected.profile.clone();
    let mut state = empty_state();
    state.connections_mut().insert(PROFILE_ID, connected);
    state.set_active_connection(PROFILE_ID);

    for database in &surroundings.pending_databases {
        state.start_pending_operation(PROFILE_ID, Some(database));
    }

    let item = Sidebar::build_profile_item_with_errors(
        &profile,
        &state,
        &surroundings.metric_fetch_errors,
        &HashMap::new(),
        &HashMap::new(),
        &surroundings.bucket_cache,
    );

    let mut text = String::new();
    write_item(&item, 0, &mut text);
    text
}

/// One line per item: indentation for depth, `+` when expanded, the id, and
/// the label in quotes.
fn write_item(item: &TreeItem, depth: usize, text: &mut String) {
    let marker = if item.is_expanded() { '+' } else { '-' };
    let disabled = if item.is_disabled() {
        " (disabled)"
    } else {
        ""
    };
    text.push_str(&format!(
        "{}{marker} {} \"{}\"{disabled}\n",
        "  ".repeat(depth),
        item.id,
        item.label
    ));

    for child in &item.children {
        write_item(child, depth + 1, text);
    }
}

/// Compares a rendered tree with `tree_shape_snapshots/<name>.txt`.
macro_rules! assert_shape {
    ($actual:expr, $name:literal $(,)?) => {{
        let actual: &str = &$actual;
        let expected = include_str!(concat!("tree_shape_snapshots/", $name, ".txt"));
        assert!(
            actual == expected,
            "tree shape `{}` changed\n--- expected\n{expected}\n--- actual\n{actual}",
            $name
        );
    }};
}

fn database(name: &str, is_current: bool) -> DatabaseInfo {
    DatabaseInfo {
        name: name.to_string(),
        is_current,
    }
}

fn table(name: &str, schema: Option<&str>) -> TableInfo {
    TableInfo {
        name: name.to_string(),
        schema: schema.map(str::to_string),
        columns: None,
        indexes: None,
        foreign_keys: None,
        constraints: None,
        sample_fields: None,
        presentation: CollectionPresentation::DataGrid,
        child_items: None,
        storage_hints: None,
        pseudo_columns: Box::default(),
    }
}

fn view(name: &str, schema: Option<&str>) -> ViewInfo {
    ViewInfo {
        name: name.to_string(),
        schema: schema.map(str::to_string),
    }
}

fn schema_group(name: &str, tables: Vec<TableInfo>, views: Vec<ViewInfo>) -> DbSchemaInfo {
    DbSchemaInfo {
        name: name.to_string(),
        tables,
        views,
        custom_types: None,
    }
}

fn field(name: &str, common_type: &str) -> FieldInfo {
    FieldInfo {
        name: name.to_string(),
        common_type: common_type.to_string(),
        occurrence_rate: Some(1.0),
        nested_fields: None,
    }
}

fn document_index(name: &str, key: &str, is_unique: bool) -> CollectionIndexInfo {
    CollectionIndexInfo {
        name: name.to_string(),
        keys: vec![(key.to_string(), IndexDirection::Ascending)],
        is_unique,
        is_sparse: false,
        expire_after_seconds: None,
    }
}

fn collection(name: &str, database: Option<&str>) -> CollectionInfo {
    CollectionInfo {
        name: name.to_string(),
        database: database.map(str::to_string),
        document_count: None,
        avg_document_size: None,
        sample_fields: None,
        indexes: None,
        validator: None,
        is_capped: false,
        presentation: CollectionPresentation::DataGrid,
        child_items: None,
    }
}

fn event_stream_collection(name: &str) -> CollectionInfo {
    CollectionInfo {
        presentation: CollectionPresentation::EventStream,
        child_items: Some(vec![CollectionChildInfo {
            id: format!("{name}/stream-1"),
            label: "stream-1".to_string(),
            last_event_ts_ms: None,
            presentation: CollectionPresentation::EventStream,
        }]),
        ..collection(name, None)
    }
}

fn measurement(name: &str) -> MeasurementInfo {
    MeasurementInfo {
        name: name.to_string(),
        tags: vec!["host".to_string()],
        fields: Vec::new(),
    }
}

/// A loaded collection as a lazily fetched document database lists it.
fn loaded_collection(name: &str, database: &str) -> TableInfo {
    TableInfo {
        sample_fields: Some(vec![field("_id", "objectId"), field("total", "double")]),
        indexes: Some(IndexData::Document(vec![
            document_index("_id_", "_id", true),
            document_index("total_1", "total", false),
        ])),
        ..table(name, Some(database))
    }
}

fn relational_server_schema() -> SchemaSnapshot {
    SchemaSnapshot::relational(RelationalSchema {
        databases: vec![database("app", true), database("postgres", false)],
        current_database: Some("app".to_string()),
        schemas: vec![
            schema_group(
                "analytics",
                vec![table("events", Some("analytics"))],
                Vec::new(),
            ),
            schema_group(
                "public",
                vec![
                    table("users", Some("public")),
                    table("orders", Some("public")),
                ],
                vec![view("active_users", Some("public"))],
            ),
        ],
        tables: Vec::new(),
        views: Vec::new(),
    })
}

fn render_relational_server(navigator_view: NavigatorView) -> String {
    let mut connected = connected_profile(
        DatabaseCategory::Relational,
        DriverCapabilities::SCHEMAS | DriverCapabilities::ROUTINES,
        SchemaLoadingStrategy::ConnectionPerDatabase,
        relational_server_schema(),
    );
    connected.profile.navigator_view = navigator_view;
    connected.active_database = Some("app".to_string());

    render_profile(connected, Surroundings::default())
}

#[test]
fn relational_server_with_schemas_keeps_its_tree() {
    assert_shape!(
        render_relational_server(NavigatorView::Advanced),
        "relational_schemas_advanced"
    );
    assert_shape!(
        render_relational_server(NavigatorView::Compact),
        "relational_schemas_compact"
    );
    assert_shape!(
        render_relational_server(NavigatorView::Simple),
        "relational_schemas_simple"
    );
}

#[test]
fn relational_per_database_connections_keep_their_tree() {
    let mut connected = connected_profile(
        DatabaseCategory::Relational,
        DriverCapabilities::SCHEMAS,
        SchemaLoadingStrategy::ConnectionPerDatabase,
        relational_server_schema(),
    );
    connected.active_database = Some("app".to_string());
    connected.database_connections.insert(
        "postgres".to_string(),
        DatabaseConnection {
            connection: connected.connection.clone(),
            schema: Some(SchemaSnapshot::relational(RelationalSchema {
                databases: vec![database("postgres", true)],
                current_database: Some("postgres".to_string()),
                schemas: vec![schema_group(
                    "public",
                    vec![table("settings", Some("public"))],
                    Vec::new(),
                )],
                tables: Vec::new(),
                views: Vec::new(),
            })),
        },
    );

    assert_shape!(
        &render_profile(connected, Surroundings::default()),
        "relational_per_database",
    );
}

#[test]
fn relational_flat_file_database_keeps_its_tree() {
    let connected = connected_profile(
        DatabaseCategory::Relational,
        DriverCapabilities::empty(),
        SchemaLoadingStrategy::SingleDatabase,
        SchemaSnapshot::relational(RelationalSchema {
            databases: Vec::new(),
            current_database: None,
            schemas: Vec::new(),
            tables: vec![table("notes", None), table("tags", None)],
            views: vec![view("recent_notes", None)],
        }),
    );

    assert_shape!(
        &render_profile(connected, Surroundings::default()),
        "relational_flat",
    );
}

#[test]
fn relational_lazy_databases_keep_loaded_and_pending_rows() {
    let mut connected = connected_profile(
        DatabaseCategory::Relational,
        DriverCapabilities::empty(),
        SchemaLoadingStrategy::LazyPerDatabase,
        SchemaSnapshot::relational(RelationalSchema {
            databases: vec![
                database("shop", true),
                database("audit", false),
                database("cold", false),
            ],
            current_database: Some("shop".to_string()),
            schemas: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
        }),
    );
    connected.active_database = Some("shop".to_string());
    connected.database_schemas.insert(
        "shop".to_string(),
        schema_group(
            "shop",
            vec![table("carts", Some("shop")), table("items", Some("shop"))],
            vec![view("open_carts", Some("shop"))],
        ),
    );

    assert_shape!(
        &render_profile(
            connected,
            Surroundings {
                pending_databases: vec!["audit"],
                ..Surroundings::default()
            },
        ),
        "relational_lazy",
    );
}

fn document_server_schema() -> SchemaSnapshot {
    SchemaSnapshot::document(DocumentSchema {
        databases: vec![
            database("shop", true),
            database("logs", false),
            database("cold", false),
        ],
        current_database: Some("shop".to_string()),
        collections: Vec::new(),
    })
}

fn render_lazy_document_server(navigator_view: NavigatorView) -> String {
    let mut connected = connected_profile(
        DatabaseCategory::Document,
        DriverCapabilities::empty(),
        SchemaLoadingStrategy::LazyPerDatabase,
        document_server_schema(),
    );
    connected.profile.navigator_view = navigator_view;
    connected.active_database = Some("shop".to_string());
    connected.database_schemas.insert(
        "shop".to_string(),
        schema_group(
            "shop",
            vec![
                loaded_collection("orders", "shop"),
                table("customers", Some("shop")),
            ],
            Vec::new(),
        ),
    );

    render_profile(
        connected,
        Surroundings {
            pending_databases: vec!["logs"],
            ..Surroundings::default()
        },
    )
}

#[test]
fn document_lazy_databases_keep_collections_indexes_and_pending_rows() {
    assert_shape!(
        render_lazy_document_server(NavigatorView::Advanced),
        "document_lazy_advanced"
    );
    assert_shape!(
        render_lazy_document_server(NavigatorView::Compact),
        "document_lazy_compact"
    );
}

#[test]
fn document_current_database_before_lazy_load_shows_its_pending_row() {
    let mut connected = connected_profile(
        DatabaseCategory::Document,
        DriverCapabilities::empty(),
        SchemaLoadingStrategy::LazyPerDatabase,
        document_server_schema(),
    );
    connected.active_database = Some("shop".to_string());

    assert_shape!(
        &render_profile(
            connected,
            Surroundings {
                pending_databases: vec!["shop"],
                ..Surroundings::default()
            },
        ),
        "document_lazy_pending_current",
    );
}

#[test]
fn document_single_database_lists_snapshot_collections() {
    let mut users = collection("users", Some("default"));
    users.sample_fields = Some(vec![field("pk", "string")]);
    users.indexes = Some(vec![document_index("by_email", "email", true)]);

    let connected = connected_profile(
        DatabaseCategory::Document,
        DriverCapabilities::empty(),
        SchemaLoadingStrategy::SingleDatabase,
        SchemaSnapshot::document(DocumentSchema {
            databases: vec![database("default", true)],
            current_database: Some("default".to_string()),
            collections: vec![
                users,
                collection("sessions", None),
                collection("elsewhere", Some("other")),
            ],
        }),
    );

    assert_shape!(
        &render_profile(connected, Surroundings::default()),
        "document_single",
    );
}

#[test]
fn document_database_connection_slot_lists_nothing() {
    let schema = SchemaSnapshot::document(DocumentSchema {
        databases: vec![database("main", true), database("replica", false)],
        current_database: Some("main".to_string()),
        collections: vec![collection("users", Some("main"))],
    });
    let mut connected = connected_profile(
        DatabaseCategory::Document,
        DriverCapabilities::empty(),
        SchemaLoadingStrategy::ConnectionPerDatabase,
        schema.clone(),
    );
    connected.database_connections.insert(
        "replica".to_string(),
        DatabaseConnection {
            connection: connected.connection.clone(),
            schema: Some(schema),
        },
    );

    assert_shape!(
        &render_profile(connected, Surroundings::default()),
        "document_per_database",
    );
}

fn log_stream_profile() -> ConnectedProfile {
    connected_profile(
        DatabaseCategory::LogStream,
        DriverCapabilities::METRIC_CATALOG,
        SchemaLoadingStrategy::SingleDatabase,
        SchemaSnapshot::document(DocumentSchema {
            databases: vec![database("default", true)],
            current_database: Some("default".to_string()),
            collections: vec![
                event_stream_collection("/aws/lambda/api"),
                event_stream_collection("/aws/lambda/worker"),
            ],
        }),
    )
}

#[test]
fn log_stream_lists_event_stream_leaves_and_the_metrics_folder() {
    assert_shape!(
        &render_profile(log_stream_profile(), Surroundings::default()),
        "log_stream",
    );
}

#[test]
fn log_stream_metrics_folder_shows_its_fetch_error() {
    let metrics_folder = dbflux_core::SchemaNodeId::MetricsFolder {
        profile_id: PROFILE_ID,
        database: "default".to_string(),
    }
    .to_string();

    assert_shape!(
        &render_profile(
            log_stream_profile(),
            Surroundings {
                metric_fetch_errors: HashMap::from([(metrics_folder, "throttled".to_string())]),
                ..Surroundings::default()
            },
        ),
        "log_stream_metrics_error",
    );
}

fn time_series_schema() -> SchemaSnapshot {
    SchemaSnapshot::time_series(TimeSeriesSchema {
        databases: vec![
            database("monitoring", true),
            database("metrics", false),
            database("archive", false),
            database("cold", false),
        ],
        current_database: Some("monitoring".to_string()),
        measurements: vec![measurement("cpu"), measurement("mem")],
        retention_policies: Vec::new(),
    })
}

#[test]
fn time_series_lazy_buckets_keep_measurements_and_pending_rows() {
    let mut connected = connected_profile(
        DatabaseCategory::TimeSeries,
        DriverCapabilities::empty(),
        SchemaLoadingStrategy::LazyPerDatabase,
        time_series_schema(),
    );
    connected.active_database = Some("monitoring".to_string());
    connected.database_schemas.insert(
        "metrics".to_string(),
        schema_group("metrics", vec![table("disk", Some("metrics"))], Vec::new()),
    );

    assert_shape!(
        &render_profile(
            connected,
            Surroundings {
                pending_databases: vec!["archive"],
                ..Surroundings::default()
            },
        ),
        "time_series_lazy",
    );
}

#[test]
fn time_series_single_database_lists_snapshot_measurements() {
    let connected = connected_profile(
        DatabaseCategory::TimeSeries,
        DriverCapabilities::empty(),
        SchemaLoadingStrategy::SingleDatabase,
        SchemaSnapshot::time_series(TimeSeriesSchema {
            databases: vec![database("telegraf", true)],
            current_database: Some("telegraf".to_string()),
            measurements: vec![measurement("cpu"), measurement("net")],
            retention_policies: Vec::new(),
        }),
    );

    assert_shape!(
        &render_profile(connected, Surroundings::default()),
        "time_series_single",
    );
}

#[test]
fn time_series_database_connection_slots_list_their_own_measurements() {
    let mut connected = connected_profile(
        DatabaseCategory::TimeSeries,
        DriverCapabilities::empty(),
        SchemaLoadingStrategy::ConnectionPerDatabase,
        time_series_schema(),
    );
    connected.database_connections.insert(
        "metrics".to_string(),
        DatabaseConnection {
            connection: connected.connection.clone(),
            schema: Some(SchemaSnapshot::time_series(TimeSeriesSchema {
                databases: vec![database("metrics", true)],
                current_database: Some("metrics".to_string()),
                measurements: vec![measurement("disk")],
                retention_policies: Vec::new(),
            })),
        },
    );
    connected.database_connections.insert(
        "archive".to_string(),
        DatabaseConnection {
            connection: connected.connection.clone(),
            schema: None,
        },
    );

    assert_shape!(
        &render_profile(
            connected,
            Surroundings {
                pending_databases: vec!["cold"],
                ..Surroundings::default()
            },
        ),
        "time_series_per_database",
    );
}

#[test]
fn key_value_keyspaces_keep_their_rows_and_folded_empties() {
    let keyspace = |db_index: u32, key_count: Option<u64>| KeySpaceInfo {
        db_index,
        key_count,
        memory_bytes: None,
        avg_ttl_seconds: None,
    };

    let mut connected = connected_profile(
        DatabaseCategory::KeyValue,
        DriverCapabilities::empty(),
        SchemaLoadingStrategy::LazyPerDatabase,
        SchemaSnapshot::key_value(KeyValueSchema {
            keyspaces: vec![
                keyspace(0, Some(10)),
                keyspace(1, Some(0)),
                keyspace(2, Some(0)),
                keyspace(3, None),
            ],
            current_keyspace: Some(0),
        }),
    );
    connected.active_database = Some("db0".to_string());

    assert_shape!(
        &render_profile(
            connected,
            Surroundings {
                pending_databases: vec!["db3"],
                ..Surroundings::default()
            },
        ),
        "key_value",
    );
}

#[test]
fn key_value_without_keyspaces_lists_the_active_database() {
    let mut connected = connected_profile(
        DatabaseCategory::KeyValue,
        DriverCapabilities::empty(),
        SchemaLoadingStrategy::LazyPerDatabase,
        SchemaSnapshot::key_value(KeyValueSchema::default()),
    );
    connected.active_database = Some("db4".to_string());

    assert_shape!(
        &render_profile(connected, Surroundings::default()),
        "key_value_empty",
    );
}

#[test]
fn key_value_counts_are_reported_beside_the_tree() {
    let mut state = empty_state();
    state.connections_mut().insert(
        PROFILE_ID,
        connected_profile(
            DatabaseCategory::KeyValue,
            DriverCapabilities::empty(),
            SchemaLoadingStrategy::LazyPerDatabase,
            SchemaSnapshot::key_value(KeyValueSchema {
                keyspaces: vec![KeySpaceInfo {
                    db_index: 2,
                    key_count: Some(7),
                    memory_bytes: None,
                    avg_ttl_seconds: None,
                }],
                current_keyspace: Some(2),
            }),
        ),
    );
    state.connections_mut().insert(
        Uuid::from_u128(2),
        connected_profile(
            DatabaseCategory::Document,
            DriverCapabilities::empty(),
            SchemaLoadingStrategy::LazyPerDatabase,
            document_server_schema(),
        ),
    );

    let counts = super::keyspace_key_counts(&state);

    assert_eq!(
        counts,
        HashMap::from([(
            dbflux_core::SchemaNodeId::Database {
                profile_id: PROFILE_ID,
                name: "db2".to_string(),
            }
            .to_string(),
            7
        )])
    );
}

#[test]
fn object_storage_lists_its_buckets() {
    let object_storage = || {
        connected_profile(
            DatabaseCategory::ObjectStorage,
            DriverCapabilities::empty(),
            SchemaLoadingStrategy::SingleDatabase,
            SchemaSnapshot::default(),
        )
    };

    assert_shape!(
        &render_profile(object_storage(), Surroundings::default()),
        "object_storage_loading",
    );

    assert_shape!(
        &render_profile(
            object_storage(),
            Surroundings {
                bucket_cache: HashMap::from([(
                    PROFILE_ID,
                    vec![
                        dbflux_core::BucketInfo {
                            name: "assets".to_string(),
                            created_at: None,
                        },
                        dbflux_core::BucketInfo {
                            name: "backups".to_string(),
                            created_at: None,
                        },
                    ],
                )]),
                ..Surroundings::default()
            },
        ),
        "object_storage_loaded",
    );
}

#[test]
fn relational_tables_keep_their_relational_index_labels() {
    let mut orders = table("orders", Some("public"));
    orders.columns = Some(Vec::new());
    orders.indexes = Some(IndexData::Relational(vec![IndexInfo {
        name: "orders_pkey".to_string(),
        columns: vec!["id".to_string()],
        is_unique: true,
        is_primary: true,
    }]));

    let connected = connected_profile(
        DatabaseCategory::Relational,
        DriverCapabilities::SCHEMAS,
        SchemaLoadingStrategy::SingleDatabase,
        SchemaSnapshot::relational(RelationalSchema {
            databases: vec![database("app", true)],
            current_database: Some("app".to_string()),
            schemas: vec![schema_group("public", vec![orders], Vec::new())],
            tables: Vec::new(),
            views: Vec::new(),
        }),
    );

    assert_shape!(
        &render_profile(connected, Surroundings::default()),
        "relational_single",
    );
}
