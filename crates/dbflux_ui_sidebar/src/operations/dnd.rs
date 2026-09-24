use super::{HeldDatabaseConnection, try_close_held_database_connection};
use crate::*;
use dbflux_core::{Connection, SchemaDropTarget, SchemaObjectKind, TaskKind, TaskTarget};
use dbflux_ui_base::toast::PendingToast;
use std::sync::Arc;

#[derive(Clone)]
struct SidebarDropOperation {
    profile_id: Uuid,
    item_id: String,
    object_name: String,
    cache_database: Option<String>,
    connection: Arc<dyn Connection>,
    target: SchemaDropTarget,
    task_target: TaskTarget,
    task_description: String,
    is_database: bool,
}

/// Options for the `DROP` statement the sidebar runs.
///
/// The default (`IF EXISTS`, no `CASCADE`) is what the generic inline
/// confirm runs. The drop table modal passes the options its preview was
/// built with, so the statement that runs is the one the user confirmed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DropOptions {
    pub(crate) if_exists: bool,
    pub(crate) cascade: bool,
}

impl Default for DropOptions {
    fn default() -> Self {
        Self {
            if_exists: true,
            cascade: false,
        }
    }
}

/// Drops `target` on `connection` with the confirmed options, returning the
/// driver's error message on failure.
fn run_drop(
    connection: &dyn Connection,
    target: &SchemaDropTarget,
    options: DropOptions,
) -> Result<(), String> {
    connection
        .drop_schema_object(target, options.cascade, options.if_exists)
        .map_err(|error| error.to_string())
}

enum DatabaseDropReleasePlan {
    None,
    ConnectionPerDatabase(Box<HeldDatabaseConnection>),
    ActiveDatabase {
        database: String,
        connection: Arc<dyn Connection>,
    },
}

enum DropExecutionOutcome {
    Dropped {
        database_release_applied: bool,
    },
    Failed {
        error: String,
        held_connection: Option<HeldDatabaseConnection>,
    },
    Cancelled {
        held_connection: Option<HeldDatabaseConnection>,
    },
}

fn describe_drop_target(target: &SchemaDropTarget) -> String {
    match target.kind {
        SchemaObjectKind::Table | SchemaObjectKind::View => match target.schema.as_deref() {
            Some(schema) => format!("{}.{}", schema, target.name),
            None => target.name.clone(),
        },
        SchemaObjectKind::Collection | SchemaObjectKind::Database => target.name.clone(),
    }
}

fn build_drop_task_details(target: &SchemaDropTarget, released_database: Option<&str>) -> String {
    let mut lines = vec![
        format!("Kind: {:?}", target.kind),
        format!("Target: {}", describe_drop_target(target)),
    ];

    if let Some(database) = target.database.as_deref() {
        lines.push(format!("Database: {}", database));
    }

    if let Some(database) = released_database {
        lines.push(format!("Released database connection: {}", database));
    }

    lines.join("\n")
}

impl Sidebar {
    fn build_drop_operation(&self, item_id: &str, cx: &App) -> Option<SidebarDropOperation> {
        let node_id = parse_node_id(item_id)?;
        let profile_id = node_id.profile_id()?;
        let connected = self.app_state.read(cx).connections().get(&profile_id)?;

        match node_id {
            SchemaNodeId::Table {
                database,
                schema,
                name,
                ..
            } => {
                let mut target = SchemaDropTarget::new(SchemaObjectKind::Table, name.clone())
                    .with_schema(schema.clone());

                if let Some(database_name) = database.clone() {
                    target = target.with_database(database_name.clone());
                }

                let connection = connected
                    .resolve_connection_for_execution(database.as_deref())
                    .unwrap_or_else(|_| connected.connection.clone());

                Some(SidebarDropOperation {
                    profile_id,
                    item_id: item_id.to_string(),
                    object_name: name.clone(),
                    cache_database: Some(database.clone().unwrap_or(schema.clone())),
                    connection,
                    task_target: TaskTarget {
                        profile_id,
                        database,
                    },
                    task_description: crate::labels::dropping_task_label(
                        &SchemaObjectKind::Table,
                        &name,
                    ),
                    target,
                    is_database: false,
                })
            }
            SchemaNodeId::View {
                database,
                schema,
                name,
                ..
            } => {
                let mut target = SchemaDropTarget::new(SchemaObjectKind::View, name.clone())
                    .with_schema(schema.clone());

                if let Some(database_name) = database.clone() {
                    target = target.with_database(database_name.clone());
                }

                let connection = connected
                    .resolve_connection_for_execution(database.as_deref())
                    .unwrap_or_else(|_| connected.connection.clone());

                Some(SidebarDropOperation {
                    profile_id,
                    item_id: item_id.to_string(),
                    object_name: name.clone(),
                    cache_database: Some(database.clone().unwrap_or(schema.clone())),
                    connection,
                    task_target: TaskTarget {
                        profile_id,
                        database,
                    },
                    task_description: crate::labels::dropping_task_label(
                        &SchemaObjectKind::View,
                        &name,
                    ),
                    target,
                    is_database: false,
                })
            }
            SchemaNodeId::Collection { database, name, .. } => Some(SidebarDropOperation {
                profile_id,
                item_id: item_id.to_string(),
                object_name: name.clone(),
                cache_database: Some(database.clone()),
                connection: connected
                    .resolve_connection_for_execution(Some(&database))
                    .unwrap_or_else(|_| connected.connection.clone()),
                target: SchemaDropTarget::new(SchemaObjectKind::Collection, name.clone())
                    .with_database(database.clone()),
                task_target: TaskTarget {
                    profile_id,
                    database: Some(database),
                },
                task_description: crate::labels::dropping_task_label(
                    &SchemaObjectKind::Collection,
                    &name,
                ),
                is_database: false,
            }),
            SchemaNodeId::Database { name, .. } => Some(SidebarDropOperation {
                profile_id,
                item_id: item_id.to_string(),
                object_name: name.clone(),
                cache_database: None,
                connection: connected.connection.clone(),
                target: SchemaDropTarget::new(SchemaObjectKind::Database, name.clone()),
                task_target: TaskTarget {
                    profile_id,
                    database: Some(name.clone()),
                },
                task_description: crate::labels::dropping_task_label(
                    &SchemaObjectKind::Database,
                    &name,
                ),
                is_database: true,
            }),
            _ => None,
        }
    }

    fn prepare_database_drop_release(
        state: &mut AppStateEntity,
        profile_id: Uuid,
        database: &str,
    ) -> Result<DatabaseDropReleasePlan, String> {
        if state.connections().get(&profile_id).is_none() {
            return Err(format!(
                "No active DBFlux connection found for database '{}'",
                database
            ));
        }

        // Slot take goes through the manager seam so the per-target slot
        // revision advances; the entry's ownership transfers into the plan.
        if let Some(connection) = state.take_database_connection(profile_id, database) {
            let Some(connected) = state.connections_mut().get_mut(&profile_id) else {
                // Unreachable: the profile existed a moment ago and the take
                // cannot remove a profile.
                return Err(format!(
                    "No active DBFlux connection found for database '{}'",
                    database
                ));
            };

            let cached_schema = connected.database_schemas.remove(database);
            let previous_active_database = connected.active_database.clone();

            if connected.active_database.as_deref() == Some(database) {
                connected.active_database = connected
                    .schema
                    .as_ref()
                    .and_then(|schema| schema.current_database().map(String::from));
            }

            return Ok(DatabaseDropReleasePlan::ConnectionPerDatabase(Box::new(
                HeldDatabaseConnection {
                    database: database.to_string(),
                    connection,
                    cached_schema,
                    previous_active_database,
                },
            )));
        }

        let Some(connected) = state.connections_mut().get_mut(&profile_id) else {
            return Err(format!(
                "No active DBFlux connection found for database '{}'",
                database
            ));
        };

        if connected.connection.schema_loading_strategy()
            == SchemaLoadingStrategy::ConnectionPerDatabase
            && connected
                .schema
                .as_ref()
                .and_then(|schema| schema.current_database())
                .is_some_and(|current| current == database)
        {
            return Err(format!(
                "Cannot drop database '{}' while DBFlux is still connected to it as the current session. Open another database first.",
                database
            ));
        }

        if connected.connection.schema_loading_strategy() == SchemaLoadingStrategy::LazyPerDatabase
            && connected.active_database.as_deref() == Some(database)
        {
            return Ok(DatabaseDropReleasePlan::ActiveDatabase {
                database: database.to_string(),
                connection: connected.connection.clone(),
            });
        }

        Ok(DatabaseDropReleasePlan::None)
    }

    pub(super) fn restore_database_drop_release(
        state: &mut AppStateEntity,
        profile_id: Uuid,
        held_connection: HeldDatabaseConnection,
    ) {
        let database = held_connection.database.clone();

        // Slot restoration goes through the manager seam so the per-target
        // slot revision advances; a missing profile drops the entry.
        if !state.restore_database_connection(
            profile_id,
            database.clone(),
            held_connection.connection,
        ) {
            log::warn!(
                "Failed to restore released database connection for profile {}: profile missing",
                profile_id
            );
            return;
        }

        let Some(connected) = state.connections_mut().get_mut(&profile_id) else {
            // Unreachable: restore reported success, so the profile exists.
            log::warn!(
                "Failed to restore released database connection for profile {}: profile missing",
                profile_id
            );
            return;
        };

        if let Some(cached_schema) = held_connection.cached_schema {
            connected.database_schemas.insert(database, cached_schema);
        }

        connected.active_database = held_connection.previous_active_database;
    }

    fn finalize_successful_database_release(
        state: &mut AppStateEntity,
        profile_id: Uuid,
        database: &str,
    ) {
        if let Some(connected) = state.connections_mut().get_mut(&profile_id) {
            connected.database_schemas.remove(database);
            connected
                .table_details
                .retain(|(db, _, _), _| db != database);

            if connected.active_database.as_deref() == Some(database) {
                connected.active_database = None;
            }
        }
    }

    /// Drop a schema object through the driver-owned schema drop API.
    pub(crate) fn execute_drop_ddl(
        &mut self,
        item_id: &str,
        options: DropOptions,
        cx: &mut Context<Self>,
    ) {
        let Some(operation) = self.build_drop_operation(item_id, cx) else {
            return;
        };

        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.pending_toast = Some(PendingToast {
                message: crate::labels::background_task_limit_toast_label(),
                is_error: true,
            });
            self.refresh_tree(cx);
            cx.notify();
            return;
        }

        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let task = state.start_task_for_target(
                TaskKind::SchemaDrop,
                operation.task_description.clone(),
                Some(operation.task_target.clone()),
            );
            cx.emit(AppStateChanged);
            task
        });

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();
        let released_database = operation.target.name.clone();

        let operation_task = cx.spawn(async move |_this, cx| {
            let release_plan = if operation.is_database {
                match cx.update(|cx| {
                    app_state.update(cx, |state, _cx| {
                        Self::prepare_database_drop_release(
                            state,
                            operation.profile_id,
                            &released_database,
                        )
                    })
                }) {
                    Ok(plan) => plan,
                    Err(error) => {
                        cx.update(|cx| {
                            sidebar.update(cx, |sidebar, _cx| {
                                sidebar.clear_tracked_operation_task(task_id);
                            });

                            app_state.update(cx, |state, cx| {
                                state.fail_task(task_id, error.clone());
                                cx.emit(AppStateChanged);
                            });

                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.pending_toast = Some(PendingToast {
                                    message: error,
                                    is_error: true,
                                });
                                sidebar.refresh_tree(cx);
                            });
                        });
                        return;
                    }
                }
            } else {
                DatabaseDropReleasePlan::None
            };

            let drop_result = cx
                .background_executor()
                .spawn({
                    let operation = operation.clone();
                    let cancel_token = cancel_token.clone();
                    async move {
                        let mut database_release_applied = false;

                        if cancel_token.is_cancelled() {
                            let held_connection = match release_plan {
                                DatabaseDropReleasePlan::ConnectionPerDatabase(held_connection) => {
                                    Some(*held_connection)
                                }
                                DatabaseDropReleasePlan::None
                                | DatabaseDropReleasePlan::ActiveDatabase { .. } => None,
                            };

                            return DropExecutionOutcome::Cancelled { held_connection };
                        }

                        match release_plan {
                            DatabaseDropReleasePlan::ConnectionPerDatabase(mut held_connection) => {
                                if let Err(error) =
                                    try_close_held_database_connection(&mut held_connection)
                                {
                                    return DropExecutionOutcome::Failed {
                                        error,
                                        held_connection: Some(*held_connection),
                                    };
                                }

                                database_release_applied = true;
                            }
                            DatabaseDropReleasePlan::ActiveDatabase {
                                database,
                                connection,
                            } => {
                                if let Err(error) = connection.set_active_database(None) {
                                    return DropExecutionOutcome::Failed {
                                        error: format!(
                                            "Failed to release active database '{}': {}",
                                            database, error
                                        ),
                                        held_connection: None,
                                    };
                                }

                                database_release_applied = true;
                            }
                            DatabaseDropReleasePlan::None => {}
                        }

                        if cancel_token.is_cancelled() {
                            return DropExecutionOutcome::Cancelled {
                                held_connection: None,
                            };
                        }

                        match run_drop(operation.connection.as_ref(), &operation.target, options) {
                            Ok(()) => DropExecutionOutcome::Dropped {
                                database_release_applied,
                            },
                            Err(error) => DropExecutionOutcome::Failed {
                                error,
                                held_connection: None,
                            },
                        }
                    }
                })
                .await;

            cx.update(|cx| match drop_result {
                DropExecutionOutcome::Dropped {
                    database_release_applied,
                } => {
                    sidebar.update(cx, |sidebar, _cx| {
                        sidebar.clear_tracked_operation_task(task_id);
                    });

                    app_state.update(cx, |state, cx| {
                        if operation.is_database && database_release_applied {
                            Self::finalize_successful_database_release(
                                state,
                                operation.profile_id,
                                &operation.object_name,
                            );
                        }

                        let details = build_drop_task_details(
                            &operation.target,
                            operation
                                .is_database
                                .then_some(operation.object_name.as_str()),
                        );
                        state.complete_task_with_details(task_id, details);
                        cx.emit(AppStateChanged);
                    });

                    sidebar.update(cx, |sidebar, cx| {
                        if operation.is_database {
                            sidebar.invalidate_database_cache(
                                operation.profile_id,
                                &operation.object_name,
                                cx,
                            );
                        } else if let Some(cache_database) = operation.cache_database.as_deref() {
                            sidebar.invalidate_object_cache(
                                operation.profile_id,
                                cache_database,
                                &operation.target,
                                cx,
                            );
                        }

                        sidebar.expansion_overrides.remove(&operation.item_id);
                        sidebar.refresh_tree(cx);
                    });
                }
                DropExecutionOutcome::Failed {
                    error,
                    held_connection,
                } => {
                    sidebar.update(cx, |sidebar, _cx| {
                        sidebar.clear_tracked_operation_task(task_id);
                    });

                    if let Some(held_connection) = held_connection {
                        app_state.update(cx, |state, _cx| {
                            Self::restore_database_drop_release(
                                state,
                                operation.profile_id,
                                held_connection,
                            );
                        });
                    }

                    let details = build_drop_task_details(&operation.target, None);

                    app_state.update(cx, |state, cx| {
                        state.fail_task_with_details(task_id, error.clone(), details);
                        cx.emit(AppStateChanged);
                    });

                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.pending_toast = Some(PendingToast {
                            message: crate::labels::drop_failed_label(&error),
                            is_error: true,
                        });
                        sidebar.refresh_tree(cx);
                    });
                }
                DropExecutionOutcome::Cancelled { held_connection } => {
                    sidebar.update(cx, |sidebar, _cx| {
                        sidebar.clear_tracked_operation_task(task_id);
                    });

                    if let Some(held_connection) = held_connection {
                        app_state.update(cx, |state, _cx| {
                            Self::restore_database_drop_release(
                                state,
                                operation.profile_id,
                                held_connection,
                            );
                        });
                    }

                    if cancel_token.is_cancelled() {
                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.refresh_tree(cx);
                        });
                        return;
                    }

                    let details = build_drop_task_details(&operation.target, None);

                    let cancelled_label = crate::labels::schema_drop_cancelled_label();

                    app_state.update(cx, |state, cx| {
                        state.fail_task_with_details(task_id, cancelled_label.clone(), details);
                        cx.emit(AppStateChanged);
                    });

                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.pending_toast = Some(PendingToast {
                            message: cancelled_label,
                            is_error: true,
                        });
                        sidebar.refresh_tree(cx);
                    });
                }
            });
        });

        self.track_operation_task(task_id, operation_task);
    }
}

#[cfg(test)]
mod tests {
    use dbflux_core::{
        Connection, DatabaseCategory, DbConfig, DbError, DbKind, DriverCapabilities,
        DriverMetadata, Icon, QueryHandle, QueryRequest, QueryResult, RelationalSchema,
        SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TransferFamily, WritePrivilege,
    };

    use std::sync::Arc;

    use dbflux_ui_base::AppStateEntity;

    use super::{DatabaseDropReleasePlan, Sidebar};
    use dbflux_core::ConnectionProfile;

    /// Minimal per-database connection so the release/restore roundtrip can
    /// run against a real `AppStateEntity` without a live database.
    struct ReleaseTestConnection {
        metadata: DriverMetadata,
    }

    impl ReleaseTestConnection {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                metadata: DriverMetadata {
                    id: "sidebar-release-test".to_string(),
                    display_name: "ReleaseTest".to_string(),
                    description: "test".to_string(),
                    category: DatabaseCategory::Relational,
                    transfer_family: TransferFamily::Sql,
                    deployment_class: None,
                    query_language: dbflux_core::QueryLanguage::Sql,
                    capabilities: DriverCapabilities::empty(),
                    default_port: None,
                    uri_scheme: "test".to_string(),
                    icon: Icon::Database,
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
            })
        }
    }

    impl Connection for ReleaseTestConnection {
        fn metadata(&self) -> &DriverMetadata {
            &self.metadata
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, _req: &QueryRequest) -> Result<QueryResult, DbError> {
            Err(DbError::NotSupported("test connection".to_string()))
        }

        fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Ok(SchemaSnapshot::default())
        }

        fn kind(&self) -> DbKind {
            DbKind::Postgres
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::ConnectionPerDatabase
        }

        fn dialect(&self) -> &dyn SqlDialect {
            &dbflux_core::DefaultSqlDialect
        }
    }

    fn schema_with_current_database(database: &str) -> SchemaSnapshot {
        SchemaSnapshot::relational(RelationalSchema {
            current_database: Some(database.to_string()),
            ..Default::default()
        })
    }

    #[test]
    fn database_drop_release_roundtrip_keeps_slot_entry_and_active_state() {
        use dbflux_storage::bootstrap::StorageRuntime;

        let rt = StorageRuntime::in_memory().expect("in-memory storage runtime");
        let mut state = AppStateEntity::new_with_storage_runtime(rt).expect("test app state");

        let profile = ConnectionProfile::new("pg", DbConfig::default_postgres());
        state.apply_connect_profile(
            profile.clone(),
            ReleaseTestConnection::new(),
            Some(schema_with_current_database("app")),
            None,
            false,
            WritePrivilege::Unknown,
        );

        let analytics: Arc<dyn Connection> = ReleaseTestConnection::new();
        state.add_database_connection(
            profile.id,
            "analytics".to_string(),
            analytics.clone(),
            Some(schema_with_current_database("analytics")),
        );
        state.set_active_database(profile.id, Some("analytics".to_string()));

        // Release transfers the slot entry out and applies the active
        // fallback to the primary schema's current database.
        let plan = Sidebar::prepare_database_drop_release(&mut state, profile.id, "analytics")
            .expect("release should find the per-database slot");
        let DatabaseDropReleasePlan::ConnectionPerDatabase(held) = plan else {
            panic!("expected a per-database release plan for a live slot");
        };
        assert_eq!(held.database, "analytics");
        assert!(
            Arc::ptr_eq(&held.connection.connection, &analytics),
            "release must transfer the slot entry itself"
        );
        assert!(
            held.connection.schema.is_some(),
            "release keeps the slot's own schema"
        );
        assert_eq!(held.previous_active_database, Some("analytics".to_string()));
        assert_eq!(
            state.get_active_database(profile.id),
            Some("app".to_string()),
            "release falls back to the primary database for browsing"
        );
        let connected = state.connections().get(&profile.id).expect("connected");
        assert!(
            connected.database_connection("analytics").is_none(),
            "the slot must be gone while the release is held"
        );

        // Restore puts the very same entry back and reinstates the prior
        // active database.
        Sidebar::restore_database_drop_release(&mut state, profile.id, *held);
        let connected = state.connections().get(&profile.id).expect("connected");
        let restored_slot = connected
            .database_connection("analytics")
            .expect("slot restored after release");
        assert!(Arc::ptr_eq(&restored_slot.connection, &analytics));
        assert!(
            restored_slot.schema.is_some(),
            "restore reinstates the slot schema"
        );
        assert_eq!(
            state.get_active_database(profile.id),
            Some("analytics".to_string()),
            "restore reinstates the previous active database"
        );
    }

    /// Records the statements `drop_schema_object` runs, through a dialect
    /// that either supports `DROP ... CASCADE` or not.
    struct RecordingConnection {
        metadata: DriverMetadata,
        supports_cascade: bool,
        executed: std::sync::Mutex<Vec<String>>,
    }

    struct RecordingDialect {
        supports_cascade: bool,
    }

    impl SqlDialect for RecordingDialect {
        fn quote_identifier(&self, name: &str) -> String {
            dbflux_core::DefaultSqlDialect.quote_identifier(name)
        }

        fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
            dbflux_core::DefaultSqlDialect.qualified_table(schema, table)
        }

        fn value_to_literal(&self, value: &dbflux_core::Value) -> String {
            dbflux_core::DefaultSqlDialect.value_to_literal(value)
        }

        fn escape_string(&self, text: &str) -> String {
            dbflux_core::DefaultSqlDialect.escape_string(text)
        }

        fn placeholder_style(&self) -> dbflux_core::PlaceholderStyle {
            dbflux_core::PlaceholderStyle::DollarNumber
        }

        fn supports_drop_cascade(&self) -> bool {
            self.supports_cascade
        }
    }

    static CASCADE_DIALECT: RecordingDialect = RecordingDialect {
        supports_cascade: true,
    };
    static PLAIN_DIALECT: RecordingDialect = RecordingDialect {
        supports_cascade: false,
    };

    impl RecordingConnection {
        fn new(supports_cascade: bool) -> Self {
            let metadata = ReleaseTestConnection::new().metadata.clone();

            Self {
                metadata,
                supports_cascade,
                executed: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn executed(&self) -> Vec<String> {
            self.executed
                .lock()
                .map(|executed| executed.clone())
                .unwrap_or_default()
        }
    }

    impl Connection for RecordingConnection {
        fn metadata(&self) -> &DriverMetadata {
            &self.metadata
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, request: &QueryRequest) -> Result<QueryResult, DbError> {
            self.executed
                .lock()
                .map_err(|_| DbError::NotSupported("poisoned".to_string()))?
                .push(request.sql.clone());
            Ok(QueryResult::empty())
        }

        fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Ok(SchemaSnapshot::default())
        }

        fn kind(&self) -> DbKind {
            DbKind::Postgres
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }

        fn dialect(&self) -> &dyn SqlDialect {
            if self.supports_cascade {
                &CASCADE_DIALECT
            } else {
                &PLAIN_DIALECT
            }
        }
    }

    #[test]
    fn confirmed_drop_table_runs_the_statement_the_preview_shows() {
        use dbflux_components::modals::{DropTableOutcome, DropTableRequest};
        use dbflux_core::{RelationKind, RelationRef, SchemaDropTarget, SchemaObjectKind};

        for supports_cascade in [true, false] {
            let connection = RecordingConnection::new(supports_cascade);
            let request = DropTableRequest::new(
                "orders".to_string(),
                Some("public".to_string()),
                vec![RelationRef {
                    kind: RelationKind::View,
                    qualified_name: "public.order_view".to_string(),
                }],
                connection.dialect(),
            );

            let DropTableOutcome::Confirmed { if_exists, cascade } = request.confirmed_outcome()
            else {
                panic!("a request confirms with its options");
            };
            assert_eq!(cascade, supports_cascade);

            let target =
                SchemaDropTarget::new(SchemaObjectKind::Table, "orders").with_schema("public");
            super::run_drop(
                &connection,
                &target,
                super::DropOptions { if_exists, cascade },
            )
            .expect("the confirmed drop runs");

            let executed = connection.executed();
            assert_eq!(executed.len(), 1);
            assert_eq!(format!("{};", executed[0]), request.sql_preview());
        }
    }
}
