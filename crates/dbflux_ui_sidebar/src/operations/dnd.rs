use crate::*;
use super::{HeldDatabaseConnection, try_close_held_database_connection};
use dbflux_core::{Connection, SchemaDropTarget, SchemaObjectKind, TaskKind, TaskTarget};
use dbflux_ui_base::AsyncUpdateResultExt;
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
                    task_description: format!("Dropping table {}", name),
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
                    task_description: format!("Dropping view {}", name),
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
                task_description: format!("Dropping collection {}", name),
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
                task_description: format!("Dropping database {}", name),
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
        let Some(connected) = state.connections_mut().get_mut(&profile_id) else {
            return Err(format!(
                "No active DBFlux connection found for database '{}'",
                database
            ));
        };

        if let Some(connection) = connected.database_connections.remove(database) {
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
        let Some(connected) = state.connections_mut().get_mut(&profile_id) else {
            log::warn!(
                "Failed to restore released database connection for profile {}: profile missing",
                profile_id
            );
            return;
        };

        let database = held_connection.database.clone();
        connected
            .database_connections
            .insert(database.clone(), held_connection.connection);

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
            connected.table_details.retain(|(db, _), _| db != database);

            if connected.active_database.as_deref() == Some(database) {
                connected.active_database = None;
            }
        }
    }

    /// Drop a schema object through the driver-owned schema drop API.
    pub(crate) fn execute_drop_ddl(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(operation) = self.build_drop_operation(item_id, cx) else {
            return;
        };

        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.pending_toast = Some(PendingToast {
                message: "Too many background tasks running, please wait".to_string(),
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
                    Ok(Ok(plan)) => plan,
                    Ok(Err(error)) => {
                        if let Err(update_error) = cx.update(|cx| {
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
                        }) {
                            log::warn!(
                                "Failed to apply database drop release error: {:?}",
                                update_error
                            );
                        }
                        return;
                    }
                    Err(update_error) => {
                        log::warn!(
                            "Failed to prepare database drop release: {:?}",
                            update_error
                        );

                        cx.update(|cx| {
                            sidebar.update(cx, |sidebar, _cx| {
                                sidebar.clear_tracked_operation_task(task_id);
                            });
                        })
                        .log_if_dropped();

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

                        match operation.connection.drop_schema_object(
                            &operation.target,
                            false,
                            true,
                        ) {
                            Ok(()) => DropExecutionOutcome::Dropped {
                                database_release_applied,
                            },
                            Err(error) => DropExecutionOutcome::Failed {
                                error: error.to_string(),
                                held_connection: None,
                            },
                        }
                    }
                })
                .await;

            if let Err(update_error) = cx.update(|cx| match drop_result {
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
                            message: format!("Failed to drop: {}", error),
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

                    app_state.update(cx, |state, cx| {
                        state.fail_task_with_details(task_id, "Schema drop cancelled", details);
                        cx.emit(AppStateChanged);
                    });

                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.pending_toast = Some(PendingToast {
                            message: "Schema drop cancelled".to_string(),
                            is_error: true,
                        });
                        sidebar.refresh_tree(cx);
                    });
                }
            }) {
                log::warn!("Failed to apply schema drop result: {:?}", update_error);
            }
        });

        self.track_operation_task(task_id, operation_task);
    }
}
