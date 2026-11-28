use super::{HeldDatabaseConnection, try_close_held_database_connection};
use crate::*;
use dbflux_core::{
    CancelPolicy, CancelToken, Connection, SchemaDropTarget, SchemaObjectKind, TaskKind,
    TaskRequest, TaskTarget,
};
use dbflux_ui_base::toast::PendingToast;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
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

/// What the release step before the `DROP` left to do.
enum DropPreparation {
    /// The drop ended before the statement was reached.
    Finished(Box<DropExecutionOutcome>),
    /// The statement may be sent once the task passes its point of no return.
    Ready {
        database_release_applied: bool,
        /// Set when the release cleared the driver's active database, so a
        /// cancel that wins before the statement can put it back.
        active_release: Option<ActiveDatabaseRelease>,
    },
}

/// The driver's active database as it stood before the release cleared it.
struct ActiveDatabaseRelease {
    database: String,
    connection: Arc<dyn Connection>,
}

/// Whether the release still stands after a cancel tried to undo it.
struct ReleaseRestoration {
    /// Nothing to restore, or the driver refused.
    release_applied: bool,
    /// Why the driver refused, for the foreground to report.
    error: Option<String>,
}

/// Puts the driver's active database back after a cancel won once the
/// release had cleared it.
fn undo_active_database_release(
    active_release: Option<ActiveDatabaseRelease>,
    release_applied: bool,
) -> ReleaseRestoration {
    let Some(release) = active_release else {
        return ReleaseRestoration {
            release_applied,
            error: None,
        };
    };

    match release
        .connection
        .set_active_database(Some(release.database.as_str()))
    {
        Ok(()) => ReleaseRestoration {
            release_applied: false,
            error: None,
        },
        Err(error) => ReleaseRestoration {
            release_applied,
            error: Some(error.to_string()),
        },
    }
}

/// Releases the connection resources the drop needs, honoring a cancel that
/// arrives before the statement is sent. A release that fails after the user
/// cancelled ends as cancelled: no DDL ran, and the cancel was accepted.
fn prepare_drop_release(
    release_plan: DatabaseDropReleasePlan,
    cancel_token: &CancelToken,
) -> DropPreparation {
    match release_resources(release_plan, cancel_token) {
        DropPreparation::Finished(outcome) => match *outcome {
            DropExecutionOutcome::Failed {
                held_connection, ..
            } if cancel_token.is_cancelled() => {
                DropPreparation::Finished(Box::new(DropExecutionOutcome::Cancelled {
                    held_connection,
                    release_applied: false,
                    restore_error: None,
                }))
            }
            outcome => DropPreparation::Finished(Box::new(outcome)),
        },
        ready @ DropPreparation::Ready { .. } => ready,
    }
}

fn release_resources(
    release_plan: DatabaseDropReleasePlan,
    cancel_token: &CancelToken,
) -> DropPreparation {
    if cancel_token.is_cancelled() {
        let held_connection = match release_plan {
            DatabaseDropReleasePlan::ConnectionPerDatabase(held_connection) => {
                Some(*held_connection)
            }
            DatabaseDropReleasePlan::None | DatabaseDropReleasePlan::ActiveDatabase { .. } => None,
        };

        return DropPreparation::Finished(Box::new(DropExecutionOutcome::Cancelled {
            held_connection,
            release_applied: false,
            restore_error: None,
        }));
    }

    match release_plan {
        DatabaseDropReleasePlan::ConnectionPerDatabase(mut held_connection) => {
            if let Err(error) = try_close_held_database_connection(&mut held_connection) {
                return DropPreparation::Finished(Box::new(DropExecutionOutcome::Failed {
                    error,
                    held_connection: Some(*held_connection),
                }));
            }

            DropPreparation::Ready {
                database_release_applied: true,
                active_release: None,
            }
        }
        DatabaseDropReleasePlan::ActiveDatabase {
            database,
            connection,
        } => {
            if let Err(error) = connection.set_active_database(None) {
                return DropPreparation::Finished(Box::new(DropExecutionOutcome::Failed {
                    error: format!(
                        "Failed to release active database '{}': {}",
                        database, error
                    ),
                    held_connection: None,
                }));
            }

            DropPreparation::Ready {
                database_release_applied: true,
                active_release: Some(ActiveDatabaseRelease {
                    database,
                    connection,
                }),
            }
        }
        DatabaseDropReleasePlan::None => DropPreparation::Ready {
            database_release_applied: false,
            active_release: None,
        },
    }
}

/// Runs on the foreground, where no cancel request can interleave: a cancel
/// that already reached the task stops the drop, otherwise the task is marked
/// as past its point of no return and the statement may be sent.
fn enter_drop_point_of_no_return(
    state: &mut dbflux_ui_base::AppStateEntity,
    task_id: dbflux_core::TaskId,
    cancel_token: &CancelToken,
    reason: String,
) -> bool {
    if cancel_token.is_cancelled() {
        return false;
    }

    state.mark_task_point_of_no_return(task_id, reason);
    true
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
        /// The release step already closed the database's connection (or
        /// cleared its active database) before the cancel won, and that
        /// cannot be undone from here.
        release_applied: bool,
        /// The driver refused to put its active database back.
        restore_error: Option<String>,
    },
}

/// Tells the user what a cancel that won after the release step left behind:
/// nothing was dropped, but DBFlux no longer holds a connection to the
/// database.
fn released_connection_notice(release_applied: bool, database: &str) -> Option<String> {
    release_applied.then(|| {
        dbflux_i18n::t!(
            "sidebar.toast.schema_drop_cancelled_after_release",
            database = database
        )
    })
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

        let started = self.app_state.update(cx, |state, cx| {
            let handle = start_drop_task(state, &operation)?;
            cx.emit(AppStateChanged);
            Ok::<_, dbflux_core::TaskBusy>((handle.id(), handle.token().clone()))
        });

        let (task_id, cancel_token) = match started {
            Ok(started) => started,
            Err(busy) => {
                // The target is already busy with another drop or a switch;
                // say so instead of ignoring the request silently.
                log::info!("Drop refused: {busy}");
                self.pending_toast = Some(PendingToast {
                    message: crate::labels::drop_target_busy_toast_label(),
                    is_error: true,
                });
                cx.notify();
                return;
            }
        };

        self.refresh_tree(cx);

        let run = DropRun {
            app_state: self.app_state.clone(),
            sidebar: cx.entity().clone(),
            operation,
            options,
            task_id,
            cancel_token,
        };
        let operation_task = cx.spawn(async move |_this, cx| run.drive(cx).await);

        self.track_operation_task(task_id, operation_task);
    }
}

/// Starts the task of a drop. A second drop on the same target is refused
/// while one is in flight, `Cancelling` included: a cancelled DDL that has
/// not stopped yet may still change the schema.
fn start_drop_task(
    state: &mut AppStateEntity,
    operation: &SidebarDropOperation,
) -> Result<dbflux_core::TaskHandle, dbflux_core::TaskBusy> {
    state.try_start_task(
        TaskRequest::new(TaskKind::SchemaDrop, operation.task_description.clone())
            .target(operation.task_target.clone())
            .policy(CancelPolicy::WorkerAcknowledged),
    )
}

/// One running `DROP`: the steps that release resources, send the statement
/// and settle the task, each in its own function so the dispatch sequence in
/// [`DropRun::drive`] reads top to bottom.
struct DropRun {
    app_state: Entity<AppStateEntity>,
    sidebar: Entity<Sidebar>,
    operation: SidebarDropOperation,
    options: DropOptions,
    task_id: dbflux_core::TaskId,
    cancel_token: CancelToken,
}

impl DropRun {
    async fn drive(self, cx: &mut AsyncApp) {
        let release_plan = if self.operation.is_database {
            match self.plan_database_release(cx) {
                Ok(plan) => plan,
                Err(error) => {
                    cx.update(|cx| self.fail_before_release(error, cx));
                    return;
                }
            }
        } else {
            DatabaseDropReleasePlan::None
        };

        let preparation = cx
            .background_executor()
            .spawn({
                let cancel_token = self.cancel_token.clone();
                async move { prepare_drop_release(release_plan, &cancel_token) }
            })
            .await;

        let drop_result = match preparation {
            DropPreparation::Finished(outcome) => *outcome,
            DropPreparation::Ready {
                database_release_applied,
                active_release,
            } => {
                self.send_statement(database_release_applied, active_release, cx)
                    .await
            }
        };

        cx.update(|cx| self.settle(drop_result, cx));
    }

    fn plan_database_release(&self, cx: &mut AsyncApp) -> Result<DatabaseDropReleasePlan, String> {
        cx.update(|cx| {
            self.app_state.update(cx, |state, _cx| {
                Sidebar::prepare_database_drop_release(
                    state,
                    self.operation.profile_id,
                    &self.operation.target.name,
                )
            })
        })
    }

    fn fail_before_release(&self, error: String, cx: &mut App) {
        self.sidebar.update(cx, |sidebar, _cx| {
            sidebar.clear_tracked_operation_task(self.task_id);
        });

        self.app_state.update(cx, |state, cx| {
            state.fail_task(self.task_id, error.clone());
            cx.emit(AppStateChanged);
        });

        report_error(UserFacingError::new(ErrorKind::User, error), cx);

        self.sidebar.update(cx, |sidebar, cx| {
            sidebar.refresh_tree(cx);
        });
    }

    /// Passes the point-of-no-return gate on the foreground, then sends the
    /// statement on a background thread.
    async fn send_statement(
        &self,
        database_release_applied: bool,
        active_release: Option<ActiveDatabaseRelease>,
        cx: &mut AsyncApp,
    ) -> DropExecutionOutcome {
        let may_send = cx.update(|cx| {
            self.app_state.update(cx, |state, cx| {
                let may_send = enter_drop_point_of_no_return(
                    state,
                    self.task_id,
                    &self.cancel_token,
                    dbflux_i18n::t!("tasks_panel.drop_point_of_no_return"),
                );
                cx.emit(AppStateChanged);
                may_send
            })
        });

        if !may_send {
            let restoration = cx
                .background_executor()
                .spawn(async move {
                    undo_active_database_release(active_release, database_release_applied)
                })
                .await;

            return DropExecutionOutcome::Cancelled {
                held_connection: None,
                release_applied: restoration.release_applied,
                restore_error: restoration.error,
            };
        }

        let operation = self.operation.clone();
        let options = self.options;
        cx.background_executor()
            .spawn(async move {
                match run_drop(operation.connection.as_ref(), &operation.target, options) {
                    Ok(()) => DropExecutionOutcome::Dropped {
                        database_release_applied,
                    },
                    Err(error) => DropExecutionOutcome::Failed {
                        error,
                        held_connection: None,
                    },
                }
            })
            .await
    }

    fn settle(&self, outcome: DropExecutionOutcome, cx: &mut App) {
        self.sidebar.update(cx, |sidebar, _cx| {
            sidebar.clear_tracked_operation_task(self.task_id);
        });

        match outcome {
            DropExecutionOutcome::Dropped {
                database_release_applied,
            } => self.finish_dropped(database_release_applied, cx),
            DropExecutionOutcome::Failed {
                error,
                held_connection,
            } => self.finish_failed(error, held_connection, cx),
            DropExecutionOutcome::Cancelled {
                held_connection,
                release_applied,
                restore_error,
            } => self.finish_cancelled(held_connection, release_applied, restore_error, cx),
        }
    }

    fn restore_released_connection(
        &self,
        held_connection: Option<HeldDatabaseConnection>,
        cx: &mut App,
    ) {
        if let Some(held_connection) = held_connection {
            self.app_state.update(cx, |state, _cx| {
                Sidebar::restore_database_drop_release(
                    state,
                    self.operation.profile_id,
                    held_connection,
                );
            });
        }
    }

    fn finish_dropped(&self, database_release_applied: bool, cx: &mut App) {
        let operation = &self.operation;

        self.app_state.update(cx, |state, cx| {
            if operation.is_database && database_release_applied {
                Sidebar::finalize_successful_database_release(
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
            state.complete_task_with_details(self.task_id, details);
            cx.emit(AppStateChanged);
        });

        self.sidebar.update(cx, |sidebar, cx| {
            if operation.is_database {
                sidebar.invalidate_database_cache(operation.profile_id, &operation.object_name, cx);
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

    fn finish_failed(
        &self,
        error: String,
        held_connection: Option<HeldDatabaseConnection>,
        cx: &mut App,
    ) {
        self.restore_released_connection(held_connection, cx);

        let details = build_drop_task_details(&self.operation.target, None);

        self.app_state.update(cx, |state, cx| {
            state.fail_task_with_details(self.task_id, error.clone(), details);
            cx.emit(AppStateChanged);
        });

        report_error(
            UserFacingError::new(ErrorKind::Driver, crate::labels::drop_failed_label(&error)),
            cx,
        );

        self.sidebar.update(cx, |sidebar, cx| {
            sidebar.refresh_tree(cx);
        });
    }

    fn finish_cancelled(
        &self,
        held_connection: Option<HeldDatabaseConnection>,
        release_applied: bool,
        restore_error: Option<String>,
        cx: &mut App,
    ) {
        self.restore_released_connection(held_connection, cx);

        if self.cancel_token.is_cancelled() {
            self.app_state.update(cx, |state, cx| {
                state.acknowledge_task_cancelled(self.task_id);
                cx.emit(AppStateChanged);
            });
            let notice = released_connection_notice(release_applied, &self.operation.target.name);
            let restore_failed = restore_error.is_some();
            if let (Some(message), Some(cause)) = (notice.as_ref(), restore_error) {
                report_error(
                    UserFacingError::new(ErrorKind::Driver, message.clone()).with_cause(cause),
                    cx,
                );
            }
            self.sidebar.update(cx, |sidebar, cx| {
                if let Some(message) = notice
                    && !restore_failed
                {
                    sidebar.pending_toast = Some(PendingToast {
                        message,
                        is_error: true,
                    });
                }
                sidebar.refresh_tree(cx);
            });
            return;
        }

        let details = build_drop_task_details(&self.operation.target, None);
        let cancelled_label = crate::labels::schema_drop_cancelled_label();

        self.app_state.update(cx, |state, cx| {
            state.fail_task_with_details(self.task_id, cancelled_label.clone(), details);
            cx.emit(AppStateChanged);
        });

        self.sidebar.update(cx, |sidebar, cx| {
            sidebar.pending_toast = Some(PendingToast {
                message: cancelled_label,
                is_error: true,
            });
            sidebar.refresh_tree(cx);
        });
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
        /// Cancelled from inside `set_active_database`, so a cancel lands
        /// while the release step runs.
        cancel_during_set_active: Option<dbflux_core::CancelToken>,
        fail_set_active: bool,
        active_calls: std::sync::Mutex<Vec<Option<String>>>,
    }

    impl ReleaseTestConnection {
        fn new() -> Arc<Self> {
            Self::build(None, false)
        }

        fn build(
            cancel_during_set_active: Option<dbflux_core::CancelToken>,
            fail_set_active: bool,
        ) -> Arc<Self> {
            Arc::new(Self {
                cancel_during_set_active,
                fail_set_active,
                active_calls: std::sync::Mutex::new(Vec::new()),
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

        fn set_active_database(&self, database: Option<&str>) -> Result<(), DbError> {
            self.active_calls
                .lock()
                .map_err(|_| DbError::NotSupported("poisoned".to_string()))?
                .push(database.map(str::to_string));

            if let Some(token) = &self.cancel_during_set_active {
                token.cancel();
            }

            if self.fail_set_active {
                return Err(DbError::NotSupported("set active refused".to_string()));
            }

            Ok(())
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

    fn app_state_with_drop_task() -> (
        AppStateEntity,
        dbflux_core::TaskId,
        dbflux_core::CancelToken,
    ) {
        use dbflux_storage::bootstrap::StorageRuntime;

        let rt = StorageRuntime::in_memory().expect("in-memory storage runtime");
        let mut state = AppStateEntity::new_with_storage_runtime(rt).expect("test app state");
        let handle = state.start_task_handle(
            dbflux_core::TaskRequest::new(dbflux_core::TaskKind::SchemaDrop, "DROP TABLE users")
                .policy(dbflux_core::CancelPolicy::WorkerAcknowledged),
        );

        (state, handle.id(), handle.token().clone())
    }

    #[test]
    fn a_cancel_before_the_ddl_stops_the_drop_and_ends_cancelled() {
        let (mut state, task_id, token) = app_state_with_drop_task();

        assert_eq!(
            state.request_cancel_task(task_id),
            dbflux_core::CancelRequestOutcome::Cancelling
        );

        assert!(
            !super::enter_drop_point_of_no_return(&mut state, task_id, &token, "late".to_string()),
            "the statement must not be sent after a cancel"
        );
        assert!(state.acknowledge_task_cancelled(task_id));
        assert_eq!(
            state.tasks().get(task_id).expect("task").status,
            dbflux_core::TaskStatus::Cancelled
        );
    }

    #[test]
    fn a_cancel_after_the_point_of_no_return_is_refused_and_the_drop_completes() {
        let (mut state, task_id, token) = app_state_with_drop_task();
        let reason = "The DROP reached the dispatch stage and can no longer be cancelled.";

        assert!(super::enter_drop_point_of_no_return(
            &mut state,
            task_id,
            &token,
            reason.to_string()
        ));

        assert_eq!(
            state.request_cancel_task(task_id),
            dbflux_core::CancelRequestOutcome::Refused {
                reason: reason.to_string()
            }
        );
        assert!(
            !token.is_cancelled(),
            "the running drop must not see a cancel"
        );
        let snapshot = state.tasks().get(task_id).expect("task");
        assert!(!snapshot.is_cancellable);
        assert_eq!(snapshot.cancel_unavailable_reason.as_deref(), Some(reason));

        state.complete_task(task_id);
        assert_eq!(
            state.tasks().get(task_id).expect("task").status,
            dbflux_core::TaskStatus::Completed,
            "a drop that committed never shows as cancelled"
        );
    }

    #[test]
    fn a_cancel_before_the_release_step_skips_the_ddl() {
        let token = dbflux_core::CancelToken::new();
        token.cancel();

        let preparation = super::prepare_drop_release(DatabaseDropReleasePlan::None, &token);

        let super::DropPreparation::Finished(outcome) = preparation else {
            panic!("a cancelled drop finishes before the statement");
        };
        assert!(matches!(
            *outcome,
            super::DropExecutionOutcome::Cancelled { .. }
        ));
    }

    #[test]
    fn a_cancel_during_a_failed_release_ends_cancelled_not_failed() {
        let token = dbflux_core::CancelToken::new();
        let connection = ReleaseTestConnection::build(Some(token.clone()), true);
        let plan = DatabaseDropReleasePlan::ActiveDatabase {
            database: "analytics".to_string(),
            connection,
        };

        let super::DropPreparation::Finished(outcome) = super::prepare_drop_release(plan, &token)
        else {
            panic!("a failed release finishes before the statement");
        };

        assert!(
            matches!(
                *outcome,
                super::DropExecutionOutcome::Cancelled {
                    release_applied: false,
                    ..
                }
            ),
            "the user cancelled and no DDL ran"
        );
    }

    #[test]
    fn a_failed_release_without_a_cancel_still_fails() {
        let token = dbflux_core::CancelToken::new();
        let connection = ReleaseTestConnection::build(None, true);
        let plan = DatabaseDropReleasePlan::ActiveDatabase {
            database: "analytics".to_string(),
            connection,
        };

        let super::DropPreparation::Finished(outcome) = super::prepare_drop_release(plan, &token)
        else {
            panic!("a failed release finishes before the statement");
        };

        assert!(matches!(
            *outcome,
            super::DropExecutionOutcome::Failed { .. }
        ));
    }

    #[test]
    fn a_released_active_database_is_prepared_for_restoration() {
        let token = dbflux_core::CancelToken::new();
        let connection = ReleaseTestConnection::build(None, false);
        let plan = DatabaseDropReleasePlan::ActiveDatabase {
            database: "analytics".to_string(),
            connection: connection.clone(),
        };

        let super::DropPreparation::Ready {
            database_release_applied,
            active_release,
        } = super::prepare_drop_release(plan, &token)
        else {
            panic!("a successful release is ready for the statement");
        };

        assert!(database_release_applied);
        let release = active_release.expect("the cleared active database is remembered");
        assert_eq!(release.database, "analytics");
        assert_eq!(
            *connection.active_calls.lock().expect("calls"),
            vec![None::<String>]
        );
    }

    #[test]
    fn a_cancel_after_the_active_database_release_restores_the_driver() {
        let connection = ReleaseTestConnection::build(None, false);
        let release = super::ActiveDatabaseRelease {
            database: "analytics".to_string(),
            connection: connection.clone(),
        };

        assert!(
            !super::undo_active_database_release(Some(release), true).release_applied,
            "nothing is left released once the driver is restored"
        );
        assert_eq!(
            *connection.active_calls.lock().expect("calls"),
            vec![Some("analytics".to_string())]
        );
    }

    #[test]
    fn a_driver_that_refuses_the_restore_keeps_the_release_notice() {
        let connection = ReleaseTestConnection::build(None, true);
        let release = super::ActiveDatabaseRelease {
            database: "analytics".to_string(),
            connection,
        };

        let refused = super::undo_active_database_release(Some(release), true);
        assert!(refused.release_applied);
        assert!(
            refused.error.is_some(),
            "the refusal reaches the foreground"
        );
        assert!(
            super::undo_active_database_release(None, true).release_applied,
            "a release with nothing to restore still stands"
        );
        assert!(!super::undo_active_database_release(None, false).release_applied);
    }

    #[test]
    fn a_cancel_that_won_after_the_release_tells_the_user_to_reconnect() {
        let notice = super::released_connection_notice(true, "analytics")
            .expect("the released connection is reported");

        assert!(notice.contains("analytics"), "{notice}");
        assert_eq!(super::released_connection_notice(false, "analytics"), None);
    }

    fn drop_run(
        state: &gpui::Entity<AppStateEntity>,
        sidebar: &gpui::Entity<Sidebar>,
        cx: &mut gpui::VisualTestContext,
    ) -> super::DropRun {
        let profile_id = uuid::Uuid::new_v4();
        let handle = state.update(cx, |state, _| {
            state.start_task_handle(
                dbflux_core::TaskRequest::new(
                    dbflux_core::TaskKind::SchemaDrop,
                    "DROP TABLE users",
                )
                .policy(dbflux_core::CancelPolicy::WorkerAcknowledged),
            )
        });

        super::DropRun {
            app_state: state.clone(),
            sidebar: sidebar.clone(),
            operation: super::SidebarDropOperation {
                profile_id,
                item_id: "item".to_string(),
                object_name: "users".to_string(),
                cache_database: None,
                connection: ReleaseTestConnection::new(),
                target: dbflux_core::SchemaDropTarget::new(
                    dbflux_core::SchemaObjectKind::Table,
                    "users",
                ),
                task_target: dbflux_core::TaskTarget {
                    profile_id,
                    database: None,
                },
                task_description: "DROP TABLE users".to_string(),
                is_database: false,
            },
            options: super::DropOptions::default(),
            task_id: handle.id(),
            cancel_token: handle.token().clone(),
        }
    }

    fn assert_reported_once_without_a_manual_toast(
        state: &gpui::Entity<AppStateEntity>,
        sidebar: &gpui::Entity<Sidebar>,
        task_id: dbflux_core::TaskId,
        cx: &mut gpui::VisualTestContext,
    ) {
        state.read_with(cx, |state, _| {
            assert_eq!(state.unread_error_count, 1, "reported exactly once");
            let task = state.tasks().get(task_id).expect("task");
            assert!(matches!(task.status, dbflux_core::TaskStatus::Failed(_)));
        });
        sidebar.read_with(cx, |sidebar, _| {
            assert!(
                sidebar.pending_toast.is_none(),
                "the central reporter owns the toast"
            );
        });
    }

    #[gpui::test]
    async fn a_second_drop_on_the_same_target_is_refused_until_the_first_stops(
        cx: &mut gpui::TestAppContext,
    ) {
        let state = crate::table_loading::object_tree_adapter_tests::test_app_state(cx);
        let (sidebar, cx) =
            cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));
        let run = drop_run(&state, &sidebar, cx);

        state.update(cx, |state, _| {
            let first = super::start_drop_task(state, &run.operation).expect("first drop starts");
            let refused = super::start_drop_task(state, &run.operation)
                .err()
                .expect("same target is busy");
            assert_eq!(refused.existing, first.id());

            state.request_cancel_task(first.id());
            assert_eq!(
                state.tasks().get(first.id()).expect("task").status,
                dbflux_core::TaskStatus::Cancelling
            );
            assert!(
                super::start_drop_task(state, &run.operation).is_err(),
                "a cancelling DDL may still change the schema"
            );

            assert!(state.acknowledge_task_cancelled(first.id()));
            assert!(super::start_drop_task(state, &run.operation).is_ok());
        });
    }

    #[gpui::test]
    async fn a_server_rejected_drop_goes_through_the_error_reporter(cx: &mut gpui::TestAppContext) {
        let state = crate::table_loading::object_tree_adapter_tests::test_app_state(cx);
        let (sidebar, cx) =
            cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));
        let run = drop_run(&state, &sidebar, cx);

        cx.update(|_, cx| run.finish_failed("permission denied".to_string(), None, cx));

        assert_reported_once_without_a_manual_toast(&state, &sidebar, run.task_id, cx);
    }

    #[gpui::test]
    async fn a_drop_that_fails_before_the_release_goes_through_the_error_reporter(
        cx: &mut gpui::TestAppContext,
    ) {
        let state = crate::table_loading::object_tree_adapter_tests::test_app_state(cx);
        let (sidebar, cx) =
            cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));
        let run = drop_run(&state, &sidebar, cx);

        cx.update(|_, cx| run.fail_before_release("no connection".to_string(), cx));

        assert_reported_once_without_a_manual_toast(&state, &sidebar, run.task_id, cx);
    }

    #[gpui::test]
    async fn a_cancelled_drop_without_a_notice_keeps_the_queued_toast(
        cx: &mut gpui::TestAppContext,
    ) {
        let state = crate::table_loading::object_tree_adapter_tests::test_app_state(cx);
        let (sidebar, cx) =
            cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));
        let run = drop_run(&state, &sidebar, cx);
        sidebar.update(cx, |sidebar, _| {
            sidebar.pending_toast = Some(dbflux_ui_base::toast::PendingToast {
                message: "connection released".to_string(),
                is_error: true,
            });
        });
        state.update(cx, |state, _| {
            state.request_cancel_task(run.task_id);
        });

        // Read inside the same update: the next render takes the slot.
        cx.update(|_, cx| {
            run.finish_cancelled(None, false, None, cx);

            assert_eq!(
                sidebar
                    .read(cx)
                    .pending_toast
                    .as_ref()
                    .map(|toast| toast.message.as_str()),
                Some("connection released")
            );
        });
        state.read_with(cx, |state, _| {
            assert_eq!(
                state.tasks().get(run.task_id).expect("task").status,
                dbflux_core::TaskStatus::Cancelled
            );
        });
    }

    #[gpui::test]
    async fn a_failed_active_database_restore_is_reported_once_with_the_guidance(
        cx: &mut gpui::TestAppContext,
    ) {
        let state = crate::table_loading::object_tree_adapter_tests::test_app_state(cx);
        let (sidebar, cx) =
            cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));
        let run = drop_run(&state, &sidebar, cx);
        state.update(cx, |state, _| {
            state.request_cancel_task(run.task_id);
        });

        let ((), events) = dbflux_ui_base::user_error::capture::capture_user_error_events(|| {
            cx.update(|_, cx| {
                run.finish_cancelled(None, true, Some("driver refused".to_string()), cx);
            });
        });

        assert_eq!(events.len(), 1, "reported exactly once: {events:?}");
        let event = &events[0];
        assert!(
            event
                .get("message")
                .is_some_and(|message| message.contains(&run.operation.target.name)),
            "the reconnection guidance names the database: {event:?}"
        );
        assert_eq!(
            event.get("cause").map(String::as_str),
            Some("driver refused")
        );
        sidebar.read_with(cx, |sidebar, _| {
            assert!(
                sidebar.pending_toast.is_none(),
                "no manual toast next to the reported error"
            );
        });
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
