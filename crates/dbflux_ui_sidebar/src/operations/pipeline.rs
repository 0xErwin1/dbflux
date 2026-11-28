use crate::operations::connect_stages::{
    ConnectAttempt, ConnectOutcome, ConnectStage, StagePlanInput, plan_stages,
};
use crate::*;
use dbflux_core::observability::actions::{CONNECTION_CONNECT, CONNECTION_CONNECT_FAILED};
use dbflux_core::{
    CancelPolicy, Connection, HookContext, HookPhase, PipelineState, SchemaLoadingStrategy,
    SchemaSnapshot, TableInfo, TaskKind, TaskRequest, TaskTarget,
};
use dbflux_storage::error::StorageError;
use dbflux_ui_base::hook_phase_runner::{HookPhaseState, run_hook_phase};
use dbflux_ui_base::schema_snapshot_manager::CaptureOutcome;
use dbflux_ui_base::toast::PendingToast;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use std::sync::Arc;

/// Why a pipeline connect stopped before it started.
pub(super) enum PrepareStop {
    /// A benign concurrency skip, logged only.
    Skipped(String),
    /// A preparation failure the caller still has to show.
    Failed(String),
    /// A failure already reported through `report_error`.
    Reported,
    /// Too many background tasks are running to start another connect.
    LimitReached,
}

fn discover_capture_tables(
    connection: &dyn Connection,
    database: Option<&str>,
    initial_schema: Option<&SchemaSnapshot>,
) -> Result<Vec<TableInfo>, StorageError> {
    let database = database.ok_or_else(|| {
        StorageError::Data("cannot capture schema without a resolved database".to_string())
    })?;
    match connection.schema_for_database(database) {
        Ok(schema) => Ok(schema.tables),
        Err(dbflux_core::DbError::NotSupported(_))
            if connection.schema_loading_strategy() == SchemaLoadingStrategy::SingleDatabase =>
        {
            let schema = initial_schema.ok_or_else(|| {
                StorageError::Data(format!(
                    "initial schema unavailable for database '{database}'"
                ))
            })?;
            match &schema.structure {
                dbflux_core::DataStructure::Relational(relational) => {
                    let mut tables = relational.tables.clone();
                    for nested in &relational.schemas {
                        for table in &nested.tables {
                            let mut table = table.clone();
                            if table.schema.is_none() {
                                table.schema = Some(nested.name.clone());
                            }
                            tables.push(table);
                        }
                    }
                    Ok(tables)
                }
                _ => Err(StorageError::Data(
                    "relational schema unavailable for capture".to_string(),
                )),
            }
        }
        Err(error) => Err(StorageError::Data(format!(
            "schema discovery failed for database '{database}': {error}"
        ))),
    }
}

fn capture_connected_schema(
    connection: &dyn Connection,
    repo: Arc<dbflux_storage::repositories::sch_schema_snapshots::SchemaSnapshotRepo>,
    profile_id: &str,
    database: Option<&str>,
    initial_schema: Option<&SchemaSnapshot>,
    retention: usize,
) -> Result<Vec<TableInfo>, StorageError> {
    let tables = discover_capture_tables(connection, database, initial_schema)?;
    let mut manager = dbflux_ui_base::SchemaSnapshotManager::new(repo);
    let outcome = manager.capture_deep(connection, profile_id, database, &tables, retention)?;
    let id = match outcome {
        CaptureOutcome::Inserted { id } => id.to_string(),
        CaptureOutcome::Deduped { existing_id } => existing_id,
    };
    manager.deep_details_by_id(&id, &tables)
}

fn hydrate_current_capture(
    state: &mut dbflux_ui_base::AppStateEntity,
    profile_id: Uuid,
    database: &str,
    witness: &Arc<dyn Connection>,
    details: Vec<TableInfo>,
) {
    let is_current = state
        .connections()
        .get(&profile_id)
        .is_some_and(|connected| {
            Arc::ptr_eq(&connected.connection, witness)
                && connected.active_database.as_deref().or_else(|| {
                    connected
                        .schema
                        .as_ref()
                        .and_then(|schema| schema.current_database())
                }) == Some(database)
        });
    if !is_current {
        return;
    }
    for table in details {
        if state.needs_table_details(profile_id, database, table.schema.as_deref(), &table.name) {
            state.set_table_details(
                profile_id,
                database.to_string(),
                table.schema.clone(),
                table.name.clone(),
                table,
            );
        }
    }
}

struct ConnectSnapshotCapture {
    app_state: gpui::Entity<dbflux_ui_base::AppStateEntity>,
    profile_id: Uuid,
    witness: Arc<dyn Connection>,
    repo: Arc<dbflux_storage::repositories::sch_schema_snapshots::SchemaSnapshotRepo>,
    database: Option<String>,
    schema: Option<SchemaSnapshot>,
    retention: usize,
    token: dbflux_storage::repositories::sch_schema_snapshots::ConnectCaptureToken,
    #[cfg(test)]
    pre_capture_pause: Option<std::sync::mpsc::Sender<()>>,
    #[cfg(test)]
    completion_pause: Option<std::sync::mpsc::Sender<()>>,
}

async fn run_connect_snapshot_capture(capture: ConnectSnapshotCapture, cx: &mut gpui::AsyncApp) {
    let ConnectSnapshotCapture {
        app_state,
        profile_id,
        witness,
        repo,
        database,
        schema,
        retention,
        token,
        #[cfg(test)]
        pre_capture_pause,
        #[cfg(test)]
        completion_pause,
    } = capture;
    let profile_id_string = profile_id.to_string();
    let capture_connection = Arc::clone(&witness);
    #[cfg(test)]
    if let Some(entered) = pre_capture_pause {
        entered.send(()).expect("pre-capture pause receiver");
        cx.background_executor()
            .timer(std::time::Duration::from_secs(60))
            .await;
    }
    let (database, capture_result) = cx
        .background_executor()
        .spawn(async move {
            let database = database.or_else(|| capture_connection.active_database());
            let result = repo.with_current_connect_capture(&token, || {
                capture_connected_schema(
                    &*capture_connection,
                    Arc::clone(&repo),
                    &profile_id_string,
                    database.as_deref(),
                    schema.as_ref(),
                    retention,
                )
            });
            (database, result)
        })
        .await;

    #[cfg(test)]
    if let Some(entered) = completion_pause {
        entered.send(()).expect("capture completion pause receiver");
        cx.background_executor()
            .timer(std::time::Duration::from_secs(60))
            .await;
    }

    match capture_result {
        Err(error) => report_error_async(
            UserFacingError::new(
                ErrorKind::Storage,
                crate::labels::schema_snapshot_failed_label(&error.to_string()),
            ),
            cx,
        ),
        Ok(None) => {}
        Ok(Some(details)) => {
            if let Some(database) = database {
                cx.update(|cx| {
                    app_state.update(cx, |state, _| {
                        hydrate_current_capture(state, profile_id, &database, &witness, details);
                    });
                });
            }
        }
    }
}

fn pipeline_stage_task_detail_line(state: &PipelineState) -> Option<String> {
    crate::labels::pipeline_stage_label(state).map(|description| format!("> {description}"))
}

/// Everything `connect_via_pipeline` needs once the attempt's task and
/// stages exist.
struct PreparedConnect {
    input: dbflux_core::PipelineInput,
    profile_name: String,
    driver: Arc<dyn dbflux_core::DbDriver>,
    keyring_password: Option<dbflux_core::secrecy::SecretString>,
    pre_connect_hooks: Vec<dbflux_core::ConnectionHook>,
    post_connect_hooks: Vec<dbflux_core::ConnectionHook>,
    hook_context: HookContext,
    attempt: ConnectAttempt,
}

/// Starts the Connect parent task of an attempt, which is also the
/// duplicate check: the task manager refuses a second connect for the same
/// profile. The task claims the connection target, so the sidebar spinner
/// and the retry guards read the same record.
pub(super) fn begin_connect_attempt(
    state: &mut dbflux_ui_base::AppStateEntity,
    profile_id: Uuid,
    label: String,
) -> Result<dbflux_core::TaskHandle, PrepareStop> {
    let target = TaskTarget {
        profile_id,
        database: None,
    };

    if state.is_task_target_claimed(&target) {
        return Err(PrepareStop::Skipped(
            crate::labels::connection_already_pending_toast_label(),
        ));
    }

    if state.is_background_task_limit_reached() {
        return Err(PrepareStop::LimitReached);
    }

    let handle = state
        .try_start_task(
            TaskRequest::new(TaskKind::Connect, label)
                .target(target)
                .policy(CancelPolicy::WorkerAcknowledged)
                .claims_target(),
        )
        .map_err(|_| {
            PrepareStop::Skipped(crate::labels::connection_already_pending_toast_label())
        })?;

    state.clear_connect_failure(profile_id);

    Ok(handle)
}

/// Registers the stages that apply to `profile` as pending children of the
/// attempt's Connect task.
pub(super) fn register_attempt(
    state: &mut dbflux_ui_base::AppStateEntity,
    task_id: TaskId,
    profile: &dbflux_core::ConnectionProfile,
    auth_provider: Option<&str>,
    hooks: &dbflux_core::ConnectionHooks,
) -> ConnectAttempt {
    let plan = plan_stages(&StagePlanInput {
        profile,
        auth_provider,
        has_pre_connect_hooks: hooks.pre_connect.iter().any(|hook| hook.enabled),
        has_post_connect_hooks: hooks.post_connect.iter().any(|hook| hook.enabled),
    });

    ConnectAttempt::register(state, task_id, profile.id, plan)
}

/// Starts the pipeline attempt's Connect task, which runs the pipeline on
/// its own cancel token, and registers its stages.
fn prepare_connect_attempt(
    state: &mut dbflux_ui_base::AppStateEntity,
    profile_id: Uuid,
    password: Option<dbflux_core::secrecy::SecretString>,
    cx: &mut Context<dbflux_ui_base::AppStateEntity>,
) -> Result<PreparedConnect, PrepareStop> {
    let Some(profile_name) = state
        .profiles()
        .iter()
        .find(|profile| profile.id == profile_id)
        .map(|profile| profile.name.clone())
    else {
        let error = format!("Profile {} not found", profile_id);
        state.record_connect_failure(profile_id, error.clone());
        return Err(PrepareStop::Failed(error));
    };

    let handle = begin_connect_attempt(
        state,
        profile_id,
        crate::labels::pipeline_connecting_task_label(&profile_name),
    )?;
    let task_id = handle.id();

    // A failed preparation leaves no trace in the panel.
    let abandon = |state: &mut dbflux_ui_base::AppStateEntity| {
        state.tasks_mut().remove(task_id);
    };

    let (input, profile_name, driver) =
        match state.prepare_pipeline_input(profile_id, handle.token().clone()) {
            Ok(prepared) => prepared,
            Err(error) => {
                abandon(state);
                state.record_connect_failure(profile_id, error.clone());
                return Err(PrepareStop::Failed(error));
            }
        };

    let keyring_password = match password {
        Some(password) => Some(password),
        None => match dbflux_core::keyring_password_for_connect(
            &input.profile,
            driver.requires_password(),
            state.get_password(&input.profile),
        ) {
            Ok(password) => password,
            Err(error) => {
                abandon(state);
                state.record_connect_failure(profile_id, error.clone());
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        "Could not read the connection password from the system keyring",
                    )
                    .with_cause(error),
                    cx,
                );
                return Err(PrepareStop::Reported);
            }
        },
    };

    let hooks = state.resolve_profile_hooks(&input.profile);
    let auth_provider = input
        .auth_profile
        .as_ref()
        .and(input.auth_provider.as_deref())
        .map(|provider| provider.display_name().to_string());
    let attempt = register_attempt(
        state,
        task_id,
        &input.profile,
        auth_provider.as_deref(),
        &hooks,
    );
    let hook_context = HookContext::from_profile(&input.profile);
    cx.emit(dbflux_ui_base::AppStateChanged);

    Ok(PreparedConnect {
        input,
        profile_name,
        driver,
        keyring_password,
        pre_connect_hooks: hooks.pre_connect,
        post_connect_hooks: hooks.post_connect,
        hook_context,
        attempt,
    })
}

/// Follows the pipeline's progress: every state is a detail line of the
/// Connect task, and a state that starts a stage starts that stage's task.
async fn follow_pipeline_stages(
    app_state: gpui::Entity<dbflux_ui_base::AppStateEntity>,
    attempt: ConnectAttempt,
    mut watcher: dbflux_core::StateWatcher,
    cx: &mut gpui::AsyncApp,
) {
    let mut last_line: Option<String> = None;

    while watcher.changed().await.is_ok() {
        let state = watcher.borrow().clone();

        if let Some(line) = pipeline_stage_task_detail_line(&state)
            && last_line.as_ref() != Some(&line)
        {
            last_line = Some(line.clone());
            cx.update(|cx| {
                app_state.update(cx, |app_state, cx| {
                    app_state.append_task_details(attempt.task_id(), format!("{line}\n"));
                    if let Some(stage) = ConnectStage::of_state(&state) {
                        attempt.enter(app_state, stage);
                    }
                    cx.emit(AppStateChanged);
                });
            });
        }

        if matches!(
            state,
            PipelineState::Connected | PipelineState::Failed { .. } | PipelineState::Cancelled
        ) {
            break;
        }
    }
}

pub(super) fn start_stage(
    app_state: &gpui::Entity<dbflux_ui_base::AppStateEntity>,
    attempt: &ConnectAttempt,
    stage: ConnectStage,
    cx: &mut gpui::AsyncApp,
) {
    cx.update(|cx| {
        app_state.update(cx, |state, cx| {
            attempt.enter(state, stage);
            cx.emit(dbflux_ui_base::AppStateChanged);
        });
    });
}

pub(super) fn finish_stage(
    app_state: &gpui::Entity<dbflux_ui_base::AppStateEntity>,
    attempt: &ConnectAttempt,
    stage: ConnectStage,
    cx: &mut gpui::AsyncApp,
) {
    cx.update(|cx| {
        app_state.update(cx, |state, cx| {
            attempt.complete(state, stage);
            cx.emit(dbflux_ui_base::AppStateChanged);
        });
    });
}

/// Why a connect attempt failed, as the task and the user see it.
pub(super) struct ConnectFailure {
    /// The failure the Connect task and the sidebar record.
    error: String,
    /// What the toast says, when it differs from `error`.
    toast: Option<String>,
    /// A remembered password may be the reason the connect failed, so the
    /// next attempt asks again.
    forget_password: bool,
}

impl ConnectFailure {
    pub(super) fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            toast: None,
            forget_password: false,
        }
    }

    pub(super) fn toast(mut self, message: impl Into<String>) -> Self {
        self.toast = Some(message.into());
        self
    }

    pub(super) fn forgetting_password(mut self) -> Self {
        self.forget_password = true;
        self
    }
}

/// Records the failure, settles the attempt and its stages, and tells the
/// user.
pub(super) fn fail_connect(
    app_state: &gpui::Entity<dbflux_ui_base::AppStateEntity>,
    sidebar: &gpui::Entity<Sidebar>,
    attempt: &ConnectAttempt,
    failure: ConnectFailure,
    cx: &mut gpui::AsyncApp,
) {
    let ConnectFailure {
        error,
        toast,
        forget_password,
    } = failure;
    let message = toast.unwrap_or_else(|| error.clone());

    cx.update(|cx| {
        app_state.update(cx, |state, cx| {
            if forget_password {
                state.forget_connection_password(attempt.profile_id());
            }
            attempt.finish(state, ConnectOutcome::Failed(&error));
            state.record_connect_failure(attempt.profile_id(), message.clone());
            cx.emit(dbflux_ui_base::AppStateChanged);
        });

        sidebar.update(cx, |sidebar, cx| {
            sidebar.pending_toast = Some(PendingToast {
                message,
                is_error: true,
            });
            sidebar.refresh_tree(cx);
        });
    });
}

/// Settles an attempt the user cancelled: its worker has stopped, so the
/// stage and the attempt read `Cancelled` and the profile is free again.
pub(super) fn cancel_connect(
    app_state: &gpui::Entity<dbflux_ui_base::AppStateEntity>,
    sidebar: &gpui::Entity<Sidebar>,
    attempt: &ConnectAttempt,
    toast: Option<String>,
    cx: &mut gpui::AsyncApp,
) {
    cx.update(|cx| {
        app_state.update(cx, |state, cx| {
            attempt.finish(state, ConnectOutcome::Cancelled);
            cx.emit(dbflux_ui_base::AppStateChanged);
        });

        sidebar.update(cx, |sidebar, cx| {
            if let Some(message) = toast {
                sidebar.pending_toast = Some(PendingToast {
                    message,
                    is_error: true,
                });
            }
            sidebar.refresh_tree(cx);
        });
    });
}

fn record_connect_failure_audit(
    app_state: &gpui::Entity<dbflux_ui_base::AppStateEntity>,
    profile_id: Uuid,
    profile_name: &str,
    driver_name: String,
    error: &str,
    cx: &mut gpui::AsyncApp,
) {
    let failed_at_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
    cx.update(|cx| {
        let audit_service = app_state.read(cx).audit_service().clone();
        let mut event = dbflux_core::observability::EventRecord::new(
            failed_at_ms,
            dbflux_core::observability::EventSeverity::Error,
            dbflux_core::observability::EventCategory::Connection,
            dbflux_core::observability::EventOutcome::Failure,
        );
        event.action = CONNECTION_CONNECT_FAILED.as_str().to_string();
        event.actor_type = dbflux_core::observability::EventActorType::User;
        event.source_id = dbflux_core::observability::EventSourceId::Local;
        event.connection_id = Some(profile_id.to_string());
        event.driver_id = Some(driver_name);
        event.summary = format!("Connection to '{}' failed: {}", profile_name, error);
        event.error_message = Some(error.to_string());
        if let Err(e) = audit_service.record(event) {
            log::warn!("Failed to record connect failure audit event: {}", e);
        }
    });
}

impl Sidebar {
    /// Connect using the pipeline path (auth, value resolution, access, connect).
    /// `password` is the password typed into the connect prompt; it replaces
    /// the keyring lookup and serves the whole live session.
    pub(super) fn connect_via_pipeline(
        &mut self,
        profile_id: Uuid,
        password: Option<dbflux_core::secrecy::SecretString>,
        cx: &mut Context<Self>,
    ) {
        let session_password = password.clone();
        let prepared = self.app_state.update(cx, |state, cx| {
            prepare_connect_attempt(state, profile_id, password, cx)
        });

        let PreparedConnect {
            input,
            profile_name,
            driver,
            keyring_password,
            pre_connect_hooks,
            post_connect_hooks,
            hook_context,
            attempt,
        } = match prepared {
            Ok(prepared) => prepared,
            Err(stop) => {
                // Benign concurrency skips stay info-only. A real preparation
                // failure is actionable (e.g. a missing auth profile) and must
                // reach the user, not just logs.
                match stop {
                    PrepareStop::Failed(message) => {
                        log::warn!("Pipeline connect failed: {}", message);
                        self.pending_toast = Some(PendingToast {
                            message,
                            is_error: true,
                        });
                        self.refresh_tree(cx);
                        cx.notify();
                    }
                    PrepareStop::LimitReached => {
                        self.pending_toast = Some(PendingToast {
                            message: crate::labels::background_task_limit_toast_label(),
                            is_error: true,
                        });
                        self.refresh_tree(cx);
                        cx.notify();
                    }
                    PrepareStop::Reported => {
                        self.refresh_tree(cx);
                        cx.notify();
                    }
                    PrepareStop::Skipped(message) => {
                        log::info!("Pipeline connect skipped: {}", message);
                    }
                }
                return;
            }
        };
        let cancel_token = input.cancel.clone();

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();
        let (state_tx, state_rx) = dbflux_core::pipeline_state_channel();

        cx.spawn({
            let app_state = app_state.clone();
            let attempt = attempt.clone();
            let watcher = state_rx.clone();
            async move |_this, cx| follow_pipeline_stages(app_state, attempt, watcher, cx).await
        })
        .detach();

        cx.emit(SidebarEvent::PipelineStarted {
            profile_id,
            profile_name: profile_name.clone(),
            watcher: state_rx,
        });

        cx.spawn(async move |_this, cx| {
            let mut hook_warnings = Vec::new();

            start_stage(&app_state, &attempt, ConnectStage::PreConnectHooks, cx);
            match run_hook_phase(
                app_state.clone(),
                profile_id,
                profile_name.clone(),
                HookPhase::PreConnect,
                pre_connect_hooks,
                hook_context.clone(),
                Some(cancel_token.clone()),
                attempt.detached_hooks(),
                cx,
            )
            .await
            {
                HookPhaseState::Continue { warnings } => {
                    hook_warnings.extend(warnings);
                    finish_stage(&app_state, &attempt, ConnectStage::PreConnectHooks, cx);
                }
                HookPhaseState::Aborted { error } => {
                    let _ = state_tx.send(PipelineState::Failed {
                        stage: "pre_connect_hook".to_string(),
                        error: error.clone(),
                    });
                    fail_connect(&app_state, &sidebar, &attempt, ConnectFailure::new(error), cx);
                    return;
                }
                HookPhaseState::Cancelled => {
                    let _ = state_tx.send(PipelineState::Cancelled);

                    if cancel_token.is_cancelled() {
                        cancel_connect(&app_state, &sidebar, &attempt, None, cx);
                    } else {
                        fail_connect(
                            &app_state,
                            &sidebar,
                            &attempt,
                            ConnectFailure::new(crate::labels::connection_hook_cancelled_task_label())
                                .toast(crate::labels::connection_cancelled_by_hook_toast_label()),
                            cx,
                        );
                    }
                    return;
                }
            }

            let state_tx_for_pipeline = state_tx.clone();
            let driver_name = driver.display_name().to_string();

            let pipeline_result = cx
                .background_executor()
                .spawn(
                    async move { dbflux_core::run_pipeline(input, &state_tx_for_pipeline).await },
                )
                .await;

            let output = match pipeline_result {
                Ok(output) => output,
                Err(pipeline_error) => {
                    let was_cancelled = pipeline_error.stage == "cancelled";
                    let error_msg = pipeline_error.to_string();

                    if was_cancelled {
                        let _ = state_tx.send(PipelineState::Cancelled);
                    } else {
                        let _ = state_tx.send(PipelineState::Failed {
                            stage: pipeline_error.stage.clone(),
                            error: pipeline_error.source.to_string(),
                        });
                    }

                    record_connect_failure_audit(
                        &app_state,
                        profile_id,
                        &profile_name,
                        driver_name,
                        &error_msg,
                        cx,
                    );

                    if was_cancelled {
                        cancel_connect(&app_state, &sidebar, &attempt, Some(error_msg), cx);
                    } else {
                        fail_connect(&app_state, &sidebar, &attempt, ConnectFailure::new(error_msg), cx);
                    }
                    return;
                }
            };

            let resolved_profile = output.resolved_profile;
            let resolved_password = output.resolved_password;
            let access_handle = output.access_handle;

            let connect_profile = resolved_profile.clone();
            let effective_password = resolved_password.or(keyring_password);
            let overrides = dbflux_core::ConnectionOverrides::new(effective_password);
            let state_tx_for_connect = state_tx.clone();
            let driver_name_for_state = driver_name.clone();

            // The driver connect and the schema fetch are blocking calls with
            // no way to interrupt them. A cancel that arrives meanwhile
            // leaves the stage `Cancelling`; the connection is dropped below
            // and the stage reads `Cancelled` only then.
            let connect_result = cx
                .background_executor()
                .spawn(async move {
                    let _ = state_tx_for_connect.send(PipelineState::Connecting {
                        driver_name: driver_name_for_state,
                    });

                    let mut profile = connect_profile;
                    if access_handle.is_tunneled() {
                        profile
                            .config
                            .redirect_to_tunnel(access_handle.local_port());
                    }

                    let connection = driver
                        .connect_with_overrides(&profile, &overrides)
                        .map_err(|e| e.to_string())?;

                    let _ = state_tx_for_connect.send(PipelineState::FetchingSchema);

                    let schema = match connection.schema() {
                        Ok(s) => Some(s),
                        Err(e) => {
                            log::error!("Pipeline: Failed to fetch schema: {:?}", e);
                            None
                        }
                    };

                    let tunnel_handle: Option<Box<dyn std::any::Any + Send + Sync>> =
                        if access_handle.is_tunneled() {
                            Some(Box::new(access_handle))
                        } else {
                            None
                        };

                    Ok::<_, String>((profile, connection, schema, tunnel_handle))
                })
                .await;

            let (profile, connection, schema, tunnel_handle) = match connect_result {
                Ok(values) => values,
                Err(error) => {
                    let _ = state_tx.send(PipelineState::Failed {
                        stage: "driver_connect".to_string(),
                        error: error.clone(),
                    });
                    record_connect_failure_audit(
                        &app_state,
                        profile_id,
                        &profile_name,
                        driver_name,
                        &error,
                        cx,
                    );
                    fail_connect(
                        &app_state,
                        &sidebar,
                        &attempt,
                        ConnectFailure::new(error).forgetting_password(),
                        cx,
                    );
                    return;
                }
            };

            // A cancel that arrived while the driver was connecting must not
            // leave the connection open: drop it, with its tunnel, instead of
            // registering a connection the user already cancelled.
            if cancel_token.is_cancelled() {
                drop((connection, tunnel_handle));
                abandon_cancelled_connect(&app_state, &sidebar, &attempt, &state_tx, cx);
                return;
            }

            start_stage(&app_state, &attempt, ConnectStage::PostConnectHooks, cx);
            match run_hook_phase(
                app_state.clone(),
                profile_id,
                profile_name.clone(),
                HookPhase::PostConnect,
                post_connect_hooks,
                hook_context,
                Some(cancel_token.clone()),
                attempt.detached_hooks(),
                cx,
            )
            .await
            {
                HookPhaseState::Continue { warnings } => {
                    hook_warnings.extend(warnings);
                }
                HookPhaseState::Aborted { error } => {
                    let _ = state_tx.send(PipelineState::Failed {
                        stage: "post_connect_hook".to_string(),
                        error: error.clone(),
                    });
                    fail_connect(&app_state, &sidebar, &attempt, ConnectFailure::new(error), cx);
                    return;
                }
                HookPhaseState::Cancelled => {
                    let _ = state_tx.send(PipelineState::Cancelled);

                    if cancel_token.is_cancelled() {
                        cancel_connect(&app_state, &sidebar, &attempt, None, cx);
                    } else {
                        fail_connect(
                            &app_state,
                            &sidebar,
                            &attempt,
                            ConnectFailure::new(
                                crate::labels::post_connect_hook_cancelled_task_label(),
                            )
                            .toast(crate::labels::connection_cancelled_by_post_connect_hook_toast_label()),
                            cx,
                        );
                    }
                    return;
                }
            }

            // A cancel that arrived while the post-connect hooks ran must not
            // leave the connection open either.
            if cancel_token.is_cancelled() {
                drop((connection, tunnel_handle));
                abandon_cancelled_connect(&app_state, &sidebar, &attempt, &state_tx, cx);
                return;
            }

            let _ = state_tx.send(PipelineState::Connected);

            let connected_name = profile.name.clone();
            let connected_driver_id = profile.driver_id.clone();

            // Emit pipeline connection success audit event.
            let connect_success_now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
            cx.update(|cx| {
                let audit_service = app_state.read(cx).audit_service().clone();
                let mut event = dbflux_core::observability::EventRecord::new(
                    connect_success_now_ms,
                    dbflux_core::observability::EventSeverity::Info,
                    dbflux_core::observability::EventCategory::Connection,
                    dbflux_core::observability::EventOutcome::Success,
                );
                event.action = CONNECTION_CONNECT.as_str().to_string();
                event.actor_type = dbflux_core::observability::EventActorType::User;
                event.source_id = dbflux_core::observability::EventSourceId::Local;
                event.connection_id = Some(profile_id.to_string());
                event.driver_id = connected_driver_id;
                event.summary = format!("Connected to '{}'", connected_name);
                if let Err(e) = audit_service.record(event) {
                    log::warn!(
                        "Failed to record pipeline connect success audit event: {}",
                        e
                    );
                }
            });

            let connection: Arc<dyn Connection> = connection.into();
            let capture_witness = Arc::clone(&connection);
            let capture_category = connection.metadata().category;
            let capture_schema = schema.clone();
            let capture_database = schema
                .as_ref()
                .and_then(|s| s.current_database().map(str::to_string));

            let capture_ctx = if capture_category == dbflux_core::DatabaseCategory::Relational {
                Some(cx.update(|cx| {
                    let state = app_state.read(cx);
                    (
                        Arc::clone(&state.schema_snapshot_repo),
                        state.general_settings().schema_snapshot_retention,
                    )
                }))
            } else {
                None
            };

            let mut capture_token = None;
            cx.update(|cx| {
                for warning in &hook_warnings {
                    log::warn!("{}", warning);
                }

                let probe = connection.probe_write_privilege();

                app_state.update(cx, |state, cx| {
                    attempt.finish(state, ConnectOutcome::Completed);
                    state.apply_connect_profile(
                        profile,
                        connection,
                        schema,
                        tunnel_handle,
                        false,
                        probe,
                    );
                    if let Some(password) = session_password {
                        state.set_session_password(profile_id, password);
                    }
                    cx.emit(dbflux_ui_base::AppStateChanged);
                    cx.notify();
                });

                if let Some((capture_repo, _)) = &capture_ctx {
                    match capture_repo.begin_connect_capture(profile_id) {
                        Ok(token) => capture_token = Some(token),
                        Err(error) => dbflux_ui_base::user_error::report_error(
                            UserFacingError::new(
                                ErrorKind::Storage,
                                crate::labels::schema_snapshot_failed_label(&error.to_string()),
                            ),
                            cx,
                        ),
                    }
                }

                let message =
                    crate::labels::connected_toast_label(&connected_name, hook_warnings.len());

                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_toast = Some(PendingToast {
                        message,
                        is_error: false,
                    });
                    sidebar.refresh_tree(cx);
                });
            });

            if let (Some((capture_repo, capture_retention)), Some(capture_token)) =
                (capture_ctx, capture_token)
            {
                run_connect_snapshot_capture(
                    ConnectSnapshotCapture {
                        app_state,
                        profile_id,
                        witness: capture_witness,
                        repo: capture_repo,
                        token: capture_token,
                        database: capture_database,
                        schema: capture_schema,
                        retention: capture_retention,
                        #[cfg(test)]
                        pre_capture_pause: None,
                        #[cfg(test)]
                        completion_pause: None,
                    },
                    cx,
                )
                .await;
            }
        })
        .detach();
    }
}

/// Settles a connect whose task was cancelled after the connection opened:
/// the attempt reads `Cancelled` and the profile is released, so the sidebar
/// never reports a connection the user cancelled as connected.
fn abandon_cancelled_connect(
    app_state: &gpui::Entity<dbflux_ui_base::AppStateEntity>,
    sidebar: &gpui::Entity<Sidebar>,
    attempt: &ConnectAttempt,
    state_tx: &dbflux_core::StateSender,
    cx: &mut gpui::AsyncApp,
) {
    if state_tx.send(PipelineState::Cancelled).is_err() {
        log::debug!("Pipeline: no observer left for the cancelled connect");
    }

    cancel_connect(app_state, sidebar, attempt, None, cx);
}

#[cfg(test)]
mod connect_capture_tests {
    use super::{
        ConnectSnapshotCapture, capture_connected_schema, discover_capture_tables,
        hydrate_current_capture, run_connect_snapshot_capture,
    };
    use dbflux_core::{
        Connection, ConnectionProfile, DbConfig, DbError, DbKind, DbSchemaInfo, DriverMetadata,
        QueryHandle, QueryRequest, QueryResult, RelationalSchema, SchemaLoadingStrategy,
        SchemaSnapshot, SnapshotDepth, TableInfo, WritePrivilege,
    };
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_storage::repositories::sch_schema_snapshots::SchemaSnapshotRepo;
    use dbflux_ui_base::AppStateEntity;
    use gpui::{AppContext as _, TestAppContext};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    use uuid::Uuid;

    #[gpui::test]
    fn connect_capture_stale_pre_capture_cannot_prune_new_generation(cx: &mut TestAppContext) {
        let runtime = StorageRuntime::in_memory().expect("storage");
        let repo = Arc::new(SchemaSnapshotRepo::new(
            runtime.viz_connection().expect("connection"),
        ));
        let app_state = cx.update(|cx| {
            cx.new(|_| AppStateEntity::new_with_storage_runtime(runtime).expect("app state"))
        });
        let profile = ConnectionProfile::new("capture", DbConfig::default_sqlite());
        let profile_id = profile.id;
        let (entered_tx, entered_rx) = mpsc::channel();
        let (first_done_tx, first_done_rx) = mpsc::channel();
        let first: Arc<dyn Connection> = Arc::new(MockConnection {
            discovery: Ok(vec![shallow_table("old")]),
            active_database: Some("db".into()),
        });
        app_state.update(cx, |state, _| {
            state.add_profile_in_folder(profile.clone(), None);
            state.apply_connect_profile(
                profile.clone(),
                Arc::clone(&first),
                None,
                None,
                false,
                WritePrivilege::Unknown,
            );
        });
        let token1 = repo.begin_connect_capture(profile_id).expect("first token");
        let first_state = app_state.clone();
        let first_repo = Arc::clone(&repo);
        cx.update(|cx| {
            cx.spawn(async move |cx| {
                run_connect_snapshot_capture(
                    ConnectSnapshotCapture {
                        app_state: first_state,
                        profile_id,
                        witness: first,
                        repo: first_repo,
                        token: token1,
                        database: Some("db".into()),
                        schema: None,
                        retention: 1,
                        pre_capture_pause: Some(entered_tx),
                        completion_pause: None,
                    },
                    cx,
                )
                .await;
                first_done_tx.send(()).expect("first completion receiver");
            })
            .detach()
        });
        cx.run_until_parked();
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("first entered pause");
        let second: Arc<dyn Connection> = Arc::new(MockConnection {
            discovery: Ok(vec![shallow_table("new")]),
            active_database: Some("db".into()),
        });
        let second_schema = SchemaSnapshot::relational(RelationalSchema {
            databases: Vec::new(),
            current_database: Some("db".into()),
            schemas: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
        });
        app_state.update(cx, |state, _| {
            state.apply_connect_profile(
                profile,
                Arc::clone(&second),
                Some(second_schema),
                None,
                false,
                WritePrivilege::Unknown,
            );
            assert!(state.needs_table_details(profile_id, "db", Some("public"), "new"));
        });
        let token2 = repo
            .begin_connect_capture(profile_id)
            .expect("second token");
        let (second_done_tx, second_done_rx) = mpsc::channel();
        let second_state = app_state.clone();
        let second_repo = Arc::clone(&repo);
        cx.update(|cx| {
            cx.spawn(async move |cx| {
                run_connect_snapshot_capture(
                    ConnectSnapshotCapture {
                        app_state: second_state,
                        profile_id,
                        witness: second,
                        repo: second_repo,
                        token: token2,
                        database: Some("db".into()),
                        schema: None,
                        retention: 1,
                        pre_capture_pause: None,
                        completion_pause: None,
                    },
                    cx,
                )
                .await;
                second_done_tx.send(()).expect("second completion receiver");
            })
            .detach()
        });
        let second_completed = (0..200).any(|_| {
            cx.run_until_parked();
            if second_done_rx.try_recv().is_ok() {
                true
            } else {
                std::thread::sleep(Duration::from_millis(2));
                false
            }
        });
        assert!(second_completed, "second capture completed");
        cx.executor().advance_clock(Duration::from_secs(60));
        let first_completed = (0..200).any(|_| {
            cx.run_until_parked();
            first_done_rx.try_recv().is_ok()
        });
        assert!(first_completed, "first capture completed");
        let rows = repo
            .list(&profile_id.to_string(), Some("db"))
            .expect("list");
        assert_eq!(rows.len(), 1);
        let record = repo.get(&rows[0].id).expect("get").expect("deep row");
        assert_eq!(record.tables[0].name, "new");
        app_state.read_with(cx, |state, _| {
            assert!(state.connections().contains_key(&profile_id));
            assert!(!state.needs_table_details(profile_id, "db", Some("public"), "new"));
            assert_eq!(state.unread_error_count, 0);
        });
    }

    #[gpui::test]
    fn connect_capture_discovery_failure_reports_error_without_disconnect(cx: &mut TestAppContext) {
        let runtime = StorageRuntime::in_memory().expect("storage");
        let repo = Arc::new(SchemaSnapshotRepo::new(
            runtime.viz_connection().expect("connection"),
        ));
        let app_state = cx.update(|cx| {
            cx.new(|_| AppStateEntity::new_with_storage_runtime(runtime).expect("app state"))
        });
        cx.update(|cx| {
            let host = cx.new(|_| dbflux_ui_base::toast::ToastHost::new());
            cx.set_global(dbflux_ui_base::toast::ToastGlobal { host });
            cx.set_global(dbflux_ui_base::AppStateGlobal {
                entity: app_state.clone(),
            });
        });
        let profile = ConnectionProfile::new("capture", DbConfig::default_sqlite());
        let profile_id = profile.id;
        let witness: Arc<dyn Connection> = Arc::new(MockConnection {
            discovery: Err(DbError::NotSupported("discovery failed".into())),
            active_database: None,
        });
        app_state.update(cx, |state, _| {
            state.add_profile_in_folder(profile.clone(), None);
            state.apply_connect_profile(
                profile,
                Arc::clone(&witness),
                None,
                None,
                false,
                WritePrivilege::Unknown,
            );
        });
        let (done_tx, done_rx) = mpsc::channel();
        let app_state_for_capture = app_state.clone();
        cx.update(|cx| {
            cx.spawn(async move |cx| {
                run_connect_snapshot_capture(
                    ConnectSnapshotCapture {
                        app_state: app_state_for_capture,
                        profile_id,
                        witness,
                        repo: Arc::clone(&repo),
                        database: Some("db".into()),
                        schema: None,
                        retention: 10,
                        token: repo
                            .begin_connect_capture(profile_id)
                            .expect("capture token"),
                        pre_capture_pause: None,
                        completion_pause: None,
                    },
                    cx,
                )
                .await;
                done_tx.send(repo).expect("capture completion receiver");
            })
            .detach();
        });
        let repo = (0..200)
            .find_map(|_| {
                cx.executor().advance_clock(Duration::from_millis(50));
                cx.run_until_parked();
                done_rx.try_recv().ok().or_else(|| {
                    std::thread::sleep(Duration::from_millis(2));
                    None
                })
            })
            .expect("capture completed");
        assert!(
            repo.list(&profile_id.to_string(), Some("db"))
                .expect("list")
                .is_empty()
        );
        app_state.read_with(cx, |state, _| {
            assert_eq!(state.unread_error_count, 1);
            assert!(state.connections().contains_key(&profile_id));
        });
    }

    #[gpui::test]
    fn connect_capture_resolves_active_database_without_schema(cx: &mut TestAppContext) {
        let runtime = StorageRuntime::in_memory().expect("storage");
        let repo = Arc::new(SchemaSnapshotRepo::new(
            runtime.viz_connection().expect("connection"),
        ));
        let app_state = cx.update(|cx| {
            cx.new(|_| AppStateEntity::new_with_storage_runtime(runtime).expect("app state"))
        });
        cx.update(|cx| {
            let host = cx.new(|_| dbflux_ui_base::toast::ToastHost::new());
            cx.set_global(dbflux_ui_base::toast::ToastGlobal { host });
            cx.set_global(dbflux_ui_base::AppStateGlobal {
                entity: app_state.clone(),
            });
        });
        let profile = ConnectionProfile::new("configured_db", DbConfig::default_sqlite());
        let profile_id = profile.id;
        let witness: Arc<dyn Connection> = Arc::new(MockConnection {
            discovery: Ok(vec![shallow_table("users")]),
            active_database: Some("from_uri".into()),
        });
        app_state.update(cx, |state, _| {
            state.add_profile_in_folder(profile.clone(), None);
            state.apply_connect_profile(
                profile,
                Arc::clone(&witness),
                None,
                None,
                false,
                WritePrivilege::Unknown,
            );
        });
        let (done_tx, done_rx) = mpsc::channel();
        let app_state_for_capture = app_state.clone();
        cx.update(|cx| {
            cx.spawn(async move |cx| {
                run_connect_snapshot_capture(
                    ConnectSnapshotCapture {
                        app_state: app_state_for_capture,
                        profile_id,
                        witness,
                        repo: Arc::clone(&repo),
                        database: None,
                        schema: None,
                        retention: 10,
                        token: repo
                            .begin_connect_capture(profile_id)
                            .expect("capture token"),
                        pre_capture_pause: None,
                        completion_pause: None,
                    },
                    cx,
                )
                .await;
                done_tx.send(repo).expect("capture completion receiver");
            })
            .detach();
        });
        let repo = (0..200)
            .find_map(|_| {
                cx.executor().advance_clock(Duration::from_millis(50));
                cx.run_until_parked();
                done_rx.try_recv().ok().or_else(|| {
                    std::thread::sleep(Duration::from_millis(2));
                    None
                })
            })
            .expect("capture completed");
        let rows = repo
            .list(&profile_id.to_string(), Some("from_uri"))
            .expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].depth, SnapshotDepth::Deep);
        assert!(
            repo.list(&profile_id.to_string(), Some("configured_db"))
                .expect("list")
                .is_empty()
        );
        app_state.read_with(cx, |state, _| {
            assert_eq!(state.unread_error_count, 0);
            assert!(state.connections().contains_key(&profile_id));
        });
    }

    #[test]
    fn connect_capture_persists_deep_not_shallow() {
        let runtime = StorageRuntime::in_memory().expect("in-memory storage");
        let connection = runtime.viz_connection().expect("storage connection");
        let profile_id = Uuid::now_v7().to_string();
        connection
            .lock()
            .expect("storage lock")
            .execute(
                "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'capture-test')",
                rusqlite::params![profile_id],
            )
            .expect("insert profile");
        let repo = Arc::new(SchemaSnapshotRepo::new(connection));
        let mock = MockConnection {
            discovery: Ok(Vec::new()),
            active_database: None,
        };
        let details =
            capture_connected_schema(&mock, Arc::clone(&repo), &profile_id, Some("db"), None, 10)
                .expect("connect capture");
        assert!(details.is_empty());
        let rows = repo.list(&profile_id, Some("db")).expect("snapshot list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].depth, SnapshotDepth::Deep);
    }

    struct MockConnection {
        discovery: Result<Vec<TableInfo>, DbError>,
        active_database: Option<String>,
    }

    impl Connection for MockConnection {
        fn metadata(&self) -> &DriverMetadata {
            panic!("metadata is not used during capture")
        }
        fn active_database(&self) -> Option<String> {
            self.active_database.clone()
        }
        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }
        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }
        fn execute(&self, _: &QueryRequest) -> Result<QueryResult, DbError> {
            Err(DbError::NotSupported("mock".into()))
        }
        fn cancel(&self, _: &QueryHandle) -> Result<(), DbError> {
            Ok(())
        }
        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Ok(SchemaSnapshot::default())
        }
        fn kind(&self) -> DbKind {
            DbKind::Postgres
        }
        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            if matches!(&self.discovery, Err(DbError::NotSupported(message)) if message == "single database")
            {
                SchemaLoadingStrategy::SingleDatabase
            } else {
                SchemaLoadingStrategy::LazyPerDatabase
            }
        }
        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &dbflux_core::DefaultSqlDialect
        }
        fn schema_for_database(&self, _: &str) -> Result<DbSchemaInfo, DbError> {
            self.discovery
                .as_ref()
                .map(|tables| DbSchemaInfo {
                    name: "public".into(),
                    tables: tables.clone(),
                    views: Vec::new(),
                    custom_types: None,
                })
                .map_err(|error| DbError::NotSupported(error.to_string()))
        }
        fn table_creation_metadata(
            &self,
            _: &str,
            _: Option<&str>,
            table: &str,
        ) -> Result<Option<dbflux_core::TableCreationMetadata>, DbError> {
            Ok(Some(dbflux_core::TableCreationMetadata {
                schema: Some("public".into()),
                table: table.into(),
                completeness: dbflux_core::MetadataCompleteness::Complete,
                identity: None,
                primary_key: None,
                blockers: Vec::new(),
            }))
        }
        fn table_details(
            &self,
            _: &str,
            _: Option<&str>,
            table: &str,
        ) -> Result<TableInfo, DbError> {
            if table == "broken" {
                return Err(DbError::NotSupported("mock detail".into()));
            }
            let mut detail = match &self.discovery {
                Ok(tables) => tables[0].clone(),
                Err(DbError::NotSupported(message)) if message == "single database" => {
                    shallow_table(table)
                }
                Err(_) => panic!("unexpected detail lookup"),
            };
            detail.columns = Some(Vec::new());
            Ok(detail)
        }
    }

    fn shallow_table(name: &str) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            schema: Some("public".to_string()),
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
            storage_hints: None,
            pseudo_columns: Box::default(),
        }
    }

    #[test]
    fn connect_capture_discovers_tables_despite_empty_initial_schema_and_dedups() {
        let runtime = StorageRuntime::in_memory().expect("in-memory storage");
        let connection = runtime.viz_connection().expect("storage connection");
        let profile_id = Uuid::now_v7().to_string();
        connection
            .lock()
            .expect("storage lock")
            .execute(
                "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'capture-test')",
                rusqlite::params![profile_id],
            )
            .expect("insert profile");
        let repo = Arc::new(SchemaSnapshotRepo::new(connection));
        let mock = MockConnection {
            discovery: Ok(vec![shallow_table("users")]),
            active_database: None,
        };
        for _ in 0..2 {
            let details = capture_connected_schema(
                &mock,
                Arc::clone(&repo),
                &profile_id,
                Some("db"),
                None,
                10,
            )
            .expect("capture discovered table");
            assert_eq!(details.len(), 1);
            assert!(details[0].columns.is_some());
        }
        let rows = repo.list(&profile_id, Some("db")).expect("snapshot list");
        assert_eq!(
            rows.len(),
            1,
            "identical deep captures must reuse the same row"
        );
        assert_eq!(rows[0].depth, SnapshotDepth::Deep);
        let record = repo
            .get(&rows[0].id)
            .expect("snapshot read")
            .expect("snapshot row");
        assert_eq!(
            record.creation_metadata.len(),
            1,
            "driver metadata must persist"
        );
    }

    #[test]
    fn connect_capture_lookup_failure_persists_no_deep_row() {
        let runtime = StorageRuntime::in_memory().expect("in-memory storage");
        let connection = runtime.viz_connection().expect("storage connection");
        let profile_id = Uuid::now_v7().to_string();
        connection
            .lock()
            .expect("storage lock")
            .execute(
                "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'capture-test')",
                rusqlite::params![profile_id],
            )
            .expect("insert profile");
        let repo = Arc::new(SchemaSnapshotRepo::new(connection));
        let mock = MockConnection {
            discovery: Ok(vec![shallow_table("broken")]),
            active_database: None,
        };
        assert!(
            capture_connected_schema(&mock, Arc::clone(&repo), &profile_id, Some("db"), None, 10)
                .is_err()
        );
        assert!(
            repo.list(&profile_id, Some("db"))
                .expect("snapshot list")
                .is_empty()
        );
    }

    #[gpui::test]
    fn connect_capture_hydrates_only_its_current_connected_profile(cx: &mut TestAppContext) {
        let app_state = cx.update(|cx| {
            cx.new(|_| {
                AppStateEntity::new_with_storage_runtime(
                    StorageRuntime::in_memory().expect("test storage"),
                )
                .expect("test app state")
            })
        });
        let profile = ConnectionProfile::new("capture", DbConfig::default_sqlite());
        let profile_id = profile.id;
        let schema = SchemaSnapshot::relational(RelationalSchema {
            databases: Vec::new(),
            current_database: Some("db".to_string()),
            schemas: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
        });
        let first: Arc<dyn Connection> = Arc::new(MockConnection {
            discovery: Ok(vec![shallow_table("users")]),
            active_database: None,
        });
        app_state.update(cx, |state, _| {
            state.apply_connect_profile(
                profile.clone(),
                Arc::clone(&first),
                Some(schema.clone()),
                None,
                false,
                WritePrivilege::Unknown,
            );
            assert!(state.needs_table_details(profile_id, "db", Some("public"), "users"));
            hydrate_current_capture(
                state,
                profile_id,
                "db",
                &first,
                vec![shallow_table("users")],
            );
            assert!(!state.needs_table_details(profile_id, "db", Some("public"), "users"));
        });
        let replacement: Arc<dyn Connection> = Arc::new(MockConnection {
            discovery: Ok(vec![shallow_table("users")]),
            active_database: None,
        });
        app_state.update(cx, |state, _| {
            state.apply_connect_profile(
                profile,
                Arc::clone(&replacement),
                Some(schema),
                None,
                false,
                WritePrivilege::Unknown,
            );
            assert!(state.needs_table_details(profile_id, "db", Some("public"), "users"));
            hydrate_current_capture(
                state,
                profile_id,
                "db",
                &first,
                vec![shallow_table("users")],
            );
            assert!(
                state.needs_table_details(profile_id, "db", Some("public"), "users"),
                "old connection must not seed replacement cache"
            );
            hydrate_current_capture(
                state,
                profile_id,
                "db",
                &replacement,
                vec![shallow_table("users")],
            );
            assert!(!state.needs_table_details(profile_id, "db", Some("public"), "users"));
        });
    }

    #[gpui::test]
    fn connect_capture_replacement_after_background_completion_does_not_seed_new_connection(
        cx: &mut TestAppContext,
    ) {
        let runtime = StorageRuntime::in_memory().expect("storage");
        let repo = Arc::new(SchemaSnapshotRepo::new(
            runtime.viz_connection().expect("connection"),
        ));
        let app_state = cx.update(|cx| {
            cx.new(|_| AppStateEntity::new_with_storage_runtime(runtime).expect("app state"))
        });
        let profile = ConnectionProfile::new("capture", DbConfig::default_sqlite());
        let profile_id = profile.id;
        let schema = SchemaSnapshot::relational(RelationalSchema {
            databases: Vec::new(),
            current_database: Some("db".into()),
            schemas: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
        });
        let (entered_tx, entered_rx) = mpsc::channel();
        let first: Arc<dyn Connection> = Arc::new(MockConnection {
            discovery: Ok(vec![shallow_table("users")]),
            active_database: Some("other_db".into()),
        });
        app_state.update(cx, |state, _| {
            state.add_profile_in_folder(profile.clone(), None);
            state.apply_connect_profile(
                profile.clone(),
                Arc::clone(&first),
                Some(schema.clone()),
                None,
                false,
                WritePrivilege::Unknown,
            );
        });
        let (done_tx, done_rx) = mpsc::channel();
        let app_state_for_capture = app_state.clone();
        let repo_for_capture = Arc::clone(&repo);
        cx.update(|cx| {
            cx.spawn(async move |cx| {
                run_connect_snapshot_capture(
                    ConnectSnapshotCapture {
                        app_state: app_state_for_capture,
                        profile_id,
                        witness: first,
                        repo: Arc::clone(&repo_for_capture),
                        database: Some("db".into()),
                        schema: Some(schema),
                        retention: 10,
                        token: repo_for_capture
                            .begin_connect_capture(profile_id)
                            .expect("capture token"),
                        pre_capture_pause: None,
                        completion_pause: Some(entered_tx),
                    },
                    cx,
                )
                .await;
                done_tx.send(()).expect("capture completion receiver");
            })
            .detach();
        });
        cx.run_until_parked();
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("background capture completed before replacement");
        assert!(
            done_rx.try_recv().is_err(),
            "hydration must still be pending"
        );
        let replacement: Arc<dyn Connection> = Arc::new(MockConnection {
            discovery: Ok(vec![shallow_table("users")]),
            active_database: None,
        });
        app_state.update(cx, |state, _| {
            state.apply_connect_profile(
                profile,
                Arc::clone(&replacement),
                Some(SchemaSnapshot::relational(RelationalSchema {
                    databases: Vec::new(),
                    current_database: Some("db".into()),
                    schemas: Vec::new(),
                    tables: Vec::new(),
                    views: Vec::new(),
                })),
                None,
                false,
                WritePrivilege::Unknown,
            );
            assert!(Arc::ptr_eq(
                &state
                    .connections()
                    .get(&profile_id)
                    .expect("C2 connected")
                    .connection,
                &replacement
            ));
            assert!(state.needs_table_details(profile_id, "db", Some("public"), "users"));
        });
        cx.executor().advance_clock(Duration::from_secs(60));
        let completed = (0..200).any(|_| {
            cx.run_until_parked();
            done_rx.try_recv().is_ok()
        });
        assert!(completed, "capture completed");
        let rows = repo
            .list(&profile_id.to_string(), Some("db"))
            .expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].depth, SnapshotDepth::Deep);
        assert!(
            repo.list(&profile_id.to_string(), Some("other_db"))
                .expect("list")
                .is_empty()
        );
        let record = repo.get(&rows[0].id).expect("get").expect("deep row");
        assert_eq!(record.tables.len(), 1);
        assert!(
            record.tables[0].columns.is_some(),
            "C1 persisted table details"
        );
        app_state.read_with(cx, |state, _| {
            assert!(state.connections().contains_key(&profile_id));
            assert!(state.needs_table_details(profile_id, "db", Some("public"), "users"));
            assert_eq!(state.unread_error_count, 0);
        });
    }

    #[gpui::test]
    fn connect_capture_profile_deleted_before_hydration_stays_deleted(cx: &mut TestAppContext) {
        let runtime = StorageRuntime::in_memory().expect("storage");
        let repo = Arc::new(SchemaSnapshotRepo::new(
            runtime.viz_connection().expect("connection"),
        ));
        let app_state = cx.update(|cx| {
            cx.new(|_| AppStateEntity::new_with_storage_runtime(runtime).expect("app state"))
        });
        let profile = ConnectionProfile::new("capture", DbConfig::default_sqlite());
        let profile_id = profile.id;
        let schema = SchemaSnapshot::relational(RelationalSchema {
            databases: Vec::new(),
            current_database: Some("db".into()),
            schemas: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
        });
        let witness: Arc<dyn Connection> = Arc::new(MockConnection {
            discovery: Ok(vec![shallow_table("users")]),
            active_database: None,
        });
        app_state.update(cx, |state, _| {
            state.add_profile_in_folder(profile.clone(), None);
            state.apply_connect_profile(
                profile,
                Arc::clone(&witness),
                Some(schema.clone()),
                None,
                false,
                WritePrivilege::Unknown,
            );
        });
        assert!(
            repo.profile_exists(&profile_id.to_string())
                .expect("stored profile")
        );
        let (entered_tx, entered_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let app_state_for_capture = app_state.clone();
        let repo_for_capture = Arc::clone(&repo);
        cx.update(|cx| {
            cx.spawn(async move |cx| {
                run_connect_snapshot_capture(
                    ConnectSnapshotCapture {
                        app_state: app_state_for_capture,
                        profile_id,
                        witness,
                        repo: Arc::clone(&repo_for_capture),
                        database: Some("db".into()),
                        schema: Some(schema),
                        retention: 10,
                        token: repo_for_capture
                            .begin_connect_capture(profile_id)
                            .expect("capture token"),
                        pre_capture_pause: None,
                        completion_pause: Some(entered_tx),
                    },
                    cx,
                )
                .await;
                done_tx.send(()).expect("capture completion receiver");
            })
            .detach();
        });
        cx.run_until_parked();
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("background capture completed before deletion");
        assert!(
            done_rx.try_recv().is_err(),
            "hydration must still be pending"
        );
        let rows = repo
            .list(&profile_id.to_string(), Some("db"))
            .expect("C1 capture persisted");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].depth, SnapshotDepth::Deep);
        app_state.update(cx, |state, _| {
            let index = state
                .profiles()
                .iter()
                .position(|profile| profile.id == profile_id)
                .expect("profile present before deletion");
            assert_eq!(
                state.remove_profile(index).expect("remove profile").id,
                profile_id
            );
            assert!(
                !state
                    .profiles()
                    .iter()
                    .any(|profile| profile.id == profile_id)
            );
            assert!(!state.connections().contains_key(&profile_id));
        });
        assert!(
            !repo
                .profile_exists(&profile_id.to_string())
                .expect("profile deleted from storage")
        );
        cx.executor().advance_clock(Duration::from_secs(60));
        let completed = (0..200).any(|_| {
            cx.run_until_parked();
            done_rx.try_recv().is_ok()
        });
        assert!(completed, "capture completed after deletion");
        app_state.read_with(cx, |state, _| {
            assert!(
                !state
                    .profiles()
                    .iter()
                    .any(|profile| profile.id == profile_id)
            );
            assert!(!state.connections().contains_key(&profile_id));
            assert!(
                state
                    .get_table_details(profile_id, "db", Some("public"), "users")
                    .is_none()
            );
            assert_eq!(state.unread_error_count, 0);
        });
        assert!(
            !repo
                .profile_exists(&profile_id.to_string())
                .expect("profile remains deleted")
        );
    }

    fn nested_schema() -> SchemaSnapshot {
        SchemaSnapshot::relational(RelationalSchema {
            databases: Vec::new(),
            current_database: None,
            schemas: vec![DbSchemaInfo {
                name: "public".to_string(),
                tables: vec![shallow_table("nested_users")],
                views: Vec::new(),
                custom_types: None,
            }],
            tables: Vec::new(),
            views: Vec::new(),
        })
    }

    #[test]
    fn connect_capture_unknown_single_database_refuses_false_deep_empty() {
        let runtime = StorageRuntime::in_memory().expect("storage");
        let connection = runtime.viz_connection().expect("storage connection");
        let profile_id = Uuid::now_v7().to_string();
        connection
            .lock()
            .expect("storage lock")
            .execute(
                "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'capture-test')",
                rusqlite::params![profile_id],
            )
            .expect("insert profile");
        let repo = Arc::new(SchemaSnapshotRepo::new(connection));
        let mock = MockConnection {
            discovery: Err(DbError::NotSupported("single database".into())),
            active_database: None,
        };
        assert!(
            capture_connected_schema(
                &mock,
                Arc::clone(&repo),
                &profile_id,
                None,
                Some(&nested_schema()),
                10
            )
            .is_err()
        );
        assert!(repo.list(&profile_id, None).expect("list").is_empty());
    }

    #[test]
    fn connect_capture_known_single_database_falls_back_to_nested_schema() {
        let runtime = StorageRuntime::in_memory().expect("storage");
        let connection = runtime.viz_connection().expect("storage connection");
        let profile_id = Uuid::now_v7().to_string();
        connection
            .lock()
            .expect("storage lock")
            .execute(
                "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'capture-test')",
                rusqlite::params![profile_id],
            )
            .expect("insert profile");
        let repo = Arc::new(SchemaSnapshotRepo::new(connection));
        let mock = MockConnection {
            discovery: Err(DbError::NotSupported("single database".into())),
            active_database: None,
        };
        let schema = nested_schema();
        let details = capture_connected_schema(
            &mock,
            Arc::clone(&repo),
            &profile_id,
            Some("target"),
            Some(&schema),
            10,
        )
        .expect("nested capture");
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].name, "nested_users");
        assert_eq!(details[0].schema.as_deref(), Some("public"));
        let rows = repo.list(&profile_id, Some("target")).expect("list");
        assert_eq!(rows.len(), 1);
        let record = repo.get(&rows[0].id).expect("get").expect("row");
        assert_eq!(record.depth, SnapshotDepth::Deep);
        assert_eq!(record.creation_metadata.len(), 1);
    }

    #[test]
    fn connect_capture_known_single_database_refuses_failed_initial_schema() {
        let mock = MockConnection {
            discovery: Err(DbError::NotSupported("single database".into())),
            active_database: None,
        };
        let error = discover_capture_tables(&mock, Some("target"), None)
            .expect_err("missing schema is not an empty database");
        assert!(error.to_string().contains("initial schema unavailable"));
    }

    #[test]
    fn connect_capture_rejects_unknown_database_and_discovery_errors() {
        let unknown = MockConnection {
            discovery: Ok(Vec::new()),
            active_database: None,
        };
        assert!(discover_capture_tables(&unknown, None, None).is_err());
        let failed = MockConnection {
            discovery: Err(DbError::NotSupported("lookup failed".into())),
            active_database: None,
        };
        assert!(
            discover_capture_tables(&failed, Some("db"), None)
                .unwrap_err()
                .to_string()
                .contains("lookup failed")
        );
    }
}
