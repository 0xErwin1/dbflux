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
enum PrepareStop {
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

/// Starts the attempt's Connect task, which is also the duplicate check:
/// the task manager refuses a second connect for the same profile. The
/// pipeline runs on the task's own cancel token.
fn prepare_connect_attempt(
    state: &mut dbflux_ui_base::AppStateEntity,
    profile_id: Uuid,
    password: Option<dbflux_core::secrecy::SecretString>,
    cx: &mut Context<dbflux_ui_base::AppStateEntity>,
) -> Result<PreparedConnect, PrepareStop> {
    let handle = admit_connect(state, profile_id)?;
    let task_id = handle.id();

    // A failed preparation leaves no trace in the panel.
    let abandon = |state: &mut dbflux_ui_base::AppStateEntity| {
        state.tasks_mut().remove(task_id);
        state.finish_pending_operation(profile_id, None);
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

    let keyring_password = match resolve_connect_password(state, &input, &*driver, password) {
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
    };

    let hooks = state.resolve_profile_hooks(&input.profile);
    let attempt = register_connect_stages(state, &input, &hooks, &handle);
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

/// Checks the profile can connect now and starts its Connect task. The
/// sidebar spinner still reads the pending-operation set, so that is claimed
/// too.
fn admit_connect(
    state: &mut dbflux_ui_base::AppStateEntity,
    profile_id: Uuid,
) -> Result<dbflux_core::TaskHandle, PrepareStop> {
    let target = TaskTarget {
        profile_id,
        database: None,
    };

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

    if state.is_task_busy(TaskKind::Connect, &target) {
        return Err(PrepareStop::Skipped(
            crate::labels::connection_already_pending_toast_label(),
        ));
    }

    if state.is_background_task_limit_reached() {
        return Err(PrepareStop::LimitReached);
    }

    let handle = state
        .try_start_task(
            TaskRequest::new(
                TaskKind::Connect,
                crate::labels::pipeline_connecting_task_label(&profile_name),
            )
            .target(target)
            .policy(CancelPolicy::WorkerAcknowledged),
        )
        .map_err(|_| {
            PrepareStop::Skipped(crate::labels::connection_already_pending_toast_label())
        })?;

    state.start_pending_operation(profile_id, None);
    state.clear_connect_failure(profile_id);

    Ok(handle)
}

/// The password the connect uses unless the pipeline resolves one: the one
/// typed into the prompt, else the keyring's.
fn resolve_connect_password(
    state: &dbflux_ui_base::AppStateEntity,
    input: &dbflux_core::PipelineInput,
    driver: &dyn dbflux_core::DbDriver,
    typed: Option<dbflux_core::secrecy::SecretString>,
) -> Result<Option<dbflux_core::secrecy::SecretString>, String> {
    match typed {
        Some(password) => Ok(Some(password)),
        None => dbflux_core::keyring_password_for_connect(
            &input.profile,
            driver.requires_password(),
            state.get_password(&input.profile),
        ),
    }
}

/// Registers the stages that apply to the profile as children of the
/// attempt's task.
fn register_connect_stages(
    state: &mut dbflux_ui_base::AppStateEntity,
    input: &dbflux_core::PipelineInput,
    hooks: &dbflux_core::ConnectionHooks,
    attempt_task: &dbflux_core::TaskHandle,
) -> ConnectAttempt {
    let auth_provider = input
        .auth_profile
        .as_ref()
        .and(input.auth_provider.as_deref())
        .map(|provider| provider.display_name().to_string());
    let plan = plan_stages(&StagePlanInput {
        profile: &input.profile,
        auth_provider: auth_provider.as_deref(),
        has_pre_connect_hooks: hooks.pre_connect.iter().any(|hook| hook.enabled),
        has_post_connect_hooks: hooks.post_connect.iter().any(|hook| hook.enabled),
    });

    ConnectAttempt::register(
        state,
        attempt_task.id(),
        attempt_task.token(),
        input.profile.id,
        plan,
    )
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

fn start_stage(
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

fn finish_stage(
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
struct ConnectFailure {
    kind: ErrorKind,
    /// The failure the Connect task records.
    error: String,
    /// What the user is told and the sidebar records, when it differs from
    /// `error`.
    message: Option<String>,
    /// The stage that failed, when the failure names one.
    stage: Option<ConnectStage>,
    /// A remembered password may be the reason the connect failed, so the
    /// next attempt asks again.
    forget_password: bool,
}

impl ConnectFailure {
    fn new(kind: ErrorKind, error: impl Into<String>) -> Self {
        Self {
            kind,
            error: error.into(),
            message: None,
            stage: None,
            forget_password: false,
        }
    }

    fn message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    fn at(mut self, stage: ConnectStage) -> Self {
        self.stage = Some(stage);
        self
    }

    fn forgetting_password(mut self) -> Self {
        self.forget_password = true;
        self
    }
}

/// The kind of error a pipeline stage failure is reported as.
fn error_kind_of_stage(stage: &str) -> ErrorKind {
    match stage {
        "authentication" => ErrorKind::Auth,
        "access" => ErrorKind::Network,
        "pre_connect_hook" | "post_connect_hook" => ErrorKind::Hook,
        _ => ErrorKind::Driver,
    }
}

/// Tells the watchers of the pipeline where it stands. Nobody watching is
/// not an error: the attempt settles whether or not anyone follows it.
fn publish_state(state_tx: &dbflux_core::StateSender, state: PipelineState) {
    if state_tx.send(state).is_err() {
        log::debug!("Pipeline: no observer left for the connect state");
    }
}

/// Records the failure, settles the attempt and its stages, and tells the
/// user once, through the central error report.
fn fail_connect(
    app_state: &gpui::Entity<dbflux_ui_base::AppStateEntity>,
    sidebar: &gpui::Entity<Sidebar>,
    attempt: &ConnectAttempt,
    failure: ConnectFailure,
    cx: &mut gpui::AsyncApp,
) {
    let ConnectFailure {
        kind,
        error,
        message,
        stage,
        forget_password,
    } = failure;
    let message = message.unwrap_or_else(|| error.clone());

    cx.update(|cx| {
        app_state.update(cx, |state, cx| {
            if forget_password {
                state.forget_connection_password(attempt.profile_id());
            }
            attempt.finish(
                state,
                ConnectOutcome::Failed {
                    error: &error,
                    stage,
                },
            );
            state.record_connect_failure(attempt.profile_id(), message.clone());
            cx.emit(dbflux_ui_base::AppStateChanged);
        });

        sidebar.update(cx, |sidebar, cx| sidebar.refresh_tree(cx));
        report_error(UserFacingError::new(kind, message), cx);
    });
}

/// Settles an attempt the user cancelled: its worker has stopped, so the
/// stage and the attempt read `Cancelled` and the profile is free again.
fn cancel_connect(
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

/// What the connect worker needs, shared by its phases. A phase that stops
/// the attempt settles it itself, through `fail_connect`, `cancel_connect` or
/// the completion in `install_connection`, and returns `None`, so every path
/// releases the attempt in one place.
struct ConnectRun {
    app_state: gpui::Entity<dbflux_ui_base::AppStateEntity>,
    sidebar: gpui::Entity<Sidebar>,
    attempt: ConnectAttempt,
    state_tx: dbflux_core::StateSender,
    cancel_token: dbflux_core::CancelToken,
    profile_id: Uuid,
    profile_name: String,
    hook_context: HookContext,
}

/// What differs between the pre- and post-connect hook phases.
struct HookStage {
    phase: HookPhase,
    stage: ConnectStage,
    /// The stage identifier the pipeline state reports a failure under.
    failure_stage: &'static str,
    /// Whether the stage settles when its hooks finish; the post-connect
    /// stage settles with the attempt.
    finish_on_success: bool,
    cancelled_task_label: String,
    cancelled_toast_label: String,
}

/// A connection ready to register, with what keeps it alive.
struct RegisteredConnection {
    profile: dbflux_core::ConnectionProfile,
    connection: Arc<dyn Connection>,
    schema: Option<SchemaSnapshot>,
    tunnel_handle: Option<Box<dyn std::any::Any + Send + Sync>>,
}

/// A driver connection opened by the worker, with what it holds.
struct ConnectedDriver {
    profile: dbflux_core::ConnectionProfile,
    connection: Box<dyn Connection>,
    schema: Option<SchemaSnapshot>,
    tunnel_handle: Option<Box<dyn std::any::Any + Send + Sync>>,
}

/// The driver connect and the schema fetch are blocking calls with no way to
/// interrupt them. A cancel that arrives while the driver connects skips the
/// schema fetch; the caller drops the connection and its tunnel and the
/// stage reads `Cancelled` only then.
fn connect_driver_blocking(
    driver: &dyn dbflux_core::DbDriver,
    mut profile: dbflux_core::ConnectionProfile,
    overrides: &dbflux_core::ConnectionOverrides,
    access_handle: dbflux_core::AccessHandle,
    state_tx: &dbflux_core::StateSender,
    cancel_token: &dbflux_core::CancelToken,
) -> Result<ConnectedDriver, String> {
    publish_state(
        state_tx,
        PipelineState::Connecting {
            driver_name: driver.display_name().to_string(),
        },
    );

    if access_handle.is_tunneled() {
        profile
            .config
            .redirect_to_tunnel(access_handle.local_port());
    }

    let connection = driver
        .connect_with_overrides(&profile, overrides)
        .map_err(|e| e.to_string())?;

    let schema = if cancel_token.is_cancelled() {
        None
    } else {
        publish_state(state_tx, PipelineState::FetchingSchema);

        match connection.schema() {
            Ok(schema) => Some(schema),
            Err(e) => {
                log::error!("Pipeline: Failed to fetch schema: {:?}", e);
                None
            }
        }
    };

    let tunnel_handle: Option<Box<dyn std::any::Any + Send + Sync>> = if access_handle.is_tunneled()
    {
        Some(Box::new(access_handle))
    } else {
        None
    };

    Ok(ConnectedDriver {
        profile,
        connection,
        schema,
        tunnel_handle,
    })
}

/// Everything the worker consumes while it runs the phases.
struct ConnectPlan {
    input: dbflux_core::PipelineInput,
    driver: Arc<dyn dbflux_core::DbDriver>,
    keyring_password: Option<dbflux_core::secrecy::SecretString>,
    session_password: Option<dbflux_core::secrecy::SecretString>,
    pre_connect_hooks: Vec<dbflux_core::ConnectionHook>,
    post_connect_hooks: Vec<dbflux_core::ConnectionHook>,
}

impl ConnectRun {
    async fn execute(self, plan: ConnectPlan, cx: &mut gpui::AsyncApp) {
        let ConnectPlan {
            input,
            driver,
            keyring_password,
            session_password,
            pre_connect_hooks,
            post_connect_hooks,
        } = plan;
        let driver_name = driver.display_name().to_string();
        let mut hook_warnings = Vec::new();

        let pre_connect = HookStage {
            phase: HookPhase::PreConnect,
            stage: ConnectStage::PreConnectHooks,
            failure_stage: "pre_connect_hook",
            finish_on_success: true,
            cancelled_task_label: crate::labels::connection_hook_cancelled_task_label(),
            cancelled_toast_label: crate::labels::connection_cancelled_by_hook_toast_label(),
        };
        let Some(warnings) = self.run_hooks(pre_connect, pre_connect_hooks, cx).await else {
            return;
        };
        hook_warnings.extend(warnings);

        let Some(output) = self.run_pipeline(input, &driver_name, cx).await else {
            return;
        };

        let Some(connected) = self
            .connect_driver(driver, driver_name, output, keyring_password, cx)
            .await
        else {
            return;
        };

        let post_connect = HookStage {
            phase: HookPhase::PostConnect,
            stage: ConnectStage::PostConnectHooks,
            failure_stage: "post_connect_hook",
            finish_on_success: false,
            cancelled_task_label: crate::labels::post_connect_hook_cancelled_task_label(),
            cancelled_toast_label:
                crate::labels::connection_cancelled_by_post_connect_hook_toast_label(),
        };
        let Some(warnings) = self.run_hooks(post_connect, post_connect_hooks, cx).await else {
            return;
        };
        hook_warnings.extend(warnings);

        // A cancel that arrived while the post-connect hooks ran must not
        // leave the connection open either.
        if self.cancel_token.is_cancelled() {
            drop((connected.connection, connected.tunnel_handle));
            abandon_cancelled_connect(
                &self.app_state,
                &self.sidebar,
                &self.attempt,
                &self.state_tx,
                cx,
            );
            return;
        }

        self.install_connection(connected, hook_warnings, session_password, cx)
            .await;
    }

    /// Runs one hook phase. `None` when the phase stopped the attempt, which
    /// is settled by then; otherwise the warnings the hooks raised.
    async fn run_hooks(
        &self,
        spec: HookStage,
        hooks: Vec<dbflux_core::ConnectionHook>,
        cx: &mut gpui::AsyncApp,
    ) -> Option<Vec<String>> {
        start_stage(&self.app_state, &self.attempt, spec.stage, cx);

        let outcome = run_hook_phase(
            self.app_state.clone(),
            self.profile_id,
            self.profile_name.clone(),
            spec.phase,
            hooks,
            self.hook_context.clone(),
            Some(self.cancel_token.clone()),
            self.attempt.detached_hooks(),
            cx,
        )
        .await;

        match outcome {
            HookPhaseState::Continue { warnings } => {
                if spec.finish_on_success {
                    finish_stage(&self.app_state, &self.attempt, spec.stage, cx);
                }
                Some(warnings)
            }
            HookPhaseState::Aborted { error } => {
                publish_state(
                    &self.state_tx,
                    PipelineState::Failed {
                        stage: spec.failure_stage.to_string(),
                        error: error.clone(),
                    },
                );
                fail_connect(
                    &self.app_state,
                    &self.sidebar,
                    &self.attempt,
                    ConnectFailure::new(ErrorKind::Hook, error).at(spec.stage),
                    cx,
                );
                None
            }
            HookPhaseState::Cancelled => {
                publish_state(&self.state_tx, PipelineState::Cancelled);

                if self.cancel_token.is_cancelled() {
                    cancel_connect(&self.app_state, &self.sidebar, &self.attempt, None, cx);
                } else {
                    fail_connect(
                        &self.app_state,
                        &self.sidebar,
                        &self.attempt,
                        ConnectFailure::new(ErrorKind::Hook, spec.cancelled_task_label)
                            .message(spec.cancelled_toast_label)
                            .at(spec.stage),
                        cx,
                    );
                }
                None
            }
        }
    }

    /// Runs authentication, value resolution and access. `None` when the
    /// pipeline stopped the attempt, which is settled by then.
    async fn run_pipeline(
        &self,
        input: dbflux_core::PipelineInput,
        driver_name: &str,
        cx: &mut gpui::AsyncApp,
    ) -> Option<dbflux_core::PipelineOutput> {
        let state_tx = self.state_tx.clone();
        let pipeline_result = cx
            .background_executor()
            .spawn(async move { dbflux_core::run_pipeline(input, &state_tx).await })
            .await;

        let pipeline_error = match pipeline_result {
            Ok(output) => return Some(output),
            Err(pipeline_error) => pipeline_error,
        };

        let error_msg = pipeline_error.to_string();

        if pipeline_error.stage == "cancelled" {
            publish_state(&self.state_tx, PipelineState::Cancelled);
            record_connect_failure_audit(
                &self.app_state,
                self.profile_id,
                &self.profile_name,
                driver_name.to_string(),
                &error_msg,
                cx,
            );
            cancel_connect(
                &self.app_state,
                &self.sidebar,
                &self.attempt,
                Some(error_msg),
                cx,
            );
            return None;
        }

        // An error that raced a cancel the user asked for is that cancel: it
        // reads Cancelled, with no failure state, audit or toast.
        if self.cancel_token.is_cancelled() {
            abandon_cancelled_connect(
                &self.app_state,
                &self.sidebar,
                &self.attempt,
                &self.state_tx,
                cx,
            );
            return None;
        }

        publish_state(
            &self.state_tx,
            PipelineState::Failed {
                stage: pipeline_error.stage.clone(),
                error: pipeline_error.source.to_string(),
            },
        );
        record_connect_failure_audit(
            &self.app_state,
            self.profile_id,
            &self.profile_name,
            driver_name.to_string(),
            &error_msg,
            cx,
        );

        let mut failure =
            ConnectFailure::new(error_kind_of_stage(&pipeline_error.stage), error_msg);
        if let Some(stage) = ConnectStage::of_failure(&pipeline_error.stage) {
            failure = failure.at(stage);
        }
        fail_connect(&self.app_state, &self.sidebar, &self.attempt, failure, cx);
        None
    }

    /// Connects the driver and fetches the schema. `None` when the attempt
    /// stopped here, which is settled by then.
    async fn connect_driver(
        &self,
        driver: Arc<dyn dbflux_core::DbDriver>,
        driver_name: String,
        output: dbflux_core::PipelineOutput,
        keyring_password: Option<dbflux_core::secrecy::SecretString>,
        cx: &mut gpui::AsyncApp,
    ) -> Option<ConnectedDriver> {
        let dbflux_core::PipelineOutput {
            resolved_profile,
            resolved_password,
            access_handle,
            ..
        } = output;
        let overrides =
            dbflux_core::ConnectionOverrides::new(resolved_password.or(keyring_password));
        let state_tx = self.state_tx.clone();
        let cancel_token = self.cancel_token.clone();

        let connect_result = cx
            .background_executor()
            .spawn(async move {
                connect_driver_blocking(
                    &*driver,
                    resolved_profile,
                    &overrides,
                    access_handle,
                    &state_tx,
                    &cancel_token,
                )
            })
            .await;

        let connected = match connect_result {
            Ok(connected) => connected,
            Err(error) => {
                // A driver error after the user cancelled is that cancel, not
                // a failed password.
                if self.cancel_token.is_cancelled() {
                    abandon_cancelled_connect(
                        &self.app_state,
                        &self.sidebar,
                        &self.attempt,
                        &self.state_tx,
                        cx,
                    );
                    return None;
                }

                publish_state(
                    &self.state_tx,
                    PipelineState::Failed {
                        stage: "driver_connect".to_string(),
                        error: error.clone(),
                    },
                );
                record_connect_failure_audit(
                    &self.app_state,
                    self.profile_id,
                    &self.profile_name,
                    driver_name,
                    &error,
                    cx,
                );
                fail_connect(
                    &self.app_state,
                    &self.sidebar,
                    &self.attempt,
                    ConnectFailure::new(ErrorKind::Driver, error)
                        .at(ConnectStage::ConnectingDriver)
                        .forgetting_password(),
                    cx,
                );
                return None;
            }
        };

        // A cancel that arrived while the driver was connecting must not
        // leave the connection open: drop it, with its tunnel, instead of
        // registering a connection the user already cancelled.
        if self.cancel_token.is_cancelled() {
            drop((connected.connection, connected.tunnel_handle));
            abandon_cancelled_connect(
                &self.app_state,
                &self.sidebar,
                &self.attempt,
                &self.state_tx,
                cx,
            );
            return None;
        }

        Some(connected)
    }

    /// Registers the connection with the app state, settles the attempt and
    /// tells the user. Returns the token of the schema capture it began.
    fn register_connection(
        &self,
        registered: RegisteredConnection,
        hook_warnings: &[String],
        session_password: Option<dbflux_core::secrecy::SecretString>,
        capture_repo: Option<
            Arc<dbflux_storage::repositories::sch_schema_snapshots::SchemaSnapshotRepo>,
        >,
        cx: &mut gpui::AsyncApp,
    ) -> Option<dbflux_storage::repositories::sch_schema_snapshots::ConnectCaptureToken> {
        let RegisteredConnection {
            profile,
            connection,
            schema,
            tunnel_handle,
        } = registered;
        let profile_id = self.profile_id;
        let connected_name = profile.name.clone();
        let mut capture_token = None;

        cx.update(|cx| {
            for warning in hook_warnings {
                log::warn!("{}", warning);
            }

            let probe = connection.probe_write_privilege();

            self.app_state.update(cx, |state, cx| {
                self.attempt.finish(state, ConnectOutcome::Completed);
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

            if let Some(repo) = &capture_repo {
                match repo.begin_connect_capture(profile_id) {
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

            self.sidebar.update(cx, |sidebar, cx| {
                sidebar.pending_toast = Some(PendingToast {
                    message,
                    is_error: false,
                });
                sidebar.refresh_tree(cx);
            });
        });

        capture_token
    }

    /// Records the success, registers the connection and captures its schema.
    async fn install_connection(
        &self,
        connected: ConnectedDriver,
        hook_warnings: Vec<String>,
        session_password: Option<dbflux_core::secrecy::SecretString>,
        cx: &mut gpui::AsyncApp,
    ) {
        let ConnectedDriver {
            profile,
            connection,
            schema,
            tunnel_handle,
        } = connected;
        let profile_id = self.profile_id;
        let app_state = &self.app_state;

        publish_state(&self.state_tx, PipelineState::Connected);

        let connected_name = profile.name.clone();
        record_connect_success_audit(
            app_state,
            profile_id,
            &connected_name,
            profile.driver_id.clone(),
            cx,
        );

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

        let capture_token = self.register_connection(
            RegisteredConnection {
                profile,
                connection,
                schema,
                tunnel_handle,
            },
            &hook_warnings,
            session_password,
            capture_ctx.as_ref().map(|(repo, _)| Arc::clone(repo)),
            cx,
        );

        if let (Some((repo, retention)), Some(token)) = (capture_ctx, capture_token) {
            run_connect_snapshot_capture(
                ConnectSnapshotCapture {
                    app_state: app_state.clone(),
                    profile_id,
                    witness: capture_witness,
                    repo,
                    token,
                    database: capture_database,
                    schema: capture_schema,
                    retention,
                    #[cfg(test)]
                    pre_capture_pause: None,
                    #[cfg(test)]
                    completion_pause: None,
                },
                cx,
            )
            .await;
        }
    }
}

fn record_connect_success_audit(
    app_state: &gpui::Entity<dbflux_ui_base::AppStateEntity>,
    profile_id: Uuid,
    profile_name: &str,
    driver_id: Option<String>,
    cx: &mut gpui::AsyncApp,
) {
    let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
    cx.update(|cx| {
        let audit_service = app_state.read(cx).audit_service().clone();
        let mut event = dbflux_core::observability::EventRecord::new(
            now_ms,
            dbflux_core::observability::EventSeverity::Info,
            dbflux_core::observability::EventCategory::Connection,
            dbflux_core::observability::EventOutcome::Success,
        );
        event.action = CONNECTION_CONNECT.as_str().to_string();
        event.actor_type = dbflux_core::observability::EventActorType::User;
        event.source_id = dbflux_core::observability::EventSourceId::Local;
        event.connection_id = Some(profile_id.to_string());
        event.driver_id = driver_id;
        event.summary = format!("Connected to '{}'", profile_name);
        if let Err(e) = audit_service.record(event) {
            log::warn!(
                "Failed to record pipeline connect success audit event: {}",
                e
            );
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
                self.settle_prepare_stop(stop, cx);
                return;
            }
        };

        self.refresh_tree(cx);

        let (state_tx, state_rx) = dbflux_core::pipeline_state_channel();

        cx.spawn({
            let app_state = self.app_state.clone();
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

        let run = ConnectRun {
            app_state: self.app_state.clone(),
            sidebar: cx.entity().clone(),
            attempt,
            state_tx,
            cancel_token: input.cancel.clone(),
            profile_id,
            profile_name,
            hook_context,
        };
        let plan = ConnectPlan {
            input,
            driver,
            keyring_password,
            session_password,
            pre_connect_hooks,
            post_connect_hooks,
        };

        cx.spawn(async move |_this, cx| run.execute(plan, cx).await)
            .detach();
    }

    /// Tells the user why a connect did not start, when it is theirs to know.
    fn settle_prepare_stop(&mut self, stop: PrepareStop, cx: &mut Context<Self>) {
        match stop {
            // A real preparation failure is actionable (e.g. a missing auth
            // profile) and must reach the user, not just logs.
            PrepareStop::Failed(message) => {
                log::warn!("Pipeline connect failed: {}", message);
                report_error(UserFacingError::new(ErrorKind::Config, message), cx);
                self.refresh_tree(cx);
                cx.notify();
            }
            PrepareStop::LimitReached => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::User,
                        crate::labels::background_task_limit_toast_label(),
                    ),
                    cx,
                );
                self.refresh_tree(cx);
                cx.notify();
            }
            PrepareStop::Reported => {
                self.refresh_tree(cx);
                cx.notify();
            }
            // Benign concurrency skips stay info-only.
            PrepareStop::Skipped(message) => {
                log::info!("Pipeline connect skipped: {}", message);
            }
        }
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
    publish_state(state_tx, PipelineState::Cancelled);

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
