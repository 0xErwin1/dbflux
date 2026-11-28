use crate::operations::connect_stages::{ConnectAttempt, ConnectOutcome, ConnectStage};
use crate::operations::pipeline::{
    ConnectFailure, PrepareStop, begin_connect_attempt, cancel_connect, fail_connect, finish_stage,
    register_attempt, start_stage,
};
use crate::*;
use dbflux_app::{ExternalDriverDiagnostic, ExternalDriverStage};
use dbflux_core::observability::actions::{
    CONNECTION_CONNECT, CONNECTION_CONNECT_FAILED, CONNECTION_CONNECTING, CONNECTION_DISCONNECT,
};
use dbflux_core::secrecy::SecretString;
use dbflux_core::{DatabaseConnection, DbSchemaInfo, HookPhase, PrepareConnectError, TaskKind};
use dbflux_ssh::is_passphrase_required_error_str;
use dbflux_ui_base::hook_phase_runner::{DetachedHookScope, HookPhaseState, run_hook_phase};
use dbflux_ui_base::toast::PendingToast;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use std::sync::Arc;

pub(crate) struct HeldDatabaseConnection {
    pub(crate) database: String,
    pub(crate) connection: DatabaseConnection,
    pub(crate) cached_schema: Option<DbSchemaInfo>,
    pub(crate) previous_active_database: Option<String>,
}

fn format_external_driver_stage_message(
    stage: &ExternalDriverStage,
    driver_id: &str,
    socket_id: &str,
    summary: &str,
) -> String {
    crate::labels::external_driver_unavailable_label(stage, driver_id, socket_id, summary)
}

pub(crate) fn format_connect_prepare_error(
    error: &PrepareConnectError,
    diagnostic: Option<&ExternalDriverDiagnostic>,
) -> String {
    match (error, diagnostic) {
        (
            PrepareConnectError::ExternalDriverUnavailable {
                driver_id,
                socket_id,
            },
            Some(diagnostic),
        ) => {
            let mut message = format_external_driver_stage_message(
                &diagnostic.stage,
                driver_id,
                socket_id,
                &diagnostic.summary,
            );

            if let Some(details) = diagnostic.details.as_deref()
                && !details.trim().is_empty()
            {
                message.push_str("\n\n");
                message.push_str(details);
            }

            message
        }
        _ => error.to_string(),
    }
}

pub(crate) fn connect_prepare_error_toast(
    error: &PrepareConnectError,
    diagnostic: Option<&ExternalDriverDiagnostic>,
) -> PendingToast {
    PendingToast {
        message: format_connect_prepare_error(error, diagnostic),
        is_error: true,
    }
}

pub(crate) fn try_close_held_database_connection(
    held_connection: &mut HeldDatabaseConnection,
) -> Result<(), String> {
    if let Err(error) = held_connection.connection.connection.cancel_active() {
        log::debug!(
            "Could not cancel active query before dropping database {}: {:?}",
            held_connection.database,
            error
        );
    }

    let Some(connection) = Arc::get_mut(&mut held_connection.connection.connection) else {
        return Err(format!(
            "Cannot drop database '{}' while DBFlux still has active references to its connection",
            held_connection.database
        ));
    };

    connection.close().map_err(|error| {
        format!(
            "Failed to release DBFlux connection for database '{}': {}",
            held_connection.database, error
        )
    })
}

pub(crate) fn retain_database_cache_entries<T>(
    entries: &mut HashMap<SchemaCacheKey, Vec<T>>,
    database: &str,
) -> HashMap<SchemaCacheKey, Vec<T>> {
    let existing = std::mem::take(entries);
    let (removed, kept): (Vec<_>, Vec<_>) = existing
        .into_iter()
        .partition(|(key, _)| key.database == database);

    *entries = kept.into_iter().collect();
    removed.into_iter().collect()
}

/// Waits for the connection teardown thread so post-disconnect hooks observe
/// a fully closed connection instead of racing the driver's cancel/close work
/// (e.g. the kill connection MySQL opens over the tunnel).
///
/// The wait is bounded so a wedged teardown cannot stall the disconnect task
/// forever; hitting the deadline returns a warning for the hook-warning toast.
/// Cancellation short-circuits the wait and defers to the hook phase runner's
/// own cancellation handling.
async fn wait_for_connection_teardown(
    teardown: std::thread::JoinHandle<Result<(), dbflux_core::DbError>>,
    cancel_token: &dbflux_core::CancelToken,
    cx: &gpui::AsyncApp,
) -> Option<String> {
    const TEARDOWN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

    let deadline = std::time::Instant::now() + TEARDOWN_DEADLINE;

    while !teardown.is_finished() {
        if cancel_token.is_cancelled() {
            return None;
        }

        if std::time::Instant::now() >= deadline {
            log::warn!(
                "Connection teardown still running after {}s; post-disconnect hooks proceed anyway",
                TEARDOWN_DEADLINE.as_secs()
            );
            return Some(format!(
                "connection teardown was still running after {}s; post-disconnect hooks may have run while the connection was closing",
                TEARDOWN_DEADLINE.as_secs()
            ));
        }

        cx.background_executor()
            .timer(std::time::Duration::from_millis(50))
            .await;
    }

    match teardown.join() {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(format!("connection cleanup failed: {error}")),
        Err(_) => Some("connection teardown thread panicked".to_string()),
    }
}

/// Everything the direct connect needs once its task and stages exist.
struct DirectConnect {
    params: dbflux_core::ConnectProfileParams,
    profile_name: String,
    pre_connect_hooks: Vec<dbflux_core::ConnectionHook>,
    post_connect_hooks: Vec<dbflux_core::ConnectionHook>,
    hook_context: dbflux_core::HookContext,
    attempt: ConnectAttempt,
    cancel_token: dbflux_core::CancelToken,
}

/// Why a connect attempt stopped before it started.
enum ConnectStop {
    /// Another attempt for the same profile is in flight; nothing to report.
    AlreadyConnecting,
    Failed(PendingToast),
}

impl Sidebar {
    pub fn connect_to_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        let (prompt, remembered_password) = {
            let state = self.app_state.read(cx);
            let idle = !state.connections().contains_key(&profile_id)
                && !state.is_operation_pending(profile_id, None);

            match state.remembered_connection_password(profile_id) {
                Some(password) => (None, Some(password)),
                None => (
                    idle.then(|| state.connection_password_prompt(profile_id))
                        .flatten(),
                    None,
                ),
            }
        };

        if let Some(prompt) = prompt {
            cx.emit(SidebarEvent::RequestConnectionPassword { prompt });
            return;
        }

        self.connect_to_profile_inner(profile_id, None, false, remembered_password, cx);
    }

    /// Disconnect a live session and reconnect once the connection has fully
    /// cleared. Used by the "Reconnect now" prompt that fires after the user
    /// edits a profile that is currently connected — the new settings only take
    /// effect on a fresh connect, but the pending-operation map blocks a
    /// back-to-back call, so we wait for the disconnect to drain first.
    pub fn reconnect_profile_after_edit(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        if !self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id)
        {
            // Not connected — just connect.
            self.connect_to_profile(profile_id, cx);
            return;
        }

        self.disconnect_profile(profile_id, cx);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            // Poll until the connection has been removed from the live map
            // (capped at ~5s to avoid hanging if the disconnect stalls).
            for _ in 0..50 {
                cx.background_executor()
                    .timer(std::time::Duration::from_millis(100))
                    .await;

                let cleared = cx.update(|cx| {
                    let still_connected =
                        app_state.read(cx).connections().contains_key(&profile_id);
                    let still_pending = app_state.read(cx).is_operation_pending(profile_id, None);
                    !still_connected && !still_pending
                });

                if cleared {
                    break;
                }
            }

            cx.update(|cx| {
                sidebar.update(cx, |sidebar, cx| {
                    sidebar.connect_to_profile(profile_id, cx);
                });
            });
        })
        .detach();
    }

    /// Connect with the password the user typed into the connect prompt.
    ///
    /// The password serves this connection's whole live session. With
    /// `remember`, it is also kept in memory until DBFlux exits, so later
    /// connects skip the prompt; it is never written to the keyring.
    pub fn connect_to_profile_with_password(
        &mut self,
        profile_id: Uuid,
        password: SecretString,
        remember: bool,
        cx: &mut Context<Self>,
    ) {
        if remember {
            self.app_state.update(cx, |state, _cx| {
                state.remember_connection_password(profile_id, password.clone());
            });
        }

        self.connect_to_profile_inner(profile_id, None, false, Some(password), cx);
    }

    /// Retry a connection with an explicit SSH passphrase supplied by the user via the modal.
    ///
    /// If this attempt also fails with a passphrase error, the modal will reopen showing
    /// an "Incorrect passphrase" banner (`last_attempt_failed = true`).
    pub fn connect_to_profile_with_passphrase(
        &mut self,
        profile_id: Uuid,
        passphrase: String,
        cx: &mut Context<Self>,
    ) {
        self.pending_tunnel_auth_profile_id = None;
        let password = self.pending_tunnel_auth_password.take();
        // Pass last_attempt_failed=true so that if this attempt also fails with a passphrase
        // error, the re-opened modal shows the "Incorrect passphrase" error banner.
        self.connect_to_profile_inner(profile_id, Some(passphrase), true, password, cx);
    }

    fn connect_to_profile_inner(
        &mut self,
        profile_id: Uuid,
        override_passphrase: Option<String>,
        last_attempt_failed: bool,
        password: Option<SecretString>,
        cx: &mut Context<Self>,
    ) {
        let uses_pipeline = {
            let app_state = self.app_state.read(cx);

            app_state
                .profiles()
                .iter()
                .find(|p| p.id == profile_id)
                .is_some_and(|p| app_state.profile_uses_connect_pipeline(p))
        };

        if uses_pipeline {
            self.connect_via_pipeline(profile_id, password, cx);
            return;
        }

        let passphrase_ref: Option<&str> = override_passphrase.as_deref();

        let DirectConnect {
            params,
            profile_name,
            pre_connect_hooks,
            post_connect_hooks,
            hook_context,
            attempt,
            cancel_token,
        } = match self.app_state.update(cx, |state, cx| {
            if state.is_operation_pending(profile_id, None) {
                return Err(ConnectStop::AlreadyConnecting);
            }

            let result = state.prepare_connect_profile_with_secrets(
                profile_id,
                passphrase_ref,
                password.clone(),
            );

            let diagnostic = result
                .as_ref()
                .err()
                .and_then(|error| error.socket_id())
                .and_then(|socket_id| state.external_driver_diagnostic(socket_id))
                .cloned();

            match result {
                Ok(p) => {
                    let name = p.profile.name.clone();
                    let handle = match begin_connect_attempt(
                        state,
                        profile_id,
                        crate::labels::connecting_task_label(&name),
                    ) {
                        Ok(handle) => handle,
                        Err(PrepareStop::LimitReached) => {
                            return Err(ConnectStop::Failed(PendingToast {
                                message: crate::labels::background_task_limit_toast_label(),
                                is_error: true,
                            }));
                        }
                        Err(_) => return Err(ConnectStop::AlreadyConnecting),
                    };
                    let hook_execution = p.prepare_hooks(state.resolve_profile_hooks(&p.profile));
                    let attempt = register_attempt(
                        state,
                        handle.id(),
                        &p.profile,
                        None,
                        &hook_execution.hooks,
                    );
                    cx.emit(AppStateChanged);

                    Ok(DirectConnect {
                        params: p,
                        profile_name: name,
                        pre_connect_hooks: hook_execution.hooks.pre_connect,
                        post_connect_hooks: hook_execution.hooks.post_connect,
                        hook_context: hook_execution.context,
                        attempt,
                        cancel_token: handle.token().clone(),
                    })
                }
                Err(error) => {
                    let toast = connect_prepare_error_toast(&error, diagnostic.as_ref());
                    state.record_connect_failure(profile_id, toast.message.clone());
                    Err(ConnectStop::Failed(toast))
                }
            }
        }) {
            Ok(p) => p,
            Err(ConnectStop::AlreadyConnecting) => {
                // A second request for a profile that is already
                // connecting (double click, repeated Enter, menu) is not
                // a failure: the attempt in flight serves it.
                log::info!(
                    "Connect skipped: {}",
                    crate::labels::connection_already_pending_toast_label()
                );
                return;
            }
            Err(ConnectStop::Failed(toast)) => {
                self.pending_toast = Some(toast);
                self.refresh_tree(cx);
                cx.notify();
                return;
            }
        };

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();

        let session_password = password;

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
                    fail_connect(&app_state, &sidebar, &attempt, ConnectFailure::new(error), cx);
                    return;
                }
                HookPhaseState::Cancelled => {
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

            let connecting_profile_id = profile_id;
            let connecting_profile_name = profile_name.clone();
            let connecting_driver_id = hook_context.db_kind.clone();
            let connecting_database = hook_context.database.clone();
            let connect_start_ms = dbflux_core::chrono::Utc::now().timestamp_millis();

            cx.update(|cx| {
                app_state.update(cx, |state, _cx| {
                    if let Err(e) = state.audit_service().record(
                        dbflux_core::observability::EventRecord::new(
                            connect_start_ms,
                            dbflux_core::observability::EventSeverity::Info,
                            dbflux_core::observability::EventCategory::Connection,
                            dbflux_core::observability::EventOutcome::Pending,
                        )
                        .with_typed_action(CONNECTION_CONNECTING)
                        .with_summary(format!("Connecting to '{}'", connecting_profile_name))
                        .with_origin(dbflux_core::observability::EventOrigin::local())
                        .with_actor_id("local")
                        .with_connection_context(
                            connecting_profile_id.to_string(),
                            connecting_database.as_deref().unwrap_or(""),
                            connecting_driver_id.clone(),
                        ),
                    ) {
                        log::warn!("Failed to record connection_connecting audit event: {}", e);
                    }
                });
            });

            // The driver connect and the schema fetch are one blocking call
            // with no way to interrupt it. A cancel that arrives meanwhile
            // leaves the stage `Cancelling`; the result is dropped below and
            // the stage reads `Cancelled` only then.
            start_stage(&app_state, &attempt, ConnectStage::ConnectingDriver, cx);
            let result = cx
                .background_executor()
                .spawn(async move { params.execute(Some(dbflux_app::proxy::create_proxy_tunnel)) })
                .await;

            if cancel_token.is_cancelled() {
                log::info!("Connection task was cancelled, discarding result");
                drop(result);
                cancel_connect(&app_state, &sidebar, &attempt, None, cx);
                return;
            }

            let connected = match result {
                Ok(value) => value,
                Err(error) => {
                    let error_clone = error.clone();
                    let profile_name_for_audit = profile_name.clone();
                    let profile_id_for_audit = profile_id;
                    let is_passphrase_error = is_passphrase_required_error_str(&error);
                    let password_for_retry = session_password.clone();

                    cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            // Emit connection failure audit event.
                            let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
                            let driver_id = state
                                .profiles()
                                .iter()
                                .find(|p| p.id == profile_id_for_audit)
                                .map(|p| p.driver_id.clone())
                                .unwrap_or_default();
                            let mut event = dbflux_core::observability::EventRecord::new(
                                now_ms,
                                dbflux_core::observability::EventSeverity::Error,
                                dbflux_core::observability::EventCategory::Connection,
                                dbflux_core::observability::EventOutcome::Failure,
                            );
                            event.actor_type = dbflux_core::observability::EventActorType::User;
                            event.source_id = dbflux_core::observability::EventSourceId::Local;
                            event.connection_id = Some(profile_id_for_audit.to_string());
                            event.driver_id = driver_id;
                            event.error_message = Some(error_clone.clone());
                            let event = event
                                .with_typed_action(CONNECTION_CONNECT_FAILED)
                                .with_summary(format!(
                                    "Connection to '{}' failed: {}",
                                    profile_name_for_audit, error_clone
                                ))
                                .with_actor_id("local");
                            if let Err(e) = state.audit_service().record(event) {
                                log::warn!(
                                    "Failed to record connection.failure audit event: {}",
                                    e
                                );
                            }

                            if !is_passphrase_error {
                                // A remembered password may be the reason the
                                // connect failed; ask again next time.
                                state.forget_connection_password(profile_id);
                            }
                            attempt.finish(state, ConnectOutcome::Failed(&error_clone));
                            state.record_connect_failure(profile_id, error_clone);
                            cx.emit(AppStateChanged);
                            cx.notify();
                        });

                        if is_passphrase_error {
                            // Evict any cached passphrase — it is wrong (or was never supplied).
                            // This prevents a stale cached passphrase from blocking future prompts.
                            app_state.update(cx, |state, _cx| {
                                if let Some(tunnel_id) = state.ssh_tunnel_id_for_profile(profile_id)
                                    && let Ok(mut guard) = state.session_passphrase_vault.write()
                                {
                                    guard.remove(&tunnel_id);
                                }
                            });

                            // Look up the SSH tunnel profile info for display in the modal.
                            let tunnel_info = app_state
                                .read(cx)
                                .ssh_tunnel_id_for_profile(profile_id)
                                .and_then(|tunnel_id| {
                                    let state = app_state.read(cx);
                                    state.ssh_tunnel_profile(tunnel_id).map(|t| {
                                        (
                                            tunnel_id,
                                            t.name.clone(),
                                            t.config.host.clone(),
                                            t.config.port,
                                            t.config.user.clone(),
                                        )
                                    })
                                });

                            if let Some((tunnel_id, tunnel_name, host, port, user)) = tunnel_info {
                                sidebar.update(cx, |sidebar, cx| {
                                    sidebar.pending_tunnel_auth_profile_id = Some(profile_id);
                                    sidebar.pending_tunnel_auth_password = password_for_retry;
                                    cx.emit(SidebarEvent::RequestTunnelAuth {
                                        profile_id,
                                        tunnel_id,
                                        tunnel_name,
                                        host,
                                        port,
                                        user,
                                        last_attempt_failed,
                                    });
                                    sidebar.refresh_tree(cx);
                                });
                            } else {
                                // Tunnel info not found — fall back to error toast.
                                sidebar.update(cx, |sidebar, cx| {
                                    sidebar.pending_toast = Some(PendingToast {
                                        message: error,
                                        is_error: true,
                                    });
                                    sidebar.refresh_tree(cx);
                                });
                            }
                        } else {
                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.pending_toast = Some(PendingToast {
                                    message: error,
                                    is_error: true,
                                });
                                sidebar.refresh_tree(cx);
                            });
                        }
                    });
                    return;
                }
            };

            // A cancel that arrived while the driver was connecting must not
            // leave the connection open: drop it instead of registering a
            // connection the user already cancelled.
            if cancel_token.is_cancelled() {
                drop(connected);
                cancel_connect(&app_state, &sidebar, &attempt, None, cx);
                return;
            }

            start_stage(&app_state, &attempt, ConnectStage::PostConnectHooks, cx);
            match run_hook_phase(
                app_state.clone(),
                profile_id,
                profile_name,
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
                    drop(connected);
                    fail_connect(&app_state, &sidebar, &attempt, ConnectFailure::new(error), cx);
                    return;
                }
                HookPhaseState::Cancelled => {
                    drop(connected);

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
                drop(connected);
                cancel_connect(&app_state, &sidebar, &attempt, None, cx);
                return;
            }

            let connected_profile_name = connected.profile.name.clone();
            let connected_driver_id = connected.profile.driver_id.clone();

            cx.update(|cx| {
                for warning in &hook_warnings {
                    log::warn!("{}", warning);
                }

                app_state.update(cx, |state, cx| {
                    // Emit connection success audit event.
                    let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
                    let mut event = dbflux_core::observability::EventRecord::new(
                        now_ms,
                        dbflux_core::observability::EventSeverity::Info,
                        dbflux_core::observability::EventCategory::Connection,
                        dbflux_core::observability::EventOutcome::Success,
                    );
                    event.actor_type = dbflux_core::observability::EventActorType::User;
                    event.source_id = dbflux_core::observability::EventSourceId::Local;
                    event.connection_id = Some(profile_id.to_string());
                    event.driver_id = connected_driver_id.clone();
                    let event = event
                        .with_typed_action(CONNECTION_CONNECT)
                        .with_summary(format!("Connected to '{}'", connected_profile_name))
                        .with_actor_id("local");
                    if let Err(e) = state.audit_service().record(event) {
                        log::warn!("Failed to record connection.success audit event: {}", e);
                    }

                    attempt.finish(state, ConnectOutcome::Completed);
                    state.apply_connect_profile(
                        connected.profile,
                        connected.connection,
                        connected.schema,
                        connected.proxy_tunnel,
                        false,
                        connected.probe,
                    );
                    if let Some(password) = session_password {
                        state.set_session_password(profile_id, password);
                    }
                    cx.emit(AppStateChanged);
                    cx.notify();
                });

                let message = crate::labels::connected_toast_label(
                    &connected_profile_name,
                    hook_warnings.len(),
                );

                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_toast = Some(PendingToast {
                        message,
                        is_error: false,
                    });
                    sidebar.refresh_tree(cx);
                });
            });
        })
        .detach();
    }

    /// Measures one `ping` round trip for every connected profile that has
    /// no measurement yet, and forgets the profiles that are no longer
    /// connected. Driver-agnostic: it only uses [`Connection::ping`].
    ///
    /// [`Connection::ping`]: dbflux_core::Connection::ping
    pub(crate) fn sync_connection_latencies(&mut self, cx: &mut Context<Self>) {
        let connected: Vec<(Uuid, Arc<dyn dbflux_core::Connection>)> = self
            .app_state
            .read(cx)
            .connections()
            .iter()
            .map(|(profile_id, connected)| (*profile_id, connected.connection.clone()))
            .collect();

        let connected_ids: HashSet<Uuid> = connected.iter().map(|(id, _)| *id).collect();

        self.connection_latencies
            .retain(|profile_id, _| connected_ids.contains(profile_id));
        self.pending_latency_probes
            .retain(|profile_id, _| connected_ids.contains(profile_id));

        for (profile_id, connection) in connected {
            let already_measured = self.connection_latencies.contains_key(&profile_id)
                || self.pending_latency_probes.contains_key(&profile_id);

            if already_measured {
                continue;
            }

            let probe = cx.background_executor().spawn(async move {
                let started = std::time::Instant::now();
                connection.ping().map(|()| started.elapsed())
            });

            let task = cx.spawn(async move |this, cx| {
                let result = probe.await;

                let update = this.update(cx, |sidebar, cx| {
                    sidebar.pending_latency_probes.remove(&profile_id);

                    let latency = match result {
                        Ok(latency) => Some(latency),
                        Err(error) => {
                            log::debug!("Latency probe for profile {profile_id} failed: {error}");
                            None
                        }
                    };

                    sidebar.connection_latencies.insert(profile_id, latency);
                    cx.notify();
                });

                if let Err(error) = update {
                    log::debug!("Sidebar dropped before the latency probe finished: {error}");
                }
            });

            self.pending_latency_probes.insert(profile_id, task);
        }
    }

    /// User-initiated disconnect. When a query is still running on the
    /// connection, emits [`SidebarEvent::RequestActiveQueryDisconnect`] so the
    /// host can ask what to do with it; otherwise disconnects right away.
    pub fn request_disconnect(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        let has_running_query = !self
            .app_state
            .read(cx)
            .running_query_tasks(Some(profile_id))
            .is_empty();

        if has_running_query {
            cx.emit(SidebarEvent::RequestActiveQueryDisconnect { profile_id });
            return;
        }

        self.disconnect_profile(profile_id, cx);
    }

    pub fn disconnect_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        let Some(profile) = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|conn| conn.profile.clone())
        else {
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

        let profile_name = profile.name.clone();
        let hook_context = self.app_state.read(cx).build_hook_context(&profile);
        let hooks = self.app_state.read(cx).resolve_profile_hooks(&profile);

        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let task = state.start_task_for_profile(
                TaskKind::Disconnect,
                crate::labels::disconnecting_task_label(&profile_name),
                Some(profile_id),
            );
            cx.emit(AppStateChanged);
            task
        });

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();

        let detached_hook_scope = DetachedHookScope::default();

        cx.spawn(async move |_this, cx| {
            let mut hook_warnings = Vec::new();

            match run_hook_phase(
                app_state.clone(),
                profile_id,
                profile_name.clone(),
                HookPhase::PreDisconnect,
                hooks.pre_disconnect,
                hook_context.clone(),
                Some(cancel_token.clone()),
                &detached_hook_scope,
                cx,
            )
            .await
            {
                HookPhaseState::Continue { warnings } => {
                    hook_warnings.extend(warnings);
                }
                HookPhaseState::Aborted { error } => {
                    cx.update(|cx| {
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
                HookPhaseState::Cancelled => {
                    cx.update(|cx| {
                        if !cancel_token.is_cancelled() {
                            app_state.update(cx, |state, cx| {
                                state.fail_task(
                                    task_id,
                                    crate::labels::disconnect_hook_cancelled_task_label(),
                                );
                                cx.emit(AppStateChanged);
                            });

                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.pending_toast = Some(PendingToast {
                                    message:
                                        crate::labels::disconnect_cancelled_by_hook_toast_label(),
                                    is_error: true,
                                });
                                sidebar.refresh_tree(cx);
                            });

                            return;
                        }

                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.refresh_tree(cx);
                        });
                    });
                    return;
                }
            }

            // Emit disconnect audit event before actual disconnect.
            let disconnect_driver_id = profile.driver_id.clone();
            let disconnect_now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
            cx.update(|cx| {
                let audit_service = app_state.read(cx).audit_service().clone();
                let mut event = dbflux_core::observability::EventRecord::new(
                    disconnect_now_ms,
                    dbflux_core::observability::EventSeverity::Info,
                    dbflux_core::observability::EventCategory::Connection,
                    dbflux_core::observability::EventOutcome::Success,
                );
                event.action = CONNECTION_DISCONNECT.as_str().to_string();
                event.actor_type = dbflux_core::observability::EventActorType::User;
                event.source_id = dbflux_core::observability::EventSourceId::Local;
                event.connection_id = Some(profile_id.to_string());
                event.driver_id = disconnect_driver_id.clone();
                event.summary = format!("Disconnected from '{}'", profile_name);
                if let Err(e) = audit_service.record(event) {
                    log::warn!("Failed to record disconnect audit event: {}", e);
                }
            });

            let teardown = cx.update(|cx| {
                let teardown = app_state.update(cx, |state, cx| {
                    let teardown = state.disconnect(profile_id);
                    cx.emit(AppStateChanged);
                    cx.notify();
                    teardown
                });
                // Cancel in-flight metric catalog fetches for this profile so
                // that stale data from a previous account cannot land in the
                // cache after invalidation (e.g. if the user reconnects the
                // same profile_id to a different AWS account). Dropping the
                // Task handle abandons the foreground awaiter, which is where
                // the cache write now lives (see spawn_fetch_* refactor).
                // Also evict the cached catalog entries so the next folder
                // expand re-runs privilege probes against the new session.
                sidebar.update(cx, |sidebar, _cx| {
                    sidebar.drop_pending_metric_fetches(profile_id);
                    sidebar.clear_instance_catalog_cache(profile_id);
                });
                teardown
            });

            // Teardown ordering: disconnect() only spawns the teardown thread
            // and returns, so both follow-up steps must wait for it. Detached
            // hook processes may own the tunnel the driver's kill connection
            // travels through, and post-disconnect hooks must observe a fully
            // closed connection instead of racing the cancel/close work.
            if let Some(teardown) = teardown
                && let Some(warning) =
                    wait_for_connection_teardown(teardown, &cancel_token, cx).await
            {
                hook_warnings.push(warning);
            }

            cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    state.cancel_detached_hook_tasks(profile_id);
                    cx.emit(AppStateChanged);
                });
            });

            match run_hook_phase(
                app_state.clone(),
                profile_id,
                profile_name.clone(),
                HookPhase::PostDisconnect,
                hooks.post_disconnect,
                hook_context,
                Some(cancel_token.clone()),
                &detached_hook_scope,
                cx,
            )
            .await
            {
                HookPhaseState::Continue { warnings } => {
                    hook_warnings.extend(warnings);
                }
                HookPhaseState::Aborted { error } => {
                    cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            state.fail_task(task_id, error.clone());
                            cx.emit(AppStateChanged);
                        });

                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.pending_toast = Some(PendingToast {
                                message: crate::labels::disconnected_hook_error_toast_label(
                                    &profile_name,
                                    &error.to_lowercase(),
                                ),
                                is_error: true,
                            });
                            sidebar.refresh_tree(cx);
                        });
                    });
                    return;
                }
                HookPhaseState::Cancelled => {
                    cx.update(|cx| {
                        if !cancel_token.is_cancelled() {
                            app_state.update(cx, |state, cx| {
                                state.fail_task(
                                    task_id,
                                    crate::labels::post_disconnect_hook_cancelled_task_label(),
                                );
                                cx.emit(AppStateChanged);
                            });

                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.pending_toast = Some(PendingToast {
                                    message: crate::labels::disconnected_hook_cancelled_toast_label(
                                    ),
                                    is_error: true,
                                });
                                sidebar.refresh_tree(cx);
                            });

                            return;
                        }

                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.refresh_tree(cx);
                        });
                    });
                    return;
                }
            }

            cx.update(|cx| {
                for warning in &hook_warnings {
                    log::warn!("{}", warning);
                }

                app_state.update(cx, |state, cx| {
                    state.complete_task(task_id);
                    cx.emit(AppStateChanged);
                });

                let message =
                    crate::labels::disconnected_toast_label(&profile_name, hook_warnings.len());

                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_toast = Some(PendingToast {
                        message,
                        is_error: false,
                    });
                    sidebar.refresh_tree(cx);
                });
            });
        })
        .detach();

        self.refresh_tree(cx);
    }

    pub(crate) fn refresh_connection(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        // Cancel pending metric catalog fetches and evict the stale cache
        // before disconnect invalidates the connection. Mirrors what
        // disconnect_profile does so reconnect always re-fetches fresh data.
        self.drop_pending_metric_fetches(profile_id);
        self.clear_instance_catalog_cache(profile_id);
        self.app_state.update(cx, |state, cx| {
            state.cancel_detached_hook_tasks(profile_id);
            // Refresh does not run disconnect hooks, so nothing is ordered
            // after the teardown; it stays detached.
            let _teardown = state.disconnect(profile_id);
            log::info!("Refreshing connection for profile {}", profile_id);
            cx.notify();
        });
        self.refresh_tree(cx);
        self.connect_to_profile(profile_id, cx);
    }

    pub(crate) fn delete_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        // Defensive eviction: even though delete_profile does not call
        // disconnect directly, removing the profile orphans any in-flight
        // metric fetches. Drop their foreground tasks so the cache-write
        // closures never run.
        self.drop_pending_metric_fetches(profile_id);
        self.app_state.update(cx, |state, cx| {
            if let Some(idx) = state.profiles().iter().position(|p| p.id == profile_id)
                && let Some(removed) = state.remove_profile(idx)
            {
                log::info!("Deleted profile: {}", removed.name);
            }
            cx.emit(dbflux_ui_base::AppStateChanged);
        });
    }

    /// Drop foreground tasks for every in-flight metric catalog fetch
    /// targeting `profile_id`.
    ///
    /// Dropping the `Task` handle abandons the `cx.spawn` awaiter where the
    /// cache-write closure now lives (see `spawn_fetch_metric_namespaces` /
    /// `spawn_fetch_metrics`). This guarantees that any data fetched in the
    /// background before the teardown can no longer be written to the
    /// session-scoped `MetricCatalogCache`.
    ///
    /// Called from every code path that invalidates the cache or removes a
    /// profile: `disconnect_profile`, `refresh_connection`, `delete_profile`.
    fn drop_pending_metric_fetches(&mut self, profile_id: Uuid) {
        self.pending_metric_namespace_fetches.remove(&profile_id);
        self.pending_metric_fetches
            .retain(|(pid, _ns), _task| *pid != profile_id);
    }

    pub(crate) fn edit_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        let profile_exists = self
            .app_state
            .read(cx)
            .profiles()
            .iter()
            .any(|p| p.id == profile_id);

        if !profile_exists {
            report_error(
                UserFacingError::new(ErrorKind::User, crate::labels::profile_not_found_label())
                    .with_cause(format!("profile id {profile_id}")),
                cx,
            );
            return;
        }

        cx.emit(SidebarEvent::RequestEditConnection { profile_id });
    }
}

#[cfg(test)]
mod tests {
    use super::wait_for_connection_teardown;
    use dbflux_core::CancelToken;
    use gpui::TestAppContext;
    use std::sync::mpsc;
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    type GatedTeardownThread = (
        std::thread::JoinHandle<Result<(), dbflux_core::DbError>>,
        Arc<(Mutex<bool>, Condvar)>,
    );

    /// Spawns a thread that blocks until the returned gate is released,
    /// standing in for a driver teardown stuck on cancel/close work.
    fn gated_teardown_thread() -> GatedTeardownThread {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));

        let thread_gate = gate.clone();
        // The JoinHandle type is fixed by `wait_for_connection_teardown` and the
        // public `ConnectionTeardownHandle` alias in dbflux_core; boxing the Err
        // here would change the production API shape these tests exercise.
        #[expect(
            clippy::result_large_err,
            reason = "the thread must return the production Result<(), DbError> because \
                      wait_for_connection_teardown and the public ConnectionTeardownHandle \
                      alias fix that type"
        )]
        let handle = std::thread::spawn(move || {
            let (lock, condvar) = &*thread_gate;

            let mut released = lock.lock().expect("gate lock");
            while !*released {
                released = condvar.wait(released).expect("gate wait");
            }
            Ok(())
        });

        (handle, gate)
    }

    fn release_gate(gate: &Arc<(Mutex<bool>, Condvar)>) {
        let (lock, condvar) = &**gate;
        *lock.lock().expect("gate lock") = true;
        condvar.notify_all();
    }

    const CONNECTION_TOAST_KEYS: [&str; 13] = [
        "sidebar.task.connecting",
        "sidebar.task.disconnecting",
        "sidebar.task.connection_hook_cancelled",
        "sidebar.task.post_connect_hook_cancelled",
        "sidebar.task.disconnect_hook_cancelled",
        "sidebar.task.post_disconnect_hook_cancelled",
        "sidebar.toast.connection_already_pending",
        "sidebar.toast.background_task_limit",
        "sidebar.toast.connection_cancelled_by_hook",
        "sidebar.toast.connection_cancelled_by_post_connect_hook",
        "sidebar.toast.disconnect_cancelled_by_hook",
        "sidebar.toast.disconnected_hook_cancelled",
        "sidebar.toast.profile_not_found",
    ];

    #[test]
    fn connection_toast_keys_resolve_in_both_locales() {
        for key in CONNECTION_TOAST_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn connected_toast_differs_between_locales() {
        let english = crate::labels::connected_toast_label("prod-db", 0);
        let spanish_key = dbflux_i18n::t!("sidebar.toast.connected.plain", locale = "es");

        assert_ne!(english, spanish_key);
        assert!(english.contains("prod-db"));
    }

    #[gpui::test]
    fn wait_for_connection_teardown_waits_until_thread_completes(cx: &mut TestAppContext) {
        let (teardown, gate) = gated_teardown_thread();
        let cancel_token = CancelToken::new();

        let (done_sender, done_receiver) = mpsc::channel();
        cx.update(|cx| {
            cx.spawn(async move |cx| {
                let warning = wait_for_connection_teardown(teardown, &cancel_token, cx).await;
                done_sender.send(warning).expect("test completion receiver");
            })
            .detach();
        });

        cx.run_until_parked();
        cx.executor().advance_clock(Duration::from_millis(200));
        cx.run_until_parked();
        assert!(
            done_receiver.try_recv().is_err(),
            "wait must not complete while the teardown thread is still running"
        );

        release_gate(&gate);

        // The teardown is a real OS thread while timers use the fake test
        // clock, so retry a few polls to absorb scheduling latency.
        let mut warning = None;
        for _ in 0..100 {
            cx.executor().advance_clock(Duration::from_millis(50));
            cx.run_until_parked();

            match done_receiver.try_recv() {
                Ok(result) => {
                    warning = Some(result);
                    break;
                }
                Err(_) => std::thread::sleep(Duration::from_millis(5)),
            }
        }

        assert_eq!(
            warning,
            Some(None),
            "wait must complete without a warning once the teardown thread finishes"
        );
    }

    #[gpui::test]
    fn wait_for_connection_teardown_returns_cleanup_error_once(cx: &mut TestAppContext) {
        let teardown = {
            #[expect(
                clippy::result_large_err,
                reason = "wait_for_connection_teardown joins a JoinHandle<Result<(), DbError>> \
                          fixed by the public ConnectionTeardownHandle alias, so the spawned \
                          closure must construct the production-sized DbError"
            )]
            std::thread::spawn(|| Err(dbflux_core::DbError::query_failed("close failed")))
        };
        // The helper polls on the executor's virtual clock while the thread
        // runs in real time; let the thread finish first so the poll loop
        // cannot outrun it and the test observes the join result.
        while !teardown.is_finished() {
            std::thread::yield_now();
        }
        let cancel_token = CancelToken::new();
        let (done_sender, done_receiver) = mpsc::channel();
        cx.update(|cx| {
            cx.spawn(async move |cx| {
                done_sender
                    .send(wait_for_connection_teardown(teardown, &cancel_token, cx).await)
                    .expect("test completion receiver");
            })
            .detach();
        });
        for _ in 0..10 {
            cx.executor().advance_clock(Duration::from_millis(50));
            cx.run_until_parked();
            if let Ok(warning) = done_receiver.try_recv() {
                assert_eq!(
                    warning.as_deref(),
                    Some("connection cleanup failed: close failed")
                );
                return;
            }
        }
        panic!("cleanup error was not reported");
    }

    #[gpui::test]
    fn wait_for_connection_teardown_stops_on_cancellation(cx: &mut TestAppContext) {
        let (teardown, gate) = gated_teardown_thread();
        let cancel_token = CancelToken::new();
        cancel_token.cancel();

        let (done_sender, done_receiver) = mpsc::channel();
        cx.update(|cx| {
            cx.spawn(async move |cx| {
                let warning = wait_for_connection_teardown(teardown, &cancel_token, cx).await;
                done_sender.send(warning).expect("test completion receiver");
            })
            .detach();
        });

        cx.run_until_parked();
        assert_eq!(
            done_receiver.try_recv().ok(),
            Some(None),
            "a cancelled disconnect must stop waiting even while teardown is running"
        );

        release_gate(&gate);
    }

    // --- Prompting for a password that is neither saved nor sourced ---

    use crate::Sidebar;
    use crate::table_loading::object_tree_adapter_tests::{AdapterFakeConnection, test_app_state};
    use dbflux_core::secrecy::{ExposeSecret, SecretString};
    use dbflux_ui_base::app_state_entity::AppStateEntity;
    use gpui::{Entity, VisualTestContext};
    use std::sync::LazyLock;
    use uuid::Uuid;

    type RecordedPasswords = Arc<Mutex<Vec<Option<String>>>>;

    static PASSWORD_DRIVER_FORM: LazyLock<dbflux_core::DriverFormDef> =
        LazyLock::new(|| dbflux_core::DriverFormDef { tabs: vec![] });

    /// A driver that authenticates with a password and records the password
    /// every connect receives.
    struct PasswordRecordingDriver {
        metadata: dbflux_core::DriverMetadata,
        passwords: RecordedPasswords,
    }

    impl dbflux_core::DbDriver for PasswordRecordingDriver {
        fn kind(&self) -> dbflux_core::DbKind {
            dbflux_core::DbKind::Postgres
        }

        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            &self.metadata
        }

        fn form_definition(&self) -> &dbflux_core::DriverFormDef {
            &PASSWORD_DRIVER_FORM
        }

        fn driver_key(&self) -> dbflux_core::DriverKey {
            "builtin:sidebar-password-test".into()
        }

        fn build_config(
            &self,
            _: &dbflux_core::FormValues,
        ) -> Result<dbflux_core::DbConfig, dbflux_core::DbError> {
            Ok(dbflux_core::DbConfig::default_postgres())
        }

        fn extract_values(&self, _: &dbflux_core::DbConfig) -> dbflux_core::FormValues {
            dbflux_core::FormValues::new()
        }

        fn connect_with_secrets(
            &self,
            _: &dbflux_core::ConnectionProfile,
            password: Option<&SecretString>,
            _: Option<&SecretString>,
        ) -> Result<Box<dyn dbflux_core::Connection>, dbflux_core::DbError> {
            self.passwords
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(password.map(|password| password.expose_secret().to_string()));

            match Arc::try_unwrap(AdapterFakeConnection::lazy()) {
                Ok(connection) => Ok(Box::new(connection)),
                Err(_) => Err(dbflux_core::DbError::NotSupported(
                    "fake connection shared".into(),
                )),
            }
        }

        fn test_connection(
            &self,
            _: &dbflux_core::ConnectionProfile,
        ) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }
    }

    struct PasswordFixture {
        state: Entity<AppStateEntity>,
        sidebar: Entity<Sidebar>,
        profile_id: Uuid,
        passwords: RecordedPasswords,
        prompts: Arc<Mutex<Vec<Uuid>>>,
    }

    fn password_fixture(
        save_password: bool,
        cx: &mut TestAppContext,
    ) -> (PasswordFixture, &mut VisualTestContext) {
        let state = test_app_state(cx);
        let passwords: RecordedPasswords = Arc::new(Mutex::new(Vec::new()));
        let metadata = dbflux_core::DriverMetadataBuilder::new(
            "sidebar-password-test",
            "SidebarPasswordTest",
            dbflux_core::DatabaseCategory::Relational,
            dbflux_core::QueryLanguage::Sql,
        )
        .capabilities(dbflux_core::DriverCapabilities::AUTHENTICATION)
        .build();

        let mut profile = dbflux_core::ConnectionProfile::new(
            "orders",
            dbflux_core::DbConfig::default_postgres(),
        );
        profile.save_password = save_password;
        let profile_id = profile.id;

        state.update(cx, |state, _| {
            state.inner.facade.connections.drivers.insert(
                "postgres".into(),
                Arc::new(PasswordRecordingDriver {
                    metadata,
                    passwords: passwords.clone(),
                }),
            );
            state.profiles_mut().push(profile);
            state.connection_tree_mut().add_node(
                dbflux_core::ConnectionTreeNode::new_connection_ref(profile_id, None, 1000),
            );
        });

        let (sidebar, cx) =
            cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));

        let prompts = Arc::new(Mutex::new(Vec::new()));
        cx.update(|_, cx| {
            let prompts = prompts.clone();
            cx.subscribe(&sidebar, move |_, event: &crate::SidebarEvent, _| {
                if let crate::SidebarEvent::RequestConnectionPassword { prompt } = event {
                    prompts.lock().expect("prompts").push(prompt.profile_id);
                }
            })
            .detach();
        });

        (
            PasswordFixture {
                state,
                sidebar,
                profile_id,
                passwords,
                prompts,
            },
            cx,
        )
    }

    fn recorded(passwords: &RecordedPasswords) -> Vec<Option<String>> {
        passwords.lock().expect("recorded passwords").clone()
    }

    #[gpui::test]
    fn connect_asks_for_an_unsaved_password_before_connecting(cx: &mut TestAppContext) {
        let (fixture, cx) = password_fixture(false, cx);
        let profile_id = fixture.profile_id;

        fixture
            .sidebar
            .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
        cx.run_until_parked();

        assert_eq!(*fixture.prompts.lock().expect("prompts"), vec![profile_id]);
        assert!(
            recorded(&fixture.passwords).is_empty(),
            "nothing connects before the prompt"
        );
        fixture.state.read_with(cx, |state, _| {
            assert!(!state.connections().contains_key(&profile_id));
            assert!(!state.is_operation_pending(profile_id, None));
        });
    }

    #[gpui::test]
    fn prompted_password_connects_and_serves_the_live_session(cx: &mut TestAppContext) {
        let (fixture, cx) = password_fixture(false, cx);
        let profile_id = fixture.profile_id;

        fixture.sidebar.update(cx, |sidebar, cx| {
            sidebar.connect_to_profile_with_password(profile_id, SecretString::from(""), false, cx)
        });
        cx.run_until_parked();

        assert_eq!(recorded(&fixture.passwords), vec![Some(String::new())]);
        fixture.state.read_with(cx, |state, _| {
            assert!(state.connections().contains_key(&profile_id));
            assert_eq!(
                state
                    .inner
                    .facade
                    .connections
                    .session_password(profile_id)
                    .map(|password| password.expose_secret().to_string()),
                Some(String::new()),
                "per-database connections and switches reuse the prompted password"
            );
            assert!(
                state.remembered_connection_password(profile_id).is_none(),
                "an unchecked remember box keeps nothing past the session"
            );
        });
        assert!(fixture.prompts.lock().expect("prompts").is_empty());
    }

    #[gpui::test]
    fn remembered_password_connects_without_asking_again(cx: &mut TestAppContext) {
        let (fixture, cx) = password_fixture(false, cx);
        let profile_id = fixture.profile_id;

        fixture.state.update(cx, |state, _| {
            state.remember_connection_password(profile_id, SecretString::from("typed"));
        });
        fixture
            .sidebar
            .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
        cx.run_until_parked();

        assert!(fixture.prompts.lock().expect("prompts").is_empty());
        assert_eq!(
            recorded(&fixture.passwords),
            vec![Some("typed".to_string())]
        );
        fixture.state.read_with(cx, |state, _| {
            assert!(state.connections().contains_key(&profile_id))
        });
    }

    #[gpui::test]
    fn remember_box_keeps_the_prompted_password_for_the_session(cx: &mut TestAppContext) {
        let (fixture, cx) = password_fixture(false, cx);
        let profile_id = fixture.profile_id;

        fixture.sidebar.update(cx, |sidebar, cx| {
            sidebar.connect_to_profile_with_password(
                profile_id,
                SecretString::from("typed"),
                true,
                cx,
            )
        });
        cx.run_until_parked();

        fixture.state.read_with(cx, |state, _| {
            assert_eq!(
                state
                    .remembered_connection_password(profile_id)
                    .map(|password| password.expose_secret().to_string()),
                Some("typed".to_string())
            );
        });
    }

    #[gpui::test]
    fn saved_password_profile_connects_without_asking(cx: &mut TestAppContext) {
        let (fixture, cx) = password_fixture(true, cx);
        let profile_id = fixture.profile_id;

        fixture
            .sidebar
            .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
        cx.run_until_parked();

        assert!(fixture.prompts.lock().expect("prompts").is_empty());
        assert_eq!(recorded(&fixture.passwords).len(), 1);
        fixture.state.read_with(cx, |state, _| {
            assert!(state.connections().contains_key(&profile_id))
        });
    }

    /// Clicks the row twice through the real element. The events are
    /// dispatched without parking the scheduler, so a connect the click starts
    /// stays in flight until the test lets the executor run.
    fn double_click_row(item_id: &str, cx: &mut VisualTestContext) {
        use gpui::{InputEvent, Modifiers, MouseButton, MouseDownEvent, MouseUpEvent};

        let selector: &'static str = Box::leak(format!("row-{item_id}").into_boxed_str());
        let bounds = cx.debug_bounds(selector).expect("the profile row is drawn");
        let position = bounds.center();

        for click_count in [1, 2] {
            let down = MouseDownEvent {
                position,
                modifiers: Modifiers::default(),
                button: MouseButton::Left,
                click_count,
                first_mouse: false,
            };
            let up = MouseUpEvent {
                position,
                modifiers: Modifiers::default(),
                button: MouseButton::Left,
                click_count,
            };

            cx.update(|window, cx| {
                window.dispatch_event(down.to_platform_input(), cx);
                window.dispatch_event(up.to_platform_input(), cx);
            });
        }
    }

    fn last_toast_title(cx: &mut VisualTestContext) -> Option<String> {
        cx.update(|_, cx| {
            cx.global::<dbflux_ui_base::toast::ToastGlobal>()
                .host
                .read(cx)
                .last_toast_title()
        })
    }

    /// The number of toasts on the stack and the title of the newest.
    fn toast_stack(cx: &mut VisualTestContext) -> (usize, Option<String>) {
        cx.update(|_, cx| {
            let host = cx
                .global::<dbflux_ui_base::toast::ToastGlobal>()
                .host
                .read(cx);
            (host.toast_count(), host.last_toast_title())
        })
    }

    #[gpui::test]
    fn double_click_on_a_connection_connects_once_without_an_error(cx: &mut TestAppContext) {
        let (fixture, cx) = password_fixture(true, cx);
        let profile_id = fixture.profile_id;
        let item_id = dbflux_core::SchemaNodeId::Profile { profile_id }.to_string();
        let pending_title = crate::labels::connection_already_pending_toast_label();

        cx.run_until_parked();

        // The connect runs on the background executor, which does not advance
        // until the scheduler is parked: the first attempt holds its pending
        // slot while the second double click arrives.
        double_click_row(&item_id, cx);
        fixture.state.read_with(cx, |state, _| {
            assert!(state.is_operation_pending(profile_id, None));
        });
        assert!(recorded(&fixture.passwords).is_empty());

        double_click_row(&item_id, cx);
        fixture.state.read_with(cx, |state, _| {
            assert!(
                state.is_operation_pending(profile_id, None),
                "the second request met the first attempt's pending slot"
            );
        });
        let (during_count, during_title) = toast_stack(cx);
        assert_ne!(during_title, Some(pending_title.clone()));

        cx.run_until_parked();

        assert_eq!(recorded(&fixture.passwords).len(), 1);
        fixture.state.read_with(cx, |state, _| {
            assert!(state.connections().contains_key(&profile_id));
        });
        // An earlier error toast would sit under the success one, so the
        // whole stack is counted, not only its top.
        let (count, title) = toast_stack(cx);
        assert_ne!(title, Some(pending_title));
        assert_eq!(
            count,
            during_count + 1,
            "only the success toast was added; no error hides beneath it"
        );
    }

    #[gpui::test]
    fn double_click_while_a_connect_is_in_flight_reports_nothing(cx: &mut TestAppContext) {
        let (fixture, cx) = password_fixture(true, cx);
        let profile_id = fixture.profile_id;
        let item_id = dbflux_core::SchemaNodeId::Profile { profile_id }.to_string();

        fixture.state.update(cx, |state, _| {
            assert!(state.start_pending_operation(profile_id, None));
        });
        double_click_row(&item_id, cx);
        cx.run_until_parked();

        assert!(recorded(&fixture.passwords).is_empty());
        assert_ne!(
            last_toast_title(cx),
            Some(crate::labels::connection_already_pending_toast_label())
        );
    }

    #[gpui::test]
    fn connect_requested_while_pending_reports_nothing(cx: &mut TestAppContext) {
        let (fixture, cx) = password_fixture(true, cx);
        let profile_id = fixture.profile_id;

        fixture.state.update(cx, |state, _| {
            assert!(state.start_pending_operation(profile_id, None));
        });
        fixture
            .sidebar
            .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
        cx.run_until_parked();

        assert!(recorded(&fixture.passwords).is_empty());
        assert_ne!(
            last_toast_title(cx),
            Some(crate::labels::connection_already_pending_toast_label())
        );
    }

    // --- The connect pipeline as a parent task with its stages as children ---

    use dbflux_core::{TaskKind, TaskSnapshot, TaskStatus};
    use std::future::Future;
    use std::pin::Pin;

    type AuthFuture<'a, T> =
        Pin<Box<dyn Future<Output = Result<T, dbflux_core::DbError>> + Send + 'a>>;

    /// An auth provider whose session is either valid or waits for a login
    /// that never ends, so a connect can be held inside its auth stage.
    struct ScriptedAuthProvider {
        login_never_ends: bool,
    }

    impl dbflux_core::DynAuthProvider for ScriptedAuthProvider {
        fn provider_id(&self) -> &str {
            "scripted-auth"
        }

        fn display_name(&self) -> &str {
            "Test Auth"
        }

        fn form_def(&self) -> &dbflux_core::AuthFormDef {
            static FORM: std::sync::OnceLock<dbflux_core::AuthFormDef> = std::sync::OnceLock::new();
            FORM.get_or_init(|| dbflux_core::AuthFormDef { tabs: vec![] })
        }

        fn validate_session<'life0, 'life1, 'async_trait>(
            &'life0 self,
            _profile: &'life1 dbflux_core::AuthProfile,
        ) -> AuthFuture<'async_trait, dbflux_core::AuthSessionState>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            let state = if self.login_never_ends {
                dbflux_core::AuthSessionState::LoginRequired
            } else {
                dbflux_core::AuthSessionState::Valid { expires_at: None }
            };
            Box::pin(async move { Ok(state) })
        }

        fn login<'life0, 'life1, 'async_trait>(
            &'life0 self,
            _profile: &'life1 dbflux_core::AuthProfile,
            _url_callback: dbflux_core::auth::UrlCallback,
        ) -> AuthFuture<'async_trait, dbflux_core::AuthSession>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(std::future::pending())
        }

        fn resolve_credentials<'life0, 'life1, 'async_trait>(
            &'life0 self,
            _profile: &'life1 dbflux_core::AuthProfile,
        ) -> AuthFuture<'async_trait, dbflux_core::ResolvedCredentials>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async { Ok(dbflux_core::ResolvedCredentials::default()) })
        }
    }

    fn true_hook() -> dbflux_core::ConnectionHook {
        dbflux_core::ConnectionHook {
            enabled: true,
            kind: dbflux_core::HookKind::Command {
                command: "true".to_string(),
                args: Vec::new(),
            },
            cwd: None,
            env: Default::default(),
            inherit_env: true,
            env_denylist: Vec::new(),
            timeout_ms: None,
            execution_mode: Default::default(),
            ready_signal: None,
            on_failure: Default::default(),
        }
    }

    /// A pipeline connect for a profile shaped by `shape`, with the scripted
    /// auth provider registered.
    fn pipeline_fixture(
        login_never_ends: bool,
        shape: impl FnOnce(&mut dbflux_core::ConnectionProfile),
        cx: &mut TestAppContext,
    ) -> (PasswordFixture, &mut VisualTestContext) {
        let (fixture, cx) = password_fixture(true, cx);
        let profile_id = fixture.profile_id;

        fixture.state.update(cx, |state, _| {
            state.register_auth_provider(Arc::new(ScriptedAuthProvider { login_never_ends }));
            let auth = dbflux_core::AuthProfile::new(
                "scripted",
                "scripted-auth",
                std::collections::HashMap::new(),
            );
            let auth_id = auth.id;
            state.add_auth_profile(auth);

            if let Some(profile) = state.profiles_mut().iter_mut().find(|p| p.id == profile_id) {
                profile.auth_profile_id = Some(auth_id);
                shape(profile);
            }
        });

        (fixture, cx)
    }

    fn connect_attempts(state: &AppStateEntity) -> Vec<TaskSnapshot> {
        state
            .tasks()
            .recent_tasks(usize::MAX)
            .into_iter()
            .filter(|task| task.kind == TaskKind::Connect && task.parent_id.is_none())
            .collect()
    }

    /// The (name, status) of the stages of the only connect attempt.
    fn stage_rows(
        fixture: &PasswordFixture,
        cx: &mut VisualTestContext,
    ) -> Vec<(String, TaskStatus)> {
        fixture.state.read_with(cx, |state, _| {
            let attempts = connect_attempts(state);
            assert_eq!(attempts.len(), 1, "one connect attempt");
            state
                .child_tasks(attempts[0].id)
                .into_iter()
                .map(|stage| (stage.description, stage.status))
                .collect()
        })
    }

    fn attempt_status(fixture: &PasswordFixture, cx: &mut VisualTestContext) -> TaskStatus {
        fixture
            .state
            .read_with(cx, |state, _| connect_attempts(state)[0].status.clone())
    }

    fn statuses(rows: &[(String, TaskStatus)]) -> Vec<TaskStatus> {
        rows.iter().map(|(_, status)| status.clone()).collect()
    }

    #[gpui::test]
    fn a_connect_registers_its_stages_in_order_under_one_task(cx: &mut TestAppContext) {
        let (fixture, cx) = pipeline_fixture(
            false,
            |profile| {
                profile.value_refs.insert(
                    "host".to_string(),
                    dbflux_core::values::ValueRef::literal("db"),
                );
                profile.access_kind = Some(dbflux_core::AccessKind::Ssh {
                    ssh_tunnel_profile_id: Uuid::new_v4(),
                });
                profile.hooks = Some(dbflux_core::ConnectionHooks {
                    pre_connect: vec![true_hook()],
                    post_connect: vec![true_hook()],
                    ..Default::default()
                });
            },
            cx,
        );
        let profile_id = fixture.profile_id;

        fixture
            .sidebar
            .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));

        let rows = stage_rows(&fixture, cx);
        let names: Vec<&str> = rows.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(
            names,
            [
                "Pre-connect hooks",
                "Authenticating via Test Auth",
                "Resolving values",
                "Opening tunnel",
                "Connecting driver",
                "Fetching schema",
                "Post-connect hooks",
            ]
        );
        assert_eq!(statuses(&rows), vec![TaskStatus::Pending; 7]);
        assert_eq!(attempt_status(&fixture, cx), TaskStatus::Running);
    }

    #[gpui::test]
    fn a_profile_without_auth_or_hooks_registers_only_the_stages_that_apply(
        cx: &mut TestAppContext,
    ) {
        let (fixture, cx) = pipeline_fixture(
            false,
            |profile| {
                profile.auth_profile_id = None;
                profile.access_kind = Some(dbflux_core::AccessKind::Direct);
            },
            cx,
        );
        let profile_id = fixture.profile_id;

        fixture
            .sidebar
            .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));

        let names: Vec<String> = stage_rows(&fixture, cx)
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(names, ["Connecting driver", "Fetching schema"]);
    }

    #[gpui::test]
    fn a_successful_connect_completes_every_stage_and_registers_the_connection(
        cx: &mut TestAppContext,
    ) {
        let (fixture, cx) = pipeline_fixture(false, |_| {}, cx);
        let profile_id = fixture.profile_id;
        let watchers = Arc::new(Mutex::new(Vec::new()));
        cx.update(|_, cx| {
            let watchers = watchers.clone();
            cx.subscribe(
                &fixture.sidebar,
                move |_, event: &crate::SidebarEvent, _| {
                    if let crate::SidebarEvent::PipelineStarted { watcher, .. } = event {
                        watchers.lock().expect("watchers").push(watcher.clone());
                    }
                },
            )
            .detach();
        });

        fixture
            .sidebar
            .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
        cx.run_until_parked();

        let rows = stage_rows(&fixture, cx);
        assert_eq!(
            rows.iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            [
                "Authenticating via Test Auth",
                "Connecting driver",
                "Fetching schema"
            ]
        );
        assert_eq!(statuses(&rows), vec![TaskStatus::Completed; 3]);
        assert_eq!(attempt_status(&fixture, cx), TaskStatus::Completed);
        fixture.state.read_with(cx, |state, _| {
            assert!(state.connections().contains_key(&profile_id));
            assert!(!state.is_operation_pending(profile_id, None));
        });
        assert_eq!(recorded(&fixture.passwords).len(), 1);

        let watchers = watchers.lock().expect("watchers");
        assert_eq!(watchers.len(), 1, "one pipeline was announced");
        assert!(matches!(
            *watchers[0].borrow(),
            dbflux_core::PipelineState::Connected
        ));
    }

    #[gpui::test]
    fn cancelling_during_authentication_skips_the_later_stages_and_connects_nothing(
        cx: &mut TestAppContext,
    ) {
        let (fixture, cx) = pipeline_fixture(true, |_| {}, cx);
        let profile_id = fixture.profile_id;

        fixture
            .sidebar
            .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
        cx.run_until_parked();

        assert_eq!(
            statuses(&stage_rows(&fixture, cx)),
            [
                TaskStatus::Running,
                TaskStatus::Pending,
                TaskStatus::Pending
            ],
            "the pipeline waits for the sign-in"
        );

        fixture.state.update(cx, |state, _| {
            assert_eq!(
                state.cancel_running_connect_tasks_for_profile(profile_id),
                1
            );
        });
        assert_eq!(
            statuses(&stage_rows(&fixture, cx)),
            [
                TaskStatus::Cancelling,
                TaskStatus::Skipped,
                TaskStatus::Skipped
            ],
            "the stage waits for the worker; the rest never started"
        );
        assert_eq!(attempt_status(&fixture, cx), TaskStatus::Cancelling);

        cx.run_until_parked();

        assert_eq!(
            statuses(&stage_rows(&fixture, cx)),
            [
                TaskStatus::Cancelled,
                TaskStatus::Skipped,
                TaskStatus::Skipped
            ]
        );
        assert_eq!(attempt_status(&fixture, cx), TaskStatus::Cancelled);
        fixture.state.read_with(cx, |state, _| {
            assert!(!state.connections().contains_key(&profile_id));
            assert!(!state.is_operation_pending(profile_id, None));
        });
        assert!(recorded(&fixture.passwords).is_empty());
    }

    #[gpui::test]
    fn a_connect_for_a_profile_that_has_a_connect_task_is_refused_by_the_task_manager(
        cx: &mut TestAppContext,
    ) {
        let (fixture, cx) = pipeline_fixture(false, |_| {}, cx);
        let profile_id = fixture.profile_id;

        // Only the task is registered; the pending-operation set stays empty,
        // so a refusal can only come from the task manager.
        fixture.state.update(cx, |state, _| {
            state
                .try_start_task(
                    dbflux_core::TaskRequest::new(TaskKind::Connect, "connecting").target(
                        dbflux_core::TaskTarget {
                            profile_id,
                            database: None,
                        },
                    ),
                )
                .expect("first connect task starts");
        });

        fixture
            .sidebar
            .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
        cx.run_until_parked();

        fixture.state.read_with(cx, |state, _| {
            assert_eq!(connect_attempts(state).len(), 1, "no second attempt");
            assert!(!state.connections().contains_key(&profile_id));
        });
        assert!(recorded(&fixture.passwords).is_empty());
    }

    // --- The direct connect path (no auth provider, value refs or access) ---

    #[gpui::test]
    fn a_plain_profile_connects_through_a_parent_task_with_driver_stages(cx: &mut TestAppContext) {
        let (fixture, cx) = password_fixture(true, cx);
        let profile_id = fixture.profile_id;

        fixture
            .sidebar
            .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));

        let rows = stage_rows(&fixture, cx);
        assert_eq!(
            rows.iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            ["Connecting driver", "Fetching schema"]
        );
        assert_eq!(statuses(&rows), vec![TaskStatus::Pending; 2]);
        assert_eq!(attempt_status(&fixture, cx), TaskStatus::Running);

        cx.run_until_parked();

        assert_eq!(
            statuses(&stage_rows(&fixture, cx)),
            vec![TaskStatus::Completed; 2]
        );
        assert_eq!(attempt_status(&fixture, cx), TaskStatus::Completed);
        fixture.state.read_with(cx, |state, _| {
            assert!(state.connections().contains_key(&profile_id));
            assert!(!state.is_operation_pending(profile_id, None));
        });
        assert_eq!(recorded(&fixture.passwords).len(), 1);
    }

    #[gpui::test]
    fn a_plain_connect_for_a_profile_that_has_a_connect_task_is_refused(cx: &mut TestAppContext) {
        let (fixture, cx) = password_fixture(true, cx);
        let profile_id = fixture.profile_id;

        // Only the task is registered; the pending-operation set stays empty,
        // so a refusal can only come from the task manager.
        fixture.state.update(cx, |state, _| {
            state
                .try_start_task(
                    dbflux_core::TaskRequest::new(TaskKind::Connect, "connecting").target(
                        dbflux_core::TaskTarget {
                            profile_id,
                            database: None,
                        },
                    ),
                )
                .expect("first connect task starts");
        });

        fixture
            .sidebar
            .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
        cx.run_until_parked();

        fixture.state.read_with(cx, |state, _| {
            assert_eq!(connect_attempts(state).len(), 1, "no second attempt");
            assert!(!state.connections().contains_key(&profile_id));
            assert!(!state.is_operation_pending(profile_id, None));
        });
        assert!(recorded(&fixture.passwords).is_empty());
    }

    #[gpui::test]
    fn cancelling_during_the_driver_connect_drops_the_connection_and_skips_the_schema(
        cx: &mut TestAppContext,
    ) {
        let (fixture, cx) = password_fixture(true, cx);
        let profile_id = fixture.profile_id;

        fixture
            .sidebar
            .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));

        // Let the foreground reach the blocking driver connect, which the
        // test scheduler has queued but not run.
        for _ in 0..50 {
            let driver_stage_running = fixture.state.read_with(cx, |state, _| {
                state.tasks().recent_tasks(usize::MAX).iter().any(|task| {
                    task.description == "Connecting driver" && task.status == TaskStatus::Running
                })
            });
            if driver_stage_running {
                break;
            }
            cx.executor().tick();
        }
        assert!(
            recorded(&fixture.passwords).is_empty(),
            "the driver connect is still in flight"
        );

        fixture.state.update(cx, |state, _| {
            assert_eq!(
                state.cancel_running_connect_tasks_for_profile(profile_id),
                1
            );
        });
        assert_eq!(
            statuses(&stage_rows(&fixture, cx)),
            [TaskStatus::Cancelling, TaskStatus::Skipped],
            "the stage waits for the blocking call; the schema never starts"
        );
        assert_eq!(attempt_status(&fixture, cx), TaskStatus::Cancelling);

        cx.run_until_parked();

        assert_eq!(
            statuses(&stage_rows(&fixture, cx)),
            [TaskStatus::Cancelled, TaskStatus::Skipped]
        );
        assert_eq!(attempt_status(&fixture, cx), TaskStatus::Cancelled);
        fixture.state.read_with(cx, |state, _| {
            assert!(
                !state.connections().contains_key(&profile_id),
                "a cancelled connect registers nothing"
            );
            assert!(!state.is_operation_pending(profile_id, None));
        });
    }
}
