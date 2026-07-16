use crate::*;
use dbflux_app::{ExternalDriverDiagnostic, ExternalDriverStage};
use dbflux_core::observability::actions::{
    CONNECTION_CONNECT, CONNECTION_CONNECT_FAILED, CONNECTION_CONNECTING, CONNECTION_DISCONNECT,
};
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
    match stage {
        ExternalDriverStage::Config => format!(
            "External driver '{}' is unavailable because service '{}' has an invalid configuration: {}",
            driver_id, socket_id, summary
        ),
        ExternalDriverStage::Launch => format!(
            "External driver '{}' is unavailable because service '{}' did not start: {}",
            driver_id, socket_id, summary
        ),
        ExternalDriverStage::Probe => format!(
            "External driver '{}' is unavailable because service '{}' failed during driver probe: {}",
            driver_id, socket_id, summary
        ),
    }
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
    teardown: std::thread::JoinHandle<()>,
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

    if teardown.join().is_err() {
        log::warn!("Connection teardown thread panicked");
    }

    None
}

impl Sidebar {
    pub fn connect_to_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        self.connect_to_profile_inner(profile_id, None, false, cx);
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

                let cleared = cx
                    .update(|cx| {
                        let still_connected =
                            app_state.read(cx).connections().contains_key(&profile_id);
                        let still_pending =
                            app_state.read(cx).is_operation_pending(profile_id, None);
                        !still_connected && !still_pending
                    })
                    .unwrap_or(false);

                if cleared {
                    break;
                }
            }

            if let Err(error) = cx.update(|cx| {
                sidebar.update(cx, |sidebar, cx| {
                    sidebar.connect_to_profile(profile_id, cx);
                });
            }) {
                log::warn!(
                    "Failed to trigger reconnect after edit for profile {}: {:?}",
                    profile_id,
                    error
                );
            }
        })
        .detach();
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
        // Pass last_attempt_failed=true so that if this attempt also fails with a passphrase
        // error, the re-opened modal shows the "Incorrect passphrase" error banner.
        self.connect_to_profile_inner(profile_id, Some(passphrase), true, cx);
    }

    fn connect_to_profile_inner(
        &mut self,
        profile_id: Uuid,
        override_passphrase: Option<String>,
        last_attempt_failed: bool,
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
            self.connect_via_pipeline(profile_id, cx);
            return;
        }

        let passphrase_ref: Option<&str> = override_passphrase.as_deref();

        let (params, profile_name, pre_connect_hooks, post_connect_hooks, hook_context) =
            match self.app_state.update(cx, |state, _cx| {
                if state.is_operation_pending(profile_id, None) {
                    return Err(PendingToast {
                        message: "Connection already pending".to_string(),
                        is_error: true,
                    });
                }

                let result =
                    state.prepare_connect_profile_with_passphrase(profile_id, passphrase_ref);

                if result.is_ok() && !state.start_pending_operation(profile_id, None) {
                    return Err(PendingToast {
                        message: "Operation started by another thread".to_string(),
                        is_error: true,
                    });
                }

                let diagnostic = result
                    .as_ref()
                    .err()
                    .and_then(|error| error.socket_id())
                    .and_then(|socket_id| state.external_driver_diagnostic(socket_id))
                    .cloned();

                result
                    .map(|p| {
                        let name = p.profile.name.clone();
                        let hook_execution =
                            p.prepare_hooks(state.resolve_profile_hooks(&p.profile));

                        (
                            p,
                            name,
                            hook_execution.hooks.pre_connect,
                            hook_execution.hooks.post_connect,
                            hook_execution.context,
                        )
                    })
                    .map_err(|error| connect_prepare_error_toast(&error, diagnostic.as_ref()))
            }) {
                Ok(p) => p,
                Err(toast) => {
                    self.pending_toast = Some(toast);
                    self.refresh_tree(cx);
                    cx.notify();
                    return;
                }
            };

        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.app_state.update(cx, |state, _cx| {
                state.finish_pending_operation(profile_id, None);
            });
            self.pending_toast = Some(PendingToast {
                message: "Too many background tasks running, please wait".to_string(),
                is_error: true,
            });
            self.refresh_tree(cx);
            cx.notify();
            return;
        }

        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let result =
                state.start_task(TaskKind::Connect, format!("Connecting to {}", profile_name));
            cx.emit(AppStateChanged);
            result
        });

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();

        let detached_hook_scope = DetachedHookScope::default();

        cx.spawn(async move |_this, cx| {
            let mut hook_warnings = Vec::new();

            match run_hook_phase(
                app_state.clone(),
                profile_id,
                profile_name.clone(),
                HookPhase::PreConnect,
                pre_connect_hooks,
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
                    if let Err(update_error) = cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            state.cancel_detached_hook_tasks(profile_id);
                            state.fail_task(task_id, error.clone());
                            state.finish_pending_operation(profile_id, None);
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
                            "Failed to apply pre-connect hook abort state: {:?}",
                            update_error
                        );
                    }
                    return;
                }
                HookPhaseState::Cancelled => {
                    if let Err(update_error) = cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            state.cancel_detached_hook_tasks(profile_id);
                            cx.emit(AppStateChanged);
                        });

                        if cancel_token.is_cancelled() {
                            app_state.update(cx, |state, cx| {
                                state.finish_pending_operation(profile_id, None);
                                cx.emit(AppStateChanged);
                            });

                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.refresh_tree(cx);
                            });

                            return;
                        }

                        app_state.update(cx, |state, cx| {
                            state.fail_task(task_id, "Connection hook cancelled");
                            state.finish_pending_operation(profile_id, None);
                            cx.emit(AppStateChanged);
                        });

                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.pending_toast = Some(PendingToast {
                                message: "Connection cancelled by hook".to_string(),
                                is_error: true,
                            });
                            sidebar.refresh_tree(cx);
                        });
                    }) {
                        log::warn!(
                            "Failed to apply pre-connect cancellation state: {:?}",
                            update_error
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

            if let Err(update_error) = cx.update(|cx| {
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
            }) {
                log::warn!(
                    "Failed to emit connection_connecting audit event: {:?}",
                    update_error
                );
            }

            let result = cx
                .background_executor()
                .spawn(async move { params.execute(Some(dbflux_app::proxy::create_proxy_tunnel)) })
                .await;

            if cancel_token.is_cancelled() {
                if let Err(update_error) = cx.update(|cx| {
                    log::info!("Connection task was cancelled, discarding result");

                    app_state.update(cx, |state, cx| {
                        state.finish_pending_operation(profile_id, None);
                        cx.emit(AppStateChanged);
                    });

                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.refresh_tree(cx);
                    });
                }) {
                    log::warn!(
                        "Failed to apply cancelled connection task state: {:?}",
                        update_error
                    );
                }
                return;
            }

            let connected = match result {
                Ok(value) => value,
                Err(error) => {
                    let error_clone = error.clone();
                    let profile_name_for_audit = profile_name.clone();
                    let profile_id_for_audit = profile_id;
                    let is_passphrase_error = is_passphrase_required_error_str(&error);

                    if let Err(update_error) = cx.update(|cx| {
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

                            state.cancel_detached_hook_tasks(profile_id);
                            state.fail_task(task_id, error_clone);
                            state.finish_pending_operation(profile_id, None);
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
                    }) {
                        log::warn!(
                            "Failed to apply connection failure state: {:?}",
                            update_error
                        );
                    }
                    return;
                }
            };

            match run_hook_phase(
                app_state.clone(),
                profile_id,
                profile_name,
                HookPhase::PostConnect,
                post_connect_hooks,
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
                    if let Err(update_error) = cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            state.cancel_detached_hook_tasks(profile_id);
                            state.fail_task(task_id, error.clone());
                            state.finish_pending_operation(profile_id, None);
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
                            "Failed to apply post-connect hook abort state: {:?}",
                            update_error
                        );
                    }
                    return;
                }
                HookPhaseState::Cancelled => {
                    if let Err(update_error) = cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            state.cancel_detached_hook_tasks(profile_id);
                            cx.emit(AppStateChanged);
                        });

                        if cancel_token.is_cancelled() {
                            app_state.update(cx, |state, cx| {
                                state.finish_pending_operation(profile_id, None);
                                cx.emit(AppStateChanged);
                            });

                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.refresh_tree(cx);
                            });

                            return;
                        }

                        app_state.update(cx, |state, cx| {
                            state.fail_task(task_id, "Post-connect hook cancelled");
                            state.finish_pending_operation(profile_id, None);
                            cx.emit(AppStateChanged);
                        });

                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.pending_toast = Some(PendingToast {
                                message: "Connection cancelled by post-connect hook".to_string(),
                                is_error: true,
                            });
                            sidebar.refresh_tree(cx);
                        });
                    }) {
                        log::warn!(
                            "Failed to apply post-connect cancellation state: {:?}",
                            update_error
                        );
                    }
                    return;
                }
            }

            let connected_profile_name = connected.profile.name.clone();
            let connected_driver_id = connected.profile.driver_id.clone();

            if let Err(update_error) = cx.update(|cx| {
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

                    state.complete_task(task_id);
                    state.finish_pending_operation(profile_id, None);
                    state.apply_connect_profile(
                        connected.profile,
                        connected.connection,
                        connected.schema,
                        connected.proxy_tunnel,
                        false,
                    );
                    cx.emit(AppStateChanged);
                    cx.notify();
                });

                let message = if hook_warnings.is_empty() {
                    format!("Connected to {}", connected_profile_name)
                } else {
                    format!(
                        "Connected to {} (with {} hook warning{})",
                        connected_profile_name,
                        hook_warnings.len(),
                        if hook_warnings.len() == 1 { "" } else { "s" }
                    )
                };

                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_toast = Some(PendingToast {
                        message,
                        is_error: false,
                    });
                    sidebar.refresh_tree(cx);
                });
            }) {
                log::warn!(
                    "Failed to apply successful connection state to sidebar: {:?}",
                    update_error
                );
            }
        })
        .detach();
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
                message: "Too many background tasks running, please wait".to_string(),
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
                format!("Disconnecting {}", profile_name),
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
                    if let Err(update_error) = cx.update(|cx| {
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
                            "Failed to apply pre-disconnect hook abort state: {:?}",
                            update_error
                        );
                    }
                    return;
                }
                HookPhaseState::Cancelled => {
                    if let Err(update_error) = cx.update(|cx| {
                        if !cancel_token.is_cancelled() {
                            app_state.update(cx, |state, cx| {
                                state.fail_task(task_id, "Disconnect hook cancelled");
                                cx.emit(AppStateChanged);
                            });

                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.pending_toast = Some(PendingToast {
                                    message: "Disconnect cancelled by hook".to_string(),
                                    is_error: true,
                                });
                                sidebar.refresh_tree(cx);
                            });

                            return;
                        }

                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.refresh_tree(cx);
                        });
                    }) {
                        log::warn!(
                            "Failed to apply pre-disconnect cancellation state: {:?}",
                            update_error
                        );
                    }
                    return;
                }
            }

            // Emit disconnect audit event before actual disconnect.
            let disconnect_driver_id = profile.driver_id.clone();
            let disconnect_now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
            let _ = cx.update(|cx| {
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

            let teardown = match cx.update(|cx| {
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
            }) {
                Ok(teardown) => teardown,
                Err(update_error) => {
                    log::warn!(
                        "Failed to apply disconnect transition to app state: {:?}",
                        update_error
                    );
                    None
                }
            };

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

            if let Err(update_error) = cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    state.cancel_detached_hook_tasks(profile_id);
                    cx.emit(AppStateChanged);
                });
            }) {
                log::warn!(
                    "Failed to cancel detached hook tasks after disconnect: {:?}",
                    update_error
                );
            }

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
                    if let Err(update_error) = cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            state.fail_task(task_id, error.clone());
                            cx.emit(AppStateChanged);
                        });

                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.pending_toast = Some(PendingToast {
                                message: format!(
                                    "Disconnected from {}, but {}",
                                    profile_name,
                                    error.to_lowercase()
                                ),
                                is_error: true,
                            });
                            sidebar.refresh_tree(cx);
                        });
                    }) {
                        log::warn!(
                            "Failed to apply post-disconnect hook abort state: {:?}",
                            update_error
                        );
                    }
                    return;
                }
                HookPhaseState::Cancelled => {
                    if let Err(update_error) = cx.update(|cx| {
                        if !cancel_token.is_cancelled() {
                            app_state.update(cx, |state, cx| {
                                state.fail_task(task_id, "Post-disconnect hook cancelled");
                                cx.emit(AppStateChanged);
                            });

                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.pending_toast = Some(PendingToast {
                                    message: "Disconnected, but post-disconnect hook was cancelled"
                                        .to_string(),
                                    is_error: true,
                                });
                                sidebar.refresh_tree(cx);
                            });

                            return;
                        }

                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.refresh_tree(cx);
                        });
                    }) {
                        log::warn!(
                            "Failed to apply post-disconnect cancellation state: {:?}",
                            update_error
                        );
                    }
                    return;
                }
            }

            if let Err(update_error) = cx.update(|cx| {
                for warning in &hook_warnings {
                    log::warn!("{}", warning);
                }

                app_state.update(cx, |state, cx| {
                    state.complete_task(task_id);
                    cx.emit(AppStateChanged);
                });

                let message = if hook_warnings.is_empty() {
                    format!("Disconnected from {}", profile_name)
                } else {
                    format!(
                        "Disconnected from {} (with {} hook warning{})",
                        profile_name,
                        hook_warnings.len(),
                        if hook_warnings.len() == 1 { "" } else { "s" }
                    )
                };

                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_toast = Some(PendingToast {
                        message,
                        is_error: false,
                    });
                    sidebar.refresh_tree(cx);
                });
            }) {
                log::warn!(
                    "Failed to apply successful disconnect state to sidebar: {:?}",
                    update_error
                );
            }
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
                UserFacingError::new(ErrorKind::User, "Profile not found")
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

    /// Spawns a thread that blocks until the returned gate is released,
    /// standing in for a driver teardown stuck on cancel/close work.
    fn gated_teardown_thread() -> (std::thread::JoinHandle<()>, Arc<(Mutex<bool>, Condvar)>) {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));

        let thread_gate = gate.clone();
        let handle = std::thread::spawn(move || {
            let (lock, condvar) = &*thread_gate;

            let mut released = lock.lock().expect("gate lock");
            while !*released {
                released = condvar.wait(released).expect("gate wait");
            }
        });

        (handle, gate)
    }

    fn release_gate(gate: &Arc<(Mutex<bool>, Condvar)>) {
        let (lock, condvar) = &**gate;
        *lock.lock().expect("gate lock") = true;
        condvar.notify_all();
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
}
