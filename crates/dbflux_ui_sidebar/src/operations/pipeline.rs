use super::connection::{HookPhaseState, run_hook_phase};
use crate::*;
use dbflux_core::observability::actions::{CONNECTION_CONNECT, CONNECTION_CONNECT_FAILED};
use dbflux_core::{CancelToken, HookContext, HookPhase, PipelineState, TaskId, TaskKind};
use dbflux_ui_base::toast::PendingToast;

fn pipeline_stage_task_description(state: &PipelineState) -> Option<String> {
    match state {
        PipelineState::Idle => None,
        PipelineState::Authenticating { provider_name } => {
            Some(format!("Pipeline: Authenticating ({provider_name})"))
        }
        PipelineState::WaitingForLogin { provider_name, .. } => {
            Some(format!("Pipeline: Waiting for {provider_name} login"))
        }
        PipelineState::ResolvingValues { total, resolved } => {
            Some(format!("Pipeline: Resolving values ({resolved}/{total})"))
        }
        PipelineState::OpeningAccess { method_label } => {
            Some(format!("Pipeline: Opening access ({method_label})"))
        }
        PipelineState::Connecting { driver_name } => {
            Some(format!("Pipeline: Connecting driver ({driver_name})"))
        }
        PipelineState::FetchingSchema => Some("Pipeline: Fetching schema".to_string()),
        PipelineState::Connected | PipelineState::Failed { .. } | PipelineState::Cancelled => None,
    }
}

fn pipeline_stage_task_detail_line(state: &PipelineState) -> Option<String> {
    pipeline_stage_task_description(state).map(|description| format!("> {description}"))
}

impl Sidebar {
    /// Connect using the pipeline path (auth, value resolution, access, connect).
    pub(super) fn connect_via_pipeline(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        let (
            input,
            profile_name,
            driver,
            keyring_password,
            pre_connect_hooks,
            post_connect_hooks,
            hook_context,
        ) = match self.app_state.update(cx, |state, _cx| {
            if state.is_operation_pending(profile_id, None) {
                return Err(("Connection already pending".to_string(), false));
            }

            if !state.start_pending_operation(profile_id, None) {
                return Err(("Operation started by another thread".to_string(), false));
            }

            let cancel = CancelToken::new();

            match state.prepare_pipeline_input(profile_id, cancel) {
                Ok((input, profile_name, driver)) => {
                    let keyring_password = state.get_password(&input.profile);
                    let hooks = state.resolve_profile_hooks(&input.profile);
                    let hook_context = HookContext::from_profile(&input.profile);

                    Ok((
                        input,
                        profile_name,
                        driver,
                        keyring_password,
                        hooks.pre_connect,
                        hooks.post_connect,
                        hook_context,
                    ))
                }
                Err(error) => {
                    state.finish_pending_operation(profile_id, None);
                    Err((error, true))
                }
            }
        }) {
            Ok(values) => values,
            Err((message, is_user_error)) => {
                // Benign concurrency skips (already pending / raced start) stay
                // info-only. A real preparation failure is actionable (e.g. a
                // missing auth profile) and must reach the user, not just logs.
                if is_user_error {
                    log::warn!("Pipeline connect failed: {}", message);
                    self.pending_toast = Some(PendingToast {
                        message,
                        is_error: true,
                    });
                    self.refresh_tree(cx);
                    cx.notify();
                } else {
                    log::info!("Pipeline connect skipped: {}", message);
                }
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
            let result = state.start_task(
                TaskKind::Connect,
                format!("Connecting to {} (pipeline)", profile_name),
            );
            cx.emit(dbflux_ui_base::AppStateChanged);
            result
        });

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();
        let (state_tx, state_rx) = dbflux_core::pipeline_state_channel();
        let task_state_rx = state_rx.clone();

        let app_state_for_stage_tasks = self.app_state.clone();
        cx.spawn(async move |_this, cx| {
            let mut watcher = task_state_rx;
            let mut current_stage: Option<(String, TaskId)> = None;

            loop {
                if watcher.changed().await.is_err() {
                    break;
                }

                let state = watcher.borrow().clone();

                if let Some(description) = pipeline_stage_task_description(&state)
                    && current_stage
                        .as_ref()
                        .is_none_or(|(active, _)| active != &description)
                    && let Err(error) = cx.update(|cx| {
                        let stage_state = state.clone();

                        app_state_for_stage_tasks.update(cx, |app_state, cx| {
                            if let Some(line) = pipeline_stage_task_detail_line(&stage_state) {
                                app_state.append_task_details(task_id, format!("{line}\n"));
                            }

                            if let Some((_, stage_task_id)) = current_stage.take() {
                                app_state.complete_task(stage_task_id);
                            }

                            let (stage_task_id, _stage_cancel_token) = app_state
                                .start_task_for_profile(
                                    TaskKind::Connect,
                                    format!("  ↳ {}", description),
                                    Some(profile_id),
                                );
                            current_stage = Some((description.clone(), stage_task_id));

                            cx.emit(AppStateChanged);
                        });
                    })
                {
                    log::warn!("Failed to update pipeline stage subtask: {:?}", error);
                    break;
                }

                if matches!(
                    state,
                    PipelineState::Connected
                        | PipelineState::Failed { .. }
                        | PipelineState::Cancelled
                ) {
                    let terminal_state = state.clone();

                    if let Err(error) = cx.update(|cx| {
                        app_state_for_stage_tasks.update(cx, |app_state, cx| {
                            if let Some((_, stage_task_id)) = current_stage.take() {
                                match &terminal_state {
                                    PipelineState::Cancelled => {
                                        app_state
                                            .append_task_details(task_id, "Pipeline cancelled\n");
                                        app_state.cancel_task(stage_task_id);
                                    }
                                    PipelineState::Failed { error, .. } => {
                                        app_state.append_task_details(
                                            task_id,
                                            format!("Pipeline failed: {error}\n"),
                                        );
                                        app_state.fail_task(stage_task_id, error.clone());
                                    }
                                    _ => {
                                        app_state
                                            .append_task_details(task_id, "Pipeline completed\n");
                                        app_state.complete_task(stage_task_id);
                                    }
                                }
                            }

                            cx.emit(AppStateChanged);
                        });
                    }) {
                        log::warn!("Failed to finalize pipeline stage subtask: {:?}", error);
                    }

                    break;
                }
            }

            if current_stage.is_some()
                && let Err(error) = cx.update(|cx| {
                    app_state_for_stage_tasks.update(cx, |state, cx| {
                        if let Some((_, stage_task_id)) = current_stage.take() {
                            state.complete_task(stage_task_id);
                            cx.emit(AppStateChanged);
                        }
                    });
                })
            {
                log::warn!("Failed to cleanup pipeline stage subtask: {:?}", error);
            }
        })
        .detach();

        cx.emit(SidebarEvent::PipelineStarted {
            profile_name: profile_name.clone(),
            watcher: state_rx,
        });

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
                cx,
            )
            .await
            {
                HookPhaseState::Continue { warnings } => {
                    hook_warnings.extend(warnings);
                }
                HookPhaseState::Aborted { error } => {
                    let _ = state_tx.send(dbflux_core::PipelineState::Failed {
                        stage: "pre_connect_hook".to_string(),
                        error: error.clone(),
                    });

                    if let Err(update_error) = cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            state.cancel_detached_hook_tasks(profile_id);
                            state.fail_task(task_id, error.clone());
                            state.finish_pending_operation(profile_id, None);
                            cx.emit(dbflux_ui_base::AppStateChanged);
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
                            "Failed to apply pipeline pre-connect hook abort state: {:?}",
                            update_error
                        );
                    }
                    return;
                }
                HookPhaseState::Cancelled => {
                    let _ = state_tx.send(dbflux_core::PipelineState::Cancelled);

                    if let Err(update_error) = cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            state.cancel_detached_hook_tasks(profile_id);
                            cx.emit(dbflux_ui_base::AppStateChanged);
                        });

                        if cancel_token.is_cancelled() {
                            app_state.update(cx, |state, cx| {
                                state.finish_pending_operation(profile_id, None);
                                cx.emit(dbflux_ui_base::AppStateChanged);
                            });

                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.refresh_tree(cx);
                            });

                            return;
                        }

                        app_state.update(cx, |state, cx| {
                            state.fail_task(task_id, "Connection hook cancelled");
                            state.finish_pending_operation(profile_id, None);
                            cx.emit(dbflux_ui_base::AppStateChanged);
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
                            "Failed to apply pipeline pre-connect hook cancellation state: {:?}",
                            update_error
                        );
                    }
                    return;
                }
            }

            let state_tx_for_pipeline = state_tx.clone();

            let pipeline_result = cx
                .background_executor()
                .spawn(
                    async move { dbflux_core::run_pipeline(input, &state_tx_for_pipeline).await },
                )
                .await;

            let output = match pipeline_result {
                Ok(output) => output,
                Err(pipeline_error) => {
                    if pipeline_error.stage == "cancelled" {
                        let _ = state_tx.send(dbflux_core::PipelineState::Cancelled);
                    } else {
                        let _ = state_tx.send(dbflux_core::PipelineState::Failed {
                            stage: pipeline_error.stage.clone(),
                            error: pipeline_error.source.to_string(),
                        });
                    }

                    let error_msg = pipeline_error.to_string();

                    // Emit pipeline connection failure audit event.
                    let pipeline_fail_now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
                    let pipeline_fail_driver_id = driver.display_name().to_string();
                    let _ = cx.update(|cx| {
                        let audit_service = app_state.read(cx).audit_service().clone();
                        let mut event = dbflux_core::observability::EventRecord::new(
                            pipeline_fail_now_ms,
                            dbflux_core::observability::EventSeverity::Error,
                            dbflux_core::observability::EventCategory::Connection,
                            dbflux_core::observability::EventOutcome::Failure,
                        );
                        event.action = CONNECTION_CONNECT_FAILED.as_str().to_string();
                        event.actor_type = dbflux_core::observability::EventActorType::User;
                        event.source_id = dbflux_core::observability::EventSourceId::Local;
                        event.connection_id = Some(profile_id.to_string());
                        event.driver_id = Some(pipeline_fail_driver_id);
                        event.summary =
                            format!("Connection to '{}' failed: {}", profile_name, error_msg);
                        event.error_message = Some(error_msg.clone());
                        if let Err(e) = audit_service.record(event) {
                            log::warn!(
                                "Failed to record pipeline connect failure audit event: {}",
                                e
                            );
                        }
                    });

                    if let Err(update_error) = cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            state.cancel_detached_hook_tasks(profile_id);
                            state.fail_task(task_id, error_msg.clone());
                            state.finish_pending_operation(profile_id, None);
                            cx.emit(dbflux_ui_base::AppStateChanged);
                        });

                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.pending_toast = Some(PendingToast {
                                message: error_msg,
                                is_error: true,
                            });
                            sidebar.refresh_tree(cx);
                        });
                    }) {
                        log::warn!("Failed to apply pipeline failure state: {:?}", update_error);
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
            let driver_name_for_state = driver.display_name().to_string();
            let driver_name_for_audit = driver.display_name().to_string();

            let connect_result = cx
                .background_executor()
                .spawn(async move {
                    let _ = state_tx_for_connect.send(dbflux_core::PipelineState::Connecting {
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

                    let _ = state_tx_for_connect.send(dbflux_core::PipelineState::FetchingSchema);

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
                    let _ = state_tx.send(dbflux_core::PipelineState::Failed {
                        stage: "driver_connect".to_string(),
                        error: error.clone(),
                    });

                    // Emit driver connect failure audit event.
                    let driver_fail_now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
                    let driver_fail_driver_id = driver_name_for_audit.clone();
                    let _ = cx.update(|cx| {
                        let audit_service = app_state.read(cx).audit_service().clone();
                        let mut event = dbflux_core::observability::EventRecord::new(
                            driver_fail_now_ms,
                            dbflux_core::observability::EventSeverity::Error,
                            dbflux_core::observability::EventCategory::Connection,
                            dbflux_core::observability::EventOutcome::Failure,
                        );
                        event.action = CONNECTION_CONNECT_FAILED.as_str().to_string();
                        event.actor_type = dbflux_core::observability::EventActorType::User;
                        event.source_id = dbflux_core::observability::EventSourceId::Local;
                        event.connection_id = Some(profile_id.to_string());
                        event.driver_id = Some(driver_fail_driver_id);
                        event.summary =
                            format!("Connection to '{}' failed: {}", profile_name, error);
                        event.error_message = Some(error.clone());
                        if let Err(e) = audit_service.record(event) {
                            log::warn!(
                                "Failed to record driver connect failure audit event: {}",
                                e
                            );
                        }
                    });

                    if let Err(update_error) = cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            state.cancel_detached_hook_tasks(profile_id);
                            state.fail_task(task_id, error.clone());
                            state.finish_pending_operation(profile_id, None);
                            cx.emit(dbflux_ui_base::AppStateChanged);
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
                            "Failed to apply pipeline driver connect failure: {:?}",
                            update_error
                        );
                    }
                    return;
                }
            };

            match run_hook_phase(
                app_state.clone(),
                profile_id,
                profile_name.clone(),
                HookPhase::PostConnect,
                post_connect_hooks,
                hook_context,
                Some(cancel_token.clone()),
                cx,
            )
            .await
            {
                HookPhaseState::Continue { warnings } => {
                    hook_warnings.extend(warnings);
                }
                HookPhaseState::Aborted { error } => {
                    let _ = state_tx.send(dbflux_core::PipelineState::Failed {
                        stage: "post_connect_hook".to_string(),
                        error: error.clone(),
                    });

                    if let Err(update_error) = cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            state.cancel_detached_hook_tasks(profile_id);
                            state.fail_task(task_id, error.clone());
                            state.finish_pending_operation(profile_id, None);
                            cx.emit(dbflux_ui_base::AppStateChanged);
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
                            "Failed to apply pipeline post-connect hook abort state: {:?}",
                            update_error
                        );
                    }
                    return;
                }
                HookPhaseState::Cancelled => {
                    let _ = state_tx.send(dbflux_core::PipelineState::Cancelled);

                    if let Err(update_error) = cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            state.cancel_detached_hook_tasks(profile_id);
                            cx.emit(dbflux_ui_base::AppStateChanged);
                        });

                        if cancel_token.is_cancelled() {
                            app_state.update(cx, |state, cx| {
                                state.finish_pending_operation(profile_id, None);
                                cx.emit(dbflux_ui_base::AppStateChanged);
                            });

                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.refresh_tree(cx);
                            });

                            return;
                        }

                        app_state.update(cx, |state, cx| {
                            state.fail_task(task_id, "Post-connect hook cancelled");
                            state.finish_pending_operation(profile_id, None);
                            cx.emit(dbflux_ui_base::AppStateChanged);
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
                            "Failed to apply pipeline post-connect hook cancellation state: {:?}",
                            update_error
                        );
                    }
                    return;
                }
            }

            let _ = state_tx.send(dbflux_core::PipelineState::Connected);

            let connected_name = profile.name.clone();
            let connected_driver_id = profile.driver_id.clone();

            // Emit pipeline connection success audit event.
            let connect_success_now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
            let _ = cx.update(|cx| {
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

            if let Err(update_error) = cx.update(|cx| {
                for warning in &hook_warnings {
                    log::warn!("{}", warning);
                }

                app_state.update(cx, |state, cx| {
                    state.complete_task(task_id);
                    state.finish_pending_operation(profile_id, None);
                    state.apply_connect_profile(
                        profile,
                        connection.into(),
                        schema,
                        tunnel_handle,
                        false,
                    );
                    cx.emit(dbflux_ui_base::AppStateChanged);
                    cx.notify();
                });

                let message = if hook_warnings.is_empty() {
                    format!("Connected to {}", connected_name)
                } else {
                    format!(
                        "Connected to {} (with {} hook warning{})",
                        connected_name,
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
                    "Failed to apply pipeline connection result: {:?}",
                    update_error
                );
            }
        })
        .detach();
    }
}
