use crate::*;
use dbflux_app::hook_executor::CompositeExecutor;
use dbflux_app::{ExternalDriverDiagnostic, ExternalDriverStage};
use dbflux_core::observability::actions::{
    CONNECTION_CONNECT, CONNECTION_CONNECT_FAILED, CONNECTION_CONNECTING, CONNECTION_DISCONNECT,
    HOOK_EXECUTE, HOOK_EXECUTE_FAILED,
};
use dbflux_core::{
    CancelToken, ConnectionHook, DatabaseConnection, DbSchemaInfo, DetachedProcessHandle,
    HookContext, HookExecutor, HookKind, HookPhase, HookResult, OutputReceiver, PipelineState,
    PrepareConnectError, ProcessExecutionError, TaskId, TaskKind,
    detached_process_channel, execute_streaming_process, output_channel,
};
use dbflux_ssh::is_passphrase_required_error_str;
use dbflux_ui_base::platform;
use dbflux_ui_base::toast::PendingToast;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DETACHED_HOOK_STARTED_MESSAGE: &str = "Detached process started in background";

type DetachedReadyReceiver = std::sync::mpsc::Receiver<Result<(), String>>;

struct DetachedHookTaskStart {
    ready_receiver: Option<DetachedReadyReceiver>,
}

pub(crate) struct HeldDatabaseConnection {
    pub(crate) database: String,
    pub(crate) connection: DatabaseConnection,
    pub(crate) cached_schema: Option<DbSchemaInfo>,
    pub(crate) previous_active_database: Option<String>,
}

enum HookPhaseState {
    Continue { warnings: Vec<String> },
    Aborted { error: String },
    Cancelled,
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

fn hook_task_details(
    hook: &ConnectionHook,
    phase: HookPhase,
    command_display: &str,
    result: &Result<HookResult, String>,
) -> String {
    let label = match hook.kind {
        HookKind::Command { .. } => "Command",
        HookKind::Script { .. } => "Script",
        HookKind::Lua { .. } => "Lua",
    };

    match result {
        Ok(output) => {
            let mut lines = vec![
                format!("Phase: {}", phase.label()),
                format!("{}: {}", label, command_display),
                format!("Summary: {}", hook.summary()),
                format!("Timed out: {}", output.timed_out),
                format!("Exit code: {:?}", output.exit_code),
                String::new(),
                "stdout:".to_string(),
            ];

            if output.stdout.trim().is_empty() {
                lines.push("<empty>".to_string());
            } else {
                lines.push(output.stdout.clone());
            }

            lines.push(String::new());
            lines.push("stderr:".to_string());

            if output.stderr.trim().is_empty() {
                lines.push("<empty>".to_string());
            } else {
                lines.push(output.stderr.clone());
            }

            if output.warnings.is_empty() {
                return lines.join("\n");
            }

            lines.push(String::new());
            lines.push("warnings:".to_string());
            lines.extend(output.warnings.iter().cloned());

            lines.join("\n")
        }
        Err(error) => {
            format!(
                "Phase: {}\n{}: {}\nSummary: {}\nError: {}",
                phase.label(),
                label,
                command_display,
                hook.summary(),
                error
            )
        }
    }
}

fn detached_hook_task_details(
    phase: HookPhase,
    command_display: &str,
    result: &Result<HookResult, String>,
) -> String {
    match result {
        Ok(output) => {
            let mut lines = vec![
                format!("Phase: {}", phase.label()),
                format!("Process: {}", command_display),
                format!("Timed out: {}", output.timed_out),
                format!("Exit code: {:?}", output.exit_code),
                String::new(),
                "stdout:".to_string(),
            ];

            if output.stdout.trim().is_empty() {
                lines.push("<empty>".to_string());
            } else {
                lines.push(output.stdout.clone());
            }

            lines.push(String::new());
            lines.push("stderr:".to_string());

            if output.stderr.trim().is_empty() {
                lines.push("<empty>".to_string());
            } else {
                lines.push(output.stderr.clone());
            }

            lines.join("\n")
        }
        Err(error) => format!(
            "Phase: {}\nProcess: {}\nError: {}",
            phase.label(),
            command_display,
            error
        ),
    }
}

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

fn hook_started_detached_details(
    hook: &ConnectionHook,
    phase: HookPhase,
    command_display: &str,
) -> String {
    let mut lines = vec![
        format!("Phase: {}", phase.label()),
        format!("Summary: {}", hook.summary()),
        format!("Process: {}", command_display),
        DETACHED_HOOK_STARTED_MESSAGE.to_string(),
    ];

    if let Some(ready_signal) = &hook.ready_signal {
        lines.push(format!("Waiting for ready signal: {ready_signal}"));
    }

    lines.join("\n")
}

fn detached_process_error_message(error: &ProcessExecutionError, description: &str) -> String {
    match error {
        ProcessExecutionError::Spawn(message) => {
            format!("Failed to execute detached hook process '{description}': {message}")
        }
        ProcessExecutionError::Wait(message) => {
            format!("Failed to wait for detached hook process '{description}': {message}")
        }
        ProcessExecutionError::Cancelled { stdout, stderr } => {
            format!("Detached hook process '{description}' cancelled\n{stdout}{stderr}")
        }
        ProcessExecutionError::TimedOut { stdout, stderr } => {
            format!("Detached hook process '{description}' timed out\n{stdout}{stderr}")
        }
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

fn start_task_output_drain(
    app_state: Entity<AppStateEntity>,
    task_id: TaskId,
    receiver: OutputReceiver,
    ready_signal: Option<String>,
    ready_sender: Option<std::sync::mpsc::Sender<Result<(), String>>>,
    ready_seen: std::sync::Arc<std::sync::atomic::AtomicBool>,
    cx: &mut AsyncApp,
) {
    cx.spawn(async move |cx| {
        drain_task_output(
            app_state,
            task_id,
            receiver,
            ready_signal,
            ready_sender,
            ready_seen,
            cx,
        )
        .await;
    })
    .detach();
}

async fn drain_task_output(
    app_state: Entity<AppStateEntity>,
    task_id: TaskId,
    receiver: OutputReceiver,
    ready_signal: Option<String>,
    ready_sender: Option<std::sync::mpsc::Sender<Result<(), String>>>,
    ready_seen: std::sync::Arc<std::sync::atomic::AtomicBool>,
    cx: &mut AsyncApp,
) {
    let mut ready_sender = ready_sender;
    let mut signal_buffer = String::new();

    loop {
        cx.background_executor()
            .timer(std::time::Duration::from_millis(100))
            .await;

        let mut chunk = String::new();
        let mut disconnected = false;

        loop {
            match receiver.try_recv() {
                Ok(event) => chunk.push_str(&event.text),
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        if !chunk.is_empty() {
            if let Some(signal) = ready_signal.as_deref() {
                signal_buffer.push_str(&chunk);

                if !ready_seen.load(std::sync::atomic::Ordering::SeqCst)
                    && signal_buffer.contains(signal)
                {
                    ready_seen.store(true, std::sync::atomic::Ordering::SeqCst);

                    if let Some(sender) = ready_sender.take() {
                        let _ = sender.send(Ok(()));
                    }
                }

                let max_buffer_len = signal.len().saturating_add(1024);
                if signal_buffer.len() > max_buffer_len {
                    let keep_from = signal_buffer.len() - max_buffer_len;
                    signal_buffer = signal_buffer.split_off(keep_from);
                }
            }

            if let Err(error) = cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    state.append_task_details(task_id, &chunk);
                    cx.emit(AppStateChanged);
                });
            }) {
                log::warn!(
                    "Failed to append detached hook output to task details: {:?}",
                    error
                );
            }
        }

        if disconnected {
            break;
        }
    }
}

fn start_detached_hook_task(
    app_state: Entity<AppStateEntity>,
    profile_id: Uuid,
    profile_name: &str,
    phase: HookPhase,
    handle: DetachedProcessHandle,
    parent_cancel_token: Option<CancelToken>,
    cx: &mut AsyncApp,
) -> Result<DetachedHookTaskStart, ()> {
    let description = handle.description.clone();
    let ready_signal = handle.ready_signal.clone();

    let (task_id, cancel_token) = cx
        .update(|cx| {
            app_state.update(cx, |state, cx| {
                let task = state.start_task_for_profile(
                    TaskKind::Hook { phase },
                    format!(
                        "Hook Process: {} — {} — {}",
                        phase.label(),
                        profile_name,
                        description
                    ),
                    Some(profile_id),
                );
                state.register_detached_hook_task(profile_id, task.0);
                cx.emit(AppStateChanged);
                task
            })
        })
        .map_err(|_| ())?;

    let (ready_sender, ready_receiver) = if ready_signal.is_some() {
        let (sender, receiver) = std::sync::mpsc::channel();
        (Some(sender), Some(receiver))
    } else {
        (None, None)
    };

    let ready_seen = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (output_sender, output_receiver) = output_channel();
    start_task_output_drain(
        app_state.clone(),
        task_id,
        output_receiver,
        ready_signal.clone(),
        ready_sender.clone(),
        ready_seen.clone(),
        cx,
    );

    let description_for_completion = description.clone();
    let app_state_for_completion = app_state.clone();
    let task_cancel_token = cancel_token.clone();
    let parent_cancel_for_task = parent_cancel_token.clone();
    let ready_signal_for_completion = ready_signal.clone();

    cx.spawn(async move |cx| {
        let result = cx
            .background_executor()
            .spawn(async move {
                let mut child = handle.child;
                execute_streaming_process(
                    &mut child,
                    &cancel_token,
                    parent_cancel_for_task.as_ref(),
                    handle.timeout,
                    None,
                    Some(&output_sender),
                )
            })
            .await;

        if let Err(error) = cx.update(|cx| {
            app_state_for_completion.update(cx, |state, cx| {
                state.unregister_detached_hook_task(profile_id, task_id);

                if task_cancel_token.is_cancelled() {
                    cx.emit(AppStateChanged);
                    return;
                }

                if !ready_seen.load(std::sync::atomic::Ordering::SeqCst)
                    && let (Some(signal), Some(sender)) =
                        (ready_signal_for_completion.as_ref(), ready_sender.as_ref())
                {
                    let _ = sender.send(Err(format!(
                        "Detached hook process exited before ready signal '{signal}'",
                    )));
                }

                let details_result = result.clone().map_err(|error| {
                    detached_process_error_message(&error, &description_for_completion)
                });

                let details =
                    detached_hook_task_details(phase, &description_for_completion, &details_result);

                match result {
                    Ok(output) if output.is_success() => {
                        state.complete_task_with_details(task_id, details);
                    }
                    Ok(output) if output.timed_out => {
                        state.fail_task_with_details(
                            task_id,
                            "Detached hook process timed out",
                            details,
                        );
                    }
                    Ok(_) => {
                        state.fail_task_with_details(
                            task_id,
                            "Detached hook process failed",
                            details,
                        );
                    }
                    Err(error) => {
                        state.fail_task_with_details(
                            task_id,
                            detached_process_error_message(&error, &description_for_completion),
                            details,
                        );
                    }
                }

                cx.emit(AppStateChanged);
            });
        }) {
            log::warn!(
                "Failed to apply detached hook completion to sidebar state: {:?}",
                error
            );
        }
    })
    .detach();

    Ok(DetachedHookTaskStart { ready_receiver })
}

async fn wait_for_detached_hook_ready(
    receiver: DetachedReadyReceiver,
    parent_cancel_token: Option<&CancelToken>,
    cx: &mut AsyncApp,
) -> Result<(), HookPhaseState> {
    loop {
        if parent_cancel_token.is_some_and(CancelToken::is_cancelled) {
            return Err(HookPhaseState::Cancelled);
        }

        match receiver.try_recv() {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => return Err(HookPhaseState::Aborted { error }),
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                return Err(HookPhaseState::Aborted {
                    error: "Detached hook readiness watcher disconnected unexpectedly".to_string(),
                });
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                cx.background_executor()
                    .timer(std::time::Duration::from_millis(50))
                    .await;
            }
        }
    }
}

fn probe_tcp_endpoint(host: &str, port: u16) -> Result<bool, String> {
    let probe_host = if host.eq_ignore_ascii_case("localhost") {
        "127.0.0.1"
    } else {
        host
    };

    let addrs: Vec<SocketAddr> = (probe_host, port)
        .to_socket_addrs()
        .map_err(|error| format!("Failed to resolve {probe_host}:{port}: {error}"))?
        .collect();

    if addrs.is_empty() {
        return Err(format!("No addresses resolved for {probe_host}:{port}"));
    }

    for addr in addrs {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return Ok(true);
        }
    }

    Ok(false)
}

async fn wait_for_hook_endpoint_ready(
    host: String,
    port: u16,
    parent_cancel_token: Option<&CancelToken>,
    cx: &mut AsyncApp,
) -> Result<(), HookPhaseState> {
    let start = Instant::now();

    loop {
        if parent_cancel_token.is_some_and(CancelToken::is_cancelled) {
            return Err(HookPhaseState::Cancelled);
        }

        let host_for_probe = host.clone();
        match cx
            .background_executor()
            .spawn(async move { probe_tcp_endpoint(&host_for_probe, port) })
            .await
        {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => {
                return Err(HookPhaseState::Aborted { error });
            }
        }

        if start.elapsed() > Duration::from_secs(15) {
            return Err(HookPhaseState::Aborted {
                error: format!(
                    "Detached pre-connect hook reported ready, but {host}:{port} never accepted connections"
                ),
            });
        }

        cx.background_executor()
            .timer(Duration::from_millis(100))
            .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_hook_phase(
    app_state: Entity<AppStateEntity>,
    profile_id: Uuid,
    profile_name: String,
    phase: HookPhase,
    hooks: Vec<ConnectionHook>,
    context: HookContext,
    parent_cancel: Option<CancelToken>,
    cx: &mut AsyncApp,
) -> HookPhaseState {
    let mut warnings = Vec::new();
    let executor = CompositeExecutor::new();

    for hook in hooks {
        if !hook.enabled {
            continue;
        }

        if parent_cancel
            .as_ref()
            .is_some_and(CancelToken::is_cancelled)
        {
            return HookPhaseState::Cancelled;
        }

        let command_display = hook.display_command();

        let (task_id, hook_cancel_token) = match cx.update(|cx| {
            app_state.update(cx, |state, cx| {
                let task = state.start_hook_task_for_profile(
                    phase,
                    profile_id,
                    &profile_name,
                    &command_display,
                );
                cx.emit(AppStateChanged);
                task
            })
        }) {
            Ok(value) => value,
            Err(_) => return HookPhaseState::Cancelled,
        };

        if phase == HookPhase::PreConnect && hook.is_detached() && hook.ready_signal.is_none() {
            let error = "Detached pre-connect hooks must set a ready signal before DBFlux can continue connecting"
                .to_string();

            if let Err(error) = cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    state.fail_task(task_id, error.clone());
                    cx.emit(AppStateChanged);
                });
            }) {
                log::warn!(
                    "Failed to apply detached hook registration failure state: {:?}",
                    error
                );
            }

            return HookPhaseState::Aborted { error };
        }

        let (output_sender, output_receiver) = output_channel();
        start_task_output_drain(
            app_state.clone(),
            task_id,
            output_receiver,
            None,
            None,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
            cx,
        );

        let (detached_sender, detached_receiver) = detached_process_channel();

        let parent_cancel_for_hook = parent_cancel.clone();
        let hook_for_execution = hook.clone();
        let mut hook_context = context.clone();
        hook_context.phase = Some(phase);
        let hook_cancel_for_execution = hook_cancel_token.clone();
        let executor = executor.clone();

        // Capture start time and emit hook start event.
        let hook_start_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
        let hook_command_for_audit = hook.display_command();
        let phase_label = phase.label();
        let _ = cx.update(|cx| {
            if let Err(error) = app_state.read(cx).audit_service().record(
                dbflux_core::observability::EventRecord::new(
                    hook_start_ms,
                    dbflux_core::observability::EventSeverity::Info,
                    dbflux_core::observability::EventCategory::Hook,
                    dbflux_core::observability::EventOutcome::Success,
                )
                .with_typed_action(HOOK_EXECUTE)
                .with_summary(format!(
                    "Hook '{}' ({}) started",
                    hook_command_for_audit, phase_label
                ))
                .with_origin(dbflux_core::observability::EventOrigin::hook())
                .with_actor_id("hook")
                .with_object_ref("hook", &hook_command_for_audit)
                .with_connection_context(
                    profile_id.to_string(),
                    context.database.as_deref().unwrap_or(""),
                    context.db_kind.clone(),
                ),
            ) {
                log::warn!("Failed to record hook lifecycle audit event: {}", error);
            }
        });

        let hook_result = cx
            .background_executor()
            .spawn(async move {
                executor.execute_hook(
                    &hook_for_execution,
                    &hook_context,
                    &hook_cancel_for_execution,
                    parent_cancel_for_hook.as_ref(),
                    Some(&output_sender),
                    Some(&detached_sender),
                )
            })
            .await;

        let detached_handles: Vec<_> = detached_receiver.try_iter().collect();

        if let Ok(output) = &hook_result {
            warnings.extend(output.warnings.iter().cloned());
        }

        let detached_started = !detached_handles.is_empty();
        let mut ready_receivers = Vec::new();

        let mut detached_task_registration_failed = None;
        for handle in detached_handles {
            match start_detached_hook_task(
                app_state.clone(),
                profile_id,
                &profile_name,
                phase,
                handle,
                parent_cancel.clone(),
                cx,
            ) {
                Ok(start) => {
                    if let Some(receiver) = start.ready_receiver {
                        ready_receivers.push(receiver);
                    }
                }
                Err(_) => {
                    detached_task_registration_failed =
                        Some("Failed to register detached hook task".to_string());
                    break;
                }
            }
        }

        if let Some(error) = detached_task_registration_failed {
            if let Err(update_error) = cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    state.fail_task(task_id, error.clone());
                    cx.emit(AppStateChanged);
                });
            }) {
                log::warn!(
                    "Failed to apply detached hook registration failure state: {:?}",
                    update_error
                );
            }

            return HookPhaseState::Aborted { error };
        }

        let (succeeded, failure_message, abort_error, cancelled) = if detached_started {
            let mut readiness_error = None;

            for receiver in ready_receivers {
                if let Err(state) =
                    wait_for_detached_hook_ready(receiver, parent_cancel.as_ref(), cx).await
                {
                    readiness_error = Some(state);
                    break;
                }
            }

            if readiness_error.is_none()
                && phase == HookPhase::PreConnect
                && let (Some(host), Some(port)) = (context.host.clone(), context.port)
                && let Err(state) =
                    wait_for_hook_endpoint_ready(host, port, parent_cancel.as_ref(), cx).await
            {
                readiness_error = Some(state);
            }

            match readiness_error {
                None => (true, None, None, false),
                Some(HookPhaseState::Cancelled) => (false, None, None, true),
                Some(HookPhaseState::Aborted { error }) => {
                    (false, Some(error.clone()), Some(error), false)
                }
                Some(HookPhaseState::Continue { warnings }) => {
                    let message = if warnings.is_empty() {
                        "Unexpected hook readiness state: continue without warning".to_string()
                    } else {
                        format!("Unexpected hook readiness state: {}", warnings.join("; "))
                    };

                    log::error!("[HOOK] {}", message);
                    (false, Some(message.clone()), Some(message), false)
                }
            }
        } else {
            let succeeded = hook_result
                .as_ref()
                .is_ok_and(|output: &HookResult| output.is_success());

            let failure_message = if succeeded {
                None
            } else {
                Some(hook.failure_message(phase, &hook_result))
            };

            let abort_error =
                if succeeded || hook.on_failure != dbflux_core::HookFailureMode::Disconnect {
                    None
                } else {
                    failure_message.clone()
                };

            (succeeded, failure_message, abort_error, false)
        };

        let details = if detached_started {
            hook_started_detached_details(&hook, phase, &command_display)
        } else {
            hook_task_details(&hook, phase, &command_display, &hook_result)
        };

        if let Err(error) = cx.update(|cx| {
            app_state.update(cx, |state, cx| {
                if let Some(message) = &failure_message {
                    state.fail_task_with_details(task_id, message.clone(), details.clone());
                } else {
                    state.complete_task_with_details(task_id, details.clone());
                }

                cx.emit(AppStateChanged);
            });
        }) {
            log::warn!(
                "Failed to apply hook phase task completion state: {:?}",
                error
            );
        }

        // Emit hook completion/failure audit event.
        let hook_end_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
        let duration_ms = hook_end_ms - hook_start_ms;
        let (hook_complete_action, hook_complete_outcome, hook_complete_severity) = if cancelled {
            (
                HOOK_EXECUTE_FAILED,
                dbflux_core::observability::EventOutcome::Cancelled,
                dbflux_core::observability::EventSeverity::Error,
            )
        } else if failure_message.is_some() {
            (
                HOOK_EXECUTE_FAILED,
                dbflux_core::observability::EventOutcome::Failure,
                dbflux_core::observability::EventSeverity::Error,
            )
        } else {
            (
                HOOK_EXECUTE,
                dbflux_core::observability::EventOutcome::Success,
                dbflux_core::observability::EventSeverity::Info,
            )
        };
        let hook_complete_summary = if cancelled {
            format!("Hook '{}' ({}) cancelled", command_display, phase.label())
        } else if let Some(ref msg) = failure_message {
            format!(
                "Hook '{}' ({}) failed: {}",
                command_display,
                phase.label(),
                msg
            )
        } else {
            format!("Hook '{}' ({}) completed", command_display, phase.label())
        };
        let _ = cx.update(|cx| {
            let audit_service = app_state.read(cx).audit_service().clone();
            let mut event = dbflux_core::observability::EventRecord::new(
                hook_end_ms,
                hook_complete_severity,
                dbflux_core::observability::EventCategory::Hook,
                hook_complete_outcome,
            );
            event.action = hook_complete_action.as_str().to_string();
            event = event.with_origin(dbflux_core::observability::EventOrigin::hook());
            event.connection_id = Some(profile_id.to_string());
            event.database_name = context.database.clone();
            event.driver_id = Some(context.db_kind.clone());
            event.object_type = Some("hook".to_string());
            event.object_id = Some(command_display.clone());
            event.summary = hook_complete_summary.clone();
            event.duration_ms = Some(duration_ms);
            event.details_json = Some(
                serde_json::json!({
                    "hook_name": command_display,
                    "phase": phase.label(),
                })
                .to_string(),
            );
            if let Some(ref msg) = failure_message {
                event.error_message = Some(msg.clone());
            }
            if let Err(e) = audit_service.record(event) {
                log::warn!("Failed to record hook lifecycle audit event: {}", e);
            }
        });

        if cancelled {
            return HookPhaseState::Cancelled;
        }

        if succeeded {
            continue;
        }

        if hook_cancel_token.is_cancelled()
            || parent_cancel
                .as_ref()
                .is_some_and(CancelToken::is_cancelled)
        {
            return HookPhaseState::Cancelled;
        }

        if let Some(error) = abort_error {
            return HookPhaseState::Aborted { error };
        }

        match hook.on_failure {
            dbflux_core::HookFailureMode::Warn => {
                warnings.push(hook.failure_message(phase, &hook_result));
            }
            dbflux_core::HookFailureMode::Ignore => {
                log::warn!("{}", hook.failure_message(phase, &hook_result));
            }
            dbflux_core::HookFailureMode::Disconnect => {}
        }
    }

    HookPhaseState::Continue { warnings }
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

    /// Connect using the pipeline path (auth, value resolution, access, connect).
    fn connect_via_pipeline(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
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

            if let Err(update_error) = cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    state.disconnect(profile_id);
                    state.cancel_detached_hook_tasks(profile_id);
                    cx.emit(AppStateChanged);
                    cx.notify();
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
            }) {
                log::warn!(
                    "Failed to apply disconnect transition to app state: {:?}",
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
            state.disconnect(profile_id);
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
        let profile = self
            .app_state
            .read(cx)
            .profiles()
            .iter()
            .find(|p| p.id == profile_id)
            .cloned();

        let Some(profile) = profile else {
            log::error!("Profile not found: {}", profile_id);
            return;
        };

        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(600.0), px(550.0)), cx);

        let mut options = WindowOptions {
            app_id: Some(dbflux_core::ReleaseChannel::current().app_id().into()),
            titlebar: Some(TitlebarOptions {
                title: Some("Edit Connection".into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            ..Default::default()
        };
        platform::apply_window_options(&mut options, 600.0, 500.0);

        if let Err(error) = cx.open_window(options, |window, cx| {
            let manager =
                cx.new(|cx| ConnectionManagerWindow::new_for_edit(app_state, &profile, window, cx));
            cx.new(|cx| Root::new(manager, window, cx))
        }) {
            log::warn!("Failed to open connection editor window: {:?}", error);
        }
    }
}
