use super::*;
use crate::hook_executor::CompositeExecutor;
use crate::platform;
use dbflux_core::observability::actions::{
    CONNECTION_CONNECT, CONNECTION_CONNECT_FAILED, CONNECTION_CONNECTING, CONNECTION_DISCONNECT,
    HOOK_EXECUTE, HOOK_EXECUTE_FAILED,
};
use dbflux_core::{
    CancelToken, ConnectionHook, DetachedProcessHandle, HookContext, HookExecutor, HookKind,
    HookPhase, HookResult, OutputReceiver, PipelineState, ProcessExecutionError, TaskId, TaskKind,
    detached_process_channel, execute_streaming_process, output_channel,
};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

const DETACHED_HOOK_STARTED_MESSAGE: &str = "Detached process started in background";

type DetachedReadyReceiver = std::sync::mpsc::Receiver<Result<(), String>>;

struct DetachedHookTaskStart {
    ready_receiver: Option<DetachedReadyReceiver>,
}

enum HookPhaseState {
    Continue { warnings: Vec<String> },
    Aborted { error: String },
    Cancelled,
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

fn start_task_output_drain(
    app_state: Entity<AppState>,
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
    app_state: Entity<AppState>,
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
    app_state: Entity<AppState>,
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
    app_state: Entity<AppState>,
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
    pub(super) fn handle_database_click(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(SchemaNodeId::Database {
            profile_id,
            name: db_name,
        }) = parse_node_id(item_id)
        else {
            return;
        };

        let strategy = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.schema_loading_strategy());

        match strategy {
            Some(SchemaLoadingStrategy::LazyPerDatabase) => {
                self.handle_lazy_database_click(profile_id, &db_name, cx);
            }
            Some(SchemaLoadingStrategy::ConnectionPerDatabase) => {
                self.handle_connection_per_database_click(profile_id, &db_name, cx);
            }
            Some(SchemaLoadingStrategy::SingleDatabase) | None => {
                log::info!("Database click not applicable for this database type");
            }
        }
    }

    pub(super) fn close_database(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(SchemaNodeId::Database {
            profile_id,
            name: db_name,
        }) = parse_node_id(item_id)
        else {
            return;
        };

        self.app_state.update(cx, |state, cx| {
            if let Some(conn) = state.connections_mut().get_mut(&profile_id) {
                conn.database_schemas.remove(&db_name);

                if let Some(db_conn) = conn.database_connections.remove(&db_name) {
                    std::thread::spawn(move || {
                        let _ = db_conn.connection.cancel_active();
                        drop(db_conn);
                    });
                }

                if conn.active_database.as_deref() == Some(db_name.as_str()) {
                    conn.active_database = None;
                }
            }
            cx.emit(AppStateChanged);
        });

        // Collapse the database node in the tree
        self.set_expanded(item_id, false, cx);

        self.refresh_tree(cx);
    }

    /// Creates a new folder at the root level.
    pub fn create_root_folder(&mut self, cx: &mut Context<Self>) {
        let folder_id = self.app_state.update(cx, |state, cx| {
            let id = state.create_folder("New Folder", None);
            cx.emit(AppStateChanged);
            id
        });

        self.refresh_tree(cx);

        let item_id = SchemaNodeId::ConnectionFolder { node_id: folder_id }.to_string();

        self.select_and_rename_item(&item_id, cx);
    }

    pub(super) fn create_folder_from_context(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let parent_id = match parse_node_id(item_id) {
            Some(SchemaNodeId::ConnectionFolder { node_id }) => Some(node_id),
            _ => None,
        };

        if parent_id.is_some() {
            self.set_expanded(item_id, true, cx);
        }

        let folder_id = self.app_state.update(cx, |state, cx| {
            let id = state.create_folder("New Folder", parent_id);
            cx.emit(AppStateChanged);
            id
        });

        self.refresh_tree(cx);

        let new_item_id = SchemaNodeId::ConnectionFolder { node_id: folder_id }.to_string();

        self.select_and_rename_item(&new_item_id, cx);
    }

    /// Selects the item, scrolls to it, and queues a rename for the next render.
    fn select_and_rename_item(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let tree_state = self.active_tree_state().clone();

        if let Some(index) = self.find_item_index(item_id, cx) {
            tree_state.update(cx, |state, cx| {
                state.set_selected_index(Some(index), cx);
                state.scroll_to_item(index, gpui::ScrollStrategy::Center);
            });
        }

        self.pending_rename_item = Some(item_id.to_string());
        cx.notify();
    }

    pub(super) fn duplicate_profile(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(item_id) else {
            return;
        };

        let Some(new_id) = self.app_state.update(cx, |state, cx| {
            let original = state
                .profiles()
                .iter()
                .find(|p| p.id == profile_id)?
                .clone();

            let folder_id = state
                .connection_tree()
                .find_by_profile(profile_id)
                .and_then(|node| node.parent_id);

            let password = state.get_password(&original);
            let ssh_password = state.get_ssh_password(&original);

            let mut cloned = original;
            cloned.id = Uuid::new_v4();
            cloned.name = format!("{} (Copy)", cloned.name);
            let new_id = cloned.id;

            state.add_profile_in_folder(cloned.clone(), folder_id);

            if let Some(ref pw) = password {
                state.save_password(&cloned, pw);
            }
            if let Some(ref pw) = ssh_password {
                state.save_ssh_password(&cloned, pw);
            }

            cx.emit(AppStateChanged);
            Some(new_id)
        }) else {
            return;
        };

        self.refresh_tree(cx);

        let new_item_id = SchemaNodeId::Profile { profile_id: new_id }.to_string();

        self.select_and_rename_item(&new_item_id, cx);
    }

    pub(super) fn create_connection_in_folder(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(SchemaNodeId::ConnectionFolder { node_id: folder_id }) = parse_node_id(item_id)
        else {
            return;
        };

        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(600.0), px(550.0)), cx);

        let mut options = WindowOptions {
            app_id: Some("dbflux".into()),
            titlebar: Some(TitlebarOptions {
                title: Some("Connection Manager".into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            ..Default::default()
        };
        platform::apply_window_options(&mut options, 600.0, 500.0);

        if let Err(error) = cx.open_window(options, |window, cx| {
            let manager = cx
                .new(|cx| ConnectionManagerWindow::new_in_folder(app_state, folder_id, window, cx));
            cx.new(|cx| Root::new(manager, window, cx))
        }) {
            log::warn!(
                "Failed to open connection manager window for folder: {:?}",
                error
            );
        }
    }

    pub(super) fn start_rename(
        &mut self,
        item_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Handle folder rename
        if let Some(SchemaNodeId::ConnectionFolder { node_id: folder_id }) = parse_node_id(item_id)
        {
            let current_name = self
                .app_state
                .read(cx)
                .connection_tree()
                .find_by_id(folder_id)
                .map(|f| f.name.clone())
                .unwrap_or_default();

            self.editing_id = Some(folder_id);
            self.editing_is_folder = true;
            self.rename_input.update(cx, |input, cx| {
                input.set_value(&current_name, window, cx);
                input.focus(window, cx);
            });
            cx.notify();
            return;
        }

        // Handle profile rename
        if let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(item_id) {
            let current_name = self
                .app_state
                .read(cx)
                .profiles()
                .iter()
                .find(|p| p.id == profile_id)
                .map(|p| p.name.clone())
                .unwrap_or_default();

            self.editing_id = Some(profile_id);
            self.editing_is_folder = false;
            self.rename_input.update(cx, |input, cx| {
                input.set_value(&current_name, window, cx);
                input.focus(window, cx);
            });
            cx.notify();
            return;
        }

        let script_path = match parse_node_id(item_id) {
            Some(SchemaNodeId::ScriptFile { path }) => Some(std::path::PathBuf::from(path)),
            Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                Some(std::path::PathBuf::from(p))
            }
            _ => None,
        };

        if let Some(path) = script_path {
            let current_name = path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            self.editing_script_path = Some(path);
            self.rename_input.update(cx, |input, cx| {
                input.set_value(&current_name, window, cx);
                input.focus(window, cx);
            });
            cx.notify();
        }
    }

    pub(super) fn delete_folder_from_context(&mut self, item_id: &str, cx: &mut Context<Self>) {
        if let Some(SchemaNodeId::ConnectionFolder { node_id: folder_id }) = parse_node_id(item_id)
        {
            self.app_state.update(cx, |state, cx| {
                state.delete_folder(folder_id);
                cx.emit(AppStateChanged);
            });

            self.refresh_tree(cx);
        }
    }

    pub(super) fn move_item_to_folder(
        &mut self,
        item_id: &str,
        target_folder_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) {
        let node_id = match parse_node_id(item_id) {
            Some(SchemaNodeId::Profile { profile_id }) => self
                .app_state
                .read(cx)
                .connection_tree()
                .find_by_profile(profile_id)
                .map(|n| n.id),
            Some(SchemaNodeId::ConnectionFolder { node_id }) => Some(node_id),
            _ => None,
        };

        if let Some(node_id) = node_id {
            self.app_state.update(cx, |state, cx| {
                if state.move_tree_node(node_id, target_folder_id) {
                    cx.emit(AppStateChanged);
                }
            });
            self.refresh_tree(cx);
        }
    }

    pub fn commit_rename(&mut self, cx: &mut Context<Self>) {
        if let Some(old_path) = self.editing_script_path.take() {
            let new_name = self.rename_input.read(cx).value().to_string();

            if new_name.trim().is_empty() {
                self.refresh_scripts_tree(cx);
                cx.emit(SidebarEvent::RequestFocus);
                return;
            }

            let result = self.app_state.update(cx, |state, _cx| {
                let dir = state.scripts_directory_mut()?;
                dir.rename(&old_path, new_name.trim()).ok()
            });

            if result.is_some() {
                self.app_state.update(cx, |state, _cx| {
                    state.refresh_scripts();
                });
                self.refresh_scripts_tree(cx);
            }

            cx.emit(SidebarEvent::RequestFocus);
            return;
        }

        let Some(id) = self.editing_id.take() else {
            return;
        };

        let new_name = self.rename_input.read(cx).value().to_string();

        if new_name.trim().is_empty() {
            self.refresh_tree(cx);
            return;
        }

        let is_folder = self.editing_is_folder;

        self.app_state.update(cx, |state, cx| {
            if is_folder {
                if state.rename_folder(id, &new_name) {
                    cx.emit(AppStateChanged);
                }
            } else if let Some(profile) = state.profiles_mut().iter_mut().find(|p| p.id == id) {
                profile.name = new_name;
                state.save_profiles();
                cx.emit(AppStateChanged);
            }
        });

        self.refresh_tree(cx);
        cx.emit(SidebarEvent::RequestFocus);
    }

    /// Cancels the rename operation.
    pub fn cancel_rename(&mut self, cx: &mut Context<Self>) {
        self.editing_id = None;
        self.editing_script_path = None;
        cx.emit(SidebarEvent::RequestFocus);
        cx.notify();
    }

    pub fn start_rename_selected(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(entry) = self.active_tree_state().read(cx).selected_entry().cloned() else {
            return;
        };

        let item_id = entry.item().id.to_string();
        let kind = parse_node_kind(&item_id);

        match kind {
            SchemaNodeKind::ConnectionFolder | SchemaNodeKind::Profile => {
                self.start_rename(&item_id, window, cx);
            }
            SchemaNodeKind::ScriptFile => {
                self.start_rename(&item_id, window, cx);
            }
            SchemaNodeKind::ScriptsFolder => {
                // Only allow renaming subfolders, not root
                if let Some(SchemaNodeId::ScriptsFolder { path: Some(_) }) = parse_node_id(&item_id)
                {
                    self.start_rename(&item_id, window, cx);
                }
            }
            _ => {}
        }
    }

    pub fn toggle_add_menu(&mut self, cx: &mut Context<Self>) {
        self.add_menu_open = !self.add_menu_open;
        cx.notify();
    }

    pub fn close_add_menu(&mut self, cx: &mut Context<Self>) {
        if self.add_menu_open {
            self.add_menu_open = false;
            cx.notify();
        }
    }

    #[allow(dead_code)]
    pub fn is_add_menu_open(&self) -> bool {
        self.add_menu_open
    }

    pub fn is_renaming(&self) -> bool {
        self.editing_id.is_some() || self.editing_script_path.is_some()
    }

    fn handle_lazy_database_click(
        &mut self,
        profile_id: Uuid,
        db_name: &str,
        cx: &mut Context<Self>,
    ) {
        let needs_fetch = self
            .app_state
            .read(cx)
            .needs_database_schema(profile_id, db_name);

        // UI state only; driver issues USE at query time via QueryRequest.database
        self.app_state.update(cx, |state, cx| {
            state.set_active_database(profile_id, Some(db_name.to_string()));
            cx.emit(AppStateChanged);
        });

        if !needs_fetch {
            self.refresh_tree(cx);
            return;
        }

        let params = match self.app_state.update(cx, |state, cx| {
            if state.is_operation_pending(profile_id, Some(db_name)) {
                return Err("Operation already pending".to_string());
            }

            let result = state.prepare_fetch_database_schema(profile_id, db_name);

            if result.is_ok() && !state.start_pending_operation(profile_id, Some(db_name)) {
                return Err("Operation started by another thread".to_string());
            }

            cx.notify();
            result
        }) {
            Ok(p) => p,
            Err(e) => {
                // Only show toast for unexpected errors, not for expected skips
                let is_expected = e.contains("already cached")
                    || e.contains("already pending")
                    || e.contains("another thread");

                if is_expected {
                    log::info!("Fetch database schema skipped: {}", e);
                } else {
                    log::error!("Failed to load database schema: {}", e);
                    self.pending_toast = Some(PendingToast {
                        message: format!("Failed to load schema: {}", e),
                        is_error: true,
                    });
                }

                self.refresh_tree(cx);
                return;
            }
        };

        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.app_state.update(cx, |state, _cx| {
                state.finish_pending_operation(profile_id, Some(db_name));
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
                state.start_task(TaskKind::LoadSchema, format!("Loading schema: {}", db_name));
            cx.emit(AppStateChanged);
            result
        });

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let db_name_owned = db_name.to_string();
        let sidebar = cx.entity().clone();
        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(error) = cx.update(|cx| {
                if cancel_token.is_cancelled() {
                    log::info!("Fetch database schema task was cancelled");
                    app_state.update(cx, |state, cx| {
                        state.finish_pending_operation(profile_id, Some(&db_name_owned));
                        cx.emit(AppStateChanged);
                    });
                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.refresh_tree(cx);
                    });
                    return;
                }

                let (toast, failed) = match &result {
                    Ok(_) => {
                        app_state.update(cx, |state, _| {
                            state.complete_task(task_id);
                        });
                        (None, false)
                    }
                    Err(e) => {
                        app_state.update(cx, |state, _| {
                            state.fail_task(task_id, e.clone());
                        });
                        (
                            Some(PendingToast {
                                message: format!("Failed to load schema: {}", e),
                                is_error: true,
                            }),
                            true,
                        )
                    }
                };

                app_state.update(cx, |state, cx| {
                    state.finish_pending_operation(profile_id, Some(&db_name_owned));

                    if let Ok(res) = result {
                        state.set_database_schema(res.profile_id, res.database, res.schema);
                    }

                    cx.emit(AppStateChanged);
                    cx.notify();
                });

                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_toast = toast;

                    // Collapse database on failure
                    if failed {
                        let db_item_id = SchemaNodeId::Database {
                            profile_id,
                            name: db_name_owned.clone(),
                        }
                        .to_string();
                        sidebar.expansion_overrides.remove(&db_item_id);
                    }

                    sidebar.refresh_tree(cx);
                });
            }) {
                log::warn!(
                    "Failed to apply schema fetch result to sidebar state: {:?}",
                    error
                );
            }
        })
        .detach();
    }

    fn handle_connection_per_database_click(
        &mut self,
        profile_id: Uuid,
        db_name: &str,
        cx: &mut Context<Self>,
    ) {
        let already_connected = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .is_some_and(|conn| {
                conn.database_connections.contains_key(db_name)
                    || conn
                        .schema
                        .as_ref()
                        .and_then(|schema| schema.current_database())
                        .is_some_and(|current| current == db_name)
            });

        if already_connected {
            self.app_state.update(cx, |state, cx| {
                if state.get_active_database(profile_id).as_deref() != Some(db_name) {
                    state.set_active_database(profile_id, Some(db_name.to_string()));
                    cx.emit(AppStateChanged);
                }
            });

            self.refresh_tree(cx);
            return;
        }

        let params = match self.app_state.update(cx, |state, cx| {
            if state.is_operation_pending(profile_id, Some(db_name)) {
                return Err("Operation already pending".to_string());
            }

            let result = state.prepare_database_connection(profile_id, db_name);

            if result.is_ok() && !state.start_pending_operation(profile_id, Some(db_name)) {
                return Err("Operation started by another thread".to_string());
            }

            cx.notify();
            result
        }) {
            Ok(p) => p,
            Err(e) => {
                log::info!("Database connection skipped: {}", e);
                return;
            }
        };

        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.app_state.update(cx, |state, _cx| {
                state.finish_pending_operation(profile_id, Some(db_name));
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
                TaskKind::SwitchDatabase,
                format!("Connecting to database: {}", db_name),
            );
            cx.emit(AppStateChanged);
            result
        });

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let db_name_owned = db_name.to_string();
        let sidebar = cx.entity().clone();
        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(error) = cx.update(|cx| {
                if cancel_token.is_cancelled() {
                    log::info!("Database connection task was cancelled, discarding result");
                    app_state.update(cx, |state, cx| {
                        state.finish_pending_operation(profile_id, Some(&db_name_owned));
                        cx.emit(AppStateChanged);
                    });
                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.refresh_tree(cx);
                    });
                    return;
                }

                let toast = match &result {
                    Ok(_) => {
                        app_state.update(cx, |state, _| {
                            state.complete_task(task_id);
                        });
                        None
                    }
                    Err(e) => {
                        app_state.update(cx, |state, _| {
                            state.fail_task(task_id, e.clone());
                        });
                        Some(PendingToast {
                            message: format!("Failed to connect to database: {}", e),
                            is_error: true,
                        })
                    }
                };

                app_state.update(cx, |state, cx| {
                    state.finish_pending_operation(profile_id, Some(&db_name_owned));

                    if let Ok(res) = result {
                        state.add_database_connection(
                            profile_id,
                            db_name_owned.clone(),
                            res.connection,
                            res.schema,
                        );
                        state.set_active_database(profile_id, Some(db_name_owned.clone()));
                    }

                    cx.emit(AppStateChanged);
                    cx.notify();
                });

                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_toast = toast;
                    sidebar.refresh_tree(cx);
                });
            }) {
                log::warn!(
                    "Failed to apply per-database connection result to sidebar state: {:?}",
                    error
                );
            }
        })
        .detach();
    }

    pub(crate) fn connect_to_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        let uses_pipeline = self
            .app_state
            .read(cx)
            .profiles()
            .iter()
            .find(|p| p.id == profile_id)
            .is_some_and(|p| p.uses_pipeline());

        if uses_pipeline {
            self.connect_via_pipeline(profile_id, cx);
            return;
        }

        let (params, profile_name, pre_connect_hooks, post_connect_hooks, hook_context) =
            match self.app_state.update(cx, |state, _cx| {
                if state.is_operation_pending(profile_id, None) {
                    return Err("Connection already pending".to_string());
                }

                let result = state.prepare_connect_profile(profile_id);

                if result.is_ok() && !state.start_pending_operation(profile_id, None) {
                    return Err("Operation started by another thread".to_string());
                }

                result.map(|p| {
                    let name = p.profile.name.clone();
                    let hook_execution = p.prepare_hooks(state.resolve_profile_hooks(&p.profile));

                    (
                        p,
                        name,
                        hook_execution.hooks.pre_connect,
                        hook_execution.hooks.post_connect,
                        hook_execution.context,
                    )
                })
            }) {
                Ok(p) => p,
                Err(e) => {
                    log::info!("Connect skipped: {}", e);
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
                .spawn(async move { params.execute(Some(crate::proxy::create_proxy_tunnel)) })
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
                    // Emit connection failure audit event.
                    let error_clone = error.clone();
                    let profile_name_for_audit = profile_name.clone();
                    let profile_id_for_audit = profile_id;

                    if let Err(update_error) = cx.update(|cx| {
                        app_state.update(cx, |state, cx| {
                            // Emit failure audit event.
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

                        sidebar.update(cx, |sidebar, cx| {
                            sidebar.pending_toast = Some(PendingToast {
                                message: error,
                                is_error: true,
                            });
                            sidebar.refresh_tree(cx);
                        });
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
                return Err("Connection already pending".to_string());
            }

            if !state.start_pending_operation(profile_id, None) {
                return Err("Operation started by another thread".to_string());
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
                    Err(error)
                }
            }
        }) {
            Ok(values) => values,
            Err(e) => {
                log::info!("Pipeline connect skipped: {}", e);
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
            cx.emit(crate::app::AppStateChanged);
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
                            cx.emit(crate::app::AppStateChanged);
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
                            cx.emit(crate::app::AppStateChanged);
                        });

                        if cancel_token.is_cancelled() {
                            app_state.update(cx, |state, cx| {
                                state.finish_pending_operation(profile_id, None);
                                cx.emit(crate::app::AppStateChanged);
                            });

                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.refresh_tree(cx);
                            });

                            return;
                        }

                        app_state.update(cx, |state, cx| {
                            state.fail_task(task_id, "Connection hook cancelled");
                            state.finish_pending_operation(profile_id, None);
                            cx.emit(crate::app::AppStateChanged);
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
                            cx.emit(crate::app::AppStateChanged);
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
                            cx.emit(crate::app::AppStateChanged);
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
                            cx.emit(crate::app::AppStateChanged);
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
                            cx.emit(crate::app::AppStateChanged);
                        });

                        if cancel_token.is_cancelled() {
                            app_state.update(cx, |state, cx| {
                                state.finish_pending_operation(profile_id, None);
                                cx.emit(crate::app::AppStateChanged);
                            });

                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.refresh_tree(cx);
                            });

                            return;
                        }

                        app_state.update(cx, |state, cx| {
                            state.fail_task(task_id, "Post-connect hook cancelled");
                            state.finish_pending_operation(profile_id, None);
                            cx.emit(crate::app::AppStateChanged);
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
                    state.apply_connect_profile(profile, connection.into(), schema, tunnel_handle);
                    cx.emit(crate::app::AppStateChanged);
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

    pub(crate) fn disconnect_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
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

    pub(super) fn refresh_connection(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            state.cancel_detached_hook_tasks(profile_id);
            state.disconnect(profile_id);
            log::info!("Refreshing connection for profile {}", profile_id);
            cx.notify();
        });
        self.refresh_tree(cx);
        self.connect_to_profile(profile_id, cx);
    }

    pub(super) fn delete_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            if let Some(idx) = state.profiles().iter().position(|p| p.id == profile_id)
                && let Some(removed) = state.remove_profile(idx)
            {
                log::info!("Deleted profile: {}", removed.name);
            }
            cx.emit(crate::app::AppStateChanged);
        });
    }

    pub(super) fn edit_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
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
            app_id: Some("dbflux".into()),
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

    fn selected_scripts_parent_dir(&self, cx: &App) -> Option<std::path::PathBuf> {
        let entry = self.scripts_tree_state.read(cx).selected_entry()?;
        let item_id = entry.item().id.to_string();
        let node_id = parse_node_id(&item_id)?;

        match node_id {
            SchemaNodeId::ScriptsFolder { path: Some(p) } => Some(std::path::PathBuf::from(p)),
            SchemaNodeId::ScriptFile { path } => std::path::Path::new(&path)
                .parent()
                .map(|p| p.to_path_buf()),
            _ => None,
        }
    }

    fn default_script_extension(&self, cx: &App) -> &'static str {
        let state = self.app_state.read(cx);
        state
            .active_connection()
            .map(|c| c.connection.metadata().query_language.default_extension())
            .unwrap_or("sql")
    }

    /// For folders returns the folder path; for files returns the parent directory.
    pub(super) fn parent_dir_from_item_id(item_id: &str) -> Option<std::path::PathBuf> {
        match parse_node_id(item_id) {
            Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                Some(std::path::PathBuf::from(p))
            }
            Some(SchemaNodeId::ScriptFile { path }) => std::path::Path::new(&path)
                .parent()
                .map(|p| p.to_path_buf()),
            _ => None,
        }
    }

    pub(super) fn create_script_file_in(
        &mut self,
        parent: Option<std::path::PathBuf>,
        cx: &mut Context<Self>,
    ) {
        let extension = self.default_script_extension(cx);
        let name = self.generate_unique_script_name(parent.as_deref(), extension, cx);

        let path = self.app_state.update(cx, |state, _cx| {
            let dir = state.scripts_directory_mut()?;
            dir.create_file(parent.as_deref(), &name, extension).ok()
        });

        if let Some(path) = path {
            self.app_state.update(cx, |state, _cx| {
                state.refresh_scripts();
            });
            self.refresh_scripts_tree(cx);

            cx.emit(SidebarEvent::OpenScript { path });
        }
    }

    pub(super) fn create_script_file(&mut self, cx: &mut Context<Self>) {
        let parent = self.selected_scripts_parent_dir(cx);
        self.create_script_file_in(parent, cx);
    }

    pub(super) fn create_script_folder_in(
        &mut self,
        parent: Option<std::path::PathBuf>,
        cx: &mut Context<Self>,
    ) {
        let name = "new_folder";

        let created_path = self.app_state.update(cx, |state, _cx| {
            let dir = state.scripts_directory_mut()?;
            dir.create_folder(parent.as_deref(), name).ok()
        });

        let Some(path) = created_path else {
            return;
        };

        self.app_state.update(cx, |state, _cx| {
            state.refresh_scripts();
        });
        self.refresh_scripts_tree(cx);

        let item_id = SchemaNodeId::ScriptsFolder {
            path: Some(path.to_string_lossy().to_string()),
        }
        .to_string();

        self.select_and_rename_item(&item_id, cx);
    }

    pub fn create_script_folder(&mut self, cx: &mut Context<Self>) {
        let parent = self.selected_scripts_parent_dir(cx);
        self.create_script_folder_in(parent, cx);
    }

    pub(super) fn import_script(&mut self, cx: &mut Context<Self>) {
        let parent = self.selected_scripts_parent_dir(cx);
        let extensions = dbflux_core::all_script_extensions();
        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();

        let task = cx.background_executor().spawn(async move {
            let mut dialog = rfd::FileDialog::new().set_title("Import Script");
            for ext in &extensions {
                dialog = dialog.add_filter("Script files", &[ext]);
            }
            dialog.pick_file()
        });

        cx.spawn(async move |_this, cx| {
            let source = match task.await {
                Some(path) => path,
                None => return,
            };

            if let Err(error) = cx.update(|cx| {
                let path = app_state.update(cx, |state, _cx| {
                    let dir = state.scripts_directory_mut()?;
                    let imported = dir.import(&source, parent.as_deref()).ok()?;
                    state.refresh_scripts();
                    Some(imported)
                });

                if let Some(path) = path {
                    sidebar.update(cx, |this, cx| {
                        this.refresh_scripts_tree(cx);
                        cx.emit(SidebarEvent::OpenScript { path });
                    });
                }
            }) {
                log::warn!(
                    "Failed to apply imported script state to sidebar: {:?}",
                    error
                );
            }
        })
        .detach();
    }

    pub(super) fn handle_script_drop_with_position(
        &mut self,
        state: &ScriptsDragState,
        cx: &mut Context<Self>,
    ) {
        let Some(drop_target) = self.scripts_drop_target.take() else {
            return;
        };

        let Some(target_dir) = self.resolve_script_drop_target_dir(&drop_target, cx) else {
            return;
        };

        self.move_scripts(&state.all_paths(), &target_dir, cx);
    }

    pub(super) fn handle_script_drop_to_root_with_position(
        &mut self,
        state: &ScriptsDragState,
        cx: &mut Context<Self>,
    ) {
        let root = match self.app_state.read(cx).scripts_directory() {
            Some(dir) => dir.root_path().to_path_buf(),
            None => return,
        };

        self.scripts_drop_target = None;
        self.move_scripts(&state.all_paths(), &root, cx);
    }

    pub(super) fn move_selected_scripts_to_selected_folder(
        &mut self,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.scripts_multi_selection.is_empty() {
            return false;
        }

        let selected_entry = self.scripts_tree_state.read(cx).selected_entry().cloned();
        let Some(selected_entry) = selected_entry else {
            return false;
        };

        if !selected_entry.is_expanded() {
            return false;
        }

        let selected_item_id = selected_entry.item().id.to_string();
        let target_dir = self.resolve_script_drop_target_dir(
            &DropTarget {
                item_id: selected_item_id.clone(),
                position: DropPosition::Into,
            },
            cx,
        );

        let Some(target_dir) = target_dir else {
            return false;
        };

        let sources: Vec<std::path::PathBuf> = self
            .scripts_multi_selection
            .iter()
            .filter(|item_id| item_id.as_str() != selected_item_id)
            .filter_map(|item_id| match parse_node_id(item_id) {
                Some(SchemaNodeId::ScriptFile { path }) => Some(std::path::PathBuf::from(path)),
                Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                    Some(std::path::PathBuf::from(p))
                }
                _ => None,
            })
            .collect();

        if sources.is_empty() {
            return false;
        }

        self.move_scripts(&sources, &target_dir, cx)
    }

    pub(super) fn move_selected_scripts_out_of_folder(&mut self, cx: &mut Context<Self>) -> bool {
        if self.scripts_multi_selection.is_empty() {
            return false;
        }

        let mut sources: Vec<std::path::PathBuf> = self
            .scripts_multi_selection
            .iter()
            .filter_map(|item_id| match parse_node_id(item_id) {
                Some(SchemaNodeId::ScriptFile { path }) => Some(std::path::PathBuf::from(path)),
                Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                    Some(std::path::PathBuf::from(p))
                }
                _ => None,
            })
            .collect();

        if sources.is_empty() {
            return false;
        }

        sources.sort();
        sources.dedup();

        let all_sources = sources.clone();
        sources.retain(|source| {
            !all_sources
                .iter()
                .any(|candidate| candidate != source && source.starts_with(candidate))
        });

        let mut parent_dirs: Vec<std::path::PathBuf> = sources
            .iter()
            .filter_map(|source| source.parent().map(std::path::Path::to_path_buf))
            .collect();

        parent_dirs.sort();
        parent_dirs.dedup();

        if parent_dirs.len() != 1 {
            return false;
        }

        let current_parent = match parent_dirs.pop() {
            Some(path) => path,
            None => return false,
        };

        let root = match self.app_state.read(cx).scripts_directory() {
            Some(dir) => dir.root_path().to_path_buf(),
            None => return false,
        };

        if current_parent == root {
            return false;
        }

        let target_dir = current_parent
            .parent()
            .map(std::path::Path::to_path_buf)
            .unwrap_or(root);

        self.move_scripts(&sources, &target_dir, cx)
    }

    fn resolve_script_drop_target_dir(
        &self,
        drop_target: &DropTarget,
        cx: &Context<Self>,
    ) -> Option<std::path::PathBuf> {
        let root = self
            .app_state
            .read(cx)
            .scripts_directory()
            .map(|dir| dir.root_path().to_path_buf());

        let target_path = match parse_node_id(&drop_target.item_id) {
            Some(SchemaNodeId::ScriptFile { path }) => Some(std::path::PathBuf::from(path)),
            Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                Some(std::path::PathBuf::from(p))
            }
            Some(SchemaNodeId::ScriptsFolder { path: None }) => root.clone(),
            _ => None,
        }?;

        match drop_target.position {
            DropPosition::Into => {
                if target_path.is_dir() {
                    Some(target_path)
                } else {
                    target_path.parent().map(std::path::Path::to_path_buf)
                }
            }
            DropPosition::Before | DropPosition::After => target_path
                .parent()
                .map(std::path::Path::to_path_buf)
                .or(root.clone()),
        }
    }

    fn move_scripts(
        &mut self,
        sources: &[std::path::PathBuf],
        target_dir: &std::path::Path,
        cx: &mut Context<Self>,
    ) -> bool {
        let mut normalized_sources = sources.to_vec();
        normalized_sources.sort();
        normalized_sources.dedup();

        let all_sources = normalized_sources.clone();
        normalized_sources.retain(|source| {
            !all_sources
                .iter()
                .any(|candidate| candidate != source && source.starts_with(candidate))
        });

        let mut moved_any = false;
        self.app_state.update(cx, |state, _cx| {
            let Some(dir) = state.scripts_directory_mut() else {
                return;
            };

            for source in &normalized_sources {
                if source == target_dir {
                    continue;
                }

                if source.parent() == Some(target_dir) {
                    continue;
                }

                if dir.move_entry(source, target_dir).is_ok() {
                    moved_any = true;
                }
            }
        });

        if moved_any {
            self.app_state.update(cx, |state, _cx| {
                state.refresh_scripts();
            });
            self.refresh_scripts_tree(cx);
        }

        moved_any
    }

    pub(super) fn delete_script(&mut self, path: &std::path::Path, cx: &mut Context<Self>) {
        let path = path.to_path_buf();
        let result = self.app_state.update(cx, |state, _cx| {
            state.scripts_directory_mut()?.delete(&path).ok()
        });

        if result.is_some() {
            self.app_state.update(cx, |state, _cx| {
                state.refresh_scripts();
            });
            self.refresh_scripts_tree(cx);
        }
    }

    fn resolve_script_path(item_id: &str) -> Option<std::path::PathBuf> {
        match parse_node_id(item_id) {
            Some(SchemaNodeId::ScriptFile { path }) => Some(std::path::PathBuf::from(path)),
            Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                Some(std::path::PathBuf::from(p))
            }
            Some(SchemaNodeId::ScriptsFolder { path: None }) => {
                dirs::data_dir().map(|d| d.join("dbflux").join("scripts"))
            }
            _ => None,
        }
    }

    pub(super) fn reveal_in_file_manager(&self, item_id: &str) {
        let Some(path) = Self::resolve_script_path(item_id) else {
            return;
        };

        #[cfg(target_os = "macos")]
        {
            if path.is_file() {
                if let Err(e) = std::process::Command::new("open")
                    .arg("-R")
                    .arg(&path)
                    .spawn()
                {
                    log::error!("Failed to reveal in file manager: {}", e);
                }
            } else if let Err(e) = std::process::Command::new("open").arg(&path).spawn() {
                log::error!("Failed to reveal in file manager: {}", e);
            }
        }

        #[cfg(target_os = "windows")]
        {
            if path.is_file() {
                let select_arg = format!("/select,{}", path.display());
                if let Err(e) = std::process::Command::new("explorer")
                    .arg(&select_arg)
                    .spawn()
                {
                    log::error!("Failed to reveal in file manager: {}", e);
                }
            } else if let Err(e) = std::process::Command::new("explorer").arg(&path).spawn() {
                log::error!("Failed to reveal in file manager: {}", e);
            }
        }

        #[cfg(target_os = "linux")]
        {
            let target = if path.is_file() {
                path.parent().unwrap_or(&path).to_path_buf()
            } else {
                path
            };

            if let Err(_e) = std::process::Command::new("xdg-open").arg(&target).spawn()
                && let Err(e) = std::process::Command::new("gio")
                    .arg("open")
                    .arg(&target)
                    .spawn()
            {
                log::error!("Failed to reveal in file manager: {}", e);
            }
        }
    }

    pub(super) fn copy_path_to_clipboard(&self, item_id: &str, cx: &mut Context<Self>) {
        let Some(path) = Self::resolve_script_path(item_id) else {
            return;
        };

        cx.write_to_clipboard(ClipboardItem::new_string(
            path.to_string_lossy().to_string(),
        ));
    }

    fn generate_unique_script_name(
        &self,
        parent: Option<&std::path::Path>,
        extension: &str,
        cx: &App,
    ) -> String {
        let state = self.app_state.read(cx);
        let dir = match state.scripts_directory() {
            Some(d) => d,
            None => return format!("untitled.{}", extension),
        };

        let base_dir = parent.unwrap_or_else(|| dir.root_path());

        for i in 1u32.. {
            let name = if i == 1 {
                format!("untitled.{}", extension)
            } else {
                format!("untitled_{}.{}", i, extension)
            };

            if !base_dir.join(&name).exists() {
                return name;
            }
        }

        format!("untitled.{}", extension)
    }
}
