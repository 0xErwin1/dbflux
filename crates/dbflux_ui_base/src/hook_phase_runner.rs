use crate::{AppStateChanged, AppStateEntity};
use dbflux_app::hook_executor::CompositeExecutor;
use dbflux_core::observability::actions::{HOOK_EXECUTE, HOOK_EXECUTE_FAILED};
use dbflux_core::{
    CancelToken, ConnectionHook, DetachedProcessHandle, HookContext, HookExecutor, HookKind,
    HookPhase, HookResult, OutputReceiver, ProcessExecutionError, TaskId, TaskKind,
    detached_process_channel, execute_streaming_process, output_channel,
};
use gpui::{AsyncApp, Entity};
use std::collections::BTreeSet;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};
use uuid::Uuid;

const DETACHED_HOOK_STARTED_MESSAGE: &str = "Detached process started in background";

type DetachedReadyReceiver = mpsc::Receiver<Result<(), String>>;

struct DetachedHookTaskStart {
    ready_receiver: Option<DetachedReadyReceiver>,
}

enum DetachedCleanupState {
    Complete,
    Cancel(Vec<TaskId>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetachedHookCleanupError {
    task_ids: Vec<TaskId>,
    source: String,
}

impl DetachedHookCleanupError {
    pub fn new(task_ids: Vec<TaskId>, source: impl Into<String>) -> Self {
        Self {
            task_ids,
            source: source.into(),
        }
    }

    pub fn task_ids(&self) -> &[TaskId] {
        &self.task_ids
    }

    pub fn source(&self) -> &str {
        &self.source
    }
}

#[derive(Clone, Default)]
pub struct DetachedHookScope {
    task_ids: Arc<Mutex<BTreeSet<TaskId>>>,
}

impl DetachedHookScope {
    fn register(&self, task_id: TaskId) {
        self.task_ids
            .lock()
            .expect("detached hook scope poisoned")
            .insert(task_id);
    }

    fn unregister(&self, task_id: TaskId) {
        self.task_ids
            .lock()
            .expect("detached hook scope poisoned")
            .remove(&task_id);
    }

    pub async fn cancel_and_wait(
        &self,
        app_state: Entity<AppStateEntity>,
        cx: &mut AsyncApp,
    ) -> Result<(), DetachedHookCleanupError> {
        loop {
            let DetachedCleanupState::Cancel(task_ids) = self.cleanup_state() else {
                return Ok(());
            };

            cx.update(|cx| {
                app_state.update(cx, |state, cx| {
                    for task_id in &task_ids {
                        state.cancel_task(*task_id);
                    }
                    cx.emit(AppStateChanged);
                });
            })
            .map_err(|error| {
                DetachedHookCleanupError::new(
                    task_ids,
                    format!("failed to update application state: {error:?}"),
                )
            })?;

            cx.background_executor()
                .timer(Duration::from_millis(50))
                .await;
        }
    }

    #[cfg_attr(test, allow(dead_code))]
    fn task_ids(&self) -> Vec<TaskId> {
        self.task_ids
            .lock()
            .expect("detached hook scope poisoned")
            .iter()
            .copied()
            .collect()
    }

    fn cleanup_state(&self) -> DetachedCleanupState {
        let task_ids = self.task_ids();

        if task_ids.is_empty() {
            DetachedCleanupState::Complete
        } else {
            DetachedCleanupState::Cancel(task_ids)
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum HookPhaseState {
    Continue { warnings: Vec<String> },
    Aborted { error: String },
    Cancelled,
}

#[derive(Default)]
struct HookPhasePolicy {
    warnings: Vec<String>,
}

impl HookPhasePolicy {
    fn extend_warnings(&mut self, warnings: impl IntoIterator<Item = String>) {
        self.warnings.extend(warnings);
    }

    fn record(
        &mut self,
        on_failure: dbflux_core::HookFailureMode,
        succeeded: bool,
        failure_message: &str,
    ) -> Option<HookPhaseState> {
        if succeeded {
            return None;
        }

        match on_failure {
            dbflux_core::HookFailureMode::Disconnect => Some(HookPhaseState::Aborted {
                error: failure_message.to_string(),
            }),
            dbflux_core::HookFailureMode::Warn => {
                self.warnings.push(failure_message.to_string());
                None
            }
            dbflux_core::HookFailureMode::Ignore => {
                log::warn!("{failure_message}");
                None
            }
        }
    }

    fn finish(self) -> HookPhaseState {
        HookPhaseState::Continue {
            warnings: self.warnings,
        }
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
        Err(error) => format!(
            "Phase: {}\n{}: {}\nSummary: {}\nError: {}",
            phase.label(),
            label,
            command_display,
            hook.summary(),
            error
        ),
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
    app_state: Entity<AppStateEntity>,
    task_id: TaskId,
    receiver: OutputReceiver,
    ready_signal: Option<String>,
    ready_sender: Option<mpsc::Sender<Result<(), String>>>,
    ready_seen: Arc<std::sync::atomic::AtomicBool>,
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
    ready_sender: Option<mpsc::Sender<Result<(), String>>>,
    ready_seen: Arc<std::sync::atomic::AtomicBool>,
    cx: &mut AsyncApp,
) {
    let mut ready_sender = ready_sender;
    let mut signal_buffer = String::new();

    loop {
        cx.background_executor()
            .timer(Duration::from_millis(100))
            .await;

        let mut chunk = String::new();
        let mut disconnected = false;

        loop {
            match receiver.try_recv() {
                Ok(event) => chunk.push_str(&event.text),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
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

#[allow(clippy::too_many_arguments)]
fn start_detached_hook_task(
    app_state: Entity<AppStateEntity>,
    profile_id: Uuid,
    profile_name: &str,
    phase: HookPhase,
    handle: DetachedProcessHandle,
    parent_cancel_token: Option<CancelToken>,
    scope: DetachedHookScope,
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
    scope.register(task_id);

    let (ready_sender, ready_receiver) = if ready_signal.is_some() {
        let (sender, receiver) = mpsc::channel();
        (Some(sender), Some(receiver))
    } else {
        (None, None)
    };

    let ready_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
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
                scope.unregister(task_id);
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
            Err(mpsc::TryRecvError::Disconnected) => {
                return Err(HookPhaseState::Aborted {
                    error: "Detached hook readiness watcher disconnected unexpectedly".to_string(),
                });
            }
            Err(mpsc::TryRecvError::Empty) => {
                cx.background_executor()
                    .timer(Duration::from_millis(50))
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
            Err(error) => return Err(HookPhaseState::Aborted { error }),
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
pub async fn run_hook_phase(
    app_state: Entity<AppStateEntity>,
    profile_id: Uuid,
    profile_name: String,
    phase: HookPhase,
    hooks: Vec<ConnectionHook>,
    context: HookContext,
    parent_cancel: Option<CancelToken>,
    scope: &DetachedHookScope,
    cx: &mut AsyncApp,
) -> HookPhaseState {
    run_hook_phase_with_executor(
        app_state,
        profile_id,
        profile_name,
        phase,
        hooks,
        context,
        parent_cancel,
        scope,
        CompositeExecutor::new(),
        cx,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_hook_phase_with_executor<E>(
    app_state: Entity<AppStateEntity>,
    profile_id: Uuid,
    profile_name: String,
    phase: HookPhase,
    hooks: Vec<ConnectionHook>,
    context: HookContext,
    parent_cancel: Option<CancelToken>,
    scope: &DetachedHookScope,
    executor: E,
    cx: &mut AsyncApp,
) -> HookPhaseState
where
    E: HookExecutor + Clone + 'static,
{
    let mut policy = HookPhasePolicy::default();

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
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
            cx,
        );

        let (detached_sender, detached_receiver) = detached_process_channel();
        let parent_cancel_for_hook = parent_cancel.clone();
        let hook_for_execution = hook.clone();
        let mut hook_context = context.clone();
        hook_context.phase = Some(phase);
        let hook_cancel_for_execution = hook_cancel_token.clone();
        let executor = executor.clone();

        let hook_start_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
        let hook_command_for_audit = hook.display_command();
        let phase_label = phase.label();
        if let Err(error) = cx.update(|cx| {
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
        }) {
            log::warn!("Failed to record hook start audit event: {:?}", error);
        }

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
            policy.extend_warnings(output.warnings.iter().cloned());
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
                scope.clone(),
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

        let (succeeded, failure_message, cancelled) = if detached_started {
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
                None => (true, None, false),
                Some(HookPhaseState::Cancelled) => (false, None, true),
                Some(HookPhaseState::Aborted { error }) => (false, Some(error), false),
                Some(HookPhaseState::Continue { warnings }) => {
                    let message = if warnings.is_empty() {
                        "Unexpected hook readiness state: continue without warning".to_string()
                    } else {
                        format!("Unexpected hook readiness state: {}", warnings.join("; "))
                    };
                    log::error!("[HOOK] {}", message);
                    (false, Some(message), false)
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
            (succeeded, failure_message, false)
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
        } else if let Some(ref message) = failure_message {
            format!(
                "Hook '{}' ({}) failed: {}",
                command_display,
                phase.label(),
                message
            )
        } else {
            format!("Hook '{}' ({}) completed", command_display, phase.label())
        };

        if let Err(error) = cx.update(|cx| {
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
            if let Some(message) = &failure_message {
                event.error_message = Some(message.clone());
            }
            if let Err(error) = audit_service.record(event) {
                log::warn!("Failed to record hook lifecycle audit event: {}", error);
            }
        }) {
            log::warn!("Failed to record hook completion audit event: {:?}", error);
        }

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
        if let Some(failure_message) = failure_message
            && let Some(state) = policy.record(hook.on_failure, succeeded, &failure_message)
        {
            return state;
        }
    }

    policy.finish()
}

#[cfg(test)]
mod tests {
    use super::{
        DetachedCleanupState, DetachedHookScope, HookPhasePolicy, HookPhaseState,
        run_hook_phase_with_executor,
    };
    use crate::AppStateEntity;
    use dbflux_core::{
        CancelToken, ConnectionHook, DetachedProcessSender, HookContext, HookExecutionMode,
        HookExecutor, HookFailureMode, HookKind, HookPhase, HookResult, OutputSender, TaskId,
        TaskKind, TaskStatus,
    };
    use dbflux_storage::bootstrap::StorageRuntime;
    use gpui::{AppContext, Entity, TestAppContext};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, mpsc};
    use uuid::Uuid;

    #[derive(Clone)]
    struct RecordingExecutor {
        invocations: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingExecutor {
        fn failing_then_recording(invocations: Arc<Mutex<Vec<String>>>) -> Self {
            Self { invocations }
        }
    }

    impl HookExecutor for RecordingExecutor {
        fn execute_hook(
            &self,
            hook: &ConnectionHook,
            _context: &HookContext,
            _cancel_token: &CancelToken,
            _parent_cancel_token: Option<&CancelToken>,
            _output: Option<&OutputSender>,
            _detached: Option<&DetachedProcessSender>,
        ) -> Result<HookResult, String> {
            let command = hook.display_command();
            self.invocations
                .lock()
                .expect("test executor poisoned")
                .push(command.clone());

            if command.starts_with("fail") {
                Ok(HookResult {
                    stdout: String::new(),
                    stderr: "failed".to_string(),
                    exit_code: Some(1),
                    timed_out: false,
                    detached: false,
                    warnings: Vec::new(),
                })
            } else {
                Ok(HookResult {
                    stdout: String::new(),
                    stderr: String::new(),
                    exit_code: Some(0),
                    timed_out: false,
                    detached: false,
                    warnings: Vec::new(),
                })
            }
        }
    }

    fn hook(command: &str, on_failure: HookFailureMode) -> ConnectionHook {
        ConnectionHook {
            enabled: true,
            kind: HookKind::Command {
                command: command.to_string(),
                args: Vec::new(),
            },
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            env_denylist: Vec::new(),
            timeout_ms: None,
            execution_mode: HookExecutionMode::Blocking,
            ready_signal: None,
            on_failure,
        }
    }

    fn hook_context(profile_id: Uuid) -> HookContext {
        HookContext {
            profile_id,
            profile_name: "test profile".to_string(),
            db_kind: "test".to_string(),
            host: None,
            port: None,
            database: None,
            phase: Some(HookPhase::PreConnect),
        }
    }

    fn test_app_state(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
        cx.update(|cx| {
            cx.new(|_| {
                AppStateEntity::new_with_storage_runtime(
                    StorageRuntime::in_memory().expect("test storage runtime"),
                )
                .expect("test app state")
            })
        })
    }

    #[gpui::test]
    fn run_hook_phase_continues_in_order_after_warn_and_ignore(cx: &mut TestAppContext) {
        let app_state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let executor = RecordingExecutor::failing_then_recording(invocations.clone());
        let scope = DetachedHookScope::default();
        let (sender, receiver) = mpsc::channel();

        cx.update(|cx| {
            cx.spawn(async move |cx| {
                let state = run_hook_phase_with_executor(
                    app_state,
                    profile_id,
                    "test profile".to_string(),
                    HookPhase::PreConnect,
                    vec![
                        hook("fail-warn", HookFailureMode::Warn),
                        hook("fail-ignore", HookFailureMode::Ignore),
                        hook("later-success", HookFailureMode::Disconnect),
                    ],
                    hook_context(profile_id),
                    None,
                    &scope,
                    executor,
                    cx,
                )
                .await;
                sender.send(state).expect("test result receiver");
            })
            .detach();
        });

        cx.run_until_parked();

        assert_eq!(
            receiver.try_recv().expect("runner must finish"),
            HookPhaseState::Continue {
                warnings: vec![
                    "Pre-connect hook failed (exit code Some(1)): fail-warn (failed)".to_string()
                ],
            }
        );
        assert_eq!(
            *invocations.lock().expect("test executor poisoned"),
            vec!["fail-warn", "fail-ignore", "later-success"],
        );
    }

    #[gpui::test]
    fn run_hook_phase_disconnect_aborts_before_later_hook(cx: &mut TestAppContext) {
        let app_state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let executor = RecordingExecutor::failing_then_recording(invocations.clone());
        let scope = DetachedHookScope::default();
        let (sender, receiver) = mpsc::channel();

        cx.update(|cx| {
            cx.spawn(async move |cx| {
                let state = run_hook_phase_with_executor(
                    app_state,
                    profile_id,
                    "test profile".to_string(),
                    HookPhase::PreConnect,
                    vec![
                        hook("fail-disconnect", HookFailureMode::Disconnect),
                        hook("must-not-run", HookFailureMode::Warn),
                    ],
                    hook_context(profile_id),
                    None,
                    &scope,
                    executor,
                    cx,
                )
                .await;
                sender.send(state).expect("test result receiver");
            })
            .detach();
        });

        cx.run_until_parked();

        assert_eq!(
            receiver.try_recv().expect("runner must finish"),
            HookPhaseState::Aborted {
                error: "Pre-connect hook failed (exit code Some(1)): fail-disconnect (failed)"
                    .to_string(),
            }
        );
        assert_eq!(
            *invocations.lock().expect("test executor poisoned"),
            vec!["fail-disconnect"],
        );
    }

    #[gpui::test]
    fn detached_scope_cancel_and_wait_cancels_only_scoped_task_and_waits_for_unregistration(
        cx: &mut TestAppContext,
    ) {
        use std::time::Duration;

        let app_state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let scope = DetachedHookScope::default();
        let (scoped_task, scoped_token, unrelated_task, unrelated_token) = cx.update(|cx| {
            app_state.update(cx, |state, _| {
                let (scoped_task, scoped_token) = state.start_task_for_profile(
                    TaskKind::Hook {
                        phase: HookPhase::PreConnect,
                    },
                    "scoped detached hook",
                    Some(profile_id),
                );
                let (unrelated_task, unrelated_token) = state.start_task_for_profile(
                    TaskKind::Hook {
                        phase: HookPhase::PreConnect,
                    },
                    "unrelated detached hook",
                    Some(profile_id),
                );
                (scoped_task, scoped_token, unrelated_task, unrelated_token)
            })
        });
        scope.register(scoped_task);

        let (unregistered_sender, unregistered_receiver) = mpsc::channel();
        let task_scope = scope.clone();
        let task_token = scoped_token.clone();

        cx.update(|cx| {
            cx.spawn(async move |cx| {
                while !task_token.is_cancelled() {
                    cx.background_executor()
                        .timer(Duration::from_millis(10))
                        .await;
                }

                task_scope.unregister(scoped_task);
                unregistered_sender
                    .send(())
                    .expect("test unregistration receiver");
            })
            .detach();
        });

        let (finished_sender, finished_receiver) = mpsc::channel();
        let cleanup_scope = scope.clone();
        let cleanup_state = app_state.clone();

        cx.update(|cx| {
            cx.spawn(async move |cx| {
                cleanup_scope
                    .cancel_and_wait(cleanup_state, cx)
                    .await
                    .expect("scoped cleanup must complete");
                finished_sender.send(()).expect("test completion receiver");
            })
            .detach();
        });

        cx.run_until_parked();

        assert!(
            scoped_token.is_cancelled(),
            "cancel_and_wait must cancel the scoped task before waiting"
        );
        assert!(
            !unrelated_token.is_cancelled(),
            "cancel_and_wait must not cancel an unrelated task"
        );
        assert!(
            finished_receiver.try_recv().is_err(),
            "cleanup must wait until the scoped task unregisters"
        );
        cx.executor().advance_clock(Duration::from_millis(10));
        cx.run_until_parked();

        unregistered_receiver
            .try_recv()
            .expect("the cancelled task must unregister itself");
        assert!(
            finished_receiver.try_recv().is_err(),
            "cleanup must remain waiting until its next cancellation poll observes unregistration"
        );

        cx.executor().advance_clock(Duration::from_millis(50));
        cx.run_until_parked();

        finished_receiver
            .try_recv()
            .expect("cleanup must finish after the scoped task unregisters");
        let (scoped_status, unrelated_status) = cx
            .update(|cx| {
                app_state
                    .read(cx)
                    .tasks()
                    .get(scoped_task)
                    .map(|task| task.status)
                    .zip(
                        app_state
                            .read(cx)
                            .tasks()
                            .get(unrelated_task)
                            .map(|task| task.status),
                    )
            })
            .expect("both tasks must remain observable");
        assert_eq!(scoped_status, TaskStatus::Cancelled);
        assert_eq!(unrelated_status, TaskStatus::Running);
    }

    #[test]
    fn phase_policy_keeps_invoking_after_warn_and_ignore_failures_in_order() {
        let mut policy = HookPhasePolicy::default();

        assert_eq!(
            policy.record(HookFailureMode::Warn, false, "first warning"),
            None
        );
        assert_eq!(
            policy.record(HookFailureMode::Ignore, false, "ignored"),
            None
        );
        assert_eq!(
            policy.record(HookFailureMode::Disconnect, true, "unused"),
            None
        );

        assert_eq!(
            policy.finish(),
            HookPhaseState::Continue {
                warnings: vec!["first warning".to_string()],
            }
        );
    }

    #[test]
    fn phase_policy_stops_at_a_disconnect_failure() {
        let mut policy = HookPhasePolicy::default();

        assert_eq!(policy.record(HookFailureMode::Warn, false, "warning"), None);
        assert_eq!(
            policy.record(HookFailureMode::Disconnect, false, "disconnect failed"),
            Some(HookPhaseState::Aborted {
                error: "disconnect failed".to_string(),
            })
        );
    }

    #[test]
    fn detached_scope_tracks_only_its_own_tasks() {
        let first_task = TaskId::from(Uuid::new_v4());
        let second_task = TaskId::from(Uuid::new_v4());
        let unrelated_task = TaskId::from(Uuid::new_v4());
        let scope = DetachedHookScope::default();

        scope.register(first_task);
        scope.register(second_task);
        scope.unregister(first_task);

        assert_eq!(scope.task_ids(), vec![second_task]);
        assert!(!scope.task_ids().contains(&unrelated_task));
    }

    #[test]
    fn detached_scopes_do_not_share_registered_tasks() {
        let first_task = TaskId::from(Uuid::new_v4());
        let second_task = TaskId::from(Uuid::new_v4());
        let first_scope = DetachedHookScope::default();
        let second_scope = DetachedHookScope::default();

        first_scope.register(first_task);
        second_scope.register(second_task);

        assert_eq!(first_scope.task_ids(), vec![first_task]);
        assert_eq!(second_scope.task_ids(), vec![second_task]);
    }

    #[test]
    fn detached_scope_removes_completed_tasks_before_scoped_cleanup() {
        let completed_task = TaskId::from(Uuid::new_v4());
        let active_task = TaskId::from(Uuid::new_v4());
        let scope = DetachedHookScope::default();

        scope.register(completed_task);
        scope.register(active_task);
        scope.unregister(completed_task);

        assert_eq!(scope.task_ids(), vec![active_task]);
    }

    #[test]
    fn scoped_cleanup_cancels_only_active_tasks_then_waits_for_unregistration() {
        let completed_task = TaskId::from(Uuid::new_v4());
        let active_task = TaskId::from(Uuid::new_v4());
        let unrelated_task = TaskId::from(Uuid::new_v4());
        let scope = DetachedHookScope::default();

        scope.register(completed_task);
        scope.register(active_task);
        scope.unregister(completed_task);

        let DetachedCleanupState::Cancel(task_ids) = scope.cleanup_state() else {
            panic!("an active scoped task must be cancelled before cleanup completes");
        };
        assert_eq!(task_ids, vec![active_task]);
        assert!(!task_ids.contains(&unrelated_task));

        scope.unregister(active_task);

        assert!(matches!(
            scope.cleanup_state(),
            DetachedCleanupState::Complete
        ));
    }
}
