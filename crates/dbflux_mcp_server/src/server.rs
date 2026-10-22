use rmcp::{
    ServerHandler, handler::server::router::tool::ToolRouter, model::*, tool_handler, tool_router,
};
use std::collections::{HashMap, HashSet};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};

use dbflux_core::access::{AccessHandle, AccessKind, AccessManager};
use dbflux_core::auth::SharedDynAuthProvider;
use dbflux_core::secrecy::SecretString;
use dbflux_core::values::{CompositeValueResolver, ValueCache, ValueRef};
use dbflux_core::{
    CancelToken, Connection, ConnectionOverrides, DetachedProcessHandle, DetachedProcessSender,
    HookContext, HookExecution, HookExecutor, HookKind, HookPhase, HookPhaseOutcome, HookResult,
    HookRunner, LogErr, PipelineInput, ProcessExecutor,
};
use std::time::{Duration, Instant};

use crate::{
    connection_cache::CachedConnection,
    error_messages,
    governance::GovernanceMiddleware,
    state::{ServerState, SharedDetachedProcess},
};

const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

async fn profile_connection_setup_lock(
    state: &ServerState,
    profile_id: &str,
) -> Arc<tokio::sync::Mutex<()>> {
    let mut locks = state.connection_setup_lock.lock().await;
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(profile_id).and_then(std::sync::Weak::upgrade) {
        return lock;
    }

    let lock = Arc::new(tokio::sync::Mutex::new(()));
    locks.insert(profile_id.to_string(), Arc::downgrade(&lock));
    lock
}

async fn lock_connection_setup(
    state: &ServerState,
    profile_id: &str,
) -> tokio::sync::OwnedMutexGuard<()> {
    profile_connection_setup_lock(state, profile_id)
        .await
        .lock_owned()
        .await
}

fn profile_for_connection_key(
    profile: &dbflux_core::ConnectionProfile,
    profile_id: &str,
    connection_key: &str,
) -> Result<dbflux_core::ConnectionProfile, String> {
    let mut profile = profile.clone();
    if let Some(database) = connection_key.strip_prefix(&format!("{profile_id}:")) {
        profile.config = profile.config.clone().with_database(database)?;
    } else if connection_key != profile_id {
        return Err(format!(
            "Unexpected connection cache key '{connection_key}'"
        ));
    }
    Ok(profile)
}

/// Resolved secrets from a profile
struct ResolvedSecrets {
    password: Option<SecretString>,
    ssh_secret: Option<SecretString>,
}

struct McpHookExecutor {
    detached: DetachedProcessSender,
}

impl HookExecutor for McpHookExecutor {
    fn execute_hook(
        &self,
        hook: &dbflux_core::ConnectionHook,
        context: &HookContext,
        cancel_token: &CancelToken,
        parent_cancel_token: Option<&CancelToken>,
        _output: Option<&dbflux_core::OutputSender>,
        _detached: Option<&DetachedProcessSender>,
    ) -> Result<HookResult, String> {
        let mut result = ProcessExecutor.execute_hook(
            hook,
            context,
            cancel_token,
            parent_cancel_token,
            None,
            Some(&self.detached),
        )?;

        if result.detached {
            result.detached = false;
            result.exit_code = Some(0);
            result.warnings.clear();
        }

        Ok(result)
    }
}

struct DetachedHookProcess {
    shared: Arc<SharedDetachedProcess>,
    ready_receiver: Option<mpsc::Receiver<Result<(), String>>>,
}

fn lock_detached_process(
    shared: &Arc<SharedDetachedProcess>,
) -> std::sync::MutexGuard<'_, DetachedProcessHandle> {
    match shared.handle.lock() {
        Ok(handle) => handle,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn terminate_shared_detached_process(shared: &Arc<SharedDetachedProcess>) -> Result<(), String> {
    if shared.terminated.load(Ordering::SeqCst) {
        return Ok(());
    }

    let mut handle = lock_detached_process(shared);
    if shared.terminated.load(Ordering::SeqCst) {
        return Ok(());
    }
    if shared.finished.load(Ordering::SeqCst) {
        return Ok(());
    }

    handle.terminate()?;
    shared.terminated.store(true, Ordering::SeqCst);
    shared.finished.store(true, Ordering::SeqCst);
    Ok(())
}

fn report_detached_hook_readiness(
    sender: Option<&mpsc::Sender<Result<(), String>>>,
    result: Result<(), String>,
) {
    if let Some(sender) = sender
        && let Err(error) = sender.send(result)
    {
        log::debug!("Detached MCP hook readiness receiver dropped: {error}");
    }
}

fn spawn_detached_output_reader<R>(
    mut reader: R,
    stream: &'static str,
    ready_signal: Option<String>,
    ready_sender: Option<mpsc::Sender<Result<(), String>>>,
    ready_reported: Arc<AtomicBool>,
) -> std::io::Result<std::thread::JoinHandle<std::io::Result<u64>>>
where
    R: std::io::Read + Send + 'static,
{
    std::thread::Builder::new()
        .name(format!("dbflux-mcp-hook-{stream}"))
        .spawn(move || {
            let mut bytes_read = 0;
            let mut buffer = [0_u8; 4096];
            let mut signal_buffer = Vec::new();

            loop {
                let count = reader.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                bytes_read += count as u64;

                if let Some(signal) = ready_signal.as_deref().filter(|signal| !signal.is_empty()) {
                    let chunk = buffer.get(..count).ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Hook output reader returned an invalid byte count",
                        )
                    })?;
                    signal_buffer.extend_from_slice(chunk);
                    let max_buffer_len = signal.len().saturating_add(1024);
                    if signal_buffer.len() > max_buffer_len {
                        let keep_from = signal_buffer.len() - max_buffer_len;
                        signal_buffer.drain(..keep_from);
                    }
                    if signal_buffer
                        .windows(signal.len())
                        .any(|window| window == signal.as_bytes())
                        && !ready_reported.swap(true, Ordering::SeqCst)
                    {
                        report_detached_hook_readiness(ready_sender.as_ref(), Ok(()));
                    }
                }
            }

            Ok(bytes_read)
        })
}

fn join_detached_output_reader(
    reader: std::thread::JoinHandle<std::io::Result<u64>>,
    stream: &str,
) -> Result<(), String> {
    reader
        .join()
        .map_err(|_| format!("MCP detached hook {stream} reader panicked"))?
        .map(|_| ())
        .map_err(|error| format!("Failed to read MCP detached hook {stream}: {error}"))
}

fn start_detached_hook_process(
    handle: DetachedProcessHandle,
) -> Result<DetachedHookProcess, String> {
    let ready_signal = handle.ready_signal.clone();
    let (ready_sender, ready_receiver) = ready_signal.as_ref().map_or((None, None), |_| {
        let (sender, receiver) = mpsc::channel();
        (Some(sender), Some(receiver))
    });
    let ready_reported = Arc::new(AtomicBool::new(false));
    let shared = Arc::new(SharedDetachedProcess::new(handle));
    let worker_shared = Arc::clone(&shared);
    let worker_ready_reported = Arc::clone(&ready_reported);
    let worker_ready_signal = ready_signal.clone();
    let worker = std::thread::Builder::new()
        .name("dbflux-mcp-hook-process".to_string())
        .spawn(move || {
            if let Err(error) = supervise_detached_hook_process(
                worker_shared.clone(),
                ready_sender,
                worker_ready_signal,
                worker_ready_reported,
            ) {
                log::error!("Unable to supervise detached MCP hook process: {error}");
            }
        });

    if let Err(error) = worker {
        let cleanup = terminate_shared_detached_process(&shared);
        return Err(match cleanup {
            Ok(()) => format!("Unable to start detached MCP hook process supervisor: {error}"),
            Err(cleanup_error) => format!(
                "Unable to start detached MCP hook process supervisor: {error}; {cleanup_error}"
            ),
        });
    }

    Ok(DetachedHookProcess {
        shared,
        ready_receiver,
    })
}

fn supervise_detached_hook_process(
    shared: Arc<SharedDetachedProcess>,
    ready_sender: Option<mpsc::Sender<Result<(), String>>>,
    ready_signal: Option<String>,
    ready_reported: Arc<AtomicBool>,
) -> Result<(), String> {
    let (stdout, stderr, timeout, description) = {
        let mut handle = lock_detached_process(&shared);
        (
            handle.child.stdout.take(),
            handle.child.stderr.take(),
            handle.timeout,
            handle.description.clone(),
        )
    };

    let stdout_reader = match stdout {
        Some(stdout) => match spawn_detached_output_reader(
            stdout,
            "stdout",
            ready_signal.clone(),
            ready_sender.clone(),
            Arc::clone(&ready_reported),
        ) {
            Ok(reader) => Some(reader),
            Err(error) => {
                let cleanup = terminate_shared_detached_process(&shared);
                return Err(match cleanup {
                    Ok(()) => format!("Unable to start detached hook stdout reader: {error}"),
                    Err(cleanup_error) => format!(
                        "Unable to start detached hook stdout reader: {error}; {cleanup_error}"
                    ),
                });
            }
        },
        None => None,
    };
    let stderr_reader = match stderr {
        Some(stderr) => match spawn_detached_output_reader(
            stderr,
            "stderr",
            ready_signal.clone(),
            ready_sender.clone(),
            Arc::clone(&ready_reported),
        ) {
            Ok(reader) => Some(reader),
            Err(error) => {
                let cleanup = terminate_shared_detached_process(&shared);
                if let Some(reader) = stdout_reader {
                    join_detached_output_reader(reader, "stdout")?;
                }
                return Err(match cleanup {
                    Ok(()) => format!("Unable to start detached hook stderr reader: {error}"),
                    Err(cleanup_error) => format!(
                        "Unable to start detached hook stderr reader: {error}; {cleanup_error}"
                    ),
                });
            }
        },
        None => None,
    };
    let started = Instant::now();
    let mut errors = Vec::new();

    loop {
        if shared.finished.load(Ordering::SeqCst) {
            break;
        }

        let wait_result = {
            let mut handle = lock_detached_process(&shared);
            let wait_result = handle.child.try_wait();
            if matches!(&wait_result, Ok(Some(_))) {
                shared.finished.store(true, Ordering::SeqCst);
            }
            wait_result
        };
        match wait_result {
            Ok(Some(status)) => {
                if !status.success() {
                    errors.push(format!(
                        "Detached MCP hook '{}' exited with status {status}",
                        description
                    ));
                }
                if !ready_reported.load(Ordering::SeqCst)
                    && let Some(signal) = ready_signal.as_deref()
                    && !ready_reported.swap(true, Ordering::SeqCst)
                {
                    report_detached_hook_readiness(
                        ready_sender.as_ref(),
                        Err(format!(
                            "Detached hook process exited before ready signal '{signal}'"
                        )),
                    );
                }
                break;
            }
            Ok(None) => {
                if timeout.is_some_and(|timeout| started.elapsed() >= timeout) {
                    if let Err(error) = terminate_shared_detached_process(&shared) {
                        errors.push(error);
                    }
                    if !ready_reported.load(Ordering::SeqCst)
                        && let Some(signal) = ready_signal.as_deref()
                        && !ready_reported.swap(true, Ordering::SeqCst)
                    {
                        report_detached_hook_readiness(
                            ready_sender.as_ref(),
                            Err(format!(
                                "Detached hook process timed out before ready signal '{signal}'"
                            )),
                        );
                    }
                    log::warn!(
                        "Detached MCP hook '{}' exceeded its timeout and was stopped",
                        description
                    );
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(error) => {
                errors.push(format!("Failed to wait for detached MCP hook: {error}"));
                if let Err(cleanup_error) = terminate_shared_detached_process(&shared) {
                    errors.push(cleanup_error);
                }
                if !ready_reported.load(Ordering::SeqCst)
                    && let Some(signal) = ready_signal.as_deref()
                    && !ready_reported.swap(true, Ordering::SeqCst)
                {
                    report_detached_hook_readiness(
                        ready_sender.as_ref(),
                        Err(format!(
                            "Failed to wait for detached hook before ready signal '{signal}': {error}"
                        )),
                    );
                }
                break;
            }
        }
    }

    if let Some(reader) = stdout_reader
        && let Err(error) = join_detached_output_reader(reader, "stdout")
    {
        errors.push(error);
    }
    if let Some(reader) = stderr_reader
        && let Err(error) = join_detached_output_reader(reader, "stderr")
    {
        errors.push(error);
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn terminate_detached_processes(state: &ServerState, connection_id: &str) -> Vec<String> {
    let mut failed_processes = Vec::new();
    let mut errors = Vec::new();
    for shared in state.detached_processes.take(connection_id) {
        if let Err(error) = terminate_shared_detached_process(&shared) {
            errors.push(format!("Failed to terminate detached MCP hook: {error}"));
            failed_processes.push(shared);
        }
    }
    state
        .detached_processes
        .register(connection_id, failed_processes);
    errors
}

fn terminate_detached_process_variants(state: &ServerState, profile_id: &str) -> Vec<String> {
    let mut errors = Vec::new();
    for (connection_id, processes) in state.detached_processes.take_variants(profile_id) {
        let mut failed_processes = Vec::new();
        for shared in processes {
            if let Err(error) = terminate_shared_detached_process(&shared) {
                errors.push(format!(
                    "Failed to terminate detached MCP hook for '{connection_id}': {error}"
                ));
                failed_processes.push(shared);
            }
        }
        state
            .detached_processes
            .register(&connection_id, failed_processes);
    }
    errors
}

async fn wait_for_detached_hook_ready(
    receiver: mpsc::Receiver<Result<(), String>>,
    process: Arc<SharedDetachedProcess>,
) -> Result<(), String> {
    let timeout = {
        let handle = lock_detached_process(&process);
        handle.timeout.unwrap_or(Duration::from_secs(15))
    };
    let result = tokio::task::spawn_blocking(move || receiver.recv_timeout(timeout))
        .await
        .map_err(|error| format!("Detached hook readiness task failed: {error}"))?;

    match result {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            let cleanup = terminate_shared_detached_process(&process);
            match cleanup {
                Ok(()) => Err(format!(
                    "Detached hook did not emit its ready signal within {}ms",
                    timeout.as_millis()
                )),
                Err(error) => Err(format!(
                    "Detached hook did not emit its ready signal within {}ms; {error}",
                    timeout.as_millis()
                )),
            }
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err("Detached hook readiness watcher disconnected unexpectedly".to_string())
        }
    }
}

fn probe_tcp_endpoint(host: &str, port: u16) -> Result<bool, String> {
    let probe_host = if host.eq_ignore_ascii_case("localhost") {
        "127.0.0.1"
    } else {
        host
    };
    let addresses: Vec<_> = (probe_host, port)
        .to_socket_addrs()
        .map_err(|error| format!("Failed to resolve {probe_host}:{port}: {error}"))?
        .collect();
    if addresses.is_empty() {
        return Err(format!("No addresses resolved for {probe_host}:{port}"));
    }
    Ok(addresses
        .into_iter()
        .any(|address| TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok()))
}

async fn wait_for_hook_endpoint_ready(host: String, port: u16) -> Result<(), String> {
    let start = Instant::now();
    let timeout = Duration::from_secs(15);
    loop {
        let host_for_probe = host.clone();
        if tokio::task::spawn_blocking(move || probe_tcp_endpoint(&host_for_probe, port))
            .await
            .map_err(|error| format!("Detached hook endpoint probe failed: {error}"))??
        {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(format!(
                "Detached pre-connect hook reported ready, but {host}:{port} never accepted connections"
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn fail_detached_pre_connect_hook(
    state: &ServerState,
    profile: &dbflux_core::ConnectionProfile,
    connection_id: &str,
    hook_index: usize,
    error: String,
) -> Result<(), String> {
    let cleanup_errors = terminate_detached_processes(state, connection_id);
    record_hook_execution_failure(state, profile, HookPhase::PreConnect, hook_index).await;
    let mut message = format!(
        "{} hook #{} failed: {error}",
        HookPhase::PreConnect.label(),
        hook_index + 1
    );
    if !cleanup_errors.is_empty() {
        message.push_str(&format!("; {}", cleanup_errors.join("; ")));
    }
    Err(message)
}

async fn wait_for_pre_connect_detached_hooks(
    state: &ServerState,
    profile: &dbflux_core::ConnectionProfile,
    connection_id: &str,
    processes: &mut [DetachedHookProcess],
    hook_indices: &[usize],
    host: Option<String>,
    port: Option<u16>,
) -> Result<(), String> {
    for (position, process) in processes.iter_mut().enumerate() {
        let hook_index = hook_indices.get(position).copied().unwrap_or(position);
        if let Some(receiver) = process.ready_receiver.take()
            && let Err(error) =
                wait_for_detached_hook_ready(receiver, Arc::clone(&process.shared)).await
        {
            return fail_detached_pre_connect_hook(
                state,
                profile,
                connection_id,
                hook_index,
                error,
            )
            .await;
        }

        if let (Some(host), Some(port)) = (host.clone(), port)
            && let Err(error) = wait_for_hook_endpoint_ready(host, port).await
        {
            return fail_detached_pre_connect_hook(
                state,
                profile,
                connection_id,
                hook_index,
                error,
            )
            .await;
        }
    }
    Ok(())
}

async fn validate_connection_hook_phase(
    state: &ServerState,
    profile: &dbflux_core::ConnectionProfile,
    phase: HookPhase,
) -> Result<Vec<dbflux_core::ConnectionHook>, String> {
    let configured_hooks = profile
        .hooks
        .as_ref()
        .map(|hooks| hooks.phase_hooks(phase))
        .unwrap_or_default();
    if configured_hooks.is_empty() {
        return Ok(Vec::new());
    }

    if let Some((index, _)) = configured_hooks
        .iter()
        .enumerate()
        .find(|(_, hook)| hook.enabled && matches!(&hook.kind, HookKind::Lua { .. }))
    {
        record_hook_execution_failure(state, profile, phase, index).await;
        return Err(format!(
            "{} hook #{} cannot run in the MCP server because Lua hooks require the 'lua' feature",
            phase.label(),
            index + 1
        ));
    }

    if phase == HookPhase::PreConnect
        && let Some((index, _)) = configured_hooks.iter().enumerate().find(|(_, hook)| {
            hook.enabled
                && hook.is_detached()
                && hook.ready_signal.as_deref().is_none_or(str::is_empty)
        })
    {
        record_hook_execution_failure(state, profile, phase, index).await;
        return Err(format!(
            "{} hook #{} must set a ready signal before DBFlux can continue connecting",
            phase.label(),
            index + 1
        ));
    }

    Ok(configured_hooks.to_vec())
}

fn configured_hook_indices(hooks: &[dbflux_core::ConnectionHook]) -> Vec<usize> {
    hooks
        .iter()
        .enumerate()
        .filter(|(_, hook)| hook.enabled)
        .map(|(index, _)| index)
        .collect()
}

fn detached_hook_indices(hooks: &[dbflux_core::ConnectionHook]) -> Vec<usize> {
    hooks
        .iter()
        .enumerate()
        .filter(|(_, hook)| hook.enabled && hook.is_detached())
        .map(|(index, _)| index)
        .collect()
}

struct HookPhaseExecutionMetadata {
    phase: HookPhase,
    configured_indices: Vec<usize>,
    detached_indices: Vec<usize>,
    endpoint: (Option<String>, Option<u16>),
}

fn pre_connect_probe_endpoint(
    profile: &dbflux_core::ConnectionProfile,
    context: &HookContext,
) -> (Option<String>, Option<u16>) {
    if profile.uses_pipeline() || profile.config.has_ssh_tunnel() {
        (None, None)
    } else {
        (context.host.clone(), context.port)
    }
}

async fn execute_connection_hook_phase(
    state: &ServerState,
    connection_id: &str,
    hooks: Vec<dbflux_core::ConnectionHook>,
    context: HookContext,
    phase: HookPhase,
) -> Result<(HookPhaseOutcome, Vec<DetachedProcessHandle>), String> {
    let cancel_token = CancelToken::new();
    let shutdown_cancel_token = state.detached_processes.shutdown_cancel_token();
    let (detached, receiver) = dbflux_core::detached_process_channel();
    let executor = McpHookExecutor { detached };
    let outcome = tokio::task::spawn_blocking(move || {
        HookRunner::run_phase(
            phase,
            &hooks,
            &context,
            &cancel_token,
            Some(&shutdown_cancel_token),
            &executor,
        )
    })
    .await;

    let detached_handles: Vec<_> = receiver.try_iter().collect();
    match outcome {
        Ok(outcome) => Ok((outcome, detached_handles)),
        Err(error) => {
            let mut cleanup_errors = Vec::new();
            let mut failed_processes = Vec::new();
            for handle in detached_handles {
                let shared = Arc::new(SharedDetachedProcess::new(handle));
                if let Err(cleanup_error) = terminate_shared_detached_process(&shared) {
                    cleanup_errors.push(cleanup_error);
                    failed_processes.push(shared);
                }
            }
            state
                .detached_processes
                .register(connection_id, failed_processes);
            let mut message = format!("MCP hook execution task failed: {error}");
            if !cleanup_errors.is_empty() {
                message.push_str(&format!("; {}", cleanup_errors.join("; ")));
            }
            Err(message)
        }
    }
}

async fn start_detached_hook_processes(
    state: &ServerState,
    profile: &dbflux_core::ConnectionProfile,
    connection_id: &str,
    phase: HookPhase,
    handles: Vec<DetachedProcessHandle>,
    hook_indices: &[usize],
) -> Result<Vec<DetachedHookProcess>, String> {
    let mut processes = Vec::new();
    for (position, handle) in handles.into_iter().enumerate() {
        let hook_index = hook_indices.get(position).copied().unwrap_or(position);
        match start_detached_hook_process(handle) {
            Ok(process) => processes.push(process),
            Err(error) => {
                state.detached_processes.register(
                    connection_id,
                    processes
                        .iter()
                        .map(|process: &DetachedHookProcess| Arc::clone(&process.shared))
                        .collect(),
                );
                let cleanup_errors = terminate_detached_processes(state, connection_id);
                record_hook_execution_failure(state, profile, phase, hook_index).await;
                let mut message = format!(
                    "{} hook #{} failed to start detached supervision: {error}",
                    phase.label(),
                    hook_index + 1
                );
                if !cleanup_errors.is_empty() {
                    message.push_str(&format!("; {}", cleanup_errors.join("; ")));
                }
                return Err(message);
            }
        }
    }
    state.detached_processes.register(
        connection_id,
        processes
            .iter()
            .map(|process| Arc::clone(&process.shared))
            .collect(),
    );
    Ok(processes)
}

async fn wait_for_pre_connect_if_needed(
    state: &ServerState,
    profile: &dbflux_core::ConnectionProfile,
    connection_id: &str,
    phase: HookPhase,
    processes: &mut [DetachedHookProcess],
    hook_indices: &[usize],
    endpoint: (Option<String>, Option<u16>),
) -> Result<(), String> {
    if phase == HookPhase::PreConnect && !processes.is_empty() {
        wait_for_pre_connect_detached_hooks(
            state,
            profile,
            connection_id,
            processes,
            hook_indices,
            endpoint.0,
            endpoint.1,
        )
        .await
    } else {
        Ok(())
    }
}

async fn finish_connection_hook_phase(
    state: &ServerState,
    profile: &dbflux_core::ConnectionProfile,
    connection_id: &str,
    outcome: HookPhaseOutcome,
    metadata: &HookPhaseExecutionMetadata,
    processes: &mut [DetachedHookProcess],
) -> Result<(), String> {
    let phase = metadata.phase;
    let executions = match &outcome {
        HookPhaseOutcome::Success { executions }
        | HookPhaseOutcome::Aborted { executions, .. }
        | HookPhaseOutcome::CompletedWithWarnings { executions, .. } => executions,
    };
    record_hook_executions(
        state,
        profile,
        phase,
        executions,
        &metadata.configured_indices,
    )
    .await;

    let warnings = match outcome {
        HookPhaseOutcome::Aborted { executions, .. } => {
            let cleanup_errors = terminate_detached_processes(state, connection_id);
            let executed_index = executions.len().saturating_sub(1);
            let hook_index = metadata
                .configured_indices
                .get(executed_index)
                .copied()
                .unwrap_or(executed_index)
                + 1;
            let mut error = format!("{} hook #{} failed", phase.label(), hook_index);
            if !cleanup_errors.is_empty() {
                error.push_str(&format!("; {}", cleanup_errors.join("; ")));
            }
            return Err(error);
        }
        HookPhaseOutcome::CompletedWithWarnings { warnings, .. } => warnings,
        HookPhaseOutcome::Success { .. } => Vec::new(),
    };

    for _ in warnings {
        log::warn!(
            "MCP connection hook for profile '{}' completed with a warning during {}",
            profile.name,
            phase.label().to_ascii_lowercase()
        );
    }

    wait_for_pre_connect_if_needed(
        state,
        profile,
        connection_id,
        phase,
        processes,
        &metadata.detached_indices,
        metadata.endpoint.clone(),
    )
    .await
}

async fn run_connection_hook_phase(
    state: &ServerState,
    profile: &dbflux_core::ConnectionProfile,
    connection_id: &str,
    phase: HookPhase,
) -> Result<(), String> {
    let hooks = validate_connection_hook_phase(state, profile, phase).await?;
    if hooks.is_empty() {
        return Ok(());
    }

    let configured_indices = configured_hook_indices(&hooks);
    let detached_indices = detached_hook_indices(&hooks);

    let mut context = HookContext::from_profile(profile);
    context.phase = Some(phase);
    let endpoint = pre_connect_probe_endpoint(profile, &context);
    let metadata = HookPhaseExecutionMetadata {
        phase,
        configured_indices,
        detached_indices,
        endpoint,
    };
    let (outcome, detached_handles) =
        execute_connection_hook_phase(state, connection_id, hooks, context, phase).await?;
    let mut processes = start_detached_hook_processes(
        state,
        profile,
        connection_id,
        phase,
        detached_handles,
        &metadata.detached_indices,
    )
    .await?;

    finish_connection_hook_phase(
        state,
        profile,
        connection_id,
        outcome,
        &metadata,
        &mut processes,
    )
    .await
}

async fn record_hook_execution_failure(
    state: &ServerState,
    profile: &dbflux_core::ConnectionProfile,
    phase: HookPhase,
    hook_index: usize,
) {
    use dbflux_core::observability::types::{
        EventCategory, EventOutcome, EventRecord, EventSeverity,
    };
    use dbflux_core::observability::{EventOrigin, actions};

    let audit_service = {
        let runtime = state.runtime.read().await;
        runtime.audit_service().clone()
    };
    let phase_name = phase.label().to_ascii_lowercase();
    let event = EventRecord::new(
        chrono::Utc::now().timestamp_millis(),
        EventSeverity::Error,
        EventCategory::Hook,
        EventOutcome::Failure,
    )
    .with_typed_action(actions::HOOK_EXECUTE_FAILED)
    .with_origin(EventOrigin::mcp())
    .with_actor_id(&state.client_id)
    .with_connection_context(
        profile.id.to_string(),
        profile.config.database().unwrap_or_default(),
        profile.driver_id(),
    )
    .with_object_ref(
        "connection_hook",
        format!("{phase_name}:{}", hook_index + 1),
    )
    .with_summary(format!(
        "Connection hook failed during {} for profile '{}'",
        phase_name, profile.name
    ))
    .with_details_json(
        serde_json::json!({
            "phase": phase_name,
            "hook_index": hook_index + 1,
        })
        .to_string(),
    )
    .with_error("hook_execution_failed", "Connection hook failed");

    if let Err(error) = audit_service.record(event) {
        log::warn!("Unable to record MCP connection hook audit event: {error}");
    }
}

async fn record_hook_executions(
    state: &ServerState,
    profile: &dbflux_core::ConnectionProfile,
    phase: HookPhase,
    executions: &[HookExecution],
    configured_indices: &[usize],
) {
    use dbflux_core::observability::types::{
        EventCategory, EventOutcome, EventRecord, EventSeverity,
    };
    use dbflux_core::observability::{EventOrigin, actions};

    let audit_service = {
        let runtime = state.runtime.read().await;
        runtime.audit_service().clone()
    };
    let database = profile.config.database().unwrap_or_default();

    for (position, execution) in executions.iter().enumerate() {
        let index = configured_indices
            .get(position)
            .copied()
            .unwrap_or(position);
        let succeeded = execution.result.as_ref().is_ok_and(HookResult::is_success);
        let phase_name = phase.label().to_ascii_lowercase();
        let event = EventRecord::new(
            chrono::Utc::now().timestamp_millis(),
            if succeeded {
                EventSeverity::Info
            } else {
                EventSeverity::Error
            },
            EventCategory::Hook,
            if succeeded {
                EventOutcome::Success
            } else {
                EventOutcome::Failure
            },
        )
        .with_typed_action(if succeeded {
            actions::HOOK_EXECUTE
        } else {
            actions::HOOK_EXECUTE_FAILED
        })
        .with_origin(EventOrigin::mcp())
        .with_actor_id(&state.client_id)
        .with_connection_context(
            profile.id.to_string(),
            database.clone(),
            profile.driver_id(),
        )
        .with_object_ref("connection_hook", format!("{phase_name}:{}", index + 1))
        .with_summary(format!(
            "Connection hook {} during {} for profile '{}'",
            if succeeded { "completed" } else { "failed" },
            phase_name,
            profile.name
        ))
        .with_details_json(
            serde_json::json!({
                "phase": phase_name,
                "hook_index": index + 1,
            })
            .to_string(),
        );
        let event = if succeeded {
            event
        } else {
            event.with_error("hook_execution_failed", "Connection hook failed")
        };

        if let Err(error) = audit_service.record(event) {
            log::warn!("Unable to record MCP connection hook audit event: {error}");
        }
    }
}

struct McpAccessManager {
    #[cfg(feature = "aws")]
    ssm_factory: Option<Arc<dbflux_ssm::SsmTunnelFactory>>,
}

impl McpAccessManager {
    #[cfg(feature = "aws")]
    fn new(ssm_factory: Option<Arc<dbflux_ssm::SsmTunnelFactory>>) -> Self {
        Self { ssm_factory }
    }

    #[cfg(not(feature = "aws"))]
    fn new() -> Self {
        Self {}
    }

    async fn open_managed(
        &self,
        provider: &str,
        params: &HashMap<String, String>,
        remote_host: &str,
    ) -> Result<AccessHandle, dbflux_core::DbError> {
        match provider {
            #[cfg(feature = "aws")]
            "aws-ssm" => {
                let instance_id = params.get("instance_id").map(String::as_str).unwrap_or("");
                let region = params
                    .get("region")
                    .map(String::as_str)
                    .unwrap_or("us-east-1");
                let remote_port = params
                    .get("remote_port")
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0);

                let factory = self.ssm_factory.as_ref().ok_or_else(|| {
                    dbflux_core::DbError::connection_failed("SSM tunnel factory not available")
                })?;

                let tunnel = factory.start(instance_id, region, remote_host, remote_port)?;
                Ok(AccessHandle::tunnel(tunnel.local_port(), Box::new(tunnel)))
            }
            #[cfg(not(feature = "aws"))]
            _ => {
                let _ = params;
                let _ = remote_host;
                Err(dbflux_core::DbError::connection_failed(
                    "Managed access providers are not available in this build",
                ))
            }
            #[cfg(feature = "aws")]
            other => Err(dbflux_core::DbError::connection_failed(format!(
                "Unknown managed access provider: '{}'. No handler registered.",
                other
            ))),
        }
    }
}

#[async_trait::async_trait]
impl AccessManager for McpAccessManager {
    async fn open(
        &self,
        access_kind: &AccessKind,
        remote_host: &str,
        _remote_port: u16,
    ) -> Result<AccessHandle, dbflux_core::DbError> {
        match access_kind {
            AccessKind::Direct => Ok(AccessHandle::direct()),
            AccessKind::Ssh { .. } => Err(dbflux_core::DbError::connection_failed(
                "SSH tunnels are not supported by the MCP server access manager",
            )),
            AccessKind::Proxy { .. } => Err(dbflux_core::DbError::connection_failed(
                "Proxy tunnels are not supported by the MCP server access manager",
            )),
            AccessKind::Managed { provider, params } => {
                self.open_managed(provider, params, remote_host).await
            }
        }
    }
}

struct McpConnectionFactory {
    state: ServerState,
}

impl McpConnectionFactory {
    fn new(state: ServerState) -> Self {
        Self { state }
    }

    async fn connect(
        &self,
        connection_id: &str,
        database: Option<&str>,
    ) -> Result<Arc<CachedConnection>, String> {
        let profile_uuid = connection_id
            .parse::<uuid::Uuid>()
            .map_err(|_| error_messages::invalid_connection_id(connection_id))?;

        let (mut profile, profile_load_error) = {
            let profile_manager = self.state.profile_manager.read().await;
            let profile = profile_manager
                .find_by_id(profile_uuid)
                .cloned()
                .ok_or_else(|| error_messages::connection_not_found(connection_id))?;
            let load_error = profile_manager
                .profile_load_error(profile_uuid)
                .map(str::to_string);
            (profile, load_error)
        };

        if let Some(error) = profile_load_error {
            return Err(error);
        }

        if let Some(driver_defaults) = self.state.driver_settings.get(&profile.driver_id()) {
            let mut merged_settings = driver_defaults.clone();

            if let Some(connection_settings) = profile.connection_settings.take() {
                merged_settings.extend(connection_settings);
            }

            profile.connection_settings = if merged_settings.is_empty() {
                None
            } else {
                Some(merged_settings)
            };
        }

        if let Some(database) = database {
            profile.config = profile.config.clone().with_database(database)?;
        }

        let driver_id = profile.driver_id();
        let available_drivers: Vec<String> = self.state.driver_registry.keys().cloned().collect();
        let driver = self
            .state
            .driver_registry
            .get(&driver_id)
            .cloned()
            .ok_or_else(|| error_messages::driver_not_available(&driver_id, &available_drivers))?;

        let connection_key = database
            .map(|database| format!("{}:{}", connection_id, database))
            .unwrap_or_else(|| connection_id.to_string());
        run_connection_hook_phase(
            &self.state,
            &profile,
            &connection_key,
            HookPhase::PreConnect,
        )
        .await?;

        let connection_result = if profile.uses_pipeline() {
            self.connect_with_pipeline(connection_key.clone(), driver_id, driver, profile)
                .await
        } else {
            self.connect_direct(connection_key.clone(), driver_id, driver, profile)
                .await
        };

        match connection_result {
            Ok(connection) => Ok(connection),
            Err(error) => {
                let cleanup_errors = terminate_detached_processes(&self.state, &connection_key);
                if cleanup_errors.is_empty() {
                    Err(error)
                } else {
                    Err(format!("{error}; {}", cleanup_errors.join("; ")))
                }
            }
        }
    }

    async fn connect_with_pipeline(
        &self,
        connection_key: String,
        driver_id: String,
        driver: Arc<dyn dbflux_core::DbDriver>,
        profile: dbflux_core::ConnectionProfile,
    ) -> Result<Arc<CachedConnection>, String> {
        // The keyring password is resolved before the pipeline runs: the pipeline
        // only yields a password when the profile carries a `ValueRef` for it, so a
        // profile that routes through the pipeline (auth profile, access kind, value
        // refs) but stores its password in the OS keyring would otherwise connect
        // with no password at all. The GUI applies the same fallback.
        let keyring_password =
            DbFluxServer::resolve_profile_secrets(&self.state, &profile)?.password;
        let pipeline_input = self.build_pipeline_input(profile).await?;
        let (state_tx, _state_rx) = dbflux_core::pipeline_state_channel();
        let pipeline_output = dbflux_core::run_pipeline(pipeline_input, &state_tx)
            .await
            .map_err(|error| format!("Pipeline stage '{}': {}", error.stage, error.source))?;

        let mut profile = pipeline_output.resolved_profile;
        let access_handle = pipeline_output.access_handle;

        if access_handle.is_tunneled() {
            profile
                .config
                .redirect_to_tunnel(access_handle.local_port());
        }

        let hook_profile = profile.clone();

        let overrides =
            ConnectionOverrides::new(pipeline_output.resolved_password.or(keyring_password));
        let driver_id_for_error = driver_id.clone();
        let connection_key_for_error = connection_key.clone();

        let mut connection = tokio::task::spawn_blocking(move || {
            driver
                .connect_with_overrides(&profile, &overrides)
                .map_err(|error| {
                    error_messages::connection_error(
                        &connection_key_for_error,
                        &driver_id_for_error,
                        error,
                    )
                })
        })
        .await
        .map_err(|error| format!("Blocking task failed: {}", error))??;

        if let Err(hook_error) = run_connection_hook_phase(
            &self.state,
            &hook_profile,
            &connection_key,
            HookPhase::PostConnect,
        )
        .await
        {
            let cleanup_errors = terminate_detached_processes(&self.state, &connection_key);
            let close_error = tokio::task::spawn_blocking(move || {
                connection.close().err().map(|error| error.to_string())
            })
            .await
            .map_err(|error| {
                format!("Blocking task failed while closing rejected connection: {error}")
            })?;
            drop(access_handle);
            let mut error = match close_error {
                Some(close_error) => {
                    format!("{hook_error}; failed to close rejected connection: {close_error}")
                }
                None => hook_error,
            };
            if !cleanup_errors.is_empty() {
                error.push_str(&format!("; {}", cleanup_errors.join("; ")));
            }
            return Err(error);
        }

        Ok(Arc::new(CachedConnection::new(
            Arc::from(connection),
            Some(Box::new(access_handle)),
        )))
    }

    async fn connect_direct(
        &self,
        connection_key: String,
        driver_id: String,
        driver: Arc<dyn dbflux_core::DbDriver>,
        profile: dbflux_core::ConnectionProfile,
    ) -> Result<Arc<CachedConnection>, String> {
        let resolved_secrets = DbFluxServer::resolve_profile_secrets(&self.state, &profile)?;
        let password = resolved_secrets.password;
        let ssh_secret = resolved_secrets.ssh_secret;
        let connection_key_for_error = connection_key.clone();
        let driver_id_for_error = driver_id.clone();
        let hook_profile = profile.clone();

        let mut connection = tokio::task::spawn_blocking(move || {
            driver
                .connect_with_secrets(&profile, password.as_ref(), ssh_secret.as_ref())
                .map_err(|error| {
                    error_messages::connection_error(
                        &connection_key_for_error,
                        &driver_id_for_error,
                        error,
                    )
                })
        })
        .await
        .map_err(|error| format!("Blocking task failed: {}", error))??;

        if let Err(hook_error) = run_connection_hook_phase(
            &self.state,
            &hook_profile,
            &connection_key,
            HookPhase::PostConnect,
        )
        .await
        {
            let cleanup_errors = terminate_detached_processes(&self.state, &connection_key);
            let close_error = tokio::task::spawn_blocking(move || {
                connection.close().err().map(|error| error.to_string())
            })
            .await
            .map_err(|error| {
                format!("Blocking task failed while closing rejected connection: {error}")
            })?;
            let mut error = match close_error {
                Some(close_error) => {
                    format!("{hook_error}; failed to close rejected connection: {close_error}")
                }
                None => hook_error,
            };
            if !cleanup_errors.is_empty() {
                error.push_str(&format!("; {}", cleanup_errors.join("; ")));
            }
            return Err(error);
        }

        Ok(Arc::new(CachedConnection::new(Arc::from(connection), None)))
    }

    async fn build_pipeline_input(
        &self,
        profile: dbflux_core::ConnectionProfile,
    ) -> Result<PipelineInput, String> {
        let selected_auth_profile_id = profile
            .access_kind
            .as_ref()
            .and_then(|kind| match kind {
                AccessKind::Managed { params, .. } => params
                    .get("auth_profile_id")
                    .and_then(|value| value.parse().ok()),
                _ => None,
            })
            .or(profile.auth_profile_id);

        let auth_profile = {
            let auth_profiles = self.state.auth_profile_manager.read().await;
            selected_auth_profile_id.and_then(|auth_id| {
                auth_profiles
                    .items
                    .iter()
                    .find(|profile| profile.id == auth_id && profile.enabled)
                    .cloned()
            })
        };

        let uses_managed_access = matches!(profile.access_kind, Some(AccessKind::Managed { .. }));
        if uses_managed_access && auth_profile.is_none() {
            return Err(
                "Managed access requires an auth profile. Select one in Access > SSM Auth Profile."
                    .to_string(),
            );
        }

        let registered_auth_provider_ids: HashSet<&str> = self
            .state
            .auth_provider_registry
            .keys()
            .map(String::as_str)
            .collect();

        let uses_registered_auth_value_sources = profile.value_refs.values().any(|value_ref| {
            matches!(
                value_ref,
                ValueRef::Secret { provider, .. } | ValueRef::Parameter { provider, .. }
                    if registered_auth_provider_ids.contains(provider.as_str())
            )
        });

        if uses_registered_auth_value_sources && auth_profile.is_none() {
            return Err(
                "Value sources requiring auth providers need an auth profile. Select one before connecting."
                    .to_string(),
            );
        }

        let (auth_profile, auth_provider) = if let Some(profile) = auth_profile {
            let provider = self
                .state
                .auth_provider_registry
                .get(&profile.provider_id)
                .cloned()
                .ok_or_else(|| {
                    format!("Auth provider '{}' is not available", profile.provider_id)
                })?;

            let profile_registry_snapshot: Vec<dbflux_core::auth::AuthProfile> = {
                let manager = self.state.auth_profile_manager.read().await;
                manager.items.clone()
            };
            let expanded = dbflux_core::auth::expand_auth_profile_refs(
                &profile,
                provider.form_def(),
                &|target_id| {
                    profile_registry_snapshot
                        .iter()
                        .find(|p| p.id == *target_id)
                        .cloned()
                },
            );

            (Some(expanded), Some(SharedDynAuthProvider::boxed(provider)))
        } else {
            (None, None)
        };

        let resolver = CompositeValueResolver::new(Arc::new(ValueCache::new(
            std::time::Duration::from_secs(300),
        )));

        #[cfg(feature = "aws")]
        let aws_profile_name = auth_profile
            .as_ref()
            .and_then(|profile| profile.fields.get("profile_name").cloned());

        let access_manager: Arc<dyn AccessManager> = Arc::new(McpAccessManager::new(
            #[cfg(feature = "aws")]
            Some(Arc::new(dbflux_ssm::SsmTunnelFactory::new(
                aws_profile_name,
            ))),
        ));

        Ok(PipelineInput {
            profile,
            auth_provider,
            auth_profile,
            resolver,
            access_manager,
            cancel: CancelToken::new(),
        })
    }

    async fn connect_and_cache(&self, connection_id: &str) -> Result<Arc<dyn Connection>, String> {
        DbFluxServer::get_or_connect(self.state.clone(), connection_id).await
    }
}

/// Database context of a live connection, as reported to an agent by `connect`.
///
/// Both parts are empty for a driver that has no concept of databases.
pub(crate) struct DatabaseContext {
    pub(crate) current_database: Option<String>,
    pub(crate) databases: Vec<String>,
}

/// Main DBFlux MCP Server
///
/// Public so embedders (and the transport-level end-to-end tests) can serve the
/// same handler over a transport of their choice. `run_mcp_server` is the stdio
/// entry point; this type is the seam for anything else.
#[derive(Clone)]
pub struct DbFluxServer {
    #[allow(dead_code)] // Used by governance middleware and tools
    pub(crate) state: ServerState,
    #[allow(dead_code)] // Used for policy evaluation
    pub(crate) governance: GovernanceMiddleware,
    pub(crate) tool_router: ToolRouter<DbFluxServer>,
}

#[tool_router]
impl DbFluxServer {
    pub fn new(state: ServerState) -> Self {
        let governance = GovernanceMiddleware::new(state.clone());
        Self {
            state,
            governance,
            tool_router: Self::tool_router()
                + Self::connection_router()
                + Self::schema_router()
                + Self::query_router()
                + Self::read_router()
                + Self::write_router()
                + Self::destructive_router()
                + Self::ddl_router()
                + Self::scripts_router()
                + Self::approval_router()
                + Self::audit_router(),
        }
    }

    /// Classify a query based on its SQL content
    #[allow(dead_code)] // May be used by future governance features
    fn classify_query(&self, query: &str) -> dbflux_policy::ExecutionClassification {
        use dbflux_policy::ExecutionClassification;

        let query_upper = query.trim().to_uppercase();

        if query_upper.starts_with("SELECT")
            || query_upper.starts_with("SHOW")
            || query_upper.starts_with("DESCRIBE")
            || query_upper.starts_with("EXPLAIN")
        {
            ExecutionClassification::Read
        } else if query_upper.starts_with("INSERT") || query_upper.starts_with("UPDATE") {
            ExecutionClassification::Write
        } else if query_upper.starts_with("DELETE") || query_upper.starts_with("TRUNCATE") {
            ExecutionClassification::Destructive
        } else if query_upper.starts_with("CREATE INDEX") || query_upper.starts_with("CREATE TABLE")
        {
            ExecutionClassification::AdminSafe
        } else if query_upper.starts_with("DROP TABLE")
            || query_upper.starts_with("DROP DATABASE")
            || query_upper.contains(" DROP COLUMN")
        {
            ExecutionClassification::AdminDestructive
        } else if query_upper.starts_with("CREATE")
            || query_upper.starts_with("ALTER")
            || query_upper.starts_with("GRANT")
            || query_upper.starts_with("REVOKE")
        {
            ExecutionClassification::Admin
        } else {
            // Default to read for unknown queries
            ExecutionClassification::Read
        }
    }

    /// Execute a blocking operation in a spawned thread
    #[allow(dead_code)]
    pub(crate) async fn execute_blocking<F, T, E>(f: F) -> Result<T, String>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        tokio::task::spawn_blocking(move || f().map_err(|e| format!("{}", e)))
            .await
            .map_err(|e| format!("Blocking task failed: {}", e))?
    }

    /// Get connection and execute a query in a blocking context
    /// This is the preferred method for tools that need to execute queries
    #[allow(dead_code)]
    pub(crate) async fn get_connected_query<F, T>(
        state: ServerState,
        connection_id: &str,
        f: F,
    ) -> Result<T, String>
    where
        F: FnOnce(Arc<dyn Connection>) -> Result<T, String> + Send + 'static,
        T: Send + 'static,
    {
        let connection = Self::get_or_connect(state, connection_id).await?;

        tokio::task::spawn_blocking(move || {
            let mut scope = dbflux_core::ExecutionSessionScope::new(connection)
                .map_err(|error| error.to_string())?;
            let result = f(scope.connection()).map_err(dbflux_core::DbError::query_failed);
            scope.finish(result).map_err(|error| error.to_string())
        })
        .await
        .map_err(|e| format!("Blocking task failed: {}", e))?
    }

    pub(crate) async fn execute_connection_blocking<F, T>(
        connection: Arc<dyn Connection>,
        f: F,
    ) -> Result<T, String>
    where
        F: FnOnce(Arc<dyn Connection>) -> Result<T, String> + Send + 'static,
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(move || {
            let mut scope = dbflux_core::ExecutionSessionScope::new(connection)
                .map_err(|error| error.to_string())?;
            let result = f(scope.connection()).map_err(dbflux_core::DbError::query_failed);
            scope.finish(result).map_err(|error| error.to_string())
        })
        .await
        .map_err(|e| format!("Blocking task failed: {}", e))?
    }

    /// Get or establish a connection for the given connection_id
    ///
    /// Reuse cached connections when available to avoid dropping the last driver
    /// handle at the end of each request. PostgreSQL in particular can block while
    /// tearing down the final client handle, which prevents the MCP response from
    /// being sent back to the caller.
    #[allow(dead_code)] // Used by tool implementations
    pub(crate) async fn get_or_connect(
        state: ServerState,
        connection_id: &str,
    ) -> Result<Arc<dyn Connection>, String> {
        Self::get_or_connect_cached(state, connection_id, None).await
    }

    /// Resolve secrets from a profile using keyring
    fn resolve_profile_secrets(
        state: &ServerState,
        profile: &dbflux_core::ConnectionProfile,
    ) -> Result<ResolvedSecrets, String> {
        let password = if profile.save_password {
            match state.secret_manager.get_password(profile) {
                Some(pw) => {
                    log::debug!(
                        "Resolved password from keyring for profile '{}' (id={})",
                        profile.name,
                        profile.id
                    );
                    Some(pw)
                }
                None => {
                    log::warn!(
                        "No password found in keyring for profile '{}' (id={}, save_password={}). \
                         Connection may fail with authentication error.",
                        profile.name,
                        profile.id,
                        profile.save_password
                    );
                    None
                }
            }
        } else {
            log::debug!(
                "Profile '{}' has save_password=false, skipping keyring lookup",
                profile.name
            );
            None
        };

        let ssh_secret = if profile.config.has_ssh_tunnel() {
            match state.secret_manager.get_ssh_password(profile) {
                Some(ssh) => {
                    log::debug!(
                        "Resolved SSH password from keyring for profile '{}'",
                        profile.name
                    );
                    Some(ssh)
                }
                None => {
                    log::warn!(
                        "No SSH password found in keyring for profile '{}' (has_ssh_tunnel=true). \
                         SSH tunnel connection may fail.",
                        profile.name
                    );
                    None
                }
            }
        } else {
            None
        };

        Ok(ResolvedSecrets {
            password,
            ssh_secret,
        })
    }

    /// Get the current database for a connection from the cache
    pub(crate) async fn get_current_database(
        state: &ServerState,
        connection_id: &str,
    ) -> Result<Option<String>, String> {
        let cache = state.connection_cache.read().await;

        if let Some(conn) = cache.get(connection_id) {
            let connection = conn.connection();
            let db = tokio::task::spawn_blocking(move || connection.active_database())
                .await
                .map_err(|e| format!("Blocking task failed: {}", e))?;
            return Ok(db);
        }

        drop(cache);

        Self::profile_database(state, connection_id).await
    }

    /// Get the database a connection's profile is configured for
    async fn profile_database(
        state: &ServerState,
        connection_id: &str,
    ) -> Result<Option<String>, String> {
        let profile_uuid = connection_id
            .parse::<uuid::Uuid>()
            .map_err(|_| error_messages::invalid_connection_id(connection_id))?;

        let profile_manager = state.profile_manager.read().await;
        let profile = profile_manager
            .find_by_id(profile_uuid)
            .ok_or_else(|| error_messages::connection_not_found(connection_id))?;

        Ok(profile.config.database())
    }

    /// Resolve the database a live connection is on and the databases it can reach.
    ///
    /// The current database comes from the connection's active database, then
    /// from the listed database flagged as current, then from the profile. A
    /// failed lookup is logged and leaves its part empty, because the
    /// connection itself is already established.
    pub(crate) async fn database_context(
        state: &ServerState,
        connection_id: &str,
        connection: Arc<dyn Connection>,
    ) -> DatabaseContext {
        let lookup = tokio::task::spawn_blocking(move || {
            (connection.active_database(), connection.list_databases())
        })
        .await
        .log_err_with("Database context lookup task failed");

        let (active_database, listed) = match lookup {
            Some((active_database, listed)) => (
                active_database,
                listed
                    .log_err_with("Failed to list databases after connecting")
                    .unwrap_or_default(),
            ),
            None => (None, Vec::new()),
        };

        let listed_current = listed
            .iter()
            .find(|database| database.is_current)
            .map(|database| database.name.clone());

        let current_database = match active_database.or(listed_current) {
            Some(database) => Some(database),
            None => Self::profile_database(state, connection_id)
                .await
                .log_err_with("Failed to read the profile database after connecting")
                .flatten(),
        };

        DatabaseContext {
            current_database,
            databases: listed.into_iter().map(|database| database.name).collect(),
        }
    }

    /// Connect to a different database using the same profile
    ///
    /// Reuse cached per-database connections for the same reason as `get_or_connect`.
    pub(crate) async fn connect_with_database(
        state: ServerState,
        connection_id: &str,
        database: &str,
    ) -> Result<Arc<dyn Connection>, String> {
        Self::get_or_connect_cached(state, connection_id, Some(database)).await
    }

    pub(crate) async fn disconnect_impl(
        state: ServerState,
        connection_id: &str,
    ) -> Result<(), String> {
        let _setup_guard = lock_connection_setup(&state, connection_id).await;
        let retired_connections = {
            let mut cache = state.connection_cache.write().await;
            cache.drain_connection_variants_with_keys(connection_id)
        };
        let mut errors = terminate_detached_process_variants(&state, connection_id);
        if retired_connections.is_empty() {
            return if errors.is_empty() {
                Ok(())
            } else {
                Err(errors.join("; "))
            };
        }

        let profile = if let Ok(profile_id) = connection_id.parse::<uuid::Uuid>() {
            let profile_manager = state.profile_manager.read().await;
            profile_manager.find_by_id(profile_id).cloned()
        } else {
            None
        };

        for (connection_key, _) in &retired_connections {
            if let Some(profile) = profile.as_ref() {
                match profile_for_connection_key(profile, connection_id, connection_key) {
                    Ok(variant_profile) => {
                        if let Err(error) = run_connection_hook_phase(
                            &state,
                            &variant_profile,
                            connection_key,
                            HookPhase::PreDisconnect,
                        )
                        .await
                        {
                            errors.push(error);
                        }
                    }
                    Err(error) => errors.push(error),
                }
            }
        }

        let connections_to_teardown = retired_connections
            .iter()
            .map(|(_, cached)| Arc::clone(cached))
            .collect::<Vec<_>>();
        let teardown = tokio::task::spawn_blocking(move || {
            let mut errors = Vec::new();
            for cached in connections_to_teardown {
                let connection = cached.connection();
                if let Some(factory) = connection.execution_session_factory()
                    && let Err(error) = factory.shutdown()
                {
                    errors.push(error.to_string());
                }
            }
            errors
        })
        .await;
        match teardown {
            Ok(teardown_errors) => errors.extend(teardown_errors),
            Err(error) => errors.push(format!("Connection teardown task failed: {error}")),
        }

        for (connection_key, _) in &retired_connections {
            if let Some(profile) = profile.as_ref() {
                match profile_for_connection_key(profile, connection_id, connection_key) {
                    Ok(variant_profile) => {
                        if let Err(error) = run_connection_hook_phase(
                            &state,
                            &variant_profile,
                            connection_key,
                            HookPhase::PostDisconnect,
                        )
                        .await
                        {
                            errors.push(error);
                        }
                    }
                    Err(error) => errors.push(error),
                }
            }
            errors.extend(terminate_detached_processes(&state, connection_key));
        }
        errors.extend(terminate_detached_process_variants(&state, connection_id));

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    pub(crate) async fn disconnect_all(state: ServerState) -> Result<(), String> {
        Self::disconnect_all_with_timeout(state, SHUTDOWN_DRAIN_TIMEOUT).await
    }

    async fn disconnect_all_with_timeout(
        state: ServerState,
        timeout: Duration,
    ) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        let mut connection_ids = {
            let cache = state.connection_cache.read().await;
            cache.connection_ids()
        };
        connection_ids.extend(state.detached_processes.connection_ids());
        connection_ids.sort_unstable();
        connection_ids.dedup();

        let mut errors = Vec::new();
        for connection_id in connection_ids {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                state.detached_processes.cancel_running_hooks();
                errors.push(format!(
                    "{connection_id}: shutdown drain exceeded {}ms",
                    timeout.as_millis()
                ));
                errors.extend(terminate_detached_process_variants(&state, &connection_id));
                continue;
            }

            match tokio::time::timeout(
                remaining,
                Self::disconnect_impl(state.clone(), &connection_id),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(format!("{connection_id}: {error}")),
                Err(_) => {
                    state.detached_processes.cancel_running_hooks();
                    errors.push(format!(
                        "{connection_id}: shutdown drain exceeded {}ms",
                        timeout.as_millis()
                    ));
                    errors.extend(terminate_detached_process_variants(&state, &connection_id));
                }
            }
        }

        let remaining_process_ids = state.detached_processes.connection_ids();
        for connection_id in remaining_process_ids {
            errors.extend(terminate_detached_process_variants(&state, &connection_id));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    async fn get_or_connect_cached(
        state: ServerState,
        connection_id: &str,
        database: Option<&str>,
    ) -> Result<Arc<dyn Connection>, String> {
        let cache_key = database
            .map(|database| format!("{}:{}", connection_id, database))
            .unwrap_or_else(|| connection_id.to_string());

        {
            let cache = state.connection_cache.read().await;
            if let Some(connection) = cache.get(&cache_key) {
                return Ok(connection.connection());
            }
        }

        let _setup_guard = lock_connection_setup(&state, connection_id).await;

        {
            let cache = state.connection_cache.read().await;
            if let Some(connection) = cache.get(&cache_key) {
                return Ok(connection.connection());
            }
        }

        let connection = McpConnectionFactory::new(state.clone())
            .connect(connection_id, database)
            .await?;
        let trait_object = connection.connection();

        let mut cache = state.connection_cache.write().await;
        if let Some(existing) = cache.get(&cache_key) {
            return Ok(existing.connection());
        }

        cache.insert(cache_key, connection);
        Ok(trait_object)
    }

    pub(crate) async fn connect_cached(
        state: ServerState,
        connection_id: &str,
    ) -> Result<Arc<dyn Connection>, String> {
        McpConnectionFactory::new(state)
            .connect_and_cache(connection_id)
            .await
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for DbFluxServer {
    /// Dispatches a tool call with its arguments in scope, so governance can
    /// queue a call that needs approval and match it to an approval later.
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let arguments = serde_json::Value::Object(request.arguments.clone().unwrap_or_default());
        let tool_call = rmcp::handler::server::tool::ToolCallContext::new(self, request, context);

        crate::governance::TOOL_CALL_ARGUMENTS
            .scope(arguments, self.tool_router.call(tool_call))
            .await
    }

    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "DBFlux MCP Server - AI-powered database client with governance controls.\n\
             \n\
             Supports multiple database types:\n\
             • PostgreSQL, MySQL/MariaDB\n\
             • MongoDB, Redis, DynamoDB\n\
             • SQLite\n\
             \n\
             All operations are subject to role-based access control and audit logging.\n\
             \n\
             Approvals: a policy can require a person to approve a call. Such a call does not \
             run; it fails with error data code \"approval_required\" and a \"pending_id\". \
             Then: (1) tell the user that pending execution <pending_id> is waiting for their \
             approval in DBFlux (Workspace > Pending Approvals); (2) wait, and never call \
             approve_execution or reject_execution, which are always denied to MCP clients; \
             (3) check the status with get_pending_execution: while its status is \"pending\", \
             it is still waiting, and status \"rejected\" carries the user's reason; (4) after \
             the user approves it, repeat the identical call (same tool, same arguments) and \
             it runs once. A rejected or expired call, or one \
             repeated with different arguments, is queued again instead of running.",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use dbflux_core::secrecy::{ExposeSecret, SecretString};
    use dbflux_core::{
        ConnectionHooks, ConnectionOverrides, ConnectionProfile, DbConfig, DbDriver, DbKind,
        FormValues, HookFailureMode, HookKind, NoopSecretStore, ValueRef,
    };
    use dbflux_mcp::McpRuntime;
    use dbflux_test_support::FakeDriver;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use tokio::sync::RwLock;

    #[derive(Debug, Clone)]
    struct ConnectInvocation {
        profile: ConnectionProfile,
        password: Option<String>,
    }

    #[derive(Clone)]
    struct RecordingDriver {
        inner: FakeDriver,
        invocations: Arc<Mutex<Vec<ConnectInvocation>>>,
        event_trace: Option<std::path::PathBuf>,
    }

    impl RecordingDriver {
        fn new(inner: FakeDriver) -> Self {
            Self {
                inner,
                invocations: Arc::new(Mutex::new(Vec::new())),
                event_trace: None,
            }
        }

        fn with_event_trace(mut self, path: std::path::PathBuf) -> Self {
            self.event_trace = Some(path);
            self
        }

        fn record_connection(&self) {
            if let Some(path) = &self.event_trace {
                use std::io::Write;

                let mut trace = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .expect("open hook event trace");
                writeln!(trace, "driver").expect("record driver connection");
            }
        }

        fn invocations(&self) -> Vec<ConnectInvocation> {
            self.invocations
                .lock()
                .expect("recording driver mutex poisoned")
                .clone()
        }
    }

    impl DbDriver for RecordingDriver {
        fn kind(&self) -> DbKind {
            self.inner.kind()
        }

        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            self.inner.metadata()
        }

        fn driver_key(&self) -> dbflux_core::DriverKey {
            self.inner.driver_key()
        }

        fn form_definition(&self) -> &dbflux_core::DriverFormDef {
            self.inner.form_definition()
        }

        fn build_config(&self, values: &FormValues) -> Result<DbConfig, dbflux_core::DbError> {
            self.inner.build_config(values)
        }

        fn extract_values(&self, config: &DbConfig) -> FormValues {
            self.inner.extract_values(config)
        }

        fn connect_with_secrets(
            &self,
            profile: &ConnectionProfile,
            password: Option<&SecretString>,
            ssh_secret: Option<&SecretString>,
        ) -> Result<Box<dyn Connection>, dbflux_core::DbError> {
            self.record_connection();
            self.inner
                .connect_with_secrets(profile, password, ssh_secret)
        }

        #[expect(
            clippy::unwrap_in_result,
            reason = "test recording driver: mutex poisoning makes the fixture's recorded \
                state untrustworthy, so aborting the test is intentional"
        )]
        fn connect_with_overrides(
            &self,
            profile: &ConnectionProfile,
            overrides: &ConnectionOverrides,
        ) -> Result<Box<dyn Connection>, dbflux_core::DbError> {
            self.record_connection();
            self.invocations
                .lock()
                .expect("recording driver mutex poisoned")
                .push(ConnectInvocation {
                    profile: profile.clone(),
                    password: overrides
                        .password
                        .as_ref()
                        .map(|value| value.expose_secret().to_string()),
                });

            self.inner.connect_with_overrides(profile, overrides)
        }

        fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), dbflux_core::DbError> {
            self.inner.test_connection(profile)
        }
    }

    /// In-memory secret store so tests can exercise the keyring-backed
    /// resolution paths without touching the OS keyring.
    #[derive(Default)]
    struct InMemorySecretStore {
        secrets: Mutex<HashMap<String, String>>,
    }

    impl dbflux_core::SecretStore for InMemorySecretStore {
        fn is_available(&self) -> bool {
            true
        }

        #[expect(
            clippy::unwrap_in_result,
            reason = "test in-memory secret store: mutex poisoning makes the fixture's \
                secret map untrustworthy, so aborting the test is intentional"
        )]
        fn get(&self, secret_ref: &str) -> Result<Option<SecretString>, dbflux_core::DbError> {
            Ok(self
                .secrets
                .lock()
                .expect("secret store mutex poisoned")
                .get(secret_ref)
                .map(|value| SecretString::from(value.clone())))
        }

        #[expect(
            clippy::unwrap_in_result,
            reason = "test in-memory secret store: mutex poisoning makes the fixture's \
                secret map untrustworthy, so aborting the test is intentional"
        )]
        fn set(&self, secret_ref: &str, value: &SecretString) -> Result<(), dbflux_core::DbError> {
            self.secrets
                .lock()
                .expect("secret store mutex poisoned")
                .insert(secret_ref.to_string(), value.expose_secret().to_string());
            Ok(())
        }

        #[expect(
            clippy::unwrap_in_result,
            reason = "test in-memory secret store: mutex poisoning makes the fixture's \
                secret map untrustworthy, so aborting the test is intentional"
        )]
        fn delete(&self, secret_ref: &str) -> Result<(), dbflux_core::DbError> {
            self.secrets
                .lock()
                .expect("secret store mutex poisoned")
                .remove(secret_ref);
            Ok(())
        }
    }

    fn test_state_with_driver(
        driver_id: &str,
        driver: Arc<dyn DbDriver>,
        profile: ConnectionProfile,
    ) -> ServerState {
        test_state_with_profiles_and_errors(
            driver_id,
            driver,
            vec![profile],
            HashMap::new(),
            Box::new(NoopSecretStore),
        )
    }

    fn test_state_with_driver_and_store(
        driver_id: &str,
        driver: Arc<dyn DbDriver>,
        profile: ConnectionProfile,
        secret_store: Box<dyn dbflux_core::SecretStore>,
    ) -> ServerState {
        test_state_with_profiles_and_errors(
            driver_id,
            driver,
            vec![profile],
            HashMap::new(),
            secret_store,
        )
    }

    fn test_state_with_profiles_and_errors(
        driver_id: &str,
        driver: Arc<dyn DbDriver>,
        profiles: Vec<ConnectionProfile>,
        profile_load_errors: HashMap<uuid::Uuid, String>,
        secret_store: Box<dyn dbflux_core::SecretStore>,
    ) -> ServerState {
        let audit_path = dbflux_audit::temp_sqlite_path("server_test_audit.sqlite");
        let audit_service = dbflux_audit::AuditService::new_sqlite(&audit_path)
            .expect("failed to create test audit service");

        let profile_manager =
            dbflux_core::ProfileManager::with_profiles_and_errors(profiles, profile_load_errors);

        ServerState {
            client_id: "test-client".to_string(),
            runtime: Arc::new(RwLock::new(McpRuntime::new(
                audit_service,
                Box::new(dbflux_approval::InMemoryPendingExecutionStore::default()),
            ))),
            profile_manager: Arc::new(RwLock::new(profile_manager)),
            auth_profile_manager: Arc::new(RwLock::new(dbflux_core::AuthProfileManager::default())),
            driver_registry: Arc::new(HashMap::from([(driver_id.to_string(), driver)])),
            auth_provider_registry: Arc::new(HashMap::new()),
            driver_settings: Arc::new(HashMap::new()),
            connection_cache: Arc::new(
                RwLock::new(crate::connection_cache::ConnectionCache::new()),
            ),
            connection_setup_lock: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            detached_processes: Arc::new(crate::state::DetachedProcessRegistry::new()),
            secret_manager: Arc::new(dbflux_core::SecretManager::new(secret_store)),
            mcp_enabled_by_default: true,
        }
    }

    fn process_hook(
        trace: &std::path::Path,
        phase: &str,
        fail: bool,
    ) -> dbflux_core::ConnectionHook {
        let mut env = HashMap::from([
            (
                "DBFLUX_MCP_HOOK_TEST_TRACE".to_string(),
                trace.to_string_lossy().into_owned(),
            ),
            ("DBFLUX_MCP_HOOK_TEST_PHASE".to_string(), phase.to_string()),
            (
                "PATH".to_string(),
                std::env::var("PATH").expect("test process PATH"),
            ),
        ]);
        if fail {
            env.insert("DBFLUX_MCP_HOOK_TEST_FAIL".to_string(), "1".to_string());
        }

        dbflux_core::ConnectionHook {
            enabled: true,
            kind: HookKind::Command {
                command: std::env::current_exe()
                    .expect("test executable path")
                    .to_string_lossy()
                    .into_owned(),
                args: vec!["mcp_hook_child".to_string(), "--nocapture".to_string()],
            },
            cwd: None,
            env,
            inherit_env: false,
            env_denylist: Vec::new(),
            timeout_ms: Some(10_000),
            execution_mode: dbflux_core::HookExecutionMode::Blocking,
            ready_signal: None,
            on_failure: HookFailureMode::Disconnect,
        }
    }

    fn pipeline_profile_with_hooks(hooks: ConnectionHooks) -> ConnectionProfile {
        let mut profile = ConnectionProfile::new("hooked-pg", DbConfig::default_postgres());
        profile
            .value_refs
            .insert("host".to_string(), ValueRef::literal("hook-test.example"));
        profile.hooks = Some(hooks);
        profile
    }

    #[test]
    fn mcp_hook_child() {
        let Ok(trace_path) = std::env::var("DBFLUX_MCP_HOOK_TEST_TRACE") else {
            return;
        };
        let phase = std::env::var("DBFLUX_MCP_HOOK_TEST_PHASE").expect("hook phase");
        let mut trace = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(trace_path)
            .expect("open hook event trace");
        let phase = if std::env::var_os("DBFLUX_MCP_HOOK_TEST_RECORD_DATABASE").is_some() {
            format!(
                "{phase}:{}",
                std::env::var("DBFLUX_DATABASE").expect("hook database context")
            )
        } else {
            phase
        };
        writeln!(trace, "{phase}").expect("record hook event");
        if let Ok(ready_signal) = std::env::var("DBFLUX_MCP_HOOK_TEST_READY_SIGNAL") {
            println!("{ready_signal}");
            std::io::stdout()
                .flush()
                .expect("flush hook readiness signal");
        }
        if let Ok(sleep_ms) = std::env::var("DBFLUX_MCP_HOOK_TEST_SLEEP_MS") {
            std::thread::sleep(Duration::from_millis(
                sleep_ms.parse().expect("test sleep duration"),
            ));
            let mut trace = std::fs::OpenOptions::new()
                .append(true)
                .open(std::env::var("DBFLUX_MCP_HOOK_TEST_TRACE").expect("hook trace path"))
                .expect("reopen hook event trace");
            writeln!(trace, "complete").expect("record hook completion");
        }
        if let Ok(secret) = std::env::var("DBFLUX_MCP_HOOK_TEST_SECRET") {
            eprintln!("{secret}");
        }
        if std::env::var_os("DBFLUX_MCP_HOOK_TEST_FAIL").is_some() {
            std::process::exit(1);
        }
    }

    #[tokio::test]
    async fn connection_factory_runs_connect_hooks_around_driver_connect() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver =
            RecordingDriver::new(FakeDriver::new(DbKind::Postgres)).with_event_trace(trace.clone());
        let driver_handle = Arc::new(driver) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        hooks
            .pre_connect
            .push(process_hook(&trace, "pre_connect", false));
        hooks
            .post_connect
            .push(process_hook(&trace, "post_connect", false));
        let profile = pipeline_profile_with_hooks(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        McpConnectionFactory::new(state.clone())
            .connect(&connection_id, None)
            .await
            .expect("connection should succeed");

        let events = std::fs::read_to_string(trace).expect("hook and driver events");
        assert_eq!(
            events.lines().collect::<Vec<_>>(),
            ["pre_connect", "driver", "post_connect"]
        );

        let runtime = state.runtime.read().await;
        let filter = dbflux_audit::query::AuditQueryFilter {
            category: Some("hook".to_string()),
            object_type: Some("connection_hook".to_string()),
            ..Default::default()
        };
        let audit_events = runtime
            .audit_service()
            .query_extended(&filter)
            .expect("hook audit events");
        assert_eq!(audit_events.len(), 2);
        assert!(audit_events.iter().all(|event| {
            event.action.as_deref() == Some("hook_execute")
                && event.outcome.as_deref() == Some("success")
        }));
        let audit_json = serde_json::to_string(&audit_events).expect("serialize hook audit events");
        assert!(!audit_json.contains("DBFLUX_MCP_HOOK_TEST_TRACE"));
        assert!(audit_json.contains("hook_index"));
    }

    #[tokio::test]
    async fn direct_connection_factory_runs_connect_hooks_around_driver_connect() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver =
            RecordingDriver::new(FakeDriver::new(DbKind::Postgres)).with_event_trace(trace.clone());
        let driver_handle = Arc::new(driver) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        hooks
            .pre_connect
            .push(process_hook(&trace, "pre_connect", false));
        hooks
            .post_connect
            .push(process_hook(&trace, "post_connect", false));
        let mut profile = ConnectionProfile::new("direct-hooked-pg", DbConfig::default_postgres());
        profile.hooks = Some(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        McpConnectionFactory::new(state)
            .connect(&connection_id, None)
            .await
            .expect("direct connection should succeed");

        let events = std::fs::read_to_string(trace).expect("hook and driver events");
        assert_eq!(
            events.lines().collect::<Vec<_>>(),
            ["pre_connect", "driver", "post_connect"]
        );
    }

    #[tokio::test]
    async fn pre_connect_hook_failure_prevents_driver_connect() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver = RecordingDriver::new(FakeDriver::new(DbKind::Postgres));
        let driver_handle = Arc::new(driver.clone()) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        let mut disabled_hook = process_hook(&trace, "pre_connect", false);
        disabled_hook.enabled = false;
        hooks.pre_connect.push(disabled_hook);
        let mut failing_hook = process_hook(&trace, "pre_connect", true);
        failing_hook.env.insert(
            "DBFLUX_MCP_HOOK_TEST_SECRET".to_string(),
            "private-hook-output".to_string(),
        );
        hooks.pre_connect.push(failing_hook);
        let profile = pipeline_profile_with_hooks(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        let error = DbFluxServer::get_or_connect(state.clone(), &connection_id)
            .await
            .err()
            .expect("failed PreConnect hook must abort connection");

        assert!(error.contains("Pre-connect hook #2 failed"));
        assert!(!error.contains("private-hook-output"));
        assert!(!error.contains("mcp_hook_child"));
        assert!(driver.invocations().is_empty());
        assert!(
            state
                .connection_cache
                .read()
                .await
                .get(&connection_id)
                .is_none()
        );

        let runtime = state.runtime.read().await;
        let filter = dbflux_audit::query::AuditQueryFilter {
            category: Some("hook".to_string()),
            object_type: Some("connection_hook".to_string()),
            ..Default::default()
        };
        let audit_events = runtime
            .audit_service()
            .query_extended(&filter)
            .expect("failed PreConnect audit event");
        assert!(audit_events.iter().any(|event| {
            event.object_id.as_deref() == Some("pre-connect:2")
                && event.action.as_deref() == Some("hook_execute_failed")
        }));
    }

    #[tokio::test]
    async fn unsupported_lua_hook_error_names_phase_and_hook() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver = RecordingDriver::new(FakeDriver::new(DbKind::Postgres));
        let driver_handle = Arc::new(driver.clone()) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        let mut disabled = process_hook(&trace, "pre_connect", false);
        disabled.enabled = false;
        hooks.pre_connect.push(disabled);
        hooks.pre_connect.push(dbflux_core::ConnectionHook {
            enabled: true,
            kind: HookKind::Lua {
                source: dbflux_core::ScriptSource::Inline {
                    content: String::new(),
                },
                capabilities: dbflux_core::LuaCapabilities::default(),
            },
            cwd: None,
            env: HashMap::new(),
            inherit_env: false,
            env_denylist: Vec::new(),
            timeout_ms: None,
            execution_mode: dbflux_core::HookExecutionMode::Blocking,
            ready_signal: None,
            on_failure: HookFailureMode::Disconnect,
        });
        hooks
            .pre_connect
            .push(process_hook(&trace, "pre_connect", false));
        let profile = pipeline_profile_with_hooks(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        let error = DbFluxServer::get_or_connect(state.clone(), &connection_id)
            .await
            .err()
            .expect("unsupported Lua hook must abort connection");

        assert!(error.contains("Pre-connect hook #2"));
        assert!(error.contains("Lua hooks require"));
        assert!(driver.invocations().is_empty());
        assert!(
            !trace.exists(),
            "later hooks must not run after Lua rejection"
        );

        let runtime = state.runtime.read().await;
        let filter = dbflux_audit::query::AuditQueryFilter {
            category: Some("hook".to_string()),
            object_type: Some("connection_hook".to_string()),
            ..Default::default()
        };
        let audit_events = runtime
            .audit_service()
            .query_extended(&filter)
            .expect("Lua rejection audit event");
        assert_eq!(audit_events.len(), 1);
        assert_eq!(audit_events[0].object_id.as_deref(), Some("pre-connect:2"));
        assert_eq!(
            audit_events[0].action.as_deref(),
            Some("hook_execute_failed")
        );
    }

    #[tokio::test]
    async fn profile_hook_load_error_only_blocks_that_profile_before_pre_connect() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver =
            RecordingDriver::new(FakeDriver::new(DbKind::Postgres)).with_event_trace(trace.clone());
        let driver_handle = Arc::new(driver.clone()) as Arc<dyn DbDriver>;
        let mut unavailable = ConnectionProfile::new("invalid-hooks", DbConfig::default_postgres());
        let mut hooks = ConnectionHooks::default();
        hooks
            .pre_connect
            .push(process_hook(&trace, "pre_connect", false));
        unavailable.hooks = Some(hooks);
        let unavailable_id = unavailable.id;
        let available = ConnectionProfile::new("valid-profile", DbConfig::default_postgres());
        let available_id = available.id;
        let state = test_state_with_profiles_and_errors(
            "postgres",
            driver_handle,
            vec![unavailable, available],
            HashMap::from([(
                unavailable_id,
                "invalid persisted hook configuration".to_string(),
            )]),
            Box::new(NoopSecretStore),
        );

        let error = McpConnectionFactory::new(state.clone())
            .connect(&unavailable_id.to_string(), None)
            .await
            .err()
            .expect("invalid profile must remain unavailable");
        assert_eq!(error, "invalid persisted hook configuration");
        assert!(driver.invocations().is_empty());
        assert!(
            !trace.exists(),
            "PreConnect must not run for invalid profile"
        );

        McpConnectionFactory::new(state)
            .connect(&available_id.to_string(), None)
            .await
            .expect("unrelated valid profile should remain connectable");
        assert_eq!(
            std::fs::read_to_string(trace)
                .expect("driver trace")
                .lines()
                .collect::<Vec<_>>(),
            ["driver"]
        );
    }

    #[tokio::test]
    async fn detached_pre_connect_requires_a_nonempty_ready_signal() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver = RecordingDriver::new(FakeDriver::new(DbKind::Postgres));
        let driver_handle = Arc::new(driver.clone()) as Arc<dyn DbDriver>;

        for ready_signal in [None, Some(String::new())] {
            let mut hook = process_hook(&trace, "pre_connect", false);
            hook.execution_mode = dbflux_core::HookExecutionMode::Detached;
            hook.ready_signal = ready_signal;
            let mut hooks = ConnectionHooks::default();
            hooks.pre_connect.push(hook);
            let profile = pipeline_profile_with_hooks(hooks);
            let connection_id = profile.id.to_string();
            let state = test_state_with_driver("postgres", driver_handle.clone(), profile);

            let error = McpConnectionFactory::new(state)
                .connect(&connection_id, None)
                .await
                .err()
                .expect("detached PreConnect hook without a signal must fail");

            assert!(error.contains("Pre-connect hook #1"));
            assert!(error.contains("ready signal"));
        }

        assert!(driver.invocations().is_empty());
        assert!(
            !trace.exists(),
            "invalid detached hooks must be rejected before spawning"
        );
    }

    #[test]
    fn pre_connect_endpoint_probe_is_limited_to_direct_profiles() {
        let direct = ConnectionProfile::new("direct", DbConfig::default_postgres());
        let direct_context = HookContext::from_profile(&direct);
        assert_eq!(
            pre_connect_probe_endpoint(&direct, &direct_context),
            (direct_context.host.clone(), direct_context.port)
        );

        let pipeline = pipeline_profile_with_hooks(ConnectionHooks::default());
        let pipeline_context = HookContext::from_profile(&pipeline);
        assert_eq!(
            pre_connect_probe_endpoint(&pipeline, &pipeline_context),
            (None, None)
        );

        let mut ssh = direct.clone();
        ssh.config.assign_ssh_tunnel(
            Some(dbflux_core::SshTunnelConfig {
                host: "bastion.example".to_string(),
                port: 22,
                user: "dbflux".to_string(),
                auth_method: Default::default(),
                ssh_config_host: None,
            }),
            None,
        );
        let ssh_context = HookContext::from_profile(&ssh);
        assert_eq!(pre_connect_probe_endpoint(&ssh, &ssh_context), (None, None));
    }

    #[tokio::test]
    async fn detached_pre_connect_pipeline_skips_original_endpoint_probe() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver =
            RecordingDriver::new(FakeDriver::new(DbKind::Postgres)).with_event_trace(trace.clone());
        let driver_handle = Arc::new(driver) as Arc<dyn DbDriver>;
        let mut hook = process_hook(&trace, "pre_connect", false);
        hook.execution_mode = dbflux_core::HookExecutionMode::Detached;
        hook.ready_signal = Some("HOOK_READY".to_string());
        hook.env.insert(
            "DBFLUX_MCP_HOOK_TEST_READY_SIGNAL".to_string(),
            "HOOK_READY".to_string(),
        );
        hook.env.insert(
            "DBFLUX_MCP_HOOK_TEST_SLEEP_MS".to_string(),
            "10000".to_string(),
        );
        let mut hooks = ConnectionHooks::default();
        hooks.pre_connect.push(hook);
        let mut profile = pipeline_profile_with_hooks(hooks);
        profile
            .value_refs
            .insert("host".to_string(), ValueRef::literal("127.0.0.1"));
        if let DbConfig::Postgres { host, .. } = &mut profile.config {
            *host = "invalid host".to_string();
        }
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);
        let connection = McpConnectionFactory::new(state.clone())
            .connect(&connection_id, None)
            .await
            .expect("pipeline connect should not probe the unresolved profile endpoint");
        state
            .connection_cache
            .write()
            .await
            .insert(connection_id.clone(), connection);
        DbFluxServer::disconnect_impl(state, &connection_id)
            .await
            .expect("disconnect should stop the detached hook");

        let events = std::fs::read_to_string(trace).expect("hook and driver events");
        assert!(events.lines().any(|event| event == "pre_connect"));
        assert!(events.lines().any(|event| event == "driver"));
        assert!(!events.lines().any(|event| event == "complete"));
    }

    #[tokio::test]
    async fn detached_process_termination_skips_a_reaped_child() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let profile = ConnectionProfile::new("detached-exit", DbConfig::default_postgres());
        let mut context = HookContext::from_profile(&profile);
        context.phase = Some(HookPhase::PreConnect);
        let mut hook = process_hook(&trace, "pre_connect", false);
        hook.execution_mode = dbflux_core::HookExecutionMode::Detached;
        hook.env.insert(
            "DBFLUX_MCP_HOOK_TEST_SLEEP_MS".to_string(),
            "50".to_string(),
        );

        let (sender, receiver) = dbflux_core::detached_process_channel();
        let cancel_token = CancelToken::new();
        ProcessExecutor
            .execute_hook(&hook, &context, &cancel_token, None, None, Some(&sender))
            .expect("detached hook should spawn");
        let handle = receiver.try_iter().next().expect("detached process handle");
        let process = start_detached_hook_process(handle).expect("start process supervisor");
        let shared = Arc::clone(&process.shared);

        tokio::time::timeout(Duration::from_secs(5), async {
            while !shared.finished.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("supervisor should reap the hook process");

        assert!(!shared.terminated.load(Ordering::SeqCst));
        terminate_shared_detached_process(&shared).expect("reaped process is already stopped");
        assert!(!shared.terminated.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn detached_pre_connect_waits_for_signal_and_endpoint_and_disconnect_stops_process() {
        use std::net::TcpListener;

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver =
            RecordingDriver::new(FakeDriver::new(DbKind::Postgres)).with_event_trace(trace.clone());
        let driver_handle = Arc::new(driver) as Arc<dyn DbDriver>;
        let probe = TcpListener::bind(("127.0.0.1", 0)).expect("reserve endpoint port");
        let port = probe.local_addr().expect("endpoint address").port();
        drop(probe);

        let endpoint = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(250));
            let listener = TcpListener::bind(("127.0.0.1", port)).expect("bind hook endpoint");
            listener.accept().expect("accept readiness probe");
        });

        let mut hook = process_hook(&trace, "pre_connect", false);
        hook.execution_mode = dbflux_core::HookExecutionMode::Detached;
        hook.ready_signal = Some("HOOK_READY".to_string());
        hook.timeout_ms = Some(5_000);
        hook.env.insert(
            "DBFLUX_MCP_HOOK_TEST_READY_SIGNAL".to_string(),
            "HOOK_READY".to_string(),
        );
        hook.env.insert(
            "DBFLUX_MCP_HOOK_TEST_SLEEP_MS".to_string(),
            "1500".to_string(),
        );
        let mut hooks = ConnectionHooks::default();
        hooks.pre_connect.push(hook);
        let mut profile = ConnectionProfile::new("detached-hook", DbConfig::default_postgres());
        profile.hooks = Some(hooks);
        if let DbConfig::Postgres {
            host,
            port: config_port,
            ..
        } = &mut profile.config
        {
            *host = "127.0.0.1".to_string();
            *config_port = port;
        }
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);
        let started = Instant::now();

        let connection = McpConnectionFactory::new(state.clone())
            .connect(&connection_id, None)
            .await
            .expect("hook signal and endpoint should permit connection");

        assert!(started.elapsed() >= Duration::from_millis(200));
        endpoint.join().expect("endpoint listener thread");
        state
            .connection_cache
            .write()
            .await
            .insert(connection_id.clone(), connection);
        DbFluxServer::disconnect_impl(state.clone(), &connection_id)
            .await
            .expect("disconnect should stop detached hook");
        tokio::time::sleep(Duration::from_millis(1_600)).await;

        let events = std::fs::read_to_string(trace).expect("hook events");
        assert!(events.lines().any(|event| event == "pre_connect"));
        assert!(!events.lines().any(|event| event == "complete"));
        assert!(state.detached_processes.connection_ids().is_empty());
    }

    #[tokio::test]
    async fn detached_pre_connect_timeout_names_hook_and_prevents_connection() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver =
            RecordingDriver::new(FakeDriver::new(DbKind::Postgres)).with_event_trace(trace.clone());
        let driver_handle = Arc::new(driver.clone()) as Arc<dyn DbDriver>;
        let mut hook = process_hook(&trace, "pre_connect", false);
        hook.execution_mode = dbflux_core::HookExecutionMode::Detached;
        hook.ready_signal = Some("READY".to_string());
        hook.timeout_ms = Some(50);
        hook.env.insert(
            "DBFLUX_MCP_HOOK_TEST_SLEEP_MS".to_string(),
            "500".to_string(),
        );
        let mut hooks = ConnectionHooks::default();
        hooks.pre_connect.push(hook);
        let profile = pipeline_profile_with_hooks(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        let error = McpConnectionFactory::new(state.clone())
            .connect(&connection_id, None)
            .await
            .err()
            .expect("a missing readiness signal must fail PreConnect");

        assert!(error.contains("Pre-connect hook #1 failed"));
        assert!(error.contains("ready signal"));
        assert!(driver.invocations().is_empty());
        let events = std::fs::read_to_string(trace).expect("hook events");
        assert!(events.lines().any(|event| event == "pre_connect"));
        assert!(!events.lines().any(|event| event == "driver"));
        assert!(!events.lines().any(|event| event == "complete"));
        assert!(state.detached_processes.connection_ids().is_empty());
    }

    #[tokio::test]
    async fn detached_post_connect_process_is_stopped_when_a_later_hook_fails() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver = RecordingDriver::new(FakeDriver::new(DbKind::Postgres));
        let driver_handle = Arc::new(driver.clone()) as Arc<dyn DbDriver>;
        let mut detached_hook = process_hook(&trace, "post_connect", false);
        detached_hook.execution_mode = dbflux_core::HookExecutionMode::Detached;
        detached_hook.env.insert(
            "DBFLUX_MCP_HOOK_TEST_SLEEP_MS".to_string(),
            "1200".to_string(),
        );
        let mut hooks = ConnectionHooks::default();
        hooks.post_connect.push(detached_hook);
        hooks
            .post_connect
            .push(process_hook(&trace, "post_connect", true));
        let profile = pipeline_profile_with_hooks(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        let error = McpConnectionFactory::new(state.clone())
            .connect(&connection_id, None)
            .await
            .err()
            .expect("the later PostConnect hook must fail");
        assert!(error.contains("Post-connect hook #2 failed"));
        assert_eq!(driver.invocations().len(), 1);
        tokio::time::sleep(Duration::from_millis(1_300)).await;

        let events = std::fs::read_to_string(trace).expect("hook events");
        assert!(!events.lines().any(|event| event == "complete"));
        assert!(state.detached_processes.connection_ids().is_empty());
    }

    #[tokio::test]
    async fn detached_pre_connect_process_stops_when_driver_connection_fails() {
        use std::net::TcpListener;

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver = RecordingDriver::new(
            FakeDriver::new(DbKind::Postgres).with_connect_error("expected connection failure"),
        )
        .with_event_trace(trace.clone());
        let driver_handle = Arc::new(driver) as Arc<dyn DbDriver>;
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve endpoint port");
        let port = listener.local_addr().expect("endpoint address").port();
        let endpoint = std::thread::spawn(move || {
            listener.accept().expect("accept readiness probe");
        });

        let mut hook = process_hook(&trace, "pre_connect", false);
        hook.execution_mode = dbflux_core::HookExecutionMode::Detached;
        hook.ready_signal = Some("HOOK_READY".to_string());
        hook.timeout_ms = Some(5_000);
        hook.env.insert(
            "DBFLUX_MCP_HOOK_TEST_READY_SIGNAL".to_string(),
            "HOOK_READY".to_string(),
        );
        hook.env.insert(
            "DBFLUX_MCP_HOOK_TEST_SLEEP_MS".to_string(),
            "1500".to_string(),
        );
        let mut hooks = ConnectionHooks::default();
        hooks.pre_connect.push(hook);
        let mut profile = ConnectionProfile::new("failing-connect", DbConfig::default_postgres());
        profile.hooks = Some(hooks);
        if let DbConfig::Postgres {
            host,
            port: config_port,
            ..
        } = &mut profile.config
        {
            *host = "127.0.0.1".to_string();
            *config_port = port;
        }
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        let error = McpConnectionFactory::new(state.clone())
            .connect(&connection_id, None)
            .await
            .err()
            .expect("driver connection failure should be returned");
        assert!(error.contains("expected connection failure"));

        endpoint.join().expect("endpoint listener thread");
        assert!(state.detached_processes.connection_ids().is_empty());
        tokio::time::sleep(Duration::from_millis(1_700)).await;
        let events = std::fs::read_to_string(trace).expect("hook events");
        assert!(events.lines().any(|event| event == "pre_connect"));
        assert!(events.lines().any(|event| event == "driver"));
        assert!(!events.lines().any(|event| event == "complete"));
    }

    #[tokio::test]
    async fn post_connect_hook_failure_does_not_cache_connection() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver = RecordingDriver::new(FakeDriver::new(DbKind::Postgres));
        let driver_handle = Arc::new(driver.clone()) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        hooks
            .post_connect
            .push(process_hook(&trace, "post_connect", true));
        let profile = pipeline_profile_with_hooks(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        for expected_attempt in 1..=2 {
            let error = DbFluxServer::get_or_connect(state.clone(), &connection_id)
                .await
                .err()
                .expect("failed PostConnect hook must reject connection");
            assert!(error.to_ascii_lowercase().contains("post-connect"));
            assert!(
                state
                    .connection_cache
                    .read()
                    .await
                    .get(&connection_id)
                    .is_none()
            );
            assert_eq!(driver.invocations().len(), expected_attempt);
        }
    }

    #[tokio::test]
    async fn disconnect_runs_post_hook_and_removes_cache_after_pre_hook_failure() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver =
            RecordingDriver::new(FakeDriver::new(DbKind::Postgres)).with_event_trace(trace.clone());
        let driver_handle = Arc::new(driver) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        hooks
            .pre_disconnect
            .push(process_hook(&trace, "pre_disconnect", true));
        hooks
            .post_disconnect
            .push(process_hook(&trace, "post_disconnect", false));
        let profile = pipeline_profile_with_hooks(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);
        let connection = McpConnectionFactory::new(state.clone())
            .connect(&connection_id, None)
            .await
            .expect("connection should succeed");
        state
            .connection_cache
            .write()
            .await
            .insert(connection_id.clone(), connection);

        let error = DbFluxServer::disconnect_impl(state.clone(), &connection_id)
            .await
            .expect_err("PreDisconnect failure should be reported after teardown");

        assert!(error.contains("Pre-disconnect hook #1 failed"));
        assert!(
            state
                .connection_cache
                .read()
                .await
                .get(&connection_id)
                .is_none()
        );
        let events = std::fs::read_to_string(trace).expect("hook and driver events");
        assert_eq!(
            events.lines().collect::<Vec<_>>(),
            ["driver", "pre_disconnect", "post_disconnect"]
        );

        let runtime = state.runtime.read().await;
        let filter = dbflux_audit::query::AuditQueryFilter {
            category: Some("hook".to_string()),
            object_type: Some("connection_hook".to_string()),
            ..Default::default()
        };
        let audit_events = runtime
            .audit_service()
            .query_extended(&filter)
            .expect("disconnect hook audit events");
        assert_eq!(audit_events.len(), 2);
        assert!(audit_events.iter().any(|event| {
            event.action.as_deref() == Some("hook_execute_failed")
                && event.outcome.as_deref() == Some("failure")
        }));
        assert!(audit_events.iter().any(|event| {
            event.action.as_deref() == Some("hook_execute")
                && event.outcome.as_deref() == Some("success")
        }));
    }

    #[tokio::test]
    async fn disconnect_runs_hooks_for_each_database_variant() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver = Arc::new(FakeDriver::new(DbKind::Postgres)) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        let mut pre_disconnect = process_hook(&trace, "pre_disconnect", false);
        pre_disconnect.env.insert(
            "DBFLUX_MCP_HOOK_TEST_RECORD_DATABASE".to_string(),
            "1".to_string(),
        );
        hooks.pre_disconnect.push(pre_disconnect);
        let mut post_disconnect = process_hook(&trace, "post_disconnect", false);
        post_disconnect.env.insert(
            "DBFLUX_MCP_HOOK_TEST_RECORD_DATABASE".to_string(),
            "1".to_string(),
        );
        hooks.post_disconnect.push(post_disconnect);
        let profile = pipeline_profile_with_hooks(hooks);
        let profile_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver, profile);

        for database in ["analytics", "reporting"] {
            let connection = McpConnectionFactory::new(state.clone())
                .connect(&profile_id, Some(database))
                .await
                .expect("database connection should succeed");
            state
                .connection_cache
                .write()
                .await
                .insert(format!("{profile_id}:{database}"), connection);
        }

        DbFluxServer::disconnect_impl(state.clone(), &profile_id)
            .await
            .expect("both database variants should disconnect");

        let events = std::fs::read_to_string(trace).expect("disconnect hook events");
        assert_eq!(
            events.lines().collect::<Vec<_>>(),
            [
                "pre_disconnect:analytics",
                "pre_disconnect:reporting",
                "post_disconnect:analytics",
                "post_disconnect:reporting"
            ]
        );
        assert!(
            state
                .connection_cache
                .read()
                .await
                .connection_ids()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn connection_setup_locks_are_scoped_per_profile() {
        let driver = Arc::new(FakeDriver::new(DbKind::Postgres)) as Arc<dyn DbDriver>;
        let profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        let state = test_state_with_driver("postgres", driver, profile);

        let first = profile_connection_setup_lock(&state, "profile-a").await;
        let same_profile = profile_connection_setup_lock(&state, "profile-a").await;
        let other_profile = profile_connection_setup_lock(&state, "profile-b").await;

        assert!(Arc::ptr_eq(&first, &same_profile));
        assert!(!Arc::ptr_eq(&first, &other_profile));
    }

    #[tokio::test]
    async fn server_shutdown_disconnects_every_cached_connection() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver =
            RecordingDriver::new(FakeDriver::new(DbKind::Postgres)).with_event_trace(trace.clone());
        let driver_handle = Arc::new(driver) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        hooks
            .pre_disconnect
            .push(process_hook(&trace, "pre_disconnect", false));
        hooks
            .post_disconnect
            .push(process_hook(&trace, "post_disconnect", false));
        let mut profile = ConnectionProfile::new("shutdown-hooked", DbConfig::default_postgres());
        profile.hooks = Some(hooks);
        let profile_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);
        let connection = McpConnectionFactory::new(state.clone())
            .connect(&profile_id, None)
            .await
            .expect("connection should succeed");
        state
            .connection_cache
            .write()
            .await
            .insert(profile_id.clone(), connection);

        DbFluxServer::disconnect_all(state.clone())
            .await
            .expect("shutdown should drain cached connections");

        assert!(
            state
                .connection_cache
                .read()
                .await
                .connection_ids()
                .is_empty()
        );
        let events = std::fs::read_to_string(trace).expect("shutdown hook events");
        assert_eq!(
            events.lines().collect::<Vec<_>>(),
            ["driver", "pre_disconnect", "post_disconnect"]
        );
    }

    #[tokio::test]
    async fn shutdown_deadline_cancels_blocking_hooks_and_terminates_detached_processes() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let trace = temp_dir.path().join("hook-events.log");
        let driver = Arc::new(FakeDriver::new(DbKind::Postgres)) as Arc<dyn DbDriver>;
        let mut hooks = ConnectionHooks::default();
        let mut blocking_hook = process_hook(&trace, "pre_disconnect", false);
        blocking_hook.timeout_ms = None;
        blocking_hook.env.insert(
            "DBFLUX_MCP_HOOK_TEST_SLEEP_MS".to_string(),
            "60000".to_string(),
        );
        hooks.pre_disconnect.push(blocking_hook);
        let mut profile = ConnectionProfile::new("shutdown-deadline", DbConfig::default_postgres());
        profile.hooks = Some(hooks);
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver, profile.clone());
        let connection = McpConnectionFactory::new(state.clone())
            .connect(&connection_id, None)
            .await
            .expect("connection should succeed");
        state
            .connection_cache
            .write()
            .await
            .insert(connection_id.clone(), connection);

        let mut detached_hook = process_hook(&trace, "detached", false);
        detached_hook.execution_mode = dbflux_core::HookExecutionMode::Detached;
        detached_hook.timeout_ms = None;
        detached_hook.env.insert(
            "DBFLUX_MCP_HOOK_TEST_SLEEP_MS".to_string(),
            "60000".to_string(),
        );
        let (detached_sender, detached_receiver) = dbflux_core::detached_process_channel();
        ProcessExecutor
            .execute_hook(
                &detached_hook,
                &HookContext::from_profile(&profile),
                &CancelToken::new(),
                None,
                None,
                Some(&detached_sender),
            )
            .expect("detached hook should spawn");
        let handle = detached_receiver
            .try_iter()
            .next()
            .expect("detached hook process handle");
        let shared = Arc::new(SharedDetachedProcess::new(handle));
        state
            .detached_processes
            .register(&connection_id, vec![Arc::clone(&shared)]);

        let startup_deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let started = std::fs::read_to_string(&trace)
                .map(|events| events.lines().any(|event| event == "detached"))
                .unwrap_or(false);
            if started {
                break;
            }
            assert!(
                Instant::now() < startup_deadline,
                "detached hook did not record its startup event"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let shutdown_timeout = Duration::from_secs(1);
        let started = Instant::now();
        let result =
            DbFluxServer::disconnect_all_with_timeout(state.clone(), shutdown_timeout).await;

        let error = result.expect_err("shutdown should report its deadline");
        assert!(error.contains("shutdown drain exceeded"));
        assert!(started.elapsed() < shutdown_timeout + Duration::from_millis(500));
        assert!(shared.terminated.load(Ordering::SeqCst));
        assert!(state.detached_processes.connection_ids().is_empty());
        assert!(
            state
                .connection_cache
                .read()
                .await
                .connection_ids()
                .is_empty()
        );
        let events = std::fs::read_to_string(trace).expect("hook events");
        assert!(events.lines().any(|event| event == "pre_disconnect"));
        assert!(!events.lines().any(|event| event == "complete"));
    }

    #[tokio::test]
    async fn server_handler_exposes_tools_from_all_sub_routers() {
        let driver = Arc::new(FakeDriver::new(DbKind::Postgres)) as Arc<dyn DbDriver>;
        let profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        let state = test_state_with_driver("postgres", driver, profile);

        let server = DbFluxServer::new(state);
        let tools = server.tool_router.list_all();

        assert!(!tools.is_empty(), "combined tool router must not be empty");
        for tool in tools {
            assert!(
                ServerHandler::get_tool(&server, &tool.name).is_some(),
                "tool '{}' must be exposed through the ServerHandler",
                tool.name
            );
        }
    }

    #[tokio::test]
    async fn mcp_connection_factory_uses_pipeline_resolved_values_and_database_override() {
        let driver = RecordingDriver::new(FakeDriver::new(DbKind::Postgres));
        let driver_handle = Arc::new(driver.clone()) as Arc<dyn DbDriver>;

        let mut profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        profile.value_refs.insert(
            "host".to_string(),
            ValueRef::literal("pipeline.example.internal"),
        );
        profile.value_refs.insert(
            "password".to_string(),
            ValueRef::literal("pipeline-password"),
        );

        let connection_id = profile.id.to_string();
        let state = test_state_with_driver("postgres", driver_handle, profile);

        let connection = McpConnectionFactory::new(state)
            .connect(&connection_id, Some("analytics"))
            .await
            .expect("pipeline-backed connection should succeed");

        let invocations = driver.invocations();
        assert_eq!(invocations.len(), 1, "expected a single driver connection");

        let invocation = &invocations[0];
        match &invocation.profile.config {
            DbConfig::Postgres { host, database, .. } => {
                assert_eq!(host, "pipeline.example.internal");
                assert_eq!(database, "analytics");
            }
            other => panic!("expected postgres config, got {other:?}"),
        }

        assert_eq!(
            invocation.password.as_deref(),
            Some("pipeline-password"),
            "pipeline should pass resolved password as an override"
        );
        assert_eq!(
            connection.connection().active_database().as_deref(),
            Some("analytics")
        );
    }

    #[tokio::test]
    async fn mcp_connection_factory_falls_back_to_keyring_password_on_pipeline_path() {
        let driver = RecordingDriver::new(FakeDriver::new(DbKind::Postgres));
        let driver_handle = Arc::new(driver.clone()) as Arc<dyn DbDriver>;

        // A pipeline profile whose only ValueRef is the host: the pipeline resolves
        // no password, so the keyring value is the only credential available.
        let mut profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        profile.save_password = true;
        profile.value_refs.insert(
            "host".to_string(),
            ValueRef::literal("pipeline.example.internal"),
        );
        assert!(profile.uses_pipeline());

        let store = InMemorySecretStore::default();
        dbflux_core::SecretStore::set(
            &store,
            &profile.secret_ref(),
            &SecretString::from("keyring-password".to_string()),
        )
        .expect("in-memory secret store write should succeed");

        let connection_id = profile.id.to_string();
        let state =
            test_state_with_driver_and_store("postgres", driver_handle, profile, Box::new(store));

        McpConnectionFactory::new(state)
            .connect(&connection_id, None)
            .await
            .expect("pipeline-backed connection should succeed");

        let invocations = driver.invocations();
        assert_eq!(invocations.len(), 1, "expected a single driver connection");
        assert_eq!(
            invocations[0].password.as_deref(),
            Some("keyring-password"),
            "pipeline path must fall back to the keyring password when the \
             pipeline resolves none"
        );
    }

    fn relational_schema_with_databases(databases: &[(&str, bool)]) -> dbflux_core::SchemaSnapshot {
        dbflux_core::SchemaSnapshot::relational(dbflux_core::RelationalSchema {
            databases: databases
                .iter()
                .map(|(name, is_current)| dbflux_core::DatabaseInfo {
                    name: (*name).to_string(),
                    is_current: *is_current,
                })
                .collect(),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn connect_reports_the_active_database_and_the_database_names() {
        let driver =
            FakeDriver::new(DbKind::Postgres).with_schema(relational_schema_with_databases(&[
                ("postgres", false),
                ("analytics", true),
            ]));
        let profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver(&profile.driver_id(), Arc::new(driver), profile);

        let response = DbFluxServer::connect_impl(state, &connection_id)
            .await
            .expect("connect should succeed");

        assert_eq!(response["success"], serde_json::json!(true));
        assert_eq!(
            response["message"],
            serde_json::json!(format!("Connected to {connection_id}"))
        );
        assert_eq!(
            response["current_database"],
            serde_json::json!("postgres"),
            "the connection's active database wins over the listing's is_current flag"
        );
        assert_eq!(
            response["databases"],
            serde_json::json!(["postgres", "analytics"])
        );
    }

    #[tokio::test]
    async fn connect_falls_back_to_the_listed_current_database() {
        let driver =
            FakeDriver::new(DbKind::MySQL).with_schema(relational_schema_with_databases(&[
                ("information_schema", false),
                ("app", true),
            ]));
        let profile = ConnectionProfile::new("test-mysql", DbConfig::default_mysql());
        assert_eq!(
            profile.config.database(),
            None,
            "the profile must name no database for the fallback to be exercised"
        );

        let connection_id = profile.id.to_string();
        let state = test_state_with_driver(&profile.driver_id(), Arc::new(driver), profile);

        let connection = DbFluxServer::get_or_connect(state.clone(), &connection_id)
            .await
            .expect("connect should succeed");
        assert_eq!(connection.active_database(), None);

        let response = DbFluxServer::connect_impl(state, &connection_id)
            .await
            .expect("connect should succeed");

        assert_eq!(response["current_database"], serde_json::json!("app"));
        assert_eq!(
            response["databases"],
            serde_json::json!(["information_schema", "app"])
        );
    }

    fn relational_schema_with_table_and_view() -> dbflux_core::SchemaSnapshot {
        let mut snapshot =
            dbflux_test_support::fixtures::relational_schema_with_table("app", "public", "users");

        if let dbflux_core::DataStructure::Relational(relational) = &mut snapshot.structure
            && let Some(schema) = relational.schemas.first_mut()
        {
            schema.views.push(dbflux_core::ViewInfo {
                name: "active_users".to_string(),
                schema: Some("public".to_string()),
            });
        }

        snapshot
    }

    #[tokio::test]
    async fn list_tables_returns_one_object_per_table_and_view_by_default() {
        let driver =
            FakeDriver::new(DbKind::Postgres).with_schema(relational_schema_with_table_and_view());
        let profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver(&profile.driver_id(), Arc::new(driver), profile);

        let response = DbFluxServer::list_tables_impl(state, &connection_id, None, None, false)
            .await
            .expect("list_tables should succeed");

        assert_eq!(
            response,
            serde_json::json!({
                "tables": [
                    { "name": "users", "schema": "public", "kind": "Table" },
                    { "name": "active_users", "schema": "public", "kind": "View" },
                ]
            })
        );
    }

    #[tokio::test]
    async fn list_tables_names_only_returns_table_and_view_names() {
        let driver =
            FakeDriver::new(DbKind::Postgres).with_schema(relational_schema_with_table_and_view());
        let profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver(&profile.driver_id(), Arc::new(driver), profile);

        let response = DbFluxServer::list_tables_impl(state, &connection_id, None, None, true)
            .await
            .expect("list_tables should succeed");

        assert_eq!(
            response,
            serde_json::json!({ "tables": ["users", "active_users"] })
        );
    }

    fn key_value_schema_with_keyspaces() -> dbflux_core::SchemaSnapshot {
        let keyspace = |db_index, key_count| dbflux_core::KeySpaceInfo {
            db_index,
            key_count,
            memory_bytes: None,
            avg_ttl_seconds: None,
        };

        dbflux_core::SchemaSnapshot::key_value(dbflux_core::KeyValueSchema {
            keyspaces: vec![keyspace(0, Some(12)), keyspace(3, None)],
            current_keyspace: Some(0),
        })
    }

    #[tokio::test]
    async fn list_tables_returns_one_object_per_keyspace_by_default() {
        let driver = FakeDriver::new(DbKind::Redis).with_schema(key_value_schema_with_keyspaces());
        let profile = ConnectionProfile::new("test-redis", DbConfig::default_redis());
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver(&profile.driver_id(), Arc::new(driver), profile);

        let response = DbFluxServer::list_tables_impl(state, &connection_id, None, None, false)
            .await
            .expect("list_tables should succeed");

        assert_eq!(
            response,
            serde_json::json!({
                "tables": [
                    { "db_index": 0, "key_count": 12, "kind": "Keyspace" },
                    { "db_index": 3, "key_count": null, "kind": "Keyspace" },
                ]
            })
        );
    }

    #[tokio::test]
    async fn list_tables_names_only_names_keyspaces_like_the_rest_of_the_app() {
        let driver = FakeDriver::new(DbKind::Redis).with_schema(key_value_schema_with_keyspaces());
        let profile = ConnectionProfile::new("test-redis", DbConfig::default_redis());
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver(&profile.driver_id(), Arc::new(driver), profile);

        let response = DbFluxServer::list_tables_impl(state, &connection_id, None, None, true)
            .await
            .expect("list_tables should succeed");

        assert_eq!(response, serde_json::json!({ "tables": ["db0", "db3"] }));
    }

    #[tokio::test]
    async fn connect_omits_the_database_context_without_a_database_concept() {
        let driver = FakeDriver::new(DbKind::DynamoDB);
        let profile = ConnectionProfile::new("test-dynamodb", DbConfig::default_dynamodb());
        let connection_id = profile.id.to_string();
        let state = test_state_with_driver(&profile.driver_id(), Arc::new(driver), profile);

        let response = DbFluxServer::connect_impl(state, &connection_id)
            .await
            .expect("connect should succeed");

        let fields = response
            .as_object()
            .expect("connect should return a JSON object");

        assert_eq!(response["success"], serde_json::json!(true));
        assert!(!fields.contains_key("current_database"), "got {response}");
        assert!(!fields.contains_key("databases"), "got {response}");
    }
}
