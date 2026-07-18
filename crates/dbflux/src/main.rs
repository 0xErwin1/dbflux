#![windows_subsystem = "windows"]
#![recursion_limit = "256"]

mod cli;

use dbflux_app::mcp_command::run_mcp_command;
use dbflux_audit::AuditService;
use dbflux_core::ShutdownPhase;
use dbflux_core::observability::actions::{SYSTEM_SHUTDOWN, SYSTEM_STARTUP};
use dbflux_core::observability::tracing_bridge::{
    BridgeConfig, BridgeHandle, FmtWriter, ShutdownError,
};
use dbflux_core::observability::{EventCategory, EventOutcome, EventRecord, EventSeverity};
use dbflux_driver_ipc::shutdown_managed_hosts;
use dbflux_ipc::{
    APP_CONTROL_VERSION, framing, init_process_auth_tokens,
    protocol::{AppControlRequest, AppControlResponse, IpcMessage, IpcResponse},
    read_app_control_token, shutdown_managed_auth_provider_hosts, socket_name,
};
use dbflux_ui::AppStateEntity;
use dbflux_ui::assets::Assets;
use dbflux_ui::ipc_server::IpcServer;
use dbflux_ui::keymap::{input_context_keybindings, workspace_keybindings};
use dbflux_ui::platform;
use dbflux_ui::ui::overlays::command_palette::command_palette_keybindings;
use dbflux_ui::ui::views::workspace::Workspace;
use gpui::*;
use gpui_component::Root;
use interprocess::local_socket::{
    Listener as IpcListener, ListenerNonblockingMode, ListenerOptions, Stream as IpcStream,
    prelude::*,
};
use log::info;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Global holder for the audit service, used by the panic hook.
static AUDIT_SERVICE_FOR_PANIC: Mutex<Option<AuditService>> = Mutex::new(None);

/// Global holder for the tracing bridge handle.
///
/// Kept here so the shutdown sequence can call `BridgeHandle::shutdown()` even
/// though `BridgeHandle` is created in `run_gui` before the GPUI closure runs.
static BRIDGE_HANDLE: Mutex<Option<BridgeHandle>> = Mutex::new(None);

/// Previous panic hook, chained after our hook.
#[allow(clippy::type_complexity)]
static PREV_PANIC_HOOK: Mutex<Option<Box<dyn Fn(&std::panic::PanicHookInfo) + Send + Sync>>> =
    Mutex::new(None);

const TASK_CANCEL_TIMEOUT: Duration = Duration::from_millis(2000);
const CONNECTION_CLOSE_TIMEOUT: Duration = Duration::from_millis(3000);
const TOTAL_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(10000);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Cadence for observing `SHUTDOWN_SIGNAL_RECEIVED`. Deliberately coarser than
/// `POLL_INTERVAL`: this timer lives for the whole process lifetime, so it is
/// traded against idle wakeups. The added latency is imperceptible to a user
/// pressing Ctrl+C.
#[cfg(unix)]
const SIGNAL_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Set by `handle_shutdown_signal` when SIGINT or SIGTERM arrives; polled on
/// the GPUI foreground thread so the same graceful-shutdown path used by
/// window close also runs for terminal signals.
#[cfg(unix)]
static SHUTDOWN_SIGNAL_RECEIVED: AtomicBool = AtomicBool::new(false);

/// Signal handler for SIGINT and SIGTERM.
///
/// This runs in an async-signal-unsafe context (arbitrary interrupted code,
/// possibly mid-allocation or mid-lock), so it must only perform
/// async-signal-safe operations. Setting an `AtomicBool` is safe; anything
/// else (logging, allocating, taking locks) is not. The actual shutdown work
/// happens later, on the GPUI foreground thread, once the flag is observed.
#[cfg(unix)]
extern "C" fn handle_shutdown_signal(_signum: std::ffi::c_int) {
    SHUTDOWN_SIGNAL_RECEIVED.store(true, Ordering::SeqCst);
}

/// Registers `handle_shutdown_signal` for SIGINT and SIGTERM via `sigaction`.
///
/// `SA_RESTART` is required: without it, installing these handlers would make
/// blocking syscalls elsewhere in the process (IPC accept/read, driver I/O)
/// fail with `EINTR` on every delivery.
#[cfg(unix)]
fn install_shutdown_signal_handlers() {
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = handle_shutdown_signal as *const () as usize;
    action.sa_flags = libc::SA_RESTART;
    unsafe {
        libc::sigemptyset(&mut action.sa_mask);
    }

    for (signum, name) in [(libc::SIGINT, "SIGINT"), (libc::SIGTERM, "SIGTERM")] {
        let result = unsafe { libc::sigaction(signum, &action, std::ptr::null_mut()) };

        if result != 0 {
            log::warn!(
                "Failed to install {name} handler, graceful shutdown on {name} unavailable: {}",
                io::Error::last_os_error()
            );
        }
    }
}

#[cfg(not(unix))]
fn install_shutdown_signal_handlers() {}

/// Installs a chained best-effort panic hook that:
/// 1. Attempts to record the panic via AuditService::record_panic_best_effort
/// 2. Falls back to stderr logging if the service is unavailable or fails
/// 3. Always delegates to the previously installed panic hook
fn install_panic_hook() {
    let prev = std::panic::take_hook();
    *PREV_PANIC_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(prev));

    std::panic::set_hook(Box::new(|panic_info: &std::panic::PanicHookInfo| {
        let audit_guard = AUDIT_SERVICE_FOR_PANIC
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(audit_service) = audit_guard.clone() {
            let panic_location = panic_info
                .location()
                .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
                .unwrap_or_else(|| "unknown location".to_string());

            let panic_message = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown panic payload".to_string()
            };

            let panic_info_str = format!("{} at {}", panic_message, panic_location);

            match audit_service.record_panic_best_effort(&panic_info_str) {
                Some(_) => {}
                None => {
                    let _ = std::io::stderr().write_all(
                        b"[dbflux_audit] panic hook: record_panic_best_effort returned None\n",
                    );
                }
            }
        } else {
            let _ = std::io::stderr()
                .write_all(b"[dbflux_audit] panic hook: audit service not available\n");
        }

        drop(audit_guard);

        let prev_guard = PREV_PANIC_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(ref prev_hook) = *prev_guard {
            prev_hook(panic_info);
        }
    }));
}

/// Emits a system startup audit event via the provided audit service.
fn emit_system_startup(audit_service: &AuditService) {
    let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
    let event = EventRecord::new(
        now_ms,
        EventSeverity::Info,
        EventCategory::System,
        EventOutcome::Success,
    )
    .with_typed_action(SYSTEM_STARTUP)
    .with_summary("DBFlux application started")
    .with_actor_id("system");

    if let Err(e) = audit_service.record(event) {
        log::warn!("Failed to record system_startup audit event: {}", e);
    }
}

/// Emits a system shutdown audit event via the provided audit service.
fn emit_system_shutdown(audit_service: &AuditService) {
    let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
    let event = EventRecord::new(
        now_ms,
        EventSeverity::Info,
        EventCategory::System,
        EventOutcome::Success,
    )
    .with_typed_action(SYSTEM_SHUTDOWN)
    .with_summary("DBFlux application initiating shutdown")
    .with_actor_id("system");

    if let Err(e) = audit_service.record(event) {
        log::warn!("Failed to record system_shutdown audit event: {}", e);
    }
}

/// Installs a process-wide rustls crypto provider.
///
/// rustls 0.23 only auto-selects a provider when exactly one backend is
/// compiled in. Several are linked here (the AWS SDK enables `ring`, reqwest
/// and the TLS drivers enable `aws-lc-rs`), so any consumer that builds a
/// rustls client via the auto path — notably the `mysql` driver — would panic
/// with "Could not automatically determine the process-level CryptoProvider".
/// Installing one explicitly, before any handshake, resolves that for every
/// consumer that relies on the process default.
fn install_default_crypto_provider() {
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        log::debug!("rustls crypto provider was already installed");
    }
}

fn main() {
    install_panic_hook();
    install_default_crypto_provider();

    let args: Vec<String> = std::env::args().collect();

    if args.get(1).map(|s| s.as_str()) == Some("mcp") {
        let exit_code = run_mcp_command(&args[2..]);
        std::process::exit(exit_code);
    }

    if args.get(1).map(|s| s.as_str()) == Some("--gui") {
        run_gui();
        return;
    }

    if args.len() == 1 {
        if let Ok(name) = socket_name()
            && let Ok(mut stream) = IpcStream::connect(name)
            && send_focus_request(&mut stream, 1).is_ok()
        {
            return;
        }

        run_gui();
        return;
    }

    std::process::exit(cli::run(&args));
}

fn bind_ipc_socket() -> Result<IpcListener, ()> {
    let connect_name = socket_name().map_err(|e| {
        eprintln!("Failed to create socket name: {}", e);
    })?;

    if let Ok(mut stream) = IpcStream::connect(connect_name)
        && send_focus_request(&mut stream, 1).is_ok()
    {
        std::process::exit(0);
    }

    let bind_name = socket_name().map_err(|e| {
        eprintln!("Failed to create socket name: {}", e);
    })?;

    ListenerOptions::new()
        .name(bind_name)
        .nonblocking(ListenerNonblockingMode::Accept)
        .try_overwrite(true)
        .create_sync()
        .map_err(|e| {
            eprintln!("Failed to bind IPC socket: {}", e);
        })
}

fn send_focus_request<S: Read + Write>(stream: &mut S, request_id: u64) -> io::Result<()> {
    let auth_token = read_app_control_token()?;
    let request = AppControlRequest::new(request_id, Some(auth_token), IpcMessage::Focus);
    framing::send_msg(&mut *stream, &request)?;

    let response: AppControlResponse = framing::recv_msg(&mut *stream)?;

    if !response
        .protocol_version
        .is_compatible_with(APP_CONTROL_VERSION)
    {
        return Err(io::Error::other(
            "incompatible app-control protocol version",
        ));
    }

    if response.request_id != request_id {
        return Err(io::Error::other("mismatched app-control response id"));
    }

    match response.body {
        IpcResponse::Error { message } => Err(io::Error::other(message)),
        _ => Ok(()),
    }
}

fn run_gui() {
    let fmt_writer = if let Some(path) = std::env::var_os("DBFLUX_LOG_FILE").map(PathBuf::from) {
        FmtWriter::NonBlockingFile(path)
    } else {
        FmtWriter::Stderr
    };

    let bridge_config = BridgeConfig {
        include_audit_layer: true,
        fmt_writer,
        env_filter_default: "info,hyper=warn,tokio=warn",
        ..BridgeConfig::default()
    };

    match dbflux_core::observability::tracing_bridge::init_tracing(bridge_config) {
        Ok(handle) => {
            *BRIDGE_HANDLE.lock().unwrap() = Some(handle);
        }
        Err(err) => {
            eprintln!("Failed to initialize tracing: {err}");
        }
    }

    let auth_token = match init_process_auth_tokens() {
        Ok(token) => token,
        Err(error) => {
            eprintln!("Failed to initialize IPC auth token: {}", error);
            std::process::exit(1);
        }
    };

    let listener = match bind_ipc_socket() {
        Ok(l) => l,
        Err(()) => std::process::exit(1),
    };

    info!("IPC socket bound successfully");

    Application::new().with_assets(Assets).run(|cx: &mut App| {
        dbflux_ui::theme::init(cx);
        dbflux_ui::ui::components::data_table::init(cx);
        dbflux_ui::ui::components::document_tree::init(cx);

        let app_state_inner = match AppStateEntity::new() {
            Ok(state) => state,
            Err(e) => {
                eprintln!(
                    "DBFlux: failed to initialize storage — cannot open database: {e}\n\
                     Check that ~/.local/share/dbflux is accessible and not corrupted."
                );
                cx.quit();
                return;
            }
        };
        let app_state = cx.new(|_cx| app_state_inner);

        // Wire the bridge into the audit service before cloning it out.
        // `attach_tracing_bridge` must be called on the owned `AppState`
        // because `AuditService.bridge_min_level` is not shared across clones.
        let persisted_min_level = app_state.read(cx).log_capture_min_level_setting();
        if let Some(handle) = BRIDGE_HANDLE.lock().unwrap().as_ref() {
            app_state.update(cx, |state, _| {
                state.attach_tracing_bridge(handle.min_level.clone(), handle.drop_counter.clone());
            });

            let seeded_level =
                dbflux_core::observability::EventSeverity::from_str_repr(&persisted_min_level)
                    .unwrap_or(dbflux_core::observability::EventSeverity::Info);
            handle.set_min_level(seeded_level);

            let audit_service_arc = Arc::new(app_state.read(cx).audit_service().clone());
            if let Err(err) = handle.install_sink(audit_service_arc) {
                log::warn!("Failed to install audit bridge sink: {err}");
            }
        }

        let audit_service = app_state.read(cx).audit_service().clone();
        *AUDIT_SERVICE_FOR_PANIC.lock().unwrap() = Some(audit_service.clone());

        emit_system_startup(&audit_service);

        let general_settings = app_state.read(cx).general_settings().clone();
        let theme_setting = general_settings.theme;
        let style_setting = general_settings.style;

        // Set up the density global and apply the persisted theme+style so
        // radius tokens are correct from the very first frame.
        dbflux_ui::theme::init_with_settings(theme_setting, style_setting, cx);

        let channel = dbflux_core::ReleaseChannel::current();
        let mut main_window_options = WindowOptions {
            app_id: Some(channel.app_id().into()),
            titlebar: Some(TitlebarOptions {
                title: Some(channel.display_name().into()),
                ..Default::default()
            }),
            // Request client-side decorations on Linux to enable native Wayland support.
            // On other platforms this returns Server explicitly.
            window_decorations: platform::main_window_decoration_request(),
            ..Default::default()
        };
        platform::apply_window_options(&mut main_window_options, 800.0, 600.0);

        let window_handle = cx
            .open_window(main_window_options, |window, cx| {
                cx.bind_keys(command_palette_keybindings());
                cx.bind_keys(input_context_keybindings());
                cx.bind_keys(workspace_keybindings());

                let workspace = cx.new(|cx| Workspace::new(app_state.clone(), window, cx));

                IpcServer::start_with_listener(
                    listener,
                    workspace.clone(),
                    window.window_handle(),
                    auth_token,
                    cx,
                );
                info!("IPC server started");

                cx.new(|cx| Root::new(workspace, window, cx))
            })
            .expect("Failed to open main window");

        let app_state_for_close = app_state.clone();
        window_handle
            .update(cx, |_root, window, cx| {
                window.on_window_should_close(cx, move |_window, cx| {
                    let already_shutting_down = app_state_for_close.read(cx).is_shutting_down();
                    if already_shutting_down {
                        let phase = app_state_for_close.read(cx).shutdown_phase();
                        if matches!(phase, ShutdownPhase::Complete | ShutdownPhase::Failed) {
                            return true;
                        }
                        return false;
                    }

                    initiate_graceful_shutdown(&app_state_for_close, cx);

                    false
                });
            })
            .unwrap_or_else(|error| {
                log::warn!("Failed to install window close handler: {:?}", error);
            });

        install_shutdown_signal_handlers();

        #[cfg(unix)]
        {
            let app_state_for_signal = app_state.clone();
            cx.spawn(async move |cx| {
                loop {
                    cx.background_executor().timer(SIGNAL_POLL_INTERVAL).await;

                    if !SHUTDOWN_SIGNAL_RECEIVED.load(Ordering::SeqCst) {
                        continue;
                    }

                    info!("Received shutdown signal from terminal");

                    if let Err(error) = cx.update(|cx| {
                        initiate_graceful_shutdown(&app_state_for_signal, cx);
                    }) {
                        log::warn!("Failed to start shutdown from signal: {:?}", error);
                    }

                    break;
                }
            })
            .detach();
        }
    });
}

/// Single entry point for graceful shutdown, reached from both window close
/// and OS signals (SIGINT/SIGTERM). Marks shutdown as begun, records the
/// audit event, and spawns the async shutdown sequence.
fn initiate_graceful_shutdown(app_state: &Entity<AppStateEntity>, cx: &mut App) {
    info!("Starting graceful shutdown...");
    let initiated_shutdown = app_state.update(cx, |state, _| state.begin_shutdown());

    if initiated_shutdown {
        let audit_service = app_state.read(cx).audit_service().clone();
        emit_system_shutdown(&audit_service);

        let app_state_shutdown = app_state.clone();
        cx.spawn(async move |cx| {
            run_shutdown_sequence(app_state_shutdown, cx).await;
        })
        .detach();
    }
}

async fn run_shutdown_sequence(app_state: Entity<AppStateEntity>, cx: &mut AsyncApp) {
    let start = Instant::now();

    info!("Shutdown phase: Cancelling tasks...");
    let task_cancel_result = cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state.cancel_all_tasks();
        });
    });

    if task_cancel_result.is_err() {
        log::error!("Failed to cancel tasks during shutdown");
    }

    let task_deadline = Instant::now() + TASK_CANCEL_TIMEOUT;
    loop {
        if start.elapsed() > TOTAL_SHUTDOWN_TIMEOUT {
            log::error!("Shutdown exceeded total timeout, forcing quit");
            let stopped = shutdown_managed_hosts();
            if stopped > 0 {
                info!("Stopped {} managed RPC host process(es)", stopped);
            }
            let auth_stopped = shutdown_managed_auth_provider_hosts();
            if auth_stopped > 0 {
                info!(
                    "Stopped {} managed auth-provider host process(es)",
                    auth_stopped
                );
            }
            let _ = cx.update(|cx| cx.quit());
            return;
        }

        let still_running = cx
            .update(|cx| app_state.read(cx).has_running_tasks())
            .unwrap_or(false);

        if !still_running {
            info!("All tasks finished");
            break;
        }

        if Instant::now() > task_deadline {
            log::warn!("Task cancellation timed out, proceeding with running tasks");
            break;
        }

        cx.background_executor().timer(POLL_INTERVAL).await;
    }

    info!("Shutdown phase: Closing connections...");
    let close_result = cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state.close_all_connections();
        });
    });

    if close_result.is_err() {
        log::error!("Failed to close connections during shutdown");
    }

    let conn_deadline = Instant::now() + CONNECTION_CLOSE_TIMEOUT;
    loop {
        if start.elapsed() > TOTAL_SHUTDOWN_TIMEOUT {
            log::error!("Shutdown exceeded total timeout, forcing quit");
            let stopped = shutdown_managed_hosts();
            if stopped > 0 {
                info!("Stopped {} managed RPC host process(es)", stopped);
            }
            let auth_stopped = shutdown_managed_auth_provider_hosts();
            if auth_stopped > 0 {
                info!(
                    "Stopped {} managed auth-provider host process(es)",
                    auth_stopped
                );
            }
            let _ = cx.update(|cx| cx.quit());
            return;
        }

        let has_connections = cx
            .update(|cx| app_state.read(cx).has_connections())
            .unwrap_or(false);

        if !has_connections {
            info!("All connections closed");
            break;
        }

        if Instant::now() > conn_deadline {
            log::warn!("Connection close timed out, proceeding with open connections");
            break;
        }

        cx.background_executor().timer(POLL_INTERVAL).await;
    }

    info!("Shutdown phase: Flushing logs...");
    let _ = cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state.shutdown().advance_phase(
                ShutdownPhase::ClosingConnections,
                ShutdownPhase::FlushingLogs,
            );
        });
    });

    if let Some(handle) = BRIDGE_HANDLE.lock().unwrap().take() {
        match handle.shutdown() {
            Ok(()) => {}
            Err(ShutdownError::DrainTimeout {
                remaining_in_flight,
            }) => {
                eprintln!(
                    "dbflux: audit bridge shutdown timed out, dropped {} in-flight events",
                    remaining_in_flight
                );
            }
            Err(ShutdownError::JoinPanic) => {
                eprintln!("dbflux: audit bridge drain thread panicked during shutdown");
            }
        }
    }

    cx.background_executor()
        .timer(Duration::from_millis(100))
        .await;

    info!("Shutdown complete in {:?}", start.elapsed());
    let _ = cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state.complete_shutdown();
        });
    });

    let stopped = shutdown_managed_hosts();
    if stopped > 0 {
        info!("Stopped {} managed RPC host process(es)", stopped);
    }

    let auth_stopped = shutdown_managed_auth_provider_hosts();
    if auth_stopped > 0 {
        info!(
            "Stopped {} managed auth-provider host process(es)",
            auth_stopped
        );
    }

    let _ = cx.update(|cx| {
        cx.quit();
    });
}
