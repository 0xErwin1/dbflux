#![windows_subsystem = "windows"]
#![recursion_limit = "256"]

mod cli;

use dbflux_audit::AuditService;
use dbflux_core::ShutdownPhase;
use dbflux_core::observability::actions::{SYSTEM_SHUTDOWN, SYSTEM_STARTUP};
use dbflux_core::observability::{EventCategory, EventOutcome, EventRecord, EventSeverity};
use dbflux_driver_ipc::shutdown_managed_hosts;
use dbflux_ipc::{
    APP_CONTROL_VERSION, framing, init_process_auth_tokens,
    protocol::{AppControlRequest, AppControlResponse, IpcMessage, IpcResponse},
    read_app_control_token, socket_name,
};
use dbflux_ui::ipc_server::IpcServer;
use dbflux_ui::platform;
use dbflux_ui::ui::overlays::command_palette::command_palette_keybindings;
use dbflux_ui::ui::views::workspace::Workspace;
use dbflux_ui::AppStateEntity;
use dbflux_ui::assets::Assets;
use gpui::*;
use gpui_component::Root;
use interprocess::local_socket::{
    Listener as IpcListener, ListenerNonblockingMode, ListenerOptions, Stream as IpcStream,
    prelude::*,
};
use log::info;
use std::io::{self, Read, Write};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Global holder for the audit service, used by the panic hook.
/// The panic hook needs access to the audit service, which is created
/// inside GPUI's closure. We store it here so the panic hook can access it.
static AUDIT_SERVICE_FOR_PANIC: Mutex<Option<AuditService>> = Mutex::new(None);

/// Previous panic hook, chained after our hook.
#[allow(clippy::type_complexity)]
static PREV_PANIC_HOOK: Mutex<Option<Box<dyn Fn(&std::panic::PanicHookInfo) + Send + Sync>>> =
    Mutex::new(None);

const TASK_CANCEL_TIMEOUT: Duration = Duration::from_millis(2000);
const CONNECTION_CLOSE_TIMEOUT: Duration = Duration::from_millis(3000);
const TOTAL_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(10000);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Installs a chained best-effort panic hook that:
/// 1. Attempts to record the panic via AuditService::record_panic_best_effort
/// 2. Falls back to stderr logging if the service is unavailable or fails
/// 3. Always delegates to the previously installed panic hook
fn install_panic_hook() {
    let prev = std::panic::take_hook();
    *PREV_PANIC_HOOK.lock().unwrap() = Some(Box::new(prev));

    std::panic::set_hook(Box::new(|panic_info: &std::panic::PanicHookInfo| {
        if let Some(audit_service) = AUDIT_SERVICE_FOR_PANIC.lock().unwrap().clone() {
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
                    eprintln!("[dbflux_audit] panic hook: record_panic_best_effort returned None");
                }
            }
        } else {
            eprintln!(
                "[dbflux_audit] panic hook: audit service not available, panic at {}",
                panic_info
                    .location()
                    .map(|loc| format!("{}:{}", loc.file(), loc.line()))
                    .unwrap_or_else(|| "unknown location".to_string())
            );
        }

        if let Some(ref prev_hook) = *PREV_PANIC_HOOK.lock().unwrap() {
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

fn main() {
    install_panic_hook();

    let args: Vec<String> = std::env::args().collect();

    if args.get(1).map(|s| s.as_str()) == Some("mcp") {
        let exit_code = dbflux_app::mcp_command::run_mcp_command(&args[2..]);
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

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    info!("IPC socket bound successfully");

    Application::new()
        .with_assets(Assets)
        .run(|cx: &mut App| {
            dbflux_ui::ui::theme::init(cx);
            dbflux_ui::ui::components::data_table::init(cx);
            dbflux_ui::ui::components::document_tree::init(cx);

            let app_state = cx.new(|_cx| AppStateEntity::new());

            let audit_service = app_state.read(cx).audit_service().clone();
            *AUDIT_SERVICE_FOR_PANIC.lock().unwrap() = Some(audit_service.clone());

            emit_system_startup(&audit_service);

            let theme_setting = app_state.read(cx).general_settings().theme;
            dbflux_ui::ui::theme::apply_theme(theme_setting, None, cx);

            let mut main_window_options = WindowOptions {
                app_id: Some("dbflux".into()),
                titlebar: Some(TitlebarOptions {
                    title: Some("DBFlux".into()),
                    ..Default::default()
                }),
                ..Default::default()
            };
            platform::apply_window_options(&mut main_window_options, 800.0, 600.0);

            let window_handle = cx
                .open_window(main_window_options, |window, cx| {
                    cx.bind_keys(command_palette_keybindings());

                    let workspace =
                        cx.new(|cx| Workspace::new(app_state.clone(), window, cx));

                    IpcServer::start_with_listener(listener, workspace.clone(), auth_token, cx);
                    info!("IPC server started");

                    cx.new(|cx| Root::new(workspace, window, cx))
                })
                .expect("Failed to open main window");

            let app_state_for_close = app_state.clone();
            window_handle
                .update(cx, |_root, window, cx| {
                    window.on_window_should_close(cx, move |_window, cx| {
                        let already_shutting_down =
                            app_state_for_close.read(cx).is_shutting_down();
                        if already_shutting_down {
                            let phase = app_state_for_close.read(cx).shutdown_phase();
                            if matches!(phase, ShutdownPhase::Complete | ShutdownPhase::Failed) {
                                return true;
                            }
                            return false;
                        }

                        info!("Starting graceful shutdown...");
                        let initiated_shutdown =
                            app_state_for_close.update(cx, |state, _| state.begin_shutdown());

                        if initiated_shutdown {
                            let audit_service =
                                app_state_for_close.read(cx).audit_service().clone();
                            emit_system_shutdown(&audit_service);

                            let app_state_shutdown = app_state_for_close.clone();
                            cx.spawn(async move |cx| {
                                run_shutdown_sequence(app_state_shutdown, cx).await;
                            })
                            .detach();
                        }

                        false
                    });
                })
                .unwrap_or_else(|error| {
                    log::warn!("Failed to install window close handler: {:?}", error);
                });
        });
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

    let _ = cx.update(|cx| {
        cx.quit();
    });
}
