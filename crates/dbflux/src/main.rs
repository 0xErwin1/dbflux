mod app;
mod assets;
mod cli;
mod ipc_server;
mod keymap;
mod ui;

use app::AppState;
use assets::Assets;
use dbflux_core::ShutdownPhase;
use dbflux_ipc::{
    APP_CONTROL_VERSION,
    ensure_socket_dir,
    framing,
    protocol::{AppControlRequest, AppControlResponse, IpcMessage, IpcResponse},
    socket_path,
};
use gpui::*;
use gpui_component::Root;
use ipc_server::IpcServer;
use log::info;
use std::io;
use std::os::unix::net::{UnixListener, UnixStream};
use std::time::{Duration, Instant};
use ui::command_palette::command_palette_keybindings;
use ui::workspace::Workspace;

const TASK_CANCEL_TIMEOUT: Duration = Duration::from_millis(2000);
const CONNECTION_CLOSE_TIMEOUT: Duration = Duration::from_millis(3000);
const TOTAL_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(10000);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.get(1).map(|s| s.as_str()) == Some("--gui") {
        run_gui();
        return;
    }

    if args.len() == 1 {
        match UnixStream::connect(socket_path()) {
            Ok(mut stream) => {
                let _ = send_focus_request(&mut stream, 1);
                return;
            }
            Err(_) => {
                run_gui();
                return;
            }
        }
    }

    std::process::exit(cli::run(&args));
}

fn bind_ipc_socket() -> Result<UnixListener, ()> {
    let path = socket_path();

    if let Err(e) = ensure_socket_dir() {
        eprintln!("Failed to create socket directory: {}", e);
        return Err(());
    }

    match UnixListener::bind(&path) {
        Ok(listener) => {
            if let Err(e) = listener.set_nonblocking(true) {
                eprintln!("Failed to set socket nonblocking: {}", e);
                return Err(());
            }
            Ok(listener)
        }
        Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
            if let Ok(mut stream) = UnixStream::connect(&path) {
                let _ = send_focus_request(&mut stream, 1);
                std::process::exit(0);
            }

            if let Err(e) = std::fs::remove_file(&path) {
                eprintln!("Failed to remove stale socket: {}", e);
                return Err(());
            }

            match UnixListener::bind(&path) {
                Ok(listener) => {
                    if let Err(e) = listener.set_nonblocking(true) {
                        eprintln!("Failed to set socket nonblocking: {}", e);
                        return Err(());
                    }
                    Ok(listener)
                }
                Err(e) => {
                    eprintln!("Failed to bind socket after cleanup: {}", e);
                    Err(())
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to bind IPC socket: {}", e);
            Err(())
        }
    }
}

fn send_focus_request(stream: &mut UnixStream, request_id: u64) -> io::Result<()> {
    let request = AppControlRequest::new(request_id, IpcMessage::Focus);
    framing::send_msg(&mut *stream, &request)?;

    let response: AppControlResponse = framing::recv_msg(&mut *stream)?;

    if !response
        .protocol_version
        .is_compatible_with(APP_CONTROL_VERSION)
    {
        return Err(io::Error::other("incompatible app-control protocol version"));
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
    let listener = match bind_ipc_socket() {
        Ok(l) => l,
        Err(()) => std::process::exit(1),
    };

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    info!("IPC socket bound successfully");

    Application::new().with_assets(Assets).run(|cx: &mut App| {
        ui::theme::init(cx);
        ui::components::data_table::init(cx);
        ui::components::document_tree::init(cx);
        let app_state = cx.new(|_cx| AppState::new());

        let window_handle = cx
            .open_window(
                WindowOptions {
                    app_id: Some("dbflux".into()),
                    titlebar: Some(TitlebarOptions {
                        title: Some("DBFlux".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                |window, cx| {
                    cx.bind_keys(command_palette_keybindings());

                    let workspace = cx.new(|cx| Workspace::new(app_state.clone(), window, cx));

                    IpcServer::start_with_listener(listener, workspace.clone(), cx);
                    info!("IPC server started");

                    cx.new(|cx| Root::new(workspace, window, cx))
                },
            )
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

                    info!("Starting graceful shutdown...");
                    let initiated_shutdown =
                        app_state_for_close.update(cx, |state, _| state.begin_shutdown());

                    if initiated_shutdown {
                        let app_state_shutdown = app_state_for_close.clone();
                        cx.spawn(async move |cx| {
                            run_shutdown_sequence(app_state_shutdown, cx).await;
                        })
                        .detach();
                    }

                    false
                });
            })
            .ok();
    });
}

async fn run_shutdown_sequence(app_state: Entity<AppState>, cx: &mut AsyncApp) {
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

    let socket = socket_path();
    if let Err(error) = std::fs::remove_file(&socket)
        && error.kind() != io::ErrorKind::NotFound
    {
        log::warn!(
            "Failed to remove IPC socket {}: {}",
            socket.display(),
            error
        );
    }

    let _ = cx.update(|cx| {
        cx.quit();
    });
}
