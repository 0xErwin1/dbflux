#![windows_subsystem = "windows"]

mod app;
mod assets;
mod keymap;
mod ui;

use app::AppState;
use assets::Assets;
use dbflux_core::ShutdownPhase;
use gpui::*;
use gpui_component::Root;
use log::info;
use std::time::{Duration, Instant};
use ui::command_palette::command_palette_keybindings;
use ui::workspace::Workspace;

/// Timeout for waiting for tasks to finish after cancellation.
const TASK_CANCEL_TIMEOUT: Duration = Duration::from_millis(2000);

/// Timeout for waiting for connections to close.
const CONNECTION_CLOSE_TIMEOUT: Duration = Duration::from_millis(3000);

/// Total timeout for the entire shutdown process (hard stop).
const TOTAL_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(10000);

/// Polling interval for checking task/connection state.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

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
                    // Only bind context-specific keybindings for command palette
                    // All other keybindings are handled via the context-aware keymap system
                    cx.bind_keys(command_palette_keybindings());

                    let workspace = cx.new(|cx| Workspace::new(app_state.clone(), window, cx));
                    cx.new(|cx| Root::new(workspace, window, cx))
                },
            )
            .expect("Failed to open main window");

        // Graceful shutdown when the main window is closed
        let app_state_for_close = app_state.clone();
        window_handle
            .update(cx, |_root, window, cx| {
                window.on_window_should_close(cx, move |_window, cx| {
                    // Check if we're already shutting down
                    let already_shutting_down = app_state_for_close.read(cx).is_shutting_down();
                    if already_shutting_down {
                        // Already shutting down, check if complete
                        let phase = app_state_for_close.read(cx).shutdown_phase();
                        if matches!(phase, ShutdownPhase::Complete | ShutdownPhase::Failed) {
                            return true;
                        }
                        return false;
                    }

                    // Start graceful shutdown
                    info!("Starting graceful shutdown...");
                    let initiated_shutdown =
                        app_state_for_close.update(cx, |state, _| state.begin_shutdown());

                    // Only spawn shutdown sequence if we initiated it
                    if initiated_shutdown {
                        let app_state_shutdown = app_state_for_close.clone();
                        cx.spawn(async move |cx| {
                            run_shutdown_sequence(app_state_shutdown, cx).await;
                        })
                        .detach();
                    }

                    // Return false to prevent immediate close; quit will be called after shutdown
                    false
                });
            })
            .ok();
    });
}

/// Executes the graceful shutdown sequence.
///
/// 1. Cancel all running tasks and wait for them to finish
/// 2. Close all database connections
/// 3. Flush logs
/// 4. Mark complete and quit
///
/// Each phase has its own timeout, and there's a hard total timeout
/// that forces quit if exceeded.
async fn run_shutdown_sequence(app_state: Entity<AppState>, cx: &mut AsyncApp) {
    let start = Instant::now();

    // Phase 1: Cancel all tasks and wait for them to finish
    info!("Shutdown phase: Cancelling tasks...");
    let task_cancel_result = cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state.cancel_all_tasks();
        });
    });

    if task_cancel_result.is_err() {
        log::error!("Failed to cancel tasks during shutdown");
    }

    // Poll until no running tasks or timeout
    let task_deadline = Instant::now() + TASK_CANCEL_TIMEOUT;
    loop {
        // Check total timeout (hard stop)
        if start.elapsed() > TOTAL_SHUTDOWN_TIMEOUT {
            log::error!("Shutdown exceeded total timeout, forcing quit");
            let _ = cx.update(|cx| cx.quit());
            return;
        }

        // Check if tasks are done
        let still_running = cx
            .update(|cx| app_state.read(cx).has_running_tasks())
            .unwrap_or(false);

        if !still_running {
            info!("All tasks finished");
            break;
        }

        // Check phase timeout
        if Instant::now() > task_deadline {
            log::warn!("Task cancellation timed out, proceeding with running tasks");
            break;
        }

        cx.background_executor().timer(POLL_INTERVAL).await;
    }

    // Phase 2: Close all connections
    info!("Shutdown phase: Closing connections...");
    let close_result = cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state.close_all_connections();
        });
    });

    if close_result.is_err() {
        log::error!("Failed to close connections during shutdown");
    }

    // Poll until no connections or timeout
    let conn_deadline = Instant::now() + CONNECTION_CLOSE_TIMEOUT;
    loop {
        // Check total timeout (hard stop)
        if start.elapsed() > TOTAL_SHUTDOWN_TIMEOUT {
            log::error!("Shutdown exceeded total timeout, forcing quit");
            let _ = cx.update(|cx| cx.quit());
            return;
        }

        // Check if connections are closed
        let has_connections = cx
            .update(|cx| app_state.read(cx).has_connections())
            .unwrap_or(false);

        if !has_connections {
            info!("All connections closed");
            break;
        }

        // Check phase timeout
        if Instant::now() > conn_deadline {
            log::warn!("Connection close timed out, proceeding with open connections");
            break;
        }

        cx.background_executor().timer(POLL_INTERVAL).await;
    }

    // Phase 3: Flush logs
    info!("Shutdown phase: Flushing logs...");
    let _ = cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state.shutdown().advance_phase(
                ShutdownPhase::ClosingConnections,
                ShutdownPhase::FlushingLogs,
            );
        });
    });

    // Brief pause for log flushing (no polling needed, just a flush window)
    cx.background_executor()
        .timer(Duration::from_millis(100))
        .await;

    // Mark shutdown complete
    info!("Shutdown complete in {:?}", start.elapsed());
    let _ = cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state.complete_shutdown();
        });
    });

    // Quit the application
    let _ = cx.update(|cx| {
        cx.quit();
    });
}
