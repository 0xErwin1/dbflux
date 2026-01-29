mod app;
mod assets;
mod keymap;
mod ui;

use app::AppState;
use assets::Assets;
use gpui::*;
use gpui_component::Root;
use ui::command_palette::command_palette_keybindings;
use ui::workspace::Workspace;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    Application::new().with_assets(Assets).run(|cx: &mut App| {
        ui::theme::init(cx);
        ui::components::data_table::init(cx);
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

                    let workspace = cx.new(|cx| Workspace::new(app_state, window, cx));
                    cx.new(|cx| Root::new(workspace, window, cx))
                },
            )
            .expect("Failed to open main window");

        // Quit the application when the main window is closed
        window_handle
            .update(cx, |_root, window, cx| {
                window.on_window_should_close(cx, |_window, cx| {
                    cx.quit();
                    true
                });
            })
            .ok();
    });
}
