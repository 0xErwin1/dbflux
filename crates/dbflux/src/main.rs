mod app;
mod ui;

use app::AppState;
use gpui::*;
use gpui_component::Root;
use ui::workspace::Workspace;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    Application::new().run(|cx: &mut App| {
        ui::theme::init(cx);
        let app_state = cx.new(|_cx| AppState::new());

        cx.open_window(
            WindowOptions {
                titlebar: Some(TitlebarOptions {
                    title: Some("DBFlux".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            |window, cx| {
                let workspace = cx.new(|cx| Workspace::new(app_state, window, cx));
                cx.new(|cx| Root::new(workspace, window, cx))
            },
        )
        .unwrap();
    });
}
