use gpui::App;
use gpui_component::theme::{Theme, ThemeMode};
use log::info;

pub fn init(cx: &mut App) {
    gpui_component::init(cx);

    info!("Setting dark theme");
    Theme::change(ThemeMode::Dark, None, cx);
}
