use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::*;
use gpui_component::ActiveTheme;

#[derive(Clone)]
pub enum ToolbarEvent {
    OpenHistory,
    SaveQuery,
}

pub struct EditorToolbar;

impl EditorToolbar {
    pub fn new(_window: &mut Window, _cx: &mut Context<Self>) -> Self {
        Self
    }
}

impl Render for EditorToolbar {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .flex()
            .gap_2()
            .child(
                div()
                    .id("history-btn")
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .px(Spacing::MD)
                    .h(Heights::BUTTON)
                    .rounded(Radii::MD)
                    .cursor_pointer()
                    .text_color(theme.foreground)
                    .hover(|s| s.bg(theme.secondary))
                    .on_click(cx.listener(|_, _, _, cx| {
                        cx.emit(ToolbarEvent::OpenHistory);
                    }))
                    .text_size(FontSizes::SM)
                    .child(
                        svg()
                            .path(AppIcon::History.path())
                            .size_4()
                            .text_color(theme.foreground),
                    )
                    .child("History"),
            )
            .child(
                div()
                    .id("save-btn")
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .px(Spacing::MD)
                    .h(Heights::BUTTON)
                    .rounded(Radii::MD)
                    .cursor_pointer()
                    .text_color(theme.foreground)
                    .hover(|s| s.bg(theme.secondary))
                    .on_click(cx.listener(|_, _, _, cx| {
                        cx.emit(ToolbarEvent::SaveQuery);
                    }))
                    .text_size(FontSizes::SM)
                    .child(
                        svg()
                            .path(AppIcon::Save.path())
                            .size_4()
                            .text_color(theme.foreground),
                    )
                    .child("Save"),
            )
    }
}

impl EventEmitter<ToolbarEvent> for EditorToolbar {}

#[cfg(test)]
mod tests {
    use super::EditorToolbar;

    #[test]
    fn toolbar_type_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EditorToolbar>();
    }
}
