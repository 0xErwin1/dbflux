use gpui::*;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};

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
        div()
            .flex()
            .gap_2()
            .child(
                Button::new("history-btn")
                    .ghost()
                    .small()
                    .label("History")
                    .on_click(cx.listener(|_, _, _, cx| {
                        cx.emit(ToolbarEvent::OpenHistory);
                    })),
            )
            .child(
                Button::new("save-btn")
                    .ghost()
                    .small()
                    .label("Save")
                    .on_click(cx.listener(|_, _, _, cx| {
                        cx.emit(ToolbarEvent::SaveQuery);
                    })),
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
