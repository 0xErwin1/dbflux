use gpui::prelude::*;
use gpui::{App, ClickEvent, Window, div};
use gpui_component::ActiveTheme;

use crate::tokens::{FontSizes, Heights};

/// A custom tab trigger item (not wrapping gpui_component) with three
/// visual states: Active, Inactive, and Hover.
#[derive(IntoElement)]
pub struct TabTrigger {
    id: gpui::ElementId,
    label: gpui::SharedString,
    active: bool,
    on_click: Option<Box<dyn Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync>>,
}

impl TabTrigger {
    pub fn new(id: impl Into<gpui::ElementId>, label: impl Into<gpui::SharedString>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            active: false,
            on_click: None,
        }
    }

    pub fn active(mut self, active: bool) -> Self {
        self.active = active;
        self
    }

    pub fn on_click(
        mut self,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync + 'static,
    ) -> Self {
        self.on_click = Some(Box::new(handler));
        self
    }
}

impl RenderOnce for TabTrigger {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();

        let mut el = div()
            .id(self.id)
            .h(Heights::TAB)
            .px(gpui::px(12.0))
            .flex()
            .items_center()
            .text_size(FontSizes::SM)
            .cursor_pointer()
            .whitespace_nowrap();

        if self.active {
            el = el
                .bg(theme.tab_bar)
                .text_color(theme.foreground)
                .font_weight(gpui::FontWeight::MEDIUM)
                .border_b_2()
                .border_color(theme.accent);
        } else {
            el = el
                .bg(gpui::transparent_black())
                .text_color(theme.muted_foreground)
                .hover(|s| s.bg(theme.secondary).text_color(theme.foreground));
        }

        el = el.child(self.label);

        if let Some(handler) = self.on_click {
            el = el.on_click(handler);
        }

        el
    }
}
