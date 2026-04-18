use gpui::prelude::*;
use gpui::{App, Hsla, SharedString, Window, div, px};
use gpui_component::ActiveTheme;

use crate::tokens::{FontSizes, Spacing};

/// Semantic status variant controlling the dot color.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Connected,
    Error,
    Warning,
    Idle,
}

/// Stateless status indicator: a colored dot with an optional label.
#[derive(IntoElement)]
pub struct StatusIndicator {
    status: Status,
    label: Option<SharedString>,
}

impl StatusIndicator {
    pub fn new(status: Status) -> Self {
        Self {
            status,
            label: None,
        }
    }

    /// Show a text label next to the dot.
    pub fn label(mut self, text: impl Into<SharedString>) -> Self {
        self.label = Some(text.into());
        self
    }

    fn dot_color(&self, theme: &gpui_component::Theme) -> Hsla {
        match self.status {
            Status::Connected => theme.success,
            Status::Error => theme.danger,
            Status::Warning => theme.warning,
            Status::Idle => theme.muted_foreground,
        }
    }
}

impl RenderOnce for StatusIndicator {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let color = self.dot_color(theme);

        let mut el = div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .child(div().size(px(8.0)).rounded_full().bg(color));

        if let Some(label) = self.label {
            el = el.child(
                div()
                    .text_size(FontSizes::XS)
                    .text_color(theme.muted_foreground)
                    .child(label),
            );
        }

        el
    }
}
