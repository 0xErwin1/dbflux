use gpui::prelude::*;
use gpui::{App, ClickEvent, ElementId, Pixels, Window, div, svg};
use gpui_component::ActiveTheme;
use gpui_component::IconNamed;

use crate::icon::IconSource;
use crate::tokens::{Heights, Radii};

/// Clickable icon in a consistent hit target with baked-in hover and focus styles.
#[derive(IntoElement)]
#[allow(clippy::type_complexity)]
pub struct IconButton {
    id: ElementId,
    icon: IconSource,
    icon_size: Pixels,
    on_click: Option<Box<dyn Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync>>,
}

impl IconButton {
    pub fn new(id: impl Into<ElementId>, icon: IconSource) -> Self {
        Self {
            id: id.into(),
            icon,
            icon_size: Heights::ICON_MD,
            on_click: None,
        }
    }

    /// Set the click handler.
    pub fn on_click(
        mut self,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync + 'static,
    ) -> Self {
        self.on_click = Some(Box::new(handler));
        self
    }

    /// Override the icon size (defaults to ICON_MD).
    pub fn icon_size(mut self, size: Pixels) -> Self {
        self.icon_size = size;
        self
    }
}

impl RenderOnce for IconButton {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let hover_bg = theme.accent;

        let icon_el = match self.icon {
            IconSource::Named(name) => svg()
                .path(name.path())
                .size(self.icon_size)
                .text_color(theme.muted_foreground)
                .into_any_element(),
            IconSource::Svg(path) => svg()
                .path(path)
                .size(self.icon_size)
                .text_color(theme.muted_foreground)
                .into_any_element(),
        };

        let mut btn = div()
            .id(self.id)
            .flex()
            .items_center()
            .justify_center()
            .size(Heights::BUTTON)
            .rounded(Radii::SM)
            .cursor_pointer()
            .hover(|s| s.bg(hover_bg))
            .child(icon_el);

        if let Some(handler) = self.on_click {
            btn = btn.on_click(handler);
        }

        btn
    }
}
