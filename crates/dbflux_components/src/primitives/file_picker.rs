use crate::icon::IconSource;
use crate::primitives::Icon;
use crate::tokens::{FontSizes, Heights, Radii};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use std::sync::Arc;

type ClickHandler = Arc<dyn Fn(&MouseDownEvent, &mut Window, &mut App) + Send + Sync + 'static>;

/// Compact file-picker control: a button row that shows the basename of the
/// currently selected file (or a "Browse…" placeholder) plus an optional
/// trailing clear button.
///
/// The actual native file dialog is invoked by the caller's `on_browse`
/// callback; this primitive owns only the visual control.
#[derive(IntoElement)]
pub struct FilePicker {
    id: ElementId,
    current_value: SharedString,
    placeholder: SharedString,
    folder_icon: IconSource,
    clear_icon: IconSource,
    on_browse: Option<ClickHandler>,
    on_clear: Option<ClickHandler>,
}

impl FilePicker {
    /// `id` must be unique within the parent element; `current_value` is the
    /// stored path (use an empty string when nothing is selected).
    pub fn new(
        id: impl Into<ElementId>,
        current_value: impl Into<SharedString>,
        folder_icon: impl Into<IconSource>,
        clear_icon: impl Into<IconSource>,
    ) -> Self {
        Self {
            id: id.into(),
            current_value: current_value.into(),
            placeholder: SharedString::from("Browse\u{2026}"),
            folder_icon: folder_icon.into(),
            clear_icon: clear_icon.into(),
            on_browse: None,
            on_clear: None,
        }
    }

    /// Override the default "Browse…" placeholder.
    pub fn placeholder(mut self, value: impl Into<SharedString>) -> Self {
        self.placeholder = value.into();
        self
    }

    /// Browse callback — wire a `cx.listener(...)` here to invoke the native
    /// file dialog. Without it, the picker renders disabled.
    pub fn on_browse<F>(mut self, handler: F) -> Self
    where
        F: Fn(&MouseDownEvent, &mut Window, &mut App) + Send + Sync + 'static,
    {
        self.on_browse = Some(Arc::new(handler));
        self
    }

    /// Clear callback — only emits the trailing × button when set AND a value
    /// is currently selected.
    pub fn on_clear<F>(mut self, handler: F) -> Self
    where
        F: Fn(&MouseDownEvent, &mut Window, &mut App) + Send + Sync + 'static,
    {
        self.on_clear = Some(Arc::new(handler));
        self
    }
}

impl RenderOnce for FilePicker {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme().clone();
        let has_value = !self.current_value.trim().is_empty();
        let display_label = file_picker_label(&self.current_value);

        let label_color = if has_value {
            theme.foreground
        } else {
            theme.muted_foreground
        };

        let on_browse = self.on_browse.clone();
        let picker_button = div()
            .id(self.id.clone())
            .flex()
            .items_center()
            .gap_2()
            .h(Heights::CONTROL)
            .px_2()
            .border_1()
            .border_color(theme.input)
            .rounded(Radii::SM)
            .cursor_pointer()
            .hover(|d| d.bg(theme.list_hover))
            .child(
                Icon::new(self.folder_icon.clone())
                    .size(px(14.0))
                    .color(label_color),
            )
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .text_color(label_color)
                    .child(SharedString::from(display_label)),
            )
            .when_some(on_browse, |d, handler| {
                d.on_mouse_down(MouseButton::Left, move |event, window, cx| {
                    handler(event, window, cx);
                })
            });

        let clear_handler = self.on_clear.clone();
        let clear_button = if has_value && clear_handler.is_some() {
            let clear_id: SharedString = match &self.id {
                ElementId::Name(name) => SharedString::from(format!("{}-clear", name)),
                other => SharedString::from(format!("{:?}-clear", other)),
            };
            Some(
                div()
                    .id(clear_id)
                    .flex()
                    .items_center()
                    .justify_center()
                    .h(Heights::CONTROL)
                    .w(Heights::CONTROL)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.list_hover))
                    .child(
                        Icon::new(self.clear_icon.clone())
                            .size(px(12.0))
                            .color(theme.muted_foreground),
                    )
                    .when_some(clear_handler, |d, handler| {
                        d.on_mouse_down(MouseButton::Left, move |event, window, cx| {
                            handler(event, window, cx);
                        })
                    }),
            )
        } else {
            None
        };

        div()
            .flex()
            .flex_row()
            .items_center()
            .gap_2()
            .child(picker_button)
            .when_some(clear_button, |d, btn| d.child(btn))
    }
}

/// Maps a stored file-picker value to the label shown on the picker button.
/// Empty/whitespace renders the "Browse…" placeholder; otherwise the file
/// basename is shown so the row stays compact.
pub fn file_picker_label(value: &str) -> String {
    if value.trim().is_empty() {
        return "Browse\u{2026}".to_string();
    }

    std::path::Path::new(value)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::file_picker_label;

    #[test]
    fn empty_value_returns_browse_placeholder() {
        assert_eq!(file_picker_label(""), "Browse\u{2026}");
    }

    #[test]
    fn whitespace_only_value_returns_browse_placeholder() {
        assert_eq!(file_picker_label("   "), "Browse\u{2026}");
    }

    #[test]
    fn absolute_path_returns_basename() {
        assert_eq!(file_picker_label("/home/user/certs/ca.pem"), "ca.pem");
    }

    #[test]
    fn relative_path_returns_basename() {
        assert_eq!(file_picker_label("certs/client.key"), "client.key");
    }

    #[test]
    fn bare_filename_returns_itself() {
        assert_eq!(file_picker_label("server.crt"), "server.crt");
    }
}
