//! Product-side localized placeholder over bolt's generic [`FilePicker`].
//!
//! The generic file-picker primitive lives in `bolt`; the translated
//! "Browse…" placeholder is interface copy and stays here.

use gpui::prelude::*;
use gpui::{App, ClickEvent, ElementId, SharedString, Window};

/// Compact file-picker control: a button row that shows the basename of the
/// currently selected file (or the translated "Browse…" placeholder) plus an
/// optional trailing clear button.
#[derive(IntoElement)]
pub struct FilePicker(bolt::primitives::FilePicker);

impl FilePicker {
    /// `id` must be unique within the parent element; `current_value` is the
    /// stored path (use an empty string when nothing is selected).
    pub fn new(
        id: impl Into<ElementId>,
        current_value: impl Into<SharedString>,
        folder_icon: impl Into<bolt::icon::IconSource>,
        clear_icon: impl Into<bolt::icon::IconSource>,
    ) -> Self {
        Self(bolt::primitives::FilePicker::new(
            id,
            current_value,
            browse_placeholder(),
            folder_icon,
            clear_icon,
        ))
    }

    /// Override the default "Browse…" placeholder.
    pub fn placeholder(mut self, value: impl Into<SharedString>) -> Self {
        self.0 = self.0.placeholder(value);
        self
    }

    /// Browse callback — wire a `cx.listener(...)` here to invoke the native
    /// file dialog. Without it, the picker renders disabled.
    pub fn on_browse<F>(mut self, handler: F) -> Self
    where
        F: Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync + 'static,
    {
        self.0 = self.0.on_browse(handler);
        self
    }

    /// Clear callback — only emits the trailing × button when set AND a value
    /// is currently selected.
    pub fn on_clear<F>(mut self, handler: F) -> Self
    where
        F: Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync + 'static,
    {
        self.0 = self.0.on_clear(handler);
        self
    }
}

impl RenderOnce for FilePicker {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        self.0.render(window, cx)
    }
}

/// The translated label shown while nothing is selected.
fn browse_placeholder() -> String {
    dbflux_i18n::t!("components.file_picker.browse")
}

/// Maps a stored file-picker value to the label shown on the picker button.
/// Empty/whitespace renders the "Browse…" placeholder; otherwise the file
/// basename is shown so the row stays compact.
pub fn file_picker_label(value: &str) -> String {
    bolt::primitives::file_picker_label(value, &browse_placeholder())
}

#[cfg(test)]
mod tests {
    use super::{browse_placeholder, file_picker_label};

    #[test]
    fn empty_value_returns_browse_placeholder() {
        assert_eq!(file_picker_label(""), browse_placeholder());
    }

    #[test]
    fn whitespace_only_value_returns_browse_placeholder() {
        assert_eq!(file_picker_label("   "), browse_placeholder());
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

    #[test]
    fn file_picker_browse_key_resolves() {
        let en = dbflux_i18n::t!("components.file_picker.browse", locale = "en");
        let es = dbflux_i18n::t!("components.file_picker.browse", locale = "es");

        assert!(!en.is_empty() && en != "components.file_picker.browse");
        assert!(!es.is_empty() && es != "components.file_picker.browse");
        assert_ne!(en, es);
    }
}
