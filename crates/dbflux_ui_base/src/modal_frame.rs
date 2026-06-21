use dbflux_app::keymap::ContextId;
use dbflux_components::composites::ModalFrame as ComponentModalFrame;
use dbflux_components::icon::IconSource;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Icon;
use dbflux_components::tokens::Heights;
use gpui::*;

/// Compatibility wrapper over the component-owned modal frame.
///
/// Keep the existing `dbflux_ui` builder surface during migration, but forward
/// all shared modal chrome to `dbflux_components` so overlays stop owning a
/// parallel scrim/container/header contract locally.
pub struct ModalFrame {
    inner: ComponentModalFrame,
}

impl ModalFrame {
    pub fn new(
        id: impl Into<ElementId>,
        focus_handle: &FocusHandle,
        on_close: impl Fn(&mut Window, &mut App) + Send + Sync + 'static,
    ) -> Self {
        let inner = ComponentModalFrame::new(id, focus_handle, on_close)
            .key_context(ContextId::SqlPreviewModal.as_gpui_context())
            .header_leading(Icon::new(AppIcon::X).size(Heights::ICON_SM).primary())
            .close_icon(IconSource::Svg(AppIcon::X.path().into()));

        Self { inner }
    }

    pub fn title(mut self, title: impl Into<SharedString>) -> Self {
        self.inner = self.inner.title(title);
        self
    }

    pub fn icon(mut self, icon: AppIcon) -> Self {
        self.inner = self
            .inner
            .header_leading(Icon::new(icon).size(Heights::ICON_SM).primary());
        self
    }

    pub fn context_id(mut self, context_id: ContextId) -> Self {
        self.inner = self.inner.key_context(context_id.as_gpui_context());
        self
    }

    pub fn width(mut self, width: Pixels) -> Self {
        self.inner = self.inner.width(width);
        self
    }

    pub fn height(mut self, height: Pixels) -> Self {
        self.inner = self.inner.height(height);
        self
    }

    pub fn max_height(mut self, height: Pixels) -> Self {
        self.inner = self.inner.max_height(height);
        self
    }

    pub fn top_offset(mut self, offset: Pixels) -> Self {
        self.inner = self.inner.top_offset(offset);
        self
    }

    pub fn header_extra(mut self, element: impl IntoElement) -> Self {
        self.inner = self.inner.header_extra(element);
        self
    }

    pub fn block_scroll(mut self) -> Self {
        self.inner = self.inner.block_scroll();
        self
    }

    pub fn child(mut self, child: impl IntoElement) -> Self {
        self.inner = self.inner.child(child);
        self
    }

    pub fn render(self, cx: &App) -> AnyElement {
        self.inner.render(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    const THIS_FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/modal_frame.rs");
    const UI_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../dbflux_ui/src/ui");

    fn read_this_source() -> String {
        fs::read_to_string(THIS_FILE)
            .unwrap_or_else(|error| panic!("failed to read modal_frame.rs: {error}"))
            .split("#[cfg(test)]")
            .next()
            .expect("modal_frame.rs should contain production code before tests")
            .to_string()
    }

    fn read_ui_feature_file(path: &str) -> String {
        fs::read_to_string(format!("{UI_DIR}/{path}"))
            .unwrap_or_else(|error| panic!("failed to read {path}: {error}"))
    }

    #[test]
    fn modal_frame_delegates_to_component_owned_implementation() {
        let source = read_this_source();
        assert!(
            source.contains("ComponentModalFrame"),
            "modal_frame.rs must delegate to dbflux_components ComponentModalFrame"
        );
    }

    #[test]
    fn modal_frame_drops_local_scrim_container_and_header_contracts() {
        let source = read_this_source();

        assert!(!source.contains(".bg(gpui::black().opacity(0.5))"));
        assert!(!source.contains("surface_panel(cx)"));
        assert!(!source.contains("Text::label_sm(self.title)"));
    }

    #[test]
    #[allow(clippy::single_element_loop)]
    fn overlay_call_sites_import_modal_frame_directly_from_ui_base() {
        for path in ["overlays/login_modal.rs"] {
            let source = read_ui_feature_file(path);

            assert!(
                source.contains("dbflux_ui_base::modal_frame::ModalFrame"),
                "{path} must import ModalFrame directly from dbflux_ui_base"
            );
            assert!(
                source.contains("ModalFrame::new("),
                "{path} no longer builds its modal through ModalFrame"
            );
        }
    }
}
