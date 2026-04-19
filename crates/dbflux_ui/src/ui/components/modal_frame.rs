use dbflux_components::composites::ModalFrame as ComponentModalFrame;
use dbflux_components::icon::IconSource;
use dbflux_components::primitives::Icon;
use gpui::*;

use crate::keymap::ContextId;
use crate::ui::icons::AppIcon;

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
            .header_leading(Icon::new(AppIcon::X).size(px(16.0)).primary())
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
            .header_leading(Icon::new(icon).size(px(16.0)).primary());
        self
    }

    #[allow(dead_code)]
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

    const COMPONENTS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/ui/components");
    const UI_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/ui");

    fn read_ui_file(name: &str) -> String {
        fs::read_to_string(format!("{COMPONENTS_DIR}/{name}"))
            .unwrap_or_else(|error| panic!("failed to read {name}: {error}"))
    }

    fn read_feature_file(path: &str) -> String {
        fs::read_to_string(format!("{UI_DIR}/{path}"))
            .unwrap_or_else(|error| panic!("failed to read {path}: {error}"))
    }

    fn modal_frame_source() -> String {
        read_ui_file("modal_frame.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("modal_frame.rs should contain production code before tests")
            .to_string()
    }

    #[test]
    fn modal_frame_shim_forwards_to_the_component_owned_modal_frame() {
        let source = modal_frame_source();

        assert!(source.contains("dbflux_components::composites::ModalFrame"));
        assert!(source.contains("inner: ComponentModalFrame"));
    }

    #[test]
    fn modal_frame_shim_drops_local_scrim_container_and_header_contracts() {
        let source = modal_frame_source();

        assert!(!source.contains(".bg(gpui::black().opacity(0.5))"));
        assert!(!source.contains("surface_panel(cx)"));
        assert!(!source.contains("Text::label_sm(self.title)"));
    }

    #[test]
    fn planned_modal_adopters_continue_to_flow_through_the_modal_frame_shim() {
        for path in [
            "document/add_member_modal.rs",
            "document/new_key_modal.rs",
            "overlays/cell_editor_modal.rs",
            "overlays/document_preview_modal.rs",
            "overlays/login_modal.rs",
            "overlays/sql_preview_modal.rs",
            "overlays/sso_wizard.rs",
        ] {
            let source = read_feature_file(path);

            assert!(
                source.contains("use crate::ui::components::modal_frame::ModalFrame;"),
                "{path} stopped using the compatibility shim before the full migration"
            );
            assert!(
                source.contains("ModalFrame::new("),
                "{path} no longer builds its modal through the shared shim"
            );
        }
    }
}
