use gpui::{AnyElement, App, ElementId, IntoElement, Window};
use gpui_component::checkbox::Checkbox as GpuiCheckbox;
use gpui_component::text::Text;

/// Thin wrapper around `gpui_component::checkbox::Checkbox` that applies
/// DBFlux design system defaults.
pub struct Checkbox {
    inner: GpuiCheckbox,
}

impl Checkbox {
    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            inner: GpuiCheckbox::new(id),
        }
    }

    pub fn checked(mut self, checked: bool) -> Self {
        self.inner = self.inner.checked(checked);
        self
    }

    pub fn label(mut self, label: impl Into<Text>) -> Self {
        self.inner = self.inner.label(label);
        self
    }

    pub fn on_click(mut self, handler: impl Fn(&bool, &mut Window, &mut App) + 'static) -> Self {
        self.inner = self.inner.on_click(handler);
        self
    }
}

impl IntoElement for Checkbox {
    type Element = AnyElement;

    fn into_element(self) -> Self::Element {
        self.inner.into_any_element()
    }
}
