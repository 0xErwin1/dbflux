use std::sync::Arc;

use crate::controls::dropdown::{Dropdown, DropdownItem};
use gpui::{App, ElementId, SharedString};

/// A simplified single-select dropdown wrapping `Dropdown`.
///
/// Provides a cleaner builder API for common "pick one from a list" use cases.
/// Internally stores and renders a `Dropdown`.
pub struct Select {
    pub(crate) dropdown: Dropdown,
}

impl Select {
    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            dropdown: Dropdown::new(id).placeholder("Select"),
        }
    }

    pub fn items(mut self, labels: Vec<SharedString>) -> Self {
        let items = labels.into_iter().map(DropdownItem::new).collect();
        self.dropdown = self.dropdown.items(items);
        self
    }

    pub fn placeholder(mut self, placeholder: impl Into<SharedString>) -> Self {
        self.dropdown = self.dropdown.placeholder(placeholder);
        self
    }

    pub fn selected_index(mut self, index: Option<usize>) -> Self {
        self.dropdown = self.dropdown.selected_index(index);
        self
    }

    pub fn disabled(mut self, disabled: bool) -> Self {
        self.dropdown = self.dropdown.disabled(disabled);
        self
    }

    #[allow(clippy::type_complexity)]
    pub fn on_change(
        mut self,
        handler: impl Fn(usize, &DropdownItem, &mut App) + Send + Sync + 'static,
    ) -> Self {
        self.dropdown = self.dropdown.on_select(Arc::new(move |index, item, cx| {
            handler(index, item, cx);
        }));
        self
    }

    pub fn selected_label(&self) -> Option<SharedString> {
        self.dropdown.selected_label()
    }

    pub fn selected_value(&self) -> Option<SharedString> {
        self.dropdown.selected_value()
    }
}
