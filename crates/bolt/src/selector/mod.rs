//! Test and automation selectors.
//!
//! Every component that takes an element id also records it as its gpui debug
//! selector, so a product's GPUI tests find it with `cx.debug_bounds(id)`.
//! gpui records selectors only in debug builds with `test-support`; elsewhere
//! the closure is never called and nothing is drawn differently.

use gpui::{AnyElement, Div, ElementId, InteractiveElement as _, IntoElement as _};

/// The debug selector of an element id: its display form, the same string
/// automation addresses it by. Pass it to `debug_selector` next to `.id(..)`.
pub fn id_selector(id: &ElementId) -> impl FnOnce() -> String + use<> {
    let id = id.clone();
    move || id.to_string()
}

/// `element` under `id` with its debug selector, or `element` unchanged when
/// the caller set no id: an id-less element keeps today's tree.
pub(crate) fn with_optional_id(element: Div, id: Option<ElementId>) -> AnyElement {
    match id {
        Some(id) => element
            .debug_selector(id_selector(&id))
            .id(id)
            .into_any_element(),
        None => element.into_any_element(),
    }
}

#[cfg(test)]
mod tests;
