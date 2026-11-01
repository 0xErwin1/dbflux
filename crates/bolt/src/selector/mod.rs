//! Test and automation selectors.
//!
//! Every component that takes an element id also records it as its gpui debug
//! selector, so a product's GPUI tests find it with `cx.debug_bounds(id)`.
//! gpui records selectors only in debug builds with `test-support`; elsewhere
//! the closure is never called and nothing is drawn differently.

use gpui::ElementId;

/// The debug selector of an element id: its display form, the same string
/// automation addresses it by. Pass it to `debug_selector` next to `.id(..)`.
pub fn id_selector(id: &ElementId) -> impl FnOnce() -> String + use<> {
    let id = id.clone();
    move || id.to_string()
}

#[cfg(test)]
mod tests;
