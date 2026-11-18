//! Host-provided range styles for a single-line [`super::Input`].
//!
//! The styles live on the [`InputState`], not on the element, so they are drawn
//! while the field is edited as well as at rest, follow edits (UTF-8 byte
//! ranges that never grow when typing at an edge) and are hidden while the
//! field is masked. The host owns which ranges to style; Bolt does not parse
//! the text.

use std::ops::Range;

use gpui::{App, Entity, HighlightStyle};

pub use gpui_component::input::{TextDecoration, TextDecorationCollection};

use super::InputState;

/// Install a collection of range styles on `state` and return its handle.
///
/// Replace the ranges later with [`TextDecorationCollection::set`] or drop
/// them with [`TextDecorationCollection::clear`]. Replacing the whole value
/// (`set_value`) removes ranges that covered the old text, so set them again
/// after it.
pub fn decorate_input(
    state: &Entity<InputState>,
    decorations: Vec<TextDecoration>,
    cx: &mut App,
) -> TextDecorationCollection {
    state.update(cx, |state, cx| {
        state.create_decorations_collection(decorations, cx)
    })
}

/// Give every byte range the same style.
pub fn styled_ranges(
    ranges: impl IntoIterator<Item = Range<usize>>,
    style: HighlightStyle,
) -> Vec<TextDecoration> {
    ranges
        .into_iter()
        .map(|range| TextDecoration::new(range, style))
        .collect()
}

#[cfg(test)]
mod tests;
