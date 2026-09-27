//! Visual query builder for document collections (IslDocBuilder).
//!
//! The rail composes a [`dbflux_core::DocumentQuerySpec`] and keeps it in
//! two-way sync with the collection's query slots through the driver's
//! [`dbflux_core::DocumentQueryCodec`]: every builder edit renders into the
//! slots, every slot edit parses back into the builder. The slots stay the
//! query that runs, so Find goes through the same browse path as typing in
//! them. Nothing here depends on a particular driver.

mod catalog;
mod model;
mod panel;
mod sync;
mod values;
mod view;

#[cfg(test)]
mod tests;

pub use model::{AccumulatorOp, ProblemKind};
pub use panel::{DocumentBuilderEvent, DocumentBuilderPanel, SavedQueryEntry};
pub use sync::SlotWrite;
pub use values::ValueProblem;

use gpui::{AnyView, Pixels, px};

/// Width of the builder rail: wider than the other inspectors, since a
/// condition row holds a field, an operator and a typed value.
pub const RAIL_WIDTH: Pixels = px(540.0);

/// The width the inspector rail needs for `content`, when it needs more than
/// its default.
pub fn inspector_min_width(content: &AnyView) -> Option<Pixels> {
    (content.entity_type() == std::any::TypeId::of::<DocumentBuilderPanel>()).then_some(RAIL_WIDTH)
}
