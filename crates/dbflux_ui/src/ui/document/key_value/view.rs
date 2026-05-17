//! `KeyValueView` — view-layer entity shell for `KeyValueDocument`.
//!
//! Currently a thin newtype over the document entity. The `into_pane` builder
//! in `pane.rs` uses `KeyValueDocument` directly (same pattern as `CodeDocument`),
//! so this module exists as the named view-layer boundary in the module tree
//! and can absorb additional view-only state in future arcs without touching
//! the document's data model.

use super::KeyValueDocument;
use gpui::Entity;

/// View-layer entity placeholder.
///
/// In the current implementation `KeyValueDocument` self-renders through its
/// own `impl Render`. `KeyValueView` holds a reference to the document entity
/// and is reserved for future extraction of view-only state (selection,
/// animations, per-view overrides) without coupling them to the data model.
#[allow(dead_code)]
pub struct KeyValueView {
    pub(super) document: Entity<KeyValueDocument>,
}
