//! `LogStreamView` — view-layer entity shell for `AuditDocument`.
//!
//! Mirrors the `KeyValueView` pattern: in the current implementation
//! `AuditDocument` self-renders through its own `impl Render`.
//! `LogStreamView` holds a reference to the document entity and is reserved
//! for future extraction of view-only state (virtual scroll position,
//! row height cache, per-view filter overrides) without coupling them to
//! the data model.

use super::AuditDocument;
use gpui::Entity;

/// View-layer entity placeholder.
///
/// `AuditDocument` self-renders; `LogStreamView` exists as the named
/// view-layer boundary and can absorb view-only state in future arcs.
#[allow(dead_code)]
pub struct LogStreamView {
    pub(super) document: Entity<AuditDocument>,
}
