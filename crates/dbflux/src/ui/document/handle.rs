#![allow(dead_code)]

use super::types::{DocumentId, DocumentKind, DocumentMetaSnapshot};
use crate::keymap::Command;
use gpui::{AnyElement, App, Subscription, Window};

/// Wrapper that allows storing different document types in a homogeneous collection.
/// The `id` is stored inline for quick access without needing `cx`.
///
/// New document types will be added here as variants (e.g., SqlQuery, TableView, RedisKey).
/// Each variant stores the DocumentId inline plus the Entity<T> for the actual document.
pub enum DocumentHandle {
    // Variants will be added in Phase 2:
    // SqlQuery { id: DocumentId, entity: Entity<SqlQueryDocument> },
    // TableView { id: DocumentId, entity: Entity<TableViewDocument> },
}

impl DocumentHandle {
    /// Document ID (no cx required).
    pub fn id(&self) -> DocumentId {
        match *self {
            // Match arms will be added when variants are implemented
        }
    }

    /// Document kind (no cx required).
    pub fn kind(&self) -> DocumentKind {
        match *self {
            // Match arms will be added when variants are implemented
        }
    }

    /// Gets metadata snapshot (requires cx to read entity).
    pub fn meta_snapshot(&self, _cx: &App) -> DocumentMetaSnapshot {
        match *self {
            // Match arms will be added when variants are implemented
        }
    }

    /// Title with modified indicator.
    pub fn tab_title(&self, cx: &App) -> String {
        let meta = self.meta_snapshot(cx);
        if meta.state == super::types::DocumentState::Modified {
            format!("{}*", meta.title)
        } else {
            meta.title
        }
    }

    /// Can this document be closed? (checks unsaved changes)
    pub fn can_close(&self, _cx: &App) -> bool {
        match *self {
            // Match arms will be added when variants are implemented
        }
    }

    /// Renders the document.
    pub fn render(&self) -> AnyElement {
        match *self {
            // Match arms will be added when variants are implemented
        }
    }

    /// Dispatch commands to the active document.
    pub fn dispatch_command(&self, _cmd: Command, _window: &mut Window, _cx: &mut App) -> bool {
        match *self {
            // Match arms will be added when variants are implemented
        }
    }

    /// Gives focus to the document.
    pub fn focus(&self, _window: &mut Window, _cx: &mut App) {
        match *self {
            // Match arms will be added when variants are implemented
        }
    }

    /// Subscribe to document events (returns Subscription).
    pub fn subscribe<F>(&self, _cx: &mut App, _callback: F) -> Subscription
    where
        F: Fn(&DocumentEvent, &mut App) + 'static,
    {
        match *self {
            // Match arms will be added when variants are implemented
        }
    }
}

/// Events that a document can emit.
#[derive(Clone, Debug)]
pub enum DocumentEvent {
    /// Title, state, etc. changed.
    MetaChanged,
    ExecutionStarted,
    ExecutionFinished,
    /// The document wants to close itself.
    RequestClose,
}
