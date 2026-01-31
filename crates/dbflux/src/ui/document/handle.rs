#![allow(dead_code)]

use super::sql_query::SqlQueryDocument;
use super::types::{DocumentIcon, DocumentId, DocumentKind, DocumentMetaSnapshot, DocumentState};
use crate::keymap::Command;
use gpui::{AnyElement, App, Entity, IntoElement, Subscription, Window};

/// Wrapper that allows storing different document types in a homogeneous collection.
/// The `id` is stored inline for quick access without needing `cx`.
///
/// Each variant stores the DocumentId inline plus the Entity<T> for the actual document.
pub enum DocumentHandle {
    SqlQuery {
        id: DocumentId,
        entity: Entity<SqlQueryDocument>,
    },
    // Future variants:
    // TableView { id: DocumentId, entity: Entity<TableViewDocument> },
    // RedisKeyBrowser { id: DocumentId, entity: Entity<RedisKeyBrowserDocument> },
    // RedisKey { id: DocumentId, entity: Entity<RedisKeyDocument> },
    // RedisConsole { id: DocumentId, entity: Entity<RedisConsoleDocument> },
    // MongoCollection { id: DocumentId, entity: Entity<MongoCollectionDocument> },
}

impl DocumentHandle {
    /// Creates a new SqlQuery document handle.
    pub fn sql_query(entity: Entity<SqlQueryDocument>, cx: &App) -> Self {
        let id = entity.read(cx).id();
        Self::SqlQuery { id, entity }
    }

    /// Document ID (no cx required).
    pub fn id(&self) -> DocumentId {
        match self {
            Self::SqlQuery { id, .. } => *id,
        }
    }

    /// Document kind (no cx required).
    pub fn kind(&self) -> DocumentKind {
        match self {
            Self::SqlQuery { .. } => DocumentKind::SqlQuery,
        }
    }

    /// Gets metadata snapshot (requires cx to read entity).
    pub fn meta_snapshot(&self, cx: &App) -> DocumentMetaSnapshot {
        match self {
            Self::SqlQuery { id, entity } => {
                let doc = entity.read(cx);
                DocumentMetaSnapshot {
                    id: *id,
                    kind: DocumentKind::SqlQuery,
                    title: doc.title(),
                    icon: DocumentIcon::Sql,
                    state: doc.state(),
                    closable: true,
                    connection_id: doc.connection_id(),
                }
            }
        }
    }

    /// Title with modified indicator.
    pub fn tab_title(&self, cx: &App) -> String {
        let meta = self.meta_snapshot(cx);
        if meta.state == DocumentState::Modified {
            format!("{}*", meta.title)
        } else {
            meta.title
        }
    }

    /// Can this document be closed? (checks unsaved changes)
    pub fn can_close(&self, cx: &App) -> bool {
        match self {
            Self::SqlQuery { entity, .. } => entity.read(cx).can_close(),
        }
    }

    /// Renders the document.
    pub fn render(&self) -> AnyElement {
        match self {
            Self::SqlQuery { entity, .. } => entity.clone().into_any_element(),
        }
    }

    /// Dispatch commands to the active document.
    pub fn dispatch_command(&self, cmd: Command, window: &mut Window, cx: &mut App) -> bool {
        match self {
            Self::SqlQuery { entity, .. } => {
                entity.update(cx, |doc, cx| doc.dispatch_command(cmd, window, cx))
            }
        }
    }

    /// Gives focus to the document.
    pub fn focus(&self, window: &mut Window, cx: &mut App) {
        match self {
            Self::SqlQuery { entity, .. } => {
                entity.update(cx, |doc, cx| doc.focus(window, cx));
            }
        }
    }

    /// Subscribe to document events (returns Subscription).
    pub fn subscribe<F>(&self, cx: &mut App, callback: F) -> Subscription
    where
        F: Fn(&DocumentEvent, &mut App) + 'static,
    {
        match self {
            Self::SqlQuery { entity, .. } => {
                cx.subscribe(entity, move |_entity, event, cx| callback(event, cx))
            }
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
