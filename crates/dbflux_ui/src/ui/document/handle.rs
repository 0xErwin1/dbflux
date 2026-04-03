#![allow(dead_code)]

use super::audit::AuditDocument;
use super::code::CodeDocument;
use super::data_document::{DataDocument, DataDocumentEvent};
use super::key_value::{KeyValueDocument, KeyValueDocumentEvent};
use super::types::{DataSourceKind, DocumentIcon, DocumentId, DocumentKind, DocumentMetaSnapshot};
use crate::keymap::{Command, ContextId};
use crate::ui::overlays::sql_preview_modal::SqlPreviewContext;
use dbflux_core::RefreshPolicy;
use gpui::{AnyElement, App, Entity, Focusable, IntoElement, Subscription, Window};

/// Wrapper that allows storing different document types in a homogeneous collection.
/// The `id` is stored inline for quick access without needing `cx`.
///
/// Each variant stores the DocumentId inline plus the Entity<T> for the actual document.
#[derive(Clone)]
pub enum DocumentHandle {
    /// SQL script with editor + embedded results.
    Code {
        id: DocumentId,
        entity: Entity<CodeDocument>,
    },
    /// Data grid document (table browser or promoted result).
    Data {
        id: DocumentId,
        entity: Entity<DataDocument>,
    },
    KeyValue {
        id: DocumentId,
        entity: Entity<KeyValueDocument>,
    },
    /// Audit event viewer document.
    Audit {
        id: DocumentId,
        entity: Entity<AuditDocument>,
    },
}

impl DocumentHandle {
    /// Creates a new code document handle.
    pub fn code(entity: Entity<CodeDocument>, cx: &App) -> Self {
        let id = entity.read(cx).id();
        Self::Code { id, entity }
    }

    /// Creates a new Data document handle.
    pub fn data(entity: Entity<DataDocument>, cx: &App) -> Self {
        let id = entity.read(cx).id();
        Self::Data { id, entity }
    }

    pub fn key_value(entity: Entity<KeyValueDocument>, cx: &App) -> Self {
        let id = entity.read(cx).id();
        Self::KeyValue { id, entity }
    }

    /// Creates a new Audit document handle.
    pub fn audit(entity: Entity<AuditDocument>, cx: &App) -> Self {
        let id = entity.read(cx).id();
        Self::Audit { id, entity }
    }

    /// Document ID (no cx required).
    pub fn id(&self) -> DocumentId {
        match self {
            Self::Code { id, .. } => *id,
            Self::Data { id, .. } => *id,
            Self::KeyValue { id, .. } => *id,
            Self::Audit { id, .. } => *id,
        }
    }

    /// Document kind (no cx required).
    pub fn kind(&self) -> DocumentKind {
        match self {
            Self::Code { .. } => DocumentKind::Script,
            Self::Data { .. } => DocumentKind::Data,
            Self::KeyValue { .. } => DocumentKind::RedisKeyBrowser,
            Self::Audit { .. } => DocumentKind::Audit,
        }
    }

    /// Checks if this document is backed by the given file path.
    pub fn is_file(&self, path: &std::path::Path, cx: &App) -> bool {
        match self {
            Self::Code { entity, .. } => entity.read(cx).path().map(|p| p.as_path()) == Some(path),
            _ => false,
        }
    }

    /// Checks if this is a table document matching the given table.
    pub fn is_table(&self, table: &dbflux_core::TableRef, cx: &App) -> bool {
        match self {
            Self::Data { entity, .. } => entity.read(cx).table_ref(cx).as_ref() == Some(table),
            _ => false,
        }
    }

    pub fn is_collection(&self, collection: &dbflux_core::CollectionRef, cx: &App) -> bool {
        match self {
            Self::Data { entity, .. } => {
                entity.read(cx).collection_ref(cx).as_ref() == Some(collection)
            }
            _ => false,
        }
    }

    pub fn is_key_value_database(&self, profile_id: uuid::Uuid, database: &str, cx: &App) -> bool {
        match self {
            Self::KeyValue { entity, .. } => {
                let doc = entity.read(cx);
                doc.connection_id() == Some(profile_id) && doc.database_name() == database
            }
            _ => false,
        }
    }

    /// Gets metadata snapshot (requires cx to read entity).
    pub fn meta_snapshot(&self, cx: &App) -> DocumentMetaSnapshot {
        match self {
            Self::Code { id, entity } => {
                let doc = entity.read(cx);
                let icon = if doc.is_file_backed() {
                    DocumentIcon::Script
                } else {
                    DocumentIcon::Sql
                };
                DocumentMetaSnapshot {
                    id: *id,
                    kind: DocumentKind::Script,
                    title: doc.title(),
                    icon,
                    state: doc.state(),
                    closable: true,
                    connection_id: doc.connection_id(),
                }
            }
            Self::Data { id, entity } => {
                let doc = entity.read(cx);
                let icon = match doc.source_kind() {
                    DataSourceKind::Table => DocumentIcon::Table,
                    DataSourceKind::Collection => DocumentIcon::Collection,
                    DataSourceKind::QueryResult => DocumentIcon::Table,
                };
                DocumentMetaSnapshot {
                    id: *id,
                    kind: DocumentKind::Data,
                    title: doc.title(),
                    icon,
                    state: doc.state(),
                    closable: true,
                    connection_id: doc.connection_id(cx),
                }
            }
            Self::KeyValue { id, entity } => {
                let doc = entity.read(cx);
                DocumentMetaSnapshot {
                    id: *id,
                    kind: DocumentKind::RedisKeyBrowser,
                    title: doc.title(),
                    icon: DocumentIcon::Redis,
                    state: doc.state(),
                    closable: true,
                    connection_id: doc.connection_id(),
                }
            }
            Self::Audit { id, entity } => {
                let doc = entity.read(cx);
                DocumentMetaSnapshot {
                    id: *id,
                    kind: DocumentKind::Audit,
                    title: doc.title().to_string(),
                    icon: DocumentIcon::Audit,
                    state: doc.state(),
                    closable: true,
                    connection_id: None,
                }
            }
        }
    }

    /// Title for display in the tab bar.
    pub fn tab_title(&self, cx: &App) -> String {
        self.meta_snapshot(cx).title
    }

    /// Can this document be closed? (checks unsaved changes)
    pub fn can_close(&self, cx: &App) -> bool {
        match self {
            Self::Code { entity, .. } => entity.read(cx).can_close(cx),
            Self::Data { entity, .. } => entity.read(cx).can_close(),
            Self::KeyValue { entity, .. } => entity.read(cx).can_close(),
            Self::Audit { .. } => true,
        }
    }

    pub fn flush_auto_save(&self, cx: &App) {
        if let Self::Code { entity, .. } = self {
            entity.read(cx).flush_auto_save(cx);
        }
    }

    pub fn refresh_policy(&self, cx: &App) -> RefreshPolicy {
        match self {
            Self::Code { entity, .. } => entity.read(cx).refresh_policy(),
            Self::Data { entity, .. } => entity.read(cx).refresh_policy(cx),
            Self::KeyValue { entity, .. } => entity.read(cx).refresh_policy(),
            Self::Audit { .. } => RefreshPolicy::default(),
        }
    }

    pub fn set_active_tab(&self, active: bool, cx: &mut App) {
        match self {
            Self::Code { entity, .. } => {
                entity.update(cx, |doc, _cx| doc.set_active_tab(active));
            }
            Self::Data { entity, .. } => {
                entity.update(cx, |doc, cx| doc.set_active_tab(active, cx));
            }
            Self::KeyValue { entity, .. } => {
                entity.update(cx, |doc, _cx| doc.set_active_tab(active));
            }
            Self::Audit { .. } => {
                // AuditDocument doesn't need tab state
            }
        }
    }

    pub fn set_refresh_policy(&self, policy: RefreshPolicy, cx: &mut App) {
        match self {
            Self::Code { entity, .. } => {
                entity.update(cx, |doc, cx| doc.set_refresh_policy(policy, cx));
            }
            Self::Data { entity, .. } => {
                entity.update(cx, |doc, cx| doc.set_refresh_policy(policy, cx));
            }
            Self::KeyValue { entity, .. } => {
                entity.update(cx, |doc, cx| doc.set_refresh_policy(policy, cx));
            }
            Self::Audit { .. } => {
                // AuditDocument doesn't use refresh policy
            }
        }
    }

    /// Renders the document.
    pub fn render(&self) -> AnyElement {
        match self {
            Self::Code { entity, .. } => entity.clone().into_any_element(),
            Self::Data { entity, .. } => entity.clone().into_any_element(),
            Self::KeyValue { entity, .. } => entity.clone().into_any_element(),
            Self::Audit { entity, .. } => entity.clone().into_any_element(),
        }
    }

    /// Dispatch commands to the active document.
    pub fn dispatch_command(&self, cmd: Command, window: &mut Window, cx: &mut App) -> bool {
        match self {
            Self::Code { entity, .. } => {
                entity.update(cx, |doc, cx| doc.dispatch_command(cmd, window, cx))
            }
            Self::Data { entity, .. } => {
                entity.update(cx, |doc, cx| doc.dispatch_command(cmd, window, cx))
            }
            Self::KeyValue { entity, .. } => {
                entity.update(cx, |doc, cx| doc.dispatch_command(cmd, window, cx))
            }
            Self::Audit { .. } => false,
        }
    }

    /// Gives focus to the document.
    pub fn focus(&self, window: &mut Window, cx: &mut App) {
        match self {
            Self::Code { entity, .. } => {
                entity.update(cx, |doc, cx| doc.focus(window, cx));
            }
            Self::Data { entity, .. } => {
                entity.update(cx, |doc, cx| doc.focus(window, cx));
            }
            Self::KeyValue { entity, .. } => {
                entity.update(cx, |doc, cx| doc.focus(window, cx));
            }
            Self::Audit { entity, .. } => {
                entity.update(cx, |doc, cx| {
                    let _ = doc.focus_handle(cx);
                });
            }
        }
    }

    /// Returns the active context for keyboard handling.
    /// Documents determine their context based on internal focus state.
    pub fn active_context(&self, cx: &App) -> ContextId {
        match self {
            Self::Code { entity, .. } => entity.read(cx).active_context(cx),
            Self::Data { entity, .. } => entity.read(cx).active_context(cx),
            Self::KeyValue { entity, .. } => entity.read(cx).active_context(cx),
            Self::Audit { .. } => ContextId::Editor,
        }
    }

    /// Subscribe to document events (returns Subscription).
    /// Note: For Data documents, events are converted to DocumentEvent.
    pub fn subscribe<F>(&self, cx: &mut App, callback: F) -> Subscription
    where
        F: Fn(&DocumentEvent, &mut App) + 'static,
    {
        match self {
            Self::Code { entity, .. } => {
                cx.subscribe(entity, move |_entity, event, cx| callback(event, cx))
            }
            Self::Data { entity, .. } => cx.subscribe(entity, move |_entity, event, cx| {
                let doc_event = match event {
                    DataDocumentEvent::MetaChanged => DocumentEvent::MetaChanged,
                    DataDocumentEvent::RequestFocus => DocumentEvent::RequestFocus,
                    DataDocumentEvent::RequestSqlPreview {
                        context,
                        generation_type,
                    } => DocumentEvent::RequestSqlPreview {
                        context: context.clone(),
                        generation_type: *generation_type,
                    },
                };
                callback(&doc_event, cx);
            }),
            Self::KeyValue { entity, .. } => cx.subscribe(entity, move |_entity, event, cx| {
                if matches!(event, KeyValueDocumentEvent::RequestFocus) {
                    callback(&DocumentEvent::RequestFocus, cx);
                }
            }),
            Self::Audit { entity, .. } => {
                cx.subscribe(entity, move |_entity, _event, _cx| {
                    // AuditDocument doesn't emit document events yet
                })
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
    /// The document area was clicked and wants focus.
    RequestFocus,
    /// Request to show SQL preview modal (from DataGridPanel).
    RequestSqlPreview {
        context: Box<SqlPreviewContext>,
        generation_type: crate::ui::overlays::sql_preview_modal::SqlGenerationType,
    },
}
