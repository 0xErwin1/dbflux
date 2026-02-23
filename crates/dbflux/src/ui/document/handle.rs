#![allow(dead_code)]

use super::data_document::{DataDocument, DataDocumentEvent};
use super::key_value::{KeyValueDocument, KeyValueDocumentEvent};
use super::sql_query::SqlQueryDocument;
use super::types::{DataSourceKind, DocumentIcon, DocumentId, DocumentKind, DocumentMetaSnapshot};
use crate::keymap::{Command, ContextId};
use dbflux_core::{RefreshPolicy, Value};
use gpui::{AnyElement, App, Entity, IntoElement, Subscription, Window};

/// Wrapper that allows storing different document types in a homogeneous collection.
/// The `id` is stored inline for quick access without needing `cx`.
///
/// Each variant stores the DocumentId inline plus the Entity<T> for the actual document.
#[derive(Clone)]
pub enum DocumentHandle {
    /// SQL script with editor + embedded results.
    SqlQuery {
        id: DocumentId,
        entity: Entity<SqlQueryDocument>,
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
}

impl DocumentHandle {
    /// Creates a new SqlQuery document handle.
    pub fn sql_query(entity: Entity<SqlQueryDocument>, cx: &App) -> Self {
        let id = entity.read(cx).id();
        Self::SqlQuery { id, entity }
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

    /// Document ID (no cx required).
    pub fn id(&self) -> DocumentId {
        match self {
            Self::SqlQuery { id, .. } => *id,
            Self::Data { id, .. } => *id,
            Self::KeyValue { id, .. } => *id,
        }
    }

    /// Document kind (no cx required).
    pub fn kind(&self) -> DocumentKind {
        match self {
            Self::SqlQuery { .. } => DocumentKind::Script,
            Self::Data { .. } => DocumentKind::Data,
            Self::KeyValue { .. } => DocumentKind::RedisKeyBrowser,
        }
    }

    /// Checks if this document is backed by the given file path.
    pub fn is_file(&self, path: &std::path::Path, cx: &App) -> bool {
        match self {
            Self::SqlQuery { entity, .. } => {
                entity.read(cx).path().map(|p| p.as_path()) == Some(path)
            }
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
            Self::SqlQuery { id, entity } => {
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
        }
    }

    /// Title for display in the tab bar.
    pub fn tab_title(&self, cx: &App) -> String {
        self.meta_snapshot(cx).title
    }

    /// Can this document be closed? (checks unsaved changes)
    pub fn can_close(&self, cx: &App) -> bool {
        match self {
            Self::SqlQuery { entity, .. } => entity.read(cx).can_close(cx),
            Self::Data { entity, .. } => entity.read(cx).can_close(),
            Self::KeyValue { entity, .. } => entity.read(cx).can_close(),
        }
    }

    pub fn flush_auto_save(&self, cx: &App) {
        if let Self::SqlQuery { entity, .. } = self {
            entity.read(cx).flush_auto_save(cx);
        }
    }

    pub fn refresh_policy(&self, cx: &App) -> RefreshPolicy {
        match self {
            Self::SqlQuery { entity, .. } => entity.read(cx).refresh_policy(),
            Self::Data { entity, .. } => entity.read(cx).refresh_policy(cx),
            Self::KeyValue { entity, .. } => entity.read(cx).refresh_policy(),
        }
    }

    pub fn set_refresh_policy(&self, policy: RefreshPolicy, cx: &mut App) {
        match self {
            Self::SqlQuery { entity, .. } => {
                entity.update(cx, |doc, cx| doc.set_refresh_policy(policy, cx));
            }
            Self::Data { entity, .. } => {
                entity.update(cx, |doc, cx| doc.set_refresh_policy(policy, cx));
            }
            Self::KeyValue { entity, .. } => {
                entity.update(cx, |doc, cx| doc.set_refresh_policy(policy, cx));
            }
        }
    }

    /// Renders the document.
    pub fn render(&self) -> AnyElement {
        match self {
            Self::SqlQuery { entity, .. } => entity.clone().into_any_element(),
            Self::Data { entity, .. } => entity.clone().into_any_element(),
            Self::KeyValue { entity, .. } => entity.clone().into_any_element(),
        }
    }

    /// Dispatch commands to the active document.
    pub fn dispatch_command(&self, cmd: Command, window: &mut Window, cx: &mut App) -> bool {
        match self {
            Self::SqlQuery { entity, .. } => {
                entity.update(cx, |doc, cx| doc.dispatch_command(cmd, window, cx))
            }
            Self::Data { entity, .. } => {
                entity.update(cx, |doc, cx| doc.dispatch_command(cmd, window, cx))
            }
            Self::KeyValue { entity, .. } => {
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
            Self::Data { entity, .. } => {
                entity.update(cx, |doc, cx| doc.focus(window, cx));
            }
            Self::KeyValue { entity, .. } => {
                entity.update(cx, |doc, cx| doc.focus(window, cx));
            }
        }
    }

    /// Returns the active context for keyboard handling.
    /// Documents determine their context based on internal focus state.
    pub fn active_context(&self, cx: &App) -> ContextId {
        match self {
            Self::SqlQuery { entity, .. } => entity.read(cx).active_context(cx),
            Self::Data { entity, .. } => entity.read(cx).active_context(cx),
            Self::KeyValue { entity, .. } => entity.read(cx).active_context(cx),
        }
    }

    /// Subscribe to document events (returns Subscription).
    /// Note: For Data documents, events are converted to DocumentEvent.
    pub fn subscribe<F>(&self, cx: &mut App, callback: F) -> Subscription
    where
        F: Fn(&DocumentEvent, &mut App) + 'static,
    {
        match self {
            Self::SqlQuery { entity, .. } => {
                cx.subscribe(entity, move |_entity, event, cx| callback(event, cx))
            }
            Self::Data { entity, .. } => cx.subscribe(entity, move |_entity, event, cx| {
                let doc_event = match event {
                    DataDocumentEvent::MetaChanged => DocumentEvent::MetaChanged,
                    DataDocumentEvent::RequestFocus => DocumentEvent::RequestFocus,
                    DataDocumentEvent::RequestSqlPreview {
                        profile_id,
                        schema_name,
                        table_name,
                        column_names,
                        row_values,
                        pk_indices,
                        generation_type,
                    } => DocumentEvent::RequestSqlPreview {
                        profile_id: *profile_id,
                        schema_name: schema_name.clone(),
                        table_name: table_name.clone(),
                        column_names: column_names.clone(),
                        row_values: row_values.clone(),
                        pk_indices: pk_indices.clone(),
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
        profile_id: uuid::Uuid,
        schema_name: Option<String>,
        table_name: String,
        column_names: Vec<String>,
        row_values: Vec<Value>,
        pk_indices: Vec<usize>,
        generation_type: crate::ui::sql_preview_modal::SqlGenerationType,
    },
}
