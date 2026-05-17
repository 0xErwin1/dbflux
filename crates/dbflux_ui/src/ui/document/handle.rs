#![allow(dead_code)]

use super::audit::AuditDocument;
use super::types::{DocumentIcon, DocumentId, DocumentKind, DocumentMetaSnapshot};
use crate::keymap::{Command, ContextId};
use crate::ui::overlays::sql_preview_modal::SqlPreviewContext;
use dbflux_core::RefreshPolicy;
use gpui::{AnyElement, App, Entity, IntoElement, Subscription, Window};

/// Wrapper that allows storing different document types in a homogeneous collection.
/// The `id` is stored inline for quick access without needing `cx`.
///
/// Note: `Code` and `KeyValue` documents are no longer stored here — they are wrapped
/// in `PaneHandle` via their respective `into_pane` constructors (Arc 3 and Arc 4).
/// This enum is kept only for the remaining legacy document type (`Audit`) until
/// Arc 6 cleanup.
#[derive(Clone)]
pub enum DocumentHandle {
    /// Audit event viewer document.
    Audit {
        id: DocumentId,
        entity: Entity<AuditDocument>,
    },
}

impl DocumentHandle {
    /// Creates a new Audit document handle.
    pub fn audit(entity: Entity<AuditDocument>, cx: &App) -> Self {
        let id = entity.read(cx).id();
        Self::Audit { id, entity }
    }

    /// Document ID (no cx required).
    pub fn id(&self) -> DocumentId {
        match self {
            Self::Audit { id, .. } => *id,
        }
    }

    /// Document kind (no cx required).
    pub fn kind(&self) -> DocumentKind {
        match self {
            Self::Audit { .. } => DocumentKind::Audit,
        }
    }

    /// Checks if this document is backed by the given file path.
    ///
    /// Always returns `false` — no remaining legacy type is file-backed.
    pub fn is_file(&self, _path: &std::path::Path, _cx: &App) -> bool {
        false
    }

    /// Returns `false` for all remaining legacy types.
    ///
    /// KeyValue documents are now `Tab::Pane`; the `Tab::is_key_value_database`
    /// bridge method handles dedup via `matches_dedup_key`.
    pub fn is_key_value_database(
        &self,
        _profile_id: uuid::Uuid,
        _database: &str,
        _cx: &App,
    ) -> bool {
        false
    }

    /// Returns the connection (profile) ID for this document.
    pub fn connection_id(&self, _cx: &App) -> Option<uuid::Uuid> {
        match self {
            Self::Audit { .. } => None,
        }
    }

    /// Gets metadata snapshot (requires cx to read entity).
    pub fn meta_snapshot(&self, cx: &App) -> DocumentMetaSnapshot {
        match self {
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
    pub fn can_close(&self, _cx: &App) -> bool {
        match self {
            Self::Audit { .. } => true,
        }
    }

    pub fn flush_auto_save(&self, _cx: &App) {
        // No remaining legacy document type has auto-save.
    }

    pub fn refresh_policy(&self, _cx: &App) -> RefreshPolicy {
        match self {
            Self::Audit { .. } => RefreshPolicy::default(),
        }
    }

    pub fn set_active_tab(&self, _active: bool, _cx: &mut App) {
        match self {
            Self::Audit { .. } => {
                // AuditDocument doesn't need tab state
            }
        }
    }

    pub fn set_refresh_policy(&self, _policy: RefreshPolicy, _cx: &mut App) {
        match self {
            Self::Audit { .. } => {
                // AuditDocument doesn't use refresh policy
            }
        }
    }

    /// Renders the document.
    pub fn render(&self) -> AnyElement {
        match self {
            Self::Audit { entity, .. } => entity.clone().into_any_element(),
        }
    }

    /// Dispatch commands to the active document.
    pub fn dispatch_command(&self, cmd: Command, window: &mut Window, cx: &mut App) -> bool {
        match self {
            Self::Audit { entity, .. } => {
                entity.update(cx, |doc, cx| doc.dispatch_command(cmd, window, cx))
            }
        }
    }

    /// Gives focus to the document.
    pub fn focus(&self, window: &mut Window, cx: &mut App) {
        match self {
            Self::Audit { entity, .. } => {
                entity.update(cx, |doc, cx| doc.focus(window, cx));
            }
        }
    }

    /// Returns the active context for keyboard handling.
    pub fn active_context(&self, cx: &App) -> ContextId {
        match self {
            Self::Audit { entity, .. } => entity.read(cx).active_context(),
        }
    }

    /// Short summary of pending changes for the dirty-dot tooltip.
    pub fn change_summary(&self, _cx: &App) -> Option<String> {
        // No remaining legacy document type carries unsaved changes.
        None
    }

    /// Subscribe to document events (returns Subscription).
    pub fn subscribe<F>(&self, cx: &mut App, callback: F) -> Subscription
    where
        F: Fn(&DocumentEvent, &mut App) + 'static,
    {
        match self {
            Self::Audit { entity, .. } => {
                cx.subscribe(entity, move |_, ev: &DocumentEvent, cx| callback(ev, cx))
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
    /// Request to mount content into the workspace-level inspector rail.
    OpenInspector {
        title: gpui::SharedString,
        content: gpui::AnyView,
    },
    /// User requested "Chart this query" from a data document's context menu.
    ChartThisQuery {
        query: String,
        connection_id: Option<uuid::Uuid>,
    },
}
