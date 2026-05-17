#![allow(dead_code)]

use crate::keymap::{Command, ContextId};
use crate::ui::overlays::sql_preview_modal::SqlPreviewContext;
use dbflux_core::RefreshPolicy;
use gpui::{AnyElement, App, Subscription, Window};

use super::types::{DocumentIcon, DocumentId, DocumentKind, DocumentMetaSnapshot};

/// Wrapper that allows storing different document types in a homogeneous collection.
///
/// All document types have been migrated to `PaneHandle` via their respective
/// `into_pane` constructors (Arcs 1–5). This enum is now empty and will be deleted
/// in Arc 6 cleanup (T-6-1). The match arms in `tab_manager.rs` are unreachable
/// in practice (no Legacy tabs are constructed), but they still compile because
/// a match on an uninhabited enum is exhaustive.
#[derive(Clone)]
pub enum DocumentHandle {}

impl DocumentHandle {
    pub fn id(&self) -> DocumentId {
        match *self {}
    }

    pub fn kind(&self) -> DocumentKind {
        match *self {}
    }

    pub fn is_file(&self, _path: &std::path::Path, _cx: &App) -> bool {
        match *self {}
    }

    pub fn is_key_value_database(
        &self,
        _profile_id: uuid::Uuid,
        _database: &str,
        _cx: &App,
    ) -> bool {
        match *self {}
    }

    pub fn connection_id(&self, _cx: &App) -> Option<uuid::Uuid> {
        match *self {}
    }

    pub fn meta_snapshot(&self, _cx: &App) -> DocumentMetaSnapshot {
        match *self {}
    }

    pub fn tab_title(&self, _cx: &App) -> String {
        match *self {}
    }

    pub fn can_close(&self, _cx: &App) -> bool {
        match *self {}
    }

    pub fn flush_auto_save(&self, _cx: &App) {
        match *self {}
    }

    pub fn refresh_policy(&self, _cx: &App) -> RefreshPolicy {
        match *self {}
    }

    pub fn set_active_tab(&self, _active: bool, _cx: &mut App) {
        match *self {}
    }

    pub fn set_refresh_policy(&self, _policy: RefreshPolicy, _cx: &mut App) {
        match *self {}
    }

    pub fn render(&self) -> AnyElement {
        match *self {}
    }

    pub fn dispatch_command(&self, _cmd: Command, _window: &mut Window, _cx: &mut App) -> bool {
        match *self {}
    }

    pub fn focus(&self, _window: &mut Window, _cx: &mut App) {
        match *self {}
    }

    pub fn active_context(&self, _cx: &App) -> ContextId {
        match *self {}
    }

    pub fn change_summary(&self, _cx: &App) -> Option<String> {
        match *self {}
    }

    pub fn subscribe<F>(&self, _cx: &mut App, _callback: F) -> Subscription
    where
        F: Fn(&DocumentEvent, &mut App) + 'static,
    {
        match *self {}
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
