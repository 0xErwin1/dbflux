//! `PaneHandle` — type-erased shell for open documents.
//!
//! Each open document is wrapped in a `PaneHandle` whose fields are closures
//! that capture the typed `Entity<T>`. Callers interact through these forwarding
//! methods and never observe the concrete document type.
//!
//! `PaneHandle` is NOT `Clone`. Callers that previously cloned `DocumentHandle`
//! should access documents through `TabManager::with_pane(id, |p| ...)` instead.

#![allow(clippy::type_complexity)]

use super::dedup::DocumentKey;
use super::handle::DocumentEvent;
use super::types::{DocumentId, DocumentKind, DocumentMetaSnapshot};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_core::RefreshPolicy;
use gpui::{AnyElement, App, Subscription, Window};

/// Type-erased callback for document events, used by the `subscribe` closure.
pub type BoxedDocEventCallback = Box<dyn Fn(&DocumentEvent, &mut App) + 'static>;

/// A snapshot of a code document's session state, used to reconstruct tabs
/// on next launch and to write the session manifest.
///
/// Fields carry all data that `write_session_manifest` previously read directly
/// from `DocumentHandle::Code { entity, .. }`. The `kind` field maps to the
/// `tab_kind` column in `WorkspaceTab` (values: `"FileBacked"`, `"Scratch"`).
#[derive(Clone)]
pub struct CodeSessionTabSnapshot {
    /// `"FileBacked"` or `"Scratch"` — maps to `WorkspaceTab::tab_kind`.
    pub kind: &'static str,
    pub id: super::types::DocumentId,
    pub title: String,
    pub language: dbflux_core::QueryLanguage,
    pub exec_ctx: dbflux_core::ExecutionContext,
    pub file_path: Option<std::path::PathBuf>,
    pub scratch_path: Option<std::path::PathBuf>,
    pub shadow_path: Option<std::path::PathBuf>,
}

/// Type-erased shell for an open document.
///
/// All 22 operations from the `DocumentHandle` interface are exposed here
/// without leaking the concrete entity type. Each field is a heap-allocated
/// closure capturing the typed `Entity<T>`.
///
/// GPUI constraint: closures capture `Entity<T>` (which is `Clone + 'static`).
/// `Window` and `App` are always passed as per-call parameters and never captured.
pub struct PaneHandle {
    /// Document ID — cheap, sync, no `cx` required.
    id: DocumentId,

    /// Document kind — cheap, sync, no `cx` required.
    kind: DocumentKind,

    // --- Rendering and behaviour (per-call Window + App) ---
    render: Box<dyn Fn(&mut Window, &mut App) -> AnyElement>,
    focus: Box<dyn Fn(&mut Window, &mut App)>,
    dispatch_command: Box<dyn Fn(Command, &mut Window, &mut App) -> bool>,

    // --- Pure reads (shared &App) ---
    meta_snapshot: Box<dyn Fn(&App) -> DocumentMetaSnapshot>,
    tab_title: Box<dyn Fn(&App) -> String>,
    can_close: Box<dyn Fn(&App) -> bool>,
    connection_id: Box<dyn Fn(&App) -> Option<uuid::Uuid>>,
    active_context: Box<dyn Fn(&App) -> ContextId>,
    change_summary: Box<dyn Fn(&App) -> Option<String>>,
    refresh_policy: Box<dyn Fn(&App) -> RefreshPolicy>,

    // --- Side-effect reads (shared &App) ---
    flush_auto_save: Box<dyn Fn(&App)>,

    // --- Mutations (&mut App) ---
    set_active_tab: Box<dyn Fn(bool, &mut App)>,
    set_refresh_policy: Box<dyn Fn(RefreshPolicy, &mut App)>,

    // --- Dedup (&App, since all current is_* only call entity.read(cx)) ---
    matches_dedup_key: Box<dyn Fn(&DocumentKey, &App) -> bool>,

    // --- Subscription ---
    subscribe: Box<dyn Fn(&mut App, BoxedDocEventCallback) -> Subscription>,

    // --- Optional document-specific helpers ---
    // Populated only when the pane supports the operation; `None` means
    // the call site should skip or no-op.
    /// Sets the category filter on audit-style documents.
    pub set_category_filter: Option<Box<dyn Fn(Option<String>, &mut App)>>,

    /// Sets (or clears) the correlation-id filter on audit-style documents.
    ///
    /// `Some(id_string)` applies the filter; `None` clears it.
    /// `None` on the outer `Option` means the document does not support this operation.
    pub set_correlation_filter: Option<Box<dyn Fn(Option<String>, &mut App)>>,

    /// Returns true when this pane matches a given event-stream target.
    pub matches_event_stream:
        Option<Box<dyn Fn(uuid::Uuid, &dbflux_core::EventStreamTarget, &App) -> bool>>,

    /// Returns `Some(path)` when the document is file-backed and empty
    /// (used by the empty-file-close cleanup in `actions.rs`).
    pub is_file_backed_empty: Option<Box<dyn Fn(&App) -> Option<std::path::PathBuf>>>,

    /// Returns a session snapshot for code documents (used by session manifest).
    pub session_tab_snapshot: Option<Box<dyn Fn(&App) -> Option<CodeSessionTabSnapshot>>>,

    /// Tells the document that the workspace inspector was dismissed by the
    /// user (× button or ESC). Documents that own inspector state clear it
    /// here so the rail stays closed on subsequent tab activations.
    pub mark_inspector_closed: Option<Box<dyn Fn(&mut App)>>,
}

impl PaneHandle {
    /// Constructs a `PaneHandle` for documents that have no optional helpers.
    ///
    /// Called by per-document `into_pane` constructors for simple documents
    /// (Chart, KeyValue) that do not need `set_category_filter`,
    /// `matches_event_stream`, `is_file_backed_empty`, or `session_tab_snapshot`.
    #[allow(clippy::too_many_arguments)]
    pub fn new_chart(
        id: DocumentId,
        kind: DocumentKind,
        render: Box<dyn Fn(&mut Window, &mut App) -> AnyElement>,
        focus: Box<dyn Fn(&mut Window, &mut App)>,
        dispatch_command: Box<dyn Fn(Command, &mut Window, &mut App) -> bool>,
        meta_snapshot: Box<dyn Fn(&App) -> DocumentMetaSnapshot>,
        tab_title: Box<dyn Fn(&App) -> String>,
        can_close: Box<dyn Fn(&App) -> bool>,
        connection_id: Box<dyn Fn(&App) -> Option<uuid::Uuid>>,
        active_context: Box<dyn Fn(&App) -> ContextId>,
        change_summary: Box<dyn Fn(&App) -> Option<String>>,
        refresh_policy: Box<dyn Fn(&App) -> RefreshPolicy>,
        flush_auto_save: Box<dyn Fn(&App)>,
        set_active_tab: Box<dyn Fn(bool, &mut App)>,
        set_refresh_policy: Box<dyn Fn(RefreshPolicy, &mut App)>,
        matches_dedup_key: Box<dyn Fn(&DocumentKey, &App) -> bool>,
        subscribe: Box<dyn Fn(&mut App, BoxedDocEventCallback) -> Subscription>,
    ) -> Self {
        Self {
            id,
            kind,
            render,
            focus,
            dispatch_command,
            meta_snapshot,
            tab_title,
            can_close,
            connection_id,
            active_context,
            change_summary,
            refresh_policy,
            flush_auto_save,
            set_active_tab,
            set_refresh_policy,
            matches_dedup_key,
            subscribe,
            set_category_filter: None,
            set_correlation_filter: None,
            matches_event_stream: None,
            is_file_backed_empty: None,
            session_tab_snapshot: None,
            mark_inspector_closed: None,
        }
    }

    /// Document ID — does not require `cx`.
    pub fn id(&self) -> DocumentId {
        self.id
    }

    /// Document kind — does not require `cx`.
    pub fn kind(&self) -> DocumentKind {
        self.kind
    }

    /// Renders the document into a GPUI element.
    pub fn render(&self, window: &mut Window, cx: &mut App) -> AnyElement {
        (self.render)(window, cx)
    }

    /// Transfers focus to the document's primary focus handle.
    pub fn focus(&self, window: &mut Window, cx: &mut App) {
        (self.focus)(window, cx)
    }

    /// Dispatches a keymap command to the document.
    ///
    /// Returns `true` if the command was handled.
    pub fn dispatch_command(&self, cmd: Command, window: &mut Window, cx: &mut App) -> bool {
        (self.dispatch_command)(cmd, window, cx)
    }

    /// Returns a cheap metadata snapshot for the tab bar.
    pub fn meta_snapshot(&self, cx: &App) -> DocumentMetaSnapshot {
        (self.meta_snapshot)(cx)
    }

    /// Returns the display title for the tab bar.
    pub fn tab_title(&self, cx: &App) -> String {
        (self.tab_title)(cx)
    }

    /// Returns `true` when the document can be closed without data loss.
    pub fn can_close(&self, cx: &App) -> bool {
        (self.can_close)(cx)
    }

    /// Returns the connection (profile) ID, if any.
    pub fn connection_id(&self, cx: &App) -> Option<uuid::Uuid> {
        (self.connection_id)(cx)
    }

    /// Returns the active keyboard context for this document.
    pub fn active_context(&self, cx: &App) -> ContextId {
        (self.active_context)(cx)
    }

    /// Returns a short description of pending changes for the dirty-dot tooltip.
    pub fn change_summary(&self, cx: &App) -> Option<String> {
        (self.change_summary)(cx)
    }

    /// Returns the current refresh policy.
    pub fn refresh_policy(&self, cx: &App) -> RefreshPolicy {
        (self.refresh_policy)(cx)
    }

    /// Flushes any pending auto-save for file-backed documents.
    pub fn flush_auto_save(&self, cx: &App) {
        (self.flush_auto_save)(cx)
    }

    /// Notifies the document that it became (or stopped being) the active tab.
    pub fn set_active_tab(&self, active: bool, cx: &mut App) {
        (self.set_active_tab)(active, cx)
    }

    /// Updates the refresh policy on this document.
    pub fn set_refresh_policy(&self, policy: RefreshPolicy, cx: &mut App) {
        (self.set_refresh_policy)(policy, cx)
    }

    /// Returns `true` when this pane's identity matches `key`.
    ///
    /// Used by `TabManager::find_by_key` for deduplication.
    pub fn matches_dedup_key(&self, key: &DocumentKey, cx: &App) -> bool {
        (self.matches_dedup_key)(key, cx)
    }

    /// Subscribes to document events.
    ///
    /// The returned `Subscription` must be stored; dropping it cancels delivery.
    pub fn subscribe<F>(&self, cx: &mut App, callback: F) -> Subscription
    where
        F: Fn(&DocumentEvent, &mut App) + 'static,
    {
        (self.subscribe)(cx, Box::new(callback))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Structural compile-time test: `PaneHandle` and `CodeSessionTabSnapshot`
    /// exist and the public type alias `BoxedDocEventCallback` is accessible.
    ///
    /// This test cannot construct a `PaneHandle` (all fields are private closures
    /// with no public constructor yet — constructors come in Arc 1). Instead it
    /// verifies that the associated types and the `CodeSessionTabSnapshot` struct
    /// compile without issue.
    #[test]
    fn code_session_tab_snapshot_constructs_and_clones() {
        use dbflux_core::{ExecutionContext, QueryLanguage};

        let snap = CodeSessionTabSnapshot {
            kind: "Scratch",
            id: super::super::types::DocumentId::new(),
            title: "Query 1".to_string(),
            language: QueryLanguage::Sql,
            exec_ctx: ExecutionContext::default(),
            file_path: None,
            scratch_path: Some(std::path::PathBuf::from("/tmp/scratch.sql")),
            shadow_path: None,
        };

        let cloned = snap.clone();
        assert_eq!(cloned.kind, "Scratch");
        assert!(cloned.file_path.is_none());
        assert!(cloned.scratch_path.is_some());
    }

    /// Verify that `BoxedDocEventCallback` is a valid type alias by constructing
    /// a value that satisfies it. This is a compile-time shape test.
    #[test]
    fn boxed_doc_event_callback_type_alias_is_valid() {
        // The closure type must match `Box<dyn Fn(&DocumentEvent, &mut App) + 'static>`.
        // We just verify that the alias exists and a correctly-shaped closure
        // compiles into it — we do not actually call it.
        let _cb: BoxedDocEventCallback = Box::new(|_event: &DocumentEvent, _cx: &mut App| {});
    }
}
