#![allow(dead_code)]
#![allow(unreachable_code)]

use super::handle::DocumentHandle;
use super::types::DocumentId;
use gpui::{App, Context, EventEmitter, Subscription};
use std::collections::HashMap;

/// Manages open documents (tabs) in the workspace.
///
/// Responsibilities:
/// - Track open documents in visual order (left to right in tab bar)
/// - Track active document
/// - Maintain MRU (Most Recently Used) order for Ctrl+Tab navigation
/// - Handle document subscriptions for cleanup on close
pub struct TabManager {
    /// Documents in visual order (left to right in tab bar).
    documents: Vec<DocumentHandle>,

    /// Index of the active document (in `documents`).
    active_index: Option<usize>,

    /// MRU order for Ctrl+Tab navigation (front = most recent).
    mru_order: Vec<DocumentId>,

    /// Subscriptions per document (for cleanup on close).
    subscriptions: HashMap<DocumentId, Subscription>,
}

impl TabManager {
    pub fn new() -> Self {
        Self {
            documents: Vec::new(),
            active_index: None,
            mru_order: Vec::new(),
            subscriptions: HashMap::new(),
        }
    }

    /// Opens a new document and activates it.
    pub fn open(&mut self, doc: DocumentHandle, cx: &mut Context<Self>) {
        let id = doc.id();

        // Subscribe to document events
        // Capture the TabManager entity to emit events from within the callback
        let tab_manager = cx.entity().clone();
        let subscription = doc.subscribe(cx, move |event, cx| {
            use super::handle::DocumentEvent;
            match event {
                DocumentEvent::RequestSqlPreview {
                    profile_id,
                    schema_name,
                    table_name,
                    column_names,
                    row_values,
                    pk_indices,
                    generation_type,
                } => {
                    tab_manager.update(cx, |_, cx| {
                        cx.emit(TabManagerEvent::RequestSqlPreview {
                            profile_id: *profile_id,
                            schema_name: schema_name.clone(),
                            table_name: table_name.clone(),
                            column_names: column_names.clone(),
                            row_values: row_values.clone(),
                            pk_indices: pk_indices.clone(),
                            generation_type: *generation_type,
                        });
                    });
                }
                _ => {}
            }
        });
        self.subscriptions.insert(id, subscription);

        self.documents.push(doc);
        let new_index = self.documents.len() - 1;
        self.active_index = Some(new_index);

        // Add to front of MRU
        self.mru_order.insert(0, id);

        cx.emit(TabManagerEvent::Opened(id));
        cx.notify();
    }

    /// Closes a document by ID.
    pub fn close(&mut self, id: DocumentId, cx: &mut Context<Self>) -> bool {
        let Some(idx) = self.index_of(id) else {
            return false;
        };

        // Check if can be closed (unsaved changes check)
        if !self.documents[idx].can_close(cx) {
            return false;
        }

        // Remove document
        self.documents.remove(idx);

        // Clean up subscription
        self.subscriptions.remove(&id);

        // Remove from MRU
        self.mru_order.retain(|&i| i != id);

        // Adjust active index
        self.active_index = self.compute_new_active_after_close(idx);

        cx.emit(TabManagerEvent::Closed(id));
        cx.notify();
        true
    }

    /// Computes the new active index after closing a tab.
    fn compute_new_active_after_close(&self, closed_idx: usize) -> Option<usize> {
        if self.documents.is_empty() {
            return None;
        }

        // Try to activate the next in MRU order
        for mru_id in &self.mru_order {
            if let Some(idx) = self.index_of(*mru_id) {
                return Some(idx);
            }
        }

        // Fallback: the closest tab visually
        Some(closed_idx.min(self.documents.len() - 1))
    }

    /// Activates a document by ID.
    pub fn activate(&mut self, id: DocumentId, cx: &mut Context<Self>) {
        let Some(idx) = self.index_of(id) else {
            return;
        };

        if self.active_index == Some(idx) {
            return; // Already active
        }

        self.active_index = Some(idx);

        // Move to front of MRU
        self.mru_order.retain(|&i| i != id);
        self.mru_order.insert(0, id);

        cx.emit(TabManagerEvent::Activated(id));
        cx.notify();
    }

    /// Navigates to the next tab in VISUAL order (Ctrl+PgDn).
    pub fn next_visual_tab(&mut self, cx: &mut Context<Self>) {
        if self.documents.len() <= 1 {
            return;
        }

        if let Some(active) = self.active_index {
            let next = (active + 1) % self.documents.len();
            let id = self.documents[next].id();
            self.activate(id, cx);
        }
    }

    /// Navigates to the previous tab in VISUAL order (Ctrl+PgUp).
    pub fn prev_visual_tab(&mut self, cx: &mut Context<Self>) {
        if self.documents.len() <= 1 {
            return;
        }

        if let Some(active) = self.active_index {
            let prev = if active == 0 {
                self.documents.len() - 1
            } else {
                active - 1
            };
            let id = self.documents[prev].id();
            self.activate(id, cx);
        }
    }

    /// Navigates to the next tab in MRU order (Ctrl+Tab).
    pub fn next_mru_tab(&mut self, cx: &mut Context<Self>) {
        if self.mru_order.len() <= 1 {
            return;
        }

        // The second in MRU is the "next" most recent
        if let Some(&next_id) = self.mru_order.get(1) {
            self.activate(next_id, cx);
        }
    }

    /// Navigates to the previous tab in MRU order (Ctrl+Shift+Tab).
    pub fn prev_mru_tab(&mut self, cx: &mut Context<Self>) {
        if self.mru_order.len() <= 1 {
            return;
        }

        // The last in MRU is the "least recent"
        if let Some(&prev_id) = self.mru_order.last() {
            self.activate(prev_id, cx);
        }
    }

    /// Closes the active tab.
    pub fn close_active(&mut self, cx: &mut Context<Self>) {
        if let Some(idx) = self.active_index {
            let id = self.documents[idx].id();
            self.close(id, cx);
        }
    }

    /// Switches to tab by 1-based number (Ctrl+1 through Ctrl+9).
    pub fn switch_to_tab(&mut self, n: usize, cx: &mut Context<Self>) {
        if n == 0 || n > self.documents.len() {
            return;
        }
        let id = self.documents[n - 1].id();
        self.activate(id, cx);
    }

    /// Finds a document by ID.
    fn index_of(&self, id: DocumentId) -> Option<usize> {
        self.documents.iter().position(|d| d.id() == id)
    }

    /// Returns the active document.
    pub fn active_document(&self) -> Option<&DocumentHandle> {
        self.active_index.and_then(|i| self.documents.get(i))
    }

    /// Returns the active document ID.
    pub fn active_id(&self) -> Option<DocumentId> {
        self.active_document().map(|d| d.id())
    }

    /// Returns the active document index.
    pub fn active_index(&self) -> Option<usize> {
        self.active_index
    }

    /// Returns all documents (for TabBar).
    pub fn documents(&self) -> &[DocumentHandle] {
        &self.documents
    }

    /// Finds a document by ID.
    pub fn document(&self, id: DocumentId) -> Option<&DocumentHandle> {
        self.documents.iter().find(|d| d.id() == id)
    }

    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    pub fn len(&self) -> usize {
        self.documents.len()
    }

    /// Moves a tab from one position to another (for drag & drop).
    #[allow(unused_variables)]
    pub fn move_tab(&mut self, from: usize, to: usize, cx: &mut Context<Self>) {
        if from == to || from >= self.documents.len() || to >= self.documents.len() {
            return;
        }

        let doc = self.documents.remove(from);
        self.documents.insert(to, doc);

        // Adjust active_index if needed
        if let Some(active) = self.active_index {
            self.active_index = Some(if active == from {
                to
            } else if from < active && active <= to {
                active - 1
            } else if to <= active && active < from {
                active + 1
            } else {
                active
            });
        }

        cx.emit(TabManagerEvent::Reordered);
        cx.notify();
    }
}

impl Default for TabManager {
    fn default() -> Self {
        Self::new()
    }
}

impl EventEmitter<TabManagerEvent> for TabManager {}

#[derive(Clone, Debug)]
pub enum TabManagerEvent {
    Opened(DocumentId),
    Closed(DocumentId),
    Activated(DocumentId),
    Reordered,
    /// A document requested focus (user clicked on it).
    DocumentRequestedFocus,
    /// A document requested SQL preview modal.
    RequestSqlPreview {
        profile_id: uuid::Uuid,
        schema_name: Option<String>,
        table_name: String,
        column_names: Vec<String>,
        row_values: Vec<String>,
        pk_indices: Vec<usize>,
        generation_type: crate::ui::sql_preview_modal::SqlGenerationType,
    },
}
