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
            tab_manager.update(cx, |_, cx| match event {
                DocumentEvent::RequestFocus => {
                    cx.emit(TabManagerEvent::DocumentRequestedFocus);
                }
                DocumentEvent::RequestSqlPreview {
                    context,
                    generation_type,
                } => {
                    cx.emit(TabManagerEvent::RequestSqlPreview {
                        context: context.clone(),
                        generation_type: *generation_type,
                    });
                }
                _ => {}
            });
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

        self.documents[idx].flush_auto_save(cx);
        self.remove_document(idx, id, cx);
        true
    }

    fn remove_document(&mut self, idx: usize, id: DocumentId, cx: &mut Context<Self>) {
        self.documents.remove(idx);
        self.subscriptions.remove(&id);
        self.mru_order.retain(|&i| i != id);
        self.active_index = self.compute_new_active_after_close(idx);

        cx.emit(TabManagerEvent::Closed(id));
        cx.notify();
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

    pub fn close_others(&mut self, keep_id: DocumentId, cx: &mut Context<Self>) {
        let ids_to_close: Vec<DocumentId> = self
            .documents
            .iter()
            .map(|d| d.id())
            .filter(|&id| id != keep_id)
            .collect();

        for id in ids_to_close {
            self.close(id, cx);
        }
    }

    pub fn close_all(&mut self, cx: &mut Context<Self>) {
        let ids: Vec<DocumentId> = self.documents.iter().map(|d| d.id()).collect();

        for id in ids {
            self.close(id, cx);
        }
    }

    pub fn close_to_left(&mut self, id: DocumentId, cx: &mut Context<Self>) {
        let Some(target_idx) = self.index_of(id) else {
            return;
        };

        let ids_to_close: Vec<DocumentId> = self.documents[..target_idx]
            .iter()
            .map(|d| d.id())
            .collect();

        for id in ids_to_close {
            self.close(id, cx);
        }
    }

    pub fn close_to_right(&mut self, id: DocumentId, cx: &mut Context<Self>) {
        let Some(target_idx) = self.index_of(id) else {
            return;
        };

        let ids_to_close: Vec<DocumentId> = self.documents[(target_idx + 1)..]
            .iter()
            .map(|d| d.id())
            .collect();

        for id in ids_to_close {
            self.close(id, cx);
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

fn ids_to_close_others(all_ids: &[DocumentId], keep_id: DocumentId) -> Vec<DocumentId> {
    all_ids
        .iter()
        .copied()
        .filter(|&id| id != keep_id)
        .collect()
}

fn ids_to_close_left(all_ids: &[DocumentId], target_id: DocumentId) -> Vec<DocumentId> {
    let Some(idx) = all_ids.iter().position(|&id| id == target_id) else {
        return Vec::new();
    };
    all_ids[..idx].to_vec()
}

fn ids_to_close_right(all_ids: &[DocumentId], target_id: DocumentId) -> Vec<DocumentId> {
    let Some(idx) = all_ids.iter().position(|&id| id == target_id) else {
        return Vec::new();
    };
    all_ids[(idx + 1)..].to_vec()
}

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
        context: Box<crate::ui::overlays::sql_preview_modal::SqlPreviewContext>,
        generation_type: crate::ui::overlays::sql_preview_modal::SqlGenerationType,
    },
}

#[cfg(test)]
mod tests {
    use super::{DocumentId, ids_to_close_left, ids_to_close_others, ids_to_close_right};
    use uuid::Uuid;

    fn make_ids(n: usize) -> Vec<DocumentId> {
        (0..n).map(|_| DocumentId(Uuid::new_v4())).collect()
    }

    #[test]
    fn close_others_excludes_keep_id() {
        let ids = make_ids(5);
        let keep = ids[2];
        let result = ids_to_close_others(&ids, keep);

        assert_eq!(result.len(), 4);
        assert!(!result.contains(&keep));
        assert!(result.contains(&ids[0]));
        assert!(result.contains(&ids[1]));
        assert!(result.contains(&ids[3]));
        assert!(result.contains(&ids[4]));
    }

    #[test]
    fn close_others_with_single_tab_returns_empty() {
        let ids = make_ids(1);
        let result = ids_to_close_others(&ids, ids[0]);
        assert!(result.is_empty());
    }

    #[test]
    fn close_left_returns_ids_before_target() {
        let ids = make_ids(5);
        let result = ids_to_close_left(&ids, ids[3]);

        assert_eq!(result.len(), 3);
        assert_eq!(result, &ids[..3]);
    }

    #[test]
    fn close_left_at_first_position_returns_empty() {
        let ids = make_ids(5);
        let result = ids_to_close_left(&ids, ids[0]);
        assert!(result.is_empty());
    }

    #[test]
    fn close_left_with_unknown_id_returns_empty() {
        let ids = make_ids(3);
        let unknown = DocumentId(Uuid::new_v4());
        let result = ids_to_close_left(&ids, unknown);
        assert!(result.is_empty());
    }

    #[test]
    fn close_right_returns_ids_after_target() {
        let ids = make_ids(5);
        let result = ids_to_close_right(&ids, ids[1]);

        assert_eq!(result.len(), 3);
        assert_eq!(result, &ids[2..]);
    }

    #[test]
    fn close_right_at_last_position_returns_empty() {
        let ids = make_ids(5);
        let result = ids_to_close_right(&ids, ids[4]);
        assert!(result.is_empty());
    }

    #[test]
    fn close_right_with_unknown_id_returns_empty() {
        let ids = make_ids(3);
        let unknown = DocumentId(Uuid::new_v4());
        let result = ids_to_close_right(&ids, unknown);
        assert!(result.is_empty());
    }
}
