use std::collections::{BTreeMap, HashSet};

use dbflux_core::{QueryResult, Value};
use gpui::{
    AppContext, Context, EventEmitter, FocusHandle, Focusable, UniformListScrollHandle, Window,
};
use gpui_component::input::{InputEvent, InputState};

use super::events::{DocumentTreeEvent, TreeDirection};
use super::node::{NodeId, NodeValue, TreeNode};

/// View mode for the document tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DocumentViewMode {
    /// Tree view with collapsible nodes.
    #[default]
    Tree,
    /// Raw JSON view.
    Raw,
}

/// State for the DocumentTree component.
pub struct DocumentTreeState {
    /// Root nodes (one per document in the result set).
    documents: Vec<TreeNode>,

    /// Raw document values for serialization.
    raw_documents: Vec<Value>,

    /// Set of expanded node IDs.
    expanded: HashSet<NodeId>,

    /// Currently focused/cursor node.
    cursor: Option<NodeId>,

    /// Focus handle for keyboard input.
    focus_handle: FocusHandle,

    /// Scroll handle for virtualized list.
    vertical_scroll: UniformListScrollHandle,

    /// Cache of visible nodes (flattened tree with collapsed branches hidden).
    visible_nodes: Vec<TreeNode>,

    /// Dirty flag to rebuild visible_nodes.
    needs_rebuild: bool,

    /// Current view mode (Tree or Raw JSON).
    view_mode: DocumentViewMode,

    /// Cached raw JSON string for the Raw view mode.
    raw_json_cache: Option<String>,

    /// Set of node IDs whose values are expanded inline.
    expanded_values: HashSet<NodeId>,

    /// Current search query (None if search is not active).
    search_query: Option<String>,

    /// Node IDs that match the current search.
    search_matches: Vec<NodeId>,

    /// Index of the currently focused match.
    current_match_index: Option<usize>,

    /// Whether search input is visible.
    search_visible: bool,

    /// Node currently being edited inline.
    editing_node: Option<NodeId>,

    /// Input state for inline value editing.
    inline_edit_input: Option<gpui::Entity<InputState>>,
}

impl DocumentTreeState {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            documents: Vec::new(),
            raw_documents: Vec::new(),
            expanded: HashSet::new(),
            cursor: None,
            focus_handle: cx.focus_handle(),
            vertical_scroll: UniformListScrollHandle::new(),
            visible_nodes: Vec::new(),
            needs_rebuild: true,
            view_mode: DocumentViewMode::default(),
            raw_json_cache: None,
            expanded_values: HashSet::new(),
            search_query: None,
            search_matches: Vec::new(),
            current_match_index: None,
            search_visible: false,
            editing_node: None,
            inline_edit_input: None,
        }
    }

    /// Load documents from a QueryResult.
    pub fn load_from_result(&mut self, result: &QueryResult, cx: &mut Context<Self>) {
        self.documents.clear();
        self.raw_documents.clear();
        self.expanded.clear();
        self.expanded_values.clear();
        self.cursor = None;
        self.needs_rebuild = true;
        self.raw_json_cache = None;
        self.search_query = None;
        self.search_matches.clear();
        self.current_match_index = None;
        self.search_visible = false;
        self.editing_node = None;
        self.inline_edit_input = None;

        // For document databases, each row is a single document stored in the first column
        // The _id column is typically the first, and the full document is in a "_document" column
        // or the result contains the document fields directly

        for (row_idx, row) in result.rows.iter().enumerate() {
            // Try to find the full document representation
            let doc_value = Self::extract_document_value(row, &result.columns);

            let node_id = NodeId::root(row_idx);
            let key = format!("Document {}", row_idx);
            let node_value = NodeValue::from_value(&doc_value);

            self.raw_documents.push(doc_value);
            self.documents
                .push(TreeNode::new(node_id, &key, node_value, None));
        }

        // Expand first document by default if there's only one
        if self.documents.len() == 1
            && let Some(node) = self.documents.first()
        {
            self.expanded.insert(node.id.clone());
        }

        // Set cursor to first document
        if let Some(first) = self.documents.first() {
            self.cursor = Some(first.id.clone());
        }

        self.rebuild_visible_nodes();
        cx.notify();
    }

    /// Load documents from a list of `(label, Value)` pairs.
    ///
    /// Each pair becomes a root node in the tree. Useful for displaying
    /// key-value structures (e.g., Redis Hash fields, Stream entries) without
    /// constructing a full `QueryResult`.
    pub fn load_from_values(&mut self, entries: Vec<(String, Value)>, cx: &mut Context<Self>) {
        self.documents.clear();
        self.raw_documents.clear();
        self.expanded.clear();
        self.expanded_values.clear();
        self.cursor = None;
        self.needs_rebuild = true;
        self.raw_json_cache = None;
        self.search_query = None;
        self.search_matches.clear();
        self.current_match_index = None;
        self.search_visible = false;
        self.editing_node = None;
        self.inline_edit_input = None;

        for (row_idx, (label, value)) in entries.iter().enumerate() {
            let node_id = NodeId::root(row_idx);
            let node_value = NodeValue::from_value(value);

            self.raw_documents.push(value.clone());
            self.documents
                .push(TreeNode::new(node_id, label, node_value, None));
        }

        if self.documents.len() == 1
            && let Some(node) = self.documents.first()
        {
            self.expanded.insert(node.id.clone());
        }

        if let Some(first) = self.documents.first() {
            self.cursor = Some(first.id.clone());
        }

        self.rebuild_visible_nodes();
        cx.notify();
    }

    /// Extract the document value from a result row.
    fn extract_document_value(row: &[Value], columns: &[dbflux_core::ColumnMeta]) -> Value {
        // If there's a _document column, use that
        if let Some(doc_col_idx) = columns.iter().position(|c| c.name == "_document")
            && let Some(doc_val) = row.get(doc_col_idx)
            && matches!(doc_val, Value::Document(_))
        {
            return doc_val.clone();
        }

        // Otherwise, construct a document from all columns
        let fields: BTreeMap<String, Value> = columns
            .iter()
            .zip(row.iter())
            .map(|(col, val)| (col.name.clone(), val.clone()))
            .collect();

        Value::Document(fields)
    }

    // === Accessors ===

    pub fn visible_nodes(&mut self) -> &[TreeNode] {
        if self.needs_rebuild {
            self.rebuild_visible_nodes();
        }
        &self.visible_nodes
    }

    pub fn visible_node_count(&mut self) -> usize {
        if self.needs_rebuild {
            self.rebuild_visible_nodes();
        }
        self.visible_nodes.len()
    }

    pub fn cursor(&self) -> Option<&NodeId> {
        self.cursor.as_ref()
    }

    pub fn editing_node(&self) -> Option<&NodeId> {
        self.editing_node.as_ref()
    }

    pub fn inline_edit_input(&self) -> Option<&gpui::Entity<InputState>> {
        self.inline_edit_input.as_ref()
    }

    /// Get raw document value by index (for copy/preview operations).
    pub fn get_raw_document(&self, doc_index: usize) -> Option<&Value> {
        self.raw_documents.get(doc_index)
    }

    pub fn set_cursor(&mut self, id: &NodeId, cx: &mut Context<Self>) {
        if self.cursor.as_ref() != Some(id) {
            self.cursor = Some(id.clone());
            cx.emit(DocumentTreeEvent::CursorMoved);
            cx.notify();
        }
    }

    pub fn is_expanded(&self, id: &NodeId) -> bool {
        self.expanded.contains(id)
    }

    pub fn scroll_handle(&self) -> &UniformListScrollHandle {
        &self.vertical_scroll
    }

    // === View Mode ===

    pub fn view_mode(&self) -> DocumentViewMode {
        self.view_mode
    }

    pub fn toggle_view_mode(&mut self, cx: &mut Context<Self>) {
        self.view_mode = match self.view_mode {
            DocumentViewMode::Tree => DocumentViewMode::Raw,
            DocumentViewMode::Raw => DocumentViewMode::Tree,
        };
        cx.emit(DocumentTreeEvent::ViewModeToggled);
        cx.notify();
    }

    pub fn raw_json(&mut self) -> &str {
        if self.raw_json_cache.is_none() {
            let json_values: Vec<serde_json::Value> =
                self.raw_documents.iter().map(value_to_json).collect();

            let json_str = if json_values.len() == 1 {
                serde_json::to_string_pretty(&json_values[0]).unwrap_or_default()
            } else {
                serde_json::to_string_pretty(&json_values).unwrap_or_default()
            };

            self.raw_json_cache = Some(json_str);
        }

        self.raw_json_cache.as_deref().unwrap_or("")
    }

    // === Value Expansion ===

    pub fn is_value_expanded(&self, id: &NodeId) -> bool {
        self.expanded_values.contains(id)
    }

    pub fn toggle_value_expand(&mut self, id: &NodeId, cx: &mut Context<Self>) {
        if self.expanded_values.contains(id) {
            self.expanded_values.remove(id);
        } else {
            self.expanded_values.insert(id.clone());
        }
        cx.notify();
    }

    // === Search ===

    pub fn is_search_visible(&self) -> bool {
        self.search_visible
    }

    #[allow(dead_code)]
    pub fn search_query(&self) -> Option<&str> {
        self.search_query.as_deref()
    }

    pub fn search_match_count(&self) -> usize {
        self.search_matches.len()
    }

    pub fn current_match_index(&self) -> Option<usize> {
        self.current_match_index
    }

    pub fn is_search_match(&self, id: &NodeId) -> bool {
        self.search_matches.contains(id)
    }

    pub fn is_current_match(&self, id: &NodeId) -> bool {
        self.current_match_index
            .and_then(|idx| self.search_matches.get(idx))
            .map(|m| m == id)
            .unwrap_or(false)
    }

    pub fn open_search(&mut self, cx: &mut Context<Self>) {
        self.search_visible = true;
        cx.emit(DocumentTreeEvent::SearchOpened);
        cx.notify();
    }

    pub fn close_search(&mut self, cx: &mut Context<Self>) {
        self.search_visible = false;
        self.search_query = None;
        self.search_matches.clear();
        self.current_match_index = None;
        cx.emit(DocumentTreeEvent::SearchClosed);
        cx.notify();
    }

    pub fn set_search(&mut self, query: &str, cx: &mut Context<Self>) {
        if query.is_empty() {
            self.search_query = None;
            self.search_matches.clear();
            self.current_match_index = None;
            cx.notify();
            return;
        }

        self.search_query = Some(query.to_string());
        self.perform_search(cx);

        // Jump to first match
        if !self.search_matches.is_empty() {
            self.current_match_index = Some(0);
            self.jump_to_current_match(cx);
        } else {
            self.current_match_index = None;
        }

        cx.notify();
    }

    pub fn next_match(&mut self, cx: &mut Context<Self>) {
        if self.search_matches.is_empty() {
            return;
        }

        let next = match self.current_match_index {
            Some(idx) => (idx + 1) % self.search_matches.len(),
            None => 0,
        };

        self.current_match_index = Some(next);
        self.jump_to_current_match(cx);
        cx.notify();
    }

    pub fn prev_match(&mut self, cx: &mut Context<Self>) {
        if self.search_matches.is_empty() {
            return;
        }

        let prev = match self.current_match_index {
            Some(idx) => {
                if idx == 0 {
                    self.search_matches.len() - 1
                } else {
                    idx - 1
                }
            }
            None => self.search_matches.len() - 1,
        };

        self.current_match_index = Some(prev);
        self.jump_to_current_match(cx);
        cx.notify();
    }

    fn perform_search(&mut self, cx: &mut Context<Self>) {
        self.search_matches.clear();

        let Some(query) = &self.search_query else {
            return;
        };

        let query_lower = query.to_lowercase();

        // Ensure visible nodes are up to date
        if self.needs_rebuild {
            self.rebuild_visible_nodes();
        }

        // Search through all documents recursively (not just visible nodes)
        for doc in &self.documents.clone() {
            self.search_node_recursive(doc, &query_lower, cx);
        }
    }

    fn search_node_recursive(&mut self, node: &TreeNode, query: &str, cx: &mut Context<Self>) {
        // Check if key matches
        let key_matches = node.key.to_lowercase().contains(query);

        // Check if value matches
        let value_matches = node.value.preview().to_lowercase().contains(query);

        if key_matches || value_matches {
            self.search_matches.push(node.id.clone());

            // Auto-expand parents so the match is visible
            self.expand_ancestors(&node.id, cx);
        }

        // Recursively search children
        for child in node.children() {
            self.search_node_recursive(&child, query, cx);
        }
    }

    fn expand_ancestors(&mut self, id: &NodeId, _cx: &mut Context<Self>) {
        let mut current = id.parent();
        while let Some(parent_id) = current {
            if !self.expanded.contains(&parent_id) {
                self.expanded.insert(parent_id.clone());
                self.needs_rebuild = true;
            }
            current = parent_id.parent();
        }
    }

    fn jump_to_current_match(&mut self, cx: &mut Context<Self>) {
        let Some(idx) = self.current_match_index else {
            return;
        };

        let Some(match_id) = self.search_matches.get(idx).cloned() else {
            return;
        };

        // Set cursor to the match
        self.cursor = Some(match_id.clone());

        // Ensure visible nodes are rebuilt to include the match
        if self.needs_rebuild {
            self.rebuild_visible_nodes();
        }

        // Scroll to the match
        if let Some(visible_idx) = self.visible_nodes.iter().position(|n| n.id == match_id) {
            self.vertical_scroll
                .scroll_to_item(visible_idx, gpui::ScrollStrategy::Center);
        }

        cx.emit(DocumentTreeEvent::CursorMoved);
    }

    // === Expand/Collapse ===

    pub fn toggle_expand(&mut self, id: &NodeId, cx: &mut Context<Self>) {
        if self.expanded.contains(id) {
            self.expanded.remove(id);
        } else {
            self.expanded.insert(id.clone());
        }
        self.needs_rebuild = true;
        cx.emit(DocumentTreeEvent::ExpandToggled);
        cx.notify();
    }

    pub fn expand(&mut self, id: &NodeId, cx: &mut Context<Self>) {
        if !self.expanded.contains(id) {
            self.expanded.insert(id.clone());
            self.needs_rebuild = true;
            cx.notify();
        }
    }

    pub fn collapse(&mut self, id: &NodeId, cx: &mut Context<Self>) {
        if self.expanded.remove(id) {
            self.needs_rebuild = true;
            cx.notify();
        }
    }

    // === Cursor Navigation ===

    pub fn move_cursor(&mut self, direction: TreeDirection, cx: &mut Context<Self>) {
        if self.needs_rebuild {
            self.rebuild_visible_nodes();
        }

        let current_idx = self
            .cursor
            .as_ref()
            .and_then(|id| self.visible_nodes.iter().position(|n| &n.id == id));

        let new_idx = match (current_idx, direction) {
            (None, _) if !self.visible_nodes.is_empty() => Some(0),
            (Some(idx), TreeDirection::Up) if idx > 0 => Some(idx - 1),
            (Some(idx), TreeDirection::Down) if idx + 1 < self.visible_nodes.len() => Some(idx + 1),
            _ => current_idx,
        };

        if let Some(idx) = new_idx {
            let node_id = self.visible_nodes.get(idx).map(|n| n.id.clone());
            if let Some(id) = node_id {
                self.cursor = Some(id.clone());
                self.scroll_to_cursor(cx);
                cx.emit(DocumentTreeEvent::CursorMoved);
                cx.notify();
            }
        }
    }

    /// Move cursor to parent node.
    pub fn move_to_parent(&mut self, cx: &mut Context<Self>) {
        if let Some(current) = &self.cursor
            && let Some(parent_id) = current.parent()
        {
            self.cursor = Some(parent_id.clone());
            self.scroll_to_cursor(cx);
            cx.emit(DocumentTreeEvent::CursorMoved);
            cx.notify();
        }
    }

    /// Move cursor to first child or expand if collapsed.
    pub fn move_to_first_child(&mut self, cx: &mut Context<Self>) {
        let Some(current) = self.cursor.clone() else {
            return;
        };

        // Find the node
        let node = self.visible_nodes.iter().find(|n| n.id == current).cloned();

        if let Some(node) = node {
            if !node.is_expandable() {
                return;
            }

            if !self.is_expanded(&current) {
                // Expand first
                self.expand(&current, cx);
                self.rebuild_visible_nodes();
            }

            // Find the first child in visible nodes
            let current_idx = self.visible_nodes.iter().position(|n| n.id == current);
            if let Some(idx) = current_idx
                && idx + 1 < self.visible_nodes.len()
            {
                let next_id = self.visible_nodes[idx + 1].id.clone();
                let is_child = self.visible_nodes[idx + 1].parent_id.as_ref() == Some(&current);

                if is_child {
                    self.cursor = Some(next_id.clone());
                    self.scroll_to_cursor(cx);
                    cx.emit(DocumentTreeEvent::CursorMoved);
                    cx.notify();
                }
            }
        }
    }

    /// Handle left arrow: collapse if expanded, else go to parent.
    pub fn handle_left(&mut self, cx: &mut Context<Self>) {
        let Some(current) = self.cursor.clone() else {
            return;
        };

        if self.is_expanded(&current) {
            self.collapse(&current, cx);
        } else {
            self.move_to_parent(cx);
        }
    }

    /// Handle right arrow: expand if collapsed, else go to first child.
    pub fn handle_right(&mut self, cx: &mut Context<Self>) {
        let Some(current) = self.cursor.clone() else {
            return;
        };

        let node = self.visible_nodes.iter().find(|n| n.id == current).cloned();

        if let Some(node) = node {
            if !node.is_expandable() {
                return;
            }

            if !self.is_expanded(&current) {
                self.expand(&current, cx);
            } else {
                self.move_to_first_child(cx);
            }
        }
    }

    pub fn move_to_first(&mut self, cx: &mut Context<Self>) {
        if self.needs_rebuild {
            self.rebuild_visible_nodes();
        }

        let first_id = self.visible_nodes.first().map(|n| n.id.clone());
        if let Some(id) = first_id {
            self.cursor = Some(id.clone());
            self.scroll_to_cursor(cx);
            cx.emit(DocumentTreeEvent::CursorMoved);
            cx.notify();
        }
    }

    pub fn move_to_last(&mut self, cx: &mut Context<Self>) {
        if self.needs_rebuild {
            self.rebuild_visible_nodes();
        }

        let last_id = self.visible_nodes.last().map(|n| n.id.clone());
        if let Some(id) = last_id {
            self.cursor = Some(id.clone());
            self.scroll_to_cursor(cx);
            cx.emit(DocumentTreeEvent::CursorMoved);
            cx.notify();
        }
    }

    pub fn page_down(&mut self, page_size: usize, cx: &mut Context<Self>) {
        if self.needs_rebuild {
            self.rebuild_visible_nodes();
        }

        let current_idx = self
            .cursor
            .as_ref()
            .and_then(|id| self.visible_nodes.iter().position(|n| &n.id == id))
            .unwrap_or(0);

        let new_idx = (current_idx + page_size).min(self.visible_nodes.len().saturating_sub(1));

        let node_id = self.visible_nodes.get(new_idx).map(|n| n.id.clone());
        if let Some(id) = node_id {
            self.cursor = Some(id.clone());
            self.scroll_to_cursor(cx);
            cx.emit(DocumentTreeEvent::CursorMoved);
            cx.notify();
        }
    }

    pub fn page_up(&mut self, page_size: usize, cx: &mut Context<Self>) {
        if self.needs_rebuild {
            self.rebuild_visible_nodes();
        }

        let current_idx = self
            .cursor
            .as_ref()
            .and_then(|id| self.visible_nodes.iter().position(|n| &n.id == id))
            .unwrap_or(0);

        let new_idx = current_idx.saturating_sub(page_size);

        let node_id = self.visible_nodes.get(new_idx).map(|n| n.id.clone());
        if let Some(id) = node_id {
            self.cursor = Some(id.clone());
            self.scroll_to_cursor(cx);
            cx.emit(DocumentTreeEvent::CursorMoved);
            cx.notify();
        }
    }

    fn scroll_to_cursor(&mut self, _cx: &mut Context<Self>) {
        if let Some(cursor) = &self.cursor
            && let Some(idx) = self.visible_nodes.iter().position(|n| &n.id == cursor)
        {
            self.vertical_scroll
                .scroll_to_item(idx, gpui::ScrollStrategy::Center);
        }
    }

    // === Actions ===

    pub fn focus(&self, window: &mut gpui::Window) {
        self.focus_handle.focus(window);
    }

    pub fn start_edit_at_cursor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(cursor) = self.cursor.clone() else {
            return;
        };

        self.start_inline_edit(&cursor, window, cx);
    }

    pub fn handle_value_click(&mut self, id: &NodeId, window: &mut Window, cx: &mut Context<Self>) {
        self.set_cursor(id, cx);
        self.start_inline_edit(id, window, cx);
    }

    pub fn commit_inline_edit(&mut self, cx: &mut Context<Self>) {
        let Some(node_id) = self.editing_node.take() else {
            return;
        };

        let Some(input) = self.inline_edit_input.take() else {
            return;
        };

        let new_value = input.read(cx).value().to_string();

        cx.emit(DocumentTreeEvent::InlineEditCommitted { node_id, new_value });
        cx.notify();
    }

    pub fn cancel_inline_edit(&mut self, cx: &mut Context<Self>) {
        if self.editing_node.take().is_some() {
            self.inline_edit_input = None;
            cx.notify();
        }
    }

    fn start_inline_edit(&mut self, id: &NodeId, window: &mut Window, cx: &mut Context<Self>) {
        let node = self.visible_nodes.iter().find(|n| &n.id == id).cloned();

        let Some(node) = node else {
            return;
        };

        if id.is_root() {
            self.toggle_expand(id, cx);
            return;
        }

        match &node.value {
            NodeValue::Document(_) | NodeValue::Array(_) => {
                self.toggle_expand(id, cx);
            }
            NodeValue::Scalar(value) => {
                if should_expand_scalar_value(value) {
                    self.cancel_inline_edit(cx);
                    self.toggle_value_expand(id, cx);
                    return;
                }

                if self.editing_node.as_ref() == Some(id) {
                    return;
                }

                self.cancel_inline_edit(cx);

                let initial_value = format_value_for_edit(value);
                let input = cx.new(|cx| {
                    let mut state = InputState::new(window, cx);
                    state.set_value(&initial_value, window, cx);
                    state
                });

                input.update(cx, |state, cx| {
                    state.focus(window, cx);
                });

                cx.subscribe(&input, |this, _input, event: &InputEvent, cx| match event {
                    InputEvent::PressEnter { .. } => this.commit_inline_edit(cx),
                    InputEvent::Blur => this.cancel_inline_edit(cx),
                    _ => {}
                })
                .detach();

                self.editing_node = Some(id.clone());
                self.inline_edit_input = Some(input);
                cx.notify();
            }
        }
    }

    pub fn request_document_preview(&self, cx: &mut Context<Self>) {
        let Some(cursor) = &self.cursor else {
            return;
        };

        // Find the root document for this cursor
        let doc_index = cursor.doc_index().unwrap_or(0);

        if let Some(raw_doc) = self.raw_documents.get(doc_index) {
            let json = serde_json::to_string_pretty(&value_to_json(raw_doc)).unwrap_or_default();
            cx.emit(DocumentTreeEvent::DocumentPreviewRequested {
                doc_index,
                document_json: json,
            });
        }
    }

    pub fn request_delete(&self, cx: &mut Context<Self>) {
        let Some(cursor) = &self.cursor else {
            return;
        };

        // Only allow delete on root document nodes
        if cursor.is_root() {
            cx.emit(DocumentTreeEvent::DeleteRequested(cursor.clone()));
        }
    }

    /// Request context menu at the given position for the current cursor node.
    pub fn request_context_menu(
        &self,
        position: gpui::Point<gpui::Pixels>,
        cx: &mut Context<Self>,
    ) {
        let Some(cursor) = &self.cursor else {
            return;
        };

        let doc_index = cursor.doc_index().unwrap_or(0);
        cx.emit(DocumentTreeEvent::ContextMenuRequested {
            doc_index,
            position,
        });
    }

    /// Double-click action: expand/collapse containers, edit scalar values.
    pub fn execute_node(&mut self, id: &NodeId, window: &mut Window, cx: &mut Context<Self>) {
        self.set_cursor(id, cx);

        self.start_inline_edit(id, window, cx);
    }

    /// Apply an inline value edit directly to the in-memory tree data.
    pub fn apply_inline_edit_value(&mut self, id: &NodeId, value: Value, cx: &mut Context<Self>) {
        let Some(doc_index) = id.doc_index() else {
            return;
        };

        if id.path.len() < 2 {
            return;
        }

        let Some(raw_doc) = self.raw_documents.get_mut(doc_index) else {
            return;
        };

        let updated = set_value_at_path(raw_doc, &id.path[1..], value);
        if !updated {
            return;
        }

        self.raw_json_cache = None;

        if let Some(root_node) = self.documents.get_mut(doc_index) {
            root_node.value = NodeValue::from_value(raw_doc);
        }

        self.cursor = Some(id.clone());
        self.needs_rebuild = true;
        cx.notify();
    }

    // === Internal ===

    fn rebuild_visible_nodes(&mut self) {
        self.visible_nodes.clear();

        // Clone documents to avoid borrow conflict
        let documents = self.documents.clone();
        for doc in &documents {
            self.add_node_recursive(doc);
        }

        self.needs_rebuild = false;
    }

    fn add_node_recursive(&mut self, node: &TreeNode) {
        self.visible_nodes.push(node.clone());

        if self.expanded.contains(&node.id) {
            let children = node.children();
            for child in children {
                self.add_node_recursive(&child);
            }
        }
    }
}

impl EventEmitter<DocumentTreeEvent> for DocumentTreeState {}

impl Focusable for DocumentTreeState {
    fn focus_handle(&self, _cx: &gpui::App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

fn should_expand_scalar_value(value: &Value) -> bool {
    match value {
        Value::Text(text) => text.contains('\n') || text.len() > 100,
        Value::Json(_) | Value::Bytes(_) => true,
        _ => false,
    }
}

fn set_value_at_path(current: &mut Value, path: &[String], new_value: Value) -> bool {
    if path.is_empty() {
        *current = new_value;
        return true;
    }

    let mut cursor = current;

    for (idx, segment) in path.iter().enumerate() {
        let is_last = idx + 1 == path.len();

        if is_last {
            match cursor {
                Value::Document(fields) => {
                    fields.insert(segment.clone(), new_value);
                    return true;
                }
                Value::Array(items) => {
                    let Ok(item_idx) = segment.parse::<usize>() else {
                        return false;
                    };

                    if item_idx >= items.len() {
                        return false;
                    }

                    items[item_idx] = new_value;
                    return true;
                }
                _ => return false,
            }
        }

        cursor = match cursor {
            Value::Document(fields) => {
                let Some(next) = fields.get_mut(segment) else {
                    return false;
                };
                next
            }
            Value::Array(items) => {
                let Ok(item_idx) = segment.parse::<usize>() else {
                    return false;
                };

                let Some(next) = items.get_mut(item_idx) else {
                    return false;
                };

                next
            }
            _ => return false,
        };
    }

    false
}

fn format_value_for_edit(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::ObjectId(id) => id.clone(),
        Value::DateTime(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        Value::Date(d) => d.format("%Y-%m-%d").to_string(),
        Value::Time(t) => t.format("%H:%M:%S").to_string(),
        Value::Bytes(_) => String::new(),
        Value::Decimal(d) => d.clone(),
        Value::Json(j) => j.clone(),
        Value::Document(_) | Value::Array(_) => String::new(),
    }
}

fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::ObjectId(id) => {
            serde_json::json!({ "$oid": id })
        }
        Value::DateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
        Value::Date(d) => serde_json::Value::String(d.to_string()),
        Value::Time(t) => serde_json::Value::String(t.to_string()),
        Value::Bytes(b) => {
            // Encode as hex for simplicity (avoids base64 dependency)
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            serde_json::json!({ "$binary": hex })
        }
        Value::Decimal(d) => serde_json::Value::String(d.clone()),
        Value::Json(j) => serde_json::from_str(j).unwrap_or(serde_json::Value::String(j.clone())),
        Value::Document(fields) => {
            let obj: serde_json::Map<String, serde_json::Value> = fields
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        Value::Array(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
    }
}
