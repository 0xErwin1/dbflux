use std::collections::{BTreeMap, HashSet};

use dbflux_core::{QueryResult, Value};
use gpui::{Context, EventEmitter, FocusHandle, Focusable, UniformListScrollHandle};

use super::events::{DocumentTreeEvent, TreeDirection};
use super::node::{NodeId, NodeValue, TreeNode};

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
        }
    }

    /// Load documents from a QueryResult.
    pub fn load_from_result(&mut self, result: &QueryResult, cx: &mut Context<Self>) {
        self.documents.clear();
        self.raw_documents.clear();
        self.expanded.clear();
        self.cursor = None;
        self.needs_rebuild = true;

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

    pub fn set_cursor(&mut self, id: &NodeId, cx: &mut Context<Self>) {
        if self.cursor.as_ref() != Some(id) {
            self.cursor = Some(id.clone());
            cx.emit(DocumentTreeEvent::CursorMoved(id.clone()));
            cx.notify();
        }
    }

    pub fn is_expanded(&self, id: &NodeId) -> bool {
        self.expanded.contains(id)
    }

    pub fn scroll_handle(&self) -> &UniformListScrollHandle {
        &self.vertical_scroll
    }

    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    /// Get the raw document value for a document index.
    pub fn raw_document(&self, index: usize) -> Option<&Value> {
        self.raw_documents.get(index)
    }

    // === Expand/Collapse ===

    pub fn toggle_expand(&mut self, id: &NodeId, cx: &mut Context<Self>) {
        if self.expanded.contains(id) {
            self.expanded.remove(id);
        } else {
            self.expanded.insert(id.clone());
        }
        self.needs_rebuild = true;
        cx.emit(DocumentTreeEvent::ExpandToggled(id.clone()));
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
                cx.emit(DocumentTreeEvent::CursorMoved(id));
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
            cx.emit(DocumentTreeEvent::CursorMoved(parent_id));
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
                    cx.emit(DocumentTreeEvent::CursorMoved(next_id));
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
            cx.emit(DocumentTreeEvent::CursorMoved(id));
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
            cx.emit(DocumentTreeEvent::CursorMoved(id));
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
            cx.emit(DocumentTreeEvent::CursorMoved(id));
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
            cx.emit(DocumentTreeEvent::CursorMoved(id));
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

    pub fn request_edit(&self, cx: &mut Context<Self>) {
        let Some(cursor) = &self.cursor else {
            return;
        };

        let node = self.visible_nodes.iter().find(|n| &n.id == cursor);

        if let Some(node) = node {
            let (current_value, is_json) = match &node.value {
                NodeValue::Scalar(v) => (format_value_for_edit(v), false),
                NodeValue::Document(_) | NodeValue::Array(_) => {
                    // For complex values, serialize as JSON
                    let raw_value = self.get_value_at_path(cursor);
                    if let Some(v) = raw_value {
                        (
                            serde_json::to_string_pretty(&value_to_json(&v)).unwrap_or_default(),
                            true,
                        )
                    } else {
                        return;
                    }
                }
            };

            cx.emit(DocumentTreeEvent::EditRequested {
                node_id: cursor.clone(),
                current_value,
                is_json,
            });
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

    /// Double-click action: preview (root), expand (container), or edit (scalar).
    pub fn execute_node(&mut self, id: &NodeId, cx: &mut Context<Self>) {
        self.set_cursor(id, cx);

        let is_expandable = self
            .visible_nodes
            .iter()
            .find(|n| &n.id == id)
            .map(|n| n.is_expandable())
            .unwrap_or(false);

        if id.is_root() {
            self.request_document_preview(cx);
        } else if is_expandable {
            self.toggle_expand(id, cx);
        } else {
            self.request_edit(cx);
        }
    }

    /// Get the value at a given path within the documents.
    fn get_value_at_path(&self, id: &NodeId) -> Option<Value> {
        let doc_index = id.doc_index()?;
        let raw_doc = self.raw_documents.get(doc_index)?;

        if id.is_root() {
            return Some(raw_doc.clone());
        }

        // Navigate the path
        let mut current = raw_doc.clone();
        for segment in &id.path[1..] {
            current = match current {
                Value::Document(fields) => fields.into_iter().find(|(k, _)| k == segment)?.1,
                Value::Array(items) => {
                    let idx: usize = segment.parse().ok()?;
                    items.into_iter().nth(idx)?
                }
                _ => return None,
            };
        }

        Some(current)
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
