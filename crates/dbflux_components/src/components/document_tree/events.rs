use super::node::{NodeId, NodeValue};

/// Direction for cursor navigation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeDirection {
    Up,
    Down,
}

/// Events emitted by the DocumentTree component.
#[derive(Debug, Clone)]
pub enum DocumentTreeEvent {
    /// The tree received focus.
    Focused,

    /// Cursor moved to a different node.
    CursorMoved,

    /// User committed an inline edit for a scalar value.
    InlineEditCommitted { node_id: NodeId, new_value: String },

    /// User toggled expand/collapse on a node.
    ExpandToggled,

    /// User requested to delete a document (root node only).
    DeleteRequested(NodeId),

    /// User requested to view/edit a full document in modal.
    DocumentPreviewRequested {
        doc_index: usize,
        document_json: String,
    },

    /// User toggled between Tree and Raw JSON view modes.
    ViewModeToggled,

    /// Search mode was opened.
    SearchOpened,

    /// Search mode was closed.
    SearchClosed,

    /// User requested context menu on a document node.
    ContextMenuRequested {
        doc_index: usize,
        position: gpui::Point<gpui::Pixels>,
        node_id: NodeId,
        node_value: Option<NodeValue>,
    },
}
