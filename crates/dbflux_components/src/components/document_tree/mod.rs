mod events;
mod node;
mod state;
mod tree;

pub use events::{DocumentTreeEvent, TreeDirection};
pub use node::{NodeId, NodeValue};
pub use state::DocumentTreeState;
pub use tree::{CONTEXT, DocumentTree, init};

/// GPUI actions the tree handles, bound to keys in [`CONTEXT`].
pub mod actions {
    pub use super::tree::{
        CloseSearch, DeleteDocument, MoveDown, MoveLeft, MoveRight, MoveToBottom, MoveToTop,
        MoveUp, NextMatch, OpenPreview, OpenSearch, PageDown, PageUp, PrevMatch, StartEdit,
        ToggleExpand, ToggleViewMode,
    };
}
