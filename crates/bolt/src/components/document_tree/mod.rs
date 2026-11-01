//! The document tree's shared contract and row painter: the node id a tree
//! addresses its nodes by, the value classes a document value maps onto, and
//! the row painter that draws one expanded or collapsed document node.
//!
//! The value model stays with the data owner: a document tree's nodes hold
//! the product's value type, and the owner formats the previews, summaries
//! and type labels before it hands a [`DocumentTreeRowView`] over — the same
//! display-facts-only contract [`crate::data`] establishes for grids. The
//! painter owns the row's paint decisions: the background precedence (the
//! keyboard cursor, then the current search match, then any match), the
//! hover variants, the chevron, the key, the value in its type color, and
//! the type badge.

mod row;

#[cfg(test)]
mod tests;

pub use row::{
    DocumentTreeRowHost, DocumentTreeRowView, RowPaint, TreeRowMetrics, render_row, row_paint,
};

use gpui::Hsla;
use gpui_component::Theme;

use crate::tokens::{ChromeColors, ValueTypeColors};

/// Unique identifier for a tree node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId {
    /// Path from root to this node (e.g., ["0", "address", "city"]).
    pub path: Vec<String>,
}

impl NodeId {
    pub fn root(doc_index: usize) -> Self {
        Self {
            path: vec![doc_index.to_string()],
        }
    }

    pub fn child(&self, key: &str) -> Self {
        let mut path = self.path.clone();
        path.push(key.to_string());
        Self { path }
    }

    pub fn parent(&self) -> Option<Self> {
        if self.path.len() <= 1 {
            return None;
        }
        let mut path = self.path.clone();
        path.pop();
        Some(Self { path })
    }

    pub fn depth(&self) -> usize {
        self.path.len().saturating_sub(1)
    }

    pub fn is_root(&self) -> bool {
        self.path.len() == 1
    }

    /// Returns the document index for root nodes.
    pub fn doc_index(&self) -> Option<usize> {
        self.path.first().and_then(|s| s.parse().ok())
    }
}

/// The coarse class of a value in a document tree, as the row painter paints
/// it: the value preview's and the type badge's color follow the class. The
/// data owner maps its value model onto these classes; the painter never
/// sees the value itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueTypeClass {
    /// The value holds nothing.
    Null,
    /// `true` or `false`.
    Boolean,
    /// A whole number.
    Integer,
    /// A number with a fraction, including decimals.
    Float,
    /// A string.
    Text,
    /// A document object id.
    ObjectId,
    /// A date, a time, or a timestamp.
    Temporal,
    /// Raw binary data.
    Bytes,
    /// A pre-serialized JSON string.
    Json,
    /// A value the owner could not classify.
    Other,
    /// A document or an array: the container kinds a row can step into.
    Container,
}

/// The color a document tree paints a value of `class` with: the JSON type
/// colors for the classes no theme role expresses, the theme's own roles for
/// the rest.
pub fn value_type_color(class: ValueTypeClass, theme: &Theme) -> Hsla {
    match class {
        ValueTypeClass::Null => theme.muted_foreground,
        ValueTypeClass::Boolean => ValueTypeColors::BOOLEAN,
        ValueTypeClass::Integer => ValueTypeColors::INTEGER,
        ValueTypeClass::Float => ValueTypeColors::FLOAT,
        ValueTypeClass::Text => ValueTypeColors::TEXT,
        ValueTypeClass::ObjectId => ChromeColors::tint(theme),
        ValueTypeClass::Temporal => ValueTypeColors::TEMPORAL,
        ValueTypeClass::Bytes => theme.warning,
        ValueTypeClass::Json => theme.muted_foreground,
        ValueTypeClass::Other => theme.foreground,
        ValueTypeClass::Container => theme.muted_foreground,
    }
}
