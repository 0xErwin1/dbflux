//! The column contract a chart reads: a display name and a semantic kind.
//!
//! The data owner adapts its own column metadata to this — the chart never
//! sees the owner's own metadata types or their engine-side type names. Which
//! columns a picker lists and how an axis formats its ticks follow the kind
//! alone.

use crate::data::ColumnKind;

/// A column as the chart reads it: its display name and its semantic class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChartColumn {
    /// The column's display name, as a picker row and a pill show it.
    pub name: String,
    /// The column's semantic class.
    pub kind: ColumnKind,
}

impl ChartColumn {
    /// A column of `name` and `kind`.
    pub fn new(name: impl Into<String>, kind: ColumnKind) -> Self {
        Self {
            name: name.into(),
            kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chart_column_keeps_the_name_and_kind_it_is_given() {
        let column = ChartColumn::new("cpu", ColumnKind::Float);

        assert_eq!(column.name, "cpu");
        assert_eq!(column.kind, ColumnKind::Float);

        let timestamp = ChartColumn::new("ts", ColumnKind::Timestamp);
        assert_eq!(timestamp.kind, ColumnKind::Timestamp);
    }
}
