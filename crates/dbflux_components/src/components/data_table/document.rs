//! Presentation extras for a grid that shows documents: a parent header over
//! the columns of an expanded object, a presence bar under each column name,
//! and before/after text on edited cells.

use std::ops::Range;
use std::sync::Arc;

use gpui::Pixels;

use super::theme::HEADER_HEIGHT;
use crate::tokens::CollectionMetrics;

/// Parent header shared by the child columns of an expanded object column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnGroupHeader {
    /// Dotted path of the object (`price`), unique per group.
    pub key: Arc<str>,
    /// Label drawn in the group row.
    pub label: Arc<str>,
    /// Short type drawn after the label (`obj`).
    pub type_label: Arc<str>,
}

/// Per-column document metadata, indexed like the model's columns.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DocumentColumnHeader {
    pub group: Option<ColumnGroupHeader>,
    /// Share of sampled documents that hold the field, from 0.0 to 1.0.
    pub presence: Option<f32>,
}

/// Document extras for the whole grid. Set on the state by the host; a grid
/// without it renders exactly like a relational one.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DocumentPresentation {
    pub headers: Vec<DocumentColumnHeader>,
}

/// A run of adjacent columns under the same group (or under none).
#[derive(Debug, Clone, PartialEq)]
pub struct GroupSpan {
    pub columns: Range<usize>,
    pub group: Option<ColumnGroupHeader>,
}

impl DocumentPresentation {
    pub fn header(&self, col: usize) -> Option<&DocumentColumnHeader> {
        self.headers.get(col)
    }

    pub fn has_groups(&self) -> bool {
        self.headers.iter().any(|header| header.group.is_some())
    }

    pub fn has_presence(&self) -> bool {
        self.headers.iter().any(|header| header.presence.is_some())
    }

    pub fn is_group_child(&self, col: usize) -> bool {
        self.header(col)
            .is_some_and(|header| header.group.is_some())
    }

    /// Height of the name row (with or without presence bars).
    pub fn name_row_height(&self) -> Pixels {
        if self.has_presence() {
            CollectionMetrics::PRESENCE_HEADER_HEIGHT
        } else if self.has_groups() {
            CollectionMetrics::GROUPED_HEADER_HEIGHT
        } else {
            HEADER_HEIGHT
        }
    }

    /// Total header height, group row included.
    pub fn header_height(&self) -> Pixels {
        if self.has_groups() {
            self.name_row_height() + CollectionMetrics::GROUP_ROW_HEIGHT
        } else {
            self.name_row_height()
        }
    }

    /// Splits `0..col_count` into runs of columns that share a group, so the
    /// group row can draw one cell per run.
    pub fn group_spans(&self, col_count: usize) -> Vec<GroupSpan> {
        let mut spans: Vec<GroupSpan> = Vec::new();

        for col in 0..col_count {
            let group = self.header(col).and_then(|header| header.group.clone());

            match spans.last_mut() {
                Some(span)
                    if span.group.is_some()
                        && span.group.as_ref().map(|g| &g.key)
                            == group.as_ref().map(|g| &g.key) =>
                {
                    span.columns.end = col + 1;
                }
                _ => spans.push(GroupSpan {
                    columns: col..col + 1,
                    group,
                }),
            }
        }

        spans
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grouped(key: &str) -> DocumentColumnHeader {
        DocumentColumnHeader {
            group: Some(ColumnGroupHeader {
                key: key.into(),
                label: key.into(),
                type_label: "obj".into(),
            }),
            presence: None,
        }
    }

    #[test]
    fn adjacent_children_of_one_group_share_a_span() {
        let presentation = DocumentPresentation {
            headers: vec![
                DocumentColumnHeader::default(),
                grouped("price"),
                grouped("price"),
                DocumentColumnHeader::default(),
                DocumentColumnHeader::default(),
            ],
        };

        let spans = presentation.group_spans(5);

        assert_eq!(spans.len(), 4);
        assert_eq!(spans[1].columns, 1..3);
        assert_eq!(
            spans[1].group.as_ref().map(|g| g.key.as_ref()),
            Some("price")
        );
        assert_eq!(spans[2].columns, 3..4);
        assert_eq!(spans[3].columns, 4..5);
    }

    #[test]
    fn neighbouring_groups_stay_separate() {
        let presentation = DocumentPresentation {
            headers: vec![grouped("price"), grouped("size")],
        };

        assert_eq!(presentation.group_spans(2).len(), 2);
    }

    #[test]
    fn header_height_adds_the_group_row() {
        let plain = DocumentPresentation::default();
        assert_eq!(plain.header_height(), HEADER_HEIGHT);

        let with_group = DocumentPresentation {
            headers: vec![grouped("price")],
        };
        assert_eq!(
            with_group.header_height(),
            CollectionMetrics::GROUPED_HEADER_HEIGHT + CollectionMetrics::GROUP_ROW_HEIGHT
        );

        let with_presence = DocumentPresentation {
            headers: vec![DocumentColumnHeader {
                group: None,
                presence: Some(0.5),
            }],
        };
        assert_eq!(
            with_presence.header_height(),
            CollectionMetrics::PRESENCE_HEADER_HEIGHT
        );
    }
}
