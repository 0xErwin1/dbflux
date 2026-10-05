//! Optional facts drawn on a second line under a column header, such as a
//! column's compression ratio, size and share of nulls. The host formats the
//! text; the grid only places it.

use gpui::SharedString;

/// The second header line of one column: `leading` at the start of the cell
/// and `trailing`, when present, at its end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderAnnotation {
    pub leading: SharedString,
    pub trailing: Option<SharedString>,
}

impl HeaderAnnotation {
    pub fn new(leading: impl Into<SharedString>) -> Self {
        Self {
            leading: leading.into(),
            trailing: None,
        }
    }

    pub fn trailing(mut self, trailing: impl Into<SharedString>) -> Self {
        self.trailing = Some(trailing.into());
        self
    }
}
