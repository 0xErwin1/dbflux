//! The declarative row model of a cascading menu.

use crate::composites::menu_item::MenuItem;
use crate::icon::IconSource;
use gpui::SharedString;

/// One row of a cascading menu, together with the rows of its flyout.
///
/// A node with children *is* a submenu: the chevron, the flyout and the
/// open-on-right behaviour all follow from `children` being non-empty, so a
/// product never sets a "has submenu" flag by hand.
#[derive(Clone)]
pub struct MenuNode {
    /// The visual row, shared with the flat menus [`MenuItem`] already draws.
    pub item: MenuItem,
    /// The value activation hands back to the caller. Separators and headers
    /// carry none.
    pub id: Option<SharedString>,
    /// The rows of this row's flyout. Empty for a leaf row.
    pub children: Vec<MenuNode>,
    /// Width of this row's flyout, when it must differ from the default.
    pub children_width: Option<f32>,
    /// Whether activating this row leaves the menu open. Defaults to true for
    /// a checkable row and false for every other row.
    pub keep_open: Option<bool>,
}

impl MenuNode {
    pub fn new(item: MenuItem) -> Self {
        Self {
            item,
            id: None,
            children: Vec::new(),
            children_width: None,
            keep_open: None,
        }
    }

    /// A plain action row.
    pub fn row(label: impl Into<SharedString>) -> Self {
        Self::new(MenuItem::new(label))
    }

    /// An action row that reports `id` when it is activated.
    pub fn action(id: impl Into<SharedString>, label: impl Into<SharedString>) -> Self {
        Self::row(label).id(id)
    }

    /// A checkable row. It keeps the menu open, so its check state can be
    /// toggled and re-rendered from the caller's state.
    pub fn checked(
        id: impl Into<SharedString>,
        label: impl Into<SharedString>,
        checked: bool,
    ) -> Self {
        Self::new(MenuItem::new(label).checked(checked)).id(id)
    }

    /// A radio row: exactly one row of the group carries the mark, and the
    /// row reports the radio role to assistive technology.
    pub fn radio(
        id: impl Into<SharedString>,
        label: impl Into<SharedString>,
        checked: bool,
    ) -> Self {
        Self::new(MenuItem::new(label).radio(checked)).id(id)
    }

    pub fn separator() -> Self {
        Self::new(MenuItem::separator())
    }

    pub fn header(label: impl Into<SharedString>) -> Self {
        Self::new(MenuItem::header(label))
    }

    pub fn id(mut self, id: impl Into<SharedString>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Adds the rows of this row's flyout, which makes it a submenu.
    pub fn children(mut self, children: Vec<MenuNode>) -> Self {
        self.children = children;
        self.mark_submenu();
        self
    }

    pub fn child(mut self, child: MenuNode) -> Self {
        self.children.push(child);
        self.mark_submenu();
        self
    }

    /// Keeps the row's chevron in step with its children, so a caller never
    /// sets the two by hand and gets them out of sync.
    fn mark_submenu(&mut self) {
        if !self.children.is_empty() {
            self.item.has_submenu = true;
        }
    }

    /// Sets the width of this row's flyout.
    pub fn children_width(mut self, width: f32) -> Self {
        self.children_width = Some(width);
        self
    }

    pub fn keep_open(mut self, keep_open: bool) -> Self {
        self.keep_open = Some(keep_open);
        self
    }

    pub fn icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.item = self.item.icon(icon);
        self
    }

    pub fn shortcut(mut self, shortcut: impl Into<SharedString>) -> Self {
        self.item = self.item.shortcut(shortcut);
        self
    }

    pub fn detail(mut self, detail: impl Into<SharedString>) -> Self {
        self.item = self.item.detail(detail);
        self
    }

    pub fn danger(mut self) -> Self {
        self.item = self.item.danger();
        self
    }

    pub fn disabled(mut self) -> Self {
        self.item = self.item.disabled();
        self
    }

    /// Whether the cursor may rest on this row and whether it reacts to the
    /// pointer: separators, headers and disabled rows are inert.
    pub fn is_selectable(&self) -> bool {
        !self.item.is_separator && !self.item.is_header && !self.item.disabled
    }

    /// Whether this row opens a flyout.
    pub fn opens_flyout(&self) -> bool {
        !self.children.is_empty()
    }

    /// Whether activating this row leaves the whole cascade open.
    pub fn keeps_menu_open(&self) -> bool {
        self.keep_open
            .unwrap_or_else(|| self.item.checked.is_some())
    }
}

/// The rows of one level, following `path` down from the root.
pub(crate) fn level<'a>(root: &'a [MenuNode], path: &[usize]) -> &'a [MenuNode] {
    let mut current = root;

    for index in path {
        match current.get(*index) {
            Some(node) => current = &node.children,
            None => return &[],
        }
    }

    current
}

/// The row at `index` of the level reached by `path`.
pub(crate) fn node_at<'a>(
    root: &'a [MenuNode],
    path: &[usize],
    index: usize,
) -> Option<&'a MenuNode> {
    level(root, path).get(index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_node_with_children_is_a_submenu() {
        let leaf = MenuNode::row("Leaf");
        let parent = MenuNode::row("More").child(leaf);

        assert!(parent.opens_flyout());
        assert_eq!(parent.children.len(), 1);
    }

    #[test]
    fn separators_headers_and_disabled_rows_are_not_selectable() {
        assert!(!MenuNode::separator().is_selectable());
        assert!(!MenuNode::header("caption").is_selectable());
        assert!(!MenuNode::row("Blocked").disabled().is_selectable());
        assert!(MenuNode::row("Open").is_selectable());
    }

    #[test]
    fn a_checkable_row_keeps_the_menu_open_unless_it_says_otherwise() {
        assert!(MenuNode::checked("grid", "Grid", true).keeps_menu_open());
        assert!(!MenuNode::action("open", "Open").keeps_menu_open());
        assert!(
            !MenuNode::checked("grid", "Grid", true)
                .keep_open(false)
                .keeps_menu_open()
        );
        assert!(
            MenuNode::action("open", "Open")
                .keep_open(true)
                .keeps_menu_open()
        );
    }
}
