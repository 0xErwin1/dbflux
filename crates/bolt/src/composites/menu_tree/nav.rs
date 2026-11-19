//! The cursor path of a cascading menu, and the keyboard transitions across its
//! levels.

use crate::composites::menu_tree::model::{MenuNode, level, node_at};

/// The highlighted row at every open level.
///
/// `path[0]` is the highlighted row of the root panel and `path[1]` of the
/// flyout that row opened, so the length of the path *is* the depth: a level is
/// open exactly when the path reaches it. An empty path is a menu that is not
/// open.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MenuCursor {
    path: Vec<usize>,
}

impl MenuCursor {
    /// The highlighted row of `level`, if that level is open.
    pub fn highlight(&self, level: usize) -> Option<usize> {
        self.path.get(level).copied()
    }

    /// How many levels carry a highlight.
    pub fn depth(&self) -> usize {
        self.path.len()
    }

    /// How many flyouts hang off the root panel.
    pub fn open_flyouts(&self) -> usize {
        self.path.len().saturating_sub(1)
    }

    pub fn path(&self) -> &[usize] {
        &self.path
    }
}

/// What opening the highlighted row did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildAction {
    /// The row is a submenu and its flyout is now open.
    Opened,
    /// The row is a leaf: the caller activates it.
    Activate,
    /// The row opened nothing and there is nothing to activate.
    None,
}

/// What closing one level did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackAction {
    /// One flyout closed and the cursor is back on its parent row.
    ClosedLevel,
    /// There was no flyout to close: the caller dismisses the whole menu.
    Dismissed,
}

/// The cursor of one open menu, and the moves the keyboard and the pointer make
/// on it.
#[derive(Clone, Debug, Default)]
pub struct MenuNav {
    cursor: MenuCursor,
}

impl MenuNav {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cursor(&self) -> &MenuCursor {
        &self.cursor
    }

    /// Opens the menu on its first selectable row.
    pub fn reset(&mut self, root: &[MenuNode]) {
        self.cursor.path.clear();

        if let Some(first) = first_selectable(root) {
            self.cursor.path.push(first);
        }
    }

    /// Moves the highlight within the deepest open level, wrapping at its ends.
    /// Returns whether the highlight moved.
    pub fn step(&mut self, root: &[MenuNode], forward: bool) -> bool {
        let Some(&current) = self.cursor.path.last() else {
            return false;
        };
        let depth = self.cursor.path.len();
        let Some(parents) = self.cursor.path.get(..depth - 1) else {
            return false;
        };

        let Some(next) = neighbour(level(root, parents), current, forward) else {
            return false;
        };

        if next == current {
            return false;
        }

        if let Some(slot) = self.cursor.path.last_mut() {
            *slot = next;
        }

        true
    }

    /// Right or Enter on the highlighted row.
    pub fn open_child(&mut self, root: &[MenuNode]) -> ChildAction {
        let Some(node) = self.current(root) else {
            return ChildAction::None;
        };

        if !node.is_selectable() {
            return ChildAction::None;
        }

        if !node.opens_flyout() {
            return ChildAction::Activate;
        }

        let Some(first) = first_selectable(&node.children) else {
            return ChildAction::None;
        };

        self.cursor.path.push(first);

        ChildAction::Opened
    }

    /// Left or Escape: closes the deepest open level only.
    pub fn back(&mut self) -> BackAction {
        if self.cursor.path.len() > 1 {
            self.cursor.path.pop();
            BackAction::ClosedLevel
        } else {
            BackAction::Dismissed
        }
    }

    pub fn close_all(&mut self) {
        self.cursor.path.clear();
    }

    /// The pointer entered `index` of `level`: switches the whole cascade to it.
    /// Returns whether the cursor moved.
    pub fn hover(&mut self, level: usize, index: usize, root: &[MenuNode]) -> bool {
        if !self.set_level(level, index, root) {
            return false;
        }

        let child = self
            .current(root)
            .filter(|node| node.opens_flyout())
            .and_then(|node| first_selectable(&node.children));

        if let Some(first) = child {
            self.cursor.path.push(first);
        }

        true
    }

    /// Puts the highlight on `index` of `level`, closing anything deeper.
    pub fn set_level(&mut self, level: usize, index: usize, root: &[MenuNode]) -> bool {
        if level > self.cursor.path.len() {
            return false;
        }

        let Some(parents) = self.cursor.path.get(..level) else {
            return false;
        };

        if !node_at(root, parents, index).is_some_and(MenuNode::is_selectable) {
            return false;
        }

        self.cursor.path.truncate(level);
        self.cursor.path.push(index);

        true
    }

    /// The row the highlight rests on.
    pub fn current<'a>(&self, root: &'a [MenuNode]) -> Option<&'a MenuNode> {
        let (last, parents) = self.cursor.path.split_last()?;

        node_at(root, parents, *last)
    }
}

/// The first row of a level the cursor may rest on.
fn first_selectable(nodes: &[MenuNode]) -> Option<usize> {
    nodes.iter().position(MenuNode::is_selectable)
}

/// The next selectable row from `current`, wrapping at the ends of the level.
fn neighbour(nodes: &[MenuNode], current: usize, forward: bool) -> Option<usize> {
    let count = nodes.len();

    if count == 0 {
        return None;
    }

    let advance = |index: usize| {
        if forward {
            (index + 1) % count
        } else {
            (index + count - 1) % count
        }
    };

    let mut index = if current < count { advance(current) } else { 0 };

    for _ in 0..count {
        if nodes.get(index).is_some_and(MenuNode::is_selectable) {
            return Some(index);
        }

        index = advance(index);
    }

    None
}
