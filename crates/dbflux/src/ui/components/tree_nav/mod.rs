mod gutter;

pub use gutter::{GutterInfo, render_gutter, tree_line_color};

use crate::ui::icons::AppIcon;
use gpui::SharedString;
use std::collections::HashSet;

/// A node in the tree definition.
pub struct TreeNavNode {
    pub id: SharedString,
    pub label: SharedString,
    pub icon: Option<AppIcon>,
    pub children: Vec<TreeNavNode>,
    pub selectable: bool,
}

/// A flattened row produced by `flatten_tree`, carrying layout metadata for gutter rendering.
pub struct FlatRow {
    pub id: SharedString,
    pub label: SharedString,
    pub icon: Option<AppIcon>,
    pub depth: usize,
    pub selectable: bool,
    pub has_children: bool,
    pub expanded: bool,
    pub is_last: bool,
    pub ancestors_continue: Vec<bool>,
}

/// Action returned by `activate()`.
#[derive(Debug, Clone, PartialEq)]
pub enum TreeNavAction {
    Selected(SharedString),
    Toggled { id: SharedString, expanded: bool },
    None,
}

/// Tree navigation state (plain struct, not a GPUI Entity).
///
/// The parent owns this as a field and forwards key events.
/// TreeNav does not own focus.
pub struct TreeNav {
    nodes: Vec<TreeNavNode>,
    rows: Vec<FlatRow>,
    expanded: HashSet<SharedString>,
    cursor: usize,
    selected_id: Option<SharedString>,
}

impl TreeNav {
    pub fn new(nodes: Vec<TreeNavNode>, initially_expanded: HashSet<SharedString>) -> Self {
        let rows = flatten_tree(&nodes, &initially_expanded);
        Self {
            nodes,
            rows,
            expanded: initially_expanded,
            cursor: 0,
            selected_id: None,
        }
    }

    pub fn move_next(&mut self) {
        let count = self.rows.len();
        if count <= 1 {
            return;
        }
        self.cursor = (self.cursor + 1) % count;
    }

    pub fn move_prev(&mut self) {
        let count = self.rows.len();
        if count <= 1 {
            return;
        }
        self.cursor = if self.cursor == 0 {
            count - 1
        } else {
            self.cursor - 1
        };
    }

    pub fn activate(&mut self) -> TreeNavAction {
        let Some(row) = self.rows.get(self.cursor) else {
            return TreeNavAction::None;
        };

        if row.has_children && !row.selectable {
            let id = row.id.clone();
            let was_expanded = self.expanded.contains(&id);

            if was_expanded {
                self.expanded.remove(&id);
            } else {
                self.expanded.insert(id.clone());
            }

            self.rebuild();

            TreeNavAction::Toggled {
                id,
                expanded: !was_expanded,
            }
        } else if row.selectable {
            let id = row.id.clone();
            self.selected_id = Some(id.clone());
            TreeNavAction::Selected(id)
        } else {
            TreeNavAction::None
        }
    }

    pub fn select_by_id(&mut self, id: &str) {
        if let Some(pos) = self.rows.iter().position(|r| r.id.as_ref() == id) {
            self.cursor = pos;
            self.selected_id = Some(SharedString::from(id.to_string()));
            return;
        }

        if self.expand_ancestors_for(id) {
            self.rebuild();

            if let Some(pos) = self.rows.iter().position(|r| r.id.as_ref() == id) {
                self.cursor = pos;
                self.selected_id = Some(SharedString::from(id.to_string()));
            }
        }
    }

    pub fn cursor_item(&self) -> Option<&FlatRow> {
        self.rows.get(self.cursor)
    }

    #[allow(dead_code)]
    pub fn selected_id(&self) -> Option<&SharedString> {
        self.selected_id.as_ref()
    }

    pub fn rows(&self) -> &[FlatRow] {
        &self.rows
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }

    #[allow(dead_code)]
    pub fn set_expanded(&mut self, expanded: HashSet<SharedString>) {
        self.expanded = expanded;
        self.rebuild();
    }

    pub fn expanded(&self) -> &HashSet<SharedString> {
        &self.expanded
    }

    /// Recursively search for `target_id` in the tree and expand all ancestor
    /// groups along the path. Returns true if the target was found.
    fn expand_ancestors_for(&mut self, target_id: &str) -> bool {
        Self::find_and_expand(&self.nodes, target_id, &mut self.expanded)
    }

    fn find_and_expand(
        nodes: &[TreeNavNode],
        target_id: &str,
        expanded: &mut HashSet<SharedString>,
    ) -> bool {
        for node in nodes {
            if node.id.as_ref() == target_id {
                return true;
            }

            if !node.children.is_empty()
                && Self::find_and_expand(&node.children, target_id, expanded)
            {
                expanded.insert(node.id.clone());
                return true;
            }
        }
        false
    }

    fn rebuild(&mut self) {
        self.rows = flatten_tree(&self.nodes, &self.expanded);

        if self.cursor >= self.rows.len() {
            self.cursor = self.rows.len().saturating_sub(1);
        }
    }
}

fn flatten_tree(nodes: &[TreeNavNode], expanded: &HashSet<SharedString>) -> Vec<FlatRow> {
    let mut rows = Vec::new();
    flatten_recursive(nodes, expanded, 0, &[], &mut rows);
    rows
}

fn flatten_recursive(
    nodes: &[TreeNavNode],
    expanded: &HashSet<SharedString>,
    depth: usize,
    parent_ancestors_continue: &[bool],
    out: &mut Vec<FlatRow>,
) {
    let count = nodes.len();

    for (i, node) in nodes.iter().enumerate() {
        let is_last = i == count - 1;
        let has_children = !node.children.is_empty();
        let is_expanded = has_children && expanded.contains(&node.id);

        let mut ancestors_continue = Vec::with_capacity(depth);
        if depth > 0 {
            ancestors_continue.extend_from_slice(parent_ancestors_continue);
        }

        out.push(FlatRow {
            id: node.id.clone(),
            label: node.label.clone(),
            icon: node.icon,
            depth,
            selectable: node.selectable,
            has_children,
            expanded: is_expanded,
            is_last,
            ancestors_continue: ancestors_continue.clone(),
        });

        if is_expanded {
            let mut child_ancestors = ancestors_continue;
            child_ancestors.push(!is_last);
            flatten_recursive(&node.children, expanded, depth + 1, &child_ancestors, out);
        }
    }
}

impl TreeNavNode {
    pub fn leaf(
        id: impl Into<SharedString>,
        label: impl Into<SharedString>,
        icon: Option<AppIcon>,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            icon,
            children: Vec::new(),
            selectable: true,
        }
    }

    pub fn group(
        id: impl Into<SharedString>,
        label: impl Into<SharedString>,
        icon: Option<AppIcon>,
        children: Vec<TreeNavNode>,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            icon,
            children,
            selectable: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ui::icons::AppIcon;

    fn make_test_tree() -> Vec<TreeNavNode> {
        vec![
            TreeNavNode::leaf("general", "General", Some(AppIcon::Settings)),
            TreeNavNode::leaf("keybindings", "Keybindings", Some(AppIcon::Keyboard)),
            TreeNavNode::group(
                "network",
                "Network",
                None,
                vec![TreeNavNode::leaf(
                    "ssh-tunnels",
                    "SSH Tunnels",
                    Some(AppIcon::FingerprintPattern),
                )],
            ),
            TreeNavNode::group(
                "connection",
                "Connection",
                None,
                vec![
                    TreeNavNode::leaf("services", "Services", Some(AppIcon::Plug)),
                    TreeNavNode::leaf("hooks", "Hooks", Some(AppIcon::SquareTerminal)),
                    TreeNavNode::leaf("drivers", "Drivers", Some(AppIcon::Database)),
                ],
            ),
            TreeNavNode::leaf("about", "About", Some(AppIcon::Info)),
        ]
    }

    fn all_expanded() -> HashSet<SharedString> {
        HashSet::from(["network".into(), "connection".into()])
    }

    // ── flatten_tree ────────────────────────────────────────────

    #[test]
    fn flatten_empty() {
        let rows = flatten_tree(&[], &HashSet::new());
        assert!(rows.is_empty());
    }

    #[test]
    fn flatten_single_root() {
        let nodes = vec![TreeNavNode::leaf("a", "A", None)];
        let rows = flatten_tree(&nodes, &HashSet::new());

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id.as_ref(), "a");
        assert_eq!(rows[0].depth, 0);
        assert!(rows[0].is_last);
        assert!(rows[0].ancestors_continue.is_empty());
    }

    #[test]
    fn flatten_two_roots() {
        let nodes = vec![
            TreeNavNode::leaf("a", "A", None),
            TreeNavNode::leaf("b", "B", None),
        ];
        let rows = flatten_tree(&nodes, &HashSet::new());

        assert_eq!(rows.len(), 2);
        assert!(!rows[0].is_last);
        assert!(rows[1].is_last);
    }

    #[test]
    fn flatten_nested_expanded() {
        let nodes = vec![TreeNavNode::group(
            "parent",
            "Parent",
            None,
            vec![
                TreeNavNode::leaf("child-a", "A", None),
                TreeNavNode::leaf("child-b", "B", None),
            ],
        )];
        let expanded = HashSet::from(["parent".into()]);
        let rows = flatten_tree(&nodes, &expanded);

        assert_eq!(rows.len(), 3);

        assert_eq!(rows[0].id.as_ref(), "parent");
        assert_eq!(rows[0].depth, 0);
        assert!(rows[0].has_children);
        assert!(rows[0].expanded);

        assert_eq!(rows[1].id.as_ref(), "child-a");
        assert_eq!(rows[1].depth, 1);
        assert!(!rows[1].is_last);
        assert_eq!(rows[1].ancestors_continue, vec![false]);

        assert_eq!(rows[2].id.as_ref(), "child-b");
        assert_eq!(rows[2].depth, 1);
        assert!(rows[2].is_last);
        assert_eq!(rows[2].ancestors_continue, vec![false]);
    }

    #[test]
    fn flatten_nested_collapsed() {
        let nodes = vec![TreeNavNode::group(
            "parent",
            "Parent",
            None,
            vec![
                TreeNavNode::leaf("child-a", "A", None),
                TreeNavNode::leaf("child-b", "B", None),
            ],
        )];
        let rows = flatten_tree(&nodes, &HashSet::new());

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id.as_ref(), "parent");
        assert!(rows[0].has_children);
        assert!(!rows[0].expanded);
    }

    #[test]
    fn flatten_ancestors_continue() {
        let nodes = vec![
            TreeNavNode::group(
                "g1",
                "G1",
                None,
                vec![TreeNavNode::group(
                    "g1-child",
                    "G1Child",
                    None,
                    vec![TreeNavNode::leaf("deep", "Deep", None)],
                )],
            ),
            TreeNavNode::leaf("sibling", "Sibling", None),
        ];
        let expanded = HashSet::from(["g1".into(), "g1-child".into()]);
        let rows = flatten_tree(&nodes, &expanded);

        assert_eq!(rows.len(), 4);

        // "deep" at depth 2: g1 has a sibling (true), g1-child is last child of g1 (false)
        let deep = &rows[2];
        assert_eq!(deep.id.as_ref(), "deep");
        assert_eq!(deep.depth, 2);
        assert_eq!(deep.ancestors_continue, vec![true, false]);
    }

    #[test]
    fn flatten_mixed_roots_and_groups() {
        let rows = flatten_tree(&make_test_tree(), &all_expanded());

        let ids: Vec<&str> = rows.iter().map(|r| r.id.as_ref()).collect();
        assert_eq!(
            ids,
            vec![
                "general",
                "keybindings",
                "network",
                "ssh-tunnels",
                "connection",
                "services",
                "hooks",
                "drivers",
                "about",
            ]
        );

        // "general" is not last (keybindings follows)
        assert!(!rows[0].is_last);
        // "about" is last root
        assert!(rows[8].is_last);
        // "ssh-tunnels" is child, depth 1, last in its group
        assert_eq!(rows[3].depth, 1);
        assert!(rows[3].is_last);
        // "services" is not last, "drivers" is last
        assert!(!rows[5].is_last);
        assert!(rows[7].is_last);
    }

    // ── Navigation ──────────────────────────────────────────────

    #[test]
    fn move_next_advances() {
        let mut nav = TreeNav::new(make_test_tree(), all_expanded());
        assert_eq!(nav.cursor(), 0);
        nav.move_next();
        assert_eq!(nav.cursor(), 1);
    }

    #[test]
    fn move_next_wraps() {
        let mut nav = TreeNav::new(make_test_tree(), all_expanded());
        let count = nav.rows().len();
        for _ in 0..count {
            nav.move_next();
        }
        assert_eq!(nav.cursor(), 0);
    }

    #[test]
    fn move_prev_retreats() {
        let mut nav = TreeNav::new(make_test_tree(), all_expanded());
        nav.move_next();
        nav.move_next();
        assert_eq!(nav.cursor(), 2);
        nav.move_prev();
        assert_eq!(nav.cursor(), 1);
    }

    #[test]
    fn move_prev_wraps() {
        let mut nav = TreeNav::new(make_test_tree(), all_expanded());
        assert_eq!(nav.cursor(), 0);
        nav.move_prev();
        assert_eq!(nav.cursor(), nav.rows().len() - 1);
    }

    #[test]
    fn move_on_single_row() {
        let nodes = vec![TreeNavNode::leaf("only", "Only", None)];
        let mut nav = TreeNav::new(nodes, HashSet::new());
        assert_eq!(nav.cursor(), 0);
        nav.move_next();
        assert_eq!(nav.cursor(), 0);
        nav.move_prev();
        assert_eq!(nav.cursor(), 0);
    }

    // ── activate ────────────────────────────────────────────────

    #[test]
    fn activate_selectable() {
        let mut nav = TreeNav::new(make_test_tree(), all_expanded());
        // cursor is on "general" (selectable leaf)
        let action = nav.activate();
        assert_eq!(action, TreeNavAction::Selected("general".into()));
        assert_eq!(nav.selected_id(), Some(&SharedString::from("general")));
    }

    #[test]
    fn activate_group_expands() {
        let nodes = vec![TreeNavNode::group(
            "g",
            "Group",
            None,
            vec![TreeNavNode::leaf("c", "Child", None)],
        )];
        let mut nav = TreeNav::new(nodes, HashSet::new());

        assert_eq!(nav.rows().len(), 1);
        let action = nav.activate();
        assert_eq!(
            action,
            TreeNavAction::Toggled {
                id: "g".into(),
                expanded: true,
            }
        );
        assert_eq!(nav.rows().len(), 2);
    }

    #[test]
    fn activate_group_collapses() {
        let nodes = vec![TreeNavNode::group(
            "g",
            "Group",
            None,
            vec![TreeNavNode::leaf("c", "Child", None)],
        )];
        let mut nav = TreeNav::new(nodes, HashSet::from(["g".into()]));

        assert_eq!(nav.rows().len(), 2);
        let action = nav.activate();
        assert_eq!(
            action,
            TreeNavAction::Toggled {
                id: "g".into(),
                expanded: false,
            }
        );
        assert_eq!(nav.rows().len(), 1);
    }

    #[test]
    fn activate_group_clamps_cursor() {
        let nodes = vec![TreeNavNode::group(
            "g",
            "Group",
            None,
            vec![
                TreeNavNode::leaf("c1", "C1", None),
                TreeNavNode::leaf("c2", "C2", None),
            ],
        )];
        let mut nav = TreeNav::new(nodes, HashSet::from(["g".into()]));

        // Move cursor to last child "c2" (index 2)
        nav.move_next();
        nav.move_next();
        assert_eq!(nav.cursor(), 2);
        assert_eq!(nav.cursor_item().unwrap().id.as_ref(), "c2");

        // Go back to group header and collapse
        nav.move_prev();
        nav.move_prev();
        assert_eq!(nav.cursor(), 0);
        nav.activate();

        // After collapse, only 1 row; cursor should be clamped
        assert_eq!(nav.rows().len(), 1);
        assert!(nav.cursor() < nav.rows().len());
    }

    // ── select_by_id ────────────────────────────────────────────

    #[test]
    fn select_by_id_existing() {
        let mut nav = TreeNav::new(make_test_tree(), all_expanded());
        nav.select_by_id("hooks");
        assert_eq!(nav.selected_id(), Some(&SharedString::from("hooks")));
        assert_eq!(nav.cursor_item().unwrap().id.as_ref(), "hooks");
    }

    #[test]
    fn select_by_id_missing() {
        let mut nav = TreeNav::new(make_test_tree(), all_expanded());
        nav.select_by_id("general");
        let prev_cursor = nav.cursor();
        nav.select_by_id("nonexistent");
        assert_eq!(nav.cursor(), prev_cursor);
        assert_eq!(nav.selected_id(), Some(&SharedString::from("general")));
    }

    #[test]
    fn select_by_id_in_collapsed_group() {
        let mut nav = TreeNav::new(make_test_tree(), HashSet::new());

        // "hooks" is inside collapsed "connection" group — not visible
        let visible_ids: Vec<&str> = nav.rows().iter().map(|r| r.id.as_ref()).collect();
        assert!(!visible_ids.contains(&"hooks"));

        nav.select_by_id("hooks");

        // Should have expanded "connection" to reveal "hooks"
        assert!(nav.expanded().contains("connection"));
        assert_eq!(nav.selected_id(), Some(&SharedString::from("hooks")));
        assert_eq!(nav.cursor_item().unwrap().id.as_ref(), "hooks");
    }

    // ── set_expanded / expanded ─────────────────────────────────

    #[test]
    fn set_expanded_restores() {
        let mut nav = TreeNav::new(make_test_tree(), HashSet::new());
        let collapsed_count = nav.rows().len();

        nav.set_expanded(all_expanded());
        assert!(nav.rows().len() > collapsed_count);

        let ids: Vec<&str> = nav.rows().iter().map(|r| r.id.as_ref()).collect();
        assert!(ids.contains(&"ssh-tunnels"));
        assert!(ids.contains(&"hooks"));
    }

    #[test]
    fn expanded_reflects_state() {
        let mut nav = TreeNav::new(make_test_tree(), HashSet::new());
        assert!(!nav.expanded().contains("network"));

        // Expand via activate — move to "network" group
        nav.select_by_id("network");
        nav.activate();

        assert!(nav.expanded().contains("network"));
    }
}
