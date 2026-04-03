use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionKeyboardMovePlan {
    None,
    IntoFolder {
        folder_id: Uuid,
    },
    Outdent {
        target_parent_id: Option<Uuid>,
        after_id: Option<Uuid>,
    },
}

impl Sidebar {
    pub fn select_next(&mut self, cx: &mut Context<Self>) {
        let visible_count = self.active_visible_entry_count(cx);
        if visible_count == 0 {
            return;
        }

        self.pending_delete_item = None;

        let tree = self.active_tree_state().clone();
        tree.update(cx, |state, cx| {
            let next = match state.selected_index() {
                Some(current) => (current + 1).min(visible_count.saturating_sub(1)),
                None => 0,
            };
            state.set_selected_index(Some(next), cx);
            state.scroll_to_item(next, gpui::ScrollStrategy::Center);
        });

        if let Some(entry) = tree.read(cx).selected_entry().cloned() {
            self.set_selection_anchor(entry.item().id.as_ref());
        }

        cx.notify();
    }

    pub fn select_prev(&mut self, cx: &mut Context<Self>) {
        let visible_count = self.active_visible_entry_count(cx);
        if visible_count == 0 {
            return;
        }

        self.pending_delete_item = None;

        let tree = self.active_tree_state().clone();
        tree.update(cx, |state, cx| {
            let prev = match state.selected_index() {
                Some(current) => current.saturating_sub(1),
                None => visible_count.saturating_sub(1),
            };
            state.set_selected_index(Some(prev), cx);
            state.scroll_to_item(prev, gpui::ScrollStrategy::Center);
        });

        if let Some(entry) = tree.read(cx).selected_entry().cloned() {
            self.set_selection_anchor(entry.item().id.as_ref());
        }

        cx.notify();
    }

    pub fn select_first(&mut self, cx: &mut Context<Self>) {
        let visible_count = self.active_visible_entry_count(cx);
        if visible_count == 0 {
            return;
        }

        let tree = self.active_tree_state().clone();
        tree.update(cx, |state, cx| {
            state.set_selected_index(Some(0), cx);
            state.scroll_to_item(0, gpui::ScrollStrategy::Center);
        });

        if let Some(entry) = tree.read(cx).selected_entry().cloned() {
            self.set_selection_anchor(entry.item().id.as_ref());
        }

        cx.notify();
    }

    pub fn select_last(&mut self, cx: &mut Context<Self>) {
        let visible_count = self.active_visible_entry_count(cx);
        if visible_count == 0 {
            return;
        }

        let last = visible_count.saturating_sub(1);
        let tree = self.active_tree_state().clone();
        tree.update(cx, |state, cx| {
            state.set_selected_index(Some(last), cx);
            state.scroll_to_item(last, gpui::ScrollStrategy::Center);
        });

        if let Some(entry) = tree.read(cx).selected_entry().cloned() {
            self.set_selection_anchor(entry.item().id.as_ref());
        }

        cx.notify();
    }

    pub fn extend_select_next(&mut self, cx: &mut Context<Self>) {
        self.extend_selection_by_delta(1, cx);
    }

    pub fn extend_select_prev(&mut self, cx: &mut Context<Self>) {
        self.extend_selection_by_delta(-1, cx);
    }

    fn extend_selection_by_delta(&mut self, delta: isize, cx: &mut Context<Self>) {
        let visible_count = self.active_visible_entry_count(cx);
        if visible_count == 0 {
            return;
        }

        let tree = self.active_tree_state().clone();

        if self.active_anchor().is_none()
            && let Some(entry) = tree.read(cx).selected_entry().cloned()
        {
            self.set_selection_anchor(entry.item().id.as_ref());
        }

        let target_index = {
            let current = tree.read(cx).selected_index().unwrap_or(0);

            if delta.is_negative() {
                current.saturating_sub(delta.unsigned_abs())
            } else {
                (current + delta as usize).min(visible_count.saturating_sub(1))
            }
        };

        tree.update(cx, |state, cx| {
            state.set_selected_index(Some(target_index), cx);
            state.scroll_to_item(target_index, gpui::ScrollStrategy::Center);
        });

        if let Some(target_entry) = tree.read(cx).selected_entry().cloned() {
            self.select_range_to_item(target_entry.item().id.as_ref(), cx);
        }

        cx.notify();
    }

    pub fn toggle_current_selection(&mut self, cx: &mut Context<Self>) {
        let entry = self.active_tree_state().read(cx).selected_entry().cloned();
        if let Some(entry) = entry {
            let item_id = entry.item().id.to_string();
            self.toggle_selection(&item_id, cx);
            self.set_selection_anchor(&item_id);
        }
    }

    pub fn has_multi_selection(&self) -> bool {
        !self.active_selection().is_empty()
    }

    pub fn clear_selection(&mut self, cx: &mut Context<Self>) {
        let mut changed = false;

        match self.active_tab {
            SidebarTab::Connections => {
                if !self.multi_selection.is_empty() {
                    self.multi_selection.clear();
                    changed = true;
                }

                if self.selection_anchor.take().is_some() {
                    changed = true;
                }
            }
            SidebarTab::Scripts => {
                if !self.scripts_multi_selection.is_empty() {
                    self.scripts_multi_selection.clear();
                    changed = true;
                }

                if self.scripts_selection_anchor.take().is_some() {
                    changed = true;
                }
            }
        }

        if changed {
            cx.notify();
        }
    }

    pub(super) fn toggle_selection(&mut self, item_id: &str, cx: &mut Context<Self>) {
        if !Self::is_selectable_item(item_id) {
            return;
        }

        let selection = self.active_selection_mut();
        if selection.contains(item_id) {
            selection.remove(item_id);
        } else {
            selection.insert(item_id.to_string());
        }

        self.set_selection_anchor(item_id);
        cx.notify();
    }

    pub(super) fn is_selectable_item(item_id: &str) -> bool {
        match parse_node_id(item_id) {
            Some(SchemaNodeId::Profile { .. }) | Some(SchemaNodeId::ConnectionFolder { .. }) => {
                true
            }
            Some(SchemaNodeId::ScriptFile { .. }) => true,
            Some(SchemaNodeId::ScriptsFolder { path: Some(_) }) => true,
            Some(SchemaNodeId::ScriptsFolder { path: None }) => false,
            _ => false,
        }
    }

    pub(super) fn select_range_to_item(&mut self, target_item_id: &str, cx: &mut Context<Self>) {
        let visible_ids = self.active_visible_item_ids(cx);
        if visible_ids.is_empty() {
            return;
        }

        let target_index = match visible_ids.iter().position(|id| id == target_item_id) {
            Some(index) => index,
            None => return,
        };

        let anchor_id = self
            .active_anchor()
            .map(ToOwned::to_owned)
            .or_else(|| {
                self.active_tree_state()
                    .read(cx)
                    .selected_entry()
                    .map(|entry| entry.item().id.to_string())
            })
            .unwrap_or_else(|| target_item_id.to_string());

        let anchor_index = visible_ids
            .iter()
            .position(|id| id == &anchor_id)
            .unwrap_or(target_index);

        let start = anchor_index.min(target_index);
        let end = anchor_index.max(target_index);

        let selection: HashSet<String> = visible_ids[start..=end]
            .iter()
            .filter(|id| Self::is_selectable_item(id))
            .cloned()
            .collect();

        match self.active_tab {
            SidebarTab::Connections => {
                self.multi_selection = selection;
                self.selection_anchor = Some(anchor_id);
            }
            SidebarTab::Scripts => {
                self.scripts_multi_selection = selection;
                self.scripts_selection_anchor = Some(anchor_id);
            }
        }
    }

    pub(super) fn set_selection_anchor(&mut self, item_id: &str) {
        if !Self::is_selectable_item(item_id) {
            return;
        }

        match self.active_tab {
            SidebarTab::Connections => {
                self.selection_anchor = Some(item_id.to_string());
            }
            SidebarTab::Scripts => {
                self.scripts_selection_anchor = Some(item_id.to_string());
            }
        }
    }

    fn active_selection(&self) -> &HashSet<String> {
        match self.active_tab {
            SidebarTab::Connections => &self.multi_selection,
            SidebarTab::Scripts => &self.scripts_multi_selection,
        }
    }

    fn active_selection_mut(&mut self) -> &mut HashSet<String> {
        match self.active_tab {
            SidebarTab::Connections => &mut self.multi_selection,
            SidebarTab::Scripts => &mut self.scripts_multi_selection,
        }
    }

    fn active_anchor(&self) -> Option<&str> {
        match self.active_tab {
            SidebarTab::Connections => self.selection_anchor.as_deref(),
            SidebarTab::Scripts => self.scripts_selection_anchor.as_deref(),
        }
    }

    fn active_visible_entry_count(&self, cx: &Context<Self>) -> usize {
        self.active_visible_item_ids(cx).len()
    }

    fn active_visible_item_ids(&self, cx: &Context<Self>) -> Vec<String> {
        match self.active_tab {
            SidebarTab::Connections => {
                let items = self.build_tree_items_with_overrides(cx);
                Self::collect_visible_item_ids(&items)
            }
            SidebarTab::Scripts => {
                let state = self.app_state.read(cx);
                let entries = match state.scripts_directory() {
                    Some(dir) => {
                        dbflux_core::filter_entries(dir.entries(), &self.scripts_search_query)
                    }
                    None => Vec::new(),
                };
                let items = Self::build_scripts_tree_items(&entries);
                Self::collect_visible_item_ids(&items)
            }
        }
    }

    fn collect_visible_item_ids(items: &[TreeItem]) -> Vec<String> {
        fn walk(items: &[TreeItem], out: &mut Vec<String>) {
            for item in items {
                out.push(item.id.to_string());

                if item.is_expanded() && item.is_folder() {
                    walk(&item.children, out);
                }
            }
        }

        let mut out = Vec::new();
        walk(items, &mut out);
        out
    }

    fn collect_all_item_ids(items: &[TreeItem], out: &mut HashSet<String>) {
        for item in items {
            out.insert(item.id.to_string());
            Self::collect_all_item_ids(&item.children, out);
        }
    }

    pub(super) fn prune_connection_selection(&mut self, items: &[TreeItem]) {
        let mut valid_ids = HashSet::new();
        Self::collect_all_item_ids(items, &mut valid_ids);

        self.multi_selection.retain(|id| valid_ids.contains(id));

        if self
            .selection_anchor
            .as_ref()
            .is_some_and(|anchor| !valid_ids.contains(anchor))
        {
            self.selection_anchor = None;
        }
    }

    pub(super) fn prune_scripts_selection(&mut self, items: &[TreeItem]) {
        let mut valid_ids = HashSet::new();
        Self::collect_all_item_ids(items, &mut valid_ids);

        self.scripts_multi_selection
            .retain(|id| valid_ids.contains(id));

        if self
            .scripts_selection_anchor
            .as_ref()
            .is_some_and(|anchor| !valid_ids.contains(anchor))
        {
            self.scripts_selection_anchor = None;
        }
    }

    pub fn move_selected_items(&mut self, direction: i32, cx: &mut Context<Self>) {
        if self.active_tab == SidebarTab::Scripts {
            if direction > 0 {
                self.move_selected_scripts_to_selected_folder(cx);
            } else if direction < 0 {
                self.move_selected_scripts_out_of_folder(cx);
            }
            return;
        }

        if self.multi_selection.is_empty() {
            return;
        }

        if self.try_apply_connection_keyboard_nesting(direction, cx) {
            return;
        }

        let state = self.app_state.read(cx);
        let mut nodes_to_move: Vec<(Uuid, i32)> = Vec::new();

        for item_id in &self.multi_selection {
            if let Some(node_id) = self.item_id_to_node_id(item_id, state.connection_tree())
                && let Some(node) = state.connection_tree().find_by_id(node_id)
            {
                nodes_to_move.push((node_id, node.sort_index));
            }
        }

        if nodes_to_move.is_empty() {
            return;
        }

        nodes_to_move.sort_by_key(|(_, idx)| *idx);

        if direction > 0 {
            nodes_to_move.reverse();
        }

        let _ = state;

        let mut moved = false;
        for (node_id, _) in nodes_to_move {
            if self.move_single_node(node_id, direction, cx) {
                moved = true;
            }
        }

        if moved {
            self.app_state.update(cx, |_, cx| {
                cx.emit(AppStateChanged);
            });
            self.refresh_tree(cx);
        }
    }

    fn try_apply_connection_keyboard_nesting(
        &mut self,
        direction: i32,
        cx: &mut Context<Self>,
    ) -> bool {
        let node_ids = self.resolve_selected_connection_node_ids(cx);
        if node_ids.is_empty() {
            return false;
        }

        let state = self.app_state.read(cx);
        let tree = state.connection_tree();
        let plan = Self::plan_connection_keyboard_nesting(tree, &node_ids, direction);
        let _ = state;

        let mut moved = false;

        match plan {
            ConnectionKeyboardMovePlan::None => return false,
            ConnectionKeyboardMovePlan::IntoFolder { folder_id } => {
                let mut current_after_id = None;

                for node_id in node_ids {
                    let would_cycle = self
                        .app_state
                        .read(cx)
                        .connection_tree()
                        .would_create_cycle(node_id, Some(folder_id));

                    if would_cycle {
                        continue;
                    }

                    self.app_state.update(cx, |state, _cx| {
                        if state.move_tree_node_to_position(
                            node_id,
                            Some(folder_id),
                            current_after_id,
                        ) {
                            moved = true;
                            current_after_id = Some(node_id);
                        }
                    });
                }
            }
            ConnectionKeyboardMovePlan::Outdent {
                target_parent_id,
                after_id,
            } => {
                let mut current_after_id = after_id;

                for node_id in node_ids {
                    let would_cycle = self
                        .app_state
                        .read(cx)
                        .connection_tree()
                        .would_create_cycle(node_id, target_parent_id);

                    if would_cycle {
                        continue;
                    }

                    self.app_state.update(cx, |state, _cx| {
                        if state.move_tree_node_to_position(
                            node_id,
                            target_parent_id,
                            current_after_id,
                        ) {
                            moved = true;
                            current_after_id = Some(node_id);
                        }
                    });
                }
            }
        }

        if moved {
            self.app_state.update(cx, |_, cx| {
                cx.emit(AppStateChanged);
            });
            self.refresh_tree(cx);
        }

        moved
    }

    fn plan_connection_keyboard_nesting(
        tree: &dbflux_core::ConnectionTree,
        node_ids: &[Uuid],
        direction: i32,
    ) -> ConnectionKeyboardMovePlan {
        if node_ids.is_empty() {
            return ConnectionKeyboardMovePlan::None;
        }

        let Some(first_parent_id) = node_ids
            .first()
            .and_then(|node_id| tree.find_by_id(*node_id))
            .and_then(|node| node.parent_id)
        else {
            return ConnectionKeyboardMovePlan::None;
        };

        let same_parent = node_ids.iter().all(|node_id| {
            tree.find_by_id(*node_id).and_then(|node| node.parent_id) == Some(first_parent_id)
        });

        if !same_parent {
            return ConnectionKeyboardMovePlan::None;
        }

        let siblings = tree.children_of(first_parent_id);
        let node_set: HashSet<Uuid> = node_ids.iter().copied().collect();
        let mut positions: Vec<usize> = node_ids
            .iter()
            .filter_map(|node_id| siblings.iter().position(|sibling| sibling.id == *node_id))
            .collect();

        if positions.len() != node_ids.len() {
            return ConnectionKeyboardMovePlan::None;
        }

        positions.sort_unstable();

        if direction > 0 {
            let Some(max_position) = positions.last().copied() else {
                return ConnectionKeyboardMovePlan::None;
            };

            let next_position = max_position + 1;
            if next_position >= siblings.len() {
                return ConnectionKeyboardMovePlan::None;
            }

            let next_sibling = siblings[next_position];
            if next_sibling.kind == dbflux_core::ConnectionTreeNodeKind::Folder
                && !next_sibling.collapsed
                && !node_set.contains(&next_sibling.id)
            {
                return ConnectionKeyboardMovePlan::IntoFolder {
                    folder_id: next_sibling.id,
                };
            }

            return ConnectionKeyboardMovePlan::None;
        }

        if direction < 0 {
            let Some(min_position) = positions.first().copied() else {
                return ConnectionKeyboardMovePlan::None;
            };

            if min_position != 0 {
                return ConnectionKeyboardMovePlan::None;
            }

            let target_parent_id = tree
                .find_by_id(first_parent_id)
                .and_then(|node| node.parent_id);

            if target_parent_id.is_none() {
                return ConnectionKeyboardMovePlan::None;
            }

            let parent_siblings = if let Some(parent_id) = target_parent_id {
                tree.children_of(parent_id)
            } else {
                tree.root_nodes()
            };

            let parent_position = match parent_siblings
                .iter()
                .position(|sibling| sibling.id == first_parent_id)
            {
                Some(position) => position,
                None => return ConnectionKeyboardMovePlan::None,
            };

            let after_id = if parent_position > 0 {
                Some(parent_siblings[parent_position - 1].id)
            } else {
                None
            };

            return ConnectionKeyboardMovePlan::Outdent {
                target_parent_id,
                after_id,
            };
        }

        ConnectionKeyboardMovePlan::None
    }

    fn resolve_selected_connection_node_ids(&self, cx: &Context<Self>) -> Vec<Uuid> {
        let state = self.app_state.read(cx);
        let tree = state.connection_tree();

        let mut node_ids: Vec<Uuid> = self
            .multi_selection
            .iter()
            .filter_map(|item_id| self.item_id_to_node_id(item_id, tree))
            .collect();

        if node_ids.is_empty() {
            return Vec::new();
        }

        node_ids.sort_unstable();
        node_ids.dedup();

        let node_set: HashSet<Uuid> = node_ids.iter().copied().collect();

        let mut filtered: Vec<Uuid> = node_ids
            .into_iter()
            .filter(|node_id| !Self::has_selected_ancestor(*node_id, &node_set, tree))
            .collect();

        let order = Self::flatten_connection_tree_order_for_selection(tree);
        let order_map: HashMap<Uuid, usize> =
            order.iter().enumerate().map(|(ix, id)| (*id, ix)).collect();

        filtered.sort_by_key(|id| order_map.get(id).copied().unwrap_or(usize::MAX));

        filtered
    }

    fn has_selected_ancestor(
        node_id: Uuid,
        node_set: &HashSet<Uuid>,
        tree: &dbflux_core::ConnectionTree,
    ) -> bool {
        let mut current = tree.find_by_id(node_id).and_then(|n| n.parent_id);

        while let Some(parent_id) = current {
            if node_set.contains(&parent_id) {
                return true;
            }

            current = tree.find_by_id(parent_id).and_then(|n| n.parent_id);
        }

        false
    }

    fn flatten_connection_tree_order_for_selection(
        tree: &dbflux_core::ConnectionTree,
    ) -> Vec<Uuid> {
        fn walk(tree: &dbflux_core::ConnectionTree, parent: Option<Uuid>, out: &mut Vec<Uuid>) {
            let nodes = if let Some(parent_id) = parent {
                tree.children_of(parent_id)
            } else {
                tree.root_nodes()
            };

            for node in nodes {
                out.push(node.id);
                walk(tree, Some(node.id), out);
            }
        }

        let mut out = Vec::new();
        walk(tree, None, &mut out);
        out
    }

    fn move_single_node(&mut self, node_id: Uuid, direction: i32, cx: &mut Context<Self>) -> bool {
        let state = self.app_state.read(cx);
        let tree = &state.connection_tree();

        let node = match tree.find_by_id(node_id) {
            Some(n) => n.clone(),
            None => return false,
        };

        let siblings: Vec<_> = if let Some(parent_id) = node.parent_id {
            tree.children_of(parent_id)
        } else {
            tree.root_nodes()
        };

        let current_pos = match siblings.iter().position(|n| n.id == node_id) {
            Some(p) => p,
            None => return false,
        };

        let new_pos = if direction < 0 {
            if current_pos == 0 {
                return false;
            }
            current_pos - 1
        } else {
            if current_pos >= siblings.len() - 1 {
                return false;
            }
            current_pos + 1
        };

        let swap_with = siblings[new_pos].id;
        let swap_sort_index = siblings[new_pos].sort_index;
        let node_sort_index = node.sort_index;

        let _ = state;

        self.app_state.update(cx, |state, _cx| {
            if let Some(n) = state.connection_tree_mut().find_by_id_mut(node_id) {
                n.sort_index = swap_sort_index;
            }
            if let Some(n) = state.connection_tree_mut().find_by_id_mut(swap_with) {
                n.sort_index = node_sort_index;
            }
            state.save_connection_tree();
        });

        true
    }

    pub(super) fn item_id_to_node_id(
        &self,
        item_id: &str,
        tree: &dbflux_core::ConnectionTree,
    ) -> Option<Uuid> {
        match parse_node_id(item_id) {
            Some(SchemaNodeId::ConnectionFolder { node_id }) => Some(node_id),
            Some(SchemaNodeId::Profile { profile_id }) => {
                tree.find_by_profile(profile_id).map(|n| n.id)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConnectionKeyboardMovePlan, Sidebar};
    use dbflux_core::{ConnectionTree, ConnectionTreeNode};
    use uuid::Uuid;

    struct SampleIds {
        work: Uuid,
        business: Uuid,
        conn1: Uuid,
        conn2: Uuid,
        conn3: Uuid,
        folder1: Uuid,
        folder2: Uuid,
        folder3: Uuid,
    }

    fn build_sample_tree(
        folder1_collapsed: bool,
        add_leading_sibling: bool,
    ) -> (ConnectionTree, SampleIds) {
        let mut tree = ConnectionTree::new();

        let work = ConnectionTreeNode::new_folder("work", None, 1000);
        let work_id = work.id;
        tree.add_node(work);

        let business = ConnectionTreeNode::new_folder("Business", Some(work_id), 1000);
        let business_id = business.id;
        tree.add_node(business);

        if add_leading_sibling {
            let leading = ConnectionTreeNode::new_folder("leading", Some(business_id), 500);
            tree.add_node(leading);
        }

        let conn1 = ConnectionTreeNode::new_connection_ref(Uuid::new_v4(), Some(business_id), 1000);
        let conn1_id = conn1.id;
        tree.add_node(conn1);

        let conn2 = ConnectionTreeNode::new_connection_ref(Uuid::new_v4(), Some(business_id), 2000);
        let conn2_id = conn2.id;
        tree.add_node(conn2);

        let conn3 = ConnectionTreeNode::new_connection_ref(Uuid::new_v4(), Some(business_id), 3000);
        let conn3_id = conn3.id;
        tree.add_node(conn3);

        let mut folder1 = ConnectionTreeNode::new_folder("folder1", Some(business_id), 4000);
        folder1.collapsed = folder1_collapsed;
        let folder1_id = folder1.id;
        tree.add_node(folder1);

        let folder2 = ConnectionTreeNode::new_folder("folder2", Some(folder1_id), 1000);
        let folder2_id = folder2.id;
        tree.add_node(folder2);

        let folder3 = ConnectionTreeNode::new_folder("folder3", Some(business_id), 5000);
        let folder3_id = folder3.id;
        tree.add_node(folder3);

        (
            tree,
            SampleIds {
                work: work_id,
                business: business_id,
                conn1: conn1_id,
                conn2: conn2_id,
                conn3: conn3_id,
                folder1: folder1_id,
                folder2: folder2_id,
                folder3: folder3_id,
            },
        )
    }

    fn apply_move_plan(
        tree: &mut ConnectionTree,
        node_ids: &[Uuid],
        plan: ConnectionKeyboardMovePlan,
    ) {
        match plan {
            ConnectionKeyboardMovePlan::None => {}
            ConnectionKeyboardMovePlan::IntoFolder { folder_id } => {
                let mut after_id = None;
                for node_id in node_ids {
                    if tree.move_node_to_position(*node_id, Some(folder_id), after_id) {
                        after_id = Some(*node_id);
                    }
                }
            }
            ConnectionKeyboardMovePlan::Outdent {
                target_parent_id,
                after_id,
            } => {
                let mut current_after_id = after_id;
                for node_id in node_ids {
                    if tree.move_node_to_position(*node_id, target_parent_id, current_after_id) {
                        current_after_id = Some(*node_id);
                    }
                }
            }
        }
    }

    #[test]
    fn keyboard_down_nests_into_next_expanded_folder() {
        let (tree, ids) = build_sample_tree(false, false);

        let plan =
            Sidebar::plan_connection_keyboard_nesting(&tree, &[ids.conn1, ids.conn2, ids.conn3], 1);

        assert_eq!(
            plan,
            ConnectionKeyboardMovePlan::IntoFolder {
                folder_id: ids.folder1,
            }
        );
    }

    #[test]
    fn keyboard_down_does_not_nest_into_collapsed_folder() {
        let (tree, ids) = build_sample_tree(true, false);

        let plan =
            Sidebar::plan_connection_keyboard_nesting(&tree, &[ids.conn1, ids.conn2, ids.conn3], 1);

        assert_eq!(plan, ConnectionKeyboardMovePlan::None);
    }

    #[test]
    fn keyboard_up_outdents_when_selection_starts_at_parent_top() {
        let (tree, ids) = build_sample_tree(false, false);

        let plan = Sidebar::plan_connection_keyboard_nesting(
            &tree,
            &[ids.conn1, ids.conn2, ids.conn3],
            -1,
        );

        assert_eq!(
            plan,
            ConnectionKeyboardMovePlan::Outdent {
                target_parent_id: Some(ids.work),
                after_id: None,
            }
        );
    }

    #[test]
    fn keyboard_up_outdents_before_parent_with_previous_sibling_anchor() {
        let (mut tree, ids) = build_sample_tree(false, false);

        let parent_prev = ConnectionTreeNode::new_folder("prev", Some(ids.work), 500);
        let parent_prev_id = parent_prev.id;
        tree.add_node(parent_prev);

        let plan = Sidebar::plan_connection_keyboard_nesting(
            &tree,
            &[ids.conn1, ids.conn2, ids.conn3],
            -1,
        );

        assert_eq!(
            plan,
            ConnectionKeyboardMovePlan::Outdent {
                target_parent_id: Some(ids.work),
                after_id: Some(parent_prev_id),
            }
        );
    }

    #[test]
    fn keyboard_up_does_not_outdent_when_not_at_parent_top() {
        let (tree, ids) = build_sample_tree(false, true);

        let plan = Sidebar::plan_connection_keyboard_nesting(
            &tree,
            &[ids.conn1, ids.conn2, ids.conn3],
            -1,
        );

        assert_eq!(plan, ConnectionKeyboardMovePlan::None);
    }

    #[test]
    fn keyboard_nesting_requires_same_parent() {
        let (tree, ids) = build_sample_tree(false, false);

        let plan = Sidebar::plan_connection_keyboard_nesting(&tree, &[ids.conn1, ids.folder2], 1);

        assert_eq!(plan, ConnectionKeyboardMovePlan::None);
    }

    #[test]
    fn keyboard_down_places_selection_at_top_of_target_folder() {
        let (mut tree, ids) = build_sample_tree(false, false);
        let selected = vec![ids.conn1, ids.conn2, ids.conn3];

        let plan = Sidebar::plan_connection_keyboard_nesting(&tree, &selected, 1);
        assert_eq!(
            plan,
            ConnectionKeyboardMovePlan::IntoFolder {
                folder_id: ids.folder1,
            }
        );

        apply_move_plan(&mut tree, &selected, plan);

        let folder_children: Vec<Uuid> =
            tree.children_of(ids.folder1).iter().map(|n| n.id).collect();

        assert_eq!(
            folder_children,
            vec![ids.conn1, ids.conn2, ids.conn3, ids.folder2]
        );
    }

    #[test]
    fn keyboard_up_places_selection_before_parent_folder() {
        let (mut tree, ids) = build_sample_tree(false, false);
        let selected = vec![ids.conn1, ids.conn2, ids.conn3];

        let into_plan = Sidebar::plan_connection_keyboard_nesting(&tree, &selected, 1);
        apply_move_plan(&mut tree, &selected, into_plan);

        let outdent_plan = Sidebar::plan_connection_keyboard_nesting(&tree, &selected, -1);
        assert_eq!(
            outdent_plan,
            ConnectionKeyboardMovePlan::Outdent {
                target_parent_id: Some(ids.business),
                after_id: None,
            }
        );

        apply_move_plan(&mut tree, &selected, outdent_plan);

        let business_children: Vec<Uuid> = tree
            .children_of(ids.business)
            .iter()
            .map(|n| n.id)
            .collect();

        assert_eq!(
            business_children,
            vec![ids.conn1, ids.conn2, ids.conn3, ids.folder1, ids.folder3]
        );
    }
}
