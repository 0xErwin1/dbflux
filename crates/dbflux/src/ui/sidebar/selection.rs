use super::*;

impl Sidebar {
    pub fn select_next(&mut self, cx: &mut Context<Self>) {
        if self.visible_entry_count == 0 {
            return;
        }

        self.pending_delete_item = None;

        self.tree_state.update(cx, |state, cx| {
            let next = match state.selected_index() {
                Some(current) => (current + 1).min(self.visible_entry_count.saturating_sub(1)),
                None => 0,
            };
            state.set_selected_index(Some(next), cx);
            state.scroll_to_item(next, gpui::ScrollStrategy::Center);
        });
        cx.notify();
    }

    pub fn select_prev(&mut self, cx: &mut Context<Self>) {
        if self.visible_entry_count == 0 {
            return;
        }

        self.pending_delete_item = None;

        self.tree_state.update(cx, |state, cx| {
            let prev = match state.selected_index() {
                Some(current) => current.saturating_sub(1),
                None => self.visible_entry_count.saturating_sub(1),
            };
            state.set_selected_index(Some(prev), cx);
            state.scroll_to_item(prev, gpui::ScrollStrategy::Center);
        });
        cx.notify();
    }

    pub fn select_first(&mut self, cx: &mut Context<Self>) {
        if self.visible_entry_count == 0 {
            return;
        }

        self.tree_state.update(cx, |state, cx| {
            state.set_selected_index(Some(0), cx);
            state.scroll_to_item(0, gpui::ScrollStrategy::Center);
        });
        cx.notify();
    }

    pub fn select_last(&mut self, cx: &mut Context<Self>) {
        if self.visible_entry_count == 0 {
            return;
        }

        let last = self.visible_entry_count.saturating_sub(1);
        self.tree_state.update(cx, |state, cx| {
            state.set_selected_index(Some(last), cx);
            state.scroll_to_item(last, gpui::ScrollStrategy::Center);
        });
        cx.notify();
    }

    pub fn extend_select_next(&mut self, cx: &mut Context<Self>) {
        if self.visible_entry_count == 0 {
            return;
        }

        // Add current item to selection
        if let Some(entry) = self.tree_state.read(cx).selected_entry().cloned() {
            let item_id = entry.item().id.to_string();
            self.add_to_selection(&item_id, cx);
        }

        let current = self.tree_state.read(cx).selected_index();
        let next = match current {
            Some(idx) => (idx + 1).min(self.visible_entry_count.saturating_sub(1)),
            None => 0,
        };

        // Move to next and add it to selection
        self.tree_state.update(cx, |state, cx| {
            state.set_selected_index(Some(next), cx);
            state.scroll_to_item(next, gpui::ScrollStrategy::Center);
        });

        // Add the new item to selection
        if let Some(entry) = self.tree_state.read(cx).selected_entry().cloned() {
            let item_id = entry.item().id.to_string();
            self.add_to_selection(&item_id, cx);
        }

        cx.notify();
    }

    pub fn extend_select_prev(&mut self, cx: &mut Context<Self>) {
        if self.visible_entry_count == 0 {
            return;
        }

        // Add current item to selection
        if let Some(entry) = self.tree_state.read(cx).selected_entry().cloned() {
            let item_id = entry.item().id.to_string();
            self.add_to_selection(&item_id, cx);
        }

        let current = self.tree_state.read(cx).selected_index();
        let prev = match current {
            Some(idx) => idx.saturating_sub(1),
            None => self.visible_entry_count.saturating_sub(1),
        };

        // Move to prev and add it to selection
        self.tree_state.update(cx, |state, cx| {
            state.set_selected_index(Some(prev), cx);
            state.scroll_to_item(prev, gpui::ScrollStrategy::Center);
        });

        // Add the new item to selection
        if let Some(entry) = self.tree_state.read(cx).selected_entry().cloned() {
            let item_id = entry.item().id.to_string();
            self.add_to_selection(&item_id, cx);
        }

        cx.notify();
    }

    pub fn toggle_current_selection(&mut self, cx: &mut Context<Self>) {
        let entry = self.tree_state.read(cx).selected_entry().cloned();
        if let Some(entry) = entry {
            let item_id = entry.item().id.to_string();
            self.toggle_selection(&item_id, cx);
        }
    }

    pub fn has_multi_selection(&self) -> bool {
        !self.multi_selection.is_empty()
    }

    pub fn clear_selection(&mut self, cx: &mut Context<Self>) {
        if !self.multi_selection.is_empty() {
            self.multi_selection.clear();
            cx.notify();
        }
    }

    pub(super) fn toggle_selection(&mut self, item_id: &str, cx: &mut Context<Self>) {
        if !Self::is_selectable_item(item_id) {
            return;
        }

        if self.multi_selection.contains(item_id) {
            self.multi_selection.remove(item_id);
        } else {
            self.multi_selection.insert(item_id.to_string());
        }
        cx.notify();
    }

    pub(super) fn add_to_selection(&mut self, item_id: &str, cx: &mut Context<Self>) {
        if !Self::is_selectable_item(item_id) {
            return;
        }

        if self.multi_selection.insert(item_id.to_string()) {
            cx.notify();
        }
    }

    #[allow(dead_code)]
    pub(super) fn is_multi_selected(&self, item_id: &str) -> bool {
        self.multi_selection.contains(item_id)
    }

    pub(super) fn is_selectable_item(item_id: &str) -> bool {
        matches!(
            parse_node_kind(item_id),
            SchemaNodeKind::Profile | SchemaNodeKind::ConnectionFolder
        )
    }

    #[allow(dead_code)]
    pub(super) fn extend_selection_to_index(
        &mut self,
        target_index: usize,
        cx: &mut Context<Self>,
    ) {
        if target_index >= self.visible_entry_count {
            return;
        }

        // Update tree selection
        self.tree_state.update(cx, |state, cx| {
            state.set_selected_index(Some(target_index), cx);
            state.scroll_to_item(target_index, gpui::ScrollStrategy::Center);
        });

        // Add the selected item
        if let Some(entry) = self.tree_state.read(cx).selected_entry().cloned() {
            let item_id = entry.item().id.to_string();
            self.add_to_selection(&item_id, cx);
        }
    }

    pub fn move_selected_items(&mut self, direction: i32, cx: &mut Context<Self>) {
        if self.multi_selection.is_empty() {
            return;
        }

        // Collect node IDs from selection
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

        // Sort by current sort_index
        nodes_to_move.sort_by_key(|(_, idx)| *idx);

        // If moving up, process from top to bottom
        // If moving down, process from bottom to top
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

    fn move_single_node(&mut self, node_id: Uuid, direction: i32, cx: &mut Context<Self>) -> bool {
        let state = self.app_state.read(cx);
        let tree = &state.connection_tree();

        let node = match tree.find_by_id(node_id) {
            Some(n) => n.clone(),
            None => return false,
        };

        // Get siblings
        let siblings: Vec<_> = if let Some(parent_id) = node.parent_id {
            tree.children_of(parent_id)
        } else {
            tree.root_nodes()
        };

        // Find current position
        let current_pos = match siblings.iter().position(|n| n.id == node_id) {
            Some(p) => p,
            None => return false,
        };

        // Calculate new position
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

        // Get the sibling we're swapping with
        let swap_with = siblings[new_pos].id;
        let swap_sort_index = siblings[new_pos].sort_index;
        let node_sort_index = node.sort_index;

        let _ = state;

        // Swap sort indices
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
