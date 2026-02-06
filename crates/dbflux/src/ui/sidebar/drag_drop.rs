use super::*;

impl Sidebar {
    pub(super) fn handle_drop(
        &mut self,
        drag_state: &SidebarDragState,
        target_parent_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) {
        // Collect all node IDs to move (primary + additional from multi-selection)
        let all_node_ids = drag_state.all_node_ids();

        let tree_node_ids: Vec<Uuid> = {
            let state = self.app_state.read(cx);
            all_node_ids
                .iter()
                .filter_map(|&node_id| {
                    if state.connection_tree().find_by_id(node_id).is_some() {
                        Some(node_id)
                    } else {
                        state
                            .connection_tree()
                            .find_by_profile(node_id)
                            .map(|n| n.id)
                    }
                })
                .collect()
        };

        let mut moved = false;
        for tree_node_id in tree_node_ids {
            let would_cycle = self
                .app_state
                .read(cx)
                .connection_tree()
                .would_create_cycle(tree_node_id, target_parent_id);

            if would_cycle {
                continue;
            }

            self.app_state.update(cx, |state, _cx| {
                if state.move_tree_node(tree_node_id, target_parent_id) {
                    moved = true;
                }
            });
        }

        if moved {
            self.app_state.update(cx, |_, cx| {
                cx.emit(AppStateChanged);
            });
            self.clear_selection(cx);
            self.refresh_tree(cx);
        }
    }

    pub(super) fn handle_drop_with_position(
        &mut self,
        drag_state: &SidebarDragState,
        cx: &mut Context<Self>,
    ) {
        let Some(drop_target) = self.drop_target.take() else {
            return;
        };

        // Collect all node IDs to move (primary + additional from multi-selection)
        let all_node_ids = drag_state.all_node_ids();

        let tree_node_ids: Vec<Uuid> = {
            let state = self.app_state.read(cx);
            all_node_ids
                .iter()
                .filter_map(|&node_id| {
                    if state.connection_tree().find_by_id(node_id).is_some() {
                        Some(node_id)
                    } else {
                        state
                            .connection_tree()
                            .find_by_profile(node_id)
                            .map(|n| n.id)
                    }
                })
                .collect()
        };

        if tree_node_ids.is_empty() {
            return;
        }

        let (target_parent_id, mut after_id) =
            self.resolve_drop_target(&drop_target.item_id, drop_target.position, cx);

        // Move each node, updating after_id to chain them
        let mut moved = false;
        for tree_node_id in tree_node_ids {
            let would_cycle = self
                .app_state
                .read(cx)
                .connection_tree()
                .would_create_cycle(tree_node_id, target_parent_id);

            if would_cycle {
                continue;
            }

            self.app_state.update(cx, |state, _cx| {
                if state.move_tree_node_to_position(tree_node_id, target_parent_id, after_id) {
                    moved = true;
                    // Next node should be placed after this one
                    after_id = Some(tree_node_id);
                }
            });
        }

        if moved {
            self.app_state.update(cx, |_, cx| {
                cx.emit(AppStateChanged);
            });
            self.clear_selection(cx);
            self.refresh_tree(cx);
        }
    }

    /// Resolves a drop target to (parent_id, after_id) for positioning.
    fn resolve_drop_target(
        &self,
        item_id: &str,
        position: DropPosition,
        cx: &App,
    ) -> (Option<Uuid>, Option<Uuid>) {
        let state = self.app_state.read(cx);

        // Parse the target item
        let (target_node_id, is_folder) = match parse_node_id(item_id) {
            Some(SchemaNodeId::ConnectionFolder { node_id }) => (Some(node_id), true),
            Some(SchemaNodeId::Profile { profile_id }) => {
                let node_id = state
                    .connection_tree()
                    .find_by_profile(profile_id)
                    .map(|n| n.id);
                (node_id, false)
            }
            _ => (None, false),
        };

        let Some(target_node_id) = target_node_id else {
            return (None, None);
        };

        let target_node = state.connection_tree().find_by_id(target_node_id);
        let target_parent_id = target_node.and_then(|n| n.parent_id);

        match position {
            DropPosition::Into if is_folder => {
                // Drop into folder: parent is the folder, insert at end
                (Some(target_node_id), None)
            }
            DropPosition::Before => {
                // Drop before: same parent, find the sibling before target
                let siblings = if let Some(pid) = target_parent_id {
                    state.connection_tree().children_of(pid)
                } else {
                    state.connection_tree().root_nodes()
                };
                let pos = siblings.iter().position(|n| n.id == target_node_id);
                let after_id = pos.and_then(|p| {
                    if p > 0 {
                        Some(siblings[p - 1].id)
                    } else {
                        None
                    }
                });
                (target_parent_id, after_id)
            }
            DropPosition::After | DropPosition::Into => {
                // Drop after (or Into non-folder): same parent, after target
                (target_parent_id, Some(target_node_id))
            }
        }
    }

    pub(super) fn set_drop_target(
        &mut self,
        item_id: String,
        position: DropPosition,
        cx: &mut Context<Self>,
    ) {
        let new_target = DropTarget { item_id, position };
        if self.drop_target.as_ref() != Some(&new_target) {
            self.drop_target = Some(new_target);
            cx.notify();
        }
    }

    pub(super) fn clear_drop_target(&mut self, cx: &mut Context<Self>) {
        if self.drop_target.is_some() {
            self.drop_target = None;
            cx.notify();
        }
    }

    /// Starts tracking hover over a folder during drag for auto-expand.
    pub(super) fn start_drag_hover_folder(&mut self, folder_id: Uuid, cx: &mut Context<Self>) {
        if self.drag_hover_folder != Some(folder_id) {
            self.drag_hover_folder = Some(folder_id);
            self.drag_hover_start = Some(std::time::Instant::now());

            // Schedule a check after the delay
            let delay = std::time::Duration::from_millis(600);
            cx.spawn(async move |this, cx| {
                cx.background_executor().timer(delay).await;
                this.update(cx, |this, cx| {
                    this.check_auto_expand_folder(cx);
                })
                .ok();
            })
            .detach();
        }
    }

    /// Clears the drag hover tracking.
    pub(super) fn clear_drag_hover_folder(&mut self, cx: &mut Context<Self>) {
        if self.drag_hover_folder.is_some() {
            self.drag_hover_folder = None;
            self.drag_hover_start = None;
            cx.notify();
        }
    }

    /// Checks if a folder should be auto-expanded after hover delay.
    fn check_auto_expand_folder(&mut self, cx: &mut Context<Self>) {
        let Some(folder_id) = self.drag_hover_folder else {
            return;
        };

        let Some(hover_start) = self.drag_hover_start else {
            return;
        };

        // Check if we've been hovering long enough (600ms)
        if hover_start.elapsed() >= std::time::Duration::from_millis(600) {
            // Check if the folder is collapsed
            let is_collapsed = self
                .app_state
                .read(cx)
                .connection_tree()
                .find_by_id(folder_id)
                .map(|n| n.collapsed)
                .unwrap_or(false);

            if is_collapsed {
                self.app_state.update(cx, |state, _cx| {
                    state.set_folder_collapsed(folder_id, false);
                });
                self.refresh_tree(cx);
            }
        }
    }

    /// Checks if we should auto-scroll based on the hovered item index.
    pub(super) fn check_auto_scroll(&mut self, item_index: usize, cx: &mut Context<Self>) {
        let total = self.visible_entry_count;
        if total == 0 {
            return;
        }

        // Scroll up if hovering near the top (first 2 items)
        // Scroll down if hovering near the bottom (last 2 items)
        let new_direction = if item_index <= 1 {
            -1 // Scroll up
        } else if item_index >= total.saturating_sub(2) {
            1 // Scroll down
        } else {
            0 // No scroll
        };

        if new_direction != self.auto_scroll_direction {
            self.auto_scroll_direction = new_direction;

            if new_direction != 0 {
                // Start auto-scroll timer
                cx.spawn(async move |this, cx| {
                    Self::auto_scroll_loop(this, cx).await;
                })
                .detach();
            }
        }
    }

    /// Continuously scrolls while auto_scroll_direction is non-zero.
    async fn auto_scroll_loop(this: WeakEntity<Self>, cx: &mut AsyncApp) {
        let interval = std::time::Duration::from_millis(50);

        loop {
            cx.background_executor().timer(interval).await;

            let should_continue = this
                .update(cx, |this, cx| {
                    if this.auto_scroll_direction == 0 {
                        return false;
                    }

                    this.do_auto_scroll(cx);
                    true
                })
                .unwrap_or(false);

            if !should_continue {
                break;
            }
        }
    }

    /// Performs one step of auto-scroll.
    fn do_auto_scroll(&mut self, cx: &mut Context<Self>) {
        let direction = self.auto_scroll_direction;
        if direction == 0 {
            return;
        }

        self.tree_state.update(cx, |state, cx| {
            let current = state.selected_index().unwrap_or(0);
            let total = self.visible_entry_count;

            let target = if direction < 0 {
                // Scroll up
                current.saturating_sub(1)
            } else {
                // Scroll down
                (current + 1).min(total.saturating_sub(1))
            };

            state.scroll_to_item(target, gpui::ScrollStrategy::Top);
            cx.notify();
        });
    }

    /// Stops auto-scrolling.
    pub(super) fn stop_auto_scroll(&mut self, _cx: &mut Context<Self>) {
        self.auto_scroll_direction = 0;
    }
}
