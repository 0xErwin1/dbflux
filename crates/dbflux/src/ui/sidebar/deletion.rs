use super::*;

impl Sidebar {
    pub fn request_delete_selected(&mut self, cx: &mut Context<Self>) {
        if self.pending_delete_item.is_some() {
            self.confirm_pending_delete(cx);
            return;
        }

        let Some(entry) = self.tree_state.read(cx).selected_entry().cloned() else {
            return;
        };

        let item_id = entry.item().id.to_string();
        let kind = parse_node_kind(&item_id);

        if matches!(
            kind,
            SchemaNodeKind::ConnectionFolder | SchemaNodeKind::Profile
        ) {
            self.pending_delete_item = Some(item_id);
            cx.notify();
        }
    }

    fn confirm_pending_delete(&mut self, cx: &mut Context<Self>) {
        let Some(item_id) = self.pending_delete_item.take() else {
            return;
        };

        self.execute_delete(&item_id, cx);
    }

    pub fn cancel_pending_delete(&mut self, cx: &mut Context<Self>) {
        if self.pending_delete_item.is_some() {
            self.pending_delete_item = None;
            cx.notify();
        }
    }

    pub fn has_pending_delete(&self) -> bool {
        self.pending_delete_item.is_some()
    }

    pub fn show_delete_confirm_modal(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let state = self.app_state.read(cx);

        let (item_name, is_folder) = match parse_node_id(item_id) {
            Some(SchemaNodeId::ConnectionFolder { node_id }) => {
                if let Some(node) = state.connection_tree().find_by_id(node_id) {
                    (node.name.clone(), true)
                } else {
                    return;
                }
            }
            Some(SchemaNodeId::Profile { profile_id }) => {
                if let Some(profile) = state.profiles().iter().find(|p| p.id == profile_id) {
                    (profile.name.clone(), false)
                } else {
                    return;
                }
            }
            _ => return,
        };

        self.delete_confirm_modal = Some(DeleteConfirmState {
            item_id: item_id.to_string(),
            item_name,
            is_folder,
        });
        cx.notify();
    }

    pub fn confirm_modal_delete(&mut self, cx: &mut Context<Self>) {
        let Some(modal) = self.delete_confirm_modal.take() else {
            return;
        };

        self.execute_delete(&modal.item_id, cx);
    }

    pub fn cancel_modal_delete(&mut self, cx: &mut Context<Self>) {
        if self.delete_confirm_modal.is_some() {
            self.delete_confirm_modal = None;
            cx.notify();
        }
    }

    pub fn has_delete_modal(&self) -> bool {
        self.delete_confirm_modal.is_some()
    }

    pub fn delete_modal_info(&self) -> Option<(&str, bool)> {
        self.delete_confirm_modal
            .as_ref()
            .map(|m| (m.item_name.as_str(), m.is_folder))
    }

    pub(super) fn execute_delete(&mut self, item_id: &str, cx: &mut Context<Self>) {
        match parse_node_id(item_id) {
            Some(SchemaNodeId::ConnectionFolder { .. }) => {
                self.delete_folder_from_context(item_id, cx);
            }
            Some(SchemaNodeId::Profile { profile_id }) => {
                self.delete_profile(profile_id, cx);
            }
            _ => {}
        }

        self.refresh_tree(cx);
    }
}
