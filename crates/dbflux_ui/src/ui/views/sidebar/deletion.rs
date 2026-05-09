use super::*;

impl Sidebar {
    pub fn request_delete_selected(&mut self, cx: &mut Context<Self>) {
        if self.pending_delete_item.is_some() {
            self.confirm_pending_delete(cx);
            return;
        }

        // Batch path: more than one item is multi-selected → open the modal
        // with the full set so a single confirmation deletes them all.
        let multi_ids = self.deletable_multi_selection();
        if multi_ids.len() > 1 {
            self.show_delete_confirm_modal_for_many(multi_ids, cx);
            return;
        }

        let Some(entry) = self.active_tree_state().read(cx).selected_entry().cloned() else {
            return;
        };

        let item_id = entry.item().id.to_string();
        let kind = parse_node_kind(&item_id);

        if matches!(
            kind,
            SchemaNodeKind::ConnectionFolder
                | SchemaNodeKind::Profile
                | SchemaNodeKind::ScriptFile
                | SchemaNodeKind::ScriptsFolder
        ) {
            // Don't allow deleting the scripts root folder
            if let Some(SchemaNodeId::ScriptsFolder { path: None }) = parse_node_id(&item_id) {
                return;
            }
            self.pending_delete_item = Some(item_id);
            cx.notify();
        }
    }

    /// Returns ids in the active multi-selection that point to user-deletable
    /// nodes (profiles, connection folders, script files, script folders).
    /// Schema nodes (tables/views/databases) and the scripts root are filtered
    /// out so a batch delete never accidentally hits a DDL drop or the root.
    pub(super) fn deletable_multi_selection(&self) -> Vec<String> {
        self.active_selection()
            .iter()
            .filter(|id| {
                let kind = parse_node_kind(id);
                if !matches!(
                    kind,
                    SchemaNodeKind::ConnectionFolder
                        | SchemaNodeKind::Profile
                        | SchemaNodeKind::ScriptFile
                        | SchemaNodeKind::ScriptsFolder
                ) {
                    return false;
                }

                // The scripts root has no path and is not deletable.
                !matches!(
                    parse_node_id(id),
                    Some(SchemaNodeId::ScriptsFolder { path: None })
                )
            })
            .cloned()
            .collect()
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
            Some(SchemaNodeId::ScriptFile { ref path }) => {
                let name = std::path::Path::new(path)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| path.clone());
                (name, false)
            }
            Some(SchemaNodeId::ScriptsFolder { path: Some(ref p) }) => {
                let name = std::path::Path::new(p)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| p.clone());
                (name, true)
            }
            _ => return,
        };

        self.delete_confirm_modal = Some(DeleteConfirmState {
            item_id: item_id.to_string(),
            item_name,
            is_folder,
            object_type: None,
            is_ddl: false,
            multi_item_ids: Vec::new(),
        });
        cx.notify();
    }

    /// Open the delete confirmation modal for a batch of sidebar selections.
    /// The first id acts as the visual anchor (its name is shown as a hint);
    /// confirm runs `execute_delete` for every id.
    pub(super) fn show_delete_confirm_modal_for_many(
        &mut self,
        ids: Vec<String>,
        cx: &mut Context<Self>,
    ) {
        let count = ids.len();
        let anchor_id = ids.first().cloned().unwrap_or_default();

        self.delete_confirm_modal = Some(DeleteConfirmState {
            item_id: anchor_id,
            item_name: format!("{count} items"),
            is_folder: false,
            object_type: None,
            is_ddl: false,
            multi_item_ids: ids,
        });
        cx.notify();
    }

    /// Show a DDL drop confirmation modal for schema objects (table, view,
    /// collection, database).
    pub fn show_ddl_confirm_modal(
        &mut self,
        item_id: &str,
        object_type: &str,
        cx: &mut Context<Self>,
    ) {
        let item_name = match parse_node_id(item_id) {
            Some(SchemaNodeId::Table { name, .. })
            | Some(SchemaNodeId::View { name, .. })
            | Some(SchemaNodeId::Collection { name, .. })
            | Some(SchemaNodeId::Database { name, .. }) => name,
            _ => return,
        };

        self.delete_confirm_modal = Some(DeleteConfirmState {
            item_id: item_id.to_string(),
            item_name,
            is_folder: false,
            object_type: Some(object_type.to_string()),
            is_ddl: true,
            multi_item_ids: Vec::new(),
        });
        cx.notify();
    }

    pub fn confirm_modal_delete(&mut self, cx: &mut Context<Self>) {
        let Some(modal) = self.delete_confirm_modal.take() else {
            return;
        };

        if !modal.multi_item_ids.is_empty() {
            for id in &modal.multi_item_ids {
                self.execute_delete(id, cx);
            }
            self.clear_selection(cx);
            return;
        }

        if modal.is_ddl {
            self.execute_drop_ddl(&modal.item_id, cx);
        } else {
            self.execute_delete(&modal.item_id, cx);
        }
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

    /// Returns full delete modal state for DDL-aware rendering.
    pub fn delete_modal_state(&self) -> Option<DeleteModalState<'_>> {
        self.delete_confirm_modal
            .as_ref()
            .map(|m| DeleteModalState {
                item_name: &m.item_name,
                is_folder: m.is_folder,
                is_ddl: m.is_ddl,
                object_type: m.object_type.as_deref(),
                multi_count: (!m.multi_item_ids.is_empty()).then_some(m.multi_item_ids.len()),
            })
    }

    pub(super) fn execute_delete(&mut self, item_id: &str, cx: &mut Context<Self>) {
        match parse_node_id(item_id) {
            Some(SchemaNodeId::ConnectionFolder { .. }) => {
                self.delete_folder_from_context(item_id, cx);
            }
            Some(SchemaNodeId::Profile { profile_id }) => {
                self.delete_profile(profile_id, cx);
            }
            Some(SchemaNodeId::ScriptFile { path }) => {
                self.delete_script(std::path::Path::new(&path), cx);
                return;
            }
            Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                self.delete_script(std::path::Path::new(&p), cx);
                return;
            }
            _ => {}
        }

        self.refresh_tree(cx);
    }
}
