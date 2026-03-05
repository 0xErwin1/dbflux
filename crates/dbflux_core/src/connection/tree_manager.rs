use crate::{ConnectionTree, ConnectionTreeNode, ConnectionTreeStore};
use log::{error, info};
use uuid::Uuid;

pub struct ConnectionTreeManager {
    pub tree: ConnectionTree,
    store: Option<ConnectionTreeStore>,
}

impl ConnectionTreeManager {
    pub fn new() -> Self {
        let (store, tree) = match ConnectionTreeStore::new() {
            Ok(store) => {
                let tree = store.load().unwrap_or_else(|e| {
                    error!("Failed to load connection tree: {:?}", e);
                    ConnectionTree::new()
                });
                info!("Loaded connection tree with {} nodes", tree.nodes.len());
                (Some(store), tree)
            }
            Err(e) => {
                error!("Failed to create connection tree store: {:?}", e);
                (None, ConnectionTree::new())
            }
        };

        Self { tree, store }
    }

    pub fn sync_with_profiles(&mut self, profile_ids: &[Uuid]) {
        let nodes_before = self.tree.nodes.len();
        self.tree.sync_with_profiles(profile_ids);
        let nodes_after = self.tree.nodes.len();

        if nodes_before != nodes_after {
            self.save();
            info!(
                "Synced connection tree: {} -> {} nodes",
                nodes_before, nodes_after
            );
        }
    }

    pub fn save(&self) {
        let Some(ref store) = self.store else {
            log::warn!("Cannot save connection tree: store not available");
            return;
        };

        if let Err(e) = store.save(&self.tree) {
            error!("Failed to save connection tree: {:?}", e);
        } else {
            info!("Saved connection tree with {} nodes", self.tree.nodes.len());
        }
    }

    pub fn add_profile_node(&mut self, profile_id: Uuid, folder_id: Option<Uuid>) {
        if self.tree.find_by_profile(profile_id).is_none() {
            let sort_index = self.tree.next_sort_index(folder_id);
            let node = ConnectionTreeNode::new_connection_ref(profile_id, folder_id, sort_index);
            self.tree.add_node(node);
            self.save();
        }
    }

    pub fn remove_profile_node(&mut self, profile_id: Uuid) {
        if let Some(node) = self.tree.find_by_profile(profile_id) {
            let node_id = node.id;
            self.tree.remove_node(node_id);
            self.save();
        }
    }

    pub fn create_folder(&mut self, name: impl Into<String>, parent_id: Option<Uuid>) -> Uuid {
        let sort_index = self.tree.next_sort_index(parent_id);
        let folder = ConnectionTreeNode::new_folder(name, parent_id, sort_index);
        let folder_id = folder.id;
        self.tree.add_node(folder);
        self.save();
        folder_id
    }

    pub fn rename_folder(&mut self, folder_id: Uuid, new_name: impl Into<String>) -> bool {
        if self.tree.rename_folder(folder_id, new_name) {
            self.save();
            true
        } else {
            false
        }
    }

    pub fn delete_folder(&mut self, folder_id: Uuid) -> Vec<Uuid> {
        let moved = self.tree.delete_folder_and_reparent_children(folder_id);

        if !moved.is_empty() || self.tree.find_by_id(folder_id).is_none() {
            self.save();
        }

        moved
    }

    pub fn move_node(&mut self, node_id: Uuid, new_parent_id: Option<Uuid>) -> bool {
        if self.tree.move_node(node_id, new_parent_id) {
            self.save();
            true
        } else {
            false
        }
    }

    pub fn move_node_to_position(
        &mut self,
        node_id: Uuid,
        new_parent_id: Option<Uuid>,
        after_id: Option<Uuid>,
    ) -> bool {
        if self
            .tree
            .move_node_to_position(node_id, new_parent_id, after_id)
        {
            self.save();
            true
        } else {
            false
        }
    }

    #[allow(dead_code)]
    pub fn toggle_folder_collapsed(&mut self, folder_id: Uuid) -> Option<bool> {
        let result = self.tree.toggle_folder_collapsed(folder_id);
        if result.is_some() {
            self.save();
        }
        result
    }

    pub fn set_folder_collapsed(&mut self, folder_id: Uuid, collapsed: bool) {
        self.tree.set_folder_collapsed(folder_id, collapsed);
        self.save();
    }
}

impl Default for ConnectionTreeManager {
    fn default() -> Self {
        Self::new()
    }
}
