use crate::connection::TreeStore;
use crate::{ConnectionTree, ConnectionTreeNode};
use log::{error, info};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TreeLoadState {
    Loaded,
    Failed,
}

pub struct ConnectionTreeManager {
    pub tree: ConnectionTree,
    store: Option<Box<dyn TreeStore>>,
    load_state: TreeLoadState,
}

impl ConnectionTreeManager {
    /// Creates an empty in-memory manager.
    pub fn new() -> Self {
        Self {
            tree: ConnectionTree::new(),
            store: None,
            load_state: TreeLoadState::Loaded,
        }
    }

    /// Creates a manager with a caller-supplied store.
    pub fn with_store(store: Box<dyn TreeStore>) -> Self {
        let (tree, load_state) = match store.load() {
            Ok(tree) => (tree, TreeLoadState::Loaded),
            Err(e) => {
                error!("Failed to load connection tree: {:?}", e);
                (ConnectionTree::new(), TreeLoadState::Failed)
            }
        };

        info!("Loaded connection tree with {} nodes", tree.nodes.len());

        Self {
            tree,
            store: Some(store),
            load_state,
        }
    }

    pub fn sync_with_profiles(&mut self, profile_ids: &[Uuid]) {
        let nodes_before = self.tree.nodes.len();
        self.tree.sync_with_profiles(profile_ids);
        let nodes_after = self.tree.nodes.len();

        if nodes_before != nodes_after {
            if self.load_state == TreeLoadState::Failed {
                error!(
                    "Connection tree sync added fallback nodes after a load failure; skipping automatic save to avoid overwriting persisted folder layout"
                );
                return;
            }

            self.save();
            info!(
                "Synced connection tree: {} -> {} nodes",
                nodes_before, nodes_after
            );
        }
    }

    pub fn save(&self) {
        if let Some(ref store) = self.store {
            if let Err(e) = store.save(&self.tree) {
                error!("Failed to save connection tree: {:?}", e);
            } else {
                info!("Saved connection tree with {} nodes", self.tree.nodes.len());
            }
        } else {
            log::warn!("Cannot save connection tree: store not available");
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use crate::DbError;

    struct RecordingTreeStore {
        loaded_tree: ConnectionTree,
        should_fail_load: bool,
        saved_trees: Arc<Mutex<Vec<ConnectionTree>>>,
    }

    impl RecordingTreeStore {
        fn successful(tree: ConnectionTree, saved_trees: Arc<Mutex<Vec<ConnectionTree>>>) -> Self {
            Self {
                loaded_tree: tree,
                should_fail_load: false,
                saved_trees,
            }
        }

        fn failing(saved_trees: Arc<Mutex<Vec<ConnectionTree>>>) -> Self {
            Self {
                loaded_tree: ConnectionTree::new(),
                should_fail_load: true,
                saved_trees,
            }
        }
    }

    impl TreeStore for RecordingTreeStore {
        fn load(&self) -> Result<ConnectionTree, DbError> {
            if self.should_fail_load {
                Err(DbError::query_failed("load failed"))
            } else {
                Ok(self.loaded_tree.clone())
            }
        }

        fn save(&self, tree: &ConnectionTree) -> Result<(), DbError> {
            self.saved_trees
                .lock()
                .expect("saved trees lock")
                .push(tree.clone());
            Ok(())
        }
    }

    #[test]
    fn sync_with_profiles_persists_changes_after_successful_load() {
        let saved_trees = Arc::new(Mutex::new(Vec::new()));
        let store = RecordingTreeStore::successful(ConnectionTree::new(), saved_trees.clone());
        let mut manager = ConnectionTreeManager::with_store(Box::new(store));

        manager.sync_with_profiles(&[Uuid::new_v4()]);

        assert_eq!(saved_trees.lock().expect("saved trees lock").len(), 1);
    }

    #[test]
    fn sync_with_profiles_skips_persisting_fallback_tree_after_load_failure() {
        let saved_trees = Arc::new(Mutex::new(Vec::new()));
        let store = RecordingTreeStore::failing(saved_trees.clone());
        let mut manager = ConnectionTreeManager::with_store(Box::new(store));

        manager.sync_with_profiles(&[Uuid::new_v4()]);

        assert!(saved_trees.lock().expect("saved trees lock").is_empty());
        assert_eq!(manager.tree.nodes.len(), 1);
    }
}
