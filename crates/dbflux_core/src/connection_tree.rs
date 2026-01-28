use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The kind of node in the connection tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionTreeNodeKind {
    /// A folder that can contain other folders or connection references.
    Folder,

    /// A reference to a connection profile.
    ConnectionRef,
}

/// A node in the connection tree hierarchy.
///
/// Nodes can be either folders (which can contain children) or references
/// to connection profiles. The tree structure is stored flat with parent
/// references, allowing flexible nesting while maintaining simple persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionTreeNode {
    /// Unique identifier for this node.
    pub id: Uuid,

    /// The type of this node (folder or connection reference).
    pub kind: ConnectionTreeNodeKind,

    /// Parent folder ID. `None` means this node is at the root level.
    pub parent_id: Option<Uuid>,

    /// Sort index for ordering siblings. Uses gaps (e.g., 1000, 2000) for easy insertion.
    pub sort_index: i32,

    /// Display name for folders. Ignored for connection references (the profile name is used).
    pub name: String,

    /// For `ConnectionRef` nodes, the ID of the referenced profile.
    /// `None` for folder nodes.
    pub profile_id: Option<Uuid>,

    /// Whether this folder is collapsed in the UI. Only relevant for folder nodes.
    #[serde(default)]
    pub collapsed: bool,
}

impl ConnectionTreeNode {
    /// Creates a new folder node.
    pub fn new_folder(name: impl Into<String>, parent_id: Option<Uuid>, sort_index: i32) -> Self {
        Self {
            id: Uuid::new_v4(),
            kind: ConnectionTreeNodeKind::Folder,
            parent_id,
            sort_index,
            name: name.into(),
            profile_id: None,
            collapsed: false,
        }
    }

    /// Creates a new connection reference node.
    pub fn new_connection_ref(profile_id: Uuid, parent_id: Option<Uuid>, sort_index: i32) -> Self {
        Self {
            id: Uuid::new_v4(),
            kind: ConnectionTreeNodeKind::ConnectionRef,
            parent_id,
            sort_index,
            name: String::new(),
            profile_id: Some(profile_id),
            collapsed: false,
        }
    }

    /// Returns `true` if this node is a folder.
    pub fn is_folder(&self) -> bool {
        self.kind == ConnectionTreeNodeKind::Folder
    }

    /// Returns `true` if this node is a connection reference.
    pub fn is_connection_ref(&self) -> bool {
        self.kind == ConnectionTreeNodeKind::ConnectionRef
    }
}

/// The connection tree structure containing all folder and connection nodes.
///
/// This tree organizes connection profiles into a hierarchical folder structure.
/// The structure is persisted separately from the profiles themselves, allowing
/// the same profile to exist even if the tree data is corrupted or missing.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConnectionTree {
    /// All nodes in the tree (flat list with parent references).
    pub nodes: Vec<ConnectionTreeNode>,

    /// Version number for potential future migrations.
    #[serde(default)]
    pub version: u32,
}

/// Gap between sort indices to allow easy insertion without reordering.
const SORT_INDEX_GAP: i32 = 1000;

impl ConnectionTree {
    /// Creates an empty connection tree.
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            version: 1,
        }
    }

    /// Returns all root-level nodes (nodes with no parent), sorted by sort_index.
    pub fn root_nodes(&self) -> Vec<&ConnectionTreeNode> {
        let mut nodes: Vec<_> = self
            .nodes
            .iter()
            .filter(|n| n.parent_id.is_none())
            .collect();

        nodes.sort_by_key(|n| n.sort_index);
        nodes
    }

    /// Returns all direct children of a given parent, sorted by sort_index.
    pub fn children_of(&self, parent_id: Uuid) -> Vec<&ConnectionTreeNode> {
        let mut children: Vec<_> = self
            .nodes
            .iter()
            .filter(|n| n.parent_id == Some(parent_id))
            .collect();

        children.sort_by_key(|n| n.sort_index);
        children
    }

    /// Finds a node by its profile ID (for connection references).
    pub fn find_by_profile(&self, profile_id: Uuid) -> Option<&ConnectionTreeNode> {
        self.nodes
            .iter()
            .find(|n| n.profile_id == Some(profile_id))
    }

    /// Finds a node by its profile ID (mutable reference).
    pub fn find_by_profile_mut(&mut self, profile_id: Uuid) -> Option<&mut ConnectionTreeNode> {
        self.nodes
            .iter_mut()
            .find(|n| n.profile_id == Some(profile_id))
    }

    /// Finds a node by its node ID.
    pub fn find_by_id(&self, node_id: Uuid) -> Option<&ConnectionTreeNode> {
        self.nodes.iter().find(|n| n.id == node_id)
    }

    /// Finds a node by its node ID (mutable reference).
    pub fn find_by_id_mut(&mut self, node_id: Uuid) -> Option<&mut ConnectionTreeNode> {
        self.nodes.iter_mut().find(|n| n.id == node_id)
    }

    /// Adds a node to the tree.
    pub fn add_node(&mut self, node: ConnectionTreeNode) {
        self.nodes.push(node);
    }

    /// Removes a node and all its descendants from the tree.
    ///
    /// Returns the removed nodes (if any).
    pub fn remove_node(&mut self, node_id: Uuid) -> Vec<ConnectionTreeNode> {
        let mut removed = Vec::new();
        let mut to_remove = vec![node_id];

        while let Some(id) = to_remove.pop() {
            if let Some(pos) = self.nodes.iter().position(|n| n.id == id) {
                let node = self.nodes.remove(pos);

                // Queue children for removal
                for child in self.nodes.iter() {
                    if child.parent_id == Some(id) {
                        to_remove.push(child.id);
                    }
                }

                removed.push(node);
            }
        }

        removed
    }

    /// Calculates the next sort index for a new node under the given parent.
    ///
    /// Returns a sort index that places the new node after all existing siblings.
    pub fn next_sort_index(&self, parent_id: Option<Uuid>) -> i32 {
        let max_index = self
            .nodes
            .iter()
            .filter(|n| n.parent_id == parent_id)
            .map(|n| n.sort_index)
            .max()
            .unwrap_or(0);

        max_index + SORT_INDEX_GAP
    }

    /// Checks if moving a node to a new parent would create a cycle.
    ///
    /// Returns `true` if the move would create a cycle (i.e., the new parent
    /// is a descendant of the node being moved).
    pub fn would_create_cycle(&self, node_id: Uuid, new_parent_id: Option<Uuid>) -> bool {
        let Some(target_parent) = new_parent_id else {
            // Moving to root never creates a cycle
            return false;
        };

        // Check if target_parent is a descendant of node_id
        let mut current = Some(target_parent);

        while let Some(id) = current {
            if id == node_id {
                return true;
            }

            current = self.find_by_id(id).and_then(|n| n.parent_id);
        }

        false
    }

    /// Synchronizes the tree with a list of profile IDs.
    ///
    /// - Adds connection references for profiles that don't have nodes yet.
    /// - Removes connection reference nodes for profiles that no longer exist.
    ///
    /// This ensures the tree stays consistent with the actual profiles.
    pub fn sync_with_profiles(&mut self, profile_ids: &[Uuid]) {
        // Remove orphaned connection refs (profiles that no longer exist)
        self.nodes.retain(|node| {
            if let Some(profile_id) = node.profile_id {
                profile_ids.contains(&profile_id)
            } else {
                true // Keep folders
            }
        });

        // Add missing profiles as root-level connection refs
        for &profile_id in profile_ids {
            if self.find_by_profile(profile_id).is_none() {
                let sort_index = self.next_sort_index(None);
                let node = ConnectionTreeNode::new_connection_ref(profile_id, None, sort_index);
                self.add_node(node);
            }
        }
    }

    /// Returns all folder nodes in the tree.
    pub fn folders(&self) -> Vec<&ConnectionTreeNode> {
        self.nodes.iter().filter(|n| n.is_folder()).collect()
    }

    /// Moves a node to a new parent (or root if `new_parent_id` is `None`).
    ///
    /// Returns `true` if the move was successful, `false` if it would create a cycle.
    pub fn move_node(&mut self, node_id: Uuid, new_parent_id: Option<Uuid>) -> bool {
        if self.would_create_cycle(node_id, new_parent_id) {
            return false;
        }

        let sort_index = self.next_sort_index(new_parent_id);

        if let Some(node) = self.find_by_id_mut(node_id) {
            node.parent_id = new_parent_id;
            node.sort_index = sort_index;
            true
        } else {
            false
        }
    }

    /// Renames a folder node.
    ///
    /// Returns `true` if the folder was found and renamed.
    pub fn rename_folder(&mut self, folder_id: Uuid, new_name: impl Into<String>) -> bool {
        if let Some(node) = self.find_by_id_mut(folder_id) {
            if node.is_folder() {
                node.name = new_name.into();
                return true;
            }
        }
        false
    }

    /// Toggles the collapsed state of a folder.
    ///
    /// Returns the new collapsed state, or `None` if the folder wasn't found.
    pub fn toggle_folder_collapsed(&mut self, folder_id: Uuid) -> Option<bool> {
        if let Some(node) = self.find_by_id_mut(folder_id) {
            if node.is_folder() {
                node.collapsed = !node.collapsed;
                return Some(node.collapsed);
            }
        }
        None
    }

    /// Sets the collapsed state of a folder.
    pub fn set_folder_collapsed(&mut self, folder_id: Uuid, collapsed: bool) {
        if let Some(node) = self.find_by_id_mut(folder_id) {
            if node.is_folder() {
                node.collapsed = collapsed;
            }
        }
    }

    /// Gets all descendant node IDs of a folder (children, grandchildren, etc.).
    pub fn get_descendants(&self, folder_id: Uuid) -> Vec<Uuid> {
        let mut descendants = Vec::new();
        let mut to_visit = vec![folder_id];

        while let Some(id) = to_visit.pop() {
            for node in &self.nodes {
                if node.parent_id == Some(id) {
                    descendants.push(node.id);
                    if node.is_folder() {
                        to_visit.push(node.id);
                    }
                }
            }
        }

        descendants
    }

    /// Deletes a folder, moving its children to the folder's parent (or root).
    ///
    /// Returns the IDs of children that were moved.
    pub fn delete_folder_and_reparent_children(&mut self, folder_id: Uuid) -> Vec<Uuid> {
        let folder = match self.find_by_id(folder_id) {
            Some(f) if f.is_folder() => f.clone(),
            _ => return Vec::new(),
        };

        let parent_id = folder.parent_id;

        // First, collect children IDs and compute base sort index
        let children_ids: Vec<Uuid> = self
            .nodes
            .iter()
            .filter(|n| n.parent_id == Some(folder_id))
            .map(|n| n.id)
            .collect();

        // Get the max sort index at the new parent level
        let base_sort_index = self
            .nodes
            .iter()
            .filter(|n| n.parent_id == parent_id && n.id != folder_id)
            .map(|n| n.sort_index)
            .max()
            .unwrap_or(0);

        // Move all direct children to the folder's parent
        let mut offset = 0;
        for node in &mut self.nodes {
            if children_ids.contains(&node.id) {
                node.parent_id = parent_id;
                offset += SORT_INDEX_GAP;
                node.sort_index = base_sort_index + offset;
            }
        }

        // Remove the folder itself
        self.nodes.retain(|n| n.id != folder_id);

        children_ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_folder() {
        let folder = ConnectionTreeNode::new_folder("Test Folder", None, 1000);
        assert!(folder.is_folder());
        assert!(!folder.is_connection_ref());
        assert_eq!(folder.name, "Test Folder");
        assert!(folder.parent_id.is_none());
    }

    #[test]
    fn test_create_connection_ref() {
        let profile_id = Uuid::new_v4();
        let conn_ref = ConnectionTreeNode::new_connection_ref(profile_id, None, 1000);
        assert!(conn_ref.is_connection_ref());
        assert!(!conn_ref.is_folder());
        assert_eq!(conn_ref.profile_id, Some(profile_id));
    }

    #[test]
    fn test_tree_operations() {
        let mut tree = ConnectionTree::new();

        // Add a folder
        let folder = ConnectionTreeNode::new_folder("Folder 1", None, 1000);
        let folder_id = folder.id;
        tree.add_node(folder);

        // Add a connection ref inside the folder
        let profile_id = Uuid::new_v4();
        let conn_ref = ConnectionTreeNode::new_connection_ref(profile_id, Some(folder_id), 1000);
        tree.add_node(conn_ref);

        // Test root_nodes
        let roots = tree.root_nodes();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].id, folder_id);

        // Test children_of
        let children = tree.children_of(folder_id);
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].profile_id, Some(profile_id));

        // Test find_by_profile
        let found = tree.find_by_profile(profile_id);
        assert!(found.is_some());
        assert_eq!(found.unwrap().profile_id, Some(profile_id));
    }

    #[test]
    fn test_cycle_detection() {
        let mut tree = ConnectionTree::new();

        // Create: root -> folder1 -> folder2
        let folder1 = ConnectionTreeNode::new_folder("Folder 1", None, 1000);
        let folder1_id = folder1.id;
        tree.add_node(folder1);

        let folder2 = ConnectionTreeNode::new_folder("Folder 2", Some(folder1_id), 1000);
        let folder2_id = folder2.id;
        tree.add_node(folder2);

        // Moving folder1 into folder2 would create a cycle
        assert!(tree.would_create_cycle(folder1_id, Some(folder2_id)));

        // Moving folder2 to root is fine
        assert!(!tree.would_create_cycle(folder2_id, None));
    }

    #[test]
    fn test_sync_with_profiles() {
        let mut tree = ConnectionTree::new();

        let profile1 = Uuid::new_v4();
        let profile2 = Uuid::new_v4();
        let profile3 = Uuid::new_v4();

        // Add refs for profile1 and profile2
        tree.add_node(ConnectionTreeNode::new_connection_ref(profile1, None, 1000));
        tree.add_node(ConnectionTreeNode::new_connection_ref(profile2, None, 2000));

        // Sync with profile1 and profile3 (removes profile2, adds profile3)
        tree.sync_with_profiles(&[profile1, profile3]);

        assert!(tree.find_by_profile(profile1).is_some());
        assert!(tree.find_by_profile(profile2).is_none());
        assert!(tree.find_by_profile(profile3).is_some());
    }

    #[test]
    fn test_delete_folder_reparents_children() {
        let mut tree = ConnectionTree::new();

        // Create: root -> folder -> (conn_ref, subfolder)
        let folder = ConnectionTreeNode::new_folder("Parent Folder", None, 1000);
        let folder_id = folder.id;
        tree.add_node(folder);

        let profile_id = Uuid::new_v4();
        let conn_ref = ConnectionTreeNode::new_connection_ref(profile_id, Some(folder_id), 1000);
        let conn_ref_id = conn_ref.id;
        tree.add_node(conn_ref);

        let subfolder = ConnectionTreeNode::new_folder("Subfolder", Some(folder_id), 2000);
        let subfolder_id = subfolder.id;
        tree.add_node(subfolder);

        // Delete the parent folder
        let moved = tree.delete_folder_and_reparent_children(folder_id);

        // Folder should be gone
        assert!(tree.find_by_id(folder_id).is_none());

        // Children should be moved to root
        assert!(moved.contains(&conn_ref_id) || moved.contains(&subfolder_id));

        let conn = tree.find_by_id(conn_ref_id).unwrap();
        assert!(conn.parent_id.is_none());

        let sub = tree.find_by_id(subfolder_id).unwrap();
        assert!(sub.parent_id.is_none());
    }
}
